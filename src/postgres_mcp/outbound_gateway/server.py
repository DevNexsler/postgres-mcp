"""Focused MCP surface and runtime assembly for outbound actions."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any
from uuid import uuid5

from mcp.server.fastmcp import FastMCP
from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.responses import PlainTextResponse

from postgres_mcp.sql import DbConnPool
from postgres_mcp.sql import SqlDriver

from .adapters.calendar import CalendarAdapter
from .adapters.cliq import CliqAdapter
from .adapters.email import EmailAdapter
from .adapters.quo import QuoSmsAdapter
from .context import ACTION_NAMESPACE
from .context import ActionContextLoader
from .context import RoutingPolicy
from .evidence import DatabasePreflightEvidenceLoader
from .metrics import GatewayObservability
from .metrics import render_prometheus
from .models import ExecuteRequest
from .models import Operation
from .models import PublicResult
from .models import PublicStatus
from .models import StatusRequest
from .models import parse_outbound_request
from .provider_client import McpProviderClient
from .provider_client import McpServerConfig
from .repository import OutboundGatewayRepository
from .service import OutboundActionService
from .store import PostgresActionStore
from .worker import OutboundWorker

DEFAULT_EMAIL_SENDER_DOMAINS = {"nigel-zoho": "pfg.io"}
DEFAULT_EMAIL_CC_BY_SOURCE = {
    "zillow": "management@pfg.io",
    "hotpads": "management@pfg.io",
}


@dataclass(frozen=True)
class FeaturePolicy:
    writes_enabled: bool
    kill_switch: bool


@dataclass(frozen=True)
class GatewayRuntime:
    pool: DbConnPool
    service: OutboundActionService
    store: PostgresActionStore
    policy: FeaturePolicy
    observability: GatewayObservability


async def handle_outbound_action(
    service: OutboundActionService,
    policy: FeaturePolicy,
    request: dict[str, Any],
) -> dict[str, Any]:
    try:
        parsed = parse_outbound_request(request)
    except ValidationError as exc:
        raise ValueError("invalid outbound action request") from exc
    if isinstance(parsed, StatusRequest):
        result = await service.status(parsed.action_id)
    else:
        assert isinstance(parsed, ExecuteRequest)
        if not policy.writes_enabled or policy.kill_switch:
            detail = "kill_switch_open" if policy.kill_switch else "writes_disabled"
            action_id = uuid5(
                ACTION_NAMESPACE,
                f"v1:wakeup:{parsed.wakeup_event_id}:role:{parsed.action_role}:ordinal:0",
            )
            result = PublicResult(
                status=PublicStatus.REJECTED,
                action_id=action_id,
                action_uid=None,
                provider_request_ref=None,
                retryable=False,
                detail_code=detail,
            )
        else:
            result = await service.execute(parsed)
    return result.model_dump(mode="json")


def create_server(
    service: OutboundActionService,
    policy: FeaturePolicy,
    *,
    observability: GatewayObservability | None = None,
) -> FastMCP:
    mcp = FastMCP(
        "comm-outbound-gateway",
        instructions="One durable provider-neutral outbound action tool.",
        host="127.0.0.1",
        port=8094,
        streamable_http_path="/mcp",
        json_response=True,
    )

    @mcp.tool(
        name="outbound_action",
        description=(
            "Execute or inspect one durable outbound email, Quo, Cliq, or calendar action. "
            "Recipients, accounts, and provider targets are derived from wakeup_event_id."
        ),
        structured_output=True,
    )
    async def outbound_action(request: dict[str, Any]) -> dict[str, Any]:
        return await handle_outbound_action(service, policy, request)

    @mcp.resource("health://outbound-gateway", name="outbound-gateway-health")
    def health() -> str:
        return json.dumps(
            {
                "status": "ok",
                "writes_enabled": policy.writes_enabled,
                "kill_switch": policy.kill_switch,
            },
            sort_keys=True,
        )

    if observability is not None:

        @mcp.custom_route("/healthz", methods=["GET"], include_in_schema=False)
        async def healthz(_request: Request):
            healthy = await observability.database_healthy()
            return JSONResponse(
                {
                    "status": "ok" if healthy else "unhealthy",
                    "writes_enabled": policy.writes_enabled,
                    "kill_switch": policy.kill_switch,
                },
                status_code=200 if healthy else 503,
            )

        @mcp.custom_route("/metrics", methods=["GET"], include_in_schema=False)
        async def metrics(_request: Request):
            return PlainTextResponse(
                render_prometheus(await observability.collect()),
                media_type="text/plain; version=0.0.4",
            )

    return mcp


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    if raw.casefold() in {"1", "true", "yes", "on"}:
        return True
    if raw.casefold() in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _json_mapping(name: str, default: dict[str, str]) -> dict[str, str]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = json.loads(raw)
    if not isinstance(value, dict) or not all(isinstance(key, str) and isinstance(item, str) for key, item in value.items()):
        raise ValueError(f"{name} must be a JSON string-to-string object")
    return value


def _bearer_headers(name: str) -> dict[str, str]:
    token = os.environ.get(name, "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


async def build_runtime() -> GatewayRuntime:
    database_uri = os.environ.get("DATABASE_URI")
    if not database_uri:
        raise ValueError("DATABASE_URI is required")
    pool = DbConnPool(database_uri)
    await pool.pool_connect()
    driver = SqlDriver(conn=pool)
    policy = FeaturePolicy(
        writes_enabled=_bool("OUTBOUND_GATEWAY_WRITES_ENABLED", False),
        kill_switch=_bool("OUTBOUND_GATEWAY_KILL_SWITCH", True),
    )
    routing = RoutingPolicy(
        version=os.environ.get("OUTBOUND_ROUTING_POLICY_VERSION", "appointment-v1"),
        email_account_by_provider=_json_mapping(
            "OUTBOUND_EMAIL_ACCOUNTS_JSON",
            {"zillow": "nigel-zoho", "hotpads": "nigel-zoho", "tenantcloud": "nigel-zoho"},
        ),
        quo_line_by_provider={"quo": os.environ.get("OUTBOUND_QUO_PHONE_NUMBER_ID", "")},
        calendar_by_profile={"appointment-setter": os.environ.get("OUTBOUND_CALENDAR_NAME", "nigel")},
        calendar_account_by_profile={"appointment-setter": os.environ.get("OUTBOUND_CALENDAR_ACCOUNT", "nigel-zoho")},
        cliq_target_by_intent=_json_mapping(
            "OUTBOUND_CLIQ_TARGETS_JSON",
            {"lead_alert": "tenant-leads", "manual_review_alert": "tenant-leads"},
        ),
        property_aliases=_json_mapping("OUTBOUND_PROPERTY_ALIASES_JSON", {}),
        conversation_aliases=_json_mapping("OUTBOUND_CONVERSATION_ALIASES_JSON", {}),
    )
    context_repository = OutboundGatewayRepository(driver)
    store = PostgresActionStore(driver)
    observability = GatewayObservability(
        driver,
        circuit_failure_threshold=int(os.environ.get("OUTBOUND_CIRCUIT_FAILURE_THRESHOLD", "5")),
        circuit_window_seconds=int(os.environ.get("OUTBOUND_CIRCUIT_WINDOW_SECONDS", "300")),
        circuit_open_seconds=int(os.environ.get("OUTBOUND_CIRCUIT_OPEN_SECONDS", "180")),
        old_action_seconds=int(os.environ.get("OUTBOUND_ALERT_OLD_ACTION_SECONDS", "300")),
        evidence_failure_threshold=int(os.environ.get("OUTBOUND_ALERT_EVIDENCE_FAILURE_THRESHOLD", "3")),
        alert_window_seconds=int(os.environ.get("OUTBOUND_ALERT_WINDOW_SECONDS", "300")),
    )
    provider_client = McpProviderClient(
        {
            "agent-email": McpServerConfig(
                name="agent-email",
                url=os.environ.get("AGENT_EMAIL_MCP_URL", "http://127.0.0.1:9090/mcp"),
                transport="streamable_http",
                headers=_bearer_headers("EMAIL_MCP_TOKEN"),
                allowed_tools=frozenset(
                    {
                        "email_send",
                        "email_get_thread",
                        "request_status",
                        "cliq_channel_bot_post",
                        "cliq_chat_post",
                        "calendar_create_event",
                        "calendar_update_event",
                        "calendar_delete_event",
                    }
                ),
            ),
            "quo": McpServerConfig(
                name="quo",
                url=os.environ.get("QUO_MCP_URL", "http://127.0.0.1:8080/sse"),
                transport="sse",
                headers=_bearer_headers("QUO_MCP_TOKEN"),
                allowed_tools=frozenset({"send_message", "list_messages", "get_message"}),
            ),
        }
    )
    email_domains = _json_mapping(
        "OUTBOUND_EMAIL_SENDER_DOMAINS_JSON",
        {
            "nigel-zoho": os.environ.get(
                "OUTBOUND_DEFAULT_EMAIL_DOMAIN",
                DEFAULT_EMAIL_SENDER_DOMAINS["nigel-zoho"],
            )
        },
    )
    email_cc_by_source = _json_mapping(
        "OUTBOUND_EMAIL_CC_BY_SOURCE_JSON",
        DEFAULT_EMAIL_CC_BY_SOURCE,
    )
    calendar_accounts = {routing.calendar_by_profile["appointment-setter"]: routing.calendar_account_by_profile["appointment-setter"]}
    adapters = {
        Operation.EMAIL_SEND: EmailAdapter(
            sender_domains=email_domains,
            cc_by_source=email_cc_by_source,
        ),
        Operation.QUO_SMS_SEND: QuoSmsAdapter(user_id=os.environ.get("OUTBOUND_QUO_USER_ID", "gateway")),
        Operation.CLIQ_CHANNEL_POST: CliqAdapter(Operation.CLIQ_CHANNEL_POST),
        Operation.CLIQ_CHAT_POST: CliqAdapter(Operation.CLIQ_CHAT_POST),
        Operation.CALENDAR_CREATE: CalendarAdapter(account_by_calendar=calendar_accounts),
        Operation.CALENDAR_UPDATE: CalendarAdapter(account_by_calendar=calendar_accounts),
        Operation.CALENDAR_DELETE: CalendarAdapter(account_by_calendar=calendar_accounts),
    }
    service = OutboundActionService(
        store=store,
        context_loader=ActionContextLoader(context_repository, routing),
        evidence_loader=DatabasePreflightEvidenceLoader(driver),
        adapters=adapters,
        provider_client=provider_client,
        clock=lambda: datetime.now(timezone.utc),
        lease_owner=os.environ.get("OUTBOUND_GATEWAY_LEASE_OWNER", "outbound-gateway"),
        circuit_guard=observability,
        retry_base_seconds=int(os.environ.get("OUTBOUND_RETRY_BASE_SECONDS", "5")),
        retry_max_seconds=int(os.environ.get("OUTBOUND_RETRY_MAX_SECONDS", "900")),
    )
    return GatewayRuntime(
        pool=pool,
        service=service,
        store=store,
        policy=policy,
        observability=observability,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="comm-outbound-gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8094)
    return parser


async def _serve() -> None:
    args = _parser().parse_args()
    runtime = await build_runtime()
    mcp = create_server(
        runtime.service,
        runtime.policy,
        observability=runtime.observability,
    )
    mcp.settings.host = args.host
    mcp.settings.port = args.port
    try:
        await mcp.run_streamable_http_async()
    finally:
        await runtime.pool.close()


def main() -> None:
    asyncio.run(_serve())


async def _work() -> None:
    runtime = await build_runtime()
    worker = OutboundWorker(
        store=runtime.store,
        service=runtime.service,
        batch_size=int(os.environ.get("OUTBOUND_WORKER_BATCH_SIZE", "20")),
        max_attempts=int(os.environ.get("OUTBOUND_MAX_ATTEMPTS", "5")),
        observability=runtime.observability,
    )
    interval = max(1.0, float(os.environ.get("OUTBOUND_WORKER_INTERVAL_SECONDS", "5")))
    try:
        while True:
            if runtime.policy.writes_enabled and not runtime.policy.kill_switch:
                await worker.run_once()
            await asyncio.sleep(interval)
    finally:
        await runtime.pool.close()


def worker_main() -> None:
    asyncio.run(_work())
