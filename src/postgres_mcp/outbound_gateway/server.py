"""Focused MCP surface and runtime assembly for outbound actions."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from types import ModuleType
from typing import Any
from typing import Coroutine
from uuid import uuid5

from mcp.server.fastmcp import FastMCP
from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.responses import PlainTextResponse

from postgres_mcp.sql import DbConnPool
from postgres_mcp.sql import SqlDriver

from .adapters.base import ProviderAdapter
from .adapters.calendar import CalendarAdapter
from .adapters.cliq import CliqAdapter
from .adapters.email import EmailAdapter
from .adapters.quo import QuoSmsAdapter
from .adapters.tenantcloud import TenantCloudAdapter
from .context import ACTION_NAMESPACE
from .context import ActionContextLoader
from .context import RoutingPolicy
from .delivery_workflow import RestateWorkflowSubmitter
from .delivery_workflow import SecretAuthGate
from .errors import FailureKind
from .errors import classify
from .errors import error_text
from .evidence import DatabasePreflightEvidenceLoader
from .metrics import GatewayObservability
from .metrics import render_prometheus
from .models import ActionRole
from .models import ConfirmRequest
from .models import ExecuteRequest
from .models import Operation
from .models import OutboundRequest
from .models import PublicResult
from .models import PublicStatus
from .models import RequestRefusedError
from .models import StatusRequest
from .models import SuggestRequest
from .models import normalize_target_email
from .models import normalize_target_phone
from .models import operation_catalog
from .models import parse_outbound_request
from .provider_client import McpProviderClient
from .provider_client import McpServerConfig
from .record import NEVER_ANOTHER_ROUTE
from .record import OVERRIDE_RULE
from .repository import OutboundGatewayRepository
from .service import OutboundActionService
from .store import PostgresActionStore
from .tenantcloud_shared import TENANTCLOUD_OPERATIONS
from .traffic_control import VALID_TRAFFIC_MODES
from .worker import OutboundWorker

logger = logging.getLogger(__name__)

# TenantCloud's API origin is a fixed literal, never a runtime-configurable
# value. Task 7's adapter and this module both depend on this exact string;
# nothing in this file ever reads an environment variable to build it.
TENANTCLOUD_ORIGIN = "https://api.tenantcloud.com"

# Defense in depth: these variable names do not correspond to anything this
# module reads to build the origin above. Their presence almost certainly
# means an operator believes they can retarget the TenantCloud origin through
# configuration. Fail closed and loudly instead of silently ignoring the
# attempt -- checked first, before any module loading or token acquisition.
_TENANTCLOUD_ORIGIN_OVERRIDE_ENV_VARS = (
    "TENANTCLOUD_API_BASE_URL",
    "TENANTCLOUD_API_SCHEME",
    "TENANTCLOUD_API_HOST",
    "TENANTCLOUD_API_PORT",
    "TENANTCLOUD_API_USERNAME",
    "TENANTCLOUD_API_PASSWORD",
    "TENANTCLOUD_API_QUERY",
    "TENANTCLOUD_API_FRAGMENT",
)

DEFAULT_EMAIL_SENDER_DOMAINS = {"nigel-zoho": "pfg.io"}
DEFAULT_EMAIL_CC_BY_SOURCE = {
    "zillow": "management@pfg.io",
    "hotpads": "management@pfg.io",
}
DEFAULT_PROPERTY_ALIASES = {
    "138 bullman street 144 a": "building:bullman-st",
    "144 bullman street": "building:bullman-st",
    "16 north main street 16": "building:16-n-main",
}
DEFAULT_ENABLED_OPERATIONS = frozenset({Operation.EMAIL_SEND})


@dataclass(frozen=True)
class FeaturePolicy:
    writes_enabled: bool
    kill_switch: bool
    enabled_operations: frozenset[Operation] = DEFAULT_ENABLED_OPERATIONS


@dataclass(frozen=True)
class GatewayRuntime:
    pool: DbConnPool
    service: OutboundActionService
    store: PostgresActionStore
    policy: FeaturePolicy
    observability: GatewayObservability
    tenantcloud_submitter: RestateWorkflowSubmitter | None = None
    restate_operations: frozenset[Operation] = frozenset()


async def handle_outbound_action(
    service: OutboundActionService,
    policy: FeaturePolicy,
    request: dict[str, Any],
    *,
    tenantcloud_submitter: RestateWorkflowSubmitter | None = None,
    restate_operations: frozenset[Operation] | None = None,
) -> dict[str, Any]:
    try:
        parsed = parse_outbound_request(request)
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "request"
        hint = ""
        if location.endswith("action_role"):
            hint = f" (valid: {', '.join(sorted(role.value for role in ActionRole))})"
        elif location.endswith("operation"):
            hint = f" (valid: {', '.join(sorted(op.value for op in Operation))})"
        # A malformed request is the caller's mistake, answered as an ordinary
        # result rather than an MCP tool error: hermes-agent counts tool errors
        # toward a breaker that parks the whole server for every caller
        # (#3264). No action exists yet, so there is no action_id to return.
        return {
            "status": PublicStatus.REJECTED.value,
            "retryable": False,
            "detail_code": "invalid_request",
            "detail": (
                f"invalid outbound action request: {location}: {first['msg']}{hint}. Nothing was sent: fix "
                "that field and send again; the tool description lists each operation's exact arguments."
            ),
        }
    try:
        result = await _route(
            service,
            policy,
            parsed,
            tenantcloud_submitter=tenantcloud_submitter,
            restate_operations=restate_operations,
        )
    except Exception as error:
        return _failure(error, parsed.op)
    if isinstance(result, dict):
        return result
    payload = result.model_dump(mode="json")
    # A plain send leaves detail unset, and every result except
    # needs_confirmation leaves new_context/question unset (override is set
    # only when nothing was sent) -- omit unset keys entirely so consumers
    # see no `null` field on the wire.
    for optional in ("detail", "new_context", "question", "override"):
        if payload.get(optional) is None:
            payload.pop(optional, None)
    return payload


async def _route(
    service: OutboundActionService,
    policy: FeaturePolicy,
    parsed: OutboundRequest,
    *,
    tenantcloud_submitter: RestateWorkflowSubmitter | None,
    restate_operations: frozenset[Operation] | None,
) -> PublicResult | dict[str, Any]:
    """The answer to one parsed request: suggestions, or an action's result."""
    if isinstance(parsed, SuggestRequest):
        return {
            "wakeup_event_id": parsed.wakeup_event_id,
            "suggestions": await service.suggest_targets(parsed.wakeup_event_id),
        }
    if isinstance(parsed, StatusRequest):
        result = await service.status(parsed.action_id)
    elif isinstance(parsed, ConfirmRequest):
        result = await _confirm(
            service,
            policy,
            parsed,
            tenantcloud_submitter=tenantcloud_submitter,
            restate_operations=restate_operations,
        )
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
                detail=SENDING_PAUSED_DETAIL,
            )
        elif parsed.operation not in policy.enabled_operations:
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
                detail_code="operation_disabled",
                detail=_operation_disabled_detail(parsed.operation, policy),
            )
        else:
            routed_operations = TENANTCLOUD_OPERATIONS if restate_operations is None else (TENANTCLOUD_OPERATIONS | restate_operations)
            if parsed.operation in routed_operations and tenantcloud_submitter is not None:
                # Stays fast: persist + preflight only (no provider I/O), then
                # fire-and-forget the durable Restate submission. Execute
                # returns "accepted, delivering" immediately either way -- the
                # 1h retry ceiling runs entirely in the background workflow, a
                # wake session never waits on it.
                result = await service.enqueue(parsed)
                if result.status is PublicStatus.PENDING:
                    try:
                        await tenantcloud_submitter.submit(result.action_id)
                    except Exception:
                        logger.exception(
                            "Restate delivery submission failed for action %s; CDS sweeper will retry",
                            result.action_id,
                        )
            else:
                result = await service.execute(parsed)
    return result


def _failure(error: Exception, op: str) -> dict[str, Any]:
    """Every failure is an ordinary result, never an MCP tool error:
    hermes-agent counts tool errors toward a breaker that parks the gateway
    for every caller (#3463), and raw error text reads to the agent as an
    outage to route around. A refusal (errors.classify) is the caller's to
    correct, in its own words; anything else is the gateway's. A row the
    call recorded is answered by the service itself (its action_id, and the
    override where nothing was sent), so reaching here the call may have
    failed before or after recording one."""
    if classify(error) is FailureKind.REFUSAL:
        return {
            "status": PublicStatus.REJECTED.value,
            "retryable": False,
            "detail_code": "request_refused",
            "detail": str(error) if isinstance(error, RequestRefusedError) else error_text(error),
        }
    logger.exception("outbound_action %s failed", op)
    return {
        "status": PublicStatus.FAILED.value,
        "retryable": False,
        "detail_code": "gateway_error",
        "detail": (
            f"The gateway hit an error ({error_text(error)}) before it could say what happened. The same "
            "call again is the same action, never a second send: make it again to see where it stands; if it "
            f"fails again, record needs_human. {NEVER_ANOTHER_ROUTE}"
        ),
    }


SENDING_PAUSED_DETAIL = (
    "Not sent: an operator has paused sending through this gateway, and nothing was recorded. Record "
    f"needs_human with what you meant to send. {NEVER_ANOTHER_ROUTE}"
)


def _operation_disabled_detail(operation: Operation, policy: FeaturePolicy) -> str:
    enabled = ", ".join(sorted(op.value for op in policy.enabled_operations)) or "none"
    return (
        f"Not sent: {operation.value} is not enabled on this gateway, and nothing was recorded. Enabled "
        f"operations: {enabled}. Use one of them, or record needs_human. {NEVER_ANOTHER_ROUTE}"
    )


async def _confirm(
    service: OutboundActionService,
    policy: FeaturePolicy,
    request: ConfirmRequest,
    *,
    tenantcloud_submitter: RestateWorkflowSubmitter | None,
    restate_operations: frozenset[Operation] | None = None,
) -> PublicResult:
    """Route a stale_context answer or an override. A decline is a ledger
    write only and is always accepted; a yes is a send and obeys the same
    write switches as execute. A Restate-routed successor is prepared here
    and handed to Restate, like enqueue()."""
    sends = request.decision.value != "no"
    if sends:
        if not policy.writes_enabled or policy.kill_switch:
            return PublicResult(
                status=PublicStatus.REJECTED,
                action_id=request.action_id,
                action_uid=None,
                provider_request_ref=None,
                retryable=False,
                detail_code="kill_switch_open" if policy.kill_switch else "writes_disabled",
                detail=SENDING_PAUSED_DETAIL,
            )
    parent_operation = await service.action_operation(request.action_id)
    if sends and parent_operation is not None and parent_operation not in policy.enabled_operations:
        return PublicResult(
            status=PublicStatus.REJECTED,
            action_id=request.action_id,
            action_uid=None,
            provider_request_ref=None,
            retryable=False,
            detail_code="operation_disabled",
            detail=_operation_disabled_detail(parent_operation, policy),
        )
    routed_operations = TENANTCLOUD_OPERATIONS if restate_operations is None else (TENANTCLOUD_OPERATIONS | restate_operations)
    routed = parent_operation in routed_operations and tenantcloud_submitter is not None
    result = await service.confirm(request, dispatch=not routed)
    if routed and result.status is PublicStatus.PENDING and tenantcloud_submitter is not None:
        try:
            await tenantcloud_submitter.submit(result.action_id)
        except Exception:
            logger.exception(
                "Restate delivery submission failed for action %s; CDS sweeper will retry",
                result.action_id,
            )
    return result


def create_server(
    service: OutboundActionService,
    policy: FeaturePolicy,
    *,
    observability: GatewayObservability | None = None,
    tenantcloud_submitter: RestateWorkflowSubmitter | None = None,
    restate_operations: frozenset[Operation] | None = None,
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
            "Execute or inspect one durable outbound email, Quo, Cliq, calendar, or "
            "TenantCloud action. You choose the target id (to_address, to_phone, "
            "channel_or_chat_id, calendar_id, thread_id, lead_id, etc.) as part of "
            "arguments -- it is never derived from wakeup_event_id for you. Use suggest "
            "({\"op\": \"suggest\", \"wakeup_event_id\"}) to ask what the wake implies "
            "-- it returns advisory target ids drawn from the wake, never blocks, and "
            "stays reachable even when writes are disabled. Its answer is a suggestion "
            "only: you may pass any target id you like to execute, including ones that "
            "disagree with suggest. If execute returns status needs_confirmation "
            "(detail_code stale_context), nothing was sent: read new_context -- the newer "
            "messages, including any we already sent to that recipient (direction \"sent by us\") -- and answer "
            "once with {\"op\": \"confirm\", \"wakeup_event_id\", \"action_id\", \"decision\": \"yes\"|\"no\"|\"revise\"} "
            "exactly as its question shows (revise also carries arguments with only the "
            "message content changed). Any other result that did not send (status rejected, "
            "failed, duplicate, manual_review or stale) says why in detail, and may carry "
            "\"override\": that confirm request with the wake and action filled in (decision \"no\" "
            "instead records that you chose not to send it). " + OVERRIDE_RULE + " A malformed request "
            "(detail_code invalid_request) recorded nothing: fix the field its detail names. "
            "Every send, from any wake, goes through this tool: "
            "{\"request\": {\"op\": \"execute\", \"wakeup_event_id\": <wake>, "
            "\"action_role\", \"operation\", \"intent_kind\", \"arguments\": {...}}}. "
            "The identical request again is the same action (never a second send); a "
            "different one is a new action. Ids are integers where the provider uses "
            "numbers. Operations (? = optional):\n" + operation_catalog()
        ),
        structured_output=True,
    )
    async def outbound_action(request: dict[str, Any]) -> dict[str, Any]:
        return await handle_outbound_action(
            service,
            policy,
            request,
            tenantcloud_submitter=tenantcloud_submitter,
            restate_operations=restate_operations,
        )

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


def _quo_sending_lines() -> dict[str, str]:
    """OUTBOUND_QUO_SENDING_LINES_JSON: the only lines quo.sms.send can
    text from (its from_phone), E.164 number -> Quo phone_number_id. Keys
    are normalized the way from_phone is, so the lookup matches what the
    agent sent."""
    lines = _json_mapping("OUTBOUND_QUO_SENDING_LINES_JSON", {})
    try:
        normalized = {normalize_target_phone(phone, field="phone"): line.strip() for phone, line in lines.items()}
    except ValueError as exc:
        raise ValueError("OUTBOUND_QUO_SENDING_LINES_JSON keys must be E.164 phone numbers") from exc
    if not all(normalized.values()):
        raise ValueError("OUTBOUND_QUO_SENDING_LINES_JSON values must be Quo phone_number_ids")
    return normalized


def _email_default_cc() -> str:
    """OUTBOUND_EMAIL_DEFAULT_CC: the address a customer email that omits cc
    is copied to (identity.with_default_cc). Unset or empty: off."""
    raw = os.environ.get("OUTBOUND_EMAIL_DEFAULT_CC", "").strip()
    return normalize_target_email(raw, field="OUTBOUND_EMAIL_DEFAULT_CC") if raw else ""


def _enabled_operations() -> frozenset[Operation]:
    raw = os.environ.get("OUTBOUND_ENABLED_OPERATIONS_JSON")
    if raw is None:
        return DEFAULT_ENABLED_OPERATIONS
    value = json.loads(raw)
    if not isinstance(value, list) or not value:
        raise ValueError("OUTBOUND_ENABLED_OPERATIONS_JSON must be a non-empty JSON array")
    try:
        return frozenset(Operation(item) for item in value if isinstance(item, str))
    except ValueError as exc:
        raise ValueError("OUTBOUND_ENABLED_OPERATIONS_JSON contains an unsupported operation") from exc




def _restate_operations() -> frozenset[Operation]:
    """``OUTBOUND_RESTATE_OPERATIONS``: which non-TenantCloud operations route
    through the generalized Restate delivery workflow instead of the legacy
    worker.py polling loop. Default empty -- today's behavior for SMS/email/
    Cliq/calendar is unchanged until an operation is named here. TenantCloud
    is never read from this: it stays on Restate unconditionally, exactly as
    before this flag existed (tenantcloud_delivery.py, worker.py's own
    TENANTCLOUD_OPERATIONS union)."""
    raw = os.environ.get("OUTBOUND_RESTATE_OPERATIONS")
    if raw is None or not raw.strip():
        return frozenset()
    value = json.loads(raw)
    if not isinstance(value, list):
        raise ValueError("OUTBOUND_RESTATE_OPERATIONS must be a JSON array")
    try:
        return frozenset(Operation(item) for item in value if isinstance(item, str))
    except ValueError as exc:
        raise ValueError("OUTBOUND_RESTATE_OPERATIONS contains an unsupported operation") from exc


def _traffic_mode() -> str:
    raw = os.environ.get("OUTBOUND_TRAFFIC_CONTROL", "shadow").casefold()
    if raw not in VALID_TRAFFIC_MODES:
        raise ValueError(f"OUTBOUND_TRAFFIC_CONTROL must be one of {sorted(VALID_TRAFFIC_MODES)}, got {raw!r}")
    return raw


def _bearer_headers(name: str) -> dict[str, str]:
    token = os.environ.get(name, "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _tenantcloud_enabled(enabled_operations: frozenset[Operation]) -> bool:
    return bool(enabled_operations & TENANTCLOUD_OPERATIONS)


def _reject_tenantcloud_origin_overrides() -> None:
    present = sorted(name for name in _TENANTCLOUD_ORIGIN_OVERRIDE_ENV_VARS if os.environ.get(name))
    if present:
        raise ValueError(
            "TenantCloud API origin is a fixed literal (" + TENANTCLOUD_ORIGIN + "); "
            "unsupported override variable(s) set: " + ", ".join(present)
        )


_TENANTCLOUD_MODULE_NAMES = ("tenantcloud_auth", "tenantcloud_client", "tenantcloud_mutations")


def _load_tenantcloud_modules(module_dir: str) -> tuple[ModuleType, ModuleType, ModuleType]:
    if not os.path.isdir(module_dir):
        raise ValueError(f"TenantCloud module directory not found: {module_dir} (is the CDS repo mounted at /repo?)")
    for name in _TENANTCLOUD_MODULE_NAMES:
        path = os.path.join(module_dir, f"{name}.py")
        if not os.path.isfile(path):
            raise ValueError(f"TenantCloud module not found: {path} (is the CDS repo mounted at /repo?)")

    # scripts/tenantcloud_mutations.py (Comm-Data-Store) imports
    # `from scripts.tenantcloud_client import ...` -- it belongs to that
    # repo's own `scripts` package layout. Rather than depend on
    # Comm-Data-Store as an installed package (a cross-repo dependency this
    # gateway does not otherwise have), stand up a private, process-local
    # `scripts` alias pointed at the mounted directory purely so that
    # internal import resolves, then restore whatever (if anything) was
    # already registered under that name so this cannot leak or collide.
    qualified_names = tuple(f"scripts.{name}" for name in _TENANTCLOUD_MODULE_NAMES)
    previous = {name: sys.modules.get(name) for name in ("scripts", *qualified_names)}
    package = ModuleType("scripts")
    package.__path__ = [module_dir]
    sys.modules["scripts"] = package
    for name in qualified_names:
        sys.modules.pop(name, None)
    try:
        modules = tuple(importlib.import_module(f"scripts.{name}") for name in _TENANTCLOUD_MODULE_NAMES)
    finally:
        for name, value in previous.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value
    return modules  # type: ignore[return-value]


def _build_tenantcloud_adapter() -> TenantCloudAdapter:
    # Ordering matters: reject any attempt to override the origin before
    # doing anything else -- in particular before reading, validating, or
    # opening any file, and long before any HTTP call that could acquire a
    # token.
    _reject_tenantcloud_origin_overrides()

    control_url = os.environ.get("TENANTCLOUD_RUNNER_CONTROL_URL", "").strip()
    bearer_file = os.environ.get("TENANTCLOUD_RUNNER_BEARER_FILE", "").strip()
    next_bearer_file = os.environ.get("TENANTCLOUD_RUNNER_NEXT_BEARER_FILE", "").strip() or None
    module_dir = os.environ.get("TENANTCLOUD_MODULE_DIR", "/repo/scripts")

    if not control_url:
        raise ValueError("TENANTCLOUD_RUNNER_CONTROL_URL is required while a TenantCloud operation is enabled")
    if not bearer_file:
        raise ValueError("TENANTCLOUD_RUNNER_BEARER_FILE is required while a TenantCloud operation is enabled")
    if not os.path.isfile(bearer_file):
        raise ValueError(f"TENANTCLOUD_RUNNER_BEARER_FILE does not exist: {bearer_file}")
    if next_bearer_file is not None and not os.path.isfile(next_bearer_file):
        raise ValueError(f"TENANTCLOUD_RUNNER_NEXT_BEARER_FILE does not exist: {next_bearer_file}")

    # Imported by file path, not by package name: the facade lives in a
    # different repository (Comm-Data-Store), mounted read-only at /repo in
    # the running container. This gateway cannot add a cross-repo Python
    # dependency on it.
    auth_module, client_module, mutations_module = _load_tenantcloud_modules(module_dir)

    # HttpRunnerControl itself enforces literal HTTP loopback (127.0.0.1 or
    # ::1, no credentials/query/fragment/path) at construction time, before
    # any request is made -- see scripts/tenantcloud_auth.py in Comm-Data-Store.
    control = auth_module.HttpRunnerControl(control_url, bearer_file, next_bearer_file)
    auth = auth_module.TenantCloudAuth("tenantcloud-runner", control=control, profile_access=False)

    # The control and auth objects are stateless with respect to token
    # lifetime and are safe to share. The CLIENT is not: it owns the
    # AuthRefreshBudget, which permits one refresh and then caches that token
    # for the budget's lifetime. That budget is scan-local by design, so it
    # must not outlive a single gateway operation -- see TenantCloudAdapter's
    # docstring for the 2026-08-10 incident this prevents.
    def build_mutations():
        client = client_module.TenantCloudClient(auth, base_url=TENANTCLOUD_ORIGIN)
        return mutations_module.TenantCloudMutations(client)

    return TenantCloudAdapter(mutations_factory=build_mutations)


# Quo's own call to its carrier API waits up to 30 s (QUO-Gated-MCP
# quo_client.py). Cutting it off at the 10 s default turned a slow carrier into
# an ambiguous send (action b78d5668, 2026-09-30); 30 s is this client's cap.
QUO_PROVIDER_TIMEOUT_SECONDS = 30.0


def quo_server_config() -> McpServerConfig:
    return McpServerConfig(
        name="quo",
        url=os.environ.get("QUO_MCP_URL", "http://127.0.0.1:8080/sse"),
        transport="sse",
        headers=_bearer_headers("QUO_MCP_TOKEN"),
        allowed_tools=frozenset({"send_message", "list_messages", "get_message"}),
        timeout_seconds=QUO_PROVIDER_TIMEOUT_SECONDS,
    )


def build_tenantcloud_auth_gate() -> SecretAuthGate:
    """Build same scoped auth path used by TenantCloud provider writes."""
    _reject_tenantcloud_origin_overrides()
    control_url = os.environ.get("TENANTCLOUD_RUNNER_CONTROL_URL", "").strip()
    bearer_file = os.environ.get("TENANTCLOUD_RUNNER_BEARER_FILE", "").strip()
    next_bearer_file = os.environ.get("TENANTCLOUD_RUNNER_NEXT_BEARER_FILE", "").strip() or None
    module_dir = os.environ.get("TENANTCLOUD_MODULE_DIR", "/repo/scripts")
    if not control_url or not bearer_file:
        raise ValueError("TenantCloud runner control configuration required")
    auth_module, _client_module, _mutations_module = _load_tenantcloud_modules(module_dir)
    control = auth_module.HttpRunnerControl(control_url, bearer_file, next_bearer_file)

    def factory():
        return auth_module.TenantCloudAuth(
            "tenantcloud-runner",
            control=control,
            profile_access=False,
        )

    return SecretAuthGate(
        factory,
        login_required_errors=(auth_module.TenantCloudLoginRequiredError,),
        transport_errors=(auth_module.TenantCloudAuthTransportError,),
    )


def _run_coroutine_sync(coro: Coroutine[Any, Any, Any]) -> Any:
    return asyncio.run(coro)


class _ThreadOffloadedAdapter:
    """Wraps a ``ProviderAdapter`` whose ``invoke``/``poll``/``reconcile`` are
    declared ``async`` but perform synchronous, blocking HTTP calls under the
    hood.

    TenantCloudAdapter (Task 7, adapters/tenantcloud.py) is built around the
    shared ``TenantCloudMutations`` facade (Comm-Data-Store,
    scripts/tenantcloud_mutations.py), which is entirely synchronous urllib
    HTTP -- including up to ``TenantCloudAuth.worst_case_auth_block_seconds``
    (180s) of blocking auth-refresh work per call. Task 7 explicitly parked
    the decision of whether to protect the event loop from that blocking for
    this task to make.

    Decision: offload here, at the server wiring boundary, via
    ``asyncio.to_thread``, rather than inside the adapter. The adapter stays
    a plain synchronous-under-async implementation with no event-loop
    concerns of its own (and is exercised that way, unwrapped, by its own
    unit tests); this wrapper is the one place that knows the concrete
    facade is blocking and pays the thread-hop cost for it. Every other
    ProviderAdapter in this gateway (email/quo/cliq/calendar) calls out
    through the async ``McpProviderClient`` and does not need this wrapper.
    """

    def __init__(self, inner: ProviderAdapter) -> None:
        self._inner = inner

    def validate(self, context: Any) -> None:
        self._inner.validate(context)

    def build_request(self, context: Any, action_uid: Any) -> Any:
        return self._inner.build_request(context, action_uid)

    def parse_receipt(self, context: Any, observation: Any) -> Any:
        return self._inner.parse_receipt(context, observation)

    async def invoke(self, client: Any, request: Any) -> Any:
        return await asyncio.to_thread(_run_coroutine_sync, self._inner.invoke(client, request))

    async def poll(self, client: Any, observation: Any) -> Any:
        return await asyncio.to_thread(_run_coroutine_sync, self._inner.poll(client, observation))

    async def reconcile(self, client: Any, context: Any, action_uid: Any, observation: Any) -> Any:
        return await asyncio.to_thread(
            _run_coroutine_sync,
            self._inner.reconcile(client, context, action_uid, observation),
        )


def _tenantcloud_adapters(enabled_operations: frozenset[Operation]) -> dict[Operation, ProviderAdapter]:
    """Build the (at most one) shared TenantCloud adapter, registered for all
    four TenantCloud operations, iff at least one of them is enabled.

    Fail-closed: when enabled, any missing/invalid configuration raises
    immediately (at startup) instead of registering a partially-working or
    silently-disabled adapter. When no TenantCloud operation is enabled,
    this never touches the environment beyond the operations set already in
    hand, so an unconfigured TenantCloud integration cannot break the rest
    of the gateway.
    """
    if not _tenantcloud_enabled(enabled_operations):
        return {}
    adapter: ProviderAdapter = _ThreadOffloadedAdapter(_build_tenantcloud_adapter())
    return {operation: adapter for operation in TENANTCLOUD_OPERATIONS}


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
        enabled_operations=_enabled_operations(),
    )
    routing = RoutingPolicy(
        version=os.environ.get("OUTBOUND_ROUTING_POLICY_VERSION", "appointment-v1"),
        email_account_by_provider=_json_mapping(
            "OUTBOUND_EMAIL_ACCOUNTS_JSON",
            # zoho_mail: a wake sourced from Nigel's mailbox (TenantCloud lead /
            # application notifications) replies from that same mailbox.
            {"zillow": "nigel-zoho", "hotpads": "nigel-zoho", "tenantcloud": "nigel-zoho", "zoho_mail": "nigel-zoho"},
        ),
        email_default_account=os.environ.get("OUTBOUND_EMAIL_DEFAULT_ACCOUNT", "nigel-zoho"),
        quo_sending_lines=_quo_sending_lines(),
        calendar_by_profile={"appointment-setter": os.environ.get("OUTBOUND_CALENDAR_NAME", "nigel")},
        calendar_account_by_profile={"appointment-setter": os.environ.get("OUTBOUND_CALENDAR_ACCOUNT", "nigel-zoho")},
        cliq_target_by_intent=_json_mapping(
            "OUTBOUND_CLIQ_TARGETS_JSON",
            {"lead_alert": "tenant-leads", "manual_review_alert": "tenant-leads"},
        ),
        cliq_channel_unique_names_by_id=_json_mapping(
            "OUTBOUND_CLIQ_CHANNEL_UNIQUE_NAMES_JSON",
            {},
        ),
        property_aliases=_json_mapping(
            "OUTBOUND_PROPERTY_ALIASES_JSON",
            DEFAULT_PROPERTY_ALIASES,
        ),
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
            "quo": quo_server_config(),
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
    adapters.update(_tenantcloud_adapters(policy.enabled_operations))
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
        traffic_mode=_traffic_mode(),
        traffic_probe=context_repository,
        # On: newer context is a stale_context question for the agent. Off
        # (default): never ask, and never send stale -- an action with unshown
        # newer context ends as a `stale_context_unasked` no-send.
        stale_confirm_enabled=_bool("OUTBOUND_STALE_CONFIRM_ENABLED", False),
        email_default_cc=_email_default_cc(),
        # Same union worker.py and tenantcloud_delivery_server.py's
        # coordinator already use (TENANTCLOUD_OPERATIONS unconditionally,
        # plus OUTBOUND_RESTATE_OPERATIONS): ActionRecovery.exhaust() needs
        # this to know which operations the Restate workflow, not a human,
        # settles at the retry ceiling (recovery.py's plan_exhaust
        # `restate_flagged`).
        restate_operations=TENANTCLOUD_OPERATIONS | _restate_operations(),
    )
    return GatewayRuntime(
        pool=pool,
        service=service,
        store=store,
        policy=policy,
        observability=observability,
        tenantcloud_submitter=(
            RestateWorkflowSubmitter(
                restate_ingress,
                workflow_name=os.environ.get("OUTBOUND_RESTATE_WORKFLOW_NAME", "TenantCloudDelivery"),
            )
            if (restate_ingress := os.environ.get("OUTBOUND_TENANTCLOUD_RESTATE_INGRESS_URL", "").strip())
            else None
        ),
        restate_operations=_restate_operations(),
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
        tenantcloud_submitter=runtime.tenantcloud_submitter,
        restate_operations=runtime.restate_operations,
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
        tenantcloud_submitter=runtime.tenantcloud_submitter,
        restate_operations=runtime.restate_operations,
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
