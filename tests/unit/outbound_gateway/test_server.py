# pyright: reportPrivateUsage=false

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from starlette.testclient import TestClient

from postgres_mcp.outbound_gateway.metrics import MetricSample
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicResult
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.server import DEFAULT_EMAIL_CC_BY_SOURCE
from postgres_mcp.outbound_gateway.server import DEFAULT_EMAIL_SENDER_DOMAINS
from postgres_mcp.outbound_gateway.server import DEFAULT_ENABLED_INTENTS
from postgres_mcp.outbound_gateway.server import DEFAULT_ENABLED_INTENTS_BY_PROVIDER
from postgres_mcp.outbound_gateway.server import DEFAULT_ENABLED_OPERATIONS_BY_PROVIDER
from postgres_mcp.outbound_gateway.server import DEFAULT_PROPERTY_ALIASES
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import _bearer_headers
from postgres_mcp.outbound_gateway.server import _enabled_intents_by_provider
from postgres_mcp.outbound_gateway.server import _enabled_operations_by_provider
from postgres_mcp.outbound_gateway.server import create_server
from postgres_mcp.outbound_gateway.server import handle_outbound_action

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")


def test_default_email_routing_matches_nigel_account_and_zillow_copy_policy():
    assert DEFAULT_EMAIL_SENDER_DOMAINS == {"nigel-zoho": "pfg.io"}
    assert DEFAULT_EMAIL_CC_BY_SOURCE == {
        "zillow": "management@pfg.io",
        "hotpads": "management@pfg.io",
    }
    assert DEFAULT_PROPERTY_ALIASES["138 bullman street 144 a"] == "building:bullman-st"
    assert DEFAULT_PROPERTY_ALIASES["144 bullman street"] == "building:bullman-st"
    assert DEFAULT_ENABLED_OPERATIONS_BY_PROVIDER == {
        "hotpads": frozenset({"email.send"}),
        "zillow": frozenset({"email.send"}),
    }
    assert DEFAULT_ENABLED_INTENTS == frozenset({"inquiry_reply", "showing_offer"})
    assert DEFAULT_ENABLED_INTENTS_BY_PROVIDER == {
        "hotpads": frozenset({"inquiry_reply", "showing_offer"}),
        "zillow": frozenset({"inquiry_reply", "showing_offer"}),
    }


def test_provider_bearer_headers_are_environment_only_and_optional(monkeypatch):
    monkeypatch.delenv("QUO_MCP_TOKEN", raising=False)
    assert _bearer_headers("QUO_MCP_TOKEN") == {}
    monkeypatch.setenv("QUO_MCP_TOKEN", "provider-secret")
    assert _bearer_headers("QUO_MCP_TOKEN") == {"Authorization": "Bearer provider-secret"}


@pytest.mark.parametrize(
    ("environment_name", "loader"),
    [
        ("OUTBOUND_PROVIDER_OPERATIONS_JSON", _enabled_operations_by_provider),
        ("OUTBOUND_PROVIDER_INTENTS_JSON", _enabled_intents_by_provider),
    ],
)
def test_explicit_empty_provider_scope_fails_closed(monkeypatch, environment_name, loader):
    monkeypatch.setenv(environment_name, "{}")

    with pytest.raises(ValueError, match="non-empty JSON object"):
        loader()


def public(status=PublicStatus.SENT, detail="provider_receipt_verified"):
    return PublicResult(
        status=status,
        action_id=ACTION_ID,
        action_uid=None,
        provider_request_ref="req-1",
        retryable=False,
        detail_code=detail,
    )


def execute_payload():
    return {
        "op": "execute",
        "wakeup_event_id": 7,
        "action_role": "prospect_reply",
        "operation": "email.send",
        "intent_kind": "showing_offer",
        "appointment_slot": "2026-07-17T10:30:00-04:00",
        "arguments": {"text": "Friday at 10:30 works. — Nigel"},
    }


@pytest.mark.asyncio
async def test_focused_server_exposes_only_outbound_action_and_health_resource():
    service = AsyncMock()
    mcp = create_server(service, FeaturePolicy(writes_enabled=True, kill_switch=False))
    tools = await mcp.list_tools()
    resources = await mcp.list_resources()
    assert [tool.name for tool in tools] == ["outbound_action"]
    assert [str(resource.uri) for resource in resources] == ["health://outbound-gateway"]
    assert all(tool.name not in {"execute_sql", "outbound_lock"} for tool in tools)


def test_loopback_http_health_and_metrics_routes_are_sanitized():
    service = AsyncMock()
    observability = AsyncMock()
    observability.database_healthy.return_value = True
    observability.collect.return_value = (MetricSample("outbound_gateway_actions_total", 3, {"outcome": "submitted"}),)
    mcp = create_server(
        service,
        FeaturePolicy(writes_enabled=False, kill_switch=True),
        observability=observability,
    )

    with TestClient(mcp.streamable_http_app()) as client:
        health = client.get("/healthz")
        metrics = client.get("/metrics")

    assert health.status_code == 200
    assert health.json() == {
        "kill_switch": True,
        "status": "ok",
        "writes_enabled": False,
    }
    assert metrics.status_code == 200
    assert 'outbound_gateway_actions_total{outcome="submitted"} 3' in metrics.text
    assert "recipient" not in metrics.text


@pytest.mark.asyncio
async def test_execute_and_status_delegate_only_after_strict_json_validation():
    service = AsyncMock()
    service.execute.return_value = public()
    service.status.return_value = public(PublicStatus.UNKNOWN, "provider_timeout")
    policy = FeaturePolicy(writes_enabled=True, kill_switch=False)

    executed = await handle_outbound_action(service, policy, execute_payload())
    status = await handle_outbound_action(
        service,
        policy,
        {"op": "status", "action_id": str(ACTION_ID)},
    )

    assert executed["status"] == "sent"
    assert status["status"] == "unknown"
    service.execute.assert_awaited_once()
    service.status.assert_awaited_once_with(ACTION_ID)
    with pytest.raises(ValueError, match="invalid outbound action request"):
        await handle_outbound_action(service, policy, {**execute_payload(), "recipient": "attacker@example.com"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "detail"),
    [
        (FeaturePolicy(writes_enabled=False, kill_switch=False), "writes_disabled"),
        (FeaturePolicy(writes_enabled=True, kill_switch=True), "kill_switch_open"),
    ],
)
async def test_write_policy_rejects_before_database_or_provider_call(policy, detail):
    service = AsyncMock()

    result = await handle_outbound_action(service, policy, execute_payload())

    assert result["status"] == "rejected"
    assert result["detail_code"] == detail
    service.execute.assert_not_called()


@pytest.mark.asyncio
async def test_disabled_operation_rejects_before_database_or_provider_call():
    service = AsyncMock()
    policy = FeaturePolicy(
        writes_enabled=True,
        kill_switch=False,
        enabled_operations=frozenset({Operation.QUO_SMS_SEND}),
    )

    result = await handle_outbound_action(service, policy, execute_payload())

    assert result["status"] == "rejected"
    assert result["detail_code"] == "operation_disabled"
    service.execute.assert_not_called()
