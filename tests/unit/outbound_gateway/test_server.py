from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from starlette.testclient import TestClient

from postgres_mcp.outbound_gateway.metrics import MetricSample
from postgres_mcp.outbound_gateway.models import PublicResult
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import create_server
from postgres_mcp.outbound_gateway.server import handle_outbound_action

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")


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
