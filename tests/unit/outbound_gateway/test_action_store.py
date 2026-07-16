from __future__ import annotations

from datetime import datetime
from datetime import timezone
from types import MappingProxyType
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.store import PostgresActionStore

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")
ACTION_UID = UUID("9ebddbf7-8fc8-5a4f-bba7-869ea7053521")
NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)


def action_row(state="received"):
    return {
        "action_id": ACTION_ID,
        "wakeup_event_id": 7,
        "action_role": "prospect_reply",
        "operation": "email.send",
        "intent_kind": "showing_offer",
        "appointment_slot": datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
        "arguments": {"text": "hello"},
        "state": state,
        "action_uid": ACTION_UID if state != "received" else None,
        "provider_request_ref": None,
        "provider_message_id": None,
        "completion_kind": None,
        "detail_code": state,
        "attempt_count": 0,
        "next_attempt_at": NOW,
        "payload_hash": "a" * 64,
        "canonical_context": {"identity_version": "v1"},
        "canonical_scope": {"version": "v1"},
        "recipient_scope": {
            "kind": "email_thread",
            "target_id": "lead@convo.zillow.com",
            "verified": True,
        },
        "provider_account": "nigel-zoho",
        "routing_policy_version": "v1",
    }


def context():
    return ActionContext(
        action_id=ACTION_ID,
        wakeup_event_id=7,
        action_role=ActionRole.PROSPECT_REPLY,
        operation=Operation.EMAIL_SEND,
        intent_kind=IntentKind.SHOWING_OFFER,
        appointment_slot=datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
        arguments=MappingProxyType({"text": "hello"}),
        source="zillow",
        source_message_id=700,
        source_message_key="zillow:700",
        source_sent_at=NOW,
        conversation_id="conversation:zillow-1",
        conversation_watermark=700,
        prospect_id="prospect:amanda",
        aliases=("email:amanda@example.com",),
        property_id="building:bullman-st",
        property_label="138 Bullman St #144-A",
        target=DerivedTarget("email_thread", "lead@convo.zillow.com", True),
        provider_account="nigel-zoho",
        routing_policy_version="v1",
        canonical_scope=MappingProxyType({"version": "v1"}),
        canonical_context=MappingProxyType({"identity_version": "v1"}),
        payload_hash="a" * 64,
        lock_holder=f"outbound-gateway:{ACTION_ID}",
        thread_identity="zrm-thread-1",
        showing_lifecycle_id="showing:7",
        calendar_event_uid=None,
    )


class Row:
    def __init__(self, cells):
        self.cells = cells


@pytest.mark.asyncio
async def test_store_uses_database_functions_and_sanitized_observations():
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        state = "dispatching" if "record_outbound_provider_request" in query else "received"
        return [Row(action_row(state))]

    store = PostgresActionStore(object())
    with patch(
        "postgres_mcp.outbound_gateway.store.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        created = await store.create_or_load(context())
        recorded = await store.record_provider_request(
            ACTION_ID,
            "worker-1",
            ProviderObservation(
                ProviderDisposition.PENDING,
                "provider_pending",
                provider_request_ref="req-1",
                provider_call_id="call-1",
                evidence={"secret": "must-not-persist"},
            ),
        )

    assert created.state is ActionState.RECEIVED
    assert recorded.state is ActionState.DISPATCHING
    assert "create_or_load_outbound_action" in calls[0][0]
    assert "record_outbound_provider_request" in calls[1][0]
    assert "must-not-persist" not in str(calls[1][1])
    assert calls[1][1][-1] == '{"detail_code":"provider_pending","disposition":"pending"}'


@pytest.mark.asyncio
async def test_store_work_query_includes_expired_dispatch_without_unlocking_it():
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return [Row({"action_id": ACTION_ID, "state": "dispatching"})]

    store = PostgresActionStore(object())
    with patch(
        "postgres_mcp.outbound_gateway.store.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        work = await store.list_work(20, 5)

    assert work == [(ACTION_ID, ActionState.DISPATCHING)]
    assert "lease_expires_at <= now()" in calls[0][0]
    assert "next_attempt_at <= now()" in calls[0][0]
    assert "attempt_count <" in calls[0][0]
    assert "FOR UPDATE SKIP LOCKED" not in calls[0][0]
    assert calls[0][1] == [5, 20]


@pytest.mark.asyncio
async def test_store_schedules_bounded_next_attempt_through_database_function():
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return [Row(action_row("unknown"))]

    store = PostgresActionStore(object())
    with patch(
        "postgres_mcp.outbound_gateway.store.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        scheduled = await store.schedule_next_attempt(
            ACTION_ID,
            ActionState.UNKNOWN,
            120,
            "provider_timeout",
        )

    assert scheduled.state is ActionState.UNKNOWN
    assert "schedule_outbound_action_attempt" in calls[0][0]
    assert calls[0][1] == [ACTION_ID, "unknown", 120, "provider_timeout"]


@pytest.mark.asyncio
async def test_prospect_lock_scope_includes_canonical_source_turn():
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return [Row(action_row("prepared"))]

    store = PostgresActionStore(object())
    with patch(
        "postgres_mcp.outbound_gateway.store.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        await store.prepare(context(), ActionState.RECEIVED)

    assert "prepare_outbound_action_and_acquire_lock" in calls[0][0]
    assert calls[0][1][4] == "showing_offer:turn:700"
