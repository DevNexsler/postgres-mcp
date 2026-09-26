# pyright: reportArgumentType=false, reportOptionalMemberAccess=false

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from types import MappingProxyType
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

import pytest
from pglast import parse_sql

from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.evidence import DatabasePreflightEvidenceLoader
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.preflight import CalendarDependencyState
from postgres_mcp.outbound_gateway.preflight import PreflightEvidence
from postgres_mcp.outbound_gateway.repository import OutboundTarget
from postgres_mcp.outbound_gateway.repository import outbound_target

NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)


def context(**overrides):
    values = dict(
        action_id=UUID("4cbac369-48c6-5b62-95e9-41f50259e732"),
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
        lock_holder="outbound-gateway:4cbac369-48c6-5b62-95e9-41f50259e732",
        thread_identity="zrm-thread-1",
        showing_lifecycle_id="showing:7",
        calendar_event_uid=None,
        channel_id=44,
        refresh_evidence=MappingProxyType(
            {
                "status": "covered",
                "covered_through": "2026-07-16T01:00:00Z",
                "covered_thread_identity": "zrm-thread-1",
                "attempt_count": 1,
            }
        ),
    )
    values.update(overrides)
    return ActionContext(**values)


class Row:
    def __init__(self, cells):
        self.cells = cells


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(CalendarDependencyState))
async def test_the_evidence_loader_reads_the_calendar_dependency_only(state):
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return [Row({"calendar_dependency_state": state.value})]

    with patch(
        "postgres_mcp.outbound_gateway.evidence.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        proof = await DatabasePreflightEvidenceLoader(object()).load(context())

    assert proof == PreflightEvidence(calendar_dependency=state)
    query, params = calls[0]
    parse_sql(query.replace("{}", "NULL"))
    lowered = query.casefold()
    # Newer messages are the stale-context question's, not the preflight's.
    assert "messages" not in lowered and "later_inbound" not in lowered and "later_outbound" not in lowered
    assert params == ["showing_offer", 7, 7]


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        # wake 27279: an email's target is its address, whatever the wake's source
        (
            {"source": "cliq", "target": DerivedTarget("email_thread", "YTryboujee@Gmail.com", True)},
            OutboundTarget(("zoho_mail", "nigel_mail"), "email", "ytryboujee@gmail.com"),
        ),
        (
            {"operation": Operation.QUO_SMS_SEND, "target": DerivedTarget("quo_conversation", "(516) 859-4333", True)},
            OutboundTarget(("quo", "openphone"), "sms", "15168594333"),
        ),
        (
            {"operation": Operation.CLIQ_CHAT_POST, "target": DerivedTarget("cliq_chat", "1424728044450751028", True)},
            OutboundTarget(("zoho_cliq",), "channel", "1424728044450751028"),
        ),
        (
            {"operation": Operation.TENANTCLOUD_MESSAGE_SEND, "target": DerivedTarget("tenantcloud_thread", "1861792", True)},
            OutboundTarget(("tenantcloud_api",), "channel", "tenantcloud:thread:1861792"),
        ),
        # an internal reply answers in its own chat: our other posts there count
        (
            {"action_role": ActionRole.INTERNAL_REPLY, "operation": Operation.CLIQ_CHAT_POST,
             "target": DerivedTarget("cliq_chat", "1424728044450751028", True)},
            OutboundTarget(("zoho_cliq",), "channel", "1424728044450751028"),
        ),
        # a notification's chat carries other subjects' notifications: never context
        (
            {"action_role": ActionRole.INTERNAL_NOTIFICATION, "operation": Operation.CLIQ_CHAT_POST,
             "target": DerivedTarget("cliq_chat", "1424728044450751028", True)},
            OutboundTarget((), "none", ""),
        ),
        (
            {"action_role": ActionRole.CALENDAR_MUTATION, "operation": Operation.CALENDAR_CREATE,
             "target": DerivedTarget("calendar", "nigel", True)},
            OutboundTarget((), "none", ""),
        ),
    ],
)
def test_sent_by_us_is_keyed_on_the_actions_own_target(overrides, expected):
    assert outbound_target(context(**overrides)) == expected
