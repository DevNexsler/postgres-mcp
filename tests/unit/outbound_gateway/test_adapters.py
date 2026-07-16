from __future__ import annotations

from datetime import datetime
from datetime import timezone
from types import MappingProxyType
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.calendar import CalendarAdapter
from postgres_mcp.outbound_gateway.adapters.cliq import CliqAdapter
from postgres_mcp.outbound_gateway.adapters.email import EmailAdapter
from postgres_mcp.outbound_gateway.adapters.quo import QuoSmsAdapter
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.provider_client import McpCallResult
from postgres_mcp.outbound_gateway.provider_client import TransportErrorKind

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")
ACTION_UID = UUID("9ebddbf7-8fc8-5a4f-bba7-869ea7053521")
NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)


class FakeClient:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    async def call(self, server_name, tool, arguments):
        self.calls.append((server_name, tool, arguments))
        return self.results.pop(0)


def context(operation=Operation.EMAIL_SEND, **overrides):
    role = (
        ActionRole.CALENDAR_MUTATION
        if operation.value.startswith("calendar.")
        else ActionRole.INTERNAL_NOTIFICATION
        if operation.value.startswith("cliq.")
        else ActionRole.PROSPECT_REPLY
    )
    target = {
        Operation.EMAIL_SEND: DerivedTarget("email_thread", "lead@convo.zillow.com", True),
        Operation.QUO_SMS_SEND: DerivedTarget("quo_conversation", "quo-thread-1", True),
        Operation.CLIQ_CHANNEL_POST: DerivedTarget("cliq_channel", "tenant-leads", True),
        Operation.CLIQ_CHAT_POST: DerivedTarget("cliq_chat", "CT_123", True),
        Operation.CALENDAR_CREATE: DerivedTarget("calendar", "nigel", True),
        Operation.CALENDAR_UPDATE: DerivedTarget("calendar", "nigel", True),
        Operation.CALENDAR_DELETE: DerivedTarget("calendar", "nigel", True),
    }[operation]
    intent = {
        Operation.EMAIL_SEND: IntentKind.SHOWING_OFFER,
        Operation.QUO_SMS_SEND: IntentKind.SHOWING_OFFER,
        Operation.CLIQ_CHANNEL_POST: IntentKind.LEAD_ALERT,
        Operation.CLIQ_CHAT_POST: IntentKind.MANUAL_REVIEW_ALERT,
        Operation.CALENDAR_CREATE: IntentKind.SHOWING_CREATE,
        Operation.CALENDAR_UPDATE: IntentKind.SHOWING_UPDATE,
        Operation.CALENDAR_DELETE: IntentKind.SHOWING_DELETE,
    }[operation]
    values = dict(
        action_id=ACTION_ID,
        wakeup_event_id=7,
        action_role=role,
        operation=operation,
        intent_kind=intent,
        appointment_slot=None if operation is Operation.CALENDAR_DELETE else datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
        arguments=MappingProxyType(
            {}
            if operation is Operation.CALENDAR_DELETE
            else {"text": "Friday at 10:30 works. — Nigel"}
            if not operation.value.startswith("calendar.")
            else {"description": "Tour"}
        ),
        source="zillow",
        source_message_id=700,
        source_message_key="zillow_rm_web_extract:700",
        source_sent_at=NOW,
        conversation_id="conversation:zillow-1",
        conversation_watermark=700,
        prospect_id="prospect:amanda",
        aliases=("email:amanda@example.com",),
        property_id="building:bullman-st",
        property_label="138 Bullman St #144-A",
        target=target,
        provider_account="nigel-zoho"
        if operation is Operation.EMAIL_SEND
        else "leasing-line"
        if operation is Operation.QUO_SMS_SEND
        else target.target_id,
        routing_policy_version="v1",
        canonical_scope=MappingProxyType({"version": "v1"}),
        canonical_context=MappingProxyType({"identity_version": "v1"}),
        payload_hash="a" * 64,
        lock_holder=f"outbound-gateway:{ACTION_ID}",
        thread_identity="zrm-thread-1",
        showing_lifecycle_id="showing:7",
        calendar_event_uid="existing-event" if operation in {Operation.CALENDAR_UPDATE, Operation.CALENDAR_DELETE} else None,
        source_subject="Zillow inquiry for 138 Bullman St #144-A",
        prospect_name="Amanda Snyder",
        recipient_phone="+19085550199" if operation is Operation.QUO_SMS_SEND else None,
        calendar_event_url="https://calendar.local/events/existing-event.ics"
        if operation in {Operation.CALENDAR_UPDATE, Operation.CALENDAR_DELETE}
        else None,
        calendar_event_etag='"etag-1"' if operation in {Operation.CALENDAR_UPDATE, Operation.CALENDAR_DELETE} else None,
    )
    values.update(overrides)
    return ActionContext(**values)


def pending(request_id="req-1"):
    return McpCallResult(structured_content={"status": "pending", "request_id": request_id, "call_id": request_id})


def completed(tool, content):
    return McpCallResult(
        structured_content={
            "status": "completed",
            "request_id": "req-1",
            "completed_result": {"tool_name": tool, "structured_content": content},
        }
    )


@pytest.mark.asyncio
async def test_email_adapter_derives_recipient_and_returns_structured_receipt():
    adapter = EmailAdapter(
        sender_domains={"nigel-zoho": "pfg.example"},
        cc_by_source={"zillow": "management@pfg.io"},
    )
    request = adapter.build_request(context(), ACTION_UID)
    assert request.server_name == "agent-email"
    assert request.tool == "email_send"
    assert request.arguments == {
        "account_id": "nigel-zoho",
        "to": [{"address": "lead@convo.zillow.com"}],
        "cc": [{"address": "management@pfg.io"}],
        "subject": "Re: Zillow inquiry for 138 Bullman St #144-A",
        "text": "Friday at 10:30 works. — Nigel",
        "outbound_action_uid": str(ACTION_UID),
    }
    client = FakeClient(pending(), completed("email_send", {"status": "success", "provider_message_id": "<mail-1@example.com>"}))
    observation = await adapter.invoke(client, request)
    assert observation.disposition is ProviderDisposition.PENDING
    observation = await adapter.poll(client, observation)
    assert observation.disposition is ProviderDisposition.ACCEPTED
    receipt = adapter.parse_receipt(context(), observation)
    assert receipt is not None
    assert receipt.provider_message_id == "<mail-1@example.com>"
    assert receipt.provider_request_ref == "req-1"


def test_email_adapter_applies_management_copy_only_to_configured_sources():
    adapter = EmailAdapter(
        sender_domains={"nigel-zoho": "pfg.io"},
        cc_by_source={"zillow": "management@pfg.io", "hotpads": "management@pfg.io"},
    )

    tenantcloud_request = adapter.build_request(
        context(source="tenantcloud"),
        ACTION_UID,
    )

    assert "cc" not in tenantcloud_request.arguments


@pytest.mark.asyncio
async def test_quo_adapter_requires_id_and_reconciles_exact_message_tuple():
    adapter = QuoSmsAdapter(user_id="user-1")
    request = adapter.build_request(context(Operation.QUO_SMS_SEND), ACTION_UID)
    assert request.arguments == {
        "phone_number_id": "leasing-line",
        "to": "+19085550199",
        "user_id": "user-1",
        "content": "Friday at 10:30 works. — Nigel",
    }
    client = FakeClient(
        McpCallResult(structured_content={"status": "sent", "message_id": "quo-message-1"}),
    )
    observation = await adapter.invoke(client, request)
    assert observation.disposition is ProviderDisposition.ACCEPTED
    assert adapter.parse_receipt(context(Operation.QUO_SMS_SEND), observation).provider_message_id == "quo-message-1"

    ambiguous = McpCallResult(error_kind=TransportErrorKind.TIMEOUT, is_error=True, safe_detail="transport_timeout")
    history = McpCallResult(
        structured_content={
            "messages": [
                {
                    "id": "quo-message-2",
                    "direction": "outgoing",
                    "to": "+19085550199",
                    "content": "Friday at 10:30 works. — Nigel",
                    "created_at": "2026-07-16T01:00:05Z",
                }
            ]
        }
    )
    retry_client = FakeClient(ambiguous, history)
    unknown = await adapter.invoke(retry_client, request)
    assert unknown.disposition is ProviderDisposition.AMBIGUOUS
    reconciled = await adapter.reconcile(retry_client, context(Operation.QUO_SMS_SEND), ACTION_UID, unknown)
    assert reconciled.disposition is ProviderDisposition.ACCEPTED
    assert reconciled.message_id == "quo-message-2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "tool", "target_field"),
    [
        (Operation.CLIQ_CHANNEL_POST, "cliq_channel_bot_post", "channel_unique_name"),
        (Operation.CLIQ_CHAT_POST, "cliq_chat_post", "chat_id"),
    ],
)
async def test_cliq_adapter_builds_only_derived_destination(operation, tool, target_field):
    adapter = CliqAdapter(operation)
    ctx = context(operation)
    request = adapter.build_request(ctx, ACTION_UID)
    assert request.server_name == "agent-email"
    assert request.tool == tool
    assert request.arguments[target_field] == ctx.target.target_id
    assert request.arguments["text"] == ctx.arguments["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "tool"),
    [
        (Operation.CALENDAR_CREATE, "calendar_create_event"),
        (Operation.CALENDAR_UPDATE, "calendar_update_event"),
        (Operation.CALENDAR_DELETE, "calendar_delete_event"),
    ],
)
async def test_calendar_adapter_uses_deterministic_uid_and_exact_revision(operation, tool):
    adapter = CalendarAdapter(account_by_calendar={"nigel": "nigel-zoho"})
    ctx = context(operation)
    request = adapter.build_request(ctx, ACTION_UID)
    assert request.server_name == "agent-email"
    assert request.tool == tool
    assert request.arguments["account_id"] == "nigel-zoho"
    assert request.arguments["calendar"] == "nigel"
    if operation is Operation.CALENDAR_CREATE:
        assert request.arguments["uid"] == str(ACTION_UID)
        assert request.arguments["location"] == "138 Bullman St #144-A"
        assert request.arguments["end"] == "2026-07-17T15:00:00Z"
    else:
        assert request.arguments["event_url"].endswith("existing-event.ics")
        assert request.arguments["etag"] == '"etag-1"'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result", "expected", "detail"),
    [
        (McpCallResult(error_kind=TransportErrorKind.TIMEOUT, is_error=True), ProviderDisposition.AMBIGUOUS, "provider_timeout"),
        (McpCallResult(error_kind=TransportErrorKind.CONNECTION_LOST, is_error=True), ProviderDisposition.AMBIGUOUS, "provider_connection_lost"),
        (
            McpCallResult(structured_content={"status": "failed", "category": "auth_error", "retryable": False}),
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "provider_auth_error",
        ),
        (
            McpCallResult(structured_content={"status": "failed", "category": "transient_upstream_error", "retryable": True}),
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "provider_transient_upstream_error",
        ),
        (
            McpCallResult(
                structured_content={
                    "status": "completed",
                    "completed_result": {"tool_name": "email_send", "structured_content": {"status": "success"}},
                }
            ),
            ProviderDisposition.AMBIGUOUS,
            "malformed_provider_success",
        ),
    ],
)
async def test_failures_distinguish_definite_non_acceptance_from_ambiguity(result, expected, detail):
    adapter = EmailAdapter(sender_domains={"nigel-zoho": "pfg.example"})
    client = FakeClient(result)
    observation = await adapter.invoke(client, adapter.build_request(context(), ACTION_UID))
    assert observation.disposition is expected
    assert observation.detail_code == detail
    assert observation.disposition is not ProviderDisposition.ACCEPTED
