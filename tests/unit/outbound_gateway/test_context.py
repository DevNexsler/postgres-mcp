from dataclasses import replace
from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.context import RoutingPolicy
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.repository import AliasResolution
from postgres_mcp.outbound_gateway.repository import ConversationSnapshot
from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
from postgres_mcp.outbound_gateway.repository import WakeEventRecord

ACTION_NAMESPACE = UUID("ed6fcf85-39e7-5cdf-9fb8-ccca32a62e8d")


class FakeRepository:
    def __init__(self, record, *, canonical_subject="prospect:canonical", ambiguous=False):
        self.record = record
        self.canonical_subject = canonical_subject
        self.ambiguous = ambiguous
        self.alias_calls = []

    async def load_wake_event(self, wakeup_event_id):
        return self.record if wakeup_event_id == self.record.wakeup_event_id else None

    async def load_conversation_snapshot(self, channel_id):
        return ConversationSnapshot(
            conversation_watermark=900,
            latest_message_id=900,
            latest_sent_at=datetime(2026, 7, 15, 22, 30, tzinfo=timezone.utc),
        )

    async def resolve_canonical_subject(self, aliases, property_scope):
        self.alias_calls.append((aliases, property_scope))
        return AliasResolution(
            canonical_subject=self.canonical_subject,
            ambiguous=self.ambiguous,
        )


def record(**overrides):
    values = {
        "wakeup_event_id": 12345,
        "event_source": "zoho_mail",
        "source_event_id": "wake-source-1",
        "event_created_at": datetime(2026, 7, 15, 22, 31, tzinfo=timezone.utc),
        "message_id": 700,
        "canonical_message_id": None,
        "message_source": "zoho_mail",
        "source_message_id": "mail-700",
        "message_sent_at": datetime(2026, 7, 15, 22, 20, tzinfo=timezone.utc),
        "message_updated_at": datetime(2026, 7, 15, 22, 21, tzinfo=timezone.utc),
        "subject": "Zillow inquiry for 138 Bullman St #144-A",
        "body": "I would like to schedule a tour.",
        "user_account_id": "nigel-zoho",
        "channel_id": 44,
        "source_channel_id": "zillow-thread-44",
        "channel_type": "email",
        "channel_name": "INBOX",
        "sender_participant_id": 55,
        "participant_type": "email_address",
        "participant_key": "AmandaSnyder@live.com",
        "display_name": "Amanda Snyder",
        "envelope": {
            "identity": {"factbook_entity_uuid": "aa1a1515-7929-4f17-a632-ec89c32f5895"},
            "message": {
                "prospect_name": "Amanda Snyder",
                "property": "138 Bullman St #144-A",
                "proxy_email": "amanda.abc@convo.zillow.com",
                "direct_email": "AmandaSnyder@live.com",
                "phone": "+1 (908) 555-0100",
            },
        },
        "raw_payload": {"provider": "zillow", "thread_id": "zrm-thread-44"},
    }
    values.update(overrides)
    return WakeEventRecord(**values)


def policy():
    return RoutingPolicy(
        version="appointment-v1",
        email_account_by_provider={
            "zillow": "nigel-zoho",
            "hotpads": "nigel-zoho",
            "tenantcloud": "nigel-zoho",
        },
        quo_line_by_provider={
            "quo": "leasing-main",
            "tenantcloud": "leasing-main",
            "zillow": "leasing-main",
        },
        calendar_by_profile={"appointment-setter": "nigel"},
        cliq_target_by_intent={"lead_alert": "tenant-leads"},
        property_aliases={
            "138 bullman street 144 a": "building:bullman-st",
            "144 bullman street": "building:bullman-st",
            "16 north main street 16": "building:16-n-main",
        },
        conversation_aliases={
            "zillow:zrm-thread-44": "conversation:zillow-amanda-bullman",
            "hotpads:zrm-thread-44": "conversation:zillow-amanda-bullman",
        },
    )


def request(**overrides) -> ExecuteRequest:
    payload = {
        "op": "execute",
        "wakeup_event_id": 12345,
        "action_role": "prospect_reply",
        "operation": "email.send",
        "intent_kind": "showing_offer",
        "appointment_slot": "2026-07-17T10:30:00-04:00",
        "arguments": {"text": "Friday at 10:30 works.\r\n— Nigel"},
    }
    payload.update(overrides)
    parsed = parse_outbound_request(payload)
    assert isinstance(parsed, ExecuteRequest)
    return parsed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event", "action", "target_kind", "target_id", "provider_account"),
    [
        (
            record(),
            request(),
            "email_thread",
            "amanda.abc@convo.zillow.com",
            "nigel-zoho",
        ),
        (
            record(
                raw_payload={"provider": "hotpads", "thread_id": "zrm-thread-44"},
                participant_key="lead.123@convo.zillow.com",
                envelope={
                    "identity": {},
                    "message": {
                        "property": "144 Bullman Street",
                        "proxy_email": "lead.123@convo.zillow.com",
                        "direct_email": "AmandaSnyder@live.com",
                    },
                },
            ),
            request(),
            "email_thread",
            "lead.123@convo.zillow.com",
            "nigel-zoho",
        ),
        (
            record(
                raw_payload={"provider": "tenantcloud", "thread_id": "tc-lead-1"},
                source_channel_id="tenantcloud-lead-1",
                participant_key="tenant@example.com",
                envelope={
                    "identity": {},
                    "message": {
                        "property": "16 N Main St #16",
                        "direct_email": "tenant@example.com",
                    },
                },
            ),
            request(appointment_slot="2026-07-17T10:00:00-04:00"),
            "email_thread",
            "tenant@example.com",
            "nigel-zoho",
        ),
        (
            record(
                event_source="quo",
                message_source="quo",
                source_channel_id="quo-conversation-9",
                channel_type="sms",
                participant_type="phone",
                participant_key="+19085550199",
                raw_payload={"provider": "quo", "conversation_id": "quo-conversation-9"},
                envelope={
                    "identity": {},
                    "message": {"property": "16 N Main St #16", "phone": "+1 908 555 0199"},
                },
            ),
            request(operation="quo.sms.send"),
            "quo_conversation",
            "quo-conversation-9",
            "leasing-main",
        ),
        (
            record(
                raw_payload={"provider": "tenantcloud", "thread_id": "tc-lead-1"},
                source_channel_id="tenantcloud-lead-1",
                participant_key="tenant@example.com",
                envelope={
                    "identity": {},
                    "message": {
                        "property": "16 N Main St #16",
                        "direct_email": "tenant@example.com",
                        "phone": "+1 908 555 0199",
                    },
                },
            ),
            request(operation="quo.sms.send"),
            "quo_conversation",
            "+19085550199",
            "leasing-main",
        ),
        (
            record(
                event_source="zoho_cliq",
                message_source="zoho_cliq",
                source_channel_id="tenant-leads",
                channel_type="channel",
                participant_type="user",
                participant_key="internal-user",
                raw_payload={"provider": "cliq", "channel_id": "tenant-leads"},
                envelope={"identity": {}, "message": {}},
            ),
            request(
                action_role="internal_notification",
                operation="cliq.channel.post",
                intent_kind="lead_alert",
                appointment_slot=None,
                arguments={"text": "New lead"},
            ),
            "cliq_channel",
            "tenant-leads",
            "tenant-leads",
        ),
        (
            record(raw_payload={"provider": "zillow", "thread_id": "zrm-thread-44"}),
            request(
                action_role="calendar_mutation",
                operation="calendar.create",
                intent_kind="showing_create",
                arguments={"description": "Amanda tour"},
            ),
            "calendar",
            "nigel",
            "nigel",
        ),
    ],
)
async def test_context_derives_provider_targets_server_side(event, action, target_kind, target_id, provider_account):
    event = WakeEventRecord(**{**event.__dict__, "wakeup_event_id": action.wakeup_event_id})
    context = await ActionContextLoader(FakeRepository(event), policy()).load(action)
    assert context.target.kind == target_kind
    assert context.target.target_id == target_id
    assert context.target.verified is True
    assert context.provider_account == provider_account
    assert context.lock_holder == f"outbound-gateway:{context.action_id}"
    assert context.source_message_id == event.message_id
    assert context.conversation_watermark == 700
    assert len(context.payload_hash) == 64


@pytest.mark.asyncio
async def test_rollout_policy_rejects_cross_channel_provider_route():
    restricted = replace(
        policy(),
        enabled_operations_by_provider={
            "zillow": frozenset({"email.send"}),
            "hotpads": frozenset({"email.send"}),
            "quo": frozenset({"quo.sms.send"}),
        },
        enabled_intents=frozenset({"inquiry_reply", "showing_offer"}),
    )
    tenantcloud = record(
        raw_payload={"provider": "tenantcloud", "thread_id": "tc-lead-1"},
        participant_key="+19085550199",
        participant_type="phone",
        channel_type="sms",
        envelope={
            "identity": {},
            "message": {
                "property": "16 N Main St #16",
                "phone": "+1 908 555 0199",
            },
        },
    )

    with pytest.raises(ContextDerivationError, match="provider operation is disabled"):
        await ActionContextLoader(FakeRepository(tenantcloud), restricted).load(
            request(operation="quo.sms.send")
        )


@pytest.mark.asyncio
async def test_rollout_policy_rejects_unapproved_intent():
    restricted = replace(
        policy(),
        enabled_operations_by_provider={"zillow": frozenset({"email.send"})},
        enabled_intents=frozenset({"inquiry_reply", "showing_offer"}),
    )

    with pytest.raises(ContextDerivationError, match="intent is disabled"):
        await ActionContextLoader(FakeRepository(record()), restricted).load(
            request(intent_kind="showing_confirmation")
        )


@pytest.mark.asyncio
async def test_action_identity_and_payload_hash_are_canonical_and_stable():
    event = record()
    repo = FakeRepository(event)
    loader = ActionContextLoader(repo, policy())
    first = await loader.load(request())
    second = await loader.load(request(arguments={"text": "Friday at 10:30 works.\n— Nigel"}))
    assert first.action_id == second.action_id
    assert first.action_id.version == 5
    assert first.payload_hash == second.payload_hash
    assert first.arguments == {"text": "Friday at 10:30 works.\n— Nigel"}
    assert tuple(sorted(first.aliases)) == first.aliases


@pytest.mark.asyncio
async def test_duplicate_provider_and_property_aliases_converge():
    first_repo = FakeRepository(record(), canonical_subject="prospect:amanda")
    second_repo = FakeRepository(
        record(
            raw_payload={"provider": "hotpads", "thread_id": "zrm-thread-44"},
            envelope={
                "identity": {},
                "message": {
                    "property": "144 Bullman Street",
                    "proxy_email": "other-proxy@convo.zillow.com",
                    "direct_email": "AmandaSnyder@live.com",
                },
            },
        ),
        canonical_subject="prospect:amanda",
    )
    first = await ActionContextLoader(first_repo, policy()).load(request())
    second = await ActionContextLoader(second_repo, policy()).load(request())
    assert first.prospect_id == second.prospect_id == "prospect:amanda"
    assert first.property_id == second.property_id == "building:bullman-st"
    assert first.conversation_id == second.conversation_id


@pytest.mark.asyncio
async def test_ambiguous_aliases_and_unverified_targets_fail_closed():
    with pytest.raises(ContextDerivationError, match="ambiguous"):
        await ActionContextLoader(FakeRepository(record(), ambiguous=True), policy()).load(request())
    unsafe = record(
        participant_key="unknown",
        envelope={
            "identity": {"factbook_entity_uuid": "aa1a1515-7929-4f17-a632-ec89c32f5895"},
            "message": {"property": "144 Bullman Street"},
        },
    )
    with pytest.raises(ContextDerivationError, match="verified target"):
        await ActionContextLoader(FakeRepository(unsafe), policy()).load(request())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe_address",
    (
        "no-reply@comet.zillow.com",
        "noreply@tenantcloud.com",
        "postmaster@example.com",
    ),
)
async def test_system_sender_cannot_become_customer_email_target(unsafe_address):
    unsafe = record(
        participant_key=unsafe_address,
        envelope={
            "identity": {"factbook_entity_uuid": "aa1a1515-7929-4f17-a632-ec89c32f5895"},
            "message": {
                "property": "138 Bullman St #144-A",
                "direct_email": unsafe_address,
            },
        },
    )

    with pytest.raises(ContextDerivationError, match="verified target"):
        await ActionContextLoader(FakeRepository(unsafe), policy()).load(request())


@pytest.mark.asyncio
async def test_repository_uses_parameterized_queries_for_event_and_alias_reads():
    class Row:
        def __init__(self, cells):
            self.cells = cells

    event = record()
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        if "FROM hermes_wakeup_events" in query:
            return [Row(event.__dict__)]
        return [Row({"subject_count": 1, "canonical_subject": "prospect:canonical"})]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        loaded = await repository.load_wake_event(12345)
        resolved = await repository.resolve_canonical_subject(
            ("email:a@example.com",),
            "144 bullman street",
        )

    assert loaded == event
    assert resolved.canonical_subject == "prospect:canonical"
    assert calls[0][1] == [12345]
    assert "12345" not in calls[0][0]
    assert calls[1][1] == [["email:a@example.com"], "144 bullman street"]
