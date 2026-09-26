# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportOptionalIterable=false, reportOptionalSubscript=false
"""Newer outbound is information for the agent, never an "already handled" verdict.

Wake 27279 (2026-09-26): Dan asked Nigel in the Cliq DM (channel 417) to send
a prospect the application link by email, cc management@pfg.io. Four minutes
later an unrelated cron alert was posted to the same DM as outbound. The
preflight picked that post as "verified outbound after the source message" and
completed both email.send attempts as duplicate/already_handled, fabricating a
receipt from the Cliq post. The email never went out.

The gateway no longer decides a send was already handled from conversation
activity. An outbound message sent after the source message TO THE ACTION'S
TARGET (same address, phone, chat or thread, on the operation's own channel
family) is shown to the agent through the stale-context question (sent by us,
direction outbound); the agent answers yes, no or revise. Anything else --
a Cliq post when the action is an email -- is not the target's activity and
does not touch the send. Identical requests are still the same action.

These tests run with the fake ledger/probe of test_stale_context_confirm (the
migration 192 guards); tests/integration/test_gateway_newer_outbound.py runs
the same shapes against the real SQL.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import MappingProxyType
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.context import canonical_payload_hash
from postgres_mcp.outbound_gateway.evidence import DatabasePreflightEvidenceLoader
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import CompletionKind
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.service import OutboundActionService
from postgres_mcp.outbound_gateway.traffic_control import NewerActivity

from .test_stale_context_confirm import CliqAdapter
from .test_stale_context_confirm import LedgerProbe
from .test_stale_context_confirm import LedgerStore
from .test_stale_context_confirm import action_id_for
from .test_stale_context_confirm import confirm

WAKE = 27279
DM = "1424728044450751028"
PROSPECT = "ytryboujee@gmail.com"
# Real timeline, wake 27279 (UTC).
SOURCE_AT = datetime(2026, 9, 26, 13, 1, 6, 353000, tzinfo=timezone.utc)  # message 806237, Dan
CRON_POST_AT = datetime(2026, 9, 26, 13, 5, 50, 439000, tzinfo=timezone.utc)  # message 806236, Nigel
WATERMARK = datetime(2026, 9, 26, 13, 5, 55, 544802, tzinfo=timezone.utc)  # webui_accepted_at
EXECUTED_AT = datetime(2026, 9, 26, 13, 11, 2, 842599, tzinfo=timezone.utc)
EMAIL_TEXT = "Hi Alberto, here is the application link: https://pinefield.tenantcloud.com/listings/204968"

FIRST = action_id_for(WAKE, "prospect_reply", 0)
SUCCESSOR = action_id_for(WAKE, "prospect_reply", 1)

# The earlier outbound email to the same prospect (a genuine "we already sent
# something" the agent should be told about).
EARLIER_EMAIL = NewerActivity(
    direction="outbound",
    source="zoho_mail",
    occurred_at=SOURCE_AT + timedelta(minutes=2),
    preview="Hi Alberto, the application link is https://pinefield.tenantcloud.com/listings/204968",
    message_id=806300,
    action_id=None,
    sender="Nigel Pine",
)


def email_request(text: str = EMAIL_TEXT, to_address: str = PROSPECT) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": WAKE,
            "action_role": "prospect_reply",
            "operation": "email.send",
            "intent_kind": "inquiry_reply",
            "arguments": {
                "to_address": to_address,
                "text": text,
                "subject": "Application link — 480-484 S Main St, Unit 7",
                "cc": ["management@pfg.io"],
            },
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


def sms_request(text: str = "Yes, Friday at 10 still works.", to_phone: str = "+15705550143") -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": WAKE,
            "action_role": "prospect_reply",
            "operation": "quo.sms.send",
            "intent_kind": "inquiry_reply",
            "arguments": {"text": text, "to_phone": to_phone},
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


class Loader:
    """The context ActionContextLoader derives for wake 27279 (canonical_context
    of action 22172062 in production): a Cliq DM source, the agent's own
    target."""

    source = "cliq"

    async def load(self, request: ExecuteRequest) -> ActionContext:
        arguments = request.arguments.model_dump(mode="json", exclude_none=True)
        target_id = str(arguments.get("to_address") or arguments.get("to_phone"))
        kind = "email_thread" if request.operation is Operation.EMAIL_SEND else "quo_conversation"
        canonical_context = MappingProxyType(
            {
                "identity_version": "v1",
                "prospect_id": f"prospect:{target_id}",
                "channel_id": 417,
                "conversation_watermark": 806237,
            }
        )
        action_id = action_id_for(request.wakeup_event_id, request.action_role.value, 0)
        return ActionContext(
            action_id=action_id,
            wakeup_event_id=request.wakeup_event_id,
            action_role=request.action_role,
            operation=request.operation,
            intent_kind=request.intent_kind,
            appointment_slot=request.appointment_slot,
            arguments=MappingProxyType(arguments),
            source=self.source,
            source_message_id=806237,
            source_message_key="zoho_cliq:1790427666353_7330935477762",
            source_sent_at=SOURCE_AT,
            conversation_id=f"conversation:cliq:{DM}",
            conversation_watermark=806237,
            prospect_id=f"prospect:{target_id}",
            aliases=(),
            property_id=None,
            property_label=None,
            target=DerivedTarget(kind, target_id, True),
            provider_account="nigel-zoho",
            routing_policy_version="appointment-v1",
            canonical_scope=MappingProxyType({"version": "v1", "role": "prospect_reply"}),
            canonical_context=canonical_context,
            payload_hash=canonical_payload_hash(
                {
                    "action_role": request.action_role.value,
                    "operation": request.operation.value,
                    "intent_kind": request.intent_kind,
                    "appointment_slot": request.appointment_slot,
                    "arguments": arguments,
                    "canonical_context": dict(canonical_context),
                }
            ),
            lock_holder=f"outbound-gateway:{action_id}",
            thread_identity=DM,
            showing_lifecycle_id=f"showing:wake:{request.wakeup_event_id}",
            calendar_event_uid=None,
            channel_id=417,
        )

    async def suggest_targets(self, wakeup_event_id: int) -> dict[str, str]:
        return {}


class Row:
    def __init__(self, cells: dict[str, Any]) -> None:
        self.cells = cells


# What the database returns for wake 27279's email.send. The pre-fix query
# selected the Cliq cron post (806236) as "verified outbound"; the fixed query
# selects only outbound to the action's target, and there is none. The one row
# carries both shapes so each version of the loader reads its own columns;
# tests/integration/test_gateway_newer_outbound.py proves the SQL itself.
WAKE_27279_EVIDENCE_ROW = {
    "later_inbound_message_id": None,
    "later_inbound_message_ids": None,
    "verified_outbound_message_id": 806236,
    "verified_outbound_request_ref": "1790427950439_7335230729575",
    "later_outbound_message_ids": None,
    "latest_sent_at": CRON_POST_AT,
    "calendar_dependency_state": "not_required",
    "calendar_already_applied": False,
}


def harness(
    *,
    evidence_row: dict[str, Any] | None = None,
    later_outbound: tuple[int, ...] = (),
    later_inbound: tuple[int, ...] = (),
    known: tuple[NewerActivity, ...] = (),
    enabled: bool = True,
):
    store = LedgerStore()
    probe = LedgerProbe(store)
    probe.elsewhere.extend(known)
    adapter = CliqAdapter()
    row = dict(evidence_row or WAKE_27279_EVIDENCE_ROW)
    if evidence_row is None:
        row.update(
            verified_outbound_message_id=None,
            verified_outbound_request_ref=None,
            later_outbound_message_ids=list(later_outbound) or None,
            later_inbound_message_ids=list(later_inbound) or None,
            later_inbound_message_id=max(later_inbound) if later_inbound else None,
        )
    evidence = DatabasePreflightEvidenceLoader(object())
    service = OutboundActionService(
        store=store,
        context_loader=Loader(),
        evidence_loader=evidence,
        adapters={Operation.EMAIL_SEND: adapter, Operation.QUO_SMS_SEND: adapter},
        provider_client=object(),
        clock=lambda: EXECUTED_AT,
        lease_owner="outbound-gateway",
        traffic_mode="enforce",
        traffic_probe=probe,
        stale_confirm_enabled=enabled,
    )
    return service, store, probe, adapter, row


def database_returns(row: dict[str, Any]):
    return patch(
        "postgres_mcp.outbound_gateway.evidence.SafeSqlDriver.execute_param_query",
        AsyncMock(return_value=[Row(row)]),
    )


@pytest.mark.asyncio
async def test_wake_27279_an_unrelated_cliq_post_never_completes_the_email_as_already_handled():
    service, store, _probe, adapter, _row = harness(evidence_row=WAKE_27279_EVIDENCE_ROW)

    with database_returns(WAKE_27279_EVIDENCE_ROW):
        first = await service.execute(email_request())

    record = store.rows[FIRST]
    assert (first.status, first.detail_code) == (PublicStatus.SENT, "provider_receipt_verified"), (
        f"email.send ended {first.status.value}/{first.detail_code} (completion {record.completion_kind}); "
        "the Cliq cron post in the source DM is not this email's activity"
    )
    assert adapter.sent == [EMAIL_TEXT]
    assert record.completion_kind is CompletionKind.SENT
    assert record.provider_message_id != "1790427950439_7335230729575"
    assert not [call for call in store.calls if call[0] == "block_stale"]


@pytest.mark.asyncio
async def test_a_newer_outbound_to_the_target_is_a_question_listing_it_as_sent_by_us():
    """(a) A genuine earlier email to the same prospect, after the source
    message: nothing is decided for the agent -- it is asked, and shown the
    email with direction outbound, labelled as ours."""
    service, store, _probe, adapter, row = harness(later_outbound=(806300,), known=(EARLIER_EMAIL,))

    with database_returns(row):
        result = await service.execute(email_request())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert result.detail_code == "stale_context"
    assert result.action_id == FIRST
    assert result.new_context is not None and len(result.new_context) == 1
    item = result.new_context[0]
    assert (item.id, item.source, item.direction) == ("message:806300", "zoho_mail", "outbound")
    assert item.sender == "sent by us (Nigel Pine)"
    assert item.occurred_at == EARLIER_EMAIL.occurred_at
    assert "application link" in item.preview
    assert result.detail and "sent by us" in result.detail
    assert result.question and "outbound" in result.question
    assert adapter.sent == []
    blocked = store.rows[FIRST]
    assert (blocked.state, blocked.detail_code, blocked.error_category) == (ActionState.STALE, "stale_context", None)
    assert blocked.stale_context_shown_refs == ("message:806300",)
    assert blocked.completion_kind is None


@pytest.mark.asyncio
async def test_yes_sends_once_with_the_shown_outbound_waived():
    service, store, _probe, adapter, row = harness(later_outbound=(806300,), known=(EARLIER_EMAIL,))
    with database_returns(row):
        await service.execute(email_request())
        result = await service.confirm(confirm(FIRST, "yes", wake=WAKE))
        again = await service.confirm(confirm(FIRST, "yes", wake=WAKE))

    assert result.status is PublicStatus.SENT, result
    assert result.action_id == SUCCESSOR
    assert again.status is PublicStatus.DUPLICATE and again.action_id == SUCCESSOR
    assert adapter.sent == [EMAIL_TEXT]
    assert store.rows[SUCCESSOR].completion_kind is CompletionKind.SENT


@pytest.mark.asyncio
async def test_no_sends_nothing_and_records_the_decline():
    service, store, _probe, adapter, row = harness(later_outbound=(806300,), known=(EARLIER_EMAIL,))
    with database_returns(row):
        await service.execute(email_request())
        result = await service.confirm(confirm(FIRST, "no", wake=WAKE))

    assert (result.status, result.detail_code) == (PublicStatus.STALE, "stale_context_declined")
    assert adapter.sent == []
    assert store.successor_of(FIRST) is None


@pytest.mark.asyncio
async def test_revise_sends_the_revised_text_to_the_same_recipient():
    service, store, _probe, adapter, row = harness(later_outbound=(806300,), known=(EARLIER_EMAIL,))
    revised = email_request("Following up: did the application link come through?").arguments.model_dump(mode="json", exclude_none=True)
    with database_returns(row):
        await service.execute(email_request())
        result = await service.confirm(confirm(FIRST, "revise", revised, wake=WAKE))

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Following up: did the application link come through?"]
    assert store.rows[SUCCESSOR].arguments["to_address"] == PROSPECT


@pytest.mark.asyncio
async def test_the_identical_request_again_is_the_same_action_and_never_a_second_send():
    """(b) Exact-request dedupe is what stops the agent's own retries."""
    service, store, _probe, adapter, row = harness()
    with database_returns(row):
        first = await service.execute(email_request())
        second = await service.execute(email_request())

    assert first.status is PublicStatus.SENT
    assert second.status is PublicStatus.DUPLICATE
    assert second.action_id == first.action_id == FIRST
    assert adapter.sent == [EMAIL_TEXT]
    assert len([row for row in store.rows.values() if row.wakeup_event_id == WAKE]) == 1


@pytest.mark.asyncio
async def test_a_same_thread_reply_with_a_newer_outbound_in_the_thread_is_asked():
    """(c) A Quo reply in the prospect's own conversation after we already
    texted them there (15 of the 20 historical already_handled rows)."""
    earlier_text = replace(EARLIER_EMAIL, source="quo", message_id=806400, preview="ok great, I have you scheduled")
    service, store, _probe, adapter, row = harness(later_outbound=(806400,), known=(earlier_text,))
    with database_returns(row):
        result = await service.execute(sms_request())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert [(item.id, item.direction) for item in result.new_context] == [("message:806400", "outbound")]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_newer_inbound_keeps_its_existing_suppression_on_first_execute():
    """Newer INBOUND is unchanged: the first execute is stale/newer_inbound,
    whatever outbound there is too."""
    service, _store, _probe, adapter, row = harness(later_outbound=(806300,), later_inbound=(806310,), known=(EARLIER_EMAIL,))
    with database_returns(row):
        result = await service.execute(email_request())

    assert (result.status, result.detail_code) == (PublicStatus.STALE, "newer_inbound")
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_with_confirmation_disabled_nobody_can_be_asked_so_the_send_proceeds(caplog):
    """The gateway is a recorder: when it cannot ask, it does not decide."""
    service, store, _probe, adapter, row = harness(later_outbound=(806300,), known=(EARLIER_EMAIL,), enabled=False)
    with caplog.at_level(logging.WARNING, logger="postgres_mcp.outbound_gateway"), database_returns(row):
        result = await service.execute(email_request())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == [EMAIL_TEXT]
    assert store.rows[FIRST].completion_kind is CompletionKind.SENT
    assert any("806300" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_the_worker_has_nobody_to_ask_and_sends():
    """A calendar-dependent reply resumed by the worker: no agent to ask."""
    service, store, _probe, adapter, row = harness(later_outbound=(806300,), known=(EARLIER_EMAIL,))
    ctx = await Loader().load(email_request())
    ctx = replace(ctx, intent_kind=IntentKind.INQUIRY_REPLY)
    await store.create_or_load(ctx)
    store.rows[FIRST] = replace(store.rows[FIRST], state=ActionState.DEPENDENCY_WAIT, action_role=ActionRole.PROSPECT_REPLY)

    with database_returns(row):
        result = await service.resume(FIRST)

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == [EMAIL_TEXT]
