import itertools
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import MappingProxyType
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.legacy_judgment.preflight import PreflightEvidence as LegacyEvidence
from postgres_mcp.outbound_gateway.legacy_judgment.preflight import SafetyPreflight as LegacyPreflight
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.preflight import CalendarDependencyState
from postgres_mcp.outbound_gateway.preflight import PreflightEvidence
from postgres_mcp.outbound_gateway.preflight import PreflightOutcome
from postgres_mcp.outbound_gateway.preflight import SafetyPreflight

NOW = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc)


def context(**overrides):
    values = {
        "action_id": UUID("8f8f1a45-13a7-4bd3-a15a-f8d265bbc567"),
        "wakeup_event_id": 123,
        "action_role": ActionRole.PROSPECT_REPLY,
        "operation": Operation.EMAIL_SEND,
        "intent_kind": IntentKind.SHOWING_OFFER,
        "appointment_slot": datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
        "arguments": MappingProxyType({"text": "Tour"}),
        "source": "zillow",
        "source_message_id": 700,
        "source_message_key": "zillow:700",
        "source_sent_at": NOW - timedelta(minutes=10),
        "conversation_id": "conversation:zillow-1",
        "conversation_watermark": 700,
        "prospect_id": "prospect:a",
        "aliases": ("email:a@example.com",),
        "property_id": "building:bullman",
        "property_label": "138 Bullman St #144-A",
        "target": DerivedTarget("email_thread", "a@convo.zillow.com", True),
        "provider_account": "nigel-zoho",
        "routing_policy_version": "v1",
        "canonical_scope": MappingProxyType({"role": "prospect_reply"}),
        "canonical_context": MappingProxyType({"source_message_id": 700}),
        "payload_hash": "a" * 64,
        "lock_holder": "outbound-gateway:8f8f1a45-13a7-4bd3-a15a-f8d265bbc567",
        "thread_identity": "zillow-thread-1",
        "showing_lifecycle_id": "showing:123",
        "calendar_event_uid": None,
    }
    values.update(overrides)
    return ActionContext(**values)


def evidence(state=CalendarDependencyState.NOT_REQUIRED):
    return PreflightEvidence(calendar_dependency=state)


@pytest.mark.parametrize(
    "intent",
    [
        IntentKind.SHOWING_CONFIRMATION,
        IntentKind.SHOWING_RESCHEDULE,
        IntentKind.SHOWING_CANCELLATION,
    ],
)
def test_confirmation_reschedule_and_cancellation_wait_for_calendar(intent):
    ctx = context(
        source="quo",
        intent_kind=intent,
        appointment_slot=None if intent is IntentKind.SHOWING_CANCELLATION else context().appointment_slot,
    )
    waiting = SafetyPreflight.evaluate(ctx, evidence(CalendarDependencyState.PENDING))
    assert waiting.outcome == PreflightOutcome.DEPENDENCY_WAIT
    assert waiting.detail_code == "calendar_dependency_pending"
    failed = SafetyPreflight.evaluate(ctx, evidence(CalendarDependencyState.FAILED))
    assert failed.outcome == PreflightOutcome.MANUAL_REVIEW
    assert failed.detail_code == "calendar_dependency_failed"
    ready = SafetyPreflight.evaluate(ctx, evidence(CalendarDependencyState.COMPLETED))
    assert ready.outcome == PreflightOutcome.READY


@pytest.mark.parametrize("role", list(ActionRole))
@pytest.mark.parametrize("intent", list(IntentKind))
@pytest.mark.parametrize("state", list(CalendarDependencyState))
def test_the_preflight_is_the_calendar_dependency_and_nothing_else(role, intent, state):
    """Only a prospect reply confirming, moving or cancelling a showing waits
    on the calendar; everything else is ready -- no freshness, recipient or
    context verdict exists any more."""
    decision = SafetyPreflight.evaluate(context(action_role=role, intent_kind=intent), evidence(state))
    waits = role is ActionRole.PROSPECT_REPLY and intent in {
        IntentKind.SHOWING_CONFIRMATION,
        IntentKind.SHOWING_RESCHEDULE,
        IntentKind.SHOWING_CANCELLATION,
    }
    if not waits or state is CalendarDependencyState.COMPLETED:
        assert decision.outcome is PreflightOutcome.READY
    elif state is CalendarDependencyState.FAILED:
        assert decision.outcome is PreflightOutcome.MANUAL_REVIEW
    else:
        assert decision.outcome is PreflightOutcome.DEPENDENCY_WAIT
    assert {outcome.value for outcome in PreflightOutcome} == {"ready", "dependency_wait", "manual_review"}


# ----------------------------------------------------------------------------
# Why recipient_mismatch / context_mismatch could go: they were unreachable.
# ----------------------------------------------------------------------------


def bf41be6_loader_evidence(ctx):
    """The evidence the deployed loader built at bf41be6 (evidence.py:315-318):
    the "current" recipient, property and slot were copied FROM the context
    being judged, so comparing them with that context compared a value with
    itself."""
    return LegacyEvidence(
        current_recipient_id=ctx.target.target_id,
        current_property_id=ctx.property_id,
        current_appointment_slot=ctx.appointment_slot,
        later_inbound_message_id=None,
        calendar_dependency=CalendarDependencyState.COMPLETED,
        calendar_already_applied=False,
        calendar_context_changed=False,
        overlapping_showing_prospect_ids=(),
        refresh_required_through=NOW,
        refresh=None,
    )


TARGETS = (
    DerivedTarget("email_thread", "a@convo.zillow.com", True),
    DerivedTarget("quo_conversation", "+15705550143", True),
    DerivedTarget("cliq_chat", "1424728044450751028", True),
    DerivedTarget("tenantcloud_thread", "6001", True),
    DerivedTarget("calendar", "nigel", True),
)
PROPERTIES = (None, "building:bullman", "building:other")
SLOTS = (None, datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc), datetime(2026, 7, 18, 9, 0, tzinfo=timezone.utc))


def test_recipient_and_context_mismatch_are_unreachable_through_the_deployed_loader():
    """Try every target kind, property and slot (including values that moved
    in CDS: the evidence is built per call from whatever context is judged)
    through the frozen bf41be6 preflight with the bf41be6 loader's evidence.
    With a verified target -- the only kind the context loader derives -- the
    mismatch branches never fire."""
    tried = 0
    for role, target, property_id, slot in itertools.product(list(ActionRole), TARGETS, PROPERTIES, SLOTS):
        ctx = context(action_role=role, target=target, property_id=property_id, appointment_slot=slot)
        decision = LegacyPreflight.evaluate(ctx, bf41be6_loader_evidence(ctx), now=NOW)
        assert decision.detail_code not in {"recipient_mismatch", "context_mismatch"}, (role, target, property_id, slot)
        tried += 1
    assert tried == len(ActionRole) * len(TARGETS) * len(PROPERTIES) * len(SLOTS)


def test_the_one_residual_path_was_an_unverified_target_which_nothing_writes():
    """The branch fired only on target.verified False. The context loader
    raises instead of deriving one (context.py: "verified target could not be
    derived"), every DerivedTarget it builds is verified, the store records
    recipient_scope from that target, and Comm-Data-Store migration 206
    freezes the record. Only a hand-written record without "verified" would
    reach it (prod: 0 of 844 rows, 2026-09-26)."""
    ctx = context(target=DerivedTarget("email_thread", "a@convo.zillow.com", False))
    assert LegacyPreflight.evaluate(ctx, bf41be6_loader_evidence(ctx), now=NOW).detail_code == "recipient_mismatch"
    # The current preflight has no such verdict: the record decides who receives what.
    assert SafetyPreflight.evaluate(ctx, evidence()).outcome is PreflightOutcome.READY


@pytest.mark.asyncio
async def test_the_worker_sends_the_recorded_recipient_property_and_slot_when_cds_moved_them():
    """The legitimate-change case: property and appointment slot (and even
    the live target) moved in CDS between record and dispatch. The worker
    executes the saved record (migration 206 lock + _recorded_context), so
    the preflight judges -- and the adapter receives -- the recorded values;
    there is nothing left to mismatch against."""
    from postgres_mcp.outbound_gateway.service import _recorded_context

    from .test_service import row

    recorded = context()
    record = row(
        ActionState.PREPARED,
        action_id=recorded.action_id,
        intent_kind=recorded.intent_kind,
        appointment_slot=recorded.appointment_slot,
        canonical_context={"property_id": recorded.property_id, "source_message_id": 700},
        recipient_scope={"kind": "email_thread", "target_id": recorded.target.target_id, "verified": True},
        payload_hash="b" * 64,
    )
    moved = context(
        property_id="building:moved",
        appointment_slot=datetime(2026, 7, 19, 11, 0, tzinfo=timezone.utc),
        target=DerivedTarget("email_thread", "someone-else@convo.zillow.com", True),
    )

    executed = _recorded_context(record, moved)

    assert executed.target == recorded.target
    assert executed.property_id == recorded.property_id
    assert executed.appointment_slot == recorded.appointment_slot
    legacy = LegacyPreflight.evaluate(executed, bf41be6_loader_evidence(executed), now=NOW)
    assert legacy.detail_code not in {"recipient_mismatch", "context_mismatch"}
