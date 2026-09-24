# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportOptionalIterable=false, reportOptionalSubscript=false, reportOperatorIssue=false
"""stale_context as a question: needs_confirmation -> confirm yes | no | revise.

Built on wake 27164 (2026-09-23): Dan asked Nigel in the Cliq DM to "reply
pong once"; newer activity landed in the same DM after the wake's context
watermark; the gateway refused `pong` as stale_context and wrote it
definitive_failed/traffic_blocked, the envelope called that refusal final, and
the reconciler paged delivery_failed. (That particular activity was a cron
alert, which the probe's Cliq exemption now ignores in either direction label;
here the newer activity is a human message in the same DM, the case the
question exists for. The fake probe stands in for the SQL.) The fake ledger
below enforces the same guards Comm-Data-Store migration 192 enforces in SQL
(one answer per blocked action, same wake, successor minted once, revise may
change content only), so these tests exercise the service's real control flow
end to end.
"""


from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import MappingProxyType
from typing import Any
from uuid import UUID
from uuid import uuid5

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.adapters.base import ProviderReceipt
from postgres_mcp.outbound_gateway.context import ACTION_NAMESPACE
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.context import canonical_payload_hash
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import ConfirmRequest
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.preflight import CalendarDependencyState
from postgres_mcp.outbound_gateway.preflight import PreflightEvidence
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import handle_outbound_action
from postgres_mcp.outbound_gateway.service import OutboundActionRecord
from postgres_mcp.outbound_gateway.service import OutboundActionService
from postgres_mcp.outbound_gateway.traffic_control import NewerActivity

WAKE = 27164
CHAT = "1424728044450751028"
SUBJECT = f"internal:{CHAT}"
# Real timeline, wake 27164 (UTC).
WATERMARK = datetime(2026, 9, 23, 19, 36, 29, 938418, tzinfo=timezone.utc)
CRON_ALERT_AT = datetime(2026, 9, 23, 19, 36, 56, 572000, tzinfo=timezone.utc)
EXECUTED_AT = datetime(2026, 9, 23, 19, 37, 0, 335447, tzinfo=timezone.utc)
SECOND_ALERT_AT = datetime(2026, 9, 23, 19, 38, 56, 545000, tzinfo=timezone.utc)
CRON_ALERT = NewerActivity(
    direction="inbound",
    source="zoho_cliq",
    occurred_at=CRON_ALERT_AT,
    preview="wait, is that cron alert about the gateway you are testing?",
    message_id=750824,
    action_id=None,
    sender="Dan Park",
)
DAN_FOLLOW_UP = NewerActivity(
    direction="inbound",
    source="zoho_cliq",
    occurred_at=SECOND_ALERT_AT,
    preview="never mind, skip the pong",
    message_id=750826,
    action_id=None,
    sender="Dan Park",
)


def action_id_for(wake: int, role: str, ordinal: int) -> UUID:
    return uuid5(ACTION_NAMESPACE, f"v1:wakeup:{wake}:role:{role}:ordinal:{ordinal}")


def execute_request(text: str = "pong", chat: str = CHAT, *, override: bool = False) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": WAKE,
            "action_role": "internal_reply",
            "operation": "cliq.chat.post",
            "intent_kind": "internal_reply",
            "arguments": {"text": text, "channel_or_chat_id": chat},
            "override": override,
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


class FakeLoader:
    """Derives an ActionContext from a request the way ActionContextLoader
    does for these fields: ordinal-0 identity, payload hash over the request."""

    def __init__(self) -> None:
        self.canonical_context = MappingProxyType(
            {"identity_version": "v1", "prospect_id": SUBJECT, "conversation_watermark": 750823}
        )

    async def load(self, request: ExecuteRequest) -> ActionContext:
        arguments = request.arguments.model_dump(mode="json", exclude_none=True)
        action_id = action_id_for(request.wakeup_event_id, request.action_role.value, 0)
        payload_hash = canonical_payload_hash(
            {
                "action_role": request.action_role.value,
                "operation": request.operation.value,
                "intent_kind": request.intent_kind,
                "appointment_slot": request.appointment_slot,
                "arguments": arguments,
                "canonical_context": dict(self.canonical_context),
            }
        )
        return ActionContext(
            action_id=action_id,
            wakeup_event_id=request.wakeup_event_id,
            action_role=request.action_role,
            operation=request.operation,
            intent_kind=request.intent_kind,
            appointment_slot=request.appointment_slot,
            arguments=MappingProxyType(arguments),
            source="zoho_cliq",
            source_message_id=750823,
            source_message_key="zoho_cliq:1790191970340_6703634556964",
            source_sent_at=WATERMARK - timedelta(minutes=4),
            conversation_id=f"conversation:cliq:{CHAT}",
            conversation_watermark=750823,
            prospect_id=SUBJECT,
            aliases=(),
            property_id=None,
            property_label=None,
            target=DerivedTarget(
                "cliq_chat" if "channel_or_chat_id" in arguments else "phone",
                str(arguments.get("channel_or_chat_id") or arguments.get("to_phone")),
                True,
            ),
            provider_account=CHAT,
            routing_policy_version="appointment-v1",
            canonical_scope=MappingProxyType({"version": "v1"}),
            canonical_context=self.canonical_context,
            payload_hash=payload_hash,
            lock_holder=f"outbound-gateway:{action_id}",
            thread_identity=f"cliq:{CHAT}",
            showing_lifecycle_id=f"showing:wake:{request.wakeup_event_id}",
            calendar_event_uid=None,
        )

    async def suggest_targets(self, wakeup_event_id: int) -> dict[str, str]:
        return {}


class LedgerStore:
    """An outbound_actions ledger with migration 192's stale-context guards."""

    def __init__(self) -> None:
        self.rows: dict[UUID, OutboundActionRecord] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.wake_terminal = False

    def _put(self, record: OutboundActionRecord) -> OutboundActionRecord:
        self.rows[record.action_id] = record
        return record

    def successor_of(self, action_id: UUID) -> OutboundActionRecord | None:
        return next((row for row in self.rows.values() if row.retry_of_action_id == action_id), None)

    async def create_or_load(self, ctx: ActionContext) -> OutboundActionRecord:
        self.calls.append(("create", ctx.action_id))
        existing = self.rows.get(ctx.action_id)
        if existing is not None:
            if existing.payload_hash != ctx.payload_hash:
                raise ValueError(f"outbound action immutable context mismatch for {ctx.action_id}")
            return existing
        return self._put(
            OutboundActionRecord(
                action_id=ctx.action_id,
                wakeup_event_id=ctx.wakeup_event_id,
                action_role=ctx.action_role,
                operation=ctx.operation,
                intent_kind=ctx.intent_kind,
                appointment_slot=ctx.appointment_slot,
                arguments=dict(ctx.arguments),
                state=ActionState.RECEIVED,
                action_uid=None,
                provider_request_ref=None,
                provider_message_id=None,
                provider_accepted_at=None,
                completion_kind=None,
                detail_code="received",
                attempt_count=0,
                next_attempt_at=EXECUTED_AT,
                payload_hash=ctx.payload_hash,
                canonical_context=dict(ctx.canonical_context),
                canonical_scope=dict(ctx.canonical_scope),
                recipient_scope={"kind": ctx.target.kind, "target_id": ctx.target.target_id, "verified": True},
                provider_account=ctx.provider_account,
                routing_policy_version=ctx.routing_policy_version,
            )
        )

    async def block_stale_context(self, action_id, expected_state, lease_owner, acknowledged_through):
        self.calls.append(("block_stale", action_id, expected_state, acknowledged_through))
        current = self.rows[action_id]
        assert current.state is expected_state
        if expected_state is ActionState.STALE:
            assert current.detail_code == "stale_context" and current.stale_context_decision is None
            point = max(current.stale_context_acknowledged_through or acknowledged_through, acknowledged_through)
            return self._put(replace(current, stale_context_acknowledged_through=point))
        return self._put(
            replace(
                current,
                state=ActionState.STALE,
                detail_code="stale_context",
                error_category=None,
                stale_context_acknowledged_through=acknowledged_through,
            )
        )

    async def confirm_stale_context(self, action_id, *, wakeup_event_id, decision, actor, revision=None):
        self.calls.append(("confirm_stale", action_id, decision))
        parent = self.rows[action_id]
        if parent.wakeup_event_id != wakeup_event_id:
            raise PermissionError("stale context confirmation must come from the blocked action's own wake")
        if parent.state is not ActionState.STALE or not parent.detail_code.startswith("stale_context"):
            raise ValueError("action is not awaiting a stale context confirmation")
        payload_hash = revision.payload_hash if revision is not None else parent.payload_hash
        if parent.stale_context_decision is not None:
            if parent.stale_context_decision != decision:
                raise ValueError(f"stale context already answered {parent.stale_context_decision}")
            if decision == "no":
                return parent
            successor = self.successor_of(parent.action_id)
            assert successor is not None
            if successor.payload_hash != payload_hash:
                raise ValueError("stale context already answered with a different revision")
            return successor
        if decision == "no":
            return self._put(replace(parent, stale_context_decision="no", detail_code="stale_context_declined"))
        if self.wake_terminal:
            raise ValueError("wake is terminal")
        ordinal = 1 + sum(1 for row in self.rows.values() if row.wakeup_event_id == parent.wakeup_event_id and row.retry_of_action_id)
        successor_id = action_id_for(parent.wakeup_event_id, parent.action_role.value, ordinal)
        successor = replace(
            parent,
            action_id=successor_id,
            state=ActionState.RECEIVED,
            detail_code="stale_context_confirmed" if decision == "yes" else "stale_context_revised",
            retry_of_action_id=parent.action_id,
            remediation_reason="stale_context_confirmed" if decision == "yes" else "stale_context_revised",
            stale_context_acknowledged_through=None,
            stale_context_decision=None,
            arguments=dict(revision.arguments) if revision is not None else dict(parent.arguments),
            payload_hash=payload_hash,
        )
        self._put(
            replace(
                parent,
                stale_context_decision=decision,
                detail_code="stale_context_confirmed" if decision == "yes" else "stale_context_revised",
            )
        )
        return self._put(successor)

    async def prepare(self, ctx, expected_state):
        self.calls.append(("prepare", ctx.action_id, expected_state))
        return self._put(replace(self.rows[ctx.action_id], state=ActionState.PREPARED, action_uid=ctx.action_id))

    async def claim(self, action_id, expected_state, lease_owner, lease_seconds):
        self.calls.append(("claim", action_id, expected_state))
        return self.rows[action_id]

    async def record_provider_request(self, action_id, lease_owner, observation):
        return self._put(replace(self.rows[action_id], provider_request_ref=observation.provider_request_ref))

    async def transition(self, action_id, expected_state, next_state, lease_owner, observation):
        self.calls.append(("transition", action_id, expected_state, next_state, observation.detail_code))
        return self._put(replace(self.rows[action_id], state=next_state, detail_code=observation.detail_code))

    async def complete(self, action_id, expected_state, lease_owner, receipt, completion_kind, detail_code):
        self.calls.append(("complete", action_id))
        return self._put(
            replace(
                self.rows[action_id],
                state=ActionState.COMPLETED,
                provider_request_ref=receipt.provider_request_ref,
                provider_message_id=receipt.provider_message_id,
                completion_kind=completion_kind,
                detail_code=detail_code,
            )
        )

    async def definitive_fail(self, action_id, expected_state, lease_owner, observation):
        self.calls.append(("definitive_fail", action_id, observation.detail_code))
        return self._put(
            replace(
                self.rows[action_id],
                state=ActionState.DEFINITIVE_FAILED,
                detail_code=observation.detail_code,
                error_category=observation.category,
            )
        )

    async def remediate_traffic_block(self, action_id, *, operator_identity, reason):
        raise AssertionError("the stale-context path never uses operator remediation")

    async def schedule_next_attempt(self, action_id, expected_state, delay_seconds, detail_code):
        return self._put(replace(self.rows[action_id], detail_code=detail_code))

    async def get(self, action_id):
        return self.rows.get(action_id)


class LedgerProbe:
    """Traffic probe over the same ledger: acknowledged_through is read back
    from the blocked rows exactly like the SQL reads it."""

    def __init__(self, store: LedgerStore, *activity: NewerActivity) -> None:
        self.store = store
        self.activity = list(activity)
        self.asked_after: list[datetime] = []
        self.message_times = {item.message_id: item.occurred_at for item in activity if item.message_id}

    async def in_flight_actions(self, recipient_key, exclude_action_id):
        return []

    async def activity_after(self, recipient_key, channel_id, watermark, exclude_action_id, limit):
        self.asked_after.append(watermark)
        found = [item for item in self.activity if item.occurred_at > watermark]
        return sorted(found, key=lambda item: item.occurred_at, reverse=True)[:limit]

    async def context_watermark(self, wakeup_event_id):
        return WATERMARK

    async def acknowledged_through(self, wakeup_event_id, recipient_key):
        points = [
            row.stale_context_acknowledged_through
            for row in self.store.rows.values()
            if row.wakeup_event_id == wakeup_event_id
            and row.state is ActionState.STALE
            and row.stale_context_acknowledged_through is not None
        ]
        return max(points) if points else None

    async def message_created_at(self, message_id):
        return self.message_times.get(message_id)


class CliqAdapter:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self._current: ActionContext | None = None

    def build_request(self, ctx, action_uid):
        self._current = ctx
        return ctx

    async def invoke(self, client, provider_request):
        self.sent.append(str(provider_request.arguments["text"]))
        return ProviderObservation(
            ProviderDisposition.ACCEPTED,
            "provider_accepted",
            provider_request_ref=f"cliq-{len(self.sent)}",
            message_id=f"cliq-msg-{len(self.sent)}",
            accepted_at=EXECUTED_AT,
            evidence={"kind": "provider_message_id"},
        )

    async def poll(self, client, observation):
        return observation

    def parse_receipt(self, ctx, observation):
        return ProviderReceipt(
            provider_request_ref=observation.provider_request_ref,
            provider_message_id=observation.message_id,
            accepted_at=observation.accepted_at,
            evidence=observation.evidence,
        )

    async def reconcile(self, client, ctx, action_uid, observation):
        raise AssertionError("no reconciliation in these scenarios")


class StaticEvidence:
    def __init__(self, later_inbound_message_id: int | None = None) -> None:
        self.later_inbound_message_id = later_inbound_message_id

    async def load(self, ctx: ActionContext) -> PreflightEvidence:
        return PreflightEvidence(
            current_recipient_id=ctx.target.target_id,
            current_property_id=ctx.property_id,
            current_appointment_slot=ctx.appointment_slot,
            later_inbound_message_id=self.later_inbound_message_id,
            verified_outbound_message_id=None,
            verified_outbound_request_ref=None,
            verified_outbound_covers_source=False,
            calendar_dependency=CalendarDependencyState.NOT_REQUIRED,
            calendar_already_applied=False,
            calendar_context_changed=False,
            overlapping_showing_prospect_ids=(),
            refresh_required_through=ctx.source_sent_at,
            refresh=None,
        )


def harness(*activity: NewerActivity, evidence: StaticEvidence | None = None):
    store = LedgerStore()
    probe = LedgerProbe(store, *activity)
    adapter = CliqAdapter()
    service = OutboundActionService(
        store=store,
        context_loader=FakeLoader(),
        evidence_loader=evidence or StaticEvidence(),
        adapters={Operation.CLIQ_CHAT_POST: adapter, Operation.QUO_SMS_SEND: adapter},
        provider_client=object(),
        clock=lambda: EXECUTED_AT,
        lease_owner="outbound-gateway",
        traffic_mode="enforce",
        traffic_probe=probe,
    )
    return service, store, probe, adapter


def confirm(action_id: UUID, decision: str, arguments: dict[str, Any] | None = None, *, wake: int | None = WAKE):
    payload: dict[str, Any] = {"op": "confirm", "action_id": str(action_id), "decision": decision}
    if wake is not None:
        payload["wakeup_event_id"] = wake
    if arguments is not None:
        payload["arguments"] = arguments
    parsed = parse_outbound_request(payload)
    assert isinstance(parsed, ConfirmRequest)
    return parsed


BLOCKED = action_id_for(WAKE, "internal_reply", 0)
SUCCESSOR = action_id_for(WAKE, "internal_reply", 1)


@pytest.mark.asyncio
async def test_wake_27164_refusal_is_a_question_with_the_cron_alert_as_new_context():
    service, store, _probe, adapter = harness(CRON_ALERT)

    result = await service.execute(execute_request())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION
    assert result.retryable is True
    assert result.detail_code == "stale_context"
    assert result.action_id == BLOCKED
    assert [(item.id, item.source, item.direction) for item in result.new_context] == [
        ("message:750824", "zoho_cliq", "inbound")
    ]
    assert result.new_context[0].occurred_at == CRON_ALERT_AT
    assert f'"action_id": "{BLOCKED}", "decision": "yes"' in result.question
    assert '"decision": "no"' in result.question
    assert '"decision": "revise"' in result.question
    assert adapter.sent == []
    blocked = store.rows[BLOCKED]
    # A deliberate no-send, not a failure: stale, no error_category.
    assert blocked.state is ActionState.STALE
    assert blocked.detail_code == "stale_context"
    assert blocked.error_category is None
    assert blocked.stale_context_acknowledged_through == CRON_ALERT_AT


@pytest.mark.asyncio
async def test_yes_sends_pong_once_through_a_successor_with_staleness_rechecked_at_the_acknowledged_point():
    service, store, probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())

    result = await service.confirm(confirm(BLOCKED, "yes"))

    assert result.status is PublicStatus.SENT
    assert result.action_id == SUCCESSOR
    assert adapter.sent == ["pong"]
    successor = store.rows[SUCCESSOR]
    assert successor.retry_of_action_id == BLOCKED
    assert successor.payload_hash == store.rows[BLOCKED].payload_hash
    assert successor.state is ActionState.COMPLETED
    assert store.rows[BLOCKED].stale_context_decision == "yes"
    assert store.rows[BLOCKED].state is ActionState.STALE
    # The successor's staleness check started from the acknowledged point,
    # not the wake watermark -- the shown cron alert cannot re-block it.
    assert probe.asked_after[-1] == CRON_ALERT_AT


@pytest.mark.asyncio
async def test_second_yes_returns_the_existing_successor_and_never_sends_twice():
    service, _store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())
    first = await service.confirm(confirm(BLOCKED, "yes"))

    second = await service.confirm(confirm(BLOCKED, "yes"))

    assert first.status is PublicStatus.SENT
    assert second.status is PublicStatus.DUPLICATE
    assert second.action_id == first.action_id == SUCCESSOR
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_no_records_a_decline_sends_nothing_and_repeats_idempotently():
    service, store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())

    declined = await service.confirm(confirm(BLOCKED, "no"))
    again = await service.confirm(confirm(BLOCKED, "no"))

    for result in (declined, again):
        assert result.status is PublicStatus.STALE
        assert result.action_id == BLOCKED
        assert result.detail_code == "stale_context_declined"
        assert "not a failure" in result.detail
    assert adapter.sent == []
    assert store.rows[BLOCKED].stale_context_decision == "no"
    assert store.successor_of(BLOCKED) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first", "second"),
    [("no", "yes"), ("yes", "no"), ("yes", "revise"), ("revise", "yes"), ("no", "revise")],
)
async def test_a_different_answer_after_one_was_recorded_is_refused(first, second):
    service, _store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())
    revised = {"text": "pong (the cron alert is unrelated)", "channel_or_chat_id": CHAT}
    await service.confirm(confirm(BLOCKED, first, revised if first == "revise" else None))
    sent_before = list(adapter.sent)

    with pytest.raises(ValueError, match="already answered"):
        await service.confirm(confirm(BLOCKED, second, revised if second == "revise" else None))

    assert adapter.sent == sent_before


@pytest.mark.asyncio
async def test_revise_sends_the_revised_text_to_the_same_target():
    service, store, probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())
    revised = {"text": "pong (ignore the cron alert above)", "channel_or_chat_id": CHAT}

    result = await service.confirm(confirm(BLOCKED, "revise", revised))

    assert result.status is PublicStatus.SENT
    assert result.action_id == SUCCESSOR
    assert adapter.sent == ["pong (ignore the cron alert above)"]
    successor = store.rows[SUCCESSOR]
    assert successor.retry_of_action_id == BLOCKED
    assert successor.arguments["text"] == "pong (ignore the cron alert above)"
    assert successor.payload_hash != store.rows[BLOCKED].payload_hash
    assert store.rows[BLOCKED].stale_context_decision == "revise"
    assert store.rows[BLOCKED].detail_code == "stale_context_revised"
    assert probe.asked_after[-1] == CRON_ALERT_AT

    # The same revision again is idempotent; a different one is refused.
    again = await service.confirm(confirm(BLOCKED, "revise", revised))
    assert again.status is PublicStatus.DUPLICATE and again.action_id == SUCCESSOR
    with pytest.raises(ValueError, match="different revision"):
        await service.confirm(confirm(BLOCKED, "revise", {"text": "pong!", "channel_or_chat_id": CHAT}))
    assert adapter.sent == ["pong (ignore the cron alert above)"]


@pytest.mark.asyncio
async def test_revise_that_changes_the_target_is_refused_before_anything_is_written():
    service, store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())

    with pytest.raises(ValueError, match="channel_or_chat_id must stay exactly as refused"):
        await service.confirm(confirm(BLOCKED, "revise", {"text": "pong", "channel_or_chat_id": "999"}))

    assert adapter.sent == []
    assert not any(call[0] == "confirm_stale" for call in store.calls)
    assert store.rows[BLOCKED].stale_context_decision is None


@pytest.mark.asyncio
async def test_revise_with_invalid_arguments_is_refused():
    service, _store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())

    with pytest.raises(ValueError, match="invalid arguments"):
        await service.confirm(confirm(BLOCKED, "revise", {"text": "", "channel_or_chat_id": CHAT}))
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_newer_activity_after_the_refusal_raises_another_question_on_the_successor():
    """Only activity NEWER than what the agent was shown re-asks -- and it
    re-asks on the successor, never sending over unseen context."""
    service, store, probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())
    probe.activity.append(DAN_FOLLOW_UP)

    result = await service.confirm(confirm(BLOCKED, "yes"))

    assert result.status is PublicStatus.NEEDS_CONFIRMATION
    assert result.action_id == SUCCESSOR
    assert [item.id for item in result.new_context] == ["message:750826"]
    assert result.new_context[0].sender == "Dan Park"
    assert f'"action_id": "{SUCCESSOR}"' in result.question
    assert adapter.sent == []
    assert store.rows[SUCCESSOR].state is ActionState.STALE
    assert store.rows[SUCCESSOR].stale_context_acknowledged_through == SECOND_ALERT_AT

    declined = await service.confirm(confirm(SUCCESSOR, "no"))
    assert declined.detail_code == "stale_context_declined"
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_newer_activity_after_the_refusal_raises_another_question_on_a_revise_successor():
    service, store, probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())
    probe.activity.append(DAN_FOLLOW_UP)

    result = await service.confirm(confirm(BLOCKED, "revise", {"text": "pong, sorry", "channel_or_chat_id": CHAT}))

    assert result.status is PublicStatus.NEEDS_CONFIRMATION
    assert result.action_id == SUCCESSOR
    assert store.rows[SUCCESSOR].arguments["text"] == "pong, sorry"
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_confirm_is_refused_for_another_wake_and_for_a_non_stale_action():
    service, store, _probe, adapter = harness()
    sent = await service.execute(execute_request())
    assert sent.status is PublicStatus.SENT

    with pytest.raises(ValueError, match="not awaiting a stale_context confirmation"):
        await service.confirm(confirm(BLOCKED, "yes"))

    service, store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())
    with pytest.raises(ValueError, match="belongs to wake 27164, not wake 27165"):
        await service.confirm(confirm(BLOCKED, "yes", wake=27165))
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_confirm_without_wake_id_uses_the_blocked_actions_own_wake():
    service, _store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())

    result = await service.confirm(confirm(BLOCKED, "yes", wake=None))

    assert result.status is PublicStatus.SENT
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_repeated_execute_of_the_blocked_message_asks_again_and_override_answers_yes():
    service, store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())

    again = await service.execute(execute_request())
    assert again.status is PublicStatus.NEEDS_CONFIRMATION
    assert again.action_id == BLOCKED
    assert [item.id for item in again.new_context] == ["message:750824"]
    assert ("block_stale", BLOCKED, ActionState.STALE, CRON_ALERT_AT) in store.calls
    assert adapter.sent == []

    overridden = await service.execute(execute_request(override=True))
    assert overridden.status is PublicStatus.SENT
    assert overridden.action_id == SUCCESSOR
    assert store.rows[BLOCKED].stale_context_decision == "yes"
    # And a later identical execute reports the successor, never a second send.
    repeated = await service.execute(execute_request())
    assert repeated.status is PublicStatus.DUPLICATE and repeated.action_id == SUCCESSOR
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_first_execute_with_override_records_the_block_then_sends_through_the_successor():
    service, store, _probe, adapter = harness(CRON_ALERT)

    result = await service.execute(execute_request(override=True))

    assert result.status is PublicStatus.SENT
    assert result.action_id == SUCCESSOR
    assert store.rows[BLOCKED].state is ActionState.STALE
    assert store.rows[BLOCKED].stale_context_decision == "yes"
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_a_different_message_executed_after_the_refusal_is_the_revise_answer():
    service, store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())

    result = await service.execute(execute_request("pong (cron alert is unrelated)"))

    assert result.status is PublicStatus.SENT
    assert result.action_id == SUCCESSOR
    assert store.rows[BLOCKED].stale_context_decision == "revise"
    assert adapter.sent == ["pong (cron alert is unrelated)"]

    other, _other_store, _other_probe, other_adapter = harness(CRON_ALERT)
    await other.execute(execute_request())
    with pytest.raises(ValueError, match="channel_or_chat_id must stay exactly as refused"):
        await other.execute(execute_request("pong", chat="999"))
    assert other_adapter.sent == []


@pytest.mark.asyncio
async def test_worker_resume_still_terminalizes_staleness_nobody_can_answer():
    """resume() is the worker: no agent to ask, so a stale PREPARED row keeps
    the terminal traffic_blocked failure that pages."""
    service, store, _probe, adapter = harness(CRON_ALERT)
    ctx = await FakeLoader().load(execute_request())
    await store.create_or_load(ctx)
    await store.prepare(ctx, ActionState.RECEIVED)

    result = await service.resume(BLOCKED)

    assert result.status is PublicStatus.FAILED
    assert result.detail_code == "stale_context"
    assert store.rows[BLOCKED].error_category == "traffic_blocked"
    assert adapter.sent == []


def quo_reply_request(text: str = "Yes, Friday at 10 still works.") -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": WAKE,
            "action_role": "prospect_reply",
            "operation": "quo.sms.send",
            "intent_kind": "inquiry_reply",
            "arguments": {"text": text, "to_phone": "+15705550143"},
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


@pytest.mark.asyncio
async def test_prospect_reply_yes_waives_the_shown_newer_inbound():
    """Wake 27143's class (Quo prospect reply): the preflight refuses any
    inbound newer than the source message (`newer_inbound`). Once the agent
    was shown that inbound and answered yes, it is no longer unseen."""
    prospect_text = replace(CRON_ALERT, source="quo", preview="Is Friday still open?")
    service, store, _probe, adapter = harness(prospect_text, evidence=StaticEvidence(later_inbound_message_id=750824))
    blocked = await service.execute(quo_reply_request())
    assert blocked.status is PublicStatus.NEEDS_CONFIRMATION

    result = await service.confirm(confirm(blocked.action_id, "yes"))

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Yes, Friday at 10 still works."]


@pytest.mark.asyncio
async def test_prospect_reply_yes_still_refuses_an_inbound_the_agent_never_saw():
    prospect_text = replace(CRON_ALERT, source="quo", preview="Is Friday still open?")
    service, store, probe, adapter = harness(prospect_text, evidence=StaticEvidence(later_inbound_message_id=750899))
    # 750899 reached CDS after the question was asked but is not (yet) in the
    # traffic probe's view -- the preflight is the only witness.
    probe.message_times[750899] = CRON_ALERT_AT + timedelta(seconds=30)
    blocked = await service.execute(quo_reply_request())

    result = await service.confirm(confirm(blocked.action_id, "yes"))

    assert result.status is PublicStatus.STALE
    assert result.detail_code == "newer_inbound"
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_tenantcloud_style_confirm_prepares_for_restate_without_provider_io():
    service, store, _probe, adapter = harness(CRON_ALERT)
    await service.enqueue(execute_request())
    assert store.rows[BLOCKED].state is ActionState.STALE

    result = await service.confirm(confirm(BLOCKED, "yes"), dispatch=False)

    assert result.status is PublicStatus.PENDING
    assert store.rows[SUCCESSOR].state is ActionState.PREPARED
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_wire_shape_of_needs_confirmation_and_of_an_ordinary_result():
    service, _store, _probe, _adapter = harness(CRON_ALERT)
    policy = FeaturePolicy(writes_enabled=True, kill_switch=False, enabled_operations=frozenset({Operation.CLIQ_CHAT_POST}))
    execute = {
        "op": "execute",
        "wakeup_event_id": WAKE,
        "action_role": "internal_reply",
        "operation": "cliq.chat.post",
        "intent_kind": "internal_reply",
        "arguments": {"text": "pong", "channel_or_chat_id": CHAT},
    }

    asked = await handle_outbound_action(service, policy, execute)

    assert asked["status"] == "needs_confirmation"
    assert asked["retryable"] is True
    assert asked["detail_code"] == "stale_context"
    assert asked["new_context"] == [
        {
            "id": "message:750824",
            "source": "zoho_cliq",
            "direction": "inbound",
            "sender": "Dan Park",
            "occurred_at": CRON_ALERT_AT.isoformat().replace("+00:00", "Z"),
            "preview": "wait, is that cron alert about the gateway you are testing?",
        }
    ]
    assert '"decision": "revise"' in asked["question"]

    answered = await handle_outbound_action(
        service, policy, {"op": "confirm", "action_id": asked["action_id"], "decision": "yes"}
    )
    assert answered["status"] == "sent"
    assert answered["retryable"] is False
    assert "new_context" not in answered and "question" not in answered and "detail" not in answered


@pytest.mark.asyncio
async def test_yes_obeys_the_write_switches_but_no_is_always_recordable():
    service, store, _probe, adapter = harness(CRON_ALERT)
    await service.execute(execute_request())
    closed = FeaturePolicy(writes_enabled=True, kill_switch=True, enabled_operations=frozenset({Operation.CLIQ_CHAT_POST}))

    refused = await handle_outbound_action(service, closed, {"op": "confirm", "action_id": str(BLOCKED), "decision": "yes"})
    assert refused["status"] == "rejected" and refused["detail_code"] == "kill_switch_open"
    assert store.rows[BLOCKED].stale_context_decision is None

    declined = await handle_outbound_action(service, closed, {"op": "confirm", "action_id": str(BLOCKED), "decision": "no"})
    assert declined["detail_code"] == "stale_context_declined"
    assert adapter.sent == []


def test_confirm_request_contract():
    parsed = parse_outbound_request({"op": "confirm", "action_id": str(BLOCKED), "decision": " YES "})
    assert isinstance(parsed, ConfirmRequest)
    assert parsed.decision.value == "yes" and parsed.wakeup_event_id is None
    with pytest.raises(ValueError, match="revise requires arguments"):
        parse_outbound_request({"op": "confirm", "action_id": str(BLOCKED), "decision": "revise"})
    with pytest.raises(ValueError, match="only accepted with decision revise"):
        parse_outbound_request({"op": "confirm", "action_id": str(BLOCKED), "decision": "yes", "arguments": {"text": "x"}})
    with pytest.raises(ValueError):
        parse_outbound_request({"op": "confirm", "action_id": str(BLOCKED), "decision": "maybe"})
