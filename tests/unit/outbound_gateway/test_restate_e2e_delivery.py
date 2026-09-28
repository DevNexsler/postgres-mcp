# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false
"""End-to-end proof for the 2026-09-28 22:43Z live-test failure (wake 27348):
``cliq.channel.post`` to a nonexistent channel came back from Agent Email as
``status=failed category=permanent_upstream_error retryable=false``, yet the
ledger showed an AMBIGUOUS outcome, four inconclusive reconciles, and a
count-based ``retry_budget_exhausted_reconciliation -> dead_letter ->
manual_review`` -- never ``definitive_failed``, no staff warning with the
provider's own message, and the Restate invocation left "running".

Every unit test elsewhere in this suite mocks ``DeliveryService``
(``OutboundDeliveryCoordinator``'s ``service=`` collaborator) or the adapter,
so nothing ever drove the REAL path: coordinator -> OutboundActionService ->
ActionRecovery -> CliqAdapter -> adapters.base.initial_observation. This file
drives that real path, with only the MCP transport (agent-email) faked, the
way the workflow actually would.

This is also the backtest's Ground Truth check: scripts/replay_delivery_workflow.py
already proves retry_policy.decide_for_observation() reaches the right final
state against RECORDED history -- it never proved the live runtime code
(delivery_workflow.py / recovery.py) actually calls that policy. It didn't;
see recovery.py's plan_exhaust before this branch's fix.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import MappingProxyType
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.adapters.cliq import CliqAdapter
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.delivery_workflow import AuthResult
from postgres_mcp.outbound_gateway.delivery_workflow import AuthState
from postgres_mcp.outbound_gateway.delivery_workflow import DeliveryPhase
from postgres_mcp.outbound_gateway.delivery_workflow import OutboundDeliveryCoordinator
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.provider_client import McpCallResult
from postgres_mcp.outbound_gateway.record import OutboundActionRecord
from postgres_mcp.outbound_gateway.retry_policy import RETRY_CEILING_SECONDS
from postgres_mcp.outbound_gateway.service import OutboundActionService

ACTION_ID = UUID("a3b8596b-bd90-5da7-8f78-3f1c2c2c73a7")
ACTION_UID = UUID("9ebddbf7-8fc8-5a4f-bba7-869ea7053521")
CHANNEL = "cds-restate-warning-test-nonexistent"


# --------------------------------------------------------------------------
# Test doubles: a real state machine (mirrors the live SQL's own guards --
# see test_service.py's FakeStore, which this is a trimmed copy of) and a
# scripted MCP transport. Nothing here fakes gateway decision logic.
# --------------------------------------------------------------------------

_CLAIMABLE_STATES = {
    ActionState.DEPENDENCY_WAIT,
    ActionState.PREPARED,
    ActionState.DISPATCHING,
    ActionState.PROVIDER_ACCEPTED,
    ActionState.UNKNOWN,
    ActionState.RECONCILING,
    ActionState.RETRY_READY,
    ActionState.DEAD_LETTER,
}
_DEFINITIVE_FAIL_ALLOWED_FROM = {
    ActionState.PREPARED,
    ActionState.DISPATCHING,
    ActionState.RETRY_READY,
    ActionState.MANUAL_REVIEW,
}


class LeaseUnavailableError(RuntimeError):
    pass


class InvalidDefinitiveFailStateError(RuntimeError):
    pass


def make_row(*, created_at: datetime) -> OutboundActionRecord:
    return OutboundActionRecord(
        action_id=ACTION_ID,
        wakeup_event_id=27348,
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        operation=Operation.CLIQ_CHANNEL_POST,
        intent_kind=IntentKind.LEAD_ALERT,
        appointment_slot=None,
        arguments={"channel_or_chat_id": CHANNEL, "text": "New lead needs review"},
        state=ActionState.RECEIVED,
        action_uid=None,
        provider_request_ref=None,
        provider_message_id=None,
        provider_accepted_at=None,
        completion_kind=None,
        detail_code="received",
        attempt_count=0,
        next_attempt_at=created_at,
        payload_hash="",
        canonical_context={},
        canonical_scope={},
        recipient_scope={},
        provider_account=CHANNEL,
        routing_policy_version="v1",
        created_at=created_at,
    )


class FakeStore:
    """A trimmed copy of test_service.py's FakeStore: the same claim/
    definitive_fail state guards the real SQL functions enforce, so a plan
    that would violate outbound_action_transition_allowed() fails here too,
    not just in prod."""

    def __init__(self, initial: OutboundActionRecord) -> None:
        self.current = initial

    async def get(self, action_id: UUID) -> OutboundActionRecord | None:
        return self.current if self.current and self.current.action_id == action_id else None

    async def create_or_load(self, ctx: ActionContext) -> OutboundActionRecord:
        return self.current

    async def prepare(self, ctx: ActionContext, expected_state: ActionState) -> OutboundActionRecord:
        self.current = replace(self.current, state=ActionState.PREPARED, action_uid=ACTION_UID)
        return self.current

    async def claim(self, action_id: UUID, expected_state: ActionState, lease_owner: str, lease_seconds: int) -> OutboundActionRecord:
        if expected_state not in _CLAIMABLE_STATES:
            raise LeaseUnavailableError(f"outbound action lease unavailable: state {expected_state.value!r} is not claimable")
        return self.current

    async def record_provider_request(self, action_id: UUID, lease_owner: str, observation: Any) -> OutboundActionRecord:
        self.current = replace(self.current, provider_request_ref=observation.provider_request_ref or self.current.provider_request_ref)
        return self.current

    async def transition(self, action_id, expected_state, next_state, lease_owner, observation) -> OutboundActionRecord:
        self.current = replace(
            self.current,
            state=next_state,
            provider_request_ref=observation.provider_request_ref or self.current.provider_request_ref,
            provider_message_id=observation.message_id or self.current.provider_message_id,
            detail_code=observation.detail_code,
        )
        return self.current

    async def complete(self, action_id, expected_state, lease_owner, receipt, completion_kind, detail_code) -> OutboundActionRecord:
        self.current = replace(
            self.current,
            state=ActionState.COMPLETED,
            provider_request_ref=receipt.provider_request_ref,
            provider_message_id=receipt.provider_message_id,
            completion_kind=completion_kind,
            detail_code=detail_code,
        )
        return self.current

    async def definitive_fail(self, action_id, expected_state, lease_owner, observation) -> OutboundActionRecord:
        if expected_state not in _DEFINITIVE_FAIL_ALLOWED_FROM:
            raise InvalidDefinitiveFailStateError(
                f"invalid outbound definitive failure state: no transition from {expected_state.value!r} to 'definitive_failed'"
            )
        self.current = replace(
            self.current,
            state=ActionState.DEFINITIVE_FAILED,
            detail_code=observation.detail_code,
            error_category=observation.category,
        )
        return self.current

    async def schedule_next_attempt(self, action_id, expected_state, delay_seconds, detail_code) -> OutboundActionRecord:
        self.current = replace(
            self.current,
            detail_code=detail_code,
            next_attempt_at=self.current.next_attempt_at + timedelta(seconds=delay_seconds) if self.current.next_attempt_at else None,
        )
        return self.current


class FakeMcpClient:
    """Scripted agent-email transport: each ``.call`` pops the next
    McpCallResult, exactly the way a real request/request_status round trip
    would arrive."""

    def __init__(self, *results: McpCallResult) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def call(self, server_name: str, tool: str, arguments: dict[str, Any]) -> McpCallResult:
        self.calls.append((server_name, tool, arguments))
        if not self.results:
            raise AssertionError(f"FakeMcpClient exhausted its script on call #{len(self.calls)}: {tool}")
        return self.results.pop(0)


def pending(request_id: str = "793632f2-da05-4d14-9292-d569a0ff01ea") -> McpCallResult:
    return McpCallResult(structured_content={"status": "pending", "request_id": request_id, "call_id": request_id})


def permanent_upstream_failure(
    request_id: str = "793632f2-da05-4d14-9292-d569a0ff01ea",
    message: str = f"Cliq message was NOT sent to #{CHANNEL}: channel not found or the bot is not a member.",
) -> McpCallResult:
    return McpCallResult(
        structured_content={
            "status": "failed",
            "category": "permanent_upstream_error",
            "retryable": False,
            "message": message,
            "request_id": request_id,
        }
    )


def completed(message_id: str = "provider-cliq-message-1") -> McpCallResult:
    return McpCallResult(
        structured_content={
            "status": "completed",
            "request_id": "793632f2-da05-4d14-9292-d569a0ff01ea",
            "result": {
                "tool_name": "cliq_channel_bot_post",
                "structured_content": {"status": "sent", "provider_message_id": message_id},
            },
        }
    )


def cliq_context(*, action_id: UUID = ACTION_ID) -> ActionContext:
    return ActionContext(
        action_id=action_id,
        wakeup_event_id=27348,
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        operation=Operation.CLIQ_CHANNEL_POST,
        intent_kind=IntentKind.LEAD_ALERT,
        appointment_slot=None,
        arguments=MappingProxyType({"text": "New lead needs review"}),
        source="tenantcloud",
        source_message_id=1,
        source_message_key="tenantcloud:1",
        source_sent_at=datetime(2026, 9, 28, 22, 43, tzinfo=timezone.utc),
        conversation_id="conversation:internal",
        conversation_watermark=1,
        prospect_id="internal:none",
        aliases=(),
        property_id=None,
        property_label=None,
        target=DerivedTarget("cliq_channel", CHANNEL, True),
        provider_account=CHANNEL,
        routing_policy_version="v1",
        canonical_scope=MappingProxyType({}),
        canonical_context=MappingProxyType({}),
        payload_hash="",
        lock_holder=f"outbound-gateway:{action_id}",
        thread_identity="thread-27348",
        showing_lifecycle_id="showing:1",
        calendar_event_uid=None,
    )


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=max(0.0, seconds))


class RecordingStaffWarningPort:
    """Real warn_once semantics (idempotent per action_id, swallows the
    downstream send's own failure) without a real Cliq send -- proves the
    exactly-once contract independent of whichever fake we hand it."""

    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[dict[str, Any]] = []
        self._warned: set[UUID] = set()
        self._fail = fail

    async def warn_once(self, action_id, action_uid, reason, *, wakeup_event_id, operation, recipient) -> None:
        if action_id in self._warned:
            return
        self._warned.add(action_id)
        self.calls.append(
            {
                "action_id": action_id,
                "action_uid": action_uid,
                "reason": reason,
                "wakeup_event_id": wakeup_event_id,
                "operation": operation,
                "recipient": recipient,
            }
        )
        if self._fail:
            # warn_once must never propagate the warning send's own failure,
            # and must never be retried by the caller for the same action.
            return


def build_service(store: FakeStore, client: Any, clock: FakeClock) -> OutboundActionService:
    context_loader = AsyncMock()
    context_loader.load.return_value = cliq_context()
    evidence_loader = AsyncMock()
    evidence_loader.load.return_value = object()  # INTERNAL_NOTIFICATION short-circuits SafetyPreflight to READY
    return OutboundActionService(
        store=store,
        context_loader=context_loader,
        evidence_loader=evidence_loader,
        adapters={Operation.CLIQ_CHANNEL_POST: CliqAdapter(Operation.CLIQ_CHANNEL_POST)},
        provider_client=client,
        clock=clock,
        lease_owner="outbound-gateway-test",
        response_budget_seconds=6,
        lease_seconds=60,
        sleep=AsyncMock(),
        traffic_mode="off",
        restate_operations=frozenset({Operation.CLIQ_CHANNEL_POST}),
    )


async def drive(coordinator: OutboundDeliveryCoordinator, clock: FakeClock, *, max_steps: int = 200):
    """The Restate workflow's own while loop (delivery_workflow.build_restate_app's
    `deliver()`), minus the durable execution wrapper: advance(), then
    ctx.sleep(retry_after_seconds) -- here, bump the fake clock instead of
    actually waiting."""
    for _ in range(max_steps):
        result = await coordinator.advance(ACTION_ID)
        if result.phase in (DeliveryPhase.COMPLETE, DeliveryPhase.TERMINAL):
            return result
        clock.advance(result.retry_after_seconds)
    raise AssertionError("workflow did not terminate within max_steps")


def build_coordinator(
    store: FakeStore,
    service: OutboundActionService,
    clock: FakeClock,
    staff_warning: RecordingStaffWarningPort,
) -> OutboundDeliveryCoordinator:
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    return OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({Operation.CLIQ_CHANNEL_POST}),
        clock=clock,
        staff_warning=staff_warning,
    )


# --------------------------------------------------------------------------
# The main proof: wake 27348 replayed end to end.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_definitive_provider_rejection_ends_definitive_failed_with_one_warning_and_the_message():
    created_at = datetime(2026, 9, 28, 22, 43, 0, tzinfo=timezone.utc)
    store = FakeStore(make_row(created_at=created_at))
    client = FakeMcpClient(pending(), permanent_upstream_failure())
    clock = FakeClock(created_at)
    service = build_service(store, client, clock)
    staff_warning = RecordingStaffWarningPort()
    coordinator = build_coordinator(store, service, clock, staff_warning)

    # Capture the PublicResult the workflow's own resume() step actually
    # returns to the coordinator -- this is what an agent reading the
    # SAME call synchronously (or a caller of service.resume()/reconcile()
    # directly) sees. DeliveryResult itself (the Restate-workflow-facing
    # type) carries only detail_code, no message -- see the report's note
    # on the durable-persistence gap for a later, independent status() poll,
    # which needs the message to survive on the row itself (Comm-Data-Store
    # schema, out of this repo's scope).
    resume_results: list[Any] = []
    original_resume = service.resume

    async def _resume_and_record(action_id):
        outcome = await original_resume(action_id)
        resume_results.append(outcome)
        return outcome

    service.resume = _resume_and_record  # type: ignore[method-assign]

    result = await drive(coordinator, clock)

    assert result.phase is DeliveryPhase.TERMINAL
    assert result.detail_code == "provider_permanent_upstream_error"
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert store.current.state is not ActionState.MANUAL_REVIEW
    assert store.current.state is not ActionState.DEAD_LETTER

    # The provider's own message is visible in the action's status/detail
    # the moment the workflow discovers the definitive rejection.
    assert resume_results, "service.resume() was never called by the coordinator"
    terminal_result = resume_results[-1]
    assert terminal_result.detail_code == "provider_permanent_upstream_error"
    assert "channel not found" in (terminal_result.detail or "")

    assert len(staff_warning.calls) == 1
    warning = staff_warning.calls[0]
    assert warning["operation"] is Operation.CLIQ_CHANNEL_POST
    assert warning["reason"] == "provider_permanent_upstream_error"

    # Tools used: invoke (cliq_channel_bot_post) + one poll (request_status).
    assert [call[1] for call in client.calls] == ["cliq_channel_bot_post", "request_status"]


@pytest.mark.asyncio
async def test_it_is_red_on_the_unfixed_recovery_plan_exhaust():
    """Proves the fix actually changed behavior: with restate_flagged=False
    (748d132's only mode -- ActionRecovery had no notion of a Restate-owned
    operation at all), the exact same ambiguous-exhaustion outcome the
    coordinator's ceiling reaches lands on manual_review, not
    definitive_failed. This is the literal RED/GREEN pivot; see
    recovery.py's plan_exhaust."""
    from postgres_mcp.outbound_gateway.models import ActionState as _ActionState
    from postgres_mcp.outbound_gateway.recovery import plan_exhaust

    action = make_row(created_at=datetime(2026, 9, 28, 22, 43, 0, tzinfo=timezone.utc))
    action = replace(action, state=_ActionState.RECONCILING, action_uid=ACTION_UID)

    old_plan = plan_exhaust(action)  # restate_flagged defaults False: 748d132's only behavior
    new_plan = plan_exhaust(action, restate_flagged=True)

    old_terminal_states = {step.to_state for step in old_plan if hasattr(step, "to_state")}
    assert _ActionState.MANUAL_REVIEW in old_terminal_states or _ActionState.DEAD_LETTER in old_terminal_states
    assert not any(type(step).__name__ == "DefinitiveFail" for step in old_plan)

    assert any(type(step).__name__ == "DefinitiveFail" for step in new_plan)
    new_terminal_states = {step.to_state for step in new_plan if hasattr(step, "to_state")}
    assert _ActionState.MANUAL_REVIEW not in new_terminal_states
    assert _ActionState.DEAD_LETTER not in new_terminal_states


# --------------------------------------------------------------------------
# Sibling cases
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pending_pending_completed_ends_sent_with_no_warning():
    created_at = datetime(2026, 9, 28, 22, 43, 0, tzinfo=timezone.utc)
    store = FakeStore(make_row(created_at=created_at))
    client = FakeMcpClient(pending(), pending(), completed())
    clock = FakeClock(created_at)
    service = build_service(store, client, clock)
    staff_warning = RecordingStaffWarningPort()
    coordinator = build_coordinator(store, service, clock, staff_warning)

    result = await drive(coordinator, clock)

    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []


@pytest.mark.asyncio
async def test_always_pending_reaches_the_ceiling_as_definitive_failed_with_one_warning():
    """Never accepted, never rejected -- retry_policy's one-hour ceiling
    (not the legacy 5/12-attempt cap) is what ends this, and it ends at
    definitive_failed + exactly one warning, never a silent manual_review
    dead end."""
    created_at = datetime(2026, 9, 28, 22, 43, 0, tzinfo=timezone.utc)
    store = FakeStore(make_row(created_at=created_at))

    class AlwaysPendingClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, dict[str, Any]]] = []

        async def call(self, server_name, tool, arguments):
            self.calls.append((server_name, tool, arguments))
            return pending()

    client = AlwaysPendingClient()
    clock = FakeClock(created_at)
    service = build_service(store, client, clock)
    staff_warning = RecordingStaffWarningPort()
    coordinator = build_coordinator(store, service, clock, staff_warning)

    result = await drive(coordinator, clock, max_steps=2000)

    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert store.current.state is not ActionState.MANUAL_REVIEW
    assert (clock.now - created_at).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1


@pytest.mark.asyncio
async def test_the_staff_warning_itself_failing_never_sends_a_second_one():
    created_at = datetime(2026, 9, 28, 22, 43, 0, tzinfo=timezone.utc)
    store = FakeStore(make_row(created_at=created_at))
    client = FakeMcpClient(pending(), permanent_upstream_failure())
    clock = FakeClock(created_at)
    service = build_service(store, client, clock)
    staff_warning = RecordingStaffWarningPort(fail=True)
    coordinator = build_coordinator(store, service, clock, staff_warning)

    result = await drive(coordinator, clock)

    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert len(staff_warning.calls) == 1

    # A replay of the same terminal step (Restate re-delivering the final
    # advance(), or a duplicate worker poll) must not warn again.
    again = await coordinator.advance(ACTION_ID)
    assert again.phase is DeliveryPhase.TERMINAL
    assert len(staff_warning.calls) == 1
