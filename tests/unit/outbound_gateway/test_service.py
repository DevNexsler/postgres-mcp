from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import MappingProxyType
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.adapters.base import ProviderReceipt
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.metrics import CircuitStatus
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import CompletionKind
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.preflight import CalendarDependencyState
from postgres_mcp.outbound_gateway.preflight import PreflightEvidence
from postgres_mcp.outbound_gateway.service import OutboundActionRecord
from postgres_mcp.outbound_gateway.service import OutboundActionService

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")
ACTION_UID = UUID("9ebddbf7-8fc8-5a4f-bba7-869ea7053521")
NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)


def request() -> ExecuteRequest:
    value = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": 7,
            "action_role": "prospect_reply",
            "operation": "email.send",
            "intent_kind": "showing_offer",
            "appointment_slot": "2026-07-17T10:30:00-04:00",
            "arguments": {"text": "Friday at 10:30 works. — Nigel"},
        }
    )
    assert isinstance(value, ExecuteRequest)
    return value


def context() -> ActionContext:
    return ActionContext(
        action_id=ACTION_ID,
        wakeup_event_id=7,
        action_role=ActionRole.PROSPECT_REPLY,
        operation=Operation.EMAIL_SEND,
        intent_kind=IntentKind.SHOWING_OFFER,
        appointment_slot=datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
        arguments=MappingProxyType({"text": "Friday at 10:30 works. — Nigel"}),
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
        source_subject="Zillow inquiry",
        prospect_name="Amanda Snyder",
    )


def evidence(**overrides):
    values = dict(
        current_recipient_id="lead@convo.zillow.com",
        current_property_id="building:bullman-st",
        current_appointment_slot=datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
        later_inbound_message_id=None,
        verified_outbound_message_id=None,
        verified_outbound_request_ref=None,
        verified_outbound_covers_source=False,
        calendar_dependency=CalendarDependencyState.NOT_REQUIRED,
        calendar_already_applied=False,
        calendar_context_changed=False,
        overlapping_showing_prospect_ids=("prospect:other-1", "prospect:other-2"),
        refresh_required_through=NOW,
        refresh=None,
    )
    values.update(overrides)
    return PreflightEvidence(**values)


def row(state=ActionState.RECEIVED, **overrides):
    values = dict(
        action_id=ACTION_ID,
        wakeup_event_id=7,
        action_role=ActionRole.PROSPECT_REPLY,
        operation=Operation.EMAIL_SEND,
        intent_kind=IntentKind.SHOWING_OFFER,
        appointment_slot=datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
        arguments={"text": "Friday at 10:30 works. — Nigel"},
        state=state,
        action_uid=ACTION_UID if state is not ActionState.RECEIVED else None,
        provider_request_ref=None,
        provider_message_id=None,
        completion_kind=None,
        detail_code=state.value,
        attempt_count=0,
        next_attempt_at=NOW,
        payload_hash="",
        canonical_context={},
        canonical_scope={},
        recipient_scope={},
        provider_account="",
        routing_policy_version="",
    )
    values.update(overrides)
    return OutboundActionRecord(**values)


class FakeStore:
    def __init__(self, initial=None):
        self.current = initial
        self.calls = []

    async def create_or_load(self, ctx):
        self.calls.append(("create", ctx.action_id))
        if self.current is None:
            self.current = row()
        return self.current

    async def prepare(self, ctx, expected_state):
        self.calls.append(("prepare", expected_state))
        self.current = replace(self.current, state=ActionState.PREPARED, action_uid=ACTION_UID)
        return self.current

    async def claim(self, action_id, expected_state, lease_owner, lease_seconds):
        self.calls.append(("claim", expected_state, lease_owner))
        return self.current

    async def record_provider_request(self, action_id, lease_owner, observation):
        self.calls.append(("record_request", observation.provider_request_ref))
        self.current = replace(self.current, provider_request_ref=observation.provider_request_ref)
        return self.current

    async def transition(self, action_id, expected_state, next_state, lease_owner, observation):
        self.calls.append(("transition", expected_state, next_state, observation.detail_code, lease_owner))
        self.current = replace(
            self.current,
            state=next_state,
            provider_request_ref=observation.provider_request_ref or self.current.provider_request_ref,
            provider_message_id=observation.message_id or self.current.provider_message_id,
            detail_code=observation.detail_code,
        )
        return self.current

    async def complete(self, action_id, expected_state, lease_owner, receipt, completion_kind, detail_code):
        self.calls.append(("complete", expected_state, receipt.provider_request_ref))
        self.current = replace(
            self.current,
            state=ActionState.COMPLETED,
            provider_request_ref=receipt.provider_request_ref,
            provider_message_id=receipt.provider_message_id,
            completion_kind=completion_kind,
            detail_code=detail_code,
        )
        return self.current

    async def definitive_fail(self, action_id, expected_state, lease_owner, observation):
        self.calls.append(("definitive_fail", observation.detail_code))
        self.current = replace(self.current, state=ActionState.DEFINITIVE_FAILED, detail_code=observation.detail_code)
        return self.current

    async def schedule_next_attempt(self, action_id, expected_state, delay_seconds, detail_code):
        self.calls.append(("schedule", expected_state, delay_seconds, detail_code))
        self.current = replace(
            self.current,
            detail_code=detail_code,
            next_attempt_at=NOW + timedelta(seconds=delay_seconds),
        )
        return self.current

    async def get(self, action_id):
        return self.current if self.current and self.current.action_id == action_id else None


class FakeAdapter:
    def __init__(self, *observations):
        self.observations = list(observations)
        self.calls = []

    def build_request(self, ctx, action_uid):
        self.calls.append(("build", ctx.target.target_id, action_uid))
        return object()

    async def invoke(self, client, provider_request):
        self.calls.append(("invoke",))
        return self.observations.pop(0)

    async def poll(self, client, observation):
        self.calls.append(("poll", observation.provider_request_ref))
        return self.observations.pop(0)

    def parse_receipt(self, ctx, observation):
        if observation.disposition is not ProviderDisposition.ACCEPTED:
            return None
        return ProviderReceipt(
            provider_request_ref=observation.provider_request_ref,
            provider_message_id=observation.message_id,
            accepted_at=observation.accepted_at,
            evidence=observation.evidence,
        )

    async def reconcile(self, client, ctx, action_uid, observation):
        self.calls.append(("reconcile",))
        return self.observations.pop(0)


def service(store, adapter, *, proof=None, circuit_guard=None):
    loader = AsyncMock()
    loader.load.return_value = context()
    preflight = AsyncMock()
    preflight.load.return_value = proof or evidence()
    return OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
        circuit_guard=circuit_guard,
    )


@pytest.mark.asyncio
async def test_execute_persists_dispatch_before_io_and_completes_receipt_atomically():
    accepted_at = NOW
    pending = ProviderObservation(
        ProviderDisposition.PENDING,
        "provider_pending",
        provider_request_ref="req-1",
        provider_call_id="req-1",
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "provider_accepted",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=accepted_at,
        evidence={"kind": "provider_message_id"},
    )
    store = FakeStore()
    adapter = FakeAdapter(pending, accepted)

    result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.SENT
    assert [call[0] for call in store.calls] == [
        "create",
        "prepare",
        "claim",
        "transition",
        "record_request",
        "transition",
        "complete",
    ]
    assert store.calls[3][1:3] == (ActionState.PREPARED, ActionState.DISPATCHING)
    assert adapter.calls[:2] == [("build", "lead@convo.zillow.com", ACTION_UID), ("invoke",)]
    assert result.provider_request_ref == "req-1"


@pytest.mark.asyncio
async def test_lock_contention_waits_without_dispatching_provider():
    store = FakeStore(row())

    async def contended_prepare(ctx, expected_state):
        store.calls.append(("prepare", expected_state))
        store.current = replace(
            store.current,
            state=ActionState.DEPENDENCY_WAIT,
            detail_code="intent_lock_contended",
            next_attempt_at=NOW + timedelta(seconds=5),
        )
        return store.current

    store.prepare = contended_prepare
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.ACCEPTED,
            "accepted",
            provider_request_ref="must-not-send",
            message_id="must-not-send",
            accepted_at=NOW,
        )
    )

    result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.PENDING
    assert result.detail_code == "intent_lock_contended"
    assert not adapter.calls
    assert not any(call[0] == "claim" for call in store.calls)


@pytest.mark.asyncio
async def test_repeated_completed_execute_is_duplicate_without_provider_call():
    store = FakeStore(
        row(
            ActionState.COMPLETED,
            action_uid=ACTION_UID,
            provider_request_ref="req-existing",
            provider_message_id="mail-existing",
            completion_kind=CompletionKind.SENT,
        )
    )
    adapter = FakeAdapter()

    result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.DUPLICATE
    assert adapter.calls == []
    assert [call[0] for call in store.calls] == ["create"]


@pytest.mark.asyncio
async def test_same_property_overlap_does_not_block_ready_preflight():
    store = FakeStore()
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "provider_accepted",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "provider_message_id"},
    )
    result = await service(store, FakeAdapter(accepted)).execute(request())
    assert result.status is PublicStatus.SENT
    assert any(call[0] == "prepare" for call in store.calls)


@pytest.mark.asyncio
async def test_ambiguous_timeout_retains_lock_and_never_retries_inline():
    store = FakeStore()
    adapter = FakeAdapter(ProviderObservation(ProviderDisposition.AMBIGUOUS, "provider_timeout"))

    result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.UNKNOWN
    assert adapter.calls.count(("invoke",)) == 1
    assert store.current.state is ActionState.UNKNOWN
    assert not any(call[0] == "definitive_fail" for call in store.calls)
    assert any(call[0] == "schedule" and call[3] == "provider_timeout" for call in store.calls)


@pytest.mark.asyncio
async def test_open_provider_circuit_defers_without_provider_call():
    circuit = AsyncMock()
    circuit.circuit_status.return_value = CircuitStatus(
        is_open=True,
        retry_after_seconds=120,
        failure_count=5,
    )
    store = FakeStore()
    adapter = FakeAdapter()

    result = await service(store, adapter, circuit_guard=circuit).execute(request())

    assert result.status is PublicStatus.PENDING
    assert result.detail_code == "provider_circuit_open"
    assert adapter.calls == []
    assert ("schedule", ActionState.PREPARED, 120, "provider_circuit_open") in store.calls


@pytest.mark.asyncio
async def test_repeated_execute_cannot_bypass_scheduled_retry_due_time():
    store = FakeStore(
        row(
            ActionState.RETRY_READY,
            action_uid=ACTION_UID,
            attempt_count=1,
            next_attempt_at=NOW + timedelta(minutes=2),
        )
    )
    adapter = FakeAdapter()

    result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.PENDING
    assert adapter.calls == []
    assert [call[0] for call in store.calls] == ["create"]


@pytest.mark.asyncio
async def test_unknown_reconciliation_completes_directly_from_positive_evidence():
    store = FakeStore(
        row(
            ActionState.UNKNOWN,
            action_uid=ACTION_UID,
            provider_request_ref="req-1",
        )
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "email_reconciled_by_message_id",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "exact_message_id"},
    )
    adapter = FakeAdapter(accepted)

    result = await service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert ("complete", ActionState.RECONCILING, "req-1") in store.calls
    assert not any(call[0] == "transition" and call[2] is ActionState.COMPLETED for call in store.calls)


@pytest.mark.asyncio
async def test_expired_dispatching_lease_becomes_unknown_then_reconciles_without_send():
    store = FakeStore(
        row(
            ActionState.DISPATCHING,
            action_uid=ACTION_UID,
            provider_request_ref="req-1",
        )
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "email_reconciled_by_message_id",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "exact_message_id"},
    )
    adapter = FakeAdapter(accepted)

    result = await service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert ("invoke",) not in adapter.calls
    assert ("reconcile",) in adapter.calls
    assert any(call[0] == "transition" and call[1] is ActionState.DISPATCHING and call[2] is ActionState.UNKNOWN for call in store.calls)


@pytest.mark.asyncio
async def test_explicit_non_acceptance_is_only_path_to_retry_ready():
    store = FakeStore()
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "provider_transient_upstream_error",
            provider_request_ref="req-1",
            category="transient_upstream_error",
            retryable=True,
            evidence={"status": "failed", "category": "transient_upstream_error"},
        )
    )

    result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.RETRY_READY
    assert any(call[0] == "schedule" for call in store.calls)


@pytest.mark.asyncio
async def test_retry_budget_exhaustion_dead_letters_unknown_without_redispatch():
    store = FakeStore(
        row(
            ActionState.UNKNOWN,
            action_uid=ACTION_UID,
            provider_request_ref="req-1",
            attempt_count=5,
        )
    )
    adapter = FakeAdapter()

    result = await service(store, adapter).exhaust(ACTION_ID)

    assert result.status is PublicStatus.MANUAL_REVIEW
    assert store.current.state is ActionState.MANUAL_REVIEW
    assert adapter.calls == []
    assert any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)
    assert any(call[0] == "transition" and call[1] is ActionState.DEAD_LETTER and call[2] is ActionState.MANUAL_REVIEW for call in store.calls)


@pytest.mark.asyncio
async def test_newer_inbound_marks_action_stale_before_lock_or_provider_io():
    store = FakeStore()
    adapter = FakeAdapter()
    proof = evidence(later_inbound_message_id=701)

    result = await service(store, adapter, proof=proof).execute(request())

    assert result.status is PublicStatus.STALE
    assert adapter.calls == []
    assert [call[0] for call in store.calls] == ["create", "transition"]


@pytest.mark.asyncio
async def test_gateway_does_not_duplicate_skill_owned_zillow_refresh_policy():
    old_context = replace(context(), source_sent_at=datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc))
    loader = AsyncMock()
    loader.load.return_value = old_context
    proof_loader = AsyncMock()
    proof_loader.load.return_value = evidence(refresh_required_through=NOW, refresh=None)
    store = FakeStore()
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.ACCEPTED,
            "provider_accepted",
            provider_request_ref="req-1",
            message_id="mail-1",
            accepted_at=NOW,
            evidence={"kind": "provider_message_id"},
        )
    )
    gateway = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=proof_loader,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
    )

    result = await gateway.execute(request())

    assert result.status is PublicStatus.SENT
    assert "staff" not in result.detail_code
    assert ("invoke",) in adapter.calls


@pytest.mark.asyncio
async def test_due_dependency_retry_is_claimed_so_retry_budget_advances():
    store = FakeStore(
        row(
            ActionState.DEPENDENCY_WAIT,
            action_uid=ACTION_UID,
            detail_code="zillow_refresh_required",
            attempt_count=2,
        )
    )
    adapter = FakeAdapter()
    proof = evidence(refresh_required_through=NOW, refresh=None)
    old_context = replace(
        context(),
        source_sent_at=datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc),
        intent_kind=IntentKind.SHOWING_CONFIRMATION,
    )
    loader = AsyncMock()
    loader.load.return_value = old_context
    proof_loader = AsyncMock()
    proof_loader.load.return_value = replace(
        proof,
        calendar_dependency=CalendarDependencyState.PENDING,
    )
    gateway = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=proof_loader,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
    )

    result = await gateway.resume(ACTION_ID)

    assert result.status is PublicStatus.PENDING
    assert store.calls[0][:2] == ("claim", ActionState.DEPENDENCY_WAIT)
    assert any(call[0] == "schedule" for call in store.calls)
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_due_dependency_terminal_preflight_uses_held_lease():
    store = FakeStore(
        row(
            ActionState.DEPENDENCY_WAIT,
            action_uid=ACTION_UID,
            detail_code="zillow_refresh_required",
            attempt_count=2,
        )
    )
    adapter = FakeAdapter()
    proof = evidence(later_inbound_message_id=701)

    result = await service(store, adapter, proof=proof).resume(ACTION_ID)

    assert result.status is PublicStatus.STALE
    assert store.calls[-1] == (
        "transition",
        ActionState.DEPENDENCY_WAIT,
        ActionState.STALE,
        "newer_inbound",
        "gateway-test",
    )


@pytest.mark.asyncio
async def test_worker_resume_rejects_mutated_persisted_context_before_provider_io():
    store = FakeStore(
        row(
            ActionState.PREPARED,
            action_uid=ACTION_UID,
            payload_hash="f" * 64,
            provider_account="nigel-zoho",
        )
    )
    adapter = FakeAdapter()

    result = await service(store, adapter).resume(ACTION_ID)

    assert result.status is PublicStatus.MANUAL_REVIEW
    assert store.current.state is ActionState.MANUAL_REVIEW
    assert adapter.calls == []
    assert any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "initial_state"),
    [("resume", ActionState.PREPARED), ("reconcile", ActionState.UNKNOWN)],
)
async def test_worker_terminalizes_context_that_can_no_longer_be_derived(method, initial_state):
    store = FakeStore(row(initial_state, action_uid=ACTION_UID, payload_hash="a" * 64))
    adapter = FakeAdapter()
    loader = AsyncMock()
    loader.load.side_effect = ContextDerivationError("wakeup event does not exist")
    proof_loader = AsyncMock()
    gateway = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=proof_loader,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
    )

    result = await getattr(gateway, method)(ACTION_ID)

    assert result.status is PublicStatus.MANUAL_REVIEW
    assert store.current.state is ActionState.MANUAL_REVIEW
    assert adapter.calls == []
    assert any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)
