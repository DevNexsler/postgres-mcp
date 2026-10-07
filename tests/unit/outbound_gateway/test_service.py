# pyright: reportArgumentType=false, reportOptionalMemberAccess=false
# The service's pre-192 contract. OUTBOUND_STALE_CONFIRM_ENABLED defaults off,
# and every test here runs with it off, unchanged from before the stale-context
# confirmation work: that is the proof that "off" behaves exactly as today.
# The enabled behaviour is tests/unit/outbound_gateway/test_stale_context_confirm.py.

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import MappingProxyType
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid4

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.adapters.base import ProviderReceipt
from postgres_mcp.outbound_gateway.adapters.email import EmailAdapter
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.context import canonical_payload_hash

# The world evidence both sides of the parity replay read: the current
# service reads only calendar_dependency; the frozen bf41be6 judgment also
# read newer inbound and the (tautological) recipient/context copies.
from postgres_mcp.outbound_gateway.legacy_judgment.preflight import PreflightEvidence
from postgres_mcp.outbound_gateway.metrics import CircuitStatus
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import CompletionKind
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import NewerActivity
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.preflight import CalendarDependencyState
from postgres_mcp.outbound_gateway.provider_client import McpProviderClient
from postgres_mcp.outbound_gateway.provider_client import McpServerConfig
from postgres_mcp.outbound_gateway.service import OutboundActionRecord
from postgres_mcp.outbound_gateway.service import OutboundActionService
from postgres_mcp.outbound_gateway.tenantcloud_shared import TENANTCLOUD_OPERATIONS
from postgres_mcp.outbound_gateway.tenantcloud_shared import tenantcloud_persisted_arguments

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")
ACTION_UID = UUID("9ebddbf7-8fc8-5a4f-bba7-869ea7053521")
NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)


def request(*, override: bool = False) -> ExecuteRequest:
    value = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": 7,
            "action_role": "prospect_reply",
            "operation": "email.send",
            "intent_kind": "showing_offer",
            "appointment_slot": "2026-07-17T10:30:00-04:00",
            "arguments": {"to_address": "lead@convo.zillow.com", "text": "Friday at 10:30 works. — Nigel"},
            "override": override,
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
        arguments=MappingProxyType({"to_address": "lead@convo.zillow.com", "text": "Friday at 10:30 works. — Nigel"}),
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
        arguments={"to_address": "lead@convo.zillow.com", "text": "Friday at 10:30 works. — Nigel"},
        state=state,
        action_uid=ACTION_UID if state is not ActionState.RECEIVED else None,
        provider_request_ref=None,
        provider_message_id=None,
        provider_accepted_at=None,
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


class LeaseUnavailableError(RuntimeError):
    """Mirrors claim_outbound_action's 55P03 'outbound action lease
    unavailable' -- raised by the real SQL function (Comm-Data-Store
    migrations/068_outbound_gateway_observability.sql:156-167) when the
    expected_state isn't in its live whitelist."""


# The exact live whitelist claim_outbound_action enforces today
# (migrations/068_outbound_gateway_observability.sql:156-159) -- 'received'
# is deliberately excluded: a fresh row must go through
# prepare_outbound_action_and_acquire_lock first.
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


class InvalidDefinitiveFailStateError(RuntimeError):
    """Mirrors definitively_fail_outbound_action's 'invalid outbound
    definitive failure state' (Comm-Data-Store migrations/
    067_outbound_action_gateway.sql:898-902), raised when
    outbound_action_transition_allowed(expected_state, 'definitive_failed')
    is false."""


# outbound_action_transition_allowed()'s full table (067:346-389) has these
# and only these edges into 'definitive_failed' -- notably no
# ('dependency_wait', 'definitive_failed') or ('received', 'definitive_failed').
_DEFINITIVE_FAIL_ALLOWED_FROM = {
    ActionState.PREPARED,
    ActionState.DISPATCHING,
    ActionState.RETRY_READY,
    ActionState.MANUAL_REVIEW,
}


class FakeStore:
    def __init__(self, initial=None, *, remediation_successor_id=None, remediation_error=None):
        self.current = initial
        self.calls = []
        self.last_receipt = None
        self.remediation_successor_id = remediation_successor_id
        self.remediation_error = remediation_error

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
        if expected_state not in _CLAIMABLE_STATES:
            raise LeaseUnavailableError(
                f"outbound action lease unavailable: state {expected_state.value!r} is not "
                "claimable (migrations/068_outbound_gateway_observability.sql:156-159)"
            )
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
        self.last_receipt = receipt
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
        self.calls.append(("definitive_fail", observation.detail_code, observation))
        if expected_state not in _DEFINITIVE_FAIL_ALLOWED_FROM:
            raise InvalidDefinitiveFailStateError(
                f"invalid outbound definitive failure state: no transition from "
                f"{expected_state.value!r} to 'definitive_failed' "
                "(migrations/067_outbound_action_gateway.sql:346-389)"
            )
        self.current = replace(
            self.current,
            state=ActionState.DEFINITIVE_FAILED,
            detail_code=observation.detail_code,
            error_category=observation.category,
        )
        return self.current

    async def remediate_traffic_block(self, action_id, *, operator_identity, reason):
        self.calls.append(("remediate", action_id, operator_identity, reason))
        if self.remediation_error is not None:
            raise self.remediation_error
        parent = self.current
        successor_id = self.remediation_successor_id or uuid4()
        self.current = row(
            ActionState.RECEIVED,
            action_id=successor_id,
            wakeup_event_id=parent.wakeup_event_id,
            action_role=parent.action_role,
            operation=parent.operation,
            intent_kind=parent.intent_kind,
            appointment_slot=parent.appointment_slot,
            arguments=dict(parent.arguments),
            detail_code="operator_remediation_created",
            payload_hash=parent.payload_hash,
            canonical_context=dict(parent.canonical_context),
            canonical_scope=dict(parent.canonical_scope),
            recipient_scope=dict(parent.recipient_scope),
            provider_account=parent.provider_account,
            routing_policy_version=parent.routing_policy_version,
        )
        return self.current

    async def schedule_next_attempt(self, action_id, expected_state, delay_seconds, detail_code):
        self.calls.append(("schedule", expected_state, delay_seconds, detail_code))
        self.current = replace(
            self.current,
            detail_code=detail_code,
            next_attempt_at=NOW + timedelta(seconds=delay_seconds),
        )
        return self.current

    async def reject(self, action_id, detail_code, error_detail):
        """Comm-Data-Store reject_outbound_action: received/dependency_wait
        -> rejected with its reason; anything else comes back unchanged."""
        self.calls.append(("reject", detail_code, error_detail))
        if self.current.state in {ActionState.RECEIVED, ActionState.DEPENDENCY_WAIT}:
            self.current = replace(self.current, state=ActionState.REJECTED, detail_code=detail_code, error_detail=error_detail)
        return self.current

    async def get(self, action_id):
        return self.current if self.current and self.current.action_id == action_id else None


class FakeAdapter:
    def __init__(self, *observations, outcome_polls=()):
        self.observations = list(observations)
        # Answers to reconcile's "did job X finish?" pre-check. None queued
        # means the provider cannot say (the TenantCloud facade, or a lost
        # job), so the ordinary reconcile path runs.
        self.outcome_polls = list(outcome_polls)
        self.calls = []

    def build_request(self, ctx, action_uid):
        self.calls.append(("build", ctx.target.target_id, action_uid))
        return object()

    async def invoke(self, client, provider_request):
        self.calls.append(("invoke",))
        return self.observations.pop(0)

    async def poll(self, client, observation):
        self.calls.append(("poll", observation.provider_request_ref))
        if observation.detail_code == "prior_dispatch_ambiguous":
            return self.outcome_polls.pop(0) if self.outcome_polls else observation
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


def service(store, adapter, *, proof=None, circuit_guard=None, traffic_mode="off", traffic_probe=None, provider_client=None):
    loader = AsyncMock()
    loader.load.return_value = context()
    preflight = AsyncMock()
    preflight.load.return_value = proof or evidence()
    return OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=provider_client or object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
        circuit_guard=circuit_guard,
        traffic_mode=traffic_mode,
        traffic_probe=traffic_probe,
    )


class FakeProbe:
    def __init__(self, *, in_flight=None, newer=None, watermark=NOW, raise_on_in_flight=False):
        self.in_flight = in_flight or []
        self.newer = newer
        self.watermark = watermark
        self.raise_on_in_flight = raise_on_in_flight
        self.calls = []

    async def in_flight_actions(self, recipient_key, exclude_action_id):
        self.calls.append(("in_flight", recipient_key, exclude_action_id))
        if self.raise_on_in_flight:
            raise RuntimeError("probe boom")
        return self.in_flight

    async def activity_after(self, recipient_key, channel_id, watermark, exclude_action_id, limit, exclude_refs=frozenset()):
        self.calls.append(("newest_activity", recipient_key, channel_id, watermark, exclude_action_id))
        return [self.newer] if self.newer is not None else []

    async def acknowledged_refs(self, wakeup_event_id, recipient_key):
        raise AssertionError("with stale-context confirmation disabled the probe never reads shown context")

    async def messages_by_id(self, message_ids):
        raise AssertionError("with stale-context confirmation disabled the probe never reads shown context")

    async def context_watermark(self, wakeup_event_id):
        self.calls.append(("watermark", wakeup_event_id))
        return self.watermark

    async def newer_context(self, context, *, limit, waive_shown, as_of=None):
        self.calls.append(("newer_context", context.prospect_id, context.action_id))
        return [self.newer] if self.newer is not None else []


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
async def test_repeated_execute_accepts_durable_subject_alias_promotion_before_create():
    stored_prospect = "prospect:factbook:stable-id"
    current_prospect = "subject:durable-alias-id"
    stored_context = {"identity_version": "v1", "prospect_id": stored_prospect}
    stored_scope = {"version": "v1", "prospect_id": stored_prospect}
    current_context = replace(
        context(),
        prospect_id=current_prospect,
        canonical_context=MappingProxyType(
            {"identity_version": "v1", "prospect_id": current_prospect}
        ),
        canonical_scope=MappingProxyType(
            {"version": "v1", "prospect_id": current_prospect}
        ),
    )
    payload_hash = canonical_payload_hash(
        {
            "action_role": current_context.action_role.value,
            "operation": current_context.operation.value,
            "intent_kind": current_context.intent_kind.value,
            "appointment_slot": current_context.appointment_slot,
            "arguments": current_context.arguments,
            "canonical_context": stored_context,
        }
    )
    store = FakeStore(
        row(
            ActionState.COMPLETED,
            action_uid=ACTION_UID,
            provider_request_ref="req-existing",
            provider_message_id="mail-existing",
            completion_kind=CompletionKind.SENT,
            payload_hash=payload_hash,
            canonical_context=stored_context,
            canonical_scope=stored_scope,
            recipient_scope={
                "kind": "email_thread",
                "target_id": "lead@convo.zillow.com",
                "verified": True,
            },
            provider_account="nigel-zoho",
            routing_policy_version="v1",
        )
    )
    store.create_or_load = AsyncMock(
        side_effect=RuntimeError("outbound action payload mismatch")
    )
    adapter = FakeAdapter()
    gateway = service(store, adapter)
    gateway._context_loader.load.return_value = current_context

    result = await gateway.execute(request())

    assert result.status is PublicStatus.DUPLICATE
    assert adapter.calls == []
    store.create_or_load.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_accepted_crash_recovery_completes_from_persisted_receipt():
    store = FakeStore(
        row(
            ActionState.PROVIDER_ACCEPTED,
            action_uid=ACTION_UID,
            provider_request_ref="req-accepted",
            provider_message_id="mail-accepted",
            provider_accepted_at=NOW,
        )
    )
    adapter = FakeAdapter()

    result = await service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == []
    assert [call[0] for call in store.calls] == ["claim", "complete"]
    assert store.current.provider_message_id == "mail-accepted"


@pytest.mark.asyncio
async def test_provider_accepted_exhaustion_recovers_persisted_receipt():
    store = FakeStore(
        row(
            ActionState.PROVIDER_ACCEPTED,
            action_uid=ACTION_UID,
            provider_request_ref="req-accepted",
            provider_message_id="mail-accepted",
            provider_accepted_at=NOW,
            attempt_count=5,
        )
    )
    adapter = FakeAdapter()

    result = await service(store, adapter).exhaust(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == []
    assert [call[0] for call in store.calls] == ["claim", "complete"]
    assert store.current.provider_message_id == "mail-accepted"


def tenantcloud_context(**overrides):
    values = dict(
        action_id=ACTION_ID,
        wakeup_event_id=7,
        action_role=ActionRole.PROVIDER_MUTATION,
        operation=Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        intent_kind=IntentKind.TENANTCLOUD_LEAD_STATUS,
        appointment_slot=None,
        arguments=MappingProxyType({"lead_id": 6001, "status": "working"}),
        source="tenantcloud",
        source_message_id=700,
        source_message_key="tenantcloud_api:700",
        source_sent_at=NOW,
        conversation_id="conversation:tenantcloud-1",
        conversation_watermark=700,
        prospect_id="tenantcloud:claim:301",
        aliases=(),
        property_id=None,
        property_label=None,
        target=DerivedTarget("tenantcloud_lead", "6001", True),
        provider_account="tenantcloud",
        routing_policy_version="v1",
        canonical_scope=MappingProxyType({"version": "v1"}),
        canonical_context=MappingProxyType({"identity_version": "v1"}),
        payload_hash="a" * 64,
        lock_holder=f"outbound-gateway:{ACTION_ID}",
        thread_identity="tenantcloud:lead-thread:6001",
        showing_lifecycle_id="showing:wake:7",
        calendar_event_uid=None,
    )
    values.update(overrides)
    return ActionContext(**values)


def tenantcloud_row(state=ActionState.RECEIVED, **overrides):
    values = dict(
        action_id=ACTION_ID,
        wakeup_event_id=7,
        action_role=ActionRole.PROVIDER_MUTATION,
        operation=Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        intent_kind=IntentKind.TENANTCLOUD_LEAD_STATUS,
        appointment_slot=None,
        arguments={"lead_id": 6001, "status": "working"},
        state=state,
        action_uid=ACTION_UID if state is not ActionState.RECEIVED else None,
        provider_request_ref=None,
        provider_message_id=None,
        provider_accepted_at=None,
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


def tenantcloud_service(store, adapter):
    loader = AsyncMock()
    loader.load.return_value = tenantcloud_context()
    preflight = AsyncMock()
    preflight.load.return_value = evidence(
        current_recipient_id="6001",
        current_property_id=None,
        current_appointment_slot=None,
    )
    return OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_tenantcloud_enqueue_preflights_and_prepares_without_provider_io() -> None:
    store = FakeStore(tenantcloud_row())
    adapter = AsyncMock()
    gateway = tenantcloud_service(store, adapter)

    result = await gateway.enqueue(tenantcloud_row().execute_request())

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.PREPARED
    adapter.invoke.assert_not_called()
    adapter.reconcile.assert_not_called()


@pytest.mark.asyncio
async def test_tenantcloud_prepare_remediation_preflights_without_provider_io() -> None:
    store = FakeStore(tenantcloud_row())
    adapter = AsyncMock()
    gateway = tenantcloud_service(store, adapter)

    result = await gateway.prepare(ACTION_ID)

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.PREPARED
    adapter.invoke.assert_not_called()
    adapter.reconcile.assert_not_called()


@pytest.mark.asyncio
async def test_tenantcloud_prepare_accepts_verified_remediation_successor_identity() -> None:
    successor_id = UUID("e8f10652-ae8c-528f-9d6a-05f56f7f18c0")
    context = tenantcloud_context()
    store = FakeStore(
        tenantcloud_row(
            action_id=successor_id,
            retry_of_action_id=ACTION_ID,
            detail_code="operator_remediation_created",
            payload_hash=context.payload_hash,
            canonical_context=dict(context.canonical_context),
            canonical_scope=dict(context.canonical_scope),
            recipient_scope={
                "kind": context.target.kind,
                "target_id": context.target.target_id,
                "verified": context.target.verified,
            },
            provider_account=context.provider_account,
            routing_policy_version=context.routing_policy_version,
        )
    )
    adapter = AsyncMock()
    gateway = tenantcloud_service(store, adapter)

    result = await gateway.prepare(successor_id)

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.PREPARED
    adapter.invoke.assert_not_called()
    adapter.reconcile.assert_not_called()


def tenantcloud_context_for(operation, **overrides):
    """Full ActionContext for each of the four TenantCloud operations --
    enough detail (canonical_context claim/source/provider_ids, canonical_scope
    desired_state_hash) for tenantcloud_persisted_arguments() to run for real,
    the same way create_or_load() does at enqueue time."""
    target = {
        Operation.TENANTCLOUD_MESSAGE_SEND: DerivedTarget("tenantcloud_thread", "555", True),
        Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: DerivedTarget("tenantcloud_lead", "6001", True),
        Operation.TENANTCLOUD_MAINTENANCE_CREATE: DerivedTarget("tenantcloud_property_unit", "property:12:unit:34", True),
        Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE: DerivedTarget("tenantcloud_maintenance_request", "81", True),
    }[operation]
    intent = {
        Operation.TENANTCLOUD_MESSAGE_SEND: IntentKind.INQUIRY_REPLY,
        Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: IntentKind.TENANTCLOUD_LEAD_STATUS,
        Operation.TENANTCLOUD_MAINTENANCE_CREATE: IntentKind.TENANTCLOUD_MAINTENANCE_CREATE,
        Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE: IntentKind.TENANTCLOUD_MAINTENANCE_STATUS,
    }[operation]
    role = ActionRole.PROSPECT_REPLY if operation is Operation.TENANTCLOUD_MESSAGE_SEND else ActionRole.PROVIDER_MUTATION
    arguments = {
        Operation.TENANTCLOUD_MESSAGE_SEND: {"thread_id": 555, "text": "Friday at 10:30 works. — Nigel"},
        Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: {"lead_id": 6001, "status": "working"},
        Operation.TENANTCLOUD_MAINTENANCE_CREATE: {
            "property_id": 12,
            "unit_id": 34,
            "category_id": 57,
            "title": "Kitchen leak",
            "priority": "normal",
            "initiated_at": "2026-08-04",
            "text": "Sink leaking under cabinet",
            "entry_allowed": False,
            "available_on": None,
        },
        Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE: {"request_id": 81, "status": 2},
    }[operation]
    canonical_context = {
        "identity_version": "v1",
        "tenantcloud_claim_id": 301,
        "source_event_id": "tenantcloud:claim:301",
    }
    if operation is Operation.TENANTCLOUD_MAINTENANCE_CREATE:
        canonical_context["provider_ids"] = {"property_id": "12", "unit_id": "34"}
    values = dict(
        action_id=ACTION_ID,
        wakeup_event_id=7,
        action_role=role,
        operation=operation,
        intent_kind=intent,
        appointment_slot=None,
        arguments=MappingProxyType(arguments),
        source="tenantcloud",
        source_message_id=700,
        source_message_key="tenantcloud_api:700",
        source_sent_at=NOW,
        conversation_id="conversation:tenantcloud-1",
        conversation_watermark=700,
        prospect_id="tenantcloud:claim:301",
        aliases=(),
        property_id=None,
        property_label=None,
        target=target,
        provider_account="tenantcloud",
        routing_policy_version="v1",
        canonical_scope=MappingProxyType({"version": "v1", "desired_state_hash": "d" * 64}),
        canonical_context=MappingProxyType(canonical_context),
        payload_hash="a" * 64,
        lock_holder=f"outbound-gateway:{ACTION_ID}",
        thread_identity="tenantcloud:thread-1",
        showing_lifecycle_id="showing:wake:7",
        calendar_event_uid=None,
    )
    values.update(overrides)
    return ActionContext(**values)


@pytest.mark.parametrize("operation", sorted(TENANTCLOUD_OPERATIONS, key=lambda op: op.value))
def test_execute_request_round_trips_arguments_enriched_by_create_or_load(operation):
    """Regression test for the round-2 finding: store.create_or_load()
    persists arguments enriched with desired_state/target_reference/
    idempotency_key (migration 118 reads those off outbound_actions.arguments
    directly). OutboundActionRecord.execute_request() rebuilds an
    ExecuteRequest from action.arguments to reload context on every
    reconcile()/resume() call -- and every ArgumentModel is a StrictModel
    with extra="forbid", so those three gateway-owned keys must not reach
    model_validate(). This builds arguments via the *real* enrichment path
    (tenantcloud_persisted_arguments, exactly what create_or_load calls),
    not a hand-authored dict, so it can't miss what create_or_load actually
    writes.
    """
    context = tenantcloud_context_for(operation)
    enriched_arguments = tenantcloud_persisted_arguments(context)
    assert {"desired_state", "target_reference", "idempotency_key"} <= set(enriched_arguments)

    row = tenantcloud_row(
        operation=operation,
        action_role=context.action_role,
        intent_kind=context.intent_kind,
        appointment_slot=context.appointment_slot,
        arguments=dict(enriched_arguments),
    )

    rebuilt = row.execute_request()

    assert rebuilt.operation is operation
    # The rebuilt, strict-model arguments must match the *original*
    # unenriched arguments exactly -- payload_hash was computed from these
    # at enqueue time, before enrichment, and context re-derivation depends
    # on getting the same arguments back.
    assert rebuilt.arguments.model_dump(mode="json", exclude_none=False) == dict(context.arguments)


# Exactly migration 118's six required keys
# (118_...sql:353-364 / tenantcloud_shared.READBACK_OBSERVATION_KEYS).
VERIFIED_READBACK_EVIDENCE = {
    "canonical_observed_state": {"status": "working"},
    "operation": "tenantcloud.lead.status.update",
    "provider_object_id": "6001",
    "target_reference": "lead:6001",
    "readback_timestamp": "2026-07-16T01:00:00Z",
    "readback_verified": True,
}


@pytest.mark.asyncio
async def test_tenantcloud_reconciliation_records_acceptance_before_completion():
    store = FakeStore(
        tenantcloud_row(
            ActionState.UNKNOWN,
            action_uid=ACTION_UID,
            provider_request_ref="lead:6001",
        )
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "tenantcloud_lead_status_reconciled",
        provider_request_ref="lead:6001",
        message_id="tenantcloud-lead:6001:working",
        accepted_at=NOW,
        evidence={**VERIFIED_READBACK_EVIDENCE, "evidence_hash": "e" * 64},
    )

    result = await tenantcloud_service(store, FakeAdapter(accepted)).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert (
        "transition",
        ActionState.RECONCILING,
        ActionState.PROVIDER_ACCEPTED,
        "tenantcloud_lead_status_reconciled",
        "gateway-test",
    ) in store.calls
    assert ("complete", ActionState.PROVIDER_ACCEPTED, "lead:6001") in store.calls


@pytest.mark.asyncio
async def test_tenantcloud_persisted_acceptance_with_verified_readback_recovers_without_provider_io():
    store = FakeStore(
        tenantcloud_row(
            ActionState.PROVIDER_ACCEPTED,
            action_uid=ACTION_UID,
            provider_request_ref="lead:6001",
            provider_message_id="tenantcloud-lead:6001:working",
            provider_accepted_at=NOW,
            provider_evidence_kind="verified_provider_readback",
            provider_evidence_hash="e" * 64,
            provider_readback_evidence=VERIFIED_READBACK_EVIDENCE,
        )
    )
    adapter = FakeAdapter()

    result = await tenantcloud_service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == []
    assert [call[0] for call in store.calls] == ["claim", "complete"]


@pytest.mark.asyncio
async def test_tenantcloud_persisted_acceptance_without_verified_readback_reconciles_never_dispatches_second_write():
    store = FakeStore(
        tenantcloud_row(
            ActionState.PROVIDER_ACCEPTED,
            action_uid=ACTION_UID,
            provider_request_ref="lead:6001",
            provider_message_id="tenantcloud-lead:6001:working",
            provider_accepted_at=NOW,
            # No provider_evidence_kind/hash persisted -- e.g. the worker
            # crashed after the durable PROVIDER_ACCEPTED transition but
            # before a matching outbound_action_attempts row could be
            # written or read back.
        )
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "tenantcloud_lead_status_reconciled",
        provider_request_ref="lead:6001",
        message_id="tenantcloud-lead:6001:working",
        accepted_at=NOW,
        evidence={**VERIFIED_READBACK_EVIDENCE, "evidence_hash": "e" * 64},
    )
    adapter = FakeAdapter(accepted)

    result = await tenantcloud_service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == [("reconcile",)]
    assert not any(call[0] == "invoke" for call in adapter.calls)


@pytest.mark.asyncio
async def test_tenantcloud_persisted_acceptance_with_malformed_evidence_hash_reconciles():
    store = FakeStore(
        tenantcloud_row(
            ActionState.PROVIDER_ACCEPTED,
            action_uid=ACTION_UID,
            provider_request_ref="lead:6001",
            provider_message_id="tenantcloud-lead:6001:working",
            provider_accepted_at=NOW,
            provider_evidence_kind="verified_provider_readback",
            provider_evidence_hash="not-64-hex-chars",
            provider_readback_evidence=VERIFIED_READBACK_EVIDENCE,
        )
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "tenantcloud_lead_status_reconciled",
        provider_request_ref="lead:6001",
        message_id="tenantcloud-lead:6001:working",
        accepted_at=NOW,
        evidence={**VERIFIED_READBACK_EVIDENCE, "evidence_hash": "e" * 64},
    )
    adapter = FakeAdapter(accepted)

    result = await tenantcloud_service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == [("reconcile",)]


@pytest.mark.asyncio
async def test_tenantcloud_persisted_acceptance_with_incomplete_observation_reconciles():
    incomplete_evidence = {key: value for key, value in VERIFIED_READBACK_EVIDENCE.items() if key != "target_reference"}
    store = FakeStore(
        tenantcloud_row(
            ActionState.PROVIDER_ACCEPTED,
            action_uid=ACTION_UID,
            provider_request_ref="lead:6001",
            provider_message_id="tenantcloud-lead:6001:working",
            provider_accepted_at=NOW,
            provider_evidence_kind="verified_provider_readback",
            provider_evidence_hash="e" * 64,
            provider_readback_evidence=incomplete_evidence,
        )
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "tenantcloud_lead_status_reconciled",
        provider_request_ref="lead:6001",
        message_id="tenantcloud-lead:6001:working",
        accepted_at=NOW,
        evidence={**VERIFIED_READBACK_EVIDENCE, "evidence_hash": "e" * 64},
    )
    adapter = FakeAdapter(accepted)

    result = await tenantcloud_service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == [("reconcile",)]


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
async def test_dispatch_whose_session_hangs_before_tool_request_is_retry_ready():
    # #3240: mcp-gate held the gateway's `initialize` past the 10 s deadline,
    # so `tools/call` for email_send was never written. The action must be
    # retried, not parked in reconciliation and then manual review.
    tool_calls = []

    @asynccontextmanager
    async def transport(_url, **_kwargs):
        yield object(), object(), lambda: None

    class InitializeHangs:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def initialize(self):
            await asyncio.Event().wait()

        async def call_tool(self, *args, **_kwargs):
            tool_calls.append(args)

    config = McpServerConfig(
        name="agent-email",
        url="http://127.0.0.1:9090/mcp",
        transport="streamable_http",
        allowed_tools=frozenset({"email_send", "request_status", "email_get_thread"}),
        timeout_seconds=0.05,
    )
    store = FakeStore()
    gateway = service(
        store,
        EmailAdapter(sender_domains={"nigel-zoho": "pfg.example"}),
        provider_client=McpProviderClient({config.name: config}),
    )

    with (
        patch("postgres_mcp.outbound_gateway.provider_client.streamablehttp_client", transport),
        patch("postgres_mcp.outbound_gateway.provider_client.ClientSession", InitializeHangs),
    ):
        result = await gateway.execute(request())

    assert tool_calls == []
    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.RETRY_READY
    assert ("transition", ActionState.DISPATCHING, ActionState.RETRY_READY, "provider_unavailable_before_tool_request", "gateway-test") in store.calls
    assert not any(call[0] == "transition" and call[2] is ActionState.UNKNOWN for call in store.calls)


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
async def test_tenantcloud_auth_wait_reschedules_without_spending_the_retry_budget():
    """FIX 3: a retryable TenantCloud rejection proven pre-dispatch
    (tenantcloud_auth_rejected_before_dispatch / category=provider_authentication)
    waits out the outage instead of burning the ordinary 5-attempt budget --
    8 of 9 retry_budget_exhausted TenantCloud sends in 60 days were 6x this
    exact rejection inside ~90-150s. Same shape as
    test_open_provider_circuit_defers_without_provider_call: reschedule with
    no claim() and no provider call."""
    store = FakeStore(
        tenantcloud_row(
            ActionState.RETRY_READY,
            detail_code="tenantcloud_auth_rejected_before_dispatch",
            error_category="provider_authentication",
            attempt_count=1,
            next_attempt_at=NOW,
            created_at=NOW,
        )
    )
    adapter = FakeAdapter()

    result = await tenantcloud_service(store, adapter).resume(ACTION_ID)

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.RETRY_READY
    assert not any(call[0] == "claim" for call in store.calls)
    assert adapter.calls == []
    assert ("schedule", ActionState.RETRY_READY, 60, "tenantcloud_auth_wait") in store.calls


@pytest.mark.asyncio
async def test_tenantcloud_auth_wait_grows_then_falls_through_to_the_ordinary_path_at_the_ceiling():
    """The backoff grows from 60s towards 5 minutes as the outage persists,
    and once the 2h ceiling passes the row rejoins the ordinary
    claim()/dispatch path -- today's ordinary ladder to exhaustion."""
    waited = tenantcloud_row(
        ActionState.RETRY_READY,
        detail_code="tenantcloud_auth_rejected_before_dispatch",
        error_category="provider_authentication",
        attempt_count=1,
        next_attempt_at=NOW,
        created_at=NOW - timedelta(minutes=25),
    )
    store = FakeStore(waited)
    result = await tenantcloud_service(store, FakeAdapter()).resume(ACTION_ID)
    assert result.status is PublicStatus.PENDING
    assert not any(call[0] == "claim" for call in store.calls)
    assert ("schedule", ActionState.RETRY_READY, 180, "tenantcloud_auth_wait") in store.calls

    past_ceiling = tenantcloud_row(
        ActionState.RETRY_READY,
        detail_code="tenantcloud_auth_rejected_before_dispatch",
        error_category="provider_authentication",
        attempt_count=1,
        next_attempt_at=NOW,
        created_at=NOW - timedelta(hours=2, seconds=1),
    )
    store = FakeStore(past_ceiling)
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "tenantcloud_auth_rejected_before_dispatch",
            category="provider_authentication",
            retryable=True,
        )
    )

    result = await tenantcloud_service(store, adapter).resume(ACTION_ID)

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.RETRY_READY
    assert any(call[0] == "claim" for call in store.calls)
    assert ("invoke",) in adapter.calls
    assert store.current.detail_code == "tenantcloud_auth_rejected_before_dispatch"
    assert not any(call[0] == "schedule" and call[3] == "tenantcloud_auth_wait" for call in store.calls)


@pytest.mark.asyncio
async def test_tenantcloud_auth_wait_never_applies_to_a_row_about_to_dispatch_for_real():
    """The wait only ever looks at a row already parked retry_ready by a
    prior pre-dispatch rejection: a PREPARED row -- about to dispatch for the
    first time -- always goes through the ordinary claim()/invoke(), even
    carrying the same detail_code/error_category (a retried remediation
    successor's stale fields, say). An action that already dispatched -- or
    is about to -- never gets this treatment."""
    store = FakeStore(
        tenantcloud_row(
            ActionState.PREPARED,
            detail_code="tenantcloud_auth_rejected_before_dispatch",
            error_category="provider_authentication",
            attempt_count=0,
            next_attempt_at=NOW,
            created_at=NOW,
        )
    )
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.ACCEPTED,
            "tenantcloud_lead_status_accepted",
            provider_request_ref="tenantcloud-lead:6001:working",
            message_id="tenantcloud-lead:6001:working",
            accepted_at=NOW,
            evidence={"kind": "provider_message_id"},
        )
    )

    result = await tenantcloud_service(store, adapter).resume(ACTION_ID)

    assert any(call[0] == "claim" for call in store.calls)
    assert ("invoke",) in adapter.calls
    assert result.status is PublicStatus.SENT


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
async def test_retryable_pre_dispatch_failure_retries_then_completes_with_one_provider_effect():
    """FIX 2, service-level half of the proof (the adapter-level half is
    tests/unit/outbound_gateway/test_adapters.py::
    test_tenantcloud_reconciliation_auth_unavailable_on_a_create_is_retryable_not_ambiguous,
    which shows the reconcile-time auth failure performs zero writes).
    test_explicit_non_acceptance_is_only_path_to_retry_ready above already
    proves _finish_observation promotes a retryable DEFINITIVE_NON_ACCEPTANCE
    straight to RETRY_READY for a single attempt; this extends that to a
    full two-attempt lifecycle matching the live incident's shape: the
    first, provably pre-dispatch failure retries (no manual_review, no
    reconcile detour), and once the retry is due, the very next attempt
    dispatches for real and completes -- exactly two invoke() calls, the
    first of which is the retried rejection with no provider effect."""
    store = FakeStore()
    clock_box = [NOW]
    loader = AsyncMock()
    loader.load.return_value = context()
    preflight = AsyncMock()
    preflight.load.return_value = evidence()
    retryable_rejection = ProviderObservation(
        ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
        "tenantcloud_auth_rejected_before_dispatch",
        category="provider_authentication",
        retryable=True,
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "provider_accepted",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "provider_message_id"},
    )
    adapter = FakeAdapter(retryable_rejection, accepted)
    svc = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=object(),
        clock=lambda: clock_box[0],
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
    )

    first = await svc.execute(request())

    assert first.status is PublicStatus.PENDING
    assert store.current.state is ActionState.RETRY_READY
    assert not any(call[0] == "transition" and call[2] in {ActionState.UNKNOWN, ActionState.RECONCILING} for call in store.calls)
    assert adapter.calls.count(("invoke",)) == 1

    clock_box[0] = NOW + timedelta(hours=1)
    second = await svc.execute(request())

    assert second.status is PublicStatus.SENT
    assert store.current.state is ActionState.COMPLETED
    assert adapter.calls.count(("invoke",)) == 2


@pytest.mark.asyncio
async def test_tenantcloud_message_send_404_says_the_thread_does_not_exist_and_to_use_email():
    """A rejected tenantcloud.message.send used to surface only
    tenantcloud_provider_rejected_http_404 -- true, but useless: it does not
    say a thread id is not a lead id, or that email.send is the way to reach
    this lead anyway (the shape behind wake 26156, 2026-08-30, whose
    retry_budget_exhausted gave no hint the id it kept retrying was wrong)."""
    store = FakeStore()
    loader = AsyncMock()
    loader.load.return_value = tenantcloud_context_for(Operation.TENANTCLOUD_MESSAGE_SEND)
    preflight = AsyncMock()
    preflight.load.return_value = evidence()
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "tenantcloud_provider_rejected_http_404",
            category="provider_rejected",
            retryable=False,
        )
    )
    svc = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.TENANTCLOUD_MESSAGE_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
    )

    result = await svc.execute(request())

    assert result.status is PublicStatus.FAILED
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert result.detail.startswith(
        "TenantCloud has no messenger thread 555 (a lead id is not a thread id). If this lead has no "
        "thread, reply with email.send to the lead's email instead. "
    )
    assert result.override is not None and result.override.action_id == result.action_id


@pytest.mark.asyncio
async def test_tenantcloud_message_send_target_unavailable_says_the_thread_does_not_exist_while_it_retries():
    """target_unavailable_before_dispatch (resolve_lead_thread also came up
    empty) is retryable, so the row keeps retrying -- but the same
    explanation belongs on this interim retry_ready result too, since an
    agent checking status mid-retry sees only this result, not the eventual
    retry_budget_exhausted."""
    store = FakeStore()
    loader = AsyncMock()
    loader.load.return_value = tenantcloud_context_for(Operation.TENANTCLOUD_MESSAGE_SEND)
    preflight = AsyncMock()
    preflight.load.return_value = evidence()
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "tenantcloud_target_unavailable_before_dispatch",
            category="provider_target_resolution",
            retryable=True,
        )
    )
    svc = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.TENANTCLOUD_MESSAGE_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
    )

    result = await svc.execute(request())

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.RETRY_READY
    assert result.detail == (
        "TenantCloud has no messenger thread 555 (a lead id is not a thread id). If this lead has no "
        "thread, reply with email.send to the lead's email instead."
    )


@pytest.mark.asyncio
async def test_an_unrelated_tenantcloud_rejection_keeps_the_ordinary_detail():
    """The new text is scoped to the two thread-lookup detail codes -- an
    ordinary rejection (a bad body, say) must not be misdiagnosed as a
    missing thread."""
    store = FakeStore()
    loader = AsyncMock()
    loader.load.return_value = tenantcloud_context_for(Operation.TENANTCLOUD_MESSAGE_SEND)
    preflight = AsyncMock()
    preflight.load.return_value = evidence()
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "tenantcloud_provider_rejected",
            category="provider_rejected",
            retryable=False,
        )
    )
    svc = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.TENANTCLOUD_MESSAGE_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
    )

    result = await svc.execute(request())

    assert result.status is PublicStatus.FAILED
    assert result.detail.startswith("Not sent: tenantcloud_provider_rejected. ")
    assert "messenger thread" not in result.detail


@pytest.mark.asyncio
async def test_retry_budget_exhaustion_dead_letters_unknown_without_redispatch():
    """exhaust()'s final look (test_exhaust_final_check_* below) still comes
    back inconclusive here, so today's dead_letter/manual_review ladder is
    unchanged -- just reached one adapter.reconcile() call later."""
    store = FakeStore(
        row(
            ActionState.UNKNOWN,
            action_uid=ACTION_UID,
            provider_request_ref="req-1",
            attempt_count=5,
        )
    )
    adapter = FakeAdapter(
        ProviderObservation(ProviderDisposition.AMBIGUOUS, "provider_timeout", provider_request_ref="req-1")
    )

    result = await service(store, adapter).exhaust(ACTION_ID)

    assert result.status is PublicStatus.MANUAL_REVIEW
    assert store.current.state is ActionState.MANUAL_REVIEW
    assert adapter.calls == [("reconcile",)]
    assert any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)


@pytest.mark.asyncio
async def test_exhaust_final_check_completes_a_send_the_provider_actually_made():
    """FIX 2: exhaust() used to go straight to dead_letter/manual_review for
    an unknown-outcome action with no final provider look (wakes
    27296/27297/27314 -- Cliq's poll() came back inconclusive four times
    running while the post had, or had not, actually gone out)."""
    store = FakeStore(
        row(
            ActionState.UNKNOWN,
            action_uid=ACTION_UID,
            provider_request_ref="req-1",
            attempt_count=5,
        )
    )
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

    result = await service(store, adapter).exhaust(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert store.current.state is ActionState.COMPLETED
    assert adapter.calls == [("reconcile",)]
    assert not any(call[0] == "definitive_fail" for call in store.calls)


@pytest.mark.asyncio
async def test_exhaust_final_check_fails_definitively_even_if_the_observation_says_retryable():
    """The retry budget is already spent: a proven non-acceptance here is
    final regardless of the adapter's own retryable flag -- looping back to
    retry_ready would just land the row in list_exhausted again next
    worker cycle."""
    store = FakeStore(
        row(
            ActionState.UNKNOWN,
            action_uid=ACTION_UID,
            provider_request_ref="req-1",
            attempt_count=5,
        )
    )
    adapter = FakeAdapter(
        ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
            "provider_busy",
            provider_request_ref="req-1",
            category="transient_upstream_error",
            retryable=True,
        )
    )

    result = await service(store, adapter).exhaust(ACTION_ID)

    assert result.status is PublicStatus.FAILED
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert store.current.detail_code == "provider_busy"
    assert adapter.calls == [("reconcile",)]
    assert not any(call[0] == "transition" and call[2] is ActionState.MANUAL_REVIEW for call in store.calls)


@pytest.mark.asyncio
async def test_exhaust_final_check_skipped_when_a_durable_acceptance_already_recovers():
    """plan_recover_acceptance still takes priority: a row with a trusted
    persisted receipt completes from the ledger alone, no provider call --
    exactly test_provider_accepted_exhaustion_recovers_persisted_receipt's
    shape, now proven to bypass the new final-check call too."""
    store = FakeStore(
        row(
            ActionState.PROVIDER_ACCEPTED,
            action_uid=ACTION_UID,
            provider_request_ref="req-accepted",
            provider_message_id="mail-accepted",
            provider_accepted_at=NOW,
            attempt_count=5,
        )
    )
    adapter = FakeAdapter()

    result = await service(store, adapter).exhaust(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == []
    assert [call[0] for call in store.calls] == ["claim", "complete"]


@pytest.mark.asyncio
async def test_the_preflight_never_refuses_over_newer_inbound():
    """newer_inbound is gone from the preflight: a newer message is the
    stale-context question's (asked of the agent), never a silent `stale`.
    Here nobody can be asked (no probe), so the send proceeds."""
    store = FakeStore()
    adapter = FakeAdapter(_accepted_observation())
    proof = evidence(later_inbound_message_id=701)

    result = await service(store, adapter, proof=proof).execute(request())

    assert result.status is PublicStatus.SENT
    assert not any(call[0] == "transition" and call[2] is ActionState.STALE for call in store.calls)


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
async def test_with_traffic_control_off_a_waiting_reply_is_not_staled_by_the_preflight():
    """OUTBOUND_TRAFFIC_CONTROL=off (here: no probe) switches off the one
    stale check; the preflight's newer_inbound no-send no longer exists, so a
    reply that waited (dependency_wait) is sent as saved."""
    store = FakeStore(
        row(
            ActionState.DEPENDENCY_WAIT,
            action_uid=ACTION_UID,
            detail_code="calendar_dependency_pending",
            attempt_count=2,
        )
    )
    adapter = FakeAdapter(_accepted_observation())
    proof = evidence(later_inbound_message_id=701)

    result = await service(store, adapter, proof=proof).resume(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert store.calls[0][:2] == ("claim", ActionState.DEPENDENCY_WAIT)
    assert not any(call[0] == "transition" and call[2] is ActionState.STALE for call in store.calls)


@pytest.mark.asyncio
async def test_worker_executes_the_saved_record_when_live_context_has_drifted():
    """The worker used to re-derive the context and park the action when it
    differed from the saved one; live data moves (a sender name flips, a
    message is re-threaded), so real sends were parked (wake 27244). It now
    executes the saved record -- Comm-Data-Store migration 206 makes the
    record immutable, which is what the comparison used to guard."""
    recorded_target = "saved-recipient@convo.zillow.com"
    store = FakeStore(
        row(
            ActionState.PREPARED,
            action_uid=ACTION_UID,
            payload_hash="f" * 64,  # the live derivation now hashes differently
            provider_account="nigel-zoho",
            routing_policy_version="appointment-v1",
            recipient_scope={"kind": "email_thread", "target_id": recorded_target, "verified": True},
            # The real record shape: the saved context repeats the account and
            # routing (a replay of 310 real actions caught this collision).
            canonical_context={
                "prospect_name": "Ytry Nationn", "source_subject": "Re: Your tour",
                "provider_account": "nigel-zoho", "routing_policy_version": "appointment-v1",
                "target": {"kind": "email_thread", "target_id": recorded_target},
                "identity_version": "v1", "channel_id": 866870,
            },
        )
    )
    adapter = FakeAdapter(
        ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1", provider_call_id="req-1"),
        ProviderObservation(
            ProviderDisposition.ACCEPTED, "provider_accepted", provider_request_ref="req-1",
            message_id="mail-1", accepted_at=NOW, evidence={"kind": "provider_message_id"},
        ),
    )

    result = await service(store, adapter).resume(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls[0] == ("build", recorded_target, ACTION_UID)
    assert not any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)


@pytest.mark.asyncio
async def test_dispatch_pending_job_is_caught_by_the_poll_window():
    """wake 27321: the gateway's own timestamps showed provider_queue_timeout
    firing ~0.12s after dispatch while the provider's own write landed
    ~0.42s later -- one immediate recheck gave up long before a queued but
    healthy job had a real chance to answer. A second poll inside the
    response-budget window now catches it, so the send completes instead of
    going ambiguous."""
    store = FakeStore(row(ActionState.PREPARED, action_uid=ACTION_UID))
    adapter = FakeAdapter(
        ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1"),
        ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1"),
        ProviderObservation(
            ProviderDisposition.ACCEPTED,
            "provider_accepted",
            provider_request_ref="req-1",
            message_id="mail-1",
            accepted_at=NOW,
            evidence={"kind": "provider_message_id"},
        ),
    )
    svc = service(store, adapter)

    result = await svc.resume(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert store.current.state is ActionState.COMPLETED
    assert [call[0] for call in adapter.calls].count("poll") == 2
    svc._sleep.assert_awaited()


@pytest.mark.asyncio
async def test_dispatch_pending_job_still_times_out_only_after_the_poll_window():
    """A job that never answers inside the window still ends up
    provider_queue_timeout -- the window is a wait, not an infinite one."""
    store = FakeStore(row(ActionState.PREPARED, action_uid=ACTION_UID))
    adapter = FakeAdapter(
        ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1"),
        ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1"),
        ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1"),
    )
    svc = service(store, adapter)

    result = await svc.resume(ACTION_ID)

    assert result.status is PublicStatus.UNKNOWN
    assert result.detail_code == "provider_queue_timeout"
    assert [call[0] for call in adapter.calls].count("poll") == 2
    svc._sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_worker_accepts_one_way_durable_subject_alias_promotion():
    stored_prospect = "prospect:factbook:stable-id"
    current_prospect = "subject:durable-alias-id"
    stored_context = {"identity_version": "v1", "prospect_id": stored_prospect}
    stored_scope = {"version": "v1", "prospect_id": stored_prospect}
    current_context = replace(
        context(),
        prospect_id=current_prospect,
        canonical_context=MappingProxyType(
            {"identity_version": "v1", "prospect_id": current_prospect}
        ),
        canonical_scope=MappingProxyType(
            {"version": "v1", "prospect_id": current_prospect}
        ),
    )
    payload_hash = canonical_payload_hash(
        {
            "action_role": current_context.action_role.value,
            "operation": current_context.operation.value,
            "intent_kind": current_context.intent_kind.value,
            "appointment_slot": current_context.appointment_slot,
            "arguments": current_context.arguments,
            "canonical_context": stored_context,
        }
    )
    store = FakeStore(
        row(
            ActionState.UNKNOWN,
            action_uid=ACTION_UID,
            provider_request_ref="req-1",
            payload_hash=payload_hash,
            canonical_context=stored_context,
            canonical_scope=stored_scope,
            recipient_scope={
                "kind": "email_thread",
                "target_id": "lead@convo.zillow.com",
                "verified": True,
            },
            provider_account="nigel-zoho",
            routing_policy_version="v1",
        )
    )
    loader = AsyncMock()
    loader.load.return_value = current_context
    proof_loader = AsyncMock()
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

    result = await gateway.reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert store.current.state is ActionState.COMPLETED
    assert ("reconcile",) in adapter.calls


@pytest.mark.asyncio
async def test_worker_terminalizes_context_that_can_no_longer_be_derived():
    """resume() only ever runs for a pre-dispatch row (dependency_wait /
    prepared / retry_ready): nothing was sent, so a reload failure parks it
    for a person straight away -- unlike reconcile() below, there is no
    in-flight send whose outcome would otherwise be abandoned."""
    store = FakeStore(row(ActionState.PREPARED, action_uid=ACTION_UID, payload_hash="a" * 64))
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

    result = await gateway.resume(ACTION_ID)

    assert result.status is PublicStatus.MANUAL_REVIEW
    assert store.current.state is ActionState.MANUAL_REVIEW
    assert adapter.calls == []
    assert any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)
    assert result.detail.startswith("wakeup event does not exist. ")


@pytest.mark.asyncio
async def test_reconcile_retries_a_reload_failure_on_an_unknown_action_instead_of_parking():
    """wake 27321: action 497fcaf8 went dispatching -> provider_queue_timeout
    -> unknown, then a transient ContextDerivationError on the very next
    reconcile parked it manual_review/persisted_context_unavailable -- with
    the provider's own outcome still unconfirmed. A reload failure on an
    action reconcile() only ever sees in UNKNOWN state (dispatched, outcome
    unconfirmed) must retry on the ordinary schedule, never park immediately
    -- exhaust() (once the retry budget is actually spent) is what may
    finally end this in manual_review, not reconcile()."""
    store = FakeStore(row(ActionState.UNKNOWN, action_uid=ACTION_UID, provider_request_ref=None, payload_hash="a" * 64))
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

    result = await gateway.reconcile(ACTION_ID)

    assert result.status is PublicStatus.UNKNOWN
    assert store.current.state is ActionState.UNKNOWN
    assert adapter.calls == []
    assert not any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)
    assert not any(call[0] == "transition" and call[2] is ActionState.MANUAL_REVIEW for call in store.calls)
    assert [call[0] for call in store.calls] == ["claim", "schedule"]
    assert result.detail == "wakeup event does not exist"


@pytest.mark.asyncio
async def test_reconcile_continues_normally_once_the_reload_succeeds():
    """The retry is not a dead end: once the identical transient failure
    resolves (the next reload succeeds, as it did minutes later for wake
    27321's own action), the ordinary reconcile path runs to completion --
    it does not keep retrying forever, and it never re-dispatches."""
    store = FakeStore(_identity_matched_row(ActionState.UNKNOWN, action_uid=ACTION_UID, provider_request_ref=None))
    settled = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "email_reconciled_by_message_id",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "exact_message_id"},
    )
    adapter = FakeAdapter(settled)
    loader = AsyncMock()
    loader.load.side_effect = [ContextDerivationError("wakeup event does not exist"), context()]
    proof_loader = AsyncMock()
    gateway = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=proof_loader,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=object(),
        # A fixed clock past the retry's short backoff so the second
        # reconcile() call is due -- FakeStore.schedule_next_attempt always
        # anchors the delay off the module's own NOW.
        clock=lambda: NOW + timedelta(seconds=10),
        lease_owner="gateway-test",
    )

    first = await gateway.reconcile(ACTION_ID)
    assert first.status is PublicStatus.UNKNOWN
    assert [call[0] for call in store.calls] == ["claim", "schedule"]
    assert adapter.calls == []

    result = await gateway.reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert store.current.state is ActionState.COMPLETED
    assert adapter.calls == [("reconcile",)]
    assert not any(call[0] == "build" for call in adapter.calls)


@pytest.mark.asyncio
async def test_exhaust_parks_in_manual_review_carrying_the_real_reload_reason():
    """Only once the retry budget is actually spent (exhaust(), which the
    worker calls in place of reconcile() past list_exhausted's threshold --
    never reconcile() itself) does a repeated reload failure end in
    manual_review, and it carries the real cause instead of a bare
    retry_budget_exhausted with no hint why."""
    store = FakeStore(
        row(ActionState.UNKNOWN, action_uid=ACTION_UID, provider_request_ref=None, payload_hash="a" * 64, attempt_count=5)
    )
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

    result = await gateway.exhaust(ACTION_ID)

    assert result.status is PublicStatus.MANUAL_REVIEW
    assert store.current.state is ActionState.MANUAL_REVIEW
    assert adapter.calls == []
    assert result.detail.startswith("wakeup event does not exist. ")


def _accepted_observation() -> ProviderObservation:
    return ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "provider_accepted",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "provider_message_id"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["execute", "resume"])
async def test_another_wakes_uncertain_send_never_holds_this_action(entry):
    """2026-09-30: an uncertain quo.sms.send (action b78d5668) held a
    calendar update 22 min and a staff Cliq post 9.5 min for the same person.
    There is no per-person hold any more: this action goes out on its own;
    a newer send the agent has not seen is the stale-context question."""
    store = FakeStore() if entry == "execute" else FakeStore(row(ActionState.PREPARED, action_uid=ACTION_UID))
    adapter = FakeAdapter(_accepted_observation())
    uncertain_sms = SimpleNamespace(
        action_id=uuid4(), operation="quo.sms.send", state="unknown", created_at=NOW, preview="Hi Carol"
    )
    probe = FakeProbe(in_flight=[uncertain_sms])
    gateway = service(store, adapter, traffic_mode="enforce", traffic_probe=probe)

    result = await (gateway.execute(request()) if entry == "execute" else gateway.resume(ACTION_ID))

    assert result.status is PublicStatus.SENT
    assert result.detail_code != "lease_held"
    assert adapter.calls
    assert not any(call[0] == "in_flight" for call in probe.calls)


@pytest.mark.asyncio
async def test_with_confirmation_disabled_newer_context_is_a_deliberate_no_send(caplog):
    """Confirmation disabled: never ask, and never send stale. The row ends
    `stale` / stale_context_unasked -- never the old terminal
    definitive_failed/traffic_blocked -- and the newer items are logged."""
    store = FakeStore()
    adapter = FakeAdapter(_accepted_observation())
    probe = FakeProbe(newer=NewerActivity(
        direction="inbound",
        source="zillow",
        occurred_at=NOW,
        preview="Are you still available Friday?",
        message_id=999,
        action_id=None,
    ))

    with caplog.at_level(logging.WARNING):
        result = await service(store, adapter, traffic_mode="enforce", traffic_probe=probe).execute(request())

    assert (result.status, result.detail_code) == (PublicStatus.STALE, "stale_context_unasked")
    assert adapter.calls == []
    assert not any(call[0] == "definitive_fail" for call in store.calls)
    messages = [record.getMessage() for record in caplog.records]
    assert any("nobody to ask (confirmation disabled)" in message and "message:999" in message for message in messages)


@pytest.mark.asyncio
async def test_override_does_not_bypass_newer_context_when_nobody_can_be_asked():
    """override=true is the "yes" of a question; with confirmation disabled
    there is no question, so it cannot send over newer context."""
    store = FakeStore()
    adapter = FakeAdapter(_accepted_observation())
    probe = FakeProbe(newer=NewerActivity(
        direction="inbound",
        source="zillow",
        occurred_at=NOW,
        preview="Are you still available Friday?",
        message_id=999,
        action_id=None,
    ))

    result = await service(store, adapter, traffic_mode="enforce", traffic_probe=probe).execute(request(override=True))

    assert (result.status, result.detail_code) == (PublicStatus.STALE, "stale_context_unasked")
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_traffic_control_shadow_logs_and_dispatches(caplog):
    store = FakeStore()
    adapter = FakeAdapter(_accepted_observation())
    probe = FakeProbe(newer=NewerActivity(
            direction="inbound",
            source="zillow",
            occurred_at=NOW,
            preview="Are you still available Friday?",
            message_id=999,
            action_id=None,
        ))

    with caplog.at_level(logging.WARNING):
        result = await service(store, adapter, traffic_mode="shadow", traffic_probe=probe).execute(request())

    assert result.status is PublicStatus.SENT
    assert adapter.calls
    assert not any(call[0] == "definitive_fail" for call in store.calls)
    messages = [record.getMessage() for record in caplog.records]
    assert any("newer=message:999" in message for message in messages)


@pytest.mark.asyncio
async def test_traffic_control_off_never_calls_probe():
    store = FakeStore()
    adapter = FakeAdapter(_accepted_observation())
    probe = FakeProbe()

    result = await service(store, adapter, traffic_mode="off", traffic_probe=probe).execute(request())

    assert result.status is PublicStatus.SENT
    assert probe.calls == []


def test_traffic_mode_rejects_unknown_value():
    with pytest.raises(ValueError):
        service(FakeStore(), FakeAdapter(), traffic_mode="paranoid")


def test_traffic_mode_without_probe_warns_at_construction(caplog):
    """mode in {shadow, enforce} with no probe silently behaves like 'off'
    (the gate short-circuits on `traffic_probe is None` every call) -- that
    is exactly the wiring bug (env var set, probe forgotten in
    build_runtime()) that must be loud, not invisible."""
    with caplog.at_level(logging.WARNING):
        service(FakeStore(), FakeAdapter(), traffic_mode="shadow", traffic_probe=None)

    messages = [record.getMessage() for record in caplog.records]
    assert any("traffic_probe" in message for message in messages)


def test_traffic_mode_off_without_probe_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING):
        service(FakeStore(), FakeAdapter(), traffic_mode="off", traffic_probe=None)

    assert not caplog.records


@pytest.mark.asyncio
async def test_worker_resume_over_newer_context_ends_stale_unasked_and_logs_it(caplog):
    """worker.py routes dependency_wait/prepared/retry_ready through resume():
    nobody can be asked there, so no answer means no send. The prepared row
    is claimed and ends `stale` / stale_context_unasked (never the old
    terminal traffic_blocked failure); the items are logged."""
    store = FakeStore(row(ActionState.PREPARED, action_uid=ACTION_UID))
    adapter = FakeAdapter(_accepted_observation())
    probe = FakeProbe(newer=NewerActivity(
        direction="inbound",
        source="zillow",
        occurred_at=NOW,
        preview="Are you still available Friday?",
        message_id=999,
        action_id=None,
    ))

    with caplog.at_level(logging.WARNING):
        result = await service(store, adapter, traffic_mode="enforce", traffic_probe=probe).resume(ACTION_ID)

    assert (result.status, result.detail_code) == (PublicStatus.STALE, "stale_context_unasked")
    assert adapter.calls == []
    assert not any(call[0] == "definitive_fail" for call in store.calls)
    assert any(call[0] == "claim" for call in store.calls)
    messages = [record.getMessage() for record in caplog.records]
    assert any("nobody to ask (worker)" in message and "message:999" in message for message in messages)


@pytest.mark.asyncio
async def test_a_legacy_traffic_blocked_row_stays_terminal_and_override_does_not_resend_it():
    """The traffic_blocked remediation path is gone with the block itself:
    an old definitive_failed/stale_context row is reported as it is."""
    store = FakeStore(
        row(ActionState.DEFINITIVE_FAILED, action_uid=ACTION_UID, detail_code="stale_context", error_category="traffic_blocked")
    )
    adapter = FakeAdapter()
    probe = FakeProbe(newer=NewerActivity(
            direction="inbound",
            source="zillow",
            occurred_at=NOW,
            preview="Are you still available Friday?",
            message_id=999,
            action_id=None,
        ))

    result = await service(store, adapter, traffic_mode="enforce", traffic_probe=probe).execute(request(override=True))

    assert result.status is PublicStatus.FAILED
    assert result.action_id == ACTION_ID
    assert not any(call[0] == "remediate" for call in store.calls)
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_a_contended_intent_lock_waits_instead_of_terminalizing():
    """A fresh row whose prepare() hits a contended intent lock lands in
    DEPENDENCY_WAIT and waits (the worker retries); it never terminalizes."""
    store = FakeStore()
    adapter = FakeAdapter()
    probe = FakeProbe()

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

    result = await service(store, adapter, traffic_mode="enforce", traffic_probe=probe).execute(request())

    assert result.status is PublicStatus.PENDING
    assert result.detail_code == "intent_lock_contended"
    assert store.current.state is ActionState.DEPENDENCY_WAIT
    assert not any(call[0] in ("claim", "definitive_fail") for call in store.calls)
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_execute_swallows_post_dispatch_exception_and_returns_durable_row_state(caplog):
    """A network timeout *after* the provider already accepted the HTTP
    request (adapter.invoke() raises here, simulating that) must never
    escape execute() as a raised error: FastMCP wraps any uncaught
    exception as "Error executing tool outbound_action: ...", and the CDS
    reconciler's rejection-prefix rule treats that wrapper as proof
    nothing was sent. By the time invoke() runs, claim()+transition() to
    DISPATCHING has already happened (service.py's _dispatch()), so the
    row is durably recoverable -- the caller must get that durable state
    back, not a raised exception, and an ERROR must be logged."""
    store = FakeStore()
    adapter = FakeAdapter()
    adapter.invoke = AsyncMock(side_effect=RuntimeError("network timeout after accept"))

    with caplog.at_level(logging.ERROR):
        result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.PENDING
    assert result.action_id == ACTION_ID
    assert store.current.state is ActionState.DISPATCHING
    messages = [r.getMessage() for r in caplog.records]
    assert any("post-dispatch exception" in m for m in messages)


@pytest.mark.asyncio
async def test_resume_swallows_post_dispatch_exception_and_returns_durable_row_state(caplog):
    store = FakeStore(row(ActionState.PREPARED, action_uid=ACTION_UID))
    adapter = FakeAdapter()
    adapter.invoke = AsyncMock(side_effect=RuntimeError("network timeout after accept"))

    with caplog.at_level(logging.ERROR):
        result = await service(store, adapter).resume(ACTION_ID)

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.DISPATCHING
    messages = [r.getMessage() for r in caplog.records]
    assert any("post-dispatch exception" in m for m in messages)


# ----------------------------------------------------------------------------
# An error before the provider is called: say "not sent" and let the agent
# decide. A TypeError in a fake evidence loader used to come back `pending`
# while the row sat in `received`, which the worker never lists: a silent
# no-send. Executing the same request again re-runs the send.
# ----------------------------------------------------------------------------


def _assert_not_sent(result, adapter, caplog, error_type):
    assert result.status is PublicStatus.FAILED
    assert result.detail_code == "gateway_internal_error"
    assert result.detail.startswith("Not sent: the gateway hit an internal error before sending")
    assert f"({error_type}: " in result.detail
    assert "Nothing went out. Execute the same request again to retry, or record needs_human." in result.detail
    assert ("invoke",) not in adapter.calls
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any("pre-send exception" in m and str(ACTION_ID) in m for m in errors)


def _assert_refused(result, store, adapter, error_type):
    """An error before the row was prepared ends it rejected, with the
    error's words and the override that still sends it -- never a
    `received` row nothing picks up again."""
    assert result.status is PublicStatus.REJECTED
    assert result.detail_code == "gateway_error"
    assert store.current.state is ActionState.REJECTED
    assert result.detail.startswith(f"Not sent: {error_type}: ")
    assert '"override" request' in result.detail and "Never send it through any other tool or route." in result.detail
    assert result.override is not None
    assert (result.override.op, result.override.decision, result.override.action_id) == ("confirm", "yes", ACTION_ID)
    assert ("invoke",) not in adapter.calls


@pytest.mark.asyncio
async def test_an_evidence_loading_error_is_reported_not_sent(caplog):
    store = FakeStore()
    adapter = FakeAdapter()
    svc = service(store, adapter)
    svc._evidence_loader.load.side_effect = TypeError("load() got an unexpected keyword argument 'as_of'")

    with caplog.at_level(logging.ERROR):
        result = await svc.execute(request())

    _assert_refused(result, store, adapter, "TypeError")
    assert not any(call[0] == "schedule" for call in store.calls)
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any("pre-send exception" in m and str(ACTION_ID) in m for m in errors)


@pytest.mark.asyncio
async def test_a_preflight_error_is_reported_not_sent(caplog):
    store = FakeStore()
    adapter = FakeAdapter()
    svc = service(store, adapter)

    with (
        patch(
            "postgres_mcp.outbound_gateway.service.SafetyPreflight.evaluate",
            side_effect=AttributeError("'NoneType' object has no attribute 'calendar_dependency'"),
        ),
        caplog.at_level(logging.ERROR),
    ):
        result = await svc.execute(request())

    _assert_refused(result, store, adapter, "AttributeError")


def _accepted(ref="req-1"):
    return ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "provider_accepted",
        provider_request_ref=ref,
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "provider_message_id"},
    )


@pytest.mark.asyncio
async def test_an_evidence_error_then_the_same_request_again_is_the_same_refusal(caplog):
    """The identical request is the same action: it reports the refusal
    again and never sends; sending it anyway is the override (op confirm)."""
    store = FakeStore()
    adapter = FakeAdapter(_accepted())
    svc = service(store, adapter)
    svc._evidence_loader.load.side_effect = [TypeError("boom"), evidence()]

    with caplog.at_level(logging.ERROR):
        first = await svc.execute(request())
        _assert_refused(first, store, adapter, "TypeError")
        second = await svc.execute(request())

    assert (second.status, second.action_id, second.override) == (PublicStatus.REJECTED, first.action_id, first.override)
    assert ("invoke",) not in adapter.calls


@pytest.mark.asyncio
async def test_a_request_build_error_goes_back_to_retry_ready_and_the_same_request_again_sends_once(caplog):
    """The row was already marked dispatching, which a re-execute only
    reports: the provider was provably not called, so it goes back to
    retry_ready, which a re-execute dispatches."""
    store = FakeStore()
    adapter = FakeAdapter(_accepted())
    build = adapter.build_request
    failures = [KeyError("sender_domain")]

    def build_once(ctx, action_uid):
        if failures:
            raise failures.pop()
        return build(ctx, action_uid)

    adapter.build_request = build_once
    svc = service(store, adapter)

    with caplog.at_level(logging.ERROR):
        first = await svc.execute(request())
        _assert_not_sent(first, adapter, caplog, "KeyError")
        assert store.current.state is ActionState.RETRY_READY
        second = await svc.execute(request())

    assert ("transition", ActionState.DISPATCHING, ActionState.RETRY_READY, "gateway_internal_error", "gateway-test") in store.calls
    assert second.status is PublicStatus.SENT
    assert adapter.calls.count(("invoke",)) == 1


@pytest.mark.asyncio
async def test_a_worker_resume_error_before_the_provider_call_ends_the_waiting_row_rejected(caplog):
    """A dependency_wait row that cannot be prepared is ended as a recorded
    refusal: Restate stops on it (a rejected result is terminal) instead of
    ending its workflow over a row nobody would advance again."""
    store = FakeStore(row(ActionState.DEPENDENCY_WAIT, action_uid=None))
    adapter = FakeAdapter()
    svc = service(store, adapter)
    svc._evidence_loader.load.side_effect = TypeError("boom")

    with caplog.at_level(logging.ERROR):
        result = await svc.resume(ACTION_ID)

    _assert_refused(result, store, adapter, "TypeError")
    assert [call[0] for call in store.calls] == ["claim", "reject"]


@pytest.mark.asyncio
async def test_an_error_after_the_provider_call_started_stays_uncertain_not_not_sent(caplog):
    """The provider may have the request: unknown/reconcile, never 'not sent'."""
    store = FakeStore()
    pending = ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1", provider_call_id="req-1")
    adapter = FakeAdapter(pending)
    adapter.poll = AsyncMock(side_effect=TypeError("poll() bug"))

    with caplog.at_level(logging.ERROR):
        result = await service(store, adapter).execute(request())

    assert result.status is PublicStatus.PENDING
    assert result.detail_code != "gateway_internal_error"
    assert store.current.state is ActionState.DISPATCHING
    messages = [r.getMessage() for r in caplog.records]
    assert any("post-dispatch exception" in m for m in messages)
    assert not any("pre-send exception" in m for m in messages)


@pytest.mark.asyncio
async def test_execute_still_raises_context_derivation_error_before_dispatch():
    """Regression guard: context load/validation is deliberately NOT
    covered by the post-dispatch except clause -- a true pre-dispatch
    rejection must keep raising so the MCP error wrapper (and the
    reconciler's rejection-prefix rule that depends on it) stays accurate
    for genuine rejections."""
    store = FakeStore()
    adapter = FakeAdapter()
    loader = AsyncMock()
    loader.load.side_effect = ContextDerivationError("wakeup event does not exist")
    preflight = AsyncMock()
    svc = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=preflight,
        adapters={Operation.EMAIL_SEND: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="gateway-test",
    )

    with pytest.raises(ContextDerivationError):
        await svc.execute(request())


@pytest.mark.asyncio
async def test_a_later_action_of_the_same_role_carries_its_own_identity():
    """CDS migration 204: a wake may hold several actions per role, so the row
    create_or_load returns can be a later ordinal than the loaded context's
    ordinal-0 id. Every step after it (the traffic check that excludes the
    action itself, the lock holder, the result) must use the row's id."""
    later = uuid4()
    pending = ProviderObservation(
        ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-2", provider_call_id="req-2"
    )
    accepted = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "provider_accepted",
        provider_request_ref="req-2",
        message_id="mail-2",
        accepted_at=NOW,
        evidence={"kind": "provider_message_id"},
    )
    store = FakeStore(row(action_id=later))
    probe = FakeProbe()

    result = await service(
        store, FakeAdapter(pending, accepted), traffic_mode="enforce", traffic_probe=probe
    ).execute(request())

    assert result.status is PublicStatus.SENT
    assert result.action_id == later
    assert store.calls[0] == ("create", ACTION_ID)
    assert probe.calls[0] == ("newer_context", "prospect:amanda", later)
    assert probe.calls[-1][-1] == later


def _identity_matched_row(state, **overrides):
    """A stored action whose account, routing and recipient match the loaded
    context but whose content does not (wake 27244's shape)."""
    loaded = context()
    return row(
        state,
        provider_account=loaded.provider_account,
        routing_policy_version=loaded.routing_policy_version,
        recipient_scope={
            "kind": loaded.target.kind,
            "target_id": loaded.target.target_id,
            "verified": loaded.target.verified,
        },
        payload_hash="f" * 64,
        **overrides,
    )


@pytest.mark.asyncio
async def test_a_dispatched_send_is_settled_by_its_provider_job_before_the_context_is_judged():
    """Wake 27244: the email job completed, but reconcile re-derived the wake
    context first, hit a transient mismatch and parked the action for review.
    The provider's answer about its own job needs no context, so it comes
    first; the mismatching context is never consulted for the outcome."""
    store = FakeStore(_identity_matched_row(ActionState.UNKNOWN, action_uid=ACTION_UID, provider_request_ref="req-1"))
    sent = ProviderObservation(
        ProviderDisposition.ACCEPTED,
        "provider_accepted",
        provider_request_ref="req-1",
        message_id="mail-1",
        accepted_at=NOW,
        evidence={"kind": "provider_job_completed"},
    )
    adapter = FakeAdapter(outcome_polls=[sent])

    result = await service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == [("poll", "req-1")]
    assert ("complete", ActionState.RECONCILING, "req-1") in store.calls
    assert not any(call[0] == "transition" and call[2] is ActionState.DEAD_LETTER for call in store.calls)


@pytest.mark.asyncio
async def test_a_job_still_running_is_looked_at_again_not_parked():
    """PENDING is not a failure: it is rescheduled on the pending-wait
    ladder with no claim() (no attempt spent), so a job that is merely slow
    never burns toward the ordinary 5-attempt exhaustion budget the way a
    real ambiguous/failed outcome does."""
    store = FakeStore(_identity_matched_row(ActionState.UNKNOWN, action_uid=ACTION_UID, provider_request_ref="req-1"))
    running = ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1")
    adapter = FakeAdapter(outcome_polls=[running])

    result = await service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.UNKNOWN
    assert store.current.state is ActionState.UNKNOWN
    assert [call[0] for call in store.calls] == ["schedule"]


@pytest.mark.asyncio
async def test_pending_wait_past_its_ceiling_rejoins_the_ordinary_retry_budget():
    """The several-hour ceiling is a safety net, not an escape hatch: a job
    still PENDING long after it should plausibly still be running rejoins
    the ordinary claim()-then-schedule path, so it still eventually reaches
    a person instead of waiting on the pending ladder forever."""
    old_created_at = NOW - timedelta(hours=7)
    store = FakeStore(
        _identity_matched_row(
            ActionState.UNKNOWN, action_uid=ACTION_UID, provider_request_ref="req-1", created_at=old_created_at
        )
    )
    running = ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1")
    adapter = FakeAdapter(outcome_polls=[running])

    result = await service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.UNKNOWN
    assert store.current.state is ActionState.UNKNOWN
    assert [call[0] for call in store.calls] == ["claim", "schedule"]


@pytest.mark.asyncio
async def test_when_the_provider_cannot_say_reconcile_proceeds_from_the_saved_record():
    store = FakeStore(_identity_matched_row(ActionState.UNKNOWN, action_uid=ACTION_UID, provider_request_ref="req-1"))
    sent = ProviderObservation(
        ProviderDisposition.ACCEPTED, "email_reconciled_by_message_id", provider_request_ref="req-1",
        message_id="mail-1", accepted_at=NOW, evidence={"kind": "exact_message_id"},
    )
    adapter = FakeAdapter(sent)

    result = await service(store, adapter).reconcile(ACTION_ID)

    assert result.status is PublicStatus.SENT
    assert adapter.calls == [("poll", "req-1"), ("reconcile",)]

