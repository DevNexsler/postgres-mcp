# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false
"""The 10/26 TenantCloud outage, replayed through the real delivery path.

TenantCloud's API hung or answered 503/500 from 12:23 PM to 12:10 AM ET.
Two tenant replies queued during it -- Mary Haas's (action 152dd448, created
17:25 ET) and Charles Serpone's (d2031a21, created 22:32 ET) -- were never
sent: the coordinator waited while auth was down, and the first check after
auth came back found them more than an hour old and closed them
`retry_budget_exhausted` without a single dispatch. Their one-hour budget
had run on wall-clock time, spent entirely on an outage.

These drive coordinator -> OutboundActionService -> TenantCloudAdapter with
only the TenantCloud facade faked (test_restate_e2e_operations_matrix.py's
harness), and assert how many times the provider's write actually ran.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from postgres_mcp.outbound_gateway.adapters.tenantcloud import TenantCloudAdapter
from postgres_mcp.outbound_gateway.delivery_workflow import AuthResult
from postgres_mcp.outbound_gateway.delivery_workflow import AuthState
from postgres_mcp.outbound_gateway.delivery_workflow import DeliveryPhase
from postgres_mcp.outbound_gateway.delivery_workflow import OutboundDeliveryCoordinator
from postgres_mcp.outbound_gateway.metrics import CircuitStatus
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.retry_config import RetryPolicyConfig
from postgres_mcp.outbound_gateway.retry_policy import STALENESS_CEILING_DETAIL
from postgres_mcp.outbound_gateway.service import OutboundActionService

from .test_adapters import TC_ACCEPTED
from .test_adapters import FakeMutationExecution
from .test_adapters import FakeMutationResult
from .test_adapters import FakeTenantCloudMutations
from .test_restate_e2e_delivery import FakeStore
from .test_restate_e2e_delivery import RecordingStaffWarningPort
from .test_restate_e2e_operations_matrix import ACTION_UID
from .test_restate_e2e_operations_matrix import CREATED_AT
from .test_restate_e2e_operations_matrix import TC_MESSAGE_ID
from .test_restate_e2e_operations_matrix import FakeClock
from .test_restate_e2e_operations_matrix import drive
from .test_restate_e2e_operations_matrix import mutation_observation
from .test_restate_e2e_operations_matrix import tc_message_context
from .test_restate_e2e_operations_matrix import tc_message_record

DEFAULTS = RetryPolicyConfig.from_env({})


class OutageAuth:
    """TenantCloud auth that is down until `back_at`, then ready."""

    def __init__(self, clock: FakeClock, back_at) -> None:
        self._clock = clock
        self._back_at = back_at
        self.checks = 0

    async def ensure_ready(self) -> AuthResult:
        self.checks += 1
        if self._clock() < self._back_at:
            return AuthResult(AuthState.PROVIDER_FAILURE)
        return AuthResult(AuthState.READY)


class OutageCircuit:
    """A provider circuit that is open until `closes_at`."""

    def __init__(self, clock: FakeClock, closes_at) -> None:
        self._clock = clock
        self._closes_at = closes_at

    async def circuit_status(self, operation: Operation) -> CircuitStatus:
        del operation
        remaining = (self._closes_at - self._clock()).total_seconds()
        if remaining > 0:
            return CircuitStatus(is_open=True, retry_after_seconds=min(180, int(remaining) + 1), failure_count=5)
        return CircuitStatus(is_open=False, retry_after_seconds=0, failure_count=0)


def accepting_facade() -> FakeTenantCloudMutations:
    facade = FakeTenantCloudMutations()
    facade.send_message_result = FakeMutationExecution(FakeMutationResult(TC_ACCEPTED), mutation_observation(), None)
    return facade


def harness(
    *,
    outage_hours: float,
    policies: RetryPolicyConfig = DEFAULTS,
    circuit_open_hours: float = 0.0,
) -> tuple[OutboundDeliveryCoordinator, FakeStore, FakeClock, RecordingStaffWarningPort, FakeTenantCloudMutations, OutageAuth]:
    clock = FakeClock(CREATED_AT)
    # Both 10/26 rows were prepared the moment they were asked for
    # (prepared_at = created_at) and never dispatched.
    queued = replace(tc_message_record(), state=ActionState.PREPARED, action_uid=ACTION_UID, detail_code="prepared")
    store = FakeStore(queued, clock=clock)
    facade = accepting_facade()
    context_loader = AsyncMock()
    context_loader.load.return_value = tc_message_context()
    evidence_loader = AsyncMock()
    evidence_loader.load.return_value = object()
    operation = Operation.TENANTCLOUD_MESSAGE_SEND
    service = OutboundActionService(
        store=store,
        context_loader=context_loader,
        evidence_loader=evidence_loader,
        adapters={operation: TenantCloudAdapter(mutations_factory=lambda: facade)},
        provider_client=None,
        clock=clock,
        lease_owner="outbound-gateway-test",
        response_budget_seconds=6,
        lease_seconds=60,
        sleep=AsyncMock(),
        traffic_mode="off",
        restate_operations=frozenset({operation}),
        retry_policies=policies,
        circuit_guard=OutageCircuit(clock, CREATED_AT + timedelta(hours=circuit_open_hours)),
    )
    auth = OutageAuth(clock, CREATED_AT + timedelta(hours=outage_hours))
    staff_warning = RecordingStaffWarningPort()
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({operation}),
        clock=clock,
        staff_warning=staff_warning,
        retry_policies=policies,
    )
    return coordinator, store, clock, staff_warning, facade, auth


def sends(facade: FakeTenantCloudMutations) -> int:
    return sum(1 for call in facade.calls if call[0] == "send_message")


async def drive_recording_waits(coordinator, action_id, clock) -> tuple[Any, list[int]]:
    waits: list[int] = []
    for _ in range(4000):
        result = await coordinator.advance(action_id)
        if result.phase in (DeliveryPhase.COMPLETE, DeliveryPhase.TERMINAL):
            if result.staff_warning:
                clock.advance(coordinator.staff_warning_grace_seconds)
                await coordinator.warn_unless_recovered(action_id, result.detail_code)
            return result, waits
        waits.append(result.retry_after_seconds)
        clock.advance(result.retry_after_seconds)
    raise AssertionError("workflow did not terminate")


@pytest.mark.asyncio
async def test_a_reply_queued_through_a_seven_hour_auth_outage_is_sent_once_when_auth_returns():
    """10/26 replay: auth is down for 7 hours from the moment the reply is
    queued. The clock pauses for the outage, the waits back off to the cap
    instead of spinning, and the first look after auth returns sends it --
    once. Before this, that first look closed it retry_budget_exhausted."""
    coordinator, store, clock, staff_warning, facade, auth = harness(outage_hours=7)
    policy = DEFAULTS.for_operation(Operation.TENANTCLOUD_MESSAGE_SEND)

    result, waits = await drive_recording_waits(coordinator, TC_MESSAGE_ID, clock)

    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert sends(facade) == 1
    assert staff_warning.calls == []
    # It waited out the whole outage -- far past the old one-hour budget --
    # and the outage is what the pause recorded.
    assert clock.now - CREATED_AT >= timedelta(hours=7)
    assert store.current.delivery_paused_since is None
    assert store.current.delivery_paused_seconds >= 7 * 3600
    # Exponential backoff with a cap, not a hot loop: every wait is within
    # the policy's cap, and seven hours took a few dozen looks.
    assert max(waits) <= policy.max_seconds
    assert waits[0] <= policy.base_seconds
    assert auth.checks < 60


@pytest.mark.asyncio
async def test_a_delivered_reply_is_never_sent_again_by_a_replayed_step():
    """Restate replays a step after a crash; the ledger already says sent."""
    coordinator, store, clock, _warning, facade, _auth = harness(outage_hours=7)
    await drive(coordinator, TC_MESSAGE_ID, clock)

    for _ in range(3):
        again = await coordinator.advance(TC_MESSAGE_ID)
        assert again.phase is DeliveryPhase.COMPLETE

    assert sends(facade) == 1
    assert store.current.state is ActionState.COMPLETED


@pytest.mark.asyncio
async def test_an_outage_past_the_staleness_ceiling_escalates_once_and_sends_nothing():
    """Auth stays down for 25 hours: at the 24-hour staleness ceiling the
    reply is closed unsent and a person is told -- exactly once."""
    coordinator, store, clock, staff_warning, facade, _auth = harness(outage_hours=25)

    result = await drive(coordinator, TC_MESSAGE_ID, clock)
    for _ in range(2):
        await coordinator.advance(TC_MESSAGE_ID)
        await coordinator.warn_unless_recovered(TC_MESSAGE_ID, result.detail_code)

    assert result.phase is DeliveryPhase.TERMINAL
    assert result.staff_warning is True
    assert (store.current.state, store.current.detail_code) == (ActionState.DEFINITIVE_FAILED, STALENESS_CEILING_DETAIL)
    assert sends(facade) == 0
    assert len(staff_warning.calls) == 1
    assert staff_warning.calls[0]["reason"] == STALENESS_CEILING_DETAIL
    ceiling = DEFAULTS.for_operation(Operation.TENANTCLOUD_MESSAGE_SEND).staleness_ceiling_seconds
    assert timedelta(seconds=ceiling) <= clock.now - CREATED_AT - timedelta(seconds=coordinator.staff_warning_grace_seconds)


@pytest.mark.asyncio
async def test_a_policy_that_does_not_escalate_closes_the_action_without_a_warning():
    policies = RetryPolicyConfig.from_env({"OUTBOUND_RETRY_ESCALATE": "false"})
    coordinator, store, clock, staff_warning, facade, _auth = harness(outage_hours=25, policies=policies)

    result = await drive(coordinator, TC_MESSAGE_ID, clock)

    assert (result.phase, result.staff_warning) == (DeliveryPhase.TERMINAL, False)
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert staff_warning.calls == []
    assert sends(facade) == 0


@pytest.mark.asyncio
async def test_an_operation_override_wins_over_the_global_staleness_ceiling():
    """A per-operation override shortens TenantCloud replies' ceiling to four
    hours: the same 7-hour outage now escalates instead of sending late."""
    policies = RetryPolicyConfig.from_env({"OUTBOUND_RETRY_POLICY_JSON": '{"tenantcloud.message.send": {"staleness_ceiling_seconds": 14400}}'})
    coordinator, store, clock, staff_warning, facade, _auth = harness(outage_hours=7, policies=policies)

    result = await drive(coordinator, TC_MESSAGE_ID, clock)

    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.detail_code == STALENESS_CEILING_DETAIL
    assert sends(facade) == 0
    assert len(staff_warning.calls) == 1
    assert clock.now - CREATED_AT < timedelta(hours=7)


@pytest.mark.asyncio
async def test_an_open_circuit_pauses_the_clock_and_the_send_goes_out_once_it_closes():
    """The provider's circuit is open for three hours (auth fine): the row
    waits with its clock paused and sends once when the circuit closes."""
    coordinator, store, clock, staff_warning, facade, _auth = harness(outage_hours=0, circuit_open_hours=3)

    result = await drive(coordinator, TC_MESSAGE_ID, clock)

    assert result.phase is DeliveryPhase.COMPLETE
    assert sends(facade) == 1
    assert staff_warning.calls == []
    assert clock.now - CREATED_AT >= timedelta(hours=3)
    assert store.current.delivery_paused_seconds >= 3 * 3600 - 300


@pytest.mark.asyncio
async def test_without_the_pause_the_same_outage_would_have_dropped_the_reply():
    """The control: with the deadline and ceiling both at one hour (what the
    old wall-clock budget amounted to), the reply never goes out."""
    policies = RetryPolicyConfig.from_env({"OUTBOUND_RETRY_DEADLINE_SECONDS": "3600", "OUTBOUND_RETRY_STALENESS_CEILING_SECONDS": "3600"})
    coordinator, _store, clock, _warning, facade, _auth = harness(outage_hours=7, policies=policies)

    result = await drive(coordinator, TC_MESSAGE_ID, clock)

    assert result.phase is DeliveryPhase.TERMINAL
    assert sends(facade) == 0
