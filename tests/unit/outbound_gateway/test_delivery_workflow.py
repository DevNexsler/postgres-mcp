from __future__ import annotations

import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.delivery_workflow import AuthResult
from postgres_mcp.outbound_gateway.delivery_workflow import AuthState
from postgres_mcp.outbound_gateway.delivery_workflow import DeliveryPhase
from postgres_mcp.outbound_gateway.delivery_workflow import DeliveryResult
from postgres_mcp.outbound_gateway.delivery_workflow import OutboundDeliveryCoordinator
from postgres_mcp.outbound_gateway.delivery_workflow import RestateWorkflowSubmitter
from postgres_mcp.outbound_gateway.delivery_workflow import build_restate_app
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")


def action(state: ActionState, *, attempts: int = 0):
    return SimpleNamespace(
        action_id=ACTION_ID,
        wakeup_event_id=27000,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        state=state,
        attempt_count=attempts,
        next_attempt_at=datetime(2026, 9, 14, 16, 0, tzinfo=timezone.utc),
        arguments={"to_address": "lead@example.com"},
    )


@pytest.mark.asyncio
async def test_auth_outage_waits_without_consuming_provider_attempt() -> None:
    row = action(ActionState.PREPARED, attempts=2)
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.TRANSPORT_FAILURE, retry_after_seconds=30)
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth, max_attempts=5)

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.WAIT
    assert result.detail_code == "tenantcloud_auth_transport_failure"
    assert result.retry_after_seconds == 30
    assert result.to_dict() == {
        "phase": "wait",
        "detail_code": "tenantcloud_auth_transport_failure",
        "retry_after_seconds": 30,
    }
    assert row.attempt_count == 2
    service.resume.assert_not_called()
    service.reconcile.assert_not_called()


@pytest.mark.asyncio
async def test_ready_action_resumes_once_after_auth_self_heals() -> None:
    row = action(ActionState.RETRY_READY, attempts=1)
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(status=PublicStatus.SENT, detail_code="provider_receipt_verified")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth, max_attempts=5)

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.COMPLETE
    service.resume.assert_awaited_once_with(ACTION_ID)


@pytest.mark.asyncio
async def test_received_remediation_is_prepared_before_any_provider_io() -> None:
    received = action(ActionState.RECEIVED)
    prepared = action(ActionState.PREPARED)
    store = AsyncMock()
    store.get.side_effect = [received, prepared]
    service = AsyncMock()
    service.prepare.return_value = SimpleNamespace(
        status=PublicStatus.PENDING,
        detail_code="prepared",
    )
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth)

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.WAIT
    service.prepare.assert_awaited_once_with(ACTION_ID)
    service.resume.assert_not_called()


@pytest.mark.asyncio
async def test_unavailable_persisted_context_waits_without_hot_loop() -> None:
    received = action(ActionState.RECEIVED)
    store = AsyncMock()
    store.get.return_value = received
    service = AsyncMock()
    service.prepare.return_value = SimpleNamespace(
        status=PublicStatus.PENDING,
        detail_code="operator_remediation_created",
        detail="persisted_context_unavailable",
    )
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth)

    result = await coordinator.advance(ACTION_ID)

    assert result == DeliveryResult(
        DeliveryPhase.WAIT,
        "persisted_context_unavailable",
        300,
    )
    store.get.assert_awaited_once_with(ACTION_ID)


@pytest.mark.asyncio
async def test_restarted_step_reconciles_dispatching_action_never_blind_resends() -> None:
    row = action(ActionState.PREPARED)
    store = AsyncMock()
    store.get.side_effect = lambda _action_id: row
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    service = AsyncMock()

    async def crash_after_dispatch(_action_id):
        row.state = ActionState.DISPATCHING
        raise RuntimeError("worker died after provider accepted request")

    service.resume.side_effect = crash_after_dispatch
    service.reconcile.return_value = SimpleNamespace(status=PublicStatus.SENT, detail_code="provider_receipt_verified")
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth, max_attempts=5)

    with pytest.raises(RuntimeError, match="worker died"):
        await coordinator.advance(ACTION_ID)
    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.COMPLETE
    service.resume.assert_awaited_once_with(ACTION_ID)
    service.reconcile.assert_awaited_once_with(ACTION_ID)


@pytest.mark.asyncio
async def test_ambiguous_action_reconciles_even_at_send_retry_limit() -> None:
    row = action(ActionState.UNKNOWN, attempts=5)
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.reconcile.return_value = SimpleNamespace(status=PublicStatus.UNKNOWN, detail_code="reconciliation_no_match")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth, max_attempts=5)

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.WAIT
    service.reconcile.assert_awaited_once_with(ACTION_ID)
    service.exhaust.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_restate_submission_is_success() -> None:
    calls = []

    async def request(url, body, headers, timeout):
        calls.append((url, body, headers, timeout))
        return 409, b"already invoked"

    submitter = RestateWorkflowSubmitter("http://127.0.0.1:18080", request=request)

    await submitter.submit(ACTION_ID)

    assert calls[0][0].endswith(f"/TenantCloudDelivery/{ACTION_ID}/deliver/send")
    assert calls[0][1] == b'{"action_id":"4cbac369-48c6-5b62-95e9-41f50259e732"}'


@pytest.mark.asyncio
async def test_restate_submission_rejects_non_success_status() -> None:
    async def request(_url, _body, _headers, _timeout):
        return 503, b"unavailable"

    submitter = RestateWorkflowSubmitter("http://127.0.0.1:18080", request=request)

    with pytest.raises(RuntimeError, match="HTTP 503"):
        await submitter.submit(ACTION_ID)


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1:18080",
        "http://localhost:18080",
        "http://10.0.0.1:18080",
        "http://user:password@127.0.0.1:18080",
        "http://127.0.0.1:18080/path",
    ],
)
def test_restate_submitter_rejects_non_loopback_or_malformed_ingress(url) -> None:
    with pytest.raises(ValueError, match="literal HTTP loopback"):
        RestateWorkflowSubmitter(url)


@pytest.mark.asyncio
async def test_restate_submission_rejects_oversized_response() -> None:
    async def request(_url, _body, _headers, _timeout):
        return 202, b"x" * 16_385

    submitter = RestateWorkflowSubmitter("http://127.0.0.1:18080", request=request)

    with pytest.raises(RuntimeError, match="response too large"):
        await submitter.submit(ACTION_ID)


@pytest.mark.asyncio
async def test_restate_workflow_returns_serializable_terminal_outcome(monkeypatch) -> None:
    class Workflow:
        def __init__(self, name, **_kwargs):
            self.name = name
            self.handler = None

        def main(self, **_kwargs):
            def decorate(handler):
                self.handler = handler
                return handler

            return decorate

    class TerminalError(RuntimeError):
        def __init__(self, message, *, status_code):
            super().__init__(message)
            self.status_code = status_code

    fake_restate = SimpleNamespace(
        Workflow=Workflow,
        RunOptions=lambda **kwargs: kwargs,
        TerminalError=TerminalError,
        app=lambda services: services[0],
    )
    monkeypatch.setitem(sys.modules, "restate", fake_restate)
    coordinator = AsyncMock()
    coordinator.advance.return_value = DeliveryResult(
        DeliveryPhase.COMPLETE,
        "provider_receipt_verified",
    )
    workflow = build_restate_app(coordinator)

    class Context:
        def key(self):
            return str(ACTION_ID)

        async def run_typed(self, _name, call, _options):
            return await call()

        async def sleep(self, *_args, **_kwargs):
            raise AssertionError("completed workflow must not sleep")

    with pytest.raises(TerminalError, match="action_id must equal workflow key") as error:
        await workflow.handler(Context(), None)
    assert error.value.status_code == 400
    coordinator.advance.assert_not_awaited()

    result = await workflow.handler(
        Context(),
        {"action_id": str(ACTION_ID)},
    )

    assert result == {
        "phase": "complete",
        "detail_code": "provider_receipt_verified",
        "retry_after_seconds": 0,
    }


@pytest.mark.asyncio
async def test_an_ambiguous_action_that_never_verifies_is_parked_for_a_person() -> None:
    """Action c3df14e3 (2026-09-25): TenantCloud stored a truncated text, the
    readback could never match, and the send stayed `unknown` -- holding every
    later send to that tenant. retry_policy.py's elapsed-time ceiling is the
    one budget now (not attempt_count -- see tenantcloud_delivery.py's
    advance()): past one hour from created_at it goes to manual review
    regardless of how many reconcile attempts that took."""
    created_at = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)
    row = action(ActionState.UNKNOWN, attempts=3)
    row.created_at = created_at
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.exhaust.return_value = SimpleNamespace(
        status=PublicStatus.MANUAL_REVIEW, detail_code="retry_budget_exhausted_manual_review"
    )
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        max_attempts=5,
        clock=lambda: created_at + timedelta(seconds=3601),
    )

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.TERMINAL
    service.exhaust.assert_awaited_once_with(ACTION_ID)
    service.reconcile.assert_not_called()


@pytest.mark.asyncio
async def test_an_ambiguous_action_with_many_attempts_but_inside_the_ceiling_keeps_reconciling() -> None:
    """The old 12-attempt ambiguous cap does not apply on the Restate path:
    an action that reconciles quickly and often must not get a smaller
    effective budget than one that answers slowly -- only elapsed wall-clock
    time against retry_policy.RETRY_CEILING_SECONDS decides this now."""
    created_at = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)
    row = action(ActionState.UNKNOWN, attempts=50)
    row.created_at = created_at
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.reconcile.return_value = SimpleNamespace(status=PublicStatus.UNKNOWN, detail_code="reconciliation_no_match")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        max_ambiguous_attempts=12,
        clock=lambda: created_at + timedelta(seconds=1800),
    )

    await coordinator.advance(ACTION_ID)

    service.reconcile.assert_awaited_once_with(ACTION_ID)
    service.exhaust.assert_not_called()


@pytest.mark.asyncio
async def test_a_resumable_action_with_many_attempts_but_inside_the_ceiling_still_resumes() -> None:
    """A flagged Restate operation follows retry_policy's 1h elapsed ceiling,
    not the legacy worker's 5-attempt cap: attempt_count alone (here, 40 --
    far past max_attempts=5) must never route to exhaust() while the action
    is still inside its wall-clock budget."""
    created_at = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)
    row = action(ActionState.RETRY_READY, attempts=40)
    row.created_at = created_at
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(status=PublicStatus.UNKNOWN, detail_code="provider_pending")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        max_attempts=5,
        clock=lambda: created_at + timedelta(seconds=1800),
    )

    await coordinator.advance(ACTION_ID)

    service.resume.assert_awaited_once_with(ACTION_ID)
    service.exhaust.assert_not_called()


@pytest.mark.asyncio
async def test_a_resumable_action_past_the_ceiling_is_exhausted_regardless_of_attempt_count() -> None:
    """The mirror case: a single attempt (attempt_count=1, well under the old
    5-attempt cap) still routes to exhaust() once 1h has elapsed -- proving
    the ceiling, not the count, is what governs the Restate path."""
    created_at = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)
    row = action(ActionState.RETRY_READY, attempts=1)
    row.created_at = created_at
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.exhaust.return_value = SimpleNamespace(status=PublicStatus.MANUAL_REVIEW, detail_code="retry_budget_exhausted")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        max_attempts=5,
        clock=lambda: created_at + timedelta(seconds=3601),
    )

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.TERMINAL
    service.exhaust.assert_awaited_once_with(ACTION_ID)
    service.resume.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("attempts", [5, 11])
async def test_an_ambiguous_action_below_the_bound_still_reconciles(attempts) -> None:
    row = action(ActionState.RECONCILING, attempts=attempts)
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.reconcile.return_value = SimpleNamespace(status=PublicStatus.UNKNOWN, detail_code="reconciliation_no_match")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth, max_attempts=5)

    await coordinator.advance(ACTION_ID)

    service.reconcile.assert_awaited_once_with(ACTION_ID)
    service.exhaust.assert_not_called()


# -- Generalized (non-TenantCloud) coordinator behavior --------------------
#
# The same class, now parameterized on `operations`, is the generic Restate
# delivery coordinator (OutboundDeliveryCoordinator alias). These tests use
# calendar.update, which is never a TenantCloud operation, to prove the
# generalization and the context-reload-wait fix grounded in real prod data
# (action 497fcaf8, 2026-09-28: a calendar.update reached manual_review with
# detail_code persisted_context_unavailable only 6 seconds after being
# created, at attempt_count 2).


def _generic_action(*, attempts: int = 2, created_at=None):
    return SimpleNamespace(
        action_id=ACTION_ID,
        wakeup_event_id=27000,
        operation=Operation.CALENDAR_UPDATE,
        state=ActionState.RETRY_READY,
        attempt_count=attempts,
        next_attempt_at=datetime(2026, 9, 28, 18, 41, tzinfo=timezone.utc),
        created_at=created_at if created_at is not None else datetime(2026, 9, 28, 18, 41, 26, tzinfo=timezone.utc),
        arguments={"calendar_id": "cal_123"},
    )


def test_a_non_tenantcloud_operation_is_rejected_by_the_tenantcloud_only_instance() -> None:
    coordinator = OutboundDeliveryCoordinator(store=AsyncMock(), service=AsyncMock(), auth=AsyncMock())
    assert Operation.CALENDAR_UPDATE not in coordinator._operations  # noqa: SLF001


@pytest.mark.asyncio
async def test_an_operations_set_generalizes_which_operations_the_coordinator_advances() -> None:
    row = _generic_action()
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(
        status=PublicStatus.SENT, detail_code="sent", detail=None
    )
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({Operation.CALENDAR_UPDATE}),
        clock=lambda: row.created_at + timedelta(seconds=5),
    )

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.COMPLETE
    service.resume.assert_awaited_once_with(ACTION_ID)


@pytest.mark.asyncio
async def test_a_context_reload_manual_review_waits_instead_of_parking_inside_the_ceiling() -> None:
    now = datetime(2026, 9, 28, 18, 41, 32, tzinfo=timezone.utc)
    row = _generic_action(created_at=datetime(2026, 9, 28, 18, 41, 26, tzinfo=timezone.utc))
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(
        status=PublicStatus.MANUAL_REVIEW,
        detail_code="persisted_context_unavailable",
        detail="persisted_context_unavailable",
    )
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({Operation.CALENDAR_UPDATE}),
        clock=lambda: now,
    )

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.WAIT
    assert result.detail_code == "persisted_context_unavailable"
    assert result.retry_after_seconds == 300


@pytest.mark.asyncio
async def test_a_context_reload_manual_review_past_the_ceiling_is_terminal() -> None:
    """Past retry_policy's one-hour ceiling, advance() routes straight to
    service.exhaust() instead of one more resume() -- the same
    definitive_failed-with-warning outcome the ceiling exists for (see
    retry_policy.py's decide()), not a further provider attempt."""
    now = datetime(2026, 9, 28, 19, 45, 0, tzinfo=timezone.utc)  # > 1h after created_at
    row = _generic_action(created_at=datetime(2026, 9, 28, 18, 41, 26, tzinfo=timezone.utc))
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.exhaust.return_value = SimpleNamespace(
        status=PublicStatus.MANUAL_REVIEW,
        detail_code="persisted_context_unavailable",
        detail="persisted_context_unavailable",
    )
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({Operation.CALENDAR_UPDATE}),
        clock=lambda: now,
    )

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.TERMINAL
    assert result.detail_code == "persisted_context_unavailable"
    service.exhaust.assert_awaited_once_with(ACTION_ID)
    service.resume.assert_not_called()


# -- StaffWarningPort wiring -------------------------------------------------


@pytest.mark.asyncio
async def test_a_definitive_failure_warns_staff_exactly_once() -> None:
    row = action(ActionState.RETRY_READY, attempts=1)
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(status=PublicStatus.REJECTED, detail_code="provider_rejected_request")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    staff_warning = AsyncMock()
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth, staff_warning=staff_warning)

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.TERMINAL
    staff_warning.warn_once.assert_awaited_once_with(
        ACTION_ID,
        None,
        "provider_rejected_request",
        wakeup_event_id=27000,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        recipient="lead@example.com",
    )


@pytest.mark.asyncio
async def test_a_stale_no_send_never_warns_staff() -> None:
    """STALE is the freshness preflight deliberately declining to send -- not
    a failure -- and must never page staff."""
    row = action(ActionState.RETRY_READY, attempts=1)
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(status=PublicStatus.STALE, detail_code="stale_context_unasked")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    staff_warning = AsyncMock()
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth, staff_warning=staff_warning)

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.TERMINAL
    staff_warning.warn_once.assert_not_called()


@pytest.mark.asyncio
async def test_default_staff_warning_port_is_the_inert_noop() -> None:
    """A coordinator built without staff_warning= still works -- it just logs
    instead of posting to Cliq (NoopStaffWarningPort, retry_policy.py)."""
    row = action(ActionState.RETRY_READY, attempts=1)
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(status=PublicStatus.REJECTED, detail_code="provider_rejected_request")
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    coordinator = OutboundDeliveryCoordinator(store=store, service=service, auth=auth)

    result = await coordinator.advance(ACTION_ID)

    assert result.phase is DeliveryPhase.TERMINAL
