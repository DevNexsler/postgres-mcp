"""Durable provider-neutral outbound action orchestration."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import fields as dataclass_fields
from dataclasses import replace as dataclass_replace
from datetime import datetime
from types import MappingProxyType
from typing import Any
from typing import Mapping
from typing import Protocol
from uuid import UUID

from .adapters.base import ProviderAdapter
from .adapters.base import ProviderDisposition
from .adapters.base import ProviderObservation
from .context import ActionContext
from .context import ActionContextLoader
from .context import ContextDerivationError
from .context import DerivedTarget
from .context import canonical_payload_hash
from .metrics import TENANTCLOUD_AUTH_WAIT_CEILING_SECONDS
from .metrics import CircuitStatus
from .metrics import bounded_backoff_seconds
from .metrics import tenantcloud_auth_wait_seconds
from .models import ActionState
from .models import CompletionKind
from .models import ConfirmRequest
from .models import ExecuteRequest
from .models import Operation
from .models import PublicResult
from .models import PublicStatus
from .models import StaleContextDecision
from .preflight import PreflightDecision
from .preflight import PreflightEvidence
from .preflight import PreflightOutcome
from .preflight import SafetyPreflight
from .record import ActionStore as ActionStore
from .record import OutboundActionRecord as OutboundActionRecord
from .record import action_result
from .record import is_due
from .record import require_action
from .recovery import ActionRecovery
from .retry_policy import elapsed_step_backoff_seconds
from .stale_context import ExecuteAnswer
from .stale_context import StaleContextQuestions
from .stale_context import execute_answer
from .tenantcloud_shared import TENANTCLOUD_OPERATIONS
from .traffic_control import VALID_TRAFFIC_MODES

logger = logging.getLogger(__name__)

_RELOAD_REASON_MAX_CHARS = 300

# _await_dispatch_pending's recheck cadence: short enough that a job which
# finishes in the first second or two (the common case) is caught quickly,
# small enough relative to _response_budget_seconds (capped at 29s) to leave
# several rechecks inside the window.
_DISPATCH_POLL_INTERVAL_SECONDS = 2.0


def _bounded_reload_reason(error: Exception) -> str:
    """A ContextDerivationError's own message, one line, capped so it never
    dominates a log line or an attempt's persisted observation. Every raise
    site (context.py) is a static, developer-authored sentence -- no
    provider payload or credential ever reaches this string -- but this
    stays defensive about length and newlines regardless."""
    text = " ".join(str(error).split())
    return text[:_RELOAD_REASON_MAX_CHARS] if text else type(error).__name__

# States execute() reports as-is without re-driving them.
_EXECUTE_TERMINAL_STATES = frozenset(
    {
        ActionState.STALE,
        ActionState.REJECTED,
        ActionState.DEFINITIVE_FAILED,
        ActionState.DEAD_LETTER,
        ActionState.MANUAL_REVIEW,
        ActionState.UNKNOWN,
        ActionState.RECONCILING,
        ActionState.DISPATCHING,
        ActionState.PROVIDER_ACCEPTED,
    }
)
# enqueue() additionally leaves every Restate-owned in-progress state alone.
_ENQUEUE_TERMINAL_STATES = _EXECUTE_TERMINAL_STATES | {
    ActionState.PREPARED,
    ActionState.RETRY_READY,
    ActionState.DEPENDENCY_WAIT,
}


# tenantcloud.message.send fails this same way whether thread_id was never a
# thread (a lead id, most often) or is a thread that no longer exists: a bare
# provider_rejected/target_unavailable detail_code gives the agent no way to
# tell that apart from an outage, so it either gives up with nothing sent or
# (target_unavailable_before_dispatch is retryable) burns the whole retry
# budget re-asking a provider that will never say yes (wake 26156,
# 2026-08-30: five identical rejections then retry_budget_exhausted, with no
# indication anywhere in the result that the thread id was the problem).
_TENANTCLOUD_NO_SUCH_THREAD_DETAILS = frozenset({"tenantcloud_target_unavailable_before_dispatch", "tenantcloud_provider_rejected_http_404"})


# A retryable TenantCloud rejection proven pre-dispatch (nothing was ever
# written -- adapters/tenantcloud.py's _from_execution auth branch and
# _from_reconciliation's authentication_unavailable branch, both provably
# before any write is attempted): waited out instead of burning the ordinary
# 5-attempt budget (8 of 9 retry_budget_exhausted TenantCloud sends in 60
# days were 6x this rejection inside ~90-150s, well within a single auth
# outage that can run up to ~45 minutes). error_category survives every
# schedule_next_attempt() reschedule (that call never touches it); detail_code
# does not (each reschedule overwrites it), so error_category is the durable
# signal across repeated wait cycles.
_TENANTCLOUD_AUTH_WAIT_DETAIL = "tenantcloud_auth_rejected_before_dispatch"
_TENANTCLOUD_AUTH_WAIT_CATEGORY = "provider_authentication"


def _tenantcloud_auth_wait_pending(action: OutboundActionRecord, now: datetime) -> bool:
    """True only for a TenantCloud row parked retry_ready by a provably
    pre-dispatch auth rejection, still inside the 2h ceiling. Any action that
    actually dispatched lands in dispatching/provider_accepted/unknown/
    reconciling instead of retry_ready, or (if retryable) carries a
    different category -- so this can never fire for a send that reached the
    provider."""
    if action.operation not in TENANTCLOUD_OPERATIONS or action.state is not ActionState.RETRY_READY:
        return False
    if action.detail_code != _TENANTCLOUD_AUTH_WAIT_DETAIL and action.error_category != _TENANTCLOUD_AUTH_WAIT_CATEGORY:
        return False
    elapsed = (now - action.created_at).total_seconds() if action.created_at is not None else 0.0
    return elapsed < TENANTCLOUD_AUTH_WAIT_CEILING_SECONDS


def _tenantcloud_auth_wait_delay(action: OutboundActionRecord, now: datetime) -> int:
    elapsed = (now - action.created_at).total_seconds() if action.created_at is not None else 0.0
    return tenantcloud_auth_wait_seconds(elapsed)


def _tenantcloud_no_such_thread_detail(context: ActionContext, observation: ProviderObservation) -> str | None:
    if context.operation is not Operation.TENANTCLOUD_MESSAGE_SEND:
        return None
    if observation.detail_code not in _TENANTCLOUD_NO_SUCH_THREAD_DETAILS:
        return None
    return (
        f"TenantCloud has no messenger thread {context.target.target_id} (a lead id is not a "
        "thread id). If this lead has no thread, reply with email.send to the lead's email instead."
    )


def _provider_message_detail(observation: ProviderObservation) -> str | None:
    """The provider's own rejection text (initial_observation's
    evidence["provider_message"], e.g. AES's "channel not found or ... not a
    member" for a permanent_upstream_error job), surfaced as the agent-facing
    `detail` on a definitive outcome. Without this the agent -- and a
    Restate-flagged operation's one staff warning -- saw only a bare detail
    code (`provider_permanent_upstream_error`) and had to go dig up why."""
    evidence = observation.evidence
    if not isinstance(evidence, Mapping):
        return None
    message = evidence.get("provider_message")
    return message.strip() if isinstance(message, str) and message.strip() else None


class PreflightEvidenceLoader(Protocol):
    async def load(self, context: ActionContext) -> PreflightEvidence: ...


class CircuitGuard(Protocol):
    async def circuit_status(self, operation: Operation) -> CircuitStatus: ...


class ClosedCircuitGuard:
    async def circuit_status(self, operation: Operation) -> CircuitStatus:
        del operation
        return CircuitStatus(is_open=False, retry_after_seconds=0, failure_count=0)


Clock = Callable[[], datetime]
Sleeper = Callable[[float], Awaitable[None]]


class OutboundActionService:
    """State machine coordinator. Contains no provider-specific branches."""

    def __init__(
        self,
        *,
        store: ActionStore,
        context_loader: ActionContextLoader,
        evidence_loader: PreflightEvidenceLoader,
        adapters: Mapping[Operation, ProviderAdapter],
        provider_client: Any,
        clock: Clock,
        lease_owner: str,
        response_budget_seconds: float = 25,
        lease_seconds: int = 60,
        sleep: Sleeper = asyncio.sleep,
        circuit_guard: CircuitGuard | None = None,
        retry_base_seconds: int = 5,
        retry_max_seconds: int = 900,
        traffic_mode: str = "shadow",
        traffic_probe: Any | None = None,
        stale_confirm_enabled: bool = False,
        restate_operations: frozenset[Operation] = frozenset(),
    ):
        if traffic_mode not in VALID_TRAFFIC_MODES:
            raise ValueError(f"traffic_mode must be one of {sorted(VALID_TRAFFIC_MODES)}, got {traffic_mode!r}")
        if traffic_mode != "off" and traffic_probe is None:
            # Not a hard failure -- off/shadow/enforce is a legitimate
            # operational rollout switch and a missing probe must not crash
            # the gateway -- but silently behaving like "off" is exactly the
            # kind of wiring bug (env var set, probe forgotten in
            # build_runtime()) that should be loud, not invisible.
            logger.warning(
                "traffic_mode=%r configured with no traffic_probe -- the gate will never run and this will silently behave like traffic_mode='off'",
                traffic_mode,
            )
        self._store = store
        self._context_loader = context_loader
        self._evidence_loader = evidence_loader
        self._adapters = dict(adapters)
        self._provider_client = provider_client
        self._clock = clock
        self._lease_owner = lease_owner
        self._response_budget_seconds = max(0, min(response_budget_seconds, 29))
        self._lease_seconds = lease_seconds
        self._sleep = sleep
        self._circuit_guard = circuit_guard or ClosedCircuitGuard()
        self._retry_base_seconds = max(1, retry_base_seconds)
        self._retry_max_seconds = max(self._retry_base_seconds, retry_max_seconds)
        # Every operation OutboundDeliveryCoordinator advances -- see
        # ActionRecovery's own restate_operations docstring below. _schedule()
        # reads this too: a Restate-routed reschedule must never key its
        # backoff on attempt_count (retry_policy.elapsed_step_backoff_seconds'
        # docstring).
        self._restate_operations = restate_operations
        self._traffic_mode = traffic_mode
        # The newer-context query (stale_context.NewerContextProbe).
        self._traffic_probe = traffic_probe
        self._stale = StaleContextQuestions(
            store=store,
            context_loader=context_loader,
            probe=traffic_probe,
            actor=lease_owner,
            enabled=stale_confirm_enabled,
            mode=traffic_mode,
            drive_answered=self._drive_answered,
            lease_seconds=lease_seconds,
        )
        self._recovery = ActionRecovery(
            store=store,
            provider_client=provider_client,
            adapter_for=self._adapter,
            verified_context=self._verified_context,
            finish_observation=self._finish_observation,
            schedule=self._schedule,
            clock=clock,
            actor=lease_owner,
            lease_seconds=lease_seconds,
            restate_operations=restate_operations,
        )

    @property
    def _stale_confirm_enabled(self) -> bool:
        """OUTBOUND_STALE_CONFIRM_ENABLED; owned by the stale-context questions."""
        return self._stale.enabled

    @_stale_confirm_enabled.setter
    def _stale_confirm_enabled(self, enabled: bool) -> None:
        self._stale.enabled = enabled

    async def execute(self, request: ExecuteRequest) -> PublicResult:
        return await self._execute(request, dispatch=True)

    async def enqueue(self, request: ExecuteRequest) -> PublicResult:
        """Persist and preflight one action without provider I/O.

        Restate owns every later advance. Repeated calls return the same CDS
        action, so ingress loss never loses work and never creates a new send.
        """
        return await self._execute(request, dispatch=False)

    async def _execute(self, request: ExecuteRequest, *, dispatch: bool) -> PublicResult:
        context = await self._context_loader.load(request)
        enabled = self._stale.enabled
        action = None
        existing = None
        if enabled or (dispatch and context.prospect_id.startswith("subject:")):
            existing = await self._store.get(context.action_id)
        if enabled and existing is not None:
            answered = await self._stale.after_execute(existing, request, dispatch=dispatch)
            if answered is not None:
                return answered
        if dispatch and existing is not None and context.prospect_id.startswith("subject:"):
            if self._matches_durable_subject_alias_promotion(existing, context):
                action = existing
        if action is None:
            action = await self._store.create_or_load(context)
        # A wake may hold several actions per role (CDS migration 204); the one
        # the database returned is this request's, whatever its ordinal.
        context = self._context_for(action, context)
        if action.state is ActionState.COMPLETED:
            return action_result(action, repeated=True)
        if not self._is_due(action):
            return action_result(action)
        answer = execute_answer(action, request, enabled=enabled)
        if answer is ExecuteAnswer.REASK:
            return await self._stale.reask(action, context)
        if answer is ExecuteAnswer.YES:
            return await self._stale.answer_and_drive(
                action, StaleContextDecision.YES, None, wakeup_event_id=request.wakeup_event_id, dispatch=dispatch
            )
        if action.state in (_EXECUTE_TERMINAL_STATES if dispatch else _ENQUEUE_TERMINAL_STATES):
            return action_result(action)
        # override=true is the historical spelling of "yes": the question is
        # still recorded (blocked row + successor), then answered yes.
        return await self._drive(action, context, agent_facing=True, confirm_stale=request.override, dispatch=dispatch)

    async def confirm(self, request: ConfirmRequest, *, dispatch: bool = True) -> PublicResult:
        """Answer a needs_confirmation (stale_context) result: see
        StaleContextQuestions.confirm."""
        return await self._stale.confirm(request, dispatch=dispatch)

    async def _drive(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        *,
        agent_facing: bool = False,
        confirm_stale: bool = False,
        dispatch: bool = True,
    ) -> PublicResult:
        """The send gate every action passes: the stale-context question
        (asked only of the agent), then the calendar dependency and dispatch
        (or, with dispatch=False, prepare for Restate without provider I/O)."""
        asked = await self._stale.check(action, context, agent_facing=agent_facing, confirm=confirm_stale, dispatch=dispatch)
        if asked is not None:
            return asked
        if not dispatch:
            return await self._preflight_without_dispatch(action, context)

        async def _preflight_fallback() -> PublicResult:
            return await self._preflight(action, context)

        return await self._dispatch_stage(action, context, otherwise=_preflight_fallback)

    async def _drive_answered(self, successor: OutboundActionRecord, *, dispatch: bool) -> PublicResult:
        """A yes/revise successor executes its own saved record, like any
        action, through the same gate as the agent's execute."""
        if successor.state is ActionState.COMPLETED:
            return action_result(successor, repeated=True)
        if successor.state is not ActionState.RECEIVED or not self._is_due(successor):
            return action_result(successor)
        context, context_detail, reason = await self._verified_context(successor)
        if context is None:
            return action_result(successor, detail=reason or context_detail)
        return await self._drive(successor, context, agent_facing=True, dispatch=dispatch)

    async def prepare(self, action_id: UUID) -> PublicResult:
        """Prepare a persisted remediation successor without provider I/O."""
        action = await self._require_action(action_id)
        if action.state is not ActionState.RECEIVED or not self._is_due(action):
            return action_result(action)
        context, context_detail, reason = await self._verified_context(action)
        if context is None:
            return action_result(action, detail=reason or context_detail)
        return await self._drive(action, context, dispatch=False)

    async def _preflight_without_dispatch(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        decision = SafetyPreflight.evaluate(context, await self._evidence_loader.load(context))
        if decision.outcome is PreflightOutcome.READY:
            prepared = await self._store.prepare(context, action.state)
            return action_result(prepared, repeated=prepared.state is ActionState.COMPLETED)
        return await self._apply_preflight_decision(action, decision)

    async def _dispatch_stage(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        *,
        otherwise: Callable[[], Awaitable[PublicResult]],
    ) -> PublicResult:
        """Routes to _resume_dependency/_dispatch for the states both
        execute() and resume() share, falling back to `otherwise()` for
        anything else -- execute()'s catch-all is _preflight() (a fresh
        RECEIVED action); resume()'s is just returning the row's current
        result unchanged (worker.py only ever calls resume() for
        DEPENDENCY_WAIT/PREPARED/RETRY_READY, but resume() is a public
        method with no such guarantee from other callers, so its historical
        "return the row as-is" fallback for any other state must not
        silently become _preflight()). All three routes -- including
        `otherwise` -- can reach _dispatch()'s adapter.invoke()/
        adapter.poll() provider I/O, so all three are covered by the same
        try/except below.

        An exception here never escapes to the MCP caller as a raised error:
        FastMCP wraps it as "Error executing tool outbound_action: ...", which
        the CDS reconciler reads as proof nothing was sent.

        - Once adapter.invoke() has started (_ProviderCallAttemptedError) the
          provider may have the request: log at ERROR and return the row's
          durable state (dispatching -> lease expiry -> reconcile).
        - Before it, nothing was sent: log at ERROR and tell the caller so.
          The row stays where it was left; executing the same request again
          re-runs the send from there. Nothing is retried on the agent's
          behalf -- it decides.

        Context load/validation (everything in execute() before this call)
        still raises.
        """
        try:
            if action.state is ActionState.DEPENDENCY_WAIT:
                return await self._resume_dependency(action, context)
            if action.state in {ActionState.PREPARED, ActionState.RETRY_READY}:
                return await self._dispatch(action, context)
            return await otherwise()
        except _ProviderCallAttemptedError as attempted:
            logger.error(
                "post-dispatch exception on wake %s action %s -- provider call outcome "
                "unknown, returning durable row state for lease-expiry/reconcile recovery",
                context.wakeup_event_id,
                action.action_id,
                exc_info=attempted.__cause__,
            )
            return action_result(await self._require_action(action.action_id))
        except Exception as error:
            logger.error(
                "pre-send exception on wake %s action %s -- nothing was sent",
                context.wakeup_event_id,
                action.action_id,
                exc_info=True,
            )
            lines = str(error).strip().splitlines()
            reason = f"{type(error).__name__}: {lines[0][:200]}" if lines else type(error).__name__
            return PublicResult(
                status=PublicStatus.FAILED,
                action_id=action.action_id,
                action_uid=action.action_uid,
                provider_request_ref=None,
                detail_code="gateway_internal_error",
                detail=(
                    f"Not sent: the gateway hit an internal error before sending ({reason}). Nothing went out. "
                    "Execute the same request again to retry, or record needs_human."
                ),
            )

    async def status(self, action_id: UUID) -> PublicResult:
        return action_result(await self._require_action(action_id))

    async def action_operation(self, action_id: UUID) -> Operation | None:
        action = await self._store.get(action_id)
        return action.operation if action is not None else None

    async def suggest_targets(self, wakeup_event_id: int) -> dict[str, str]:
        return await self._context_loader.suggest_targets(wakeup_event_id)

    async def resume(self, action_id: UUID) -> PublicResult:
        """The worker (or Restate) advancing a saved action. Nobody can be
        asked here, and no answer means no send: with unshown newer context
        the action ends as a `stale_context_unasked` no-send; with nothing
        newer the saved record is sent. The in-flight lease still applies."""
        action = await self._require_action(action_id)
        if not self._is_due(action):
            return action_result(action)
        context, context_detail, reason = await self._verified_context(action)
        if context is None:
            if action.operation in self._restate_operations:
                # Restate (delivery_workflow.OutboundDeliveryCoordinator) owns
                # every retry/backoff/ceiling decision for this operation --
                # it only ever calls resume() while its own elapsed-time
                # ceiling has NOT yet passed (retry_policy.RETRY_CEILING_SECONDS),
                # switching to exhaust() once it has. Parking straight to
                # manual_review() here -- unconditionally, with no wait --
                # used to end an ambiguous, still-possibly-in-flight action
                # (actions ddc5a0d8/497fcaf8, 2026-09-28, calendar.update: a
                # transient reload failure parked the row in manual_review a
                # few hundred milliseconds after prepare(), and the identical
                # reload succeeded minutes later) irrevocably, WELL inside
                # the ceiling the coordinator's own wait logic
                # (delivery_workflow.py's `_CONTEXT_WAIT_DETAILS` check)
                # exists to honor -- by the time that check ran, the row was
                # already committed to a terminal state and the wait
                # decision no longer mattered. Retry it the same way
                # reconcile() already does for the exact same failure class
                # (wake 27321): spend one attempt and let the next cycle
                # reload again. Once the ceiling passes, the coordinator
                # calls exhaust() instead of resume(), and plan_exhaust()
                # already ends this in `definitive_failed` with exactly one
                # staff warning -- never a silent dead end.
                return await self._recovery.reload_retry(action, context_detail, reason)
            return await self._recovery.manual_review(action, context_detail, reason=reason)
        unasked = await self._stale.check(action, context, agent_facing=False)
        if unasked is not None:
            return unasked

        async def _unchanged_fallback() -> PublicResult:
            return action_result(action)

        return await self._dispatch_stage(action, context, otherwise=_unchanged_fallback)

    async def reconcile(self, action_id: UUID) -> PublicResult:
        return await self._recovery.reconcile(action_id)

    async def exhaust(self, action_id: UUID) -> PublicResult:
        """Close exhausted work without another provider invocation."""
        return await self._recovery.exhaust(action_id)

    async def _preflight(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        """The calendar dependency, then dispatch (execute and confirm)."""
        decision = SafetyPreflight.evaluate(context, await self._evidence_loader.load(context))
        if decision.outcome is PreflightOutcome.READY:
            prepared = await self._store.prepare(context, action.state)
            if prepared.state is ActionState.COMPLETED:
                return action_result(prepared, repeated=True)
            if prepared.state is ActionState.DEPENDENCY_WAIT:
                return action_result(prepared)
            return await self._dispatch(prepared, context)
        return await self._apply_preflight_decision(action, decision)

    async def _resume_dependency(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        action = await self._store.claim(
            action.action_id,
            action.state,
            self._lease_owner,
            self._lease_seconds,
        )
        decision = SafetyPreflight.evaluate(context, await self._evidence_loader.load(context))
        if decision.outcome is PreflightOutcome.READY:
            prepared = await self._store.prepare(context, action.state)
            if prepared.state is ActionState.COMPLETED:
                return action_result(prepared, repeated=True)
            if prepared.state is ActionState.DEPENDENCY_WAIT:
                return action_result(prepared)
            return await self._dispatch(prepared, context)
        if decision.outcome is PreflightOutcome.DEPENDENCY_WAIT:
            scheduled = await self._schedule(action, decision.detail_code)
            return action_result(scheduled)
        return await self._apply_preflight_decision(action, decision, lease_owner=self._lease_owner)

    async def _apply_preflight_decision(
        self,
        action: OutboundActionRecord,
        decision: PreflightDecision,
        *,
        lease_owner: str | None = None,
    ) -> PublicResult:
        """A failed calendar dependency: a waiting row is parked (dead_letter);
        anything else waits for the dependency (and parks on its next turn)."""
        if decision.outcome is PreflightOutcome.MANUAL_REVIEW and action.state is ActionState.DEPENDENCY_WAIT:
            transitioned = await self._store.transition(
                action.action_id,
                action.state,
                ActionState.DEAD_LETTER,
                lease_owner,
                ProviderObservation(ProviderDisposition.AMBIGUOUS, decision.detail_code),
            )
            return action_result(transitioned)
        transitioned = await self._store.transition(
            action.action_id,
            action.state,
            ActionState.DEPENDENCY_WAIT,
            lease_owner,
            ProviderObservation(ProviderDisposition.PENDING, decision.detail_code),
        )
        return action_result(await self._schedule(transitioned, decision.detail_code))

    async def _dispatch(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        adapter = self._adapter(context.operation)
        circuit = await self._circuit_guard.circuit_status(context.operation)
        if circuit.is_open:
            scheduled = await self._store.schedule_next_attempt(
                action.action_id,
                action.state,
                max(1, circuit.retry_after_seconds),
                "provider_circuit_open",
            )
            return action_result(scheduled)
        now = self._clock()
        if _tenantcloud_auth_wait_pending(action, now):
            # Same shape as the circuit-open branch above: reschedule without
            # claim(), so this wait spends no attempt. Once the 2h ceiling
            # passes, _tenantcloud_auth_wait_pending stops matching and this
            # row falls straight through to the ordinary claim()/dispatch
            # below -- today's behaviour.
            scheduled = await self._store.schedule_next_attempt(
                action.action_id,
                action.state,
                _tenantcloud_auth_wait_delay(action, now),
                "tenantcloud_auth_wait",
            )
            return action_result(scheduled)
        claimed = await self._store.claim(action.action_id, action.state, self._lease_owner, self._lease_seconds)
        dispatching = await self._store.transition(
            claimed.action_id,
            claimed.state,
            ActionState.DISPATCHING,
            self._lease_owner,
            ProviderObservation(ProviderDisposition.PENDING, "dispatch_started"),
        )
        try:
            if dispatching.action_uid is None:
                raise RuntimeError("prepared action has no deterministic action UID")
            provider_request = adapter.build_request(context, dispatching.action_uid)
        except Exception:
            # The provider was provably not called: back to retry_ready, which
            # a re-execute dispatches (a dispatching row it only reports).
            await self._store.transition(
                dispatching.action_id,
                dispatching.state,
                ActionState.RETRY_READY,
                self._lease_owner,
                ProviderObservation(ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE, "gateway_internal_error", retryable=True),
            )
            raise
        try:
            return await self._invoke(dispatching, context, adapter, provider_request)
        except Exception as error:
            raise _ProviderCallAttemptedError() from error

    async def _invoke(
        self,
        dispatching: OutboundActionRecord,
        context: ActionContext,
        adapter: ProviderAdapter,
        provider_request: Any,
    ) -> PublicResult:
        """The provider call and everything after it: an uncertain outcome."""
        observation = await adapter.invoke(self._provider_client, provider_request)
        if observation.provider_request_ref:
            dispatching = await self._store.record_provider_request(
                dispatching.action_id,
                self._lease_owner,
                observation,
            )
        if observation.disposition is ProviderDisposition.PENDING:
            observation = await adapter.poll(self._provider_client, observation)
            if observation.provider_request_ref and observation.provider_request_ref != dispatching.provider_request_ref:
                dispatching = await self._store.record_provider_request(
                    dispatching.action_id,
                    self._lease_owner,
                    observation,
                )
            if observation.disposition is ProviderDisposition.PENDING:
                observation, dispatching = await self._await_dispatch_pending(dispatching, adapter, observation)
            if observation.disposition is ProviderDisposition.PENDING:
                observation = ProviderObservation(
                    ProviderDisposition.AMBIGUOUS,
                    "provider_queue_timeout",
                    provider_request_ref=observation.provider_request_ref,
                    provider_call_id=observation.provider_call_id,
                )
        return await self._finish_observation(dispatching, context, adapter, observation)

    async def _await_dispatch_pending(
        self,
        dispatching: OutboundActionRecord,
        adapter: ProviderAdapter,
        observation: ProviderObservation,
    ) -> tuple[ProviderObservation, OutboundActionRecord]:
        """A queued job's first recheck still PENDING is not yet a failure:
        re-poll on a short interval up to the response budget before
        `_invoke` calls it `provider_queue_timeout` (wake 27321: the
        gateway's own timestamps showed `provider_queue_timeout` fired
        ~0.12s after `provider_request_recorded`, while the agent-email
        job's write landed ~0.42s later -- one immediate recheck gave up
        before a queued-but-healthy job had any real chance to answer).
        Stays inside `_response_budget_seconds` (constructor-capped at 29s)
        so the MCP tool call itself never times out silently; never
        re-dispatches, only re-polls the same job."""
        budget = self._response_budget_seconds
        waited = 0.0
        while observation.disposition is ProviderDisposition.PENDING and waited < budget:
            interval = min(_DISPATCH_POLL_INTERVAL_SECONDS, budget - waited)
            if interval <= 0:
                break
            await self._sleep(interval)
            waited += interval
            observation = await adapter.poll(self._provider_client, observation)
            if observation.provider_request_ref and observation.provider_request_ref != dispatching.provider_request_ref:
                dispatching = await self._store.record_provider_request(
                    dispatching.action_id,
                    self._lease_owner,
                    observation,
                )
        return observation, dispatching

    async def _finish_observation(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        adapter: ProviderAdapter,
        observation: ProviderObservation,
    ) -> PublicResult:
        expected_state = action.state
        thread_detail = _tenantcloud_no_such_thread_detail(context, observation) or _provider_message_detail(observation)
        if observation.disposition is ProviderDisposition.ACCEPTED:
            receipt = adapter.parse_receipt(context, observation)
            if receipt is None:
                observation = ProviderObservation(
                    ProviderDisposition.AMBIGUOUS,
                    "provider_receipt_missing",
                    provider_request_ref=observation.provider_request_ref,
                )
            else:
                if expected_state is ActionState.DISPATCHING or (
                    expected_state is ActionState.RECONCILING and context.operation in TENANTCLOUD_OPERATIONS
                ):
                    accepted = await self._store.transition(
                        action.action_id,
                        expected_state,
                        ActionState.PROVIDER_ACCEPTED,
                        self._lease_owner,
                        observation,
                    )
                else:
                    accepted = action
                completed = await self._store.complete(
                    accepted.action_id,
                    accepted.state,
                    self._lease_owner,
                    receipt,
                    CompletionKind.SENT,
                    "provider_receipt_verified",
                )
                return action_result(completed)
        if observation.disposition is ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE:
            if observation.retryable:
                retry = await self._store.transition(
                    action.action_id,
                    expected_state,
                    ActionState.RETRY_READY,
                    self._lease_owner,
                    observation,
                )
                return action_result(await self._schedule(retry, observation.detail_code), detail=thread_detail)
            failed = await self._store.definitive_fail(
                action.action_id,
                expected_state,
                self._lease_owner,
                observation,
            )
            return action_result(failed, detail=thread_detail)
        unknown = await self._store.transition(
            action.action_id,
            expected_state,
            ActionState.UNKNOWN,
            self._lease_owner,
            observation,
        )
        return action_result(await self._schedule(unknown, observation.detail_code))

    async def _schedule(
        self,
        action: OutboundActionRecord,
        detail_code: str,
    ) -> OutboundActionRecord:
        if action.operation in self._restate_operations:
            # attempt_count is the legacy worker's own counter (bumped by
            # claim_outbound_action, Comm-Data-Store) -- a Restate-routed
            # reschedule does not reliably take a fresh claim between every
            # schedule() call, so keying backoff on it here risks a flat,
            # never-growing wait. See elapsed_step_backoff_seconds' docstring
            # for the live symptom this fixed: 360 TenantCloud status-update
            # reinvokes in one hour (one every ~10s, never doubling) instead
            # of the ~15-20 a real 5s-to-300s-capped schedule reaches by the
            # one-hour retry ceiling.
            elapsed = max(0.0, (self._clock() - action.created_at).total_seconds()) if action.created_at is not None else 0.0
            wait_seconds = elapsed_step_backoff_seconds(elapsed)
        else:
            wait_seconds = bounded_backoff_seconds(
                action.attempt_count,
                base_seconds=self._retry_base_seconds,
                max_seconds=self._retry_max_seconds,
            )
        return await self._store.schedule_next_attempt(
            action.action_id,
            action.state,
            wait_seconds,
            detail_code,
        )

    def _adapter(self, operation: Operation) -> ProviderAdapter:
        adapter = self._adapters.get(operation)
        if adapter is None:
            raise ValueError(f"no outbound provider adapter configured for {operation.value}")
        return adapter

    async def _require_action(self, action_id: UUID) -> OutboundActionRecord:
        return await require_action(self._store, action_id)

    @staticmethod
    def _context_for(action: OutboundActionRecord, context: ActionContext) -> ActionContext:
        """The loaded context names the wake role's first action (ordinal 0).
        A retry, or a later action of the same role, is a different row with
        the same wake-derived context, so carry that row's own identity."""
        if context.action_id == action.action_id:
            return context
        return dataclass_replace(
            context,
            action_id=action.action_id,
            lock_holder=f"outbound-gateway:{action.action_id}",
        )

    async def _verified_context(
        self,
        action: OutboundActionRecord,
    ) -> tuple[ActionContext | None, str, str | None]:
        """The context the worker executes an existing action with: the saved
        record of what was asked and derived at execute time (Comm-Data-Store
        migration 206 makes it immutable), never a fresh derivation compared
        against it. Live data moves -- a sender's name flips, a message is
        re-threaded -- and comparing against it parked real sends (wake
        27244). The wake is re-read only for facts the record does not carry
        (the message's source and send time, the property label, aliases),
        none of which decides who receives what.

        A failed reload never discards its own reason (wake 27321: a
        transient ContextDerivationError parked an in-flight action on
        `persisted_context_unavailable` with no hint why, and the identical
        reload succeeded minutes later): the third element is that reload's
        own bounded message, logged here and handed back for a caller to
        record on the attempt (recovery.py) or show the agent (`detail`).
        None on any success."""
        try:
            live = await self._context_loader.load(action.execute_request())
        except ContextDerivationError as error:
            reason = _bounded_reload_reason(error)
            logger.warning(
                "context reload failed for action %s (wake %s): %s",
                action.action_id,
                action.wakeup_event_id,
                reason,
            )
            return None, "persisted_context_unavailable", reason
        live = self._context_for(action, live)
        if not action.payload_hash:
            return live, "context_verified", None
        return _recorded_context(action, live), "context_recorded", None

    @staticmethod
    def _matches_durable_subject_alias_promotion(
        action: OutboundActionRecord,
        context: ActionContext,
    ) -> bool:
        expected_recipient = {
            "kind": context.target.kind,
            "target_id": context.target.target_id,
            "verified": context.target.verified,
        }
        if (
            context.action_id != action.action_id
            or context.provider_account != action.provider_account
            or context.routing_policy_version != action.routing_policy_version
            or expected_recipient != dict(action.recipient_scope)
        ):
            return False
        stored_context = dict(action.canonical_context)
        current_context = dict(context.canonical_context)
        stored_prospect = stored_context.get("prospect_id")
        current_prospect = current_context.get("prospect_id")
        if not (
            isinstance(stored_prospect, str)
            and stored_prospect.startswith("prospect:")
            and isinstance(current_prospect, str)
            and current_prospect.startswith("subject:")
        ):
            return False
        normalized_context = {**current_context, "prospect_id": stored_prospect}
        if normalized_context != stored_context:
            return False
        stored_scope = dict(action.canonical_scope)
        current_scope = dict(context.canonical_scope)
        if "prospect_id" in current_scope:
            current_scope["prospect_id"] = stored_prospect
        if current_scope != stored_scope:
            return False
        normalized_hash = canonical_payload_hash(
            {
                "action_role": context.action_role.value,
                "operation": context.operation.value,
                "intent_kind": context.intent_kind,
                "appointment_slot": context.appointment_slot,
                "arguments": context.arguments,
                "canonical_context": normalized_context,
            }
        )
        return normalized_hash == action.payload_hash

    def _is_due(self, action: OutboundActionRecord) -> bool:
        return is_due(action, self._clock())


class _ProviderCallAttemptedError(Exception):
    """Raised from once adapter.invoke() started (the cause is __cause__)."""


_RECORDED_ID_LISTS = frozenset({"cross_channel_duplicate_message_ids", "certified_older_message_ids"})
_ACTION_CONTEXT_FIELDS = frozenset(field.name for field in dataclass_fields(ActionContext))
_RECORD_COLUMN_FIELDS = frozenset({
    "target", "intent_kind", "appointment_slot", "provider_account",
    "routing_policy_version", "canonical_context", "canonical_scope", "payload_hash",
})


def _recorded_context(action: OutboundActionRecord, live: ActionContext) -> ActionContext:
    """`live` with every decision replaced by the action's saved record: the
    request (intent, slot), who receives it, from which account, and the
    context the gateway derived when the agent asked."""
    recorded = dict(action.canonical_context)
    overlay: dict[str, Any] = {}
    for key, value in recorded.items():
        # target and the columns below come from their own record columns.
        if key in _RECORD_COLUMN_FIELDS or key not in _ACTION_CONTEXT_FIELDS:
            continue
        if key in _RECORDED_ID_LISTS:
            value = tuple(value or ())
        elif isinstance(value, dict):
            value = MappingProxyType(dict(value))
        overlay[key] = value
    scope = dict(action.recipient_scope)
    target = (
        DerivedTarget(str(scope["kind"]), str(scope["target_id"]), bool(scope.get("verified")))
        if scope.get("kind") and scope.get("target_id")
        else live.target
    )
    intent = action.intent_kind
    return dataclass_replace(
        live,
        **overlay,
        target=target,
        intent_kind=str(getattr(intent, "value", intent)),
        appointment_slot=action.appointment_slot,
        provider_account=action.provider_account,
        routing_policy_version=action.routing_policy_version,
        canonical_context=MappingProxyType(recorded),
        canonical_scope=MappingProxyType(dict(action.canonical_scope)),
        payload_hash=action.payload_hash,
    )
