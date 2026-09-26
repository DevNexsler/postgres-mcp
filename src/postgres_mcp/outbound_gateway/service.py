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
from .adapters.base import ProviderReceipt
from .context import ActionContext
from .context import ActionContextLoader
from .context import ContextDerivationError
from .context import DerivedTarget
from .context import canonical_payload_hash
from .metrics import CircuitStatus
from .metrics import bounded_backoff_seconds
from .models import ActionState
from .models import CompletionKind
from .models import ConfirmRequest
from .models import ExecuteRequest
from .models import Operation
from .models import PublicResult
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
from .stale_context import ExecuteAnswer
from .stale_context import StaleContextQuestions
from .stale_context import asks_on_block
from .stale_context import execute_answer
from .tenantcloud_shared import TENANTCLOUD_OPERATIONS
from .traffic_control import VALID_TRAFFIC_MODES
from .traffic_control import TrafficProbe
from .traffic_control import TrafficVerdict
from .traffic_control import check_traffic

logger = logging.getLogger(__name__)

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
        traffic_probe: TrafficProbe | None = None,
        stale_confirm_enabled: bool = False,
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
        self._traffic_mode = traffic_mode
        self._traffic_probe = traffic_probe
        self._stale = StaleContextQuestions(
            store=store,
            context_loader=context_loader,
            traffic_probe=traffic_probe,
            actor=lease_owner,
            enabled=stale_confirm_enabled,
            drive_answered=self._drive_answered,
            terminal_block=self._terminal_traffic_block,
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
        if (
            dispatch
            and action.state is ActionState.DEFINITIVE_FAILED
            and action.error_category == "traffic_blocked"
            and request.override
        ):
            remediated = await self._remediate_traffic_block(action, context)
            if remediated is not None:
                action, context = remediated
            else:
                return action_result(
                    action,
                    detail=(
                        "override cannot resend this action yet: it is blocked by traffic "
                        "control and has no evidence-resolved operator remediation on file. "
                        "Escalate for manual review before retrying."
                    ),
                )
        elif action.state in (_EXECUTE_TERMINAL_STATES if dispatch else _ENQUEUE_TERMINAL_STATES):
            return action_result(action)
        # With confirmation enabled an agent's override never bypasses
        # staleness silently: it is the "yes" of a stale_context question,
        # recorded as one (blocked row + successor). Disabled, it is the
        # pre-192 bypass, unchanged.
        return await self._drive(
            action,
            context,
            agent_facing=True,
            confirm_stale=enabled and request.override,
            override=not enabled and request.override,
            dispatch=dispatch,
        )

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
        override: bool = False,
        dispatch: bool = True,
    ) -> PublicResult:
        """The send gate every new or answered action passes: traffic control,
        then preflight, then dispatch (or, with dispatch=False, preflight and
        prepare for Restate without provider I/O)."""
        blocked = await self._check_traffic(
            action,
            context,
            agent_facing=agent_facing,
            confirm_stale=confirm_stale,
            override=override,
            dispatch=dispatch,
        )
        if blocked is not None:
            return blocked
        if not dispatch:
            return await self._preflight_without_dispatch(action, context, agent_facing=agent_facing)

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
        context, context_detail = await self._verified_context(successor)
        if context is None:
            return action_result(successor, detail=context_detail)
        return await self._drive(successor, context, agent_facing=True, dispatch=dispatch)

    async def prepare(self, action_id: UUID) -> PublicResult:
        """Preflight a persisted remediation successor without provider I/O."""
        action = await self._require_action(action_id)
        if action.state is not ActionState.RECEIVED or not self._is_due(action):
            return action_result(action)
        context, context_detail = await self._verified_context(action)
        if context is None:
            return action_result(action, detail=context_detail)
        return await self._drive(action, context, dispatch=False)

    async def _preflight_without_dispatch(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        *,
        agent_facing: bool = False,
    ) -> PublicResult:
        evidence = await self._evidence_loader.load(context)
        evidence, unshown = await self._stale.waive_shown_inbound(context, evidence)
        decision = SafetyPreflight.evaluate(context, evidence, now=self._clock())
        if decision.outcome is PreflightOutcome.READY:
            prepared = await self._store.prepare(context, action.state)
            return action_result(prepared, repeated=prepared.state is ActionState.COMPLETED)
        asked = await self._stale.ask_about_unshown_inbound(
            action, context, decision, unshown, agent_facing=agent_facing, dispatch=False
        )
        if asked is not None:
            return asked
        return await self._apply_preflight_decision(action, evidence, decision)

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

        A post-dispatch exception (e.g. a network timeout *after* the
        provider already accepted the HTTP request -- the row is durably
        DISPATCHING with a lease by the time adapter.invoke() runs, since
        claim()+transition() to DISPATCHING happen before it) must never
        escape to the MCP caller as a raised error: FastMCP wraps any
        uncaught exception as "Error executing tool outbound_action: ...",
        and the CDS reconciler's rejection-prefix rule treats that wrapper
        as proof nothing was sent. If a real send's post-accept timeout
        propagated that far, the reconciler would uncount a REAL send and
        let the wake complete while the message was actually delivered --
        exactly the false negative the prefix rule is only safe without.

        So: catch broadly here, log at ERROR (wake + action id, for
        operator visibility), and return whatever the row's durable state
        already is -- DISPATCHING/RECONCILING with a lease, recovered by the
        existing lease-expiry/reconcile/worker machinery, same as any other
        expired-lease crash recovery. This restores the invariant that an
        MCP error wrapper strictly implies "rejected before any provider
        interaction": context load/validation (in execute(), everything
        before this call) is deliberately NOT covered by this except clause
        and still raises, so a true pre-dispatch rejection keeps the error
        wrapper the reconciler depends on.
        """
        try:
            if action.state is ActionState.DEPENDENCY_WAIT:
                return await self._resume_dependency(action, context)
            if action.state in {ActionState.PREPARED, ActionState.RETRY_READY}:
                return await self._dispatch(action, context)
            return await otherwise()
        except Exception:
            logger.error(
                "post-dispatch exception on wake %s action %s -- provider call outcome "
                "unknown, returning durable row state for lease-expiry/reconcile recovery",
                context.wakeup_event_id,
                action.action_id,
                exc_info=True,
            )
            return action_result(await self._require_action(action.action_id))

    async def _remediate_traffic_block(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
    ) -> tuple[OutboundActionRecord, ActionContext] | None:
        """override=true resend of a traffic-blocked DEFINITIVE_FAILED action.

        Reuses the same successor-action machinery operator remediation uses
        (create_outbound_remediation_context / retry_of_action_id / next
        effect_ordinal -- Comm-Data-Store migrations/067_outbound_action_gateway.sql:1200-1267,
        granted to the gateway's runtime role in migrations/120_outbound_action_terminal_wake_boundary.sql:550-551)
        rather than inventing a new one. That function's own precondition --
        an evidence-resolved `outbound_action_resolutions` row for this
        action (067:1221-1228) -- is written by resolve_outbound_action_from_evidence,
        which IS granted to the gateway's runtime role (migrations/
        079_runtime_tenantcloud_privilege_boundary.sql:427,459-475), so this
        is not a privilege wall. The real blocker is a lifecycle/evidence
        mismatch: resolve_outbound_action_from_evidence only accepts a row
        already in 'manual_review' (067:1160-1163) and requires real
        provider-side non-acceptance evidence (a 64-hex hash, a non-empty
        reference, evidence_kind='authoritative_non_acceptance', 067:1164-1173)
        -- neither of which a traffic-control block has: it goes straight to
        'definitive_failed' from a live/pending state, never through
        'manual_review', and there is no provider disposition to attest to,
        only an internal recipient-safety policy decision. So an override
        resend can only succeed once an operator has independently routed
        this action through manual_review and evidence-resolved it; until
        then this returns None and the caller stays on the original terminal
        result instead of crashing on the unhandled precondition-violation
        exception. Closing that gap for a fully autonomous, zero-operator
        unblock needs a new Comm-Data-Store migration (e.g. a successor path
        keyed on error_category='traffic_blocked' instead of an evidence
        resolution) -- out of scope for this worktree.

        The successor's context is the SAME `context` already loaded for
        this call (not re-derived via `_verified_context`): the successor
        copies wakeup_event_id/action_role/canonical_scope/canonical_context/
        recipient_scope/provider_account/routing_policy_version/operation/
        intent_kind/appointment_slot/arguments/payload_hash verbatim from the
        parent row (067:1247-1264), so nothing about the wake-derived context
        actually changed -- only `action_id` (next effect_ordinal) did.
        """
        try:
            successor = await self._store.remediate_traffic_block(
                action.action_id,
                operator_identity=self._lease_owner,
                reason="traffic_control_override_resend",
            )
        except Exception:
            logger.warning(
                "traffic control override could not remediate blocked action %s (no evidence-resolved remediation on file yet)",
                action.action_id,
                exc_info=True,
            )
            return None
        return successor, dataclass_replace(context, action_id=successor.action_id)

    async def _check_traffic(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        *,
        agent_facing: bool = False,
        confirm_stale: bool = False,
        override: bool = False,
        dispatch: bool = True,
    ) -> PublicResult | None:
        """Per-recipient traffic gate. Returns a blocking PublicResult when
        enforce mode must stop dispatch; returns None (proceed) otherwise --
        including shadow mode (which only logs) and off mode (no probe call
        at all).

        With stale-context confirmation enabled (OUTBOUND_STALE_CONFIRM_ENABLED)
        and agent_facing -- the agent's own execute/confirm -- a stale_context
        block becomes a needs_confirmation question (a `stale` no-send row plus
        the newer context) instead of a terminal failure, and items already
        shown to this wake's agent are waived by identity. Worker-driven
        resume()/prepare() have nobody to ask and keep the definitive
        traffic_blocked failure. confirm_stale: override=true, the historical
        spelling of "yes" -- the block is still recorded, then answered yes.
        Disabled, everything is the pre-192 contract (override bypasses)."""
        if self._traffic_mode == "off" or self._traffic_probe is None:
            return None
        verdict = await check_traffic(
            self._traffic_probe,
            recipient_key=context.prospect_id,
            channel_id=context.channel_id,
            wakeup_event_id=context.wakeup_event_id,
            action_id=context.action_id,
            override=override,
            logger=logger,
            acknowledged=self._stale.enabled,
        )
        if not verdict.allowed:
            if self._traffic_mode == "enforce":
                if self._stale.enabled and asks_on_block(action, verdict, agent_facing=agent_facing):
                    return await self._stale.block(action, context, verdict, confirm=confirm_stale, dispatch=dispatch)
                return await self._terminal_traffic_block(action, context, verdict)
            logger.warning(
                "traffic control shadow would-block: %s %s wake=%s recipient=%s",
                verdict.reason,
                verdict.detail,
                context.wakeup_event_id,
                context.prospect_id,
            )
        elif verdict.check_failed:
            logger.warning("traffic control fail-open on wake %s", context.wakeup_event_id)
        return None

    async def _terminal_traffic_block(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        verdict: TrafficVerdict,
    ) -> PublicResult:
        claimable = action
        if action.state is ActionState.RECEIVED:
            # claim_outbound_action's live whitelist (Comm-Data-Store
            # migrations/068_outbound_gateway_observability.sql:156-159) excludes
            # 'received' -- a fresh row must first move through
            # prepare_outbound_action_and_acquire_lock (067:524-528 only accepts
            # 'received'/'dependency_wait'), the same call the normal
            # _preflight() READY path uses, before it is claimable at all.
            claimable = await self._store.prepare(context, action.state)
            if claimable.state is ActionState.COMPLETED:
                return action_result(claimable, repeated=True)
        if claimable.state is ActionState.DEPENDENCY_WAIT or verdict.reason == "lease_held":
            # Two independent reasons land here, both deferring
            # instead of terminalizing:
            #
            # 1. claimable.state is DEPENDENCY_WAIT: DB-forced, not a
            #    policy choice. outbound_action_transition_allowed()
            #    has no dependency_wait -> definitive_failed edge
            #    (Comm-Data-Store migrations/067_outbound_action_gateway.sql:346-389)
            #    -- forcing a terminal here would raise the DB's
            #    'invalid outbound definitive failure state'
            #    uncaught. Two ways to land here: a fresh RECEIVED row
            #    whose prepare() above hit a contended intent lock
            #    (067:556-564), or resume() being called on an
            #    already-dependency_wait row.
            #
            # 2. verdict.reason == "lease_held": a policy choice
            #    (Important 6), true regardless of claimable.state. A
            #    lease block is inherently short-lived -- the other
            #    in-flight action will reach a terminal state on its
            #    own. Deterministic action_id + a terminal
            #    DEFINITIVE_FAILED meant a seconds-long lease overlap
            #    would brick that resend forever.
            #
            # Both are still a block (do-not-dispatch-now), and the
            # row stays legally re-drivable: the worker's next
            # resume() re-runs this same gate on its next poll.
            return action_result(claimable, detail_code=verdict.reason, detail=verdict.detail)
        # Worker-driven staleness (resume/prepare: nobody to ask) and a
        # retry_ready row (no retry_ready -> stale edge) keep the terminal
        # traffic_blocked failure, which pages.
        claimed = await self._store.claim(
            claimable.action_id,
            claimable.state,
            self._lease_owner,
            self._lease_seconds,
        )
        failed = await self._store.definitive_fail(
            claimed.action_id,
            claimed.state,
            self._lease_owner,
            ProviderObservation(
                ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
                verdict.reason,
                category="traffic_blocked",
                retryable=False,
                evidence={"detail": verdict.detail},
            ),
        )
        return action_result(failed, detail=verdict.detail)

    async def status(self, action_id: UUID) -> PublicResult:
        return action_result(await self._require_action(action_id))

    async def action_operation(self, action_id: UUID) -> Operation | None:
        action = await self._store.get(action_id)
        return action.operation if action is not None else None

    async def suggest_targets(self, wakeup_event_id: int) -> dict[str, str]:
        return await self._context_loader.suggest_targets(wakeup_event_id)

    async def resume(self, action_id: UUID) -> PublicResult:
        action = await self._require_action(action_id)
        if not self._is_due(action):
            return action_result(action)
        context, context_detail = await self._verified_context(action)
        if context is None:
            return await self._recovery.manual_review(action, context_detail)
        # Worker-driven resume (worker.py's list_work -> resume for
        # dependency_wait/prepared/retry_ready) has no ExecuteRequest and
        # therefore no caller-supplied override -- a long-waited action
        # never gets to skip staleness just because nobody re-asked with
        # override=true. Same off/shadow/enforce semantics as execute().
        blocked = await self._check_traffic(action, context)
        if blocked is not None:
            return blocked

        async def _unchanged_fallback() -> PublicResult:
            return action_result(action)

        return await self._dispatch_stage(action, context, otherwise=_unchanged_fallback)

    async def reconcile(self, action_id: UUID) -> PublicResult:
        return await self._recovery.reconcile(action_id)

    async def exhaust(self, action_id: UUID) -> PublicResult:
        """Close exhausted work without another provider invocation."""
        return await self._recovery.exhaust(action_id)

    async def _preflight(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        """The agent-facing preflight (execute and confirm)."""
        evidence = await self._evidence_loader.load(context)
        evidence, unshown = await self._stale.waive_shown_inbound(context, evidence)
        decision = SafetyPreflight.evaluate(context, evidence, now=self._clock())
        if decision.outcome is PreflightOutcome.READY:
            prepared = await self._store.prepare(context, action.state)
            if prepared.state is ActionState.COMPLETED:
                return action_result(prepared, repeated=True)
            if prepared.state is ActionState.DEPENDENCY_WAIT:
                return action_result(prepared)
            return await self._dispatch(prepared, context)
        asked = await self._stale.ask_about_unshown_inbound(action, context, decision, unshown, agent_facing=True, dispatch=True)
        if asked is not None:
            return asked
        return await self._apply_preflight_decision(action, evidence, decision)

    async def _resume_dependency(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        action = await self._store.claim(
            action.action_id,
            action.state,
            self._lease_owner,
            self._lease_seconds,
        )
        evidence = await self._evidence_loader.load(context)
        evidence, _unshown = await self._stale.waive_shown_inbound(context, evidence)
        decision = SafetyPreflight.evaluate(context, evidence, now=self._clock())
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
        return await self._apply_preflight_decision(
            action,
            evidence,
            decision,
            lease_owner=self._lease_owner,
        )

    async def _apply_preflight_decision(
        self,
        action: OutboundActionRecord,
        evidence: PreflightEvidence,
        decision: PreflightDecision,
        *,
        lease_owner: str | None = None,
    ) -> PublicResult:
        if decision.outcome is PreflightOutcome.DUPLICATE:
            assert evidence.verified_outbound_request_ref is not None
            assert evidence.verified_outbound_message_id is not None
            receipt = ProviderReceipt(
                provider_request_ref=evidence.verified_outbound_request_ref,
                provider_message_id=evidence.verified_outbound_request_ref,
                accepted_at=self._clock(),
                evidence={
                    "kind": "verified_existing_outbound",
                    "cds_message_id": evidence.verified_outbound_message_id,
                },
            )
            completed = await self._store.complete(
                action.action_id,
                action.state,
                lease_owner,
                receipt,
                CompletionKind.DUPLICATE,
                decision.detail_code,
            )
            return action_result(completed, repeated=True)
        if decision.outcome in {PreflightOutcome.STALE, PreflightOutcome.REJECTED}:
            target = ActionState.STALE if decision.outcome is PreflightOutcome.STALE else ActionState.REJECTED
            transitioned = await self._store.transition(
                action.action_id,
                action.state,
                target,
                lease_owner,
                ProviderObservation(ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE, decision.detail_code),
            )
            return action_result(transitioned)
        if decision.outcome is PreflightOutcome.MANUAL_REVIEW and action.state is ActionState.DEPENDENCY_WAIT:
            transitioned = await self._store.transition(
                action.action_id,
                action.state,
                ActionState.DEAD_LETTER,
                lease_owner,
                ProviderObservation(
                    ProviderDisposition.AMBIGUOUS,
                    decision.detail_code,
                ),
            )
            return action_result(transitioned)
        dependency_code = decision.detail_code
        transitioned = await self._store.transition(
            action.action_id,
            action.state,
            ActionState.DEPENDENCY_WAIT,
            lease_owner,
            ProviderObservation(ProviderDisposition.PENDING, dependency_code),
        )
        return action_result(await self._schedule(transitioned, dependency_code))

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
        claimed = await self._store.claim(action.action_id, action.state, self._lease_owner, self._lease_seconds)
        dispatching = await self._store.transition(
            claimed.action_id,
            claimed.state,
            ActionState.DISPATCHING,
            self._lease_owner,
            ProviderObservation(ProviderDisposition.PENDING, "dispatch_started"),
        )
        if dispatching.action_uid is None:
            raise RuntimeError("prepared action has no deterministic action UID")
        provider_request = adapter.build_request(context, dispatching.action_uid)
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
                observation = ProviderObservation(
                    ProviderDisposition.AMBIGUOUS,
                    "provider_queue_timeout",
                    provider_request_ref=observation.provider_request_ref,
                    provider_call_id=observation.provider_call_id,
                )
        return await self._finish_observation(dispatching, context, adapter, observation)

    async def _finish_observation(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        adapter: ProviderAdapter,
        observation: ProviderObservation,
    ) -> PublicResult:
        expected_state = action.state
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
                return action_result(await self._schedule(retry, observation.detail_code))
            failed = await self._store.definitive_fail(
                action.action_id,
                expected_state,
                self._lease_owner,
                observation,
            )
            return action_result(failed)
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
        return await self._store.schedule_next_attempt(
            action.action_id,
            action.state,
            bounded_backoff_seconds(
                action.attempt_count,
                base_seconds=self._retry_base_seconds,
                max_seconds=self._retry_max_seconds,
            ),
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
    ) -> tuple[ActionContext | None, str]:
        """The context the worker executes an existing action with: the saved
        record of what was asked and derived at execute time (Comm-Data-Store
        migration 206 makes it immutable), never a fresh derivation compared
        against it. Live data moves -- a sender's name flips, a message is
        re-threaded -- and comparing against it parked real sends (wake
        27244). The wake is re-read only for facts the record does not carry
        (the message's source and send time, the property label, aliases),
        none of which decides who receives what."""
        try:
            live = await self._context_loader.load(action.execute_request())
        except ContextDerivationError:
            return None, "persisted_context_unavailable"
        live = self._context_for(action, live)
        if not action.payload_hash:
            return live, "context_verified"
        return _recorded_context(action, live), "context_recorded"

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
