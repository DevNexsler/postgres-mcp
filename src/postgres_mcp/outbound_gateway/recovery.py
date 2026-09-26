"""Worker recovery: settle sends whose outcome is uncertain, close exhausted work.

A send that went to the provider without a confirmed receipt is never sent
again blindly. Recovery settles it from what is already durable (a persisted
acceptance), then from the provider itself (poll the job, or reconcile), and
parks what nobody can settle for a person (dead_letter -> manual_review).

Two layers:

- a pure core: which ledger steps a recovery takes, as a plan
  (`plan_exhaust`, `plan_manual_review`, `plan_recover_acceptance`, ...).
  A plan is an ordered tuple of `Claim` / `Transition` / `DefinitiveFail` /
  `Complete` steps; where the lease must be taken is written into the plan,
  never tracked by hand;
- one executor, `apply_plan`, and `ActionRecovery`, the only I/O: the store,
  the provider (poll / reconcile) and the service's context and
  observation-finishing seams, handed in as callables.

Every transition is compare-and-set in SQL (transition_outbound_action): it
lands the row in the named state or raises, and it keeps the row's own
provider_request_ref (coalesce). A plan built from the row as read is
therefore the exact sequence the step-by-step code took.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import Mapping
from typing import Union
from uuid import UUID

from .adapters.base import ProviderAdapter
from .adapters.base import ProviderDisposition
from .adapters.base import ProviderObservation
from .adapters.base import ProviderReceipt
from .context import ActionContext
from .models import ActionState
from .models import CompletionKind
from .models import Operation
from .models import PublicResult
from .record import ActionStore
from .record import OutboundActionRecord
from .record import action_result
from .record import is_due
from .record import require_action
from .tenantcloud_shared import EVIDENCE_KIND_VERIFIED_READBACK
from .tenantcloud_shared import READBACK_OBSERVATION_KEYS
from .tenantcloud_shared import TENANTCLOUD_OPERATIONS

_HEX64 = re.compile(r"^[0-9a-f]{64}$")

# A send that may already be with the provider: reconcile, never re-dispatch.
IN_FLIGHT_STATES = frozenset({ActionState.DISPATCHING, ActionState.PROVIDER_ACCEPTED, ActionState.RECONCILING})


# --------------------------------------------------------------------------
# Plan vocabulary
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Claim:
    """Take the row's lease (claim_outbound_action); spends one attempt."""

    state: ActionState


@dataclass(frozen=True)
class Transition:
    """Move the row from `from_state` to `to_state`. `leased`: under this
    worker's lease (False: the unleased dead_letter -> manual_review edge)."""

    from_state: ActionState
    to_state: ActionState
    observation: ProviderObservation
    leased: bool = True


@dataclass(frozen=True)
class DefinitiveFail:
    from_state: ActionState
    observation: ProviderObservation


@dataclass(frozen=True)
class Complete:
    from_state: ActionState
    receipt: ProviderReceipt
    kind: CompletionKind
    detail_code: str


Step = Union[Claim, Transition, DefinitiveFail, Complete]  # noqa: UP007 -- a runtime alias
Plan = tuple[Step, ...]


# --------------------------------------------------------------------------
# Pure core
# --------------------------------------------------------------------------


def _ambiguous(detail_code: str, action: OutboundActionRecord) -> ProviderObservation:
    return ProviderObservation(
        ProviderDisposition.AMBIGUOUS,
        detail_code,
        provider_request_ref=action.provider_request_ref,
    )


def verified_tenantcloud_evidence(action: OutboundActionRecord) -> bool:
    """Migration 118's transition_outbound_action already enforced the
    full acceptance guard (evidence_kind literal, six-key observation
    shape, per-key type/format checks, and equality against the
    persisted arguments' desired_state/target_reference/operation)
    atomically, in the same statement that wrote evidence_kind =
    'verified_provider_readback'. So a row bearing that literal is only
    reachable through that guarded write. This re-checks the literal,
    the evidence_hash's own format, and structural completeness of the
    persisted six-key observation -- defense against a corrupted or
    partial read, not a re-derivation of the facade's own opaque hash
    (which, for maintenance create, is computed over a target_reference
    that differs by design from what is persisted here -- see
    tenantcloud_shared.py)."""
    if action.provider_evidence_kind != EVIDENCE_KIND_VERIFIED_READBACK:
        return False
    if not action.provider_evidence_hash or not _HEX64.fullmatch(action.provider_evidence_hash):
        return False
    evidence = dict(action.provider_readback_evidence)
    if set(evidence) != READBACK_OBSERVATION_KEYS:
        return False
    if not isinstance(evidence.get("canonical_observed_state"), Mapping):
        return False
    if evidence.get("readback_verified") is not True:
        return False
    for key in ("operation", "provider_object_id", "target_reference", "readback_timestamp"):
        if not isinstance(evidence.get(key), str) or not evidence[key]:
            return False
    return True


def plan_recover_acceptance(action: OutboundActionRecord) -> Plan | None:
    """Complete a durable provider acceptance without provider I/O. None:
    the row carries no acceptance that may be trusted as proof."""
    if not (
        action.state is ActionState.PROVIDER_ACCEPTED
        and action.provider_request_ref
        and action.provider_message_id
        and action.provider_accepted_at
    ):
        return None
    if action.operation in TENANTCLOUD_OPERATIONS and not verified_tenantcloud_evidence(action):
        # TenantCloud writes are irreversible provider-side actions
        # (lead status, maintenance requests). The generic ref/id/accepted_at
        # heuristic above is not proof enough here: only durable evidence
        # that says "verified readback" AND whose hash matches the
        # persisted canonical state may complete without provider I/O.
        # Anything else -- including a crash between the PROVIDER_ACCEPTED
        # transition and the evidence write -- must go through bounded
        # reconciliation instead of being trusted blindly.
        return None
    receipt = ProviderReceipt(
        provider_request_ref=action.provider_request_ref,
        provider_message_id=action.provider_message_id,
        accepted_at=action.provider_accepted_at,
        evidence={"kind": "persisted_provider_acceptance"},
    )
    return (
        Claim(action.state),
        Complete(ActionState.PROVIDER_ACCEPTED, receipt, CompletionKind.SENT, "persisted_provider_acceptance_recovered"),
    )


def _dead_letter_for_review(
    state: ActionState,
    dead_letter: ProviderObservation,
    review: ProviderObservation,
    *,
    has_lease: bool,
) -> Plan:
    """dead_letter under a lease, then the unleased manual_review edge."""
    return (
        *(() if has_lease else (Claim(state),)),
        Transition(state, ActionState.DEAD_LETTER, dead_letter),
        Transition(ActionState.DEAD_LETTER, ActionState.MANUAL_REVIEW, review, leased=False),
    )


def plan_manual_review(action: OutboundActionRecord, detail_code: str) -> Plan:
    """Park an action nobody can settle (its saved record cannot be executed)."""
    observation = _ambiguous(detail_code, action)
    return _dead_letter_for_review(action.state, observation, observation, has_lease=False)


def plan_expire_dispatch(action: OutboundActionRecord) -> Plan:
    """An in-flight send whose lease expired becomes `unknown`."""
    return (
        Claim(action.state),
        Transition(action.state, ActionState.UNKNOWN, _ambiguous("expired_dispatch_requires_reconciliation", action)),
    )


def plan_start_reconciliation(action: OutboundActionRecord) -> Plan:
    return (
        Claim(action.state),
        Transition(ActionState.UNKNOWN, ActionState.RECONCILING, _ambiguous("reconciliation_started", action)),
    )


def plan_exhaust(action: OutboundActionRecord) -> Plan:
    """Close work whose retry budget is spent, without another provider
    invocation: a durable acceptance completes; anything that may already
    be with the provider is parked for a person; a send that never left
    fails definitively; everything else is left as it is."""
    recovered = plan_recover_acceptance(action)
    if recovered is not None:
        return recovered
    exhausted = _ambiguous("retry_budget_exhausted", action)
    review = _ambiguous("retry_budget_exhausted_manual_review", action)
    state = action.state
    steps: Plan = ()
    if state in {ActionState.DISPATCHING, ActionState.PROVIDER_ACCEPTED}:
        steps = (Claim(state), Transition(state, ActionState.UNKNOWN, exhausted))
        state = ActionState.UNKNOWN
    if state is ActionState.UNKNOWN:
        steps = (
            *steps,
            Claim(ActionState.UNKNOWN),
            Transition(ActionState.UNKNOWN, ActionState.RECONCILING, _ambiguous("retry_budget_exhausted_reconciliation", action)),
        )
        return (*steps, *_dead_letter_for_review(ActionState.RECONCILING, exhausted, review, has_lease=True))
    if state in {ActionState.RECONCILING, ActionState.DEPENDENCY_WAIT}:
        return _dead_letter_for_review(state, exhausted, review, has_lease=False)
    if state in {ActionState.PREPARED, ActionState.RETRY_READY}:
        return (
            Claim(state),
            DefinitiveFail(
                state,
                ProviderObservation(
                    ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
                    "retry_budget_exhausted",
                    provider_request_ref=action.provider_request_ref,
                    category="retry_budget_exhausted",
                    retryable=False,
                    evidence={"kind": "retry_budget"},
                ),
            ),
        )
    return ()


def asks_provider_first(action: OutboundActionRecord) -> bool:
    """A send the provider already has a job for: ask the job before
    judging the context. TenantCloud writes are synchronous (no job), and
    their reconcile is a readback of the record itself."""
    return bool(action.provider_request_ref) and action.action_uid is not None and action.operation not in TENANTCLOUD_OPERATIONS


# --------------------------------------------------------------------------
# I/O
# --------------------------------------------------------------------------


async def apply_plan(
    store: ActionStore,
    action: OutboundActionRecord,
    plan: Plan,
    *,
    actor: str,
    lease_seconds: int,
) -> OutboundActionRecord:
    """Run a plan against one row; returns the row as the last step left it."""
    current = action
    for step in plan:
        if isinstance(step, Claim):
            current = await store.claim(action.action_id, step.state, actor, lease_seconds)
        elif isinstance(step, Transition):
            current = await store.transition(
                action.action_id,
                step.from_state,
                step.to_state,
                actor if step.leased else None,
                step.observation,
            )
        elif isinstance(step, DefinitiveFail):
            current = await store.definitive_fail(action.action_id, step.from_state, actor, step.observation)
        else:
            current = await store.complete(action.action_id, step.from_state, actor, step.receipt, step.kind, step.detail_code)
    return current


VerifiedContext = Callable[[OutboundActionRecord], Awaitable[tuple[ActionContext | None, str]]]
FinishObservation = Callable[[OutboundActionRecord, ActionContext, ProviderAdapter, ProviderObservation], Awaitable[PublicResult]]
Schedule = Callable[[OutboundActionRecord, str], Awaitable[OutboundActionRecord]]
AdapterFor = Callable[[Operation], ProviderAdapter]


class ActionRecovery:
    """reconcile / exhaust / manual review for the worker."""

    def __init__(
        self,
        *,
        store: ActionStore,
        provider_client: Any,
        adapter_for: AdapterFor,
        verified_context: VerifiedContext,
        finish_observation: FinishObservation,
        schedule: Schedule,
        clock: Callable[[], datetime],
        actor: str,
        lease_seconds: int,
    ):
        self._store = store
        self._provider_client = provider_client
        self._adapter_for = adapter_for
        self._verified_context = verified_context
        self._finish_observation = finish_observation
        self._schedule = schedule
        self._clock = clock
        self._actor = actor
        self._lease_seconds = lease_seconds

    async def _apply(self, action: OutboundActionRecord, plan: Plan) -> OutboundActionRecord:
        return await apply_plan(self._store, action, plan, actor=self._actor, lease_seconds=self._lease_seconds)

    async def exhaust(self, action_id: UUID) -> PublicResult:
        """Close exhausted work without another provider invocation."""
        action = await require_action(self._store, action_id)
        return action_result(await self._apply(action, plan_exhaust(action)))

    async def manual_review(self, action: OutboundActionRecord, detail_code: str) -> PublicResult:
        return action_result(await self._apply(action, plan_manual_review(action, detail_code)))

    async def recover_persisted_acceptance(self, action: OutboundActionRecord) -> PublicResult | None:
        plan = plan_recover_acceptance(action)
        if plan is None:
            return None
        return action_result(await self._apply(action, plan))

    async def reconcile(self, action_id: UUID) -> PublicResult:
        action = await require_action(self._store, action_id)
        if not is_due(action, self._clock()):
            return action_result(action)
        recovered = await self.recover_persisted_acceptance(action)
        if recovered is not None:
            return recovered
        if action.state in IN_FLIGHT_STATES:
            action = await self._apply(action, plan_expire_dispatch(action))
        if action.state is not ActionState.UNKNOWN:
            return action_result(action)
        answered = await self._provider_outcome(action)
        if answered is not None:
            return answered
        context, context_detail = await self._verified_context(action)
        if context is None:
            return await self.manual_review(action, context_detail)
        adapter = self._adapter_for(context.operation)
        reconciling = await self._apply(action, plan_start_reconciliation(action))
        if reconciling.action_uid is None:
            raise RuntimeError("reconciling action has no deterministic action UID")
        observation = await adapter.reconcile(
            self._provider_client,
            context,
            reconciling.action_uid,
            ProviderObservation(
                ProviderDisposition.AMBIGUOUS,
                "prior_dispatch_ambiguous",
                provider_request_ref=reconciling.provider_request_ref,
            ),
        )
        return await self._finish_observation(reconciling, context, adapter, observation)

    async def _provider_outcome(self, action: OutboundActionRecord) -> PublicResult | None:
        """Ask the provider about a send it already has before anything else.
        "Did job X finish?" needs only the job id; this send has already been
        made (wake 27244 parked an email for review 4 seconds before its job
        reported "sent"). None means the provider could not say, or the saved
        record cannot be executed: the ordinary reconcile path decides."""
        if not asks_provider_first(action):
            return None
        context, _detail = await self._verified_context(action)
        if context is None:
            return None
        adapter = self._adapter_for(context.operation)
        observation = await adapter.poll(
            self._provider_client,
            ProviderObservation(
                ProviderDisposition.AMBIGUOUS,
                "prior_dispatch_ambiguous",
                provider_request_ref=action.provider_request_ref,
            ),
        )
        if observation.disposition is ProviderDisposition.PENDING:
            # The job is still running: look again later, as the ordinary
            # path does for a pending poll. Claiming spends one attempt, so a
            # job that never finishes still reaches the retry budget.
            claimed = await self._apply(action, (Claim(action.state),))
            return action_result(await self._schedule(claimed, observation.detail_code))
        if observation.disposition not in {ProviderDisposition.ACCEPTED, ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE}:
            return None
        reconciling = await self._apply(action, plan_start_reconciliation(action))
        return await self._finish_observation(reconciling, context, adapter, observation)
