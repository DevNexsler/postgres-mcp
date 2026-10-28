"""Retry decisions for the Restate delivery workflow.

Every number here comes from ``retry_config.RetryPolicy`` -- the gateway's
one retry configuration (see that module for the settings and their
defaults). This module only decides what to do with an outcome the adapter
has already classified (``adapters.base.ProviderObservation``): keep waiting
and for how long, or stop and owe a person one escalation.

Context-reload failures (``CONTEXT_RELOAD_WAIT_DETAILS``) are a gateway
hiccup, not a provider outcome -- nothing was invoked, so retrying cannot
double-send. They wait on the same backoff schedule while the action is
still inside its deadline (action 497fcaf8, calendar.update, 2026-09-28: a
transient reload failure parked the row in manual_review a few hundred
milliseconds after prepare(), and the identical reload succeeded minutes
later).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from typing import Protocol
from uuid import UUID

from .adapters.base import ProviderDisposition
from .adapters.base import ProviderObservation
from .models import Operation
from .retry_config import RetryPolicy
from .retry_config import delivery_give_up_at

logger = logging.getLogger(__name__)

CONTEXT_RELOAD_WAIT_DETAILS = frozenset({"persisted_context_unavailable", "persisted_context_mismatch"})

# The detail code of an action ended because it reached its staleness
# ceiling: never sent, because it would have been too late to send.
STALENESS_CEILING_DETAIL = "staleness_ceiling_exceeded"


class DeliveryDeadlineStore(Protocol):
    async def set_delivery_deadline(self, action_id: UUID, deadline_at: datetime) -> Any: ...


async def stamp_delivery_deadline(
    store: DeliveryDeadlineStore,
    action: Any,
    policy: RetryPolicy,
    now: datetime,
    *,
    next_check_seconds: float,
    staleness_only: bool = False,
) -> None:
    """Write this action's give-up time (``retry_config.delivery_give_up_at``)
    onto its row whenever a retry is scheduled. The stamp only tells the wake
    reconciler how long the gateway is still working the row; it never
    decides delivery, so a failure to write it is logged and the retry goes
    ahead (the row keeps its previous stamp, or the reconciler's legacy
    one-hour reading)."""
    deadline_at = delivery_give_up_at(action, policy, now, next_check_seconds=next_check_seconds, staleness_only=staleness_only)
    if deadline_at is None or deadline_at == getattr(action, "delivery_deadline_at", None):
        return
    try:
        await store.set_delivery_deadline(action.action_id, deadline_at)
    except Exception:
        logger.warning(
            "could not stamp delivery deadline %s on outbound action %s; the retry goes ahead",
            deadline_at.isoformat(),
            getattr(action, "action_id", None),
            exc_info=True,
        )


def should_wait_for_context_reload(detail_code: str, deliverable_elapsed_seconds: float, policy: RetryPolicy) -> bool:
    """True when a manual_review outcome carrying this detail code should be
    waited out instead -- only while the action is inside its deadline. Past
    it, a reload that still will not settle is what the deadline is for."""
    return detail_code in CONTEXT_RELOAD_WAIT_DETAILS and not policy.deadline_exceeded(deliverable_elapsed_seconds)


def is_definitive_rejection(observation: ProviderObservation) -> bool:
    """The one place this policy reads "was this a definitive rejection?" --
    each adapter's own classification (``adapters.base.initial_observation``:
    e.g. Cliq's ``provider_rejected_request`` for a malformed target, PR #59),
    never re-derived from raw provider payloads. A DEFINITIVE_NON_ACCEPTANCE
    the adapter marks retryable means "definitive, but worth one more look"
    and does not stop the workflow."""
    return observation.disposition is ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE and not observation.retryable


class RetryOutcome(StrEnum):
    """What the coordinator should do with an advance() step's result."""

    RETRY = "retry"
    STOP_DEFINITIVE = "stop_definitive"


@dataclass(frozen=True)
class RetryDecision:
    outcome: RetryOutcome
    wait_seconds: int = 0
    warn_staff: bool = False


def decide(
    *,
    policy: RetryPolicy,
    deliverable_elapsed_seconds: float,
    is_definitive: bool,
    jitter_key: str = "",
) -> RetryDecision:
    """Keep waiting, or stop. A definitive provider rejection stops at once
    (retrying a refused request wastes the budget and, for a provider without
    a safe re-invoke story, risks a double-send). Anything else retries on
    the policy's backoff until the deadline, then stops and owes one
    escalation if the policy escalates."""
    if is_definitive:
        return RetryDecision(RetryOutcome.STOP_DEFINITIVE, warn_staff=policy.escalate)
    if policy.deadline_exceeded(deliverable_elapsed_seconds):
        return RetryDecision(RetryOutcome.STOP_DEFINITIVE, warn_staff=policy.escalate)
    return RetryDecision(
        RetryOutcome.RETRY,
        wait_seconds=policy.backoff_seconds(deliverable_elapsed_seconds, jitter_key=jitter_key),
    )


def decide_for_observation(
    *,
    policy: RetryPolicy,
    deliverable_elapsed_seconds: float,
    observation: ProviderObservation,
    jitter_key: str = "",
) -> RetryDecision:
    """``decide()`` for a caller holding the adapter's observation."""
    return decide(
        policy=policy,
        deliverable_elapsed_seconds=deliverable_elapsed_seconds,
        is_definitive=is_definitive_rejection(observation),
        jitter_key=jitter_key,
    )


class StaffWarningPort(Protocol):
    """Exactly-one-warning seam for a definitively-failed action. A real
    implementation (``staff_warning.CliqStaffWarningPort``) posts through the
    existing internal-notification path (``action_role=internal_notification``,
    ``intent_kind=manual_review_alert``, ``cliq.channel.post`` -- see
    context.py:908 and server.py's OUTBOUND_CLIQ_TARGETS_JSON
    "manual_review_alert" entry), keyed so a replay of the same action_id can
    never send a second warning, and never recurses (see that module's
    docstring for why).

    ``warn_once`` must be idempotent on ``action_id`` alone: callers may call
    it more than once for the same action (a Restate workflow replay, or a
    worker.py re-poll of the same exhausted row). ``wakeup_event_id``,
    ``operation`` and ``recipient`` are the context the warning text names,
    per the migration brief -- the failed action's own wake, operation, and
    best-effort recipient, not the coordinator's.
    """

    async def warn_once(
        self,
        action_id: UUID,
        action_uid: UUID | None,
        reason: str,
        *,
        wakeup_event_id: int,
        operation: Operation,
        recipient: str,
    ) -> None: ...


class NoopStaffWarningPort:
    """Default: no staff-notification wiring configured. Definitive-failure
    outcomes are still recorded correctly in the ledger; only the Cliq
    warning is skipped. Logs so an operator can tell the seam is unwired
    rather than silently doing nothing forever."""

    def __init__(self) -> None:
        self._warned: set[UUID] = set()

    async def warn_once(
        self,
        action_id: UUID,
        action_uid: UUID | None,
        reason: str,
        *,
        wakeup_event_id: int,
        operation: Operation,
        recipient: str,
    ) -> None:
        if action_id in self._warned:
            return
        self._warned.add(action_id)
        import logging

        logging.getLogger(__name__).warning(
            "outbound action %s (wake %s, operation %s, recipient %s) definitively failed (%s) but no "
            "StaffWarningPort is configured; no Cliq warning was sent",
            action_id,
            wakeup_event_id,
            operation,
            recipient,
            reason,
        )
