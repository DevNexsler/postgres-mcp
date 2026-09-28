"""Shared retry policy for the generalized Restate delivery workflow.

One module, reused by every operation routed through the generic
``OutboundDeliveryCoordinator`` (``tenantcloud_delivery.py``) -- TenantCloud
today, and any operation named in ``OUTBOUND_RESTATE_OPERATIONS`` once it is
switched over. It does not decide transient-vs-definitive from raw provider
payloads (each ``adapters/*.py`` already does that, into
``adapters.base.ProviderObservation.disposition`` / ``.retryable`` --
duplicating that here would be the second mechanism the migration brief
warns against); it decides what to do once that classification is known:
how long to wait, when to give up, and when a give-up needs exactly one
staff warning.

Numbers
-------
Backoff between attempts: 5s, 10s, 20s, ... doubling, capped at 300s
(5 minutes) per step. Total time-to-give-up is bounded at
``RETRY_CEILING_SECONDS`` (one hour) measured from the action's own
``created_at`` -- not from attempt_count -- so a slow provider that answers
quickly most of the time but occasionally takes minutes does not get a
smaller effective budget than one that answers instantly every time.

Context-reload waits (``CONTEXT_RELOAD_WAIT_DETAILS``) are a separate,
flatter wait (300s) rather than the exponential schedule: today's
TenantCloud-only special case (``tenantcloud_delivery.py``'s
``_CONTEXT_WAIT_DETAILS``) only guards the RECEIVED -> prepare() step. Real
prod data (action 497fcaf8, calendar.update, 2026-09-28) shows the same
detail code reached from resume()'s ``_verified_context() is None`` branch
instead, which has no wait at all today and goes straight to
``manual_review`` after a single ambiguous reload -- see
``should_wait_for_context_reload`` and its caller in
``tenantcloud_delivery.py``'s ``advance()``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from .adapters.base import ProviderDisposition
from .adapters.base import ProviderObservation

# Exponential backoff between ordinary retry attempts: base 5s, doubling,
# capped at 5 minutes per step. Matches metrics.bounded_backoff_seconds'
# shape (this module intentionally does not import that one -- it is the
# legacy worker's own tuning knob, kept independent so a change to one
# schedule cannot silently retune the other).
RETRY_BASE_SECONDS = 5
RETRY_STEP_CAP_SECONDS = 300

# Total wall-clock budget for one action, measured from the action's
# created_at: past this, an unresolved outcome stops retrying and becomes
# `definitive_failed` with exactly one staff warning, never a silent
# dead end and never an unbounded retry loop that could outlive a Hermes
# wake session's patience many times over.
RETRY_CEILING_SECONDS = 3600

# A context reload that failed only because it raced a concurrent write, or
# a transient DB hiccup (service.py's _verified_context: "the identical
# reload succeeded minutes later") -- worth a flat wait-and-retry, not an
# immediate park. Not a provider outcome at all: nothing was invoked yet, so
# there is no double-send risk in retrying it.
CONTEXT_RELOAD_WAIT_DETAILS = frozenset({"persisted_context_unavailable", "persisted_context_mismatch"})
CONTEXT_RELOAD_WAIT_SECONDS = 300


def backoff_seconds(attempt_count: int) -> int:
    """5, 10, 20, 40, ... capped at RETRY_STEP_CAP_SECONDS. attempt_count is
    1-based (the count already recorded, i.e. how many attempts have been
    made); attempt_count <= 0 is treated as 1."""
    exponent = max(0, min(int(attempt_count) - 1, 20))
    return min(RETRY_STEP_CAP_SECONDS, RETRY_BASE_SECONDS * int(math.pow(2, exponent)))


def ceiling_exceeded(elapsed_seconds: float, *, ceiling_seconds: int = RETRY_CEILING_SECONDS) -> bool:
    return elapsed_seconds >= ceiling_seconds


def should_wait_for_context_reload(
    detail_code: str,
    elapsed_seconds: float,
    *,
    ceiling_seconds: int = RETRY_CEILING_SECONDS,
) -> bool:
    """True when a manual_review/definitive outcome carrying this detail code
    should instead be treated as a transient wait -- only while the action is
    still inside its overall retry ceiling. Past the ceiling a reload that
    still will not settle is exactly the case the ceiling exists for."""
    return detail_code in CONTEXT_RELOAD_WAIT_DETAILS and not ceiling_exceeded(elapsed_seconds, ceiling_seconds=ceiling_seconds)


def is_definitive_rejection(observation: ProviderObservation) -> bool:
    """The one place this policy reads "was this a definitive rejection?" --
    reusing each adapter's own classification (``adapters.base.initial_observation``:
    a synchronous tool-level refusal, e.g. Cliq's ``provider_rejected_request``
    for a malformed target -- PR #59, 2026-09-28) rather than re-deriving it
    from raw provider payloads a second time. ``retryable`` is read too: an
    adapter marking a DEFINITIVE_NON_ACCEPTANCE observation retryable (none
    do today) would mean "definitive, but worth one more look" and must not
    stop the workflow outright."""
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
    attempt_count: int,
    elapsed_seconds: float,
    detail_code: str,
    is_definitive: bool,
    ceiling_seconds: int = RETRY_CEILING_SECONDS,
) -> RetryDecision:
    """The one place total-budget-vs-keep-waiting is decided, for both a
    genuine provider disposition (``is_definitive`` from
    ``adapters.base.ProviderObservation``) and a context-reload wait.

    A definitive provider rejection (a validation/4xx-shaped refusal the
    adapter already classified as not retryable) stops immediately,
    regardless of the ceiling -- retrying a request the provider has
    already definitively refused wastes the budget and, for a provider
    without a safe re-invoke story, risks a double-send for nothing.

    Anything else keeps retrying until the one-hour ceiling, then stops and
    asks for exactly one staff warning: the definitive_failed-with-warning
    outcome the brief calls for instead of a silent dead end or an
    unbounded loop a wake session cannot wait out.
    """
    if is_definitive:
        return RetryDecision(RetryOutcome.STOP_DEFINITIVE, warn_staff=True)
    if should_wait_for_context_reload(detail_code, elapsed_seconds, ceiling_seconds=ceiling_seconds):
        return RetryDecision(RetryOutcome.RETRY, wait_seconds=CONTEXT_RELOAD_WAIT_SECONDS)
    if ceiling_exceeded(elapsed_seconds, ceiling_seconds=ceiling_seconds):
        return RetryDecision(RetryOutcome.STOP_DEFINITIVE, warn_staff=True)
    return RetryDecision(RetryOutcome.RETRY, wait_seconds=backoff_seconds(attempt_count))


def decide_for_observation(
    *,
    attempt_count: int,
    elapsed_seconds: float,
    observation: ProviderObservation,
    ceiling_seconds: int = RETRY_CEILING_SECONDS,
) -> RetryDecision:
    """``decide()`` for a caller that already has the adapter's
    ``ProviderObservation`` in hand (e.g. after ``adapter.invoke()`` /
    ``adapter.reconcile()``) -- reads the definitive-vs-transient call off it
    via ``is_definitive_rejection`` instead of asking the caller to compute
    that bool itself, so every caller reuses the exact same adapter-level
    signal (Cliq's ``provider_rejected_request``, PR #59; TenantCloud's own
    facade classification; etc.) with no second copy of the judgment."""
    return decide(
        attempt_count=attempt_count,
        elapsed_seconds=elapsed_seconds,
        detail_code=observation.detail_code,
        is_definitive=is_definitive_rejection(observation),
        ceiling_seconds=ceiling_seconds,
    )


class StaffWarningPort(Protocol):
    """Exactly-one-warning seam for a definitively-failed action. A real
    implementation posts through the existing internal-notification path
    (``action_role=internal_notification``, ``intent_kind=manual_review_alert``,
    ``cliq.channel.post`` -- see context.py:908 and server.py's
    OUTBOUND_CLIQ_TARGETS_JSON "manual_review_alert" entry) keyed so a
    replay of the same action_id can never send a second warning.

    NOT wired to a live sender in this change -- see the worklog note in the
    PR description. ``warn_once`` must be idempotent on ``action_id`` alone:
    callers may call it more than once for the same action (a Restate
    workflow replay, or a worker.py re-poll of the same exhausted row).
    """

    async def warn_once(self, action_id: UUID, action_uid: UUID | None, reason: str) -> None: ...


class NoopStaffWarningPort:
    """Default: no staff-notification wiring configured. Definitive-failure
    outcomes are still recorded correctly in the ledger; only the Cliq
    warning is skipped. Logs so an operator can tell the seam is unwired
    rather than silently doing nothing forever."""

    def __init__(self) -> None:
        self._warned: set[UUID] = set()

    async def warn_once(self, action_id: UUID, action_uid: UUID | None, reason: str) -> None:
        if action_id in self._warned:
            return
        self._warned.add(action_id)
        import logging

        logging.getLogger(__name__).warning(
            "outbound action %s definitively failed (%s) but no StaffWarningPort is configured; "
            "no Cliq warning was sent",
            action_id,
            reason,
        )
