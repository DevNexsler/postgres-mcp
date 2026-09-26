"""The calendar dependency: the one thing the gateway checks before a send
besides the stale-context question (stale_context.py).

A showing confirmation, reschedule or cancellation waits for this wake's
calendar mutation. Nothing here judges freshness, recipient or context: the
saved action record (Comm-Data-Store migration 206) already fixes who receives
what, and newer messages are the agent's question, never a verdict.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .context import ActionContext
from .models import ActionRole
from .models import IntentKind


class PreflightOutcome(StrEnum):
    READY = "ready"
    DEPENDENCY_WAIT = "dependency_wait"
    MANUAL_REVIEW = "manual_review"


class CalendarDependencyState(StrEnum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class PreflightEvidence:
    calendar_dependency: CalendarDependencyState


@dataclass(frozen=True)
class PreflightDecision:
    outcome: PreflightOutcome
    detail_code: str


_CALENDAR_DEPENDENT_REPLIES = frozenset(
    {
        IntentKind.SHOWING_CONFIRMATION,
        IntentKind.SHOWING_RESCHEDULE,
        IntentKind.SHOWING_CANCELLATION,
    }
)

_READY = PreflightDecision(PreflightOutcome.READY, "ready")


class SafetyPreflight:
    @staticmethod
    def evaluate(context: ActionContext, evidence: PreflightEvidence) -> PreflightDecision:
        if context.action_role is not ActionRole.PROSPECT_REPLY or context.intent_kind not in _CALENDAR_DEPENDENT_REPLIES:
            return _READY
        if evidence.calendar_dependency is CalendarDependencyState.FAILED:
            return PreflightDecision(PreflightOutcome.MANUAL_REVIEW, "calendar_dependency_failed")
        if evidence.calendar_dependency is not CalendarDependencyState.COMPLETED:
            return PreflightDecision(PreflightOutcome.DEPENDENCY_WAIT, "calendar_dependency_pending")
        return _READY
