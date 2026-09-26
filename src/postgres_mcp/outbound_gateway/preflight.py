"""Role-specific stale, dependency, and refresh safety preflight."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from .context import ActionContext
from .models import ActionRole
from .models import IntentKind


class PreflightOutcome(StrEnum):
    READY = "ready"
    DUPLICATE = "duplicate"
    STALE = "stale"
    REJECTED = "rejected"
    DEPENDENCY_WAIT = "dependency_wait"
    MANUAL_REVIEW = "manual_review"


class CalendarDependencyState(StrEnum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class RefreshStatus(StrEnum):
    COVERED = "covered"
    BROWSER_COVERED = "browser_covered"
    TIMEOUT = "timeout"
    FAILED = "failed"


@dataclass(frozen=True)
class RefreshEvidence:
    status: RefreshStatus
    covered_through: datetime | None
    covered_thread_identity: str
    attempt_count: int
    identity_resolved: bool = True
    thread_resolved: bool = True
    property_resolved: bool = True


@dataclass(frozen=True)
class PreflightEvidence:
    current_recipient_id: str
    current_property_id: str | None
    current_appointment_slot: datetime | None
    later_inbound_message_id: int | None
    calendar_dependency: CalendarDependencyState
    calendar_already_applied: bool
    calendar_context_changed: bool
    overlapping_showing_prospect_ids: tuple[str, ...]
    refresh_required_through: datetime
    refresh: RefreshEvidence | None
    # Every inbound newer than the source message (later_inbound_message_id is
    # the max of these). Empty from callers that only know the max: readers
    # treat that as {later_inbound_message_id}.
    later_inbound_message_ids: tuple[int, ...] = ()
    # Outbound sent after the source message to this action's own target
    # (evidence.outbound_target). Information for the agent -- the service
    # shows it through the stale-context question -- never a verdict here.
    later_outbound_message_ids: tuple[int, ...] = ()


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


class SafetyPreflight:
    @staticmethod
    def evaluate(
        context: ActionContext,
        evidence: PreflightEvidence,
        *,
        now: datetime,
    ) -> PreflightDecision:
        # Source freshness remains skill-owned in phase one. Duplicating that
        # policy here stranded valid sends when helper evidence was not part of
        # the narrow action request. Gateway owns recipient/context/duplicate,
        # calendar-dependency, and provider-delivery safety only.
        del now
        if not context.target.verified or evidence.current_recipient_id != context.target.target_id:
            return PreflightDecision(PreflightOutcome.REJECTED, "recipient_mismatch")
        if evidence.current_property_id != context.property_id:
            return PreflightDecision(PreflightOutcome.REJECTED, "context_mismatch")
        if evidence.current_appointment_slot != context.appointment_slot:
            return PreflightDecision(PreflightOutcome.REJECTED, "context_mismatch")

        if context.action_role is ActionRole.PROSPECT_REPLY:
            if evidence.later_inbound_message_id is not None:
                return PreflightDecision(PreflightOutcome.STALE, "newer_inbound")
            # Newer outbound is not decided here. The gateway used to complete
            # the send as duplicate/already_handled over ANY outbound in the
            # source conversation -- wake 27279's email to a prospect over a
            # cron post in the Cliq DM. It is shown to the agent instead.
            if context.intent_kind in _CALENDAR_DEPENDENT_REPLIES:
                if evidence.calendar_dependency is CalendarDependencyState.FAILED:
                    return PreflightDecision(
                        PreflightOutcome.MANUAL_REVIEW,
                        "calendar_dependency_failed",
                    )
                if evidence.calendar_dependency is not CalendarDependencyState.COMPLETED:
                    return PreflightDecision(
                        PreflightOutcome.DEPENDENCY_WAIT,
                        "calendar_dependency_pending",
                    )
        elif context.action_role is ActionRole.CALENDAR_MUTATION:
            if evidence.calendar_context_changed:
                return PreflightDecision(
                    PreflightOutcome.STALE,
                    "calendar_context_changed",
                )
            if evidence.calendar_already_applied:
                return PreflightDecision(
                    PreflightOutcome.DUPLICATE,
                    "calendar_already_applied",
                )

        return PreflightDecision(PreflightOutcome.READY, "ready")
