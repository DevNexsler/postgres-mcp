"""What the gateway checks before a send besides the stale-context question
(stale_context.py): the calendar dependency, and whether this person was
already answered (recently_answered).

A showing confirmation, reschedule or cancellation waits for this wake's
calendar mutation. A customer reply to someone another wake already answered
-- or is answering right now -- inside Comm-Data-Store's
contact_recently_answered_window() is refused once, naming that reply, so the
agent sends nothing or overrides with a reason. Nothing here judges freshness
or context: the saved action record (Comm-Data-Store migration 206) already
fixes who receives what, and newer messages are the agent's question, never a
verdict.

Who "this person" is (contact keys) and what "answered" means are
Comm-Data-Store's (migration 269, docs/contact-identity-functions.md). This
module owns only the gateway's policy on top: which new actions are asked
about, and the trigger exceptions.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .context import ActionContext
from .models import ActionRole
from .models import IntentKind
from .models import RecentlyAnswered


class PreflightOutcome(StrEnum):
    READY = "ready"
    DEPENDENCY_WAIT = "dependency_wait"
    MANUAL_REVIEW = "manual_review"
    # Not sent, recorded `rejected` with the decision's detail_code: the agent
    # reads why and sends nothing or overrides.
    REFUSED = "refused"


class CalendarDependencyState(StrEnum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class PreflightEvidence:
    calendar_dependency: CalendarDependencyState


# The customer replies a second one is asked about, and that count as the
# first: one definition (PostgresActionStore.recently_answered passes it to SQL).
RECENTLY_ANSWERED_INTENTS = frozenset({IntentKind.INQUIRY_REPLY.value, IntentKind.SHOWING_OFFER.value})
# The refusal's detail_code. Comm-Data-Store's v_outbound_action_outcome reads
# a rejected row with it as `suppressed_duplicate` (migration 269).
RECENTLY_ANSWERED_DETAIL = "recently_answered"
# How much of the earlier reply's text the refusal quotes.
RECENTLY_ANSWERED_PREVIEW_CHARS = 300
# A wake triggered by staff or a schedule: someone asked for this send.
_REQUESTED_SEND_SOURCES = frozenset({"zoho_cliq", "hermes_schedule", "internal_qualification"})


@dataclass(frozen=True)
class RecentlyAnsweredEvidence:
    """Other wakes' customer replies to this action's contact keys inside the
    window, answered or in flight, newest first (PostgresActionStore.
    recently_answered), and what triggered this wake."""

    replies: tuple[RecentlyAnswered, ...]
    # messages.source of this wake's trigger message, if it has one.
    trigger_source: str | None = None
    # The trigger arrived after the newest such reply and is not a word-for-
    # word repeat of the same sender's earlier inbound: a new question.
    trigger_is_new_question: bool = False


@dataclass(frozen=True)
class PreflightDecision:
    outcome: PreflightOutcome
    detail_code: str
    # REFUSED recently_answered: the replies it names.
    recently_answered: tuple[RecentlyAnswered, ...] = ()


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
    def counts_as_answer(action_role: ActionRole, intent_kind: str) -> bool:
        """A customer reply that answers a prospect: its prepare is serialized
        per contact (so a concurrent one sees it in flight), and a new one is
        asked about when another already answered."""
        return action_role is ActionRole.PROSPECT_REPLY and intent_kind in RECENTLY_ANSWERED_INTENTS

    @staticmethod
    def asks_recently_answered(action_role: ActionRole, intent_kind: str, *, is_answer: bool, askable: bool) -> bool:
        """Whether to look for an earlier reply at all. Never for an override
        or stale-context successor (is_answer: it carries the agent's answer),
        and never where nobody can be asked (worker / Restate prepare)."""
        return askable and not is_answer and SafetyPreflight.counts_as_answer(action_role, intent_kind)

    @staticmethod
    def evaluate(
        context: ActionContext,
        evidence: PreflightEvidence,
        answered: RecentlyAnsweredEvidence | None = None,
    ) -> PreflightDecision:
        """`answered` None: not asked (see asks_recently_answered), or the
        lookup failed -- a failed lookup never stops a send."""
        if answered is not None and answered.replies:
            if answered.trigger_source not in _REQUESTED_SEND_SOURCES and not answered.trigger_is_new_question:
                return PreflightDecision(PreflightOutcome.REFUSED, RECENTLY_ANSWERED_DETAIL, answered.replies)
        if context.action_role is not ActionRole.PROSPECT_REPLY or context.intent_kind not in _CALENDAR_DEPENDENT_REPLIES:
            return _READY
        if evidence.calendar_dependency is CalendarDependencyState.FAILED:
            return PreflightDecision(PreflightOutcome.MANUAL_REVIEW, "calendar_dependency_failed")
        if evidence.calendar_dependency is not CalendarDependencyState.COMPLETED:
            return PreflightDecision(PreflightOutcome.DEPENDENCY_WAIT, "calendar_dependency_pending")
        return _READY
