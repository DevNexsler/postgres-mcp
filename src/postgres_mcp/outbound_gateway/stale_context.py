"""The one judgment the gateway makes: "since your context was built, a new
message was received from or sent to this recipient -- still send yours?"

At the agent's own execute (and at a confirm's yes/revise), every message
received from, or sent by us to, this action's recipient after the wake's
context watermark, and not yet shown to this wake's agent, is listed in a
`needs_confirmation` / `stale_context` result. Nothing is sent. The agent
answers `yes` (send it unchanged), `no` (send nothing: the row stays a
deliberate `stale` no-send) or `revise` (same recipient, new content).

Nothing else about freshness is decided anywhere. Where nobody can be asked
-- the worker resuming a saved action, Restate preparing one, confirmation
disabled, a retry_ready row -- no answer means no send: if anything newer is
unshown, the action ends as a deliberate `stale` no-send with detail
`stale_context_unasked` (a retry_ready row too: the retry_ready -> stale
edge comes with the Comm-Data-Store migration this change deploys after),
never definitive_failed, and the items are logged. A newer inbound gets its own wake, whose agent sees everything. With
nothing newer, the saved record is sent.

Two layers:

- a pure core -- what an execute or a confirm means for the question, whether
  a revise is legal, and the question itself;
- `StaleContextQuestions`, the only I/O: the ledger (block, answer), the probe
  (what is newer and unshown -- one query) and the context loader (a
  revise's context).

Driving an answered successor through the send gate is the service's; it is
handed in as `drive_answered`.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Mapping
from typing import Protocol
from uuid import UUID

from pydantic import ValidationError

from .adapters.base import ProviderDisposition
from .adapters.base import ProviderObservation
from .context import ActionContext
from .context import ActionContextLoader
from .errors import FailureKind
from .errors import classify
from .errors import error_text
from .identity import same_request
from .models import REVISABLE_ARGUMENT_KEYS
from .models import STALE_CONTEXT_DETAIL
from .models import STALE_CONTEXT_DETAILS
from .models import ActionRole
from .models import ActionState
from .models import ConfirmRequest
from .models import ContextItem
from .models import ExecuteRequest
from .models import NewerActivity
from .models import PublicResult
from .models import PublicStatus
from .models import RequestRefusedError
from .models import StaleContextDecision
from .record import AGENT_OVERRIDE
from .record import DECLINED_DETAIL
from .record import UNASKED_DETAIL
from .record import ActionStore
from .record import OutboundActionRecord
from .record import action_result
from .tenantcloud_shared import strip_tenantcloud_persisted_argument_keys

# The service's logger, as before the split: operators' filters key on it.
logger = logging.getLogger("postgres_mcp.outbound_gateway.service")

# States with a legal edge into `stale` (outbound_action_transition_allowed,
# Comm-Data-Store migration 153): only these can hold a question.
STALE_BLOCKABLE_STATES = frozenset({ActionState.RECEIVED, ActionState.PREPARED, ActionState.DEPENDENCY_WAIT})

# Nobody could be asked and something newer was unshown: a deliberate no-send.
# Not a stale_context question (STALE_CONTEXT_DETAILS): the agent may still
# override it (record.overridable). Its words (UNASKED_DETAIL) and the
# decline's (DECLINED_DETAIL) live with the result mapping in record.py.
UNASKED_DETAIL_CODE = "stale_context_unasked"

# How many newer items a question lists. The newest are shown; anything older
# stays unshown, so it asks again after the answer.
CONTEXT_ITEM_LIMIT = 10
CONTEXT_PREVIEW_CHARS = 300

# Every refused confirm says this. A refused answer is a decision about the
# message, never a gateway outage: it must not open the guarded Cliq DM
# fallback or any other route around the gateway.
CONFIRM_REFUSAL_NOTICE = (
    "This refusal is not a gateway infrastructure failure: it does not permit the direct "
    "Cliq fallback or any other route around the outbound gateway."
)


class NewerContextProbe(Protocol):
    async def newer_context(self, context: ActionContext, *, limit: int, waive_shown: bool) -> list[NewerActivity]:
        """Every message received from, or sent to, this action's recipient
        after the wake's context watermark, newest first, at most `limit`.
        waive_shown=True leaves out every item this wake's agent was already
        shown for this recipient (by identity, never by timestamp)."""
        ...


@dataclass(frozen=True)
class StaleQuestion:
    newer: tuple[NewerActivity, ...]  # newest first, capped at CONTEXT_ITEM_LIMIT
    truncated: bool

    @property
    def shown_refs(self) -> tuple[str, ...]:
        return tuple(item.ref for item in self.newer)

    @property
    def detail(self) -> str:
        newest = self.newer[0]
        more = f" and {len(self.newer) - 1} more" if len(self.newer) > 1 else ""
        omitted = " (older items omitted; you will be asked about them next)" if self.truncated else ""
        return (
            f"Refused - stale context: since your context was built, a message was {newest.label} "
            f'({newest.ref.replace(":", " ")} via {newest.source} at {newest.occurred_at.isoformat()}: '
            f'"{newest.preview}"){more}{omitted}. Still send? Answer with op=confirm: yes, no, or revise.'
        )


def stale_question(found: list[NewerActivity]) -> StaleQuestion | None:
    if not found:
        return None
    ordered = tuple(sorted(found, key=lambda item: item.occurred_at, reverse=True))
    return StaleQuestion(newer=ordered[:CONTEXT_ITEM_LIMIT], truncated=len(ordered) > CONTEXT_ITEM_LIMIT)


def stale_context_question(*, wakeup_event_id: int, action_id: UUID) -> str:
    """The exact three-way question the agent answers: yes, no, or revise.

    Wake 27138 was once told "resend with override=true", the override path
    could not work, and the agent left the gateway for the provider directly.
    The answer is a first-class gateway call, and the only one."""
    base = f'"op": "confirm", "wakeup_event_id": {wakeup_event_id}, "action_id": "{action_id}"'
    return (
        "Refused - stale context: nothing was sent, because the messages in new_context are newer than "
        "your context (direction received = from the recipient; sent by us = a message we already sent "
        "this recipient). Read them, then answer exactly once with outbound_action. "
        f'YES - send your message unchanged: {{{base}, "decision": "yes"}}. '
        f'NO - send nothing (the new context makes it redundant or wrong): {{{base}, "decision": "no"}}. '
        f'REVISE - send a corrected message instead: {{{base}, "decision": "revise", '
        '"arguments": {<the same arguments with only the message content changed>}}; '
        "the operation, intent, slot, recipient and target must stay the same. "
        "One answer per action. Never send it any other way: circumventing the outbound gateway is never an option."
    )


# --------------------------------------------------------------------------
# Pure core
# --------------------------------------------------------------------------


def refusal(message: str) -> RequestRefusedError:
    """Every refused stale_context answer names itself as a decision, not an
    outage, so no agent reads it as leave to use a direct provider route."""
    return RequestRefusedError(f"{message} {CONFIRM_REFUSAL_NOTICE}")


def answer_refusal(action_id: UUID, error: BaseException) -> RequestRefusedError | None:
    """The database's refusal of an answer (a different answer already
    stands, the cap, a sent message ...), in its own words. None: not a
    refusal (a passing database error, a fault) -- the caller raises it."""
    if classify(error) is not FailureKind.REFUSAL:
        return None
    return refusal(f"confirm refused for action {action_id}: {error_text(error).rstrip('.')}.")


def confirmation_disabled() -> RequestRefusedError:
    return refusal(
        "confirm refused: stale_context confirmation is not enabled on this gateway; "
        "there is no question to answer."
    )


def awaits_stale_confirmation(action: OutboundActionRecord) -> bool:
    return (
        action.state is ActionState.STALE
        and action.detail_code == STALE_CONTEXT_DETAIL
        and action.stale_context_decision is None
    )


class ExecuteAnswer(Enum):
    """What an execute of an action awaiting its answer means."""

    REASK = "reask"  # the same message again, unanswered: ask again
    YES = "yes"  # override=true, the historical spelling of "yes"


def execute_answer(action: OutboundActionRecord, request: ExecuteRequest, *, enabled: bool) -> ExecuteAnswer | None:
    """None: the execute is not about an open stale_context question (a
    message already answered yes reports its successor: the service's
    _latest_answer)."""
    if not enabled or not awaits_stale_confirmation(action):
        return None
    return ExecuteAnswer.YES if request.override else ExecuteAnswer.REASK


def implicit_revise(existing: OutboundActionRecord, request: ExecuteRequest) -> dict[str, Any] | None:
    """A DIFFERENT message executed for a wake role whose first message is
    waiting on a stale_context answer is that answer's `revise` -- and a
    revise changes message content only. Operation, role, intent and
    appointment slot must be the refused message's own; anything else is
    refused rather than silently sent with the refused message's values.
    Returns the revise's arguments; None means "not this case"."""
    # The same message again is not an answer; only a different message is
    # its revise. "Same" is what was asked, not the derived context hash.
    if not awaits_stale_confirmation(existing) or same_request(existing.execute_request(), request):
        return None
    differing = [
        name
        for name, asked, refused in (
            ("operation", request.operation, existing.operation),
            ("action_role", request.action_role, existing.action_role),
            ("intent_kind", request.intent_kind, str(existing.intent_kind)),
            ("appointment_slot", request.appointment_slot, existing.appointment_slot),
        )
        if asked != refused
    ]
    if differing:
        raise refusal(
            f"execute refused: action {existing.action_id} is awaiting your stale_context answer, and a "
            "different message for it counts as its revise, which may change message content only; "
            f"{', '.join(differing)} differ from the refused message. Answer the question with "
            'op "confirm" (yes, no, or revise with the same operation, intent, slot and target).'
        )
    return dict(request.arguments.model_dump(mode="json", exclude_none=True))


def check_own_wake(parent: OutboundActionRecord, request: ConfirmRequest) -> None:
    """Refuse an answer from another wake: a confirmation never crosses wakes."""
    if parent.wakeup_event_id != request.wakeup_event_id:
        raise refusal(
            f"confirm refused: action {parent.action_id} belongs to wake {parent.wakeup_event_id}, "
            f"not wake {request.wakeup_event_id}; a confirmation never crosses wakes."
        )


def check_confirmable(parent: OutboundActionRecord, request: ConfirmRequest) -> None:
    """Refuse an answer from another wake, or to an action not asking."""
    check_own_wake(parent, request)
    if parent.state is not ActionState.STALE or parent.detail_code not in STALE_CONTEXT_DETAILS:
        raise refusal(
            f"confirm refused: action {parent.action_id} is not awaiting a stale_context "
            f"confirmation (state {parent.state.value}, detail {parent.detail_code})."
        )


def revised_request(parent: OutboundActionRecord, arguments: Mapping[str, Any]) -> ExecuteRequest:
    """Validate a `revise` answer. The request is rebuilt from the refused
    row -- same operation, role, intent and slot -- with only `arguments`
    replaced, and only the operation's content keys may differ from the
    refused arguments."""
    revisable = REVISABLE_ARGUMENT_KEYS[parent.operation]
    if not revisable:
        raise refusal(f"revise refused: {parent.operation.value} has no message content to revise; answer yes or no.")
    original = dict(strip_tenantcloud_persisted_argument_keys(parent.operation, parent.arguments))
    if "cc" in original and "cc" not in arguments:
        # A revise that leaves cc out keeps the refused email's cc -- the
        # gateway's default included (identity.with_default_cc).
        arguments = {**arguments, "cc": original["cc"]}
    try:
        base = parent.execute_request()
        revised = ExecuteRequest.model_validate({**base.model_dump(mode="python"), "arguments": dict(arguments)})
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "arguments"
        raise refusal(f"revise refused: invalid arguments: {location}: {first['msg']}.") from exc
    normalized = revised.arguments.model_dump(mode="json", exclude_none=True)
    comparable_original = {key: value for key, value in original.items() if value is not None}
    changed = sorted(
        key
        for key in set(comparable_original) | set(normalized)
        if key not in revisable and comparable_original.get(key) != normalized.get(key)
    )
    if changed:
        raise refusal(
            "revise refused: only the message content ("
            + ", ".join(sorted(revisable))
            + ") may change; "
            + ", ".join(changed)
            + " must stay exactly as refused (same operation, recipient and target)."
        )
    return revised


def needs_confirmation(action: OutboundActionRecord, question: StaleQuestion | None) -> PublicResult:
    """The question: the newer context (oldest first) and how to answer."""
    newer = question.newer if question is not None else ()
    items = tuple(
        ContextItem(
            id=item.ref,
            source=item.source,
            direction=item.label,
            sender=item.sender,
            occurred_at=item.occurred_at,
            preview=item.preview[:CONTEXT_PREVIEW_CHARS],
        )
        # question.newer is newest first; the agent reads oldest -> newest.
        for item in reversed(newer)
    )
    return PublicResult(
        status=PublicStatus.NEEDS_CONFIRMATION,
        action_id=action.action_id,
        action_uid=action.action_uid,
        provider_request_ref=None,
        retryable=True,
        detail_code=STALE_CONTEXT_DETAIL,
        detail=question.detail if question is not None else "Refused - stale context: this message is still awaiting your yes/no answer.",
        new_context=items,
        question=stale_context_question(wakeup_event_id=action.wakeup_event_id, action_id=action.action_id),
    )


# --------------------------------------------------------------------------
# I/O
# --------------------------------------------------------------------------

DriveAnswered = Callable[..., Awaitable[PublicResult]]
"""drive_answered(successor, *, dispatch) -> result: the service's send gate."""


class StaleContextQuestions:
    """Asks, re-asks and records answers to the stale_context question."""

    def __init__(
        self,
        *,
        store: ActionStore,
        context_loader: ActionContextLoader,
        probe: NewerContextProbe | None,
        actor: str,
        enabled: bool,
        mode: str,
        drive_answered: DriveAnswered,
        lease_seconds: int = 60,
    ):
        self._store = store
        self._context_loader = context_loader
        self._probe = probe
        self._actor = actor
        # OUTBOUND_STALE_CONFIRM_ENABLED. Off: never ask, and never send
        # stale -- an action with unshown newer context ends as a
        # `stale_context_unasked` no-send -- and confirm is refused.
        self.enabled = enabled
        self._lease_seconds = lease_seconds
        # OUTBOUND_TRAFFIC_CONTROL: off (never look), shadow (log what would
        # be asked), enforce (ask).
        self._mode = mode
        self._drive_answered = drive_answered

    async def check(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        *,
        agent_facing: bool,
        confirm: bool = False,
        dispatch: bool = True,
    ) -> PublicResult | None:
        """The one stale-context check, before any send. Returns the question
        (or, where nobody can be asked, the `stale_context_unasked` no-send),
        or None: send. confirm=True (override=true) records the question and
        answers yes at once."""
        if action.action_role is ActionRole.INTERNAL_NOTIFICATION:
            # The question is "since your context was built, a new message
            # was sent to or received from this recipient -- still send?" An
            # internal_notification has no such recipient: it posts to a
            # staff review channel about something that happened, not a
            # reply in a conversation that can go stale. newer_context's
            # "received" match is channel-scoped, not role-scoped, so a busy
            # wake's own channel (e.g. a Quo line still getting texts) used
            # to trip this for a manual_review_alert with nothing to do with
            # its content (wakes 27313/27314, 2026-09-28: the bug report
            # itself was blocked as stale_context, delaying it behind a
            # confirm round trip it never needed). Always send.
            return None
        if action.remediation_reason == AGENT_OVERRIDE:
            # The agent already saw this message refused and chose to send it
            # anyway: asking again would only refuse its own answer.
            return None
        if self._mode == "off" or self._probe is None:
            return None
        askable = agent_facing and self.enabled and action.state in STALE_BLOCKABLE_STATES
        try:
            question = stale_question(
                await self._probe.newer_context(context, limit=CONTEXT_ITEM_LIMIT + 1, waive_shown=True)
            )
        except Exception:
            # Fail-open: a broken check must never stop outbound traffic.
            logger.error(
                "stale-context check failed for action %s on wake %s; the send proceeds",
                action.action_id,
                context.wakeup_event_id,
                exc_info=True,
            )
            return None
        if question is None:
            return None
        if self._mode == "shadow":
            logger.warning(
                "stale-context shadow would-%s: wake=%s action=%s newer=%s",
                "ask" if askable else "not send",
                context.wakeup_event_id,
                action.action_id,
                ",".join(question.shown_refs),
            )
            return None
        if not askable:
            return await self._end_unasked(action, context, question, agent_facing=agent_facing)
        return await self._block(action, context, question, confirm=confirm, dispatch=dispatch)

    async def _end_unasked(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        question: StaleQuestion,
        *,
        agent_facing: bool,
    ) -> PublicResult | None:
        """No answer means no send: end the action as a deliberate
        `stale_context_unasked` no-send, retry_ready included (its edge into
        `stale` comes with the Comm-Data-Store migration this change deploys
        after). None (send as before) only for a state that cannot hold this,
        which no caller reaches."""
        if action.state not in STALE_BLOCKABLE_STATES | {ActionState.RETRY_READY}:
            return None
        target = ActionState.STALE
        logger.warning(
            "newer context and nobody to ask (%s): wake=%s action=%s state=%s newer=%s; not sent (%s)",
            "confirmation disabled" if agent_facing and not self.enabled else ("agent" if agent_facing else "worker"),
            context.wakeup_event_id,
            action.action_id,
            action.state.value,
            ",".join(question.shown_refs),
            target.value,
        )
        current, lease_owner = action, None
        if action.state is not ActionState.RECEIVED:
            # claim_outbound_action's whitelist: prepared, dependency_wait,
            # retry_ready (a received row transitions without a lease).
            current = await self._store.claim(action.action_id, action.state, self._actor, self._lease_seconds)
            lease_owner = self._actor
        ended = await self._store.transition(
            current.action_id,
            current.state,
            target,
            lease_owner,
            ProviderObservation(
                ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
                UNASKED_DETAIL_CODE,
                evidence={"newer": list(question.shown_refs)},
            ),
        )
        return action_result(ended, detail=UNASKED_DETAIL)

    async def confirm(self, request: ConfirmRequest, parent: OutboundActionRecord, *, dispatch: bool) -> PublicResult:
        """Answer a needs_confirmation (stale_context) result.

        `no` records the decline and sends nothing. `yes` mints (once) a
        successor carrying the same payload; `revise` mints one carrying the
        revised content to the same target. Either successor is driven through
        the same gate every execute passes, with only the items the agent was
        SHOWN waived. With dispatch=False (TenantCloud) the successor is
        prepared for Restate instead of dispatched inline."""
        if not self.enabled:
            raise confirmation_disabled()
        check_confirmable(parent, request)
        if request.decision is StaleContextDecision.NO:
            declined = await self._answer(parent, StaleContextDecision.NO, None, wakeup_event_id=request.wakeup_event_id)
            return action_result(declined, detail=DECLINED_DETAIL)
        return await self.answer_and_drive(
            parent, request.decision, request.arguments, wakeup_event_id=request.wakeup_event_id, dispatch=dispatch
        )

    async def after_execute(
        self,
        existing: OutboundActionRecord,
        request: ExecuteRequest,
        *,
        dispatch: bool,
    ) -> PublicResult | None:
        """A different message for a wake role awaiting an answer is its
        revise (see implicit_revise). None: the ordinary execute continues."""
        arguments = implicit_revise(existing, request)
        if arguments is None:
            return None
        return await self.answer_and_drive(
            existing, StaleContextDecision.REVISE, arguments, wakeup_event_id=request.wakeup_event_id, dispatch=dispatch
        )

    async def answer_and_drive(
        self,
        parent: OutboundActionRecord,
        decision: StaleContextDecision,
        arguments: Mapping[str, Any] | None,
        *,
        wakeup_event_id: int,
        dispatch: bool,
    ) -> PublicResult:
        """Record a yes/revise and drive the successor it mints."""
        successor = await self._answer(parent, decision, arguments, wakeup_event_id=wakeup_event_id)
        return await self._drive_answered(successor, dispatch=dispatch)

    async def reask(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        """The agent re-executed a message whose question is still
        unanswered. Ask again, listing everything newer than the wake's
        context, and add what it now saw to the shown set."""
        question = None
        if self._probe is not None:
            try:
                question = stale_question(
                    await self._probe.newer_context(context, limit=CONTEXT_ITEM_LIMIT + 1, waive_shown=False)
                )
            except Exception:
                logger.error("stale-context listing failed for action %s on wake %s", action.action_id, context.wakeup_event_id, exc_info=True)
        if question is not None:
            try:
                action = await self._store.block_stale_context(action.action_id, ActionState.STALE, None, question.shown_refs)
            except Exception:
                logger.error("stale-context shown set could not be extended for action %s", action.action_id, exc_info=True)
        return needs_confirmation(action, question)

    async def _block(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        question: StaleQuestion,
        *,
        confirm: bool,
        dispatch: bool,
    ) -> PublicResult:
        """Record the refused message as a confirmable `stale` no-send and
        ask. A question that cannot be recorded sends nothing and raises (the
        service's _settled ends the row rejected, in these words)."""
        try:
            blocked = await self._store.block_stale_context(
                action.action_id,
                action.state,
                None if action.state is ActionState.RECEIVED else self._actor,
                question.shown_refs,
            )
        except Exception as exc:
            logger.error(
                "stale_context question for action %s on wake %s could not be recorded; nothing was sent",
                action.action_id,
                context.wakeup_event_id,
                exc_info=True,
            )
            raise RuntimeError(
                f"stale_context: newer messages reached this recipient, and the question about them could not be "
                f"recorded for action {action.action_id}; nothing was sent"
            ) from exc
        if confirm:
            logger.warning(
                "override=true answered stale_context yes: wake=%s action=%s shown=%s",
                context.wakeup_event_id,
                blocked.action_id,
                ",".join(question.shown_refs),
            )
            return await self.answer_and_drive(
                blocked, StaleContextDecision.YES, None, wakeup_event_id=context.wakeup_event_id, dispatch=dispatch
            )
        return needs_confirmation(blocked, question)

    async def _answer(
        self,
        parent: OutboundActionRecord,
        decision: StaleContextDecision,
        arguments: Mapping[str, Any] | None,
        *,
        wakeup_event_id: int,
    ) -> OutboundActionRecord:
        revision = None
        if decision is StaleContextDecision.REVISE:
            revision = await self._context_loader.load(
                revised_request(parent, arguments or {}), recorded_account=parent.provider_account
            )
        try:
            return await self._store.confirm_stale_context(
                parent.action_id,
                wakeup_event_id=wakeup_event_id,
                decision=decision.value,
                actor=self._actor,
                revision=revision,
            )
        except Exception as exc:
            refused = answer_refusal(parent.action_id, exc)
            if refused is None:
                raise
            raise refused from exc
