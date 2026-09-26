"""The stale-context question: needs_confirmation -> confirm yes | no | revise.

When newer activity reached a recipient after the agent's context was built,
the gateway does not decide for the agent: it records the refused message as a
`stale` no-send, shows the agent what it has not seen, and asks. That includes
outbound we already sent to the action's own target after the source message
(direction outbound, labelled as sent by us): the gateway never completes a
send as "already handled" on the agent's behalf. The agent
answers `yes` (send it unchanged), `no` (send nothing) or `revise` (send a
corrected message, same operation, recipient and target).

Two layers:

- a pure core -- what an execute or a confirm means for the question, whether
  a revise is legal, which inbound was already shown, and the question itself;
- `StaleContextQuestions`, the only I/O: the ledger (block, answer), the
  traffic probe (what is newer, what was shown) and the context loader (a
  revise's context).

Driving an answered successor through the send gate (traffic, preflight,
dispatch) is the service's; it is handed in as `drive_answered`, and a block
that cannot be recorded falls back to the service's `terminal_block`.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import replace as dataclass_replace
from enum import Enum
from typing import Any
from typing import Mapping

from pydantic import ValidationError

from .context import ActionContext
from .context import ActionContextLoader
from .identity import same_request
from .models import REVISABLE_ARGUMENT_KEYS
from .models import STALE_CONTEXT_DETAIL
from .models import STALE_CONTEXT_DETAILS
from .models import ActionState
from .models import ConfirmRequest
from .models import ContextItem
from .models import ExecuteRequest
from .models import PublicResult
from .models import PublicStatus
from .models import StaleContextDecision
from .preflight import PreflightDecision
from .preflight import PreflightEvidence
from .preflight import PreflightOutcome
from .record import ActionStore
from .record import OutboundActionRecord
from .record import action_result
from .record import require_action
from .tenantcloud_shared import strip_tenantcloud_persisted_argument_keys
from .traffic_control import CONFIRM_REFUSAL_NOTICE
from .traffic_control import CONTEXT_ITEM_LIMIT
from .traffic_control import CONTEXT_PREVIEW_CHARS
from .traffic_control import NewerActivity
from .traffic_control import TrafficProbe
from .traffic_control import TrafficVerdict
from .traffic_control import list_stale_context
from .traffic_control import stale_context_question
from .traffic_control import stale_context_verdict

# The service's logger, as before the split: operators' filters key on it.
logger = logging.getLogger("postgres_mcp.outbound_gateway.service")

# States with a legal edge into `stale` (outbound_action_transition_allowed,
# Comm-Data-Store migration 153): a stale_context refusal can become a
# confirmable no-send only from these. retry_ready has no such edge.
STALE_BLOCKABLE_STATES = frozenset({ActionState.RECEIVED, ActionState.PREPARED, ActionState.DEPENDENCY_WAIT})

STALE_CONTEXT_SUCCESSOR_REASONS = frozenset({"stale_context_confirmed", "stale_context_revised"})

DECLINED_DETAIL = "Declined: nothing was sent for this action. This is a recorded no-send, not a failure."


# --------------------------------------------------------------------------
# Pure core
# --------------------------------------------------------------------------


def refusal(message: str) -> ValueError:
    """Every refused stale_context answer names itself as a decision, not an
    outage, so no agent reads it as leave to use a direct provider route."""
    return ValueError(f"{message} {CONFIRM_REFUSAL_NOTICE}")


def confirmation_disabled() -> ValueError:
    return refusal(
        "confirm refused: stale_context confirmation is not enabled on this gateway; "
        "a stale_context refusal from it is final."
    )


def awaits_stale_confirmation(action: OutboundActionRecord) -> bool:
    return (
        action.state is ActionState.STALE
        and action.detail_code == STALE_CONTEXT_DETAIL
        and action.stale_context_decision is None
    )


def is_stale_context_successor(action: OutboundActionRecord) -> bool:
    return action.remediation_reason in STALE_CONTEXT_SUCCESSOR_REASONS


class ExecuteAnswer(Enum):
    """What an execute of an existing action means for its question."""

    REASK = "reask"  # the same message again, unanswered: ask again
    YES = "yes"  # override=true (the historical "yes"), or already answered yes


def execute_answer(action: OutboundActionRecord, request: ExecuteRequest, *, enabled: bool) -> ExecuteAnswer | None:
    """None: the execute is not about a stale_context question."""
    if not enabled:
        return None
    if awaits_stale_confirmation(action):
        # The same message again after a needs_confirmation. override=true
        # is the historical spelling of "yes"; anything else re-asks.
        return ExecuteAnswer.YES if request.override else ExecuteAnswer.REASK
    if action.state is ActionState.STALE and action.stale_context_decision == StaleContextDecision.YES.value:
        # A repeated execute of a message already confirmed: report the
        # successor that carries it (idempotent, never a second send).
        return ExecuteAnswer.YES
    return None


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


def check_confirmable(parent: OutboundActionRecord, request: ConfirmRequest) -> None:
    """Refuse an answer from another wake, or to an action not asking."""
    if parent.wakeup_event_id != request.wakeup_event_id:
        raise refusal(
            f"confirm refused: action {parent.action_id} belongs to wake {parent.wakeup_event_id}, "
            f"not wake {request.wakeup_event_id}; a confirmation never crosses wakes."
        )
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


def asks_on_block(action: OutboundActionRecord, verdict: TrafficVerdict, *, agent_facing: bool) -> bool:
    """With confirmation enabled, an enforced stale_context block on the
    agent's own execute/confirm becomes a question instead of a terminal
    failure -- when there is something to show and the row can become
    `stale`. Worker-driven resume()/prepare() have nobody to ask."""
    return (
        agent_facing
        and verdict.reason == "stale_context"
        and bool(verdict.shown_refs)
        and action.state in STALE_BLOCKABLE_STATES
    )


def later_inbound(evidence: PreflightEvidence) -> tuple[int, ...]:
    return tuple(
        evidence.later_inbound_message_ids
        or ((evidence.later_inbound_message_id,) if evidence.later_inbound_message_id is not None else ())
    )


def waive_shown(evidence: PreflightEvidence, shown: frozenset[str]) -> tuple[PreflightEvidence, tuple[int, ...]]:
    """The evidence restricted to the newer inbound and outbound NOT in
    `shown` (by message id, never by timestamp), and the unshown inbound ids."""
    unshown = tuple(message_id for message_id in later_inbound(evidence) if f"message:{message_id}" not in shown)
    unshown_outbound = tuple(
        message_id for message_id in evidence.later_outbound_message_ids if f"message:{message_id}" not in shown
    )
    return (
        dataclass_replace(
            evidence,
            later_inbound_message_id=max(unshown) if unshown else None,
            later_inbound_message_ids=unshown,
            later_outbound_message_ids=unshown_outbound,
        ),
        unshown,
    )


# Preflight outcomes that lead toward a send (now, or after the calendar
# dependency): the ones an unshown newer outbound turns into a question.
_SENDING_OUTCOMES = frozenset({PreflightOutcome.READY, PreflightOutcome.DEPENDENCY_WAIT})


def asks_about_outbound(
    action: OutboundActionRecord,
    decision: PreflightDecision,
    outbound: tuple[int, ...],
    *,
    agent_facing: bool,
) -> bool:
    """An outbound to this action's target, sent after the source message and
    never shown to this wake's agent, is asked about before a send -- on the
    agent's own execute or confirm, while the row can still become `stale`."""
    return (
        agent_facing
        and bool(outbound)
        and decision.outcome in _SENDING_OUTCOMES
        and action.state in STALE_BLOCKABLE_STATES
    )


def sent_by_us(item: NewerActivity) -> NewerActivity:
    """Label an outbound item as ours for the agent reading new_context."""
    if item.direction != "outbound":
        return item
    return dataclass_replace(item, sender=f"sent by us ({item.sender})" if item.sender else "sent by us")


def newer_outbound_verdict(items: list[NewerActivity]) -> TrafficVerdict | None:
    """The question over outbound we already sent to the action's target."""
    if not items:
        return None
    ordered = tuple(sorted((sent_by_us(item) for item in items), key=lambda item: item.occurred_at, reverse=True))
    newer = ordered[:CONTEXT_ITEM_LIMIT]
    newest = newer[0]
    more = f" and {len(newer) - 1} more" if len(newer) > 1 else ""
    omitted = " (older items omitted; you will be asked about them next)" if len(ordered) > CONTEXT_ITEM_LIMIT else ""
    detail = (
        "Not sent yet - stale context: since the message you are answering, we already sent this recipient "
        f"a message (direction outbound, sent by us): {newest.ref.replace(':', ' ')} via {newest.source} at "
        f'{newest.occurred_at.isoformat()}: "{newest.preview}"{more}{omitted}. Nothing was decided for you. '
        "Read new_context, then answer with op=confirm: yes (send yours as well), no (it is already covered), "
        "or revise."
    )
    return TrafficVerdict(
        allowed=False,
        reason="stale_context",
        detail=detail,
        check_failed=False,
        newer=newer,
        truncated=len(ordered) > CONTEXT_ITEM_LIMIT,
    )


def asks_about_unshown(
    action: OutboundActionRecord,
    decision: PreflightDecision,
    unshown: tuple[int, ...],
    *,
    agent_facing: bool,
) -> bool:
    """After the agent answered, an inbound it was never shown asks again on
    the successor instead of silently suppressing the send it confirmed."""
    return (
        agent_facing
        and decision.outcome is PreflightOutcome.STALE
        and decision.detail_code == "newer_inbound"
        and bool(unshown)
        and is_stale_context_successor(action)
        and action.state in STALE_BLOCKABLE_STATES
    )


def needs_confirmation(action: OutboundActionRecord, verdict: TrafficVerdict | None) -> PublicResult:
    """The question: the newer context (oldest first) and how to answer."""
    newer = verdict.newer if verdict is not None else ()
    items = tuple(
        ContextItem(
            id=item.ref,
            source=item.source,
            direction=item.direction,
            sender=item.sender,
            occurred_at=item.occurred_at,
            preview=item.preview[:CONTEXT_PREVIEW_CHARS],
        )
        # verdict.newer is newest first; the agent reads oldest -> newest.
        for item in reversed(newer)
    )
    detail = (
        verdict.detail
        if verdict is not None and verdict.detail
        else "Refused - stale context: this message is still awaiting your yes/no answer."
    )
    return PublicResult(
        status=PublicStatus.NEEDS_CONFIRMATION,
        action_id=action.action_id,
        action_uid=action.action_uid,
        provider_request_ref=None,
        retryable=True,
        detail_code=STALE_CONTEXT_DETAIL,
        detail=detail,
        new_context=items,
        question=stale_context_question(wakeup_event_id=action.wakeup_event_id, action_id=action.action_id),
    )


# --------------------------------------------------------------------------
# I/O
# --------------------------------------------------------------------------

DriveAnswered = Callable[..., Awaitable[PublicResult]]
"""drive_answered(successor, *, dispatch) -> result: the service's send gate."""

TerminalBlock = Callable[[OutboundActionRecord, ActionContext, TrafficVerdict], Awaitable[PublicResult]]
"""terminal_block(action, context, verdict) -> result: the pre-192 traffic_blocked failure."""


class StaleContextQuestions:
    """Asks, re-asks and records answers to the stale_context question."""

    def __init__(
        self,
        *,
        store: ActionStore,
        context_loader: ActionContextLoader,
        traffic_probe: TrafficProbe | None,
        actor: str,
        enabled: bool,
        drive_answered: DriveAnswered,
        terminal_block: TerminalBlock,
    ):
        self._store = store
        self._context_loader = context_loader
        self._probe = traffic_probe
        self._actor = actor
        # OUTBOUND_STALE_CONFIRM_ENABLED. Off (default) is the pre-192 gateway:
        # a stale_context block is the terminal traffic_blocked failure and
        # confirm is refused. Turn it on only after Comm-Data-Store's reconciler
        # (which reads a confirmed successor's outcome) is live and migration
        # 192 is applied; before that a successor's failure would be invisible.
        self.enabled = enabled
        self._drive_answered = drive_answered
        self._terminal_block = terminal_block

    @property
    def can_ask(self) -> bool:
        return self.enabled and self._probe is not None

    async def confirm(self, request: ConfirmRequest, *, dispatch: bool) -> PublicResult:
        """Answer a needs_confirmation (stale_context) result.

        `no` records the decline and sends nothing. `yes` mints (once) a
        successor carrying the same payload; `revise` mints one carrying the
        revised content to the same target. Either successor is driven through
        the same gate every execute passes -- in-flight lease, recipient
        safety, intent lock, preflight -- with only the context items the agent
        was SHOWN waived. With dispatch=False (TenantCloud) the successor is
        preflighted and prepared for Restate instead of dispatched inline."""
        if not self.enabled:
            raise confirmation_disabled()
        parent = await require_action(self._store, request.action_id)
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
        """The agent re-executed a message whose stale_context question is
        still unanswered. Ask again, listing everything newer than the wake's
        context, and add what it now saw to the shown set."""
        verdict = None
        if self._probe is not None:
            verdict = await list_stale_context(
                self._probe,
                recipient_key=context.prospect_id,
                channel_id=context.channel_id,
                wakeup_event_id=context.wakeup_event_id,
                action_id=action.action_id,
                logger=logger,
            )
        if verdict is not None and verdict.shown_refs:
            try:
                action = await self._store.block_stale_context(action.action_id, ActionState.STALE, None, verdict.shown_refs)
            except Exception:
                logger.warning("stale-context shown set could not be extended for action %s", action.action_id, exc_info=True)
        return needs_confirmation(action, verdict)

    async def block(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        verdict: TrafficVerdict,
        *,
        confirm: bool,
        dispatch: bool,
    ) -> PublicResult:
        """Record the refused message as a confirmable `stale` no-send and
        ask; confirm=True (override=true) answers yes at once."""
        try:
            blocked = await self._record_block(action, verdict)
        except Exception:
            # Migration 192 not applied (or a concurrent writer moved the
            # row): keep the pre-192 contract rather than dispatching.
            logger.error(
                "stale_context block for action %s on wake %s could not be recorded as a "
                "confirmable no-send; falling back to the terminal traffic block",
                action.action_id,
                context.wakeup_event_id,
                exc_info=True,
            )
            current = await require_action(self._store, action.action_id)
            return await self._terminal_block(current, context, verdict)
        if confirm:
            logger.warning(
                "override=true answered stale_context yes: wake=%s action=%s shown=%s",
                context.wakeup_event_id,
                blocked.action_id,
                ",".join(verdict.shown_refs),
            )
            return await self.answer_and_drive(
                blocked, StaleContextDecision.YES, None, wakeup_event_id=context.wakeup_event_id, dispatch=dispatch
            )
        return needs_confirmation(blocked, verdict)

    async def _record_block(self, action: OutboundActionRecord, verdict: TrafficVerdict) -> OutboundActionRecord:
        return await self._store.block_stale_context(
            action.action_id,
            action.state,
            None if action.state is ActionState.RECEIVED else self._actor,
            verdict.shown_refs,
        )

    async def waive_shown_inbound(
        self,
        context: ActionContext,
        evidence: PreflightEvidence,
    ) -> tuple[PreflightEvidence, tuple[int, ...]]:
        """A prospect reply's preflight declines to send over ANY inbound newer
        than its source message (`newer_inbound`), across channels. An inbound
        this wake's agent was SHOWN in a stale_context question -- by message
        id, never by timestamp -- is no longer unseen context and is waived;
        every other one still counts. The same holds for newer outbound to the
        action's target. Returns the evidence restricted to the unshown items,
        and the unshown inbound ids."""
        later = later_inbound(evidence)
        if not (later or evidence.later_outbound_message_ids) or not self.can_ask:
            return evidence, later
        assert self._probe is not None
        try:
            shown = await self._probe.acknowledged_refs(context.wakeup_event_id, context.prospect_id)
        except Exception:
            logger.warning(
                "shown-context read failed on wake %s; waiving no newer inbound",
                context.wakeup_event_id,
                exc_info=True,
            )
            return evidence, later
        return waive_shown(evidence, frozenset(shown))

    async def ask_about_unshown_inbound(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        decision: PreflightDecision,
        unshown: tuple[int, ...],
        *,
        agent_facing: bool,
        dispatch: bool,
    ) -> PublicResult | None:
        """After the agent answered a stale_context question, an inbound it
        was never shown (another channel of the same prospect, ingested late,
        or past the display cap) asks again -- on the successor -- instead of
        silently suppressing the send it just confirmed."""
        if not (self.can_ask and asks_about_unshown(action, decision, unshown, agent_facing=agent_facing)):
            return None
        assert self._probe is not None
        try:
            items = await self._probe.messages_by_id(list(unshown))
        except Exception:
            logger.warning("unshown-inbound read failed for action %s", action.action_id, exc_info=True)
            return None
        verdict = stale_context_verdict(items)
        if verdict is None:
            return None
        return await self.block(action, context, verdict, confirm=False, dispatch=dispatch)

    async def ask_about_newer_outbound(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        decision: PreflightDecision,
        evidence: PreflightEvidence,
        *,
        agent_facing: bool,
        dispatch: bool,
    ) -> PublicResult | None:
        """Outbound we already sent to this action's target after the source
        message is information, not a verdict: the agent is shown it (sent by
        us) and answers yes, no or revise. Where nobody can be asked -- the
        worker, confirmation disabled, a row that cannot become `stale`, a
        read or ledger write that fails -- the gateway does not decide either:
        None, and the send proceeds."""
        outbound = evidence.later_outbound_message_ids
        if not outbound or decision.outcome not in _SENDING_OUTCOMES:
            return None
        refs = ",".join(f"message:{message_id}" for message_id in outbound)
        if not (self.can_ask and asks_about_outbound(action, decision, outbound, agent_facing=agent_facing)):
            logger.warning(
                "newer outbound to %s on wake %s (%s) not shown to an agent: nobody can be asked here; "
                "the send for action %s proceeds",
                context.prospect_id,
                context.wakeup_event_id,
                refs,
                action.action_id,
            )
            return None
        assert self._probe is not None
        try:
            verdict = newer_outbound_verdict(await self._probe.messages_by_id(list(outbound)))
            if verdict is None:
                return None
            blocked = await self._record_block(action, verdict)
        except Exception:
            logger.warning(
                "newer outbound (%s) could not be shown for action %s; the send proceeds",
                refs,
                action.action_id,
                exc_info=True,
            )
            return None
        return needs_confirmation(blocked, verdict)

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
            revision = await self._context_loader.load(revised_request(parent, arguments or {}))
        try:
            return await self._store.confirm_stale_context(
                parent.action_id,
                wakeup_event_id=wakeup_event_id,
                decision=decision.value,
                actor=self._actor,
                revision=revision,
            )
        except Exception as exc:
            reason = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
            raise refusal(f"confirm refused for action {parent.action_id}: {reason}.") from exc
