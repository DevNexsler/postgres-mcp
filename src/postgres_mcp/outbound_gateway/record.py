"""The durable outbound action record and the store seam every part of the
gateway reads and writes it through."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from typing import Any
from typing import Mapping
from typing import Protocol
from typing import Sequence
from uuid import UUID

from .adapters.base import ProviderObservation
from .adapters.base import ProviderReceipt
from .cliq_failure import CLIQ_CHAT_ACCOUNT_MISMATCH
from .cliq_failure import CLIQ_CHAT_ACCOUNT_MISMATCH_DETAIL
from .context import ActionContext
from .models import STALE_CONTEXT_DETAIL
from .models import STALE_CONTEXT_DETAILS
from .models import STORED_ACTION_CONTEXT
from .models import ActionRole
from .models import ActionState
from .models import CompletionKind
from .models import ExecuteRequest
from .models import Operation
from .models import OverrideRequest
from .models import PublicResult
from .models import RequestRefusedError
from .state_machine import public_result
from .tenantcloud_shared import strip_tenantcloud_persisted_argument_keys


@dataclass(frozen=True)
class OutboundActionRecord:
    action_id: UUID
    wakeup_event_id: int
    action_role: ActionRole
    operation: Operation
    intent_kind: str
    appointment_slot: datetime | None
    arguments: dict[str, Any]
    state: ActionState
    action_uid: UUID | None
    provider_request_ref: str | None
    provider_message_id: str | None
    provider_accepted_at: datetime | None
    completion_kind: CompletionKind | None
    detail_code: str
    attempt_count: int
    next_attempt_at: datetime
    payload_hash: str
    canonical_context: Mapping[str, Any]
    canonical_scope: Mapping[str, Any]
    recipient_scope: Mapping[str, Any]
    provider_account: str
    routing_policy_version: str
    provider_evidence_kind: str | None = None
    provider_evidence_reference: str | None = None
    provider_evidence_hash: str | None = None
    provider_readback_evidence: Mapping[str, Any] = dataclass_field(default_factory=dict)
    error_category: str | None = None
    retry_of_action_id: UUID | None = None
    remediation_reason: str | None = None
    # Comm-Data-Store migration 251: the agent's answer to this action (op
    # confirm, on a refusal or a stale_context question): yes / revise (a
    # successor carries the send) or no (dropped on purpose); NULL while
    # unanswered.
    override_decision: str | None = None
    # outbound_actions.dispatch_started_at: a send was started (the provider
    # may have it). None: never dispatched.
    dispatch_started_at: datetime | None = None
    # outbound_actions.error_detail (generated: the provider's error text,
    # else the uncertainty reason, else detail_code): why it was not sent.
    error_detail: str | None = None
    # Comm-Data-Store migration 192: the stale-context question's durable
    # half. shown_refs are the exact context items the agent was shown
    # (message:<id> / action:<uuid>); decision is its answer (yes | no |
    # revise), NULL while unanswered.
    stale_context_shown_refs: tuple[str, ...] = ()
    stale_context_decision: str | None = None
    # outbound_actions.created_at already exists (not previously read by this
    # dataclass): the anchor for the TenantCloud auth-outage wait's 2h
    # ceiling (service.py's _tenantcloud_auth_wait_pending). None on a
    # hand-built record (most test fixtures): treated as "just created".
    created_at: datetime | None = None

    def execute_request(self) -> ExecuteRequest:
        # create_or_load() persists TenantCloud arguments enriched with
        # desired_state/target_reference/idempotency_key (migration 118
        # reads those directly off outbound_actions.arguments). Every
        # ArgumentModel is a StrictModel with extra="forbid", so those
        # gateway-owned keys must be stripped back out before they reach
        # model_validate() here -- this method rebuilds context on every
        # reconcile()/resume() call, including the crash-recovery path.
        arguments = strip_tenantcloud_persisted_argument_keys(self.operation, self.arguments)
        return ExecuteRequest.model_validate(
            {
                "op": "execute",
                "wakeup_event_id": self.wakeup_event_id,
                "action_role": self.action_role,
                "operation": self.operation,
                "intent_kind": self.intent_kind,
                "appointment_slot": self.appointment_slot,
                "arguments": arguments,
            },
            # An already-stored action: only NEW requests get the new-content
            # rules (see refuse_tenantcloud_wide_characters).
            context=STORED_ACTION_CONTEXT,
        )


class ActionStore(Protocol):
    async def create_or_load(self, context: ActionContext) -> OutboundActionRecord: ...

    async def prepare(
        self,
        context: ActionContext,
        expected_state: ActionState,
    ) -> OutboundActionRecord: ...

    async def claim(
        self,
        action_id: UUID,
        expected_state: ActionState,
        lease_owner: str,
        lease_seconds: int,
    ) -> OutboundActionRecord: ...

    async def record_provider_request(
        self,
        action_id: UUID,
        lease_owner: str,
        observation: ProviderObservation,
    ) -> OutboundActionRecord: ...

    async def transition(
        self,
        action_id: UUID,
        expected_state: ActionState,
        next_state: ActionState,
        lease_owner: str | None,
        observation: ProviderObservation,
    ) -> OutboundActionRecord: ...

    async def complete(
        self,
        action_id: UUID,
        expected_state: ActionState,
        lease_owner: str | None,
        receipt: ProviderReceipt,
        completion_kind: CompletionKind,
        detail_code: str,
    ) -> OutboundActionRecord: ...

    async def definitive_fail(
        self,
        action_id: UUID,
        expected_state: ActionState,
        lease_owner: str,
        observation: ProviderObservation,
    ) -> OutboundActionRecord: ...

    async def remediate_traffic_block(
        self,
        action_id: UUID,
        *,
        operator_identity: str,
        reason: str,
    ) -> OutboundActionRecord: ...

    async def block_stale_context(
        self,
        action_id: UUID,
        expected_state: ActionState,
        lease_owner: str | None,
        shown_refs: Sequence[str],
    ) -> OutboundActionRecord: ...

    async def confirm_stale_context(
        self,
        action_id: UUID,
        *,
        wakeup_event_id: int,
        decision: str,
        actor: str,
        revision: ActionContext | None = None,
    ) -> OutboundActionRecord: ...

    async def reject(
        self,
        action_id: UUID,
        expected_state: ActionState,
        detail_code: str,
        error_detail: str,
    ) -> OutboundActionRecord: ...

    async def override(
        self,
        action_id: UUID,
        *,
        wakeup_event_id: int,
        actor: str,
        reason: str | None,
        decision: str,
    ) -> OutboundActionRecord: ...

    async def successor(self, action_id: UUID) -> OutboundActionRecord | None: ...

    async def get(self, action_id: UUID) -> OutboundActionRecord | None: ...

    async def request_account(self, request: ExecuteRequest) -> str | None: ...

    async def schedule_next_attempt(
        self,
        action_id: UUID,
        expected_state: ActionState,
        delay_seconds: int,
        detail_code: str,
    ) -> OutboundActionRecord: ...


class UnknownActionError(RequestRefusedError, LookupError):
    """The caller named an action_id the gateway has no record of."""


async def require_action(store: ActionStore, action_id: UUID) -> OutboundActionRecord:
    action = await store.get(action_id)
    if action is None:
        raise UnknownActionError(
            f"outbound action does not exist (action_id {action_id}). Check the action_id: copy it from "
            "the execute result it came from."
        )
    return action


def is_due(action: OutboundActionRecord, now: datetime) -> bool:
    return action.next_attempt_at <= now


# remediation_reason of a successor an agent's override minted
# (Comm-Data-Store override_outbound_action).
AGENT_OVERRIDE = "agent_override"

DECLINED_DETAIL = "Declined: nothing was sent for this action. This is a recorded no-send, not a failure."
UNASKED_DETAIL = (
    "Not sent: newer messages reached this recipient since your context was built, and nobody could be "
    "asked about them. This is a deliberate no-send, not a failure."
)

# The one override instruction, word for word wherever it is taught (the
# tool description, every result that carries `override`, and
# Comm-Data-Store's agent instructions). The override is a first-class
# gateway call and the only route: wake 27138 was once told to "resend with
# override=true", the call could not work, and the agent sent through the
# provider directly.
NEVER_ANOTHER_ROUTE = "Never send through another tool or route."
OVERRIDE_RULE = (
    'If an outbound_action result includes an "override" object, you may still send it: send that object '
    'back as the outbound_action request, adding "reason" (why you still want to send). '
    f"To change the message, send a new execute. {NEVER_ANOTHER_ROUTE}"
)

# A completed duplicate that sent nothing because an earlier action or lock
# already covered it. Not operator_positive_evidence: an operator proved
# that one delivered, so it reads as sent.
NO_SEND_DUPLICATE_DETAILS = frozenset(
    {"already_handled", "existing_lock_receipt", "duplicate_inquiry_already_handled"}
)
# A failure that may still have reached the recipient: the retry ceiling also
# ends a send whose earlier attempts may have landed.
_MAYBE_DELIVERED_DETAILS = frozenset({"retry_budget_exhausted"})
_PARKED_STATES = frozenset({ActionState.MANUAL_REVIEW, ActionState.DEAD_LETTER})


def answer(action: OutboundActionRecord) -> str | None:
    """The agent's recorded answer to this action (yes, no or revise), or
    None. 192's stale_context answers predate override_decision."""
    return action.override_decision or action.stale_context_decision


def answered_by_successor(action: OutboundActionRecord) -> bool:
    """The agent's yes (or revise) sent this action as a successor: report
    that one, never offer it again."""
    return answer(action) in {"yes", "revise"}


def _not_sent(action: OutboundActionRecord) -> bool:
    """Nothing of this action reached a provider. Comm-Data-Store
    answer_outbound_action (migration 251) answers only these; this is its
    rule, read here so a result offers only an override the database takes."""
    state = action.state
    if state in {ActionState.REJECTED, ActionState.STALE}:
        return True
    if state is ActionState.DEFINITIVE_FAILED:
        return action.detail_code not in _MAYBE_DELIVERED_DETAILS
    if state in _PARKED_STATES:
        return action.dispatch_started_at is None
    if state is ActionState.COMPLETED:
        return action.completion_kind is CompletionKind.DUPLICATE and action.detail_code in NO_SEND_DUPLICATE_DETAILS
    return False


def overridable(action: OutboundActionRecord) -> bool:
    """Whether the agent may still send this action unchanged (op confirm,
    decision yes, with a reason): nothing of it was sent and it is not yet
    answered. An open stale_context question is answered as the question."""
    if answer(action) is not None or not _not_sent(action):
        return False
    return action.state is not ActionState.STALE or action.detail_code not in STALE_CONTEXT_DETAILS


def not_sent_reason(action: OutboundActionRecord) -> str:
    """The row's own words for why it was not sent."""
    text = " ".join((action.error_detail or "").split())
    return text if text else action.detail_code


def _row_detail(action: OutboundActionRecord, *, repeated: bool) -> str | None:
    """What a row that did not send (or already sent) means for the agent.
    None: public_result's pending/unknown/manual_review wording applies."""
    state = action.state
    if answer(action) == "no":
        return DECLINED_DETAIL
    if answered_by_successor(action):
        return "Not sent by this action: your answer sent it as a successor action."
    if state is ActionState.COMPLETED:
        if overridable(action):
            ref = f", {action.provider_request_ref}" if action.provider_request_ref else ""
            return f"Not sent: an earlier send already covers this ({action.detail_code}{ref})."
        if repeated and action.completion_kind is CompletionKind.SENT:
            when = f" at {action.provider_accepted_at.isoformat()}" if action.provider_accepted_at else ""
            return (
                f"Already sent{when}: this identical request is that same action, so nothing new went out. "
                "To send more, execute a new message."
            )
        if action.completion_kind is CompletionKind.DUPLICATE:
            return f"Already sent: delivery was confirmed ({action.detail_code}). To send more, execute a new message."
        return None
    if state is ActionState.STALE:
        if action.detail_code == STALE_CONTEXT_DETAIL:
            return 'Not sent: awaiting your stale_context answer -- op "confirm" with decision yes, no or revise.'
        if action.detail_code == "stale_context_unasked":
            return UNASKED_DETAIL
        return f"Not sent: {not_sent_reason(action)}."
    if state in {ActionState.REJECTED, ActionState.DEFINITIVE_FAILED} or (state in _PARKED_STATES and _not_sent(action)):
        if action.operation in {Operation.CLIQ_CHANNEL_POST, Operation.CLIQ_CHAT_POST} and action.detail_code == CLIQ_CHAT_ACCOUNT_MISMATCH:
            return CLIQ_CHAT_ACCOUNT_MISMATCH_DETAIL
        maybe = "" if _not_sent(action) else " It may already have reached the recipient: read the thread first."
        return f"Not sent: {not_sent_reason(action)}.{maybe}"
    return None


def why_not_overridable(action: OutboundActionRecord) -> str:
    """Why op confirm cannot send this action anyway, and what to do instead."""
    if answered_by_successor(action):
        return (
            f"confirm refused: you already answered yes for action {action.action_id}, and its successor "
            'carries the send. Check it with op "status".'
        )
    if answer(action) == "no":
        return (
            f"confirm refused: you already answered no for action {action.action_id}, and the first answer "
            "stands. To send it now, execute it as a new message."
        )
    if action.state is ActionState.COMPLETED:
        when = f" at {action.provider_accepted_at.isoformat()}" if action.provider_accepted_at else ""
        return (
            f"confirm refused: action {action.action_id} was already sent{when}; send a new message instead "
            "(a new execute with the new content)."
        )
    if action.state in {ActionState.DEFINITIVE_FAILED, *_PARKED_STATES}:
        return (
            f"confirm refused: action {action.action_id} reached the provider and may already have been "
            "delivered, so it is not sent again from here. Read the thread; if it did not arrive, record "
            "needs_human with the message you meant to send."
        )
    if action.state in {ActionState.UNKNOWN, ActionState.RECONCILING}:
        return (
            f"confirm refused: action {action.action_id} may already have reached the recipient and is still "
            'being checked. Check it with op "status" before sending anything else.'
        )
    return (
        f"confirm refused: action {action.action_id} is still being delivered (state {action.state.value}); "
        'nothing failed, so there is nothing to override. Check it with op "status".'
    )


def action_result(
    action: OutboundActionRecord,
    *,
    repeated: bool = False,
    detail: str | None = None,
    detail_code: str | None = None,
) -> PublicResult:
    """The agent-facing result for a row as it stands: what happened, and,
    when nothing was sent, the override request that still sends it. Every
    result the gateway returns for a row is built here."""
    result = public_result(
        state=action.state,
        action_id=action.action_id,
        action_uid=action.action_uid,
        provider_request_ref=action.provider_request_ref,
        detail_code=detail_code or action.detail_code,
        completion_kind=action.completion_kind,
        repeated_execute=repeated,
        detail=detail or _row_detail(action, repeated=repeated),
    )
    if not overridable(action):
        return result
    why = (result.detail or "").rstrip()
    if why and not why.endswith((".", "!", "?")):
        why += "."
    return result.model_copy(
        update={
            "detail": " ".join(
                part for part in (why, NEVER_ANOTHER_ROUTE if action.detail_code == CLIQ_CHAT_ACCOUNT_MISMATCH else OVERRIDE_RULE) if part
            ),
            "override": OverrideRequest(wakeup_event_id=action.wakeup_event_id, action_id=action.action_id),
        }
    )
