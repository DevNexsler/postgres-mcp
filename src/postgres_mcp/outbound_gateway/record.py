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
from .context import ActionContext
from .models import STORED_ACTION_CONTEXT
from .models import ActionRole
from .models import ActionState
from .models import CompletionKind
from .models import ExecuteRequest
from .models import Operation
from .models import PublicResult
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
    # Comm-Data-Store migration 192: the stale-context question's durable
    # half. shown_refs are the exact context items the agent was shown
    # (message:<id> / action:<uuid>); decision is its answer (yes | no |
    # revise), NULL while unanswered.
    stale_context_shown_refs: tuple[str, ...] = ()
    stale_context_decision: str | None = None

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

    async def get(self, action_id: UUID) -> OutboundActionRecord | None: ...

    async def schedule_next_attempt(
        self,
        action_id: UUID,
        expected_state: ActionState,
        delay_seconds: int,
        detail_code: str,
    ) -> OutboundActionRecord: ...


async def require_action(store: ActionStore, action_id: UUID) -> OutboundActionRecord:
    action = await store.get(action_id)
    if action is None:
        raise LookupError("outbound action does not exist")
    return action


def is_due(action: OutboundActionRecord, now: datetime) -> bool:
    return action.next_attempt_at <= now


def action_result(
    action: OutboundActionRecord,
    *,
    repeated: bool = False,
    detail: str | None = None,
    detail_code: str | None = None,
) -> PublicResult:
    """The agent-facing result for a row as it stands."""
    return public_result(
        state=action.state,
        action_id=action.action_id,
        action_uid=action.action_uid,
        provider_request_ref=action.provider_request_ref,
        detail_code=detail_code or action.detail_code,
        completion_kind=action.completion_kind,
        repeated_execute=repeated,
        detail=detail,
    )
