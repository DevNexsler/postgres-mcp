"""Durable provider-neutral outbound action orchestration."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any
from typing import Mapping
from typing import Protocol
from uuid import UUID

from .adapters.base import ProviderAdapter
from .adapters.base import ProviderDisposition
from .adapters.base import ProviderObservation
from .adapters.base import ProviderReceipt
from .context import ActionContext
from .context import ActionContextLoader
from .models import ActionRole
from .models import ActionState
from .models import CompletionKind
from .models import ExecuteRequest
from .models import IntentKind
from .models import Operation
from .models import PublicResult
from .preflight import PreflightDecision
from .preflight import PreflightEvidence
from .preflight import PreflightOutcome
from .preflight import SafetyPreflight
from .state_machine import public_result


@dataclass(frozen=True)
class OutboundActionRecord:
    action_id: UUID
    wakeup_event_id: int
    action_role: ActionRole
    operation: Operation
    intent_kind: IntentKind
    appointment_slot: datetime | None
    arguments: dict[str, Any]
    state: ActionState
    action_uid: UUID | None
    provider_request_ref: str | None
    provider_message_id: str | None
    completion_kind: CompletionKind | None
    detail_code: str
    attempt_count: int

    def execute_request(self) -> ExecuteRequest:
        return ExecuteRequest.model_validate(
            {
                "op": "execute",
                "wakeup_event_id": self.wakeup_event_id,
                "action_role": self.action_role,
                "operation": self.operation,
                "intent_kind": self.intent_kind,
                "appointment_slot": self.appointment_slot,
                "arguments": self.arguments,
            }
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

    async def get(self, action_id: UUID) -> OutboundActionRecord | None: ...


class PreflightEvidenceLoader(Protocol):
    async def load(self, context: ActionContext) -> PreflightEvidence: ...


Clock = Callable[[], datetime]
Sleeper = Callable[[float], Awaitable[None]]


class OutboundActionService:
    """State machine coordinator. Contains no provider-specific branches."""

    def __init__(
        self,
        *,
        store: ActionStore,
        context_loader: ActionContextLoader,
        evidence_loader: PreflightEvidenceLoader,
        adapters: Mapping[Operation, ProviderAdapter],
        provider_client: Any,
        clock: Clock,
        lease_owner: str,
        response_budget_seconds: float = 25,
        lease_seconds: int = 60,
        sleep: Sleeper = asyncio.sleep,
    ):
        self._store = store
        self._context_loader = context_loader
        self._evidence_loader = evidence_loader
        self._adapters = dict(adapters)
        self._provider_client = provider_client
        self._clock = clock
        self._lease_owner = lease_owner
        self._response_budget_seconds = max(0, min(response_budget_seconds, 29))
        self._lease_seconds = lease_seconds
        self._sleep = sleep

    async def execute(self, request: ExecuteRequest) -> PublicResult:
        context = await self._context_loader.load(request)
        action = await self._store.create_or_load(context)
        if action.state is ActionState.COMPLETED:
            return self._result(action, repeated=True)
        if action.state in {
            ActionState.STALE,
            ActionState.REJECTED,
            ActionState.DEFINITIVE_FAILED,
            ActionState.DEAD_LETTER,
            ActionState.MANUAL_REVIEW,
            ActionState.UNKNOWN,
            ActionState.RECONCILING,
            ActionState.DISPATCHING,
            ActionState.PROVIDER_ACCEPTED,
        }:
            return self._result(action)
        if action.state is ActionState.DEPENDENCY_WAIT:
            return await self._resume_dependency(action, context)
        if action.state in {ActionState.PREPARED, ActionState.RETRY_READY}:
            return await self._dispatch(action, context)
        return await self._preflight(action, context)

    async def status(self, action_id: UUID) -> PublicResult:
        return self._result(await self._require_action(action_id))

    async def resume(self, action_id: UUID) -> PublicResult:
        action = await self._require_action(action_id)
        context = await self._context_loader.load(action.execute_request())
        if action.state is ActionState.DEPENDENCY_WAIT:
            return await self._resume_dependency(action, context)
        if action.state in {ActionState.PREPARED, ActionState.RETRY_READY}:
            return await self._dispatch(action, context)
        return self._result(action)

    async def reconcile(self, action_id: UUID) -> PublicResult:
        action = await self._require_action(action_id)
        if action.state in {ActionState.DISPATCHING, ActionState.RECONCILING}:
            await self._store.claim(action.action_id, action.state, self._lease_owner, self._lease_seconds)
            action = await self._store.transition(
                action.action_id,
                action.state,
                ActionState.UNKNOWN,
                self._lease_owner,
                ProviderObservation(
                    ProviderDisposition.AMBIGUOUS,
                    "expired_dispatch_requires_reconciliation",
                    provider_request_ref=action.provider_request_ref,
                ),
            )
        if action.state is not ActionState.UNKNOWN:
            return self._result(action)
        context = await self._context_loader.load(action.execute_request())
        adapter = self._adapter(context.operation)
        await self._store.claim(action.action_id, action.state, self._lease_owner, self._lease_seconds)
        reconciling = await self._store.transition(
            action.action_id,
            ActionState.UNKNOWN,
            ActionState.RECONCILING,
            self._lease_owner,
            ProviderObservation(
                ProviderDisposition.AMBIGUOUS,
                "reconciliation_started",
                provider_request_ref=action.provider_request_ref,
            ),
        )
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

    async def _preflight(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        evidence = await self._evidence_loader.load(context)
        decision = SafetyPreflight.evaluate(context, evidence, now=self._clock())
        if decision.outcome is PreflightOutcome.READY:
            prepared = await self._store.prepare(context, action.state)
            if prepared.state is ActionState.COMPLETED:
                return self._result(prepared, repeated=True)
            return await self._dispatch(prepared, context)
        return await self._apply_preflight_decision(action, evidence, decision)

    async def _resume_dependency(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        evidence = await self._evidence_loader.load(context)
        decision = SafetyPreflight.evaluate(context, evidence, now=self._clock())
        if decision.outcome is PreflightOutcome.READY:
            prepared = await self._store.prepare(context, action.state)
            if prepared.state is ActionState.COMPLETED:
                return self._result(prepared, repeated=True)
            return await self._dispatch(prepared, context)
        if decision.outcome is PreflightOutcome.DEPENDENCY_WAIT:
            return public_result(
                state=action.state,
                action_id=action.action_id,
                action_uid=action.action_uid,
                provider_request_ref=action.provider_request_ref,
                detail_code=decision.detail_code,
            )
        return await self._apply_preflight_decision(action, evidence, decision)

    async def _apply_preflight_decision(
        self,
        action: OutboundActionRecord,
        evidence: PreflightEvidence,
        decision: PreflightDecision,
    ) -> PublicResult:
        if decision.outcome is PreflightOutcome.DUPLICATE:
            assert evidence.verified_outbound_request_ref is not None
            assert evidence.verified_outbound_message_id is not None
            receipt = ProviderReceipt(
                provider_request_ref=evidence.verified_outbound_request_ref,
                provider_message_id=str(evidence.verified_outbound_message_id),
                accepted_at=self._clock(),
                evidence={"kind": "verified_existing_outbound"},
            )
            completed = await self._store.complete(
                action.action_id,
                action.state,
                None,
                receipt,
                CompletionKind.DUPLICATE,
                decision.detail_code,
            )
            return self._result(completed, repeated=True)
        if decision.outcome in {PreflightOutcome.STALE, PreflightOutcome.REJECTED}:
            target = ActionState.STALE if decision.outcome is PreflightOutcome.STALE else ActionState.REJECTED
            transitioned = await self._store.transition(
                action.action_id,
                action.state,
                target,
                None,
                ProviderObservation(ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE, decision.detail_code),
            )
            return self._result(transitioned)
        dependency_code = decision.detail_code
        transitioned = await self._store.transition(
            action.action_id,
            action.state,
            ActionState.DEPENDENCY_WAIT,
            None,
            ProviderObservation(ProviderDisposition.PENDING, dependency_code),
        )
        return self._result(transitioned)

    async def _dispatch(self, action: OutboundActionRecord, context: ActionContext) -> PublicResult:
        adapter = self._adapter(context.operation)
        claimed = await self._store.claim(action.action_id, action.state, self._lease_owner, self._lease_seconds)
        dispatching = await self._store.transition(
            claimed.action_id,
            claimed.state,
            ActionState.DISPATCHING,
            self._lease_owner,
            ProviderObservation(ProviderDisposition.PENDING, "dispatch_started"),
        )
        if dispatching.action_uid is None:
            raise RuntimeError("prepared action has no deterministic action UID")
        provider_request = adapter.build_request(context, dispatching.action_uid)
        observation = await adapter.invoke(self._provider_client, provider_request)
        if observation.provider_request_ref:
            dispatching = await self._store.record_provider_request(
                dispatching.action_id,
                self._lease_owner,
                observation,
            )
        if observation.disposition is ProviderDisposition.PENDING:
            observation = await adapter.poll(self._provider_client, observation)
            if observation.provider_request_ref and observation.provider_request_ref != dispatching.provider_request_ref:
                dispatching = await self._store.record_provider_request(
                    dispatching.action_id,
                    self._lease_owner,
                    observation,
                )
            if observation.disposition is ProviderDisposition.PENDING:
                observation = ProviderObservation(
                    ProviderDisposition.AMBIGUOUS,
                    "provider_queue_timeout",
                    provider_request_ref=observation.provider_request_ref,
                    provider_call_id=observation.provider_call_id,
                )
        return await self._finish_observation(dispatching, context, adapter, observation)

    async def _finish_observation(
        self,
        action: OutboundActionRecord,
        context: ActionContext,
        adapter: ProviderAdapter,
        observation: ProviderObservation,
    ) -> PublicResult:
        expected_state = action.state
        if observation.disposition is ProviderDisposition.ACCEPTED:
            receipt = adapter.parse_receipt(context, observation)
            if receipt is None:
                observation = ProviderObservation(
                    ProviderDisposition.AMBIGUOUS,
                    "provider_receipt_missing",
                    provider_request_ref=observation.provider_request_ref,
                )
            else:
                if expected_state is ActionState.DISPATCHING:
                    accepted = await self._store.transition(
                        action.action_id,
                        expected_state,
                        ActionState.PROVIDER_ACCEPTED,
                        self._lease_owner,
                        observation,
                    )
                else:
                    accepted = action
                completed = await self._store.complete(
                    accepted.action_id,
                    accepted.state,
                    self._lease_owner,
                    receipt,
                    CompletionKind.SENT,
                    "provider_receipt_verified",
                )
                return self._result(completed)
        if observation.disposition is ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE:
            if observation.retryable:
                retry = await self._store.transition(
                    action.action_id,
                    expected_state,
                    ActionState.RETRY_READY,
                    self._lease_owner,
                    observation,
                )
                return self._result(retry)
            failed = await self._store.definitive_fail(
                action.action_id,
                expected_state,
                self._lease_owner,
                observation,
            )
            return self._result(failed)
        unknown = await self._store.transition(
            action.action_id,
            expected_state,
            ActionState.UNKNOWN,
            self._lease_owner,
            observation,
        )
        return self._result(unknown)

    def _adapter(self, operation: Operation) -> ProviderAdapter:
        adapter = self._adapters.get(operation)
        if adapter is None:
            raise ValueError(f"no outbound provider adapter configured for {operation.value}")
        return adapter

    async def _require_action(self, action_id: UUID) -> OutboundActionRecord:
        action = await self._store.get(action_id)
        if action is None:
            raise LookupError("outbound action does not exist")
        return action

    @staticmethod
    def evidence_hash(evidence: Mapping[str, Any] | None) -> str:
        return sha256(json.dumps(evidence or {}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @staticmethod
    def _result(action: OutboundActionRecord, *, repeated: bool = False) -> PublicResult:
        return public_result(
            state=action.state,
            action_id=action.action_id,
            action_uid=action.action_uid,
            provider_request_ref=action.provider_request_ref,
            detail_code=action.detail_code,
            completion_kind=action.completion_kind,
            repeated_execute=repeated,
        )
