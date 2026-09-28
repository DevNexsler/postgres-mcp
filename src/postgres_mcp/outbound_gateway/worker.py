"""Bounded durable outbound reconciliation worker."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any
from typing import Protocol
from uuid import UUID

from .models import ActionState
from .models import Operation
from .service import OutboundActionService
from .tenantcloud_shared import TENANTCLOUD_OPERATIONS

logger = logging.getLogger(__name__)


class WorkerStore(Protocol):
    async def list_work(self, limit: int, max_attempts: int) -> list[tuple[UUID, ActionState]]: ...

    async def list_exhausted(self, limit: int, max_attempts: int) -> list[tuple[UUID, ActionState]]: ...

    async def get(self, action_id: UUID) -> Any: ...


class TenantCloudSubmitter(Protocol):
    async def submit(self, action_id: UUID) -> None: ...


class OutboundWorker:
    def __init__(
        self,
        *,
        store: WorkerStore,
        service: OutboundActionService,
        batch_size: int = 20,
        max_attempts: int = 5,
        observability=None,
        on_error: Callable[[UUID, str, Exception], None] | None = None,
        tenantcloud_submitter: TenantCloudSubmitter | None = None,
        restate_operations: frozenset[Operation] | None = None,
    ):
        self._store = store
        self._service = service
        self._batch_size = max(1, min(batch_size, 100))
        self._max_attempts = max(1, min(max_attempts, 100))
        self._observability = observability
        self._on_error = on_error or self._default_error
        self._tenantcloud_submitter = tenantcloud_submitter
        # Which operations this worker hands off to Restate instead of
        # driving itself. Always includes TenantCloud (unconditional, exactly
        # as before OUTBOUND_RESTATE_OPERATIONS existed); server.py unions in
        # whatever the flag names for the generalized rollout. Backward
        # compatible: a caller that never passes this keeps today's
        # TenantCloud-only behavior.
        self._restate_operations = (
            TENANTCLOUD_OPERATIONS if restate_operations is None else (TENANTCLOUD_OPERATIONS | restate_operations)
        )

    @staticmethod
    def _default_error(action_id: UUID, operation: str, error: Exception) -> None:
        print(
            json.dumps(
                {
                    "action_id": str(action_id),
                    "error": str(error)[:500],
                    "error_type": type(error).__name__,
                    "event": "outbound_worker_action_failed",
                    "operation": operation,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    async def _run_isolated(self, action_id: UUID, operation: str) -> None:
        try:
            action = await self._store.get(action_id)
            if action is not None and action.operation in self._restate_operations:
                # Restate (delivery_workflow.py's OutboundDeliveryCoordinator)
                # owns every retry/backoff/ceiling decision for this
                # operation -- retry_policy.py's one-hour elapsed ceiling,
                # not this worker's count-based 5/12-attempt cap
                # (list_work/list_exhausted). Falling through to
                # self._service.reconcile()/.exhaust() here would drive the
                # SAME row through the legacy count-based path concurrently
                # with (and much faster than) the workflow's own ceiling,
                # landing an unresolved ambiguity in dead_letter/manual_review
                # in minutes instead of the intended hour -- exactly the
                # 2026-09-28 cliq.channel.post incident this guard exists
                # for. A misconfigured submitter (unset
                # OUTBOUND_TENANTCLOUD_RESTATE_INGRESS_URL on this process
                # while the flag names the operation) is a deploy error to
                # surface loudly, never a silent fallback to the old path.
                if self._tenantcloud_submitter is None:
                    self._on_error(
                        action_id,
                        operation,
                        RuntimeError(
                            f"action {action_id} (operation {action.operation.value}) is Restate-flagged "
                            "but this worker has no tenantcloud_submitter configured; refusing to drive "
                            "it with the legacy count-based worker"
                        ),
                    )
                    return
                await self._tenantcloud_submitter.submit(action_id)
                return
            await getattr(self._service, operation)(action_id)
        except Exception as exc:
            logger.error("outbound worker %s failed for action %s", operation, action_id, exc_info=True)
            self._on_error(action_id, operation, exc)

    async def run_once(self) -> int:
        exhausted = await self._store.list_exhausted(self._batch_size, self._max_attempts)
        for action_id, _state in exhausted:
            await self._run_isolated(action_id, "exhaust")
        work = await self._store.list_work(self._batch_size, self._max_attempts)
        for action_id, state in work:
            if state in {
                ActionState.UNKNOWN,
                ActionState.RECONCILING,
                ActionState.DISPATCHING,
                ActionState.PROVIDER_ACCEPTED,
            }:
                await self._run_isolated(action_id, "reconcile")
            elif state in {ActionState.PREPARED, ActionState.RETRY_READY, ActionState.DEPENDENCY_WAIT}:
                await self._run_isolated(action_id, "resume")
        if self._observability is not None:
            alerts = await self._observability.scan_alerts()
            for alert in alerts:
                print(alert.as_json(), flush=True)
        return len(exhausted) + len(work)
