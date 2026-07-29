"""Bounded durable outbound reconciliation worker."""

from __future__ import annotations

import json
from collections.abc import Callable
from inspect import isawaitable
from typing import Protocol
from uuid import UUID

from .models import ActionState
from .service import OutboundActionService
from .store import LeaseContentionError


class WorkerStore(Protocol):
    async def list_work(self, limit: int, max_attempts: int) -> list[tuple[UUID, ActionState]]: ...

    async def list_exhausted(self, limit: int, max_attempts: int) -> list[tuple[UUID, ActionState]]: ...


class OutboundWorker:
    def __init__(
        self,
        *,
        store: WorkerStore,
        service: OutboundActionService,
        batch_size: int = 20,
        max_attempts: int = 5,
        lease_owner: str = "outbound-gateway",
        observability=None,
        on_error: Callable[[UUID, str, ActionState, str, Exception], None] | None = None,
        on_contention: Callable[[UUID, str, ActionState, str], None] | None = None,
    ):
        self._store = store
        self._service = service
        self._batch_size = max(1, min(batch_size, 100))
        self._max_attempts = max(1, min(max_attempts, 100))
        self._lease_owner = lease_owner
        self._observability = observability
        self._on_error = on_error or self._default_error
        self._on_contention = on_contention or self._default_contention

    @staticmethod
    def _default_error(
        action_id: UUID,
        operation: str,
        expected_state: ActionState,
        lease_owner: str,
        error: Exception,
    ) -> None:
        sqlstate = getattr(error, "sqlstate", None)
        detail_code = (
            f"database_sqlstate_{str(sqlstate).lower()}"
            if sqlstate
            else "unexpected_runtime_error"
        )
        print(
            json.dumps(
                {
                    "action_id": str(action_id),
                    "detail_code": detail_code,
                    "error_type": type(error).__name__,
                    "event": "outbound_worker_action_failed",
                    "expected_state": expected_state.value,
                    "lease_owner": lease_owner,
                    "level": "error",
                    "operation": operation,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    @staticmethod
    def _default_contention(
        action_id: UUID,
        operation: str,
        expected_state: ActionState,
        lease_owner: str,
    ) -> None:
        print(
            json.dumps(
                {
                    "action_id": str(action_id),
                    "detail_code": "lease_contended",
                    "event": "outbound_worker_lease_contended",
                    "expected_state": expected_state.value,
                    "lease_owner": lease_owner,
                    "level": "info",
                    "operation": operation,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    async def _run_isolated(
        self,
        action_id: UUID,
        operation: str,
        expected_state: ActionState,
    ) -> None:
        try:
            await getattr(self._service, operation)(action_id)
        except LeaseContentionError:
            self._on_contention(
                action_id,
                operation,
                expected_state,
                self._lease_owner,
            )
            if self._observability is not None:
                recorded = self._observability.record_lease_contention()
                if isawaitable(recorded):
                    await recorded
        except Exception as exc:
            self._on_error(
                action_id,
                operation,
                expected_state,
                self._lease_owner,
                exc,
            )

    async def run_once(self) -> int:
        exhausted = await self._store.list_exhausted(self._batch_size, self._max_attempts)
        for action_id, state in exhausted:
            await self._run_isolated(action_id, "exhaust", state)
        work = await self._store.list_work(self._batch_size, self._max_attempts)
        for action_id, state in work:
            if state in {
                ActionState.UNKNOWN,
                ActionState.RECONCILING,
                ActionState.DISPATCHING,
                ActionState.PROVIDER_ACCEPTED,
            }:
                await self._run_isolated(action_id, "reconcile", state)
            elif state in {ActionState.PREPARED, ActionState.RETRY_READY, ActionState.DEPENDENCY_WAIT}:
                await self._run_isolated(action_id, "resume", state)
        if self._observability is not None:
            alerts = await self._observability.scan_alerts()
            for alert in alerts:
                print(alert.as_json(), flush=True)
        return len(exhausted) + len(work)
