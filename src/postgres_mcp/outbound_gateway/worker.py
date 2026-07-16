"""Bounded durable outbound reconciliation worker."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from .models import ActionState
from .service import OutboundActionService


class WorkerStore(Protocol):
    async def list_work(self, limit: int) -> list[tuple[UUID, ActionState]]: ...


class OutboundWorker:
    def __init__(self, *, store: WorkerStore, service: OutboundActionService, batch_size: int = 20):
        self._store = store
        self._service = service
        self._batch_size = max(1, min(batch_size, 100))

    async def run_once(self) -> int:
        work = await self._store.list_work(self._batch_size)
        for action_id, state in work:
            if state in {ActionState.UNKNOWN, ActionState.RECONCILING, ActionState.DISPATCHING}:
                await self._service.reconcile(action_id)
            elif state in {ActionState.PREPARED, ActionState.RETRY_READY, ActionState.DEPENDENCY_WAIT}:
                await self._service.resume(action_id)
        return len(work)
