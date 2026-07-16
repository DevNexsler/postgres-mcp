from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.worker import OutboundWorker


@pytest.mark.asyncio
async def test_worker_never_redispatches_unknown_action_and_reconciles_it():
    action_id = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")
    store = AsyncMock()
    store.list_work.return_value = [(action_id, ActionState.UNKNOWN)]
    service = AsyncMock()
    worker = OutboundWorker(store=store, service=service, batch_size=20)

    count = await worker.run_once()

    assert count == 1
    service.reconcile.assert_awaited_once_with(action_id)
    service.resume.assert_not_called()


@pytest.mark.asyncio
async def test_worker_resumes_only_prepared_retry_and_dependency_states():
    ids = [UUID(int=index) for index in range(1, 4)]
    store = AsyncMock()
    store.list_work.return_value = [
        (ids[0], ActionState.PREPARED),
        (ids[1], ActionState.RETRY_READY),
        (ids[2], ActionState.DEPENDENCY_WAIT),
    ]
    service = AsyncMock()
    worker = OutboundWorker(store=store, service=service, batch_size=20)

    assert await worker.run_once() == 3
    assert service.resume.await_count == 3
    service.reconcile.assert_not_called()
