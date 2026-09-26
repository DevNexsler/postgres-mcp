from __future__ import annotations

import logging
import re
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.state_machine import ALLOWED_TRANSITIONS
from postgres_mcp.outbound_gateway.store import PostgresActionStore
from postgres_mcp.outbound_gateway.worker import OutboundWorker


@pytest.mark.asyncio
async def test_worker_never_redispatches_unknown_action_and_reconciles_it():
    action_id = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")
    store = AsyncMock()
    store.list_exhausted.return_value = []
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
    store.list_exhausted.return_value = []
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


@pytest.mark.asyncio
async def test_worker_exhausts_retry_budget_before_listing_due_work():
    exhausted_id = UUID(int=9)
    store = AsyncMock()
    store.list_exhausted.return_value = [(exhausted_id, ActionState.UNKNOWN)]
    store.list_work.return_value = []
    service = AsyncMock()
    worker = OutboundWorker(store=store, service=service, batch_size=20, max_attempts=5)

    assert await worker.run_once() == 1
    store.list_exhausted.assert_awaited_once_with(20, 5)
    store.list_work.assert_awaited_once_with(20, 5)
    service.exhaust.assert_awaited_once_with(exhausted_id)
    service.reconcile.assert_not_called()
    service.resume.assert_not_called()


@pytest.mark.asyncio
async def test_worker_isolates_poison_action_and_continues_batch():
    poison = UUID(int=21)
    healthy = UUID(int=22)
    store = AsyncMock()
    store.list_exhausted.return_value = []
    store.list_work.return_value = [
        (poison, ActionState.UNKNOWN),
        (healthy, ActionState.UNKNOWN),
    ]
    service = AsyncMock()
    service.reconcile.side_effect = [RuntimeError("poison"), None]
    failures = []
    worker = OutboundWorker(
        store=store,
        service=service,
        batch_size=20,
        on_error=lambda action_id, operation, error: failures.append((action_id, operation, type(error).__name__)),
    )

    assert await worker.run_once() == 2
    assert service.reconcile.await_args_list[1].args == (healthy,)
    assert failures == [(poison, "reconcile", "RuntimeError")]


@pytest.mark.asyncio
async def test_worker_delegates_tenantcloud_work_to_restate() -> None:
    action_id = UUID(int=31)
    store = AsyncMock()
    store.list_exhausted.return_value = []
    store.list_work.return_value = [(action_id, ActionState.RETRY_READY)]
    store.get.return_value = type("Action", (), {"operation": Operation.TENANTCLOUD_MESSAGE_SEND})()
    service = AsyncMock()
    submitter = AsyncMock()
    worker = OutboundWorker(
        store=store,
        service=service,
        tenantcloud_submitter=submitter,
    )

    assert await worker.run_once() == 1
    submitter.submit.assert_awaited_once_with(action_id)
    service.resume.assert_not_called()
    service.reconcile.assert_not_called()


def test_default_error_line_names_the_error(capsys):
    OutboundWorker._default_error(UUID(int=7), "reconcile", KeyError("nigel-zoho"))
    line = capsys.readouterr().out.strip()
    assert '"error_type": "KeyError"' in line
    assert "nigel-zoho" in line


# ----------------------------------------------------------------------------
# No state the gateway leaves an action in is invisible to the worker unless
# nobody should drive it: terminal, parked for a person, or `received` -- the
# agent's own execute call. A pre-send error that leaves a row `received`
# says "not sent" to the agent, and executing again re-runs the send
# (test_service).
# ----------------------------------------------------------------------------


async def _listed_states(method: str) -> set[ActionState]:
    queries = []

    async def execute(_driver, query, params):
        queries.append(query)
        return []

    with patch("postgres_mcp.outbound_gateway.store.SafeSqlDriver.execute_param_query", AsyncMock(side_effect=execute)):
        await getattr(PostgresActionStore(object()), method)(20, 5)
    in_list = re.search(r"state IN \(([^)]*)\)", queries[0])
    assert in_list is not None
    return {ActionState(value) for value in re.findall(r"'([a-z_]+)'", in_list.group(1))}


TERMINAL_STATES = {state for state, targets in ALLOWED_TRANSITIONS.items() if not targets}
PARKED_FOR_A_PERSON = {ActionState.DEAD_LETTER, ActionState.MANUAL_REVIEW}
THE_AGENTS_OWN_CALL = {ActionState.RECEIVED}


@pytest.mark.asyncio
async def test_every_state_is_worker_driven_terminal_parked_or_the_agents_own_call():
    work = await _listed_states("list_work")
    assert await _listed_states("list_exhausted") == work
    classes = [work, TERMINAL_STATES, PARKED_FOR_A_PERSON, THE_AGENTS_OWN_CALL]
    assert set().union(*classes) == set(ActionState)
    assert sum(len(group) for group in classes) == len(ActionState), "a state is in two classes"
    assert TERMINAL_STATES == {
        ActionState.COMPLETED,
        ActionState.STALE,
        ActionState.REJECTED,
        ActionState.DEFINITIVE_FAILED,
    }


@pytest.mark.asyncio
async def test_the_worker_drives_every_state_it_lists():
    work = sorted(await _listed_states("list_work"))
    ids = {state: UUID(int=100 + index) for index, state in enumerate(work)}
    store = AsyncMock()
    store.list_exhausted.return_value = []
    store.list_work.return_value = [(ids[state], state) for state in work]
    service = AsyncMock()

    assert await OutboundWorker(store=store, service=service).run_once() == len(work)
    driven = {call.args[0] for call in service.resume.await_args_list + service.reconcile.await_args_list}
    assert driven == set(ids.values())


@pytest.mark.asyncio
async def test_a_swallowed_worker_error_is_logged_at_error_with_the_action_id(caplog):
    action_id = UUID(int=41)
    store = AsyncMock()
    store.list_exhausted.return_value = []
    store.list_work.return_value = [(action_id, ActionState.RETRY_READY)]
    service = AsyncMock()
    service.resume.side_effect = TypeError("boom")
    worker = OutboundWorker(store=store, service=service, on_error=lambda *_args: None)

    with caplog.at_level(logging.ERROR):
        assert await worker.run_once() == 1

    assert any(record.levelno == logging.ERROR and str(action_id) in record.getMessage() for record in caplog.records)
