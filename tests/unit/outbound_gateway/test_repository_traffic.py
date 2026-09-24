from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
from postgres_mcp.outbound_gateway.traffic_control import InFlightAction
from postgres_mcp.outbound_gateway.traffic_control import NewerActivity

ACTION_ID = UUID("ed6fcf85-39e7-5cdf-9fb8-ccca32a62e8d")
OTHER_ACTION_ID = UUID("11111111-2222-3333-4444-555555555555")


class Row:
    def __init__(self, cells):
        self.cells = cells


@pytest.mark.asyncio
async def test_in_flight_actions_builds_sql_and_maps_rows():
    row = Row(
        {
            "action_id": OTHER_ACTION_ID,
            "operation": "email.send",
            "state": "prepared",
            "created_at": datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
            "preview": "Friday at 10:30 works.",
        }
    )
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return [row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.in_flight_actions("email:amanda@example.com", ACTION_ID)

    assert len(calls) == 1
    query, params = calls[0]
    assert "outbound_actions" in query
    assert "subject_key" in query
    assert "action_id" in query
    assert "state" in query
    assert "ANY" in query
    assert params[0] == "email:amanda@example.com"
    assert params[1] == ACTION_ID
    assert isinstance(params[2], list) and set(params[2]) == {
        "received",
        "dependency_wait",
        "prepared",
        "dispatching",
        "provider_accepted",
        "unknown",
        "reconciling",
        "retry_ready",
    }
    assert result == [
        InFlightAction(
            action_id=OTHER_ACTION_ID,
            operation="email.send",
            state="prepared",
            created_at=datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
            preview="Friday at 10:30 works.",
        )
    ]


@pytest.mark.asyncio
async def test_in_flight_actions_returns_empty_list_when_no_rows():
    async def execute(_driver, query, params):
        return []

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.in_flight_actions("email:amanda@example.com", ACTION_ID)

    assert result == []


@pytest.mark.asyncio
async def test_newest_activity_after_prefers_the_more_recent_of_ledger_and_messages():
    watermark = datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc)
    ledger_row = Row(
        {
            "action_id": OTHER_ACTION_ID,
            "created_at": datetime(2026, 8, 27, 11, 0, tzinfo=timezone.utc),
            "preview": '{"to_address": "amanda@example.com"}',
        }
    )
    message_row = Row(
        {
            "message_id": 900,
            "created_at": datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
            "direction": "inbound",
            "preview": "Can we push to 11?",
        }
    )
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        if "subject_key = {}" in query:
            return [ledger_row]
        return [message_row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.newest_activity_after("email:amanda@example.com", 44, watermark, ACTION_ID)

    assert len(calls) == 2
    ledger_query, ledger_params = calls[0]
    assert "outbound_actions" in ledger_query
    assert "subject_key" in ledger_query
    assert "action_id" in ledger_query
    # CRITICAL 1b: never-dispatched terminals (e.g. a traffic-blocked
    # definitive_failed row) must not count as "outbound activity" and
    # cascade false-stales -- only rows that plausibly reached a provider
    # count. dispatch_started_at is set exactly at the prepared ->
    # dispatching transition (Comm-Data-Store migrations/
    # 067_outbound_action_gateway.sql:774-775), so "dispatch_started_at IS
    # NOT NULL" is the live signal for that, with 'completed' covering the
    # duplicate-completion path that never dispatches at all.
    assert "dispatch_started_at" in ledger_query
    assert "completed" in ledger_query
    assert "ORDER BY created_at DESC" in ledger_query
    # newest_activity_after is activity_after(limit=1): the limit is a parameter.
    assert "LIMIT {}" in ledger_query
    assert ledger_params == [ACTION_ID, "email:amanda@example.com", watermark, 1]

    messages_query, messages_params = calls[1]
    assert "messages" in messages_query
    assert "channel_id" in messages_query
    assert "ORDER BY message.created_at DESC" in messages_query
    assert "LIMIT {}" in messages_query
    assert messages_params == [ACTION_ID, 44, watermark, 1]

    assert result == NewerActivity(
        direction="inbound",
        source="messages",
        occurred_at=datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
        preview="Can we push to 11?",
        message_id=900,
        action_id=None,
    )


@pytest.mark.asyncio
async def test_newest_activity_after_picks_ledger_row_when_it_is_newer():
    watermark = datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc)
    ledger_row = Row(
        {
            "action_id": OTHER_ACTION_ID,
            "created_at": datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
            "preview": '{"to_address": "amanda@example.com"}',
        }
    )
    message_row = Row(
        {
            "message_id": 900,
            "created_at": datetime(2026, 8, 27, 11, 0, tzinfo=timezone.utc),
            "direction": None,
            "preview": "Can we push to 11?",
        }
    )

    async def execute(_driver, query, params):
        if "subject_key = {}" in query:
            return [ledger_row]
        return [message_row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.newest_activity_after("email:amanda@example.com", 44, watermark, ACTION_ID)

    assert result == NewerActivity(
        direction="outbound",
        source="outbound_actions",
        occurred_at=datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
        preview='{"to_address": "amanda@example.com"}',
        message_id=None,
        action_id=OTHER_ACTION_ID,
    )


@pytest.mark.asyncio
async def test_newest_activity_after_defaults_message_direction_to_unknown_when_null():
    """MINOR (b): NULL direction must not be silently reported as
    "inbound" -- that would make the staleness detail text claim inbound
    activity that was never actually confirmed as such."""
    watermark = datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc)
    message_row = Row(
        {
            "message_id": 901,
            "created_at": datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
            "direction": None,
            "preview": "hi",
        }
    )

    async def execute(_driver, query, params):
        if "subject_key = {}" in query:
            return []
        return [message_row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.newest_activity_after("email:amanda@example.com", 44, watermark, ACTION_ID)

    assert result.direction == "unknown"


@pytest.mark.asyncio
async def test_newest_activity_after_excludes_the_calling_action_id_from_the_ledger_query():
    """CRITICAL 1: execute() persists the action's own durable row before
    the traffic gate runs (created_at=now(), definitely > any watermark) --
    without excluding it by action_id, the ledger query would find that
    row as "newer outbound activity" and self-block every real send. Fake-
    driver assertion that the exclusion parameter actually reaches SQL."""
    watermark = datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc)
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return []

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        await repository.newest_activity_after("email:amanda@example.com", 44, watermark, ACTION_ID)

    ledger_query, ledger_params = calls[0]
    assert "WITH RECURSIVE retry_lineage" in ledger_query
    assert "retry_of_action_id" in ledger_query
    assert "action_id NOT IN" in ledger_query
    assert ACTION_ID in ledger_params


@pytest.mark.asyncio
async def test_newest_activity_after_excludes_retry_ancestors_from_the_ledger_query():
    """A remediation successor must not see its authoritatively failed
    predecessor as newer outbound traffic and permanently self-block."""
    watermark = datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc)
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return []

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        await repository.newest_activity_after("email:amanda@example.com", 44, watermark, ACTION_ID)

    ledger_query, ledger_params = calls[0]
    assert "JOIN retry_lineage AS child" in ledger_query
    assert "ancestor.action_id = child.retry_of_action_id" in ledger_query
    assert "SELECT action_id FROM retry_lineage" in ledger_query
    assert ledger_params == [ACTION_ID, "email:amanda@example.com", watermark, 1]


@pytest.mark.asyncio
async def test_newest_activity_after_returns_none_when_both_probes_are_empty():
    watermark = datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc)

    async def execute(_driver, query, params):
        return []

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.newest_activity_after("email:amanda@example.com", 44, watermark, ACTION_ID)

    assert result is None


@pytest.mark.asyncio
async def test_context_watermark_returns_coalesced_timestamp():
    row = Row({"watermark": datetime(2026, 8, 27, 9, 0, tzinfo=timezone.utc)})
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return [row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.context_watermark(12345)

    assert len(calls) == 1
    query, params = calls[0]
    assert "hermes_wakeup_events" in query
    assert "webui_accepted_at" in query
    assert "created_at" in query
    assert params == [12345]
    assert result == datetime(2026, 8, 27, 9, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_context_watermark_returns_none_when_no_row():
    async def execute(_driver, query, params):
        return []

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.context_watermark(12345)

    assert result is None


@pytest.mark.asyncio
async def test_activity_after_lists_both_arms_newest_first_with_sender_and_limit():
    watermark = datetime(2026, 9, 23, 19, 36, 29, tzinfo=timezone.utc)
    ledger_rows = [
        Row(
            {
                "action_id": OTHER_ACTION_ID,
                "operation": "cliq.chat.post",
                "created_at": datetime(2026, 9, 23, 19, 36, 40, tzinfo=timezone.utc),
                "preview": "earlier reply",
            }
        )
    ]
    message_rows = [
        Row(
            {
                "message_id": 750825,
                "created_at": datetime(2026, 9, 23, 19, 38, 56, tzinfo=timezone.utc),
                "direction": "inbound",
                "source": "zoho_cliq",
                "sender_name": "Nigel",
                "preview": "second alert",
            }
        ),
        Row(
            {
                "message_id": 750824,
                "created_at": datetime(2026, 9, 23, 19, 36, 56, tzinfo=timezone.utc),
                "direction": "inbound",
                "source": "zoho_cliq",
                "sender_name": None,
                "preview": "first alert",
            }
        ),
    ]
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return ledger_rows if "subject_key = {}" in query else message_rows

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        items = await repository.activity_after("internal:1424728044450751028", 417, watermark, ACTION_ID, 2)

    assert [item.ref for item in items] == ["message:750825", "message:750824"]
    assert items[0].sender == "Nigel" and items[0].source == "zoho_cliq"
    assert items[1].sender is None
    assert calls[0][1][-1] == 2 and calls[1][1][-1] == 2
    assert "left(coalesce(message.body,''), 300)" in calls[1][0]
    assert "participants AS sender" in calls[1][0]


@pytest.mark.asyncio
async def test_acknowledged_through_reads_stale_rows_of_this_wake_and_recipient():
    calls = []
    point = datetime(2026, 9, 23, 19, 36, 56, tzinfo=timezone.utc)

    async def execute(_driver, query, params):
        calls.append((query, params))
        return [Row({"acknowledged_through": point})]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.acknowledged_through(27164, "internal:1424728044450751028")

    assert result == point
    query, params = calls[0]
    assert "max(stale_context_acknowledged_through)" in query
    assert "state = 'stale'" in query
    assert params == [27164, "internal:1424728044450751028"]
