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
        if "outbound_actions" in query:
            return [ledger_row]
        return [message_row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.newest_activity_after(
            "email:amanda@example.com", 44, watermark
        )

    assert len(calls) == 2
    ledger_query, ledger_params = calls[0]
    assert "outbound_actions" in ledger_query
    assert "subject_key" in ledger_query
    assert "rejected" in ledger_query
    assert "ORDER BY created_at DESC" in ledger_query
    assert "LIMIT 1" in ledger_query
    assert ledger_params == ["email:amanda@example.com", watermark]

    messages_query, messages_params = calls[1]
    assert "messages" in messages_query
    assert "channel_id" in messages_query
    assert "ORDER BY created_at DESC" in messages_query
    assert "LIMIT 1" in messages_query
    assert messages_params == [44, watermark]

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
        if "outbound_actions" in query:
            return [ledger_row]
        return [message_row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.newest_activity_after(
            "email:amanda@example.com", 44, watermark
        )

    assert result == NewerActivity(
        direction="outbound",
        source="outbound_actions",
        occurred_at=datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc),
        preview='{"to_address": "amanda@example.com"}',
        message_id=None,
        action_id=OTHER_ACTION_ID,
    )


@pytest.mark.asyncio
async def test_newest_activity_after_defaults_message_direction_to_inbound_when_null():
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
        if "outbound_actions" in query:
            return []
        return [message_row]

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        result = await repository.newest_activity_after(
            "email:amanda@example.com", 44, watermark
        )

    assert result.direction == "inbound"


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
        result = await repository.newest_activity_after(
            "email:amanda@example.com", 44, watermark
        )

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
