from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

import pytest
from pglast import parse_sql

from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
from postgres_mcp.outbound_gateway.traffic_control import InFlightAction

from .test_evidence import context

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
    # The same id again names the wake whose own actions never block it.
    assert params[2] == ACTION_ID
    assert "wakeup_event_id IS DISTINCT FROM" in query
    assert isinstance(params[3], list) and set(params[3]) == {
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


async def _newer(rows, **kwargs):
    calls = []

    async def execute(_driver, query, params):
        calls.append((query, params))
        return rows

    repository = OutboundGatewayRepository(object())
    with patch(
        "postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=execute),
    ):
        items = await repository.newer_context(kwargs.pop("ctx", None) or context(), limit=kwargs.pop("limit", 11), **kwargs)
    return items, calls


@pytest.mark.asyncio
async def test_newer_context_is_one_query_binding_the_action_its_recipient_and_the_shown_waiver():
    ctx = context(cross_channel_duplicate_message_ids=(196337,), certified_older_message_ids=(5,))
    _items, calls = await _newer([], ctx=ctx, waive_shown=True)

    assert len(calls) == 1
    query, params = calls[0]
    parse_sql(query.replace("{}", "NULL"))
    assert params == [
        7,
        ctx.action_id,
        "prospect:amanda",
        44,
        ctx.source_sent_at,
        700,
        "zillow",
        "lead@convo.zillow.com",
        "nigel-zoho",
        "zrm-thread-1",
        "",
        "email.send",
        [700, 196337],
        [5],
        ["zoho_mail", "nigel_mail"],
        "email",
        "lead@convo.zillow.com",
        True,
        None,
        11,
    ]
    for clause in (
        "coalesce(event.webui_accepted_at, event.created_at)",  # the wake's context watermark
        "message.channel_id = p.channel_id AND message.received_at > watermark.at",  # the wake's channel
        "seen.received_at <= watermark.at",  # a re-ingest / re-scrape of what had reached CDS
        "(message.sent_at, message.id) > (p.source_sent_at, p.source_message_id)",  # other channels
        "unnest(action.stale_context_shown_refs)",  # shown, by identity
        "coalesce(related.canonical_message_id, related.id) = ANY(p.equivalent_ids)",
        "related.id = ANY(p.certified_older_ids)",
        "message.body LIKE '⚠️ Cron issue —%'",
        "agency.label = 'nigel-zoho'",
        "candidate.source = ANY(p.sent_sources)",
        "ledger.dispatch_started_at IS NOT NULL OR ledger.state = 'completed'",
        "retry_lineage",
    ):
        assert clause in query, clause


@pytest.mark.asyncio
async def test_newer_context_maps_received_messages_and_our_sends_newest_first():
    rows = [
        Row({"message_id": 750826, "action_id": None, "created_at": datetime(2026, 9, 23, 19, 38, tzinfo=timezone.utc),
             "direction": "inbound", "source": "zoho_cliq", "sender": "Dan Park", "preview": "skip the pong"}),
        Row({"message_id": None, "action_id": OTHER_ACTION_ID, "created_at": datetime(2026, 9, 23, 19, 37, tzinfo=timezone.utc),
             "direction": "outbound", "source": "outbound_actions", "sender": "outbound gateway (cliq.chat.post)",
             "preview": "earlier reply"}),
    ]
    items, calls = await _newer(rows, waive_shown=False, limit=2)

    assert [(item.ref, item.label) for item in items] == [
        ("message:750826", "received"),
        (f"action:{OTHER_ACTION_ID}", "sent by us"),
    ]
    assert items[0].sender == "Dan Park"
    assert calls[0][1][-3:] == [False, None, 2]
