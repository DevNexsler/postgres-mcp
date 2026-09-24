"""Guard the historical fixture against drift from the live repository SQL."""

import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest.mock import AsyncMock
from uuid import UUID

import psycopg
import pytest
from historical import backtest_gateway_traffic as backtest
from integration.test_gateway_traffic_isolation import traffic_database as _traffic_database
from psycopg.rows import dict_row

from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
from postgres_mcp.sql import SqlDriver

traffic_database = _traffic_database

AT = datetime(2026, 9, 9, tzinfo=timezone.utc)


def action(number, parent=None):
    return {
        "action_id": str(UUID(int=number)),
        "retry_of_action_id": str(UUID(int=parent)) if parent else None,
        "subject_key": "subject:test",
        "operation": "email.send",
        "created_at": AT + timedelta(seconds=number),
        "phone": None,
        "dispatch_started_at": AT + timedelta(seconds=number),
        "state": "definitive_failed",
        "watermark": AT,
    }


def snapshot():
    actions = [action(1), action(2, 1), action(3, 2), action(4)]
    return {
        "actions": actions,
        "attempts": [
            {"attempt_id": n, "action_id": a["action_id"], "created_at": a["created_at"], "to_state": a["state"]} for n, a in enumerate(actions, 1)
        ],
        "messages": [],
        "wakes": [],
    }


@pytest.mark.asyncio
async def test_legacy_snapshot_requires_recapture_before_creating_schema():
    data = snapshot()
    del data["actions"][1]["retry_of_action_id"]
    conn = AsyncMock()
    with pytest.raises(ValueError, match="recapture"):
        await backtest.load_snapshot(conn, data)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_retry_ancestor_requires_recapture():
    data = snapshot()
    data["actions"].pop(0)
    conn = AsyncMock()
    with pytest.raises(ValueError, match="recapture"):
        await backtest.load_snapshot(conn, data)
    conn.execute.assert_not_awaited()


def test_expected_decision_excludes_only_retry_ancestry():
    data = snapshot()["actions"]
    assert backtest.expected_decision(data[2], data[:3], []) == "pass"
    assert backtest.expected_decision(data[2], data, []) == "stale_context"
    data[0]["state"] = "prepared"
    assert backtest.expected_decision(data[2], data[:3], []) == "lease_held"


def test_expected_decision_handles_cycles_and_uuid_rows():
    data = snapshot()["actions"][:3]
    data[0]["retry_of_action_id"] = data[1]["action_id"]
    visible = [{**a, "action_id": UUID(a["action_id"])} for a in data]
    assert backtest.expected_decision(data[2], visible, []) == "pass"


def test_capture_preserves_real_retry_parents(traffic_database, monkeypatch, tmp_path):
    # Minimal source schema, independent of the replay schema being tested.
    with psycopg.connect(traffic_database, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA capture_test")
        conn.execute("SET search_path=capture_test")
        conn.execute("""
            CREATE TABLE outbound_actions (
                action_id uuid, retry_of_action_id uuid, wakeup_event_id bigint,
                subject_key text, operation text, created_at timestamptz,
                canonical_context jsonb, dispatch_started_at timestamptz,
                state text, detail_code text, provenance jsonb
            );
            CREATE TABLE outbound_action_attempts (
                attempt_id bigint, action_id uuid, created_at timestamptz,
                from_state text, to_state text, event_kind text
            );
            CREATE TABLE hermes_wakeup_events (id bigint, created_at timestamptz, webui_accepted_at timestamptz);
            CREATE TABLE messages (
                id bigint, channel_id bigint, created_at timestamptz, updated_at timestamptz,
                direction text, source text, raw_event_id bigint
            );
            CREATE TABLE raw_events (id bigint, payload jsonb);
        """)
        for a in snapshot()["actions"]:
            conn.execute(
                "INSERT INTO outbound_actions (action_id,retry_of_action_id,subject_key,created_at) VALUES (%s,%s,%s,%s)",
                (a["action_id"], a["retry_of_action_id"], a["subject_key"], a["created_at"]),
            )
        monkeypatch.setenv("GATEWAY_BACKTEST_PRODUCTION_DSN", traffic_database + " options='-c search_path=capture_test'")
        path = tmp_path / "snapshot.json"
        try:
            backtest.capture(path)
            captured = json.loads(path.read_text())["actions"]
            assert [a["retry_of_action_id"] for a in captured] == [a["retry_of_action_id"] for a in snapshot()["actions"]]
        finally:
            conn.execute("DROP SCHEMA capture_test CASCADE")


@pytest.mark.asyncio
async def test_current_repository_sql_against_temporal_replay(traffic_database):
    # Rollback removes every replay object even if a SQL/assertion failure occurs.
    async with await psycopg.AsyncConnection.connect(traffic_database) as conn:
        try:
            await backtest.load_snapshot(conn, snapshot())
            repository = OutboundGatewayRepository(SqlDriver(conn=conn))
            await conn.execute("UPDATE replay_clock SET at=%s", (AT + timedelta(seconds=3),))
            # Execute production SQL first: pre-fix fixture raises UndefinedColumn.
            assert await repository.newest_activity_after("subject:test", 1, AT, UUID(int=3)) is None
            async with conn.cursor(row_factory=dict_row) as cursor:
                rows = await (await cursor.execute("SELECT * FROM outbound_actions ORDER BY created_at")).fetchall()
            assert [r["retry_of_action_id"] for r in rows] == [None, UUID(int=1), UUID(int=2)]
            assert len(rows) == 3  # Future unrelated send must stay invisible.
            assert backtest.expected_decision(snapshot()["actions"][2], rows, []) == "pass"
            await conn.execute("UPDATE replay_clock SET at=%s", (AT + timedelta(seconds=4),))
            activity = await repository.newest_activity_after("subject:test", 1, AT, UUID(int=3))
            assert activity is not None and activity.action_id == UUID(int=4)
            assert await repository.in_flight_actions("subject:test", UUID(int=3)) == []
        finally:
            await conn.rollback()
