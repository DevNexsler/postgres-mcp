"""Traffic decisions against PostgreSQL, with no mocked SQL or traffic probe.

Run: uv run pytest tests/integration/test_gateway_traffic_isolation.py -q
Docker required. Uses a disposable PostgreSQL container, never an application DB.
"""

import logging
import time
from datetime import datetime
from datetime import timezone
from uuid import UUID

import docker
import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb

from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
from postgres_mcp.outbound_gateway.traffic_control import check_traffic
from postgres_mcp.sql import SqlDriver

ACTION = UUID("10000000-0000-0000-0000-000000000001")
SUBJECT = "subject:jessica"
JESSICA = "+12025550101"
GUNTHER = "+12025550102"
LINE = "+12025550100"
WATERMARK = datetime(2026, 9, 9, 23, 9, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def traffic_database():
    client = docker.from_env()
    container = client.containers.run(
        "postgres:16",
        detach=True,
        remove=True,
        environment={"POSTGRES_HOST_AUTH_METHOD": "trust"},
        ports={"5432/tcp": ("127.0.0.1", None)},
    )
    try:
        container.reload()
        port = container.ports["5432/tcp"][0]["HostPort"]
        dsn = f"host=127.0.0.1 port={port} user=postgres dbname=postgres"
        deadline = time.monotonic() + 30
        while True:
            try:
                with psycopg.connect(dsn, connect_timeout=1):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        yield dsn
    finally:
        container.stop(timeout=1)
        client.close()


@pytest_asyncio.fixture
async def traffic(traffic_database):
    async with await psycopg.AsyncConnection.connect(traffic_database, autocommit=True) as conn:
        # Connection-local tables mirror the production columns used by the probe.
        await conn.execute("""
            CREATE TEMP TABLE messages (
                id bigint PRIMARY KEY, channel_id bigint, created_at timestamptz,
                direction text, body text, source text, raw_event_id bigint,
                sender_participant_id bigint, recipient_participant_id bigint
            );
            CREATE TEMP TABLE raw_events (id bigint PRIMARY KEY, payload jsonb);
            CREATE TEMP TABLE participants (id bigint PRIMARY KEY, participant_key text, participant_type text, display_name text);
            CREATE TEMP TABLE outbound_actions (
                action_id uuid PRIMARY KEY, subject_key text, operation text, state text,
                created_at timestamptz, arguments jsonb, canonical_context jsonb,
                dispatch_started_at timestamptz, retry_of_action_id uuid,
                wakeup_event_id bigint DEFAULT 26817,
                stale_context_acknowledged_through timestamptz
            );
            CREATE TEMP TABLE hermes_wakeup_events (
                id bigint PRIMARY KEY, webui_accepted_at timestamptz, created_at timestamptz
            );
        """)
        await conn.execute("INSERT INTO hermes_wakeup_events VALUES (26817, %s, %s)", (WATERMARK, WATERMARK))
        await conn.execute(
            "INSERT INTO outbound_actions VALUES (%s,%s,'quo.sms.send','prepared',%s,'{}',%s,NULL,NULL)",
            (ACTION, SUBJECT, WATERMARK, Jsonb({"recipient_phone": JESSICA})),
        )
        yield conn, OutboundGatewayRepository(SqlDriver(conn=conn))


async def add_message(conn, *, sender=GUNTHER, recipient=LINE, direction="inbound", channel=18, minute=13, message_id=694983):
    await conn.execute(
        "INSERT INTO raw_events VALUES (%s,%s)",
        (message_id, Jsonb({"data": {"object": {"from": sender, "to": recipient}}})),
    )
    await conn.execute(
        "INSERT INTO messages VALUES (%s,%s,%s,%s,'showing update','quo',%s,NULL,NULL)",
        (message_id, channel, WATERMARK.replace(minute=minute), direction, message_id),
    )


async def verdict(repository, *, override=False):
    return await check_traffic(
        repository,
        recipient_key=SUBJECT,
        channel_id=18,
        wakeup_event_id=26817,
        action_id=ACTION,
        override=override,
        logger=logging.getLogger(__name__),
    )


@pytest.mark.asyncio
async def test_gunther_activity_on_shared_line_does_not_block_jessica(traffic):
    conn, repository = traffic
    await add_message(conn)
    result = await verdict(repository)
    assert result.allowed, result.detail
    assert result.reason == "pass"
    assert not result.check_failed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sender", "recipient", "direction", "blocked"),
    [
        (GUNTHER, LINE, "inbound", False),
        (LINE, GUNTHER, "outbound", False),
        (LINE, [GUNTHER], "outbound", False),
        (JESSICA, LINE, "inbound", True),
        (LINE, JESSICA, "outbound", True),
        (LINE, [GUNTHER, JESSICA], "outbound", True),
        ("+1 (202) 555-0101", LINE, "inbound", True),
        (LINE, "+1 (202) 555-0101", "outbound", True),
        (JESSICA, LINE, None, True),
        (LINE, JESSICA, "inbound", True),  # mislabeled outbound
        (GUNTHER, LINE, None, False),
        (None, None, None, False),
    ],
)
async def test_only_jessica_endpoints_make_her_reply_stale(traffic, sender, recipient, direction, blocked):
    conn, repository = traffic
    await add_message(conn, sender=sender, recipient=recipient, direction=direction)
    result = await verdict(repository)
    assert result.allowed is not blocked, result.detail
    assert result.reason == ("stale_context" if blocked else "pass")
    assert not result.check_failed


@pytest.mark.asyncio
@pytest.mark.parametrize(("channel", "minute", "blocked"), [(19, 13, False), (18, 8, False), (18, 9, False), (18, 10, True)])
async def test_line_and_watermark_boundaries(traffic, channel, minute, blocked):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, channel=channel, minute=minute)
    result = await verdict(repository)
    assert result.allowed is not blocked
    assert not result.check_failed


@pytest.mark.asyncio
async def test_newer_gunther_message_cannot_hide_relevant_jessica_message(traffic):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=10, message_id=10)
    await add_message(conn, sender=GUNTHER, minute=13, message_id=13)
    result = await verdict(repository)
    assert not result.allowed
    assert result.reason == "stale_context"
    assert "message 10 " in result.detail
    assert not result.check_failed


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["email.send", "tenantcloud.message.send"])
async def test_non_sms_channel_freshness_is_preserved(traffic, operation):
    conn, repository = traffic
    await conn.execute("UPDATE outbound_actions SET operation=%s", (operation,))
    await add_message(conn)
    result = await verdict(repository)
    assert not result.allowed
    assert result.reason == "stale_context"
    assert not result.check_failed


@pytest.mark.asyncio
async def test_cliq_internal_reply_ignores_cron_alert_but_blocks_new_human_message(traffic):
    conn, repository = traffic
    await conn.execute("UPDATE outbound_actions SET operation='cliq.chat.post'")
    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,direction,body,source) "
        "VALUES (750824,18,%s,'outbound',%s,'zoho_cliq')",
        (WATERMARK.replace(minute=10), "⚠️ Cron issue — comms-review-stall-watch"),
    )
    alert_only = await verdict(repository)
    assert alert_only.allowed and alert_only.reason == "pass"

    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,direction,body,source) "
        "VALUES (750825,18,%s,'inbound',%s,'zoho_cliq')",
        (WATERMARK.replace(minute=11), "Could you call me?"),
    )
    human_followup = await verdict(repository)
    assert not human_followup.allowed
    assert human_followup.reason == "stale_context"
    assert "message 750825" in human_followup.detail


@pytest.mark.asyncio
async def test_cliq_internal_reply_still_blocks_non_alert_outbound(traffic):
    conn, repository = traffic
    await conn.execute("UPDATE outbound_actions SET operation='cliq.chat.post'")
    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,direction,body,source) "
        "VALUES (750825,18,%s,'outbound',%s,'zoho_cliq')",
        (WATERMARK.replace(minute=10), "I already sent pong."),
    )
    result = await verdict(repository)
    assert not result.allowed
    assert result.reason == "stale_context"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("subject", "state", "dispatched", "reason"),
    [
        (SUBJECT, "prepared", False, "lease_held"),
        (SUBJECT, "dispatching", True, "lease_held"),
        (SUBJECT, "unknown", True, "lease_held"),
        (SUBJECT, "completed", True, "stale_context"),
        (SUBJECT, "completed", False, "stale_context"),
        (SUBJECT, "definitive_failed", False, "pass"),
        (SUBJECT, "definitive_failed", True, "stale_context"),
        ("subject:gunther", "prepared", False, "pass"),
        ("subject:gunther", "completed", True, "pass"),
    ],
)
async def test_recipient_leases_and_actual_outbound_activity(traffic, subject, state, dispatched, reason):
    conn, repository = traffic
    created = WATERMARK.replace(minute=12)
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s,%s,'quo.sms.send',%s,%s,'{}','{}',%s,NULL)",
        (UUID(int=2), subject, state, created, created if dispatched else None),
    )
    await add_message(conn)  # unrelated traffic must not alter the verdict
    result = await verdict(repository)
    assert result.reason == reason
    assert result.allowed is (reason == "pass")
    assert not result.check_failed


@pytest.mark.asyncio
async def test_own_dispatched_action_does_not_block_itself_on_resume(traffic):
    conn, repository = traffic
    await conn.execute(
        "UPDATE outbound_actions SET state='dispatching', created_at=%s, dispatch_started_at=%s",
        (WATERMARK.replace(minute=12), WATERMARK.replace(minute=12)),
    )
    await add_message(conn)
    result = await verdict(repository)
    assert result.allowed
    assert result.reason == "pass"
    assert not result.check_failed


@pytest.mark.asyncio
async def test_dispatched_retry_ancestor_does_not_block_remediation_successor(traffic):
    conn, repository = traffic
    failed_ancestor = UUID(int=2)
    created = WATERMARK.replace(minute=12)
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s,%s,'tenantcloud.message.send','definitive_failed',%s,'{}','{}',%s,NULL)",
        (failed_ancestor, SUBJECT, created, created),
    )
    await conn.execute("UPDATE outbound_actions SET retry_of_action_id=%s WHERE action_id=%s", (failed_ancestor, ACTION))

    result = await verdict(repository)

    assert result.allowed
    assert result.reason == "pass"
    assert not result.check_failed


@pytest.mark.asyncio
async def test_override_bypasses_real_staleness_but_never_recipient_lease(traffic):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA)
    stale = await verdict(repository)
    assert stale.reason == "stale_context"
    overridden = await verdict(repository, override=True)
    assert overridden.allowed and not overridden.check_failed
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s,%s,'quo.sms.send','prepared',%s,'{}','{}',NULL,NULL)",
        (UUID(int=2), SUBJECT, WATERMARK.replace(minute=12)),
    )
    leased = await verdict(repository, override=True)
    assert not leased.allowed
    assert leased.reason == "lease_held"



@pytest.mark.asyncio
async def test_needs_confirmation_lists_every_relevant_item_newest_first(traffic):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=10, message_id=10)
    await add_message(conn, sender=GUNTHER, minute=11, message_id=11)
    await add_message(conn, sender=LINE, recipient=JESSICA, direction="outbound", minute=12, message_id=12)
    await conn.execute("INSERT INTO participants VALUES (7, 'phone:+12025550101', 'phone', 'Jessica')")
    await conn.execute("UPDATE messages SET sender_participant_id = 7 WHERE id = 10")

    result = await verdict(repository)

    assert result.reason == "stale_context"
    # Gunther shares the line but is not Jessica's context.
    assert [item.message_id for item in result.newer] == [12, 10]
    assert result.newer[1].sender == "Jessica"
    assert result.acknowledged_through == WATERMARK.replace(minute=12)
    assert not result.truncated


@pytest.mark.asyncio
async def test_acknowledged_point_on_a_stale_row_waives_only_what_was_shown(traffic):
    """The agent was shown Jessica's minute-10 text in a needs_confirmation.
    Answering it must not be refused over that same text again -- but her
    minute-11 text, which it never saw, still asks again."""
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=10, message_id=10)
    await conn.execute(
        "INSERT INTO outbound_actions (action_id, subject_key, operation, state, created_at, arguments, "
        "canonical_context, wakeup_event_id, stale_context_acknowledged_through) "
        "VALUES (%s,%s,'quo.sms.send','stale',%s,'{}','{}',26817,%s)",
        (UUID(int=3), SUBJECT, WATERMARK.replace(minute=9), WATERMARK.replace(minute=10)),
    )
    assert await repository.acknowledged_through(26817, SUBJECT) == WATERMARK.replace(minute=10)
    assert await repository.acknowledged_through(26818, SUBJECT) is None
    assert await repository.acknowledged_through(26817, "subject:gunther") is None

    shown = await verdict(repository)
    assert shown.allowed and shown.reason == "pass"

    await add_message(conn, sender=JESSICA, minute=11, message_id=11)
    unseen = await verdict(repository)
    assert unseen.reason == "stale_context"
    assert [item.message_id for item in unseen.newer] == [11]
