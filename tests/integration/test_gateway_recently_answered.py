# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportOptionalIterable=false
"""PostgresActionStore.recently_answered against PostgreSQL, on the
production rows of the two incidents it exists for, plus the shapes it must
leave alone.

- Amy McDermott, 2026-10-07/09: TenantCloud lead 2512395. Wake 27994 emailed
  her the viewing windows (action 95d784bf) for message 854668; 27 hours later
  message 856923 repeated the same text and wake 28084 emailed her again
  (action b7a380d1). Asked.
- Aimee Tapia, 2026-09-28: one tour request on two lanes. Wake 27332 (Zillow)
  emailed the relay at 20:09 (action 4fae7656); wake 27331 (TenantCloud, its
  trigger 816806 sent at 18:38, before that reply) emailed it again at 20:13
  (action 96f92ed3). Asked.
- A prospect's genuinely new question after our reply: not asked.
- A staff directive (zoho_cliq) or a schedule (hermes_schedule): not asked.
- A reply to someone else, a reply older than 72 h, a reply that sent
  nothing, the wake's own earlier reply: not listed.

Run: uv run pytest tests/integration/test_gateway_recently_answered.py -q
Docker required. Uses a disposable PostgreSQL container (removed on exit),
never an application DB.
"""

from __future__ import annotations

import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from uuid import UUID

import docker
import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb

from postgres_mcp.outbound_gateway.store import PostgresActionStore
from postgres_mcp.sql import SqlDriver

AMY = "amc51816@gmail.com"
AMY_FIRST_REPLY = UUID("95d784bf-0a66-59d1-9fb9-1e7d325d484a")
AMY_REPEAT_REPLY = UUID("b7a380d1-3563-5577-8849-7f4240efb174")
AMY_TEXT = "I am interested in this rental and would like to schedule a viewing. Please let me know when this would be possible."
AIMEE_RELAY = "2p76acgkd4bm2i4j2pznecx07b1@convo.zillow.com"
AIMEE_ZILLOW_REPLY = UUID("4fae7656-5e0f-581f-af14-f50c842998c6")
AIMEE_TC_REPLY = UUID("96f92ed3-033c-5484-8417-b89aa8fbbfb0")
NEW_QUESTION_REPLY = UUID("00000000-0000-5000-8000-000000000001")
CLIQ_REPLY = UUID("00000000-0000-5000-8000-000000000002")
SCHEDULE_REPLY = UUID("00000000-0000-5000-8000-000000000003")
UTC = timezone.utc


@pytest.fixture(scope="module")
def database():
    client = docker.from_env()
    container = client.containers.run(  # pyright: ignore[reportCallIssue] -- docker's stub overloads (same as the traffic test)
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


# The columns the lookup reads, with production's names and types.
SCHEMA = """
    CREATE TEMP TABLE messages (
        id bigint PRIMARY KEY, source text, sent_at timestamptz, sender_participant_id bigint,
        direction text, body text
    );
    CREATE TEMP TABLE hermes_wakeup_events (id bigint PRIMARY KEY, message_id bigint);
    CREATE TEMP TABLE outbound_actions (
        action_id uuid PRIMARY KEY, wakeup_event_id bigint, action_role text, intent_kind text,
        provenance text, state text, completion_kind text, detail_code text, recipient_scope jsonb,
        arguments jsonb, created_at timestamptz, completed_at timestamptz, provider_accepted_at timestamptz
    );
"""


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


async def message(conn, message_id: int, source: str, sent_at: datetime, sender: int, body: str, direction: str = "inbound") -> None:
    await conn.execute("INSERT INTO messages VALUES (%s, %s, %s, %s, %s, %s)", (message_id, source, sent_at, sender, direction, body))


async def wake(conn, wake_id: int, message_id: int) -> None:
    await conn.execute("INSERT INTO hermes_wakeup_events VALUES (%s, %s)", (wake_id, message_id))


async def action(
    conn,
    action_id: UUID,
    wake_id: int,
    recipient: str,
    created_at: datetime,
    *,
    text: str = "a reply",
    intent: str = "inquiry_reply",
    role: str = "prospect_reply",
    state: str = "completed",
    completion_kind: str | None = "sent",
    detail_code: str = "provider_receipt_verified",
) -> None:
    completed = created_at + timedelta(seconds=3) if state == "completed" else None
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s, %s, %s, %s, 'customer', %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            action_id,
            wake_id,
            role,
            intent,
            state,
            completion_kind,
            detail_code,
            Jsonb({"kind": "email_thread", "verified": True, "target_id": recipient}),
            Jsonb({"text": text}),
            created_at,
            completed,
            completed,
        ),
    )


@pytest_asyncio.fixture
async def store(database):
    async with await psycopg.AsyncConnection.connect(database, autocommit=True) as conn:
        await conn.execute(SCHEMA)
        # Amy: the same text twice on one TenantCloud lead, 27 h apart.
        await message(conn, 854668, "tenantcloud_api", at("2026-10-07 21:40:22"), 1900001, AMY_TEXT)
        await message(conn, 856923, "tenantcloud_api", at("2026-10-09 00:24:40"), 1900001, AMY_TEXT + "  ")
        await wake(conn, 27994, 854668)
        await wake(conn, 28084, 856923)
        await action(conn, AMY_FIRST_REPLY, 27994, AMY, at("2026-10-07 21:48:07"), text="Hi Amy,\n\nThanks for reaching out about #7")
        await action(conn, AMY_REPEAT_REPLY, 28084, AMY, at("2026-10-09 01:06:44"), text="Hi Amy, again")
        # Aimee: the Zillow lane answered at 20:09; the TenantCloud copy's wake
        # (trigger sent 18:38) answered again at 20:13, to the same relay.
        await message(conn, 817301, "zillow_rm_web_extract", at("2026-09-28 18:38:00"), 1838192, "prospect: Aimee Tapia | message: tour")
        await message(conn, 816806, "tenantcloud_api", at("2026-09-28 18:38:44"), 1837476, "I would like to schedule a tour.")
        await wake(conn, 27332, 817301)
        await wake(conn, 27331, 816806)
        await action(conn, AIMEE_ZILLOW_REPLY, 27332, AIMEE_RELAY, at("2026-09-28 20:09:24"), intent="showing_offer", text="Hi Aimee, slots")
        await action(conn, AIMEE_TC_REPLY, 27331, AIMEE_RELAY.upper(), at("2026-09-28 20:13:16"), text="Hi Aimee, units")
        # Amy again on 10-10 with a new question: answered after our reply.
        await message(conn, 860001, "tenantcloud_api", at("2026-10-09 12:00:00"), 1900001, "Is parking included with Unit 7?")
        await wake(conn, 28200, 860001)
        await action(conn, NEW_QUESTION_REPLY, 28200, AMY, at("2026-10-09 12:05:00"), text="Yes, one space.")
        # Dan's Cliq directive and a scheduled wake, both emailing Amy.
        await message(conn, 860002, "zoho_cliq", at("2026-10-09 01:20:00"), 407, "send Amy the application link")
        await wake(conn, 28210, 860002)
        await action(conn, CLIQ_REPLY, 28210, AMY, at("2026-10-09 01:21:00"))
        await message(conn, 860003, "hermes_schedule", at("2026-10-09 02:00:00"), None, "SCHEDULED follow-up")
        await wake(conn, 28220, 860003)
        await action(conn, SCHEDULE_REPLY, 28220, AMY, at("2026-10-09 02:01:00"))
        # Never listed: someone else, older than 72 h, sent nothing, staff.
        await action(conn, UUID(int=11), 27000, "someone@else.com", at("2026-10-08 10:00:00"))
        await action(conn, UUID(int=12), 26000, AMY, at("2026-10-05 21:00:00"))
        await action(conn, UUID(int=13), 27990, AMY, at("2026-10-08 09:00:00"), state="rejected", completion_kind=None, detail_code="wake_terminal")
        await action(conn, UUID(int=14), 27991, AMY, at("2026-10-08 09:30:00"), detail_code="already_handled", completion_kind="duplicate")
        await action(conn, UUID(int=15), 27992, AMY, at("2026-10-08 10:00:00"), role="internal_notification", intent="manual_review_alert")
        yield PostgresActionStore(SqlDriver(conn=conn))


@pytest.mark.asyncio
async def test_amys_repeated_inquiry_finds_the_reply_she_already_got(store) -> None:
    [found] = await store.recently_answered(AMY_REPEAT_REPLY)

    assert found["action_id"] == AMY_FIRST_REPLY
    assert found["wakeup_event_id"] == 27994
    assert found["intent"] == "inquiry_reply"
    assert found["recipient"] == AMY
    assert found["text_excerpt"].startswith("Hi Amy,")
    assert found["sent_at"] == at("2026-10-07 21:48:10")


@pytest.mark.asyncio
async def test_the_second_lane_finds_the_first_lanes_reply(store) -> None:
    [found] = await store.recently_answered(AIMEE_TC_REPLY)

    assert (found["action_id"], found["intent"]) == (AIMEE_ZILLOW_REPLY, "showing_offer")


@pytest.mark.asyncio
async def test_the_first_reply_finds_nothing(store) -> None:
    assert await store.recently_answered(AMY_FIRST_REPLY) == []
    assert await store.recently_answered(AIMEE_ZILLOW_REPLY) == []


@pytest.mark.asyncio
async def test_a_new_question_after_our_reply_is_not_asked_about(store) -> None:
    assert await store.recently_answered(NEW_QUESTION_REPLY) == []


@pytest.mark.asyncio
async def test_a_staff_directive_or_a_schedule_is_not_asked_about(store) -> None:
    assert await store.recently_answered(CLIQ_REPLY) == []
    assert await store.recently_answered(SCHEDULE_REPLY) == []


@pytest.mark.asyncio
async def test_unknown_action_finds_nothing(store) -> None:
    assert await store.recently_answered(UUID(int=99)) == []
