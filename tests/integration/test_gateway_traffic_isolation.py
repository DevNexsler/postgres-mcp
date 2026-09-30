"""The stale-context query (repository.newer_context) and the in-flight lease
against PostgreSQL, with no mocked SQL.

Run: uv run pytest tests/integration/test_gateway_traffic_isolation.py -q
Docker required. Uses a disposable PostgreSQL container, never an application DB.
"""

import time
from datetime import datetime
from datetime import timezone
from uuid import UUID

import docker
import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb

from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
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
                sender_participant_id bigint, recipient_participant_id bigint,
                source_message_id text, canonical_message_id bigint, sent_at timestamptz,
                -- production: created_at is a generated alias of sent_at;
                -- received_at is when the message reached CDS.
                received_at timestamptz
            );
            CREATE TEMP TABLE channels (id bigint PRIMARY KEY, source_channel_id text);
            CREATE TEMP TABLE raw_events (id bigint PRIMARY KEY, payload jsonb);
            CREATE TEMP TABLE participants (id bigint PRIMARY KEY, participant_key text, participant_type text, display_name text);
            CREATE TEMP TABLE outbound_actions (
                action_id uuid PRIMARY KEY, subject_key text, operation text, state text,
                created_at timestamptz, arguments jsonb, canonical_context jsonb,
                dispatch_started_at timestamptz, retry_of_action_id uuid,
                -- Rows default to ANOTHER wake: since multi-action wakes (CDS
                -- migration 204) the probe ignores the wake's own actions.
                wakeup_event_id bigint DEFAULT 26800,
                stale_context_shown_refs text[],
                provider_message_id text
            );
            CREATE TEMP TABLE agency_identifiers (kind text, value text, label text);
            CREATE TEMP TABLE hermes_wakeup_events (
                id bigint PRIMARY KEY, webui_accepted_at timestamptz, created_at timestamptz
            );
        """)
        await conn.execute("INSERT INTO hermes_wakeup_events VALUES (26817, %s, %s)", (WATERMARK, WATERMARK))
        await conn.execute("INSERT INTO channels VALUES (18, %s), (19, %s)", (LINE, "+12025550199"))
        # Production seed (Comm-Data-Store migration 146): Nigel's own Cliq user.
        await conn.execute(
            "INSERT INTO agency_identifiers VALUES ('cliq_user_id','918334727','nigel-zoho'),"
            "('cliq_user_id','720844989','dan-zoho')"
        )
        await conn.execute(
            "INSERT INTO participants VALUES (918, '918334727', 'user', 'Nigel Pine'),"
            "(720, '720844989', 'user', 'Dan Park')"
        )
        await conn.execute(
            "INSERT INTO outbound_actions VALUES (%s,%s,'quo.sms.send','prepared',%s,'{}',%s,NULL,NULL)",
            (ACTION, SUBJECT, WATERMARK, Jsonb({"recipient_phone": JESSICA})),
        )
        await conn.execute("UPDATE outbound_actions SET wakeup_event_id = 26817 WHERE action_id = %s", (ACTION,))
        yield conn, OutboundGatewayRepository(SqlDriver(conn=conn))


async def add_message(conn, *, sender=GUNTHER, recipient=LINE, direction="inbound", channel=18, minute=13, message_id=694983, sent_minute=None):
    await conn.execute(
        "INSERT INTO raw_events VALUES (%s,%s)",
        (message_id, Jsonb({"data": {"object": {"from": sender, "to": recipient}}})),
    )
    await conn.execute(
        "INSERT INTO messages VALUES (%s,%s,%s,%s,'showing update','quo',%s,NULL,NULL,%s,NULL,%s,%s)",
        (
            message_id,
            channel,
            WATERMARK.replace(minute=minute if sent_minute is None else sent_minute),
            direction,
            message_id,
            f"AC-{message_id}",
            WATERMARK.replace(minute=minute if sent_minute is None else sent_minute),
            WATERMARK.replace(minute=minute),
        ),
    )


SOURCE_AT = WATERMARK.replace(minute=5)


def context(**overrides) -> ActionContext:
    """Jessica's Quo reply on the shared leasing line (channel 18), wake 26817."""
    values = dict(
        action_id=ACTION,
        wakeup_event_id=26817,
        action_role=ActionRole.PROSPECT_REPLY,
        operation=Operation.QUO_SMS_SEND,
        intent_kind=IntentKind.INQUIRY_REPLY,
        appointment_slot=None,
        arguments={"text": "Friday works", "to_phone": JESSICA},
        source="quo",
        source_message_id=1,
        source_message_key="quo:AC-1",
        source_sent_at=SOURCE_AT,
        conversation_id="conversation:quo:line",
        conversation_watermark=1,
        prospect_id=SUBJECT,
        aliases=(),
        property_id=None,
        property_label=None,
        target=DerivedTarget("quo_conversation", JESSICA, True),
        provider_account="PN-leasing",
        routing_policy_version="v1",
        canonical_scope={},
        canonical_context={},
        payload_hash="a" * 64,
        lock_holder=f"outbound-gateway:{ACTION}",
        thread_identity="CN-jessica",
        showing_lifecycle_id="showing:26817",
        calendar_event_uid=None,
        recipient_phone=JESSICA,
        channel_id=18,
    )
    values.update(overrides)
    return ActionContext(**values)


INTERNAL_REPLY = dict(
    action_role=ActionRole.INTERNAL_REPLY,
    operation=Operation.CLIQ_CHAT_POST,
    intent_kind=IntentKind.INTERNAL_REPLY,
    target=DerivedTarget("cliq_chat", LINE, True),
    source="zoho_cliq",
    recipient_phone=None,
)


async def newer(repository, *, waive_shown=True, limit=11, **overrides):
    return await repository.newer_context(context(**overrides), limit=limit, waive_shown=waive_shown)


async def refs(repository, **overrides):
    return [item.ref for item in await newer(repository, **overrides)]


@pytest.mark.asyncio
async def test_gunther_activity_on_shared_line_is_not_jessicas_context(traffic):
    conn, repository = traffic
    await add_message(conn)
    assert await refs(repository) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sender", "recipient", "direction", "listed", "label"),
    [
        (GUNTHER, LINE, "inbound", False, None),
        (LINE, GUNTHER, "outbound", False, None),
        (LINE, [GUNTHER], "outbound", False, None),
        (JESSICA, LINE, "inbound", True, "received"),
        (LINE, JESSICA, "outbound", True, "sent by us"),
        (LINE, [GUNTHER, JESSICA], "outbound", True, "sent by us"),
        ("+1 (202) 555-0101", LINE, "inbound", True, "received"),
        (LINE, "+1 (202) 555-0101", "outbound", True, "sent by us"),
        (JESSICA, LINE, None, True, "received"),
        (LINE, JESSICA, "inbound", True, "received"),  # mislabeled outbound: still hers
        (GUNTHER, LINE, None, False, None),
        (None, None, None, False, None),
    ],
)
async def test_only_jessicas_endpoints_are_her_context(traffic, sender, recipient, direction, listed, label):
    conn, repository = traffic
    await add_message(conn, sender=sender, recipient=recipient, direction=direction)
    items = await newer(repository)
    assert [item.label for item in items] == ([label] if listed else [])


@pytest.mark.asyncio
@pytest.mark.parametrize(("channel", "minute", "listed"), [(19, 13, False), (18, 8, False), (18, 9, False), (18, 10, True)])
async def test_line_and_watermark_boundaries(traffic, channel, minute, listed):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, channel=channel, minute=minute)
    assert bool(await refs(repository)) is listed


@pytest.mark.asyncio
async def test_a_newer_gunther_message_cannot_hide_jessicas(traffic):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=10, message_id=10)
    await add_message(conn, sender=GUNTHER, minute=13, message_id=13)
    assert await refs(repository, limit=1) == ["message:10"]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", [Operation.EMAIL_SEND, Operation.TENANTCLOUD_MESSAGE_SEND])
async def test_a_non_sms_wake_channel_is_the_conversation(traffic, operation):
    conn, repository = traffic
    await add_message(conn)
    assert await refs(repository, operation=operation) == ["message:694983"]


@pytest.mark.asyncio
async def test_cliq_internal_reply_ignores_cron_alert_but_lists_a_new_human_message(traffic):
    conn, repository = traffic
    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,sent_at,received_at,direction,body,source,sender_participant_id) "
        "VALUES (750824,18,%s,%s,%s,'outbound',%s,'zoho_cliq',918)",
        (WATERMARK.replace(minute=10), WATERMARK.replace(minute=10), WATERMARK.replace(minute=10), "⚠️ Cron issue — comms-review-stall-watch"),
    )
    assert await refs(repository, **INTERNAL_REPLY) == []

    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,sent_at,received_at,direction,body,source) "
        "VALUES (750825,18,%s,%s,%s,'inbound',%s,'zoho_cliq')",
        (WATERMARK.replace(minute=11), WATERMARK.replace(minute=11), WATERMARK.replace(minute=11), "Could you call me?"),
    )
    assert await refs(repository, **INTERNAL_REPLY) == ["message:750825"]


@pytest.mark.asyncio
async def test_cliq_internal_reply_lists_our_own_non_alert_post_as_sent_by_us(traffic):
    conn, repository = traffic
    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,sent_at,received_at,direction,body,source) "
        "VALUES (750825,18,%s,%s,%s,'outbound',%s,'zoho_cliq')",
        (WATERMARK.replace(minute=10), WATERMARK.replace(minute=10), WATERMARK.replace(minute=10), "I already sent pong."),
    )
    items = await newer(repository, **INTERNAL_REPLY)
    assert [(item.ref, item.label) for item in items] == [("message:750825", "sent by us")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("subject", "state", "dispatched", "held", "listed"),
    [
        (SUBJECT, "prepared", False, True, False),
        (SUBJECT, "dispatching", True, True, True),
        (SUBJECT, "unknown", True, True, True),
        (SUBJECT, "completed", True, False, True),
        (SUBJECT, "completed", False, False, True),
        (SUBJECT, "definitive_failed", False, False, False),
        (SUBJECT, "definitive_failed", True, False, True),
        ("subject:gunther", "prepared", False, False, False),
        ("subject:gunther", "completed", True, False, False),
    ],
)
async def test_another_wakes_send_holds_while_in_flight_and_is_listed_once_dispatched(
    traffic, subject, state, dispatched, held, listed
):
    conn, repository = traffic
    created = WATERMARK.replace(minute=12)
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s,%s,'quo.sms.send',%s,%s,'{}','{}',%s,NULL)",
        (UUID(int=2), subject, state, created, created if dispatched else None),
    )
    await add_message(conn)  # unrelated traffic changes nothing
    del held  # the per-person in-flight hold was removed (2026-09-30)
    items = await newer(repository)
    assert [(item.ref, item.label) for item in items] == ([(f"action:{UUID(int=2)}", "sent by us")] if listed else [])


@pytest.mark.asyncio
async def test_the_action_itself_and_its_retry_ancestors_are_never_listed(traffic):
    conn, repository = traffic
    await conn.execute(
        "UPDATE outbound_actions SET state='dispatching', created_at=%s, dispatch_started_at=%s",
        (WATERMARK.replace(minute=12), WATERMARK.replace(minute=12)),
    )
    failed_ancestor = UUID(int=2)
    created = WATERMARK.replace(minute=12)
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s,%s,'tenantcloud.message.send','definitive_failed',%s,'{}','{}',%s,NULL)",
        (failed_ancestor, SUBJECT, created, created),
    )
    await conn.execute("UPDATE outbound_actions SET retry_of_action_id=%s WHERE action_id=%s", (failed_ancestor, ACTION))
    assert await refs(repository) == []


@pytest.mark.asyncio
async def test_this_wakes_own_sends_ingested_back_are_not_news(traffic):
    conn, repository = traffic
    await conn.execute(
        "INSERT INTO outbound_actions (action_id, subject_key, operation, state, created_at, arguments, canonical_context,"
        " wakeup_event_id, provider_message_id) VALUES (%s,%s,'quo.sms.send','completed',%s,'{}','{}',26817,'quo:AC-12')",
        (UUID(int=5), SUBJECT, WATERMARK.replace(minute=11)),
    )
    await add_message(conn, sender=LINE, recipient=JESSICA, direction="outbound", minute=12, message_id=12)
    assert await refs(repository) == []


@pytest.mark.asyncio
async def test_every_item_is_listed_newest_first_with_its_sender(traffic):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=10, message_id=10)
    await add_message(conn, sender=GUNTHER, minute=11, message_id=11)
    await add_message(conn, sender=LINE, recipient=JESSICA, direction="outbound", minute=12, message_id=12)
    await conn.execute("INSERT INTO participants VALUES (7, 'phone:+12025550101', 'phone', 'Jessica')")
    await conn.execute("UPDATE messages SET sender_participant_id = 7 WHERE id = 10")

    items = await newer(repository)

    # Gunther shares the line but is not Jessica's context.
    assert [(item.message_id, item.label) for item in items] == [(12, "sent by us"), (10, "received")]
    assert items[1].sender == "Jessica"


@pytest.mark.asyncio
async def test_shown_refs_on_a_stale_row_waive_only_what_was_shown(traffic):
    """Waived by identity: a text shown in a question is not asked again, but
    one never shown still is -- INCLUDING one sent before the shown text and
    ingested after the question (a timestamp waiver would hide it)."""
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=11, message_id=11)
    await conn.execute(
        "INSERT INTO outbound_actions (action_id, subject_key, operation, state, created_at, arguments, "
        "canonical_context, wakeup_event_id, stale_context_shown_refs) "
        "VALUES (%s,%s,'quo.sms.send','stale',%s,'{}','{}',26817,ARRAY['message:11'])",
        (UUID(int=3), SUBJECT, WATERMARK.replace(minute=9)),
    )
    assert await refs(repository) == []
    # Re-asking lists everything again.
    assert await refs(repository, waive_shown=False) == ["message:11"]
    # Another subject's or another wake's shown set waives nothing here.
    await conn.execute("UPDATE outbound_actions SET subject_key='subject:gunther' WHERE action_id=%s", (UUID(int=3),))
    assert await refs(repository) == ["message:11"]
    await conn.execute("UPDATE outbound_actions SET subject_key=%s WHERE action_id=%s", (SUBJECT, UUID(int=3)))

    # Sent at minute 10 -- before the shown text -- but ingested only now.
    await add_message(conn, sender=JESSICA, minute=13, sent_minute=10, message_id=10)
    assert await refs(repository) == ["message:10"]


@pytest.mark.asyncio
async def test_cliq_internal_reply_ignores_a_cron_alert_stored_as_inbound(traffic):
    """Wake 27164: the alert that refused `pong` was stored `inbound`. The
    exemption is about the alert (Nigel's own account), not the label -- and
    only for Cliq chat posts."""
    conn, repository = traffic
    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,sent_at,received_at,direction,body,source,sender_participant_id) "
        "VALUES (750824,18,%s,%s,%s,'inbound',%s,'zoho_cliq',918)",
        (WATERMARK.replace(minute=10), WATERMARK.replace(minute=10), WATERMARK.replace(minute=10), "⚠️ Cron issue — comms-review-stall-watch"),
    )
    assert await refs(repository, **INTERNAL_REPLY) == []
    assert await refs(repository, operation=Operation.EMAIL_SEND) == ["message:750824"]


@pytest.mark.asyncio
async def test_a_human_pasting_a_cron_alert_is_not_exempt(traffic):
    conn, repository = traffic
    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,sent_at,received_at,direction,body,source,sender_participant_id) "
        "VALUES (750830,18,%s,%s,%s,'inbound',%s,'zoho_cliq',720)",
        (
            WATERMARK.replace(minute=10),
            WATERMARK.replace(minute=10),
            WATERMARK.replace(minute=10),
            "⚠️ Cron issue — comms-review-stall-watch\nis this the gateway?",
        ),
    )
    assert await refs(repository, **INTERNAL_REPLY) == ["message:750830"]
    await conn.execute("UPDATE messages SET sender_participant_id = NULL WHERE id = 750830")
    assert await refs(repository, **INTERNAL_REPLY) == ["message:750830"]


# ----------------------------------------------------------------------------
# One definition: what the preflight's later_inbound covered, in the same query
# ----------------------------------------------------------------------------


async def add_quo_elsewhere(conn, message_id, *, channel=19, minute=13, sent_minute=None, frm=JESSICA, line="PN-leasing", conversation="CN-jessica"):
    payload = {"data": {"object": {"from": frm, "to": [LINE], "phoneNumberId": line, "conversationId": conversation}}}
    await conn.execute("INSERT INTO raw_events VALUES (%s,%s)", (message_id, Jsonb(payload)))
    await conn.execute(
        "INSERT INTO messages VALUES (%s,%s,%s,'inbound','are you there?','quo',%s,NULL,NULL,%s,NULL,%s,%s)",
        (message_id, channel, WATERMARK.replace(minute=minute if sent_minute is None else sent_minute), message_id,
         f"AC-{message_id}", WATERMARK.replace(minute=minute if sent_minute is None else sent_minute),
         WATERMARK.replace(minute=minute)),
    )


@pytest.mark.asyncio
async def test_her_text_on_another_channel_of_the_same_line_and_conversation_is_listed(traffic):
    conn, repository = traffic
    await add_quo_elsewhere(conn, 30)
    await add_quo_elsewhere(conn, 31, line="PN-other")
    await add_quo_elsewhere(conn, 32, conversation="CN-other")
    await add_quo_elsewhere(conn, 33, frm=GUNTHER)
    assert await refs(repository) == ["message:30"]


@pytest.mark.asyncio
async def test_a_zillow_relay_message_on_another_channel_is_listed(traffic):
    conn, repository = traffic
    relay = "lead-7@convo.zillow.com"
    await conn.execute("INSERT INTO raw_events VALUES (40, %s)", (Jsonb({"proxy_email": relay}),))
    await conn.execute(
        "INSERT INTO messages VALUES (40,77,%s,'inbound','still available?','zillow',40,NULL,NULL,'z-40',NULL,%s,%s)",
        (WATERMARK.replace(minute=12), WATERMARK.replace(minute=12), WATERMARK.replace(minute=12)),
    )
    zillow = dict(
        operation=Operation.EMAIL_SEND,
        source="zillow",
        target=DerivedTarget("email_thread", relay, True),
        recipient_phone=None,
        channel_id=76,
    )
    assert await refs(repository, **zillow) == ["message:40"]
    assert await refs(repository, **{**zillow, "target": DerivedTarget("email_thread", "other@convo.zillow.com", True)}) == []


@pytest.mark.asyncio
async def test_the_source_message_its_duplicates_and_certified_older_scrapes_are_not_news(traffic):
    conn, repository = traffic
    email = {"operation": Operation.EMAIL_SEND}  # the wake's channel is the conversation
    await add_message(conn, sender=JESSICA, minute=10, message_id=1)  # the source message itself, re-ingested
    await add_message(conn, sender=JESSICA, minute=11, message_id=50)
    await conn.execute("UPDATE messages SET canonical_message_id = 1 WHERE id = 50")  # its canonical duplicate
    await add_message(conn, sender=JESSICA, minute=12, message_id=51)  # a cross-channel duplicate
    assert await refs(repository, **email, cross_channel_duplicate_message_ids=(51,)) == []
    await conn.execute("UPDATE messages SET source = 'zillow_rm_web_extract' WHERE id = 51")
    assert await refs(repository, **email, certified_older_message_ids=(51,)) == []
    assert await refs(repository, **email) == ["message:51"]


@pytest.mark.asyncio
async def test_before_the_watermark_the_wake_channel_is_context_but_another_channel_is_not(traffic):
    """Sent after the message being answered, reached CDS before the wake's
    watermark: on the wake's own channel the agent's context had it (not
    listed -- the old preflight refused it as newer_inbound); on another
    channel the context did not (listed, as the old preflight saw it)."""
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=7, message_id=60)
    assert await refs(repository) == []
    await add_quo_elsewhere(conn, 61, minute=7)
    assert await refs(repository) == ["message:61"]
    # Sent before the message being answered, elsewhere: already history.
    await add_quo_elsewhere(conn, 62, minute=7, sent_minute=4)
    assert await refs(repository) == ["message:61"]


@pytest.mark.asyncio
async def test_an_internal_notification_lists_what_was_received_not_its_chats_other_posts(traffic):
    conn, repository = traffic
    notification = dict(
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        operation=Operation.CLIQ_CHAT_POST,
        intent_kind=IntentKind.LEAD_ALERT,
        target=DerivedTarget("cliq_chat", "+12025550199", True),
        recipient_phone=None,
    )
    await add_message(conn, sender=JESSICA, minute=10, message_id=70)
    await conn.execute(
        "INSERT INTO messages (id,channel_id,created_at,sent_at,received_at,direction,body,source) "
        "VALUES (71,19,%s,%s,%s,'outbound','lead alert about someone else','zoho_cliq')",
        (WATERMARK.replace(minute=11), WATERMARK.replace(minute=11), WATERMARK.replace(minute=11)),
    )
    assert await refs(repository, **notification) == ["message:70"]


@pytest.mark.asyncio
async def test_as_of_replays_the_world_as_it_stood(traffic):
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=10, message_id=80)
    await add_message(conn, sender=JESSICA, minute=14, message_id=81)
    items = await repository.newer_context(context(), limit=11, waive_shown=True, as_of=WATERMARK.replace(minute=12))
    assert [item.ref for item in items] == ["message:80"]


# ----------------------------------------------------------------------------
# Re-ingests and re-scrapes are not new messages
# ----------------------------------------------------------------------------

RELAY = "lead-9@convo.zillow.com"
ZILLOW_REPLY = dict(
    operation=Operation.EMAIL_SEND,
    source="zillow",
    target=DerivedTarget("email_thread", RELAY, True),
    recipient_phone=None,
    channel_id=30235,
)


SCRAPED_TEXT = "Is the unit still available for a tour this week?"


async def add_scrape(conn, message_id, *, channel, received_minute, sent_minute=8, sender=59237, text=SCRAPED_TEXT):
    """A zillow_rm_web_extract row as production stores it (messages 29008 /
    29052, 2026-06-02): the scraped timestamp as sent_at (and so created_at),
    the relay address in the payload, no direction, no canonical id; a later
    extraction batch re-scrapes the same message onto another channel under a
    new source_message_id, 1.9 hours later."""
    await conn.execute("INSERT INTO raw_events VALUES (%s,%s)", (message_id, Jsonb({"proxy_email": RELAY})))
    await conn.execute(
        "INSERT INTO messages VALUES (%s,%s,%s,NULL,%s,'zillow_rm_web_extract',%s,%s,NULL,%s,NULL,%s,%s)",
        (message_id, channel, WATERMARK.replace(minute=sent_minute), text, message_id, sender,
         f"zrm-msg:{message_id}", WATERMARK.replace(minute=sent_minute), WATERMARK.replace(minute=received_minute)),
    )


@pytest.mark.asyncio
async def test_a_re_scrape_of_a_message_already_in_cds_is_not_new(traffic):
    conn, repository = traffic
    await add_scrape(conn, 29008, channel=30235, received_minute=8)  # the wake's channel, in context
    await add_scrape(conn, 29052, channel=30572, received_minute=14)  # the later batch, another channel
    assert await refs(repository, **ZILLOW_REPLY) == []


@pytest.mark.asyncio
async def test_a_re_scrape_does_not_hide_a_new_message_or_another_senders_words(traffic):
    conn, repository = traffic
    await add_scrape(conn, 29008, channel=30235, received_minute=8)
    await add_scrape(conn, 29052, channel=30572, received_minute=14)
    await add_scrape(conn, 29060, channel=30572, received_minute=14, sent_minute=12, text="Actually, can we do Saturday?")
    await add_scrape(conn, 29061, channel=30572, received_minute=14, sender=59999)
    assert await refs(repository, **ZILLOW_REPLY) == ["message:29060", "message:29061"]


@pytest.mark.asyncio
async def test_a_scrape_that_first_reached_cds_after_the_watermark_is_still_new(traffic):
    """Only a copy of something that had reached CDS by the watermark is old."""
    conn, repository = traffic
    await add_scrape(conn, 29008, channel=30572, received_minute=11)
    await add_scrape(conn, 29052, channel=30573, received_minute=14)
    assert await refs(repository, **ZILLOW_REPLY) == ["message:29008", "message:29052"]


@pytest.mark.asyncio
async def test_a_re_ingest_of_a_canonical_message_already_in_cds_is_not_new(traffic):
    conn, repository = traffic
    email = {"operation": Operation.EMAIL_SEND}
    await add_message(conn, sender=JESSICA, minute=8, message_id=90)  # in context (before the watermark)
    await add_message(conn, sender=JESSICA, minute=12, sent_minute=8, message_id=91)
    await conn.execute("UPDATE messages SET canonical_message_id = 90, body = 'resent copy' WHERE id = 91")
    assert await refs(repository, **email) == []
    await conn.execute("UPDATE messages SET canonical_message_id = NULL WHERE id = 91")
    assert await refs(repository, **email) == ["message:91"]


@pytest.mark.asyncio
async def test_reaching_cds_is_received_at_not_the_send_time(traffic):
    """messages.created_at is a generated alias of sent_at in production. A
    text Jessica sent before the watermark that reached CDS only after it was
    not in the agent's context (zoho_mail's median ingest lag is ~7 min)."""
    conn, repository = traffic
    await add_message(conn, sender=JESSICA, minute=12, sent_minute=8, message_id=95)
    assert await refs(repository) == ["message:95"]
    # Received before the watermark, it was context.
    await conn.execute("UPDATE messages SET received_at = %s WHERE id = 95", (WATERMARK.replace(minute=8),))
    assert await refs(repository) == []
