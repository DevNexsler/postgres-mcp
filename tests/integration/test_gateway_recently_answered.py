# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportOptionalIterable=false, reportOptionalSubscript=false
"""PostgresActionStore.recently_answered and contact_serialized against
PostgreSQL, over Comm-Data-Store's own contact identity and "answered"
functions (migration 269, vendored verbatim in
fixtures/cds_contact_identity_269.sql), and the trigger-key matcher from
migration 270 (fixtures/cds_prospect_replies_match_trigger_keys_270.sql).

- Amy McDermott, 2026-10-07/09: TenantCloud lead 2512395. Wake 27994 emailed
  her the viewing windows (action 95d784bf) for message 854668; 27 hours later
  message 856923 repeated the same text and wake 28084 emailed her again
  (action b7a380d1). Found.
- Aimee Tapia, 2026-09-28: one tour request on two lanes. Wake 27332 (Zillow)
  emailed the relay at 20:09 (action 4fae7656); wake 27331 (TenantCloud, its
  trigger 816806 sent at 18:38, before that reply) emailed it again at 20:13
  (action 96f92ed3). Found.
- One person, three addresses: a reply by SMS, to the Zillow proxy or to the
  email counts against a later reply to any other of them.
- Two lanes at once (the race): through OutboundActionService.enqueue with
  real advisory locks, exactly one reply is prepared and the other is refused
  naming it in flight. Without the lock (negative control) both prepare.
- A contact lock held past the wait: refused retryable, nothing recorded.
- Comm-Data-Store functions missing: the reply is prepared (fail open).

Only the ledger writes the service makes here (create_or_load / prepare /
reject) are minimal SQL stand-ins for Comm-Data-Store's stored functions; the
identity, "answered", locking and transaction behaviour are real.

Run: uv run pytest tests/integration/test_gateway_recently_answered.py -q
Docker required. Uses a disposable PostgreSQL container (removed on exit),
never an application DB.
"""

from __future__ import annotations

import asyncio
import importlib.util
import re
import sys
import time
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import docker
import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb

from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.context import RoutingPolicy
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.preflight import RECENTLY_ANSWERED_DETAIL
from postgres_mcp.outbound_gateway.preflight import CalendarDependencyState
from postgres_mcp.outbound_gateway.preflight import PreflightEvidence
from postgres_mcp.outbound_gateway.preflight import PreflightOutcome
from postgres_mcp.outbound_gateway.preflight import SafetyPreflight
from postgres_mcp.outbound_gateway.repository import AliasResolution
from postgres_mcp.outbound_gateway.repository import ConversationSnapshot
from postgres_mcp.outbound_gateway.repository import WakeEventRecord
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import create_server
from postgres_mcp.outbound_gateway.service import CONTACT_REPLY_BUSY_DETAIL
from postgres_mcp.outbound_gateway.service import OutboundActionService
from postgres_mcp.outbound_gateway.service import _recently_answered_evidence  # pyright: ignore[reportPrivateUsage] -- exercise the service boundary
from postgres_mcp.outbound_gateway.store import PostgresActionStore
from postgres_mcp.sql import DbConnPool
from postgres_mcp.sql import SqlDriver

TRIGGER_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "cds_prospect_replies_match_trigger_keys_270.sql"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "cds_contact_identity_269.sql"
UTC = timezone.utc

AMY = "amc51816@gmail.com"
AMY_FIRST_REPLY = UUID("95d784bf-0a66-59d1-9fb9-1e7d325d484a")
AMY_REPEAT_REPLY = UUID("b7a380d1-3563-5577-8849-7f4240efb174")
AMY_TEXT = "I am interested in this rental and would like to schedule a viewing. Please let me know when this would be possible."
AIMEE_RELAY = "2p76acgkd4bm2i4j2pznecx07b1@convo.zillow.com"
AIMEE_EMAIL = "aimee.tapia@example.com"
AIMEE_PHONE = "+15705550143"
AIMEE_ZILLOW_REPLY = UUID("4fae7656-5e0f-581f-af14-f50c842998c6")
AIMEE_TC_REPLY = UUID("96f92ed3-033c-5484-8417-b89aa8fbbfb0")
AIMEE_EXTRACT = (
    f"prospect: Aimee Tapia | property: 138 Bullman St #144-A | phone: (570) 555-0143 | proxy_email: {AIMEE_RELAY} "
    f"| direct_email: {AIMEE_EMAIL} | direction: prospect | message: I would like to schedule a tour."
)


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def uid(n: int) -> UUID:
    return UUID(int=n)


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


# The columns the gateway and the 269 functions read, with production's
# names and types, and minimal stand-ins for the three ledger functions the
# service calls on this path (Comm-Data-Store owns the real ones).
SCHEMA = """
    DROP SCHEMA IF EXISTS public CASCADE;
    CREATE SCHEMA public;
    CREATE EXTENSION pgcrypto WITH SCHEMA public;
    CREATE TABLE raw_events (id bigint PRIMARY KEY, payload jsonb);
    CREATE TABLE messages (
        id bigint PRIMARY KEY, source text, sent_at timestamptz, sender_participant_id bigint,
        direction text, body text, sender_identifier text, raw_event_id bigint
    );
    CREATE TABLE agency_identifiers (kind text, value text, label text);
    CREATE TABLE channels (id bigint PRIMARY KEY, source text, name text);
    CREATE TABLE hermes_wakeup_events (id bigint PRIMARY KEY, message_id bigint);
    CREATE TABLE outbound_actions (
        action_id uuid PRIMARY KEY, wakeup_event_id bigint NOT NULL, action_role text NOT NULL,
        source_message_id bigint, operation text NOT NULL DEFAULT 'email.send', intent_kind text NOT NULL, appointment_slot timestamptz,
        provenance text NOT NULL DEFAULT 'customer', state text NOT NULL, completion_kind text,
        detail_code text NOT NULL DEFAULT 'received', recipient_scope jsonb NOT NULL, arguments jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz, provider_accepted_at timestamptz,
        action_uid uuid, provider_request_ref text, provider_message_id text, attempt_count integer DEFAULT 0,
        next_attempt_at timestamptz NOT NULL DEFAULT '2000-01-01 00:00:00+00', payload_hash text, canonical_context jsonb,
        canonical_scope jsonb, provider_account text, routing_policy_version text, error_category text,
        retry_of_action_id uuid, remediation_reason text, override_decision text, dispatch_started_at timestamptz,
        error_detail text, stale_context_shown_refs text[], stale_context_decision text
    );
    CREATE TABLE outbound_action_quarantines (action_id uuid PRIMARY KEY);

    CREATE FUNCTION create_or_load_outbound_action(
        p_wake bigint, p_role text, p_operation text, p_intent text, p_slot timestamptz, p_hash text,
        p_context jsonb, p_source_message bigint, p_scope jsonb, p_recipient jsonb, p_account text,
        p_policy text, p_arguments jsonb
    ) RETURNS SETOF outbound_actions LANGUAGE sql AS $$
        INSERT INTO outbound_actions (action_id, wakeup_event_id, action_role, operation, intent_kind,
            appointment_slot, state, recipient_scope, arguments, payload_hash, canonical_context,
            canonical_scope, provider_account, routing_policy_version, source_message_id)
        VALUES (md5('v1:wakeup:' || p_wake || ':role:' || p_role)::uuid, p_wake, p_role, p_operation, p_intent,
            p_slot, 'received', p_recipient, p_arguments, p_hash, p_context, p_scope, p_account, p_policy, p_source_message)
        ON CONFLICT (action_id) DO UPDATE SET action_id = EXCLUDED.action_id
        RETURNING *
    $$;

    -- Prepared is in flight (Comm-Data-Store prospect_reply_status). The sleep
    -- widens the window between a check and its prepare's commit, so an
    -- unserialized second reply checks before the first is visible.
    CREATE FUNCTION prepare_outbound_action_and_acquire_lock(
        p_action uuid, p_expected text, p_prospect text, p_property text, p_intent text, p_slot text,
        p_aliases text[], p_slot_lock boolean, p_lease integer, p_ttl integer
    ) RETURNS SETOF outbound_actions LANGUAGE sql AS $$
        SELECT pg_sleep(0.3);
        UPDATE outbound_actions SET state = 'prepared', detail_code = 'prepared', action_uid = p_action
        WHERE action_id = p_action AND state = p_expected
        RETURNING *
    $$;

    CREATE FUNCTION reject_outbound_action(p_action uuid, p_expected text, p_code text, p_detail text)
    RETURNS SETOF outbound_actions LANGUAGE sql AS $$
        WITH moved AS (
            UPDATE outbound_actions SET state = 'rejected', detail_code = p_code, error_detail = p_detail
            WHERE action_id = p_action AND state = p_expected AND state IN ('received', 'dependency_wait')
            RETURNING *
        )
        SELECT * FROM moved
        UNION ALL
        SELECT * FROM outbound_actions WHERE action_id = p_action AND NOT EXISTS (SELECT 1 FROM moved)
    $$;
"""


@pytest_asyncio.fixture
async def conn(database):
    async with await psycopg.AsyncConnection.connect(database, autocommit=True) as connection:
        await connection.execute(SCHEMA)
        await connection.execute(FIXTURE.read_text())  # pyright: ignore[reportCallIssue, reportArgumentType] -- a checked-in SQL file
        await connection.execute(TRIGGER_FIXTURE.read_text())  # pyright: ignore[reportCallIssue, reportArgumentType] -- checked-in SQL fixture
        yield connection


@pytest_asyncio.fixture
async def store(conn):
    return PostgresActionStore(SqlDriver(conn=conn))


async def message(
    conn,
    message_id: int,
    source: str,
    sent_at: datetime,
    sender: int | None,
    body: str,
    *,
    direction: str = "inbound",
    identifier: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    if payload is not None:
        await conn.execute("INSERT INTO raw_events VALUES (%s, %s)", (message_id, Jsonb(payload)))
    await conn.execute(
        "INSERT INTO messages VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (message_id, source, sent_at, sender, direction, body, identifier, message_id if payload is not None else None),
    )


async def wake(conn, wake_id: int, message_id: int | None) -> None:
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
        """
        INSERT INTO outbound_actions (action_id, wakeup_event_id, action_role, intent_kind, state, completion_kind,
            detail_code, recipient_scope, arguments, created_at, completed_at, provider_accepted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
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


async def amy(conn) -> None:
    """Amy: the same text twice on one TenantCloud lead, 27 h apart."""
    payload = {"email": AMY, "message_data": {"auth_name": "Amy McDermott"}}
    await message(conn, 854668, "tenantcloud_api", at("2026-10-07 21:40:22"), 1900001, AMY_TEXT, payload=payload)
    await message(conn, 856923, "tenantcloud_api", at("2026-10-09 00:24:40"), 1900001, AMY_TEXT + "  ", payload=payload)
    await wake(conn, 27994, 854668)
    await wake(conn, 28084, 856923)
    await action(conn, AMY_FIRST_REPLY, 27994, AMY, at("2026-10-07 21:48:07"), text="Hi Amy,\n\nThanks for reaching out about #7")
    await action(conn, AMY_REPEAT_REPLY, 28084, AMY, at("2026-10-09 01:06:44"), text="Hi Amy, again")


AMY_AS_OF = at("2026-10-09 01:06:44")


# --- Courtney: staff accepted 50 ms after a new customer question -------------

COURTNEY_RELAY = "courtney@convo.zillow.com"
QUESTION_TIME = at("2026-10-23 15:42:26.000")
INCIDENT_TIME = at("2026-10-23 15:45:35.357")
DEPOSIT_QUESTION = "what would the deposit be to move in"


async def catalog_context(conn, wake_id, operation, purpose, recipient):
    """Build the request from what an agent actually sees, then derive real
    context. The legacy universal recipe is a deliberate negative control:
    on the old catalog it reproduces the staff-as-customer incident."""
    mcp = create_server(AsyncMock(), FeaturePolicy(writes_enabled=True, kill_switch=False))
    [tool] = await mcp.list_tools()
    recipes = [line for line in tool.description.splitlines() if line.startswith(f"{operation.value}:")]
    recipe = next((line for line in recipes if f"-- {purpose}" in line), recipes[0])
    match = re.search(r'action_role "([^"]+)", intent_kind "([^"]+)"', recipe)
    assert match is not None
    arguments = {"text": "Showing coordination"}
    if operation is Operation.QUO_SMS_SEND:
        arguments.update(to_phone=recipient, from_phone="+16105550100")
    else:
        arguments.update(to_address=recipient, cc=[])
    request = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": wake_id,
            "operation": operation.value,
            "action_role": match[1],
            "intent_kind": match[2],
            "arguments": arguments,
        }
    )
    assert isinstance(request, ExecuteRequest)
    cursor = await conn.execute(
        "SELECT m.id, m.sent_at, m.body FROM messages m JOIN hermes_wakeup_events w ON w.message_id = m.id WHERE w.id = %s",
        (wake_id,),
    )
    message_id, sent_at, body = await cursor.fetchone()
    record = WakeEventRecord(
        wakeup_event_id=wake_id,
        event_source="zoho_mail",
        source_event_id=str(wake_id),
        event_created_at=sent_at,
        message_id=message_id,
        canonical_message_id=None,
        message_source="zoho_mail",
        source_message_id=str(message_id),
        message_sent_at=sent_at,
        message_updated_at=sent_at,
        subject="Showing",
        body=body,
        user_account_id="nigel-zoho",
        channel_id=44,
        source_channel_id="courtney-thread",
        channel_type="email",
        channel_name="INBOX",
        sender_participant_id=5001,
        participant_type="email_address",
        participant_key=COURTNEY_RELAY,
        display_name="Courtney",
        envelope={},
        raw_payload={},
    )
    repository = AsyncMock()
    repository.load_wake_event.return_value = record
    repository.load_conversation_snapshot.return_value = ConversationSnapshot(message_id, message_id, sent_at)
    repository.resolve_canonical_subject.return_value = AliasResolution("prospect:courtney", (f"email:{COURTNEY_RELAY}",))
    policy = RoutingPolicy(
        version="test",
        email_account_by_provider={"zoho_mail": "nigel-zoho"},
        email_default_account="nigel-zoho",
        calendar_by_profile={},
        cliq_target_by_intent={},
        conversation_aliases={},
        quo_sending_lines={"+16105550100": "test-line"},
    )
    return await ActionContextLoader(repository, policy).load(request)


async def courtney_history(conn, store, operation, purpose, provenance, offset_ms, *, repeated=False):
    await message(conn, 888866, "zoho_mail", at("2026-10-23 15:33:39"), 5001, "I'm going to 21 now", identifier=COURTNEY_RELAY)
    await message(conn, 888897, "zoho_mail", QUESTION_TIME, 5001, DEPOSIT_QUESTION, identifier=COURTNEY_RELAY)
    if repeated:
        await message(conn, 888811, "zoho_mail", at("2026-10-23 15:09:07"), 5001, " WHAT would the deposit BE to move in  ")
    await wake(conn, 29389, 888866)
    await wake(conn, 29390, 888866)
    await wake(conn, 29394, 888897)
    # Genuine earlier customer email remains in every counterfactual.
    await action(conn, uid(100), 29389, COURTNEY_RELAY, at("2026-10-23 15:40:44.917"), text="Showing offer")
    # Neither destination appears in the trigger's keys. Customer controls
    # must still match through those keys, across SMS / email destinations.
    recipient = "+19085550199" if operation is Operation.QUO_SMS_SEND else "coordination@example.com"
    context = await catalog_context(conn, 29390, operation, purpose, recipient)
    recorded = await store.create_or_load(context)
    accepted_at = QUESTION_TIME + timedelta(milliseconds=offset_ms)
    await conn.execute(
        """UPDATE outbound_actions SET state = 'completed', completion_kind = 'sent',
           detail_code = 'provider_receipt_verified', provenance = %s,
           created_at = '2026-10-23 15:42:24.908+00', provider_accepted_at = %s, completed_at = %s
           WHERE action_id = %s""",
        (provenance, accepted_at, accepted_at + timedelta(milliseconds=4), recorded.action_id),
    )
    next_context = await catalog_context(conn, 29394, Operation.EMAIL_SEND, "customer reply", COURTNEY_RELAY)
    next_action = await store.create_or_load(next_context)
    rows = await store.recently_answered(next_action.action_id, as_of=INCIDENT_TIME)
    decision = SafetyPreflight.evaluate(
        next_context,
        PreflightEvidence(CalendarDependencyState.NOT_REQUIRED),
        _recently_answered_evidence(rows),
    )
    cursor = await conn.execute(
        "SELECT action_role, intent_kind, prospect_reply_status(a) FROM outbound_actions a WHERE action_id = %s",
        (recorded.action_id,),
    )
    classification = await cursor.fetchone()
    return context, recorded, rows, decision, classification


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", [Operation.QUO_SMS_SEND, Operation.EMAIL_SEND])
@pytest.mark.parametrize("provenance", ["customer", "operator_remediation"])
@pytest.mark.parametrize("offset_ms", [-50, 0, 50])
@pytest.mark.parametrize("purpose", ["staff coordination", "customer reply"])
async def test_catalog_recipient_purpose_reaches_persisted_classification_and_preflight(conn, store, operation, provenance, offset_ms, purpose):
    context, recorded, rows, decision, classification = await courtney_history(conn, store, operation, purpose, provenance, offset_ms)
    staff = purpose == "staff coordination"
    is_new = staff or offset_ms < 0
    assert rows and all(row["trigger_is_new_question"] is is_new for row in rows), rows
    assert decision.outcome is (PreflightOutcome.READY if is_new else PreflightOutcome.REFUSED)
    if staff:
        assert classification == ("internal_notification", "manual_review_alert", None)
        assert recorded.action_role is context.action_role is ActionRole.INTERNAL_NOTIFICATION
        assert context.prospect_id == f"internal:{context.target.target_id}"
        assert context.aliases == ()
        assert [row["action_id"] for row in rows] == [uid(100)]
    else:
        assert classification == ("prospect_reply", "inquiry_reply", "answered")
        assert context.aliases and context.prospect_id == "prospect:courtney"
        assert rows[0]["action_id"] == recorded.action_id
        assert rows[0]["recipient"] == context.target.target_id
        if not is_new:
            assert decision.detail_code == RECENTLY_ANSWERED_DETAIL


@pytest.mark.asyncio
async def test_legacy_staff_as_customer_recipe_reproduces_incident_without_subject_or_channel_comparison(conn, store):
    # Force the customer recipe on the same coordination destination. This
    # proves why old polluted rows still need separately audited remediation.
    _, recorded, rows, decision, classification = await courtney_history(
        conn,
        store,
        Operation.QUO_SMS_SEND,
        "customer reply",
        "operator_remediation",
        50,
    )
    assert classification[2] == "answered"
    assert rows[0]["action_id"] == recorded.action_id
    assert rows[0]["sent_at"] == at("2026-10-23 15:42:26.050")
    assert decision.outcome is PreflightOutcome.REFUSED
    assert decision.detail_code == RECENTLY_ANSWERED_DETAIL


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", [Operation.QUO_SMS_SEND, Operation.EMAIL_SEND])
async def test_staff_exclusion_does_not_exempt_same_sender_repeated_body(conn, store, operation):
    _, recorded, rows, decision, _ = await courtney_history(
        conn,
        store,
        operation,
        "staff coordination",
        "operator_remediation",
        50,
        repeated=True,
    )
    assert [row["action_id"] for row in rows] == [uid(100)]
    assert recorded.action_id not in {row["action_id"] for row in rows}
    assert not rows[0]["trigger_is_new_question"]
    assert decision.outcome is PreflightOutcome.REFUSED
    assert decision.detail_code == RECENTLY_ANSWERED_DETAIL


# --- the incidents ------------------------------------------------------------


@pytest.mark.asyncio
async def test_amys_repeated_inquiry_finds_the_reply_she_already_got(conn, store) -> None:
    await amy(conn)

    [found] = await store.recently_answered(AMY_REPEAT_REPLY, as_of=AMY_AS_OF)

    assert found["action_id"] == AMY_FIRST_REPLY
    assert (found["wakeup_event_id"], found["intent"], found["recipient"]) == (27994, "inquiry_reply", AMY)
    assert found["reply_text"].startswith("Hi Amy,")
    assert found["reply_status"] == "answered"
    assert found["sent_at"] == at("2026-10-07 21:48:10")
    # A repeat of her own earlier words, after our reply: still asked.
    assert (found["trigger_source"], found["trigger_is_new_question"]) == ("tenantcloud_api", False)


@pytest.mark.asyncio
async def test_the_second_lane_finds_the_first_lanes_reply(conn, store) -> None:
    await message(conn, 817301, "zillow_rm_web_extract", at("2026-09-28 18:38:00"), 1838192, AIMEE_EXTRACT)
    await message(conn, 816806, "tenantcloud_api", at("2026-09-28 18:38:44"), 1837476, "I would like to schedule a tour.")
    await wake(conn, 27332, 817301)
    await wake(conn, 27331, 816806)
    await action(conn, AIMEE_ZILLOW_REPLY, 27332, AIMEE_RELAY, at("2026-09-28 20:09:24"), intent="showing_offer", text="slots")
    # The first reply, before the second lane's exists, finds nothing.
    assert await store.recently_answered(AIMEE_ZILLOW_REPLY, as_of=at("2026-09-28 20:09:24")) == []
    await action(conn, AIMEE_TC_REPLY, 27331, AIMEE_RELAY.upper(), at("2026-09-28 20:13:16"), text="units")

    [found] = await store.recently_answered(AIMEE_TC_REPLY, as_of=at("2026-09-28 20:13:16"))

    assert (found["action_id"], found["intent"]) == (AIMEE_ZILLOW_REPLY, "showing_offer")


@pytest.mark.asyncio
async def test_a_new_question_after_our_reply_is_marked_new(conn, store) -> None:
    await amy(conn)
    await message(conn, 860001, "tenantcloud_api", at("2026-10-09 12:00:00"), 1900001, "Is parking included?", payload={"email": AMY})
    await wake(conn, 28200, 860001)
    await action(conn, uid(1), 28200, AMY, at("2026-10-09 12:05:00"), state="received", completion_kind=None, detail_code="received")

    rows = await store.recently_answered(uid(1), as_of=at("2026-10-09 12:05:00"))

    assert [row["action_id"] for row in rows] == [AMY_REPEAT_REPLY, AMY_FIRST_REPLY]
    assert all(row["trigger_is_new_question"] for row in rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["zoho_cliq", "hermes_schedule"])
async def test_a_staff_directive_or_a_schedule_carries_its_trigger_source(conn, store, source) -> None:
    await amy(conn)
    await message(conn, 860002, source, at("2026-10-09 01:20:00"), 407, "send Amy the application link")
    await wake(conn, 28210, 860002)
    await action(conn, uid(2), 28210, AMY, at("2026-10-09 01:21:00"), state="received", completion_kind=None, detail_code="received")

    rows = await store.recently_answered(uid(2), as_of=at("2026-10-09 01:21:00"))

    assert rows and {row["trigger_source"] for row in rows} == {source}


@pytest.mark.asyncio
async def test_only_other_wakes_sent_or_in_flight_customer_replies_inside_the_window_are_listed(conn, store) -> None:
    await amy(conn)
    as_of = AMY_AS_OF
    await action(conn, uid(11), 27000, "someone@else.com", at("2026-10-08 10:00:00"))
    await action(conn, uid(12), 26000, AMY, as_of - timedelta(hours=72, minutes=1))
    await action(conn, uid(13), 27990, AMY, at("2026-10-08 09:00:00"), state="rejected", completion_kind=None, detail_code="wake_terminal")
    await action(conn, uid(14), 27991, AMY, at("2026-10-08 09:30:00"), detail_code="already_handled", completion_kind="duplicate")
    await action(conn, uid(15), 27992, AMY, at("2026-10-08 10:00:00"), role="internal_notification", intent="manual_review_alert")
    await action(conn, uid(16), 28084, AMY, at("2026-10-08 11:00:00"))  # this wake's own earlier reply
    await action(conn, uid(17), 27993, AMY, at("2026-10-08 12:00:00"), intent="showing_confirmation")
    await action(conn, uid(18), 27996, AMY, at("2026-10-08 13:00:00"))
    await conn.execute("INSERT INTO outbound_action_quarantines VALUES (%s)", (uid(18),))

    assert [row["action_id"] for row in await store.recently_answered(AMY_REPEAT_REPLY, as_of=as_of)] == [AMY_FIRST_REPLY]


@pytest.mark.asyncio
async def test_a_reply_in_flight_is_listed_as_in_flight(conn, store) -> None:
    await amy(conn)
    await action(conn, uid(20), 28001, AMY, at("2026-10-09 01:06:40"), state="prepared", completion_kind=None, detail_code="prepared")

    rows = await store.recently_answered(AMY_REPEAT_REPLY, as_of=AMY_AS_OF)

    assert [(row["action_id"], row["reply_status"]) for row in rows] == [(uid(20), "in_flight"), (AMY_FIRST_REPLY, "answered")]


@pytest.mark.asyncio
async def test_unknown_action_finds_nothing(store) -> None:
    assert await store.recently_answered(uid(99), as_of=AMY_AS_OF) == []


# --- one person, three addresses ---------------------------------------------


async def aimee_lanes(conn) -> None:
    """Wake 30001: Zillow extract (proxy, direct email, phone). Wake 30002:
    TenantCloud lead whose payload carries her email and phone."""
    await message(conn, 900001, "zillow_rm_web_extract", at("2026-10-18 10:00:00"), 5001, AIMEE_EXTRACT)
    await message(
        conn, 900002, "tenantcloud_api", at("2026-10-18 10:00:05"), 5002, "I would like to schedule a tour.",
        payload={"email": AIMEE_EMAIL, "phone": AIMEE_PHONE},
    )  # fmt: skip
    await wake(conn, 30001, 900001)
    await wake(conn, 30002, 900002)


AS_OF = at("2026-10-18 11:00:00")


@pytest.mark.asyncio
async def test_the_contact_keys_are_the_trigger_messages_and_the_recipients(conn, store) -> None:
    await aimee_lanes(conn)
    await action(conn, uid(31), 30001, "+1 (570) 555-0143", at("2026-10-18 10:01:00"), state="received", completion_kind=None)

    keys = await store.contact_keys(uid(31))

    hashed = "email_sha256:" + sha256(AIMEE_EMAIL.encode()).hexdigest()
    assert {f"email:{AIMEE_EMAIL}", f"email:{AIMEE_RELAY}", "phone:5705550143", hashed} <= set(keys)
    assert keys == sorted(keys)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("earlier_wake", "earlier_to", "later_wake", "later_to"),
    [
        (30001, AIMEE_PHONE, 30002, AIMEE_EMAIL),  # SMS, then email
        (30001, AIMEE_RELAY, 30001 + 10, AIMEE_EMAIL),  # Zillow proxy, then email
        (30002, AIMEE_EMAIL, 30001, AIMEE_PHONE),  # email, then SMS
        (30002, AIMEE_EMAIL, 30001, AIMEE_RELAY),  # email, then Zillow proxy
        (30002, "(570) 555-0143", 30001, AIMEE_RELAY),  # phone (any format), then proxy
    ],
)
async def test_a_reply_to_any_of_her_addresses_counts_against_another(conn, store, earlier_wake, earlier_to, later_wake, later_to) -> None:
    await aimee_lanes(conn)
    await wake(conn, 30011, 900001)  # a second Zillow-lane wake on the same extract
    await action(conn, uid(41), earlier_wake, earlier_to, at("2026-10-18 10:01:00"))
    await action(conn, uid(42), later_wake, later_to, at("2026-10-18 10:30:00"), state="received", completion_kind=None)

    [found] = await store.recently_answered(uid(42), as_of=AS_OF)

    assert found["action_id"] == uid(41)


@pytest.mark.asyncio
async def test_someone_else_on_the_same_lane_does_not_count(conn, store) -> None:
    await aimee_lanes(conn)
    await action(conn, uid(51), 30001, "+19085550199", at("2026-10-18 10:01:00"))
    await message(conn, 900003, "quo", at("2026-10-18 10:10:00"), 5003, "hi", identifier="+19085550123")
    await wake(conn, 30003, 900003)
    await action(conn, uid(52), 30003, "+19085550123", at("2026-10-18 10:30:00"), state="received", completion_kind=None)

    assert await store.recently_answered(uid(52), as_of=AS_OF) == []


# --- the race, through the service and real advisory locks -------------------


def _ledger_module():
    path = Path(__file__).resolve().parents[1] / "unit" / "outbound_gateway" / "test_stale_context_confirm.py"
    name = "_recently_answered_ledger"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


class Loader:
    """The unit tests' context loader, addressed to the request's recipient
    (an email's to_address too)."""

    def __init__(self) -> None:
        self._inner = _ledger_module().FakeLoader()

    async def load(self, request, *, recorded_account=None):
        context = await self._inner.load(request, recorded_account=recorded_account)
        to = context.arguments.get("to_address")
        return replace(context, target=DerivedTarget("email_thread", str(to), True)) if to else context

    async def suggest_targets(self, wakeup_event_id):
        return {}


def service_on(store: PostgresActionStore, *, lock_ms: int = 5000) -> OutboundActionService:
    ledger = _ledger_module()
    return OutboundActionService(
        store=store,
        context_loader=Loader(),
        evidence_loader=ledger.StaticEvidence(),
        adapters={},
        provider_client=object(),
        clock=lambda: datetime.now(UTC),
        lease_owner="outbound-gateway",
        traffic_mode="off",
        contact_lock_timeout_ms=lock_ms,
    )


def zillow_lane_sms(text: str = "Hi Aimee, we show weekdays 7:00-3:30.") -> Any:
    return parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": 30001,
            "action_role": "prospect_reply",
            "operation": "quo.sms.send",
            "intent_kind": "inquiry_reply",
            "arguments": {"text": text, "to_phone": AIMEE_PHONE},
        }
    )


def tenantcloud_lane_email(text: str = "Hi Aimee, how about Tuesday at 10?") -> Any:
    return parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": 30002,
            "action_role": "prospect_reply",
            "operation": "email.send",
            "intent_kind": "showing_offer",
            "appointment_slot": "2026-10-20T14:00:00Z",
            "arguments": {"text": text, "to_address": AIMEE_EMAIL, "cc": []},
        }
    )


@pytest_asyncio.fixture
async def pooled(conn, database):
    await aimee_lanes(conn)
    pool = DbConnPool(database)
    await pool.pool_connect()
    try:
        yield PostgresActionStore(SqlDriver(conn=pool))
    finally:
        await pool.close()


async def states(conn) -> dict[int, tuple[str, str]]:
    cursor = await conn.execute("SELECT wakeup_event_id, state, detail_code FROM outbound_actions ORDER BY wakeup_event_id")
    return {row[0]: (row[1], row[2]) for row in await cursor.fetchall()}


@pytest.mark.asyncio
async def test_two_lanes_at_once_to_two_addresses_of_one_person_prepare_exactly_one(conn, pooled) -> None:
    results = await asyncio.gather(
        service_on(pooled).enqueue(zillow_lane_sms()),
        service_on(pooled).enqueue(tenantcloud_lane_email()),
    )

    assert sorted(result.status.value for result in results) == [PublicStatus.PENDING.value, PublicStatus.REJECTED.value], results
    [refused] = [result for result in results if result.status is PublicStatus.REJECTED]
    assert refused.detail_code == RECENTLY_ANSWERED_DETAIL
    assert refused.recently_answered[0].reply_status == "in_flight"
    assert "is being sent a reply right now" in (refused.detail or "")
    assert refused.override is not None
    rows = await states(conn)
    assert sorted(state for state, _code in rows.values()) == ["prepared", "rejected"]


@pytest.mark.asyncio
async def test_negative_control_without_the_lock_both_lanes_prepare(conn, pooled) -> None:
    """The stand-in really races: unserialized, both checks run before
    either prepare commits, and both replies go out."""
    pooled.contact_serialized = None  # type: ignore[method-assign]

    results = await asyncio.gather(
        service_on(pooled).enqueue(zillow_lane_sms()),
        service_on(pooled).enqueue(tenantcloud_lane_email()),
    )

    assert [result.status for result in results] == [PublicStatus.PENDING, PublicStatus.PENDING], results
    assert sorted(state for state, _code in (await states(conn)).values()) == ["prepared", "prepared"]


@pytest.mark.asyncio
async def test_the_second_lane_later_is_refused_naming_the_first(conn, pooled) -> None:
    first = await service_on(pooled).enqueue(zillow_lane_sms())
    second = await service_on(pooled).enqueue(tenantcloud_lane_email())

    assert first.status is PublicStatus.PENDING
    assert (second.status, second.detail_code) == (PublicStatus.REJECTED, RECENTLY_ANSWERED_DETAIL)
    assert second.recently_answered[0].recipient == AIMEE_PHONE


@pytest.mark.asyncio
async def test_a_contact_lock_held_past_the_wait_refuses_retryably_and_records_nothing(conn, pooled, database) -> None:
    async with await psycopg.AsyncConnection.connect(database) as holder:
        await holder.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("outbound-contact-reply:phone:5705550143",))

        busy = await service_on(pooled, lock_ms=200).enqueue(zillow_lane_sms())

        assert (busy.status, busy.detail_code, busy.retryable) == (PublicStatus.FAILED, CONTACT_REPLY_BUSY_DETAIL, True)
        assert busy.override is None
        assert (await states(conn))[30001] == ("received", "received")
        await holder.rollback()

    again = await service_on(pooled, lock_ms=200).enqueue(zillow_lane_sms())

    assert again.status is PublicStatus.PENDING, again
    assert (await states(conn))[30001][0] == ActionState.PREPARED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["prospect_replies_to_keys(text[], timestamptz, boolean, timestamptz)", "message_contact_keys(bigint)"])
async def test_without_comm_data_stores_functions_the_reply_still_goes_out(conn, pooled, missing) -> None:
    await action(conn, uid(61), 30009, AIMEE_PHONE, datetime.now(UTC) - timedelta(minutes=5))
    await conn.execute(f"DROP FUNCTION {missing} CASCADE")

    result = await service_on(pooled).enqueue(zillow_lane_sms())

    assert result.status is PublicStatus.PENDING, result
    assert (await states(conn))[30001][0] == "prepared"


@pytest.mark.asyncio
async def test_an_override_of_the_refusal_prepares_it(conn, pooled) -> None:
    await service_on(pooled).enqueue(zillow_lane_sms())
    refused = await service_on(pooled).enqueue(tenantcloud_lane_email())
    assert refused.override is not None
    # The override path itself is Comm-Data-Store's override_outbound_action
    # (unit-tested with its ledger fake); here: the successor it mints is an
    # answer, so it is prepared without being asked again, under the lock.
    await conn.execute(
        """
        INSERT INTO outbound_actions (action_id, wakeup_event_id, action_role, operation, intent_kind, state,
            recipient_scope, arguments, retry_of_action_id, remediation_reason, appointment_slot)
        SELECT %s, wakeup_event_id, action_role, operation, intent_kind, 'received', recipient_scope, arguments,
               action_id, 'agent_override', appointment_slot
        FROM outbound_actions WHERE action_id = %s
        """,
        (uid(71), refused.action_id),
    )

    prepared = await service_on(pooled).prepare(uid(71))

    assert prepared.status is PublicStatus.PENDING, prepared
