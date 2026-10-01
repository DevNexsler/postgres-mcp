# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportOptionalIterable=false
"""Newer outbound against PostgreSQL: the real context, evidence and probe SQL.

Wake 27279 (2026-09-26) exactly as production stored it: Dan's request in the
Cliq DM (channel 417, message 806237), then an unrelated cron alert posted to
the same DM as outbound (806236), then Nigel's email.send to the prospect with
management@pfg.io on cc. The pre-fix preflight took the cron post for "we
already replied" and completed the email as duplicate/already_handled.

Everything the gateway reads runs as SQL here -- the wake/context load, the
calendar evidence query, the stale-context query. Only the outbound_actions ledger
writes (CDS stored functions) are the migration-192 fake of the unit tests, and
the provider is a recorder.

Run: uv run pytest tests/integration/test_gateway_newer_outbound.py -q
Docker required. Uses a disposable PostgreSQL container (removed on exit),
never an application DB.
"""

from __future__ import annotations

import importlib.util
import sys
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import docker
import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb

from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.context import RoutingPolicy
from postgres_mcp.outbound_gateway.evidence import DatabasePreflightEvidenceLoader
from postgres_mcp.outbound_gateway.models import ConfirmRequest
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
from postgres_mcp.outbound_gateway.service import OutboundActionService
from postgres_mcp.sql import SqlDriver


def _ledger_module():
    """The migration-192 ledger fake the unit tests use (one definition)."""
    path = Path(__file__).resolve().parents[1] / "unit" / "outbound_gateway" / "test_stale_context_confirm.py"
    name = "_newer_outbound_ledger"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


ledger = _ledger_module()

WAKE = 27279
DM = "1424728044450751028"
PROSPECT = "ytryboujee@gmail.com"
SOURCE_AT = datetime(2026, 9, 26, 13, 1, 6, 353000, tzinfo=timezone.utc)
CRON_POST_AT = datetime(2026, 9, 26, 13, 5, 50, 439000, tzinfo=timezone.utc)
WAKE_CREATED_AT = datetime(2026, 9, 26, 13, 5, 55, 380524, tzinfo=timezone.utc)
WAKE_ACCEPTED_AT = datetime(2026, 9, 26, 13, 5, 55, 544802, tzinfo=timezone.utc)
EMAIL_TEXT = "Hi Alberto, here is the application link: https://pinefield.tenantcloud.com/listings/204968"
FIRST = ledger.action_id_for(WAKE, "prospect_reply", 0)
SUCCESSOR = ledger.action_id_for(WAKE, "prospect_reply", 1)

QUO_WAKE = 27300
QUO_LINE = "PNtjMqMO2h"
QUO_LINE_PHONE = "+17623726083"
QUO_PROSPECT = "+15168594333"
QUO_OTHER = "+14843530553"
QUO_SOURCE_AT = datetime(2026, 8, 27, 18, 20, 16, tzinfo=timezone.utc)

POLICY = RoutingPolicy(
    version="appointment-v1",
    email_account_by_provider={},
    quo_line_by_provider={},
    calendar_by_profile={},
    cliq_target_by_intent={},
    property_aliases={},
    conversation_aliases={},
    email_default_account="nigel-zoho",
    quo_default_line=QUO_LINE,
)


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


SCHEMA = """
    CREATE TEMP TABLE channels (
        id bigint PRIMARY KEY, source_channel_id text, channel_type text, name text
    );
    CREATE TEMP TABLE participants (
        id bigint PRIMARY KEY, participant_type text, participant_key text, display_name text
    );
    CREATE TEMP TABLE raw_events (id bigint PRIMARY KEY, payload jsonb);
    CREATE TEMP TABLE messages (
        id bigint PRIMARY KEY, canonical_message_id bigint, source text, source_message_id text,
        sent_at timestamptz, created_at timestamptz, updated_at timestamptz, subject text, body text,
        user_account_id text, channel_id bigint, sender_participant_id bigint,
        recipient_participant_id bigint, raw_event_id bigint, direction text,
        received_at timestamptz  -- when it reached CDS (created_at is sent_at in production)
    );
    CREATE TEMP TABLE hermes_wakeup_events (
        id bigint PRIMARY KEY, source text, source_event_id text, created_at timestamptz,
        webui_accepted_at timestamptz, provenance text, qualification_run_id text,
        message_id bigint, envelope jsonb, tenantcloud_claim_id bigint,
        webui_session_id text, webui_stream_id text  -- the Hermes turn (CDS steering)
    );
    CREATE TEMP TABLE tenantcloud_event_claims (
        claim_id bigint PRIMARY KEY, event_family text, claim_state text, action_owner text,
        entity_scope_key text
    );
    CREATE TEMP TABLE outbound_action_subject_aliases (alias_key text, scope_key text, canonical_subject text);
    CREATE TEMP TABLE outbound_actions (
        action_id uuid PRIMARY KEY, wakeup_event_id bigint, action_role text, operation text,
        state text, subject_key text, created_at timestamptz, arguments jsonb,
        canonical_context jsonb, dispatch_started_at timestamptz, retry_of_action_id uuid,
        stale_context_shown_refs text[], provider_message_id text
    );
    CREATE TEMP TABLE agency_identifiers (kind text, value text, label text);
    -- These scenario rows reach CDS when they are sent.
    CREATE FUNCTION pg_temp.received_when_sent() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN NEW.received_at := coalesce(NEW.received_at, NEW.sent_at); RETURN NEW; END $$;
    CREATE TRIGGER received_when_sent BEFORE INSERT ON messages
        FOR EACH ROW EXECUTE FUNCTION pg_temp.received_when_sent();
"""


async def seed_wake_27279(conn) -> None:
    """Wake 27279's rows as production stored them (payload keys trimmed to
    the ones any gateway query reads)."""
    await conn.execute("INSERT INTO channels VALUES (417, %s, 'dm', 'Dan Park')", (DM,))
    await conn.execute("INSERT INTO participants VALUES (407, 'user', '720844989', 'Dan Park'), (997, 'user', '918334727', 'Nigel Pine')")
    await conn.execute("INSERT INTO agency_identifiers VALUES ('cliq_user_id', '918334727', 'nigel-zoho')")
    people = [{"id": "918334727", "name": "Nigel Pine"}, {"id": "720844989", "name": "Dan Park"}]
    source_payload = {
        "direction": "inbound",
        "conversation_id": DM,
        "participants": people,
        "provider_ids": {"cliq": DM, "message": "1790427666353_7330935477762", "conversation": DM},
    }
    # Stored `inbound` in the payload and `outbound` on the row, as in production.
    cron_payload = {
        "direction": "inbound",
        "conversation_id": DM,
        "participants": people,
        "provider_ids": {"cliq": DM, "message": "1790427950439_7335230729575", "conversation": DM},
    }
    await conn.execute("INSERT INTO raw_events VALUES (827530, %s), (827529, %s)", (Jsonb(source_payload), Jsonb(cron_payload)))
    await conn.execute(
        """
        INSERT INTO messages VALUES
        (806237, NULL, 'zoho_cliq', '1790427666353_7330935477762', %s, %s, %s, NULL,
         'If this person already saw. Give them application link and instruction. Also cc management@pfg.io',
         'nigel-zoho', 417, 407, NULL, 827530, 'inbound'),
        (806236, NULL, 'zoho_cliq', '1790427950439_7335230729575', %s, %s, %s, NULL,
         '⚠️ Cron issue — comms-review-stall-watch Time: 2026-09-26 09:05 AM ET',
         'nigel-zoho', 417, 997, NULL, 827529, 'outbound')
        """,
        (SOURCE_AT, SOURCE_AT, SOURCE_AT + timedelta(minutes=4), CRON_POST_AT, CRON_POST_AT, CRON_POST_AT),
    )
    envelope = {
        "message": {
            "id": 806237,
            "phone": None,
            "property": None,
            "proxy_email": None,
            "direct_email": None,
            "prospect_name": None,
            "sender": {"display_name": "Dan Park", "participant_key": "720844989", "participant_type": "user"},
        },
        "identity": {"factbook_entity_uuid": None, "link_type": "source_participant"},
        "routing_hints": {},
        "conversation_context": {"nearby_messages": []},
    }
    await conn.execute(
        "INSERT INTO hermes_wakeup_events VALUES (%s, 'zoho_cliq', '1790427666353_7330935477762', %s, %s, 'customer', NULL, 806237, %s, NULL)",
        (WAKE, WAKE_CREATED_AT, WAKE_ACCEPTED_AT, Jsonb(envelope)),
    )


async def add_sent_email(conn, message_id: int, *, to: str, at: datetime, source_message_id: str, body: str) -> None:
    """A zoho_mail Sent-folder message from Nigel's mailbox, participants as
    the collector stores them."""
    await conn.execute("INSERT INTO channels VALUES (900, 'nigel@pfg.io:Sent', 'email', 'Sent') ON CONFLICT DO NOTHING")
    await conn.execute("INSERT INTO participants VALUES (998, 'email', 'nigel@pfg.io', 'Nigel Pine') ON CONFLICT DO NOTHING")
    payload = {
        "source_folder": "Sent",
        "participants": [
            {"kind": "from", "address": "nigel@pfg.io"},
            {"kind": "to", "address": to},
            {"kind": "cc", "address": "management@pfg.io"},
        ],
    }
    await conn.execute("INSERT INTO raw_events VALUES (%s, %s)", (message_id, Jsonb(payload)))
    await conn.execute(
        "INSERT INTO messages VALUES (%s, NULL, 'zoho_mail', %s, %s, %s, %s, 'Application link', %s, 'nigel-zoho', 900, 998, NULL, %s, 'outbound')",
        (message_id, source_message_id, at, at, at, body, message_id),
    )


async def seed_quo_wake(conn) -> None:
    """A Quo prospect texting the leasing line (channel 18 is the LINE: many
    prospects share it)."""
    await conn.execute("INSERT INTO channels VALUES (18, %s, 'phone_number', 'PFG Leasing')", (QUO_LINE_PHONE,))
    await conn.execute("INSERT INTO participants VALUES (5001, 'phone', %s, NULL)", (QUO_PROSPECT,))
    await conn.execute("INSERT INTO participants VALUES (5002, 'phone', %s, NULL)", (QUO_LINE_PHONE,))
    inbound = {
        "data": {
            "object": {
                "id": "AC-in-1",
                "from": QUO_PROSPECT,
                "to": [QUO_LINE_PHONE],
                "direction": "incoming",
                "phoneNumberId": QUO_LINE,
                "conversationId": "CN-prospect",
            }
        }
    }
    await conn.execute("INSERT INTO raw_events VALUES (9001, %s)", (Jsonb(inbound),))
    await conn.execute(
        "INSERT INTO messages VALUES (9001, NULL, 'quo', 'AC-in-1', %s, %s, %s, NULL, "
        "'Hi! Yes I am interested', NULL, 18, 5001, NULL, 9001, 'inbound')",
        (QUO_SOURCE_AT, QUO_SOURCE_AT, QUO_SOURCE_AT),
    )
    await conn.execute(
        "INSERT INTO hermes_wakeup_events VALUES (%s, 'quo', 'AC-in-1', %s, %s, 'customer', NULL, 9001, %s, NULL)",
        # The wake was built five minutes after the text: an outbound sent in
        # between is before its watermark, so only the preflight can see it.
        (
            QUO_WAKE,
            QUO_SOURCE_AT + timedelta(minutes=5),
            QUO_SOURCE_AT + timedelta(minutes=5, seconds=1),
            Jsonb({"message": {"phone": QUO_PROSPECT}}),
        ),
    )


async def add_quo_outbound(conn, message_id: int, *, to: str, at: datetime, conversation: str) -> None:
    payload = {
        "data": {
            "object": {
                "id": f"AC-out-{message_id}",
                "from": QUO_LINE_PHONE,
                "to": [to],
                "direction": "outgoing",
                "phoneNumberId": QUO_LINE,
                "conversationId": conversation,
            }
        }
    }
    await conn.execute("INSERT INTO raw_events VALUES (%s, %s)", (message_id, Jsonb(payload)))
    await conn.execute(
        "INSERT INTO messages VALUES (%s, NULL, 'quo', %s, %s, %s, %s, NULL, "
        "'ok great, I have you scheduled for Saturday', NULL, 18, 5002, NULL, %s, 'outbound')",
        (message_id, f"AC-out-{message_id}", at, at, at, message_id),
    )


class Probe:
    """The real stale-context query, except that the shown set is waived from
    the fake ledger (the ledger's writes are the part that is faked)."""

    def __init__(self, repository: OutboundGatewayRepository, store: Any) -> None:
        self._repository = repository
        self._store = store

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repository, name)

    async def newer_context(self, context, *, limit, waive_shown, as_of=None):
        found = await self._repository.newer_context(context, limit=100, waive_shown=False, as_of=as_of)
        shown = {
            ref
            for row in self._store.rows.values()
            if waive_shown and row.wakeup_event_id == context.wakeup_event_id and row.state.value == "stale"
            for ref in row.stale_context_shown_refs
        }
        return [item for item in found if item.ref not in shown][:limit]


def build(conn):
    repository = OutboundGatewayRepository(SqlDriver(conn=conn))
    store = ledger.LedgerStore()
    adapter = ledger.CliqAdapter()
    service = OutboundActionService(
        store=store,
        context_loader=ActionContextLoader(repository, POLICY),
        evidence_loader=DatabasePreflightEvidenceLoader(SqlDriver(conn=conn)),
        adapters={Operation.EMAIL_SEND: adapter, Operation.QUO_SMS_SEND: adapter, Operation.CLIQ_CHAT_POST: adapter},
        provider_client=object(),
        clock=lambda: datetime(2026, 9, 26, 13, 11, 2, tzinfo=timezone.utc),
        lease_owner="outbound-gateway",
        traffic_mode="enforce",
        traffic_probe=Probe(repository, store),
        stale_confirm_enabled=True,
    )
    return service, store, adapter


IDENTITY_WAKE = 27500
IDENTITY_DM = "1424728044450751029"
IDENTITY_SOURCE_AT = datetime(2026, 9, 28, 20, 2, 55, tzinfo=timezone.utc)
IDENTITY_WATERMARK = datetime(2026, 9, 28, 20, 3, 45, tzinfo=timezone.utc)
IDENTITY_PROSPECT = "identity-prospect@example.com"
# Wake 27332's resolved identity (Aimee Tapia), reused verbatim from prod.
ENTITY_UUID = "4b2d6a38-aa45-4010-9e36-6056d0ce0fd8"
OTHER_ENTITY_UUID = "11111111-1111-1111-1111-111111111111"


async def seed_identity_wake(conn) -> None:
    """A wake CDS resolved to a person -- the same DM-sourced shape as wake
    27279, on its own channel, carrying an `identity.factbook_entity_uuid`."""
    await conn.execute("INSERT INTO channels VALUES (418, %s, 'dm', 'Dan Park (identity)')", (IDENTITY_DM,))
    await conn.execute("INSERT INTO participants VALUES (1407, 'user', '720844989', 'Dan Park') ON CONFLICT DO NOTHING")
    payload = {
        "direction": "inbound",
        "conversation_id": IDENTITY_DM,
        "participants": [{"id": "720844989", "name": "Dan Park"}],
        "provider_ids": {"cliq": IDENTITY_DM, "message": "identity-source-msg", "conversation": IDENTITY_DM},
    }
    await conn.execute("INSERT INTO raw_events VALUES (900100, %s)", (Jsonb(payload),))
    await conn.execute(
        "INSERT INTO messages VALUES (900100, NULL, 'zoho_cliq', 'identity-source-msg', %s, %s, %s, NULL, "
        "'reply to Aimee', 'nigel-zoho', 418, 1407, NULL, 900100, 'inbound')",
        (IDENTITY_SOURCE_AT, IDENTITY_SOURCE_AT, IDENTITY_SOURCE_AT),
    )
    envelope = {
        "message": {
            "id": 900100,
            "phone": None,
            "property": None,
            "proxy_email": None,
            "direct_email": None,
            "prospect_name": None,
            "sender": {"display_name": "Dan Park", "participant_key": "720844989", "participant_type": "user"},
        },
        "identity": {"factbook_entity_uuid": ENTITY_UUID, "link_type": "phone"},
        "routing_hints": {},
        "conversation_context": {"nearby_messages": []},
    }
    await conn.execute(
        "INSERT INTO hermes_wakeup_events VALUES (%s, 'zoho_cliq', 'identity-source-msg', %s, %s, 'customer', NULL, 900100, %s, NULL)",
        (IDENTITY_WAKE, IDENTITY_SOURCE_AT, IDENTITY_WATERMARK, Jsonb(envelope)),
    )


async def insert_identity_wake(conn, wake_id: int, entity_uuid: str | None) -> None:
    """Another wake's own row: only `envelope.identity.factbook_entity_uuid`
    matters to identity_wakes -- no message or channel needed."""
    envelope = {"identity": {"factbook_entity_uuid": entity_uuid}} if entity_uuid else {}
    await conn.execute(
        "INSERT INTO hermes_wakeup_events VALUES (%s, 'tenantcloud_api', %s, %s, %s, 'customer', NULL, NULL, %s, NULL)",
        (wake_id, f"tc-{wake_id}", IDENTITY_SOURCE_AT, IDENTITY_SOURCE_AT, Jsonb(envelope)),
    )


async def insert_ledger_send(
    conn,
    action_id: UUID,
    wake_id: int,
    *,
    created_at: datetime,
    state: str = "completed",
    subject_key: str = "prospect:someone-unrelated@example.com",
    operation: str = "tenantcloud.message.send",
) -> None:
    """Another wake's own outbound_actions row -- a completely different
    operation and recipient than the requesting action's, to prove the match
    is on identity, never on subject_key or the recipient address."""
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s, %s, 'prospect_reply', %s, %s, %s, %s, '{}', '{}', %s, NULL, NULL, %s)",
        (action_id, wake_id, operation, state, subject_key, created_at, created_at, f"tc-msg-{action_id}"),
    )


def identity_email_request(text: str = "Following up on Aimee's application.", to_address: str = IDENTITY_PROSPECT) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": IDENTITY_WAKE,
            "action_role": "prospect_reply",
            "operation": "email.send",
            "intent_kind": "inquiry_reply",
            "arguments": {"to_address": to_address, "text": text, "subject": "Application"},
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


@pytest_asyncio.fixture
async def conn(database):
    async with await psycopg.AsyncConnection.connect(database, autocommit=True) as connection:
        await connection.execute(SCHEMA)
        await seed_wake_27279(connection)
        await seed_quo_wake(connection)
        await seed_identity_wake(connection)
        yield connection


def email_request(text: str = EMAIL_TEXT) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": WAKE,
            "action_role": "prospect_reply",
            "operation": "email.send",
            "intent_kind": "inquiry_reply",
            "arguments": {
                "to_address": PROSPECT,
                "text": text,
                "subject": "Application link — 480-484 S Main St, Unit 7 (1BR, 3rd Floor)",
                "cc": ["management@pfg.io"],
            },
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


def sms_request(to_phone: str = QUO_PROSPECT) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": QUO_WAKE,
            "action_role": "prospect_reply",
            "operation": "quo.sms.send",
            "intent_kind": "inquiry_reply",
            "arguments": {"text": "Saturday at 9 works, see you then.", "to_phone": to_phone},
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


def answer(action_id: UUID, decision: str, arguments: dict[str, Any] | None = None, *, wake: int = WAKE) -> ConfirmRequest:
    payload: dict[str, Any] = {"op": "confirm", "wakeup_event_id": wake, "action_id": str(action_id), "decision": decision}
    if arguments is not None:
        payload["arguments"] = arguments
    parsed = parse_outbound_request(payload)
    assert isinstance(parsed, ConfirmRequest)
    return parsed


@pytest.mark.asyncio
async def test_wake_27279_the_email_dispatches_despite_the_cron_post_in_the_source_dm(conn):
    service, store, adapter = build(conn)

    first = await service.execute(email_request())

    record = store.rows[FIRST]
    assert record.canonical_context["prospect_id"] == f"prospect:{PROSPECT}"
    assert record.canonical_context["channel_id"] == 417
    assert (first.status, first.detail_code) == (PublicStatus.SENT, "provider_receipt_verified"), (
        f"email.send ended {first.status.value}/{first.detail_code} "
        f"(completion {record.completion_kind}, provider_message_id {record.provider_message_id})"
    )
    assert adapter.sent == [EMAIL_TEXT]
    assert not [call for call in store.calls if call[0] == "block_stale"]


@pytest.mark.asyncio
async def test_an_earlier_email_to_the_same_prospect_is_shown_and_the_agent_answers(conn):
    await add_sent_email(
        conn,
        806300,
        to=PROSPECT,
        at=SOURCE_AT + timedelta(minutes=2),
        source_message_id="<manual-1@pfg.io>",
        body="Hi Alberto, the application link is https://pinefield.tenantcloud.com/listings/204968",
    )
    # Another prospect's email in the same Sent folder is not this target's.
    await add_sent_email(
        conn,
        806301,
        to="someone.else@example.com",
        at=SOURCE_AT + timedelta(minutes=3),
        source_message_id="<manual-2@pfg.io>",
        body="unrelated",
    )

    outcomes = {}
    for decision in ("yes", "no", "revise"):
        service, store, adapter = build(conn)
        asked = await service.execute(email_request())
        assert asked.status is PublicStatus.NEEDS_CONFIRMATION, asked
        assert [(item.id, item.direction, item.sender) for item in asked.new_context] == [("message:806300", "sent by us", "Nigel Pine")]
        revised = None
        if decision == "revise":
            revised = email_request("Following up: did the application link come through?").arguments.model_dump(mode="json", exclude_none=True)
        result = await service.confirm(answer(FIRST, decision, revised))
        outcomes[decision] = (result.status, result.detail_code, list(adapter.sent), store.successor_of(FIRST))

    assert outcomes["yes"][0] is PublicStatus.SENT and outcomes["yes"][2] == [EMAIL_TEXT]
    assert outcomes["no"][:3] == (PublicStatus.STALE, "stale_context_declined", [])
    assert outcomes["no"][3] is None
    assert outcomes["revise"][0] is PublicStatus.SENT
    assert outcomes["revise"][2] == ["Following up: did the application link come through?"]
    assert outcomes["revise"][3].arguments["to_address"] == PROSPECT


@pytest.mark.asyncio
async def test_the_identical_request_again_is_the_same_action(conn):
    service, store, adapter = build(conn)

    first = await service.execute(email_request())
    second = await service.execute(email_request())

    assert first.status is PublicStatus.SENT
    assert second.status is PublicStatus.DUPLICATE
    assert second.action_id == first.action_id == FIRST
    assert adapter.sent == [EMAIL_TEXT]


@pytest.mark.asyncio
async def test_this_wakes_own_earlier_send_is_not_newer_context(conn):
    """A second, different email of the same wake (multi-action wakes): the
    first one's Sent-folder copy is the agent's own work, not news."""
    await add_sent_email(
        conn,
        806302,
        to=PROSPECT,
        at=SOURCE_AT + timedelta(minutes=6),
        source_message_id="<outbound-action-own@pfg.io>",
        body="the first email of this wake",
    )
    await conn.execute(
        "INSERT INTO outbound_actions VALUES (%s, %s, 'prospect_reply', 'email.send', 'completed', %s, %s, '{}', '{}', %s, NULL, NULL, %s)",
        (
            UUID("22172062-03d8-5314-8c30-1af753927536"),
            WAKE,
            f"prospect:{PROSPECT}",
            SOURCE_AT + timedelta(minutes=5),
            SOURCE_AT + timedelta(minutes=5),
            "<outbound-action-own@pfg.io>",
        ),
    )
    service, _store, adapter = build(conn)

    result = await service.execute(email_request("A second note: the deposit is $2,175."))

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["A second note: the deposit is $2,175."]


@pytest.mark.asyncio
async def test_a_text_we_sent_on_the_wake_channel_before_its_watermark_was_in_the_agents_context(conn):
    """DECLARED (was asked by the bf41be6 newer-outbound question): the wake's
    own channel is the agent's context up to the watermark, so our text at
    +2 min -- before the wake was built at +5 min -- is not news."""
    await add_quo_outbound(conn, 9002, to=QUO_PROSPECT, at=QUO_SOURCE_AT + timedelta(minutes=2), conversation="CN-prospect")
    service, _store, adapter = build(conn)

    result = await service.execute(sms_request())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Saturday at 9 works, see you then."]


@pytest.mark.asyncio
async def test_a_text_we_sent_that_prospect_after_the_watermark_is_asked(conn):
    await add_quo_outbound(conn, 9004, to=QUO_PROSPECT, at=QUO_SOURCE_AT + timedelta(minutes=6), conversation="CN-prospect")
    service, _store, adapter = build(conn)

    result = await service.execute(sms_request())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert [(item.id, item.source, item.direction) for item in result.new_context] == [("message:9004", "quo", "sent by us")]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_her_new_text_after_the_watermark_is_the_same_question(conn):
    """Newer inbound is the same question (was: a silent stale/newer_inbound)."""
    payload = {
        "data": {
            "object": {
                "id": "AC-in-2",
                "from": QUO_PROSPECT,
                "to": [QUO_LINE_PHONE],
                "direction": "incoming",
                "phoneNumberId": QUO_LINE,
                "conversationId": "CN-prospect",
            }
        }
    }
    at = QUO_SOURCE_AT + timedelta(minutes=7)
    await conn.execute("INSERT INTO raw_events VALUES (9005, %s)", (Jsonb(payload),))
    await conn.execute(
        "INSERT INTO messages VALUES (9005, NULL, 'quo', 'AC-in-2', %s, %s, %s, NULL, 'actually can we do Sunday?', "
        "NULL, 18, 5001, NULL, 9005, 'inbound')",
        (at, at, at),
    )
    service, store, adapter = build(conn)

    asked = await service.execute(sms_request())

    assert asked.status is PublicStatus.NEEDS_CONFIRMATION, asked
    assert [(item.id, item.direction) for item in asked.new_context] == [("message:9005", "received")]
    declined = await service.confirm(answer(asked.action_id, "no", wake=QUO_WAKE))
    assert declined.status is PublicStatus.STALE
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_an_outbound_text_to_another_prospect_on_the_same_line_is_not_this_targets(conn):
    """Quo channel 18 is a line, not a conversation."""
    await add_quo_outbound(conn, 9003, to=QUO_OTHER, at=QUO_SOURCE_AT + timedelta(minutes=2), conversation="CN-other")
    service, _store, adapter = build(conn)

    result = await service.execute(sms_request())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Saturday at 9 works, see you then."]


@pytest.mark.asyncio
async def test_the_same_person_on_another_source_is_asked_regardless_of_channel_or_recipient(conn):
    """Wakes 27331/27332 (2026-09-28): a TenantCloud-sourced wake and a
    Zillow-sourced wake, resolved to the SAME person, sent to two unrelated
    addresses. The old per-channel/per-subject match never saw the other
    wake's send; identity does, regardless of operation or recipient."""
    other_action = UUID("22222222-2222-2222-2222-222222222222")
    await insert_identity_wake(conn, 27331, ENTITY_UUID)
    await insert_ledger_send(conn, other_action, 27331, created_at=IDENTITY_WATERMARK + timedelta(minutes=1))
    service, _store, adapter = build(conn)

    result = await service.execute(identity_email_request())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert [(item.id, item.source) for item in result.new_context] == [(f"action:{other_action}", "outbound_actions")]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_a_different_person_is_not_asked(conn):
    other_action = UUID("33333333-3333-3333-3333-333333333333")
    await insert_identity_wake(conn, 27331, OTHER_ENTITY_UUID)
    await insert_ledger_send(conn, other_action, 27331, created_at=IDENTITY_WATERMARK + timedelta(minutes=1))
    service, _store, adapter = build(conn)

    result = await service.execute(identity_email_request())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Following up on Aimee's application."]


@pytest.mark.asyncio
async def test_an_earlier_send_before_the_context_time_is_not_asked(conn):
    other_action = UUID("44444444-4444-4444-4444-444444444444")
    await insert_identity_wake(conn, 27331, ENTITY_UUID)
    await insert_ledger_send(conn, other_action, 27331, created_at=IDENTITY_WATERMARK - timedelta(minutes=1))
    service, _store, adapter = build(conn)

    result = await service.execute(identity_email_request())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Following up on Aimee's application."]


@pytest.mark.asyncio
async def test_a_definitively_failed_send_does_not_count(conn):
    other_action = UUID("55555555-5555-5555-5555-555555555555")
    await insert_identity_wake(conn, 27331, ENTITY_UUID)
    await insert_ledger_send(conn, other_action, 27331, created_at=IDENTITY_WATERMARK + timedelta(minutes=1), state="definitive_failed")
    service, _store, adapter = build(conn)

    result = await service.execute(identity_email_request())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Following up on Aimee's application."]


# One Hermes turn answering three wakes (CDS steering), as production stored
# them on 2026-10-01: Dan sent "2+4", then "also tell me a short story" and
# "and 9+4" while the turn was running; the last two were steered into it.
# Nigel sent "6" (27457) and the story (27458); the story then made 27459's
# "13" stale twice, so it went out 3.5 min late.
TURN_SESSION = "gw0a206b9be42d44"
TURN_STREAM = "c80c27ed14c64289bedaa3754536fcac"
TURN_WAKES = {
    27457: (
        839373,
        "1790822607653_9165281454634",
        "2026-10-01 02:43:27.653+00",
        "2026-10-01 02:43:31.494932+00",
        "2026-10-01 02:43:31.531572+00",
        "2026-10-01 02:43:31.791453+00",
        "{@918334727} this is quick test message.   2+4",
    ),
    27458: (
        839377,
        "1790822615181_9169576429470",
        "2026-10-01 02:43:35.181+00",
        "2026-10-01 02:44:01.507186+00",
        "2026-10-01 02:44:01.526953+00",
        "2026-10-01 02:44:01.807227+00",
        "{@918334727}  also tell me a short story",
    ),
    27459: (
        839378,
        "1790822623382_9173871404969",
        "2026-10-01 02:43:43.382+00",
        "2026-10-01 02:44:01.527349+00",
        "2026-10-01 02:44:01.736748+00",
        "2026-10-01 02:44:02.057595+00",
        "{@918334727}  and 9+4",
    ),
}
SIX = UUID("eb73af99-c747-5fcd-8a9a-5e91207dcab6")
STORY = UUID("0d59a25f-b523-59bc-9e5c-73400854dc6b")
# (message id, provider id, sent_at, received_at, ledger action, wake, created, dispatch started, text)
TURN_SENDS = (
    (
        839375,
        "1790822640801_9178166389921",
        "2026-10-01 02:44:00.801+00",
        "2026-10-01 02:44:01.054811+00",
        SIX,
        27457,
        "2026-10-01 02:43:59.996057+00",
        "2026-10-01 02:44:00.555177+00",
        "6",
    ),
    (
        839398,
        "1790822716297_9182461432713",
        "2026-10-01 02:45:16.297+00",
        "2026-10-01 02:45:16.534519+00",
        STORY,
        27458,
        "2026-10-01 02:45:15.676178+00",
        "2026-10-01 02:45:16.121499+00",
        "Here's a short one: A lighthouse keeper's cat sat by the lamp every evening.",
    ),
)


def _turn_payload(provider_message_id: str) -> dict[str, Any]:
    return {
        "direction": "inbound",
        "conversation_id": DM,
        "participants": [{"id": "918334727", "name": "Nigel Pine"}, {"id": "720844989", "name": "Dan Park"}],
        "provider_ids": {"cliq": DM, "message": provider_message_id, "conversation": DM},
    }


async def seed_steered_turn(conn, *, story_stream: str = TURN_STREAM) -> None:
    """Wakes 27457-27459 and the two sends. Dan's messages are stored
    `outbound` (Dan's own account posted them), as in production.
    story_stream: the turn 27458 was delivered into."""
    for wake, (message_id, provider_id, sent_at, received_at, created_at, accepted_at, body) in TURN_WAKES.items():
        await conn.execute("INSERT INTO raw_events VALUES (%s, %s)", (message_id, Jsonb(_turn_payload(provider_id))))
        await conn.execute(
            "INSERT INTO messages VALUES (%s, NULL, 'zoho_cliq', %s, %s, %s, %s, NULL, %s, 'dan-zoho', 417, 407, NULL, %s, 'outbound', %s)",
            (message_id, provider_id, sent_at, sent_at, sent_at, body, message_id, received_at),
        )
        envelope = {
            "message": {
                "id": message_id,
                "phone": None,
                "property": None,
                "proxy_email": None,
                "direct_email": None,
                "prospect_name": None,
                "sender": {"display_name": "Dan Park", "participant_key": "720844989", "participant_type": "user"},
            },
            "identity": {"factbook_entity_uuid": None, "link_type": "source_participant"},
            "routing_hints": {},
            "conversation_context": {"nearby_messages": []},
        }
        await conn.execute(
            "INSERT INTO hermes_wakeup_events VALUES (%s, 'zoho_cliq', %s, %s, %s, 'customer', NULL, %s, %s, NULL, %s, %s)",
            (wake, provider_id, created_at, accepted_at, message_id, Jsonb(envelope), TURN_SESSION, story_stream if wake == 27458 else TURN_STREAM),
        )
    for message_id, provider_id, sent_at, received_at, action_id, wake, created_at, started_at, text in TURN_SENDS:
        await conn.execute("INSERT INTO raw_events VALUES (%s, %s)", (message_id, Jsonb(_turn_payload(provider_id))))
        await conn.execute(
            "INSERT INTO messages VALUES (%s, NULL, 'zoho_cliq', %s, %s, %s, %s, NULL, %s, 'nigel-zoho', 417, 997, NULL, %s, 'outbound', %s)",
            (message_id, provider_id, sent_at, sent_at, sent_at, text, message_id, received_at),
        )
        await conn.execute(
            "INSERT INTO outbound_actions VALUES (%s, %s, 'internal_reply', 'cliq.chat.post', 'completed', %s, %s, %s, '{}', %s, NULL, NULL, %s)",
            (
                action_id,
                wake,
                f"internal:{DM}",
                created_at,
                Jsonb({"text": text, "channel_or_chat_id": DM}),
                started_at,
                provider_id.replace("_", "%20"),
            ),
        )


def turn_reply(text: str = "13", wake: int = 27459) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": wake,
            "action_role": "internal_reply",
            "operation": "cliq.chat.post",
            "intent_kind": "internal_reply",
            "arguments": {"text": text, "channel_or_chat_id": DM},
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


@pytest.mark.asyncio
async def test_wake_27459_a_send_by_another_wake_of_the_same_turn_is_not_news(conn):
    """The story (wake 27458, same turn) reached Dan after 27459's context
    was built. The agent sent it itself: "13" goes out at once."""
    await seed_steered_turn(conn)
    service, _store, adapter = build(conn)

    result = await service.execute(turn_reply())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["13"]


@pytest.mark.asyncio
async def test_the_same_send_from_another_turn_is_still_asked(conn):
    """Had 27458 been its own turn, its story is another agent's send: asked,
    as before steering (message and ledger action both listed)."""
    await seed_steered_turn(conn, story_stream="another-turn")
    service, _store, adapter = build(conn)

    result = await service.execute(turn_reply())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert {item.id for item in result.new_context} == {"message:839398", f"action:{STORY}"}
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_a_new_message_from_dan_in_the_same_turn_is_still_asked(conn):
    """Inbound is never the turn's own work: a message steered in after
    27459's context may not have reached the agent yet."""
    await seed_steered_turn(conn)
    at = "2026-10-01 02:45:30+00"
    await conn.execute("INSERT INTO raw_events VALUES (839400, %s)", (Jsonb(_turn_payload("never-mind")),))
    await conn.execute(
        "INSERT INTO messages VALUES (839400, NULL, 'zoho_cliq', 'never-mind', %s, %s, %s, NULL, "
        "'{@918334727} never mind the math', 'dan-zoho', 417, 407, NULL, 839400, 'outbound', %s)",
        (at, at, at, at),
    )
    await conn.execute(
        "INSERT INTO hermes_wakeup_events VALUES (27460, 'zoho_cliq', 'never-mind', %s, %s, 'customer', NULL, 839400, '{}', NULL, %s, %s)",
        (at, at, TURN_SESSION, TURN_STREAM),
    )
    service, _store, adapter = build(conn)

    result = await service.execute(turn_reply())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert [item.id for item in result.new_context] == ["message:839400"]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_a_same_person_send_by_a_wake_of_the_same_turn_is_not_news(conn):
    """Identity path: another wake of the same person counts, unless it is
    in this wake's turn."""
    await conn.execute(
        "UPDATE hermes_wakeup_events SET webui_session_id = 'gw-identity', webui_stream_id = 'turn-1' WHERE id = %s",
        (IDENTITY_WAKE,),
    )
    for wake, stream in ((27331, "turn-1"), (27332, "turn-2")):
        await insert_identity_wake(conn, wake, ENTITY_UUID)
        await conn.execute(
            "UPDATE hermes_wakeup_events SET webui_session_id = 'gw-identity', webui_stream_id = %s WHERE id = %s",
            (stream, wake),
        )
    same_turn = UUID("66666666-6666-6666-6666-666666666666")
    other_turn = UUID("77777777-7777-7777-7777-777777777777")
    await insert_ledger_send(conn, same_turn, 27331, created_at=IDENTITY_WATERMARK + timedelta(minutes=1))
    await insert_ledger_send(conn, other_turn, 27332, created_at=IDENTITY_WATERMARK + timedelta(minutes=2))
    service, _store, adapter = build(conn)

    result = await service.execute(identity_email_request())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert [item.id for item in result.new_context] == [f"action:{other_turn}"]
    assert adapter.sent == []
