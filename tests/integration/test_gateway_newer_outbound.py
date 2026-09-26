# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportOptionalIterable=false
"""Newer outbound against PostgreSQL: the real context, evidence and probe SQL.

Wake 27279 (2026-09-26) exactly as production stored it: Dan's request in the
Cliq DM (channel 417, message 806237), then an unrelated cron alert posted to
the same DM as outbound (806236), then Nigel's email.send to the prospect with
management@pfg.io on cc. The pre-fix preflight took the cron post for "we
already replied" and completed the email as duplicate/already_handled.

Everything the gateway reads runs as SQL here -- the wake/context load, the
preflight evidence query, the traffic probe. Only the outbound_actions ledger
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
        recipient_participant_id bigint, raw_event_id bigint, direction text
    );
    CREATE TEMP TABLE hermes_wakeup_events (
        id bigint PRIMARY KEY, source text, source_event_id text, created_at timestamptz,
        webui_accepted_at timestamptz, provenance text, qualification_run_id text,
        message_id bigint, envelope jsonb, tenantcloud_claim_id bigint
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
    """The real traffic probe, except that the shown set is read from the fake
    ledger (the ledger's writes are the part that is faked)."""

    def __init__(self, repository: OutboundGatewayRepository, store: Any) -> None:
        self._repository = repository
        self._store = store

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repository, name)

    async def acknowledged_refs(self, wakeup_event_id: int, recipient_key: str) -> frozenset[str]:
        return frozenset(
            ref
            for row in self._store.rows.values()
            if row.wakeup_event_id == wakeup_event_id and row.state.value == "stale"
            for ref in row.stale_context_shown_refs
        )


def build(conn):
    repository = OutboundGatewayRepository(SqlDriver(conn=conn))
    store = ledger.LedgerStore()
    adapter = ledger.CliqAdapter()
    service = OutboundActionService(
        store=store,
        context_loader=ActionContextLoader(repository, POLICY),
        evidence_loader=DatabasePreflightEvidenceLoader(SqlDriver(conn=conn)),
        adapters={Operation.EMAIL_SEND: adapter, Operation.QUO_SMS_SEND: adapter},
        provider_client=object(),
        clock=lambda: datetime(2026, 9, 26, 13, 11, 2, tzinfo=timezone.utc),
        lease_owner="outbound-gateway",
        traffic_mode="enforce",
        traffic_probe=Probe(repository, store),
        stale_confirm_enabled=True,
    )
    return service, store, adapter


@pytest_asyncio.fixture
async def conn(database):
    async with await psycopg.AsyncConnection.connect(database, autocommit=True) as connection:
        await connection.execute(SCHEMA)
        await seed_wake_27279(connection)
        await seed_quo_wake(connection)
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
        assert [(item.id, item.direction, item.sender) for item in asked.new_context] == [("message:806300", "outbound", "sent by us (Nigel Pine)")]
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
async def test_a_quo_reply_after_we_already_texted_that_prospect_is_asked(conn):
    await add_quo_outbound(conn, 9002, to=QUO_PROSPECT, at=QUO_SOURCE_AT + timedelta(minutes=2), conversation="CN-prospect")
    service, store, adapter = build(conn)

    result = await service.execute(sms_request())

    assert result.status is PublicStatus.NEEDS_CONFIRMATION, result
    assert [(item.id, item.source, item.direction) for item in result.new_context] == [("message:9002", "quo", "outbound")]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_an_outbound_text_to_another_prospect_on_the_same_line_is_not_this_targets(conn):
    """Quo channel 18 is a line, not a conversation."""
    await add_quo_outbound(conn, 9003, to=QUO_OTHER, at=QUO_SOURCE_AT + timedelta(minutes=2), conversation="CN-other")
    service, _store, adapter = build(conn)

    result = await service.execute(sms_request())

    assert result.status is PublicStatus.SENT, result
    assert adapter.sent == ["Saturday at 9 works, see you then."]
