# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false, reportPrivateUsage=false
"""A customer reply to someone another wake already answered in the last
72 hours is refused once, naming that reply; the agent sends nothing or
overrides with a reason. Never a silent drop.

2026-10-09, wake 28084: Amy McDermott re-sent her TenantCloud inquiry 27 hours
after wake 27994 had emailed her the viewing windows (action 95d784bf). The
new wake did not see that reply's text and emailed her again in different
words; byte-identical idempotency could not tell. 2026-09-28, wakes
27331/27332: one tour request on two lanes, two different slot lists four
minutes apart.

Which earlier replies count (other wake, any of the person's contact keys,
Comm-Data-Store's window, answered or in flight) is the store's SQL over
Comm-Data-Store's migration 269 functions, proven against PostgreSQL in
tests/integration/test_gateway_recently_answered.py, including two
concurrent replies through real advisory locks. What the rows mean (the
trigger exceptions) is SafetyPreflight's (test_preflight.py). These tests pin
what the service does with the answer, and how it serializes and fails.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.errors import ContactLockTimeoutError
from postgres_mcp.outbound_gateway.errors import ContactSerializationUnavailableError
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicResult
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.preflight import RECENTLY_ANSWERED_DETAIL
from postgres_mcp.outbound_gateway.record import OVERRIDE_RULE
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import handle_outbound_action
from postgres_mcp.outbound_gateway.service import CONTACT_REPLY_BUSY_DETAIL
from postgres_mcp.outbound_gateway.service import OutboundActionService

from . import test_stale_context_confirm as stale_tests

WAKE = stale_tests.WAKE
REFUSED = stale_tests.action_id_for(WAKE, "prospect_reply", 0)
SUCCESSOR = stale_tests.action_id_for(WAKE, "prospect_reply", 1)
EARLIER = UUID("95d784bf-0a66-59d1-9fb9-1e7d325d484a")
RECIPIENT = "amc51816@gmail.com"
EARLIER_TEXT = (
    "Hi Amy,\n\nThanks for reaching out about 480-484 S Main St #7 (Carriage Apartments) in Phillipsburg, NJ. "
    "I'd be glad to set up a viewing. We show units Monday-Friday 7:00 AM-3:30 PM."
)
PRIOR = {
    "action_id": EARLIER,
    "wakeup_event_id": 27994,
    "intent": "inquiry_reply",
    "sent_at": stale_tests.EXECUTED_AT - timedelta(hours=27),
    "recipient": RECIPIENT,
    "reply_text": EARLIER_TEXT,
    "reply_status": "answered",
    "trigger_source": "tenantcloud_api",
    "trigger_is_new_question": False,
}
REPLY = "Hi Amy, thanks for reaching out about Unit 7! We show weekdays 7:00 AM-3:30 PM."
REASON = "She asked a new question about parking that the earlier reply did not cover."
POLICY = FeaturePolicy(writes_enabled=True, kill_switch=False, enabled_operations=frozenset(Operation))


class AnsweredLedger(stale_tests.LedgerStore):
    """The migration 192/251 ledger, plus the store's recently_answered
    lookup: `prior` for every action, or `error` raised."""

    def __init__(self, prior: list[dict[str, Any]] | None = None, error: Exception | None = None) -> None:
        super().__init__()
        self.prior = prior or []
        self.error = error
        self.looked_up: list[UUID] = []

    async def recently_answered(self, action_id: UUID, *, as_of: Any = None) -> list[dict[str, Any]]:
        self.looked_up.append(action_id)
        if self.error is not None:
            raise self.error
        return list(self.prior)


def harness(store: AnsweredLedger):
    probe = stale_tests.LedgerProbe(store)
    adapter = stale_tests.CliqAdapter()
    service = OutboundActionService(
        store=store,
        context_loader=stale_tests.FakeLoader(),
        evidence_loader=stale_tests.StaticEvidence(),
        adapters={Operation.CLIQ_CHAT_POST: adapter, Operation.QUO_SMS_SEND: adapter},
        provider_client=object(),
        clock=lambda: stale_tests.EXECUTED_AT,
        lease_owner="outbound-gateway",
        traffic_mode="enforce",
        traffic_probe=probe,
        stale_confirm_enabled=True,
    )
    return service, adapter


def execute(text: str = REPLY, *, intent: str = "inquiry_reply", role: str = "prospect_reply") -> dict[str, Any]:
    if role == "internal_reply":
        return {
            "op": "execute",
            "wakeup_event_id": WAKE,
            "action_role": "internal_reply",
            "operation": "cliq.chat.post",
            "intent_kind": "internal_reply",
            "arguments": {"text": text, "channel_or_chat_id": stale_tests.CHAT},
        }
    return {
        "op": "execute",
        "wakeup_event_id": WAKE,
        "action_role": role,
        "operation": "quo.sms.send",
        "intent_kind": intent,
        "arguments": {"text": text, "to_phone": "+18622665285"},
    }


async def call(service: OutboundActionService, payload: dict[str, Any]) -> dict[str, Any]:
    return await handle_outbound_action(service, POLICY, payload)


def confirm(action_id: UUID, decision: str, reason: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"op": "confirm", "wakeup_event_id": WAKE, "action_id": str(action_id), "decision": decision}
    if reason is not None:
        payload["reason"] = reason
    return payload


@pytest.mark.asyncio
async def test_amy_repeat_is_refused_naming_the_earlier_reply_and_offering_the_override() -> None:
    store = AnsweredLedger([PRIOR])
    service, adapter = harness(store)

    result = await call(service, execute())

    assert adapter.sent == []
    assert result["status"] == PublicStatus.REJECTED.value
    assert result["detail_code"] == RECENTLY_ANSWERED_DETAIL
    assert result["detail"].startswith(
        f"Not sent: RECENTLY ANSWERED: {RECIPIENT} already got a reply 27h ago (action {EARLIER}, wake 27994, inquiry_reply): "
        '"Hi Amy, Thanks for reaching out about 480-484 S Main St #7'
    )
    assert "send nothing (op confirm, decision no)" in result["detail"]
    assert result["detail"].endswith(OVERRIDE_RULE)
    assert result["override"] == {"op": "confirm", "wakeup_event_id": WAKE, "action_id": str(REFUSED), "decision": "yes"}
    assert result["recently_answered"] == [
        {
            "action_id": str(EARLIER),
            "wakeup_event_id": 27994,
            "intent": "inquiry_reply",
            "sent_at": "2026-09-22T16:37:00.335447Z",
            "recipient": RECIPIENT,
            "text_excerpt": EARLIER_TEXT,
            "reply_status": "answered",
        }
    ]
    row = store.rows[REFUSED]
    assert (row.state, row.detail_code) == (ActionState.REJECTED, RECENTLY_ANSWERED_DETAIL)
    # The words live on the row, so op status says the same thing later.
    assert row.error_detail.startswith("RECENTLY ANSWERED:")


@pytest.mark.asyncio
async def test_a_showing_offer_is_asked_about_too() -> None:
    service, adapter = harness(AnsweredLedger([PRIOR]))

    result = await service.execute(parse_outbound_request({**execute(intent="showing_offer"), "appointment_slot": "2026-09-29T14:00:00Z"}))

    assert (result.status, result.detail_code) == (PublicStatus.REJECTED, RECENTLY_ANSWERED_DETAIL)
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_the_override_sends_it_and_is_never_asked_again() -> None:
    store = AnsweredLedger([PRIOR])
    service, adapter = harness(store)
    refused = await call(service, execute())

    sent = await call(service, {**refused["override"], "reason": REASON})

    assert sent["status"] == PublicStatus.SENT.value, sent
    assert sent["action_id"] == str(SUCCESSOR)
    assert adapter.sent == [REPLY]
    # Looked up once, for the agent's own execute; the successor is its answer.
    assert store.looked_up == [REFUSED]
    assert store.rows[SUCCESSOR].remediation_reason == "agent_override"


@pytest.mark.asyncio
async def test_decision_no_records_the_choice_and_sends_nothing() -> None:
    store = AnsweredLedger([PRIOR])
    service, adapter = harness(store)
    await call(service, execute())

    declined = await call(service, confirm(REFUSED, "no"))

    assert declined["status"] == PublicStatus.REJECTED.value
    assert store.rows[REFUSED].override_decision == "no"
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_the_same_request_again_is_the_same_refusal_not_a_send() -> None:
    store = AnsweredLedger([PRIOR])
    service, adapter = harness(store)
    await call(service, execute())

    again = await call(service, execute())

    assert again["status"] == PublicStatus.REJECTED.value
    assert again["detail"].startswith("Not sent: RECENTLY ANSWERED:")
    assert "override" in again
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_nobody_answered_recently_sends_as_before() -> None:
    store = AnsweredLedger([])
    service, adapter = harness(store)

    result = await call(service, execute())

    assert result["status"] == PublicStatus.SENT.value, result
    assert "recently_answered" not in result
    assert store.looked_up == [REFUSED]
    assert adapter.sent == [REPLY]


@pytest.mark.asyncio
async def test_a_failed_lookup_never_stops_the_send() -> None:
    store = AnsweredLedger(error=RuntimeError("database unavailable"))
    service, adapter = harness(store)

    result = await call(service, execute())

    assert result["status"] == PublicStatus.SENT.value, result
    assert adapter.sent == [REPLY]


@pytest.mark.asyncio
async def test_only_prospect_inquiry_replies_and_offers_are_looked_up() -> None:
    store = AnsweredLedger([PRIOR])
    service, adapter = harness(store)

    staff = await call(service, execute("pong", role="internal_reply"))

    assert staff["status"] == PublicStatus.SENT.value, staff
    assert store.looked_up == []
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_a_stale_context_successor_is_not_asked_again() -> None:
    """The agent already answered a stale_context question with yes: its
    successor carries that answer, so recently_answered does not ask."""
    store = AnsweredLedger([PRIOR])
    newer = stale_tests.replace(stale_tests.CRON_ALERT, source="quo", preview="Is Unit 7 still open?", sender="Amy")
    probe = stale_tests.LedgerProbe(store, newer)
    adapter = stale_tests.CliqAdapter()
    service = OutboundActionService(
        store=store,
        context_loader=stale_tests.FakeLoader(),
        evidence_loader=stale_tests.StaticEvidence(),
        adapters={Operation.QUO_SMS_SEND: adapter},
        provider_client=object(),
        clock=lambda: stale_tests.EXECUTED_AT,
        lease_owner="outbound-gateway",
        traffic_mode="enforce",
        traffic_probe=probe,
        stale_confirm_enabled=True,
    )
    blocked = await call(service, execute())
    assert blocked["status"] == PublicStatus.NEEDS_CONFIRMATION.value

    sent = await call(service, confirm(REFUSED, "yes"))

    assert sent["status"] == PublicStatus.SENT.value, sent
    assert store.looked_up == []
    assert adapter.sent == [REPLY]


@pytest.mark.asyncio
async def test_the_background_prepare_never_asks() -> None:
    """Nobody is waiting on Restate's prepare: it never asks (an agent's
    execute already did, or chose to send)."""
    store = AnsweredLedger([PRIOR])
    service, _adapter = harness(store)
    context = await stale_tests.FakeLoader().load(parse_outbound_request(execute()))
    await store.create_or_load(context)

    await service.prepare(REFUSED)

    assert store.looked_up == []
    assert store.rows[REFUSED].state is ActionState.PREPARED


@pytest.mark.asyncio
async def test_status_lists_no_structured_field_when_none() -> None:
    service = AsyncMock()
    service.status.return_value = PublicResult(
        status=PublicStatus.SENT, action_id=REFUSED, action_uid=None, provider_request_ref="x", detail_code="provider_receipt_verified"
    )
    payload = await handle_outbound_action(service, POLICY, {"op": "status", "action_id": str(REFUSED)})
    assert "recently_answered" not in payload


# ----------------------------------------------------------------------------
# SafetyPreflight's exceptions, read from the rows' trigger facts.
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "facts",
    [
        {"trigger_source": "zoho_cliq"},
        {"trigger_source": "hermes_schedule"},
        {"trigger_source": "internal_qualification"},
        {"trigger_is_new_question": True},
    ],
)
async def test_a_staff_or_scheduled_wake_or_a_new_question_sends(facts: dict[str, Any]) -> None:
    store = AnsweredLedger([{**PRIOR, **facts}])
    service, adapter = harness(store)

    result = await call(service, execute())

    assert result["status"] == PublicStatus.SENT.value, result
    assert adapter.sent == [REPLY]


@pytest.mark.asyncio
async def test_a_reply_still_in_flight_is_named_as_being_sent_now() -> None:
    in_flight = {**PRIOR, "reply_status": "in_flight", "sent_at": stale_tests.EXECUTED_AT - timedelta(minutes=2)}
    store = AnsweredLedger([in_flight])
    service, adapter = harness(store)

    result = await call(service, execute())

    assert (result["status"], result["detail_code"]) == (PublicStatus.REJECTED.value, RECENTLY_ANSWERED_DETAIL)
    assert result["detail"].startswith(
        f"Not sent: RECENTLY ANSWERED: {RECIPIENT} is being sent a reply right now (action {EARLIER}, wake 27994, inquiry_reply, started 2 min ago)"
    )
    assert result["recently_answered"][0]["reply_status"] == "in_flight"
    assert "override" in result
    assert adapter.sent == []


# ----------------------------------------------------------------------------
# Serialization: two replies to one person at once. The real advisory locks
# are proven against PostgreSQL (tests/integration); this fake keeps their
# contract -- one holder per contact key, the check and the prepare inside.
# ----------------------------------------------------------------------------

AMY_KEYS = frozenset({"email:amc51816@gmail.com", "phone:8622665285", "phone:9085550101"})
LANE_A, LANE_B = 28300, 28301
PHONE_A, PHONE_B = "+18622665285", "+19085550101"
_IN_FLIGHT_STATES = {ActionState.PREPARED, ActionState.DISPATCHING, ActionState.PROVIDER_ACCEPTED, ActionState.UNKNOWN}


def phone_key(value: str) -> str:
    return "phone:" + "".join(ch for ch in value if ch.isdigit())[-10:]


class SerializedLedger(AnsweredLedger):
    """The ledger with PostgresActionStore's contact_serialized contract: a
    lock per contact key (each wake's trigger keys plus the action's own
    recipient), and recently_answered read from the ledger itself, in flight
    included. Every step yields to the event loop, so two unserialized
    replies interleave exactly like the 2026-09-28 lanes did."""

    def __init__(self, *, serialized: bool = True, lock_timeout: float = 0.5) -> None:
        super().__init__()
        self.serialized = serialized
        self.lock_timeout = lock_timeout
        self.locks: dict[str, asyncio.Lock] = {}
        self.locked_for: list[UUID] = []
        self.unavailable = False
        if not serialized:
            self.contact_serialized = None  # type: ignore[assignment]

    def keys(self, action_id: UUID) -> list[str]:
        row = self.rows[action_id]
        return sorted(AMY_KEYS | {phone_key(str(row.recipient_scope["target_id"]))})

    @asynccontextmanager
    async def contact_serialized(self, action_id: UUID, *, lock_timeout_ms: int) -> AsyncIterator[SerializedLedger]:  # type: ignore[override]
        if self.unavailable:
            raise ContactSerializationUnavailableError("function message_contact_keys(bigint) does not exist")
        held: list[asyncio.Lock] = []
        try:
            for key in self.keys(action_id):
                lock = self.locks.setdefault(key, asyncio.Lock())
                try:
                    await asyncio.wait_for(lock.acquire(), self.lock_timeout)
                except TimeoutError as error:
                    raise ContactLockTimeoutError(f"{key} held past {lock_timeout_ms} ms") from error
                held.append(lock)
            self.locked_for.append(action_id)
            yield self
        finally:
            for lock in held:
                lock.release()

    async def recently_answered(self, action_id: UUID, *, as_of: Any = None) -> list[dict[str, Any]]:
        self.looked_up.append(action_id)
        await asyncio.sleep(0)
        if self.error is not None:
            raise self.error
        this = self.rows[action_id]
        keys = set(self.keys(action_id))
        found = []
        for row in self.rows.values():
            answered = row.state is ActionState.COMPLETED and row.completion_kind is not None and row.completion_kind.value == "sent"
            if (
                row.wakeup_event_id == this.wakeup_event_id
                or row.intent_kind not in {"inquiry_reply", "showing_offer"}
                or phone_key(str(row.recipient_scope["target_id"])) not in keys
                or not (answered or row.state in _IN_FLIGHT_STATES)
            ):
                continue
            found.append(
                {
                    "action_id": row.action_id,
                    "wakeup_event_id": row.wakeup_event_id,
                    "intent": row.intent_kind,
                    "sent_at": stale_tests.EXECUTED_AT,
                    "recipient": row.recipient_scope["target_id"],
                    "reply_text": row.arguments.get("text"),
                    "reply_status": "answered" if answered else "in_flight",
                    "trigger_source": "zillow_rm_web_extract",
                    "trigger_is_new_question": False,
                }
            )
        return found

    async def prepare(self, ctx, expected_state):
        await asyncio.sleep(0)
        return await super().prepare(ctx, expected_state)


def lane(wake: int, phone: str, text: str) -> dict[str, Any]:
    return {**execute(text), "wakeup_event_id": wake, "arguments": {"text": text, "to_phone": phone}}


async def both_lanes(store: SerializedLedger) -> tuple[list[dict[str, Any]], list[str]]:
    service, adapter = harness(store)
    results = await asyncio.gather(
        call(service, lane(LANE_A, PHONE_A, "Hi Amy, we show weekdays 7-3:30.")),
        call(service, lane(LANE_B, PHONE_B, "Hi Amy, how about Tuesday at 10?")),
    )
    return list(results), adapter.sent


@pytest.mark.asyncio
async def test_two_lanes_at_once_to_two_numbers_of_one_person_send_exactly_once() -> None:
    results, sent = await both_lanes(SerializedLedger())

    statuses = sorted(result["status"] for result in results)
    assert statuses == [PublicStatus.REJECTED.value, PublicStatus.SENT.value], results
    assert len(sent) == 1
    [refused] = [result for result in results if result["status"] == PublicStatus.REJECTED.value]
    assert refused["detail_code"] == RECENTLY_ANSWERED_DETAIL
    # In flight or already sent by then: either way the first lane's reply.
    assert refused["recently_answered"][0]["reply_status"] in {"in_flight", "answered"}
    assert "override" in refused


@pytest.mark.asyncio
async def test_negative_control_without_the_lock_both_lanes_send() -> None:
    """The fake really races: unserialized, each check runs before the other
    prepares, and both send -- the 2026-09-28 double reply."""
    results, sent = await both_lanes(SerializedLedger(serialized=False))

    assert [result["status"] for result in results] == [PublicStatus.SENT.value] * 2, results
    assert len(sent) == 2


@pytest.mark.asyncio
async def test_a_lock_held_too_long_refuses_retryably_and_records_nothing() -> None:
    store = SerializedLedger(lock_timeout=0.01)
    service, adapter = harness(store)
    blocker = store.locks.setdefault("email:amc51816@gmail.com", asyncio.Lock())
    await blocker.acquire()

    busy = await call(service, lane(LANE_A, PHONE_A, REPLY))

    assert busy["status"] == PublicStatus.FAILED.value
    assert (busy["detail_code"], busy["retryable"]) == (CONTACT_REPLY_BUSY_DETAIL, True)
    assert "override" not in busy
    assert "Execute the same request again" in busy["detail"]
    assert adapter.sent == []
    action = stale_tests.action_id_for(LANE_A, "prospect_reply", 0)
    assert store.rows[action].state is ActionState.RECEIVED

    blocker.release()
    again = await call(service, lane(LANE_A, PHONE_A, REPLY))

    assert again["status"] == PublicStatus.SENT.value, again
    assert adapter.sent == [REPLY]


@pytest.mark.asyncio
async def test_after_a_lock_timeout_the_retry_sees_the_reply_that_held_it() -> None:
    store = SerializedLedger(lock_timeout=0.01)
    service, adapter = harness(store)
    sent = await call(service, lane(LANE_A, PHONE_A, "Hi Amy, we show weekdays 7-3:30."))
    assert sent["status"] == PublicStatus.SENT.value

    retried = await call(service, lane(LANE_B, PHONE_B, "Hi Amy, how about Tuesday at 10?"))

    assert (retried["status"], retried["detail_code"]) == (PublicStatus.REJECTED.value, RECENTLY_ANSWERED_DETAIL)
    assert retried["recently_answered"][0]["reply_status"] == "answered"
    assert len(adapter.sent) == 1


@pytest.mark.asyncio
async def test_the_background_prepare_leaves_a_busy_row_for_its_next_advance() -> None:
    """Nobody waits on Restate's prepare: a lock timeout is a passing error
    there, the row stays received (never rejected) and nothing is sent."""
    store = SerializedLedger(lock_timeout=0.01)
    service, _adapter = harness(store)
    context = await stale_tests.FakeLoader().load(parse_outbound_request(lane(LANE_A, PHONE_A, REPLY)))
    await store.create_or_load(context)
    blocker = store.locks.setdefault("email:amc51816@gmail.com", asyncio.Lock())
    await blocker.acquire()

    result = await service.prepare(context.action_id)

    assert result.status is PublicStatus.FAILED
    assert store.rows[context.action_id].state is ActionState.RECEIVED
    assert store.looked_up == []
    blocker.release()


@pytest.mark.asyncio
async def test_an_override_still_takes_the_lock_so_a_concurrent_reply_sees_it() -> None:
    store = SerializedLedger()
    service, adapter = harness(store)
    await call(service, lane(LANE_A, PHONE_A, "Hi Amy, we show weekdays 7-3:30."))
    refused = await call(service, lane(LANE_B, PHONE_B, "Hi Amy, parking is included."))
    assert refused["detail_code"] == RECENTLY_ANSWERED_DETAIL

    overridden = await call(service, {**refused["override"], "reason": REASON})

    assert overridden["status"] == PublicStatus.SENT.value, overridden
    successor = UUID(overridden["action_id"])
    assert successor in store.locked_for
    assert successor not in store.looked_up
    assert len(adapter.sent) == 2


@pytest.mark.asyncio
async def test_unreadable_contact_keys_send_unserialized() -> None:
    store = SerializedLedger()
    store.unavailable = True
    service, adapter = harness(store)

    result = await call(service, lane(LANE_A, PHONE_A, REPLY))

    assert result["status"] == PublicStatus.SENT.value, result
    assert adapter.sent == [REPLY]
    assert store.locked_for == []


@pytest.mark.asyncio
async def test_a_failed_lookup_inside_the_lock_still_sends() -> None:
    store = SerializedLedger()
    store.error = RuntimeError("function prospect_replies_to_keys(text[], timestamp with time zone, boolean) does not exist")
    service, adapter = harness(store)

    result = await call(service, lane(LANE_A, PHONE_A, REPLY))

    assert result["status"] == PublicStatus.SENT.value, result
    assert adapter.sent == [REPLY]
    assert store.locked_for == [stale_tests.action_id_for(LANE_A, "prospect_reply", 0)]
