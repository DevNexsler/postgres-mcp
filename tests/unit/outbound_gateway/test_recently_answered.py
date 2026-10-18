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

Which earlier replies count (other wake, same recipient, 72 h, and the
trigger not a genuinely new question) is the store's SQL, proven against
PostgreSQL in tests/integration/test_gateway_recently_answered.py. These
tests pin what the service does with the answer.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicResult
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.record import OVERRIDE_RULE
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import handle_outbound_action
from postgres_mcp.outbound_gateway.service import RECENTLY_ANSWERED_DETAIL
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
    "text_excerpt": EARLIER_TEXT,
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

    async def recently_answered(self, action_id: UUID) -> list[dict[str, Any]]:
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
