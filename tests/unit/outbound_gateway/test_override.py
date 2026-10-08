# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false, reportPrivateUsage=false
"""Outbound -> error -> the agent reads why and may still send it.

Built on wake 27865 (2026-10-06): one prospect's aliases had been minted two
outbound-action subjects, acquire_outbound_intent_lock raised "ambiguous
aliases resolve to multiple outbound-action subjects" after the row was
already recorded, the agent got a raw MCP tool error (three of them parked
the gateway for every caller), and three rows sat `received` forever.

Now every failure is an ordinary result: a row nothing started on is ended
rejected with the error's words, and every result that did not send carries
the one request that still sends it -- op confirm, decision yes, with a
reason (Comm-Data-Store override_outbound_action) -- never another route.
The ledger below adds migration 251's reject/override to the stale-context
ledger, and an intent lock that refuses until an override skips it.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.delivery_workflow import AuthResult
from postgres_mcp.outbound_gateway.delivery_workflow import AuthState
from postgres_mcp.outbound_gateway.delivery_workflow import DeliveryPhase
from postgres_mcp.outbound_gateway.delivery_workflow import OutboundDeliveryCoordinator
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import CompletionKind
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import OverrideRequest
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.preflight import CalendarDependencyState
from postgres_mcp.outbound_gateway.record import AGENT_OVERRIDE
from postgres_mcp.outbound_gateway.record import OVERRIDE_RULE
from postgres_mcp.outbound_gateway.repository import AliasResolution
from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository
from postgres_mcp.outbound_gateway.retry_policy import CONTEXT_RELOAD_WAIT_SECONDS
from postgres_mcp.outbound_gateway.retry_policy import RETRY_CEILING_SECONDS
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import create_server
from postgres_mcp.outbound_gateway.server import handle_outbound_action
from postgres_mcp.outbound_gateway.service import OutboundActionService

from . import test_context as context_tests
from . import test_service as service_tests
from . import test_stale_context_confirm as stale_tests

WAKE = stale_tests.WAKE
CHAT = stale_tests.CHAT
BLOCKED = stale_tests.BLOCKED
SUCCESSOR = stale_tests.SUCCESSOR
ROUTED = frozenset({Operation.CLIQ_CHAT_POST, Operation.QUO_SMS_SEND})
POLICY = FeaturePolicy(writes_enabled=True, kill_switch=False, enabled_operations=frozenset(Operation))
REASON = "Dan asked for this reply; the refusal was a duplicate-subject glitch, not a reason to stay silent."


SqlError = stale_tests.SqlError

AMBIGUOUS = SqlError(
    "ambiguous aliases resolve to multiple outbound-action subjects\n"
    "CONTEXT:  PL/pgSQL function acquire_outbound_intent_lock(text,text,text,text,integer,integer,text,text[],boolean) line 52 at RAISE",
    "22023",
)


class OverrideLedger(stale_tests.LedgerStore):
    """The migration 192/251 ledger, plus an intent lock that refuses
    (`prepare_error`) -- which an agent_override successor never takes."""

    def __init__(self) -> None:
        super().__init__()
        self.prepare_error: Exception | None = None

    async def prepare(self, ctx, expected_state):
        current = self.rows[ctx.action_id]
        if self.prepare_error is not None and current.remediation_reason != AGENT_OVERRIDE:
            self.calls.append(("prepare_refused", ctx.action_id))
            raise self.prepare_error
        return await super().prepare(ctx, expected_state)


def harness(*activity):
    store = OverrideLedger()
    probe = stale_tests.LedgerProbe(store, *activity)
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
        restate_operations=ROUTED,
    )
    return service, store, adapter


EXECUTE = {
    "op": "execute",
    "wakeup_event_id": WAKE,
    "action_role": "internal_reply",
    "operation": "cliq.chat.post",
    "intent_kind": "internal_reply",
    "arguments": {"text": "pong", "channel_or_chat_id": CHAT},
}


def confirm(action_id, decision="yes", *, reason: str | None = REASON, wake=WAKE) -> dict[str, Any]:
    payload: dict[str, Any] = {"op": "confirm", "wakeup_event_id": wake, "action_id": str(action_id), "decision": decision}
    if reason is not None:
        payload["reason"] = reason
    return payload


async def call(service, payload, *, submitter=None, routed=ROUTED):
    return await handle_outbound_action(service, POLICY, payload, tenantcloud_submitter=submitter or AsyncMock(), restate_operations=routed)


def assert_offers_override(result: dict[str, Any], action_id) -> None:
    # The request without a reason: there is no placeholder to echo back.
    assert result["override"] == {"op": "confirm", "wakeup_event_id": WAKE, "action_id": str(action_id), "decision": "yes"}
    assert result["detail"].endswith(f"{OVERRIDE_RULE} Add reason: why you still want to send.")


def assert_no_override(result: dict[str, Any]) -> None:
    assert "override" not in result
    assert OVERRIDE_RULE not in result.get("detail", "")


async def recorded_parent(store, **fields):
    """The wake's first internal_reply, recorded, then set to `fields`."""
    ctx = await stale_tests.FakeLoader().load(stale_tests.execute_request())
    await store.create_or_load(ctx)
    return store._put(replace(store.rows[BLOCKED], **fields))


# ----------------------------------------------------------------------------
# An error after the row is recorded: rejected, explained, overridable
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wake_27865_a_refused_prepare_ends_the_recorded_row_rejected_with_the_override():
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    submitter = AsyncMock()

    result = await call(service, EXECUTE, submitter=submitter)

    assert (result["status"], result["action_id"], result["detail_code"]) == ("rejected", str(BLOCKED), "gateway_refused")
    assert result["detail"].startswith("Not sent: ambiguous aliases resolve to multiple outbound-action subjects. ")
    assert "CONTEXT" not in result["detail"]
    assert_offers_override(result, BLOCKED)
    assert store.rows[BLOCKED].state is ActionState.REJECTED
    # A refusal fails the same way every time: tried once, never again.
    assert [call for call in store.calls if call[0] == "prepare_refused"] == [("prepare_refused", BLOCKED)]
    submitter.submit.assert_not_called()
    assert adapter.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "code", "tries"),
    [
        (SqlError("gateway action 9f0e retains outbound lock 41", "55000"), "gateway_refused", 1),
        (SqlError("canonical outbound-action key resolves to multiple durable actions", "22023"), "gateway_refused", 1),
        (SqlError("could not serialize access due to concurrent update", "40001"), "gateway_transient_error", 2),
        (ConnectionResetError("connection reset by peer"), "gateway_transient_error", 2),
        (RuntimeError("anything at all"), "gateway_error", 1),
    ],
    ids=["refusal_55000", "refusal_22023", "serialization", "connection_reset", "fault"],
)
async def test_any_error_between_record_and_prepare_never_leaves_the_row_received(error, code, tries):
    service, store, _adapter = harness()
    store.prepare_error = error

    result = await call(service, EXECUTE)

    assert (result["status"], result["detail_code"]) == ("rejected", code)
    assert store.rows[BLOCKED].state is ActionState.REJECTED
    assert str(error).splitlines()[0] in result["detail"]
    assert_offers_override(result, BLOCKED)
    # A passing error is tried once more before the row is ended; the
    # result says so.
    assert len([call for call in store.calls if call[0] == "prepare_refused"]) == tries
    assert ("an override retries it" in result["detail"]) is (code == "gateway_transient_error")
    # The identical request is the same action: the same refusal, no send.
    again = await call(service, EXECUTE)
    assert (again["status"], again["action_id"]) == ("rejected", str(BLOCKED))


@pytest.mark.asyncio
async def test_a_passing_error_tried_once_more_that_succeeds_just_sends():
    service, store, adapter = harness()
    errors = [SqlError("deadlock detected", "40P01")]
    original = store.prepare

    async def deadlock_once(ctx, expected_state):
        if errors:
            raise errors.pop()
        return await original(ctx, expected_state)

    store.prepare = deadlock_once

    result = await call(service, EXECUTE, routed=frozenset())

    assert (result["status"], result["action_id"]) == ("sent", str(BLOCKED))
    assert not any(call[0] == "reject" for call in store.calls)
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_a_refusal_that_cannot_be_recorded_names_the_action_and_an_override_still_sends_it():
    """The database is gone: the row cannot be ended either. The agent still
    gets an ordinary result naming the action. The identical request
    re-drives it, and so does an override: the `received` row is ended
    first, so it can never be left where nothing picks it up."""
    service, store, adapter = harness()
    store.prepare_error = ConnectionResetError("server closed the connection unexpectedly")
    reject = store.reject
    store.reject = AsyncMock(side_effect=ConnectionResetError("server closed the connection unexpectedly"))

    result = await call(service, EXECUTE)

    assert (result["status"], result["detail_code"], result["action_id"]) == ("failed", "gateway_internal_error", str(BLOCKED))
    assert "Execute the same request again to retry" in result["detail"]
    assert store.rows[BLOCKED].state is ActionState.RECEIVED

    store.prepare_error = None
    store.reject = reject
    sent = await call(service, confirm(BLOCKED), routed=frozenset())

    assert (sent["status"], sent["action_id"]) == ("sent", str(SUCCESSOR))
    assert (store.rows[BLOCKED].state, store.rows[BLOCKED].detail_code) == (ActionState.REJECTED, "superseded_by_override")
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_a_failed_readback_after_the_provider_call_never_says_not_sent():
    """The provider may have the message: if even the row cannot be read
    back, the result says "not yet confirmed", never "not sent"."""
    service, store, adapter = harness()
    get = store.get
    adapter.invoke = AsyncMock(side_effect=TimeoutError("provider read timed out"))

    async def flaky_get(action_id):
        if store.rows.get(action_id) is not None and store.rows[action_id].state is ActionState.DISPATCHING:
            raise ConnectionResetError("reset")
        return await get(action_id)

    store.get = flaky_get

    result = await call(service, EXECUTE, routed=frozenset())

    assert (result["status"], result["action_id"]) == ("pending", str(BLOCKED))
    assert "not yet confirmed sent" in result["detail"]
    assert "Not sent" not in result["detail"]
    assert_no_override(result)


# ----------------------------------------------------------------------------
# op confirm yes + reason: the override
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_override_of_the_refused_row_submits_its_successor_to_restate():
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    await call(service, EXECUTE)
    submitter = AsyncMock()

    result = await call(service, confirm(BLOCKED), submitter=submitter)

    assert (result["status"], result["action_id"]) == ("pending", str(SUCCESSOR))
    assert_no_override(result)
    submitter.submit.assert_awaited_once_with(SUCCESSOR)
    successor = store.rows[SUCCESSOR]
    assert (successor.state, successor.retry_of_action_id, successor.remediation_reason) == (
        ActionState.PREPARED,
        BLOCKED,
        AGENT_OVERRIDE,
    )
    assert ("override", BLOCKED, "yes", REASON) in store.calls
    assert adapter.sent == []  # Restate sends it


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent",
    [
        {"state": ActionState.REJECTED, "detail_code": "wake_terminal", "error_detail": "wake 27164 is already closed"},
        {"state": ActionState.REJECTED, "detail_code": "gateway_refused", "error_detail": "ambiguous aliases"},
        {"state": ActionState.DEFINITIVE_FAILED, "detail_code": "provider_permanent_upstream_error"},
        {"state": ActionState.MANUAL_REVIEW, "detail_code": "persisted_context_unavailable"},
        {"state": ActionState.DEAD_LETTER, "detail_code": "calendar_dependency_failed"},
        {"state": ActionState.STALE, "detail_code": "stale_context_unasked"},
        {
            "state": ActionState.COMPLETED,
            "detail_code": "existing_lock_receipt",
            "completion_kind": CompletionKind.DUPLICATE,
            "provider_request_ref": "cliq-earlier",
        },
    ],
    ids=lambda parent: f"{parent['state'].value}/{parent['detail_code']}",
)
async def test_every_not_sent_outcome_offers_the_override_and_the_override_sends_once(parent):
    service, store, adapter = harness()
    await recorded_parent(store, **parent)

    status = await call(service, {"op": "status", "action_id": str(BLOCKED)})
    assert status["detail"].startswith("Not sent")
    assert_offers_override(status, BLOCKED)

    sent = await call(service, confirm(BLOCKED), routed=frozenset())
    again = await call(service, confirm(BLOCKED, reason="asked twice"), routed=frozenset())

    assert (sent["status"], sent["action_id"]) == ("sent", str(SUCCESSOR))
    assert (again["status"], again["action_id"]) == ("duplicate", str(SUCCESSOR))
    assert adapter.sent == ["pong"]
    assert store.rows[BLOCKED].state is parent["state"]
    assert (store.rows[BLOCKED].override_decision, store.rows[SUCCESSOR].remediation_reason) == ("yes", AGENT_OVERRIDE)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent",
    [
        {"state": ActionState.COMPLETED, "detail_code": "provider_receipt_verified", "completion_kind": CompletionKind.SENT},
        {"state": ActionState.COMPLETED, "detail_code": "operator_positive_evidence", "completion_kind": CompletionKind.DUPLICATE},
        {"state": ActionState.DEFINITIVE_FAILED, "detail_code": "retry_budget_exhausted"},
        {"state": ActionState.MANUAL_REVIEW, "detail_code": "retry_budget_exhausted_manual_review", "dispatch_started_at": stale_tests.EXECUTED_AT},
        {"state": ActionState.DEAD_LETTER, "detail_code": "prior_dispatch_ambiguous", "dispatch_started_at": stale_tests.EXECUTED_AT},
    ],
    ids=lambda parent: f"{parent['state'].value}/{parent['detail_code']}",
)
async def test_a_message_that_may_have_reached_the_recipient_is_never_offered_again(parent):
    """A real send, an operator-proven delivery, and anything that reached
    the provider are not overridable (Comm-Data-Store answer_outbound_action
    refuses them): no override is offered, and confirm says what to do
    instead."""
    service, store, adapter = harness()
    await recorded_parent(store, **{"provider_request_ref": "cliq-1", **parent})

    status = await call(service, {"op": "status", "action_id": str(BLOCKED)})
    refused = await call(service, confirm(BLOCKED), routed=frozenset())

    assert_no_override(status)
    assert (refused["status"], refused["detail_code"]) == ("rejected", "request_refused")
    assert ("already sent" in refused["detail"]) or ("may already have been delivered" in refused["detail"])
    assert not any(call[0] == "override" for call in store.calls)
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_after_an_override_the_parent_reports_its_successor_and_never_offers_it_again():
    """T4: status on the refused action, and the identical request again,
    answer with the send the override made -- never "not sent" plus another
    override, which invites a second message."""
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    await call(service, EXECUTE)
    await call(service, confirm(BLOCKED), routed=frozenset())
    assert adapter.sent == ["pong"]

    status = await call(service, {"op": "status", "action_id": str(BLOCKED)})
    again = await call(service, EXECUTE, routed=frozenset())

    for result in (status, again):
        assert result["action_id"] == str(SUCCESSOR)
        assert result["status"] in {"sent", "duplicate"}
        assert result["detail"].startswith(f"Action {BLOCKED} was not sent itself; your answer sent it as action {SUCCESSOR}.")
        assert_no_override(result)
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_a_failed_override_can_itself_be_overridden_until_the_cap_says_so_plainly():
    service, store, _adapter = harness()
    store.prepare_error = AMBIGUOUS
    store.override_limit = 1
    await call(service, EXECUTE)
    first = await call(service, confirm(BLOCKED))
    store._put(replace(store.rows[SUCCESSOR], state=ActionState.DEFINITIVE_FAILED, detail_code="provider_rejected"))

    failed = await call(service, {"op": "status", "action_id": str(SUCCESSOR)})
    assert_offers_override(failed, SUCCESSOR)
    capped = await call(service, confirm(SUCCESSOR))

    assert first["action_id"] == str(SUCCESSOR)
    assert (capped["status"], capped["detail_code"]) == ("rejected", "request_refused")
    assert capped["detail"].startswith(
        f"confirm refused for action {SUCCESSOR}: override limit reached: wake {WAKE} already re-sent internal_reply 1 times; "
        "record needs_human with the message you meant to send."
    )
    assert "any other route around the outbound gateway" in capped["detail"]


@pytest.mark.asyncio
async def test_confirm_on_a_message_that_was_sent_says_so_and_to_send_a_new_one():
    service, store, adapter = harness()
    sent = await call(service, EXECUTE, routed=frozenset())
    assert sent["status"] == "sent" and "override" not in sent and "detail" not in sent

    refused = await call(service, confirm(BLOCKED))

    assert (refused["status"], refused["detail_code"]) == ("rejected", "request_refused")
    assert refused["detail"].startswith(f"confirm refused: action {BLOCKED} was already sent; send a new message instead")
    assert not any(call[0] == "override" for call in store.calls)
    assert adapter.sent == ["pong"]
    repeated = await call(service, EXECUTE, routed=frozenset())
    assert repeated["status"] == "duplicate" and "override" not in repeated
    assert repeated["detail"].startswith("Already sent")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason", [None, "   ", "<why>", "<required: why you still want to send it>", "reason", "Add reason: why you still want to send."]
)
async def test_an_override_needs_a_written_reason_not_a_blank_or_a_template(reason):
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    result = await call(service, EXECUTE)
    # The hint, echoed back with whatever stands in for a reason.
    echoed = {**result["override"], **({} if reason is None else {"reason": reason})}

    refused = await call(service, echoed)

    assert refused["status"] == "rejected" and 'needs a reason -- say in "reason"' in refused["detail"]
    assert not any(call[0] == "override" for call in store.calls)
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_an_override_comes_from_its_own_wake_and_never_revises():
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    await call(service, EXECUTE)

    other_wake = await call(service, confirm(BLOCKED, wake=WAKE + 1))
    revise = await call(
        service,
        {"op": "confirm", "wakeup_event_id": WAKE, "action_id": str(BLOCKED), "decision": "revise", "arguments": {"text": "x"}},
    )

    assert "never crosses wakes" in other_wake["detail"]
    assert "execute it as a new message" in revise["detail"]
    assert not any(call[0] == "override" for call in store.calls)
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_no_is_recorded_on_the_refused_action_as_a_deliberate_no_send():
    """T7: the agent's no is durable (override_decision), so the receipt
    barrier and the outcome readers see a decision, not an abandoned
    failure; the action is never offered again, and a later yes is refused."""
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    await call(service, EXECUTE)

    dropped = await call(service, confirm(BLOCKED, "no", reason=None))
    status = await call(service, {"op": "status", "action_id": str(BLOCKED)})
    again = await call(service, confirm(BLOCKED, "no", reason=None))
    late_yes = await call(service, confirm(BLOCKED))

    assert ("override", BLOCKED, "no", None) in store.calls
    assert store.rows[BLOCKED].override_decision == "no"
    for result in (dropped, status, again):
        assert (result["status"], result["action_id"]) == ("rejected", str(BLOCKED))
        assert result["detail"].startswith("Declined: nothing was sent for this action.")
        assert_no_override(result)
    assert late_yes["status"] == "rejected" and "you already answered no" in late_yes["detail"]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_an_override_is_not_asked_the_stale_question_it_already_answered():
    """The agent saw the message refused and chose to send it: newer
    activity is not asked about again."""
    service, store, adapter = harness(stale_tests.CRON_ALERT)
    await recorded_parent(store, state=ActionState.REJECTED, detail_code="gateway_error")

    result = await call(service, confirm(BLOCKED), routed=frozenset())

    assert result["status"] == "sent"
    assert not any(call[0] == "block_stale" for call in store.calls)
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_a_stale_question_answered_yes_after_the_wake_ended_still_sends():
    """T6: the wake was closed (the reconciler, a slow turn) while its
    stale_context question was open. Comm-Data-Store's one answer body sends
    a late yes as an agent_override, so the gateway answers it like any
    other yes -- no special case, no dead end."""
    service, store, adapter = harness(stale_tests.CRON_ALERT)
    blocked = await call(service, EXECUTE, routed=frozenset())
    assert blocked["status"] == "needs_confirmation"
    store.wake_terminal = True

    result = await call(service, stale_tests.confirm(BLOCKED, "yes").model_dump(mode="json"), routed=frozenset())

    assert (result["status"], result["action_id"]) == ("sent", str(SUCCESSOR))
    assert store.rows[SUCCESSOR].remediation_reason == AGENT_OVERRIDE
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_an_override_successor_still_waits_on_the_calendar_dependency():
    """T2: the override waives only the stale_context question and the
    intent-lock dedupe. A showing confirmation still waits for its calendar
    event: the agent never saw that dependency, and overriding a refusal
    about something else must not confirm a showing with no event."""
    store = service_tests.FakeStore(
        service_tests.row(
            ActionState.RECEIVED,
            intent_kind=IntentKind.SHOWING_CONFIRMATION,
            retry_of_action_id=service_tests.ACTION_UID,
            remediation_reason=AGENT_OVERRIDE,
        )
    )
    svc = service_tests.service(store, service_tests.FakeAdapter())
    svc._context_loader.load.return_value = replace(service_tests.context(), intent_kind=IntentKind.SHOWING_CONFIRMATION)
    svc._evidence_loader.load.return_value = service_tests.evidence(calendar_dependency=CalendarDependencyState.PENDING)

    result = await svc.prepare(service_tests.ACTION_ID)

    assert (result.status, result.detail_code) == (PublicStatus.PENDING, "calendar_dependency_pending")
    assert store.current.state is ActionState.DEPENDENCY_WAIT
    assert not any(call[0] == "prepare" for call in store.calls)


# ----------------------------------------------------------------------------
# Results say why
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_of_a_provider_rejection_carries_the_providers_words():
    service, store, _adapter = harness()
    await recorded_parent(
        store,
        state=ActionState.DEFINITIVE_FAILED,
        detail_code="provider_permanent_upstream_error",
        error_detail="channel not found or bot is not a member",
    )

    result = await call(service, {"op": "status", "action_id": str(BLOCKED)})

    assert result["status"] == "failed"
    assert result["detail"].startswith(f"Not sent: channel not found or bot is not a member. {OVERRIDE_RULE}")
    assert_offers_override(result, BLOCKED)


@pytest.mark.asyncio
async def test_a_parked_row_that_never_dispatched_says_it_was_not_sent():
    """T12: manual_review / dead_letter before any dispatch never reached a
    provider -- not "may already have reached the recipient"."""
    service, store, _adapter = harness()
    await recorded_parent(store, state=ActionState.MANUAL_REVIEW, detail_code="persisted_context_unavailable")

    result = await call(service, {"op": "status", "action_id": str(BLOCKED)})

    assert result["status"] == "manual_review"
    assert result["detail"].startswith("Not sent: persisted_context_unavailable.")
    assert "may already have reached" not in result["detail"]
    assert_offers_override(result, BLOCKED)


@pytest.mark.asyncio
async def test_a_message_for_a_closed_wake_is_recorded_rejected_and_overridable():
    """Comm-Data-Store 251: create_or_load records a request on a terminal
    wake as rejected (wake_terminal) instead of raising."""
    service, store, _adapter = harness()
    original = store.create_or_load

    async def closed_wake(ctx):
        row = await original(ctx)
        return store._put(replace(row, state=ActionState.REJECTED, detail_code="wake_terminal", error_detail=f"wake {WAKE} is already closed"))

    store.create_or_load = closed_wake

    result = await call(service, EXECUTE)

    assert (result["status"], result["detail_code"]) == ("rejected", "wake_terminal")
    assert result["detail"].startswith(f"Not sent: wake {WAKE} is already closed. {OVERRIDE_RULE}")
    assert_offers_override(result, BLOCKED)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "code", "words"),
    [
        (FeaturePolicy(writes_enabled=True, kill_switch=True), "kill_switch_open", "an operator has paused sending"),
        (FeaturePolicy(writes_enabled=False, kill_switch=False), "writes_disabled", "an operator has paused sending"),
        (
            FeaturePolicy(writes_enabled=True, kill_switch=False, enabled_operations=frozenset({Operation.EMAIL_SEND})),
            "operation_disabled",
            "cliq.chat.post is not enabled on this gateway, and nothing was recorded. Enabled operations: email.send.",
        ),
    ],
)
async def test_a_switched_off_send_says_why_and_what_to_do(policy, code, words):
    result = await handle_outbound_action(AsyncMock(), policy, EXECUTE)

    assert (result["status"], result["detail_code"]) == ("rejected", code)
    assert words in result["detail"] and "Never send it through any other tool or route." in result["detail"]
    assert "override" not in result


@pytest.mark.asyncio
async def test_three_failures_in_a_row_are_three_ordinary_results():
    """hermes-agent parks the gateway after three MCP tool errors in a row
    (wake 27865, 2026-10-06 18:14:55Z): none of these is one."""
    service, store, _adapter = harness()
    store.prepare_error = AMBIGUOUS
    mcp = create_server(service, POLICY, tenantcloud_submitter=AsyncMock(), restate_operations=ROUTED)
    async with create_connected_server_and_client_session(mcp) as client:
        results = [
            await client.call_tool("outbound_action", {"request": payload})
            for payload in (EXECUTE, confirm(BLOCKED, reason=None), {"op": "status", "action_id": str(SUCCESSOR)})
        ]

    assert [result.isError for result in results] == [False, False, False]
    assert [(result.structuredContent or {})["status"] for result in results] == ["rejected", "rejected", "rejected"]


# ----------------------------------------------------------------------------
# Restate: a refusal ends the row once; anything else waits under the
# ceiling, and never ends the workflow over a live row
# ----------------------------------------------------------------------------


def coordinator_for(store, service, *, clock=lambda: stale_tests.EXECUTED_AT, warning=None):
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    return OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({Operation.CLIQ_CHAT_POST}),
        staff_warning=warning or AsyncMock(),
        clock=clock,
    )


@pytest.mark.asyncio
async def test_restate_prepare_of_a_refused_row_terminalizes_it_once():
    service, store, _adapter = harness()
    await recorded_parent(store)
    store.prepare_error = AMBIGUOUS
    warning = AsyncMock()
    coordinator = coordinator_for(store, service, warning=warning)

    first = await coordinator.advance(BLOCKED)
    second = await coordinator.advance(BLOCKED)

    assert (first.phase, first.detail_code) == (DeliveryPhase.TERMINAL, "gateway_refused")
    assert (second.phase, second.detail_code) == (DeliveryPhase.TERMINAL, "rejected")
    assert store.rows[BLOCKED].state is ActionState.REJECTED
    assert [call for call in store.calls if call[0] == "prepare_refused"] == [("prepare_refused", BLOCKED)]
    warning.warn_once.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [SqlError("canceling statement due to statement timeout", "57014"), ConnectionResetError("reset"), RuntimeError("bug")],
    ids=["statement_timeout", "connection_reset", "fault"],
)
async def test_restate_prepare_that_fails_for_any_other_reason_waits_and_leaves_the_row(error):
    """Nobody is waiting on Restate's prepare: a passing error (or a fault)
    is retried on the next advance under the elapsed ceiling, never written
    as a refusal no agent will see (findings: dependency_wait/received rows
    ended rejected on a DB blip)."""
    service, store, _adapter = harness()
    await recorded_parent(store)
    store.prepare_error = error
    warning = AsyncMock()

    result = await coordinator_for(store, service, warning=warning).advance(BLOCKED)

    assert (result.phase, result.detail_code) == (DeliveryPhase.WAIT, "gateway_internal_error")
    assert result.retry_after_seconds >= 1
    assert store.rows[BLOCKED].state is ActionState.RECEIVED
    assert not any(call[0] == "reject" for call in store.calls)
    warning.warn_once.assert_not_called()


@pytest.mark.asyncio
async def test_restate_waits_on_a_context_reload_of_a_received_row_and_ends_it_at_the_ceiling():
    """The reload's own words are the result's detail; the wait keys on its
    detail_code (persisted_context_unavailable), so it is a 5-minute wait,
    not a 1-second spin, and the elapsed ceiling ends the row rejected."""
    service, store, _adapter = harness()
    await recorded_parent(store, created_at=stale_tests.EXECUTED_AT)
    service._context_loader.load = AsyncMock(side_effect=ContextDerivationError("verified target could not be derived"))

    waiting = await coordinator_for(store, service).advance(BLOCKED)
    late = coordinator_for(store, service, clock=lambda: stale_tests.EXECUTED_AT + timedelta(seconds=RETRY_CEILING_SECONDS + 1))
    ended = await late.advance(BLOCKED)

    assert (waiting.phase, waiting.detail_code, waiting.retry_after_seconds) == (
        DeliveryPhase.WAIT,
        "persisted_context_unavailable",
        CONTEXT_RELOAD_WAIT_SECONDS,
    )
    assert ended.phase is DeliveryPhase.TERMINAL
    assert (store.rows[BLOCKED].state, store.rows[BLOCKED].detail_code) == (ActionState.REJECTED, "prepare_retry_exhausted")


@pytest.mark.asyncio
async def test_a_confirm_successor_whose_context_will_not_reload_is_refused_to_the_agent_not_left_waiting():
    """With the agent waiting, a successor whose saved context cannot be
    reloaded ends rejected in the reload's words (overridable), instead of a
    `pending` Restate would spin on."""
    service, store, adapter = harness()
    await recorded_parent(store, state=ActionState.REJECTED, detail_code="gateway_refused")
    service._context_loader.load = AsyncMock(side_effect=ContextDerivationError("verified target could not be derived"))
    submitter = AsyncMock()

    result = await call(service, confirm(BLOCKED), submitter=submitter)

    assert (result["status"], result["action_id"], result["detail_code"]) == ("rejected", str(SUCCESSOR), "gateway_refused")
    assert result["detail"].startswith("Not sent: verified target could not be derived.")
    assert_offers_override(result, SUCCESSOR)
    submitter.submit.assert_not_called()
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_restate_waits_on_a_live_row_after_a_pre_send_error_instead_of_ending():
    row = SimpleNamespace(
        action_id=BLOCKED,
        wakeup_event_id=WAKE,
        operation=Operation.CLIQ_CHAT_POST,
        state=ActionState.RETRY_READY,
        created_at=stale_tests.EXECUTED_AT,
        next_attempt_at=stale_tests.EXECUTED_AT,
        arguments={},
    )
    store = AsyncMock()
    store.get.return_value = row
    service = AsyncMock()
    service.resume.return_value = SimpleNamespace(status=PublicStatus.FAILED, detail_code="gateway_internal_error", detail="Not sent")
    warning = AsyncMock()

    result = await coordinator_for(store, service, warning=warning).advance(BLOCKED)

    assert (result.phase, result.detail_code) == (DeliveryPhase.WAIT, "gateway_internal_error")
    assert result.retry_after_seconds >= 1
    warning.warn_once.assert_not_called()


# ----------------------------------------------------------------------------
# Staff posts are keyed on their recipient; a prospect is never named by a
# hub alias
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("role", "operation", "intent"),
    [
        ("internal_notification", "cliq.channel.post", "manual_review_alert"),
        ("internal_reply", "cliq.chat.post", "internal_reply"),
    ],
)
async def test_a_staff_post_on_a_split_prospects_wake_keys_on_its_recipient(role, operation, intent):
    """Wake 27865: the manual_review_alert about the split prospect was
    itself refused, because it carried that prospect's aliases."""
    dm = context_tests.record(event_source="zoho_cliq", message_source="zoho_cliq", source_channel_id="1424728044450751028", channel_type="dm")
    repository = context_tests.FakeRepository(dm, ambiguous=True)
    loader = ActionContextLoader(repository, context_tests.policy())

    ctx = await loader.load(
        context_tests.request(
            action_role=role,
            operation=operation,
            intent_kind=intent,
            appointment_slot=None,
            arguments={"channel_or_chat_id": "1424728044450751028", "text": "split prospect, please check"},
        )
    )

    assert ctx.prospect_id == "internal:1424728044450751028"
    assert ctx.aliases == ()
    assert ctx.canonical_context["prospect_id"] == "internal:1424728044450751028"
    assert repository.alias_calls == []


@pytest.mark.asyncio
async def test_a_prospect_reply_on_a_split_prospects_wake_passes_every_alias_to_the_database():
    """The database merges the split subjects when it takes the lock
    (Comm-Data-Store 251); the gateway names the person by its preferred
    alias and passes every alias through."""
    repository = context_tests.FakeRepository(context_tests.record(), ambiguous=True)

    ctx = await ActionContextLoader(repository, context_tests.policy()).load(context_tests.request())

    assert ctx.prospect_id == "prospect:factbook:aa1a1515-7929-4f17-a632-ec89c32f5895"
    assert "factbook:aa1a1515-7929-4f17-a632-ec89c32f5895" in ctx.aliases
    assert "email:amanda.abc@convo.zillow.com" in ctx.aliases
    assert len(repository.alias_calls) == 1


@pytest.mark.asyncio
async def test_a_prospect_is_never_named_by_a_hub_alias():
    """T10: our own and shared addresses (the one rule: Comm-Data-Store
    outbound_alias_is_hub, applied by the resolver's query) never name the
    prospect -- not as its subject, not as its fallback alias."""
    wake = context_tests.record(
        envelope={"identity": {}, "message": {"prospect_name": "x", "property": "138 Bullman St #144-A", "direct_email": "noreply@tenantcloud.com"}},
        participant_type="email_address",
        participant_key="noreply@tenantcloud.com",
    )
    hubs = ("email:noreply@tenantcloud.com",)
    only_hubs = await ActionContextLoader(context_tests.FakeRepository(wake, hubs=hubs), context_tests.policy()).load(context_tests.request())
    mixed = await ActionContextLoader(
        context_tests.FakeRepository(context_tests.record(), hubs=("email:amandasnyder@live.com",), ambiguous=True),
        context_tests.policy(),
    ).load(context_tests.request())

    assert only_hubs.prospect_id == f"prospect:{only_hubs.target.target_id}"
    assert "noreply" not in only_hubs.prospect_id
    assert mixed.prospect_id == "prospect:factbook:aa1a1515-7929-4f17-a632-ec89c32f5895"


@pytest.mark.asyncio
async def test_the_subject_resolver_leaves_hub_aliases_to_the_database_rule():
    """One hub rule: the resolver's query applies outbound_alias_is_hub; the
    gateway has no copy of it."""
    driver = object()
    rows = [SimpleNamespace(cells={"personal_aliases": ["phone:+19085550100"], "canonical_subject": None})]
    with patch("postgres_mcp.outbound_gateway.repository.SafeSqlDriver.execute_param_query", AsyncMock(return_value=rows)) as query:
        resolved = await OutboundGatewayRepository(driver).resolve_canonical_subject(("email:dan@pfg.io", "phone:+19085550100"), "")

    assert "NOT outbound_alias_is_hub(alias)" in query.await_args.args[1]
    assert resolved == AliasResolution(canonical_subject=None, personal_aliases=("phone:+19085550100",))


def test_override_request_is_the_confirm_contract_without_a_reason_to_echo():
    hint = OverrideRequest(wakeup_event_id=WAKE, action_id=BLOCKED).model_dump(mode="json")
    parsed = parse_outbound_request({**hint, "reason": REASON})

    assert "reason" not in hint
    assert (parsed.op, parsed.decision.value, parsed.reason) == ("confirm", "yes", REASON)
    assert parse_outbound_request({**hint, "reason": "  "}).reason is None
    assert parse_outbound_request({**hint, "reason": "<why you still want to send>"}).reason is None
