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
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from postgres_mcp.outbound_gateway.context import ActionContextLoader
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


class SqlError(Exception):
    """A psycopg error as the gateway sees it: the database's words and SQLSTATE."""

    def __init__(self, message: str, sqlstate: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


AMBIGUOUS = SqlError(
    "ambiguous aliases resolve to multiple outbound-action subjects\n"
    "CONTEXT:  PL/pgSQL function acquire_outbound_intent_lock(text,text,text,text,integer,integer,text,text[],boolean) line 52 at RAISE",
    "22023",
)


class OverrideLedger(stale_tests.LedgerStore):
    """Migration 192's ledger plus migration 251: reject_outbound_action,
    override_outbound_action, and an intent lock that refuses (`prepare_error`)
    -- which an agent_override successor never takes."""

    def __init__(self) -> None:
        super().__init__()
        self.prepare_error: Exception | None = None
        self.override_limit = 5

    async def prepare(self, ctx, expected_state):
        current = self.rows[ctx.action_id]
        if self.prepare_error is not None and current.remediation_reason != AGENT_OVERRIDE:
            self.calls.append(("prepare_refused", ctx.action_id))
            raise self.prepare_error
        return await super().prepare(ctx, expected_state)

    async def reject(self, action_id, detail_code, error_detail):
        self.calls.append(("reject", action_id, detail_code))
        current = self.rows[action_id]
        if current.state not in {ActionState.RECEIVED, ActionState.DEPENDENCY_WAIT}:
            return current
        return self._put(replace(current, state=ActionState.REJECTED, detail_code=detail_code, error_detail=error_detail))

    async def override(self, action_id, *, wakeup_event_id, actor, reason):
        self.calls.append(("override", action_id, reason))
        parent = self.rows[action_id]
        if parent.wakeup_event_id != wakeup_event_id:
            raise SqlError("override must come from the action's own wake", "42501")
        if parent.state is ActionState.COMPLETED and parent.completion_kind is CompletionKind.SENT:
            raise SqlError(f"outbound action {action_id} already sent; send a new message instead", "55000")
        existing = self.successor_of(action_id)
        if existing is not None:
            return existing
        overrides = [
            row
            for row in self.rows.values()
            if row.wakeup_event_id == parent.wakeup_event_id and row.action_role is parent.action_role and row.remediation_reason == AGENT_OVERRIDE
        ]
        if len(overrides) >= self.override_limit:
            raise SqlError(
                f"override limit reached: wake {parent.wakeup_event_id} already re-sent {parent.action_role.value} "
                f"{len(overrides)} times; record needs_human with the message you meant to send",
                "55000",
            )
        ordinal = 1 + sum(1 for row in self.rows.values() if row.wakeup_event_id == parent.wakeup_event_id and row.retry_of_action_id)
        return self._put(
            replace(
                parent,
                action_id=stale_tests.action_id_for(parent.wakeup_event_id, parent.action_role.value, ordinal),
                state=ActionState.RECEIVED,
                detail_code="received",
                completion_kind=None,
                provider_request_ref=None,
                action_uid=None,
                error_detail=None,
                retry_of_action_id=parent.action_id,
                remediation_reason=AGENT_OVERRIDE,
                stale_context_shown_refs=(),
                stale_context_decision=None,
            )
        )


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
    assert result["override"] == {
        "op": "confirm",
        "wakeup_event_id": WAKE,
        "action_id": str(action_id),
        "decision": "yes",
        "reason": "<required: why you still want to send it>",
    }
    assert '"override" request' in result["detail"]
    assert result["detail"].endswith("Never send it through any other tool or route.")


# ----------------------------------------------------------------------------
# An error after the row is recorded: rejected, explained, overridable
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wake_27865_ambiguous_aliases_end_the_recorded_row_rejected_with_the_override():
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    submitter = AsyncMock()

    result = await call(service, EXECUTE, submitter=submitter)

    assert (result["status"], result["action_id"], result["detail_code"]) == ("rejected", str(BLOCKED), "gateway_error")
    assert result["detail"].startswith("Not sent: ambiguous aliases resolve to multiple outbound-action subjects. ")
    assert "CONTEXT" not in result["detail"]
    assert_offers_override(result, BLOCKED)
    assert store.rows[BLOCKED].state is ActionState.REJECTED
    submitter.submit.assert_not_called()
    assert adapter.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        SqlError("gateway action 9f0e retains outbound lock 41", "55000"),
        SqlError("canonical outbound-action key resolves to multiple durable actions", "22023"),
        ConnectionResetError("connection reset by peer"),
        RuntimeError("anything at all"),
    ],
)
async def test_any_error_between_record_and_prepare_never_leaves_the_row_received(error):
    service, store, _adapter = harness()
    store.prepare_error = error

    result = await call(service, EXECUTE)

    assert result["status"] == "rejected"
    assert store.rows[BLOCKED].state is ActionState.REJECTED
    assert str(error).splitlines()[0] in result["detail"]
    assert_offers_override(result, BLOCKED)
    # The identical request is the same action: the same refusal, no send.
    again = await call(service, EXECUTE)
    assert (again["status"], again["action_id"]) == ("rejected", str(BLOCKED))


@pytest.mark.asyncio
async def test_a_refusal_that_cannot_be_recorded_is_still_an_ordinary_result():
    """The database is gone: the row cannot be ended either. The agent still
    gets a result (no MCP tool error), and the identical request re-drives
    the same row once it is back."""
    service, store, _adapter = harness()
    store.prepare_error = ConnectionResetError("server closed the connection unexpectedly")
    store.reject = AsyncMock(side_effect=ConnectionResetError("server closed the connection unexpectedly"))

    result = await call(service, EXECUTE)

    assert (result["status"], result["detail_code"]) == ("failed", "gateway_error")
    assert "nothing was sent by this call. Make the same call again" in result["detail"]
    assert "action_id" not in result
    assert store.rows[BLOCKED].state is ActionState.RECEIVED


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
    assert "override" not in result
    submitter.submit.assert_awaited_once_with(SUCCESSOR)
    successor = store.rows[SUCCESSOR]
    assert (successor.state, successor.retry_of_action_id, successor.remediation_reason) == (
        ActionState.PREPARED,
        BLOCKED,
        AGENT_OVERRIDE,
    )
    assert ("override", BLOCKED, REASON) in store.calls
    assert adapter.sent == []  # Restate sends it


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent",
    [
        {"state": ActionState.REJECTED, "detail_code": "wake_terminal", "error_detail": "wake 27164 is already closed"},
        {"state": ActionState.DEFINITIVE_FAILED, "detail_code": "provider_permanent_upstream_error"},
        {"state": ActionState.DEFINITIVE_FAILED, "detail_code": "retry_budget_exhausted"},
        {"state": ActionState.MANUAL_REVIEW, "detail_code": "retry_budget_exhausted_manual_review"},
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
    ctx = await stale_tests.FakeLoader().load(stale_tests.execute_request())
    await store.create_or_load(ctx)
    store._put(replace(store.rows[BLOCKED], **parent))

    status = await call(service, {"op": "status", "action_id": str(BLOCKED)})
    assert status["detail"]
    assert_offers_override(status, BLOCKED)

    sent = await service.confirm(stale_tests.confirm(BLOCKED, "yes").model_copy(update={"reason": REASON}))
    again = await service.confirm(stale_tests.confirm(BLOCKED, "yes").model_copy(update={"reason": "asked twice"}))

    assert (sent.status, sent.action_id) == (PublicStatus.SENT, SUCCESSOR)
    assert (again.status, again.action_id) == (PublicStatus.DUPLICATE, SUCCESSOR)
    assert adapter.sent == ["pong"]
    assert store.rows[BLOCKED].state is parent["state"]


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
        "record needs_human with the message you meant to send. Nothing was sent by this answer."
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
async def test_override_needs_a_reason_its_own_wake_and_yes_and_no_drops_it():
    service, store, adapter = harness()
    store.prepare_error = AMBIGUOUS
    await call(service, EXECUTE)

    no_reason = await call(service, confirm(BLOCKED, reason="   "))
    other_wake = await call(service, confirm(BLOCKED, wake=WAKE + 1))
    revise = await call(
        service,
        {"op": "confirm", "wakeup_event_id": WAKE, "action_id": str(BLOCKED), "decision": "revise", "arguments": {"text": "x"}},
    )
    dropped = await call(service, confirm(BLOCKED, "no", reason=None))

    assert no_reason["status"] == "rejected" and 'needs a reason -- say in "reason"' in no_reason["detail"]
    assert "never crosses wakes" in other_wake["detail"]
    assert "execute it as a new message" in revise["detail"]
    assert (dropped["status"], dropped["action_id"]) == ("rejected", str(BLOCKED))
    assert dropped["detail"].startswith("Not sent, as you decided")
    assert "override" not in dropped
    assert not any(call[0] == "override" for call in store.calls)
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_an_override_is_not_asked_the_stale_question_it_already_answered():
    """The agent saw the message refused and chose to send it: newer
    activity is not asked about again, and the calendar dependency is not
    waited on (the override is the agent's call on both)."""
    service, store, adapter = harness(stale_tests.CRON_ALERT)
    ctx = await stale_tests.FakeLoader().load(stale_tests.execute_request())
    await store.create_or_load(ctx)
    store._put(replace(store.rows[BLOCKED], state=ActionState.REJECTED, detail_code="gateway_error"))

    result = await service.confirm(stale_tests.confirm(BLOCKED, "yes").model_copy(update={"reason": REASON}))

    assert result.status is PublicStatus.SENT
    assert not any(call[0] == "block_stale" for call in store.calls)
    assert adapter.sent == ["pong"]


@pytest.mark.asyncio
async def test_an_override_successor_skips_the_calendar_dependency():
    store = service_tests.FakeStore(
        service_tests.row(
            ActionState.RECEIVED,
            intent_kind=IntentKind.SHOWING_CONFIRMATION,
            retry_of_action_id=service_tests.ACTION_UID,
            remediation_reason=AGENT_OVERRIDE,
        )
    )
    svc = service_tests.service(store, service_tests.FakeAdapter())
    svc._evidence_loader.load.return_value = service_tests.evidence(calendar_dependency=CalendarDependencyState.PENDING)

    result = await svc.prepare(service_tests.ACTION_ID)

    assert result.status is PublicStatus.PENDING
    assert store.current.state is ActionState.PREPARED
    svc._evidence_loader.load.assert_not_called()


# ----------------------------------------------------------------------------
# Results say why
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_of_a_provider_rejection_carries_the_providers_words():
    service, store, _adapter = harness()
    ctx = await stale_tests.FakeLoader().load(stale_tests.execute_request())
    await store.create_or_load(ctx)
    store._put(
        replace(
            store.rows[BLOCKED],
            state=ActionState.DEFINITIVE_FAILED,
            detail_code="provider_permanent_upstream_error",
            error_detail="channel not found or bot is not a member",
        )
    )

    result = await call(service, {"op": "status", "action_id": str(BLOCKED)})

    assert result["status"] == "failed"
    assert result["detail"].startswith("Not sent: channel not found or bot is not a member. To still send it")
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
    assert result["detail"].startswith(f"Not sent: wake {WAKE} is already closed. To still send it")
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
# Restate never retries what can only fail the same way, and never ends over
# a live row
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restate_prepare_of_a_refused_row_terminalizes_it_once():
    service, store, _adapter = harness()
    ctx = await stale_tests.FakeLoader().load(stale_tests.execute_request())
    await store.create_or_load(ctx)
    store.prepare_error = AMBIGUOUS
    warning = AsyncMock()
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=AsyncMock(),
        operations=frozenset({Operation.CLIQ_CHAT_POST}),
        staff_warning=warning,
        clock=lambda: stale_tests.EXECUTED_AT,
    )

    first = await coordinator.advance(BLOCKED)
    second = await coordinator.advance(BLOCKED)

    assert (first.phase, first.detail_code) == (DeliveryPhase.TERMINAL, "gateway_error")
    assert (second.phase, second.detail_code) == (DeliveryPhase.TERMINAL, "rejected")
    assert store.rows[BLOCKED].state is ActionState.REJECTED
    assert [call for call in store.calls if call[0] == "prepare_refused"] == [("prepare_refused", BLOCKED)]
    warning.warn_once.assert_awaited_once()


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
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    warning = AsyncMock()
    coordinator = OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({Operation.CLIQ_CHAT_POST}),
        staff_warning=warning,
        clock=lambda: stale_tests.EXECUTED_AT,
    )

    result = await coordinator.advance(BLOCKED)

    assert (result.phase, result.detail_code) == (DeliveryPhase.WAIT, "gateway_internal_error")
    assert result.retry_after_seconds >= 1
    warning.warn_once.assert_not_called()


# ----------------------------------------------------------------------------
# Staff posts are keyed on their recipient, never the prospect's aliases
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


def test_override_request_is_the_confirm_contract():
    hint = OverrideRequest(wakeup_event_id=WAKE, action_id=BLOCKED).model_dump(mode="json")
    parsed = parse_outbound_request({**hint, "reason": REASON})

    assert (parsed.op, parsed.decision.value, parsed.reason) == ("confirm", "yes", REASON)
    assert parse_outbound_request({**hint, "reason": "  "}).reason is None
