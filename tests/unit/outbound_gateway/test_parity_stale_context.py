# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false
"""Parity: the stale-context question in stale_context.py vs the pre-split service.

Every scenario runs through LegacyOutboundActionService (the service as it was
before the split) and OutboundActionService (delegating to
StaleContextQuestions), and must produce the identical trace -- public
results, store calls, adapter/probe/loader calls, log records. See parity.py.

Three layers:

1. every existing unit test of the service and of the stale-context question,
   replayed through both sides (their own assertions run on both sides too);
2. generated rows: every action state x stale detail x recorded decision x
   due/not-due x each way an agent can touch the row (execute the same
   message, a different one, override, enqueue, confirm yes/no/revise, a
   revise that moves the target, a malformed revise, another wake's answer),
   with confirmation on and off;
3. generated conversations: seeded random sequences of executes, confirms,
   worker resumes/prepares, newly arriving activity and ledger failures over
   one ledger, across traffic modes and dispatch modes.
"""

from __future__ import annotations

import itertools
import random
import zlib
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import ConfirmRequest
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import StaleContextDecision
from postgres_mcp.outbound_gateway.traffic_control import NewerActivity

from . import test_service as service_tests
from . import test_stale_context_confirm as stale_tests
from .parity import assert_parity
from .parity import existing_scenarios
from .parity import replay_existing

EXISTING = [*existing_scenarios(stale_tests), *existing_scenarios(service_tests)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "function", "case"), EXISTING, ids=[name for name, _f, _c in EXISTING])
async def test_existing_scenario_is_identical_through_both_sides(name, function, case, caplog):
    del name
    module = stale_tests if function.__module__ == stale_tests.__name__ else service_tests
    await replay_existing(module, function, case, caplog)


def test_the_replay_covers_every_existing_scenario():
    assert len(EXISTING) >= 90
    assert {function.__module__ for _n, function, _c in EXISTING} == {stale_tests.__name__, service_tests.__name__}


# ----------------------------------------------------------------------------
# Shared world: the stale-context test module's ledger, probe, loader, adapter
# ----------------------------------------------------------------------------

WAKE = stale_tests.WAKE
CHAT = stale_tests.CHAT
BLOCKED = stale_tests.BLOCKED
SUCCESSOR = stale_tests.SUCCESSOR
NOW = stale_tests.EXECUTED_AT


def _activity(count: int) -> list[NewerActivity]:
    return [
        replace(
            stale_tests.CRON_ALERT,
            message_id=760000 + index,
            occurred_at=stale_tests.CRON_ALERT_AT + timedelta(seconds=index),
            preview=f"newer item {index} " + "x" * (index * 40),
        )
        for index in range(count)
    ]


class FlakyLedger(stale_tests.LedgerStore):
    """The migration-192 ledger, plus scripted failures of the two
    stale-context writes (migration missing, concurrent writer)."""

    def __init__(self, *, fail_block: bool = False, fail_confirm: bool = False) -> None:
        super().__init__()
        self.fail_block = fail_block
        self.fail_confirm = fail_confirm

    async def block_stale_context(self, action_id, expected_state, lease_owner, shown_refs):
        if self.fail_block:
            raise RuntimeError("function block_outbound_stale_context does not exist")
        return await super().block_stale_context(action_id, expected_state, lease_owner, shown_refs)

    async def confirm_stale_context(self, action_id, *, wakeup_event_id, decision, actor, revision=None):
        if self.fail_confirm:
            raise RuntimeError("")
        return await super().confirm_stale_context(
            action_id, wakeup_event_id=wakeup_event_id, decision=decision, actor=actor, revision=revision
        )


def _build(
    service_cls: type,
    *,
    store: stale_tests.LedgerStore,
    activity: list[NewerActivity],
    evidence: tuple[int, ...] = (),
    enabled: bool = True,
    traffic_mode: str = "enforce",
    with_probe: bool = True,
):
    probe = stale_tests.LedgerProbe(store, *activity)
    adapter = stale_tests.CliqAdapter()
    service = service_cls(
        store=store,
        context_loader=stale_tests.FakeLoader(),
        evidence_loader=stale_tests.StaticEvidence(*evidence),
        adapters={Operation.CLIQ_CHAT_POST: adapter, Operation.QUO_SMS_SEND: adapter, Operation.CLIQ_CHANNEL_POST: adapter},
        provider_client=object(),
        clock=lambda: NOW,
        lease_owner="outbound-gateway",
        traffic_mode=traffic_mode,
        traffic_probe=probe if with_probe else None,
        stale_confirm_enabled=enabled,
    )
    return service, probe


def _confirm(action_id, decision: str, arguments: dict[str, Any] | None = None, *, wake: int = WAKE) -> ConfirmRequest:
    # model_construct: the service must hold its own line even for an answer
    # the wire model would have refused (revise without arguments).
    return ConfirmRequest.model_construct(
        op="confirm", wakeup_event_id=wake, action_id=action_id, decision=StaleContextDecision(decision), arguments=arguments
    )


async def _attempt(call) -> None:
    """A raised refusal is part of the recorded trace; keep going."""
    try:
        await call
    except Exception:  # noqa: BLE001
        pass


# ----------------------------------------------------------------------------
# 2. Generated rows
# ----------------------------------------------------------------------------

DETAILS = ("stale_context", "stale_context_confirmed", "received")
DECISIONS = (None, "yes", "no", "revise")
TOUCHES = (
    "execute_same",
    "execute_different",
    "execute_override",
    "execute_other_operation",
    "enqueue_same",
    "confirm_yes",
    "confirm_no",
    "confirm_revise",
    "confirm_revise_target",
    "confirm_revise_malformed",
    "confirm_revise_without_arguments",
    "confirm_other_wake",
)
ROW_CASES = list(itertools.product(list(ActionState), DETAILS, DECISIONS, (True, False), TOUCHES))


def _row_case_id(case) -> str:
    state, detail, decision, due, touch = case
    return f"{state.value}-{detail}-{decision}-{'due' if due else 'later'}-{touch}"


async def _seed(store: stale_tests.LedgerStore, state, detail, decision, due) -> None:
    ctx = await stale_tests.FakeLoader().load(stale_tests.execute_request())
    parent = await store.create_or_load(ctx)
    later = NOW if due else NOW + timedelta(minutes=5)
    store.rows[BLOCKED] = replace(
        parent,
        state=state,
        detail_code=detail,
        stale_context_decision=decision,
        stale_context_shown_refs=("message:750824",) if state is ActionState.STALE else (),
        action_uid=BLOCKED if state is not ActionState.RECEIVED else None,
        next_attempt_at=later,
        completion_kind=service_tests.CompletionKind.SENT if state is ActionState.COMPLETED else None,
    )
    if decision in {"yes", "revise"}:
        store.rows[SUCCESSOR] = replace(
            store.rows[BLOCKED],
            action_id=SUCCESSOR,
            state=ActionState.RECEIVED,
            detail_code=f"stale_context_{'confirmed' if decision == 'yes' else 'revised'}",
            retry_of_action_id=BLOCKED,
            remediation_reason=f"stale_context_{'confirmed' if decision == 'yes' else 'revised'}",
            stale_context_decision=None,
            stale_context_shown_refs=(),
            completion_kind=None,
            action_uid=None,
        )


async def _touch(service, touch: str) -> None:
    requests = {
        "execute_same": lambda: service.execute(stale_tests.execute_request()),
        "execute_different": lambda: service.execute(stale_tests.execute_request("pong, but later")),
        "execute_override": lambda: service.execute(stale_tests.execute_request(override=True)),
        "execute_other_operation": lambda: service.execute(
            stale_tests.parse_outbound_request(
                {
                    "op": "execute",
                    "wakeup_event_id": WAKE,
                    "action_role": "internal_reply",
                    "operation": "cliq.channel.post",
                    "intent_kind": "internal_reply",
                    "arguments": {"text": "pong", "channel_or_chat_id": CHAT},
                }
            )
        ),
        "enqueue_same": lambda: service.enqueue(stale_tests.execute_request()),
        "confirm_yes": lambda: service.confirm(_confirm(BLOCKED, "yes")),
        "confirm_no": lambda: service.confirm(_confirm(BLOCKED, "no")),
        "confirm_revise": lambda: service.confirm(_confirm(BLOCKED, "revise", {"text": "pong v2", "channel_or_chat_id": CHAT})),
        "confirm_revise_target": lambda: service.confirm(
            _confirm(BLOCKED, "revise", {"text": "pong", "channel_or_chat_id": "another-chat"})
        ),
        "confirm_revise_malformed": lambda: service.confirm(_confirm(BLOCKED, "revise", {"text": 7, "surprise": True})),
        "confirm_revise_without_arguments": lambda: service.confirm(_confirm(BLOCKED, "revise", None)),
        "confirm_other_wake": lambda: service.confirm(_confirm(BLOCKED, "yes", wake=WAKE + 1)),
    }
    await _attempt(requests[touch]())


async def _row_case_parity(case) -> None:
    state, detail, decision, due, touch = case
    # Confirmation on for most, off for a deterministic third: the pre-192
    # contract must be preserved byte for byte too.
    enabled = zlib.crc32(f"{state.value}/{detail}/{decision}/{touch}".encode()) % 3 != 0

    async def scenario(service_cls):
        store = stale_tests.LedgerStore()
        await _seed(store, state, detail, decision, due)
        service, _probe = _build(service_cls, store=store, activity=[stale_tests.CRON_ALERT], enabled=enabled)
        await _touch(service, touch)

    await assert_parity(scenario)


async def assert_all(cases, check, describe) -> None:
    """Run every case; report every divergent one, not just the first."""
    failures = []
    for case in cases:
        try:
            await check(case)
        except AssertionError as exc:
            failures.append(f"{describe(case)}: {exc}")
    assert not failures, f"{len(failures)} of {len(cases)} diverged:\n" + "\n".join(failures[:5])


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(ActionState), ids=[state.value for state in ActionState])
async def test_every_row_shape_answers_identically(state):
    cases = [case for case in ROW_CASES if case[0] is state]
    assert len(cases) == len(DETAILS) * len(DECISIONS) * 2 * len(TOUCHES)
    await assert_all(cases, _row_case_parity, _row_case_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("touch", TOUCHES)
async def test_every_touch_of_an_awaiting_row_is_identical_with_confirmation_on_and_off(touch, enabled):
    async def scenario(service_cls):
        store = stale_tests.LedgerStore()
        await _seed(store, ActionState.STALE, "stale_context", None, True)
        service, _probe = _build(service_cls, store=store, activity=[stale_tests.CRON_ALERT], enabled=enabled)
        await _touch(service, touch)

    await assert_parity(scenario)


# ----------------------------------------------------------------------------
# 3. Generated conversations
# ----------------------------------------------------------------------------

STEPS = (
    "execute",
    "execute_revised",
    "execute_override",
    "enqueue",
    "quo_reply",
    "confirm_yes",
    "confirm_no",
    "confirm_revise",
    "confirm_successor_yes",
    "confirm_successor_no",
    "confirm_other_wake",
    "resume_blocked",
    "resume_successor",
    "prepare_successor",
    "status_blocked",
    "new_activity",
    "flood_activity",
    "wake_terminal",
    "ledger_block_fails",
    "ledger_confirm_fails",
    "toggle_enabled",
)
CONVERSATIONS = 600


def _conversation(seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    return {
        "steps": [rng.choice(STEPS) for _ in range(rng.randint(2, 6))],
        "activity": rng.choice([0, 1, 1, 2, 12]),
        "evidence": rng.choice([(), (750824,), (750824, 751000), (760000,)]),
        "enabled": rng.random() < 0.8,
        "traffic_mode": rng.choice(["enforce", "enforce", "enforce", "shadow", "off"]),
        "with_probe": rng.random() < 0.9,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", range(CONVERSATIONS // 50))
async def test_generated_conversations_are_identical(batch):
    seeds = range(batch * 50, (batch + 1) * 50)
    await assert_all(seeds, _conversation_parity, lambda seed: f"seed {seed} {_conversation(seed)}")


async def _conversation_parity(seed: int) -> None:
    plan = _conversation(seed)

    async def scenario(service_cls):
        store = FlakyLedger()
        service, probe = _build(
            service_cls,
            store=store,
            activity=_activity(plan["activity"]),
            evidence=plan["evidence"],
            enabled=plan["enabled"],
            traffic_mode=plan["traffic_mode"],
            with_probe=plan["with_probe"],
        )
        probe.elsewhere.append(replace(stale_tests.QUO_TEXT, message_id=751000, source="zillow", preview="by email too"))
        reply_successor = stale_tests.SUCCESSOR_REPLY
        for step in plan["steps"]:
            if step == "execute":
                await _attempt(service.execute(stale_tests.execute_request()))
            elif step == "execute_revised":
                await _attempt(service.execute(stale_tests.execute_request("pong (revised)")))
            elif step == "execute_override":
                await _attempt(service.execute(stale_tests.execute_request(override=True)))
            elif step == "enqueue":
                await _attempt(service.enqueue(stale_tests.execute_request()))
            elif step == "quo_reply":
                await _attempt(service.execute(stale_tests.quo_reply_request()))
            elif step == "confirm_yes":
                await _attempt(service.confirm(_confirm(BLOCKED, "yes")))
            elif step == "confirm_no":
                await _attempt(service.confirm(_confirm(BLOCKED, "no")))
            elif step == "confirm_revise":
                await _attempt(service.confirm(_confirm(BLOCKED, "revise", {"text": "pong v2", "channel_or_chat_id": CHAT})))
            elif step == "confirm_successor_yes":
                await _attempt(service.confirm(_confirm(SUCCESSOR, "yes")))
                await _attempt(service.confirm(_confirm(reply_successor, "yes")))
            elif step == "confirm_successor_no":
                await _attempt(service.confirm(_confirm(reply_successor, "no")))
            elif step == "confirm_other_wake":
                await _attempt(service.confirm(_confirm(BLOCKED, "no", wake=WAKE + 1)))
            elif step == "resume_blocked":
                await _attempt(service.resume(BLOCKED))
            elif step == "resume_successor":
                await _attempt(service.resume(SUCCESSOR))
            elif step == "prepare_successor":
                await _attempt(service.prepare(SUCCESSOR))
            elif step == "status_blocked":
                await _attempt(service.status(BLOCKED))
            elif step == "new_activity":
                probe.activity.append(
                    replace(stale_tests.DAN_FOLLOW_UP, message_id=770000 + len(probe.activity), occurred_at=NOW)
                )
            elif step == "flood_activity":
                probe.activity.extend(_activity(12))
            elif step == "wake_terminal":
                store.wake_terminal = True
            elif step == "ledger_block_fails":
                store.fail_block = not store.fail_block
            elif step == "ledger_confirm_fails":
                store.fail_confirm = not store.fail_confirm
            elif step == "toggle_enabled":
                service._stale_confirm_enabled = not service._stale_confirm_enabled

    await assert_parity(scenario)


@pytest.mark.asyncio
async def test_the_parity_harness_sees_a_real_difference():
    """Guard against a harness that compares nothing: a service whose
    stale-context detail differs by one character must be caught."""
    from postgres_mcp.outbound_gateway import stale_context
    from postgres_mcp.outbound_gateway.service import OutboundActionService

    from . import parity

    async def scenario(service_cls):
        store = stale_tests.LedgerStore()
        await _seed(store, ActionState.STALE, "stale_context", None, True)
        service, _probe = _build(service_cls, store=store, activity=[stale_tests.CRON_ALERT])
        await _attempt(service.confirm(_confirm(BLOCKED, "no")))

    original = stale_context.DECLINED_DETAIL
    stale_context.DECLINED_DETAIL = original + "!"
    try:
        with pytest.raises(AssertionError, match="traces diverge"):
            await parity.assert_parity(scenario)
    finally:
        stale_context.DECLINED_DETAIL = original
    trace = await parity.run_side(OutboundActionService, scenario)
    assert ("call", "store.confirm_stale_context") in trace.kinds
