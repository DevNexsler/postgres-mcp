# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false
"""Parity: worker recovery in recovery.py vs the pre-split service.

Every scenario runs through LegacyOutboundActionService (the service as it was
before the split, with exhaust's hand-tracked lease flag) and
OutboundActionService (delegating to ActionRecovery and its transition
plans), and must produce the identical trace -- public results, store calls,
adapter calls, log records. See parity.py.

Layers:

1. every existing unit test of the service replayed through both sides;
2. generated rows over a compare-and-set ledger that behaves like the SQL
   (claim whitelist, allowed transitions, provider_request_ref coalesce):
   every action state x reconcile / exhaust / resume x provider ref /
   persisted acceptance / TenantCloud evidence x due / not due x lease held
   elsewhere x a ledger write failing at each step x derivable context x
   what the provider says when polled and when reconciled;
3. the plan vocabulary itself: plan_exhaust never transitions without the
   lease and claims exactly where the old code did.
"""

from __future__ import annotations

import itertools
import random
from dataclasses import replace
from datetime import timedelta

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.adapters.base import ProviderReceipt
from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import CompletionKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.recovery import Claim
from postgres_mcp.outbound_gateway.recovery import Transition
from postgres_mcp.outbound_gateway.recovery import plan_exhaust
from postgres_mcp.outbound_gateway.state_machine import ALLOWED_TRANSITIONS

from . import test_service as service_tests
from .parity import assert_parity
from .parity import existing_scenarios
from .parity import replay_existing

EXISTING = existing_scenarios(service_tests)


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "function", "case"), EXISTING, ids=[name for name, _f, _c in EXISTING])
async def test_existing_scenario_is_identical_through_both_sides(name, function, case, caplog):
    del name
    await replay_existing(service_tests, function, case, caplog)


# ----------------------------------------------------------------------------
# A ledger that behaves like the SQL functions
# ----------------------------------------------------------------------------

NOW = service_tests.NOW
ACTION_ID = service_tests.ACTION_ID
ACTION_UID = service_tests.ACTION_UID


class LedgerError(RuntimeError):
    pass


class CasLedger:
    """One outbound_actions row behind the SQL's guards: claim only from the
    068 whitelist, every write compare-and-set on the expected state, only
    allowed edges, provider_request_ref coalesced. `fail_at`: the n-th write
    raises (a crash / concurrent writer at that step); `lease_elsewhere`:
    every claim is refused (another worker holds the lease)."""

    CLAIMABLE = service_tests._CLAIMABLE_STATES

    def __init__(self, row, *, fail_at: int | None = None, lease_elsewhere: bool = False) -> None:
        self.row = row
        self.fail_at = fail_at
        self.lease_elsewhere = lease_elsewhere
        self.writes = 0

    def _write(self, expected_state) -> None:
        self.writes += 1
        if self.fail_at is not None and self.writes == self.fail_at:
            raise LedgerError(f"write {self.writes} failed")
        if self.row.state is not expected_state:
            raise LedgerError(f"outbound action state changed: expected {expected_state.value}, is {self.row.state.value}")

    async def get(self, action_id):
        return self.row if action_id == self.row.action_id else None

    async def claim(self, action_id, expected_state, lease_owner, lease_seconds):
        self._write(expected_state)
        if self.lease_elsewhere or expected_state not in self.CLAIMABLE:
            raise LedgerError("outbound action lease unavailable")
        self.row = replace(self.row, attempt_count=self.row.attempt_count + 1)
        return self.row

    async def transition(self, action_id, expected_state, next_state, lease_owner, observation):
        self._write(expected_state)
        if next_state not in ALLOWED_TRANSITIONS[expected_state]:
            raise LedgerError(f"invalid outbound transition {expected_state.value} -> {next_state.value}")
        self.row = replace(
            self.row,
            state=next_state,
            detail_code=observation.detail_code,
            provider_request_ref=observation.provider_request_ref or self.row.provider_request_ref,
        )
        return self.row

    async def definitive_fail(self, action_id, expected_state, lease_owner, observation):
        self._write(expected_state)
        if ActionState.DEFINITIVE_FAILED not in ALLOWED_TRANSITIONS[expected_state]:
            raise LedgerError("invalid outbound definitive failure state")
        self.row = replace(
            self.row, state=ActionState.DEFINITIVE_FAILED, detail_code=observation.detail_code, error_category=observation.category
        )
        return self.row

    async def complete(self, action_id, expected_state, lease_owner, receipt, completion_kind, detail_code):
        self._write(expected_state)
        if ActionState.COMPLETED not in ALLOWED_TRANSITIONS[expected_state]:
            raise LedgerError("invalid outbound completion state")
        self.row = replace(
            self.row,
            state=ActionState.COMPLETED,
            provider_request_ref=receipt.provider_request_ref,
            provider_message_id=receipt.provider_message_id,
            completion_kind=completion_kind,
            detail_code=detail_code,
        )
        return self.row

    async def record_provider_request(self, action_id, lease_owner, observation):
        self.row = replace(self.row, provider_request_ref=observation.provider_request_ref)
        return self.row

    async def schedule_next_attempt(self, action_id, expected_state, delay_seconds, detail_code):
        self._write(expected_state)
        self.row = replace(self.row, detail_code=detail_code, next_attempt_at=NOW + timedelta(seconds=delay_seconds))
        return self.row

    async def prepare(self, ctx, expected_state):
        self._write(expected_state)
        self.row = replace(self.row, state=ActionState.PREPARED, action_uid=ACTION_UID)
        return self.row

    async def create_or_load(self, ctx):
        return self.row

    async def remediate_traffic_block(self, action_id, *, operator_identity, reason):
        raise LedgerError("no remediation on file")

    async def block_stale_context(self, action_id, expected_state, lease_owner, shown_refs):
        raise LedgerError("not in recovery")

    async def confirm_stale_context(self, action_id, **kwargs):
        raise LedgerError("not in recovery")


def _observation(kind: str) -> ProviderObservation | None:
    return {
        "none": None,
        "pending": ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="req-1"),
        "accepted": ProviderObservation(
            ProviderDisposition.ACCEPTED,
            "provider_accepted",
            provider_request_ref="req-1",
            message_id="mail-1",
            accepted_at=NOW,
            evidence={"kind": "provider_message_id"},
        ),
        "accepted_without_receipt": ProviderObservation(ProviderDisposition.ACCEPTED, "provider_accepted", provider_request_ref="req-1"),
        "refused": ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE, "provider_refused", provider_request_ref="req-1", retryable=False
        ),
        "refused_retryable": ProviderObservation(
            ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE, "provider_busy", provider_request_ref="req-1", retryable=True
        ),
        "ambiguous": ProviderObservation(ProviderDisposition.AMBIGUOUS, "provider_timeout", provider_request_ref="req-1"),
    }[kind]


class ScriptedAdapter:
    """Answers poll (the "did job X finish?" pre-check and any dispatch
    poll), reconcile and invoke from a script; parse_receipt like a real
    adapter (None without a message id)."""

    def __init__(self, *, poll: str, reconcile: str, invoke: str) -> None:
        self._poll, self._reconcile, self._invoke = poll, reconcile, invoke

    def build_request(self, ctx, action_uid):
        return ("request", ctx.target.target_id, action_uid)

    async def invoke(self, client, provider_request):
        return _observation(self._invoke)

    async def poll(self, client, observation):
        answer = _observation(self._poll)
        return observation if answer is None else answer

    async def reconcile(self, client, ctx, action_uid, observation):
        answer = _observation(self._reconcile)
        if answer is None:
            raise TimeoutError("provider reconcile timed out")
        return answer

    def parse_receipt(self, ctx, observation):
        if observation.disposition is not ProviderDisposition.ACCEPTED or not observation.message_id:
            return None
        return ProviderReceipt(
            provider_request_ref=observation.provider_request_ref,
            provider_message_id=observation.message_id,
            accepted_at=observation.accepted_at,
            evidence=observation.evidence,
        )


class Loader:
    def __init__(self, context, *, derivable: bool) -> None:
        self._context, self._derivable = context, derivable

    async def load(self, request):
        if not self._derivable:
            raise ContextDerivationError("the wake's source message is gone")
        return self._context

    async def suggest_targets(self, wakeup_event_id):
        return {}


OPERATIONS = (Operation.EMAIL_SEND, Operation.TENANTCLOUD_LEAD_STATUS_UPDATE)
METHODS = ("reconcile", "exhaust", "resume")


def _row(state, operation, *, ref: bool, accepted: bool, evidence: str, uid: bool, due: bool):
    build = service_tests.row if operation is Operation.EMAIL_SEND else service_tests.tenantcloud_row
    row = build(
        state,
        action_uid=ACTION_UID if uid else None,
        provider_request_ref="req-1" if ref else None,
        provider_message_id="mail-0" if accepted else None,
        provider_accepted_at=NOW if accepted else None,
        next_attempt_at=NOW if due else NOW + timedelta(minutes=5),
        attempt_count=2,
        completion_kind=CompletionKind.SENT if state is ActionState.COMPLETED else None,
    )
    if operation is not Operation.EMAIL_SEND and evidence != "none":
        row = replace(
            row,
            provider_evidence_kind="verified_provider_readback",
            provider_evidence_hash="e" * 64 if evidence == "verified" else "not-a-hash",
            provider_readback_evidence=service_tests.VERIFIED_READBACK_EVIDENCE,
        )
    return row


CORE = list(itertools.product(list(ActionState), METHODS, OPERATIONS, (True, False)))
VARIANTS_PER_CORE = 16
OUTCOMES = ("none", "pending", "accepted", "accepted_without_receipt", "refused", "refused_retryable", "ambiguous")


def _variants(core) -> list[dict]:
    """Deterministic spread of the secondary dimensions for one core case."""
    rng = random.Random(repr(core))
    return [
        {
            "accepted": rng.random() < 0.5,
            "evidence": rng.choice(["none", "verified", "malformed"]),
            "uid": rng.random() < 0.85,
            "due": rng.random() < 0.85,
            "derivable": rng.random() < 0.8,
            "fail_at": rng.choice([None, None, None, 1, 2, 3, 4]),
            "lease_elsewhere": rng.random() < 0.1,
            "poll": rng.choice(OUTCOMES),
            "reconcile": rng.choice(OUTCOMES),
            "invoke": rng.choice(OUTCOMES[1:]),
        }
        for _ in range(VARIANTS_PER_CORE)
    ]


async def _recovery_parity(core, variant) -> None:
    state, method, operation, ref = core
    row = _row(
        state,
        operation,
        ref=ref,
        accepted=variant["accepted"],
        evidence=variant["evidence"],
        uid=variant["uid"],
        due=variant["due"],
    )
    context = service_tests.context() if operation is Operation.EMAIL_SEND else service_tests.tenantcloud_context()

    async def scenario(service_cls):
        store = CasLedger(row, fail_at=variant["fail_at"], lease_elsewhere=variant["lease_elsewhere"])
        adapter = ScriptedAdapter(poll=variant["poll"], reconcile=variant["reconcile"], invoke=variant["invoke"])
        evidence_loader = service_tests.AsyncMock()
        evidence_loader.load.return_value = service_tests.evidence()
        service = service_cls(
            store=store,
            context_loader=Loader(context, derivable=variant["derivable"]),
            evidence_loader=evidence_loader,
            adapters={operation: adapter},
            provider_client=object(),
            clock=lambda: NOW,
            lease_owner="gateway-test",
            response_budget_seconds=1,
            sleep=service_tests.AsyncMock(),
        )
        try:
            await getattr(service, method)(ACTION_ID)
        except Exception:  # noqa: BLE001 -- the raise is in the trace
            pass

    await assert_parity(scenario)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(ActionState), ids=[state.value for state in ActionState])
async def test_every_recovery_shape_is_identical(state):
    cases = [(core, variant) for core in CORE if core[0] is state for variant in _variants(core)]
    failures = []
    for core, variant in cases:
        try:
            await _recovery_parity(core, variant)
        except AssertionError as exc:
            failures.append(f"{core} {variant}: {exc}")
    assert not failures, f"{len(failures)} of {len(cases)} diverged:\n" + "\n".join(failures[:5])


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", [None, 1, 2, 3, 4, 5, 6])
@pytest.mark.parametrize(
    "state",
    [
        ActionState.DISPATCHING,
        ActionState.PROVIDER_ACCEPTED,
        ActionState.UNKNOWN,
        ActionState.RECONCILING,
        ActionState.DEPENDENCY_WAIT,
        ActionState.PREPARED,
        ActionState.RETRY_READY,
    ],
)
async def test_exhaust_ladder_fails_identically_at_every_step(state, fail_at):
    """The longest ladders, with the ledger refusing each write in turn."""
    await _recovery_parity(
        (state, "exhaust", Operation.EMAIL_SEND, True),
        {
            "accepted": False,
            "evidence": "none",
            "uid": True,
            "due": True,
            "derivable": True,
            "fail_at": fail_at,
            "lease_elsewhere": False,
            "poll": "none",
            "reconcile": "none",
            "invoke": "ambiguous",
        },
    )


# ----------------------------------------------------------------------------
# 3. The plan vocabulary
# ----------------------------------------------------------------------------


@pytest.mark.parametrize("state", list(ActionState), ids=[state.value for state in ActionState])
def test_exhaust_plan_only_transitions_under_a_lease(state):
    """Every leased transition follows a claim taken since the last unleased
    edge -- the invariant the old hand-tracked lease_held flag stood for."""
    plan = plan_exhaust(service_tests.row(state, provider_request_ref="req-1"))
    held = False
    for step in plan:
        if isinstance(step, Claim):
            held = True
        elif isinstance(step, Transition) and not step.leased:
            held = False
        else:
            assert held, f"{state.value}: {step} without a lease"
    if state in {ActionState.RECEIVED, ActionState.COMPLETED, ActionState.STALE, ActionState.REJECTED}:
        assert plan == ()
