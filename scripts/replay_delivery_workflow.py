"""Backtest: does the NEW delivery-workflow decision logic (retry_policy.py's
elapsed-ceiling retry/backoff rules + idempotency_policy.py's per-operation
reinvoke safety + PR #59's definitive-rejection signal) reach the same final
outcome real production actions already reached?

Read-only. Reads the last 7 days of ``outbound_actions`` +
``outbound_action_attempts`` from prod (plus three named categories the
change explicitly asked to cover, even if older than 7 days -- see
``_SELECT_SQL``), rebuilds each action's sequence of recorded provider
observations from its attempts, and replays retry_policy.decide_for_observation()
across that sequence with a FAKE provider (the recorded observations
themselves -- no live adapter, no live MCP call, nothing sent anywhere).
No docker build, no Restate, no writes: this only runs SELECTs (via
``psql`` in the postgres container) and pure-Python decision functions
already in this repo.

What "same final outcome" means here, and what counts as a difference:

  SENT              -- any attempt observed ACCEPTED (unconditional; retry
                       policy never overrides an acceptance)
  STALE             -- the action's own final state is `stale` (the
                       freshness preflight's call, orthogonal to retry
                       policy -- always "same" by construction, since
                       nothing about this change touches it)
  DEFINITIVE_FAILED -- the sequence reaches a definitive provider rejection
                       (retry_policy.is_definitive_rejection), or elapsed
                       time from created_at crosses RETRY_CEILING_SECONDS
                       (one hour) with no acceptance yet
  STILL_RETRYING    -- the recorded sequence ends (no more attempts) while
                       still inside the one-hour ceiling and never accepted
                       or definitively rejected

The real, EXPECTED difference this backtest exists to surface: the OLD
system parked an action at `manual_review`/`dead_letter` once attempt_count
hit its 5 (or 12, ambiguous) cap, even when elapsed time was still well
inside one hour. The NEW system (this branch's whole point) does not -- see
delivery_workflow.py's advance(). Any action whose OLD outcome is
manual_review/dead_letter, but whose full attempt history never crossed the
one-hour ceiling, is EXPECTED to disagree: classified as
``EXPECTED_ATTEMPT_CAP_VS_CEILING`` rather than an unexplained difference.

Bookkeeping transitions (RECEIVED -> PREPARED, an auth wait, ...) carry no
provider_observation at all (``{}``) and are skipped -- retry_policy.decide()
only applies to an actual provider outcome, never a ledger bookkeeping step.

Usage (read-only; run wherever docker can reach the prod postgres container):
  python3 scripts/replay_delivery_workflow.py --days 7 --out /tmp/restate-backtest.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from collections import defaultdict
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

_REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_REPO_SRC) not in sys.path:
    sys.path.insert(0, str(_REPO_SRC))

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition  # noqa: E402
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation  # noqa: E402
from postgres_mcp.outbound_gateway.idempotency_policy import reinvoke_safety  # noqa: E402
from postgres_mcp.outbound_gateway.models import Operation  # noqa: E402
from postgres_mcp.outbound_gateway.retry_policy import RETRY_CEILING_SECONDS  # noqa: E402
from postgres_mcp.outbound_gateway.retry_policy import RetryOutcome  # noqa: E402
from postgres_mcp.outbound_gateway.retry_policy import decide_for_observation  # noqa: E402

DEFAULT_CONTAINER = "comm-data-store-postgres-1"

NAMED_ACTION_ID = "497fcaf8-cc37-5893-b8ba-34d05205713e"

_SELECT_SQL = """
with selected_actions as (
    select action_id from outbound_actions where created_at > now() - make_interval(days => %(days)s)
    union
    select action_id from outbound_actions where action_id = %(named_action)s
    union
    -- Cliq numeric/CT-id posts: the two target shapes cliq_target.py
    -- classifies (a numeric chat/channel id, and a CT_<lead>_<contact>
    -- conversation id) -- included even outside the days window so the
    -- backtest always exercises both.
    (
        select action_id from outbound_actions
        where operation like 'cliq.%%'
          and (
            arguments->>'channel_or_chat_id' ~ '^[0-9]+$'
            or arguments->>'channel_or_chat_id' like 'CT\\_%%' escape '\\'
          )
        order by action_id
        limit 200
    )
    union
    -- TenantCloud auth-before-dispatch rows: the auth gate rejected the
    -- action before any write was attempted.
    (
        select distinct o.action_id from outbound_actions o
        join outbound_action_attempts a using (action_id)
        where o.operation like 'tenantcloud.%%'
          and a.detail_code in ('tenantcloud_auth_rejected_before_dispatch', 'tenantcloud_write_ambiguous_authentication_rejected')
        limit 200
    )
)
select
    o.action_id::text as action_id,
    o.operation,
    o.state,
    o.detail_code,
    o.attempt_count,
    o.created_at,
    coalesce(
        jsonb_agg(
            jsonb_build_object(
                'attempt_number', a.attempt_number,
                'to_state', a.to_state,
                'detail_code', a.detail_code,
                'created_at', a.created_at,
                'provider_observation', a.provider_observation
            )
            order by a.attempt_id
        ) filter (where a.attempt_id is not null),
        '[]'::jsonb
    ) as attempts
from selected_actions s
join outbound_actions o using (action_id)
left join outbound_action_attempts a using (action_id)
group by o.action_id, o.operation, o.state, o.detail_code, o.attempt_count, o.created_at
order by o.created_at;
"""


def _fetch(container: str, days: int) -> list[dict[str, Any]]:
    sql = _SELECT_SQL % {
        "days": str(int(days)),
        "named_action": f"'{NAMED_ACTION_ID}'",
    }
    wrapped = f"select jsonb_agg(row_to_json(t)) from ({sql.strip().rstrip(';')}) t;"
    proc = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container,
            "sh",
            "-c",
            'psql -X -q -t -A -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
        ],
        input=wrapped,
        capture_output=True,
        text=True,
        check=True,
    )
    raw = proc.stdout.strip()
    if not raw or raw == "":
        return []
    return json.loads(raw) or []


def _parse_disposition(raw: str | None) -> ProviderDisposition | None:
    if not raw:
        return None
    try:
        return ProviderDisposition(raw)
    except ValueError:
        return None


# TenantCloud auth-gate rejections: recorded with disposition
# definitive_non_acceptance (so the ledger shows why nothing was attempted),
# but the live coordinator never runs these through
# retry_policy.decide_for_observation() at all -- OutboundDeliveryCoordinator
# checks AuthGate.ensure_ready() BEFORE calling the service/adapter, on its
# own retry schedule (see advance()'s auth branch), entirely separate from a
# provider's own accept/reject of the write. Real data confirms this: an
# action carrying one of these mid-sequence, with disposition
# definitive_non_acceptance, still goes on to complete (e.g. action
# cf12c8d1, 2026-09-2x) -- feeding it to is_definitive_rejection() would
# have wrongly stopped the replay here.
_AUTH_GATE_DETAIL_CODES = frozenset(
    {
        "tenantcloud_auth_rejected_before_dispatch",
        "tenantcloud_write_ambiguous_authentication_rejected",
        # Persisted with disposition definitive_non_acceptance, but every
        # occurrence in production (6/6, checked over the full history, not
        # just this backtest's window) goes on to complete -- the facade's
        # "not yet visible in a readback, worth one more look" case
        # retry_policy.py's own docstring anticipates ("an adapter marking a
        # DEFINITIVE_NON_ACCEPTANCE observation retryable ... would mean
        # 'definitive, but worth one more look'"), just not persisted with an
        # explicit retryable=true key. Treated the same as the auth-gate
        # codes: not a genuine stop signal, so excluded from
        # is_definitive_rejection() here rather than misreported as one.
        "tenantcloud_lead_status_not_yet_applied",
    }
)


def _observation_from_attempt(attempt: dict[str, Any]) -> ProviderObservation | None:
    payload = attempt.get("provider_observation") or {}
    detail_code = str(payload.get("detail_code") or attempt.get("detail_code") or "")
    if detail_code in _AUTH_GATE_DETAIL_CODES:
        return None
    disposition = _parse_disposition(payload.get("disposition"))
    if disposition is None:
        # Bookkeeping transition (no provider outcome was observed): retry
        # policy has nothing to decide here.
        return None
    return ProviderObservation(
        disposition=disposition,
        detail_code=detail_code,
        retryable=bool(payload.get("retryable", False)),
    )


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def replay_action(row: dict[str, Any]) -> dict[str, Any]:
    created_at = _parse_ts(row["created_at"])
    operation = row["operation"]
    old_state = row["state"]

    if old_state == "stale":
        return {
            "action_id": row["action_id"],
            "operation": operation,
            "old_outcome": "stale",
            "new_outcome": "stale",
            "same": True,
            "reason": "stale is the freshness preflight's call, not retry policy's",
        }

    all_attempts = row["attempts"]
    attempts = [a for a in all_attempts if _observation_from_attempt(a) is not None]
    new_outcome = "STILL_RETRYING"
    new_reason = "no attempt ever accepted, definitively rejected, or crossed the ceiling"
    warn_staff = False
    for attempt in all_attempts:
        if attempt["to_state"] == "completed":
            # A completing attempt's provider_observation carries receipt
            # evidence (receipt_keys), not a `disposition` field -- see
            # record.py/store.py's complete() -- so this is checked on
            # to_state directly rather than through _observation_from_attempt.
            new_outcome = "SENT"
            new_reason = f"attempt {attempt['attempt_number']} transitioned to completed (receipt-verified)"
            break
        observation = _observation_from_attempt(attempt)
        if observation is None:
            continue
        elapsed = (_parse_ts(attempt["created_at"]) - created_at).total_seconds()
        decision = decide_for_observation(
            attempt_count=int(attempt["attempt_number"]) or 1,
            elapsed_seconds=elapsed,
            observation=observation,
            ceiling_seconds=RETRY_CEILING_SECONDS,
        )
        if decision.outcome is RetryOutcome.STOP_DEFINITIVE:
            new_outcome = "DEFINITIVE_FAILED"
            warn_staff = decision.warn_staff
            is_rejection = observation.disposition is ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE
            stop_cause = "definitive provider rejection" if is_rejection else "past the one-hour ceiling"
            new_reason = f"attempt {attempt['attempt_number']} at {elapsed:.0f}s: {stop_cause}"
            break

    old_outcome = {
        "completed": "SENT",
        "rejected": "DEFINITIVE_FAILED",
        "definitive_failed": "DEFINITIVE_FAILED",
        "manual_review": "DEFINITIVE_FAILED",
        "dead_letter": "DEFINITIVE_FAILED",
    }.get(old_state, "STILL_RETRYING")

    same = old_outcome == new_outcome
    reason = "matches" if same else new_reason
    category = "same"
    if not same:
        last_attempt_elapsed = (
            (_parse_ts(attempts[-1]["created_at"]) - created_at).total_seconds() if attempts else 0.0
        )
        if old_outcome == "DEFINITIVE_FAILED" and new_outcome == "STILL_RETRYING" and last_attempt_elapsed < RETRY_CEILING_SECONDS:
            category = "EXPECTED_ATTEMPT_CAP_VS_CEILING"
            reason = (
                f"old system parked this at {old_state} via the legacy attempt-count cap; "
                f"the full recorded history only reached {last_attempt_elapsed:.0f}s (< {RETRY_CEILING_SECONDS}s "
                "ceiling), so the new elapsed-time policy would still be retrying -- exactly this "
                "branch's intended change (delivery_workflow.py's advance())"
            )
        else:
            category = "UNEXPLAINED"

    try:
        op_enum = Operation(operation)
        safety = reinvoke_safety(op_enum).value
    except ValueError:
        safety = "unclassified"

    return {
        "action_id": row["action_id"],
        "operation": operation,
        "old_outcome": old_outcome,
        "old_state": old_state,
        "new_outcome": new_outcome,
        "same": same,
        "category": category,
        "reason": reason,
        "warn_staff": warn_staff,
        "reinvoke_safety": safety,
        "attempts_replayed": len(attempts),
    }


def run(days: int, container: str) -> dict[str, Any]:
    rows = _fetch(container, days)
    results = [replay_action(row) for row in rows]

    per_operation: dict[str, Counter[str]] = defaultdict(Counter)
    differences: list[dict[str, Any]] = []
    for result in results:
        per_operation[result["operation"]]["same" if result["same"] else "different"] += 1
        if not result["same"]:
            differences.append(result)

    named = next((r for r in results if r["action_id"] == NAMED_ACTION_ID), None)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": days,
        "total_actions": len(results),
        "per_operation": {op: dict(counts) for op, counts in per_operation.items()},
        "differences_by_category": dict(Counter(d["category"] for d in differences)),
        "differences": differences,
        "named_action_497fcaf8": named,
        "unexplained_count": sum(1 for d in differences if d["category"] == "UNEXPLAINED"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--out", default="/tmp/restate-backtest.json")
    args = parser.parse_args()

    report = run(args.days, args.container)
    with open(args.out, "w") as handle:
        json.dump(report, handle, indent=2, sort_keys=True, default=str)

    print(f"replayed {report['total_actions']} actions over {args.days}d -> {args.out}")
    for operation, counts in sorted(report["per_operation"].items()):
        print(f"  {operation}: same={counts.get('same', 0)} different={counts.get('different', 0)}")
    print(f"differences by category: {report['differences_by_category']}")
    print(f"UNEXPLAINED differences: {report['unexplained_count']}")
    sys.exit(1 if report["unexplained_count"] else 0)


if __name__ == "__main__":
    main()
