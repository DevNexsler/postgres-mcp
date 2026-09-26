"""Replay the stale-context decision for real actions; which outcomes change.

Read-only. For every action of a role in the window, rebuild the context the
gateway judged with (the saved record, as the worker does), run the ONE
stale-context query as the world stood when the agent executed it
(repository.newer_context(as_of=created_at)), waive what this wake's agent
had already been shown in an earlier question, and compare what the gateway
would do now with what it did then:

  asked      -- unshown newer context: the agent gets needs_confirmation
  proceeds   -- nothing unshown: the send gate goes on (calendar, dispatch)

Run inside the outbound worker container with the tree under test:
  docker exec -i -e PYTHONPATH=/tmp/newsrc comm-data-store-outbound-worker \\
      python - --days 120 < scripts/replay_stale_decisions.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter

from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.server import build_runtime
from postgres_mcp.sql import SafeSqlDriver
from postgres_mcp.sql import SqlDriver


def old_outcome(row: dict) -> str:
    state, detail, category = row["state"], row["detail_code"], row["error_category"]
    if category == "traffic_blocked":
        return "definitive_failed/traffic_blocked"
    if state == "stale" and detail == "newer_inbound":
        return "stale/newer_inbound"
    if state == "stale" and (detail or "").startswith("stale_context"):
        return "stale_context question"
    if state == "completed" and detail in {"already_handled", "duplicate_inquiry_already_handled"}:
        return "completed/already_handled"
    if state == "completed":
        return "completed/sent"
    return f"{state}/{detail}"


async def record_context(driver, action) -> ActionContext:
    """The judged context from the saved record alone, for an action whose
    request no longer validates under today's models (the worker would not
    rebuild it either). thread_identity is not recorded: the Quo
    cross-channel relation is approximate for these."""
    recorded = dict(action.canonical_context)
    scope = dict(action.recipient_scope)
    source_id = int(recorded["source_message_id"])
    rows = await SafeSqlDriver.execute_param_query(driver, "SELECT source, sent_at FROM messages WHERE id = {}", [source_id])
    source = str(rows[0].cells["source"]) if rows else str(recorded.get("source_message_key", ":")).split(":")[0]
    return ActionContext(
        action_id=action.action_id,
        wakeup_event_id=action.wakeup_event_id,
        action_role=action.action_role,
        operation=action.operation,
        intent_kind=str(action.intent_kind),
        appointment_slot=action.appointment_slot,
        arguments=dict(action.arguments),
        # The loader's provider family: a Zillow relay target is Zillow's,
        # whatever message the wake is on.
        source=(
            "zillow"
            if str(scope.get("target_id", "")).casefold().endswith("@convo.zillow.com") or "zillow" in source
            else ("quo" if source in {"quo", "openphone"} else source)
        ),
        source_message_id=source_id,
        source_message_key=str(recorded.get("source_message_key") or ""),
        source_sent_at=rows[0].cells["sent_at"],
        conversation_id=str(recorded.get("conversation_id") or ""),
        conversation_watermark=int(recorded.get("conversation_watermark") or 0),
        prospect_id=str(recorded.get("prospect_id") or ""),
        aliases=(),
        property_id=recorded.get("property_id"),
        property_label=None,
        target=DerivedTarget(str(scope.get("kind")), str(scope.get("target_id")), bool(scope.get("verified"))),
        provider_account=action.provider_account,
        routing_policy_version=action.routing_policy_version,
        canonical_scope=dict(action.canonical_scope),
        canonical_context=recorded,
        payload_hash=action.payload_hash,
        lock_holder=f"outbound-gateway:{action.action_id}",
        thread_identity="",
        showing_lifecycle_id="",
        calendar_event_uid=None,
        recipient_phone=recorded.get("recipient_phone"),
        channel_id=int(recorded.get("channel_id") or 0),
        cross_channel_duplicate_message_ids=tuple(recorded.get("cross_channel_duplicate_message_ids") or ()),
        certified_older_message_ids=tuple(recorded.get("certified_older_message_ids") or ()),
    )


async def replay(days: int, role: str) -> None:
    runtime = await build_runtime()
    service = runtime.service
    driver = SqlDriver(conn=runtime.pool)
    repository = service._stale._probe
    rows = await SafeSqlDriver.execute_param_query(
        driver,
        "SELECT action_id, wakeup_event_id, subject_key, state, detail_code, error_category, created_at "
        "FROM outbound_actions WHERE created_at > now() - make_interval(days => {}) AND action_role = {} "
        "ORDER BY created_at",
        [days, role],
    )
    shown_rows = await SafeSqlDriver.execute_param_query(
        driver,
        "SELECT wakeup_event_id, subject_key, created_at, stale_context_shown_refs FROM outbound_actions "
        "WHERE stale_context_shown_refs IS NOT NULL AND created_at > now() - make_interval(days => {}) - interval '1 day'",
        [days],
    )
    shown_by = [row.cells for row in shown_rows or []]
    tally: Counter = Counter()
    kinds: Counter = Counter()
    for row in rows or []:
        cells = row.cells
        before = old_outcome(cells)
        action = await runtime.store.get(cells["action_id"])
        try:
            try:
                context, detail = await service._verified_context(action)
                if context is None:
                    raise RuntimeError(detail)
            except Exception:  # noqa: BLE001 -- the request no longer validates: judge the record itself
                context = await record_context(driver, action)
                tally[("(context from record only)", before)] += 1
            found = await repository.newer_context(context, limit=50, waive_shown=False, as_of=cells["created_at"])
        except Exception as error:  # noqa: BLE001 -- counted, never fatal
            tally[(before, f"unavailable ({type(error).__name__})")] += 1
            continue
        shown = {
            ref
            for other in shown_by
            if other["wakeup_event_id"] == cells["wakeup_event_id"]
            and other["subject_key"] == cells["subject_key"]
            and other["created_at"] < cells["created_at"]
            for ref in other["stale_context_shown_refs"]
        }
        unshown = [item for item in found if item.ref not in shown]
        after = "asked" if unshown else "proceeds"
        tally[(before, after)] += 1
        for item in unshown:
            kinds[(before, item.label, item.arm)] += 1
        print(
            json.dumps(
                {
                    "action_id": str(cells["action_id"]),
                    "wake": cells["wakeup_event_id"],
                    "before": before,
                    "after": after,
                    "items": [f"{item.label}:{item.ref}:{item.source}" for item in unshown][:5],
                }
            )
        )
    print("SUMMARY before -> after : count")
    for (before, after), count in sorted(tally.items()):
        print(f"  {before} -> {after}: {count}")
    print("ITEMS (before, label, arm) : count")
    for key, count in sorted(kinds.items()):
        print(f"  {key}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=120)
    parser.add_argument("--role", default="prospect_reply")
    args = parser.parse_args()
    asyncio.run(replay(args.days, args.role))


if __name__ == "__main__":
    main()
