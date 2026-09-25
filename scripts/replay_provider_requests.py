"""Replay the provider request the worker would build for real actions.

Read-only. For every outbound action in the window, derive the context the
worker would use and ask the operation's adapter for its provider request
(server, tool, arguments), using the action's stored action_uid so the result
is deterministic. Record them as a baseline, or compare against one.

The regression guard for how the worker obtains an action's context: the
provider request must not change for any real action, except where a
difference is explained (the live data drifted since the send).

Run inside the outbound worker container, which has the runtime wiring:
  docker exec -i comm-data-store-outbound-worker python - --record /tmp/base.json < scripts/replay_provider_requests.py
  docker exec -i comm-data-store-outbound-worker python - --compare /tmp/base.json < scripts/replay_provider_requests.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from postgres_mcp.outbound_gateway.server import build_runtime
from postgres_mcp.sql import SafeSqlDriver
from postgres_mcp.sql import SqlDriver


async def _worker_context(service, action):
    """The context the worker executes an existing action with."""
    context, detail = await service._verified_context(action)
    if context is None:
        raise RuntimeError(f"worker would park this action: {detail}")
    return context


async def replay(days: int) -> dict[str, dict]:
    runtime = await build_runtime()
    service = runtime.service
    rows = await SafeSqlDriver.execute_param_query(
        SqlDriver(conn=runtime.pool),
        "SELECT action_id FROM outbound_actions "
        "WHERE created_at > now() - make_interval(days => {}) AND action_uid IS NOT NULL "
        "ORDER BY created_at",
        [days],
    )
    results: dict[str, dict] = {}
    for row in rows or []:
        action = await runtime.store.get(row.cells["action_id"])
        key = str(action.action_id)
        try:
            context = await _worker_context(service, action)
            request = service._adapter(context.operation).build_request(context, action.action_uid)
            results[key] = {
                "operation": action.operation.value,
                "state": action.state.value,
                "request": {"server": request.server_name, "tool": request.tool, "arguments": request.arguments},
            }
        except Exception as error:  # noqa: BLE001 -- recorded, compared like any outcome
            results[key] = {
                "operation": action.operation.value,
                "state": action.state.value,
                "error": f"{type(error).__name__}: {error}"[:300],
            }
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--record")
    mode.add_argument("--compare")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    results = asyncio.run(replay(args.days))
    if args.record:
        with open(args.record, "w") as handle:
            json.dump(results, handle, indent=1, sort_keys=True, default=str)
        errors = sum(1 for value in results.values() if "error" in value)
        print(f"recorded {len(results)} actions ({errors} could not build a request) -> {args.record}")
        return
    with open(args.compare) as handle:
        baseline = json.load(handle)
    changed = []
    for key, before in baseline.items():
        after = json.loads(json.dumps(results.get(key), default=str))
        if after is None:
            continue
        if before.get("request") != after.get("request") or ("error" in before) != ("error" in after):
            changed.append((key, before, after))
    print(f"compared {len(baseline)} actions: {len(changed)} changed")
    for key, before, after in changed:
        print(json.dumps({"action_id": key, "before": before, "after": after}, default=str)[:1200])
    sys.exit(1 if changed else 0)


if __name__ == "__main__":
    main()
