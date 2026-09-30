"""Traffic-control mode for the stale-context question.

off: no probe calls; shadow: log what would be asked, never stop a send;
enforce: ask the stale-context question (stale_context.py).

There is no per-recipient in-flight hold. It made every later action for a
person -- a calendar update, a staff Cliq post -- wait behind one uncertain
send for up to an hour (action b78d5668, 2026-09-30). A send whose outcome is
uncertain is settled by its own reconcile against the provider, and a newer
message or send the agent has not seen is the stale-context question."""

from __future__ import annotations

VALID_TRAFFIC_MODES = frozenset({"off", "shadow", "enforce"})
