"""Per-recipient in-flight lease: one send to a recipient at a time.

Not a judgment about the message: while another wake's send to the same
recipient is still in flight, this one waits (the row stays re-drivable and
the worker re-runs the check). Newer messages are the stale-context question
(stale_context.py), never a block here.

Fail-open on probe malfunction: a broken check must never stop outbound
traffic (the kill switch is the only fail-closed control)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True)
class InFlightAction:
    action_id: UUID
    operation: str
    state: str
    created_at: datetime
    preview: str


class InFlightProbe(Protocol):
    async def in_flight_actions(self, recipient_key: str, exclude_action_id: UUID) -> list[InFlightAction]: ...


# off: no probe calls; shadow: log what would be held or asked, never stop a
# send; enforce: hold for an in-flight send and ask the stale-context question.
VALID_TRAFFIC_MODES = frozenset({"off", "shadow", "enforce"})


async def in_flight_hold(
    probe: InFlightProbe,
    *,
    recipient_key: str,
    action_id: UUID,
    logger: logging.Logger,
) -> str | None:
    """The agent-facing detail of a hold, or None when nothing else is in
    flight to this recipient (or the probe failed: fail-open)."""
    try:
        in_flight = await probe.in_flight_actions(recipient_key, action_id)
    except Exception:
        logger.error("traffic control check failed (in_flight) for %s action %s", recipient_key, action_id, exc_info=True)
        return None
    if not in_flight:
        return None
    other = in_flight[0]
    return (
        f"Another send to this recipient is in flight: action {other.action_id} "
        f"({other.operation}, state {other.state}, started {other.created_at.isoformat()}): "
        f'"{other.preview}". Wait for it to reach a terminal state, then re-check '
        f"the thread before sending."
    )
