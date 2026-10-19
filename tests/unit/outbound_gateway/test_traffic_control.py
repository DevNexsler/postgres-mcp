"""Traffic control is only the stale-context question's mode now; the
per-recipient in-flight hold is gone (action b78d5668, 2026-09-30)."""

from __future__ import annotations

import postgres_mcp.outbound_gateway.traffic_control as traffic_control
from postgres_mcp.outbound_gateway.repository import OutboundGatewayRepository


def test_modes_are_unchanged():
    assert traffic_control.VALID_TRAFFIC_MODES == frozenset({"off", "shadow", "enforce"})


def test_no_in_flight_hold_remains():
    assert not hasattr(traffic_control, "in_flight_hold")
    assert not hasattr(traffic_control, "InFlightAction")
    assert not hasattr(OutboundGatewayRepository, "in_flight_actions")
