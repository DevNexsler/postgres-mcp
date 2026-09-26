"""The in-flight lease: the only thing left of traffic control. Newer
messages are the stale-context question (test_stale_context_confirm.py)."""

from __future__ import annotations

import logging
from datetime import datetime
from datetime import timezone
from uuid import uuid4

import pytest

from postgres_mcp.outbound_gateway.traffic_control import InFlightAction
from postgres_mcp.outbound_gateway.traffic_control import in_flight_hold

NOW = datetime(2026, 8, 27, 12, 5, tzinfo=timezone.utc)
LOGGER = logging.getLogger("test.traffic")
RECIPIENT = "prospect:email:melody@example.com"


class FakeProbe:
    def __init__(self, *, in_flight=(), raises=False):
        self._in_flight = list(in_flight)
        self._raises = raises
        self.excluded = []

    async def in_flight_actions(self, recipient_key, exclude_action_id):
        self.excluded.append(exclude_action_id)
        if self._raises:
            raise RuntimeError("db down")
        return self._in_flight


@pytest.mark.asyncio
async def test_no_hold_when_nothing_else_is_in_flight():
    assert await in_flight_hold(FakeProbe(), recipient_key=RECIPIENT, action_id=uuid4(), logger=LOGGER) is None


@pytest.mark.asyncio
async def test_another_send_in_flight_holds_with_what_it_is():
    other = InFlightAction(uuid4(), "email.send", "dispatching", NOW, "Hi Melody...")
    detail = await in_flight_hold(FakeProbe(in_flight=[other]), recipient_key=RECIPIENT, action_id=uuid4(), logger=LOGGER)
    assert detail is not None
    assert "email.send" in detail and "Hi Melody" in detail and str(other.action_id) in detail
    assert "override" not in detail.casefold()


@pytest.mark.asyncio
async def test_the_action_itself_is_excluded_so_a_retry_never_holds_itself():
    action_id = uuid4()
    probe = FakeProbe()
    await in_flight_hold(probe, recipient_key=RECIPIENT, action_id=action_id, logger=LOGGER)
    assert probe.excluded == [action_id]


@pytest.mark.asyncio
async def test_a_broken_probe_fails_open(caplog):
    with caplog.at_level(logging.WARNING):
        detail = await in_flight_hold(FakeProbe(raises=True), recipient_key=RECIPIENT, action_id=uuid4(), logger=LOGGER)
    assert detail is None
    assert any("traffic control check failed" in r.message for r in caplog.records)
