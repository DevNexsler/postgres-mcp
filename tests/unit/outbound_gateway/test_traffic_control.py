from __future__ import annotations

import logging
from datetime import datetime
from datetime import timezone
from uuid import uuid4

import pytest

from postgres_mcp.outbound_gateway.traffic_control import CONTEXT_ITEM_LIMIT
from postgres_mcp.outbound_gateway.traffic_control import InFlightAction
from postgres_mcp.outbound_gateway.traffic_control import NewerActivity
from postgres_mcp.outbound_gateway.traffic_control import TrafficVerdict
from postgres_mcp.outbound_gateway.traffic_control import check_traffic
from postgres_mcp.outbound_gateway.traffic_control import list_stale_context
from postgres_mcp.outbound_gateway.traffic_control import stale_context_question

NOW = datetime(2026, 8, 27, 12, 5, tzinfo=timezone.utc)
WATERMARK = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
LOGGER = logging.getLogger("test.traffic")


class FakeProbe:
    """Filters by watermark and by excluded refs, like the SQL."""

    def __init__(self, *, in_flight=(), newer=None, activity=(), watermark=WATERMARK, shown=(), raises=None):
        self._in_flight = list(in_flight)
        self._activity = list(activity) + ([newer] if newer is not None else [])
        self._watermark = watermark
        self._shown = frozenset(shown)
        self._raises = raises
        self.asked = []

    async def in_flight_actions(self, recipient_key, exclude_action_id):
        if self._raises == "in_flight":
            raise RuntimeError("db down")
        return self._in_flight

    async def activity_after(self, recipient_key, channel_id, watermark, exclude_action_id, limit, exclude_refs=frozenset()):
        if self._raises == "staleness":
            raise RuntimeError("db down")
        self.asked.append((watermark, frozenset(exclude_refs)))
        found = [item for item in self._activity if item.occurred_at > watermark and item.ref not in exclude_refs]
        return sorted(found, key=lambda item: item.occurred_at, reverse=True)[:limit]

    async def context_watermark(self, wakeup_event_id):
        if self._raises == "watermark":
            raise RuntimeError("db down")
        return self._watermark

    async def acknowledged_refs(self, wakeup_event_id, recipient_key):
        if self._raises == "acknowledged":
            raise RuntimeError("db down")
        return self._shown

    async def messages_by_id(self, message_ids):
        return [item for item in self._activity if item.message_id in set(message_ids)]


def _kwargs(**over):
    base = dict(
        recipient_key="prospect:email:melody@example.com",
        channel_id=676079,
        wakeup_event_id=25789,
        action_id=uuid4(),
        override=False,
        logger=LOGGER,
    )
    base.update(over)
    return base


@pytest.mark.asyncio
async def test_pass_when_quiet():
    verdict = await check_traffic(FakeProbe(), **_kwargs())
    assert verdict == TrafficVerdict(allowed=True, reason="pass", detail="", check_failed=False)


@pytest.mark.asyncio
async def test_lease_held_blocks():
    other = InFlightAction(uuid4(), "email.send", "dispatching", NOW, "Hi Melody...")
    verdict = await check_traffic(FakeProbe(in_flight=[other]), **_kwargs())
    assert not verdict.allowed
    assert verdict.reason == "lease_held"
    assert "email.send" in verdict.detail and "Hi Melody" in verdict.detail


@pytest.mark.asyncio
async def test_own_action_is_excluded_by_probe_contract():
    # exclude_action_id is forwarded so a retry of the same action never self-blocks
    action_id = uuid4()
    probe = FakeProbe()
    seen = {}
    orig = probe.in_flight_actions

    async def spy(recipient_key, exclude_action_id):
        seen["exclude"] = exclude_action_id
        return await orig(recipient_key, exclude_action_id)

    probe.in_flight_actions = spy
    await check_traffic(probe, **_kwargs(action_id=action_id))
    assert seen["exclude"] == action_id


@pytest.mark.asyncio
async def test_stale_context_blocks_with_detail():
    newer = NewerActivity("inbound", "zoho_mail", NOW, "Melody withdrew their application", 670826, None)
    verdict = await check_traffic(FakeProbe(newer=newer), **_kwargs())
    assert not verdict.allowed
    assert verdict.reason == "stale_context"
    assert "withdrew" in verdict.detail
    assert "needs_human" in verdict.detail
    assert "stale_context, reply still needed" in verdict.detail
    # The preview is the only customer text. The instruction must not hand the
    # agent the operator-only override switch (wake 27138).
    assert "override" not in verdict.detail.casefold()


@pytest.mark.asyncio
async def test_override_skips_staleness_not_lease():
    newer = NewerActivity("outbound", "quo", NOW, "already replied", None, uuid4())
    assert (await check_traffic(FakeProbe(newer=newer), **_kwargs(override=True))).allowed
    other = InFlightAction(uuid4(), "quo.sms.send", "dispatching", NOW, "sending...")
    verdict = await check_traffic(FakeProbe(in_flight=[other], newer=newer), **_kwargs(override=True))
    assert not verdict.allowed and verdict.reason == "lease_held"


@pytest.mark.asyncio
async def test_missing_watermark_fails_open():
    verdict = await check_traffic(FakeProbe(watermark=None), **_kwargs())
    assert verdict.allowed and verdict.check_failed and verdict.reason == "gate_check_failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["in_flight", "staleness", "watermark"])
async def test_probe_exception_fails_open(stage, caplog):
    with caplog.at_level(logging.WARNING):
        verdict = await check_traffic(FakeProbe(raises=stage), **_kwargs())
    assert verdict.allowed and verdict.check_failed
    assert any("traffic control check failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_own_row_excluded_from_staleness_query():
    """CRITICAL 1: execute() persists the action's own durable row (via
    create_or_load) *before* the traffic gate runs, so with no exclusion
    the ledger query would find that just-created row as "newer outbound
    activity" and self-block every real send. exclude_action_id must reach
    newest_activity_after, not just in_flight_actions."""
    action_id = uuid4()
    probe = FakeProbe()
    seen = {}
    orig = probe.activity_after

    async def spy(recipient_key, channel_id, watermark, exclude_action_id, limit, exclude_refs=frozenset()):
        seen["exclude"] = exclude_action_id
        return await orig(recipient_key, channel_id, watermark, exclude_action_id, limit, exclude_refs)

    probe.activity_after = spy
    await check_traffic(probe, **_kwargs(action_id=action_id))
    assert seen["exclude"] == action_id


@pytest.mark.asyncio
async def test_override_with_newer_activity_logs_audit_warning(caplog):
    """IMPORTANT 4: override used to short-circuit before ever fetching
    newer activity, so an overridden staleness block left nothing
    auditable. Now it still fetches (for the audit trail only -- this
    must remain log-only, no ledger schema change) and logs a WARNING
    naming what got overridden."""
    newer = NewerActivity("inbound", "zoho_mail", NOW, "Melody withdrew their application", 670826, None)
    with caplog.at_level(logging.WARNING):
        verdict = await check_traffic(FakeProbe(newer=newer), **_kwargs(override=True))
    assert verdict.allowed
    messages = [r.getMessage() for r in caplog.records]
    assert any("override bypassed staleness" in m and "message 670826" in m and "prospect:email:melody@example.com" in m for m in messages)


@pytest.mark.asyncio
async def test_override_without_newer_activity_logs_nothing(caplog):
    with caplog.at_level(logging.WARNING):
        verdict = await check_traffic(FakeProbe(), **_kwargs(override=True))
    assert verdict.allowed
    assert not caplog.records



# --- stale-context confirmation enabled (acknowledged=True) -----------------


def _item(second, message_id, preview="hi"):
    return NewerActivity("inbound", "zoho_cliq", WATERMARK.replace(second=second), preview, message_id, None)


@pytest.mark.asyncio
async def test_enabled_stale_context_lists_every_newer_item_newest_first_and_caps():
    items = [_item(second + 1, 1000 + second, f"item {second}") for second in range(CONTEXT_ITEM_LIMIT + 3)]
    verdict = await check_traffic(FakeProbe(activity=items), **_kwargs(), acknowledged=True)
    assert not verdict.allowed and verdict.reason == "stale_context"
    assert len(verdict.newer) == CONTEXT_ITEM_LIMIT and verdict.truncated
    assert verdict.newer[0].preview == f"item {CONTEXT_ITEM_LIMIT + 2}"
    assert verdict.shown_refs == tuple(f"message:{1000 + n}" for n in range(CONTEXT_ITEM_LIMIT + 2, 2, -1))
    assert "Still send?" in verdict.detail and "needs_human" not in verdict.detail


@pytest.mark.asyncio
async def test_enabled_waives_only_the_items_shown_by_identity():
    shown = _item(27, 750824, "shown")
    verdict = await check_traffic(FakeProbe(activity=[shown], shown={"message:750824"}), **_kwargs(), acknowledged=True)
    assert verdict.allowed and verdict.reason == "pass"


@pytest.mark.asyncio
async def test_late_ingested_item_older_than_what_was_shown_still_asks_again():
    """Blocker 1: messages carry the provider's send time and are ingested
    minutes to hours later. One sent BEFORE the shown item but stored after
    the question was asked was never shown -- it must ask again."""
    shown = _item(27, 750824, "shown")
    late = _item(20, 750899, "sent earlier, ingested later")
    probe = FakeProbe(activity=[shown, late], shown={"message:750824"})
    verdict = await check_traffic(probe, **_kwargs(), acknowledged=True)
    assert verdict.reason == "stale_context"
    assert verdict.shown_refs == ("message:750899",)
    assert probe.asked == [(WATERMARK, frozenset({"message:750824"}))]


@pytest.mark.asyncio
async def test_items_past_the_display_cap_ask_again_after_the_answer():
    items = [_item(second + 1, 2000 + second) for second in range(CONTEXT_ITEM_LIMIT + 2)]
    first = await check_traffic(FakeProbe(activity=items), **_kwargs(), acknowledged=True)
    assert first.truncated
    second = await check_traffic(FakeProbe(activity=items, shown=set(first.shown_refs)), **_kwargs(), acknowledged=True)
    assert second.reason == "stale_context"
    assert set(second.shown_refs) == {"message:2000", "message:2001"}
    third = await check_traffic(
        FakeProbe(activity=items, shown=set(first.shown_refs) | set(second.shown_refs)), **_kwargs(), acknowledged=True
    )
    assert third.allowed


@pytest.mark.asyncio
async def test_shown_read_failure_waives_nothing(caplog):
    probe = FakeProbe(newer=_item(27, 750824), shown={"message:750824"}, raises="acknowledged")
    with caplog.at_level(logging.WARNING):
        verdict = await check_traffic(probe, **_kwargs(), acknowledged=True)
    assert verdict.reason == "stale_context"
    assert any("waiving nothing" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_disabled_never_reads_shown_context_and_keeps_the_legacy_text():
    probe = FakeProbe(newer=_item(27, 750824), shown={"message:750824"}, raises="acknowledged")
    verdict = await check_traffic(probe, **_kwargs())
    assert verdict.reason == "stale_context"
    assert "stale_context, reply still needed" in verdict.detail


@pytest.mark.asyncio
async def test_relisting_shows_the_whole_picture_again():
    probe = FakeProbe(activity=[_item(27, 750824)], shown={"message:750824"})
    verdict = await list_stale_context(probe, recipient_key="r", channel_id=1, wakeup_event_id=1, action_id=uuid4(), logger=LOGGER)
    assert verdict is not None and verdict.shown_refs == ("message:750824",)


def test_question_offers_exactly_yes_no_and_revise_as_exact_json():
    action_id = uuid4()
    question = stale_context_question(wakeup_event_id=27164, action_id=action_id)
    base = f'"op": "confirm", "wakeup_event_id": 27164, "action_id": "{action_id}"'
    assert f'{{{base}, "decision": "yes"}}' in question
    assert f'{{{base}, "decision": "no"}}' in question
    assert f'{{{base}, "decision": "revise", "arguments": ' in question
    assert "circumventing the outbound gateway is never an option" in question.casefold()
    assert "override" not in question.casefold()
