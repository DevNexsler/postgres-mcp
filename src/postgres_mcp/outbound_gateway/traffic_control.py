"""Per-recipient traffic control: in-flight lease + context-staleness watermark.

Pure decision logic over a small probe interface. Fail-open on probe
malfunction: a broken check must never stop outbound traffic (the kill
switch is the only fail-closed control). A check that RUNS and FIRES
blocks normally."""

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


@dataclass(frozen=True)
class NewerActivity:
    direction: str
    source: str
    occurred_at: datetime
    preview: str
    message_id: int | None
    action_id: UUID | None
    sender: str | None = None

    @property
    def ref(self) -> str:
        return f"message:{self.message_id}" if self.message_id else f"action:{self.action_id}"


@dataclass(frozen=True)
class TrafficVerdict:
    allowed: bool
    reason: str  # pass | lease_held | stale_context | gate_check_failed
    detail: str
    check_failed: bool
    # stale_context only: every newer item (newest FIRST, capped at
    # CONTEXT_ITEM_LIMIT) and whether older ones were left out. The newest
    # item's occurred_at is the point the agent acknowledges by answering.
    newer: tuple[NewerActivity, ...] = ()
    truncated: bool = False

    @property
    def acknowledged_through(self) -> datetime | None:
        return self.newer[0].occurred_at if self.newer else None


class TrafficProbe(Protocol):
    async def in_flight_actions(self, recipient_key: str, exclude_action_id: UUID) -> list[InFlightAction]: ...

    async def activity_after(
        self,
        recipient_key: str,
        channel_id: int,
        watermark: datetime,
        exclude_action_id: UUID,
        limit: int,
    ) -> list[NewerActivity]:
        """Activity newer than `watermark`, newest first, at most `limit`."""
        ...

    async def context_watermark(self, wakeup_event_id: int) -> datetime | None: ...

    async def acknowledged_through(self, wakeup_event_id: int, recipient_key: str) -> datetime | None:
        """Newest point of stale context this wake's agent was already shown
        for this recipient (a needs_confirmation it received), or None."""
        ...

    async def message_created_at(self, message_id: int) -> datetime | None:
        """When CDS stored one message (the clock activity_after compares)."""
        ...


# Single source of truth for the three operating modes (service.py and
# server.py both validate against this instead of each keeping their own
# copy of the literal set).
VALID_TRAFFIC_MODES = frozenset({"off", "shadow", "enforce"})

# How many newer items a needs_confirmation result lists. Newest are kept;
# anything older is summarized as a count-free "older items omitted" flag.
CONTEXT_ITEM_LIMIT = 10
CONTEXT_PREVIEW_CHARS = 300


def stale_context_question(*, wakeup_event_id: int, action_id: UUID) -> str:
    """The exact three-way question the agent answers after a stale_context
    refusal: yes, no, or revise.

    Wake 27138 was once told "resend with override=true", the override path
    could not work, and the agent left the gateway for the provider directly.
    The answer is now a first-class gateway call, and the only one."""
    base = f'"op": "confirm", "wakeup_event_id": {wakeup_event_id}, "action_id": "{action_id}"'
    return (
        "Refused - stale context: nothing was sent, because the messages in new_context arrived "
        "after your context was built. Read them, then answer exactly once with outbound_action. "
        f'YES - send your message unchanged: {{{base}, "decision": "yes"}}. '
        f'NO - send nothing (the new context makes it redundant or wrong): {{{base}, "decision": "no"}}. '
        f'REVISE - send a corrected message instead: {{{base}, "decision": "revise", '
        '"arguments": {<the same arguments with only the message content changed>}}; '
        "the operation, recipient and target must stay the same. "
        "One answer per action. Never send it any other way: circumventing the outbound gateway is never an option."
    )


def stale_context_detail(newer: tuple[NewerActivity, ...], *, truncated: bool) -> str:
    newest = newer[0]
    more = f" and {len(newer) - 1} more" if len(newer) > 1 else ""
    omitted = " (older items omitted)" if truncated else ""
    return (
        f"Refused - stale context: new {newest.direction} activity since your context was built: "
        f'{newest.ref.replace(":", " ")} via {newest.source} at {newest.occurred_at.isoformat()}: "{newest.preview}"'
        f"{more}{omitted}. Still send? Answer with op=confirm: yes, no, or revise."
    )


_PASS = TrafficVerdict(allowed=True, reason="pass", detail="", check_failed=False)
_FAIL_OPEN = TrafficVerdict(allowed=True, reason="gate_check_failed", detail="", check_failed=True)


async def _effective_watermark(
    probe: TrafficProbe,
    *,
    wakeup_event_id: int,
    recipient_key: str,
    watermark: datetime,
    logger: logging.Logger,
) -> datetime:
    """The wake's context watermark, advanced to whatever stale context this
    wake's agent was already shown for this recipient. Only activity NEWER
    than a question the agent answered can raise another one. A failed read
    falls back to the wake watermark: that can only ask again, never send
    past unseen context."""
    try:
        acknowledged = await probe.acknowledged_through(wakeup_event_id, recipient_key)
    except Exception:
        logger.warning(
            "traffic control acknowledged-context read failed for wake %s; using the wake watermark",
            wakeup_event_id,
            exc_info=True,
        )
        return watermark
    if acknowledged is not None and acknowledged > watermark:
        return acknowledged
    return watermark


async def check_traffic(
    probe: TrafficProbe,
    *,
    recipient_key: str,
    channel_id: int,
    wakeup_event_id: int,
    action_id: UUID,
    override: bool,
    logger: logging.Logger,
) -> TrafficVerdict:
    try:
        in_flight = await probe.in_flight_actions(recipient_key, action_id)
    except Exception:
        logger.warning("traffic control check failed (in_flight) for %s", recipient_key, exc_info=True)
        return _FAIL_OPEN
    if in_flight:
        other = in_flight[0]
        return TrafficVerdict(
            allowed=False,
            reason="lease_held",
            detail=(
                f"Another send to this recipient is in flight: action {other.action_id} "
                f"({other.operation}, state {other.state}, started {other.created_at.isoformat()}): "
                f'"{other.preview}". Wait for it to reach a terminal state, then re-check '
                f"the thread before sending."
            ),
            check_failed=False,
        )
    if override:
        # override intentionally bypasses a staleness block, but that must
        # still be auditable. Still read the newer activity purely for the
        # audit trail; a probe failure here must not degrade the override
        # itself (fail-open, log-only), so any exception is swallowed after
        # logging. service.py no longer passes override=True for agent
        # requests (an agent's override is routed through the confirm
        # successor path instead); this branch stays for direct callers.
        try:
            watermark = await probe.context_watermark(wakeup_event_id)
            if watermark is not None:
                newer_items = await probe.activity_after(recipient_key, channel_id, watermark, action_id, 1)
                if newer_items:
                    newer = newer_items[0]
                    logger.warning(
                        "override bypassed staleness: wake=%s, recipient=%s, overrode %s at %s",
                        wakeup_event_id,
                        recipient_key,
                        newer.ref.replace(":", " "),
                        newer.occurred_at.isoformat(),
                    )
        except Exception:
            logger.warning(
                "traffic control check failed (staleness, override audit) for %s",
                recipient_key,
                exc_info=True,
            )
        return _PASS
    try:
        watermark = await probe.context_watermark(wakeup_event_id)
        if watermark is None:
            logger.warning("traffic control check failed (no watermark) for wake %s", wakeup_event_id)
            return _FAIL_OPEN
        effective = await _effective_watermark(
            probe,
            wakeup_event_id=wakeup_event_id,
            recipient_key=recipient_key,
            watermark=watermark,
            logger=logger,
        )
        found = await probe.activity_after(
            recipient_key, channel_id, effective, action_id, CONTEXT_ITEM_LIMIT + 1
        )
    except Exception:
        logger.warning("traffic control check failed (staleness) for %s", recipient_key, exc_info=True)
        return _FAIL_OPEN
    if not found:
        return _PASS
    ordered = tuple(sorted(found, key=lambda item: item.occurred_at, reverse=True))
    newer = ordered[:CONTEXT_ITEM_LIMIT]
    truncated = len(ordered) > CONTEXT_ITEM_LIMIT
    return TrafficVerdict(
        allowed=False,
        reason="stale_context",
        detail=stale_context_detail(newer, truncated=truncated),
        check_failed=False,
        newer=newer,
        truncated=truncated,
    )


async def list_stale_context(
    probe: TrafficProbe,
    *,
    recipient_key: str,
    channel_id: int,
    wakeup_event_id: int,
    action_id: UUID,
    logger: logging.Logger,
) -> TrafficVerdict | None:
    """Everything newer than the wake's own context watermark, for re-asking
    an unanswered stale_context question. Unlike check_traffic this ignores
    the acknowledgement point on purpose: the agent re-sent the message
    without answering, so it is shown the whole picture again. None when the
    probe cannot answer."""
    try:
        watermark = await probe.context_watermark(wakeup_event_id)
        if watermark is None:
            return None
        found = await probe.activity_after(recipient_key, channel_id, watermark, action_id, CONTEXT_ITEM_LIMIT + 1)
    except Exception:
        logger.warning("stale-context listing failed for wake %s", wakeup_event_id, exc_info=True)
        return None
    if not found:
        return None
    ordered = tuple(sorted(found, key=lambda item: item.occurred_at, reverse=True))
    newer = ordered[:CONTEXT_ITEM_LIMIT]
    truncated = len(ordered) > CONTEXT_ITEM_LIMIT
    return TrafficVerdict(
        allowed=False,
        reason="stale_context",
        detail=stale_context_detail(newer, truncated=truncated),
        check_failed=False,
        newer=newer,
        truncated=truncated,
    )
