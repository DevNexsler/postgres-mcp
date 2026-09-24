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
        """Stable identity of this item: what a stale_context question records
        as shown, and the only thing a later answer can waive."""
        return f"message:{self.message_id}" if self.message_id else f"action:{self.action_id}"

    @property
    def arm(self) -> str:
        return "messages" if self.message_id else "outbound_actions"


@dataclass(frozen=True)
class TrafficVerdict:
    allowed: bool
    reason: str  # pass | lease_held | stale_context | gate_check_failed
    detail: str
    check_failed: bool
    # stale_context only: newer items not yet shown to this wake's agent for
    # this recipient (newest FIRST, capped at CONTEXT_ITEM_LIMIT) and whether
    # more were left out. Only these refs become "shown" when asked.
    newer: tuple[NewerActivity, ...] = ()
    truncated: bool = False

    @property
    def shown_refs(self) -> tuple[str, ...]:
        return tuple(item.ref for item in self.newer)


class TrafficProbe(Protocol):
    async def in_flight_actions(self, recipient_key: str, exclude_action_id: UUID) -> list[InFlightAction]: ...

    async def activity_after(
        self,
        recipient_key: str,
        channel_id: int,
        watermark: datetime,
        exclude_action_id: UUID,
        limit: int,
        exclude_refs: frozenset[str] = frozenset(),
    ) -> list[NewerActivity]:
        """Activity newer than `watermark`, newest first, at most `limit`,
        never an item whose ref is in `exclude_refs`."""
        ...

    async def context_watermark(self, wakeup_event_id: int) -> datetime | None: ...

    async def acknowledged_refs(self, wakeup_event_id: int, recipient_key: str) -> frozenset[str]:
        """Refs of every item this wake's agent was shown for this recipient
        in a stale_context question (Comm-Data-Store migration 192)."""
        ...

    async def messages_by_id(self, message_ids: list[int]) -> list[NewerActivity]:
        """The given messages as context items (for inbound the staleness probe
        cannot see, e.g. another channel of the same prospect)."""
        ...


# Single source of truth for the three operating modes (service.py and
# server.py both validate against this instead of each keeping their own
# copy of the literal set).
VALID_TRAFFIC_MODES = frozenset({"off", "shadow", "enforce"})

# How many newer items a needs_confirmation result lists. The newest are
# shown; anything older is left unshown, so it asks again after the answer.
CONTEXT_ITEM_LIMIT = 10
CONTEXT_PREVIEW_CHARS = 300


# Agent-facing close of a stale_context block while stale-context
# confirmation is disabled (OUTBOUND_STALE_CONFIRM_ENABLED unset/false): the
# pre-192 contract, byte for byte. Override remains an operator remediation and
# is not named here: wake 27138 followed the old sentence, then sent through
# the provider directly.
STALE_CONTEXT_AGENT_INSTRUCTION = (
    "Re-read the thread and skip if your message is now redundant. "
    "If the reply is still needed, record needs_human with the reason "
    '"stale_context, reply still needed". '
    "A gateway refusal is final. Circumventing the outbound gateway is never an option."
)

# Every refused confirm says this. A refused answer is a decision about the
# message, never a gateway outage: it must not open the guarded Cliq DM
# fallback or any other route around the gateway.
CONFIRM_REFUSAL_NOTICE = (
    "This refusal is not a gateway infrastructure failure: it does not permit the direct "
    "Cliq fallback or any other route around the outbound gateway."
)


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
        "the operation, intent, slot, recipient and target must stay the same. "
        "One answer per action. Never send it any other way: circumventing the outbound gateway is never an option."
    )


def stale_context_detail(newer: tuple[NewerActivity, ...], *, truncated: bool) -> str:
    newest = newer[0]
    more = f" and {len(newer) - 1} more" if len(newer) > 1 else ""
    omitted = " (older items omitted; you will be asked about them next)" if truncated else ""
    return (
        f"Refused - stale context: new {newest.direction} activity since your context was built: "
        f'{newest.ref.replace(":", " ")} via {newest.source} at {newest.occurred_at.isoformat()}: "{newest.preview}"'
        f"{more}{omitted}. Still send? Answer with op=confirm: yes, no, or revise."
    )


def legacy_stale_context_detail(newest: NewerActivity) -> str:
    """The pre-192 detail text, used while confirmation is disabled."""
    return (
        f"New {newest.direction} activity since your context was built: {newest.ref.replace(':', ' ')} via "
        f'{newest.arm} at {newest.occurred_at.isoformat()}: "{newest.preview[:120]}". ' + STALE_CONTEXT_AGENT_INSTRUCTION
    )


_PASS = TrafficVerdict(allowed=True, reason="pass", detail="", check_failed=False)
_FAIL_OPEN = TrafficVerdict(allowed=True, reason="gate_check_failed", detail="", check_failed=True)


def _stale_verdict(found: list[NewerActivity], *, legacy: bool) -> TrafficVerdict:
    ordered = tuple(sorted(found, key=lambda item: item.occurred_at, reverse=True))
    newer = ordered[:CONTEXT_ITEM_LIMIT]
    truncated = len(ordered) > CONTEXT_ITEM_LIMIT
    return TrafficVerdict(
        allowed=False,
        reason="stale_context",
        detail=legacy_stale_context_detail(newer[0]) if legacy else stale_context_detail(newer, truncated=truncated),
        check_failed=False,
        newer=newer,
        truncated=truncated,
    )


def stale_context_verdict(items: list[NewerActivity]) -> TrafficVerdict | None:
    """A stale_context verdict over items found outside the probe (the
    preflight's cross-channel newer inbound)."""
    return _stale_verdict(items, legacy=False) if items else None


async def check_traffic(
    probe: TrafficProbe,
    *,
    recipient_key: str,
    channel_id: int,
    wakeup_event_id: int,
    action_id: UUID,
    override: bool,
    logger: logging.Logger,
    acknowledged: bool = False,
) -> TrafficVerdict:
    """acknowledged=True (stale-context confirmation enabled): items this
    wake's agent was already SHOWN for this recipient are waived -- by
    identity, never by timestamp. Messages are stored with the provider's send
    time and ingested minutes to hours later, so "older than what was shown"
    is not "was shown". Everything else newer than the wake's watermark still
    counts, including items ingested late and items past the display cap."""
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
        # logging. With confirmation enabled service.py never passes
        # override=True (an agent's override is the "yes" answer instead).
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
        shown: frozenset[str] = frozenset()
        if acknowledged:
            try:
                shown = frozenset(await probe.acknowledged_refs(wakeup_event_id, recipient_key))
            except Exception:
                # Waiving nothing can only ask again; it never sends past
                # unseen context.
                logger.warning(
                    "traffic control acknowledged-context read failed for wake %s; waiving nothing",
                    wakeup_event_id,
                    exc_info=True,
                )
        found = await probe.activity_after(
            recipient_key, channel_id, watermark, action_id, CONTEXT_ITEM_LIMIT + 1, shown
        )
    except Exception:
        logger.warning("traffic control check failed (staleness) for %s", recipient_key, exc_info=True)
        return _FAIL_OPEN
    found = [item for item in found if item.ref not in shown]
    if not found:
        return _PASS
    return _stale_verdict(found, legacy=not acknowledged)


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
    an unanswered stale_context question: the agent re-sent the message
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
    return _stale_verdict(found, legacy=False)
