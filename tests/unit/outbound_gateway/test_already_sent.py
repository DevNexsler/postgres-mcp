# pyright: reportArgumentType=false, reportPrivateUsage=false, reportAttributeAccessIssue=false
"""A refusal the agent may override names an identical send another action
already delivered, with its message id, and still offers the override.

2026-10-17: one wake emailed an attorney a case packet at 11:52. A second
wake's identical send was refused at 12:13 with only "wake ended ... nothing
was sent"; the agent overrode it at 12:15 and the attorney got the packet
twice. Dan: "Need to say. this was already sent. still over ride? Here is
message ID." The override stays: the agent decides, now with the earlier
send in view.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.models import AlreadySent
from postgres_mcp.outbound_gateway.models import OverrideRequest
from postgres_mcp.outbound_gateway.models import PublicResult
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import handle_outbound_action
from postgres_mcp.outbound_gateway.service import OutboundActionService

REFUSED = UUID("5f0c2a1e-7d43-5b9e-9c21-3a8e6b0d4f11")
EARLIER = UUID("a7b1c9d2-4e5f-5a6b-8c7d-9e0f1a2b3c4d")
MESSAGE_ID = "<outbound-action-0e1f2a3b-4c5d-5e6f-8a9b-0c1d2e3f4a5b@example.com>"
POLICY = FeaturePolicy(writes_enabled=True, kill_switch=False)
ROW = {
    "action_id": EARLIER,
    "wakeup_event_id": 901,
    "provider_message_id": MESSAGE_ID,
    "sent_at": datetime(2026, 10, 17, 15, 52, 18, tzinfo=timezone.utc),
    "recipient": "attorney@example.com",
}
WAKE_TERMINAL = "Not sent: wake 900 ended at 2026-10-17 15:22:50 UTC, before this was asked for; nothing was sent."


def refused(override: bool = True) -> PublicResult:
    return PublicResult(
        status=PublicStatus.REJECTED,
        action_id=REFUSED,
        action_uid=None,
        provider_request_ref=None,
        detail_code="wake_terminal",
        detail=WAKE_TERMINAL,
        override=OverrideRequest(wakeup_event_id=900, action_id=REFUSED) if override else None,
    )


async def status_of(result: PublicResult, earlier: tuple[AlreadySent, ...]) -> tuple[dict[str, Any], AsyncMock]:
    service = AsyncMock()
    service.status.return_value = result
    service.earlier_identical_sends.return_value = earlier
    payload = await handle_outbound_action(service, POLICY, {"op": "status", "action_id": str(REFUSED)})
    return payload, service


@pytest.mark.asyncio
async def test_a_refusal_names_the_identical_send_already_delivered_and_still_offers_the_override() -> None:
    payload, service = await status_of(refused(), (AlreadySent.model_validate(ROW),))

    service.earlier_identical_sends.assert_awaited_once_with(REFUSED)
    assert payload["detail"].startswith(
        "ALREADY SENT: this exact message already went to attorney@example.com at 2026-10-17T15:52:18+00:00 "
        f"(message id {MESSAGE_ID}; action {EARLIER}, wake 901)."
    )
    assert "Still override?" in payload["detail"] and WAKE_TERMINAL in payload["detail"]
    assert payload["already_sent"] == [
        {
            "action_id": str(EARLIER),
            "wakeup_event_id": 901,
            "provider_message_id": MESSAGE_ID,
            "sent_at": "2026-10-17T15:52:18Z",
            "recipient": "attorney@example.com",
        }
    ]
    assert payload["override"] == {"op": "confirm", "wakeup_event_id": 900, "action_id": str(REFUSED), "decision": "yes"}


@pytest.mark.asyncio
async def test_nothing_identical_leaves_the_refusal_as_it_was() -> None:
    payload, _ = await status_of(refused(), ())

    assert payload["detail"].startswith("Not sent: wake 900")
    assert "already_sent" not in payload and "override" in payload


@pytest.mark.asyncio
async def test_a_result_with_no_override_is_not_looked_up() -> None:
    payload, service = await status_of(refused(override=False), (AlreadySent.model_validate(ROW),))

    service.earlier_identical_sends.assert_not_awaited()
    assert "already_sent" not in payload


class _NoLookupStore:
    pass


class _FailingStore:
    async def earlier_identical_sends(self, action_id: UUID) -> list[dict[str, object]]:
        raise RuntimeError("database unavailable")


class _Store:
    async def earlier_identical_sends(self, action_id: UUID) -> list[dict[str, object]]:
        return [ROW] if action_id == REFUSED else []


def _service(store: object) -> OutboundActionService:
    service = object.__new__(OutboundActionService)
    service._store = store
    return service


@pytest.mark.asyncio
async def test_the_lookup_never_blocks_a_refusal() -> None:
    assert await _service(_NoLookupStore()).earlier_identical_sends(REFUSED) == ()
    assert await _service(_FailingStore()).earlier_identical_sends(REFUSED) == ()
    [found] = await _service(_Store()).earlier_identical_sends(REFUSED)
    assert (found.action_id, found.provider_message_id) == (EARLIER, MESSAGE_ID)
