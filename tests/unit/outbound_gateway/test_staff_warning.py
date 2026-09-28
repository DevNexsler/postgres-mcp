from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import CliqArguments
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.staff_warning import DEFAULT_STAFF_WARNING_CHANNEL
from postgres_mcp.outbound_gateway.staff_warning import CliqStaffWarningPort

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")


@pytest.mark.asyncio
async def test_warn_once_posts_a_cliq_channel_internal_notification() -> None:
    service = AsyncMock()
    port = CliqStaffWarningPort(service)

    await port.warn_once(
        ACTION_ID,
        None,
        "provider_rejected_request",
        wakeup_event_id=27123,
        operation=Operation.QUO_SMS_SEND,
        recipient="+15555550100",
    )

    service.execute.assert_awaited_once()
    (request,) = service.execute.await_args.args
    assert isinstance(request, ExecuteRequest)
    assert request.wakeup_event_id == 27123
    assert request.action_role is ActionRole.INTERNAL_NOTIFICATION
    assert request.operation is Operation.CLIQ_CHANNEL_POST
    assert request.intent_kind == IntentKind.MANUAL_REVIEW_ALERT.value
    assert isinstance(request.arguments, CliqArguments)
    assert request.arguments.channel_or_chat_id == DEFAULT_STAFF_WARNING_CHANNEL
    text = request.arguments.text
    assert "quo.sms.send" in text
    assert "+15555550100" in text
    assert "27123" in text
    assert "provider_rejected_request" in text


@pytest.mark.asyncio
async def test_warn_once_is_idempotent_on_action_id() -> None:
    service = AsyncMock()
    port = CliqStaffWarningPort(service)

    await port.warn_once(ACTION_ID, None, "reason", wakeup_event_id=1, operation=Operation.EMAIL_SEND, recipient="a@example.com")
    await port.warn_once(ACTION_ID, None, "reason", wakeup_event_id=1, operation=Operation.EMAIL_SEND, recipient="a@example.com")

    service.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_warn_once_uses_the_configured_target_channel() -> None:
    service = AsyncMock()
    port = CliqStaffWarningPort(service, target_channel="ops-alerts")

    await port.warn_once(ACTION_ID, None, "reason", wakeup_event_id=1, operation=Operation.EMAIL_SEND, recipient="a@example.com")

    (request,) = service.execute.await_args.args
    assert isinstance(request.arguments, CliqArguments)
    assert request.arguments.channel_or_chat_id == "ops-alerts"


@pytest.mark.asyncio
async def test_warn_once_swallows_a_send_failure_without_raising() -> None:
    """The warning itself failing must never bubble up and fail the
    coordinator's advance() step -- a broken Cliq post is not grounds to
    retry (or re-warn about) the already-failed customer action."""
    service = AsyncMock()
    service.execute.side_effect = RuntimeError("cliq unavailable")
    port = CliqStaffWarningPort(service)

    await port.warn_once(ACTION_ID, None, "reason", wakeup_event_id=1, operation=Operation.EMAIL_SEND, recipient="a@example.com")
