"""Calendar writes go only to Nigel's calendar (#4360).

Wakes 28838, 29019 and 29652 sent a second calendar.update for Dan's copy
of an event (calendar_id "dan" / "dan-zoho"). Dan's copy is an attendee
copy of Nigel's event and follows it on its own; the gateway has no write
account for it. The gateway used to accept the request, record it, and
then fault it at dispatch ("calendar account is not configured", a plain
ValueError classified as a gateway fault) about 17 times until the retry
budget ran out -- definitive_failed, and the wake watch paged the wake.

It is now refused at admission: nothing is recorded, nothing retries, and
the agent reads a specific detail_code and what to do instead.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock

import pytest

from postgres_mcp.outbound_gateway.adapters.calendar import CalendarAdapter
from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.context import CalendarNotWritableError
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.errors import FailureKind
from postgres_mcp.outbound_gateway.errors import classify
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import create_server
from postgres_mcp.outbound_gateway.server import handle_outbound_action
from postgres_mcp.outbound_gateway.service import OutboundActionService

from .test_adapters import context as adapter_context
from .test_context import FakeRepository
from .test_context import policy
from .test_context import record
from .test_context import request

# Dan's caldav principal, as wake 29652 sent it (action d7ae0917).
_DAN_COPY_URL = "https://calendar.zoho.com/caldav/4b001eab07474b1eba5b7e4f72a5dfe6/events/379622bc-249a-cc01-a549-478d909510d3.ics"
UNWRITABLE = ("dan", "dan-zoho", "dan.park", "nigel-zoho")


def _calendar_request(operation: str, calendar_id: str, **overrides):
    intent, slot = {
        "calendar.create": ("showing_create", "2026-10-27T10:30:00-04:00"),
        "calendar.update": ("showing_update", "2026-10-27T10:30:00-04:00"),
        "calendar.delete": ("showing_delete", None),
    }[operation]
    arguments = {"calendar_id": calendar_id}
    if operation != "calendar.create":
        arguments.update(event_url=_DAN_COPY_URL, etag="1792790467349")
    values = dict(
        action_role="calendar_mutation",
        operation=operation,
        intent_kind=intent,
        appointment_slot=slot,
        arguments=arguments,
    )
    values.update(overrides)
    return request(**values)


def _loader():
    event = record(raw_payload={"provider": "zillow", "thread_id": "zrm-thread-44"})
    return ActionContextLoader(FakeRepository(event), policy())


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["calendar.create", "calendar.update", "calendar.delete"])
@pytest.mark.parametrize("calendar_id", UNWRITABLE)
async def test_a_write_to_a_calendar_with_no_write_account_is_refused_at_admission(operation, calendar_id):
    with pytest.raises(CalendarNotWritableError) as refused:
        await _loader().load(_calendar_request(operation, calendar_id))
    error = refused.value
    assert classify(error) is FailureKind.REFUSAL
    assert error.detail_code == "calendar_not_writable"
    text = str(error)
    assert f'calendar "{calendar_id}" is not writable' in text
    assert 'calendar_id "nigel"' in text
    assert "attendee copies" in text and "Nothing was sent" in text


@pytest.mark.asyncio
async def test_nigels_calendar_is_still_admitted():
    context = await _loader().load(_calendar_request("calendar.update", "nigel"))
    assert context.target == DerivedTarget("calendar", "nigel", True)


def _service(store):
    return OutboundActionService(
        store=store,
        context_loader=_loader(),
        evidence_loader=AsyncMock(),
        adapters={Operation.CALENDAR_UPDATE: CalendarAdapter(account_by_calendar={"nigel": "nigel-zoho"})},
        provider_client=object(),
        clock=lambda: datetime(2026, 10, 26, 14, 1, tzinfo=timezone.utc),
        lease_owner="gateway-test",
        response_budget_seconds=1,
        sleep=AsyncMock(),
        traffic_mode="off",
    )


@pytest.mark.asyncio
async def test_wake_29652_dan_copy_update_is_rejected_without_recording_or_retrying():
    """The exact request wake 29652 sent for Dan's copy: rejected at once,
    not retryable, with its own detail_code, and no outbound action is
    recorded -- so there is nothing to dispatch, retry or page about."""
    store = AsyncMock()
    body = await handle_outbound_action(
        _service(store),
        FeaturePolicy(writes_enabled=True, kill_switch=False, enabled_operations=frozenset(Operation)),
        {
            "op": "execute",
            "wakeup_event_id": 12345,
            "action_role": "calendar_mutation",
            "operation": "calendar.update",
            "intent_kind": "showing_update",
            "appointment_slot": "2026-10-27T10:30:00-04:00",
            "arguments": {
                "calendar_id": "dan",
                "event_url": _DAN_COPY_URL,
                "etag": "1792790467349",
                "duration_minutes": 180,
            },
        },
    )
    assert (body["status"], body["detail_code"], body["retryable"]) == ("rejected", "calendar_not_writable", False)
    assert 'calendar_id "nigel"' in body["detail"]
    store.create_or_load.assert_not_awaited()
    assert store.method_calls == []


@pytest.mark.parametrize("operation", [Operation.CALENDAR_CREATE, Operation.CALENDAR_UPDATE, Operation.CALENDAR_DELETE])
def test_a_recorded_action_for_an_unwritable_calendar_is_a_refusal_at_dispatch_not_a_fault(operation):
    """A row recorded before admission refused these (or by any other path)
    must not be classified a gateway fault: a fault is retried until the
    retry budget runs out (18 attempts on action d7ae0917)."""
    adapter = CalendarAdapter(account_by_calendar={"nigel": "nigel-zoho"})
    ctx = dataclasses.replace(adapter_context(operation), target=DerivedTarget("calendar", "dan", True))
    with pytest.raises(CalendarNotWritableError) as refused:
        adapter.validate(ctx)
    assert classify(refused.value) is FailureKind.REFUSAL


@pytest.mark.asyncio
async def test_the_tool_description_states_the_calendar_write_contract():
    mcp = create_server(AsyncMock(), FeaturePolicy(writes_enabled=True, kill_switch=False))
    description = next(tool.description or "" for tool in await mcp.list_tools() if tool.name == "outbound_action")
    assert "Calendar writes go only to Nigel's calendar" in description
    assert "never write a dan or dan-zoho copy" in description
    assert "calendar_not_writable" in description
    assert description.count('calendar_id "nigel" only') == 3
