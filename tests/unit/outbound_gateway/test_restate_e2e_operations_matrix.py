# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false
"""Extends test_restate_e2e_delivery.py's real-path proof (coordinator ->
OutboundActionService -> ActionRecovery -> adapter -> adapters.base) from
cliq.channel.post alone to every operation in OUTBOUND_RESTATE_OPERATIONS'
flag set: quo.sms.send, email.send, cliq.chat.post, cliq.channel.post,
calendar.create/update/delete, and all four tenantcloud.* operations.

Only the transport is faked per operation:

- quo / email / cliq / calendar: the MCP client (``ScriptThenRepeatClient``),
  scripted with response shapes drawn from ``test_adapters.py``'s own
  fixtures and from the real Agent Email / Quo payload shapes documented in
  ``adapters/base.py`` and ``idempotency_policy.py``.
- tenantcloud.*: the ``TenantCloudMutations`` facade double already used by
  ``test_adapters.py`` (``FakeTenantCloudMutations``) -- the real
  ``TenantCloudAdapter.invoke``/``reconcile`` never touch the MCP client at
  all (``del client``), so faking the client there would fake nothing.

Every case drives the SAME real code every other e2e test in this file's
sibling drives: ``OutboundDeliveryCoordinator.advance()`` ->
``OutboundActionService`` -> ``ActionRecovery`` -> the real adapter's
``build_request``/``invoke``/``poll``/``reconcile``. Nothing here mocks
``DeliveryService`` or the adapter.

Idempotency-policy findings this matrix proves out (idempotency_policy.py):

- cliq.*/calendar.*: no independent read-back exists -- ``reconcile()`` only
  re-polls the same job id. An ambiguous outcome that never settles has
  exactly one shape: never accepted, never rejected, ends
  ``definitive_failed`` at the ceiling with one warning.
- quo/email (CONFIRM_BY_READBACK): an independent read-back
  (``list_messages`` / ``email_get_thread``) can still settle an ambiguous
  outcome after the fact -- both a "found" (completes, no warning) and a
  "never found" (``definitive_failed`` at the ceiling, one warning) case are
  covered.
- tenantcloud.* (SAFE_TO_REINVOKE via the facade's own idempotent
  precheck+write+readback): the facade's ``reconcile_*`` methods are that
  read-back. Both "eventually confirmed" and "never confirmed" are covered.

Every case also asserts how many times the provider's effect call
(``adapter.invoke()`` / the facade's write method) actually ran. For every
operation except tenantcloud.lead.status.update and tenantcloud.maintenance
.status.update, that count is exactly ONE even on an ambiguous-forever
outcome: nothing here ever blindly re-invokes. Those two are the one
genuine SAFE_TO_REINVOKE case this matrix exercises for real -- their
reconciliation's "not yet applied" outcome is DEFINITIVE_NON_ACCEPTANCE
with ``retryable=True`` (adapters/tenantcloud.py's ``_from_reconciliation``),
which really does route back through RETRY_READY to a fresh invoke, each one
still behind the facade's own idempotent precheck. See those two tests'
docstrings.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import MappingProxyType
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.adapters.calendar import CalendarAdapter
from postgres_mcp.outbound_gateway.adapters.cliq import CliqAdapter
from postgres_mcp.outbound_gateway.adapters.email import EmailAdapter
from postgres_mcp.outbound_gateway.adapters.quo import QuoSmsAdapter
from postgres_mcp.outbound_gateway.adapters.tenantcloud import TenantCloudAdapter
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.delivery_workflow import AuthResult
from postgres_mcp.outbound_gateway.delivery_workflow import AuthState
from postgres_mcp.outbound_gateway.delivery_workflow import DeliveryPhase
from postgres_mcp.outbound_gateway.delivery_workflow import OutboundDeliveryCoordinator
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.provider_client import McpCallResult
from postgres_mcp.outbound_gateway.provider_client import TransportErrorKind
from postgres_mcp.outbound_gateway.record import OutboundActionRecord
from postgres_mcp.outbound_gateway.retry_policy import RETRY_CEILING_SECONDS
from postgres_mcp.outbound_gateway.service import OutboundActionService

from .test_adapters import TC_ACCEPTED
from .test_adapters import TC_DEFINITIVE_NON_ACCEPTANCE
from .test_adapters import TC_UNKNOWN
from .test_adapters import FakeMutationExecution
from .test_adapters import FakeMutationObservation
from .test_adapters import FakeMutationResult
from .test_adapters import FakeReconciliationResult
from .test_adapters import FakeTenantCloudMutations
from .test_restate_e2e_delivery import FakeStore
from .test_restate_e2e_delivery import RecordingStaffWarningPort

ACTION_UID = UUID("9ebddbf7-8fc8-5a4f-bba7-869ea7053521")
CREATED_AT = datetime(2026, 9, 28, 22, 43, 0, tzinfo=timezone.utc)
APPOINTMENT = datetime(2026, 9, 30, 14, 30, tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# Generic harness (shared shape with test_restate_e2e_delivery.py's FakeStore/
# RecordingStaffWarningPort/FakeClock/drive, generalized over every action id
# and operation instead of just wake 27348's cliq.channel.post).
# --------------------------------------------------------------------------


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=max(0.0, seconds))


class ScriptThenRepeatClient:
    """Pop a finite script of responses; once one remains, repeat it forever.

    An "ambiguous forever" scenario runs an unpredictable number of poll/
    reconcile round trips before the coordinator's one-hour ceiling fires --
    this makes that count irrelevant to the fixture instead of requiring a
    hand-computed script length.
    """

    def __init__(self, *results: McpCallResult) -> None:
        if not results:
            raise ValueError("ScriptThenRepeatClient needs at least one scripted result")
        self._script = list(results)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def call(self, server_name: str, tool: str, arguments: dict[str, Any]) -> McpCallResult:
        self.calls.append((server_name, tool, arguments))
        if len(self._script) > 1:
            return self._script.pop(0)
        return self._script[0]

    def tool_calls(self) -> list[str]:
        return [call[1] for call in self.calls]


async def drive(coordinator: OutboundDeliveryCoordinator, action_id: UUID, clock: FakeClock, *, max_steps: int = 4000):
    for _ in range(max_steps):
        result = await coordinator.advance(action_id)
        if result.phase in (DeliveryPhase.COMPLETE, DeliveryPhase.TERMINAL):
            return result
        clock.advance(result.retry_after_seconds)
    raise AssertionError("workflow did not terminate within max_steps")


def build_coordinator(
    store: FakeStore,
    service: OutboundActionService,
    clock: FakeClock,
    staff_warning: RecordingStaffWarningPort,
    operation: Operation,
) -> OutboundDeliveryCoordinator:
    auth = AsyncMock()
    auth.ensure_ready.return_value = AuthResult(AuthState.READY)
    return OutboundDeliveryCoordinator(
        store=store,
        service=service,
        auth=auth,
        operations=frozenset({operation}),
        clock=clock,
        staff_warning=staff_warning,
    )


def build_service(
    store: FakeStore,
    adapter: Any,
    operation: Operation,
    provider_client: Any,
    clock: FakeClock,
    context: ActionContext,
) -> OutboundActionService:
    context_loader = AsyncMock()
    context_loader.load.return_value = context
    evidence_loader = AsyncMock()
    evidence_loader.load.return_value = object()
    return OutboundActionService(
        store=store,
        context_loader=context_loader,
        evidence_loader=evidence_loader,
        adapters={operation: adapter},
        provider_client=provider_client,
        clock=clock,
        lease_owner="outbound-gateway-test",
        response_budget_seconds=6,
        lease_seconds=60,
        sleep=AsyncMock(),
        traffic_mode="off",
        restate_operations=frozenset({operation}),
    )


def make_row(
    *,
    action_id: UUID,
    operation: Operation,
    action_role: ActionRole,
    intent_kind: IntentKind,
    arguments: dict[str, Any],
    provider_account: str,
    appointment_slot: datetime | None = None,
    wakeup_event_id: int = 27400,
) -> OutboundActionRecord:
    return OutboundActionRecord(
        action_id=action_id,
        wakeup_event_id=wakeup_event_id,
        action_role=action_role,
        operation=operation,
        intent_kind=intent_kind,
        appointment_slot=appointment_slot,
        arguments=arguments,
        state=ActionState.RECEIVED,
        action_uid=None,
        provider_request_ref=None,
        provider_message_id=None,
        provider_accepted_at=None,
        completion_kind=None,
        detail_code="received",
        attempt_count=0,
        next_attempt_at=CREATED_AT,
        payload_hash="",
        canonical_context={},
        canonical_scope={},
        recipient_scope={},
        provider_account=provider_account,
        routing_policy_version="v1",
        created_at=CREATED_AT,
    )


def make_context(
    *,
    action_id: UUID,
    operation: Operation,
    action_role: ActionRole,
    intent_kind: IntentKind,
    target: DerivedTarget,
    arguments: dict[str, Any],
    provider_account: str,
    appointment_slot: datetime | None = None,
    recipient_phone: str | None = None,
    calendar_event_uid: str | None = None,
    calendar_event_url: str | None = None,
    calendar_event_etag: str | None = None,
    canonical_context: dict[str, Any] | None = None,
    wakeup_event_id: int = 27400,
) -> ActionContext:
    return ActionContext(
        action_id=action_id,
        wakeup_event_id=wakeup_event_id,
        action_role=action_role,
        operation=operation,
        intent_kind=intent_kind,
        appointment_slot=appointment_slot,
        arguments=MappingProxyType(arguments),
        source="tenantcloud",
        source_message_id=1,
        source_message_key="tenantcloud:1",
        source_sent_at=CREATED_AT,
        conversation_id="conversation:matrix",
        conversation_watermark=1,
        prospect_id="prospect:matrix",
        aliases=(),
        property_id=None,
        property_label="138 Bullman St #144-A",
        target=target,
        provider_account=provider_account,
        routing_policy_version="v1",
        canonical_scope=MappingProxyType({}),
        canonical_context=MappingProxyType(canonical_context or {}),
        payload_hash="",
        lock_holder=f"outbound-gateway:{action_id}",
        thread_identity="thread-matrix",
        showing_lifecycle_id="showing:matrix",
        calendar_event_uid=calendar_event_uid,
        prospect_name="Amanda Snyder",
        recipient_phone=recipient_phone,
        calendar_event_url=calendar_event_url,
        calendar_event_etag=calendar_event_etag,
    )


def pending(request_id: str = "req-matrix-1") -> McpCallResult:
    return McpCallResult(structured_content={"status": "pending", "request_id": request_id, "call_id": request_id})


def transport_timeout() -> McpCallResult:
    return McpCallResult(error_kind=TransportErrorKind.TIMEOUT, is_error=True, safe_detail="provider_transport_timeout")


async def run_case(
    *,
    action_id: UUID,
    operation: Operation,
    adapter: Any,
    context: ActionContext,
    record: OutboundActionRecord,
    provider_client: Any,
) -> tuple[Any, FakeStore, FakeClock, RecordingStaffWarningPort]:
    store = FakeStore(record)
    clock = FakeClock(CREATED_AT)
    service = build_service(store, adapter, operation, provider_client, clock, context)
    staff_warning = RecordingStaffWarningPort()
    coordinator = build_coordinator(store, service, clock, staff_warning, operation)
    result = await drive(coordinator, action_id, clock)
    return result, store, clock, staff_warning


# ==========================================================================
# quo.sms.send -- CONFIRM_BY_READBACK
# ==========================================================================

QUO_ACTION_ID = UUID("1a000000-0000-5000-8000-000000000001")


def quo_context() -> ActionContext:
    return make_context(
        action_id=QUO_ACTION_ID,
        operation=Operation.QUO_SMS_SEND,
        action_role=ActionRole.PROSPECT_REPLY,
        intent_kind=IntentKind.INQUIRY_REPLY,
        target=DerivedTarget("quo_conversation", "quo-thread-1", True),
        arguments={"text": "Friday at 10:30 works. — Nigel"},
        provider_account="leasing-line",
        recipient_phone="+19085550199",
    )


def quo_record() -> OutboundActionRecord:
    return make_row(
        action_id=QUO_ACTION_ID,
        operation=Operation.QUO_SMS_SEND,
        action_role=ActionRole.PROSPECT_REPLY,
        intent_kind=IntentKind.INQUIRY_REPLY,
        arguments={"to_phone": "+19085550199", "text": "Friday at 10:30 works. — Nigel"},
        provider_account="leasing-line",
    )


@pytest.mark.asyncio
async def test_quo_success_completes_with_receipt_and_no_warning():
    client = ScriptThenRepeatClient(
        McpCallResult(structured_content={"status": "sent", "message_id": "quo-message-1"})
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=QUO_ACTION_ID,
        operation=Operation.QUO_SMS_SEND,
        adapter=QuoSmsAdapter(user_id="user-1"),
        context=quo_context(),
        record=quo_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert store.current.provider_message_id == "quo-message-1"
    assert staff_warning.calls == []
    assert client.tool_calls() == ["send_message"]


@pytest.mark.asyncio
async def test_quo_definitive_rejection_ends_definitive_failed_with_one_warning():
    client = ScriptThenRepeatClient(
        McpCallResult(is_error=True, text="Error executing send_message: to is not a valid phone number")
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=QUO_ACTION_ID,
        operation=Operation.QUO_SMS_SEND,
        adapter=QuoSmsAdapter(user_id="user-1"),
        context=quo_context(),
        record=quo_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert len(staff_warning.calls) == 1
    assert client.tool_calls() == ["send_message"]


@pytest.mark.asyncio
async def test_quo_ambiguous_forever_settled_by_readback_completes_with_no_warning():
    client = ScriptThenRepeatClient(
        transport_timeout(),
        McpCallResult(
            structured_content={
                "messages": [
                    {
                        "id": "quo-message-2",
                        "direction": "outgoing",
                        "to": "+19085550199",
                        "content": "Friday at 10:30 works. — Nigel",
                        "created_at": "2026-09-28T22:44:00Z",
                    }
                ]
            }
        ),
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=QUO_ACTION_ID,
        operation=Operation.QUO_SMS_SEND,
        adapter=QuoSmsAdapter(user_id="user-1"),
        context=quo_context(),
        record=quo_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert store.current.provider_message_id == "quo-message-2"
    assert staff_warning.calls == []
    assert client.tool_calls().count("send_message") == 1


@pytest.mark.asyncio
async def test_quo_ambiguous_forever_never_found_reaches_ceiling_definitive_failed_one_warning():
    client = ScriptThenRepeatClient(
        transport_timeout(),
        McpCallResult(structured_content={"messages": []}),
    )
    result, store, clock, staff_warning = await run_case(
        action_id=QUO_ACTION_ID,
        operation=Operation.QUO_SMS_SEND,
        adapter=QuoSmsAdapter(user_id="user-1"),
        context=quo_context(),
        record=quo_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    assert client.tool_calls().count("send_message") == 1


# ==========================================================================
# email.send -- CONFIRM_BY_READBACK
# ==========================================================================

EMAIL_ACTION_ID = UUID("2a000000-0000-5000-8000-000000000002")


def email_context() -> ActionContext:
    return make_context(
        action_id=EMAIL_ACTION_ID,
        operation=Operation.EMAIL_SEND,
        action_role=ActionRole.PROSPECT_REPLY,
        intent_kind=IntentKind.INQUIRY_REPLY,
        target=DerivedTarget("email_thread", "lead@convo.zillow.com", True),
        arguments={"text": "Friday at 10:30 works. — Nigel"},
        provider_account="nigel-zoho",
    )


def email_record() -> OutboundActionRecord:
    return make_row(
        action_id=EMAIL_ACTION_ID,
        operation=Operation.EMAIL_SEND,
        action_role=ActionRole.PROSPECT_REPLY,
        intent_kind=IntentKind.INQUIRY_REPLY,
        arguments={"to_address": "lead@convo.zillow.com", "text": "Friday at 10:30 works. — Nigel"},
        provider_account="nigel-zoho",
    )


def email_adapter() -> EmailAdapter:
    return EmailAdapter(
        sender_domains={"nigel-zoho": "pfg.example"},
        reconciliation_wait_seconds=0.05,
        reconciliation_poll_interval_seconds=0.01,
        reconciliation_sleep=AsyncMock(),
    )


def email_thread_found() -> McpCallResult:
    return McpCallResult(
        structured_content={
            "status": "completed",
            "request_id": "thread-lookup-1",
            "result": {
                "tool_name": "email_get_thread",
                "structured_content": {
                    "status": "success",
                    "data": {"content": [{"type": "text", "text": "**Thread:** exact deterministic message"}]},
                },
            },
        }
    )


def email_thread_not_found() -> McpCallResult:
    return McpCallResult(
        structured_content={
            "status": "completed",
            "request_id": "thread-lookup-1",
            "result": {
                "tool_name": "email_get_thread",
                "structured_content": {"status": "success", "data": {"content": [{"type": "text", "text": "No messages."}]}},
            },
        }
    )


@pytest.mark.asyncio
async def test_email_success_completes_with_receipt_and_no_warning():
    client = ScriptThenRepeatClient(
        pending(),
        McpCallResult(
            structured_content={
                "status": "completed",
                "request_id": "req-matrix-1",
                "result": {"tool_name": "email_send", "structured_content": {"status": "success", "provider_message_id": "<mail-1@example.com>"}},
            }
        ),
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=EMAIL_ACTION_ID,
        operation=Operation.EMAIL_SEND,
        adapter=email_adapter(),
        context=email_context(),
        record=email_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert store.current.provider_message_id == "<mail-1@example.com>"
    assert staff_warning.calls == []
    assert client.tool_calls() == ["email_send", "request_status"]


@pytest.mark.asyncio
async def test_email_definitive_rejection_ends_definitive_failed_with_one_warning():
    client = ScriptThenRepeatClient(
        McpCallResult(
            structured_content={
                "status": "failed",
                "category": "permanent_upstream_error",
                "retryable": False,
                "message": "SMTP rejected recipient: mailbox does not exist",
                "request_id": "req-matrix-1",
            }
        )
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=EMAIL_ACTION_ID,
        operation=Operation.EMAIL_SEND,
        adapter=email_adapter(),
        context=email_context(),
        record=email_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert len(staff_warning.calls) == 1
    assert client.tool_calls() == ["email_send"]


@pytest.mark.asyncio
async def test_email_ambiguous_forever_settled_by_readback_completes_with_no_warning():
    client = ScriptThenRepeatClient(transport_timeout(), email_thread_found())
    result, store, _clock, staff_warning = await run_case(
        action_id=EMAIL_ACTION_ID,
        operation=Operation.EMAIL_SEND,
        adapter=email_adapter(),
        context=email_context(),
        record=email_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert client.tool_calls().count("email_send") == 1


@pytest.mark.asyncio
async def test_email_ambiguous_forever_never_found_reaches_ceiling_definitive_failed_one_warning():
    client = ScriptThenRepeatClient(transport_timeout(), email_thread_not_found())
    result, store, clock, staff_warning = await run_case(
        action_id=EMAIL_ACTION_ID,
        operation=Operation.EMAIL_SEND,
        adapter=email_adapter(),
        context=email_context(),
        record=email_record(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    assert client.tool_calls().count("email_send") == 1


# ==========================================================================
# cliq.chat.post / cliq.channel.post -- SAFE_TO_REINVOKE, no independent
# read-back (reconcile() only re-polls the same job id).
# ==========================================================================

CLIQ_CHAT_ACTION_ID = UUID("3a000000-0000-5000-8000-000000000003")
CLIQ_CHANNEL_ACTION_ID = UUID("3a000000-0000-5000-8000-000000000004")


def cliq_chat_context() -> ActionContext:
    return make_context(
        action_id=CLIQ_CHAT_ACTION_ID,
        operation=Operation.CLIQ_CHAT_POST,
        action_role=ActionRole.INTERNAL_REPLY,
        intent_kind=IntentKind.INTERNAL_REPLY,
        target=DerivedTarget("cliq_chat", "CT_123", True),
        arguments={"text": "On it, checking now."},
        provider_account="CT_123",
    )


def cliq_chat_record() -> OutboundActionRecord:
    return make_row(
        action_id=CLIQ_CHAT_ACTION_ID,
        operation=Operation.CLIQ_CHAT_POST,
        action_role=ActionRole.INTERNAL_REPLY,
        intent_kind=IntentKind.INTERNAL_REPLY,
        arguments={"channel_or_chat_id": "CT_123", "text": "On it, checking now."},
        provider_account="CT_123",
    )


def cliq_channel_context() -> ActionContext:
    return make_context(
        action_id=CLIQ_CHANNEL_ACTION_ID,
        operation=Operation.CLIQ_CHANNEL_POST,
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        intent_kind=IntentKind.LEAD_ALERT,
        target=DerivedTarget("cliq_channel", "tenant-leads", True),
        arguments={"text": "New lead needs review"},
        provider_account="tenant-leads",
    )


def cliq_channel_record() -> OutboundActionRecord:
    return make_row(
        action_id=CLIQ_CHANNEL_ACTION_ID,
        operation=Operation.CLIQ_CHANNEL_POST,
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        intent_kind=IntentKind.LEAD_ALERT,
        arguments={"channel_or_chat_id": "tenant-leads", "text": "New lead needs review"},
        provider_account="tenant-leads",
    )


CLIQ_CASES = [
    pytest.param(Operation.CLIQ_CHAT_POST, "cliq_chat_post", CLIQ_CHAT_ACTION_ID, cliq_chat_context, cliq_chat_record, id="chat"),
    pytest.param(
        Operation.CLIQ_CHANNEL_POST, "cliq_channel_bot_post", CLIQ_CHANNEL_ACTION_ID, cliq_channel_context, cliq_channel_record, id="channel"
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "tool", "action_id", "make_ctx", "make_rec"), CLIQ_CASES)
async def test_cliq_success_completes_with_receipt_and_no_warning(operation, tool, action_id, make_ctx, make_rec):
    client = ScriptThenRepeatClient(
        pending(),
        McpCallResult(
            structured_content={
                "status": "completed",
                "request_id": "req-matrix-1",
                "result": {"tool_name": tool, "structured_content": {"status": "sent", "provider_message_id": "provider-cliq-1"}},
            }
        ),
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=action_id,
        operation=operation,
        adapter=CliqAdapter(operation),
        context=make_ctx(),
        record=make_rec(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert client.tool_calls() == [tool, "request_status"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "tool", "action_id", "make_ctx", "make_rec"), CLIQ_CASES)
async def test_cliq_definitive_rejection_ends_definitive_failed_with_one_warning(operation, tool, action_id, make_ctx, make_rec):
    client = ScriptThenRepeatClient(
        McpCallResult(
            structured_content={
                "status": "failed",
                "category": "permanent_upstream_error",
                "retryable": False,
                "message": "Cliq message was NOT sent: channel not found or the bot is not a member.",
                "request_id": "req-matrix-1",
            }
        )
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=action_id,
        operation=operation,
        adapter=CliqAdapter(operation),
        context=make_ctx(),
        record=make_rec(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert len(staff_warning.calls) == 1
    assert client.tool_calls() == [tool]


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "tool", "action_id", "make_ctx", "make_rec"), CLIQ_CASES)
async def test_cliq_ambiguous_forever_reaches_ceiling_definitive_failed_one_warning(operation, tool, action_id, make_ctx, make_rec):
    client = ScriptThenRepeatClient(pending())
    result, store, clock, staff_warning = await run_case(
        action_id=action_id,
        operation=operation,
        adapter=CliqAdapter(operation),
        context=make_ctx(),
        record=make_rec(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    assert client.tool_calls().count(tool) == 1


# ==========================================================================
# calendar.create (NEVER_REINVOKE) / calendar.update / calendar.delete
# (SAFE_TO_REINVOKE) -- no independent read-back for any of the three.
# ==========================================================================

CAL_CREATE_ID = UUID("4a000000-0000-5000-8000-000000000005")
CAL_UPDATE_ID = UUID("4a000000-0000-5000-8000-000000000006")
CAL_DELETE_ID = UUID("4a000000-0000-5000-8000-000000000007")


def calendar_adapter() -> CalendarAdapter:
    return CalendarAdapter(account_by_calendar={"nigel": "nigel-zoho"})


def cal_create_context() -> ActionContext:
    return make_context(
        action_id=CAL_CREATE_ID,
        operation=Operation.CALENDAR_CREATE,
        action_role=ActionRole.CALENDAR_MUTATION,
        intent_kind=IntentKind.SHOWING_CREATE,
        target=DerivedTarget("calendar", "nigel", True),
        arguments={"description": "Tour"},
        provider_account="nigel-zoho",
        appointment_slot=APPOINTMENT,
    )


def cal_create_record() -> OutboundActionRecord:
    return make_row(
        action_id=CAL_CREATE_ID,
        operation=Operation.CALENDAR_CREATE,
        action_role=ActionRole.CALENDAR_MUTATION,
        intent_kind=IntentKind.SHOWING_CREATE,
        arguments={"calendar_id": "nigel", "description": "Tour"},
        provider_account="nigel-zoho",
        appointment_slot=APPOINTMENT,
    )


def cal_update_context() -> ActionContext:
    return make_context(
        action_id=CAL_UPDATE_ID,
        operation=Operation.CALENDAR_UPDATE,
        action_role=ActionRole.CALENDAR_MUTATION,
        intent_kind=IntentKind.SHOWING_UPDATE,
        target=DerivedTarget("calendar", "nigel", True),
        arguments={"description": "Tour, moved"},
        provider_account="nigel-zoho",
        appointment_slot=APPOINTMENT,
        calendar_event_uid="existing-event",
        calendar_event_url="https://calendar.local/events/existing-event.ics",
        calendar_event_etag='"etag-1"',
    )


def cal_update_record() -> OutboundActionRecord:
    return make_row(
        action_id=CAL_UPDATE_ID,
        operation=Operation.CALENDAR_UPDATE,
        action_role=ActionRole.CALENDAR_MUTATION,
        intent_kind=IntentKind.SHOWING_UPDATE,
        arguments={"calendar_id": "nigel", "description": "Tour, moved"},
        provider_account="nigel-zoho",
        appointment_slot=APPOINTMENT,
    )


def cal_delete_context() -> ActionContext:
    return make_context(
        action_id=CAL_DELETE_ID,
        operation=Operation.CALENDAR_DELETE,
        action_role=ActionRole.CALENDAR_MUTATION,
        intent_kind=IntentKind.SHOWING_DELETE,
        target=DerivedTarget("calendar", "nigel", True),
        arguments={},
        provider_account="nigel-zoho",
        calendar_event_uid="existing-event",
        calendar_event_url="https://calendar.local/events/existing-event.ics",
        calendar_event_etag='"etag-1"',
    )


def cal_delete_record() -> OutboundActionRecord:
    return make_row(
        action_id=CAL_DELETE_ID,
        operation=Operation.CALENDAR_DELETE,
        action_role=ActionRole.CALENDAR_MUTATION,
        intent_kind=IntentKind.SHOWING_DELETE,
        arguments={"calendar_id": "nigel"},
        provider_account="nigel-zoho",
    )


CALENDAR_CASES = [
    pytest.param(
        Operation.CALENDAR_CREATE,
        "calendar_create_event",
        CAL_CREATE_ID,
        cal_create_context,
        cal_create_record,
        "**Event Created**\nUID: created-event\nURL: https://calendar.local/events/created-event.ics",
        id="create",
    ),
    pytest.param(
        Operation.CALENDAR_UPDATE,
        "calendar_update_event",
        CAL_UPDATE_ID,
        cal_update_context,
        cal_update_record,
        "**Event Updated**\nUID: existing-event",
        id="update",
    ),
    pytest.param(
        Operation.CALENDAR_DELETE,
        "calendar_delete_event",
        CAL_DELETE_ID,
        cal_delete_context,
        cal_delete_record,
        "**Event Deleted**\nURL: https://calendar.local/events/existing-event.ics",
        id="delete",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "tool", "action_id", "make_ctx", "make_rec", "success_text"), CALENDAR_CASES)
async def test_calendar_success_completes_with_receipt_and_no_warning(operation, tool, action_id, make_ctx, make_rec, success_text):
    client = ScriptThenRepeatClient(
        pending(),
        McpCallResult(
            structured_content={
                "status": "completed",
                "request_id": "req-matrix-1",
                "result": {
                    "tool_name": tool,
                    "structured_content": {
                        "status": "success",
                        "data": {"content": [{"type": "text", "text": success_text}]},
                    },
                },
            }
        ),
    )
    result, store, _clock, staff_warning = await run_case(
        action_id=action_id,
        operation=operation,
        adapter=calendar_adapter(),
        context=make_ctx(),
        record=make_rec(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert client.tool_calls() == [tool, "request_status"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "tool", "action_id", "make_ctx", "make_rec", "success_text"), CALENDAR_CASES)
async def test_calendar_definitive_rejection_ends_definitive_failed_with_one_warning(
    operation, tool, action_id, make_ctx, make_rec, success_text
):
    del success_text
    client = ScriptThenRepeatClient(McpCallResult(is_error=True, text=f"Error executing {tool}: calendar account is not shared with this bot"))
    result, store, _clock, staff_warning = await run_case(
        action_id=action_id,
        operation=operation,
        adapter=calendar_adapter(),
        context=make_ctx(),
        record=make_rec(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert len(staff_warning.calls) == 1
    assert client.tool_calls() == [tool]


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "tool", "action_id", "make_ctx", "make_rec", "success_text"), CALENDAR_CASES)
async def test_calendar_ambiguous_forever_reaches_ceiling_definitive_failed_one_warning(
    operation, tool, action_id, make_ctx, make_rec, success_text
):
    del success_text
    client = ScriptThenRepeatClient(pending())
    result, store, clock, staff_warning = await run_case(
        action_id=action_id,
        operation=operation,
        adapter=calendar_adapter(),
        context=make_ctx(),
        record=make_rec(),
        provider_client=client,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    assert client.tool_calls().count(tool) == 1


# ==========================================================================
# tenantcloud.* -- SAFE_TO_REINVOKE via the facade's own idempotency
# precheck + at-most-one write + readback. Only the facade is faked -- the
# adapter never touches the MCP client (del client) -- see tenantcloud.py's
# module docstring.
# ==========================================================================

TC_MESSAGE_ID = UUID("5a000000-0000-5000-8000-000000000008")
TC_LEAD_ID = UUID("5a000000-0000-5000-8000-000000000009")
TC_MAINT_CREATE_ID = UUID("5a000000-0000-5000-8000-00000000000a")
TC_MAINT_STATUS_ID = UUID("5a000000-0000-5000-8000-00000000000b")


def tc_message_context() -> ActionContext:
    return make_context(
        action_id=TC_MESSAGE_ID,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        action_role=ActionRole.PROSPECT_REPLY,
        intent_kind=IntentKind.INQUIRY_REPLY,
        target=DerivedTarget("tenantcloud_thread", "555", True),
        arguments={"text": "Friday at 10:30 works. — Nigel"},
        provider_account="tenantcloud",
        canonical_context={"identity_version": "v1"},
    )


def tc_message_record() -> OutboundActionRecord:
    return make_row(
        action_id=TC_MESSAGE_ID,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        action_role=ActionRole.PROSPECT_REPLY,
        intent_kind=IntentKind.INQUIRY_REPLY,
        arguments={"thread_id": 555, "text": "Friday at 10:30 works. — Nigel"},
        provider_account="tenantcloud",
    )


def tc_lead_context() -> ActionContext:
    return make_context(
        action_id=TC_LEAD_ID,
        operation=Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        action_role=ActionRole.PROVIDER_MUTATION,
        intent_kind=IntentKind.TENANTCLOUD_LEAD_STATUS,
        target=DerivedTarget("tenantcloud_lead", "6001", True),
        arguments={"status": "working"},
        provider_account="tenantcloud",
        canonical_context={"identity_version": "v1"},
    )


def tc_lead_record() -> OutboundActionRecord:
    return make_row(
        action_id=TC_LEAD_ID,
        operation=Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        action_role=ActionRole.PROVIDER_MUTATION,
        intent_kind=IntentKind.TENANTCLOUD_LEAD_STATUS,
        arguments={"lead_id": 6001, "status": "working"},
        provider_account="tenantcloud",
    )


def tc_maint_create_context() -> ActionContext:
    return make_context(
        action_id=TC_MAINT_CREATE_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_CREATE,
        action_role=ActionRole.PROVIDER_MUTATION,
        intent_kind=IntentKind.TENANTCLOUD_MAINTENANCE_CREATE,
        target=DerivedTarget("tenantcloud_property_unit", "property:12:unit:34", True),
        arguments={
            "category_id": 57,
            "title": "Kitchen leak",
            "priority": "normal",
            "initiated_at": "2026-08-04",
            "text": "Sink leaking under cabinet",
            "entry_allowed": False,
            "available_on": None,
        },
        provider_account="tenantcloud",
        canonical_context={"identity_version": "v1", "provider_ids": {"property_id": "12", "unit_id": "34"}},
    )


def tc_maint_create_record() -> OutboundActionRecord:
    return make_row(
        action_id=TC_MAINT_CREATE_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_CREATE,
        action_role=ActionRole.PROVIDER_MUTATION,
        intent_kind=IntentKind.TENANTCLOUD_MAINTENANCE_CREATE,
        arguments={
            "property_id": 12,
            "unit_id": 34,
            "category_id": 57,
            "title": "Kitchen leak",
            "priority": "normal",
            "initiated_at": "2026-08-04",
            "text": "Sink leaking under cabinet",
            "entry_allowed": False,
            "available_on": None,
        },
        provider_account="tenantcloud",
    )


def tc_maint_status_context() -> ActionContext:
    return make_context(
        action_id=TC_MAINT_STATUS_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE,
        action_role=ActionRole.PROVIDER_MUTATION,
        intent_kind=IntentKind.TENANTCLOUD_MAINTENANCE_STATUS,
        target=DerivedTarget("tenantcloud_maintenance_request", "81", True),
        arguments={"status": 2},
        provider_account="tenantcloud",
        canonical_context={"identity_version": "v1"},
    )


def tc_maint_status_record() -> OutboundActionRecord:
    return make_row(
        action_id=TC_MAINT_STATUS_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE,
        action_role=ActionRole.PROVIDER_MUTATION,
        intent_kind=IntentKind.TENANTCLOUD_MAINTENANCE_STATUS,
        arguments={"request_id": 81, "status": 2},
        provider_account="tenantcloud",
    )


def mutation_observation(**overrides: Any) -> FakeMutationObservation:
    defaults = dict(
        target_reference="thread:555",
        provider_object_id="9001",
        canonical_observed_state={"thread_id": "555"},
    )
    defaults.update(overrides)
    return FakeMutationObservation(**defaults)


async def run_tenantcloud_case(
    *, action_id: UUID, operation: Operation, context: ActionContext, record: OutboundActionRecord, facade: FakeTenantCloudMutations
):
    adapter = TenantCloudAdapter(mutations_factory=lambda: facade)
    return await run_case(
        action_id=action_id,
        operation=operation,
        adapter=adapter,
        context=context,
        record=record,
        provider_client=None,
    )


@pytest.mark.asyncio
async def test_tenantcloud_message_send_success_completes_with_no_warning():
    facade = FakeTenantCloudMutations()
    facade.send_message_result = FakeMutationExecution(FakeMutationResult(TC_ACCEPTED), mutation_observation(), None)
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MESSAGE_ID,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        context=tc_message_context(),
        record=tc_message_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert [call[0] for call in facade.calls if call[0] in {"send_message"}] == ["send_message"]


@pytest.mark.asyncio
async def test_tenantcloud_message_send_definitive_rejection_ends_definitive_failed_with_one_warning():
    facade = FakeTenantCloudMutations()
    facade.send_message_result = FakeMutationExecution(
        FakeMutationResult(TC_DEFINITIVE_NON_ACCEPTANCE, error_code="validation_error", status=422),
        None,
        "validation_error",
    )
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MESSAGE_ID,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        context=tc_message_context(),
        record=tc_message_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert store.current.detail_code == "tenantcloud_provider_rejected_http_422"
    assert len(staff_warning.calls) == 1
    assert [call[0] for call in facade.calls].count("send_message") == 1


@pytest.mark.asyncio
async def test_tenantcloud_message_send_ambiguous_forever_settled_by_readback_completes_with_no_warning():
    facade = FakeTenantCloudMutations()
    # First reconcile_message (the pre-write precheck): no match, write goes
    # ahead. The write itself lands unverified (ambiguous). Every reconcile()
    # call after that is the facade's own idempotent read-back -- eventually
    # it finds the write and settles.
    facade.reconcile_message_results = [
        FakeReconciliationResult(TC_UNKNOWN, None, "no_match"),
        FakeReconciliationResult(TC_UNKNOWN, None, "no_match"),
    ]
    facade.reconcile_message_result = FakeReconciliationResult(
        TC_ACCEPTED,
        mutation_observation(provider_object_id="9002"),
        None,
    )
    facade.send_message_result = FakeMutationExecution(FakeMutationResult(TC_UNKNOWN), None, "unverified")
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MESSAGE_ID,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        context=tc_message_context(),
        record=tc_message_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert [call[0] for call in facade.calls].count("send_message") == 1


@pytest.mark.asyncio
async def test_tenantcloud_message_send_ambiguous_forever_never_found_reaches_ceiling_definitive_failed_one_warning():
    facade = FakeTenantCloudMutations()
    # Default reconcile_message_result stays TC_UNKNOWN/no_match forever.
    facade.send_message_result = FakeMutationExecution(FakeMutationResult(TC_UNKNOWN), None, "unverified")
    result, store, clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MESSAGE_ID,
        operation=Operation.TENANTCLOUD_MESSAGE_SEND,
        context=tc_message_context(),
        record=tc_message_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    assert [call[0] for call in facade.calls].count("send_message") == 1


@pytest.mark.asyncio
async def test_tenantcloud_lead_status_success_completes_with_no_warning():

    facade = FakeTenantCloudMutations()
    facade.mark_lead_working_result = FakeMutationExecution(
        FakeMutationResult(TC_ACCEPTED),
        mutation_observation(target_reference="lead:6001", provider_object_id="6001"),
        None,
    )
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_LEAD_ID,
        operation=Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        context=tc_lead_context(),
        record=tc_lead_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert [call[0] for call in facade.calls].count("mark_lead_working") == 1


@pytest.mark.asyncio
async def test_tenantcloud_lead_status_definitive_rejection_ends_definitive_failed_with_one_warning():
    facade = FakeTenantCloudMutations()
    facade.mark_lead_working_result = FakeMutationExecution(
        FakeMutationResult(TC_DEFINITIVE_NON_ACCEPTANCE, error_code="validation_error", status=422),
        None,
        "validation_error",
    )
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_LEAD_ID,
        operation=Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        context=tc_lead_context(),
        record=tc_lead_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert store.current.detail_code == "tenantcloud_provider_rejected_http_422"
    assert len(staff_warning.calls) == 1
    assert [call[0] for call in facade.calls].count("mark_lead_working") == 1


@pytest.mark.asyncio
async def test_tenantcloud_lead_status_ambiguous_forever_reinvokes_safely_then_reaches_ceiling():
    """Unlike message.send/maintenance.create (whose default ambiguous
    outcome is genuinely AMBIGUOUS -- reconcile() loops without ever
    touching invoke() again), lead.status.update's "not yet applied"
    reconciliation is DEFINITIVE_NON_ACCEPTANCE with retryable=True
    (adapters/tenantcloud.py's ``_from_reconciliation``). That specific
    shape routes the row to RETRY_READY -> resume() -> a genuine re-invoke
    of ``mark_lead_working`` -- exactly what SAFE_TO_REINVOKE is FOR
    (idempotency_policy.py: "re-invoke allowed, up to the ceiling"), backed
    by the facade's own idempotent precheck (reconcile_lead_status) ahead of
    every write. This is the one operation in the matrix where a bare
    invoke-count-of-1 assertion would be wrong."""
    facade = FakeTenantCloudMutations()
    # Default reconcile_lead_status_result stays "not yet applied" (retryable
    # DEFINITIVE_NON_ACCEPTANCE) forever -- never settles into ACCEPTED.
    facade.mark_lead_working_result = FakeMutationExecution(FakeMutationResult(TC_UNKNOWN), None, "unverified")
    result, store, clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_LEAD_ID,
        operation=Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        context=tc_lead_context(),
        record=tc_lead_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    # SAFE_TO_REINVOKE really did reinvoke -- every one preceded by the
    # facade's own precheck, never a blind resend -- but bounded to what
    # retry_policy's 5s-to-300s-capped backoff reaches in one hour (~15-20
    # reschedules total, so well under half that many real writes), never
    # the un-doubled ~360 a flat attempt_count-keyed backoff produced before
    # service.py's _schedule() was fixed to use elapsed_step_backoff_seconds
    # for a Restate-flagged operation.
    reinvokes = [call[0] for call in facade.calls].count("mark_lead_working")
    assert 1 < reinvokes < 30, reinvokes


@pytest.mark.asyncio
async def test_tenantcloud_maintenance_create_success_completes_with_no_warning():

    facade = FakeTenantCloudMutations()
    facade.create_maintenance_request_result = FakeMutationExecution(
        FakeMutationResult(TC_ACCEPTED),
        mutation_observation(target_reference="property:12:unit:34", provider_object_id="7001"),
        None,
    )
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MAINT_CREATE_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_CREATE,
        context=tc_maint_create_context(),
        record=tc_maint_create_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert [call[0] for call in facade.calls].count("create_maintenance_request") == 1


@pytest.mark.asyncio
async def test_tenantcloud_maintenance_create_definitive_rejection_ends_definitive_failed_with_one_warning():
    facade = FakeTenantCloudMutations()
    facade.create_maintenance_request_result = FakeMutationExecution(
        FakeMutationResult(TC_DEFINITIVE_NON_ACCEPTANCE, error_code="validation_error", status=422),
        None,
        "validation_error",
    )
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MAINT_CREATE_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_CREATE,
        context=tc_maint_create_context(),
        record=tc_maint_create_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert store.current.detail_code == "tenantcloud_provider_rejected_http_422"
    assert len(staff_warning.calls) == 1
    assert [call[0] for call in facade.calls].count("create_maintenance_request") == 1


@pytest.mark.asyncio
async def test_tenantcloud_maintenance_create_ambiguous_forever_never_found_reaches_ceiling_definitive_failed_one_warning():
    facade = FakeTenantCloudMutations()
    facade.create_maintenance_request_result = FakeMutationExecution(FakeMutationResult(TC_UNKNOWN), None, "unverified")
    result, store, clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MAINT_CREATE_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_CREATE,
        context=tc_maint_create_context(),
        record=tc_maint_create_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    assert [call[0] for call in facade.calls].count("create_maintenance_request") == 1


@pytest.mark.asyncio
async def test_tenantcloud_maintenance_status_success_completes_with_no_warning():

    facade = FakeTenantCloudMutations()
    facade.update_maintenance_status_result = FakeMutationExecution(
        FakeMutationResult(TC_ACCEPTED),
        mutation_observation(target_reference="request:81", provider_object_id="81"),
        None,
    )
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MAINT_STATUS_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE,
        context=tc_maint_status_context(),
        record=tc_maint_status_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.COMPLETE
    assert store.current.state is ActionState.COMPLETED
    assert staff_warning.calls == []
    assert [call[0] for call in facade.calls].count("update_maintenance_status") == 1


@pytest.mark.asyncio
async def test_tenantcloud_maintenance_status_definitive_rejection_ends_definitive_failed_with_one_warning():
    facade = FakeTenantCloudMutations()
    facade.update_maintenance_status_result = FakeMutationExecution(
        FakeMutationResult(TC_DEFINITIVE_NON_ACCEPTANCE, error_code="validation_error", status=422),
        None,
        "validation_error",
    )
    result, store, _clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MAINT_STATUS_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE,
        context=tc_maint_status_context(),
        record=tc_maint_status_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert store.current.detail_code == "tenantcloud_provider_rejected_http_422"
    assert len(staff_warning.calls) == 1
    assert [call[0] for call in facade.calls].count("update_maintenance_status") == 1


@pytest.mark.asyncio
async def test_tenantcloud_maintenance_status_ambiguous_forever_reinvokes_safely_then_reaches_ceiling():
    """Same shape as lead.status.update's sibling test: maintenance.status
    .update's "not yet applied" reconciliation is also DEFINITIVE_NON_
    ACCEPTANCE/retryable=True, so it genuinely reinvokes (SAFE_TO_REINVOKE),
    each time behind the facade's own precheck."""
    facade = FakeTenantCloudMutations()
    facade.update_maintenance_status_result = FakeMutationExecution(FakeMutationResult(TC_UNKNOWN), None, "unverified")
    result, store, clock, staff_warning = await run_tenantcloud_case(
        action_id=TC_MAINT_STATUS_ID,
        operation=Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE,
        context=tc_maint_status_context(),
        record=tc_maint_status_record(),
        facade=facade,
    )
    assert result.phase is DeliveryPhase.TERMINAL
    assert store.current.state is ActionState.DEFINITIVE_FAILED
    assert (clock.now - CREATED_AT).total_seconds() >= RETRY_CEILING_SECONDS
    assert len(staff_warning.calls) == 1
    # Same bound as lead.status.update's sibling assertion: real backoff
    # growth, not the flat ~360-reinvoke cadence a counter-keyed schedule
    # produced before the fix.
    reinvokes = [call[0] for call in facade.calls].count("update_maintenance_status")
    assert 1 < reinvokes < 30, reinvokes
