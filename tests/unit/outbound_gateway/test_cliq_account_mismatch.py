"""Wake 28166: a Dan-only chat is not a Nigel post target."""

from unittest.mock import AsyncMock

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.adapters.cliq import CliqAdapter
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.provider_client import McpCallResult
from postgres_mcp.outbound_gateway.record import action_result
from postgres_mcp.outbound_gateway.service import OutboundActionService

from .test_adapters import FakeClient
from .test_adapters import context
from .test_service import NOW
from .test_service import FakeStore
from .test_service import evidence
from .test_service import row

REFUSAL = (
    "Cliq message was NOT sent to chat CT_1424657680898345423_721156495: "
    "local history has this chat only under dan-zoho, and the Cliq write identity "
    'for upstream "nigel_cliq" (account nigel-zoho) does not participate. '
    "A conversation id from another account is not a post target. The provider was not called."
)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_ref", [True, False])
async def test_poll_names_account_mismatch_and_preserves_non_acceptance(include_ref):
    payload = {"status": "failed", "category": "permanent_upstream_error", "retryable": False, "message": REFUSAL}
    if include_ref:
        payload["request_id"] = "job-28166"
    adapter = CliqAdapter(Operation.CLIQ_CHANNEL_POST)
    result = await adapter.poll(
        FakeClient(McpCallResult(structured_content=payload)),
        ProviderObservation(ProviderDisposition.PENDING, "provider_pending", provider_request_ref="job-28166"),
    )
    assert result.detail_code == "cliq_chat_account_mismatch"
    assert result.disposition is ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE
    assert result.category == "provider_validation"
    assert result.retryable is False
    assert result.provider_request_ref == "job-28166"
    assert REFUSAL in result.evidence["provider_message"]


def test_status_from_persisted_code_explains_correction_without_raw_provider_text():
    result = action_result(
        row(
            ActionState.DEFINITIVE_FAILED,
            operation=Operation.CLIQ_CHANNEL_POST,
            detail_code="cliq_chat_account_mismatch",
            error_detail="cliq_chat_account_mismatch",
        )
    )
    assert "provider was not called" in result.detail
    assert "Do not retry" in result.detail
    assert "unique name" in result.detail
    assert "Nigel" in result.detail
    assert "you may still send it" not in result.detail


def test_unrelated_permanent_failure_keeps_existing_classification():
    result = CliqAdapter._parse(
        McpCallResult(
            structured_content={
                "status": "failed",
                "category": "permanent_upstream_error",
                "message": "Cliq rejected the message: chat is archived",
            }
        )
    )
    assert result.detail_code == "provider_permanent_upstream_error"
    assert result.category == "permanent_upstream_error"


@pytest.mark.asyncio
async def test_execute_and_later_status_keep_recovery_guidance():
    failed = McpCallResult(
        structured_content={
            "status": "failed",
            "category": "permanent_upstream_error",
            "message": REFUSAL,
            "request_id": "job-28166",
        }
    )
    chat_id = "CT_1424657680898345423_721156495"
    ctx = context(Operation.CLIQ_CHANNEL_POST, target=DerivedTarget("cliq_chat", chat_id, True))
    store = FakeStore(row(operation=ctx.operation, action_role=ctx.action_role))
    loader = AsyncMock()
    loader.load.return_value = ctx
    proof = AsyncMock()
    proof.load.return_value = evidence()
    client = FakeClient(McpCallResult(structured_content={"status": "pending", "request_id": "job-28166"}), failed)
    gateway = OutboundActionService(
        store=store,
        context_loader=loader,
        evidence_loader=proof,
        adapters={ctx.operation: CliqAdapter(ctx.operation)},
        provider_client=client,
        clock=lambda: NOW,
        lease_owner="gateway-test",
        sleep=AsyncMock(),
        traffic_mode="off",
    )
    first = await gateway.execute(
        parse_outbound_request(
            {
                "op": "execute",
                "wakeup_event_id": 7,
                "action_role": "internal_notification",
                "operation": "cliq.channel.post",
                "intent_kind": "lead_alert",
                "arguments": {"channel_or_chat_id": chat_id, "text": "Internal status"},
            }
        )
    )
    # FakeStore mirrors SQL: persists detail_code, not raw provider text.
    later = await gateway.status(store.current.action_id)
    assert first.detail_code == later.detail_code == "cliq_chat_account_mismatch"
    assert "Do not retry" in first.detail
    assert "Do not retry" in later.detail
    assert later.status.value == "failed"
    assert [call[1] for call in client.calls] == ["cliq_chat_post", "request_status"]
