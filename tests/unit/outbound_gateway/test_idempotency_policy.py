from __future__ import annotations

import pytest

from postgres_mcp.outbound_gateway.idempotency_policy import OPERATION_REINVOKE_SAFETY
from postgres_mcp.outbound_gateway.idempotency_policy import ReinvokeSafety
from postgres_mcp.outbound_gateway.idempotency_policy import must_never_resend_when_ambiguous
from postgres_mcp.outbound_gateway.idempotency_policy import reinvoke_safety
from postgres_mcp.outbound_gateway.models import Operation


def test_every_operation_has_a_reinvoke_safety_classification() -> None:
    """A new Operation added to models.py without a row here would silently
    default to "safe" the moment it is routed through Restate -- this must
    fail loud instead."""
    assert set(OPERATION_REINVOKE_SAFETY) == set(Operation)


@pytest.mark.parametrize(
    "operation,expected",
    [
        # No provider idempotency key; settled by an independent read-back.
        (Operation.QUO_SMS_SEND, ReinvokeSafety.CONFIRM_BY_READBACK),
        (Operation.EMAIL_SEND, ReinvokeSafety.CONFIRM_BY_READBACK),
        # Provider-native idempotency_key / duplicate_suppressed.
        (Operation.CLIQ_CHAT_POST, ReinvokeSafety.SAFE_TO_REINVOKE),
        (Operation.CLIQ_CHANNEL_POST, ReinvokeSafety.SAFE_TO_REINVOKE),
        # No independent read-back and no verified provider-side dedup.
        (Operation.CALENDAR_CREATE, ReinvokeSafety.NEVER_REINVOKE),
        # etag-guarded: a stale-etag resend fails closed, never double-applies.
        (Operation.CALENDAR_UPDATE, ReinvokeSafety.SAFE_TO_REINVOKE),
        (Operation.CALENDAR_DELETE, ReinvokeSafety.SAFE_TO_REINVOKE),
        # The shared facade does its own idempotency pre-check + readback.
        (Operation.TENANTCLOUD_MESSAGE_SEND, ReinvokeSafety.SAFE_TO_REINVOKE),
        (Operation.TENANTCLOUD_LEAD_STATUS_UPDATE, ReinvokeSafety.SAFE_TO_REINVOKE),
        (Operation.TENANTCLOUD_MAINTENANCE_CREATE, ReinvokeSafety.SAFE_TO_REINVOKE),
        (Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE, ReinvokeSafety.SAFE_TO_REINVOKE),
    ],
)
def test_reinvoke_safety_matches_each_adapters_actual_confirmability(operation: Operation, expected: ReinvokeSafety) -> None:
    assert reinvoke_safety(operation) is expected


def test_the_owners_top_concern_never_double_texting_a_customer() -> None:
    """quo.sms.send and email.send are the two customer-facing sends with no
    provider-side dedup at all (see idempotency_policy.py's module
    docstring). Neither may ever be classified SAFE_TO_REINVOKE."""
    assert reinvoke_safety(Operation.QUO_SMS_SEND) is not ReinvokeSafety.SAFE_TO_REINVOKE
    assert reinvoke_safety(Operation.EMAIL_SEND) is not ReinvokeSafety.SAFE_TO_REINVOKE


def test_calendar_create_must_never_resend_when_ambiguous() -> None:
    assert must_never_resend_when_ambiguous(Operation.CALENDAR_CREATE) is True


@pytest.mark.parametrize(
    "operation",
    [op for op in Operation if op is not Operation.CALENDAR_CREATE],
)
def test_only_calendar_create_is_never_reinvoke(operation: Operation) -> None:
    assert must_never_resend_when_ambiguous(operation) is False
