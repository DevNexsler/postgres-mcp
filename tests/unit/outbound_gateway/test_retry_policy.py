from __future__ import annotations

from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway import retry_policy
from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.adapters.base import initial_observation
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.provider_client import McpCallResult
from postgres_mcp.outbound_gateway.retry_config import RetryPolicy

POLICY = RetryPolicy(jitter_ratio=0)


def test_should_wait_for_context_reload_only_inside_the_deadline() -> None:
    assert retry_policy.should_wait_for_context_reload("persisted_context_unavailable", 0, POLICY)
    assert retry_policy.should_wait_for_context_reload("persisted_context_mismatch", 3000, POLICY)
    assert not retry_policy.should_wait_for_context_reload("persisted_context_unavailable", 3600, POLICY)
    assert not retry_policy.should_wait_for_context_reload("some_other_detail", 0, POLICY)


def test_decide_stops_a_definitive_rejection_at_once_and_owes_an_escalation() -> None:
    decision = retry_policy.decide(policy=POLICY, deliverable_elapsed_seconds=1.0, is_definitive=True)
    assert decision.outcome is retry_policy.RetryOutcome.STOP_DEFINITIVE
    assert decision.warn_staff is True


def test_decide_retries_on_the_policy_backoff_inside_the_deadline() -> None:
    decision = retry_policy.decide(policy=POLICY, deliverable_elapsed_seconds=15.0, is_definitive=False)
    assert decision.outcome is retry_policy.RetryOutcome.RETRY
    assert decision.wait_seconds == POLICY.backoff_seconds(15.0) == 20


def test_decide_stops_at_the_deadline() -> None:
    decision = retry_policy.decide(policy=POLICY, deliverable_elapsed_seconds=3600.0, is_definitive=False)
    assert decision.outcome is retry_policy.RetryOutcome.STOP_DEFINITIVE
    assert decision.warn_staff is True


def test_decide_owes_no_escalation_when_the_policy_does_not_escalate() -> None:
    quiet = RetryPolicy(escalate=False)
    assert retry_policy.decide(policy=quiet, deliverable_elapsed_seconds=0, is_definitive=True).warn_staff is False
    assert retry_policy.decide(policy=quiet, deliverable_elapsed_seconds=3600, is_definitive=False).warn_staff is False


def test_is_definitive_rejection_reuses_the_cliq_provider_rejected_request_signal() -> None:
    """PR #59 (2026-09-28): a Cliq target Agent Email's synchronous argument
    validation refuses (a CT_* id given to cliq_channel_bot_post) is
    DEFINITIVE_NON_ACCEPTANCE/provider_rejected_request/retryable=False.
    retry_policy must recognize this exact adapter-produced observation as
    definitive without re-deriving it from the raw MCP result itself."""
    result = McpCallResult(
        is_error=True,
        text="Error executing cliq_channel_bot_post: cliq_channel_bot_post requires channel_unique_name, "
        "not a CT_* chat id",
    )
    observation = initial_observation(result, effect_call=True)
    assert observation is not None
    assert observation.detail_code == "provider_rejected_request"

    assert retry_policy.is_definitive_rejection(observation) is True


def test_is_definitive_rejection_is_false_for_an_ambiguous_observation() -> None:
    observation = ProviderObservation(ProviderDisposition.AMBIGUOUS, "provider_mcp_error")
    assert retry_policy.is_definitive_rejection(observation) is False


def test_is_definitive_rejection_respects_an_adapter_marking_it_retryable() -> None:
    # No adapter does this today, but the policy must not hardcode "definitive
    # implies stop" independent of the adapter's own retryable bit.
    observation = ProviderObservation(
        ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
        "provider_auth_rejected",
        retryable=True,
    )
    assert retry_policy.is_definitive_rejection(observation) is False


def test_decide_for_observation_stops_on_the_cliq_definitive_rejection() -> None:
    result = McpCallResult(is_error=True, text="cliq_channel_bot_post requires channel_unique_name")
    observation = initial_observation(result, effect_call=True)
    assert observation is not None

    decision = retry_policy.decide_for_observation(policy=POLICY, deliverable_elapsed_seconds=1.0, observation=observation)

    assert decision.outcome is retry_policy.RetryOutcome.STOP_DEFINITIVE
    assert decision.warn_staff is True


def test_decide_for_observation_retries_an_ambiguous_observation() -> None:
    observation = ProviderObservation(ProviderDisposition.AMBIGUOUS, "provider_queue_timeout")

    decision = retry_policy.decide_for_observation(policy=POLICY, deliverable_elapsed_seconds=15.0, observation=observation)

    assert decision.outcome is retry_policy.RetryOutcome.RETRY
    assert decision.wait_seconds == POLICY.backoff_seconds(15.0)


@pytest.mark.asyncio
async def test_noop_staff_warning_port_warns_at_most_once_per_action(caplog: pytest.LogCaptureFixture) -> None:
    port = retry_policy.NoopStaffWarningPort()
    action_id = UUID("11111111-1111-1111-1111-111111111111")
    with caplog.at_level("WARNING"):
        await port.warn_once(
            action_id,
            None,
            "retry_budget_exhausted",
            wakeup_event_id=27000,
            operation=Operation.QUO_SMS_SEND,
            recipient="+15555550100",
        )
        await port.warn_once(
            action_id,
            None,
            "retry_budget_exhausted",
            wakeup_event_id=27000,
            operation=Operation.QUO_SMS_SEND,
            recipient="+15555550100",
        )
    warnings = [record for record in caplog.records if "no Cliq warning was sent" in record.getMessage()]
    assert len(warnings) == 1
