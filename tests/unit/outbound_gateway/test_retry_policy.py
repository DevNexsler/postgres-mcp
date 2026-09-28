from __future__ import annotations

from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway import retry_policy
from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.base import ProviderObservation
from postgres_mcp.outbound_gateway.adapters.base import initial_observation
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.provider_client import McpCallResult


def test_backoff_doubles_from_five_seconds_and_caps_at_five_minutes() -> None:
    assert retry_policy.backoff_seconds(1) == 5
    assert retry_policy.backoff_seconds(2) == 10
    assert retry_policy.backoff_seconds(3) == 20
    assert retry_policy.backoff_seconds(4) == 40
    assert retry_policy.backoff_seconds(5) == 80
    assert retry_policy.backoff_seconds(6) == 160
    assert retry_policy.backoff_seconds(7) == 300
    assert retry_policy.backoff_seconds(20) == 300


def test_backoff_treats_non_positive_attempt_count_as_the_first_attempt() -> None:
    assert retry_policy.backoff_seconds(0) == 5
    assert retry_policy.backoff_seconds(-3) == 5


def test_ceiling_exceeded_is_a_one_hour_default() -> None:
    assert retry_policy.RETRY_CEILING_SECONDS == 3600
    assert not retry_policy.ceiling_exceeded(3599)
    assert retry_policy.ceiling_exceeded(3600)
    assert retry_policy.ceiling_exceeded(7200)


def test_context_reload_detail_waits_flat_five_minutes_inside_the_ceiling() -> None:
    assert retry_policy.should_wait_for_context_reload("persisted_context_unavailable", 0)
    assert retry_policy.should_wait_for_context_reload("persisted_context_mismatch", 3000)
    assert not retry_policy.should_wait_for_context_reload("persisted_context_unavailable", 3600)
    assert not retry_policy.should_wait_for_context_reload("some_other_detail", 0)


def test_decide_stops_immediately_on_a_definitive_rejection_regardless_of_ceiling() -> None:
    decision = retry_policy.decide(
        attempt_count=1,
        elapsed_seconds=1.0,
        detail_code="provider_rejected_http_422",
        is_definitive=True,
    )
    assert decision.outcome is retry_policy.RetryOutcome.STOP_DEFINITIVE
    assert decision.warn_staff is True
    assert decision.wait_seconds == 0


def test_decide_waits_on_a_context_reload_detail_before_the_ceiling() -> None:
    decision = retry_policy.decide(
        attempt_count=1,
        elapsed_seconds=10.0,
        detail_code="persisted_context_unavailable",
        is_definitive=False,
    )
    assert decision.outcome is retry_policy.RetryOutcome.RETRY
    assert decision.wait_seconds == retry_policy.CONTEXT_RELOAD_WAIT_SECONDS
    assert decision.warn_staff is False


def test_decide_uses_exponential_backoff_for_an_ordinary_ambiguous_outcome() -> None:
    decision = retry_policy.decide(
        attempt_count=3,
        elapsed_seconds=30.0,
        detail_code="provider_queue_timeout",
        is_definitive=False,
    )
    assert decision.outcome is retry_policy.RetryOutcome.RETRY
    assert decision.wait_seconds == retry_policy.backoff_seconds(3)


def test_decide_stops_and_warns_once_the_ceiling_is_exceeded_even_mid_wait() -> None:
    decision = retry_policy.decide(
        attempt_count=9,
        elapsed_seconds=3601.0,
        detail_code="persisted_context_unavailable",
        is_definitive=False,
    )
    assert decision.outcome is retry_policy.RetryOutcome.STOP_DEFINITIVE
    assert decision.warn_staff is True


def test_decide_respects_a_custom_ceiling() -> None:
    decision = retry_policy.decide(
        attempt_count=1,
        elapsed_seconds=100.0,
        detail_code="provider_queue_timeout",
        is_definitive=False,
        ceiling_seconds=90,
    )
    assert decision.outcome is retry_policy.RetryOutcome.STOP_DEFINITIVE
    assert decision.warn_staff is True


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

    decision = retry_policy.decide_for_observation(
        attempt_count=1,
        elapsed_seconds=1.0,
        observation=observation,
    )

    assert decision.outcome is retry_policy.RetryOutcome.STOP_DEFINITIVE
    assert decision.warn_staff is True


def test_decide_for_observation_retries_an_ambiguous_observation() -> None:
    observation = ProviderObservation(ProviderDisposition.AMBIGUOUS, "provider_queue_timeout")

    decision = retry_policy.decide_for_observation(
        attempt_count=2,
        elapsed_seconds=15.0,
        observation=observation,
    )

    assert decision.outcome is retry_policy.RetryOutcome.RETRY
    assert decision.wait_seconds == retry_policy.backoff_seconds(2)


@pytest.mark.asyncio
async def test_noop_staff_warning_port_warns_at_most_once_per_action(caplog: pytest.LogCaptureFixture) -> None:
    port = retry_policy.NoopStaffWarningPort()
    action_id = UUID("11111111-1111-1111-1111-111111111111")
    kwargs = dict(wakeup_event_id=27000, operation=Operation.QUO_SMS_SEND, recipient="+15555550100")
    with caplog.at_level("WARNING"):
        await port.warn_once(action_id, None, "retry_budget_exhausted", **kwargs)
        await port.warn_once(action_id, None, "retry_budget_exhausted", **kwargs)
    warnings = [record for record in caplog.records if "no Cliq warning was sent" in record.getMessage()]
    assert len(warnings) == 1
