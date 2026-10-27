"""The gateway's one retry configuration (retry_config.py)."""

from __future__ import annotations

import json
import logging

import pytest

from postgres_mcp.outbound_gateway import server
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.retry_config import RetryPolicy
from postgres_mcp.outbound_gateway.retry_config import RetryPolicyConfig
from postgres_mcp.outbound_gateway.retry_config import RetryPolicyConfigError

# What production sets today (docker-compose.yaml), plus the new keys.
PRODUCTION = {
    "OUTBOUND_RETRY_BASE_SECONDS": "5",
    "OUTBOUND_RETRY_MAX_SECONDS": "900",
    "OUTBOUND_MAX_ATTEMPTS": "5",
}


def test_unset_keys_take_the_documented_defaults() -> None:
    policy = RetryPolicyConfig.from_env({}).default
    assert policy == RetryPolicy(
        base_seconds=5,
        max_seconds=900,
        max_attempts=5,
        jitter_ratio=0.2,
        deadline_seconds=3600,
        staleness_ceiling_seconds=86400,
        escalate=True,
    )


def test_the_global_defaults_come_from_the_existing_settings_and_the_new_keys() -> None:
    config = RetryPolicyConfig.from_env(
        {
            "OUTBOUND_RETRY_BASE_SECONDS": "10",
            "OUTBOUND_RETRY_MAX_SECONDS": "600",
            "OUTBOUND_MAX_ATTEMPTS": "7",
            "OUTBOUND_RETRY_JITTER_RATIO": "0.1",
            "OUTBOUND_RETRY_DEADLINE_SECONDS": "1800",
            "OUTBOUND_RETRY_STALENESS_CEILING_SECONDS": "43200",
            "OUTBOUND_RETRY_ESCALATE": "true",
        }
    )
    assert config.default == RetryPolicy(10, 600, 7, 0.1, 1800, 43200, True)


@pytest.mark.parametrize("operation", list(Operation))
def test_an_operation_with_no_override_uses_the_global_defaults(operation: Operation) -> None:
    config = RetryPolicyConfig.from_env({**PRODUCTION, "OUTBOUND_RETRY_POLICY_JSON": '{"tenantcloud.message.send": {"base_seconds": 30}}'})
    if operation is Operation.TENANTCLOUD_MESSAGE_SEND:
        return
    assert config.for_operation(operation) is config.default


def test_an_operation_override_wins_and_inherits_every_field_it_does_not_name() -> None:
    config = RetryPolicyConfig.from_env(
        {
            **PRODUCTION,
            "OUTBOUND_RETRY_POLICY_JSON": json.dumps(
                {"tenantcloud.message.send": {"base_seconds": 30, "staleness_ceiling_seconds": 43200, "escalate": True}}
            ),
        }
    )
    policy = config.for_operation(Operation.TENANTCLOUD_MESSAGE_SEND)
    assert (policy.base_seconds, policy.staleness_ceiling_seconds) == (30, 43200)
    assert (policy.max_seconds, policy.max_attempts, policy.deadline_seconds) == (900, 5, 3600)


def test_a_provider_override_covers_its_operations_and_an_operation_key_beats_it() -> None:
    config = RetryPolicyConfig.from_env(
        {"OUTBOUND_RETRY_POLICY_JSON": json.dumps({"tenantcloud": {"max_seconds": 300}, "tenantcloud.lead.status.update": {"max_seconds": 600}})}
    )
    assert config.for_operation(Operation.TENANTCLOUD_MESSAGE_SEND).max_seconds == 300
    assert config.for_operation(Operation.TENANTCLOUD_MAINTENANCE_CREATE).max_seconds == 300
    assert config.for_operation(Operation.TENANTCLOUD_LEAD_STATUS_UPDATE).max_seconds == 600
    assert config.for_operation(Operation.QUO_SMS_SEND) is config.default


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"OUTBOUND_RETRY_BASE_SECONDS": "five"}, "OUTBOUND_RETRY_BASE_SECONDS must be an integer"),
        ({"OUTBOUND_RETRY_BASE_SECONDS": "0"}, "base_seconds must be at least 1"),
        ({"OUTBOUND_RETRY_MAX_SECONDS": "3"}, "max_seconds must be between base_seconds"),
        ({"OUTBOUND_RETRY_MAX_SECONDS": "7200"}, "max_seconds must be between base_seconds"),
        ({"OUTBOUND_MAX_ATTEMPTS": "0"}, "max_attempts must be between 1 and 100"),
        ({"OUTBOUND_RETRY_JITTER_RATIO": "1"}, "jitter_ratio must be at least 0 and below 1"),
        ({"OUTBOUND_RETRY_JITTER_RATIO": "nan"}, "jitter_ratio must be a number"),
        ({"OUTBOUND_RETRY_DEADLINE_SECONDS": "60"}, "deadline_seconds (60) must be at least max_seconds"),
        ({"OUTBOUND_RETRY_STALENESS_CEILING_SECONDS": "1800"}, "staleness_ceiling_seconds must be between"),
        ({"OUTBOUND_RETRY_STALENESS_CEILING_SECONDS": "999999"}, "staleness_ceiling_seconds must be between"),
        ({"OUTBOUND_RETRY_ESCALATE": "maybe"}, "OUTBOUND_RETRY_ESCALATE must be true or false"),
        ({"OUTBOUND_RETRY_POLICY_JSON": "{nope"}, "is not valid JSON"),
        ({"OUTBOUND_RETRY_POLICY_JSON": "[]"}, "must be a JSON object"),
        ({"OUTBOUND_RETRY_POLICY_JSON": '{"sms.send": {"base_seconds": 1}}'}, "is neither an operation"),
        ({"OUTBOUND_RETRY_POLICY_JSON": '{"quo.sms.send": {}}'}, "must be a non-empty JSON object"),
        ({"OUTBOUND_RETRY_POLICY_JSON": '{"quo.sms.send": {"retries": 3}}'}, "unknown fields ['retries']"),
        ({"OUTBOUND_RETRY_POLICY_JSON": '{"quo.sms.send": {"base_seconds": "5"}}'}, "base_seconds must be an integer"),
        ({"OUTBOUND_RETRY_POLICY_JSON": '{"quo.sms.send": {"escalate": "no"}}'}, "escalate must be true or false"),
        ({"OUTBOUND_RETRY_POLICY_JSON": '{"quo.sms.send": {"max_seconds": 4000}}'}, "max_seconds must be between"),
    ],
)
def test_invalid_config_is_refused_with_the_reason(env: dict[str, str], message: str) -> None:
    with pytest.raises(RetryPolicyConfigError) as refused:
        RetryPolicyConfig.from_env(env)
    assert message in str(refused.value)


@pytest.mark.asyncio
async def test_the_gateway_refuses_to_start_on_invalid_retry_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validated before the database is even opened: the process exits on a
    bad value instead of running on a default nobody chose."""
    monkeypatch.setenv("OUTBOUND_RETRY_POLICY_JSON", '{"quo.sms.send": {"retries": 3}}')
    monkeypatch.delenv("DATABASE_URI", raising=False)
    with pytest.raises(RetryPolicyConfigError, match="unknown fields"):
        await server.build_runtime()


def test_startup_logs_the_effective_policy_for_every_operation(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    for name, value in PRODUCTION.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("OUTBOUND_RETRY_POLICY_JSON", '{"tenantcloud": {"max_seconds": 300}}')
    with caplog.at_level(logging.INFO, logger=server.__name__):
        policies = server.load_retry_policies()

    (record,) = [record for record in caplog.records if record.getMessage().startswith("outbound retry policy effective: ")]
    logged = json.loads(record.getMessage().removeprefix("outbound retry policy effective: "))
    assert logged == policies.describe()
    assert set(logged["effective"]) == {operation.value for operation in Operation}
    assert logged["effective"]["tenantcloud.message.send"]["max_seconds"] == 300
    assert logged["effective"]["quo.sms.send"] == logged["default"]


def test_backoff_doubles_from_the_base_and_stops_at_the_cap() -> None:
    policy = RetryPolicy(base_seconds=5, max_seconds=300, jitter_ratio=0)
    assert [policy.backoff_seconds(elapsed) for elapsed in (0, 5, 15, 35, 75, 155, 315, 10_000)] == [
        5,
        10,
        20,
        40,
        80,
        160,
        300,
        300,
    ]
    assert [policy.attempt_backoff_seconds(n) for n in (0, 1, 2, 3, 7, 50)] == [5, 5, 10, 20, 300, 300]


def test_jitter_only_ever_shortens_a_wait_by_up_to_its_ratio_and_is_stable_per_key() -> None:
    policy = RetryPolicy(base_seconds=5, max_seconds=900, jitter_ratio=0.2)
    plain = RetryPolicy(base_seconds=5, max_seconds=900, jitter_ratio=0)
    keys = [f"action-{index}" for index in range(200)]
    for elapsed in (0, 100, 5_000, 50_000):
        unjittered = plain.backoff_seconds(elapsed)
        waits = [policy.backoff_seconds(elapsed, jitter_key=key) for key in keys]
        assert all(1 <= wait <= unjittered for wait in waits)
        assert all(wait >= unjittered * 0.8 - 1 for wait in waits)
        assert waits == [policy.backoff_seconds(elapsed, jitter_key=key) for key in keys]
    # Spread: actions queued by one outage do not all retry together.
    assert len({policy.backoff_seconds(50_000, jitter_key=key) for key in keys}) > 50
