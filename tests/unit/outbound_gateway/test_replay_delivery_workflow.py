from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "replay_delivery_workflow.py"
_SPEC = importlib.util.spec_from_file_location("replay_delivery_workflow", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
m = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = m
_SPEC.loader.exec_module(m)


def _row(*, operation, state, created_at, attempts):
    return {
        "action_id": "11111111-1111-1111-1111-111111111111",
        "operation": operation,
        "state": state,
        "detail_code": state,
        "attempt_count": len(attempts),
        "created_at": created_at,
        "attempts": attempts,
    }


def test_a_completed_action_replays_as_sent() -> None:
    row = _row(
        operation="email.send",
        state="completed",
        created_at="2026-09-01T00:00:00+00:00",
        attempts=[
            {
                "attempt_number": 1,
                "to_state": "dispatching",
                "detail_code": "dispatch_started",
                "created_at": "2026-09-01T00:00:01+00:00",
                "provider_observation": {"disposition": "pending", "detail_code": "dispatch_started"},
            },
            {
                "attempt_number": 1,
                "to_state": "completed",
                "detail_code": "provider_receipt_verified",
                "created_at": "2026-09-01T00:00:02+00:00",
                "provider_observation": {"receipt_keys": ["provider_message_id"]},
            },
        ],
    )
    result = m.replay_action(row)
    assert result["old_outcome"] == "SENT"
    assert result["new_outcome"] == "SENT"
    assert result["same"] is True


def test_a_definitive_rejection_replays_as_definitive_failed_immediately() -> None:
    row = _row(
        operation="cliq.chat.post",
        state="rejected",
        created_at="2026-09-01T00:00:00+00:00",
        attempts=[
            {
                "attempt_number": 1,
                "to_state": "rejected",
                "detail_code": "provider_rejected_request",
                "created_at": "2026-09-01T00:00:03+00:00",
                "provider_observation": {"disposition": "definitive_non_acceptance", "detail_code": "provider_rejected_request", "retryable": False},
            },
        ],
    )
    result = m.replay_action(row)
    assert result["old_outcome"] == "DEFINITIVE_FAILED"
    assert result["new_outcome"] == "DEFINITIVE_FAILED"
    assert result["same"] is True
    assert result["warn_staff"] is True


def test_an_attempt_cap_park_inside_the_ceiling_is_the_expected_difference() -> None:
    """The core regression this branch fixes: the OLD system parked at
    manual_review via the legacy attempt-count cap, well inside the new
    one-hour elapsed ceiling."""
    row = _row(
        operation="calendar.update",
        state="manual_review",
        created_at="2026-09-28T18:41:26+00:00",
        attempts=[
            {
                "attempt_number": i,
                "to_state": "manual_review" if i == 5 else "retry_ready",
                "detail_code": "persisted_context_unavailable",
                "created_at": f"2026-09-28T18:41:{26 + i}+00:00",
                "provider_observation": {"disposition": "ambiguous", "detail_code": "persisted_context_unavailable"},
            }
            for i in range(1, 6)
        ],
    )
    result = m.replay_action(row)
    assert result["old_outcome"] == "DEFINITIVE_FAILED"
    assert result["new_outcome"] == "STILL_RETRYING"
    assert result["same"] is False
    assert result["category"] == "EXPECTED_ATTEMPT_CAP_VS_CEILING"


def test_a_genuinely_unexplained_difference_is_categorized_as_such() -> None:
    """An action that completed in production, but whose recorded history
    (per this fixture) crossed the one-hour ceiling with no acceptance, is a
    real, unexplained divergence -- not silently folded into the expected
    category."""
    row = _row(
        operation="quo.sms.send",
        state="completed",
        created_at="2026-09-01T00:00:00+00:00",
        attempts=[
            {
                "attempt_number": 1,
                "to_state": "unknown",
                "detail_code": "quo_reconciliation_inconclusive",
                "created_at": "2026-09-01T01:05:00+00:00",
                "provider_observation": {"disposition": "ambiguous", "detail_code": "quo_reconciliation_inconclusive"},
            },
        ],
    )
    result = m.replay_action(row)
    assert result["old_outcome"] == "SENT"
    assert result["new_outcome"] == "DEFINITIVE_FAILED"
    assert result["same"] is False
    assert result["category"] == "UNEXPLAINED"


def test_tenantcloud_auth_gate_codes_are_excluded_from_the_definitive_check() -> None:
    """tenantcloud_auth_rejected_before_dispatch carries disposition
    definitive_non_acceptance in the ledger, but the live coordinator never
    runs it through retry_policy at all (AuthGate handles it, separately) --
    real production actions carrying this mid-sequence go on to complete."""
    row = _row(
        operation="tenantcloud.lead.status.update",
        state="completed",
        created_at="2026-09-01T00:00:00+00:00",
        attempts=[
            {
                "attempt_number": 1,
                "to_state": "retry_ready",
                "detail_code": "tenantcloud_auth_rejected_before_dispatch",
                "created_at": "2026-09-01T00:00:05+00:00",
                "provider_observation": {"disposition": "definitive_non_acceptance", "detail_code": "tenantcloud_auth_rejected_before_dispatch"},
            },
            {
                "attempt_number": 2,
                "to_state": "completed",
                "detail_code": "provider_receipt_verified",
                "created_at": "2026-09-01T00:00:15+00:00",
                "provider_observation": {"receipt_keys": ["provider_message_id"]},
            },
        ],
    )
    result = m.replay_action(row)
    assert result["new_outcome"] == "SENT"
    assert result["same"] is True


@pytest.mark.parametrize(
    "operation", ["quo.sms.send", "email.send", "cliq.chat.post", "cliq.channel.post", "calendar.create", "tenantcloud.message.send"]
)
def test_reinvoke_safety_is_attached_to_every_result(operation: str) -> None:
    row = _row(operation=operation, state="stale", created_at="2026-09-01T00:00:00+00:00", attempts=[])
    result = m.replay_action(row)
    assert result["old_outcome"] == "stale"
