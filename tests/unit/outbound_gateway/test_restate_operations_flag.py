from __future__ import annotations

import pytest

from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.server import _restate_operations
from postgres_mcp.outbound_gateway.tenantcloud_shared import TENANTCLOUD_OPERATIONS


def test_default_is_empty_and_leaves_tenantcloud_out_of_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OUTBOUND_RESTATE_OPERATIONS", raising=False)
    assert _restate_operations() == frozenset()
    # TenantCloud is never read from this flag -- it is unioned in by every
    # caller (worker.py, tenantcloud_delivery_server.py) regardless of what
    # this returns.
    assert not (_restate_operations() & TENANTCLOUD_OPERATIONS)


def test_blank_string_is_also_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBOUND_RESTATE_OPERATIONS", "  ")
    assert _restate_operations() == frozenset()


def test_parses_a_json_array_of_operation_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "OUTBOUND_RESTATE_OPERATIONS",
        '["calendar.update", "quo.sms.send"]',
    )
    assert _restate_operations() == frozenset({Operation.CALENDAR_UPDATE, Operation.QUO_SMS_SEND})


def test_rejects_a_non_array(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBOUND_RESTATE_OPERATIONS", '{"not": "a list"}')
    with pytest.raises(ValueError, match="must be a JSON array"):
        _restate_operations()


def test_rejects_an_unsupported_operation_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBOUND_RESTATE_OPERATIONS", '["not.a.real.operation"]')
    with pytest.raises(ValueError, match="unsupported operation"):
        _restate_operations()


def test_including_a_tenantcloud_operation_is_accepted_but_redundant(monkeypatch: pytest.MonkeyPatch) -> None:
    """Naming a TenantCloud operation here is harmless (the union is
    idempotent) but pointless -- TenantCloud is unconditional already."""
    monkeypatch.setenv("OUTBOUND_RESTATE_OPERATIONS", '["tenantcloud.message.send"]')
    assert _restate_operations() == frozenset({Operation.TENANTCLOUD_MESSAGE_SEND})
