"""tenantcloud_delivery.py is now a thin compatibility shim over
delivery_workflow.py (the generic Restate coordinator moved there). This
covers only what's left here: the re-exported names still resolve to the
same objects, and the TenantCloud-only default instance still rejects a
non-TenantCloud operation. See test_delivery_workflow.py for coordinator
behavior."""

from __future__ import annotations

from unittest.mock import AsyncMock

from postgres_mcp.outbound_gateway import delivery_workflow
from postgres_mcp.outbound_gateway import tenantcloud_delivery
from postgres_mcp.outbound_gateway.models import Operation


def test_tenantcloud_delivery_coordinator_alias_is_the_generic_class() -> None:
    assert tenantcloud_delivery.TenantCloudDeliveryCoordinator is delivery_workflow.OutboundDeliveryCoordinator


def test_tenantcloud_auth_gate_alias_is_the_generic_secret_auth_gate() -> None:
    assert tenantcloud_delivery.TenantCloudAuthGate is delivery_workflow.SecretAuthGate


def test_a_non_tenantcloud_operation_is_rejected_by_the_tenantcloud_only_instance() -> None:
    coordinator = tenantcloud_delivery.TenantCloudDeliveryCoordinator(
        store=AsyncMock(), service=AsyncMock(), auth=AsyncMock()
    )
    assert Operation.CALENDAR_UPDATE not in coordinator._operations  # noqa: SLF001
