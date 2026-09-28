"""ASGI worker for the keyed Restate delivery workflow.

Serves ONE workflow (``TenantCloudDelivery`` by default, or
``OUTBOUND_RESTATE_WORKFLOW_NAME`` if set) whose coordinator advances
TenantCloud operations unconditionally, plus whatever
``OUTBOUND_RESTATE_OPERATIONS`` names for the generalized rollout -- the
same env vars ``server.py``'s worker process reads, so the submitter and
this deployment always agree on both the workflow name and which
operations it owns. See ``tenantcloud_delivery.py``'s module docstring."""

from __future__ import annotations

import asyncio
import os

from hypercorn.asyncio import serve
from hypercorn.config import Config

from .delivery_workflow import OutboundDeliveryCoordinator
from .delivery_workflow import build_restate_app
from .server import _restate_operations
from .server import build_runtime
from .server import build_tenantcloud_auth_gate
from .staff_warning import DEFAULT_STAFF_WARNING_CHANNEL
from .staff_warning import CliqStaffWarningPort
from .tenantcloud_shared import TENANTCLOUD_OPERATIONS


async def _serve() -> None:
    runtime = await build_runtime()
    coordinator = OutboundDeliveryCoordinator(
        store=runtime.store,
        service=runtime.service,
        auth=build_tenantcloud_auth_gate(),
        max_attempts=int(os.environ.get("OUTBOUND_MAX_ATTEMPTS", "5")),
        operations=TENANTCLOUD_OPERATIONS | _restate_operations(),
        staff_warning=CliqStaffWarningPort(
            runtime.service,
            target_channel=os.environ.get("OUTBOUND_STAFF_WARNING_CHANNEL", DEFAULT_STAFF_WARNING_CHANNEL),
        ),
    )
    config = Config()
    config.bind = [
        os.environ.get(
            "OUTBOUND_TENANTCLOUD_RESTATE_WORKER_BIND",
            "127.0.0.1:9083",
        )
    ]
    config.accesslog = None
    workflow_name = os.environ.get("OUTBOUND_RESTATE_WORKFLOW_NAME", "TenantCloudDelivery")
    try:
        await serve(build_restate_app(coordinator, workflow_name=workflow_name), config, mode="asgi")
    finally:
        await runtime.pool.close()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
