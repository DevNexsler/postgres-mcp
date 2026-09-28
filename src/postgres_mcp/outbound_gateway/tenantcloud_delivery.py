"""TenantCloud-specific glue over the generic delivery workflow.

The Restate coordinator, workflow app, and HTTP ingress submitter used to
live in this module under TenantCloud-flavored names, back when TenantCloud
was the only operation Restate delivered. That machinery is generic now and
lives in ``delivery_workflow.py``; nothing about it is TenantCloud-specific.

This module keeps exactly two things:

1. The compatibility aliases below, so source that still imports
   ``TenantCloudDeliveryCoordinator`` / ``TenantCloudAuthGate`` from here
   (this project's own tests, and any external caller) keeps working
   unchanged. Prefer importing directly from ``delivery_workflow`` in new
   code.
2. Anywhere genuinely TenantCloud-specific glue for the delivery workflow
   ends up living (there is none today beyond the aliases -- the actual
   TenantCloud provider adapter is ``adapters/tenantcloud.py``, and its auth/
   client/mutations wiring is ``server.py``'s ``_build_tenantcloud_adapter``/
   ``build_tenantcloud_auth_gate``, neither of which needed to move).
"""

from __future__ import annotations

from .delivery_workflow import AuthGate as AuthGate
from .delivery_workflow import AuthResult as AuthResult
from .delivery_workflow import AuthState as AuthState
from .delivery_workflow import DeliveryPhase as DeliveryPhase
from .delivery_workflow import DeliveryResult as DeliveryResult
from .delivery_workflow import DeliveryService as DeliveryService
from .delivery_workflow import DeliveryStore as DeliveryStore
from .delivery_workflow import OutboundDeliveryCoordinator as OutboundDeliveryCoordinator
from .delivery_workflow import RequestFn as RequestFn
from .delivery_workflow import RestateWorkflowSubmitter as RestateWorkflowSubmitter
from .delivery_workflow import SecretAuthGate as SecretAuthGate
from .delivery_workflow import build_restate_app as build_restate_app

# Pre-split names, kept working: this class and this auth gate are
# constructed for TenantCloud unconditionally (tenantcloud_delivery_server.py,
# server.py's build_tenantcloud_auth_gate) exactly as before the split into
# delivery_workflow.py -- only the generic implementation's home changed.
TenantCloudDeliveryCoordinator = OutboundDeliveryCoordinator
TenantCloudAuthGate = SecretAuthGate
