"""TenantCloud provider adapter.

Adapts the shared, readback-verified ``TenantCloudMutations`` facade (owned by
a different repository -- see ``scripts/tenantcloud_mutations.py`` in
Comm-Data-Store) to this gateway's provider-neutral ``ProviderAdapter``
contract. This module contains no URLs, no HTTP, and no authentication: all
of that lives behind the facade instance injected at construction.

The facade already performs, for every write: an idempotency pre-check via
its named ``reconcile_*`` methods, at most one exact write, and an exact
post-write readback. This adapter only translates facade results
(``MutationExecution`` / ``ReconciliationResult`` / ``MutationObservation``)
into ``ProviderObservation``. It never imports the facade's
``MutationOperation`` enum -- doing so would require a cross-repo import --
so it relies exclusively on the facade's per-operation named methods and
duck-types disposition checks off the ``.value`` string (``MutationDisposition``
mixes in ``str``, so this is stable across repos).
"""

from __future__ import annotations

from typing import Any
from typing import Mapping
from typing import Protocol
from uuid import UUID

from ..context import ActionContext
from ..models import Operation
from .base import ProviderDisposition
from .base import ProviderObservation
from .base import ProviderReceipt
from .base import ProviderRequest
from .base import accepted_observation
from .base import receipt_from_observation

_TENANTCLOUD_OPERATIONS = frozenset(
    {
        Operation.TENANTCLOUD_MESSAGE_SEND,
        Operation.TENANTCLOUD_LEAD_STATUS_UPDATE,
        Operation.TENANTCLOUD_MAINTENANCE_CREATE,
        Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE,
    }
)

_MAINTENANCE_CREATE_FIELDS = (
    "category_id",
    "title",
    "priority",
    "initiated_at",
    "text",
    "entry_allowed",
    "available_on",
)


class TenantCloudMutationsProtocol(Protocol):
    """Structural shape of the shared facade this adapter depends on."""

    def send_message(self, thread_id: object, body: object) -> Any: ...

    def mark_lead_working(self, lead_id: object) -> Any: ...

    def create_maintenance_request(self, **kwargs: object) -> Any: ...

    def update_maintenance_status(self, request_id: object, status: object) -> Any: ...

    def reconcile_message(self, thread_id: object, body: object, *, source_turn_at: object) -> Any: ...

    def reconcile_lead_status(self, lead_id: object) -> Any: ...

    def reconcile_maintenance_create(self, *, dispatched_after: object, **kwargs: object) -> Any: ...

    def reconcile_maintenance_status(self, request_id: object, status: object) -> Any: ...


class TenantCloudAdapter:
    def __init__(self, *, mutations: TenantCloudMutationsProtocol):
        self._mutations = mutations

    def validate(self, context: ActionContext) -> None:
        if context.operation not in _TENANTCLOUD_OPERATIONS:
            raise ValueError("TenantCloud adapter requires a tenantcloud.* operation")
        if not context.target.verified:
            raise ValueError("TenantCloud adapter requires a verified target")

    def build_request(self, context: ActionContext, action_uid: UUID) -> ProviderRequest:
        self.validate(context)
        del action_uid
        operation = context.operation
        if operation is Operation.TENANTCLOUD_MESSAGE_SEND:
            arguments: dict[str, Any] = {
                "thread_id": context.target.target_id,
                "body": str(context.arguments["text"]),
                "source_sent_at": context.source_sent_at,
            }
        elif operation is Operation.TENANTCLOUD_LEAD_STATUS_UPDATE:
            arguments = {"lead_id": context.target.target_id}
        elif operation is Operation.TENANTCLOUD_MAINTENANCE_CREATE:
            provider_ids = context.canonical_context["provider_ids"]
            arguments = {
                "property_id": provider_ids["property_id"],
                "unit_id": provider_ids["unit_id"],
                "source_sent_at": context.source_sent_at,
                **{field: context.arguments[field] for field in _MAINTENANCE_CREATE_FIELDS},
            }
        else:
            arguments = {
                "request_id": context.target.target_id,
                "status": context.arguments["status"],
            }
        return ProviderRequest(server_name="tenantcloud", tool=operation.value, arguments=arguments)

    async def invoke(self, client: Any, request: ProviderRequest) -> ProviderObservation:
        del client
        arguments = request.arguments
        if request.tool == Operation.TENANTCLOUD_MESSAGE_SEND.value:
            return self._invoke_message(arguments)
        if request.tool == Operation.TENANTCLOUD_LEAD_STATUS_UPDATE.value:
            return self._invoke_lead_status(arguments)
        if request.tool == Operation.TENANTCLOUD_MAINTENANCE_CREATE.value:
            return self._invoke_maintenance_create(arguments)
        return self._invoke_maintenance_status(arguments)

    async def poll(self, client: Any, observation: ProviderObservation) -> ProviderObservation:
        # The facade's writes and readbacks are synchronous and always
        # terminal: there is no queued/pending provider state to poll.
        del client
        return observation

    def parse_receipt(self, context: ActionContext, observation: ProviderObservation) -> ProviderReceipt | None:
        self.validate(context)
        return receipt_from_observation(observation)

    async def reconcile(
        self,
        client: Any,
        context: ActionContext,
        action_uid: UUID,
        observation: ProviderObservation,
    ) -> ProviderObservation:
        del client, action_uid, observation
        operation = context.operation
        if operation is Operation.TENANTCLOUD_MESSAGE_SEND:
            result = self._mutations.reconcile_message(
                context.target.target_id,
                str(context.arguments["text"]),
                source_turn_at=context.source_sent_at,
            )
            return self._from_reconciliation(result, kind="message", accepted_detail="tenantcloud_message_reconciled")
        if operation is Operation.TENANTCLOUD_LEAD_STATUS_UPDATE:
            result = self._mutations.reconcile_lead_status(context.target.target_id)
            return self._from_reconciliation(
                result,
                kind="lead",
                accepted_detail="tenantcloud_lead_status_reconciled",
                definitive_absence_detail="tenantcloud_lead_status_not_yet_applied",
            )
        if operation is Operation.TENANTCLOUD_MAINTENANCE_CREATE:
            provider_ids = context.canonical_context["provider_ids"]
            kwargs = {
                "property_id": provider_ids["property_id"],
                "unit_id": provider_ids["unit_id"],
                **{field: context.arguments[field] for field in _MAINTENANCE_CREATE_FIELDS},
            }
            result = self._mutations.reconcile_maintenance_create(dispatched_after=context.source_sent_at, **kwargs)
            return self._from_reconciliation(
                result,
                kind="maintenance",
                accepted_detail="tenantcloud_maintenance_create_reconciled",
            )
        result = self._mutations.reconcile_maintenance_status(context.target.target_id, context.arguments["status"])
        return self._from_reconciliation(
            result,
            kind="maintenance",
            accepted_detail="tenantcloud_maintenance_status_reconciled",
            definitive_absence_detail="tenantcloud_maintenance_status_not_yet_applied",
        )

    # -- per-operation dispatch -------------------------------------------------

    def _invoke_message(self, arguments: Mapping[str, Any]) -> ProviderObservation:
        thread_id = arguments["thread_id"]
        body = arguments["body"]
        pre = self._mutations.reconcile_message(thread_id, body, source_turn_at=arguments["source_sent_at"])
        if pre.disposition.value == "accepted":
            return self._accepted_from_observation(
                pre.observation, kind="message", detail_code="tenantcloud_message_already_present"
            )
        execution = self._mutations.send_message(thread_id, body)
        return self._from_execution(execution, kind="message", accepted_detail="tenantcloud_message_accepted")

    def _invoke_lead_status(self, arguments: Mapping[str, Any]) -> ProviderObservation:
        lead_id = arguments["lead_id"]
        pre = self._mutations.reconcile_lead_status(lead_id)
        if pre.disposition.value == "accepted":
            return self._accepted_from_observation(
                pre.observation, kind="lead", detail_code="tenantcloud_lead_status_already_present"
            )
        execution = self._mutations.mark_lead_working(lead_id)
        return self._from_execution(execution, kind="lead", accepted_detail="tenantcloud_lead_status_accepted")

    def _invoke_maintenance_create(self, arguments: Mapping[str, Any]) -> ProviderObservation:
        kwargs = {
            "property_id": arguments["property_id"],
            "unit_id": arguments["unit_id"],
            **{field: arguments[field] for field in _MAINTENANCE_CREATE_FIELDS},
        }
        pre = self._mutations.reconcile_maintenance_create(dispatched_after=arguments["source_sent_at"], **kwargs)
        if pre.disposition.value == "accepted":
            return self._accepted_from_observation(
                pre.observation, kind="maintenance", detail_code="tenantcloud_maintenance_already_present"
            )
        execution = self._mutations.create_maintenance_request(**kwargs)
        return self._from_execution(execution, kind="maintenance", accepted_detail="tenantcloud_maintenance_create_accepted")

    def _invoke_maintenance_status(self, arguments: Mapping[str, Any]) -> ProviderObservation:
        request_id = arguments["request_id"]
        status = arguments["status"]
        pre = self._mutations.reconcile_maintenance_status(request_id, status)
        if pre.disposition.value == "accepted":
            return self._accepted_from_observation(
                pre.observation, kind="maintenance", detail_code="tenantcloud_maintenance_status_already_present"
            )
        execution = self._mutations.update_maintenance_status(request_id, status)
        return self._from_execution(execution, kind="maintenance", accepted_detail="tenantcloud_maintenance_status_accepted")

    # -- result translation -------------------------------------------------

    @staticmethod
    def _provider_reference(kind: str, provider_object_id: str) -> str:
        if kind == "message":
            return f"tenantcloud-message:{provider_object_id}"
        if kind == "lead":
            return f"tenantcloud-lead:{provider_object_id}:working"
        return f"tenantcloud-maintenance:{provider_object_id}"

    @classmethod
    def _accepted_from_observation(cls, observation: Any, *, kind: str, detail_code: str) -> ProviderObservation:
        evidence = {
            "canonical_observed_state": dict(observation.canonical_observed_state),
            "readback_timestamp": observation.readback_timestamp,
            "evidence_hash": observation.evidence_hash,
        }
        return accepted_observation(
            request_ref_value=observation.target_reference,
            message_id=cls._provider_reference(kind, observation.provider_object_id),
            detail_code=detail_code,
            evidence=evidence,
        )

    @classmethod
    def _from_execution(cls, execution: Any, *, kind: str, accepted_detail: str) -> ProviderObservation:
        if execution.verified:
            return cls._accepted_from_observation(execution.observation, kind=kind, detail_code=accepted_detail)
        disposition_value = execution.mutation.disposition.value
        error_code = execution.mutation.audit.error_code
        if disposition_value == "definitive_non_acceptance":
            retryable = error_code == "authentication_unavailable"
            return ProviderObservation(
                ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
                "tenantcloud_auth_rejected_before_dispatch" if retryable else "tenantcloud_provider_rejected",
                category="provider_authentication" if retryable else "provider_rejected",
                retryable=retryable,
                evidence={"kind": "http_status", "error_code": error_code} if error_code else None,
            )
        # ACCEPTED-but-unverified or UNKNOWN: the write may or may not have
        # taken effect. Never trust it and never retry blindly -- route to
        # bounded reconciliation instead.
        return ProviderObservation(
            ProviderDisposition.AMBIGUOUS,
            f"tenantcloud_write_ambiguous_{execution.error_code or 'unknown'}",
        )

    @classmethod
    def _from_reconciliation(
        cls,
        result: Any,
        *,
        kind: str,
        accepted_detail: str,
        definitive_absence_detail: str | None = None,
    ) -> ProviderObservation:
        disposition_value = result.disposition.value
        if disposition_value == "accepted":
            return cls._accepted_from_observation(result.observation, kind=kind, detail_code=accepted_detail)
        if disposition_value == "definitive_non_acceptance" and definitive_absence_detail is not None:
            return ProviderObservation(
                ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE,
                definitive_absence_detail,
                category="provider_state_not_yet_applied",
                retryable=True,
            )
        return ProviderObservation(
            ProviderDisposition.AMBIGUOUS,
            f"tenantcloud_reconciliation_{result.error_code or 'inconclusive'}",
        )
