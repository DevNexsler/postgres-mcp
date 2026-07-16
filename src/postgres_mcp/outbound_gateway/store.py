"""PostgreSQL persistence adapter for durable outbound actions."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any
from typing import Mapping
from uuid import UUID

from postgres_mcp.sql import SafeSqlDriver

from .adapters.base import ProviderObservation
from .adapters.base import ProviderReceipt
from .context import ActionContext
from .models import ActionRole
from .models import ActionState
from .models import CompletionKind
from .models import IntentKind
from .models import Operation
from .service import OutboundActionRecord


def _json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Mapping[str, Any]) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _observation(value: ProviderObservation) -> dict[str, Any]:
    return {
        "detail_code": value.detail_code,
        "disposition": value.disposition.value,
    }


class PostgresActionStore:
    def __init__(self, driver: Any):
        self._driver = driver

    async def _one(self, query: str, params: list[Any]) -> OutboundActionRecord:
        rows = await SafeSqlDriver.execute_param_query(self._driver, query, params)  # type: ignore[arg-type]
        if not rows:
            raise LookupError("outbound action database function returned no row")
        return self._record(rows[0].cells)

    async def create_or_load(self, context: ActionContext) -> OutboundActionRecord:
        recipient_scope = {
            "kind": context.target.kind,
            "target_id": context.target.target_id,
            "verified": context.target.verified,
        }
        return await self._one(
            """
            SELECT * FROM create_or_load_outbound_action(
                {}, {}, {}, {}, {}, {}, {}::jsonb, {}, {}::jsonb,
                {}::jsonb, {}, {}, {}::jsonb
            )
            """,
            [
                context.wakeup_event_id,
                context.action_role.value,
                context.operation.value,
                context.intent_kind.value,
                context.appointment_slot,
                context.payload_hash,
                _json(context.canonical_context),
                context.source_message_id,
                _json(context.canonical_scope),
                _json(recipient_scope),
                context.provider_account,
                context.routing_policy_version,
                _json(context.arguments),
            ],
        )

    async def prepare(self, context: ActionContext, expected_state: ActionState) -> OutboundActionRecord:
        slot = context.appointment_slot.isoformat() if context.appointment_slot else ""
        return await self._one(
            """
            SELECT * FROM prepare_outbound_action_and_acquire_lock(
                {}, {}, {}, {}, {}, {}, {}, {}, 900, 86400
            )
            """,
            [
                context.action_id,
                expected_state.value,
                context.prospect_id,
                context.property_id or context.property_label or "",
                context.intent_kind.value,
                slot,
                list(context.aliases),
                bool(slot and context.property_id),
            ],
        )

    async def claim(
        self,
        action_id: UUID,
        expected_state: ActionState,
        lease_owner: str,
        lease_seconds: int,
    ) -> OutboundActionRecord:
        return await self._one(
            "SELECT * FROM claim_outbound_action({}, {}, {}, {})",
            [action_id, expected_state.value, lease_owner, lease_seconds],
        )

    async def record_provider_request(
        self,
        action_id: UUID,
        lease_owner: str,
        observation: ProviderObservation,
    ) -> OutboundActionRecord:
        return await self._one(
            "SELECT * FROM record_outbound_provider_request({}, {}, {}, {}, {}::jsonb)",
            [
                action_id,
                lease_owner,
                observation.provider_call_id,
                observation.provider_request_ref,
                _json(_observation(observation)),
            ],
        )

    async def transition(
        self,
        action_id: UUID,
        expected_state: ActionState,
        next_state: ActionState,
        lease_owner: str | None,
        observation: ProviderObservation,
    ) -> OutboundActionRecord:
        authoritative = next_state is ActionState.RETRY_READY
        evidence = dict(observation.evidence or {}) if authoritative else {}
        evidence_reference = observation.provider_request_ref or observation.detail_code if authoritative else None
        return await self._one(
            """
            SELECT * FROM transition_outbound_action(
                {}, {}, {}, {}, {}, {}, {}, {}, {}::jsonb,
                {}, {}, {}, {}, {}
            )
            """,
            [
                action_id,
                expected_state.value,
                next_state.value,
                lease_owner,
                observation.detail_code,
                observation.provider_call_id,
                observation.provider_request_ref,
                observation.message_id,
                _json(_observation(observation)),
                "authoritative_non_acceptance" if authoritative else None,
                evidence_reference,
                _hash(evidence) if authoritative else None,
                observation.category,
                observation.detail_code if next_state is ActionState.UNKNOWN else None,
            ],
        )

    async def complete(
        self,
        action_id: UUID,
        expected_state: ActionState,
        lease_owner: str | None,
        receipt: ProviderReceipt,
        completion_kind: CompletionKind,
        detail_code: str,
    ) -> OutboundActionRecord:
        evidence = {
            "accepted_at": receipt.accepted_at.isoformat(),
            "evidence": dict(receipt.evidence),
            "provider_message_id": receipt.provider_message_id,
            "provider_request_ref": receipt.provider_request_ref,
        }
        return await self._one(
            """
            SELECT * FROM complete_outbound_action(
                {}, {}, {}, {}, {}, {}::jsonb, {}, {}, {}
            )
            """,
            [
                action_id,
                expected_state.value,
                lease_owner,
                receipt.provider_request_ref,
                receipt.provider_message_id,
                _json(evidence),
                completion_kind.value,
                _hash(evidence),
                detail_code,
            ],
        )

    async def definitive_fail(
        self,
        action_id: UUID,
        expected_state: ActionState,
        lease_owner: str,
        observation: ProviderObservation,
    ) -> OutboundActionRecord:
        evidence = dict(observation.evidence or {})
        reference = observation.provider_request_ref or observation.detail_code
        return await self._one(
            """
            SELECT * FROM definitively_fail_outbound_action(
                {}, {}, {}, 'authoritative_non_acceptance', {}, {}, {}, {}
            )
            """,
            [
                action_id,
                expected_state.value,
                lease_owner,
                reference,
                _hash(evidence),
                observation.category or "provider_non_acceptance",
                observation.detail_code,
            ],
        )

    async def get(self, action_id: UUID) -> OutboundActionRecord | None:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            "SELECT * FROM outbound_actions WHERE action_id = {}",
            [action_id],
        )
        return self._record(rows[0].cells) if rows else None

    async def list_work(self, limit: int) -> list[tuple[UUID, ActionState]]:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT action_id, state
            FROM outbound_actions
            WHERE state IN (
                'dependency_wait', 'prepared', 'dispatching', 'unknown',
                'reconciling', 'retry_ready'
            )
              AND (lease_owner IS NULL OR lease_expires_at <= now())
            ORDER BY updated_at, action_id
            LIMIT {}
            """,
            [limit],
        )
        return [(UUID(str(row.cells["action_id"])), ActionState(str(row.cells["state"]))) for row in rows or []]

    @staticmethod
    def _record(cells: Mapping[str, Any]) -> OutboundActionRecord:
        completion = cells.get("completion_kind")
        action_uid = cells.get("action_uid")
        return OutboundActionRecord(
            action_id=UUID(str(cells["action_id"])),
            wakeup_event_id=int(cells["wakeup_event_id"]),
            action_role=ActionRole(str(cells["action_role"])),
            operation=Operation(str(cells["operation"])),
            intent_kind=IntentKind(str(cells["intent_kind"])),
            appointment_slot=cells.get("appointment_slot"),
            arguments=dict(cells.get("arguments") or {}),
            state=ActionState(str(cells["state"])),
            action_uid=UUID(str(action_uid)) if action_uid else None,
            provider_request_ref=cells.get("provider_request_ref"),
            provider_message_id=cells.get("provider_message_id"),
            completion_kind=CompletionKind(str(completion)) if completion else None,
            detail_code=str(cells["detail_code"]),
            attempt_count=int(cells.get("attempt_count") or 0),
        )
