"""Agent Email Cliq adapters."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID

from ..cliq_failure import CLIQ_CHAT_ACCOUNT_MISMATCH
from ..cliq_failure import CLIQ_CHAT_ACCOUNT_MISMATCH_DETAIL
from ..cliq_target import CliqTargetKind
from ..cliq_target import cliq_tool_for_kind
from ..context import ActionContext
from ..models import Operation
from ..provider_client import McpCallResult
from ..provider_client import McpProviderClient
from .base import ProviderDisposition
from .base import ProviderObservation
from .base import ProviderReceipt
from .base import ProviderRequest
from .base import accepted_observation
from .base import initial_observation
from .base import json_objects
from .base import receipt_from_observation
from .base import request_ref
from .base import terminal_content


class CliqAdapter:
    def __init__(self, operation: Operation):
        if operation not in {Operation.CLIQ_CHANNEL_POST, Operation.CLIQ_CHAT_POST}:
            raise ValueError("Cliq adapter requires a Cliq operation")
        self._operation = operation

    def validate(self, context: ActionContext) -> None:
        if context.operation is not self._operation or not context.target.verified:
            raise ValueError("Cliq operation or target mismatch")

    def build_request(self, context: ActionContext, action_uid: UUID) -> ProviderRequest:
        self.validate(context)
        # The AES tool is chosen by what context derivation resolved the
        # target to (context.target.kind), not by which operation the agent
        # named: cliq.channel.post with a chat-shaped id (a CT_* conversation
        # id, or a numeric chat id) is resolved to a CHAT target there --
        # AES's cliq_channel_bot_post rejects that shape outright
        # (2026-09-28 manual_review pileup) -- and must still go to
        # cliq_chat_post here. See cliq_target.py for the single shared
        # classification this and context derivation both use.
        tool, target_field = cliq_tool_for_kind(CliqTargetKind(context.target.kind))
        return ProviderRequest(
            server_name="agent-email",
            tool=tool,
            arguments={
                target_field: context.target.target_id,
                "text": str(context.arguments["text"]),
                "sync_message": True,
                "idempotency_key": (
                    f"cliq-wake:{context.wakeup_event_id}:"
                    f"{context.action_role.value}:{self._operation.value}"
                ),
            },
        )

    async def invoke(self, client: McpProviderClient, request: ProviderRequest) -> ProviderObservation:
        return self._parse(await client.call(request.server_name, request.tool, request.arguments), effect_call=True)

    async def poll(self, client: McpProviderClient, observation: ProviderObservation) -> ProviderObservation:
        if not observation.provider_request_ref:
            return ProviderObservation(ProviderDisposition.AMBIGUOUS, "provider_request_ref_missing")
        result = await client.call("agent-email", "request_status", {"request_id": observation.provider_request_ref})
        return self._parse(result, prior_ref=observation.provider_request_ref)

    def parse_receipt(self, context: ActionContext, observation: ProviderObservation) -> ProviderReceipt | None:
        self.validate(context)
        return receipt_from_observation(observation)

    async def reconcile(
        self,
        client: McpProviderClient,
        context: ActionContext,
        action_uid: UUID,
        observation: ProviderObservation,
    ) -> ProviderObservation:
        if observation.provider_request_ref:
            polled = await self.poll(client, observation)
            if polled.disposition is not ProviderDisposition.AMBIGUOUS:
                return polled
            # Keep poll()'s own detail (e.g. malformed_provider_success,
            # provider_request_lost) instead of the generic
            # cliq_reconciliation_inconclusive: wakes 27296/27297/27314 hit
            # exhaust() with the ledger and logs showing only the generic
            # string, never what request_status actually said.
            return ProviderObservation(
                ProviderDisposition.AMBIGUOUS,
                polled.detail_code,
                provider_request_ref=polled.provider_request_ref or observation.provider_request_ref,
                category=polled.category,
            )
        return ProviderObservation(
            ProviderDisposition.AMBIGUOUS,
            "cliq_reconciliation_inconclusive",
            provider_request_ref=observation.provider_request_ref,
        )

    @staticmethod
    def _parse(result: McpCallResult, *, prior_ref: str | None = None, effect_call: bool = False) -> ProviderObservation:
        common = initial_observation(result, effect_call=effect_call)
        if common is not None:
            message = (common.evidence or {}).get("provider_message")
            # AES's local participation guard proves no Zoho call occurred.
            # Other permanent failures retain their provider classification.
            if (
                common.disposition is ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE
                and isinstance(message, str)
                and "A conversation id from another account is not a post target." in message
                and "The provider was not called." in message
            ):
                common = replace(
                    common,
                    detail_code=CLIQ_CHAT_ACCOUNT_MISMATCH,
                    category="provider_validation",
                    evidence={**(common.evidence or {}), "provider_message": f"{message} {CLIQ_CHAT_ACCOUNT_MISMATCH_DETAIL}"},
                )
            if prior_ref and common.provider_request_ref is None:
                return replace(common, provider_request_ref=prior_ref)
            return common
        payload = terminal_content(result.structured_content)
        ref = request_ref(result.structured_content) or prior_ref
        for item in json_objects(payload):
            status = item.get("status")
            message_id = item.get("provider_message_id")
            if status in {"sent", "duplicate_suppressed"} and isinstance(message_id, str) and message_id.strip():
                return accepted_observation(request_ref_value=ref, message_id=message_id.strip())
        return ProviderObservation(
            ProviderDisposition.AMBIGUOUS,
            "malformed_provider_success",
            provider_request_ref=ref,
        )
