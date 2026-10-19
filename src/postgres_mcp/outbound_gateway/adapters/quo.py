"""Quo SMS adapter."""

from __future__ import annotations

from asyncio import timeout

from datetime import datetime
from datetime import timezone
from time import monotonic
from collections.abc import Callable
from typing import Any
from typing import Mapping
from uuid import UUID

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


class QuoSmsAdapter:
    def __init__(self, *, user_id: str, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc), reconciliation_budget_seconds: float = 25):
        if not user_id.strip():
            raise ValueError("Quo user ID is required")
        self._user_id = user_id
        self._clock = clock
        self._reconciliation_budget_seconds = reconciliation_budget_seconds

    def validate(self, context: ActionContext) -> None:
        if context.operation is not Operation.QUO_SMS_SEND:
            raise ValueError("Quo adapter requires quo.sms.send")
        if context.target.kind != "quo_conversation" or not context.target.verified:
            raise ValueError("Quo adapter requires a verified conversation")
        if not context.recipient_phone or not context.provider_account:
            raise ValueError("Quo line and recipient phone are required")

    def build_request(self, context: ActionContext, action_uid: UUID) -> ProviderRequest:
        self.validate(context)
        return ProviderRequest(
            server_name="quo",
            tool="send_message",
            arguments={
                "phone_number_id": context.provider_account,
                "to": context.recipient_phone,
                "user_id": self._user_id,
                "content": str(context.arguments["text"]),
            },
        )

    async def invoke(self, client: McpProviderClient, request: ProviderRequest) -> ProviderObservation:
        return self._parse(await client.call(request.server_name, request.tool, request.arguments), effect_call=True)

    async def poll(self, client: McpProviderClient, observation: ProviderObservation) -> ProviderObservation:
        return observation

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
        """Accept positive provider evidence; history absence never permits resend."""
        inconclusive = ProviderObservation(
            ProviderDisposition.AMBIGUOUS,
            "quo_reconciliation_inconclusive",
            provider_request_ref=observation.provider_request_ref,
            message_id=observation.message_id,
            dispatch_started_at=observation.dispatch_started_at,
        )
        if observation.message_id:
            result = await client.call("quo", "get_message", {"message_id": observation.message_id})
            if initial_observation(result) is not None:
                return inconclusive
            for item in json_objects(result.structured_content if result.structured_content is not None else result.text):
                if self._message_id(item) == observation.message_id and self._accepted_status(item):
                    return accepted_observation(
                        request_ref_value=observation.provider_request_ref,
                        message_id=observation.message_id,
                        detail_code="quo_reconciled_by_message_id",
                        evidence={"kind": "provider_message_id", "provider_message_id": observation.message_id},
                    )
            return inconclusive
        start = observation.dispatch_started_at
        if start is None or start.tzinfo is None:
            return inconclusive
        end = self._clock()
        arguments = {
            "participant": context.recipient_phone,
            "phone_number_id": context.provider_account,
            "created_after": start.isoformat(),
            "created_before": end.isoformat(),
            "max_results": 100,
        }
        seen_tokens: set[str] = set()
        # Keep pagination within the gateway response budget. Another recovery
        # attempt can read again; exhausting this budget never permits resend.
        deadline = monotonic() + self._reconciliation_budget_seconds
        while monotonic() < deadline:
            try:
                async with timeout(max(0.0, deadline - monotonic())):
                    result = await client.call("quo", "list_messages_page", arguments)
            except TimeoutError:
                return inconclusive
            if initial_observation(result) is not None:
                return inconclusive
            objects = list(json_objects(result.structured_content if result.structured_content is not None else result.text))
            page = next((item for item in objects if isinstance(item.get("data"), list)), None)
            if page is None:
                return inconclusive
            for item in page["data"]:
                if not isinstance(item, Mapping):
                    continue
                if item.get("phoneNumberId") not in {None, context.provider_account}:
                    continue
                if not self._matches(item, context, str(context.arguments["text"]), start=start, end=end):
                    continue
                message_id = self._message_id(item)
                if message_id and self._accepted_status(item):
                    return accepted_observation(
                        request_ref_value=observation.provider_request_ref,
                        message_id=message_id,
                        detail_code="quo_reconciled_by_exact_tuple",
                        evidence={"kind": "exact_target_content_time", "provider_message_id": message_id},
                    )
            token = page.get("nextPageToken")
            if not isinstance(token, str) or not token or token in seen_tokens:
                return inconclusive
            seen_tokens.add(token)
            arguments["page_token"] = token
        return inconclusive

    @staticmethod
    def _accepted_status(payload: Mapping[str, Any]) -> bool:
        return str(payload.get("status") or "").casefold() in {
            "queued", "sending", "sent", "delivered", "success", "accepted", "completed"
        }

    @staticmethod
    def _message_id(payload: Mapping[str, Any]) -> str | None:
        for key in ("message_id", "id", "provider_message_id"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _content(payload: Mapping[str, Any]) -> str | None:
        for key in ("content", "text", "body"):
            value = payload.get(key)
            if isinstance(value, str):
                return value
        return None

    @classmethod
    def _matches(
        cls, payload: Mapping[str, Any], context: ActionContext, expected_text: str,
        *, start: datetime, end: datetime,
    ) -> bool:
        direction = str(payload.get("direction") or payload.get("type") or "").casefold()
        if direction and direction not in {"outgoing", "outbound", "sent"}:
            return False
        target = payload.get("to") or payload.get("participant") or payload.get("phone_number")
        recipients = target if isinstance(target, list) else [target]
        if context.recipient_phone not in recipients or cls._content(payload) != expected_text:
            return False
        timestamp = (
            payload.get("createdAt")
            or payload.get("created_at")
            or payload.get("sent_at")
            or payload.get("timestamp")
        )
        if not isinstance(timestamp, str):
            return False
        try:
            sent_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return False
        return sent_at.tzinfo is not None and max(start, context.source_sent_at) <= sent_at <= end

    @classmethod
    def _parse(cls, result: McpCallResult, *, effect_call: bool = False) -> ProviderObservation:
        common = initial_observation(result, effect_call=effect_call)
        if common is not None:
            return common
        payload = result.structured_content
        ref = request_ref(payload)
        for item in json_objects(payload if payload is not None else result.text):
            message_id = cls._message_id(item)
            status = str(item.get("status") or "").casefold()
            if message_id and (cls._accepted_status(item) or not status):
                return accepted_observation(request_ref_value=ref, message_id=message_id)
        return ProviderObservation(
            ProviderDisposition.AMBIGUOUS,
            "malformed_provider_success",
            provider_request_ref=ref,
        )
