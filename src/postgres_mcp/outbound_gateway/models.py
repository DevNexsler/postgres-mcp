"""Strict public contracts for the outbound action gateway."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from enum import StrEnum
from typing import Annotated
from typing import Any
from typing import Literal
from typing import TypeAlias
from unicodedata import normalize
from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import TypeAdapter
from pydantic import field_validator
from pydantic import model_validator


class ActionRole(StrEnum):
    PROSPECT_REPLY = "prospect_reply"
    CALENDAR_MUTATION = "calendar_mutation"
    INTERNAL_NOTIFICATION = "internal_notification"


class Operation(StrEnum):
    EMAIL_SEND = "email.send"
    QUO_SMS_SEND = "quo.sms.send"
    CLIQ_CHANNEL_POST = "cliq.channel.post"
    CLIQ_CHAT_POST = "cliq.chat.post"
    CALENDAR_CREATE = "calendar.create"
    CALENDAR_UPDATE = "calendar.update"
    CALENDAR_DELETE = "calendar.delete"


class IntentKind(StrEnum):
    INQUIRY_REPLY = "inquiry_reply"
    SHOWING_OFFER = "showing_offer"
    SHOWING_CONFIRMATION = "showing_confirmation"
    SHOWING_RESCHEDULE = "showing_reschedule"
    SHOWING_CANCELLATION = "showing_cancellation"
    SHOWING_CREATE = "showing_create"
    SHOWING_UPDATE = "showing_update"
    SHOWING_DELETE = "showing_delete"
    LEAD_ALERT = "lead_alert"
    MANUAL_REVIEW_ALERT = "manual_review_alert"


class ActionState(StrEnum):
    RECEIVED = "received"
    DEPENDENCY_WAIT = "dependency_wait"
    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    PROVIDER_ACCEPTED = "provider_accepted"
    UNKNOWN = "unknown"
    RECONCILING = "reconciling"
    RETRY_READY = "retry_ready"
    COMPLETED = "completed"
    STALE = "stale"
    REJECTED = "rejected"
    DEFINITIVE_FAILED = "definitive_failed"
    DEAD_LETTER = "dead_letter"
    MANUAL_REVIEW = "manual_review"


class CompletionKind(StrEnum):
    SENT = "sent"
    DUPLICATE = "duplicate"


class PublicStatus(StrEnum):
    SENT = "sent"
    DUPLICATE = "duplicate"
    PENDING = "pending"
    STALE = "stale"
    REJECTED = "rejected"
    FAILED = "failed"
    UNKNOWN = "unknown"
    MANUAL_REVIEW = "manual_review"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def normalize_public_text(value: Any, *, field: str, minimum: int, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))
    if not minimum <= len(normalized) <= maximum:
        raise ValueError(f"{field} length must be between {minimum} and {maximum}")
    return normalized


class TextArguments(StrictModel):
    text: str

    @field_validator("text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        return normalize_public_text(value, field="text", minimum=1, maximum=10_000)


class CalendarDescriptionArguments(StrictModel):
    description: str | None = None

    @field_validator("description", mode="before")
    @classmethod
    def normalize_description(cls, value: Any) -> str | None:
        if value is None:
            return None
        return normalize_public_text(value, field="description", minimum=0, maximum=10_000)


class EmptyArguments(StrictModel):
    pass


ArgumentModel: TypeAlias = TextArguments | CalendarDescriptionArguments | EmptyArguments
PositiveBigInt = Annotated[int, Field(strict=True, gt=0, le=9_223_372_036_854_775_807)]


ARGUMENT_MODELS: dict[Operation, type[StrictModel]] = {
    Operation.EMAIL_SEND: TextArguments,
    Operation.QUO_SMS_SEND: TextArguments,
    Operation.CLIQ_CHANNEL_POST: TextArguments,
    Operation.CLIQ_CHAT_POST: TextArguments,
    Operation.CALENDAR_CREATE: CalendarDescriptionArguments,
    Operation.CALENDAR_UPDATE: CalendarDescriptionArguments,
    Operation.CALENDAR_DELETE: EmptyArguments,
}


ALLOWED_COMBINATIONS: frozenset[tuple[ActionRole, Operation, IntentKind]] = frozenset(
    {
        (role, operation, intent)
        for role, operations, intents in (
            (
                ActionRole.PROSPECT_REPLY,
                (Operation.EMAIL_SEND, Operation.QUO_SMS_SEND),
                (
                    IntentKind.INQUIRY_REPLY,
                    IntentKind.SHOWING_OFFER,
                    IntentKind.SHOWING_CONFIRMATION,
                    IntentKind.SHOWING_RESCHEDULE,
                    IntentKind.SHOWING_CANCELLATION,
                ),
            ),
            (
                ActionRole.INTERNAL_NOTIFICATION,
                (Operation.CLIQ_CHANNEL_POST, Operation.CLIQ_CHAT_POST),
                (IntentKind.LEAD_ALERT, IntentKind.MANUAL_REVIEW_ALERT),
            ),
        )
        for operation in operations
        for intent in intents
    }
    | {
        (ActionRole.CALENDAR_MUTATION, Operation.CALENDAR_CREATE, IntentKind.SHOWING_CREATE),
        (ActionRole.CALENDAR_MUTATION, Operation.CALENDAR_UPDATE, IntentKind.SHOWING_UPDATE),
        (ActionRole.CALENDAR_MUTATION, Operation.CALENDAR_DELETE, IntentKind.SHOWING_DELETE),
    }
)

SLOT_REQUIRED_INTENTS = frozenset(
    {
        IntentKind.SHOWING_OFFER,
        IntentKind.SHOWING_CONFIRMATION,
        IntentKind.SHOWING_RESCHEDULE,
        IntentKind.SHOWING_CREATE,
        IntentKind.SHOWING_UPDATE,
    }
)


class ExecuteRequest(StrictModel):
    op: Literal["execute"]
    wakeup_event_id: PositiveBigInt
    action_role: ActionRole
    operation: Operation
    intent_kind: IntentKind
    arguments: ArgumentModel
    appointment_slot: datetime | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_adapter_arguments(cls, raw: Any) -> Any:
        if not isinstance(raw, dict):
            return raw
        operation_value = raw.get("operation")
        try:
            operation = Operation(operation_value)
        except (TypeError, ValueError):
            return raw
        data = dict(raw)
        data["arguments"] = ARGUMENT_MODELS[operation].model_validate(raw.get("arguments"))
        return data

    @field_validator("appointment_slot", mode="before")
    @classmethod
    def normalize_appointment_slot(cls, value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("appointment_slot must be RFC 3339") from exc
        elif isinstance(value, datetime):
            parsed = value
        else:
            raise ValueError("appointment_slot must be an RFC 3339 string")
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("appointment_slot requires an explicit UTC offset")
        return parsed.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_matrix(self) -> ExecuteRequest:
        combination = (self.action_role, self.operation, self.intent_kind)
        if combination not in ALLOWED_COMBINATIONS:
            raise ValueError("unsupported action role, operation, and intent combination")
        if self.intent_kind in SLOT_REQUIRED_INTENTS and self.appointment_slot is None:
            raise ValueError("appointment_slot is required for this intent")
        if self.intent_kind not in SLOT_REQUIRED_INTENTS and self.appointment_slot is not None:
            raise ValueError("appointment_slot is forbidden for this intent")
        return self


class StatusRequest(StrictModel):
    op: Literal["status"]
    action_id: UUID


OutboundRequest: TypeAlias = Annotated[ExecuteRequest | StatusRequest, Field(discriminator="op")]
_REQUEST_ADAPTER = TypeAdapter(OutboundRequest)


def parse_outbound_request(payload: Any) -> OutboundRequest:
    return _REQUEST_ADAPTER.validate_python(payload)


class PublicResult(StrictModel):
    status: PublicStatus
    action_id: UUID
    action_uid: UUID | None
    provider_request_ref: str | None
    retryable: Literal[False] = False
    detail_code: Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[a-z0-9_]+$")]

