"""Strict public contracts for the outbound action gateway."""

from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timezone
from enum import StrEnum
from typing import Annotated
from typing import Any
from typing import Literal
from typing import TypeAlias
from unicodedata import category
from unicodedata import normalize
from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import TypeAdapter
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_validator


class ActionRole(StrEnum):
    PROSPECT_REPLY = "prospect_reply"
    CALENDAR_MUTATION = "calendar_mutation"
    INTERNAL_NOTIFICATION = "internal_notification"
    INTERNAL_REPLY = "internal_reply"
    PROVIDER_MUTATION = "provider_mutation"


class Operation(StrEnum):
    EMAIL_SEND = "email.send"
    QUO_SMS_SEND = "quo.sms.send"
    CLIQ_CHANNEL_POST = "cliq.channel.post"
    CLIQ_CHAT_POST = "cliq.chat.post"
    CALENDAR_CREATE = "calendar.create"
    CALENDAR_UPDATE = "calendar.update"
    CALENDAR_DELETE = "calendar.delete"
    TENANTCLOUD_MESSAGE_SEND = "tenantcloud.message.send"
    TENANTCLOUD_LEAD_STATUS_UPDATE = "tenantcloud.lead.status.update"
    TENANTCLOUD_MAINTENANCE_CREATE = "tenantcloud.maintenance.create"
    TENANTCLOUD_MAINTENANCE_STATUS_UPDATE = "tenantcloud.maintenance.status.update"


class IntentKind(StrEnum):
    INQUIRY_REPLY = "inquiry_reply"
    INTERNAL_REPLY = "internal_reply"
    SHOWING_OFFER = "showing_offer"
    SHOWING_CONFIRMATION = "showing_confirmation"
    SHOWING_RESCHEDULE = "showing_reschedule"
    SHOWING_CANCELLATION = "showing_cancellation"
    SHOWING_CREATE = "showing_create"
    SHOWING_UPDATE = "showing_update"
    SHOWING_DELETE = "showing_delete"
    LEAD_ALERT = "lead_alert"
    MANUAL_REVIEW_ALERT = "manual_review_alert"
    TENANTCLOUD_LEAD_STATUS = "tenantcloud_lead_status"
    TENANTCLOUD_MAINTENANCE_CREATE = "tenantcloud_maintenance_create"
    TENANTCLOUD_MAINTENANCE_STATUS = "tenantcloud_maintenance_status"


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
    # A stale_context traffic block is a question to the agent, not a failure:
    # the action is durably recorded as a deliberate no-send (state `stale`)
    # and the result carries the newer context plus a yes/no confirm prompt.
    NEEDS_CONFIRMATION = "needs_confirmation"


class StaleContextDecision(StrEnum):
    YES = "yes"
    NO = "no"
    REVISE = "revise"


# The stale-context family of `detail_code`s on a `stale` row. Every one of
# them is a deliberate no-send, never a failure:
#   stale_context            -- refused, question asked, no answer (yet)
#   stale_context_confirmed  -- agent answered yes; a successor carries the send
#   stale_context_declined   -- agent answered no; nothing is sent
#   stale_context_revised    -- agent answered revise; a successor carries
#                               the revised content to the same target
STALE_CONTEXT_DETAIL = "stale_context"
STALE_CONTEXT_DETAILS = frozenset(
    {
        "stale_context",
        "stale_context_confirmed",
        "stale_context_declined",
        "stale_context_revised",
    }
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def normalize_public_text(value: Any, *, field: str, minimum: int, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))
    if not minimum <= len(normalized) <= maximum:
        raise ValueError(f"{field} length must be between {minimum} and {maximum}")
    return normalized


_EMAIL_ADDRESS = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_E164_PHONE = re.compile(r"^\+[1-9]\d{1,14}$")


def normalize_target_email(value: Any, *, field: str) -> str:
    """Format-only email check: syntactically an address, nothing more.
    Never checks that the address belongs to any prospect, wake, or thread --
    the agent asserts the recipient, this only rejects garbage."""
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    candidate = normalize("NFC", value).strip()
    if not _EMAIL_ADDRESS.fullmatch(candidate):
        raise ValueError(f"{field} must look like an email address")
    return candidate


def normalize_target_phone(value: Any, *, field: str) -> str:
    """Format-only E.164 check. Never checks that the number belongs to any
    prospect, wake, or conversation."""
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    candidate = value.strip()
    if not _E164_PHONE.fullmatch(candidate):
        raise ValueError(f"{field} must be E.164 formatted, e.g. +19085550100")
    return candidate


def normalize_target_id(value: Any, *, field: str) -> str:
    """Format-only non-empty-string check for opaque provider target ids
    (Cliq channel/chat ids, calendar ids). No ownership or membership check."""
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    candidate = normalize("NFC", value).strip()
    if not candidate:
        raise ValueError(f"{field} must not be empty")
    return candidate


def normalize_optional_target_id(value: Any, *, field: str) -> str | None:
    """Same format-only check as normalize_target_id, but None means the
    agent omitted the field entirely (a caller-side fallback may still
    apply) rather than supplying a blank value."""
    if value is None:
        return None
    return normalize_target_id(value, field=field)


_ABSOLUTE_HTTP_URL = re.compile(r"^https?://\S+$", re.IGNORECASE)


def normalize_event_url(value: Any, *, field: str) -> str | None:
    """Format-only check that the value is a plausible absolute http(s)
    URL. Never verifies the event exists or belongs to any wake, prospect,
    or calendar -- the agent asserts the event, this only rejects garbage.
    None means the field was omitted (a caller-side fallback may apply)."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    candidate = normalize("NFC", value).strip()
    if not _ABSOLUTE_HTTP_URL.fullmatch(candidate):
        raise ValueError(f"{field} must be an absolute http(s) URL")
    return candidate


def normalize_email_list(
    value: Any, *, field: str, maximum: int, allow_empty: bool = False
) -> tuple[str, ...] | None:
    """Optional list of addresses (Cc, attendees): each format-checked, order
    kept, duplicates dropped. None means omitted; allow_empty keeps an
    explicit [] as () -- "none", which is not the same as omitted."""
    if value is None:
        return None
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field} must be a list of email addresses")
    addresses: list[str] = []
    for item in value:
        address = normalize_target_email(item, field=field)
        if address.casefold() not in {existing.casefold() for existing in addresses}:
            addresses.append(address)
    if not (0 if allow_empty else 1) <= len(addresses) <= maximum:
        raise ValueError(f"{field} must list between {0 if allow_empty else 1} and {maximum} addresses")
    return tuple(addresses)


def normalize_optional_title(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    return normalize_public_text(value, field=field, minimum=1, maximum=255).strip() or None


# Email attachments travel inside the request as base64: the agent shares no
# filesystem with the gateway container. They are stored in the action's
# saved arguments (the worker sends the saved record), so the cap keeps one
# row small: 10 files, 10 MiB decoded in total (about 13.4 MiB of base64).
MAX_EMAIL_ATTACHMENTS = 10
MAX_EMAIL_ATTACHMENT_BYTES = 10 * 1024 * 1024
_MIME_TYPE = re.compile(r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$")


class EmailAttachment(StrictModel):
    filename: str
    mime_type: str
    content_base64: str

    @field_validator("filename", mode="before")
    @classmethod
    def normalize_filename(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("filename must be a string, e.g. lease.pdf")
        candidate = normalize("NFC", value).strip()
        if not candidate:
            raise ValueError("filename must not be empty; name the file as the recipient should see it, e.g. lease.pdf")
        if len(candidate) > 255:
            raise ValueError("filename must be at most 255 characters")
        if "/" in candidate or "\\" in candidate or candidate in {".", ".."} or any(
            category(character) == "Cc" for character in candidate
        ):
            raise ValueError("filename must be a bare file name like lease.pdf, without folders or control characters")
        return candidate

    @field_validator("mime_type", mode="before")
    @classmethod
    def normalize_mime_type(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("mime_type must be a string such as application/pdf or image/jpeg")
        candidate = value.strip().casefold()
        if len(candidate) > 255 or not _MIME_TYPE.fullmatch(candidate):
            raise ValueError("mime_type must look like type/subtype, e.g. application/pdf or image/jpeg")
        return candidate

    @field_validator("content_base64", mode="before")
    @classmethod
    def normalize_content_base64(cls, value: Any) -> str:
        """Stored in one canonical form: line breaks and spaces (as the
        `base64` command wraps its output) are dropped, so the same bytes are
        the same request however they were wrapped."""
        if not isinstance(value, str):
            raise ValueError("content_base64 must be a base64 string of the file's bytes")
        compact = "".join(value.split())
        if not compact:
            raise ValueError("content_base64 must not be empty; attach a file with content")
        try:
            data = base64.b64decode(compact, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(
                "content_base64 must be base64 of the file's bytes (standard alphabet, e.g. Python base64.b64encode)"
            ) from exc
        if not data:
            raise ValueError("content_base64 must not be empty; attach a file with content")
        return base64.b64encode(data).decode("ascii")

    @property
    def size(self) -> int:
        return len(base64.b64decode(self.content_base64))


class SorPropertyReference(StrictModel):
    """Caller-resolved SOR inventory identity; labels are never identifiers."""

    building_id: int = Field(gt=0, strict=True, description="Buildings.id from SOR inventory; never a TenantCloud ID or Factbook UUID.")
    unit_id: int | None = Field(default=None, gt=0, strict=True, description="Building Units.id from the same SOR building, when this action concerns one unit.")


PROPERTY_REFERENCE_GUIDANCE = (
    'Optional arguments.property_ref is {"building_id": <SOR Buildings.id>, '
    '"unit_id": <SOR Building Units.id or null>}. Use upstream property_identity '
    "when available; otherwise resolve building/unit through SOR inventory before "
    "property-specific work. SOR owns addresses/aliases and Building Units.Buildings_id "
    "links unit to building. Factbook inventory_building_id/inventory_unit_id are linked "
    "SOR IDs. Pass both IDs for one unit; omit when unresolved. Never invent IDs from "
    "address text. This reference controls locking, not the displayed location."
)


class PropertyArguments(StrictModel):
    property_ref: SorPropertyReference | None = Field(
        default=None,
        description=PROPERTY_REFERENCE_GUIDANCE,
    )


class EmailArguments(PropertyArguments):
    to_address: str
    text: str
    # Optional: the subject defaults to "Re: <the wake's subject>". cc
    # omitted (None) takes the gateway's default copy (identity.
    # with_default_cc); cc given, [] included, is sent exactly as given.
    subject: str | None = None
    cc: tuple[str, ...] | None = None
    attachments: tuple[EmailAttachment, ...] | None = None

    @field_validator("attachments", mode="before")
    @classmethod
    def normalize_attachments(cls, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            raise ValueError("attachments must be a list of {filename, mime_type, content_base64}")
        if not value:
            # No files is the same request as leaving attachments out.
            return None
        if len(value) > MAX_EMAIL_ATTACHMENTS:
            raise ValueError(
                f"attachments may list at most {MAX_EMAIL_ATTACHMENTS} files; send the rest in another email"
            )
        return value

    @model_validator(mode="after")
    def limit_attachment_size(self) -> EmailArguments:
        total = sum(attachment.size for attachment in self.attachments or ())
        if total > MAX_EMAIL_ATTACHMENT_BYTES:
            raise ValueError(
                f"attachments total {total} bytes, over the 10 MiB limit; send fewer or smaller files "
                "(split them across emails)"
            )
        return self

    @field_validator("to_address", mode="before")
    @classmethod
    def normalize_to_address(cls, value: Any) -> str:
        return normalize_target_email(value, field="to_address")

    @field_validator("subject", mode="before")
    @classmethod
    def normalize_subject(cls, value: Any) -> str | None:
        return normalize_optional_title(value, field="subject")

    @field_validator("cc", mode="before")
    @classmethod
    def normalize_cc(cls, value: Any) -> tuple[str, ...] | None:
        return normalize_email_list(value, field="cc", maximum=10, allow_empty=True)

    @field_validator("text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        return normalize_public_text(value, field="text", minimum=1, maximum=10_000)


class QuoSmsArguments(PropertyArguments):
    to_phone: str
    text: str
    # The line to send from, by its number (context.RoutingPolicy.
    # quo_sending_lines). Required of every new text -- context derivation
    # refuses one without it -- but optional here so texts recorded before
    # it existed still parse and hash as stored (LATER_OPTIONAL_ARGUMENTS).
    # Only stripped here: the map's keys are E.164, so a malformed value is
    # simply not a line, and its refusal lists the lines it can send from.
    from_phone: str | None = None

    @field_validator("to_phone", mode="before")
    @classmethod
    def normalize_to_phone(cls, value: Any) -> str:
        return normalize_target_phone(value, field="to_phone")

    @field_validator("from_phone", mode="before")
    @classmethod
    def normalize_from_phone(cls, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("from_phone must be a string")
        return value.strip() or None

    @field_validator("text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        return normalize_public_text(value, field="text", minimum=1, maximum=10_000)


class CliqArguments(StrictModel):
    channel_or_chat_id: str
    text: str

    @field_validator("channel_or_chat_id", mode="before")
    @classmethod
    def normalize_channel_or_chat_id(cls, value: Any) -> str:
        return normalize_target_id(value, field="channel_or_chat_id")

    @field_validator("text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        return normalize_public_text(value, field="text", minimum=1, maximum=10_000)


class CalendarCreateArguments(PropertyArguments):
    calendar_id: str
    description: str | None = None
    # Optional event details. Defaults suit a tour: "Tour — <prospect>", 30
    # minutes, at the property. A maintenance visit sets its own.
    title: str | None = None
    duration_minutes: int | None = Field(default=None, strict=True, ge=1, le=480)
    location: str | None = None
    attendees: tuple[str, ...] | None = None

    @field_validator("title", mode="before")
    @classmethod
    def normalize_title(cls, value: Any) -> str | None:
        return normalize_optional_title(value, field="title")

    @field_validator("location", mode="before")
    @classmethod
    def normalize_location(cls, value: Any) -> str | None:
        return normalize_optional_title(value, field="location")

    @field_validator("attendees", mode="before")
    @classmethod
    def normalize_attendees(cls, value: Any) -> tuple[str, ...] | None:
        return normalize_email_list(value, field="attendees", maximum=20)

    @field_validator("calendar_id", mode="before")
    @classmethod
    def normalize_calendar_id(cls, value: Any) -> str:
        return normalize_target_id(value, field="calendar_id")

    @field_validator("description", mode="before")
    @classmethod
    def normalize_description(cls, value: Any) -> str | None:
        if value is None:
            return None
        return normalize_public_text(value, field="description", minimum=0, maximum=10_000)


class CalendarUpdateArguments(PropertyArguments):
    calendar_id: str
    event_url: str | None = None
    etag: str | None = None
    event_uid: str | None = None
    description: str | None = None
    # Optional event details. Defaults suit a tour: "Tour — <prospect>", 30
    # minutes, at the property. A maintenance visit sets its own.
    title: str | None = None
    duration_minutes: int | None = Field(default=None, strict=True, ge=1, le=480)
    location: str | None = None
    attendees: tuple[str, ...] | None = None

    @field_validator("title", mode="before")
    @classmethod
    def normalize_title(cls, value: Any) -> str | None:
        return normalize_optional_title(value, field="title")

    @field_validator("location", mode="before")
    @classmethod
    def normalize_location(cls, value: Any) -> str | None:
        return normalize_optional_title(value, field="location")

    @field_validator("attendees", mode="before")
    @classmethod
    def normalize_attendees(cls, value: Any) -> tuple[str, ...] | None:
        return normalize_email_list(value, field="attendees", maximum=20)

    @field_validator("calendar_id", mode="before")
    @classmethod
    def normalize_calendar_id(cls, value: Any) -> str:
        return normalize_target_id(value, field="calendar_id")

    @field_validator("event_url", mode="before")
    @classmethod
    def normalize_event_url_field(cls, value: Any) -> str | None:
        return normalize_event_url(value, field="event_url")

    @field_validator("etag", mode="before")
    @classmethod
    def normalize_etag_field(cls, value: Any) -> str | None:
        return normalize_optional_target_id(value, field="etag")

    @field_validator("event_uid", mode="before")
    @classmethod
    def normalize_event_uid_field(cls, value: Any) -> str | None:
        return normalize_optional_target_id(value, field="event_uid")

    @field_validator("description", mode="before")
    @classmethod
    def normalize_description(cls, value: Any) -> str | None:
        if value is None:
            return None
        return normalize_public_text(value, field="description", minimum=0, maximum=10_000)


class CalendarDeleteArguments(PropertyArguments):
    calendar_id: str
    event_url: str | None = None
    etag: str | None = None
    event_uid: str | None = None

    @field_validator("calendar_id", mode="before")
    @classmethod
    def normalize_calendar_id(cls, value: Any) -> str:
        return normalize_target_id(value, field="calendar_id")

    @field_validator("event_url", mode="before")
    @classmethod
    def normalize_event_url_field(cls, value: Any) -> str | None:
        return normalize_event_url(value, field="event_url")

    @field_validator("etag", mode="before")
    @classmethod
    def normalize_etag_field(cls, value: Any) -> str | None:
        return normalize_optional_target_id(value, field="etag")

    @field_validator("event_uid", mode="before")
    @classmethod
    def normalize_event_uid_field(cls, value: Any) -> str | None:
        return normalize_optional_target_id(value, field="event_uid")


PositiveBigInt = Annotated[int, Field(strict=True, gt=0, le=9_223_372_036_854_775_807)]


def normalize_tenantcloud_text(value: Any, *, field: str, maximum: int) -> str:
    normalized = normalize_public_text(value, field=field, minimum=1, maximum=maximum)
    if normalized != normalized.strip():
        raise ValueError(f"{field} must not have surrounding whitespace")
    if any(category(character) == "Cc" and character not in {"\n", "\t"} for character in normalized):
        raise ValueError(f"{field} contains unsupported control characters")
    return normalized


STORED_ACTION_CONTEXT = {"stored_action": True}


def refuse_tenantcloud_wide_characters(value: str, *, field: str, info: ValidationInfo) -> str:
    """Remove the characters TenantCloud cannot store, on NEW requests only.

    TenantCloud accepts the write (201) but stores the text only up to the
    first character outside the Basic Multilingual Plane -- most emoji. Wake
    27226 (2026-09-25) sent "... the lazy dog. \U0001f98a\U0001f415 ..." and the
    tenant received "... the lazy dog. ". Those characters are dropped here,
    with the zero-width joiners that glued them together, so the whole reply
    arrives and verifies; the agent never has to know. Accented letters,
    dashes and symbols such as \u2713 and \u2705 are kept.

    A stored action is rebuilt through this model on every reconcile and
    resume (OutboundActionRecord.execute_request, STORED_ACTION_CONTEXT); its
    text is left exactly as stored so it still matches its own desired state.
    """
    if (info.context or {}).get("stored_action"):
        return value
    if all(ord(character) <= 0xFFFF for character in value):
        return value
    kept = "".join(character for character in value if ord(character) <= 0xFFFF)
    kept = kept.replace("\u200d", "")
    kept = re.sub(r"[ \t]{2,}", " ", kept)
    kept = re.sub(r"[ \t]+\n", "\n", kept)
    kept = re.sub(r" ([!?.,;:])", r"\1", kept).strip()
    if not kept:
        raise ValueError(f"{field} has nothing TenantCloud can store once emoji are removed")
    return kept


def parse_iso_date(value: Any, *, field: str) -> date:
    if type(value) is not str:
        raise ValueError(f"{field} must be an ISO date string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a real YYYY-MM-DD date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field} must be a canonical YYYY-MM-DD date")
    return parsed


# TenantCloud refuses a thread message over 1000 characters with HTTP 422.
# Every rejected send through 2026-09-30 was 1025-1227 characters and every
# accepted one was at most 837; the longest body TenantCloud ever returned is
# exactly 1000 (an inbound tenant message cut off mid-sentence).
TENANTCLOUD_MESSAGE_MAX_CHARACTERS = 1000


class TenantCloudMessageArguments(StrictModel):
    thread_id: PositiveBigInt
    text: str

    @field_validator("text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any, info: ValidationInfo) -> str:
        text = normalize_tenantcloud_text(value, field="text", maximum=10_000)
        text = refuse_tenantcloud_wide_characters(text, field="text", info=info)
        # A stored action is rebuilt on reconcile and resume; the rejected
        # over-long ones must still rebuild so they can settle.
        if len(text) > TENANTCLOUD_MESSAGE_MAX_CHARACTERS and not (info.context or {}).get("stored_action"):
            raise ValueError(
                f"text is {len(text)} characters; a TenantCloud message can be at most "
                f"{TENANTCLOUD_MESSAGE_MAX_CHARACTERS} characters. Shorten it and send again"
            )
        return text


class LeadStatusArguments(StrictModel):
    lead_id: PositiveBigInt
    status: Literal["working"]


class MaintenanceCreateArguments(StrictModel):
    property_id: PositiveBigInt
    unit_id: PositiveBigInt
    category_id: PositiveBigInt
    title: str
    priority: Literal["normal"]
    initiated_at: date
    text: str
    entry_allowed: Annotated[bool, Field(strict=True)]
    available_on: date | None = None

    @field_validator("title", mode="before")
    @classmethod
    def normalize_title(cls, value: Any, info: ValidationInfo) -> str:
        title = normalize_tenantcloud_text(value, field="title", maximum=255)
        return refuse_tenantcloud_wide_characters(title, field="title", info=info)

    @field_validator("text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any, info: ValidationInfo) -> str:
        text = normalize_tenantcloud_text(value, field="text", maximum=10_000)
        return refuse_tenantcloud_wide_characters(text, field="text", info=info)

    @field_validator("initiated_at", "available_on", mode="before")
    @classmethod
    def normalize_date(cls, value: Any, info: Any) -> date | None:
        if value is None and info.field_name == "available_on":
            return None
        return parse_iso_date(value, field=info.field_name)


class MaintenanceStatusArguments(StrictModel):
    request_id: PositiveBigInt
    status: Literal[1, 2, 3]

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, value: Any) -> int:
        if type(value) is not int or value not in {1, 2, 3}:
            raise ValueError("status must be exactly 1, 2, or 3")
        return value


ArgumentModel: TypeAlias = (
    EmailArguments
    | QuoSmsArguments
    | CliqArguments
    | CalendarCreateArguments
    | CalendarUpdateArguments
    | CalendarDeleteArguments
    | TenantCloudMessageArguments
    | LeadStatusArguments
    | MaintenanceCreateArguments
    | MaintenanceStatusArguments
)


ARGUMENT_MODELS: dict[Operation, type[StrictModel]] = {
    Operation.EMAIL_SEND: EmailArguments,
    Operation.QUO_SMS_SEND: QuoSmsArguments,
    Operation.CLIQ_CHANNEL_POST: CliqArguments,
    Operation.CLIQ_CHAT_POST: CliqArguments,
    Operation.CALENDAR_CREATE: CalendarCreateArguments,
    Operation.CALENDAR_UPDATE: CalendarUpdateArguments,
    Operation.CALENDAR_DELETE: CalendarDeleteArguments,
    Operation.TENANTCLOUD_MESSAGE_SEND: TenantCloudMessageArguments,
    Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: LeadStatusArguments,
    Operation.TENANTCLOUD_MAINTENANCE_CREATE: MaintenanceCreateArguments,
    Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE: MaintenanceStatusArguments,
}


# Only Nigel's calendar is written (Dan, 2026-10-27; #4360). Staff copies
# of an event are attendee copies that follow Nigel's event on their own.
CALENDAR_WRITE_CONTRACT = 'calendar_id "nigel" only'
CALENDAR_WRITE_GUIDANCE = (
    "Calendar writes go only to Nigel's calendar (calendar_id \"nigel\"). Staff on the invite, Dan included, "
    "hold attendee copies of Nigel's event that follow it automatically: never write a dan or dan-zoho copy "
    "(refused, detail_code calendar_not_writable), and a copy still showing the old time just after a "
    "receipted update is lag, not a failure."
)


# Request recipes are selected by recipient purpose, not just transport.
# The tool description is generated from these and ARGUMENT_MODELS, so
# customer replies and staff coordination each get their own accepted shape.
OPERATION_USAGE: dict[Operation, tuple[tuple[ActionRole, IntentKind, str], ...]] = {
    Operation.EMAIL_SEND: (
        (
            ActionRole.PROSPECT_REPLY,
            IntentKind.INQUIRY_REPLY,
            "customer reply by email; sent from Nigel's mailbox; management@pfg.io is cc'd on customer emails "
            "unless you pass cc (use [] for none); attachments is a list of "
            "{filename, mime_type, content_base64}, at most 10 files and 10 MiB in total",
        ),
        (
            ActionRole.INTERNAL_NOTIFICATION,
            IntentKind.MANUAL_REVIEW_ALERT,
            "staff coordination by email, including updates about a prospect; sent from Nigel's mailbox; "
            "pass cc explicitly (use [] for none); attachments is a list of "
            "{filename, mime_type, content_base64}, at most 10 files and 10 MiB in total",
        ),
    ),
    Operation.QUO_SMS_SEND: (
        (
            ActionRole.PROSPECT_REPLY,
            IntentKind.INQUIRY_REPLY,
            "customer reply by SMS (to_phone E.164); from_phone (E.164) is the line to send from, always required",
        ),
        (
            ActionRole.INTERNAL_NOTIFICATION,
            IntentKind.MANUAL_REVIEW_ALERT,
            "staff coordination by SMS, including updates about a prospect (to_phone E.164); "
            "from_phone (E.164) is the line to send from, always required",
        ),
    ),
    Operation.CLIQ_CHAT_POST: ((ActionRole.INTERNAL_REPLY, IntentKind.INTERNAL_REPLY, "reply in the Cliq chat that woke you"),),
    Operation.CLIQ_CHANNEL_POST: (
        (ActionRole.INTERNAL_NOTIFICATION, IntentKind.MANUAL_REVIEW_ALERT, "post to a Cliq channel (lead_alert for a new lead)"),
    ),
    Operation.CALENDAR_CREATE: (
        (
            ActionRole.CALENDAR_MUTATION,
            IntentKind.SHOWING_CREATE,
            f"create an event; also needs top-level appointment_slot; {CALENDAR_WRITE_CONTRACT}",
        ),
    ),
    Operation.CALENDAR_UPDATE: (
        (ActionRole.CALENDAR_MUTATION, IntentKind.SHOWING_UPDATE, f"move or edit an event; also needs appointment_slot; {CALENDAR_WRITE_CONTRACT}"),
    ),
    Operation.CALENDAR_DELETE: ((ActionRole.CALENDAR_MUTATION, IntentKind.SHOWING_DELETE, f"delete an event; {CALENDAR_WRITE_CONTRACT}"),),
    Operation.TENANTCLOUD_MESSAGE_SEND: (
        (
            ActionRole.PROSPECT_REPLY,
            IntentKind.INQUIRY_REPLY,
            f"reply in a TenantCloud thread; text at most {TENANTCLOUD_MESSAGE_MAX_CHARACTERS} characters",
        ),
    ),
    Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: ((ActionRole.PROVIDER_MUTATION, IntentKind.TENANTCLOUD_LEAD_STATUS, "mark a lead working"),),
    Operation.TENANTCLOUD_MAINTENANCE_CREATE: (
        (ActionRole.PROVIDER_MUTATION, IntentKind.TENANTCLOUD_MAINTENANCE_CREATE, "open a maintenance request"),
    ),
    Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE: (
        (ActionRole.PROVIDER_MUTATION, IntentKind.TENANTCLOUD_MAINTENANCE_STATUS, "status 1 New, 2 In progress, 3 Resolved"),
    ),
}


# Optional in the model only so stored actions still parse; every new
# request must carry them (QuoSmsArguments.from_phone).
REQUIRED_OF_NEW_REQUESTS = frozenset({"from_phone"})


def operation_catalog() -> str:
    """One line per recipient-purpose recipe, with exact role, intent and
    argument names (optional ones marked ?), drawn from argument models."""
    lines = []
    for operation, model in ARGUMENT_MODELS.items():
        fields = ", ".join(
            name if field.is_required() or name in REQUIRED_OF_NEW_REQUESTS else f"{name}?" for name, field in model.model_fields.items()
        )
        for role, intent, purpose in OPERATION_USAGE[operation]:
            lines.append(f'{operation.value}: action_role "{role.value}", intent_kind "{intent.value}", arguments {{{fields}}} -- {purpose}')
    return "\n".join(lines)


SLOT_REQUIRED_INTENTS = frozenset(
    {
        IntentKind.SHOWING_OFFER,
        IntentKind.SHOWING_CONFIRMATION,
        IntentKind.SHOWING_RESCHEDULE,
        IntentKind.SHOWING_CREATE,
        IntentKind.SHOWING_UPDATE,
    }
)


_KNOWN_INTENT_KINDS = frozenset(member.value for member in IntentKind)


class ExecuteRequest(StrictModel):
    op: Literal["execute"]
    wakeup_event_id: PositiveBigInt
    action_role: ActionRole
    operation: Operation
    intent_kind: str
    arguments: ArgumentModel
    appointment_slot: datetime | None = None
    override: bool = False

    @model_validator(mode="before")
    @classmethod
    def validate_adapter_arguments(cls, raw: Any, info: ValidationInfo) -> Any:
        if not isinstance(raw, dict):
            return raw
        operation_value = raw.get("operation")
        try:
            operation = Operation(operation_value)
        except (TypeError, ValueError):
            return raw
        data = dict(raw)
        # Carry the caller's context (STORED_ACTION_CONTEXT) into the arguments.
        data["arguments"] = ARGUMENT_MODELS[operation].model_validate(
            raw.get("arguments"), context=info.context
        )
        return data

    @field_validator("intent_kind", mode="before")
    @classmethod
    def normalize_intent_kind(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("intent_kind must be a string")
        candidate = value.strip().casefold()
        if not candidate:
            raise ValueError("intent_kind must not be empty")
        if len(candidate) > 64:
            raise ValueError("intent_kind must be at most 64 characters")
        return candidate

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
        if self.intent_kind == IntentKind.INTERNAL_REPLY or self.action_role is ActionRole.INTERNAL_REPLY:
            if (
                self.action_role is not ActionRole.INTERNAL_REPLY
                or self.operation is not Operation.CLIQ_CHAT_POST
                or self.intent_kind != IntentKind.INTERNAL_REPLY
            ):
                raise ValueError("internal_reply requires internal_reply cliq.chat.post")
        if self.intent_kind in SLOT_REQUIRED_INTENTS and self.appointment_slot is None:
            raise ValueError("appointment_slot is required for this intent")
        known_intent = self.intent_kind in _KNOWN_INTENT_KINDS
        if self.appointment_slot is not None and self.intent_kind not in SLOT_REQUIRED_INTENTS and known_intent:
            raise ValueError("appointment_slot is forbidden for this intent")
        return self


class StatusRequest(StrictModel):
    op: Literal["status"]
    action_id: UUID


class SuggestRequest(StrictModel):
    op: Literal["suggest"]
    wakeup_event_id: PositiveBigInt


# A reason copied from an instruction instead of written: "<why>",
# "<required: ...>", "reason", "why", "...", or the hint's own words.
_TEMPLATE_REASON = re.compile(r"<[^>]*>|reason|why|\.+|add reason\b.*|\(?why you still want to send\)?\.?", re.IGNORECASE)


class ConfirmRequest(StrictModel):
    """Answer to a needs_confirmation (stale_context) result, or the agent's
    override of an action that was not sent.

    `yes` sends the blocked message unchanged, `revise` sends `arguments`
    instead (same operation, role, intent and target -- only content fields
    may change), each through a successor action (retry_of_action_id =
    action_id) that still passes every other gateway check. `no` records the
    decline and sends nothing. One answer per blocked action: repeating it
    returns the recorded result, a different answer is refused.
    wakeup_event_id is REQUIRED and must be the blocked action's own wake --
    a confirmation never crosses wakes.

    On a result that carries `override` (rejected, failed, a no-send
    duplicate, ...), `yes` with a `reason` sends the same message anyway
    through a successor action (Comm-Data-Store override_outbound_action),
    and `no` records that the agent chose not to send it; the first answer
    and reason are recorded on the refused action. A blank or template
    reason ("<why>", "reason") is no reason."""

    op: Literal["confirm"]
    wakeup_event_id: PositiveBigInt
    action_id: UUID
    decision: StaleContextDecision
    arguments: dict[str, Any] | None = None
    reason: Annotated[str, Field(max_length=500)] | None = None

    @field_validator("decision", mode="before")
    @classmethod
    def normalize_decision(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().casefold()
        return value

    @field_validator("reason", mode="before")
    @classmethod
    def normalize_reason(cls, value: Any) -> Any:
        if isinstance(value, str):
            value = value.strip()
            return None if not value or _TEMPLATE_REASON.fullmatch(value) else value
        return value

    @model_validator(mode="after")
    def validate_revision(self) -> ConfirmRequest:
        if self.decision is StaleContextDecision.REVISE:
            if not self.arguments:
                raise ValueError("decision revise requires arguments with the revised message")
        elif self.arguments is not None:
            raise ValueError("arguments are only accepted with decision revise")
        return self


# The argument keys a `revise` answer may change, per operation: content only.
# Every other key names the target (recipient, chat, thread, calendar, lead,
# request) or the effect itself, and must stay exactly as it was refused.
REVISABLE_ARGUMENT_KEYS: dict[Operation, frozenset[str]] = {
    Operation.EMAIL_SEND: frozenset({"text"}),
    Operation.QUO_SMS_SEND: frozenset({"text"}),
    Operation.CLIQ_CHANNEL_POST: frozenset({"text"}),
    Operation.CLIQ_CHAT_POST: frozenset({"text"}),
    Operation.CALENDAR_CREATE: frozenset({"description"}),
    Operation.CALENDAR_UPDATE: frozenset({"description"}),
    Operation.CALENDAR_DELETE: frozenset(),
    Operation.TENANTCLOUD_MESSAGE_SEND: frozenset({"text"}),
    Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: frozenset(),
    Operation.TENANTCLOUD_MAINTENANCE_CREATE: frozenset({"title", "text"}),
    Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE: frozenset(),
}


OutboundRequest: TypeAlias = Annotated[
    ExecuteRequest | StatusRequest | SuggestRequest | ConfirmRequest,
    Field(discriminator="op"),
]
_REQUEST_ADAPTER = TypeAdapter(OutboundRequest)


def parse_outbound_request(payload: Any) -> OutboundRequest:
    return _REQUEST_ADAPTER.validate_python(_default_execute_op(payload))


def _default_execute_op(payload: Any) -> Any:
    """Fill a missing `op` with "execute" when the request can only be an execute.

    Agents often send a complete send request without `op`, and the discriminator
    error it produces does not tell them what to add. Only execute carries
    `operation`; status and confirm carry `action_id` instead, so a request with
    `operation` and no `action_id` is unambiguous. Everything else is left for the
    normal validation error."""
    if (
        isinstance(payload, dict)
        and "op" not in payload
        and "operation" in payload
        and "action_id" not in payload
    ):
        return {"op": "execute", **payload}
    return payload


_SENT_DIRECTIONS = frozenset({"outbound", "outgoing", "sent"})


@dataclass(frozen=True)
class NewerActivity:
    """One message newer than the wake's context: received from the
    recipient, or sent by us to it (a message, or another wake's gateway
    send)."""

    direction: str
    source: str
    occurred_at: datetime
    preview: str
    message_id: int | None
    action_id: UUID | None
    sender: str | None = None

    @property
    def ref(self) -> str:
        """Stable identity of this item: what a question records as shown,
        and the only thing a later answer can waive."""
        return f"message:{self.message_id}" if self.message_id else f"action:{self.action_id}"

    @property
    def arm(self) -> str:
        return "messages" if self.message_id else "outbound_actions"

    @property
    def label(self) -> str:
        """What the agent reads: received, or sent by us."""
        return "sent by us" if self.direction.casefold() in _SENT_DIRECTIONS else "received"


class ContextItem(StrictModel):
    """One piece of activity newer than the agent's context watermark."""

    id: str
    source: str
    direction: str
    sender: str | None = None
    occurred_at: datetime
    preview: Annotated[str, Field(max_length=300)]


class RequestRefusedError(ValueError):
    """The gateway refused this request as asked. Nothing was sent, and the
    message tells the caller what to do instead: fix the request, answer
    differently, or record needs_human. The tool answers it as an ordinary
    rejected result, not an MCP tool error, because hermes-agent counts tool
    errors toward a breaker that parks the gateway for every caller (#3463).
    A gateway fault is never one of these.

    detail_code is what the tool answers with: the generic request_refused,
    unless a subclass names a more specific, stable reason."""

    detail_code = "request_refused"


class OverrideRequest(StrictModel):
    """The outbound_action request that still sends a message that was not
    sent: confirm yes on the refused action. The agent adds its own
    `reason`; there is deliberately no placeholder to echo back."""

    op: Literal["confirm"] = "confirm"
    wakeup_event_id: PositiveBigInt
    action_id: UUID
    decision: Literal["yes"] = "yes"


class AlreadySent(StrictModel):
    """A completed send of exactly the same request (operation, recipient and
    arguments) by another action: what the agent must see before it
    overrides a refusal. 2026-10-17: one wake emailed an attorney a case
    packet; 21 minutes later a second wake's identical send was refused with
    only "nothing was sent", the agent overrode it, and the attorney got the
    packet twice."""

    action_id: UUID
    wakeup_event_id: int | None = None
    provider_message_id: str | None = None
    sent_at: datetime | None = None
    recipient: str | None = None


class RecentlyAnswered(StrictModel):
    """A customer reply another wake already sent this person (any of their
    contact keys), or is sending right now, inside Comm-Data-Store's
    contact_recently_answered_window(): what the agent must read before it
    sends another. 2026-09-28
    (wakes 27331/27332, one tour request on two lanes) and 2026-10-09 (Amy
    McDermott's repeated TenantCloud inquiry, wake 28084): a second wake
    answered a prospect who already had an answer, in different words, so
    byte-identical idempotency never saw it."""

    action_id: UUID
    wakeup_event_id: int | None = None
    intent: str | None = None
    sent_at: datetime | None = None
    recipient: str | None = None
    text_excerpt: str | None = None
    # Comm-Data-Store prospect_reply_status: 'answered' (sent) or 'in_flight'
    # (prepared, dispatching, provider_accepted or unknown -- it may already
    # be in front of the prospect).
    reply_status: str | None = None


class PublicResult(StrictModel):
    status: PublicStatus
    action_id: UUID
    action_uid: UUID | None
    provider_request_ref: str | None
    # True only for needs_confirmation: the agent may answer with op=confirm.
    # Every other result keeps the historical False.
    retryable: bool = False
    detail_code: Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[a-z0-9_]+$")]
    # Human-readable elaboration of detail_code: what happened and what to do
    # next. Set on every result that did not send (and on pending/unknown);
    # None on a plain send, and then omitted from the wire (server.py drops
    # every None optional key).
    detail: str | None = None
    # needs_confirmation only: every item newer than the context the agent
    # was shown (newest last, capped) and the exact yes/no question. None
    # everywhere else, and omitted from the wire like `detail`.
    new_context: tuple[ContextItem, ...] | None = None
    question: str | None = None
    # Set when nothing was sent and the agent may still send it unchanged:
    # the request to make (record.overridable). None otherwise, and omitted
    # from the wire like `detail`.
    override: OverrideRequest | None = None
    # Set with `override` when another action already delivered this exact
    # request (newest first): the agent sees the earlier send and its message
    # id before it decides. None otherwise, and omitted from the wire.
    already_sent: tuple[AlreadySent, ...] | None = None
    # Set with `override` when this person already got (or is getting) a
    # customer reply from another wake inside the recently-answered window
    # (recently_answered, newest first). None otherwise, and omitted from
    # the wire.
    recently_answered: tuple[RecentlyAnswered, ...] | None = None
