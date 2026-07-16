"""Server-owned canonical context derivation for outbound actions."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from datetime import timezone
from hashlib import sha256
from types import MappingProxyType
from typing import Any
from typing import Mapping
from unicodedata import normalize
from uuid import UUID
from uuid import uuid5

from .models import ActionRole
from .models import ExecuteRequest
from .models import IntentKind
from .models import Operation
from .repository import ContextRepository
from .repository import WakeEventRecord

ACTION_NAMESPACE = UUID("ed6fcf85-39e7-5cdf-9fb8-ccca32a62e8d")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_EMAIL = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_SYSTEM_EMAIL_LOCAL_PARTS = frozenset({"mailer-daemon", "no-reply", "noreply", "do-not-reply", "donotreply", "postmaster"})
_ZILLOW_PROVIDER_FAMILY = frozenset({"hotpads", "zillow", "zumper"})
_EMAIL_PARTICIPANT_TYPES = frozenset({"email", "email_address"})
_PHONE_PARTICIPANT_TYPES = frozenset({"phone", "phone_number", "sms", "tel"})
_CROSS_CHANNEL_DUPLICATE_MAX_SECONDS = 120
_ADDRESS_WORDS = {
    "n": "north",
    "s": "south",
    "e": "east",
    "w": "west",
    "st": "street",
    "rd": "road",
    "ave": "avenue",
    "blvd": "boulevard",
    "dr": "drive",
    "ln": "lane",
    "ct": "court",
}


class ContextDerivationError(ValueError):
    pass


@dataclass(frozen=True)
class DerivedTarget:
    kind: str
    target_id: str
    verified: bool


@dataclass(frozen=True)
class RoutingPolicy:
    version: str
    email_account_by_provider: Mapping[str, str]
    quo_line_by_provider: Mapping[str, str]
    calendar_by_profile: Mapping[str, str]
    cliq_target_by_intent: Mapping[str, str]
    property_aliases: Mapping[str, str]
    conversation_aliases: Mapping[str, str]
    calendar_account_by_profile: Mapping[str, str] = dataclass_field(default_factory=dict)
    enabled_operations_by_provider: Mapping[str, frozenset[str]] = dataclass_field(default_factory=dict)
    enabled_intents: frozenset[str] = frozenset()
    enabled_intents_by_provider: Mapping[str, frozenset[str]] = dataclass_field(default_factory=dict)


@dataclass(frozen=True)
class ActionContext:
    action_id: UUID
    wakeup_event_id: int
    action_role: ActionRole
    operation: Operation
    intent_kind: IntentKind
    appointment_slot: datetime | None
    arguments: Mapping[str, Any]
    source: str
    source_message_id: int
    source_message_key: str
    source_sent_at: datetime
    conversation_id: str
    conversation_watermark: int
    prospect_id: str
    aliases: tuple[str, ...]
    property_id: str | None
    property_label: str | None
    target: DerivedTarget
    provider_account: str
    routing_policy_version: str
    canonical_scope: Mapping[str, Any]
    canonical_context: Mapping[str, Any]
    payload_hash: str
    lock_holder: str
    thread_identity: str
    showing_lifecycle_id: str
    calendar_event_uid: str | None
    source_subject: str | None = None
    prospect_name: str | None = None
    recipient_phone: str | None = None
    calendar_event_url: str | None = None
    calendar_event_etag: str | None = None
    channel_id: int = 0
    refresh_evidence: Mapping[str, Any] = dataclass_field(default_factory=dict)
    cross_channel_duplicate_message_ids: tuple[int, ...] = ()


def normalize_string(value: str) -> str:
    return normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))


def normalize_property_key(value: str | None) -> str:
    tokens = _NON_ALNUM.sub(" ", normalize_string(value or "").casefold()).split()
    return " ".join(_ADDRESS_WORDS.get(token, token) for token in tokens)


def normalize_phone(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(character for character in value if character.isdigit())
    if len(digits) == 10:
        digits = "1" + digits
    return f"+{digits}" if 10 < len(digits) <= 15 else None


def replyable_email(value: str | None, *, require_zillow_proxy: bool = False) -> str | None:
    address = _nonblank(value)
    if not address:
        return None
    address = address.casefold()
    if not _EMAIL.fullmatch(address):
        return None
    local_part, domain = address.rsplit("@", 1)
    normalized_local = local_part.replace("_", "-").replace(".", "-")
    if normalized_local in _SYSTEM_EMAIL_LOCAL_PARTS or any(
        token in normalized_local for token in ("do-not-reply", "donotreply", "no-reply", "noreply")
    ):
        return None
    if require_zillow_proxy and domain != "convo.zillow.com":
        return None
    return address


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _nonblank(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    result = normalize_string(value).strip()
    return result or None


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonicalize(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, str):
        return normalize_string(value)
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, UUID):
        return str(value)
    return value


def canonical_payload_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _canonicalize(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


class ActionContextLoader:
    def __init__(self, repository: ContextRepository, policy: RoutingPolicy):
        self._repository = repository
        self._policy = policy

    async def load(self, request: ExecuteRequest) -> ActionContext:
        record = await self._repository.load_wake_event(request.wakeup_event_id)
        if record is None:
            raise ContextDerivationError("wakeup event does not exist")
        envelope = _mapping(record.envelope)
        message = _mapping(envelope.get("message"))
        raw = _mapping(record.raw_payload)
        provider = self._provider(record, raw, message)
        if self._policy.enabled_operations_by_provider:
            allowed_operations = self._policy.enabled_operations_by_provider.get(
                provider,
                frozenset(),
            )
            if request.operation.value not in allowed_operations:
                raise ContextDerivationError("provider operation is disabled")
        if self._policy.enabled_intents and request.intent_kind.value not in self._policy.enabled_intents:
            raise ContextDerivationError("intent is disabled")
        if self._policy.enabled_intents_by_provider:
            provider_intents = self._policy.enabled_intents_by_provider.get(
                provider,
                frozenset(),
            )
            if request.intent_kind.value not in provider_intents:
                raise ContextDerivationError("provider intent is disabled")
        property_label = self._property_label(record, raw, message)
        property_scope = normalize_property_key(property_label)
        property_id = self._policy.property_aliases.get(property_scope)
        if property_scope and property_id is None:
            property_id = f"property:{property_scope.replace(' ', '-')}"

        aliases = self._aliases(record, envelope, message, raw)
        if not aliases and request.action_role is not ActionRole.INTERNAL_NOTIFICATION:
            raise ContextDerivationError("no stable prospect aliases")
        if aliases:
            resolved = await self._repository.resolve_canonical_subject(aliases, property_scope)
            if resolved.ambiguous:
                raise ContextDerivationError("prospect aliases are ambiguous")
            prospect_id = resolved.canonical_subject or f"prospect:{self._preferred_alias(aliases)}"
        else:
            prospect_id = "internal:none"

        thread_identity = self._thread_identity(record, raw, message, provider)
        conversation_provider = "zillow" if provider in _ZILLOW_PROVIDER_FAMILY else provider
        raw_conversation = f"{conversation_provider}:{thread_identity}"
        conversation_id = self._policy.conversation_aliases.get(
            raw_conversation,
            f"conversation:{raw_conversation}",
        )
        target, provider_account = self._target(
            request,
            record,
            provider,
            thread_identity,
            message,
            raw,
        )
        if not target.verified:
            raise ContextDerivationError("verified target could not be derived")

        requires_property = request.intent_kind not in {
            IntentKind.INQUIRY_REPLY,
            IntentKind.LEAD_ALERT,
            IntentKind.MANUAL_REVIEW_ALERT,
        }
        if requires_property and property_id is None:
            raise ContextDerivationError("verified property could not be derived")

        action_id = uuid5(
            ACTION_NAMESPACE,
            f"v1:wakeup:{request.wakeup_event_id}:role:{request.action_role}:ordinal:0",
        )
        showing_lifecycle_id = (
            _nonblank(raw.get("showing_lifecycle_id")) or _nonblank(raw.get("booking_id")) or f"showing:wake:{request.wakeup_event_id}"
        )
        calendar_event_uid = _nonblank(raw.get("calendar_event_uid")) or _nonblank(_mapping(raw.get("booking")).get("calendar_event_uid"))
        booking = _mapping(raw.get("booking"))
        calendar_event_url = _nonblank(raw.get("calendar_event_url")) or _nonblank(booking.get("calendar_event_url"))
        calendar_event_etag = _nonblank(raw.get("calendar_event_etag")) or _nonblank(booking.get("calendar_event_etag"))
        if request.operation in {Operation.CALENDAR_UPDATE, Operation.CALENDAR_DELETE}:
            if not calendar_event_uid:
                raise ContextDerivationError("canonical calendar event UID is required")
            if not calendar_event_url or not calendar_event_etag:
                raise ContextDerivationError("canonical calendar event revision is required")

        source_subject = _nonblank(record.subject)
        prospect_name = _nonblank(message.get("prospect_name")) or _nonblank(record.display_name)
        recipient_phone = normalize_phone(
            _nonblank(message.get("phone")) or (record.participant_key if record.participant_type in _PHONE_PARTICIPANT_TYPES else None)
        )
        refresh_evidence = _mapping(raw.get("zillow_refresh") or envelope.get("zillow_refresh"))
        cross_channel_duplicate_message_ids = self._cross_channel_duplicate_message_ids(
            record,
            envelope,
        )

        canonical_scope = self._scope(
            request,
            record,
            prospect_id,
            conversation_id,
            property_id,
            target,
            showing_lifecycle_id,
            calendar_event_uid,
        )
        arguments = MappingProxyType(request.arguments.model_dump(mode="json", exclude_none=False))
        appointment_slot = request.appointment_slot
        canonical_message_id = record.canonical_message_id or record.message_id
        conversation_watermark = self._event_conversation_watermark(record, envelope)
        canonical_context_data = {
            "identity_version": "v1",
            "source_message_id": canonical_message_id,
            "source_message_key": f"{record.message_source}:{record.source_message_id}",
            "conversation_id": conversation_id,
            "conversation_watermark": conversation_watermark,
            "prospect_id": prospect_id,
            "property_id": property_id,
            "target": {"kind": target.kind, "target_id": target.target_id},
            "provider_account": provider_account,
            "routing_policy_version": self._policy.version,
            "showing_lifecycle_id": showing_lifecycle_id,
            "calendar_event_uid": calendar_event_uid,
            "calendar_event_url": calendar_event_url,
            "calendar_event_etag": calendar_event_etag,
            "source_subject": source_subject,
            "prospect_name": prospect_name,
            "recipient_phone": recipient_phone,
            "channel_id": record.channel_id,
            "refresh_evidence": refresh_evidence,
            "cross_channel_duplicate_message_ids": list(cross_channel_duplicate_message_ids),
        }
        canonical_context = MappingProxyType(canonical_context_data)
        payload_hash = canonical_payload_hash(
            {
                "action_role": request.action_role.value,
                "operation": request.operation.value,
                "intent_kind": request.intent_kind.value,
                "appointment_slot": appointment_slot,
                "arguments": arguments,
                "canonical_context": canonical_context,
            }
        )
        return ActionContext(
            action_id=action_id,
            wakeup_event_id=request.wakeup_event_id,
            action_role=request.action_role,
            operation=request.operation,
            intent_kind=request.intent_kind,
            appointment_slot=appointment_slot,
            arguments=arguments,
            source=provider,
            source_message_id=canonical_message_id,
            source_message_key=f"{record.message_source}:{record.source_message_id}",
            source_sent_at=record.message_sent_at,
            conversation_id=conversation_id,
            conversation_watermark=conversation_watermark,
            prospect_id=prospect_id,
            aliases=aliases,
            property_id=property_id,
            property_label=property_label,
            target=target,
            provider_account=provider_account,
            routing_policy_version=self._policy.version,
            canonical_scope=MappingProxyType(canonical_scope),
            canonical_context=canonical_context,
            payload_hash=payload_hash,
            lock_holder=f"outbound-gateway:{action_id}",
            thread_identity=thread_identity,
            showing_lifecycle_id=showing_lifecycle_id,
            calendar_event_uid=calendar_event_uid,
            source_subject=source_subject,
            prospect_name=prospect_name,
            recipient_phone=recipient_phone,
            calendar_event_url=calendar_event_url,
            calendar_event_etag=calendar_event_etag,
            channel_id=record.channel_id,
            refresh_evidence=MappingProxyType(dict(refresh_evidence)),
            cross_channel_duplicate_message_ids=cross_channel_duplicate_message_ids,
        )

    @staticmethod
    def _cross_channel_duplicate_message_ids(
        record: WakeEventRecord,
        envelope: Mapping[str, Any],
    ) -> tuple[int, ...]:
        routing_hints = _mapping(envelope.get("routing_hints"))
        hint = _mapping(routing_hints.get("potential_cross_channel_duplicate"))
        duplicate_id = hint.get("duplicate_of_message_id")
        duplicate_source = _nonblank(hint.get("duplicate_of_source"))
        duplicate_sent_at = _nonblank(hint.get("duplicate_of_sent_at"))
        if (
            isinstance(duplicate_id, bool)
            or not isinstance(duplicate_id, int)
            or duplicate_id <= 0
            or not duplicate_source
            or duplicate_source.casefold() == record.message_source.casefold()
            or not duplicate_sent_at
            or record.message_sent_at.tzinfo is None
        ):
            return ()
        try:
            parsed_sent_at = datetime.fromisoformat(duplicate_sent_at.replace("Z", "+00:00"))
        except ValueError:
            return ()
        if parsed_sent_at.tzinfo is None:
            return ()
        delta_seconds = abs(
            (
                parsed_sent_at.astimezone(timezone.utc)
                - record.message_sent_at.astimezone(timezone.utc)
            ).total_seconds()
        )
        if delta_seconds > _CROSS_CHANNEL_DUPLICATE_MAX_SECONDS:
            return ()
        return (duplicate_id,)

    @staticmethod
    def _provider(
        record: WakeEventRecord,
        raw: Mapping[str, Any],
        message: Mapping[str, Any],
    ) -> str:
        explicit = _nonblank(raw.get("provider"))
        if explicit:
            return explicit.casefold()
        participant_proxy = replyable_email(
            record.participant_key if record.participant_type in _EMAIL_PARTICIPANT_TYPES else None,
            require_zillow_proxy=True,
        )
        if (
            record.message_source == "zillow_rm_web_extract"
            or message.get("proxy_email")
            or message.get("zillow_proxy_email")
            or raw.get("proxy_email")
            or raw.get("zillow_proxy_email")
            or participant_proxy
        ):
            return "zillow"
        if record.message_source == "quo":
            return "quo"
        if record.message_source == "zoho_cliq":
            return "cliq"
        return record.message_source.casefold()

    @staticmethod
    def _property_label(
        record: WakeEventRecord,
        raw: Mapping[str, Any],
        message: Mapping[str, Any],
    ) -> str | None:
        direct = _nonblank(message.get("property")) or _nonblank(raw.get("property"))
        if direct:
            return direct
        subject = record.subject or ""
        match = re.search(r"\bfor\s+(.+)$", subject, re.IGNORECASE)
        return _nonblank(match.group(1)) if match else None

    @staticmethod
    def _aliases(
        record: WakeEventRecord,
        envelope: Mapping[str, Any],
        message: Mapping[str, Any],
        raw: Mapping[str, Any],
    ) -> tuple[str, ...]:
        values: set[str] = set()
        identity = _mapping(envelope.get("identity"))
        factbook = _nonblank(identity.get("factbook_entity_uuid"))
        if factbook:
            values.add(f"factbook:{factbook.casefold()}")
        for candidate in (
            message.get("direct_email"),
            message.get("proxy_email"),
            record.participant_key if record.participant_type in _EMAIL_PARTICIPANT_TYPES else None,
        ):
            email = _nonblank(candidate)
            if email and _EMAIL.fullmatch(email):
                values.add(f"email:{email.casefold()}")
        phone = normalize_phone(
            _nonblank(message.get("phone")) or (record.participant_key if record.participant_type in _PHONE_PARTICIPANT_TYPES else None)
        )
        if phone:
            values.add(f"phone:{phone}")
        for field in ("prospect_id", "contact_id", "zillow_contact_id"):
            value = _nonblank(raw.get(field))
            if value:
                values.add(f"{field}:{value.casefold()}")
        return tuple(sorted(values))

    @staticmethod
    def _preferred_alias(aliases: tuple[str, ...]) -> str:
        for prefix in ("factbook:", "prospect_id:", "contact_id:", "email:", "phone:"):
            match = next((alias for alias in aliases if alias.startswith(prefix)), None)
            if match:
                return match
        return aliases[0]

    @staticmethod
    def _thread_identity(
        record: WakeEventRecord,
        raw: Mapping[str, Any],
        message: Mapping[str, Any],
        provider: str,
    ) -> str:
        if provider in _ZILLOW_PROVIDER_FAMILY:
            for candidate in (
                message.get("proxy_email"),
                message.get("zillow_proxy_email"),
                raw.get("proxy_email"),
                raw.get("zillow_proxy_email"),
                record.participant_key if record.participant_type in _EMAIL_PARTICIPANT_TYPES else None,
            ):
                proxy = replyable_email(
                    _nonblank(candidate),
                    require_zillow_proxy=True,
                )
                if proxy:
                    return proxy
        nested = _mapping(_mapping(raw.get("data")).get("object"))
        if provider == "quo":
            conversation = _nonblank(nested.get("conversationId") or nested.get("conversation_id"))
            if conversation:
                return conversation
            line = _nonblank(nested.get("phoneNumberId") or nested.get("phone_number_id") or nested.get("to"))
            prospect = normalize_phone(
                _nonblank(message.get("phone")) or (record.participant_key if record.participant_type in _PHONE_PARTICIPANT_TYPES else None)
            )
            if line and prospect:
                return f"line:{line.casefold()}:prospect:{prospect}"
        for field in ("thread_id", "conversation_id", "channel_id"):
            value = _nonblank(raw.get(field))
            if value:
                return value
        return record.source_channel_id

    @staticmethod
    def _event_conversation_watermark(
        record: WakeEventRecord,
        envelope: Mapping[str, Any],
    ) -> int:
        conversation = _mapping(envelope.get("conversation_context"))
        nearby = conversation.get("nearby_messages")
        message_ids = [record.message_id]
        if isinstance(nearby, list):
            message_ids.extend(value for item in nearby if isinstance(item, Mapping) and isinstance((value := item.get("id")), int))
        return max(message_ids)

    def _target(
        self,
        request: ExecuteRequest,
        record: WakeEventRecord,
        provider: str,
        thread_identity: str,
        message: Mapping[str, Any],
        raw: Mapping[str, Any],
    ) -> tuple[DerivedTarget, str]:
        if request.operation is Operation.EMAIL_SEND:
            proxy = replyable_email(
                _nonblank(message.get("proxy_email") or message.get("zillow_proxy_email") or raw.get("proxy_email") or raw.get("zillow_proxy_email")),
                require_zillow_proxy=provider in _ZILLOW_PROVIDER_FAMILY,
            )
            direct = replyable_email(_nonblank(message.get("direct_email")))
            participant = replyable_email(
                record.participant_key
                if record.participant_type in _EMAIL_PARTICIPANT_TYPES and record.channel_type in {"email", "email_thread"}
                else None,
                require_zillow_proxy=provider in _ZILLOW_PROVIDER_FAMILY,
            )
            target = proxy or participant if provider in _ZILLOW_PROVIDER_FAMILY else proxy or direct or participant
            target = target or ""
            account = self._policy.email_account_by_provider.get(provider, "")
            return DerivedTarget("email_thread", target, bool(account and _EMAIL.fullmatch(target))), account
        if request.operation is Operation.QUO_SMS_SEND:
            phone = normalize_phone(
                _nonblank(message.get("phone"))
                or (_nonblank(record.participant_key) if record.participant_type in _PHONE_PARTICIPANT_TYPES else None)
            )
            account = self._policy.quo_line_by_provider.get(provider, "")
            nested = _mapping(_mapping(raw.get("data")).get("object"))
            observed_account = _nonblank(nested.get("phoneNumberId") or nested.get("phone_number_id"))
            target_id = thread_identity if provider == "quo" else phone or ""
            return DerivedTarget(
                "quo_conversation",
                target_id,
                bool(account and target_id and phone and (not observed_account or observed_account == account)),
            ), account
        if request.operation in {Operation.CLIQ_CHANNEL_POST, Operation.CLIQ_CHAT_POST}:
            target_id = self._policy.cliq_target_by_intent.get(request.intent_kind.value, "")
            kind = "cliq_channel" if request.operation is Operation.CLIQ_CHANNEL_POST else "cliq_chat"
            return DerivedTarget(kind, target_id, bool(target_id)), target_id
        profile = "appointment-setter"
        calendar = self._policy.calendar_by_profile.get(profile, "")
        account = self._policy.calendar_account_by_profile.get(profile, calendar)
        return DerivedTarget("calendar", calendar, bool(calendar and account)), account

    @staticmethod
    def _scope(
        request: ExecuteRequest,
        record: WakeEventRecord,
        prospect_id: str,
        conversation_id: str,
        property_id: str | None,
        target: DerivedTarget,
        showing_lifecycle_id: str,
        calendar_event_uid: str | None,
    ) -> dict[str, Any]:
        if request.action_role is ActionRole.PROSPECT_REPLY:
            return {
                "version": "v1",
                "prospect_id": prospect_id,
                "conversation_id": conversation_id,
                "source_message_id": record.canonical_message_id or record.message_id,
                "role": request.action_role.value,
            }
        if request.action_role is ActionRole.CALENDAR_MUTATION:
            return {
                "version": "v1",
                "prospect_id": prospect_id,
                "property_id": property_id,
                "showing_lifecycle_id": showing_lifecycle_id,
                "calendar_event_uid": calendar_event_uid,
                "trigger_event_id": record.wakeup_event_id,
                "operation": request.operation.value,
            }
        return {
            "version": "v1",
            "source_message_id": record.canonical_message_id or record.message_id,
            "target_id": target.target_id,
            "intent_kind": request.intent_kind.value,
            "role": request.action_role.value,
        }
