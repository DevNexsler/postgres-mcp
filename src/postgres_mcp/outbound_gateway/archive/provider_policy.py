"""Archive of the retired provider-policy layer (staged-rollout blast-radius control).

Origin:
- src/postgres_mcp/outbound_gateway/server.py, lines 89-97 and 277-335
- src/postgres_mcp/outbound_gateway/models.py, lines 407-452

Archived: 2026-08-27

See ../archive/README.md for failure narrative and revival requirements.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from postgres_mcp.outbound_gateway.models import IntentKind
    from postgres_mcp.outbound_gateway.models import Operation


DEFAULT_ENABLED_OPERATIONS_BY_PROVIDER = {
    "hotpads": frozenset({"email.send"}),
    "zillow": frozenset({"email.send"}),
}
DEFAULT_ENABLED_INTENTS = frozenset({"inquiry_reply", "showing_offer"})
DEFAULT_ENABLED_INTENTS_BY_PROVIDER = {
    "hotpads": frozenset({"inquiry_reply", "showing_offer"}),
    "zillow": frozenset({"inquiry_reply", "showing_offer"}),
}

ALLOWED_COMBINATIONS: frozenset[tuple[str, str, str]] = frozenset(
    {
        (role, operation, intent)
        for role, operations, intents in (
            (
                "prospect_reply",
                ("email.send", "quo.sms.send"),
                (
                    "inquiry_reply",
                    "showing_offer",
                    "showing_confirmation",
                    "showing_reschedule",
                    "showing_cancellation",
                ),
            ),
            (
                "internal_notification",
                ("cliq.channel.post", "cliq.chat.post"),
                ("lead_alert", "manual_review_alert"),
            ),
        )
        for operation in operations
        for intent in intents
    }
    | {
        ("calendar_mutation", "calendar.create", "showing_create"),
        ("calendar_mutation", "calendar.update", "showing_update"),
        ("calendar_mutation", "calendar.delete", "showing_delete"),
        ("prospect_reply", "tenantcloud.message.send", "inquiry_reply"),
        (
            "provider_mutation",
            "tenantcloud.lead.status.update",
            "tenantcloud_lead_status",
        ),
        (
            "provider_mutation",
            "tenantcloud.maintenance.create",
            "tenantcloud_maintenance_create",
        ),
        (
            "provider_mutation",
            "tenantcloud.maintenance.status.update",
            "tenantcloud_maintenance_status",
        ),
    }
)


def _enabled_operations_by_provider() -> dict[str, frozenset[str]]:
    raw = os.environ.get("OUTBOUND_PROVIDER_OPERATIONS_JSON")
    if raw is None:
        return DEFAULT_ENABLED_OPERATIONS_BY_PROVIDER
    value = json.loads(raw)
    if not isinstance(value, dict) or not value:
        raise ValueError("OUTBOUND_PROVIDER_OPERATIONS_JSON must be a non-empty JSON object")
    parsed: dict[str, frozenset[str]] = {}
    for provider, operations in value.items():
        if (
            not isinstance(provider, str)
            or not provider.strip()
            or not isinstance(operations, list)
            or not operations
            or not all(isinstance(item, str) for item in operations)
        ):
            raise ValueError("OUTBOUND_PROVIDER_OPERATIONS_JSON values must be non-empty string arrays")
        try:
            # Import here to avoid circular dependency
            from postgres_mcp.outbound_gateway.models import Operation
            parsed[provider.casefold()] = frozenset(Operation(item).value for item in operations)
        except ValueError as exc:
            raise ValueError("OUTBOUND_PROVIDER_OPERATIONS_JSON contains an unsupported operation") from exc
    return parsed


def _enabled_intents() -> frozenset[str]:
    raw = os.environ.get("OUTBOUND_ENABLED_INTENTS_JSON")
    if raw is None:
        return DEFAULT_ENABLED_INTENTS
    value = json.loads(raw)
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise ValueError("OUTBOUND_ENABLED_INTENTS_JSON must be a non-empty string array")
    try:
        from postgres_mcp.outbound_gateway.models import IntentKind
        return frozenset(IntentKind(item).value for item in value)
    except ValueError as exc:
        raise ValueError("OUTBOUND_ENABLED_INTENTS_JSON contains an unsupported intent") from exc


def _enabled_intents_by_provider() -> dict[str, frozenset[str]]:
    raw = os.environ.get("OUTBOUND_PROVIDER_INTENTS_JSON")
    if raw is None:
        return DEFAULT_ENABLED_INTENTS_BY_PROVIDER
    value = json.loads(raw)
    if not isinstance(value, dict) or not value:
        raise ValueError("OUTBOUND_PROVIDER_INTENTS_JSON must be a non-empty JSON object")
    parsed: dict[str, frozenset[str]] = {}
    for provider, intents in value.items():
        if (
            not isinstance(provider, str)
            or not provider.strip()
            or not isinstance(intents, list)
            or not intents
            or not all(isinstance(item, str) for item in intents)
        ):
            raise ValueError("OUTBOUND_PROVIDER_INTENTS_JSON values must be non-empty string arrays")
        try:
            from postgres_mcp.outbound_gateway.models import IntentKind
            parsed[provider.casefold()] = frozenset(IntentKind(item).value for item in intents)
        except ValueError as exc:
            raise ValueError("OUTBOUND_PROVIDER_INTENTS_JSON contains an unsupported intent") from exc
    return parsed
