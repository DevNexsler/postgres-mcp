from datetime import timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from postgres_mcp.outbound_gateway.models import CalendarDescriptionArguments
from postgres_mcp.outbound_gateway.models import EmptyArguments
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import StatusRequest
from postgres_mcp.outbound_gateway.models import TextArguments
from postgres_mcp.outbound_gateway.models import parse_outbound_request


def execute_payload(**overrides):
    payload = {
        "op": "execute",
        "wakeup_event_id": 12345,
        "action_role": "prospect_reply",
        "operation": "email.send",
        "intent_kind": "showing_offer",
        "appointment_slot": "2026-07-17T10:30:00-04:00",
        "arguments": {"text": "Hello"},
    }
    payload.update(overrides)
    return payload


def test_execute_has_exact_required_top_level_contract():
    request = parse_outbound_request(execute_payload())
    assert isinstance(request, ExecuteRequest)
    assert request.wakeup_event_id == 12345
    assert request.appointment_slot.isoformat() == "2026-07-17T14:30:00+00:00"
    assert request.appointment_slot.tzinfo == timezone.utc
    assert isinstance(request.arguments, TextArguments)

    for field in (
        "op",
        "wakeup_event_id",
        "action_role",
        "operation",
        "intent_kind",
        "arguments",
    ):
        invalid = execute_payload()
        invalid.pop(field)
        with pytest.raises(ValidationError):
            parse_outbound_request(invalid)


def test_execute_rejects_unknown_fields_and_non_positive_or_non_integer_wake_ids():
    with pytest.raises(ValidationError, match="extra"):
        parse_outbound_request(execute_payload(recipient="victim@example.com"))
    for value in (0, -1, 1.5, "123"):
        with pytest.raises(ValidationError):
            parse_outbound_request(execute_payload(wakeup_event_id=value))


@pytest.mark.parametrize(
    ("operation", "role", "intent", "slot", "arguments", "argument_type"),
    [
        ("email.send", "prospect_reply", "inquiry_reply", None, {"text": "Email"}, TextArguments),
        ("quo.sms.send", "prospect_reply", "showing_offer", "2026-07-17T14:30:00Z", {"text": "SMS"}, TextArguments),
        ("cliq.channel.post", "internal_notification", "lead_alert", None, {"text": "Lead"}, TextArguments),
        ("cliq.chat.post", "internal_notification", "manual_review_alert", None, {"text": "Review"}, TextArguments),
        (
            "calendar.create",
            "calendar_mutation",
            "showing_create",
            "2026-07-17T14:30:00Z",
            {"description": "Tour"},
            CalendarDescriptionArguments,
        ),
        (
            "calendar.update",
            "calendar_mutation",
            "showing_update",
            "2026-07-17T14:30:00Z",
            {},
            CalendarDescriptionArguments,
        ),
        ("calendar.delete", "calendar_mutation", "showing_delete", None, {}, EmptyArguments),
    ],
)
def test_all_seven_operations_use_adapter_owned_strict_argument_schemas(operation, role, intent, slot, arguments, argument_type):
    request = parse_outbound_request(
        execute_payload(
            operation=operation,
            action_role=role,
            intent_kind=intent,
            appointment_slot=slot,
            arguments=arguments,
        )
    )
    assert request.operation == Operation(operation)
    assert isinstance(request.arguments, argument_type)


@pytest.mark.parametrize(
    "overrides",
    [
        {"operation": "email.send", "arguments": {"text": "x", "to": "a@example.com"}},
        {"operation": "calendar.create", "arguments": {"description": "x", "calendar": "nigel"}},
        {"operation": "calendar.delete", "arguments": {"event_id": "raw-id"}},
        {"operation": "not.a.provider"},
        {"action_role": "staff_approval"},
        {"intent_kind": "freeform"},
    ],
)
def test_adapter_arguments_and_enums_reject_unknown_values(overrides):
    with pytest.raises(ValidationError):
        parse_outbound_request(execute_payload(**overrides))


@pytest.mark.parametrize(
    "overrides",
    [
        {"action_role": "calendar_mutation", "operation": "email.send", "intent_kind": "showing_create"},
        {
            "action_role": "prospect_reply",
            "operation": "calendar.create",
            "intent_kind": "showing_offer",
            "arguments": {},
        },
        {"action_role": "internal_notification", "operation": "cliq.chat.post", "intent_kind": "showing_offer"},
        {"action_role": "prospect_reply", "operation": "email.send", "intent_kind": "showing_create"},
        {
            "action_role": "calendar_mutation",
            "operation": "calendar.delete",
            "intent_kind": "showing_update",
            "arguments": {},
        },
    ],
)
def test_role_operation_intent_matrix_fails_closed(overrides):
    with pytest.raises(ValidationError, match="combination"):
        parse_outbound_request(execute_payload(**overrides))


def test_appointment_slot_matrix_requires_explicit_offset_and_normalizes_utc():
    with pytest.raises(ValidationError, match="appointment_slot is required"):
        parse_outbound_request(execute_payload(appointment_slot=None))
    with pytest.raises(ValidationError, match="explicit UTC offset"):
        parse_outbound_request(execute_payload(appointment_slot="2026-07-17T10:30:00"))
    with pytest.raises(ValidationError, match="forbidden"):
        parse_outbound_request(execute_payload(intent_kind="inquiry_reply", appointment_slot="2026-07-17T14:30:00Z"))


def test_text_is_nfc_lf_normalized_and_length_bounded():
    request = parse_outbound_request(execute_payload(arguments={"text": "Cafe\u0301\r\nTour"}))
    assert request.arguments.text == "Café\nTour"
    for value in ("", "x" * 10001):
        with pytest.raises(ValidationError):
            parse_outbound_request(execute_payload(arguments={"text": value}))


def test_status_accepts_only_op_and_uuid_action_id():
    action_id = "8f8f1a45-13a7-4bd3-a15a-f8d265bbc567"
    request = parse_outbound_request({"op": "status", "action_id": action_id})
    assert isinstance(request, StatusRequest)
    assert request.action_id == UUID(action_id)
    with pytest.raises(ValidationError):
        parse_outbound_request({"op": "status", "action_id": "bad"})
    with pytest.raises(ValidationError, match="extra"):
        parse_outbound_request({"op": "status", "action_id": action_id, "wake": 1})
