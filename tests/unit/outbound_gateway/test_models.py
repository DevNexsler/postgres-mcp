# pyright: reportAttributeAccessIssue=false, reportOptionalMemberAccess=false

from datetime import timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from postgres_mcp.outbound_gateway.models import CalendarDescriptionArguments
from postgres_mcp.outbound_gateway.models import EmptyArguments
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import StatusRequest
from postgres_mcp.outbound_gateway.models import SuggestRequest
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


def test_suggest_accepts_only_op_and_wakeup_event_id():
    request = parse_outbound_request({"op": "suggest", "wakeup_event_id": 1})
    assert isinstance(request, SuggestRequest)
    assert request.wakeup_event_id == 1
    with pytest.raises(ValidationError):
        parse_outbound_request({"op": "suggest", "wakeup_event_id": -1})
    with pytest.raises(ValidationError, match="extra"):
        parse_outbound_request({"op": "suggest", "wakeup_event_id": 1, "lead_id": "123"})


@pytest.mark.parametrize(
    ("operation", "role", "intent", "arguments", "argument_type"),
    [
        (
            "tenantcloud.message.send",
            "prospect_reply",
            "inquiry_reply",
            {"thread_id": 8001, "text": "Reply"},
            "TenantCloudMessageArguments",
        ),
        (
            "tenantcloud.lead.status.update",
            "provider_mutation",
            "tenantcloud_lead_status",
            {"lead_id": 6001, "status": "working"},
            "LeadStatusArguments",
        ),
        (
            "tenantcloud.maintenance.create",
            "provider_mutation",
            "tenantcloud_maintenance_create",
            {
                "property_id": 12,
                "unit_id": 34,
                "category_id": 57,
                "title": "Kitchen leak",
                "priority": "normal",
                "initiated_at": "2026-08-04",
                "text": "Pipe is leaking\r\nunder sink",
                "entry_allowed": False,
                "available_on": "2026-08-05",
            },
            "MaintenanceCreateArguments",
        ),
        (
            "tenantcloud.maintenance.status.update",
            "provider_mutation",
            "tenantcloud_maintenance_status",
            {"request_id": 81, "status": 3},
            "MaintenanceStatusArguments",
        ),
    ],
)
def test_tenantcloud_operations_use_exact_strict_argument_models(operation, role, intent, arguments, argument_type):
    parsed = parse_outbound_request(
        execute_payload(
            operation=operation,
            action_role=role,
            intent_kind=intent,
            appointment_slot=None,
            arguments=arguments,
        )
    )

    assert type(parsed.arguments).__name__ == argument_type
    assert parsed.arguments.model_config["frozen"] is True
    if operation == "tenantcloud.maintenance.create":
        assert parsed.arguments.text == "Pipe is leaking\nunder sink"


@pytest.mark.parametrize(
    "overrides",
    [
        {
            "operation": "tenantcloud.message.send",
            "action_role": "provider_mutation",
            "intent_kind": "inquiry_reply",
            "arguments": {"text": "Reply"},
        },
        {
            "operation": "tenantcloud.lead.status.update",
            "action_role": "provider_mutation",
            "intent_kind": "tenantcloud_maintenance_status",
            "arguments": {"status": "working"},
        },
        {
            "operation": "tenantcloud.maintenance.create",
            "action_role": "calendar_mutation",
            "intent_kind": "tenantcloud_maintenance_create",
            "arguments": {},
        },
        {
            "operation": "tenantcloud.maintenance.status.update",
            "action_role": "provider_mutation",
            "intent_kind": "tenantcloud_lead_status",
            "arguments": {"status": 1},
        },
    ],
)
def test_tenantcloud_role_operation_intent_matrix_fails_closed(overrides):
    with pytest.raises(ValidationError):
        parse_outbound_request(execute_payload(appointment_slot=None, **overrides))


def test_tenantcloud_arguments_carry_agent_supplied_targets():
    msg = parse_outbound_request({
        "op": "execute", "wakeup_event_id": 1, "action_role": "prospect_reply",
        "operation": "tenantcloud.message.send", "intent_kind": "inquiry_reply",
        "appointment_slot": None,
        "arguments": {"thread_id": 2002331, "text": "Thanks"},
    })
    assert msg.arguments.thread_id == 2002331

    lead = parse_outbound_request({
        "op": "execute", "wakeup_event_id": 1, "action_role": "provider_mutation",
        "operation": "tenantcloud.lead.status.update", "intent_kind": "tenantcloud_lead_status",
        "appointment_slot": None,
        "arguments": {"lead_id": 2405115, "status": "working"},
    })
    assert lead.arguments.lead_id == 2405115


@pytest.mark.parametrize("bad", [0, -1, "12", 1.5, None, True])
def test_tenantcloud_target_ids_reject_non_positive_integers(bad):
    with pytest.raises(ValueError):
        parse_outbound_request({
            "op": "execute", "wakeup_event_id": 1, "action_role": "prospect_reply",
            "operation": "tenantcloud.message.send", "intent_kind": "inquiry_reply",
            "appointment_slot": None,
            "arguments": {"thread_id": bad, "text": "Thanks"},
        })


@pytest.mark.parametrize("field", ["thread_id", "request_id", "property_id", "unit_id"])
def test_tenantcloud_arguments_reject_cross_operation_target_smuggling(field):
    """lead_id is now legitimate on LeadStatusArguments (it's the agent-
    supplied target), but the *other* three operations' target-id field
    names must still be rejected as extra -- an agent can't smuggle a
    maintenance/message target onto a lead status update."""
    with pytest.raises(ValidationError, match="extra"):
        parse_outbound_request(
            execute_payload(
                operation="tenantcloud.lead.status.update",
                action_role="provider_mutation",
                intent_kind="tenantcloud_lead_status",
                appointment_slot=None,
                arguments={"lead_id": 6001, "status": "working", field: 7},
            )
        )


@pytest.mark.parametrize("status", ["new", "closed", "WORKING", 1, True])
def test_tenantcloud_lead_status_is_exact(status):
    with pytest.raises(ValidationError):
        parse_outbound_request(
            execute_payload(
                operation="tenantcloud.lead.status.update",
                action_role="provider_mutation",
                intent_kind="tenantcloud_lead_status",
                appointment_slot=None,
                arguments={"status": status},
            )
        )


@pytest.mark.parametrize("status", [True, False, 0, 4, "1", None])
def test_tenantcloud_maintenance_status_is_strict(status):
    with pytest.raises(ValidationError):
        parse_outbound_request(
            execute_payload(
                operation="tenantcloud.maintenance.status.update",
                action_role="provider_mutation",
                intent_kind="tenantcloud_maintenance_status",
                appointment_slot=None,
                arguments={"status": status},
            )
        )


def maintenance_create_arguments(**overrides):
    arguments = {
        "property_id": 12,
        "unit_id": 34,
        "category_id": 57,
        "title": "Kitchen leak",
        "priority": "normal",
        "initiated_at": "2026-08-04",
        "text": "Pipe is leaking",
        "entry_allowed": False,
        "available_on": None,
    }
    arguments.update(overrides)
    return arguments


@pytest.mark.parametrize("category_id", [True, False, 0, -1, 1.5, "57", 9_223_372_036_854_775_808])
def test_maintenance_create_category_id_is_positive_strict_bigint(category_id):
    with pytest.raises(ValidationError):
        parse_outbound_request(
            execute_payload(
                operation="tenantcloud.maintenance.create",
                action_role="provider_mutation",
                intent_kind="tenantcloud_maintenance_create",
                appointment_slot=None,
                arguments=maintenance_create_arguments(category_id=category_id),
            )
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"priority": "high"},
        {"priority": "NORMAL"},
        {"entry_allowed": 0},
        {"entry_allowed": "false"},
        {"initiated_at": "08/04/2026"},
        {"initiated_at": "2026-02-30"},
        {"available_on": "08/05/2026"},
        {"available_on": "2026-02-30"},
        {"title": ""},
        {"title": " Kitchen leak"},
        {"title": "x" * 256},
        {"text": ""},
        {"text": "Pipe is leaking "},
        {"text": "x" * 10_001},
        {"unknown": "value"},
    ],
)
def test_maintenance_create_rejects_noncanonical_fields(overrides):
    with pytest.raises(ValidationError):
        parse_outbound_request(
            execute_payload(
                operation="tenantcloud.maintenance.create",
                action_role="provider_mutation",
                intent_kind="tenantcloud_maintenance_create",
                appointment_slot=None,
                arguments=maintenance_create_arguments(**overrides),
            )
        )


def test_maintenance_create_normalizes_unicode_and_newlines_before_hashing():
    parsed = parse_outbound_request(
        execute_payload(
            operation="tenantcloud.maintenance.create",
            action_role="provider_mutation",
            intent_kind="tenantcloud_maintenance_create",
            appointment_slot=None,
            arguments=maintenance_create_arguments(text="Cafe\u0301\r\nPipe"),
        )
    )

    assert parsed.arguments.text == "Café\nPipe"
