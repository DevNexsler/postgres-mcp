"""email.send carries optional file attachments.

The agent sends the bytes in the request (the gateway runs in a container and
shares no filesystem with the agent); they are stored in the action's saved
arguments, so the worker can send the saved record again, and handed to Agent
Email's email_send, which already accepts inline attachments.
"""

from __future__ import annotations

import base64
from datetime import datetime
from datetime import timezone
from types import MappingProxyType
from uuid import UUID

import pytest
from pydantic import ValidationError

from postgres_mcp.outbound_gateway.adapters.email import EmailAdapter
from postgres_mcp.outbound_gateway.identity import request_arguments
from postgres_mcp.outbound_gateway.identity import same_request
from postgres_mcp.outbound_gateway.models import MAX_EMAIL_ATTACHMENT_BYTES
from postgres_mcp.outbound_gateway.models import MAX_EMAIL_ATTACHMENTS
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import EmailArguments
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import operation_catalog
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.record import OutboundActionRecord

from .test_adapters import ACTION_UID
from .test_adapters import context

PDF = b"%PDF-1.4 lease"
PDF_B64 = base64.b64encode(PDF).decode()


def _attachment(**overrides):
    return {"filename": "lease.pdf", "mime_type": "application/pdf", "content_base64": PDF_B64, **overrides}


def _email(**extra):
    return parse_outbound_request({
        "op": "execute", "wakeup_event_id": 7, "action_role": "prospect_reply",
        "operation": "email.send", "intent_kind": "inquiry_reply",
        "arguments": {"to_address": "tenant@example.com", "text": "Your lease is attached.", **extra},
    })


def test_an_email_may_carry_attachments():
    request = _email(attachments=[_attachment()])
    (attachment,) = request.arguments.attachments
    assert attachment.filename == "lease.pdf"
    assert attachment.mime_type == "application/pdf"
    assert base64.b64decode(attachment.content_base64) == PDF


def test_an_email_without_attachments_stores_and_hashes_exactly_as_before():
    assert request_arguments(EmailArguments(to_address="dan@pfg.io", text="hi")) == {
        "to_address": "dan@pfg.io", "text": "hi",
    }
    # An empty list is the same request as no list.
    assert request_arguments(_email(attachments=[]).arguments) == request_arguments(_email().arguments)


def test_attachments_are_part_of_the_request_identity():
    assert same_request(_email(attachments=[_attachment()]), _email(attachments=[_attachment()]))
    assert not same_request(_email(attachments=[_attachment()]), _email())
    other = base64.b64encode(b"%PDF-1.4 other").decode()
    assert not same_request(_email(attachments=[_attachment()]), _email(attachments=[_attachment(content_base64=other)]))


def test_wrapped_base64_is_stored_in_one_canonical_form():
    """`base64 file` wraps lines at 76 characters; the same bytes are the
    same request however they were wrapped."""
    data = bytes(range(256)) * 3
    wrapped = base64.encodebytes(data).decode()  # has newlines
    assert "\n" in wrapped.strip()
    stored = request_arguments(_email(attachments=[_attachment(content_base64=wrapped)]).arguments)
    assert stored["attachments"] == [
        {"filename": "lease.pdf", "mime_type": "application/pdf", "content_base64": base64.b64encode(data).decode()}
    ]


def test_mime_type_is_lower_cased():
    request = _email(attachments=[_attachment(mime_type=" Application/PDF ")])
    assert request.arguments.attachments[0].mime_type == "application/pdf"


@pytest.mark.parametrize(
    ("attachment", "expected"),
    [
        (_attachment(content_base64="not base64!"), "content_base64 must be base64"),
        (_attachment(content_base64=""), "content_base64 must not be empty"),
        (_attachment(filename="../etc/passwd"), "filename must be a bare file name"),
        (_attachment(filename="a\\b.pdf"), "filename must be a bare file name"),
        (_attachment(filename="  "), "filename must not be empty"),
        (_attachment(mime_type="pdf"), "mime_type must look like type/subtype"),
        ({"filename": "lease.pdf", "content_base64": PDF_B64}, "mime_type"),
        ({**_attachment(), "path": "/tmp/lease.pdf"}, "path"),
    ],
)
def test_a_malformed_attachment_is_refused_and_says_what_to_fix(attachment, expected):
    with pytest.raises(ValidationError, match=expected):
        _email(attachments=[attachment])


def test_too_many_attachments_are_refused():
    with pytest.raises(ValidationError, match=f"at most {MAX_EMAIL_ATTACHMENTS} files"):
        _email(attachments=[_attachment(filename=f"f{i}.pdf") for i in range(MAX_EMAIL_ATTACHMENTS + 1)])


def test_attachments_over_the_total_size_cap_are_refused():
    half = base64.b64encode(b"x" * (MAX_EMAIL_ATTACHMENT_BYTES // 2 + 1)).decode()
    with pytest.raises(ValidationError, match="10 MiB"):
        _email(attachments=[_attachment(filename="a.pdf", content_base64=half), _attachment(filename="b.pdf", content_base64=half)])
    exactly = base64.b64encode(b"x" * MAX_EMAIL_ATTACHMENT_BYTES).decode()
    assert _email(attachments=[_attachment(content_base64=exactly)]).arguments.attachments


def test_the_saved_record_rebuilds_the_same_attachments_for_the_worker():
    """The worker executes the SAVED arguments: the bytes must come back out of
    the stored JSON unchanged."""
    stored = request_arguments(_email(attachments=[_attachment()]).arguments)
    record = OutboundActionRecord(
        action_id=UUID("4cbac369-48c6-5b62-95e9-41f50259e732"),
        wakeup_event_id=7,
        action_role="prospect_reply",
        operation=Operation.EMAIL_SEND,
        intent_kind="inquiry_reply",
        appointment_slot=None,
        arguments=stored,
        state=ActionState.RECEIVED,
        action_uid=None,
        provider_request_ref=None,
        provider_message_id=None,
        provider_accepted_at=None,
        completion_kind=None,
        detail_code="received",
        attempt_count=0,
        next_attempt_at=datetime(2026, 9, 26, tzinfo=timezone.utc),
        payload_hash="a" * 64,
        canonical_context={"identity_version": "v1"},
        canonical_scope={"version": "v1"},
        recipient_scope={"kind": "email_thread", "target_id": "tenant@example.com", "verified": True},
        provider_account="nigel-zoho",
        routing_policy_version="v1",
    )
    rebuilt = record.execute_request()
    assert request_arguments(rebuilt.arguments) == stored


def test_the_email_adapter_hands_the_attachments_to_agent_email():
    adapter = EmailAdapter(sender_domains={"nigel-zoho": "pfg.io"})
    stored = request_arguments(_email(attachments=[_attachment()]).arguments)
    request = adapter.build_request(context(arguments=MappingProxyType(stored)), ACTION_UID)
    assert request.tool == "email_send"
    assert request.arguments["attachments"] == [
        {"filename": "lease.pdf", "content_base64": PDF_B64, "content_type": "application/pdf"}
    ]
    assert request.arguments["text"] == "Your lease is attached."


def test_an_email_without_attachments_sends_the_same_provider_request_as_before():
    adapter = EmailAdapter(sender_domains={"nigel-zoho": "pfg.io"})
    request = adapter.build_request(context(), ACTION_UID)
    assert "attachments" not in request.arguments


def test_the_tool_description_teaches_the_attachment_shape():
    line = next(line for line in operation_catalog().splitlines() if line.startswith("email.send:"))
    assert "attachments?" in line
    assert "filename, mime_type, content_base64" in line
