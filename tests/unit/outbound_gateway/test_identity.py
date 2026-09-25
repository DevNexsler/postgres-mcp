"""Identity: two executes are the same request when they ask for the same
effect, whatever the gateway derived around them."""

from postgres_mcp.outbound_gateway.identity import request_arguments
from postgres_mcp.outbound_gateway.identity import same_request
from postgres_mcp.outbound_gateway.models import EmailArguments
from postgres_mcp.outbound_gateway.models import parse_outbound_request


def _email(text="hi", **extra):
    return parse_outbound_request({
        "op": "execute", "wakeup_event_id": 7, "action_role": "prospect_reply",
        "operation": "email.send", "intent_kind": "inquiry_reply",
        "arguments": {"to_address": "dan@pfg.io", "text": text, **extra},
    })


def test_the_identical_request_is_the_same():
    assert same_request(_email(), _email())


def test_different_text_recipient_or_intent_is_a_different_request():
    assert not same_request(_email(), _email("hello"))
    assert not same_request(_email(), _email(cc=["x@pfg.io"]))
    other_intent = _email().model_copy(update={"intent_kind": "showing_offer"})
    assert not same_request(_email(), other_intent)


def test_omitted_later_optional_fields_are_left_out_of_the_stored_form():
    """subject/cc were added after actions were stored; an email that omits
    them must store (and hash) exactly as before."""
    assert request_arguments(EmailArguments(to_address="dan@pfg.io", text="hi")) == {
        "to_address": "dan@pfg.io", "text": "hi",
    }
    assert request_arguments(EmailArguments(to_address="dan@pfg.io", text="hi", subject="S")) == {
        "to_address": "dan@pfg.io", "text": "hi", "subject": "S",
    }
