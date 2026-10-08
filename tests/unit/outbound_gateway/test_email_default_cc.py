# pyright: reportArgumentType=false
"""The default management Cc (identity.with_default_cc, OUTBOUND_EMAIL_DEFAULT_CC).

A customer email that omits cc gets the configured address; any cc the agent
passes -- [] included -- is sent exactly. A default, never a refusal."""

from unittest.mock import AsyncMock

import pytest

from postgres_mcp.outbound_gateway.adapters.email import EmailAdapter
from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.identity import request_arguments
from postgres_mcp.outbound_gateway.identity import same_request
from postgres_mcp.outbound_gateway.identity import with_default_cc
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.service import OutboundActionService
from postgres_mcp.outbound_gateway.stale_context import revised_request

from .test_adapters import ACTION_UID
from .test_adapters import context as adapter_context
from .test_context import FakeRepository
from .test_context import policy
from .test_context import record
from .test_service import row as service_row

MANAGEMENT = "management@pfg.io"
PROSPECT = "amanda.abc@convo.zillow.com"


def _email(*, role="prospect_reply", intent="inquiry_reply", to=PROSPECT, **extra) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": 12345,
            "action_role": role,
            "operation": "email.send",
            "intent_kind": intent,
            "arguments": {"to_address": to, "text": "Friday at 10:30 works.", **extra},
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


def _cc(request: ExecuteRequest):
    return request_arguments(request.arguments).get("cc")


def test_a_customer_email_without_cc_gets_the_default_and_stores_it():
    assert _cc(with_default_cc(_email(), MANAGEMENT)) == [MANAGEMENT]


def test_an_explicit_empty_cc_is_the_override_and_sends_none():
    request = _email(cc=[])
    assert _cc(request) == []  # stored as [], which is not "omitted"
    assert with_default_cc(request, MANAGEMENT) is request


@pytest.mark.parametrize(
    "cc",
    [["dan@pfg.io"], ["Management@PFG.io"], ["tenant@example.com", "MANAGEMENT@pfg.io"]],
)
def test_an_explicit_cc_is_kept_exactly_never_duplicated(cc):
    request = _email(cc=cc)
    assert with_default_cc(request, MANAGEMENT) is request


@pytest.mark.parametrize("to", ["dan@pfg.io", "Management@PFG.IO"])
def test_an_internal_only_recipient_gets_no_cc(to):
    request = _email(to=to)
    assert with_default_cc(request, MANAGEMENT) is request


def test_non_customer_roles_get_no_cc():
    request = _email(role="internal_notification", intent="manual_review_alert")
    assert with_default_cc(request, MANAGEMENT) is request


def test_an_empty_setting_is_off():
    request = _email()
    assert with_default_cc(request, "") is request


@pytest.mark.asyncio
async def test_an_identical_repeat_is_the_same_action_and_payload_hash():
    loader = ActionContextLoader(FakeRepository(record()), policy())
    first = with_default_cc(_email(), MANAGEMENT)
    again = with_default_cc(_email(), MANAGEMENT)
    first_context = await loader.load(first)
    again_context = await loader.load(again)

    assert same_request(first, again)
    assert first_context.action_id == again_context.action_id
    assert first_context.payload_hash == again_context.payload_hash
    assert first_context.arguments["cc"] == [MANAGEMENT]
    # The default is part of the request: without it, it is another payload.
    assert (await loader.load(_email())).payload_hash != first_context.payload_hash


@pytest.mark.asyncio
async def test_the_service_applies_the_default_before_deriving_the_context():
    loader = AsyncMock()
    loader.load.side_effect = RuntimeError("stop after load")
    service = OutboundActionService(
        store=AsyncMock(),
        context_loader=loader,
        evidence_loader=AsyncMock(),
        adapters={},
        provider_client=object(),
        clock=AsyncMock(),
        lease_owner="test",
        email_default_cc=MANAGEMENT,
    )
    with pytest.raises(RuntimeError, match="stop after load"):
        await service.execute(_email())
    loaded = loader.load.await_args.args[0]
    assert _cc(loaded) == [MANAGEMENT]


def test_the_adapter_sends_a_stored_cc_exactly_and_the_source_copy_only_when_none_is_stored():
    adapter = EmailAdapter(sender_domains={"nigel-zoho": "pfg.io"}, cc_by_source={"zillow": MANAGEMENT})
    text = "Friday at 10:30 works."

    def sent_cc(arguments):
        return adapter.build_request(adapter_context(arguments=arguments), ACTION_UID).arguments.get("cc")

    assert sent_cc({"text": text, "cc": []}) is None
    assert sent_cc({"text": text, "cc": ["dan@pfg.io"]}) == [{"address": "dan@pfg.io"}]
    # An action stored before the default existed keeps its per-source copy.
    assert sent_cc({"text": text}) == [{"address": MANAGEMENT}]


def test_a_revise_that_leaves_cc_out_keeps_the_refused_emails_cc():
    to = "lead@convo.zillow.com"
    parent = service_row(arguments={"to_address": to, "text": "old", "cc": [MANAGEMENT]})
    revised = revised_request(parent, {"to_address": to, "text": "new"})
    assert request_arguments(revised.arguments) == {"to_address": to, "text": "new", "cc": [MANAGEMENT]}
    # Changing it is still not a revise.
    with pytest.raises(Exception, match="cc must stay"):
        revised_request(parent, {"to_address": to, "text": "new", "cc": []})
