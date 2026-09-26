"""Every refusal the agent reads says what happened and what to do next.

Text only: the same requests are refused at the same points as before. Each
message keeps its original words first (Comm-Data-Store's system tests and
operators search for them) and adds the explanation after them.
"""

from __future__ import annotations

import dataclasses
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.record import require_action
from postgres_mcp.outbound_gateway.server import FeaturePolicy
from postgres_mcp.outbound_gateway.server import handle_outbound_action
from postgres_mcp.outbound_gateway.state_machine import InvalidTransitionError
from postgres_mcp.outbound_gateway.state_machine import validate_transition
from postgres_mcp.outbound_gateway.store import PostgresActionStore

from .test_action_store import context as store_context
from .test_context import FakeRepository
from .test_context import policy
from .test_context import record
from .test_context import request

ACTION_ID = UUID("4cbac369-48c6-5b62-95e9-41f50259e732")


class DatabaseError(Exception):
    """Stands in for the psycopg error a CDS stored function raises."""


async def _create_raising(message: str) -> Exception:
    store = PostgresActionStore(object())
    with patch(
        "postgres_mcp.outbound_gateway.store.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=DatabaseError(message)),
    ):
        with pytest.raises(Exception) as raised:
            await store.create_or_load(store_context())
    return raised.value


async def _confirm_raising(message: str) -> Exception:
    store = PostgresActionStore(object())
    with patch(
        "postgres_mcp.outbound_gateway.store.SafeSqlDriver.execute_param_query",
        AsyncMock(side_effect=DatabaseError(message)),
    ):
        with pytest.raises(Exception) as raised:
            await store.confirm_stale_context(ACTION_ID, wakeup_event_id=27250, decision="yes", actor="agent")
    return raised.value


@pytest.mark.asyncio
async def test_terminal_wake_says_the_wake_is_closed_nothing_was_sent_and_not_to_retry():
    error = await _create_raising("ordinary outbound action cannot attach to terminal wake 27250")
    text = str(error)
    assert text.startswith("ordinary outbound action cannot attach to terminal wake 27250")
    assert "already closed" in text
    assert "finalized" in text and "outside the gateway" in text
    assert "Nothing was sent" in text
    assert "needs_human" in text and "new wake" in text
    assert "Do not retry" in text


@pytest.mark.asyncio
async def test_immutable_mismatch_says_the_action_is_recorded_and_new_content_is_a_new_request():
    error = await _create_raising(f"outbound action immutable context mismatch for {ACTION_ID}")
    text = str(error)
    assert text.startswith(f"outbound action immutable context mismatch for {ACTION_ID}")
    assert "already recorded with different content" in text
    assert "Nothing was sent" in text
    assert "new request" in text and '"revise"' in text
    assert "Do not retry" in text


@pytest.mark.asyncio
async def test_action_limit_says_nothing_was_sent_and_to_record_needs_human():
    text = str(await _create_raising("outbound action limit reached: wake 27250 already has 10 actions"))
    assert text.startswith("outbound action limit reached")
    assert "Nothing was sent" in text and "needs_human" in text


@pytest.mark.asyncio
async def test_confirming_on_a_closed_wake_says_nothing_was_sent_and_what_to_do():
    text = str(await _confirm_raising("stale context confirmation cannot send for terminal wake 27250"))
    assert text.startswith("stale context confirmation cannot send for terminal wake 27250")
    assert "already closed" in text and "Nothing was sent" in text
    assert "needs_human" in text and "new wake" in text


@pytest.mark.asyncio
async def test_an_already_answered_question_says_the_first_answer_stands():
    text = str(await _confirm_raising("stale context already answered no"))
    assert text.startswith("stale context already answered no")
    assert "first answer stands" in text and '"status"' in text


@pytest.mark.asyncio
async def test_any_other_database_error_passes_through_unchanged():
    error = await _create_raising("connection reset by peer")
    assert isinstance(error, DatabaseError)
    assert str(error) == "connection reset by peer"


@pytest.mark.asyncio
async def test_a_missing_wake_names_the_id_and_what_to_pass():
    loader = ActionContextLoader(FakeRepository(record()), policy())
    with pytest.raises(ContextDerivationError) as raised:
        await loader.load(request(wakeup_event_id=99999))
    text = str(raised.value)
    assert text.startswith("wakeup event does not exist")
    assert "99999" in text and "Nothing was sent" in text and "wakeup_event_id" in text


@pytest.mark.asyncio
async def test_a_wrong_cliq_reply_target_says_where_a_reply_goes_and_the_alternative():
    inbound = record(
        event_source="zoho_cliq", message_source="zoho_cliq", source_channel_id="CT_1",
        channel_type="dm", subject=None, envelope={"identity": {}, "message": {}}, raw_payload={},
    )
    outbound = request(
        action_role="internal_reply", operation="cliq.chat.post", intent_kind="internal_reply",
        appointment_slot=None, arguments={"channel_or_chat_id": "CT_2", "text": "pong"},
    )
    with pytest.raises(ContextDerivationError) as raised:
        await ActionContextLoader(FakeRepository(inbound), policy()).load(outbound)
    text = str(raised.value)
    assert text.startswith("Cliq reply target must match the inbound chat")
    assert "Nothing was sent" in text and "cliq.channel.post" in text


@pytest.mark.asyncio
async def test_an_unconfigured_email_source_says_nothing_was_sent_and_what_to_do():
    event = record(message_source="zoho_mail", channel_type="email_thread", participant_key="someone@gmail.com",
                   raw_payload={}, envelope={"identity": {}, "message": {"prospect_name": "Dan", "property": "x"}})
    unmapped = dataclasses.replace(policy(), email_account_by_provider={"zillow": "nigel-zoho"})
    with pytest.raises(ContextDerivationError) as raised:
        await ActionContextLoader(FakeRepository(event), unmapped).load(
            request(arguments={"to_address": "dan@pfg.io", "text": "hi"})
        )
    text = str(raised.value)
    assert text.startswith("no outbound email account is configured for provider 'zoho_mail'")
    assert "Nothing was sent" in text and "needs_human" in text


@pytest.mark.asyncio
async def test_an_unknown_action_id_says_where_to_get_the_right_one():
    store = AsyncMock()
    store.get.return_value = None
    with pytest.raises(LookupError) as raised:
        await require_action(store, ACTION_ID)
    text = str(raised.value)
    assert text.startswith("outbound action does not exist")
    assert str(ACTION_ID) in text and "action_id" in text and "execute result" in text


@pytest.mark.asyncio
async def test_an_invalid_request_says_nothing_was_sent_and_where_the_shapes_are():
    with pytest.raises(ValueError) as raised:
        await handle_outbound_action(
            AsyncMock(), FeaturePolicy(writes_enabled=True, kill_switch=False),
            {"op": "execute", "wakeup_event_id": 7, "action_role": "prospect_reply", "operation": "email.send",
             "intent_kind": "inquiry_reply", "arguments": {"to": "dan@pfg.io", "text": "hi"}},
        )
    text = str(raised.value)
    assert text.startswith("invalid outbound action request: ")
    assert "Nothing was sent" in text and "tool description" in text


def test_an_invalid_transition_says_to_check_status_first():
    with pytest.raises(InvalidTransitionError) as raised:
        validate_transition(ActionState.COMPLETED, ActionState.DISPATCHING)
    text = str(raised.value)
    assert text.startswith("invalid outbound action transition: completed -> dispatching")
    assert '"status"' in text
