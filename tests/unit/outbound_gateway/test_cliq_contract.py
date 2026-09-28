# pyright: reportArgumentType=false, reportOptionalMemberAccess=false
"""Contract tests: gateway request builders vs. AES's actual tool schemas.

2026-09-28: every real Cliq post the gateway built for a channel/chat id
shaped like AES's chat_id (a numeric id, or a CT_* conversation id) violated
AES's own inputSchema for cliq_channel_bot_post (channel_unique_name has no
`pattern`, but AES's parseCliqChannelBotPost rejects a CT_* value at
runtime, and the pattern IS declared on cliq_chat_post's chat_id -- see the
vendored fixture). The gateway had no test that ever built a request against
AES's actual schema, so nothing caught it before it hit prod and parked five
actions in manual_review.

These tests build real ProviderRequest objects with the gateway's own
adapters, using the real-shaped ids from that incident (queried read-only
from outbound_actions on 2026-09-28), and validate the resulting arguments
against tests/unit/outbound_gateway/fixtures/agent_email_tool_schemas.json --
a vendored, machine-extracted (not hand-copied) snapshot of AES's own
inputSchema for every tool the gateway calls. Refresh the fixture with
scripts/refresh_agent_email_tool_schemas.py after any AES schema change.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

import jsonschema
import pytest

from postgres_mcp.outbound_gateway.adapters.cliq import CliqAdapter
from postgres_mcp.outbound_gateway.cliq_target import CliqChannelIdUnresolvedError
from postgres_mcp.outbound_gateway.cliq_target import CliqTargetKind
from postgres_mcp.outbound_gateway.cliq_target import resolve_cliq_target
from postgres_mcp.outbound_gateway.context import ActionContext
from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import Operation

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "agent_email_tool_schemas.json"
SCHEMAS = json.loads(FIXTURE_PATH.read_text())

ACTION_UID = UUID("9ebddbf7-8fc8-5a4f-bba7-869ea7053521")
NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)

# Real target ids from outbound_actions where operation='cliq.channel.post',
# queried read-only on prod 2026-09-28 (see the ticket this branch fixes).
PROD_NUMERIC_CHAT_ID = "1424728044450751028"  # 19 digits -- a Cliq DM, not a channel
PROD_CT_STAR_CHAT_ID = "CT_2243214485353125021_721156495"
PROD_UNIQUE_NAMES = ("maintenance", "tenantleads", "nigeldebugging")


class AesContractViolationError(AssertionError):
    pass


def assert_matches_aes_schema(tool: str, arguments: dict) -> None:
    """Validate `arguments` against AES's actual contract for `tool`: both
    its declared inputSchema (required keys, field names, patterns like
    CLIQ_CHAT_ID_PATTERN) and the runtime-only rules AES enforces in handler
    code that never show up in tools/list (see _runtime_rules in the
    fixture -- e.g. cliq_channel_bot_post rejecting a CT_* channel_unique_name).
    A jsonschema.validate() alone would miss the CT_* rule entirely, since
    AES does not declare it as a schema `pattern`."""
    schema = {key: value for key, value in SCHEMAS[tool].items() if not key.startswith("_")}
    jsonschema.validate(instance=arguments, schema=schema)
    if tool == "cliq_channel_bot_post":
        chat_id_pattern = SCHEMAS["cliq_chat_post"]["properties"]["chat_id"]["pattern"]
        value = arguments.get("channel_unique_name", "")
        if re.match(chat_id_pattern, value):
            reason = SCHEMAS["_runtime_rules"]["cliq_channel_bot_post.channel_unique_name"]["reason"]
            raise AesContractViolationError(f"channel_unique_name {value!r} is chat-shaped, not a channel_unique_name: {reason}")


def cliq_context(*, operation: Operation, target_kind: str, target_id: str) -> ActionContext:
    return ActionContext(
        action_id=ACTION_UID,
        wakeup_event_id=27314,
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        operation=operation,
        intent_kind=IntentKind.LEAD_ALERT,
        appointment_slot=None,
        arguments=MappingProxyType({"text": "New lead needs review"}),
        source="tenantcloud",
        source_message_id=1,
        source_message_key="tenantcloud:1",
        source_sent_at=NOW,
        conversation_id="conversation:internal",
        conversation_watermark=1,
        prospect_id="internal:none",
        aliases=(),
        property_id=None,
        property_label=None,
        target=DerivedTarget(target_kind, target_id, True),
        provider_account=target_id,
        routing_policy_version="v1",
        canonical_scope=MappingProxyType({}),
        canonical_context=MappingProxyType({}),
        payload_hash="a" * 64,
        lock_holder="outbound-gateway:test",
        thread_identity="thread-1",
        showing_lifecycle_id="showing:1",
        calendar_event_uid=None,
    )


# --- The exact 2026-09-28 shapes, run through context derivation's own
# resolver, then through the adapter -- proving both stages agree and the
# resulting request satisfies AES's actual schema. ---


@pytest.mark.parametrize(
    "raw_target_id",
    [PROD_NUMERIC_CHAT_ID, PROD_CT_STAR_CHAT_ID],
    ids=["prod_numeric_chat_id", "prod_ct_star_chat_id"],
)
def test_cliq_channel_post_with_a_chat_shaped_prod_id_is_resolved_to_a_chat_target(raw_target_id):
    kind, target_id = resolve_cliq_target(raw_target_id, CliqTargetKind.CHANNEL, {})
    assert kind is CliqTargetKind.CHAT
    assert target_id == raw_target_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_target_id",
    [PROD_NUMERIC_CHAT_ID, PROD_CT_STAR_CHAT_ID],
    ids=["prod_numeric_chat_id", "prod_ct_star_chat_id"],
)
async def test_cliq_channel_post_with_a_chat_shaped_prod_id_builds_a_schema_valid_chat_post(raw_target_id):
    """The exact failure class from 2026-09-28: the agent called
    cliq.channel.post with a target that is actually chat-shaped. The
    gateway must route it to cliq_chat_post, and the built request must
    satisfy AES's real schema for that tool (in particular chat_id's
    pattern)."""
    kind, target_id = resolve_cliq_target(raw_target_id, CliqTargetKind.CHANNEL, {})
    ctx = cliq_context(operation=Operation.CLIQ_CHANNEL_POST, target_kind=kind.value, target_id=target_id)
    adapter = CliqAdapter(Operation.CLIQ_CHANNEL_POST)

    request = adapter.build_request(ctx, ACTION_UID)

    assert request.tool == "cliq_chat_post"
    assert request.arguments["chat_id"] == raw_target_id
    assert_matches_aes_schema("cliq_chat_post", request.arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize("unique_name", PROD_UNIQUE_NAMES)
async def test_cliq_channel_post_with_a_unique_name_builds_a_schema_valid_channel_post(unique_name):
    kind, target_id = resolve_cliq_target(unique_name, CliqTargetKind.CHANNEL, {})
    ctx = cliq_context(operation=Operation.CLIQ_CHANNEL_POST, target_kind=kind.value, target_id=target_id)
    adapter = CliqAdapter(Operation.CLIQ_CHANNEL_POST)

    request = adapter.build_request(ctx, ACTION_UID)

    assert request.tool == "cliq_channel_bot_post"
    assert request.arguments["channel_unique_name"] == unique_name
    assert_matches_aes_schema("cliq_channel_bot_post", request.arguments)


def test_short_numeric_channel_id_without_a_mapping_refuses_instead_of_guessing():
    with pytest.raises(CliqChannelIdUnresolvedError) as excinfo:
        resolve_cliq_target("42", CliqTargetKind.CHANNEL, {})
    message = str(excinfo.value)
    assert "Nothing was sent" in message
    assert "cliq_channels_list" in message
    assert "cliq.chat.post" in message


@pytest.mark.asyncio
async def test_short_numeric_channel_id_with_a_configured_mapping_resolves_to_its_unique_name():
    kind, target_id = resolve_cliq_target("42", CliqTargetKind.CHANNEL, {"42": "maintenance"})
    ctx = cliq_context(operation=Operation.CLIQ_CHANNEL_POST, target_kind=kind.value, target_id=target_id)
    adapter = CliqAdapter(Operation.CLIQ_CHANNEL_POST)

    request = adapter.build_request(ctx, ACTION_UID)

    assert kind is CliqTargetKind.CHANNEL
    assert request.tool == "cliq_channel_bot_post"
    assert request.arguments["channel_unique_name"] == "maintenance"
    assert_matches_aes_schema("cliq_channel_bot_post", request.arguments)


@pytest.mark.asyncio
async def test_cliq_chat_post_is_unaffected_by_channel_resolution():
    ctx = cliq_context(operation=Operation.CLIQ_CHAT_POST, target_kind="cliq_chat", target_id=PROD_CT_STAR_CHAT_ID)
    adapter = CliqAdapter(Operation.CLIQ_CHAT_POST)

    request = adapter.build_request(ctx, ACTION_UID)

    assert request.tool == "cliq_chat_post"
    assert_matches_aes_schema("cliq_chat_post", request.arguments)


def test_the_vendored_pattern_matches_the_one_cliq_target_py_hardcodes():
    """Catches drift between AES's actual chat_id pattern and the copy
    cliq_target.py keeps for its own is_cliq_chat_id() -- if AES ever widens
    or narrows CLIQ_CHAT_ID_PATTERN, this fails instead of silently
    misclassifying."""
    from postgres_mcp.outbound_gateway.cliq_target import CLIQ_CHAT_ID_PATTERN

    assert SCHEMAS["cliq_chat_post"]["properties"]["chat_id"]["pattern"] == CLIQ_CHAT_ID_PATTERN


def _pre_fix_build_request(operation: Operation, target_id: str, *, wakeup_event_id: int, action_role: str) -> dict:
    """The pre-fix CliqAdapter.build_request routing rule, verified against
    origin/main d0f93c4 (the commit this branch forked from):

        channel = self._operation is Operation.CLIQ_CHANNEL_POST
        ...
        "channel_unique_name" if channel else "chat_id": context.target.target_id,

    i.e. the AES tool/field was chosen from the agent's stated operation
    alone, never from the shape of the id. Reproduced inline (not imported)
    so this test does not depend on git history being reachable at test
    time; the reproduction was verified against the actual old source
    (`git show origin/main:src/postgres_mcp/outbound_gateway/adapters/cliq.py`)
    before this branch changed it.
    """
    channel = operation is Operation.CLIQ_CHANNEL_POST
    return {
        "channel_unique_name" if channel else "chat_id": target_id,
        "text": "New lead needs review",
        "sync_message": True,
        "idempotency_key": f"cliq-wake:{wakeup_event_id}:{action_role}:{operation.value}",
    }


@pytest.mark.parametrize(
    "raw_target_id",
    [PROD_NUMERIC_CHAT_ID, PROD_CT_STAR_CHAT_ID],
    ids=["prod_numeric_chat_id", "prod_ct_star_chat_id"],
)
def test_old_routing_logic_violates_the_aes_contract_for_the_2026_09_28_prod_ids(raw_target_id):
    """Proves this contract test would have caught the incident: run the
    verified pre-fix routing rule (_pre_fix_build_request, reproducing
    origin/main d0f93c4's cliq.py) against the exact ids that actually
    failed in prod on 2026-09-28, and confirm the resulting request violates
    AES's real contract -- the same rejection AES itself returned."""
    old_request = _pre_fix_build_request(
        Operation.CLIQ_CHANNEL_POST,
        raw_target_id,
        wakeup_event_id=27314,
        action_role="internal_notification",
    )
    assert old_request["channel_unique_name"] == raw_target_id
    with pytest.raises((jsonschema.ValidationError, AesContractViolationError)):
        assert_matches_aes_schema("cliq_channel_bot_post", old_request)


def test_fixed_routing_produces_a_contract_valid_request_for_the_same_ids():
    """The other half of the proof: the current adapter, given the same
    inputs, produces a request that passes."""
    for raw_target_id in (PROD_NUMERIC_CHAT_ID, PROD_CT_STAR_CHAT_ID):
        kind, target_id = resolve_cliq_target(raw_target_id, CliqTargetKind.CHANNEL, {})
        ctx = cliq_context(operation=Operation.CLIQ_CHANNEL_POST, target_kind=kind.value, target_id=target_id)
        request = CliqAdapter(Operation.CLIQ_CHANNEL_POST).build_request(ctx, ACTION_UID)
        assert_matches_aes_schema(request.tool, request.arguments)
