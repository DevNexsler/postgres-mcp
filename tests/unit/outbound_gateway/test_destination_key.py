# pyright: reportArgumentType=false
"""The destination key: one person, one key, whatever wake produced the send.

2026-11-05: a maintenance-supervisor wake (the tenant's Maintenance-line
texts) and a general-qa wake (a staff Cliq directive) texted one tenant
minutes apart with contradictory messages. The same phone was keyed
`subject:<hash>`, `prospect:+1...` and `internal:+1...` by the three sends'
wakes, so no send's stale-context check saw another's. The integration
counterpart (the probe SQL) is tests/integration/test_gateway_traffic_isolation.py.
"""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

from postgres_mcp.outbound_gateway.context import DerivedTarget
from postgres_mcp.outbound_gateway.context import destination_key
from postgres_mcp.outbound_gateway.models import ActionRole
from postgres_mcp.outbound_gateway.models import ActionState
from postgres_mcp.outbound_gateway.models import IntentKind
from postgres_mcp.outbound_gateway.models import NewerActivity
from postgres_mcp.outbound_gateway.models import Operation
from postgres_mcp.outbound_gateway.models import PublicStatus
from postgres_mcp.outbound_gateway.repository import probe_destination
from postgres_mcp.outbound_gateway.stale_context import StaleContextQuestions

from .test_evidence import NOW
from .test_evidence import context

TENANT = "+12025550142"


@pytest.mark.parametrize(
    ("operation", "target", "key"),
    [
        (Operation.QUO_SMS_SEND, TENANT, f"phone:{TENANT}"),
        (Operation.QUO_SMS_SEND, "(202) 555-0142", f"phone:{TENANT}"),
        (Operation.QUO_SMS_SEND, "12025550142", f"phone:{TENANT}"),
        (Operation.QUO_SMS_SEND, "555", None),
        (Operation.EMAIL_SEND, " Tenant@Example.COM ", "email:tenant@example.com"),
        (Operation.EMAIL_SEND, "not-an-address", None),
        (Operation.CLIQ_CHAT_POST, "1424728044450751028", None),
        (Operation.CLIQ_CHANNEL_POST, "tenantmanagementchat", None),
        (Operation.TENANTCLOUD_MESSAGE_SEND, "123", None),
        (Operation.QUO_SMS_SEND, "", None),
    ],
)
def test_destination_key_is_the_canonical_phone_or_address(operation, target, key):
    assert destination_key(operation, target) == key


def sms(**overrides):
    values = dict(
        operation=Operation.QUO_SMS_SEND,
        intent_kind=IntentKind.INQUIRY_REPLY,
        appointment_slot=None,
        target=DerivedTarget("quo_conversation", TENANT, True),
        recipient_phone=TENANT,
    )
    values.update(overrides)
    return replace(context(), **values)


@pytest.mark.parametrize(
    ("prospect_id", "source", "role"),
    [
        ("subject:7ab2fbbe", "quo", ActionRole.PROSPECT_REPLY),  # maintenance-supervisor, woken by her texts
        (f"prospect:{TENANT}", "zoho_cliq", ActionRole.PROSPECT_REPLY),  # general-qa, woken by a staff directive
        (f"internal:{TENANT}", "zoho_cliq", ActionRole.INTERNAL_NOTIFICATION),  # a staff-role text to her
    ],
)
def test_three_wake_derivations_of_one_phone_share_one_destination(prospect_id, source, role):
    ctx = sms(prospect_id=prospect_id, source=source, action_role=role)
    assert ctx.destination_key == f"phone:{TENANT}"
    destination = probe_destination(ctx)
    assert (destination.kind, destination.value) == ("phone", "2025550142")
    # A staff-role text is checked on its destination alone.
    assert destination.only is (role is ActionRole.INTERNAL_NOTIFICATION)


def test_a_cliq_post_has_no_destination_and_keeps_todays_matching():
    ctx = replace(
        context(),
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        operation=Operation.CLIQ_CHANNEL_POST,
        target=DerivedTarget("cliq_channel", "tenantmanagementchat", True),
    )
    assert probe_destination(ctx) == probe_destination(ctx).__class__(kind=None, value=None, only=False)


# --------------------------------------------------------------------------
# StaleContextQuestions.check: which sends are asked at all
# --------------------------------------------------------------------------

ACTION = UUID("10000000-0000-0000-0000-000000000042")
HER_TEXT = NewerActivity(direction="inbound", source="quo", occurred_at=NOW, preview="the heat is back on", message_id=91, action_id=None)


class Probe:
    def __init__(self, found):
        self.found = found
        self.asked = []

    async def newer_context(self, ctx, *, limit, waive_shown):
        self.asked.append(ctx)
        return list(self.found)


class Store:
    def __init__(self):
        self.blocked = []

    async def block_stale_context(self, action_id, state, lease_owner, shown_refs):
        self.blocked.append((action_id, tuple(shown_refs)))
        return action_row(state=ActionState.STALE)


def action_row(*, role=ActionRole.INTERNAL_NOTIFICATION, state=ActionState.RECEIVED):
    return SimpleNamespace(
        action_id=ACTION,
        action_uid=UUID("20000000-0000-0000-0000-000000000042"),
        action_role=role,
        remediation_reason=None,
        state=state,
        wakeup_event_id=7,
    )


def questions(probe, store, *, mode):
    async def drive_answered(*_args, **_kwargs):  # pragma: no cover - not reached
        raise AssertionError("no answer is driven here")

    return StaleContextQuestions(
        store=store,
        context_loader=SimpleNamespace(),
        probe=probe,
        actor="test",
        enabled=True,
        mode=mode,
        drive_answered=drive_answered,
    )


@pytest.mark.asyncio
async def test_a_staff_role_text_to_her_is_asked_about_her_newer_text():
    """The `internal:+1...` send of 2026-11-05 skipped the check entirely: an
    internal_notification never asked. A text has a person at the other end
    whatever its role."""
    probe, store = Probe([HER_TEXT]), Store()
    ctx = sms(action_role=ActionRole.INTERNAL_NOTIFICATION, intent_kind=IntentKind.MANUAL_REVIEW_ALERT)

    result = await questions(probe, store, mode="enforce").check(action_row(), ctx, agent_facing=True)

    assert result is not None and result.status is PublicStatus.NEEDS_CONFIRMATION
    assert store.blocked == [(ACTION, ("message:91",))]
    assert probe.asked == [ctx]


@pytest.mark.asyncio
async def test_shadow_mode_only_logs_a_staff_role_text_and_sends(caplog):
    probe, store = Probe([HER_TEXT]), Store()
    ctx = sms(action_role=ActionRole.INTERNAL_NOTIFICATION, intent_kind=IntentKind.MANUAL_REVIEW_ALERT)

    result = await questions(probe, store, mode="shadow").check(action_row(), ctx, agent_facing=True)

    assert result is None
    assert store.blocked == []
    assert "stale-context shadow would-ask" in caplog.text


@pytest.mark.asyncio
async def test_off_mode_never_looks():
    probe, store = Probe([HER_TEXT]), Store()
    ctx = sms(action_role=ActionRole.INTERNAL_NOTIFICATION, intent_kind=IntentKind.MANUAL_REVIEW_ALERT)

    assert await questions(probe, store, mode="off").check(action_row(), ctx, agent_facing=True) is None
    assert probe.asked == []


@pytest.mark.asyncio
async def test_a_staff_alert_posted_to_a_review_channel_is_still_never_asked():
    probe, store = Probe([HER_TEXT]), Store()
    ctx = replace(
        context(),
        action_role=ActionRole.INTERNAL_NOTIFICATION,
        operation=Operation.CLIQ_CHANNEL_POST,
        intent_kind=IntentKind.MANUAL_REVIEW_ALERT,
        appointment_slot=None,
        target=DerivedTarget("cliq_channel", "review", True),
        thread_identity="some-other-chat",
    )

    assert await questions(probe, store, mode="enforce").check(action_row(), ctx, agent_facing=True) is None
    assert probe.asked == []
