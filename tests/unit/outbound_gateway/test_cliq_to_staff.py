# pyright: reportArgumentType=false
"""A staff Cliq DM addressed by person (CliqArguments.to_staff, #4209).

The agent names the person; the gateway posts to the DM chat Nigel is in with
them, from Comm-Data-Store's resolve_cliq_staff_dm (migration 260). Wake 28410
(2026-10-14) picked CDS channel 457, "Gunther Stegmann", which is the
operator's own Dan<->Gunther DM, and the post was refused (action 9b3a6009);
the SOULs answered by carrying each staff DM's chat id. Exactly one person
must match, and the request is that chat from then on: the identical request
naming the chat directly is the same action."""

from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from postgres_mcp.outbound_gateway.context import ActionContextLoader
from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.identity import same_request
from postgres_mcp.outbound_gateway.models import ExecuteRequest
from postgres_mcp.outbound_gateway.models import operation_catalog
from postgres_mcp.outbound_gateway.models import parse_outbound_request
from postgres_mcp.outbound_gateway.repository import StaffDm
from postgres_mcp.outbound_gateway.service import OutboundActionService

from .test_context import FakeRepository
from .test_context import policy
from .test_context import record

# The DM chats Nigel is in, as resolve_cliq_staff_dm returned them on
# production 2026-10-15 (ticket #4175's read-only replay).
DAN = StaffDm("720844989", "Dan Park", "1424728044450751028")
GUNTHER = StaffDm("845641266", "Gunther", "1424921573680601358")
JOYCELYN = StaffDm("901234567", "Joycelyn", "1424728071652095349")


class StaffRepository(FakeRepository):
    """resolve_cliq_staff_dm as the database answers it: the rows `person`
    names, everyone for None."""

    def __init__(self, matches_by_person, *, everyone=(DAN, GUNTHER, JOYCELYN)):
        super().__init__(
            record(
                event_source="zoho_cliq",
                message_source="zoho_cliq",
                source_channel_id=DAN.chat_id,
                channel_type="dm",
                participant_type="user",
                participant_key=DAN.person_key,
                display_name=DAN.person_name,
                raw_payload={"provider": "cliq"},
                envelope={"identity": {}, "message": {}},
            )
        )
        self.matches_by_person = matches_by_person
        self.everyone = list(everyone)
        self.staff_calls = []

    async def resolve_cliq_staff_dm(self, person):
        self.staff_calls.append(person)
        return self.everyone if person is None else list(self.matches_by_person.get(person, ()))


def alert(**target) -> ExecuteRequest:
    parsed = parse_outbound_request(
        {
            "op": "execute",
            "wakeup_event_id": 12345,
            "action_role": "internal_notification",
            "operation": "cliq.channel.post",
            "intent_kind": "manual_review_alert",
            "arguments": {**target, "text": "Possible phishing email from a prospect; message ids 1, 2."},
        }
    )
    assert isinstance(parsed, ExecuteRequest)
    return parsed


@pytest.mark.asyncio
async def test_a_staff_dm_by_person_is_sent_to_the_chat_nigel_is_in():
    loader = ActionContextLoader(StaffRepository({"gunther": [GUNTHER]}), policy())

    resolved = await loader.resolve_staff_dm(alert(to_staff="gunther"))
    context = await loader.load(resolved)

    assert (context.target.kind, context.target.target_id) == ("cliq_chat", GUNTHER.chat_id)
    # Stored as the chat it goes to: the ledger, payload hash and reconcile
    # never re-resolve the person.
    assert context.arguments["channel_or_chat_id"] == GUNTHER.chat_id
    assert "to_staff" not in context.arguments


@pytest.mark.asyncio
async def test_by_person_and_by_its_chat_id_are_the_same_action():
    loader = ActionContextLoader(StaffRepository({"dan": [DAN]}), policy())
    by_person = await loader.resolve_staff_dm(alert(to_staff="dan"))
    by_chat = alert(channel_or_chat_id=DAN.chat_id)

    assert same_request(by_person, by_chat)
    first, second = await loader.load(by_person), await loader.load(by_chat)
    assert (first.action_id, first.payload_hash) == (second.action_id, second.payload_hash)


@pytest.mark.asyncio
async def test_nobody_matching_refuses_and_lists_the_people_nigel_can_dm():
    repository = StaffRepository({})
    with pytest.raises(ContextDerivationError) as refused:
        await ActionContextLoader(repository, policy()).resolve_staff_dm(alert(to_staff="rafael"))

    detail = str(refused.value)
    assert "no Cliq DM with 'rafael' that Nigel is in. Nothing was sent" in detail
    assert "people Nigel can DM: Dan Park, Gunther, Joycelyn" in detail
    assert repository.staff_calls == ["rafael", None]


@pytest.mark.asyncio
async def test_no_staff_dm_at_all_refuses_without_an_empty_list():
    with pytest.raises(ContextDerivationError, match="Nigel is in no staff DM"):
        await ActionContextLoader(StaffRepository({}, everyone=()), policy()).resolve_staff_dm(alert(to_staff="dan"))


@pytest.mark.asyncio
async def test_several_people_matching_refuses_naming_them_never_a_guess():
    other_dan = StaffDm("700000001", "Dan Smith", "1424700000000000001")
    with pytest.raises(ContextDerivationError) as refused:
        await ActionContextLoader(StaffRepository({"dan": [DAN, other_dan]}), policy()).resolve_staff_dm(alert(to_staff="dan"))

    assert "matches several people Nigel can DM: Dan Park, Dan Smith. Nothing was sent" in str(refused.value)


@pytest.mark.parametrize(
    "target",
    [{}, {"channel_or_chat_id": DAN.chat_id, "to_staff": "dan"}, {"to_staff": "  "}, {"to_staff": 5}],
)
def test_exactly_one_target_is_named(target):
    with pytest.raises(ValidationError):
        alert(**target)


def test_the_catalog_teaches_to_staff():
    line = next(line for line in operation_catalog().splitlines() if line.startswith("cliq.channel.post:"))
    assert "arguments {channel_or_chat_id?, to_staff?, text}" in line
    assert 'pass to_staff (e.g. "gunther") instead of channel_or_chat_id' in line


@pytest.mark.asyncio
async def test_the_service_resolves_the_person_before_deriving_the_context():
    loader = ActionContextLoader(StaffRepository({"gunther": [GUNTHER]}), policy())
    loaded = []

    async def stop_after_load(request, **_):
        loaded.append(request)
        raise RuntimeError("stop after load")

    loader.load = stop_after_load  # type: ignore[method-assign]
    service = OutboundActionService(
        store=AsyncMock(),
        context_loader=loader,
        evidence_loader=AsyncMock(),
        adapters={},
        provider_client=object(),
        clock=AsyncMock(),
        lease_owner="test",
    )
    with pytest.raises(RuntimeError, match="stop after load"):
        await service.execute(alert(to_staff="gunther"))
    assert same_request(loaded[0], alert(channel_or_chat_id=GUNTHER.chat_id))
