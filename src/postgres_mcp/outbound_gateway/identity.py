"""What makes two outbound requests the same request.

An action's identity is what the agent asked for -- role, operation, intent,
appointment slot and arguments. It is never the gateway's derived context,
which moves with live data (a sender's name flips, a message is re-threaded):
that is a record of the circumstances, not of the request. Comm-Data-Store
migration 206 dedupes on the same fields.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from .models import ExecuteRequest

# Optional argument fields added after actions were already stored. Left out
# when omitted, so every existing action keeps its stored arguments and
# payload hash.
LATER_OPTIONAL_ARGUMENTS = frozenset({"subject", "cc", "title", "duration_minutes", "location", "attendees"})


def request_arguments(arguments: BaseModel) -> dict[str, Any]:
    """The canonical stored form of a request's arguments."""
    return {
        key: value
        for key, value in arguments.model_dump(mode="json", exclude_none=False).items()
        if not (value is None and key in LATER_OPTIONAL_ARGUMENTS)
    }


def same_request(first: ExecuteRequest, second: ExecuteRequest) -> bool:
    """True when two executes ask for exactly the same effect."""
    return (
        first.action_role == second.action_role
        and first.operation == second.operation
        and first.intent_kind == second.intent_kind
        and first.appointment_slot == second.appointment_slot
        and request_arguments(first.arguments) == request_arguments(second.arguments)
    )
