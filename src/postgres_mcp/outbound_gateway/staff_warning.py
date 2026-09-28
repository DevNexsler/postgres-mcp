"""A real StaffWarningPort: one internal Cliq notification per failed action.

Posts through the gateway's OWN internal-notification path -- a
``cliq.channel.post`` action with ``action_role=internal_notification`` and
``intent_kind=manual_review_alert`` -- exactly the shape ``context.py``
already documents as the escape hatch from ``cliq.chat.post``'s internal_reply
guard. That gets this warning the ledger, the idempotency, and PR #59's
target normalization for free: it is not a second notification mechanism,
it is the SAME ``OutboundActionService.execute()`` every other send in this
gateway goes through.

Never recurses: this calls ``service.execute()`` directly (the synchronous
legacy dispatch path), never ``service.enqueue()`` + a Restate submission --
so even if ``cliq.channel.post`` is itself a flagged Restate operation
(``OUTBOUND_RESTATE_OPERATIONS``), a warning send is never advanced by
``OutboundDeliveryCoordinator`` and can never trigger a second warn_once call
about itself.

Idempotency granularity: the gateway derives an internal_notification
action's id from ``(wakeup_event_id, action_role)`` alone (ordinal is always
0 -- see context.py) -- there is no lower-level identity for "the Nth
internal notification on this wake" to key a warning on today. This port is
therefore idempotent per WAKE, not per failed action id: two different
actions failing on the same wake collapse into the one warning send the
ledger already recorded for that wake (the second call is read back as the
same completed/duplicate action, so it never double-texts a customer or
re-posts to Cliq -- see warn_once's docstring). Splitting that identity
further needs a change to the action-id derivation itself, which is out of
this change's scope.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from .models import ActionRole
from .models import ExecuteRequest
from .models import IntentKind
from .models import Operation
from .models import parse_outbound_request

logger = logging.getLogger(__name__)

DEFAULT_STAFF_WARNING_CHANNEL = "tenantleads"


class CliqStaffWarningPort:
    """Records exactly one internal Cliq notification per failed action
    (see the module docstring for the wake-scoped idempotency this actually
    achieves).

    ``target_channel`` is the Cliq channel's unique name (config-driven,
    default ``tenantleads`` -- see ``DEFAULT_STAFF_WARNING_CHANNEL`` /
    ``OUTBOUND_STAFF_WARNING_CHANNEL`` in server.py); it is looked up through
    the SAME ``cliq_target_by_intent`` / ``cliq_channel_unique_names_by_id``
    config every other manual_review_alert already uses, by construction --
    this port just names the intent, the routing policy already loaded into
    ``service``'s ``ActionContextLoader`` does the resolving.
    """

    def __init__(self, service: Any, *, target_channel: str = DEFAULT_STAFF_WARNING_CHANNEL) -> None:
        self._service = service
        self._target_channel = target_channel
        self._warned: set[UUID] = set()

    async def warn_once(
        self,
        action_id: UUID,
        action_uid: UUID | None,
        reason: str,
        *,
        wakeup_event_id: int,
        operation: Operation,
        recipient: str,
    ) -> None:
        if action_id in self._warned:
            return
        self._warned.add(action_id)
        text = (
            f"Outbound delivery failed: operation={operation.value} "
            f"recipient={recipient} wakeup_event_id={wakeup_event_id} "
            f"action_id={action_id} reason={reason}"
        )
        request = parse_outbound_request(
            {
                "op": "execute",
                "wakeup_event_id": wakeup_event_id,
                "action_role": ActionRole.INTERNAL_NOTIFICATION.value,
                "operation": Operation.CLIQ_CHANNEL_POST.value,
                "intent_kind": IntentKind.MANUAL_REVIEW_ALERT.value,
                "arguments": {
                    "channel_or_chat_id": self._target_channel,
                    "text": text[:10_000],
                },
            }
        )
        assert isinstance(request, ExecuteRequest)
        try:
            # The legacy synchronous path (never Restate) -- see the module
            # docstring for why that is what makes this incapable of
            # recursing into a second warning about itself.
            await self._service.execute(request)
        except Exception:
            logger.exception(
                "staff warning failed for action %s (wake %s, operation %s); "
                "no Cliq notification was posted",
                action_id,
                wakeup_event_id,
                operation.value,
            )
