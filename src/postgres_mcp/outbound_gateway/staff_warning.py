"""A real StaffWarningPort: one internal Cliq notification per failed action.

Posts through the gateway's OWN internal-notification path -- a
``cliq.channel.post`` action with ``action_role=internal_notification`` and
``intent_kind=manual_review_alert`` -- exactly the shape ``context.py``
already documents as the escape hatch from ``cliq.chat.post``'s internal_reply
guard. That gets this warning the ledger, the idempotency, and PR #59's
target normalization for free: it is not a second notification mechanism,
it is the SAME ``OutboundActionService.execute()`` every other send in this
gateway goes through.

Posted on the failed action's own wake through ``service.escalate()``, which
creates the row with CDS's ``create_outbound_delivery_escalation()``
(migration 279). That wake has usually ended by the time a send gives up --
and a TenantCloud wake's claim has closed -- so an ordinary row there is
refused (``wake_terminal``, or ``guard_tenantcloud_outbound_action_ownership``:
10/27, the escalations for Mary Haas's and Charles Serpone's dropped replies
both died this way). CDS lets this row through because it names a failed
action CDS can check, one escalation per failed action; an agent has no way to
create one.

Never recurses: ``service.escalate()`` dispatches synchronously (the legacy
dispatch path), never ``service.enqueue()`` + a Restate submission --
so even if ``cliq.channel.post`` is itself a flagged Restate operation
(``OUTBOUND_RESTATE_OPERATIONS``), a warning send is never advanced by
``OutboundDeliveryCoordinator`` and can never trigger a second warn_once call
about itself.

Idempotency: one escalation per failed action. The text names the failed
action_id, so CDS's create-or-load identity (same wake, role and arguments)
returns the same row on a replay, and migration 279 refuses a second
escalation row for the same failed action outright.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from .models import ActionRole
from .models import ExecuteRequest
from .models import IntentKind
from .models import Operation
from .models import PublicStatus
from .models import parse_outbound_request

logger = logging.getLogger(__name__)

DEFAULT_STAFF_WARNING_CHANNEL = "tenantleads"
# store.later_sent_sibling() reads this prefix to keep a warning from counting
# as the agent's own recovery send.
STAFF_WARNING_TEXT_PREFIX = "Outbound delivery failed:"
_NOT_POSTED = frozenset({PublicStatus.REJECTED, PublicStatus.FAILED, PublicStatus.STALE, PublicStatus.MANUAL_REVIEW})


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
            f"{STAFF_WARNING_TEXT_PREFIX} operation={operation.value} "
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
            result = await self._service.escalate(action_id, request)
            if getattr(result, "status", None) in _NOT_POSTED:
                # A refusal is a result now, not a raise (a closed wake
                # records the warning rejected): keep it as visible as the
                # raise was.
                logger.error(
                    "staff warning for action %s (wake %s) was not posted: %s %s",
                    action_id,
                    wakeup_event_id,
                    result.status.value,
                    result.detail_code,
                )
        except Exception:
            logger.exception(
                "staff warning failed for action %s (wake %s, operation %s); "
                "no Cliq notification was posted",
                action_id,
                wakeup_event_id,
                operation.value,
            )
