"""Per-operation answer to: is a re-invoke after an AMBIGUOUS provider
outcome safe?

The ONE place this decision is written down, so no two callers can answer it
differently. Read this before changing any adapter's ``invoke``/``reconcile``,
and before changing what ``OutboundDeliveryCoordinator.advance()`` does on an
ambiguous outcome.

The gateway's dispatch code already upholds the actual invariant this table
documents: once ``adapter.invoke()`` has been called for an action, nothing
in this codebase ever calls it again for that same action.
``recovery.ActionRecovery`` (worker path) and ``OutboundDeliveryCoordinator``
(Restate path) both only ever call ``adapter.reconcile()`` on an ambiguous
row -- see ``_AMBIGUOUS_STATES`` handling in ``delivery_workflow.py`` and
``recovery.py``'s ``_final_provider_check``. A row that stays AMBIGUOUS all
the way to the retry ceiling is parked (``manual_review`` / ``dead_letter``)
with a staff warning, never blindly re-sent.

This table exists anyway, for two reasons: (1) it is the auditable record of
*why* that invariant is safe to keep for each operation -- what a re-invoke
would risk, and what actually settles an ambiguous outcome instead -- read
directly out of each adapter's ``invoke``/``reconcile`` and, for email and
Quo, their upstream tool sources; (2) if a future change ever needs a
"resend once confirmed-safe" path (there is none today), this is where that
per-operation call must be looked up, not re-derived at the call site.

Findings, one line each (see the operation's own adapter module for the
full reasoning):

- ``quo.sms.send`` -- CONFIRM_BY_READBACK. Quo's public ``/messages`` POST
  (QUO-Gated-MCP's ``quo_client.send_message``) takes no idempotency key at
  all: a blind resend is a genuine second text to the customer. Settled
  instead by ``QuoSmsAdapter.reconcile()``'s ``list_messages`` exact-tuple
  read-back (recipient + content + time >= source_sent_at) -- independent of
  any job/request state, so it still works after the fact.
- ``email.send`` -- CONFIRM_BY_READBACK. Agent-Email-Server's job records are
  in-memory (``JobQueue``'s ``jobs = new Map()``) and evict on a TTL; its own
  ``request_status`` "lost" response literally says "Re-run the original
  tool call", which is NOT safe here. ``EmailAdapter`` never needs to take
  that advice: it embeds a deterministic Message-ID
  (``<outbound-action-{action_uid}@{domain}>``) on send and reconciles by
  looking that exact Message-ID up via ``email_get_thread`` -- independent of
  the job record, so a lost job is not a lost answer.
- ``cliq.chat.post`` / ``cliq.channel.post`` -- SAFE_TO_REINVOKE.
  ``CliqAdapter.build_request`` sends a deterministic
  ``idempotency_key`` (``cliq-wake:<wake>:<role>:<operation>``) on every
  invoke; Agent-Email-Server's own success shape includes a
  ``duplicate_suppressed`` status this adapter already treats as accepted.
  A resend is provider-deduplicated by construction. (The coordinator still
  never actually re-invokes -- see the module docstring -- this only
  documents that doing so would not double-post.)
- ``calendar.create`` -- NEVER_REINVOKE. No independent read-back exists
  (``CalendarAdapter.reconcile()`` only polls the same in-memory job by
  request id) and CalDAV UID-collision behavior on a raw resend is not
  verified anywhere in this codebase. An ambiguous ``calendar.create`` that
  never settles must end in the one staff warning, never a resend.
- ``calendar.update`` / ``calendar.delete`` -- SAFE_TO_REINVOKE. Both carry
  the event's own ``etag`` (optimistic concurrency): if the first attempt
  already landed, the etag changed server-side and a resend with the stale
  etag fails closed (a safe no-op) instead of double-applying. A delete is
  additionally idempotent by nature (deleting an already-deleted event is a
  safe no-op on every CalDAV server this gateway targets).
- ``tenantcloud.*`` (all four operations) -- SAFE_TO_REINVOKE. The shared
  ``TenantCloudMutations`` facade (Comm-Data-Store,
  scripts/tenantcloud_mutations.py) performs its own idempotency pre-check,
  at most one exact write, and an exact post-write readback for every
  mutation -- see ``adapters/tenantcloud.py``'s module docstring.
"""

from __future__ import annotations

from enum import StrEnum

from .models import Operation


class ReinvokeSafety(StrEnum):
    """What ``adapter.invoke()`` a second time, for the same action, would do."""

    # Provider-side dedup (an idempotency key, or an optimistic-concurrency
    # token) makes a bare resend a safe no-op if the first attempt landed.
    SAFE_TO_REINVOKE = "safe_to_reinvoke"
    # A resend risks a real duplicate effect (a second customer-facing
    # message). Never resend; an independent read-back (not the same
    # in-memory job/request id) can still settle the ambiguous outcome.
    CONFIRM_BY_READBACK = "confirm_by_readback"
    # A resend risks a real duplicate effect AND no independent read-back
    # exists to settle it. An ambiguous outcome that never settles must end
    # in exactly one staff warning, never a resend.
    NEVER_REINVOKE = "never_reinvoke"


# One row per gateway operation. See the module docstring for the reasoning
# behind each value -- do not change a value here without updating it.
OPERATION_REINVOKE_SAFETY: dict[Operation, ReinvokeSafety] = {
    Operation.QUO_SMS_SEND: ReinvokeSafety.CONFIRM_BY_READBACK,
    Operation.EMAIL_SEND: ReinvokeSafety.CONFIRM_BY_READBACK,
    Operation.CLIQ_CHAT_POST: ReinvokeSafety.SAFE_TO_REINVOKE,
    Operation.CLIQ_CHANNEL_POST: ReinvokeSafety.SAFE_TO_REINVOKE,
    Operation.CALENDAR_CREATE: ReinvokeSafety.NEVER_REINVOKE,
    Operation.CALENDAR_UPDATE: ReinvokeSafety.SAFE_TO_REINVOKE,
    Operation.CALENDAR_DELETE: ReinvokeSafety.SAFE_TO_REINVOKE,
    Operation.TENANTCLOUD_MESSAGE_SEND: ReinvokeSafety.SAFE_TO_REINVOKE,
    Operation.TENANTCLOUD_LEAD_STATUS_UPDATE: ReinvokeSafety.SAFE_TO_REINVOKE,
    Operation.TENANTCLOUD_MAINTENANCE_CREATE: ReinvokeSafety.SAFE_TO_REINVOKE,
    Operation.TENANTCLOUD_MAINTENANCE_STATUS_UPDATE: ReinvokeSafety.SAFE_TO_REINVOKE,
}


def reinvoke_safety(operation: Operation) -> ReinvokeSafety:
    """Look up ``operation``'s row. Raises on an operation this table has
    never classified -- silently defaulting a new operation to "safe" is
    exactly the mistake this module exists to prevent."""
    try:
        return OPERATION_REINVOKE_SAFETY[operation]
    except KeyError as error:
        raise ValueError(
            f"operation {operation.value!r} has no reinvoke-safety classification in idempotency_policy.py; "
            "add one (and the reasoning behind it) before routing this operation through Restate."
        ) from error


def must_never_resend_when_ambiguous(operation: Operation) -> bool:
    """True when an ambiguous, never-settled outcome for ``operation`` must
    end in the one staff warning rather than any further provider attempt --
    ``NEVER_REINVOKE`` specifically: no provider-side dedup AND no
    independent read-back."""
    return reinvoke_safety(operation) is ReinvokeSafety.NEVER_REINVOKE
