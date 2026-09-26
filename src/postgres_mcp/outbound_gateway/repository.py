"""Parameterized database reads for immutable outbound event context."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import Protocol
from uuid import UUID

from postgres_mcp.sql import SafeSqlDriver

from .models import ActionRole
from .models import NewerActivity
from .models import Operation
from .traffic_control import InFlightAction

if TYPE_CHECKING:
    from .context import ActionContext

# Non-terminal outbound_actions.state values: an action in one of these
# states still has an in-flight lease on its recipient. Everything else
# (completed, stale, rejected, definitive_failed, dead_letter,
# manual_review) is terminal or parked and does not hold the lease.
NON_TERMINAL_STATES = (
    "received",
    "dependency_wait",
    "prepared",
    "dispatching",
    "provider_accepted",
    "unknown",
    "reconciling",
    "retry_ready",
)


@dataclass(frozen=True)
class WakeEventRecord:
    wakeup_event_id: int
    event_source: str
    source_event_id: str
    event_created_at: datetime
    message_id: int
    canonical_message_id: int | None
    message_source: str
    source_message_id: str
    message_sent_at: datetime
    message_updated_at: datetime
    subject: str | None
    body: str | None
    user_account_id: str | None
    channel_id: int
    source_channel_id: str
    channel_type: str
    channel_name: str | None
    sender_participant_id: int | None
    participant_type: str | None
    participant_key: str | None
    display_name: str | None
    envelope: dict[str, Any]
    raw_payload: dict[str, Any]
    tenantcloud_claim_id: int | None = None
    tenantcloud_claim_family: str | None = None
    tenantcloud_claim_state: str | None = None
    tenantcloud_action_owner: str | None = None
    tenantcloud_entity_scope_key: str | None = None
    provenance: str = "customer"
    qualification_run_id: str | None = None


@dataclass(frozen=True)
class ConversationSnapshot:
    conversation_watermark: int
    latest_message_id: int
    latest_sent_at: datetime


@dataclass(frozen=True)
class AliasResolution:
    canonical_subject: str | None
    ambiguous: bool = False


class ContextRepository(Protocol):
    async def load_wake_event(self, wakeup_event_id: int) -> WakeEventRecord | None: ...

    async def load_conversation_snapshot(self, channel_id: int) -> ConversationSnapshot: ...

    async def resolve_canonical_subject(
        self,
        aliases: tuple[str, ...],
        property_scope: str,
    ) -> AliasResolution: ...

    async def in_flight_actions(
        self, recipient_key: str, exclude_action_id: UUID
    ) -> list[InFlightAction]: ...

    async def newer_context(
        self,
        context: ActionContext,
        *,
        limit: int,
        waive_shown: bool,
        as_of: datetime | None = None,
    ) -> list[NewerActivity]: ...


class OutboundGatewayRepository:
    """SQL-only repository. Derivation and policy stay in separate modules."""

    def __init__(self, driver: Any):
        self._driver = driver

    async def load_wake_event(self, wakeup_event_id: int) -> WakeEventRecord | None:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT
                event_row.id AS wakeup_event_id,
                event_row.source AS event_source,
                event_row.source_event_id,
                event_row.created_at AS event_created_at,
                event_row.provenance,
                event_row.qualification_run_id,
                message_row.id AS message_id,
                message_row.canonical_message_id,
                message_row.source AS message_source,
                message_row.source_message_id,
                message_row.sent_at AS message_sent_at,
                message_row.updated_at AS message_updated_at,
                message_row.subject,
                message_row.body,
                message_row.user_account_id,
                channel_row.id AS channel_id,
                channel_row.source_channel_id,
                channel_row.channel_type,
                channel_row.name AS channel_name,
                participant_row.id AS sender_participant_id,
                participant_row.participant_type,
                participant_row.participant_key,
                participant_row.display_name,
                event_row.envelope,
                coalesce(raw_row.payload, '{{}}'::jsonb) AS raw_payload,
                event_row.tenantcloud_claim_id,
                claim_row.event_family AS tenantcloud_claim_family,
                claim_row.claim_state AS tenantcloud_claim_state,
                claim_row.action_owner AS tenantcloud_action_owner,
                claim_row.entity_scope_key AS tenantcloud_entity_scope_key
            FROM hermes_wakeup_events AS event_row
            JOIN messages AS message_row ON message_row.id = event_row.message_id
            JOIN channels AS channel_row ON channel_row.id = message_row.channel_id
            LEFT JOIN participants AS participant_row
              ON participant_row.id = message_row.sender_participant_id
            LEFT JOIN raw_events AS raw_row ON raw_row.id = message_row.raw_event_id
            LEFT JOIN tenantcloud_event_claims AS claim_row
              ON claim_row.claim_id = event_row.tenantcloud_claim_id
            WHERE event_row.id = {}
            """,
            [wakeup_event_id],
        )
        if not rows:
            return None
        return WakeEventRecord(**rows[0].cells)

    async def load_conversation_snapshot(self, channel_id: int) -> ConversationSnapshot:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT
                coalesce(max(id), 0) AS conversation_watermark,
                coalesce(max(id), 0) AS latest_message_id,
                coalesce(max(sent_at), '-infinity'::timestamptz) AS latest_sent_at
            FROM messages
            WHERE channel_id = {}
            """,
            [channel_id],
        )
        if not rows:
            raise LookupError(f"conversation channel {channel_id} is unavailable")
        return ConversationSnapshot(**rows[0].cells)

    async def resolve_canonical_subject(
        self,
        aliases: tuple[str, ...],
        property_scope: str,
    ) -> AliasResolution:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT
                count(DISTINCT canonical_subject)::integer AS subject_count,
                min(canonical_subject) AS canonical_subject
            FROM outbound_action_subject_aliases
            WHERE alias_key = ANY({})
              AND scope_key IN ('', {})
            """,
            [list(aliases), property_scope],
        )
        if not rows:
            return AliasResolution(canonical_subject=None)
        cells = rows[0].cells
        return AliasResolution(
            canonical_subject=cells.get("canonical_subject"),
            ambiguous=int(cells.get("subject_count") or 0) > 1,
        )

    async def in_flight_actions(
        self, recipient_key: str, exclude_action_id: UUID
    ) -> list[InFlightAction]:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT
                action_id,
                operation,
                state,
                created_at,
                left(coalesce(arguments::text,''), 120) AS preview
            FROM outbound_actions
            WHERE subject_key = {}
              AND action_id <> {}
              -- The wake's own other actions are its agent's work, not
              -- someone else's send racing this one (CDS migration 204).
              AND wakeup_event_id IS DISTINCT FROM (
                  SELECT own.wakeup_event_id FROM outbound_actions AS own
                  WHERE own.action_id = {}
              )
              AND state = ANY({})
            ORDER BY created_at
            """,
            [recipient_key, exclude_action_id, exclude_action_id, list(NON_TERMINAL_STATES)],
        )
        return [
            InFlightAction(
                action_id=UUID(str(row.cells["action_id"])),
                operation=str(row.cells["operation"]),
                state=str(row.cells["state"]),
                created_at=row.cells["created_at"],
                preview=str(row.cells.get("preview") or ""),
            )
            for row in rows or []
        ]

    async def newer_context(
        self,
        context: ActionContext,
        *,
        limit: int,
        waive_shown: bool,
        as_of: datetime | None = None,
    ) -> list[NewerActivity]:
        """The stale-context question's one query: every message received
        from, or sent by us to, this action's recipient that is not in the
        agent's context, newest first, at most `limit`. Not in its context: on
        the wake's own channel, reached CDS (received_at) after the wake's
        context watermark (hermes_wakeup_events accepted/created time); on
        any other channel, sent after the message being answered. Never a
        re-ingest or re-scrape of a message that had reached CDS by the
        watermark. waive_shown leaves out what this wake's agent was already
        shown for this recipient (migration 192), by identity.

        Received: a message in the recipient's conversation that is not ours
        -- the wake's own channel (a Quo reply only from/to that phone, since a
        Quo channel is a line), the Zillow relay address across channels, or
        the same Quo line, conversation and phone across channels.
        Sent by us: a message to this action's own target on the operation's
        channel family (an email to the address, a text to the phone, a post
        in that Cliq chat, a message in that TenantCloud thread), and another
        wake's gateway send to the same subject that started dispatch.
        Never this wake's own sends, the source message or its duplicates,
        certified-older Zillow scrapes, or Nigel's automated cron alerts.

        as_of: replay only -- the world as it stood at that time."""
        target = outbound_target(context)
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            WITH RECURSIVE p AS (
                SELECT
                    {}::bigint AS wake,
                    {}::uuid AS action_id,
                    {}::text AS subject,
                    {}::bigint AS channel_id,
                    {}::timestamptz AS source_sent_at,
                    {}::bigint AS source_message_id,
                    {}::text AS family,
                    {}::text AS target_id,
                    {}::text AS provider_account,
                    {}::text AS thread_identity,
                    {}::text AS phone,
                    {}::text AS operation,
                    {}::bigint[] AS equivalent_ids,
                    {}::bigint[] AS certified_older_ids,
                    {}::text[] AS sent_sources,
                    {}::text AS sent_match,
                    {}::text AS sent_recipient,
                    {}::boolean AS waive_shown,
                    {}::timestamptz AS as_of
            ), watermark AS (
                SELECT coalesce(event.webui_accepted_at, event.created_at) AS at
                FROM hermes_wakeup_events AS event, p
                WHERE event.id = p.wake
            ), shown AS (
                SELECT DISTINCT shown.ref
                FROM outbound_actions AS action
                CROSS JOIN LATERAL unnest(action.stale_context_shown_refs) AS shown(ref), p
                WHERE p.waive_shown
                  AND action.wakeup_event_id = p.wake
                  AND action.subject_key = p.subject
                  AND action.state = 'stale'
            ), own_send AS (
                -- This wake's own sends are its agent's work, not news.
                -- Providers spell the id differently in the ledger and in
                -- messages ("tenantcloud-message:1" vs
                -- "tenantcloud:thread-message:1", "a%20b" vs "a_b"): compare
                -- the id after its last colon with punctuation removed.
                SELECT
                    own.action_id,
                    nullif(regexp_replace(replace(regexp_replace(
                        own.provider_message_id, '^.*:', ''), '%20', ''),
                        '[^0-9A-Za-z]', '', 'g'), '') AS message_key
                FROM outbound_actions AS own, p
                WHERE own.wakeup_event_id = p.wake
            ), candidate AS (
                SELECT
                    message.id,
                    message.created_at,
                    message.channel_id,
                    message.sent_at,
                    message.received_at,
                    message.canonical_message_id,
                    message.source,
                    message.source_message_id,
                    message.body,
                    raw.payload,
                    sender.participant_type,
                    sender.participant_key,
                    sender.display_name,
                    CASE
                        WHEN lower(coalesce(
                            message.direction,
                            raw.payload->>'direction',
                            raw.payload#>>'{{data,object,direction}}',
                            ''
                        )) IN ('outbound', 'outgoing', 'sent') THEN 'outbound'
                        ELSE coalesce(nullif(message.direction, ''), 'inbound')
                    END AS direction
                FROM messages AS message
                CROSS JOIN p
                CROSS JOIN watermark
                LEFT JOIN raw_events AS raw ON raw.id = message.raw_event_id
                LEFT JOIN participants AS sender ON sender.id = message.sender_participant_id
                -- received_at is when a message reached CDS. created_at is a
                -- generated alias of sent_at, and a web-extract scrape's
                -- sent_at is the scraped timestamp, a day before it lands.
                WHERE (p.as_of IS NULL OR message.received_at <= p.as_of)
                  AND (
                      -- The wake's own channel is in the agent's context up
                      -- to the watermark; anything that reached CDS after it
                      -- is new.
                      (message.channel_id = p.channel_id AND message.received_at > watermark.at)
                      -- Other channels are not in its context: anything sent
                      -- after the message it is answering is new.
                      OR (
                          message.channel_id IS DISTINCT FROM p.channel_id
                          AND (message.sent_at, message.id) > (p.source_sent_at, p.source_message_id)
                      )
                  )
                  -- The message being answered, its duplicates, and the
                  -- scrapes certified older than it are the context itself.
                  AND NOT (coalesce(message.canonical_message_id, message.id) = ANY(p.equivalent_ids))
                  AND NOT (message.id = ANY(p.certified_older_ids))
                  AND NOT (coalesce(message.canonical_message_id, message.id) = ANY(p.certified_older_ids))
                  -- A re-ingest or re-scrape of a message that had reached
                  -- CDS by the watermark is not new: the same canonical
                  -- message, or -- a scrape carries no canonical id -- the
                  -- same source, sender, text and send time (within a minute:
                  -- scrapes round to it), on any channel.
                  AND NOT EXISTS (
                      SELECT 1 FROM messages AS seen
                      WHERE message.canonical_message_id IS NOT NULL
                        AND (seen.id = message.canonical_message_id OR seen.canonical_message_id = message.canonical_message_id)
                        AND seen.id <> message.id
                        AND seen.received_at <= watermark.at
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM messages AS seen
                      WHERE seen.source = message.source
                        AND seen.sent_at BETWEEN message.sent_at - interval '1 minute' AND message.sent_at + interval '1 minute'
                        AND seen.id <> message.id
                        AND seen.received_at <= watermark.at
                        AND seen.sender_participant_id IS NOT DISTINCT FROM message.sender_participant_id
                        AND nullif(regexp_replace(lower(coalesce(seen.body, '')), '[^a-z0-9]', '', 'g'), '')
                            = regexp_replace(lower(coalesce(message.body, '')), '[^a-z0-9]', '', 'g')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM own_send
                      WHERE own_send.message_key = regexp_replace(replace(regexp_replace(
                              message.source_message_id, '^.*:', ''), '%20', ''),
                              '[^0-9A-Za-z]', '', 'g')
                         OR raw.payload->>'outbound_action_id' = own_send.action_id::text
                  )
                  AND NOT (
                      -- Automated operations alerts share Nigel's Cliq DM with
                      -- Dan and answer nothing. Direction labels are unreliable
                      -- for them (stored outbound and inbound alike); the
                      -- sender is not: only Nigel's own Cliq user posts them.
                      -- A human pasting an alert still counts. coalesce: an
                      -- unknown sender must never make NOT(...) NULL.
                      p.operation = 'cliq.chat.post'
                      AND message.source = 'zoho_cliq'
                      AND message.body LIKE '⚠️ Cron issue —%'
                      AND coalesce(sender.participant_key, '') IN (
                          SELECT agency.value
                          FROM agency_identifiers AS agency
                          WHERE agency.kind = 'cliq_user_id'
                            AND agency.label = 'nigel-zoho'
                      )
                  )
            ), received AS (
                SELECT candidate.*
                FROM candidate, p
                WHERE candidate.direction <> 'outbound'
                  AND (
                      (
                          candidate.channel_id = p.channel_id
                          AND (
                              -- A Quo channel is a shared business line, not a
                              -- person: only this phone's messages count.
                              -- Either endpoint (direction labels can be
                              -- missing); never match empties.
                              p.operation IS DISTINCT FROM 'quo.sms.send'
                              OR (
                                  lower(candidate.source) IN ('quo', 'openphone')
                                  AND EXISTS (
                                      SELECT 1
                                      FROM (VALUES
                                          (candidate.payload#>'{{data,object,from}}'),
                                          (candidate.payload#>'{{data,object,to}}')
                                      ) AS endpoint(value)
                                      CROSS JOIN LATERAL jsonb_array_elements_text(
                                          CASE WHEN jsonb_typeof(endpoint.value) = 'array'
                                               THEN endpoint.value
                                               ELSE jsonb_build_array(endpoint.value) END
                                      ) AS endpoint_phone(value)
                                      WHERE nullif(regexp_replace(endpoint_phone.value, '[^0-9]', '', 'g'), '')
                                          = nullif(p.phone, '')
                                  )
                              )
                          )
                      ) OR (
                          -- The same Zillow relay address on any channel.
                          p.family = 'zillow'
                          AND (
                              lower(coalesce(
                                  candidate.payload->>'proxy_email',
                                  candidate.payload->>'zillow_proxy_email',
                                  candidate.payload->>'relay_email',
                                  CASE
                                      WHEN lower(coalesce(candidate.participant_type, ''))
                                               IN ('email', 'email_address')
                                        AND split_part(lower(coalesce(candidate.participant_key, '')), '@', 2)
                                            = 'convo.zillow.com'
                                      THEN candidate.participant_key
                                  END,
                                  ''
                              )) = lower(p.target_id)
                              OR EXISTS (
                                  SELECT 1
                                  FROM jsonb_array_elements(
                                      CASE
                                          WHEN jsonb_typeof(candidate.payload->'participants') = 'array'
                                          THEN candidate.payload->'participants'
                                          ELSE '[]'::jsonb
                                      END
                                  ) AS recipient(value)
                                  WHERE lower(coalesce(recipient.value->>'kind', '')) = 'to'
                                    AND lower(coalesce(recipient.value->>'address', '')) = lower(p.target_id)
                              )
                          )
                      ) OR (
                          -- The same Quo line, conversation and phone on any channel.
                          p.family = 'quo'
                          AND lower(candidate.source) IN ('quo', 'openphone')
                          AND lower(coalesce(
                              candidate.payload#>>'{{data,object,phoneNumberId}}',
                              candidate.payload#>>'{{data,object,phone_number_id}}',
                              ''
                          )) = lower(p.provider_account)
                          AND (
                              lower(coalesce(
                                  candidate.payload#>>'{{data,object,conversationId}}',
                                  candidate.payload#>>'{{data,object,conversation_id}}',
                                  ''
                              )) = lower(p.thread_identity)
                              OR (
                                  nullif(coalesce(
                                      candidate.payload#>>'{{data,object,conversationId}}',
                                      candidate.payload#>>'{{data,object,conversation_id}}',
                                      ''
                                  ), '') IS NULL
                                  AND p.thread_identity LIKE 'line:%'
                              )
                          )
                          AND regexp_replace(
                              coalesce(candidate.payload#>>'{{data,object,from}}', ''), '[^0-9]', '', 'g'
                          ) = p.phone
                      )
                  )
            ), sent_message AS (
                SELECT candidate.*
                FROM candidate
                CROSS JOIN p
                JOIN channels AS candidate_channel ON candidate_channel.id = candidate.channel_id
                WHERE candidate.direction = 'outbound'
                  AND candidate.source = ANY(p.sent_sources)
                  AND CASE p.sent_match
                      WHEN 'email' THEN EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(
                              CASE
                                  WHEN jsonb_typeof(candidate.payload->'participants') = 'array'
                                  THEN candidate.payload->'participants'
                                  ELSE '[]'::jsonb
                              END
                          ) AS recipient(value)
                          WHERE lower(coalesce(recipient.value->>'kind', '')) IN ('to', 'cc', 'bcc')
                            AND lower(btrim(coalesce(recipient.value->>'address', ''))) = p.sent_recipient
                      )
                      WHEN 'sms' THEN EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements_text(
                              CASE
                                  WHEN jsonb_typeof(candidate.payload#>'{{data,object,to}}') = 'array'
                                  THEN candidate.payload#>'{{data,object,to}}'
                                  WHEN jsonb_typeof(candidate.payload#>'{{data,object,to}}') = 'string'
                                  THEN jsonb_build_array(candidate.payload#>'{{data,object,to}}')
                                  ELSE '[]'::jsonb
                              END
                          ) AS to_phone(value)
                          WHERE nullif(regexp_replace(to_phone.value, '[^0-9]', '', 'g'), '') = p.sent_recipient
                      )
                      WHEN 'channel' THEN candidate_channel.source_channel_id = p.sent_recipient
                      ELSE false
                  END
            ), retry_lineage AS (
                SELECT action.action_id, action.retry_of_action_id
                FROM outbound_actions AS action, p
                WHERE action.action_id = p.action_id
                UNION
                SELECT ancestor.action_id, ancestor.retry_of_action_id
                FROM outbound_actions AS ancestor
                JOIN retry_lineage AS child ON ancestor.action_id = child.retry_of_action_id
            ), sent_action AS (
                -- Another wake's gateway send to this subject that started
                -- dispatch (it may not be ingested back as a message yet).
                SELECT
                    ledger.action_id,
                    ledger.operation,
                    ledger.created_at,
                    left(coalesce(ledger.arguments->>'text', ledger.arguments::text, ''), 300) AS preview
                FROM outbound_actions AS ledger
                CROSS JOIN p
                CROSS JOIN watermark
                WHERE ledger.subject_key = p.subject
                  AND ledger.wakeup_event_id IS DISTINCT FROM p.wake
                  AND ledger.created_at > watermark.at
                  AND (p.as_of IS NULL OR ledger.created_at <= p.as_of)
                  AND (ledger.dispatch_started_at IS NOT NULL OR ledger.state = 'completed')
                  AND ledger.action_id NOT IN (SELECT retry_lineage.action_id FROM retry_lineage)
            ), found AS (
                SELECT 'message:' || id AS ref, id AS message_id, NULL::uuid AS action_id, created_at,
                       direction, source, display_name AS sender, left(coalesce(body, ''), 300) AS preview
                FROM received
                UNION
                SELECT 'message:' || id, id, NULL::uuid, created_at,
                       direction, source, display_name, left(coalesce(body, ''), 300)
                FROM sent_message
                UNION
                SELECT 'action:' || action_id, NULL::bigint, action_id, created_at,
                       'outbound', 'outbound_actions', 'outbound gateway (' || operation || ')', preview
                FROM sent_action
            )
            SELECT found.*
            FROM found
            WHERE found.ref NOT IN (SELECT shown.ref FROM shown)
            ORDER BY found.created_at DESC, found.ref
            LIMIT {}
            """,
            [
                context.wakeup_event_id,
                context.action_id,
                context.prospect_id,
                context.channel_id,
                context.source_sent_at,
                context.source_message_id,
                "zillow" if context.source in _ZILLOW_FAMILY else context.source,
                context.target.target_id,
                context.provider_account,
                context.thread_identity,
                "".join(character for character in str(context.recipient_phone or "") if character.isdigit()),
                context.operation.value,
                sorted({context.source_message_id, *context.cross_channel_duplicate_message_ids}),
                list(context.certified_older_message_ids),
                list(target.sources),
                target.match,
                target.recipient,
                waive_shown,
                as_of,
                max(1, int(limit)),
            ],
        )
        return [
            NewerActivity(
                direction=str(row.cells["direction"]),
                source=str(row.cells["source"]),
                occurred_at=row.cells["created_at"],
                preview=str(row.cells.get("preview") or ""),
                message_id=int(row.cells["message_id"]) if row.cells.get("message_id") is not None else None,
                action_id=UUID(str(row.cells["action_id"])) if row.cells.get("action_id") is not None else None,
                sender=str(row.cells["sender"]) if row.cells.get("sender") else None,
            )
            for row in rows or []
        ]


_ZILLOW_FAMILY = frozenset({"hotpads", "zillow", "zumper"})


@dataclass(frozen=True)
class OutboundTarget:
    """How to recognise a message we sent to the action's own target: which
    message sources are the operation's channel family, how the recipient is
    matched (`email`, `sms`, `channel`, or `none`), and the normalized
    recipient key."""

    sources: tuple[str, ...]
    match: str
    recipient: str


_NO_OUTBOUND_TARGET = OutboundTarget(sources=(), match="none", recipient="")

# Roles whose target is the counterpart of the conversation: a reply. An
# internal notification's target is an operator's chat, where other wakes'
# notifications about other subjects are not this action's context.
_REPLY_ROLES = frozenset({ActionRole.PROSPECT_REPLY, ActionRole.INTERNAL_REPLY})


def outbound_target(context: ActionContext) -> OutboundTarget:
    """The target key a "sent by us" message is matched on (PR #48): keyed on
    the recipient, never on the source conversation -- a Cliq post in the
    wake's DM is not an email's activity (wake 27279), and a Quo channel is a
    line, a mail channel a folder."""
    from .context import normalize_phone

    if context.action_role not in _REPLY_ROLES:
        return _NO_OUTBOUND_TARGET
    target = context.target.target_id.strip()
    if not target:
        return _NO_OUTBOUND_TARGET
    if context.operation is Operation.EMAIL_SEND:
        return OutboundTarget(sources=("zoho_mail", "nigel_mail"), match="email", recipient=target.casefold())
    if context.operation is Operation.QUO_SMS_SEND:
        digits = "".join(character for character in (normalize_phone(target) or target) if character.isdigit())
        return OutboundTarget(sources=("quo", "openphone"), match="sms", recipient=digits) if digits else _NO_OUTBOUND_TARGET
    if context.operation in {Operation.CLIQ_CHANNEL_POST, Operation.CLIQ_CHAT_POST}:
        return OutboundTarget(sources=("zoho_cliq",), match="channel", recipient=target)
    if context.operation is Operation.TENANTCLOUD_MESSAGE_SEND:
        return OutboundTarget(sources=("tenantcloud_api",), match="channel", recipient=f"tenantcloud:thread:{target}")
    return _NO_OUTBOUND_TARGET
