"""Parameterized database reads for immutable outbound event context."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import Protocol
from uuid import UUID

from postgres_mcp.sql import SafeSqlDriver

from .traffic_control import InFlightAction
from .traffic_control import NewerActivity

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

    async def activity_after(
        self,
        recipient_key: str,
        channel_id: int,
        watermark: datetime,
        exclude_action_id: UUID,
        limit: int,
        exclude_refs: frozenset[str] = frozenset(),
    ) -> list[NewerActivity]: ...

    async def context_watermark(self, wakeup_event_id: int) -> datetime | None: ...

    async def acknowledged_refs(self, wakeup_event_id: int, recipient_key: str) -> frozenset[str]: ...

    async def messages_by_id(self, message_ids: list[int]) -> list[NewerActivity]: ...


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

    async def newest_activity_after(
        self, recipient_key: str, channel_id: int, watermark: datetime, exclude_action_id: UUID
    ) -> NewerActivity | None:
        """The single newest item of activity_after (kept for callers that
        only need to know whether anything is newer)."""
        items = await self.activity_after(recipient_key, channel_id, watermark, exclude_action_id, 1)
        return items[0] if items else None

    async def activity_after(
        self,
        recipient_key: str,
        channel_id: int,
        watermark: datetime,
        exclude_action_id: UUID,
        limit: int,
        exclude_refs: frozenset[str] = frozenset(),
    ) -> list[NewerActivity]:
        """Every ledger send and message newer than `watermark` that this
        recipient's context depends on, newest first, at most `limit`. The
        two arms apply the same exclusions the staleness gate always did;
        what changed is that a needs_confirmation result lists them all
        instead of naming only the newest."""
        limit = max(1, int(limit))
        excluded_actions = sorted(ref.removeprefix("action:") for ref in exclude_refs if ref.startswith("action:"))
        excluded_messages = sorted(
            int(ref.removeprefix("message:")) for ref in exclude_refs if ref.startswith("message:")
        )
        ledger_rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            WITH RECURSIVE retry_lineage AS (
                SELECT action_id, retry_of_action_id
                FROM outbound_actions
                WHERE action_id = {}

                UNION

                SELECT ancestor.action_id, ancestor.retry_of_action_id
                FROM outbound_actions AS ancestor
                JOIN retry_lineage AS child
                  ON ancestor.action_id = child.retry_of_action_id
            )
            SELECT
                action_id,
                operation,
                created_at,
                left(coalesce(arguments->>'text', arguments::text, ''), 300) AS preview
            FROM outbound_actions
            WHERE subject_key = {}
              AND action_id NOT IN (SELECT action_id FROM retry_lineage)
              -- The wake's own earlier actions: its agent sent them, so they
              -- are not newer context it has not seen (wake 27235).
              AND wakeup_event_id IS DISTINCT FROM (
                  SELECT own.wakeup_event_id FROM outbound_actions AS own
                  WHERE own.action_id = {}
              )
              AND NOT (action_id::text = ANY({}::text[]))
              AND created_at > {}
              AND (dispatch_started_at IS NOT NULL OR state = 'completed')
            ORDER BY created_at DESC
            LIMIT {}
            """,
            [exclude_action_id, recipient_key, exclude_action_id, excluded_actions, watermark, limit],
        )
        message_rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT
                message.id AS message_id,
                message.created_at,
                message.direction,
                message.source,
                sender.display_name AS sender_name,
                left(coalesce(message.body,''), 300) AS preview
            FROM messages AS message
            LEFT JOIN raw_events AS raw ON raw.id = message.raw_event_id
            LEFT JOIN participants AS sender ON sender.id = message.sender_participant_id
            LEFT JOIN outbound_actions AS sending ON sending.action_id = {}
            WHERE message.channel_id = {}
              AND message.created_at > {}
              AND NOT (message.id = ANY({}::bigint[]))
              -- A message the wake's own sends produced (the gateway records
              -- a TenantCloud send as a CDS message; a Cliq post is ingested
              -- back) is the agent's own work. Providers spell the id
              -- differently in the two places ("tenantcloud-message:1" vs
              -- "tenantcloud:thread-message:1", "a%20b" vs "a_b"), so compare
              -- the id after its last colon with punctuation removed.
              AND NOT EXISTS (
                  SELECT 1 FROM outbound_actions AS own
                  WHERE own.wakeup_event_id = sending.wakeup_event_id
                    AND nullif(regexp_replace(replace(regexp_replace(
                            own.provider_message_id, '^.*:', ''), '%20', ''),
                            '[^0-9A-Za-z]', '', 'g'), '')
                        = regexp_replace(replace(regexp_replace(
                            message.source_message_id, '^.*:', ''), '%20', ''),
                            '[^0-9A-Za-z]', '', 'g')
              )
              AND NOT (
                  -- Automated operations alerts share Nigel's Cliq DM with Dan.
                  -- They do not answer an inbound DM and must not stale its
                  -- internal_reply action. Keep human follow-ups in the probe.
                  -- Direction is not part of the test: the same bot-posted
                  -- alerts are stored `outbound` or `inbound` (14 days to
                  -- 2026-09-24: 101 vs 37, all from Nigel's own account), and
                  -- wake 27164's refusal was one labelled `inbound`. The
                  -- SENDER is: only Nigel's own Cliq user (agency_identifiers
                  -- cliq_user_id labelled nigel-zoho, the gateway's Nigel
                  -- account) posts these. A human pasting an alert still counts.
                  sending.operation = 'cliq.chat.post'
                  AND message.source = 'zoho_cliq'
                  AND message.body LIKE '⚠️ Cron issue —%'
                  -- coalesce: an unknown sender must never make NOT(...) NULL
                  -- and silently drop the row.
                  AND coalesce(sender.participant_key, '') IN (
                      SELECT agency.value
                      FROM agency_identifiers AS agency
                      WHERE agency.kind = 'cliq_user_id'
                        AND agency.label = 'nigel-zoho'
                  )
              )
              AND (
                  sending.operation IS DISTINCT FROM 'quo.sms.send'
                  OR (
                      -- Quo channels identify shared business lines, not people.
                      -- Match either endpoint: direction labels can be missing
                      -- or incorrect on imported messages. Never match empties.
                      lower(message.source) IN ('quo', 'openphone')
                      AND EXISTS (
                          SELECT 1
                          FROM (VALUES
                              (raw.payload#>'{{data,object,from}}'),
                              (raw.payload#>'{{data,object,to}}')
                          ) AS endpoint(value)
                          CROSS JOIN LATERAL jsonb_array_elements_text(
                              CASE WHEN jsonb_typeof(endpoint.value) = 'array'
                                   THEN endpoint.value
                                   ELSE jsonb_build_array(endpoint.value) END
                          ) AS phone(value)
                          WHERE nullif(regexp_replace(phone.value, '[^0-9]', '', 'g'), '')
                              = nullif(regexp_replace(
                                  sending.canonical_context->>'recipient_phone', '[^0-9]', '', 'g'
                              ), '')
                      )
                  )
              )
            ORDER BY message.created_at DESC
            LIMIT {}
            """,
            [exclude_action_id, channel_id, watermark, excluded_messages, limit],
        )
        candidates: list[NewerActivity] = []
        for row in ledger_rows or []:
            cells = row.cells
            candidates.append(
                NewerActivity(
                    direction="outbound",
                    source="outbound_actions",
                    occurred_at=cells["created_at"],
                    preview=str(cells.get("preview") or ""),
                    message_id=None,
                    action_id=UUID(str(cells["action_id"])),
                    sender=f"outbound gateway ({cells.get('operation')})" if cells.get("operation") else None,
                )
            )
        for row in message_rows or []:
            cells = row.cells
            candidates.append(
                NewerActivity(
                    # NULL direction must not be silently reported as
                    # "inbound" -- that would make the staleness detail text
                    # claim inbound activity that was never actually
                    # confirmed as such. "unknown" is the honest label.
                    direction=str(cells.get("direction") or "unknown"),
                    source=str(cells.get("source") or "messages"),
                    occurred_at=cells["created_at"],
                    preview=str(cells.get("preview") or ""),
                    message_id=int(cells["message_id"]),
                    action_id=None,
                    sender=(str(cells["sender_name"]) if cells.get("sender_name") else None),
                )
            )
        candidates.sort(key=lambda item: item.occurred_at, reverse=True)
        return candidates[:limit]

    async def acknowledged_refs(self, wakeup_event_id: int, recipient_key: str) -> frozenset[str]:
        """Every item this wake's agent was shown for this recipient in a
        stale_context question (Comm-Data-Store migration 192). Identity, not
        time: an inbound stored with an earlier send time but ingested after
        the question was asked was never shown and is never in this set."""
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT DISTINCT shown.ref
            FROM outbound_actions AS action
            CROSS JOIN LATERAL unnest(action.stale_context_shown_refs) AS shown(ref)
            WHERE action.wakeup_event_id = {}
              AND action.subject_key = {}
              AND action.state = 'stale'
            """,
            [wakeup_event_id, recipient_key],
        )
        return frozenset(str(row.cells["ref"]) for row in rows or [])

    async def messages_by_id(self, message_ids: list[int]) -> list[NewerActivity]:
        if not message_ids:
            return []
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT
                message.id AS message_id,
                message.created_at,
                message.direction,
                message.source,
                sender.display_name AS sender_name,
                left(coalesce(message.body,''), 300) AS preview
            FROM messages AS message
            LEFT JOIN participants AS sender ON sender.id = message.sender_participant_id
            WHERE message.id = ANY({}::bigint[])
            ORDER BY message.created_at DESC
            """,
            [sorted(message_ids)],
        )
        return [
            NewerActivity(
                direction=str(row.cells.get("direction") or "unknown"),
                source=str(row.cells.get("source") or "messages"),
                occurred_at=row.cells["created_at"],
                preview=str(row.cells.get("preview") or ""),
                message_id=int(row.cells["message_id"]),
                action_id=None,
                sender=(str(row.cells["sender_name"]) if row.cells.get("sender_name") else None),
            )
            for row in rows or []
        ]

    async def context_watermark(self, wakeup_event_id: int) -> datetime | None:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            SELECT coalesce(webui_accepted_at, created_at) AS watermark
            FROM hermes_wakeup_events
            WHERE id = {}
            """,
            [wakeup_event_id],
        )
        if not rows:
            return None
        return rows[0].cells["watermark"]
