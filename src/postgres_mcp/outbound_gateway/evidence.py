"""Current database evidence for outbound safety preflight."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Mapping

from postgres_mcp.sql import SafeSqlDriver

from .context import ActionContext
from .preflight import CalendarDependencyState
from .preflight import PreflightEvidence
from .preflight import RefreshEvidence
from .preflight import RefreshStatus


class DatabasePreflightEvidenceLoader:
    """Loads message/dependency facts only. Calendar owns slot selection."""

    def __init__(self, driver: Any):
        self._driver = driver

    async def load(self, context: ActionContext) -> PreflightEvidence:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
            WITH conversation AS (
                SELECT
                    max(message_row.id) FILTER (
                        WHERE message_row.id > {}
                          AND message_row.direction IS DISTINCT FROM 'outbound'
                    ) AS later_inbound_message_id,
                    max(message_row.sent_at) AS latest_sent_at
                FROM messages AS message_row
                WHERE message_row.channel_id = {}
            ), verified_outbound AS (
                SELECT
                    message_row.id AS verified_outbound_message_id,
                    coalesce(
                        raw_row.payload->>'provider_request_ref',
                        raw_row.payload->>'request_ref',
                        raw_row.payload->>'provider_message_id',
                        raw_row.payload->>'message_id'
                    ) AS verified_outbound_request_ref
                FROM messages AS message_row
                LEFT JOIN raw_events AS raw_row ON raw_row.id = message_row.raw_event_id
                WHERE message_row.channel_id = {}
                  AND message_row.direction = 'outbound'
                  AND message_row.id > {}
                  AND nullif(btrim(coalesce(
                      raw_row.payload->>'provider_request_ref',
                      raw_row.payload->>'request_ref',
                      raw_row.payload->>'provider_message_id',
                      raw_row.payload->>'message_id'
                  )), '') IS NOT NULL
                ORDER BY message_row.id DESC
                LIMIT 1
            ), dependency AS (
                SELECT CASE
                    WHEN {} NOT IN (
                        'showing_confirmation', 'showing_reschedule',
                        'showing_cancellation'
                    ) THEN 'not_required'
                    WHEN EXISTS (
                        SELECT 1 FROM outbound_actions
                        WHERE wakeup_event_id = {}
                          AND action_role = 'calendar_mutation'
                          AND state = 'completed'
                    ) THEN 'completed'
                    WHEN EXISTS (
                        SELECT 1 FROM outbound_actions
                        WHERE wakeup_event_id = {}
                          AND action_role = 'calendar_mutation'
                          AND state IN (
                              'rejected', 'definitive_failed', 'dead_letter',
                              'manual_review'
                          )
                    ) THEN 'failed'
                    ELSE 'pending'
                END AS calendar_dependency_state
            )
            SELECT
                conversation.later_inbound_message_id,
                verified_outbound.verified_outbound_message_id,
                verified_outbound.verified_outbound_request_ref,
                coalesce(conversation.latest_sent_at, {}::timestamptz) AS latest_sent_at,
                dependency.calendar_dependency_state,
                false AS calendar_already_applied
            FROM conversation
            CROSS JOIN dependency
            LEFT JOIN verified_outbound ON true
            """,
            [
                context.source_message_id,
                context.channel_id,
                context.channel_id,
                context.source_message_id,
                context.intent_kind.value,
                context.wakeup_event_id,
                context.wakeup_event_id,
                context.source_sent_at,
            ],
        )
        if not rows:
            raise LookupError("preflight evidence query returned no row")
        cells = rows[0].cells
        verified_id = cells.get("verified_outbound_message_id")
        verified_ref = cells.get("verified_outbound_request_ref")
        latest_sent_at = cells.get("latest_sent_at") or context.source_sent_at
        return PreflightEvidence(
            current_recipient_id=context.target.target_id,
            current_property_id=context.property_id,
            current_appointment_slot=context.appointment_slot,
            later_inbound_message_id=cells.get("later_inbound_message_id"),
            verified_outbound_message_id=verified_id,
            verified_outbound_request_ref=verified_ref,
            verified_outbound_covers_source=bool(verified_id and verified_ref),
            calendar_dependency=CalendarDependencyState(str(cells["calendar_dependency_state"])),
            calendar_already_applied=bool(cells.get("calendar_already_applied")),
            calendar_context_changed=False,
            overlapping_showing_prospect_ids=(),
            refresh_required_through=latest_sent_at,
            refresh=self._refresh(context.refresh_evidence),
        )

    @staticmethod
    def _refresh(value: Mapping[str, Any]) -> RefreshEvidence | None:
        if not value:
            return None
        try:
            status = RefreshStatus(str(value["status"]))
            covered_through = _datetime(value.get("covered_through"))
            thread = str(value["covered_thread_identity"])
            attempts = int(value["attempt_count"])
        except (KeyError, TypeError, ValueError):
            return RefreshEvidence(
                status=RefreshStatus.FAILED,
                covered_through=None,
                covered_thread_identity="",
                attempt_count=0,
                identity_resolved=False,
                thread_resolved=False,
                property_resolved=False,
            )
        return RefreshEvidence(
            status=status,
            covered_through=covered_through,
            covered_thread_identity=thread,
            attempt_count=attempts,
            identity_resolved=bool(value.get("identity_resolved", True)),
            thread_resolved=bool(value.get("thread_resolved", True)),
            property_resolved=bool(value.get("property_resolved", True)),
        )


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)
