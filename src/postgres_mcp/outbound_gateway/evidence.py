"""Current database evidence for the calendar dependency preflight."""

from __future__ import annotations

from typing import Any

from postgres_mcp.sql import SafeSqlDriver

from .context import ActionContext
from .preflight import CalendarDependencyState
from .preflight import PreflightEvidence


class DatabasePreflightEvidenceLoader:
    """Loads the calendar dependency only. Newer messages are the
    stale-context question's (repository.newer_context); calendar owns slot
    selection."""

    def __init__(self, driver: Any):
        self._driver = driver

    async def load(self, context: ActionContext) -> PreflightEvidence:
        rows = await SafeSqlDriver.execute_param_query(
            self._driver,
            """
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
            """,
            [context.intent_kind, context.wakeup_event_id, context.wakeup_event_id],
        )
        if not rows:
            raise LookupError("preflight evidence query returned no row")
        return PreflightEvidence(calendar_dependency=CalendarDependencyState(str(rows[0].cells["calendar_dependency_state"])))
