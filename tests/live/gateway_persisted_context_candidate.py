"""Persisted-context prepare/resume checks for v2-internal durable IDs.

Executed inside the candidate by scripts/qualify-outbound-gateway.sh after CDS's
bundled live suite. Uses the production ActionContextLoader + service against
the candidate PostgreSQL, and observes results through public MCP status.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import sys
from uuid import UUID
from uuid import uuid5

from psycopg import Connection
from psycopg.rows import DictRow
from psycopg.rows import dict_row

sys.path.insert(0, "/repo")
qualification = importlib.import_module("tests.system.test_outbound_gateway_qualification_candidate")

from postgres_mcp.outbound_gateway.context import ACTION_NAMESPACE  # noqa: E402
from postgres_mcp.outbound_gateway.models import ActionRole  # noqa: E402
from postgres_mcp.outbound_gateway.models import PublicStatus  # noqa: E402
from postgres_mcp.outbound_gateway.models import parse_outbound_request  # noqa: E402
from postgres_mcp.outbound_gateway.server import build_runtime  # noqa: E402


def _v1_action_id(wakeup_event_id: int) -> UUID:
    return uuid5(
        ACTION_NAMESPACE,
        f"v1:wakeup:{wakeup_event_id}:role:{ActionRole.PROSPECT_REPLY}:ordinal:0",
    )


async def _status(url: str, action_id: UUID) -> dict:
    return await qualification._execute(url, {"op": "status", "action_id": str(action_id)})


async def qualify() -> None:
    run_id = os.environ["MAINT_DOCKER_RUN_ID"]
    url = os.environ["COMM_DATA_STORE_CANDIDATE_GATEWAY_URL"]
    runtime = await build_runtime()
    try:
        with Connection[DictRow].connect(
            os.environ["COMM_DATA_STORE_QUALIFICATION_ADMIN_DSN"],
            row_factory=dict_row,
        ) as conn:
            wake = qualification._create_wake(
                conn,
                run_id,
                "persisted-context",
                "email.send",
                "email_thread",
                "persisted-context@example.invalid",
            )
            bad_wake = qualification._create_wake(
                conn,
                run_id,
                "persisted-context-bad",
                "email.send",
                "email_thread",
                "persisted-context-bad@example.invalid",
            )
            conn.commit()

        request = parse_outbound_request(
            {
                "op": "execute",
                "wakeup_event_id": wake,
                "action_role": "prospect_reply",
                "operation": "email.send",
                "intent_kind": "inquiry_reply",
                "arguments": {
                    "to_address": "persisted-context@example.invalid",
                    "text": "resume after prepare",
                },
            }
        )
        context = await runtime.service._context_loader.load(request)
        action = await runtime.store.create_or_load(context)
        with Connection[DictRow].connect(
            os.environ["COMM_DATA_STORE_QUALIFICATION_ADMIN_DSN"],
            row_factory=dict_row,
        ) as conn:
            row = conn.execute(
                "SELECT action_id, identity_version, payload_hash, state "
                "FROM outbound_actions WHERE action_id=%s",
                (action.action_id,),
            ).fetchone()
        assert row is not None
        assert row["identity_version"] == "v2-internal"
        assert row["payload_hash"] == context.payload_hash
        assert action.action_id != context.action_id
        assert action.action_id != _v1_action_id(wake)
        assert row["state"] == "received"

        prepared = await runtime.service.prepare(action.action_id)
        assert prepared.detail_code != "persisted_context_mismatch", prepared
        assert prepared.status is PublicStatus.PENDING
        assert prepared.action_id == action.action_id

        sent = await runtime.service.resume(action.action_id)
        assert sent.detail_code != "persisted_context_mismatch", sent
        assert sent.status is PublicStatus.SENT, sent
        assert sent.action_id == action.action_id

        observed = await _status(url, action.action_id)
        assert observed["status"] == "sent"
        assert observed["action_id"] == str(action.action_id)

        bad_request = parse_outbound_request(
            {
                "op": "execute",
                "wakeup_event_id": bad_wake,
                "action_role": "prospect_reply",
                "operation": "email.send",
                "intent_kind": "inquiry_reply",
                "arguments": {
                    "to_address": "persisted-context-bad@example.invalid",
                    "text": "must reject altered hash",
                },
            }
        )
        bad_context = await runtime.service._context_loader.load(bad_request)
        bad_action = await runtime.store.create_or_load(bad_context)
        prepared_bad = await runtime.service.prepare(bad_action.action_id)
        assert prepared_bad.status is PublicStatus.PENDING
        with Connection[DictRow].connect(
            os.environ["COMM_DATA_STORE_QUALIFICATION_ADMIN_DSN"],
            row_factory=dict_row,
        ) as conn:
            conn.execute(
                "UPDATE outbound_actions SET payload_hash=%s WHERE action_id=%s",
                ("f" * 64, bad_action.action_id),
            )
            conn.commit()
        rejected = await runtime.service.resume(bad_action.action_id)
        assert rejected.status is PublicStatus.MANUAL_REVIEW
        assert rejected.detail_code == "persisted_context_mismatch"
        observed_bad = await _status(url, bad_action.action_id)
        assert observed_bad["status"] == "manual_review"
        assert observed_bad["detail_code"] == "persisted_context_mismatch"
    finally:
        await runtime.pool.close()

    print(
        "persisted-context candidate: 2 passed "
        "(v2-internal prepare/resume accepted; altered hash rejected)"
    )


if __name__ == "__main__":
    asyncio.run(qualify())
