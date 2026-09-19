"""Enforce-mode internal identity and competing-recipient checks over public MCP.

Executed inside the candidate by scripts/qualify-outbound-gateway.sh, after CDS's
bundled live suite. Uses its synthetic wake helper and loopback provider stub.
"""

import asyncio
import importlib
import json
import os
import sys
from pathlib import Path
from uuid import UUID
from uuid import uuid5

from psycopg import Connection
from psycopg.rows import DictRow
from psycopg.rows import dict_row

sys.path.insert(0, "/repo")
qualification = importlib.import_module("tests.system.test_outbound_gateway_qualification_candidate")

from postgres_mcp.outbound_gateway.context import ACTION_NAMESPACE  # noqa: E402
from postgres_mcp.outbound_gateway.models import ActionRole  # noqa: E402


async def qualify():
    assert os.environ["OUTBOUND_TRAFFIC_CONTROL"] == "enforce"
    run_id = os.environ["MAINT_DOCKER_RUN_ID"]
    url = os.environ["COMM_DATA_STORE_CANDIDATE_GATEWAY_URL"]
    log = Path(os.environ["OUTBOUND_QUALIFICATION_STUB_LOG"])
    with Connection[DictRow].connect(os.environ["COMM_DATA_STORE_QUALIFICATION_ADMIN_DSN"], row_factory=dict_row) as conn:
        wake = qualification._create_wake(conn, run_id, "enforce", "email.send", "email_thread", "enforce@example.invalid")
        competing_wake = qualification._create_wake(conn, run_id, "competing", "email.send", "email_thread", "enforce@example.invalid")
        conn.commit()
        request = {
            "op": "execute",
            "wakeup_event_id": wake,
            "action_role": "prospect_reply",
            "operation": "email.send",
            "intent_kind": "inquiry_reply",
            "arguments": {"to_address": "enforce@example.invalid", "text": "isolated enforce qualification"},
        }
        before = len(log.read_text().splitlines()) if log.exists() else 0
        sent = await qualification._execute(url, request)
        assert sent["status"] == "sent", sent
        own = conn.execute("SELECT * FROM outbound_actions WHERE wakeup_event_id=%s", (wake,)).fetchone()
        assert own is not None
        assert own["identity_version"] == "v2-internal"
        assert own["action_id"] != uuid5(ACTION_NAMESPACE, f"v1:wakeup:{wake}:role:{ActionRole.PROSPECT_REPLY}:ordinal:0")
        assert own["state"] == "completed" and own["provider_receipt"]
        assert len(log.read_text().splitlines()) == before + 1

        # Persist another same-recipient internal action through the real SQL API.
        # Leave it received: the traffic probe must still exclude only itself.
        conn.execute(
            """SELECT * FROM create_or_load_outbound_action(
                %s, 'prospect_reply', 'email.send', 'inquiry_reply', NULL,
                %s, %s::jsonb, NULL, %s::jsonb, %s::jsonb, %s, %s, %s::jsonb
            )""",
            (
                competing_wake,
                own["payload_hash"],
                json.dumps(own["canonical_scope"]),
                json.dumps(own["canonical_context"]),
                json.dumps(own["recipient_scope"]),
                own["provider_account"],
                own["routing_policy_version"],
                json.dumps(own["arguments"]),
            ),
        )
        competitor = conn.execute("SELECT action_id, subject_key FROM outbound_actions WHERE wakeup_event_id=%s", (competing_wake,)).fetchone()
        assert competitor is not None
        assert competitor["subject_key"] == own["subject_key"]
        blocked_wake = qualification._create_wake(conn, run_id, "blocked", "email.send", "email_thread", "enforce@example.invalid")
        conn.commit()
        blocked = await qualification._execute(url, {**request, "wakeup_event_id": blocked_wake})
        assert blocked["status"] == "pending" and blocked["detail_code"] == "lease_held", blocked
        assert str(competitor["action_id"]) in blocked["detail"], blocked
        assert UUID(blocked["action_id"]) != competitor["action_id"]
        assert len(log.read_text().splitlines()) == before + 1, "blocked send reached provider"
    print("enforce candidate: 2 passed (v2-internal terminal send; same-recipient competitor blocks)")


if __name__ == "__main__":
    asyncio.run(qualify())
