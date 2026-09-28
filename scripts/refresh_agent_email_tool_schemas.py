#!/usr/bin/env python3
"""Refresh the vendored Agent Email Server tool-schema fixture.

The gateway's Cliq/email/calendar adapters build MCP requests against AES's
tool contracts by hand -- required keys, field names, patterns like
CLIQ_CHAT_ID_PATTERN. AES's own tool-definition source is the only
authoritative copy of that contract (nothing publishes it as a standalone
schema), and it has drifted from the gateway's assumptions before (2026-09-28:
cliq_channel_bot_post rejecting a CT_* channel_or_chat_id the gateway kept
sending it).

This script imports AES's own `createXTools()` functions with `tsx` (no
network access, no provider calls -- it only evaluates the static tool
definitions) and writes their `inputSchema` for the tools the gateway calls
into tests/unit/outbound_gateway/fixtures/agent_email_tool_schemas.json.
test_cliq_contract.py then validates every request the gateway's builders
would send against these vendored schemas, so a real AES schema change shows
up as a failing test here instead of a silent misroute in production.

Usage:
    python3 scripts/refresh_agent_email_tool_schemas.py [--aes-path PATH]

Requires a checkout of Agent-Email-Server with `npm install` already run
(for its `tsx` devDependency). Defaults to the sibling checkout at
~/projects/Agent-Email-Server; override with --aes-path or the
AGENT_EMAIL_SERVER_PATH environment variable.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# The AES tools the gateway actually calls (see the adapters in
# src/postgres_mcp/outbound_gateway/adapters/): cliq.py, email.py,
# calendar.py, plus request_status polled by every adapter.
WANTED_TOOLS = (
    "cliq_channel_bot_post",
    "cliq_chat_post",
    "email_send",
    "calendar_create_event",
    "calendar_update_event",
    "calendar_delete_event",
    "request_status",
)

_DUMP_SCRIPT_TEMPLATE = """
import {{ createCliqBotTools }} from "{aes_path}/src/tools/cliq-bot-tools.ts";
import {{ createEmailTools }} from "{aes_path}/src/tools/emailTools.ts";
import {{ createCalendarTools }} from "{aes_path}/src/tools/calendarTools.ts";
import {{ createQueueTools }} from "{aes_path}/src/tools/queue-tools.ts";

const wanted = new Set({wanted_json});

const all = [
  ...createCliqBotTools(),
  ...createEmailTools(undefined as any, undefined as any),
  ...createCalendarTools(undefined as any),
  ...createQueueTools(),
];

const out: Record<string, unknown> = {{}};
for (const tool of all) {{
  if (wanted.has(tool.name)) {{
    out[tool.name] = tool.inputSchema;
  }}
}}
const missing = [...wanted].filter(name => !(name in out));
if (missing.length > 0) {{
  console.error("missing tools: " + missing.join(", "));
  process.exit(1);
}}
console.log(JSON.stringify(out, null, 2));
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--aes-path",
        default=os.environ.get("AGENT_EMAIL_SERVER_PATH", str(Path.home() / "projects" / "Agent-Email-Server")),
        help="Path to an Agent-Email-Server checkout with node_modules installed",
    )
    args = parser.parse_args()

    aes_path = Path(args.aes_path).resolve()
    if not (aes_path / "src" / "tools" / "cliq-bot-tools.ts").is_file():
        print(f"error: {aes_path} does not look like an Agent-Email-Server checkout", file=sys.stderr)
        return 1
    tsx = aes_path / "node_modules" / ".bin" / "tsx"
    if not tsx.is_file():
        print(f"error: {tsx} not found -- run `npm install` in {aes_path} first", file=sys.stderr)
        return 1

    dump_script = _DUMP_SCRIPT_TEMPLATE.format(aes_path=aes_path.as_posix(), wanted_json=json.dumps(list(WANTED_TOOLS)))

    with tempfile.NamedTemporaryFile("w", suffix=".mts", delete=False) as handle:
        handle.write(dump_script)
        dump_path = Path(handle.name)
    try:
        result = subprocess.run([str(tsx), str(dump_path)], capture_output=True, text=True, cwd=aes_path)
    finally:
        dump_path.unlink(missing_ok=True)

    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        return result.returncode

    schemas = json.loads(result.stdout)
    schemas = {"_provenance": _provenance(), "_runtime_rules": _runtime_rules(), **schemas}

    fixture_path = Path(__file__).resolve().parent.parent / "tests" / "unit" / "outbound_gateway" / "fixtures" / "agent_email_tool_schemas.json"
    fixture_path.write_text(json.dumps(schemas, indent=2) + "\n")
    print(f"wrote {fixture_path}")
    return 0


def _runtime_rules() -> dict[str, object]:
    """AES enforces some Cliq target rules in handler code (or by design,
    documented in comments) rather than in the declared JSON Schema, so
    tools/list never advertises them and a plain jsonschema.validate()
    cannot catch a violation. Vendored here (not extracted -- these are
    small, stable, and reading the enforcing code/comments is the only way
    to get them) so the contract test can check them too. Re-verify against
    the cited source whenever this script runs.
    """
    return {
        "note": (
            "Rules AES enforces in code or by documented design, not in inputSchema. Source: "
            "Agent-Email-Server/src/tools/cliq-bot-tools.ts (parseCliqChannelBotPost/"
            "parseCliqChatPost) and cliq-chat-id.ts (CLIQ_CHAT_ID_PATTERN + its 2026-09-03 "
            "incident comment)."
        ),
        "cliq_channel_bot_post.channel_unique_name": {
            "must_not_match_pattern": "cliq_chat_post.chat_id.pattern",
            "reason": (
                "Explicit for a CT_* value (cliq-bot-tools.ts parseCliqChannelBotPost: "
                "channelUniqueName.startsWith('CT_') throws). A >=15-digit numeric value has no "
                "matching local throw, but cliq-chat-id.ts's own rationale for "
                "MIN_CLIQ_NUMERIC_CHAT_ID_DIGITS establishes that space as chat ids, not channel "
                "unique names, and its 2026-09-03 incident shows Zoho rejects it with an opaque "
                "operation_failed 400 rather than a clear channel-not-found error -- exactly the "
                "kind of failure this contract test exists to catch before it reaches Zoho."
            ),
        },
    }


def _provenance() -> dict[str, str]:
    return {
        "source_repo": "Agent-Email-Server",
        "generated_by": "scripts/refresh_agent_email_tool_schemas.py",
        "note": (
            "Vendored MCP inputSchema for every AES tool the gateway calls. Extracted directly from "
            "AES's own tool-definition source via tsx, not hand-copied. Refresh after any AES schema "
            "change and re-run tests/unit/outbound_gateway/test_cliq_contract.py."
        ),
    }


if __name__ == "__main__":
    raise SystemExit(main())
