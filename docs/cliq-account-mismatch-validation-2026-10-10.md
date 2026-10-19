# Cliq account mismatch validation — 2026-10-10

PR: https://github.com/DevNexsler/postgres-mcp/pull/78

Runtime candidate: `d71864d8f7a325ba2d8a74c4b4a00a69f2a8e210`.
Production baseline: `7a1286583e4bc69bdd0d8f02ea4589ee4c68153f`.
Subsequent branch changes only format the regression test and add this report.

## Backtests

- Replayed five recorded Agent Email Server Cliq failures. Three authoritative
  local account refusals now report `cliq_chat_account_mismatch`; two channel
  membership refusals retain their existing classification. Non-acceptance,
  retryability and request references were unchanged in all five cases.
- `scripts/replay_delivery_workflow.py --days 7` replayed 541 production actions
  using SELECTs and recorded observations, without invoking providers. Zero
  unexplained differences. Its 20 expected differences are legacy attempt caps
  versus the existing elapsed-time ceiling, unrelated to this change.
- The new regression exercises queued dispatch through the real Cliq adapter,
  subsequent status without raw provider text, missing request references and
  unrelated permanent refusals.

## Regression and integration

- Gateway suite: 1,621 passed, five skipped. Reran all five skipped TenantCloud
  tests against the current CDS scripts checkout by translating their obsolete
  hardcoded fixture path in memory: five passed. Product and fixture files were
  unchanged by that translation.
- Ruff passed on changed files. New files pass format checks. Pyright passed on
  all changed runtime modules: zero errors or warnings.
- Isolated Cliq qualification passed: one receipt-backed DM despite repeated
  execute, persistent provider 401 -> needs_human, no second provider send,
  database probes and healthy containers with no restarts or OOM.
- The unmodified isolated AES harness cannot qualify the current production
  baseline: it lacks required `CLIQ_BUDGET_DIR`, rejects a known concurrent
  lease-busy response, and expects the obsolete MCP-error form of a closed-wake
  refusal. These were not silently treated as passing.
- With test-only overrides, both baseline and candidate passed the complete
  SMTP/IMAP round trip, single-action/receipt checks, wake reconciliation,
  concurrent callers and closed-wake non-delivery. Overrides set an isolated
  budget directory, retry only the explicitly reported lease-busy loser, and
  require the current structured `rejected` / `wake_terminal` refusal. Provider
  delivery counts were still asserted: one per legitimate wake, zero for the
  closed wake. Production configuration and the repository harness were not
  changed. The existing lease-busy behavior remains outside this fix.

All integration projects used disposable databases and synthetic destinations.
Their runners removed run-owned containers, networks, volumes and image tags.
The prior production image remains tagged for rollback.

The live Hermes Agent Email Server skill was committed locally as `b38cbfe`:
use a channel unique name, do not retry the same account mismatch, and check
successful replacement actions before resending. Its existing Hermes metadata
is unchanged; Codex's skill validator rejects those pre-existing metadata keys.
