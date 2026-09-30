#!/usr/bin/env bash
# Full test suite runner that feeds the landing gate (#3965).
#
# Runs `uv run pytest` (the suite `.github/workflows/build.yml` would run) and,
# on a green full-scope run over a clean tree, attests that tree for `pre-push`
# on `main`. Any pytest argument marks a focused run and records nothing: a
# path, `-k`, `-m`, `--lf` or `--deselect` can each leave out the tests that
# went red, which is how dd0dfa8 landed 48 red integration tests.
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"

scope=full
if [[ "$#" -gt 0 ]]; then
  scope=focused
fi

set +e
uv run pytest "$@"
status=$?
set -e

if [[ "${status}" -eq 0 && "${scope}" == "full" ]]; then
  "${POSTGRES_MCP_MERGE_GATE_PYTHON:-python3}" "${root}/scripts/merge_gate.py" record --tier pytest --scope full
fi
exit "${status}"
