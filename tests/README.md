# PostgreSQL MCP Tests

This directory contains tests for the PostgreSQL MCP package.

## Running Tests

To run all tests:

```bash
uv run pytest
```

To run a specific test file:

```bash
uv run pytest tests/unit/test_obfuscate_password.py
```

To run a specific test:

```bash
uv run pytest tests/unit/test_db_conn_pool.py::test_pool_connect_success
```

## Landing Gate

GitHub Actions does not run on this fork, so `main` is gated locally instead.
A push that advances `main` is refused unless the full suite passed on the
tree being pushed:

```bash
./scripts/run_test_suite.sh      # `uv run pytest`; a green run attests the committed tree
git push origin main
```

Land PRs by merging locally and pushing, never with `gh pr merge` or the GitHub
merge button: a server-side merge runs no hook. Install the hook once per clone
with `python3 scripts/merge_gate.py install`. A run over a dirty tree, or with
any pytest argument, records nothing.

## Test Structure

- **Unit Tests** (`tests/unit/`): Tests for individual components and functions
  - `test_obfuscate_password.py`: Tests for password obfuscation functionality
  - `test_db_conn_pool.py`: Tests for database connection pool
  - `test_sql_driver.py`: Tests for SQL driver and transaction handling
