# Gateway candidate qualification

Run after committing the candidate. The driver builds the exact gateway HEAD with
its production Dockerfile and exports a pinned Comm-Data-Store commit for migrations,
entrypoint, provider stub, and bundled live suite. No operator files are copied.

```bash
CDS_REPO=/path/to/Comm-Data-Store CDS_REVISION=<commit-sha> \
  scripts/qualify-outbound-gateway.sh
```

`COMPOSE_PROJECT_NAME` and `MAINT_DOCKER_RUN_ID` must already identify this run.
The driver preserves those values and tags its image with the run ID. It runs the
bundled CDS live suite, then prepare/resume checks for v2-internal durable IDs
with a populated payload hash (accept matching immutable context; reject altered
hash). It records commit/image/health evidence and removes project resources on exit.

The driver first runs the full pytest suite against its exported candidate, so
the pre-deploy gate always executes the narrow regression. It also runs in the
default `uv run pytest` suite and CI:
`tests/unit/outbound_gateway/test_service.py::test_resume_accepts_database_owned_action_id_when_immutable_context_matches`.
