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
bundled suite and internal identity/competing-recipient checks with traffic control
**enforced**, rejects fail-open probe logs, and prints commit/image/health evidence.
The Compose network is internal, PostgreSQL data is tmpfs, and providers are stubs;
no host ports or production credentials are used. Exit cleanup removes and checks
project containers, networks, volumes, the candidate image, and exported source.

The narrow regression runs in the default `uv run pytest` suite and CI:
`tests/unit/outbound_gateway/test_service.py::test_traffic_gate_excludes_durable_id_when_context_identity_differs`.
