"""Deployment identity contract for the production gateway image."""

from __future__ import annotations

from pathlib import Path

REPOSITORY_URL = "https://github.com/DevNexsler/postgres-mcp"
UPSTREAM_REPOSITORY_URL = "https://github.com/crystaldba/postgres-mcp"
DOCKERFILE = Path("Dockerfile")


def _dockerfile() -> str:
    return DOCKERFILE.read_text()


def test_dockerfile_names_the_first_party_gateway_repository() -> None:
    dockerfile = _dockerfile()
    source_line = next(
        line for line in dockerfile.splitlines() if "org.opencontainers.image.source=" in line
    )

    assert source_line == f'LABEL org.opencontainers.image.source="{REPOSITORY_URL}"'
    assert UPSTREAM_REPOSITORY_URL not in source_line


def test_dockerfile_records_upstream_attribution_separately() -> None:
    dockerfile = _dockerfile()

    assert (
        f'io.postgres-mcp.upstream-source="{UPSTREAM_REPOSITORY_URL}"' in dockerfile
    ), "upstream attribution must not replace first-party build-source identity"


def test_dockerfile_declares_source_revision_label_contract() -> None:
    dockerfile = _dockerfile()

    assert "ARG SOURCE_REVISION" in dockerfile
    assert 'LABEL org.opencontainers.image.revision="$SOURCE_REVISION"' in dockerfile
