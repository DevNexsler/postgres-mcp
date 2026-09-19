from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from postgres_mcp.outbound_gateway import admin
from postgres_mcp.outbound_gateway.admin import build_parser


def test_admin_requires_operator_and_authoritative_evidence():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["resolve", "00000000-0000-0000-0000-000000000001"])
    args = parser.parse_args(
        [
            "resolve",
            "00000000-0000-0000-0000-000000000001",
            "--operator",
            "danpark",
            "--evidence-kind",
            "authoritative_acceptance",
            "--evidence-reference",
            "provider-message-1",
            "--evidence-hash",
            "a" * 64,
            "--resolution",
            "completed",
            "--reason",
            "verified in provider history",
        ]
    )
    assert args.operator == "danpark"
    assert args.resolution == "completed"


def test_admin_remediation_accepts_no_recipient_or_provider_override():
    parser = build_parser()
    args = parser.parse_args(
        [
            "remediate",
            "00000000-0000-0000-0000-000000000001",
            "--operator",
            "danpark",
            "--reason",
            "provider proved first action failed",
        ]
    )
    assert args.command == "remediate"
    assert not hasattr(args, "recipient")
    assert not hasattr(args, "provider")


def test_admin_resolution_query_survives_literal_empty_json_object(monkeypatch, capsys):
    class Row:
        cells = {"action_id": "00000000-0000-0000-0000-000000000001"}

    class Driver:
        async def execute_query(self, query, *args, **kwargs):
            assert "'{}'::jsonb" in query
            return [Row()]

    args = build_parser().parse_args(
        [
            "resolve",
            "00000000-0000-0000-0000-000000000001",
            "--operator",
            "danpark",
            "--evidence-kind",
            "authoritative_acceptance",
            "--evidence-reference",
            "provider-message-1",
            "--evidence-hash",
            "a" * 64,
            "--resolution",
            "completed",
            "--reason",
            "verified in provider history",
        ]
    )

    monkeypatch.setenv("DATABASE_URI", "postgresql://unused/test")
    monkeypatch.setattr(admin, "build_parser", lambda: Mock(parse_args=lambda: args))
    pool = AsyncMock()
    monkeypatch.setattr(admin, "DbConnPool", lambda uri: pool)
    monkeypatch.setattr(admin, "SqlDriver", lambda **kwargs: Driver())
    admin.main()
    assert capsys.readouterr().out.strip() == "00000000-0000-0000-0000-000000000001"
    pool.close.assert_awaited_once()
