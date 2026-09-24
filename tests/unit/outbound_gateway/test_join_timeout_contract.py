"""#3240: an MCP initialize timeout precedes any provider tool request."""

from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx
import pytest

from postgres_mcp.outbound_gateway.provider_client import McpProviderClient
from postgres_mcp.outbound_gateway.provider_client import McpServerConfig
from postgres_mcp.outbound_gateway.provider_client import TransportErrorKind


@pytest.mark.asyncio
async def test_httpx_initialize_timeout_is_safe_to_retry_before_tools_call():
    calls = []

    @asynccontextmanager
    async def transport(_url, **_kwargs):
        yield object(), object(), lambda: None

    class Session:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def initialize(self):
            calls.append("initialize")
            raise httpx.ReadTimeout("mcp-gate initialize timed out")

        async def call_tool(self, *_args, **_kwargs):
            calls.append("tools/call")
            pytest.fail("provider tool must not run after initialize timeout")

    config = McpServerConfig(
        name="agent-email",
        url="http://127.0.0.1:9090/mcp",
        transport="streamable_http",
        allowed_tools=frozenset({"email_send"}),
        timeout_seconds=1.0,
    )
    with (
        patch("postgres_mcp.outbound_gateway.provider_client.streamablehttp_client", transport),
        patch("postgres_mcp.outbound_gateway.provider_client.ClientSession", Session),
    ):
        result = await McpProviderClient({config.name: config}).call(
            "agent-email", "email_send", {"to": [{"address": "canary@example.invalid"}]}
        )

    assert result.error_kind is TransportErrorKind.TRANSPORT
    assert result.before_tool_request is True
    assert calls == ["initialize"]
