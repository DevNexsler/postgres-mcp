"""Authoritative POST non-dispatch evidence, scoped to the effect invocation."""
import json

import pytest

from postgres_mcp.outbound_gateway.adapters.base import ProviderDisposition
from postgres_mcp.outbound_gateway.adapters.quo import QuoSmsAdapter
from postgres_mcp.outbound_gateway.provider_client import McpCallResult


@pytest.mark.parametrize("as_text", [False, True])
def test_quo_pre_request_evidence_allows_recovery(as_text):
    payload = {"error": "[Errno -3] Temporary failure in name resolution", "before_provider_request": True}
    result = McpCallResult(text=json.dumps(payload)) if as_text else McpCallResult(structured_content=payload)
    observed = QuoSmsAdapter._parse(result, effect_call=True)
    assert observed.disposition is ProviderDisposition.DEFINITIVE_NON_ACCEPTANCE
    assert observed.retryable
    assert observed.category == "provider_unavailable"
    assert observed.evidence == {"kind": "failed_before_provider_request"}


@pytest.mark.parametrize("payload", [
    {"error": "[Errno -3] Temporary failure in name resolution"},
    {"error": "read timeout"},
    {"error": "failed", "before_provider_request": False},
    {"error": "failed", "before_provider_request": "true"},
    {"before_provider_request": True},
    {"content": {"error": "failed", "before_provider_request": True}},
])
def test_quo_unknown_errors_cannot_authorize_retry(payload):
    observed = QuoSmsAdapter._parse(McpCallResult(structured_content=payload), effect_call=True)
    assert observed.disposition is ProviderDisposition.AMBIGUOUS
    assert not observed.retryable


def test_quo_history_failure_cannot_prove_send_non_acceptance():
    observed = QuoSmsAdapter._parse(McpCallResult(structured_content={
        "error": "connection failed", "before_provider_request": True,
    }), effect_call=False)
    assert observed.disposition is ProviderDisposition.AMBIGUOUS
    assert not observed.retryable
