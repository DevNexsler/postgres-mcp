"""The one classifier every gateway path reads an exception through."""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from postgres_mcp.outbound_gateway.context import ContextDerivationError
from postgres_mcp.outbound_gateway.errors import FailureKind
from postgres_mcp.outbound_gateway.errors import classify
from postgres_mcp.outbound_gateway.errors import error_text
from postgres_mcp.outbound_gateway.models import RequestRefusedError

from .test_stale_context_confirm import SqlError


def _wrapped(cause: BaseException) -> ValueError:
    """The SQL driver's own wrapping: SafeSqlDriver turns a query timeout,
    and the pool a failed connect, into a ValueError raised from the cause."""
    try:
        raise ValueError("Query execution timed out after 30 seconds in restricted mode.") from cause
    except ValueError as error:
        return error


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (RequestRefusedError("execute refused: ..."), FailureKind.REFUSAL),
        (ContextDerivationError("verified target could not be derived"), FailureKind.REFUSAL),
        (SqlError("ambiguous aliases resolve to multiple outbound-action subjects", "22023"), FailureKind.REFUSAL),
        (SqlError("action x was already sent", "55000"), FailureKind.REFUSAL),
        (SqlError("an answer must come from the action's own wake 7", "42501"), FailureKind.REFUSAL),
        (SqlError("duplicate key value violates unique constraint", "23505"), FailureKind.REFUSAL),
        (SqlError("raise_exception", "P0001"), FailureKind.REFUSAL),
        (SqlError("could not serialize access", "40001"), FailureKind.TRANSIENT),
        (SqlError("deadlock detected", "40P01"), FailureKind.TRANSIENT),
        (SqlError("lease unavailable", "55P03"), FailureKind.TRANSIENT),
        (SqlError("canceling statement due to statement timeout", "57014"), FailureKind.TRANSIENT),
        (SqlError("terminating connection due to administrator command", "57P01"), FailureKind.TRANSIENT),
        (SqlError("server closed the connection unexpectedly", "08006"), FailureKind.TRANSIENT),
        (psycopg.OperationalError("server closed the connection unexpectedly"), FailureKind.TRANSIENT),
        (ConnectionResetError("connection reset by peer"), FailureKind.TRANSIENT),
        (_wrapped(asyncio.TimeoutError()), FailureKind.TRANSIENT),
        (SqlError("internal error", "XX000"), FailureKind.FAULT),
        (TypeError("load() got an unexpected keyword argument 'as_of'"), FailureKind.FAULT),
        (ValueError("no outbound provider adapter configured for x"), FailureKind.FAULT),
    ],
    ids=lambda value: type(value).__name__ if isinstance(value, BaseException) else value.value,
)
def test_every_failure_has_one_kind(error, kind):
    assert classify(error) is kind


def test_an_error_raised_while_handling_another_is_read_through_its_cause():
    try:
        try:
            raise SqlError("deadlock detected", "40P01")
        except SqlError as exc:
            raise RuntimeError("stale_context: the question could not be recorded") from exc
    except RuntimeError as error:
        assert classify(error) is FailureKind.TRANSIENT


def test_error_text_is_the_first_line_and_names_a_bare_exception():
    psycopg_like = SqlError("ambiguous aliases\nCONTEXT:  PL/pgSQL function acquire_outbound_intent_lock line 52", "22023")

    assert error_text(psycopg_like) == "ambiguous aliases"
    assert error_text(TypeError("boom")) == "TypeError: boom"
    assert error_text(RuntimeError()) == "RuntimeError"
