"""What an exception means for an outbound action: one classifier for every
gateway path (the tool seam, execute and confirm, Restate's prepare and
resume, the worker).

- REFUSAL: the request, as asked, will be refused the same way every time
  (a gateway refusal, or a database refusal: SQLSTATE 22xxx, 23xxx, 42xxx,
  55000, P0xxx). Retrying cannot help; the row is ended and the agent told.
- TRANSIENT: the database or the network failed this time (serialization,
  deadlock, lock or statement timeout, a lost connection). The same step
  again may well succeed.
- FAULT: anything else -- a gateway bug or an unexpected provider error.
"""

from __future__ import annotations

from enum import Enum

import psycopg

from .models import RequestRefusedError

# A step failed before any send and left its row as it was: a synthesized
# result, not the row's state (the Restate coordinator waits on it).
GATEWAY_INTERNAL_ERROR = "gateway_internal_error"


class FailureKind(Enum):
    REFUSAL = "refusal"
    TRANSIENT = "transient"
    FAULT = "fault"


# serialization_failure, deadlock_detected, lock_not_available,
# query_canceled (statement_timeout).
_TRANSIENT_SQLSTATES = frozenset({"40001", "40P01", "55P03", "57014"})
# connection_exception, insufficient_resources, admin/crash shutdown and
# cannot_connect_now.
_TRANSIENT_SQLSTATE_PREFIXES = ("08", "53", "57P")
_REFUSAL_SQLSTATE_PREFIXES = ("22", "23", "42", "55", "P0")


def classify(error: BaseException) -> FailureKind:
    """The kind of the first link in the cause chain that says what it is.
    A wrapper (the SQL driver turns a query timeout or a failed pool
    connect into a ValueError) is read through to its cause."""
    seen: set[int] = set()
    link: BaseException | None = error
    while link is not None and id(link) not in seen:
        seen.add(id(link))
        if isinstance(link, RequestRefusedError):
            return FailureKind.REFUSAL
        sqlstate = getattr(link, "sqlstate", None)
        if isinstance(sqlstate, str) and sqlstate:
            if sqlstate in _TRANSIENT_SQLSTATES or sqlstate.startswith(_TRANSIENT_SQLSTATE_PREFIXES):
                return FailureKind.TRANSIENT
            if sqlstate.startswith(_REFUSAL_SQLSTATE_PREFIXES):
                return FailureKind.REFUSAL
            return FailureKind.FAULT
        if isinstance(link, (ConnectionError, TimeoutError, psycopg.OperationalError)):
            return FailureKind.TRANSIENT
        link = link.__cause__ or (None if link.__suppress_context__ else link.__context__)
    return FailureKind.FAULT


def error_text(error: BaseException) -> str:
    """An exception's first line, bounded: what the agent reads and the
    refused row records (psycopg appends CONTEXT/DETAIL lines after it). A
    database refusal or a gateway refusal is already in words; anything
    else is named by its type."""
    lines = str(error).strip().splitlines()
    first = " ".join(lines[0].split()) if lines else ""
    if not first:
        return type(error).__name__
    if not isinstance(error, RequestRefusedError) and getattr(error, "sqlstate", None) is None:
        first = f"{type(error).__name__}: {first}"
    return first[:300]
