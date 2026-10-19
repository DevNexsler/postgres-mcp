# pyright: reportArgumentType=false, reportAttributeAccessIssue=false
"""Differential harness: the frozen bf41be6 send judgment vs the current one.

Each scenario runs twice -- once through LegacyJudgmentService (the gateway's
service, stale-context question, preflight and traffic control exactly as
deployed at bf41be6, legacy_judgment/) and once through OutboundActionService
-- with every collaborator instrumented. A run's trace is the ordered list of
everything externally visible:

- every public service call and its PublicResult (status, action_id,
  detail_code, detail, ...) or raised exception;
- every store call with its arguments and returned row;
- every adapter, context-loader, evidence-loader, probe and circuit call with
  its arguments and result;
- every log record the gateway emits.

UUIDs are replaced by their order of first appearance.

The simplification changes behaviour on purpose, so equality is judged in
steps (see `compare`): identical traces; the same observable outcome (the
store writes, provider calls and public results -- reads and logs differ
because the detection is now one query); the same outcome up to the
question's wording; and otherwise the FIRST observable divergence must be one
of the DECLARED differences, each a named predicate. Anything else fails.
"""

from __future__ import annotations

import dataclasses
import inspect
import itertools
import logging
import os
import re
from collections.abc import Callable
from collections.abc import Mapping
from datetime import date
from datetime import datetime
from enum import Enum
from types import ModuleType
from typing import Any
from unittest.mock import NonCallableMock
from uuid import UUID

from pydantic import BaseModel

from postgres_mcp.outbound_gateway.legacy_judgment import LegacyJudgmentService
from postgres_mcp.outbound_gateway.service import OutboundActionService

_UUID_TEXT = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

STORE_METHODS = (
    "create_or_load",
    "prepare",
    "claim",
    "record_provider_request",
    "transition",
    "complete",
    "definitive_fail",
    "remediate_traffic_block",
    "block_stale_context",
    "confirm_stale_context",
    "reject",
    "override",
    "successor",
    "get",
    "schedule_next_attempt",
)
ADAPTER_METHODS = ("build_request", "invoke", "poll", "parse_receipt", "reconcile")
LOADER_METHODS = ("load", "suggest_targets")
PROBE_METHODS = (
    "newer_context",
    "activity_after",
    "context_watermark",
    "acknowledged_refs",
    "messages_by_id",
)
PUBLIC_METHODS = (
    "execute",
    "enqueue",
    "confirm",
    "prepare",
    "resume",
    "reconcile",
    "exhaust",
    "status",
    "action_operation",
    "suggest_targets",
)


class Trace:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self.kinds: list[tuple[Any, ...]] = []  # (event kind, label) un-normalized, for sanity checks
        self._ids: dict[UUID, str] = {}

    def add(self, *event: Any) -> None:
        self.kinds.append(event[:2])
        self.events.append(self.normalize(event))

    def _token(self, value: UUID) -> str:
        return self._ids.setdefault(value, f"uuid#{len(self._ids)}")

    def normalize(self, value: Any) -> Any:  # noqa: PLR0911 -- one branch per shape
        if value is None or isinstance(value, bool | int | float):
            return value
        if isinstance(value, str):
            return _UUID_TEXT.sub(lambda match: self._token(UUID(match.group(0))), value)
        if isinstance(value, UUID):
            return self._token(value)
        if isinstance(value, Enum):
            return (type(value).__name__, value.value)
        if isinstance(value, datetime | date):
            return value.isoformat()
        if isinstance(value, BaseException):
            return ("raised", type(value).__name__, self.normalize(str(value)))
        if isinstance(value, BaseModel):
            return (type(value).__name__, self.normalize(value.model_dump(mode="python")))
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return (
                type(value).__name__,
                tuple((field.name, self.normalize(getattr(value, field.name))) for field in dataclasses.fields(value)),
            )
        if isinstance(value, Mapping):
            return (type(value).__name__, tuple((self.normalize(k), self.normalize(v)) for k, v in value.items()))
        if isinstance(value, list | tuple):
            return (type(value).__name__, tuple(self.normalize(item) for item in value))
        if isinstance(value, set | frozenset):
            return (type(value).__name__, tuple(sorted((self.normalize(item) for item in value), key=repr)))
        if isinstance(value, NonCallableMock):
            return ("mock",)
        return ("object", type(value).__name__)


class _Recorded:
    """A recorded stand-in for one collaborator method. Attribute reads and
    writes pass through, so a test that reconfigures an AsyncMock
    (`loader.load.return_value = ...`) after construction still works."""

    def __init__(self, target: Callable[..., Any], label: str, trace: Trace) -> None:
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_label", label)
        object.__setattr__(self, "_trace", trace)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._target, name, value)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        trace, label = self._trace, self._label
        trace.add("call", label, args, tuple(sorted(kwargs.items())))
        try:
            result = self._target(*args, **kwargs)
        except BaseException as exc:
            trace.add("raise", label, exc)
            raise
        if not inspect.isawaitable(result):
            trace.add("return", label, result)
            return result

        async def _awaited() -> Any:
            try:
                value = await result
            except BaseException as exc:
                trace.add("raise", label, exc)
                raise
            trace.add("return", label, value)
            return value

        return _awaited()


def _instrument(obj: Any, label: str, names: tuple[str, ...], trace: Trace) -> None:
    if obj is None:
        return
    for name in names:
        current = getattr(obj, name, None)
        if isinstance(current, _Recorded):
            object.__setattr__(current, "_trace", trace)  # a fake shared across runs: record into this run
            continue
        if current is None or not callable(current):
            continue
        try:
            setattr(obj, name, _Recorded(current, f"{label}.{name}", trace))
        except (AttributeError, TypeError) as exc:  # pragma: no cover -- a fake that cannot be instrumented
            raise AssertionError(f"cannot instrument {label}.{name}: {exc}") from exc


def recording(service_cls: type, trace: Trace) -> type:
    """A subclass of `service_cls` that instruments its collaborators and
    records its public calls into `trace`."""

    class Recording(service_cls):  # type: ignore[misc,valid-type]
        def __init__(self, **kwargs: Any) -> None:
            _instrument(kwargs.get("store"), "store", STORE_METHODS, trace)
            _instrument(kwargs.get("context_loader"), "loader", LOADER_METHODS, trace)
            _instrument(kwargs.get("evidence_loader"), "evidence", ("load",), trace)
            _instrument(kwargs.get("traffic_probe"), "probe", PROBE_METHODS, trace)
            _instrument(kwargs.get("circuit_guard"), "circuit", ("circuit_status",), trace)
            for operation, adapter in sorted((kwargs.get("adapters") or {}).items(), key=lambda item: item[0].value):
                del operation
                _instrument(adapter, "adapter", ADAPTER_METHODS, trace)
            super().__init__(**kwargs)

    for name in PUBLIC_METHODS:
        original = getattr(service_cls, name)

        def _make(method_name: str, method: Callable[..., Any]) -> Callable[..., Any]:
            async def _public(self: Any, *args: Any, **kwargs: Any) -> Any:
                trace.add("service", method_name, args, tuple(sorted(kwargs.items())))
                try:
                    result = await method(self, *args, **kwargs)
                except BaseException as exc:
                    trace.add("service_raise", method_name, exc)
                    raise
                trace.add("service_return", method_name, result)
                return result

            return _public

        setattr(Recording, name, _make(name, original))
    Recording.__name__ = f"Recording{service_cls.__name__}"
    return Recording


class _LogTap(logging.Handler):
    def __init__(self, trace: Trace) -> None:
        super().__init__(level=logging.DEBUG)
        self.trace = trace

    def emit(self, record: logging.LogRecord) -> None:
        exc_type = record.exc_info[0].__name__ if record.exc_info and record.exc_info[0] else None
        self.trace.add("log", record.name, record.levelname, record.getMessage(), exc_type)


class tapped_logs:  # noqa: N801 -- used as a context manager
    """Record every gateway log record that is emitted at the configured
    level (the gateway logs warnings and errors)."""

    def __init__(self, trace: Trace, *, isolate: bool = False) -> None:
        self._tap = _LogTap(trace)
        self._logger = logging.getLogger("postgres_mcp.outbound_gateway")
        self._isolate = isolate
        self._propagate = self._logger.propagate

    def __enter__(self) -> None:
        self._logger.addHandler(self._tap)
        if self._isolate:
            # Generated scenarios: keep thousands of expected error tracebacks
            # out of the console handler. Replayed tests keep propagation
            # (their caplog assertions read the root logger).
            self._logger.propagate = False

    def __exit__(self, *exc: object) -> None:
        self._logger.removeHandler(self._tap)
        self._logger.propagate = self._propagate


SIDES: tuple[type, type] = (LegacyJudgmentService, OutboundActionService)


async def run_side(service_cls: type, scenario: Callable[[type], Any]) -> Trace:
    """Run one scenario against one side. `scenario(ServiceClass)` builds its
    fakes, constructs the service with keyword arguments and drives it."""
    trace = Trace()
    with tapped_logs(trace, isolate=True):
        try:
            outcome = scenario(recording(service_cls, trace))
            if inspect.isawaitable(outcome):
                await outcome
        except Exception as exc:  # the scenario's own raise is part of the outcome
            trace.add("scenario_raised", exc)
        else:
            trace.add("scenario_completed")
    return trace


# ----------------------------------------------------------------------------
# Comparing a changed judgment: observable outcome, wording, declared changes
# ----------------------------------------------------------------------------

IDENTICAL = "identical"
# Only reads and log lines differ: the newer-context detection is one query
# (probe.newer_context) instead of four reads plus the evidence arms.
SAME_OUTCOME = "same_outcome_one_query"
# Only the question's words differ: one detail text for received and sent by
# us, direction "received" / "sent by us", the plain sender name.
WORDING = "question_wording"

_TOKEN = re.compile(r"uuid#\d+")


def _retokenize(events: list[Any]) -> list[Any]:
    """Renumber UUID tokens by first appearance in `events` (a projection
    drops the reads that first mentioned some of them)."""
    seen: dict[str, str] = {}

    def walk(value: Any) -> Any:
        if isinstance(value, str):
            return _TOKEN.sub(lambda match: seen.setdefault(match.group(0), f"id#{len(seen)}"), value)
        if isinstance(value, tuple):
            return tuple(walk(item) for item in value)
        return value

    return [walk(event) for event in events]


def observable(events: list[Any]) -> list[Any]:
    """What the world sees: public calls and their results or raises, every
    store write and every provider (adapter) call. Not reads, not logs."""
    kept = []
    for event in events:
        event = event[1]  # a normalized event is ("tuple", (kind, label, ...))
        kind = event[0]
        if kind in {"service", "service_return", "service_raise", "scenario_raised", "scenario_completed"}:
            kept.append(event)
        elif kind in {"call", "raise"} and ((event[1].startswith("store.") and event[1] != "store.get") or event[1].startswith("adapter.")):
            kept.append(event)
    return _retokenize(kept)


def _mask_wording(value: Any) -> Any:
    """Blank the question's words: a needs_confirmation result's detail,
    question and each item's direction/sender, and the confirmation-disabled
    refusal's text."""
    if isinstance(value, str):
        if "stale_context confirmation is not enabled on this gateway" in value:
            return "<confirmation disabled refusal>"
        return value
    if not isinstance(value, tuple):
        return value
    if len(value) == 2 and value[0] == "dict" and isinstance(value[1], tuple):
        pairs = value[1]
        keys = {pair[0] for pair in pairs if isinstance(pair, tuple) and len(pair) == 2}
        status = dict(pair for pair in pairs if isinstance(pair, tuple) and len(pair) == 2).get("status")
        if "status" in keys and status == "needs_confirmation":
            return (
                "dict",
                tuple((key, "<words>") if key in {"detail", "question"} else (key, _mask_wording(item)) for key, item in pairs),
            )
        if {"id", "direction", "sender", "preview"} <= keys:
            return ("dict", tuple((key, "<label>") if key in {"direction", "sender"} else (key, item) for key, item in pairs))
    return tuple(_mask_wording(item) for item in value)


def _call(event: Any, label: str) -> bool:
    return event is not None and event[0] == "call" and event[1] == label


_SEND_PATH = frozenset(
    {
        "store.prepare",
        "store.claim",
        "store.transition",
        "store.schedule_next_attempt",
        "adapter.build_request",
        "adapter.invoke",
    }
)


def _refs(event: Any) -> set[str]:
    return set(re.findall(r"(?:message|action):[\w#-]+", repr(event)))


def _result(event: Any) -> dict[str, Any] | None:
    if event is None or event[0] != "service_return" or not isinstance(event[2], tuple) or event[2][0] != "PublicResult":
        return None
    return dict(event[2][1][1])


def _this_call(events: tuple[Any, ...]) -> tuple[Any, ...]:
    """The events up to and including the end of the public call they are in."""
    for index, event in enumerate(events):
        if event[0] in {"service_return", "service_raise"}:
            return events[: index + 1]
    return events


def _declared(  # noqa: PLR0911, PLR0912 -- one branch per declared difference
    legacy_rest: tuple[Any, ...],
    current_rest: tuple[Any, ...],
    *,
    prefix: tuple[Any, ...],
    current_logs: str,
    current_queried: bool = True,
) -> str | None:
    """Name the declared difference that explains the FIRST observable
    divergence; `*_rest` start at it. Everything after it follows from it."""
    legacy = legacy_rest[0] if legacy_rest else None
    current = current_rest[0] if current_rest else None
    old, new = repr(_this_call(legacy_rest)), repr(_this_call(current_rest))
    legacy_result, current_result = _result(legacy), _result(current)
    # An unexpected error before the provider was called: the legacy
    # swallowed it and returned the row as it stood (`pending`, often still
    # `received`, which the worker never lists -- a silent no-send). Now the
    # result says "not sent" (and a row already marked dispatching goes back
    # to retry_ready, where a re-execute dispatches it).
    if "gateway_internal_error" in new and "gateway_internal_error" not in old:
        return "pre_send_error_is_reported_not_sent"
    # The same error on a row nothing had started on (received,
    # dependency_wait) now ends it rejected, with the error's words and the
    # override that still sends it: a `received` row is one nothing picks up
    # again.
    # exhaust() on a row still `received` -- Restate could not prepare it
    # within the retry ceiling -- ends it rejected (nothing was sent); the
    # legacy left it `received`, where nothing picks it up again.
    if _call(current, "store.reject") and "prepare_retry_exhausted" in new:
        return "exhaust_ends_an_unprepared_row_rejected"
    rejected_codes = {"gateway_error", "gateway_refused", "gateway_transient_error"}
    if (_call(current, "store.reject") or (current_result or {}).get("detail_code") in rejected_codes) and "gateway_error" not in old:
        return "pre_send_error_ends_the_action_rejected"
    # op confirm on an action holding no stale_context question is now the
    # agent's answer to a refusal (yes sends what was not sent, with a
    # reason; no is recorded on it), refused with its own words where there
    # is nothing to answer. The legacy refused every such confirm ("not
    # awaiting", or "confirmation is not enabled" when the question was
    # switched off).
    if (
        legacy is not None
        and legacy[:2] == ("service_raise", "confirm")
        and ("is not awaiting a stale_context confirmation" in old or "<confirmation disabled refusal>" in old)
        and current is not None
        and (current[1] == "confirm" or _call(current, "store.override") or _call(current, "store.reject"))
    ):
        return "confirm_without_a_question_is_the_override"
    # A write of the agent's answer that fails for a reason that is not a
    # refusal (a lost connection, a fault) is raised as what it is -- the
    # tool answers it failed / gateway_error, "the same call again" -- not
    # dressed as a refusal of the answer the agent must not retry.
    if (
        legacy is not None
        and current is not None
        and legacy[:2] == current[:2] == ("service_raise", "confirm")
        and "confirm refused for action" in old
        and "confirm refused" not in new
    ):
        return "a_failed_answer_write_is_not_a_refusal"
    # A yes that comes after the wake ended sends as an agent_override
    # (Comm-Data-Store 251's one answer body: the ledger's wake_terminal),
    # which is not asked the stale question again; the legacy asked it of
    # the successor. A yes during the wake is asked again on both sides.
    answered = next((event for event in reversed(prefix) if event[0] == "service"), None)
    if (
        _call(legacy, "store.block_stale_context")
        and _call(current, "store.prepare")
        and answered is not None
        and answered[1] == "confirm"
        and "('decision', 'yes')" in repr(answered)
    ):
        return "late_yes_is_an_override_not_asked_again"
    # An action the agent already answered yes or revise (an override, or a
    # stale_context answer) reports the successor carrying its send --
    # status, and the identical request again -- looked up by its parent.
    # The legacy re-answered the question, or reported the parent "not sent".
    if _call(current, "store.successor"):
        return "answered_action_reports_its_successor"
    # FIX 3: a TenantCloud auth rejection proven pre-dispatch
    # (tenantcloud_auth_rejected_before_dispatch / category=provider_authentication)
    # now waits out the outage -- schedule_next_attempt with no claim() --
    # instead of the legacy's ordinary claim()-then-dispatch on every due
    # retry_ready row.
    if _call(current, "store.schedule_next_attempt") and "tenantcloud_auth_wait" in new:
        return "tenantcloud_auth_wait_skips_the_retry_budget"
    # FIX 4 (wake 27321): a dispatch-time poll still PENDING after the first
    # immediate recheck now gets re-polled on a short window (up to the
    # response budget) instead of becoming provider_queue_timeout on the
    # spot -- the gateway's own timestamps showed the timeout firing ~0.12s
    # after dispatch while the provider's write landed ~0.42s later. The
    # extra adapter.poll call is the first observable sign; the legacy's own
    # remaining trace for this call still shows the immediate timeout.
    if _call(current, "adapter.poll") and "provider_queue_timeout" in old:
        return "dispatch_pending_gets_a_poll_window"
    # The per-recipient in-flight hold is gone (action b78d5668, 2026-09-30:
    # one uncertain SMS held that person's calendar update and staff Cliq
    # post for up to an hour). Where the legacy returned lease_held, the
    # action now carries on to its own claim/dispatch.
    if legacy_result is not None and legacy_result.get("detail_code") == "lease_held":
        return "in_flight_hold_removed"
    # The override resend of a legacy traffic_blocked row needed the terminal
    # block that no longer exists.
    if _call(legacy, "store.remediate_traffic_block"):
        return "traffic_block_override_resend_removed"
    # recipient/context mismatch and the calendar STALE/DUPLICATE verdicts:
    # unreachable in production (test_preflight proves it); only a
    # hand-built evidence object reaches them.
    if any(code in old for code in ("recipient_mismatch", "context_mismatch", "calendar_context_changed", "has no provider receipt")):
        return "unreachable_preflight_verdict_removed"
    # A question that cannot be recorded: nothing is sent and the call
    # raises (was: the terminal traffic_blocked failure).
    if current is not None and current[0] == "service_raise" and "could not be recorded" in new:
        return "unrecordable_question_raises_nothing_sent"
    # No answer means no send: where nobody can be asked (worker, Restate
    # prepare, confirmation off, retry_ready) unshown newer context ends the
    # action as a `stale_context_unasked` no-send (retry_ready included).
    # The legacy terminal-failed, preflight-staled, deferred -- or sent.
    if "stale_context_unasked" in new and "stale_context_unasked" not in old:
        return "unasked_newer_context_is_a_stale_no_send"
    asked = "store.block_stale_context" in new or (current_result is not None and current_result.get("status") == "needs_confirmation")
    legacy_stale_verdict = (
        "newer_inbound" in old
        or ("store.definitive_fail" in old and "traffic_blocked" in old)
        or (legacy_result is not None and legacy_result.get("detail_code") == "stale_context" and legacy_result.get("status") != "needs_confirmation")
    )
    # One switch: in OUTBOUND_TRAFFIC_CONTROL=shadow the question is only
    # logged. The legacy's preflight newer_inbound and newer-outbound
    # question ignored the mode.
    if not asked and "stale-context shadow would-" in current_logs and (legacy_stale_verdict or "store.block_stale_context" in old):
        return "shadow_mode_only_logs"
    # A stale verdict of the legacy (preflight newer_inbound `stale`, the
    # terminal traffic_blocked failure, the stale_context deferral): now the
    # question -- or, where nobody can be asked, the normal gate goes on.
    if legacy_stale_verdict and "store.block_stale_context" not in old:
        if asked:
            return "newer_inbound_is_the_question"
        # OUTBOUND_TRAFFIC_CONTROL=off (or no probe) now switches off every
        # stale check; the legacy preflight's newer_inbound ignored it.
        if not current_queried:
            return "traffic_control_off_means_no_stale_check"
        # The one query waives what this wake's agent was already shown, with
        # confirmation on or off; the legacy waived nothing while it was off.
        return "already_shown_items_are_not_new"
    # The legacy newer-outbound question ignored override=true (the
    # historical "yes"); the one question takes it as yes for every item.
    last_call = next((event for event in reversed(prefix) if event[0] == "service"), None)
    if (
        legacy_result is not None
        and legacy_result.get("status") == "needs_confirmation"
        and _call(current, "store.confirm_stale_context")
        and "('decision', 'yes')" in repr(current)
        and "('override', True)" in repr(last_call)
    ):
        return "override_is_yes_for_every_item"
    # One query lists received AND sent-by-us items from every arm at once:
    # the question is a superset of what the legacy asked.
    if _call(legacy, "store.block_stale_context") and _call(current, "store.block_stale_context"):
        if _refs(legacy) <= _refs(current):
            return "one_question_lists_every_newer_item"
    if legacy_result is not None and current_result is not None:
        if legacy_result.get("status") == current_result.get("status") == "needs_confirmation" and _refs(legacy_result.get("new_context")) <= _refs(
            current_result.get("new_context")
        ):
            return "one_question_lists_every_newer_item"
    # The legacy never looked where the one check now asks: its newer_inbound
    # check covered prospect replies only, and a re-executed prepared (or
    # waiting) row went to dispatch without any preflight look.
    if asked and "store.block_stale_context" not in old:
        return "asked_where_legacy_never_looked"
    # An internal_notification has no recipient conversation to go stale
    # against -- it posts to a staff review channel about something that
    # happened, not a reply to whoever is still texting the wake's own
    # channel. It now always sends, where the legacy's channel-scoped
    # newer_context check raised the same question it raises for a
    # prospect/internal reply (wakes 27313/27314, 2026-09-28: a
    # manual_review_alert reporting a gateway bug was itself blocked
    # stale_context, then its confirmed successor exhausted a retry budget
    # without ever landing).
    if _call(legacy, "store.block_stale_context") and not asked and "('action_role', 'internal_notification')" in new:
        return "internal_notification_never_asks_stale_context"
    return None


def _legacy_refusal_type(value: Any) -> Any:
    """A refused stale_context answer is raised as RequestRefusedError, with
    the legacy's exact words: the type server.handle_outbound_action answers
    as a rejected result instead of an MCP tool error (#3463). The legacy
    raised a plain ValueError. Read it as the legacy's type, so every event
    after it is still compared."""
    if isinstance(value, list | tuple):
        if len(value) == 3 and value[0] == "raised" and value[1] == "RequestRefusedError":
            return ("raised", "ValueError", value[2])
        return type(value)(_legacy_refusal_type(item) for item in value)
    return value


def compare(legacy: Trace, current: Trace) -> tuple[str, str]:
    """(name, where): IDENTICAL, SAME_OUTCOME, WORDING, a declared difference,
    or raises AssertionError naming the first unexplained divergence."""
    if legacy.events == current.events:
        return IDENTICAL, ""
    current_events = _legacy_refusal_type(current.events)
    if legacy.events == current_events:
        return "refusal_is_a_request_refused_error", ""
    plain_old, plain_new = observable(legacy.events), observable(current_events)
    if plain_old == plain_new:
        return SAME_OUTCOME, ""
    old, new = _mask_wording(tuple(plain_old)), _mask_wording(tuple(plain_new))
    if old == new:
        return WORDING, ""
    index = next(i for i, (before, after) in enumerate(itertools.zip_longest(old, new)) if before != after)
    if index == len(old) and plain_old[:index] != plain_new[:index]:
        # The legacy stopped (a replayed test's assertion on the words failed)
        # after an identical course that differed only in wording.
        return WORDING, ""
    logs = " | ".join(str(event[1][3]) for event in current_events if event[1][0] == "log")
    queried = any(event[1][0] == "call" and event[1][1] == "probe.newer_context" for event in current_events)
    name = _declared(old[index:], new[index:], prefix=old[:index], current_logs=logs, current_queried=queried)
    before = old[index] if index < len(old) else None
    after = new[index] if index < len(new) else None
    where = f"observable event {index}:\n  legacy : {before!r}\n  current: {after!r}"
    if name is None:
        raise AssertionError(f"undeclared difference at {where}")
    return name, where


def record_declared(layer: str, name: str) -> None:
    """Tally every comparison (PARITY_COUNTS_FILE=path appends `layer<TAB>name`)."""
    path = os.environ.get("PARITY_COUNTS_FILE")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(f"{layer}\t{name}\n")


async def assert_parity(scenario: Callable[[type], Any], *, layer: str = "generated", strict: bool = False) -> str:
    """Run `scenario` through both sides. strict: the traces must be
    identical (a harness self-test). Otherwise returns the comparison's name
    (see compare) and fails only on an undeclared difference."""
    legacy, current = [await run_side(side, scenario) for side in SIDES]
    if strict:
        if legacy.events != current.events:
            raise AssertionError(_first_difference(legacy.events, current.events))
        return IDENTICAL
    name, _where = compare(legacy, current)
    record_declared(layer, name)
    return name


def _first_difference(legacy: list[Any], current: list[Any]) -> str:
    for index, (old, new) in enumerate(itertools.zip_longest(legacy, current)):
        if old != new:
            return f"traces diverge at event {index}:\n  legacy : {old!r}\n  current: {new!r}"
    return "traces differ"  # pragma: no cover


# ----------------------------------------------------------------------------
# Replaying the existing unit tests through both sides
# ----------------------------------------------------------------------------

SUPPORTED_FIXTURES = frozenset({"caplog"})


def existing_scenarios(module: ModuleType) -> list[tuple[str, Callable[..., Any], dict[str, Any]]]:
    """Every test function of `module` (parametrizations expanded) whose
    fixtures the replay can supply."""
    found = []
    for name, function in inspect.getmembers(module, inspect.isfunction):
        if not name.startswith("test_") or function.__module__ != module.__name__:
            continue
        cases: list[dict[str, Any]] = [{}]
        for mark in reversed(getattr(function, "pytestmark", [])):
            if mark.name != "parametrize":
                continue
            argnames = mark.args[0]
            names = [part.strip() for part in argnames.split(",")] if isinstance(argnames, str) else list(argnames)
            values = []
            for value in mark.args[1]:
                value = value.values if hasattr(value, "values") and hasattr(value, "marks") else value
                values.append(dict(zip(names, value if len(names) > 1 else (value,), strict=True)))
            cases = [{**case, **value} for case in cases for value in values]
        parameters = set(inspect.signature(function).parameters)
        for index, case in enumerate(cases):
            fixtures = parameters - set(case)
            unsupported = fixtures - SUPPORTED_FIXTURES
            if unsupported:
                raise AssertionError(f"{name} needs fixtures the parity replay cannot supply: {sorted(unsupported)}")
            found.append((f"{name}[{index}]" if len(cases) > 1 else name, function, case))
    return found


async def replay_existing(module: ModuleType, function: Callable[..., Any], case: dict[str, Any], caplog: Any) -> str:
    """Run an existing test through both sides (its module's
    OutboundActionService swapped for each). The test's own assertions
    describe the current gateway: they must pass on the current side. The
    two traces are compared like any scenario (see compare); a legacy-side
    assertion failure is only the declared difference showing."""

    traces = []
    original = module.OutboundActionService
    for side in SIDES:
        trace = Trace()
        caplog.clear()
        kwargs = dict(case)
        if "caplog" in inspect.signature(function).parameters:
            kwargs["caplog"] = caplog
        module.OutboundActionService = recording(side, trace)
        try:
            with tapped_logs(trace):
                try:
                    outcome = function(**kwargs)
                    if inspect.isawaitable(outcome):
                        await outcome
                except BaseException as exc:
                    if isinstance(exc, KeyboardInterrupt):
                        raise
                    trace.add("test_failed", exc)
                else:
                    trace.add("test_passed")
        finally:
            module.OutboundActionService = original
        traces.append(trace)
    legacy, current = traces
    assert current.events[-1] == current.normalize(("test_passed",)), current.events[-1]
    if legacy.events[-1] != legacy.normalize(("test_passed",)):
        # Red on the frozen judgment, green on the current gateway.
        record_declared("existing_red_on_legacy", function.__name__)
    # The verdict lines are the tests' own; compare what the gateway did.
    legacy.events = [event for event in legacy.events if event[0] not in {"test_passed", "test_failed"}]
    current.events = current.events[:-1]
    name, _where = compare(legacy, current)
    record_declared("existing", name)
    if os.environ.get("PARITY_NAMES"):
        record_declared("existing_names", f"{name}\t{function.__name__}")
    return name
