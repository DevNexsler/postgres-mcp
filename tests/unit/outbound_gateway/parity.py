# pyright: reportArgumentType=false, reportAttributeAccessIssue=false
"""Differential harness: the frozen pre-split service vs the current one.

Each scenario runs twice -- once through LegacyOutboundActionService (the
service exactly as it was before it was split into modules) and once through
OutboundActionService -- with every collaborator instrumented. A run's trace
is the ordered list of everything externally visible:

- every public service call and its PublicResult (status, action_id,
  detail_code, detail, ...) or raised exception;
- every store call with its arguments and returned row;
- every adapter, context-loader, evidence-loader, traffic-probe and circuit
  call with its arguments and result;
- every log record the gateway emits.

Two runs are equal when their traces are equal after UUIDs are replaced by
their order of first appearance (a scenario that mints uuid4()s mints them in
the same order on both sides, or the traces differ anyway).
"""

from __future__ import annotations

import dataclasses
import inspect
import itertools
import logging
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

from postgres_mcp.outbound_gateway.legacy_service import LegacyOutboundActionService
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
    "get",
    "schedule_next_attempt",
)
ADAPTER_METHODS = ("build_request", "invoke", "poll", "parse_receipt", "reconcile")
LOADER_METHODS = ("load", "suggest_targets")
PROBE_METHODS = ("in_flight_actions", "activity_after", "context_watermark", "acknowledged_refs", "messages_by_id")
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


SIDES: tuple[type, type] = (LegacyOutboundActionService, OutboundActionService)


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


async def assert_parity(scenario: Callable[[type], Any]) -> Trace:
    legacy, current = [await run_side(side, scenario) for side in SIDES]
    if legacy.events != current.events:
        raise AssertionError(_first_difference(legacy.events, current.events))
    return current


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


async def replay_existing(module: ModuleType, function: Callable[..., Any], case: dict[str, Any], caplog: Any) -> Trace:
    """Run an existing test through both sides (its module's
    OutboundActionService swapped for each), asserting identical traces.
    The test's own assertions run on both sides too."""

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
    if legacy.events != current.events:
        raise AssertionError(_first_difference(legacy.events, current.events))
    assert current.events[-1] == current.normalize(("test_passed",)), current.events[-1]
    return current
