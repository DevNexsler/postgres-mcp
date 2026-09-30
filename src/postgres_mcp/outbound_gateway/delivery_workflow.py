"""Generic durable delivery workflow: coordinator, Restate app, and ingress
adapter -- the Restate/workflow machinery for EVERY operation this gateway
routes through Restate, TenantCloud included.

``tenantcloud_delivery.py`` used to hold this module's contents under
TenantCloud-flavored names, back when TenantCloud was the only operation
Restate delivered. It now imports from here and keeps only TenantCloud's own
adapter glue (``TenantCloudAdapter`` construction, TenantCloud auth) plus a
few compatibility aliases so existing callers/tests do not need to change on
this split.

``OutboundDeliveryCoordinator`` takes an ``operations`` set, so the same
class, same retry policy (``retry_policy.py``), and same Restate workflow
shape serve TenantCloud (unconditional) and any operation named in
``OUTBOUND_RESTATE_OPERATIONS`` (server.py). See the class docstring."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from enum import StrEnum
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Protocol
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import quote
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler
from urllib.request import ProxyHandler
from urllib.request import Request
from urllib.request import build_opener
from uuid import UUID

from .idempotency_policy import reinvoke_safety
from .models import ActionState
from .models import Operation
from .models import PublicStatus
from .retry_policy import CONTEXT_RELOAD_WAIT_DETAILS
from .retry_policy import CONTEXT_RELOAD_WAIT_SECONDS
from .retry_policy import RETRY_CEILING_SECONDS
from .retry_policy import NoopStaffWarningPort
from .retry_policy import StaffWarningPort
from .retry_policy import ceiling_exceeded
from .retry_policy import should_wait_for_context_reload
from .tenantcloud_shared import TENANTCLOUD_OPERATIONS

_RECIPIENT_ARGUMENT_KEYS = (
    "to_address",
    "to_phone",
    "channel_or_chat_id",
    "calendar_id",
    "lead_id",
    "thread_id",
)


def _best_effort_recipient(arguments: Any) -> str:
    if isinstance(arguments, dict):
        for key in _RECIPIENT_ARGUMENT_KEYS:
            value = arguments.get(key)
            if value:
                return str(value)
    return "unknown"


class AuthState(StrEnum):
    READY = "ready"
    LOGIN_REQUIRED = "login_required"
    RUNNER_UNAVAILABLE = "runner_unavailable"
    TRANSPORT_FAILURE = "transport_failure"
    PROVIDER_FAILURE = "provider_failure"


@dataclass(frozen=True)
class AuthResult:
    state: AuthState
    retry_after_seconds: int = 0


class DeliveryPhase(StrEnum):
    COMPLETE = "complete"
    WAIT = "wait"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class DeliveryResult:
    phase: DeliveryPhase
    detail_code: str
    retry_after_seconds: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "phase": self.phase.value,
            "detail_code": self.detail_code,
            "retry_after_seconds": self.retry_after_seconds,
        }


class DeliveryStore(Protocol):
    async def get(self, action_id: UUID) -> Any: ...


class DeliveryService(Protocol):
    async def prepare(self, action_id: UUID) -> Any: ...

    async def resume(self, action_id: UUID) -> Any: ...

    async def reconcile(self, action_id: UUID) -> Any: ...

    async def exhaust(self, action_id: UUID) -> Any: ...


class AuthGate(Protocol):
    async def ensure_ready(self) -> AuthResult: ...


_AMBIGUOUS_STATES = {
    ActionState.DISPATCHING,
    ActionState.PROVIDER_ACCEPTED,
    ActionState.UNKNOWN,
    ActionState.RECONCILING,
}
_RESUMABLE_STATES = {
    ActionState.RECEIVED,
    ActionState.PREPARED,
    ActionState.RETRY_READY,
    ActionState.DEPENDENCY_WAIT,
}
_TERMINAL_STATES = {
    ActionState.COMPLETED,
    ActionState.STALE,
    ActionState.REJECTED,
    ActionState.DEFINITIVE_FAILED,
    ActionState.DEAD_LETTER,
    ActionState.MANUAL_REVIEW,
}
# Kept as an alias of retry_policy's shared constant: several call sites and
# tests in this module predate the generalized policy module and refer to
# the old local name.
_CONTEXT_WAIT_DETAILS = CONTEXT_RELOAD_WAIT_DETAILS


class OutboundDeliveryCoordinator:
    """One-idempotent-advance-over-CDS-state coordinator behind the
    ``OutboundDelivery`` Restate workflow (``build_restate_app`` below). No
    workflow/runtime details leak in here -- ``advance()`` is a pure async
    function of durable state plus its ``store``/``service``/``auth``
    collaborators, which is what makes it safe for ``ctx.run_typed`` to
    replay.

    The ONE coordinator for every operation routed through Restate:
    ``operations`` selects which ones a given instance will advance.
    TenantCloud's own instance always passes ``TENANTCLOUD_OPERATIONS``
    (unconditional, ignoring ``OUTBOUND_RESTATE_OPERATIONS`` -- see
    ``tenantcloud_delivery.py`` and server.py); a generic instance for the
    flagged non-TenantCloud operations passes
    ``TENANTCLOUD_OPERATIONS | <flagged operations>`` so the same class,
    same Restate app, and same retry policy serve both.
    """

    def __init__(
        self,
        *,
        store: DeliveryStore,
        service: DeliveryService,
        auth: AuthGate,
        max_attempts: int = 5,
        max_ambiguous_attempts: int = 12,
        clock: Callable[[], datetime] | None = None,
        operations: frozenset[Operation] | None = None,
        context_wait_ceiling_seconds: int = RETRY_CEILING_SECONDS,
        staff_warning: StaffWarningPort | None = None,
    ) -> None:
        self._store = store
        self._service = service
        self._auth = auth
        self._operations = operations if operations is not None else TENANTCLOUD_OPERATIONS
        # Fail at construction, not at the first ambiguous outcome: every
        # operation this coordinator will ever advance must already have a
        # reinvoke-safety classification (idempotency_policy.py). A new
        # operation added to OUTBOUND_RESTATE_OPERATIONS without one is
        # exactly the silent "assume it's safe" mistake that module exists
        # to prevent.
        for operation in self._operations:
            reinvoke_safety(operation)
        self._context_wait_ceiling_seconds = max(1, context_wait_ceiling_seconds)
        # Default is inert (NoopStaffWarningPort just logs): a caller that
        # wants the real Cliq notification passes
        # staff_warning.CliqStaffWarningPort(service). See retry_policy.py's
        # StaffWarningPort docstring for the exactly-one-per-action contract.
        self._staff_warning = staff_warning or NoopStaffWarningPort()
        # NOT read by advance() (see its body): retry_policy.py's elapsed-time
        # ceiling (context_wait_ceiling_seconds, above) is the one budget for
        # every operation this coordinator serves, including the ambiguous-
        # reconcile-loop protection action c3df14e3 (2026-09-25, an unknown
        # TenantCloud send whose in-flight state held every later send to the
        # same recipient -- lease_held) needed a count cap for. Kept only as
        # constructor parameters for source compatibility with existing
        # callers (tenantcloud_delivery_server.py); passing a non-default
        # value here no longer changes advance()'s behavior.
        self._max_attempts = max(1, max_attempts)
        self._max_ambiguous_attempts = max(self._max_attempts, max_ambiguous_attempts)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def status(self, action_id: UUID) -> DeliveryResult:
        action = await self._store.get(action_id)
        if action is None:
            return DeliveryResult(DeliveryPhase.TERMINAL, "action_not_found")
        if action.operation not in self._operations:
            return DeliveryResult(DeliveryPhase.TERMINAL, "operation_not_tenantcloud")
        if action.state is ActionState.COMPLETED:
            return DeliveryResult(DeliveryPhase.COMPLETE, "completed")
        if action.state in _TERMINAL_STATES:
            return DeliveryResult(DeliveryPhase.TERMINAL, action.state.value)
        delay = max(
            1,
            int((action.next_attempt_at - self._clock()).total_seconds()),
        )
        return DeliveryResult(DeliveryPhase.WAIT, action.state.value, delay)

    async def advance(self, action_id: UUID) -> DeliveryResult:
        action = await self._store.get(action_id)
        if action is None:
            return DeliveryResult(DeliveryPhase.TERMINAL, "action_not_found")
        if action.operation not in self._operations:
            return DeliveryResult(DeliveryPhase.TERMINAL, "operation_not_tenantcloud")
        if action.state is ActionState.COMPLETED:
            return DeliveryResult(DeliveryPhase.COMPLETE, "completed")
        if action.state in _TERMINAL_STATES:
            return DeliveryResult(DeliveryPhase.TERMINAL, action.state.value)
        if action.state not in (_AMBIGUOUS_STATES | _RESUMABLE_STATES):
            return DeliveryResult(DeliveryPhase.WAIT, "action_not_prepared", 5)

        # Only TenantCloud operations need a TenantCloud login. Gating every
        # Restate-routed send on it parked Cliq/email/SMS replies for 300 s
        # whenever TenantCloud reported login_required (action c6d15f16).
        auth = (
            await self._auth.ensure_ready()
            if action.operation in TENANTCLOUD_OPERATIONS
            else AuthResult(AuthState.READY)
        )
        if auth.state is not AuthState.READY:
            default_delay = {
                AuthState.LOGIN_REQUIRED: 300,
                AuthState.RUNNER_UNAVAILABLE: 30,
                AuthState.TRANSPORT_FAILURE: 30,
                AuthState.PROVIDER_FAILURE: 120,
            }[auth.state]
            return DeliveryResult(
                DeliveryPhase.WAIT,
                f"tenantcloud_auth_{auth.state.value}",
                max(1, auth.retry_after_seconds or default_delay),
            )

        if action.state is ActionState.RECEIVED:
            result = await self._service.prepare(action_id)
            if (
                result.status is PublicStatus.PENDING
                and getattr(result, "detail", None) in _CONTEXT_WAIT_DETAILS
            ):
                return DeliveryResult(
                    DeliveryPhase.WAIT,
                    result.detail,
                    300,
                )
        else:
            # retry_policy.py owns every retry/backoff/ceiling decision for
            # every action this coordinator advances (by construction, only
            # operations in ``self._operations`` -- Restate-routed ones).
            # attempt_count is never read here: the legacy worker's 5/12
            # attempt caps (``self._max_attempts`` / ``self._max_ambiguous_attempts``,
            # kept only for the non-Restate path in worker.py/service.py) do
            # not apply once an operation is on Restate -- a send that answers
            # slowly a handful of times must not get a smaller effective
            # budget than one that answers instantly every time (see
            # retry_policy.py's module docstring). The one ceiling is
            # elapsed wall-clock time since the action's own created_at,
            # bounded at ``self._context_wait_ceiling_seconds``
            # (RETRY_CEILING_SECONDS, one hour, by default).
            action_created_at = getattr(action, "created_at", None)
            elapsed = max(0.0, (self._clock() - action_created_at).total_seconds()) if action_created_at is not None else 0.0
            exhausted = ceiling_exceeded(elapsed, ceiling_seconds=self._context_wait_ceiling_seconds)
            if action.state in _AMBIGUOUS_STATES:
                result = await self._service.exhaust(action_id) if exhausted else await self._service.reconcile(action_id)
            elif exhausted:
                result = await self._service.exhaust(action_id)
            else:
                result = await self._service.resume(action_id)
        if result.status in {PublicStatus.SENT, PublicStatus.DUPLICATE}:
            return DeliveryResult(DeliveryPhase.COMPLETE, result.detail_code)
        if result.status is PublicStatus.MANUAL_REVIEW:
            # None (a hand-built record, or a store that predates
            # created_at) is treated as "just created" -- same convention
            # as service.py's TenantCloud auth-wait ceiling anchor.
            action_created_at = getattr(action, "created_at", None)
            elapsed = max(0.0, (self._clock() - action_created_at).total_seconds()) if action_created_at is not None else 0.0
            wait_detail = result.detail_code if result.detail_code in _CONTEXT_WAIT_DETAILS else getattr(result, "detail", None)
            if wait_detail in _CONTEXT_WAIT_DETAILS and should_wait_for_context_reload(
                wait_detail,
                elapsed,
                ceiling_seconds=self._context_wait_ceiling_seconds,
            ):
                # Generalizes the RECEIVED-only wait above to every state:
                # real prod data (action 497fcaf8, calendar.update,
                # 2026-09-28) shows this same detail code reaching
                # manual_review from other branches too, well inside the
                # retry ceiling, on an action that had not actually failed
                # -- only its context reload had. Wait and let the next
                # advance() try the reload again instead of parking it.
                return DeliveryResult(DeliveryPhase.WAIT, wait_detail, CONTEXT_RELOAD_WAIT_SECONDS)
        if result.status in {
            PublicStatus.FAILED,
            PublicStatus.REJECTED,
            PublicStatus.STALE,
            PublicStatus.MANUAL_REVIEW,
        }:
            if result.status is not PublicStatus.STALE:
                # STALE is a deliberate no-send (freshness preflight), never a
                # failure -- see PublicStatus's own docstring -- so it never
                # warns. Everything else here is the workflow giving up for
                # good: a definitive provider rejection, or retry_policy's
                # ceiling. Exactly one warning per action (warn_once is keyed
                # on action_id; see StaffWarningPort's docstring for the
                # coarser wake-scoped grain the current action-id derivation
                # actually gives it).
                await self._staff_warning.warn_once(
                    action_id,
                    getattr(action, "action_uid", None),
                    result.detail_code,
                    wakeup_event_id=getattr(action, "wakeup_event_id", 0),
                    operation=action.operation,
                    recipient=_best_effort_recipient(getattr(action, "arguments", None)),
                )
            return DeliveryResult(DeliveryPhase.TERMINAL, result.detail_code)
        return await self.status(action_id)


class SecretAuthGate:
    """Classify secret-bearing auth implementation into secret-free outcomes.

    Generic despite living next to TenantCloud's only current caller
    (``server.py``'s ``build_tenantcloud_auth_gate``): nothing here reads
    anything TenantCloud-specific, it only wraps an ``auth_factory`` that
    exposes ``get_token()``. ``tenantcloud_delivery.py`` re-exports this as
    ``TenantCloudAuthGate`` for source compatibility."""

    def __init__(
        self,
        auth_factory: Callable[[], Any],
        *,
        login_required_errors: tuple[type[BaseException], ...] = (),
        transport_errors: tuple[type[BaseException], ...] = (),
    ) -> None:
        self._auth_factory = auth_factory
        self._login_required_errors = login_required_errors
        self._transport_errors = transport_errors

    async def ensure_ready(self) -> AuthResult:
        try:
            await asyncio.to_thread(self._auth_factory().get_token)
        except self._login_required_errors:
            return AuthResult(AuthState.LOGIN_REQUIRED, 300)
        except self._transport_errors:
            return AuthResult(AuthState.TRANSPORT_FAILURE, 30)
        except Exception:
            return AuthResult(AuthState.RUNNER_UNAVAILABLE, 30)
        return AuthResult(AuthState.READY)


RequestFn = Callable[[str, bytes, dict[str, str], float], Awaitable[tuple[int, bytes]]]


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


async def _post(url: str, body: bytes, headers: dict[str, str], timeout: float) -> tuple[int, bytes]:
    def send() -> tuple[int, bytes]:
        request = Request(url, data=body, headers=headers, method="POST")
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.status, response.read(16_385)
        except HTTPError as error:
            try:
                return error.code, error.read(16_385)
            finally:
                error.close()
        except (URLError, OSError, TimeoutError) as error:
            raise RuntimeError("Restate ingress request failed") from error

    return await asyncio.to_thread(send)


class RestateWorkflowSubmitter:
    """Submit exactly one Workflow invocation keyed by CDS action UUID.

    ``workflow_name`` defaults to ``"TenantCloudDelivery"``, the name
    already registered with the live Restate server -- changing it requires
    a coordinated re-registration (see the deploy notes in this change's
    report), so it is NOT renamed automatically just because the coordinator
    behind it is now generic. A second submitter instance constructed with
    ``workflow_name="OutboundDelivery"`` (or any other name you register)
    points the same client at a different deployed workflow -- still one
    coordinator class and one retry policy, just a second named deployment,
    which is the safest way to roll out the generalized worker without
    touching TenantCloud's already-live one.
    """

    def __init__(self, ingress_url: str, *, request: RequestFn = _post, workflow_name: str = "TenantCloudDelivery") -> None:
        if not workflow_name or "/" in workflow_name:
            raise ValueError("workflow_name must be a non-empty path segment")
        self._workflow_name = workflow_name
        parsed = urlsplit(ingress_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "::1"}
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Restate ingress URL must be literal HTTP loopback")
        self._ingress_url = ingress_url.rstrip("/")
        self._request = request

    async def submit(self, action_id: UUID) -> None:
        encoded_id = quote(str(action_id), safe="")
        url = f"{self._ingress_url}/{self._workflow_name}/{encoded_id}/deliver/send"
        body = json.dumps({"action_id": str(action_id)}, separators=(",", ":")).encode("utf-8")
        status, _body = await self._request(
            url,
            body,
            {"Content-Type": "application/json"},
            15.0,
        )
        if len(_body) > 16_384:
            raise RuntimeError("Restate ingress response too large")
        if status not in {200, 201, 202, 409}:
            raise RuntimeError(f"Restate ingress returned HTTP {status}")


def build_restate_app(coordinator: OutboundDeliveryCoordinator, *, workflow_name: str = "TenantCloudDelivery"):
    """Build lazily so normal gateway processes do not require Restate imports.

    ``workflow_name`` must match what ``RestateWorkflowSubmitter`` was given
    and what is registered with the Restate admin server -- see that
    class's docstring for why this is not renamed by default.

    One Restate SERVICE (this ``restate.Workflow``) exists per registered
    deployment: today that is one deployment, named ``TenantCloudDelivery``,
    serving TenantCloud unconditionally plus whatever
    ``OUTBOUND_RESTATE_OPERATIONS`` flags in -- there are not two workflow
    services in production. Renaming ``workflow_name`` on an existing
    deployment is a BREAKING re-registration, not a relabel: Restate keys
    every invocation (and its durable journal/sleep timers) by
    ``(service_name, key)``. An in-flight invocation under the old name has
    no journal under the new one, so the running Restate server would 404 on
    its next journal replay/wake the moment the old service name stops being
    served. Safe rollout of a rename is: (1) deploy a SECOND service under
    the new name (register it alongside the old one -- Restate serves both
    from the same or a different endpoint), (2) point new submissions
    (``RestateWorkflowSubmitter(..., workflow_name=new_name)``) at it, (3)
    let every in-flight invocation under the old name drain naturally (its
    ``workflow_retention`` window, 30 days here), (4) only then deregister
    the old service. This function and ``RestateWorkflowSubmitter`` already
    support that: each takes ``workflow_name`` independently, so a second
    ``build_restate_app(coordinator, workflow_name="OutboundDelivery")`` is a
    second deployable app, not a change to this one."""
    import restate

    workflow = restate.Workflow(
        workflow_name,
        inactivity_timeout=timedelta(minutes=10),
        abort_timeout=timedelta(minutes=15),
        ingress_private=False,
    )

    @workflow.main(workflow_retention=timedelta(days=30))
    async def deliver(ctx: Any, payload: object) -> dict[str, object]:
        if (
            not isinstance(payload, dict)
            or set(payload) != {"action_id"}
            or payload.get("action_id") != ctx.key()
        ):
            raise restate.TerminalError("action_id must equal workflow key", status_code=400)
        try:
            action_id = UUID(ctx.key())
        except ValueError as error:
            raise restate.TerminalError("invalid action_id", status_code=400) from error
        retry = restate.RunOptions(
            initial_retry_interval=timedelta(seconds=2),
            max_retry_interval=timedelta(minutes=5),
            retry_interval_factor=2.0,
        )
        step = 0
        while True:

            async def advance_step() -> dict[str, object]:
                return (await coordinator.advance(action_id)).to_dict()

            outcome = await ctx.run_typed(
                f"advance {step}",
                advance_step,
                retry,
            )
            if outcome["phase"] in {DeliveryPhase.COMPLETE.value, DeliveryPhase.TERMINAL.value}:
                return outcome
            delay = outcome.get("retry_after_seconds")
            if type(delay) is not int or delay < 1:
                raise restate.TerminalError("invalid delivery retry delay", status_code=500)
            await ctx.sleep(timedelta(seconds=min(delay, 3600)), name=f"wait {step}")
            step += 1

    return restate.app([workflow])
