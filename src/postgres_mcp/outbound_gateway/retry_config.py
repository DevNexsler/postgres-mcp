"""The gateway's one retry policy: how long to wait, when to give up, and
whether giving up reaches a person -- for every operation and every path
(the Restate delivery workflow, the legacy worker, and the provider-down and
provider-pending waits).

Configuration
-------------
Global defaults, all optional (defaults in brackets):

``OUTBOUND_RETRY_BASE_SECONDS`` [5]
    First wait between attempts. Each later wait doubles.
``OUTBOUND_RETRY_MAX_SECONDS`` [900]
    Cap on any single wait.
``OUTBOUND_MAX_ATTEMPTS`` [5]
    Attempt cap for the legacy (non-Restate) worker, which counts claims.
    The Restate workflow does not count attempts (a reschedule there does
    not always take a claim); its budget is the deadline below.
``OUTBOUND_RETRY_JITTER_RATIO`` [0.2]
    Each wait is shortened by up to this fraction, so actions queued during
    one outage do not all retry at the same moment. 0 turns jitter off.
``OUTBOUND_RETRY_DEADLINE_SECONDS`` [3600]
    How long an action may be deliverable but failing before the gateway
    gives up. Time the provider is known to be down -- its circuit is open,
    or its auth is not ready -- does not count, and the first check after
    the provider recovers is always a real attempt, never the end.
``OUTBOUND_RETRY_STALENESS_CEILING_SECONDS`` [86400]
    Wall-clock age from ``created_at`` past which an action is never sent,
    whatever the cause: a reply a day late is worse than a person deciding.
``OUTBOUND_RETRY_ESCALATE`` [true]
    Whether giving up posts one staff escalation. Off only for a test stack.

``OUTBOUND_RETRY_POLICY_JSON`` [unset]
    Optional overrides, a JSON object keyed by operation (``"tenantcloud.message.send"``)
    or provider (``"tenantcloud"``, ``"quo"``, ``"agent-email"``). Each value
    is an object naming any of the fields below; anything it does not name
    comes from the global defaults. An operation key wins over its
    provider's key. An operation with no override uses the global defaults.

    Field names: ``base_seconds``, ``max_seconds``, ``max_attempts``,
    ``jitter_ratio``, ``deadline_seconds``, ``staleness_ceiling_seconds``,
    ``escalate``.

Every value is validated when the gateway starts (``RetryPolicyConfig.from_env``):
a bad value, an unknown key or an inconsistent combination raises
``RetryPolicyConfigError`` and the process does not start.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import fields
from dataclasses import replace
from datetime import datetime
from typing import Any

from .metrics import PROVIDER_BY_OPERATION
from .models import Operation

ENV_BASE = "OUTBOUND_RETRY_BASE_SECONDS"
ENV_MAX = "OUTBOUND_RETRY_MAX_SECONDS"
ENV_MAX_ATTEMPTS = "OUTBOUND_MAX_ATTEMPTS"
ENV_JITTER = "OUTBOUND_RETRY_JITTER_RATIO"
ENV_DEADLINE = "OUTBOUND_RETRY_DEADLINE_SECONDS"
ENV_STALENESS = "OUTBOUND_RETRY_STALENESS_CEILING_SECONDS"
ENV_ESCALATE = "OUTBOUND_RETRY_ESCALATE"
ENV_OVERRIDES = "OUTBOUND_RETRY_POLICY_JSON"

# The Restate workflow sleeps at most an hour per step; a longer wait would
# be silently shortened, so it is refused instead.
MAX_SINGLE_WAIT_SECONDS = 3600
# A reply older than a week is never what anyone wants sent.
MAX_STALENESS_CEILING_SECONDS = 7 * 86400


class RetryPolicyConfigError(ValueError):
    """The retry configuration is invalid; the gateway must not start."""


@dataclass(frozen=True)
class RetryPolicy:
    base_seconds: int = 5
    max_seconds: int = 900
    max_attempts: int = 5
    jitter_ratio: float = 0.2
    deadline_seconds: int = 3600
    staleness_ceiling_seconds: int = 86400
    escalate: bool = True

    def __post_init__(self) -> None:
        def require(condition: bool, message: str) -> None:
            if not condition:
                raise RetryPolicyConfigError(message)

        for name in ("base_seconds", "max_seconds", "max_attempts", "deadline_seconds", "staleness_ceiling_seconds"):
            value = getattr(self, name)
            require(type(value) is int, f"{name} must be an integer, got {value!r}")
        require(type(self.escalate) is bool, f"escalate must be true or false, got {self.escalate!r}")
        require(
            type(self.jitter_ratio) in (int, float) and math.isfinite(self.jitter_ratio),
            f"jitter_ratio must be a number, got {self.jitter_ratio!r}",
        )
        require(self.base_seconds >= 1, f"base_seconds must be at least 1, got {self.base_seconds}")
        require(
            self.base_seconds <= self.max_seconds <= MAX_SINGLE_WAIT_SECONDS,
            f"max_seconds must be between base_seconds ({self.base_seconds}) and {MAX_SINGLE_WAIT_SECONDS}, got {self.max_seconds}",
        )
        require(1 <= self.max_attempts <= 100, f"max_attempts must be between 1 and 100, got {self.max_attempts}")
        require(0 <= self.jitter_ratio < 1, f"jitter_ratio must be at least 0 and below 1, got {self.jitter_ratio}")
        require(
            self.deadline_seconds >= self.max_seconds,
            f"deadline_seconds ({self.deadline_seconds}) must be at least max_seconds ({self.max_seconds})",
        )
        require(
            self.deadline_seconds <= self.staleness_ceiling_seconds <= MAX_STALENESS_CEILING_SECONDS,
            f"staleness_ceiling_seconds must be between deadline_seconds ({self.deadline_seconds}) "
            f"and {MAX_STALENESS_CEILING_SECONDS}, got {self.staleness_ceiling_seconds}",
        )

    # -- waits ---------------------------------------------------------

    def backoff_seconds(self, elapsed_seconds: float, *, jitter_key: str = "") -> int:
        """The wait after ``elapsed_seconds`` of retrying: base, doubling,
        capped at max_seconds, then jittered. A pure function of time spent,
        never of an attempt counter, so it cannot go flat when a reschedule
        does not take a claim (2026-09-28: 360 TenantCloud reinvokes in an
        hour on an un-doubled 10 s wait)."""
        remaining = max(0.0, float(elapsed_seconds))
        step = self.base_seconds
        while remaining >= step and step < self.max_seconds:
            remaining -= step
            step = min(self.max_seconds, step * 2)
        return self._jittered(step, jitter_key)

    def attempt_backoff_seconds(self, attempt_count: int, *, jitter_key: str = "") -> int:
        """The legacy worker's wait after ``attempt_count`` claims (1-based)."""
        exponent = max(0, min(int(attempt_count) - 1, 20))
        return self._jittered(min(self.max_seconds, self.base_seconds * 2**exponent), jitter_key)

    def _jittered(self, seconds: int, jitter_key: str) -> int:
        """Shorten by up to jitter_ratio, never below 1 s and never above the
        cap. Deterministic per key (the action and how far along it is), so a
        replayed Restate step computes the same wait and a test can pin it."""
        if self.jitter_ratio <= 0 or seconds <= 1:
            return seconds
        digest = hashlib.sha256(f"{jitter_key}:{seconds}".encode()).digest()
        unit = int.from_bytes(digest[:8], "big") / 2**64
        return max(1, seconds - int(seconds * self.jitter_ratio * unit))

    # -- budgets -------------------------------------------------------

    def staleness_exceeded(self, wall_elapsed_seconds: float) -> bool:
        return wall_elapsed_seconds >= self.staleness_ceiling_seconds

    def deadline_exceeded(self, deliverable_elapsed_seconds: float) -> bool:
        return deliverable_elapsed_seconds >= self.deadline_seconds

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_FIELDS = {field.name for field in fields(RetryPolicy)}
_PROVIDERS = frozenset(PROVIDER_BY_OPERATION.values())


def wall_elapsed_seconds(action: Any, now: datetime) -> float:
    """Seconds since the action was created. A record with no created_at (a
    hand-built test record) counts as just created."""
    created_at = getattr(action, "created_at", None)
    return max(0.0, (now - created_at).total_seconds()) if created_at is not None else 0.0


def paused_seconds(action: Any, now: datetime) -> float:
    """Seconds this action spent waiting on a provider known to be down:
    the closed pauses CDS has accumulated plus any pause still open."""
    total = float(getattr(action, "delivery_paused_seconds", 0.0) or 0.0)
    since = getattr(action, "delivery_paused_since", None)
    if since is not None:
        total += max(0.0, (now - since).total_seconds())
    return total


def deliverable_elapsed_seconds(action: Any, now: datetime) -> float:
    """Seconds the action could have been delivered and was not: its age
    less the time the provider was known to be down."""
    return max(0.0, wall_elapsed_seconds(action, now) - paused_seconds(action, now))


def jitter_key(action: Any) -> str:
    return str(getattr(action, "action_id", ""))


@dataclass(frozen=True)
class RetryPolicyConfig:
    default: RetryPolicy
    overrides: Mapping[str, RetryPolicy]

    def for_operation(self, operation: Operation) -> RetryPolicy:
        """Operation override, else the operation's provider override, else
        the global defaults."""
        if operation.value in self.overrides:
            return self.overrides[operation.value]
        provider = PROVIDER_BY_OPERATION.get(operation)
        if provider is not None and provider in self.overrides:
            return self.overrides[provider]
        return self.default

    def max_attempts_by_operation(self) -> dict[str, int]:
        """Every operation's attempt cap, for the legacy worker's queue SQL."""
        return {operation.value: self.for_operation(operation).max_attempts for operation in Operation}

    def describe(self) -> dict[str, Any]:
        """The effective policy, logged once at startup."""
        return {
            "default": self.default.as_dict(),
            "overrides": {key: policy.as_dict() for key, policy in sorted(self.overrides.items())},
            "effective": {operation.value: self.for_operation(operation).as_dict() for operation in Operation},
        }

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> RetryPolicyConfig:
        defaults = RetryPolicy()
        try:
            default = RetryPolicy(
                base_seconds=_int(env, ENV_BASE, defaults.base_seconds),
                max_seconds=_int(env, ENV_MAX, defaults.max_seconds),
                max_attempts=_int(env, ENV_MAX_ATTEMPTS, defaults.max_attempts),
                jitter_ratio=_float(env, ENV_JITTER, defaults.jitter_ratio),
                deadline_seconds=_int(env, ENV_DEADLINE, defaults.deadline_seconds),
                staleness_ceiling_seconds=_int(env, ENV_STALENESS, defaults.staleness_ceiling_seconds),
                escalate=_bool(env, ENV_ESCALATE, defaults.escalate),
            )
        except RetryPolicyConfigError as error:
            raise RetryPolicyConfigError(f"outbound retry defaults: {error}") from None
        return cls(default=default, overrides=_overrides(env.get(ENV_OVERRIDES), default))


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        raise RetryPolicyConfigError(f"{name} must be an integer, got {raw!r}") from None


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError:
        raise RetryPolicyConfigError(f"{name} must be a number, got {raw!r}") from None


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().casefold()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise RetryPolicyConfigError(f"{name} must be true or false, got {raw!r}")


def _overrides(raw: str | None, default: RetryPolicy) -> dict[str, RetryPolicy]:
    if raw is None or not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RetryPolicyConfigError(f"{ENV_OVERRIDES} is not valid JSON: {error}") from None
    if not isinstance(value, dict):
        raise RetryPolicyConfigError(f"{ENV_OVERRIDES} must be a JSON object keyed by operation or provider")
    operations = {operation.value for operation in Operation}
    overrides: dict[str, RetryPolicy] = {}
    for key, fields_ in value.items():
        if key not in operations and key not in _PROVIDERS:
            raise RetryPolicyConfigError(
                f"{ENV_OVERRIDES} key {key!r} is neither an operation ({sorted(operations)}) nor a provider ({sorted(_PROVIDERS)})"
            )
        if not isinstance(fields_, dict) or not fields_:
            raise RetryPolicyConfigError(f"{ENV_OVERRIDES}[{key!r}] must be a non-empty JSON object")
        unknown = sorted(set(fields_) - _FIELDS)
        if unknown:
            raise RetryPolicyConfigError(f"{ENV_OVERRIDES}[{key!r}] has unknown fields {unknown}; allowed: {sorted(_FIELDS)}")
        try:
            overrides[key] = replace(default, **fields_)
        except (RetryPolicyConfigError, TypeError) as error:
            raise RetryPolicyConfigError(f"{ENV_OVERRIDES}[{key!r}]: {error}") from None
    return overrides
