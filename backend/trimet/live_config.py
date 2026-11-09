"""Live configuration for M2 Stage S10 polling (REV 3 plan §4).

Rules implemented exactly:

- Separate frozen ``LiveConfig``; ``GtfsConfig`` and M1 behavior untouched.
- Reuses ``REPO_ROOT``, ``_load_env_file`` and ``_env_value`` from
  :mod:`backend.trimet.config` with unchanged semantics.
- Precedence: process environment > repository ``.env`` > defaults.
- ``TRIMET_APP_ID`` is required, trimmed, non-empty, and never echoed:
  configuration output redacts the entire credential field.
- ``TRIMET_QUOTA_INITIAL_USED`` is a required numeric estimate; there is no
  sentinel and no implicit fallback.  Unknown usage is the operator explicitly
  entering the configured daily budget.
- Validation is strict: integers excluding booleans, positive finite values,
  valid IANA reset zone, valid ``HH:MM``, ``max_attempts`` in {1, 2},
  full interval at least the vehicle interval, ``0 <= initial_used <= budget``.
"""
from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.trimet.config import REPO_ROOT, _load_env_file, _env_value
from backend.trimet.poll_errors import PollConfigError


@dataclass(frozen=True, slots=True)
class AppIDSecret:
    """Credential wrapper; ``repr`` is always redacted."""

    value: str

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return "AppIDSecret(<redacted>)"


_INT_RE = re.compile(r"^-?\d+$")
_RESET_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


@dataclass(frozen=True, slots=True)
class LiveConfig:
    """Frozen live settings.  Tests construct directly or via loader."""

    app_id: AppIDSecret = dataclasses.field(repr=False)
    quota_initial_used: int
    vehicles_poll_seconds: int = 10
    tripupdates_poll_seconds: int = 60
    alerts_poll_seconds: int = 120
    vehicles_full_seconds: int = 300
    quota_daily_budget: int = 1_000_000
    quota_reset_hour: int = 0
    quota_reset_minute: int = 0
    quota_reset_zone_name: str = "UTC"
    vehicles_max_bytes: int = 2 * 1024 * 1024
    tripupdates_max_bytes: int = 2 * 1024 * 1024
    alerts_max_bytes: int = 512 * 1024
    connect_timeout_s: float = 3.0
    read_timeout_s: float = 5.0
    write_timeout_s: float = 3.0
    pool_timeout_s: float = 1.0
    max_attempts: int = 2
    backoff_min_s: float = 1.0
    backoff_max_s: float = 2.0
    attempt_deadline_s: float = 10.0

    def __post_init__(self) -> None:
        if not isinstance(self.app_id, AppIDSecret):
            raise PollConfigError("app_id must be wrapped in AppIDSecret")
        _strict_int(self, "quota_initial_used", minimum=0)
        for name in (
            "vehicles_poll_seconds",
            "tripupdates_poll_seconds",
            "alerts_poll_seconds",
            "vehicles_full_seconds",
            "quota_daily_budget",
            "vehicles_max_bytes",
            "tripupdates_max_bytes",
            "alerts_max_bytes",
        ):
            _strict_int(self, name, minimum=1)
        for name in (
            "connect_timeout_s",
            "read_timeout_s",
            "write_timeout_s",
            "pool_timeout_s",
            "backoff_min_s",
            "backoff_max_s",
            "attempt_deadline_s",
        ):
            _strict_float(self, name, minimum=0.0, allow_zero=False)
        _strict_int(self, "quota_reset_hour", minimum=0)
        _strict_int(self, "quota_reset_minute", minimum=0)
        if not (0 <= self.quota_reset_hour < 24 and 0 <= self.quota_reset_minute < 60):
            raise PollConfigError(
                "quota_reset time must be inside a valid 24-hour day"
            )
        _strict_int(self, "max_attempts", minimum=1)
        if self.max_attempts not in (1, 2):
            raise PollConfigError("max_attempts must be 1 or 2")
        if self.backoff_max_s < self.backoff_min_s:
            raise PollConfigError("backoff_max must be at least backoff_min")
        if self.vehicles_full_seconds < self.vehicles_poll_seconds:
            raise PollConfigError(
                "vehicles_full_seconds must be at least vehicles_poll_seconds"
            )
        if self.quota_initial_used > self.quota_daily_budget:
            raise PollConfigError(
                "quota_initial_used must be at most quota_daily_budget"
            )
        try:
            ZoneInfo(self.quota_reset_zone_name)
        except Exception:
            raise PollConfigError(
                f"quota_reset_zone {self.quota_reset_zone_name!r} is not a "
                "loaded IANA time zone"
            )

    def reset_zone(self) -> ZoneInfo:
        return ZoneInfo(self.quota_reset_zone_name)


def _strict_int(owner: LiveConfig, name: str, *, minimum: int | None = None) -> None:
    value = getattr(owner, name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PollConfigError(
            f"{name} must be a strict integer (booleans excluded); got type "
            f"{type(value).__name__}"
        )
    if minimum is not None and value < minimum:
        raise PollConfigError(f"{name} must be at least {minimum}")


def _strict_float(
    owner: LiveConfig, name: str, *, minimum: float | None = None,
    allow_zero: bool = True,
) -> None:
    value = getattr(owner, name)
    if isinstance(value, bool) or not (
        isinstance(value, float) or isinstance(value, int)
    ):
        raise PollConfigError(
            f"{name} must be a finite number; got type {type(value).__name__}"
        )
    if isinstance(value, float) and (
        value != value or value in (float("inf"), float("-inf"))
    ):
        raise PollConfigError(f"{name} must be finite")
    if minimum is not None and value < minimum:
        raise PollConfigError(f"{name} must be at least {minimum}")
    if not allow_zero and value <= 0:
        raise PollConfigError(f"{name} must be strictly positive")


def _int_from_env(name: str, raw: str | None) -> int:
    if raw is None or raw.strip() == "":
        raise PollConfigError(f"{name} is required")
    if not _INT_RE.match(raw):
        raise PollConfigError(
            f"{name} must be a plain base-10 integer (no sign markers, no "
            "decimal points)"
        )
    try:
        return int(raw)
    except ValueError:  # pragma: no cover - regex-guarded
        raise PollConfigError(f"{name} is not a valid integer")


def _positive_float_from_env(name: str, raw: str | None) -> float:
    if raw is None or raw.strip() == "":
        return None  # type: ignore[return-value]
    try:
        value = float(raw)
    except ValueError:
        raise PollConfigError(f"{name} is not a number")
    if value != value or value in (float("inf"), float("-inf")):
        raise PollConfigError(f"{name} must be finite")
    if value <= 0:
        raise PollConfigError(f"{name} must be strictly positive")
    return value


def load_live_config(
    *,
    env_file: Path | None = None,
    environ: dict[str, str] | None = None,
    repo_root: Path | None = None,
) -> LiveConfig:
    """Load and validate live configuration.

    ``environ`` (when provided by tests) takes precedence over the env file,
    which takes precedence over defaults.  When ``environ`` is ``None`` the
    real process environment is used; tests must pass a dict instead.
    """
    repo = repo_root or REPO_ROOT
    path = env_file if env_file is not None else repo / ".env"
    file_env = _load_env_file(path) if path.exists() else {}
    merged: dict[str, str] = dict(file_env)
    if environ:
        merged.update(environ)

    def get(name: str) -> str | None:
        return _env_value(name, merged)

    app_raw = get("TRIMET_APP_ID")
    if app_raw is None or app_raw.strip() == "":
        raise PollConfigError("TRIMET_APP_ID is required for production clients")
    app_id = AppIDSecret(app_raw.strip())

    initial_used = _int_from_env("TRIMET_QUOTA_INITIAL_USED", get("TRIMET_QUOTA_INITIAL_USED"))

    int_fields = {
        "TRIMET_VEHICLES_POLL_SECONDS": 10,
        "TRIMET_TRIPUPDATES_POLL_SECONDS": 60,
        "TRIMET_ALERTS_POLL_SECONDS": 120,
        "TRIMET_VEHICLES_FULL_SECONDS": 300,
        "TRIMET_QUOTA_DAILY_BUDGET": 1_000_000,
    }
    optional_ints: dict[str, int] = {}
    for env_name, default in int_fields.items():
        raw = get(env_name)
        optional_ints[env_name] = default if raw in (None, "") else _int_from_env(env_name, raw)

    reset_raw = get("TRIMET_QUOTA_RESET_TIME")
    reset_hour, reset_minute = 0, 0
    if reset_raw not in (None, ""):
        m = _RESET_TIME_RE.match(reset_raw.strip())
        if not m:
            raise PollConfigError("TRIMET_QUOTA_RESET_TIME must be formatted HH:MM")
        reset_hour, reset_minute = int(m.group(1)), int(m.group(2))
        if reset_hour > 23 or reset_minute > 59:
            raise PollConfigError("TRIMET_QUOTA_RESET_TIME must be inside a valid day")

    zone_raw = get("TRIMET_QUOTA_RESET_ZONE")
    zone_name = "UTC" if zone_raw in (None, "") else zone_raw.strip()

    max_bytes: dict[str, int | None] = {}
    for env_name, default in (
        ("TRIMET_VEHICLES_MAX_BYTES", 2 * 1024 * 1024),
        ("TRIMET_TRIPUPDATES_MAX_BYTES", 2 * 1024 * 1024),
        ("TRIMET_ALERTS_MAX_BYTES", 512 * 1024),
    ):
        raw = get(env_name)
        max_bytes[env_name] = default if raw in (None, "") else _int_from_env(env_name, raw)

    def floats(*pairs: tuple[str, float]) -> dict[str, float]:
        out: dict[str, float] = {}
        for env_name, default in pairs:
            value = _positive_float_from_env(env_name, get(env_name))
            out[env_name] = default if value is None else value
        return out

    fl = floats(
        ("TRIMET_HTTP_CONNECT_TIMEOUT_SECONDS", 3.0),
        ("TRIMET_HTTP_READ_TIMEOUT_SECONDS", 5.0),
        ("TRIMET_HTTP_WRITE_TIMEOUT_SECONDS", 3.0),
        ("TRIMET_HTTP_POOL_TIMEOUT_SECONDS", 1.0),
        ("TRIMET_BACKOFF_MIN_SECONDS", 1.0),
        ("TRIMET_BACKOFF_MAX_SECONDS", 2.0),
        ("TRIMET_ATTEMPT_DEADLINE_SECONDS", 10.0),
    )

    raw_attempts = get("TRIMET_MAX_ATTEMPTS")
    max_attempts = 2
    if raw_attempts not in (None, ""):
        max_attempts = _int_from_env("TRIMET_MAX_ATTEMPTS", raw_attempts)

    return LiveConfig(
        app_id=app_id,
        quota_initial_used=initial_used,
        vehicles_poll_seconds=optional_ints["TRIMET_VEHICLES_POLL_SECONDS"],
        tripupdates_poll_seconds=optional_ints["TRIMET_TRIPUPDATES_POLL_SECONDS"],
        alerts_poll_seconds=optional_ints["TRIMET_ALERTS_POLL_SECONDS"],
        vehicles_full_seconds=optional_ints["TRIMET_VEHICLES_FULL_SECONDS"],
        quota_daily_budget=optional_ints["TRIMET_QUOTA_DAILY_BUDGET"],
        quota_reset_hour=reset_hour,
        quota_reset_minute=reset_minute,
        quota_reset_zone_name=zone_name,
        vehicles_max_bytes=max_bytes["TRIMET_VEHICLES_MAX_BYTES"],
        tripupdates_max_bytes=max_bytes["TRIMET_TRIPUPDATES_MAX_BYTES"],
        alerts_max_bytes=max_bytes["TRIMET_ALERTS_MAX_BYTES"],
        connect_timeout_s=fl["TRIMET_HTTP_CONNECT_TIMEOUT_SECONDS"],
        read_timeout_s=fl["TRIMET_HTTP_READ_TIMEOUT_SECONDS"],
        write_timeout_s=fl["TRIMET_HTTP_WRITE_TIMEOUT_SECONDS"],
        pool_timeout_s=fl["TRIMET_HTTP_POOL_TIMEOUT_SECONDS"],
        max_attempts=max_attempts,
        backoff_min_s=fl["TRIMET_BACKOFF_MIN_SECONDS"],
        backoff_max_s=fl["TRIMET_BACKOFF_MAX_SECONDS"],
        attempt_deadline_s=fl["TRIMET_ATTEMPT_DEADLINE_SECONDS"],
    )
