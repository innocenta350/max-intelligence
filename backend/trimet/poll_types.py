"""Immutable type vocabulary for M2 Stage S10 (feed polling and quota).

REV 3 plan §9.  These types are S10-owned; they never mutate S9 types and
never import S9 models.  All snapshot types are frozen and tuple-only.

Failure codes and gate reasons are a **closed vocabulary** (REV 3, second-pass
disposition): HTTP status numbers and typed S9 diagnostics travel as separate
metadata and are never concatenated into these codes.
"""
from __future__ import annotations

import enum
import time as _time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol, Tuple

# Re-exported vehicle request kinds come from S9 (VehiclesRequestKind);
# S10 records them verbatim for telemetry.


class PollFeed(enum.Enum):
    """The three polled feeds (plan §5)."""

    VEHICLES = "vehicles"
    TRIPUPDATES = "tripupdates"
    ALERTS = "alerts"


class RequestPurpose(enum.Enum):
    """Why one admitted attempt exists (REV 3 plan §6).

    A full request's purpose is *not* reconciliation authority.
    """

    STARTUP_FULL = "startup_full"
    SCHEDULED_FULL = "scheduled_full"
    INCREMENTAL = "incremental"
    RECOVERY_FULL = "recovery_full"


class LifecycleState(enum.Enum):
    """Runtime lifecycle (REV 3 plan §9)."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"


class GateReason(enum.Enum):
    """Why admission is currently refused (REV 3 plan §9).

    ``NONE`` is the closed-vocabulary way to say "no gate active"; it maps to
    the plan's "(or None)".  Multiple gates can coexist; see :mod:`quota`.
    """

    NONE = None
    STOPPING = "stopping"
    QUOTA_EXHAUSTED = "quota_exhausted"
    RATE_LIMITED = "rate_limited"
    CLOCK_UNTRUSTED = "clock_untrusted"
    RETRY_AFTER_UNREPRESENTABLE = "retry_after_unrepresentable"
    AUTH_BLOCKED = "auth_blocked"


class FailureCode(enum.Enum):
    """Stable S10 failure codes (REV 3 plan §9)."""

    NONE = None
    INVALID_CONFIG = "invalid_config"
    TRANSPORT_TIMEOUT = "transport_timeout"
    TRANSPORT_ERROR = "transport_error"
    HTTP_STATUS_ERROR = "http_status_error"
    AUTH_BLOCKED = "auth_blocked"
    INVALID_CONTENT_TYPE = "invalid_content_type"
    UNSUPPORTED_CONTENT_ENCODING = "unsupported_content_encoding"
    RESPONSE_TOO_LARGE = "response_too_large"
    INVALID_JSON = "invalid_json"
    FEED_REJECTED = "feed_rejected"
    OUT_OF_ORDER_FEED = "out_of_order_feed"
    INCOMPLETE_VEHICLE_COLLECTION = "incomplete_vehicle_collection"
    DELIVERY_FAILED = "delivery_failed"
    WORKER_FAILED = "worker_failed"
    START_FAILED = "start_failed"
    SHUTDOWN_TIMEOUT = "shutdown_timeout"
    INVALID_LIFECYCLE = "invalid_lifecycle"


class ClockSource(Protocol):
    """Injected wall + monotonic clock pair (plan §11 verification matrix)."""

    def mono(self) -> float: ...

    def wall_utc(self) -> datetime: ...


class SystemClock:
    """Production clock pair: injectable everywhere, never patched."""

    def mono(self) -> float:
        return _time.monotonic()

    def wall_utc(self) -> datetime:
        return datetime.now(timezone.utc)


def utc_ms(when: datetime) -> int:
    """Milliseconds since the UTC epoch for an aware datetime."""
    return int(when.timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class VehicleCollectionEvidence:
    """Pre-parser evidence about ``resultSet.vehicle`` (REV 3 plan §6).

    Facts only; never authority.  All three feeds may appear in a
    :class:`PollDelivery`, but only vehicles carry this evidence.

    ``raw_count``/``rejected_count`` are ``None`` (unknown) whenever the
    collection is absent or not a list; an explicit empty list carries zero.
    """

    key_present: bool
    is_list: bool
    raw_count: int | None
    rejected_count: int | None


@dataclass(frozen=True, slots=True)
class PollDelivery:
    """One delivered parse result (REV 3 plan §9).

    No raw response bodies, HTTPX objects or credentials leave the boundary.
    """

    feed: PollFeed
    sequence: int  # per-feed delivery sequence, upstream-ordered
    purpose: RequestPurpose
    request_sent_utc_ms: int
    receipt_utc_ms: int
    elapsed_mono_ms: int
    http_status: int
    model: Any  # S9 snapshot type (frozen); referenced, never re-normalized
    diagnostics: Tuple[Any, ...]
    evidence: VehicleCollectionEvidence | None


@dataclass(frozen=True, slots=True)
class QuotaSnapshot:
    """Frozen governor state as visible to telemetry (REV 3 plan §9)."""

    budget: int
    initial_used: int
    window_id: str
    epoch_id: int
    charged: int
    transport_handoffs: int
    responses_received: int
    transport_failures: int
    feed_rejections: int
    deliveries_accepted: int
    retry_handoffs: int
    pending_admitted: int  # admitted but not yet handed off or abandoned
    unused_admissions: int
    lifetime_charged: int
    warning_fired: bool
    warning_threshold: int
    clock_untrusted: bool
    shared_cooldown_until_mono: float | None
    feed_cooldowns: Tuple[Tuple[str, float | None], ...]
    unrepresentable_scopes: Tuple[str, ...]
    auth_blocked_feeds: Tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """Attempt metadata used only inside PollDelivery bookkeeping."""

    global_attempt: int
    retry: bool


@dataclass(frozen=True, slots=True)
class PauseResolutionRecord:
    """One bounded, credential-free operator resolution (REV 3 plan §9)."""

    timestamp_utc_ms: int
    target: str  # "shared" or PollFeed.value
    previous_reason: str
    resolution: str


@dataclass(frozen=True, slots=True)
class FeedTelemetry:
    """One feed's bounded immutable telemetry (REV 3 plan §9)."""

    feed: PollFeed
    state: LifecycleState
    gate_reason: GateReason
    next_gate_deadline_utc_ms: int | None
    last_attempt_utc_ms: int | None
    last_receipt_utc_ms: int | None
    last_delivery_utc_ms: int | None
    last_latency_ms: int | None
    last_http_status: int | None
    consecutive_failures: int
    failure_code: FailureCode
    request_kind: str | None  # "default" | "since"
    watermark_since_ms: int | None
    purpose: RequestPurpose | None
    delivery_sequence: int


@dataclass(frozen=True, slots=True)
class RuntimeTelemetry:
    """Frozen bounded runtime telemetry (REV 3 plan §9).

    ``prior_pause_state`` is ``"unknown"`` for every fresh runtime object and
    is preserved across ordinary same-object stop/start.  It never claims a
    prior incident was resolved; per-target resolution records are separate.
    """

    state: LifecycleState
    prior_pause_state: str
    resolution_records: Tuple[PauseResolutionRecord, ...]
    quota: QuotaSnapshot
    feeds: Tuple[FeedTelemetry, ...]
    surviving_workers: Tuple[str, ...]
    shutdown_incomplete: bool


PRIOR_PAUSE_UNKNOWN = "unknown"
