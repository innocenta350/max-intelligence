"""Three independent feed workers with shared-quota admission, scheduling,
delivery and lifecycle management (REV 3 plan §5, §6, §9).

Design anchors:

- One synchronous ``httpx.Client`` and one daemon thread per feed; one
  vehicle worker (no separate incremental/reconciliation threads); at most
  one in-flight request and one sink callback per feed.
- Every transport handoff consumes one shared-governor admission
  (charged, single-use, no refunds).
- Scheduling uses monotonic grid slots anchored at runtime construction;
  elapsed slots are skipped, never replayed.
- The vehicles watermark advances only from an upstream ``queryTime`` after
  an acknowledged sink delivery, merged with ``max``.
- Sinks are invoked outside all shared locks; failures are isolated per feed
  (``delivery_failed``, no thread death).
- Operator recovery APIs require a fully stopped, fully joined runtime.
"""
from __future__ import annotations

import enum
import math
import random as _random
import threading
import dataclasses
from dataclasses import dataclass
from typing import Any, Callable, Mapping

import httpx

from backend.trimet.feed_http import (
    AttemptOutcome,
    AuthRejected,
    BoundaryFailure,
    Delivered,
    FeedHttpBoundary,
    FeedRejected,
    HttpStatusFailure,
    RateLimited,
    TransportFailed,
)
from backend.trimet.feed_types import (
    VehiclesRequestKind,
    VehiclesRequestProvenance,
)
from backend.trimet.live_config import LiveConfig
from backend.trimet.poll_errors import (
    LifecycleError,
    PollConfigError,
    ProgrammerStateError,
)
from backend.trimet.poll_types import (
    ClockSource,
    FailureCode,
    FeedTelemetry,
    GateReason,
    LifecycleState,
    PauseResolutionRecord,
    PollDelivery,
    PollFeed,
    PRIOR_PAUSE_UNKNOWN,
    RequestPurpose,
    RuntimeTelemetry,
    SystemClock,
    utc_ms,
)
from backend.trimet.quota import QuotaGovernor

# Server-independent fallback when a rate-limit response carries no usable
# Retry-After (plan §8 policy minimum).  A parsed zero or past-date value
# contributes no additional wait beyond this policy floor.
FALLBACK_RETRY_AFTER_S = 60.0

# Post-override guard window after an operator resolves an unrepresentable
# Retry-After hold (plan §9: the override is declared with this cooldown).
OVERRIDE_GUARD_S = 60.0


class PauseResolution(enum.Enum):
    """Closed set of operator resolutions (REV 3 plan §9)."""

    CREDENTIALS_CORRECTED = "credentials_corrected"
    CREDENTIALS_REVALIDATED = "credentials_revalidated"
    OVERRIDE_UNREPRESENTABLE_RETRY_AFTER = "override_unrepresentable_retry_after"


@dataclass
class _MutablePerFeedTelemetry:
    """Worker-owned mutable telemetry; frozen views are built on demand."""

    last_attempt_utc_ms: int | None = None
    last_receipt_utc_ms: int | None = None
    last_delivery_utc_ms: int | None = None
    last_latency_ms: int | None = None
    last_http_status: int | None = None
    consecutive_failures: int = 0
    failure_code: FailureCode = FailureCode.NONE
    request_kind: str | None = None
    watermark_since_ms: int | None = None
    purpose: RequestPurpose | None = None
    delivery_sequence: int = 0
    gate_reason: GateReason = GateReason.NONE
    next_gate_deadline_utc_ms: int | None = None


def _clamp(value: float, low: float, high: float) -> float:
    if value < low:
        return low
    if value > high:
        return high
    return value


def _retry_delay_representable(delay_s: float | None) -> bool:
    """Can the server delay be carried through every gate-deadline form?

    Floating conversion alone is insufficient: a perfectly finite value
    such as ``1e306`` seconds still overflows the shared deadline
    arithmetic (``int(delay_s * 1000)`` — float infinity → int).  A
    missing delay is not a representation problem (the caller applies its
    documented fallback floor); anything else that fails the checked
    duration/deadline conversion is a genuine unsupported representation
    and must take the operator-only hold instead of crashing downstream.
    """
    if delay_s is None:
        return True
    if not math.isfinite(delay_s):
        return False
    try:
        int(delay_s * 1000.0)
    except (OverflowError, ValueError):  # inf, or nan → not an integer
        return False
    return True


class PollingRuntime:
    """Owns the three worker threads and the runtime-level state machine.

    All configuration and dependency validation happens at construction;
    ``start`` only spawns threads (a runtime may be started, stopped and —
    when the stop fully joined — started again on the same object).
    """

    OPERATOR_RESOLVABLE = frozenset(
        {GateReason.AUTH_BLOCKED, GateReason.RETRY_AFTER_UNREPRESENTABLE}
    )

    def __init__(
        self,
        config: LiveConfig,
        *,
        governor: QuotaGovernor,
        sinks: Mapping[PollFeed | str, Callable[[PollDelivery], None]],
        clock: ClockSource | None = None,
        jitter: Callable[[], float] | None = None,
        transport_factory: Callable[[PollFeed], httpx.Client] | None = None,
        origin_overrides: Mapping[PollFeed, str] | None = None,
        stop_join_timeout_s: float = 20.0,
    ) -> None:
        if set(PollFeed(k) for k in sinks.keys()) != set(PollFeed):
            raise PollConfigError(
                "runtime requires exactly one sink per feed (vehicles, "
                "tripupdates, alerts)"
            )
        self._config = config
        self._governor = governor
        self._sinks = {PollFeed(k): sinks[k] for k in sinks}
        self._clock = clock or SystemClock()
        self._jitter = jitter or (
            lambda: _random.uniform(
                self._config.backoff_min_s, self._config.backoff_max_s
            )
        )
        self._transport_factory = transport_factory
        self._origin_overrides = dict(origin_overrides or {})
        self._join_timeout_s = stop_join_timeout_s

        self._lock = threading.RLock()
        self._state = LifecycleState.STOPPED
        self._stop_event = threading.Event()
        self._threads: dict[str, threading.Thread] = {}

        self._telemetry: dict[PollFeed, _MutablePerFeedTelemetry] = {
            feed: _MutablePerFeedTelemetry() for feed in PollFeed
        }

        # Vehicles worker state (single-writer; mutated under self._lock).
        self._watermark: int | None = None
        self._full_recovery_next: bool = False
        self._grid_anchor_mono: float = self._clock.mono()
        self._last_full_succ_mono: float | None = None

        self._resolutions: dict[str, PauseResolutionRecord] = {}
        self._survivors: tuple[str, ...] = ()
        self._shutdown_incomplete = False
        self._prior_pause_state = PRIOR_PAUSE_UNKNOWN

    # ------------------------------------------------------------------
    # lifecycle

    def start(self) -> None:
        with self._lock:
            if self._state is not LifecycleState.STOPPED:
                raise LifecycleError(
                    "PollingRuntime.start requires a fully stopped, fully "
                    "joined runtime"
                )
            if self._survivors:
                raise LifecycleError(
                    "previous stop timed out with surviving workers; "
                    "replacement workers may not start while old workers "
                    "remain alive"
                )
            self._state = LifecycleState.STARTING
            self._stop_event = threading.Event()
        try:
            self._spawn_workers()
        except Exception:
            # Roll back to a coherent STOPPED state; no worker of this
            # runtime remains (failed spawns never joined a running set).
            self._stop_event.set()
            self._join_workers(self._join_timeout_s)
            with self._lock:
                if not self._survivors:
                    self._state = LifecycleState.STOPPED
            raise
        with self._lock:
            self._state = LifecycleState.RUNNING

    def stop(self, timeout_seconds: float | None = None) -> tuple[str, ...]:
        """Signal stop, join with timeout; returns surviving worker names."""
        timeout = (
            timeout_seconds if timeout_seconds is not None else self._join_timeout_s
        )
        with self._lock:
            if self._state is LifecycleState.STOPPED:
                return ()
            self._state = LifecycleState.STOPPING
            self._stop_event.set()
        self._join_workers(timeout)
        with self._lock:
            if not self._survivors:
                self._state = LifecycleState.STOPPED
            else:
                self._state = LifecycleState.STOPPING
        return self._survivors

    def _join_workers(self, timeout: float) -> None:
        deadline_mono = self._clock.mono() + timeout
        for name, thread in list(self._threads.items()):
            if thread.ident is None:
                # Never started: there is nothing to join, and Python would
                # raise ``RuntimeError``; such threads are cleaned up by the
                # spawn path.  ``is_alive()`` is False for them anyway.
                continue
            remaining = max(0.0, deadline_mono - self._clock.mono())
            thread.join(remaining)
        survivors = tuple(
            name for name, t in list(self._threads.items())
            if t.ident is not None and t.is_alive()
        )
        self._threads = {
            name: t for name, t in self._threads.items()
            if t.ident is not None and t.is_alive()
        }
        with self._lock:
            self._survivors = survivors
            self._shutdown_incomplete = bool(survivors)

    def _spawn_workers(self) -> None:
        for feed in PollFeed:
            thread = threading.Thread(
                target=self._run_feed_worker,
                args=(feed,),
                name=f"poller-{feed.value}",
                daemon=True,
            )
            self._threads[feed.value] = thread
            try:
                thread.start()
            except BaseException:
                # A failed ``start()`` leaves a never-started thread (ident
                # is None) behind; it must be dropped so the rollback join
                # in ``start()`` stays valid and the original spawn failure
                # is preserved unchanged.
                del self._threads[feed.value]
                raise

    # ------------------------------------------------------------------
    # operator recovery (REV 3 plan §9)

    def _require_stopped_runtime(self) -> None:
        with self._lock:
            if self._state is not LifecycleState.STOPPED or self._survivors:
                raise LifecycleError(
                    "operator recovery requires a fully stopped, fully "
                    "joined runtime; no recovery may bypass an active "
                    "attempt or delivery"
                )

    def reinitialize_quota(self, opening_used: int, expected_window_id: str) -> int:
        """Clock-recovery reinitialization; clears ``clock_untrusted``.

        Validation (stale window identity, undercut estimates) lives in the
        governor; the runtime additionally requires the fully stopped state.
        Returns the new accounting epoch identity.
        """
        self._require_stopped_runtime()
        new_epoch = self._governor.reinitialize(
            opening_used=opening_used, expected_window_id=expected_window_id
        )
        return new_epoch

    def resolve_operator_pause(
        self,
        target: PollFeed | str,
        expected_reason: GateReason,
        resolution: PauseResolution,
        replacement_config: LiveConfig | None = None,
    ) -> None:
        """Resolve one pause on a fully stopped runtime (REV 3 plan §9).

        Only ``auth_blocked`` (per feed) and ``retry_after_unrepresentable``
        (shared or per feed) are operator-resolvable.  One frozen record is
        kept per target; other gates, quota accounting and clock trust are
        untouched, and a credential-replacement config may change the AppID
        only.  Emits a credential-free enum-only audit line.
        """
        if (
            not isinstance(expected_reason, GateReason)
            or expected_reason not in self.OPERATOR_RESOLVABLE
        ):
            raise LifecycleError(
                "reason is not an operator-resolvable pause; expected "
                "auth_blocked or retry_after_unrepresentable"
            )
        if expected_reason is GateReason.AUTH_BLOCKED:
            if resolution not in (
                PauseResolution.CREDENTIALS_CORRECTED,
                PauseResolution.CREDENTIALS_REVALIDATED,
            ):
                raise LifecycleError(
                    "auth_blocked requires credentials_corrected or "
                    "credentials_revalidated"
                )
            if isinstance(target, str) and target == "shared":
                raise LifecycleError(
                    "auth_blocked requires a specific feed target"
                )
        if expected_reason is GateReason.RETRY_AFTER_UNREPRESENTABLE:
            if resolution is not PauseResolution.OVERRIDE_UNREPRESENTABLE_RETRY_AFTER:
                raise LifecycleError(
                    "unrepresentable waits require the "
                    "override_unrepresentable_retry_after resolution"
                )
            if replacement_config is not None:
                raise LifecycleError(
                    "a replacement config applies only to auth_blocked "
                    "resolutions"
                )
        if isinstance(target, PollFeed):
            scope: PollFeed | None = target
            target_name = target.value
        else:
            scope = None if str(target) == "shared" else PollFeed(str(target))
            target_name = "shared" if scope is None else scope.value
        if scope is None and expected_reason is GateReason.AUTH_BLOCKED:
            raise LifecycleError("auth_blocked requires a specific feed target")

        if replacement_config is not None:
            if not isinstance(replacement_config, LiveConfig):
                raise PollConfigError(
                    "replacement_config must be a validated LiveConfig"
                )
            for fld in dataclasses.fields(LiveConfig):
                if fld.name == "app_id":
                    continue  # never compared: credential values stay opaque
                if getattr(replacement_config, fld.name) != getattr(
                    self._config, fld.name
                ):
                    raise PollConfigError(
                        "replacement_config may change only the AppID credential"
                    )

        self._require_stopped_runtime()
        if expected_reason is GateReason.RETRY_AFTER_UNREPRESENTABLE:
            guard_mono = self._clock.mono() + OVERRIDE_GUARD_S
            guard_ms = utc_ms(self._clock.wall_utc()) + int(OVERRIDE_GUARD_S * 1000)
            resolved = self._governor.resolve(
                expected_reason,
                scope,
                cooldown_mono=guard_mono,
                cooldown_utc_ms=guard_ms,
            )
        else:
            resolved = self._governor.resolve(expected_reason, scope)
        if not resolved:
            raise LifecycleError(
                f"target {target_name} currently has no "
                f"{expected_reason.value} gate to resolve"
            )
        if replacement_config is not None:
            self._config = replacement_config
        record = PauseResolutionRecord(
            timestamp_utc_ms=utc_ms(self._clock.wall_utc()),
            target=target_name,
            previous_reason=expected_reason.value or "none",
            resolution=resolution.value,
        )
        with self._lock:
            self._resolutions[target_name] = record
        print(
            f"[audit] target={target_name} reason={expected_reason.value} "
            f"resolution={resolution.value}",
            flush=True,
        )

    # ------------------------------------------------------------------
    # telemetry

    def snapshot(self) -> RuntimeTelemetry:
        with self._lock:
            feeds = []
            for feed in PollFeed:
                t = self._telemetry[feed]
                feeds.append(
                    FeedTelemetry(
                        feed=feed,
                        state=self._state,
                        gate_reason=t.gate_reason,
                        next_gate_deadline_utc_ms=t.next_gate_deadline_utc_ms,
                        last_attempt_utc_ms=t.last_attempt_utc_ms,
                        last_receipt_utc_ms=t.last_receipt_utc_ms,
                        last_delivery_utc_ms=t.last_delivery_utc_ms,
                        last_latency_ms=t.last_latency_ms,
                        last_http_status=t.last_http_status,
                        consecutive_failures=t.consecutive_failures,
                        failure_code=t.failure_code,
                        request_kind=t.request_kind,
                        watermark_since_ms=t.watermark_since_ms,
                        purpose=t.purpose,
                        delivery_sequence=t.delivery_sequence,
                    )
                )
            return RuntimeTelemetry(
                state=self._state,
                prior_pause_state=self._prior_pause_state,
                resolution_records=tuple(self._resolutions.values()),
                quota=self._governor.snapshot(),
                feeds=tuple(feeds),
                surviving_workers=self._survivors,
                shutdown_incomplete=self._shutdown_incomplete,
            )

    def _telemetry_update(self, feed: PollFeed, **changes: Any) -> None:
        with self._lock:
            rec = self._telemetry[feed]
            for key, value in changes.items():
                setattr(rec, key, value)

    def _telemetry_failure(self, feed: PollFeed, code: FailureCode) -> None:
        with self._lock:
            rec = self._telemetry[feed]
            rec.failure_code = code
            rec.consecutive_failures += 1

    def _telemetry_delivery_ok(self, feed: PollFeed) -> None:
        with self._lock:
            rec = self._telemetry[feed]
            rec.failure_code = FailureCode.NONE
            rec.consecutive_failures = 0
            rec.last_delivery_utc_ms = utc_ms(self._clock.wall_utc())

    def _next_sequence(self, feed: PollFeed) -> int:
        with self._lock:
            self._telemetry[feed].delivery_sequence += 1
            return self._telemetry[feed].delivery_sequence

    # ------------------------------------------------------------------
    # workers

    def _boundary_for(self, feed: PollFeed) -> FeedHttpBoundary:
        override = self._origin_overrides.get(feed)

        if self._transport_factory is None:
            def client_factory() -> httpx.Client:
                from backend.trimet.feed_http import build_production_client

                return build_production_client(feed, self._config)
        else:
            factory = self._transport_factory

            def client_factory() -> httpx.Client:
                return factory(feed)

        return FeedHttpBoundary(
            feed,
            self._config,
            self._clock,
            client_factory=client_factory,
            origin_override=override,
        )

    def interval_of(self, feed: PollFeed) -> float:
        return {
            PollFeed.VEHICLES: self._config.vehicles_poll_seconds,
            PollFeed.TRIPUPDATES: self._config.tripupdates_poll_seconds,
            PollFeed.ALERTS: self._config.alerts_poll_seconds,
        }[feed]

    def _next_grid_slot(self, feed: PollFeed, now_mono: float) -> float:
        """First grid slot strictly after ``now_mono`` (elapsed slots skip)."""
        interval = float(self.interval_of(feed))
        anchor = self._grid_anchor_mono
        return anchor + (math.floor((now_mono - anchor) / interval) + 1.0) * interval

    def _run_feed_worker(self, feed: PollFeed) -> None:
        stop_event = self._stop_event
        sink = self._sinks[feed]
        boundary = self._boundary_for(feed)
        try:
            next_due = self._grid_anchor_mono  # first cycle runs immediately
            while not stop_event.is_set():
                now = self._clock.mono()
                if now < next_due:
                    # Stop-aware wait: a set event returns True.
                    if stop_event.wait(next_due - now):
                        return
                self._feed_cycle(feed, boundary, sink, stop_event, due=next_due)
                next_due = self._next_grid_slot(feed, self._clock.mono())
        except Exception:
            # Worker-level failure isolation: keep the runtime's other
            # workers running, record the failure, and end this thread.
            self._telemetry_failure(feed, FailureCode.WORKER_FAILED)
        finally:
            boundary.close()

    # -- per-cycle -------------------------------------------------------

    def _feed_cycle(
        self,
        feed: PollFeed,
        boundary: FeedHttpBoundary,
        sink: Callable[[PollDelivery], None],
        stop_event: threading.Event,
        *,
        due: float,
    ) -> None:
        """One scheduled slot: one logical request, at most one explicit retry.

        ``due`` is this slot's scheduled monotonic time; the retry wait only
        runs when it fits before the *next* regular slot (plan §8) — it must
        never overlap or delay the following slot.
        """
        if stop_event.is_set():
            return
        if feed is PollFeed.VEHICLES:
            purpose, kind, since = self._vehicle_plan()
        else:
            purpose = RequestPurpose.INCREMENTAL
            kind, since = None, None
            self._telemetry_update(feed, purpose=purpose)

        next_regular_slot = due + self.interval_of(feed)
        attempt = 0
        while attempt < self._config.max_attempts:
            if stop_event.is_set():
                return
            attempt += 1
            admission = self._governor.admit(feed, purpose, retry=attempt > 1)
            if not admission.granted:
                self._telemetry_update(
                    feed,
                    gate_reason=admission.gate_reason,
                    next_gate_deadline_utc_ms=admission.next_deadline_utc_ms,
                )
                return
            token = admission.token
            if token is None:  # defensive: granted admits always carry one
                return
            if stop_event.is_set():
                # Stopped between admission and handoff: settle the token
                # conservatively (charged, no refund — plan §7).
                self._governor.abandon(token)
                return
            handoff = self._governor.mark_handoff(token)
            if not handoff.granted:
                # Refused re-admission across a reset: the attempt is
                # charged conservatively and MUST NOT reach the transport.
                self._telemetry_update(
                    feed,
                    gate_reason=handoff.gate_reason,
                    next_gate_deadline_utc_ms=handoff.next_deadline_utc_ms,
                )
                return
            self._telemetry_update(
                feed,
                gate_reason=GateReason.NONE,
                next_gate_deadline_utc_ms=None,
                purpose=purpose,
                request_kind=kind.value if kind is not None else None,
                last_attempt_utc_ms=utc_ms(self._clock.wall_utc()),
            )
            sent_ms = self._telemetry[feed].last_attempt_utc_ms or 0
            started_mono = self._clock.mono()
            if feed is PollFeed.VEHICLES:
                provenance: Any = VehiclesRequestProvenance(
                    request_kind=kind,
                    since_ms=since,
                    requested_at_utc_ms=sent_ms,
                )
            else:
                provenance = None
            outcome = boundary.execute_attempt(
                purpose=purpose, provenance=provenance
            )
            retry = self._classify_and_act(
                feed=feed,
                outcome=outcome,
                token=token,
                sink=sink,
                stop_event=stop_event,
                started_mono=started_mono,
                sent_ms=sent_ms,
                purpose=purpose,
            )
            if feed is PollFeed.VEHICLES and purpose is RequestPurpose.INCREMENTAL \
                    and not isinstance(outcome, Delivered):
                # Incremental polling failed: the next scheduled cycle falls
                # back to a full snapshot; there is no hidden immediate
                # unbudgeted fallback (plan §6 rule 3).
                with self._lock:
                    self._full_recovery_next = True
            if not retry or stop_event.is_set() or attempt >= self._config.max_attempts:
                return
            backoff = _clamp(
                float(self._jitter()),
                self._config.backoff_min_s,
                self._config.backoff_max_s,
            )
            if self._clock.mono() + backoff >= next_regular_slot:
                # The retry wait does not fit before the next regular slot;
                # skip the in-cycle retry entirely (plan §8).
                return
            if stop_event.wait(backoff):
                return

    def _vehicle_plan(self) -> tuple[RequestPurpose, VehiclesRequestKind, int | None]:
        """Select purpose/kind/watermark for this vehicles cycle (plan §6)."""
        with self._lock:
            now_mono = self._clock.mono()
            full_needed = (
                self._watermark is None
                or self._last_full_succ_mono is None
                or self._full_recovery_next
                or (now_mono - self._last_full_succ_mono)
                >= self._config.vehicles_full_seconds
            )
            if full_needed:
                if self._watermark is None and self._last_full_succ_mono is None:
                    purpose = RequestPurpose.STARTUP_FULL
                elif self._full_recovery_next:
                    purpose = RequestPurpose.RECOVERY_FULL
                else:
                    purpose = RequestPurpose.SCHEDULED_FULL
                self._full_recovery_next = False
                rec = self._telemetry[PollFeed.VEHICLES]
                rec.purpose = purpose
                rec.request_kind = VehiclesRequestKind.DEFAULT.value
                return purpose, VehiclesRequestKind.DEFAULT, None
            purpose = RequestPurpose.INCREMENTAL
            rec = self._telemetry[PollFeed.VEHICLES]
            rec.purpose = purpose
            rec.request_kind = VehiclesRequestKind.SINCE.value
            return purpose, VehiclesRequestKind.SINCE, self._watermark

    # -- outcome classification -------------------------------------------

    def _classify_and_act(
        self,
        *,
        feed: PollFeed,
        outcome: AttemptOutcome,
        token: Any,
        sink: Callable[[PollDelivery], None],
        stop_event: threading.Event,
        started_mono: float,
        sent_ms: int,
        purpose: RequestPurpose,
    ) -> bool:
        """Record counters/telemetry for one attempt; return the retry flag.

        Retry policy (plan §8): uncertain transport failures and retryable
        HTTP statuses (408/500/502/504) retry; 429 never retries immediately
        (shared pause); 503 retries on the normal policy when its Retry-After
        is missing/invalid/zero; content-level failures do not retry.
        """
        if isinstance(outcome, Delivered):
            self._process_delivery(
                feed=feed,
                outcome=outcome,
                sink=sink,
                token=token,
                stop_event=stop_event,
                started_mono=started_mono,
                sent_ms=sent_ms,
                purpose=purpose,
            )
            return False
        if isinstance(outcome, TransportFailed):
            self._governor.record_transport_failure(token)
            code = (
                FailureCode.TRANSPORT_TIMEOUT
                if outcome.timeout
                else FailureCode.TRANSPORT_ERROR
            )
            self._telemetry_update(feed, last_http_status=None)
            self._telemetry_failure(feed, code)
            return True
        if isinstance(outcome, HttpStatusFailure):
            self._governor.record_response(token)
            self._telemetry_update(feed, last_http_status=outcome.meta.status)
            self._telemetry_failure(feed, FailureCode.HTTP_STATUS_ERROR)
            return bool(outcome.retryable)
        if isinstance(outcome, RateLimited):
            self._governor.record_response(token)
            self._telemetry_update(feed, last_http_status=outcome.meta.status)
            self._telemetry_failure(feed, FailureCode.HTTP_STATUS_ERROR)
            if outcome.unrepresentable or not _retry_delay_representable(
                outcome.parsed_delay_s
            ):
                # Operator-only hold (plan §8): either the parser already
                # failed to represent the wait at all (float-range or
                # digit-safeguard limits), or the value is finite yet
                # cannot be converted into the governor's stored deadline
                # forms (`int(delay_s * 1000)` would overflow).  Such a
                # wait is never silently clipped into a shorter cooldown
                # and never crashes the worker; the gate is visible and
                # operator-resolvable.
                self._governor.declare_unrepresentable(
                    None if outcome.scope_shared else feed
                )
                self._telemetry_update(
                    feed,
                    gate_reason=GateReason.RETRY_AFTER_UNREPRESENTABLE,
                    next_gate_deadline_utc_ms=None,
                )
                return False
            delay = outcome.parsed_delay_s
            if outcome.scope_shared:
                # 429: no immediate retry; shared AppID cooldown uses
                # max(60 s application floor, valid server delay); missing,
                # malformed, negative or signed values use the floor.  Once
                # and repeated refusals only ever extend the cooldown.
                effective = max(FALLBACK_RETRY_AFTER_S, delay or 0.0)
                self._declare_cooldown_locked(scope=None, delay_s=effective)
                return False
            # 503: a valid positive value supplies a minimum per-feed delay;
            # missing/invalid/zero values fall back to normal bounded retry.
            if delay is not None and delay > 0.0:
                self._declare_cooldown_locked(scope=feed, delay_s=delay)
            return True
        if isinstance(outcome, AuthRejected):
            self._governor.record_response(token)
            self._governor.note_auth_blocked(feed)
            self._telemetry_update(
                feed,
                last_http_status=outcome.meta.status,
                gate_reason=GateReason.AUTH_BLOCKED,
                next_gate_deadline_utc_ms=None,
            )
            self._telemetry_failure(feed, FailureCode.AUTH_BLOCKED)
            return False
        if isinstance(outcome, FeedRejected):
            self._governor.record_response(token)
            self._governor.record_feed_rejection(token)
            self._telemetry_update(feed, last_http_status=outcome.meta.status)
            self._telemetry_failure(feed, FailureCode.FEED_REJECTED)
            return False
        if isinstance(outcome, BoundaryFailure):
            self._governor.record_response(token)
            self._telemetry_update(feed, last_http_status=outcome.meta.status)
            self._telemetry_failure(feed, FailureCode(outcome.kind))
            # A cooperative deadline hit mid-decode is an uncertain
            # transport-level timeout and may retry; content defects may not.
            return outcome.kind == "transport_timeout"
        raise LifecycleError(f"unhandled outcome type {type(outcome).__name__}")

    def _declare_cooldown_locked(self, *, scope: PollFeed | None, delay_s: float) -> None:
        # Checked duration/deadline arithmetic: the caller has already
        # classified non-representable values into the operator hold, but
        # the conversion itself must never raise into a worker.
        if not _retry_delay_representable(delay_s):  # defensive; also blocks inf/nan
            raise ProgrammerStateError(
                f"cooldown delay {delay_s!r} is not representable as a deadline"
            )
        self._governor.declare_cooldown(
            scope=scope,
            deadline_mono=self._clock.mono() + delay_s,
            deadline_utc_ms=utc_ms(self._clock.wall_utc()) + int(delay_s * 1000),
        )

    # -- delivery processing ---------------------------------------------

    def _process_delivery(
        self,
        *,
        feed: PollFeed,
        outcome: Delivered,
        sink: Callable[[PollDelivery], None],
        token: Any,
        stop_event: threading.Event,
        started_mono: float,
        sent_ms: int,
        purpose: RequestPurpose,
    ) -> None:
        self._governor.record_response(token)
        result = outcome.result
        model = result.model
        receipt_mono = self._clock.mono()
        receipt_ms = utc_ms(self._clock.wall_utc())
        elapsed_ms = int(round((receipt_mono - started_mono) * 1000.0))

        if feed is PollFeed.VEHICLES:
            query_time = model.query_time_utc_ms
            with self._lock:
                stale = (
                    self._watermark is not None and query_time < self._watermark
                )
            if stale:
                # Older than the committed watermark: publish nothing and
                # advance nothing; force a full snapshot next cycle (§6 r5).
                self._telemetry_update(
                    feed,
                    last_http_status=outcome.meta.status,
                    last_receipt_utc_ms=receipt_ms,
                )
                self._telemetry_failure(feed, FailureCode.OUT_OF_ORDER_FEED)
                with self._lock:
                    self._full_recovery_next = True
                return

        if stop_event.is_set():
            # Stop was observed after handoff: record the response but do
            # not invoke a new sink callback (plan §9).
            self._telemetry_update(
                feed,
                last_http_status=outcome.meta.status,
                last_receipt_utc_ms=receipt_ms,
            )
            return

        delivery = PollDelivery(
            feed=feed,
            sequence=self._next_sequence(feed),
            purpose=purpose,
            request_sent_utc_ms=sent_ms,
            receipt_utc_ms=receipt_ms,
            elapsed_mono_ms=elapsed_ms,
            http_status=outcome.meta.status,
            model=model,
            diagnostics=tuple(result.diagnostics),
            evidence=outcome.evidence,
        )
        acked = self._invoke_sink(feed, sink, delivery)
        if not acked:
            return
        self._governor.record_delivery(token)
        self._telemetry_update(
            feed,
            last_http_status=outcome.meta.status,
            last_receipt_utc_ms=receipt_ms,
            last_latency_ms=elapsed_ms,
        )
        self._telemetry_delivery_ok(feed)

        if feed is not PollFeed.VEHICLES:
            return
        # Watermark commit (§6 rule 4): only a delivery with an explicit,
        # complete vehicle collection advances the watermark.  Missing/null
        # collections (valid empty snapshots) and collections with any
        # rejected record leave it untouched and force a full next cycle.
        evidence = outcome.evidence
        complete = (
            evidence is not None
            and evidence.key_present
            and evidence.is_list
            and evidence.rejected_count == 0
        )
        if not complete:
            self._telemetry_failure(feed, FailureCode.INCOMPLETE_VEHICLE_COLLECTION)
            with self._lock:
                self._full_recovery_next = True
            return
        query_time = model.query_time_utc_ms
        with self._lock:
            self._watermark = (
                query_time
                if self._watermark is None
                else max(self._watermark, query_time)
            )
            self._telemetry[PollFeed.VEHICLES].watermark_since_ms = self._watermark
        if purpose is not RequestPurpose.INCREMENTAL:
            with self._lock:
                self._last_full_succ_mono = receipt_mono

    def _invoke_sink(
        self,
        feed: PollFeed,
        sink: Callable[[PollDelivery], None],
        delivery: PollDelivery,
    ) -> bool:
        # Outside all shared locks (plan §9): the runtime holds no locks
        # across sink callbacks, and sink exceptions never kill a worker.
        try:
            sink(delivery)
            return True
        except Exception:
            self._telemetry_failure(feed, FailureCode.DELIVERY_FAILED)
            return False
