"""Shared request-budget governor, counters, reset windows and cooldowns.

REV 3 plan §7.  One instance is injected into all three workers; there are no
module-global counters, no per-feed allowances, and no refunds.

Locking discipline: this module never performs network I/O, parsing, logging
or sink calls while holding its lock.  Scheduler preemption can delay a token
handoff after admission; that is allowed and reconciled explicitly.
"""
from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from backend.trimet.poll_errors import LifecycleError, PollConfigError, ProgrammerStateError
from backend.trimet.poll_types import (
    ClockSource,
    GateReason,
    PollFeed,
    QuotaSnapshot,
)


# Suspend/resume is platform-dependent; this threshold is application policy
# for "abnormal wall-vs-monotonic discontinuity" (plan §7).
CLOCK_DIVERGENCE_LIMIT_S = 300.0


def compute_reset_window(
    now_utc: datetime, zone: ZoneInfo, reset_hour: int, reset_minute: int
) -> tuple[datetime, datetime, str]:
    """Return ``(start_utc, end_utc, window_id)`` for the window holding now.

    A window is bounded by successive local reset instants converted to UTC.
    For an ambiguous local reset time both UTC occurrences are boundaries
    (earlier one starts the window).  For a nonexistent local time the first
    valid local minute after the requested time is used (plan §7).
    """
    local_now = now_utc.astimezone(zone)
    candidates: list[datetime] = []
    for day_offset in (-1, 0, 1):
        day = (local_now + timedelta(days=day_offset)).date()
        for inst in _reset_instants_for_day(day, reset_hour, reset_minute, zone):
            candidates.append(inst)
    candidates.sort()
    unique: list[datetime] = []
    seen: set[float] = set()
    for c in candidates:
        k = c.timestamp()
        if k not in seen:
            seen.add(k)
            unique.append(c)
    now_ts = now_utc.timestamp()
    for i in range(len(unique) - 1):
        if unique[i].timestamp() <= now_ts < unique[i + 1].timestamp():
            return unique[i], unique[i + 1], unique[i].isoformat()
    raise ProgrammerStateError(
        f"reset window bracket failed for now={now_utc.isoformat()}"
    )


def _reset_instants_for_day(
    day: date, reset_hour: int, reset_minute: int, zone: ZoneInfo
) -> list[datetime]:
    """All distinct UTC instants for that local day's reset (fold-aware).

    Nonexistent local times resolve to the first valid local minute after.
    """
    requested = datetime(
        day.year, day.month, day.day, reset_hour, reset_minute, tzinfo=zone
    )
    out: list[datetime] = []
    for fold in (0, 1):
        candidate = requested.replace(fold=fold)
        back = candidate.astimezone(timezone.utc).astimezone(zone)
        if (back.hour, back.minute) == (reset_hour, reset_minute):
            out.append(candidate.astimezone(timezone.utc))
    if not out:
        # Nonexistent local time: first valid local minute after the requested
        # time.  Probe minute by minute up to two hours of wall shifts.
        probe = requested
        for _ in range(150):
            probe = probe + timedelta(minutes=1)
            candidate = probe.replace(fold=0, tzinfo=zone)
            back = candidate.astimezone(timezone.utc).astimezone(zone)
            if (back.hour, back.minute) == (probe.hour, probe.minute):
                out.append(candidate.astimezone(timezone.utc))
                break
    if not out:  # pragma: no cover - defensive
        raise ProgrammerStateError(
            f"no resolvable local reset on {day} for {reset_hour:02d}:{reset_minute:02d}"
        )
    return sorted(out)


@dataclass
class AttemptToken:
    """Single-use admission token (plan §7).

    Not frozen: the governor moves the token's state and window identity at
    handoff time (a delayed pre-reset reservation is re-admitted into the
    new window).  Only the owning worker mutates the state via governor
    methods.
    """

    attempt_id: int
    feed: PollFeed
    purpose: object
    retry: bool
    window_id: str
    epoch_id: int
    state: str = "pending"  # "pending" -> "handed_off"


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    granted: bool
    token: AttemptToken | None
    gate_reason: GateReason
    next_deadline_utc_ms: int | None


@dataclass(frozen=True, slots=True)
class HandoffDecision:
    """Typed result of ``mark_handoff`` (explicit granted/refused).

    A delayed token re-admitted across a reset is refused when the *current*
    window is already gated (cooldown, unrepresentable hold, auth block) or
    exhausted.  The runtime must not emit the transport attempt on refusal;
    the old charge stays conservative and the counters are reconciled.
    """

    granted: bool
    gate_reason: GateReason
    next_deadline_utc_ms: int | None


class QuotaGovernor:
    """Budget `B` including `initial_used`; B-th allowed, B+1 refused."""

    _WINDOW_COUNTERS = (
        "charged",
        "transport_handoffs",
        "responses_received",
        "transport_failures",
        "feed_rejections",
        "deliveries_accepted",
        "retry_handoffs",
    )

    def __init__(
        self,
        budget: int,
        initial_used: int,
        zone: ZoneInfo,
        reset_hour: int,
        reset_minute: int,
        clock: ClockSource,
        *,
        warn_fraction: float = 0.8,
    ) -> None:
        if isinstance(budget, bool) or not isinstance(budget, int) or budget <= 0:
            raise PollConfigError("budget must be a positive strict integer")
        if (
            isinstance(initial_used, bool)
            or not isinstance(initial_used, int)
            or not (0 <= initial_used <= budget)
        ):
            raise PollConfigError("initial_used must be 0..budget")
        if not (0 <= warn_fraction <= 1):
            raise PollConfigError("warn_fraction must be within [0, 1]")
        self._lock = threading.RLock()
        self._budget = budget
        self._initial_used = initial_used
        self._zone = zone
        self._reset_hour = reset_hour
        self._reset_minute = reset_minute
        self._clock = clock
        self._warn_fraction = warn_fraction
        self._warn_threshold = math.ceil(warn_fraction * budget)

        self._epoch = 1
        self._attempt_counter = 0
        self._warning_fired = False
        self._clock_untrusted = False
        self._shared_cooldown_until_mono: float | None = None
        self._shared_cooldown_until_ms: int | None = None
        self._feed_cooldowns: dict[str, float | None] = {f.value: None for f in PollFeed}
        self._feed_cooldown_ms: dict[str, int | None] = {f.value: None for f in PollFeed}
        self._unrepresentable: set[str | None] = set()  # None == shared
        self._auth_blocked: set[str] = set()
        self._pending_admitted = 0
        self._unused_admissions_lifetime = 0

        w = self._clock.wall_utc()
        m = self._clock.mono()
        self._anchor_wall = w
        self._anchor_mono = m
        self._window_id, self._window_start, self._window_end = self._compute_window(w)
        # The opening estimate describes usage observed at construction; it
        # is scoped to the window that was current at that moment and stops
        # contributing on a legitimate forward rollover (plan §7: the
        # estimate must not pause polling beyond its original window).
        self._seed_window_id = self._window_id

        self._window = {name: 0 for name in self._WINDOW_COUNTERS}
        self._lifetime = {name + "_lifetime": 0 for name in self._WINDOW_COUNTERS}

        self._check_clock_divergence_locked()
        self._maybe_warn_locked()

    # -- internals -----------------------------------------------------

    @property
    def lock(self):
        return self._lock

    def _compute_window(self, wall_utc: datetime):
        start, end, wid = compute_reset_window(
            wall_utc, self._zone, self._reset_hour, self._reset_minute
        )
        return wid, start, end

    def _seed_contrib_locked(self) -> int:
        """Opening estimate that still applies to the *current* window.

        The seed counts only while the runtime window is the one it was
        scored in; once `_ensure_window_locked` has legitimately rolled
        forward, the new window starts from zero known usage.
        """
        if self._window_id == self._seed_window_id:
            return self._initial_used
        return 0

    def _check_clock_divergence_locked(self) -> None:
        if self._clock_untrusted:
            return
        wall = self._clock.wall_utc()
        mono = self._clock.mono()
        dwall = (wall - self._anchor_wall).total_seconds()
        dmono = mono - self._anchor_mono
        if abs(dwall - dmono) > CLOCK_DIVERGENCE_LIMIT_S:
            self._clock_untrusted = True

    def _ensure_window_locked(self) -> None:
        # Backward wall movement never rolls counters: an older window id
        # cannot reopen an already-consumed window (plan §7).
        wall = self._clock.wall_utc()
        if wall.timestamp() < self._window_start.timestamp():
            return
        newid, start, end = self._compute_window(wall)
        if newid != self._window_id:
            # Per-event accounting convention: every window event already
            # incremented its lifetime counter when it happened, so a
            # legitimate rollover only clears the current window (adding
            # the window again would double-count).  Server cooldown
            # minima deliberately persist across application resets
            # (REV 3 plan §8: reset does not clear Retry-After).
            for name in self._WINDOW_COUNTERS:
                self._window[name] = 0
            self._warning_fired = False
            self._window_id, self._window_start, self._window_end = newid, start, end

    def _next_deadline_locked(
        self, feed: PollFeed | None = None
    ) -> tuple[GateReason, float | None, int | None]:
        """Shared-level (``feed is None``) or feed-scoped active gate.

        A shared cooldown gates every feed; a per-feed cooldown gates only
        its own feed; named unrepresentable holds match by scope.
        """
        if self._clock_untrusted:
            return GateReason.CLOCK_UNTRUSTED, None, None
        if None in self._unrepresentable or (
            feed is not None and feed.value in self._unrepresentable
        ):
            return GateReason.RETRY_AFTER_UNREPRESENTABLE, None, None
        if feed is not None and self._auth_gate_locked(feed):
            return GateReason.AUTH_BLOCKED, None, None
        now = self._clock.mono()
        deadline: float | None = None
        dl_ms: int | None = None
        if (
            self._shared_cooldown_until_mono is not None
            and self._shared_cooldown_until_mono > now
        ):
            deadline = self._shared_cooldown_until_mono
            dl_ms = self._shared_cooldown_until_ms
        if feed is not None:
            d = self._feed_cooldowns[feed.value]
            if d is not None and d > now and (deadline is None or d > deadline):
                deadline = d
                dl_ms = self._feed_cooldown_ms[feed.value]
        if deadline is not None:
            return GateReason.RATE_LIMITED, deadline, dl_ms
        return GateReason.NONE, None, None

    def _auth_gate_locked(self, feed: PollFeed | None) -> bool:
        if feed is None:
            return False
        return feed.value in self._auth_blocked

    def _maybe_warn_locked(self) -> None:
        if self._warning_fired:
            return
        if self._seed_contrib_locked() + self._window["charged"] >= self._warn_threshold:
            self._warning_fired = True

    # -- admission -----------------------------------------------------

    def admit(
        self, feed: PollFeed, purpose: object, retry: bool = False
    ) -> AdmissionResult:
        """Admission gate for one attempt; charges a unit before handoff.

        Refusals never touch the transport and never hand out tokens.
        """
        with self._lock:
            self._check_clock_divergence_locked()
            if self._clock_untrusted:
                return AdmissionResult(False, None, GateReason.CLOCK_UNTRUSTED, None)
            self._ensure_window_locked()
            reason, deadline, dl_ms = self._next_deadline_locked(feed)
            if reason is not GateReason.NONE:
                return AdmissionResult(False, None, reason, dl_ms)
            if self._auth_gate_locked(feed):
                return AdmissionResult(False, None, GateReason.AUTH_BLOCKED, None)
            if self._seed_contrib_locked() + self._window["charged"] >= self._budget:
                self._maybe_warn_locked()
                return AdmissionResult(False, None, GateReason.QUOTA_EXHAUSTED, None)
            self._window["charged"] += 1
            self._lifetime["charged_lifetime"] += 1
            self._attempt_counter += 1
            token = AttemptToken(
                attempt_id=self._attempt_counter,
                feed=feed,
                purpose=purpose,
                retry=retry,
                window_id=self._window_id,
                epoch_id=self._epoch,
            )
            self._maybe_warn_locked()
            self._pending_admitted += 1
            return AdmissionResult(True, token, GateReason.NONE, None)

    def mark_handoff(self, token: AttemptToken) -> HandoffDecision:
        """Record the transport handoff for one admitted attempt.

        The token is re-admitted when it was detained across a reset: this
        method refreshes the reset window itself (it must not trust any
        earlier refresh from another caller), applies the *current* gates
        and budget to that re-admission, and returns an explicit granted or
        refused decision.  On refusal the old charge stays conservative (no
        refund), the admission is recorded as unused, and the attempt must
        not be sent.  Same-window tokens pass straight through as before.
        """
        with self._lock:
            if token.state != "pending":
                raise LifecycleError("attempt token is single-use and already handed off")
            if token.epoch_id != self._epoch:
                raise LifecycleError("attempt token belongs to a stale accounting epoch")
            self._ensure_window_locked()
            if token.window_id != self._window_id:
                # The reservation is re-admitted in the current window: the
                # old charge is retained conservatively (no refund) and the
                # old admission is recorded as unused.
                self._unused_admissions_lifetime += 1
                self._pending_admitted -= 1
                reason, deadline, dl_ms = self._next_deadline_locked(token.feed)
                if reason is not GateReason.NONE:
                    token.window_id = self._window_id
                    token.state = "aborted"
                    return HandoffDecision(False, reason, dl_ms)
                if self._seed_contrib_locked() + self._window["charged"] >= self._budget:
                    # Refused re-admission in an exhausted window: the old
                    # charge's lifetime unit already stands from its
                    # admission event, no new charge is made, and the
                    # attempt must not be sent.
                    self._maybe_warn_locked()
                    token.window_id = self._window_id
                    token.state = "aborted"
                    return HandoffDecision(False, GateReason.QUOTA_EXHAUSTED, None)
                # One unit is additionally charged in the new window.
                self._lifetime["charged_lifetime"] += 1
                self._window["charged"] += 1
                token.window_id = self._window_id
            self._maybe_warn_locked()
            self._window["transport_handoffs"] += 1
            self._lifetime["transport_handoffs_lifetime"] += 1
            if token.retry:
                self._window["retry_handoffs"] += 1
                self._lifetime["retry_handoffs_lifetime"] += 1
            token.state = "handed_off"
            self._pending_admitted -= 1
            return HandoffDecision(True, GateReason.NONE, None)

    def abandon(self, token: AttemptToken) -> None:
        """Settle an admitted-but-never-handed-off attempt.

        Abandonment happens only when the worker stops between admission and
        transport handoff.  The charge **stays** (no refund, plan §7); the
        admission is additionally recorded as unused.
        """
        with self._lock:
            if token.state != "pending":
                raise LifecycleError(
                    "attempt token is single-use and already settled"
                )
            token.state = "aborted"
            self._pending_admitted -= 1
            self._unused_admissions_lifetime += 1

    def _record_locked(self, token: AttemptToken, counter: str) -> None:
        if token.window_id != self._window_id or token.epoch_id != self._epoch:
            self._lifetime[counter + "_lifetime"] += 1
            return
        self._window[counter] += 1
        self._lifetime[counter + "_lifetime"] += 1

    def record_response(self, token: AttemptToken) -> None:
        with self._lock:
            self._record_locked(token, "responses_received")

    def record_transport_failure(self, token: AttemptToken) -> None:
        with self._lock:
            self._record_locked(token, "transport_failures")

    def record_feed_rejection(self, token: AttemptToken) -> None:
        with self._lock:
            self._record_locked(token, "feed_rejections")

    def record_delivery(self, token: AttemptToken) -> None:
        with self._lock:
            self._record_locked(token, "deliveries_accepted")

    # -- gates and cooldowns -------------------------------------------

    def declare_cooldown(
        self,
        scope: PollFeed | None,
        deadline_mono: float,
        deadline_utc_ms: int,
    ) -> None:
        """Register a representable server wait (plan §8)."""
        with self._lock:
            if scope is None:
                if (
                    self._shared_cooldown_until_mono is None
                    or deadline_mono > self._shared_cooldown_until_mono
                ):
                    self._shared_cooldown_until_mono = deadline_mono
                    self._shared_cooldown_until_ms = deadline_utc_ms
            else:
                key = scope.value
                current = self._feed_cooldowns[key]
                if current is None or deadline_mono > current:
                    self._feed_cooldowns[key] = deadline_mono
                    self._feed_cooldown_ms[key] = deadline_utc_ms

    def declare_unrepresentable(self, scope: PollFeed | None) -> None:
        """Named operator-only hold (plan §8)."""
        with self._lock:
            self._unrepresentable.add(None if scope is None else scope.value)

    def note_auth_blocked(self, feed: PollFeed) -> None:
        with self._lock:
            self._auth_blocked.add(feed.value)

    def _clear_unrepresentable(self, scope: PollFeed | None) -> None:
        with self._lock:
            self._unrepresentable.discard(None if scope is None else scope.value)

    def _clear_auth_block(self, feed: PollFeed) -> None:
        with self._lock:
            self._auth_blocked.discard(feed.value)

    def resolve(
        self,
        expected_reason: GateReason,
        scope: PollFeed | None,
        *,
        cooldown_mono: float | None = None,
        cooldown_utc_ms: int | None = None,
    ) -> bool:
        """Resolve one operator-targeted pause (REV 3 plan §9).

        Clears exactly the selected gate and nothing else:

        - ``AUTH_BLOCKED``: the per-feed block (``scope`` must be a feed);
        - ``RETRY_AFTER_UNREPRESENTABLE``: the named hold on ``scope``
          (``None`` = shared); when a post-override policy cooldown is
          supplied it is declared immediately so lifting the hold never
          opens an unguarded window.

        Returns ``False`` when the targeted gate is absent (nothing to
        resolve); quota accounting, other gates and representable server
        cooldown minima are untouched on both paths.
        """
        with self._lock:
            if expected_reason is GateReason.AUTH_BLOCKED:
                if scope is None or scope.value not in self._auth_blocked:
                    return False
                self._clear_auth_block(scope)
                return True
            if expected_reason is GateReason.RETRY_AFTER_UNREPRESENTABLE:
                key = None if scope is None else scope.value
                if key not in self._unrepresentable:
                    return False
                self._clear_unrepresentable(scope)
                if cooldown_mono is not None and cooldown_utc_ms is not None:
                    self.declare_cooldown(scope, cooldown_mono, cooldown_utc_ms)
                return True
            raise LifecycleError(
                f"reason {expected_reason.value} is not operator-resolvable"
            )

    # -- recovery ------------------------------------------------------

    def reinitialize(
        self, opening_used: int, expected_window_id: str
    ) -> int:
        """Clock-recovery reinitialization (plan §9).

        Rejects stale window identity and estimates below known charged
        usage within the same window.  Return the new epoch identity.
        """
        with self._lock:
            self._ensure_window_locked()
            wall = self._clock.wall_utc()
            current_id, _, _ = self._compute_window(wall)
            if expected_window_id != current_id:
                raise LifecycleError("stale window identity for quota reinitialization")
            if (
                isinstance(opening_used, bool)
                or not isinstance(opening_used, int)
                or opening_used < 0
            ):
                raise PollConfigError("opening_used must be a non-negative integer")
            if opening_used < self._seed_contrib_locked() + self._window["charged"]:
                raise LifecycleError(
                    "opening_used may not undercut known usage in the same window"
                )
            if opening_used > self._budget:
                raise PollConfigError("opening_used must be at most the budget")
            self._epoch += 1
            self._initial_used = opening_used
            self._seed_window_id = current_id
            # Per-event accounting convention: keep the lifetime counters
            # (each of their increments already happened when the event
            # occurred) and clear only the current window.
            for name in self._WINDOW_COUNTERS:
                self._window[name] = 0
            self._warning_fired = False
            self._clock_untrusted = False
            self._anchor_wall = wall
            self._anchor_mono = self._clock.mono()
            self._maybe_warn_locked()
            return self._epoch

    def evaluate_warning(self) -> bool:
        with self._lock:
            self._maybe_warn_locked()
            return self._warning_fired

    # -- telemetry -----------------------------------------------------

    def current_usage(self) -> int:
        with self._lock:
            return self._seed_contrib_locked() + self._window["charged"]

    def snapshot(self) -> QuotaSnapshot:
        with self._lock:
            self._check_clock_divergence_locked()
            self._ensure_window_locked()
            reason, deadline, dl_ms = self._next_deadline_locked()
            shares: list[tuple[str, float | None]] = []
            for f in PollFeed:
                if self._feed_cooldowns[f.value] is not None:
                    shares.append((f.value, self._feed_cooldowns[f.value]))
            gates: list[GateReason] = [reason] if reason is not GateReason.NONE else []
            if self._clock_untrusted:
                gates.append(GateReason.CLOCK_UNTRUSTED)
            for scope in self._unrepresentable:
                gates.append(GateReason.RETRY_AFTER_UNREPRESENTABLE)
            for f in self._auth_blocked:
                gates.append(GateReason.AUTH_BLOCKED)
            if self._seed_contrib_locked() + self._window["charged"] >= self._budget:
                gates.append(GateReason.QUOTA_EXHAUSTED)
            gate = gates[0] if gates else GateReason.NONE
            return QuotaSnapshot(
                budget=self._budget,
                initial_used=self._initial_used,
                window_id=self._window_id,
                epoch_id=self._epoch,
                charged=self._window["charged"],
                transport_handoffs=self._window["transport_handoffs"],
                responses_received=self._window["responses_received"],
                transport_failures=self._window["transport_failures"],
                feed_rejections=self._window["feed_rejections"],
                deliveries_accepted=self._window["deliveries_accepted"],
                retry_handoffs=self._window["retry_handoffs"],
                pending_admitted=self._pending_admitted,
                unused_admissions=self._unused_admissions_lifetime,
                lifetime_charged=self._lifetime["charged_lifetime"],
                warning_fired=self._warning_fired,
                warning_threshold=self._warn_threshold,
                clock_untrusted=self._clock_untrusted,
                shared_cooldown_until_mono=self._shared_cooldown_until_mono,
                feed_cooldowns=tuple(shares),
                unrepresentable_scopes=tuple(
                    sorted(
                        "shared" if key is None else key
                        for key in self._unrepresentable
                    )
                ),
                auth_blocked_feeds=tuple(sorted(self._auth_blocked)),
            )
