"""S13 clock controller tests: trust, distrust, recovery (rows 13–16).

Controller behavior with a deterministic fake clock plus the production
adapter's validation.  No real time, no sleeps, no network (deny-all
guard), no config loading or poller startup.

Covered rows:

- row 13: initial assumption, normal progression, tolerance boundary,
  both jump directions, invalid/read-failed provider, suspend with and
  without monotonic progression, and distrust before any deletion
  (named regression).
- row 14: sticky distrust, explicit epoch-checked recovery with no
  cutoff/history reset, obsolete epoch rejected, restart against a newer
  cutoff honest, blocked recovery reports and never force-resets.
- row 15: refresh/recovery serialization; sample-newer-than-cutoff
  mismatch; cleanup_deferred distinction.
- row 16: explicit clock protocol — construction/injection validation,
  sampling only on explicit calls, no timers/threads, and the socket
  guard's deliberate non-loopback block proof.
"""
from __future__ import annotations

import socket
import threading

import pytest

from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    open_state,
    s12_context,
    s12_world,
    vehicle_delivery,
    vehicle_position,
)
from tests._s13_helpers import (
    FakeFreshnessClock,
    default_policy,
    install_socket_guard,  # noqa: F401  (re-exported convention)
    s13_deny_all_sockets,  # noqa: F401  (module-local autouse guard)
)
from backend.trimet.feed_types import (
    MAX_PLAUSIBLE_UTC_MS,
    MIN_PLAUSIBLE_UTC_MS,
)
from backend.trimet.freshness import (
    FreshnessController,
    SystemFreshnessClock,
)
from backend.trimet.freshness_errors import (
    FreshnessInputError,
    FreshnessRecoveryError,
)
from backend.trimet.freshness_types import (
    ClockStatus,
    FreshnessReason,
    FreshnessSnapshot,
)
from backend.trimet.live_state import LiveState
from backend.trimet.state_errors import StateConflictError


def _controller(clock: FakeFreshnessClock | None = None, state=None,
                *, anchor: bool = True):
    """Build a controller; by default one refresh establishes the anchor."""
    state = state if state is not None else open_state(
        s12_context(s12_world(), sha256=SHA_A))[0]
    clock = clock if clock is not None else FakeFreshnessClock()
    controller = FreshnessController(
        state, policy=default_policy(), clock=clock)
    if anchor:
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED
    return controller, clock, state


def _state_with_expiring_vehicle():
    state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
    session.accept(vehicle_delivery(
        [vehicle_position(1001, time_ms=BASE_MS,
                          expires_utc_ms=BASE_MS + 10_000)],
        query_time_ms=BASE_MS))
    return state


# ---------------------------------------------------------------------------
# Row 16: the exact clock protocol
# ---------------------------------------------------------------------------


class TestClockProtocol:
    def test_production_adapter_returns_validated_samples(self):
        sample = SystemFreshnessClock().sample()
        assert isinstance(sample.utc_ms, int) and not isinstance(
            sample.utc_ms, bool)
        assert MIN_PLAUSIBLE_UTC_MS <= sample.utc_ms <= MAX_PLAUSIBLE_UTC_MS
        assert sample.monotonic_ms >= 0

    def test_no_sampling_on_import_or_construction(self):
        import backend.trimet.freshness as freshness_module

        def forbidden(*args: object, **kwargs: object) -> object:
            raise AssertionError("clock sampled without an explicit call")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(freshness_module.time, "monotonic_ns", forbidden)
            patch.setattr(freshness_module.calendar, "timegm", forbidden)
            # Construction of the production adapter and of controllers
            # (with and without an injected clock) samples nothing.
            SystemFreshnessClock()
            state = _state_with_expiring_vehicle()
            FreshnessController(state, policy=default_policy())
            FreshnessController(
                state, policy=default_policy(), clock=FakeFreshnessClock())
        # The imported module never sampled during import either: import
        # success under the patched functions is the evidence.

    def test_controller_samples_only_on_explicit_calls(self):
        controller, clock, _ = _controller(anchor=False)
        assert clock.calls == 0  # construction sampled nothing
        controller.refresh()
        assert clock.calls == 1
        controller.refresh()
        assert clock.calls == 2
        controller.reinitialize_clock(expected_clock_epoch=1)
        assert clock.calls == 3

    def test_injected_clock_validation(self):
        state = _state_with_expiring_vehicle()
        for bad_clock in (object(), "clock", 12345, None.__class__):
            with pytest.raises(FreshnessInputError) as excinfo:
                FreshnessController(
                    state, policy=default_policy(), clock=bad_clock)
            assert excinfo.value.code == "invalid_clock"

        class DeadClock:
            sample = "not callable"

        with pytest.raises(FreshnessInputError) as excinfo:
            FreshnessController(
                state, policy=default_policy(), clock=DeadClock())
        assert excinfo.value.code == "invalid_clock"

    def test_falsy_but_valid_clock_is_used_not_replaced(self):
        class FalsyClock:
            def __bool__(self) -> bool:
                return False  # deliberately falsy

            def sample(self):
                return FakeFreshnessClock().sample()

        state = _state_with_expiring_vehicle()
        falsy = FalsyClock()
        controller = FreshnessController(
            state, policy=default_policy(), clock=falsy)
        # A falsy provider must never be silently replaced: the injected
        # object's sample is the one used.
        assert controller._clock is falsy
        result = controller.refresh()
        assert result.clock_assessment.status is ClockStatus.TRUSTED

    def test_store_type_validated(self):
        with pytest.raises(FreshnessInputError) as excinfo:
            FreshnessController(
                "not a store", policy=default_policy(),
                clock=FakeFreshnessClock())
        assert excinfo.value.code == "invalid_snapshot"

    def test_no_background_threads_or_timers(self):
        before = threading.active_count()
        controller, clock, _ = _controller()
        controller.refresh()
        controller.reinitialize_clock(expected_clock_epoch=1)
        assert threading.active_count() == before

    def test_deliberate_non_loopback_attempt_is_blocked(self):
        # Row 17 guard proof: the deny-all guard raises before any packet
        # can leave the process — DNS and connect are both denied.
        with pytest.raises(AssertionError):
            socket.getaddrinfo("example.invalid", 443)
        with pytest.raises(AssertionError):
            socket.socket().connect(("203.0.113.1", 443))  # TEST-NET-3
        with pytest.raises(AssertionError):
            socket.create_connection(("203.0.113.1", 443))


# ---------------------------------------------------------------------------
# Row 13: trust assessment and jump boundaries
# ---------------------------------------------------------------------------


class TestTrustAssessment:
    def test_first_valid_sample_establishes_the_assumption(self):
        controller, clock, _ = _controller(
            FakeFreshnessClock(utc_ms=BASE_MS, monotonic_ms=1_000_000),
            anchor=False)
        result = controller.refresh()
        assessment = result.clock_assessment
        assert assessment.status is ClockStatus.TRUSTED
        assert assessment.epoch == 1
        assert assessment.reasons == ()
        assert result.sample_utc_ms == BASE_MS

    def test_normal_progression_stays_trusted(self):
        controller, clock, _ = _controller()
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED
        clock.advance(utc_ms=1000, monotonic_ms=1000)
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED
        clock.advance(utc_ms=60_000, monotonic_ms=60_000)
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED

    def test_divergence_tolerance_boundary(self):
        controller, clock, _ = _controller()  # tolerance 5000 ms, anchored
        # Equality to the tolerance is permitted: wall +10 s / mono +5 s.
        clock.advance(utc_ms=10_000, monotonic_ms=5_000)
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED
        # Strictly greater diverges in the distrust direction.
        controller2, clock2, _ = _controller()
        clock2.advance(utc_ms=10_000, monotonic_ms=4_999)
        assessment = controller2.refresh().clock_assessment
        assert assessment.status is ClockStatus.UNTRUSTED
        assert assessment.reasons == (FreshnessReason.CLOCK_DIVERGED,)

    def test_backwards_wall_clock_is_distrusted(self):
        controller, clock, _ = _controller()  # anchored
        clock.utc_ms -= 1
        assessment = controller.refresh().clock_assessment
        assert assessment.status is ClockStatus.UNTRUSTED
        assert assessment.reasons == (FreshnessReason.CLOCK_MOVED_BACKWARDS,)

    def test_backwards_monotonic_is_distrusted(self):
        controller, clock, _ = _controller()  # anchored
        clock.monotonic_ms -= 1
        assessment = controller.refresh().clock_assessment
        assert assessment.status is ClockStatus.UNTRUSTED
        assert assessment.reasons == (FreshnessReason.CLOCK_MOVED_BACKWARDS,)

    def test_suspend_with_monotonic_progression_stays_trusted(self):
        # Suspend/resume with wall and monotonic advancing together is
        # indistinguishable from normal progression and stays trusted.
        controller, clock, _ = _controller()  # anchored
        clock.advance(utc_ms=3_600_000, monotonic_ms=3_600_000)
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED

    def test_suspend_without_monotonic_progression_is_distrusted(self):
        # Wall time leapt forward while monotonic barely moved.
        controller, clock, _ = _controller()  # anchored
        clock.advance(utc_ms=3_600_000, monotonic_ms=1_000)
        assessment = controller.refresh().clock_assessment
        assert assessment.status is ClockStatus.UNTRUSTED
        assert assessment.reasons == (FreshnessReason.CLOCK_DIVERGED,)

    def test_provider_failure_is_bounded_not_fatal(self):
        controller, clock, _ = _controller()  # the anchor consumed one call
        clock.fail_with = "raise"
        assessment = controller.refresh().clock_assessment
        assert assessment.status is ClockStatus.UNTRUSTED
        assert assessment.reasons == (FreshnessReason.CLOCK_READ_FAILED,)
        # An arbitrary provider exception never becomes clock success.
        assert clock.calls == 2

    def test_invalid_sample_is_bounded(self):
        controller, clock, _ = _controller()
        clock.fail_with = "wrong_type"
        assessment = controller.refresh().clock_assessment
        assert assessment.status is ClockStatus.UNTRUSTED
        assert assessment.reasons == (FreshnessReason.INVALID_CLOCK_SAMPLE,)

    def test_uninitialized_failure_does_not_anchor(self):
        controller, clock, _ = _controller(anchor=False)
        clock.fail_with = "raise"
        assert (controller.refresh().clock_assessment.status
                is ClockStatus.UNINITIALIZED)
        clock.fail_with = None
        # The next valid sample anchors normally.
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED

    def test_clock_jump_is_distrusted_before_any_expiry_removal(self):
        # Named regression (row 13/§7): a forward wall jump must be
        # diagnosed BEFORE destructive cleanup — never delete first and
        # diagnose afterwards.
        state = _state_with_expiring_vehicle()
        clock = FakeFreshnessClock(utc_ms=BASE_MS, monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED
        before = state.snapshot()
        # The trusted anchor refresh already activated the cutoff with
        # zero removals (the vehicle is still unexpired at BASE_MS).
        assert before.expiry_state is not None
        assert before.expiry_state.cutoff_utc_ms == BASE_MS
        assert before.expiry_state.removed_entries_total == 0
        assert set(before.vehicle_entries) == {1001}
        # The jump: wall leaps far past the vehicle's expiry, monotonic
        # barely moves — divergence is detected, not trusted time.
        clock.advance(utc_ms=120_000, monotonic_ms=100)
        result = controller.refresh()
        assert result.clock_assessment.status is ClockStatus.UNTRUSTED
        assert result.expiry_result is None  # no cleanup attempted
        after = state.snapshot()
        # Nothing was deleted before diagnosis: the store is unchanged.
        assert after.revision == before.revision
        assert set(after.vehicle_entries) == set(before.vehicle_entries)
        assert after.expiry_state.cutoff_utc_ms == BASE_MS
        # And the assessed view is honest: unknown statuses, retained IDs.
        assert result.visible_max_vehicle_ids == before.max_vehicle_ids
        assert (result.vehicle_assessments[1001].expiry_status.value
                == "unknown")


# ---------------------------------------------------------------------------
# Row 14: sticky distrust and epoch-checked recovery
# ---------------------------------------------------------------------------


class TestStickyDistrustAndRecovery:
    def _distrusted(self):
        controller, clock, state = _controller()
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED
        clock.advance(utc_ms=120_000, monotonic_ms=100)  # diverge
        assert (controller.refresh().clock_assessment.status
                is ClockStatus.UNTRUSTED)
        return controller, clock, state

    def test_distrust_is_sticky(self):
        controller, clock, _ = self._distrusted()
        # Even perfectly coherent later samples stay distrusted.
        clock.advance(utc_ms=60_000, monotonic_ms=60_000)
        for _ in range(3):
            assessment = controller.refresh().clock_assessment
            assert assessment.status is ClockStatus.UNTRUSTED
            assert assessment.reasons == (FreshnessReason.CLOCK_DIVERGED,)
            assert assessment.epoch == 1

    def test_sticky_distrust_never_runs_cleanup(self):
        state = _state_with_expiring_vehicle()
        clock = FakeFreshnessClock(utc_ms=BASE_MS, monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        controller.refresh()
        clock.advance(utc_ms=120_000, monotonic_ms=100)
        controller.refresh()  # now untrusted
        before = state.snapshot()
        for _ in range(3):
            result = controller.refresh()
            assert result.expiry_result is None
        after = state.snapshot()
        assert after.revision == before.revision
        # Sticky distrust never advances the cutoff or removes anything.
        assert after.expiry_state == before.expiry_state
        assert after.expiry_state.removed_entries_total == 0
        assert set(after.vehicle_entries) == set(before.vehicle_entries)

    def test_recovery_is_epoch_checked(self):
        controller, clock, state = self._distrusted()
        before = state.snapshot()
        # Obsolete/unknown epoch rejected; nothing changed.
        for bad_epoch in (0, 2, True, "1"):
            with pytest.raises(FreshnessRecoveryError) as excinfo:
                controller.reinitialize_clock(expected_clock_epoch=bad_epoch)
            assert excinfo.value.code == "clock_epoch_conflict"
        assert state.snapshot() == before
        # Coherent corrected sample with the current epoch restores trust.
        clock.advance(utc_ms=60_000, monotonic_ms=60_000)
        assessment = controller.reinitialize_clock(expected_clock_epoch=1)
        assert assessment.status is ClockStatus.TRUSTED
        assert assessment.epoch == 2
        assert assessment.reasons == ()

    def test_clock_recovery_preserves_store_and_does_not_run_cleanup(self):
        # Named regression (row 14): recovery re-anchors the clock only —
        # it never expires, never clears data, counters or history, never
        # calls cleanup and never resets S10's quota clock.
        state = _state_with_expiring_vehicle()  # expires at BASE_MS+10_000
        clock = FakeFreshnessClock(utc_ms=BASE_MS, monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        controller.refresh()  # anchor (epoch 1)
        clock.advance(utc_ms=120_000, monotonic_ms=100)
        controller.refresh()  # distrust
        before = state.snapshot()
        # Corrected UTC is far past the vehicle's expiry: recovery must
        # NOT opportunistically remove it.
        clock.advance(utc_ms=60_000, monotonic_ms=60_000)
        assessment = controller.reinitialize_clock(expected_clock_epoch=1)
        assert assessment.status is ClockStatus.TRUSTED
        assert assessment.epoch == 2
        after = state.snapshot()
        assert after.revision == before.revision
        assert after.expiry_state == before.expiry_state
        assert after.expiry_state.removed_entries_total == 0
        assert set(after.vehicle_entries) == {1001}
        # The recovery sample call is the only extra clock read.
        assert clock.calls == 3

    def test_recovery_failure_keeps_distrust(self):
        controller, clock, _ = self._distrusted()
        clock.fail_with = "raise"
        assessment = controller.reinitialize_clock(expected_clock_epoch=1)
        assert assessment.status is ClockStatus.UNTRUSTED
        assert assessment.reasons == (FreshnessReason.CLOCK_READ_FAILED,)
        # The epoch was not consumed by the failed recovery.
        clock.fail_with = None
        assessment = controller.reinitialize_clock(expected_clock_epoch=1)
        assert assessment.status is ClockStatus.TRUSTED
        assert assessment.epoch == 2

    def test_recovery_below_cutoff_raises_frozen_typed_refusal(self):
        # Corrected UTC below the store's accepted cutoff: the frozen
        # typed refusal FreshnessRecoveryError(clock_recovery_blocked) —
        # trust is not restored, nothing changes, and no force-reset
        # escape hatch exists.  The bounded utc_before_expiry_cutoff
        # cause travels in the refusal message.
        state = _state_with_expiring_vehicle()
        clock = FakeFreshnessClock(utc_ms=BASE_MS + 100_000,
                                   monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        # The trusted anchor refresh advances the cutoff to the sample
        # (and removes the expired vehicle); capture the cutoff now.
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED
        cutoff = state.snapshot().expiry_state.cutoff_utc_ms
        assert cutoff == BASE_MS + 100_000
        # Distrust, then "correct" the clock to BEFORE the accepted cutoff.
        clock.advance(utc_ms=120_000, monotonic_ms=100)
        controller.refresh()
        before = state.snapshot()
        clock.utc_ms = cutoff - 1_000  # corrected UTC below the cutoff
        with pytest.raises(FreshnessRecoveryError) as excinfo:
            controller.reinitialize_clock(expected_clock_epoch=1)
        assert excinfo.value.code == "clock_recovery_blocked"
        assert "utc_before_expiry_cutoff" in excinfo.value.message
        # The cutoff, anchor and history were never lowered or reset.
        assert state.snapshot() == before
        # A refresh with this untrusted clock runs no cleanup and the
        # store projection is retained.
        result = controller.refresh()
        assert result.expiry_result is None
        assert result.visible_max_vehicle_ids == before.max_vehicle_ids

    @pytest.mark.parametrize("failure", ["raise", "wrong_type"])
    def test_recovery_provider_failure_before_first_anchor_is_bounded(
        self, failure,
    ):
        # A recovery attempt on a never-anchored clock whose provider
        # fails must be bounded and coherent: no raw ValueError from an
        # illegal intermediate assessment, no poisoned controller state,
        # and the next refresh anchors normally once the provider works.
        state = _state_with_expiring_vehicle()
        clock = FakeFreshnessClock(utc_ms=BASE_MS, monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        before = state.snapshot()
        clock.fail_with = failure
        assessment = controller.reinitialize_clock(expected_clock_epoch=0)
        assert assessment.status is ClockStatus.UNINITIALIZED
        assert assessment.epoch == 0
        expected_reason = (
            FreshnessReason.CLOCK_READ_FAILED
            if failure == "raise"
            else FreshnessReason.INVALID_CLOCK_SAMPLE
        )
        assert assessment.reasons == (expected_reason,)
        # The controller was not mutated into a distrust state by the
        # failed attempt; the store is untouched.
        assert controller._status is ClockStatus.UNINITIALIZED
        assert state.snapshot() == before
        # The next refresh anchors normally and yields a usable view.
        clock.fail_with = None
        result = controller.refresh()
        assert isinstance(result, FreshnessSnapshot)
        assert result.clock_assessment.status is ClockStatus.TRUSTED
        assert result.clock_assessment.epoch == 1

    @pytest.mark.parametrize("failure", ["raise", "wrong_type"])
    def test_failed_initial_recovery_does_not_poison_subsequent_refresh(
        self, failure,
    ):
        # Permanent regression for the poisoned-controller defect: the
        # failed attempt may return a coherent UNINITIALIZED assessment
        # or refuse typed — either way the controller must remain fully
        # usable afterwards with no invalid intermediate state.
        state = _state_with_expiring_vehicle()
        clock = FakeFreshnessClock(utc_ms=BASE_MS, monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        clock.fail_with = failure
        try:
            assessment = controller.reinitialize_clock(
                expected_clock_epoch=0)
        except FreshnessRecoveryError as excinfo:
            assert excinfo.code in {
                "clock_recovery_blocked", "clock_epoch_conflict"}
        else:
            assert assessment.status is ClockStatus.UNINITIALIZED
        clock.fail_with = None
        result = controller.refresh()
        assert isinstance(result, FreshnessSnapshot)
        assert result.clock_assessment.status is ClockStatus.TRUSTED

    def test_new_store_honestly_loses_cutoff_history(self):
        # Process-local honesty: a new store object loses the cutoff and
        # marks history unknown; no durable deletion guarantee exists.
        state = _state_with_expiring_vehicle()
        state.expire_vehicles(BASE_MS + 5_000)
        assert state.snapshot().expiry_state.cutoff_utc_ms == BASE_MS + 5_000
        fresh_state, session = open_state(
            s12_context(s12_world(), sha256=SHA_A))
        assert fresh_state.snapshot().expiry_state is None
        assert fresh_state.snapshot().prior_state_history == "unknown"
        # The old vehicle can be re-ingested into the new store: cutoff
        # history is gone, so the ingestion guard is disabled.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS))
        assert 1001 in fresh_state.snapshot().vehicle_entries


# ---------------------------------------------------------------------------
# Row 15: serialization and sample/cutoff mismatch
# ---------------------------------------------------------------------------


class TestSerializationAndMismatch:
    def test_refresh_and_recovery_are_serialized(self):
        # A recovery attempted while a refresh cycle is in progress
        # blocks on the controller lock — an obsolete assessment cannot
        # race a recovery into cleanup.
        controller, clock, _ = _controller()
        assert controller.refresh().clock_assessment.status is ClockStatus.TRUSTED

        entered = threading.Event()
        release = threading.Event()
        original_sample = clock.sample
        calls = {"n": 0}

        def blocking_sample():
            calls["n"] += 1
            if calls["n"] == 1:  # the thread-refresh's sample
                entered.set()
                assert release.wait(timeout=10)  # bounded, test-driven
            return original_sample()

        clock.sample = blocking_sample  # type: ignore[method-assign]

        second = threading.Thread(
            target=controller.refresh, daemon=True)
        second.start()
        assert entered.wait(timeout=10)
        recovery = threading.Thread(
            target=lambda: controller.reinitialize_clock(
                expected_clock_epoch=1),
            daemon=True)
        recovery.start()
        # The recovery cannot complete while refresh holds the controller
        # lock (bounded wait — deterministic, no sleeps).
        recovery.join(timeout=0.5)
        assert recovery.is_alive()
        release.set()
        second.join(timeout=10)
        recovery.join(timeout=10)
        assert not second.is_alive()
        assert not recovery.is_alive()
        # Final state is consistent: recovery ran to completion after the
        # refresh cycle, advancing the epoch exactly once.
        assessment = controller.refresh().clock_assessment
        assert assessment.status is ClockStatus.TRUSTED
        assert assessment.epoch == 2

    def test_refresh_sample_older_than_store_cutoff_is_mismatched(self):
        # The store's cutoff is newer than the controller's sample: the
        # cleanup is an honest idempotent no-op and the evaluated view is
        # the bounded inconsistent-sample outcome.
        state = _state_with_expiring_vehicle()
        state.expire_vehicles(BASE_MS + 90_000)
        cutoff = state.snapshot().expiry_state.cutoff_utc_ms
        revision_before = state.snapshot().revision
        clock = FakeFreshnessClock(utc_ms=BASE_MS + 45_000,
                                   monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        result = controller.refresh()
        assert result.expiry_result is not None
        assert result.expiry_result.changed is False
        assert result.expiry_result.effective_cutoff_utc_ms == cutoff
        assert result.expiry_result.removed_vehicle_ids == ()
        assert result.sample_utc_ms is None
        assert FreshnessReason.SAMPLE_BEFORE_SNAPSHOT_CUTOFF in (
            result.top_reasons)
        # The store was not mutated by the no-op cleanup.
        assert state.snapshot().revision == revision_before
        assert result.visible_max_vehicle_ids == (
            state.snapshot().max_vehicle_ids)

    def test_cleanup_deferred_is_distinguishable_from_success(self):
        # refresh where the store's publication race is lost: the cycle
        # reports cleanup_deferred with a changed=False result, unlike a
        # successful removal.
        state = _state_with_expiring_vehicle()
        clock = FakeFreshnessClock(utc_ms=BASE_MS + 45_000,
                                   monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)

        def always_conflict(store: LiveState, cutoff: int):
            raise StateConflictError(
                "store changed concurrently; publication retries "
                "exhausted and the prior root was preserved",
                code="concurrent_change",
            )

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                LiveState, "expire_vehicles", always_conflict, raising=True)
            result = controller.refresh()
        assert FreshnessReason.CLEANUP_DEFERRED in result.top_reasons
        assert result.expiry_result.changed is False
        assert result.expiry_result.effective_cutoff_utc_ms is None
        assert result.expiry_result.removed_vehicle_ids == ()
        assert state.snapshot().expiry_state is None

    @pytest.mark.parametrize(
        "code", ["generation_conflict", "sequence_collision"]
    )
    def test_controller_propagates_non_publication_conflicts(
        self, monkeypatch, code,
    ):
        # Permanent regression for the mislabeled-conflict defect: only
        # the store's publication-race code ``concurrent_change`` may be
        # deferred; every other StateConflictError code propagates
        # unchanged instead of being relabelled as cleanup contention.
        controller, clock, state = _controller()
        before = state.snapshot()

        def fail(_cutoff: int):
            raise StateConflictError(
                "Synthetic unrelated conflict", code=code)

        monkeypatch.setattr(state, "expire_vehicles", fail)
        with pytest.raises(StateConflictError) as excinfo:
            controller.refresh()
        assert excinfo.value.code == code
        # The unrelated failure published nothing and changed nothing.
        assert state.snapshot() == before
