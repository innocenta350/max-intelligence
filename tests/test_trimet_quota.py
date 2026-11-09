"""S10 shared quota governor tests (plan §7)."""
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from tests._s10_helpers import (
    FakeClock,
    install_socket_guard,
)

from backend.trimet.poll_errors import (
    LifecycleError,
    PollConfigError,
    ProgrammerStateError,
)
from backend.trimet.poll_types import GateReason, PollFeed, RequestPurpose
from backend.trimet.quota import QuotaGovernor, compute_reset_window

from tests.test_trimet_polling import FakeRoutes, harness, invoke_cycle, json_status

FEEDS = [PollFeed.VEHICLES, PollFeed.TRIPUPDATES, PollFeed.ALERTS]


@pytest.fixture(autouse=True)
def _s10_socket_guard(monkeypatch):
    install_socket_guard(monkeypatch)


def make_governor(clock, budget=10, initial_used=0, **kw):
    return QuotaGovernor(
        budget,
        initial_used,
        ZoneInfo("UTC"),
        kw.get("reset_hour", 0),
        kw.get("reset_minute", 0),
        clock,
        **{k: v for k, v in kw.items() if k == "warn_fraction"},
    )


class TestBoundary:
    def test_bth_admission_allowed_bplus1_refused(self):
        clock = FakeClock()
        governor = QuotaGovernor(5, 0, ZoneInfo("UTC"), 0, 0, clock)
        for i in range(5):
            result = governor.admit(PollFeed.VEHICLES, "t")
            assert result.granted, i
            governor.mark_handoff(result.token)
        denied = governor.admit(PollFeed.VEHICLES, "t")
        assert not denied.granted
        assert denied.gate_reason is GateReason.QUOTA_EXHAUSTED
        assert denied.token is None

    def test_total_includes_high_initial_used(self):
        clock = FakeClock()
        governor = QuotaGovernor(5, 3, ZoneInfo("UTC"), 0, 0, clock)
        for _ in range(2):
            result = governor.admit(PollFeed.ALERTS, "t")
            assert result.granted
            governor.mark_handoff(result.token)
        assert not governor.admit(PollFeed.ALERTS, "t").granted

    def test_initial_used_equal_budget_blocks_startup(self):
        clock = FakeClock()
        governor = QuotaGovernor(4, 4, ZoneInfo("UTC"), 0, 0, clock)
        snap = governor.snapshot()
        assert snap.charged == 0
        denied = governor.admit(PollFeed.TRIPUPDATES, "t")
        assert not denied.granted
        assert denied.gate_reason is GateReason.QUOTA_EXHAUSTED

    def test_warning_at_ceil_80_percent_including_seed(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        assert governor.snapshot().warning_fired is False
        for _ in range(8):  # ceil(0.8 * 10) = 8
            result = governor.admit(PollFeed.VEHICLES, "t")
            assert result.granted
            governor.mark_handoff(result.token)
        assert governor.snapshot().warning_fired is True

    def test_warning_fired_by_high_opening_estimate_at_construction(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 9, ZoneInfo("UTC"), 0, 0, clock)
        assert governor.snapshot().warning_fired is True


class TestAccounting:
    def test_500_is_response_not_delivery(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.TRIPUPDATES, "t")
        governor.mark_handoff(result.token)
        governor.record_response(result.token)
        snap = governor.snapshot()
        assert snap.responses_received == 1
        assert snap.deliveries_accepted == 0
        assert snap.transport_handoffs == 1

    def test_connection_failure_counts_transport_failure_with_no_refund(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "t")
        governor.mark_handoff(result.token)
        governor.record_transport_failure(result.token)
        snap = governor.snapshot()
        assert snap.transport_failures == 1
        assert snap.charged == 1  # retained, never refunded
        assert snap.transport_handoffs == 1

    def test_parser_rejection_counts_feed_rejections_and_response(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.VEHICLES, "t")
        governor.mark_handoff(result.token)
        governor.record_response(result.token)
        governor.record_feed_rejection(result.token)
        snap = governor.snapshot()
        assert snap.feed_rejections == 1
        assert snap.responses_received == 1
        assert snap.deliveries_accepted == 0

    def test_successful_delivery_counts_delivery(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "t")
        governor.mark_handoff(result.token)
        governor.record_response(result.token)
        governor.record_delivery(result.token)
        snap = governor.snapshot()
        assert snap.deliveries_accepted == 1

    def test_retry_handoff_is_subset_of_handoffs(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        first = governor.admit(PollFeed.VEHICLES, "t")
        governor.mark_handoff(first.token)
        retry = governor.admit(PollFeed.VEHICLES, "t", retry=True)
        governor.mark_handoff(retry.token)
        snap = governor.snapshot()
        assert snap.retry_handoffs == 1
        assert snap.transport_handoffs == 2
        assert snap.charged == 2

    def test_tokens_single_use(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "t")
        governor.mark_handoff(result.token)
        with pytest.raises(LifecycleError):
            governor.mark_handoff(result.token)
        with pytest.raises(LifecycleError):
            governor.abandon(result.token)

    def test_pending_admitted_accounting(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "t")
        assert governor.snapshot().pending_admitted == 1
        governor.mark_handoff(result.token)
        assert governor.snapshot().pending_admitted == 0

    def test_abandon_retains_charge_and_records_unused(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "t")
        governor.abandon(result.token)
        snap = governor.snapshot()
        assert snap.pending_admitted == 0
        assert snap.charged == 1  # no refund
        assert snap.unused_admissions == 1
        assert snap.transport_handoffs == 0
        with pytest.raises(LifecycleError):
            governor.abandon(result.token)

    def test_delayed_handoff_across_reset_recharges_and_marks_unused(self):
        # Admitted pre-reset, handed off post-reset: the old charge is
        # retained conservatively, the old admission is unused, and one unit
        # is additionally charged in the new window.
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 50, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(
            20, 0, ZoneInfo("UTC"), 0, 0, clock
        )
        result = governor.admit(PollFeed.VEHICLES, "t")
        old_window = governor.snapshot().window_id
        clock.advance(15.0)  # cross 00:00 UTC boundary
        # old-token completion must not update the new window counters
        governor.record_response(result.token)  # completion from old window
        snap = governor.snapshot()
        assert snap.window_id != old_window
        assert snap.responses_received == 0
        governor.mark_handoff(result.token)
        new_snap = governor.snapshot()
        assert new_snap.unused_admissions == 1
        assert new_snap.charged == 1  # re-admitted in the current window
        assert new_snap.transport_handoffs == 1


class TestResetWindows:
    def test_boundary_crossing_resets_window_counters_once(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 58, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "t")
        governor.mark_handoff(result.token)
        before = governor.snapshot()
        assert before.charged == 1
        clock.advance(3.0)  # cross into the next local day (00:00 UTC)
        after = governor.snapshot()
        assert after.window_id != before.window_id
        assert after.charged == 0
        assert after.lifetime_charged == 1  # exactly the one admission
        # A shared boundary resets once even when all workers arrive together
        again = governor.snapshot()
        assert again.lifetime_charged == after.lifetime_charged

    def test_non_midnight_local_reset(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 8, 3, 14, 0, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 3, 15, clock)
        first = governor.snapshot()
        result = governor.admit(PollFeed.ALERTS, "t")
        governor.mark_handoff(result.token)
        clock.advance(120.0)  # 03:16 UTC: past the 03:15 boundary
        second = governor.snapshot()
        assert second.window_id != first.window_id
        assert second.charged == 0

    def test_dst_short_day_window_duration(self):
        # America/Los_Angeles spring-forward day: the window containing
        # 2026-03-08 03:00 PDT spans Mar 8 00:00 PST -> Mar 9 00:00 PDT.
        start, end, _ = compute_reset_window(
            datetime(2026, 3, 8, 10, 0, tzinfo=timezone.utc),  # 03:00 PDT
            ZoneInfo("America/Los_Angeles"),
            0,
            0,
        )
        assert (end - start) == timedelta(hours=23), (start, end)

    def test_dst_25_hour_day_window_duration(self):
        # Fall-back day: the window containing 2026-11-01 04:00 PST spans
        # Nov 1 00:00 PDT -> Nov 2 00:00 PST (25 hours).
        start, end, _ = compute_reset_window(
            datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc),  # 04:00 PST
            ZoneInfo("America/Los_Angeles"),
            0,
            0,
        )
        assert (end - start) == timedelta(hours=25), (start, end)

    def test_ambiguous_reset_time_earlier_occurrence_boundaries(self):
        # Reset at 01:30 on 2026-11-01 is ambiguous; both UTC occurrences are
        # boundaries: 01:30 PDT = 08:30 and 01:30 PST = 09:30.
        inside_early = datetime(2026, 11, 1, 9, 0, tzinfo=timezone.utc)  # 02:00 PST
        start, end, _ = compute_reset_window(
            inside_early, ZoneInfo("America/Los_Angeles"), 1, 30
        )
        assert start.isoformat().startswith("2026-11-01T08:30:00+00:00")
        assert end.isoformat().startswith("2026-11-01T09:30:00+00:00")

    def test_nonexistent_reset_time_uses_first_valid_local_minute(self):
        # Reset at 02:30 on 2026-03-08 does not exist (02:00->03:00 jump);
        # first valid local minute is 03:00 PDT = 10:00 UTC.
        later = datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc)
        start, end, _ = compute_reset_window(
            later, ZoneInfo("America/Los_Angeles"), 2, 30
        )
        assert start.isoformat().startswith("2026-03-08T10:00:00+00:00")
        assert end.isoformat().startswith("2026-03-09T09:30:00+00:00")

    def test_skipped_days_advance_window_identity(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        first = governor.snapshot()
        clock.advance(3 * 86400.0)  # skip two full days
        third_day = governor.snapshot()
        assert third_day.window_id != first.window_id
        assert third_day.charged == 0
        result = governor.admit(PollFeed.ALERTS, "t")
        assert result.granted
        governor.mark_handoff(result.token)
        assert governor.snapshot().charged == 1
        assert governor.snapshot().window_id == third_day.window_id

    def test_backed_down_wall_clock_cannot_reopen_window_or_replenish(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 10, 12, 10, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(2, 0, ZoneInfo("UTC"), 0, 0, clock)
        for _ in range(2):
            result = governor.admit(PollFeed.ALERTS, "t")
            governor.mark_handoff(result.token)
        before = governor.snapshot()
        assert before.charged == 2
        # Wall moves backwards 40 days; mono does not.  The window holds.
        clock._wall = clock._wall - timedelta(days=40)
        snap = governor.snapshot()
        assert snap.window_id == before.window_id
        denied = governor.admit(PollFeed.ALERTS, "t")
        assert not denied.granted
        assert governor.snapshot().charged == 2  # usage never replenished


class TestClockTrust:
    def test_matching_suspend_resume_advance_is_trusted(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        clock.advance(90.0)  # both tick together (resume matching)
        result = governor.admit(PollFeed.VEHICLES, "t")
        assert result.granted
        assert governor.snapshot().clock_untrusted is False

    def test_divergent_wall_jump_marks_clock_untrusted(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        clock.jump_wall_offset(301.0)  # wall moved, mono did not
        result = governor.admit(PollFeed.VEHICLES, "t")
        assert not result.granted
        assert result.gate_reason is GateReason.CLOCK_UNTRUSTED
        assert governor.snapshot().clock_untrusted is True

    def test_cannot_replenish_via_backward_wall_clock(self):
        clock = FakeClock()
        governor = QuotaGovernor(2, 0, ZoneInfo("UTC"), 0, 0, clock)
        for _ in range(2):
            result = governor.admit(PollFeed.ALERTS, "t")
            governor.mark_handoff(result.token)
        clock.jump_wall_offset(-30.0)  # small observed divergence
        clock.advance(-0.0)
        result = governor.admit(PollFeed.ALERTS, "t")
        # Refusal comes from exhaustion even if window identity was confused
        assert not result.granted
        assert governor.snapshot().charged == 2

    def test_reinitialize_requires_stale_window_id_match(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        with pytest.raises(LifecycleError):
            governor.reinitialize(0, "2025-01-01T00:00:00+00:00")

    def test_reinitialize_rejects_low_same_window_seed(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.VEHICLES, "t")
        governor.mark_handoff(result.token)
        current = governor.snapshot().window_id
        with pytest.raises(LifecycleError):
            governor.reinitialize(0, current)  # undercuts known charged usage
        epoch = governor.reinitialize(1, current)  # equal is fine
        assert epoch == 2

    def test_reinitialize_resets_warning_clears_untrusted_keeps_cooldowns(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        for _ in range(8):
            result = governor.admit(PollFeed.VEHICLES, "t")
            governor.mark_handoff(result.token)
        assert governor.snapshot().warning_fired is True
        governor.declare_cooldown(PollFeed.ALERTS, clock.mono() + 500.0, 0)
        current = governor.snapshot().window_id
        epoch = governor.reinitialize(10, current)
        assert epoch == 2
        snap = governor.snapshot()
        assert snap.warning_fired is True  # 10 of 10 already used
        assert snap.feed_cooldowns == (("alerts", clock.mono() + 500.0),)
        assert snap.lifetime_charged == 8  # exactly the eight admissions

    def test_reinitialize_invalid_seeds_fail(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        current = governor.snapshot().window_id
        for bad in (-1, True, 1.5):
            with pytest.raises((LifecycleError, PollConfigError)):
                governor.reinitialize(bad, current)
        with pytest.raises((LifecycleError, PollConfigError)):
            governor.reinitialize(11, current)  # exceeds budget


class TestCooldowns:
    def test_per_feed_cooldown_does_not_gate_other_feeds(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_cooldown(PollFeed.ALERTS, clock.mono() + 30.0, 0)
        denied = governor.admit(PollFeed.ALERTS, "t")
        assert not denied.granted
        assert denied.gate_reason is GateReason.RATE_LIMITED
        assert governor.admit(PollFeed.VEHICLES, "t").granted

    def test_cooldown_expiry_restores_admission(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_cooldown(PollFeed.TRIPUPDATES, clock.mono() + 30.0, 0)
        assert not governor.admit(PollFeed.TRIPUPDATES, "t").granted
        clock.advance(30.1)
        assert governor.admit(PollFeed.TRIPUPDATES, "t").granted

    def test_repeated_cooldowns_extend_never_shorten(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_cooldown(None, clock.mono() + 100.0, 0)
        governor.declare_cooldown(None, clock.mono() + 10.0, 0)  # shorter
        snap = governor.snapshot()
        assert snap.shared_cooldown_until_mono == clock.mono() + 100.0

    def test_shared_cooldown_gates_every_feed(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_cooldown(None, clock.mono() + 60.0, 0)
        for feed in (PollFeed.VEHICLES, PollFeed.TRIPUPDATES, PollFeed.ALERTS):
            denied = governor.admit(feed, "t")
            assert not denied.granted
            assert denied.gate_reason is GateReason.RATE_LIMITED

    def test_named_unrepresentable_hold(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_unrepresentable(PollFeed.ALERTS)
        denied = governor.admit(PollFeed.ALERTS, "t")
        assert not denied.granted
        assert denied.gate_reason is GateReason.RETRY_AFTER_UNREPRESENTABLE
        assert next_gate_is_none(denied)
        assert governor.admit(PollFeed.VEHICLES, "t").granted


def next_gate_is_none(result):
    return result.next_deadline_utc_ms is None


class TestResolution:
    def test_resolve_auth_blocked_and_absent_returns_false(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        assert governor.resolve(GateReason.AUTH_BLOCKED, PollFeed.VEHICLES) is False
        governor.note_auth_blocked(PollFeed.VEHICLES)
        assert governor.resolve(GateReason.AUTH_BLOCKED, PollFeed.VEHICLES) is True
        assert governor.admit(PollFeed.VEHICLES, "t").granted

    def test_resolve_auth_requires_feed_target(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.note_auth_blocked(PollFeed.ALERTS)
        # Auth blocks are per-feed only; a shared target resolves nothing.
        assert governor.resolve(GateReason.AUTH_BLOCKED, None) is False
        assert governor.resolve(GateReason.AUTH_BLOCKED, PollFeed.ALERTS) is True

    def test_resolve_unrepresentable_declares_guard_cooldown(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_unrepresentable(None)
        assert governor.resolve(
            GateReason.RETRY_AFTER_UNREPRESENTABLE,
            None,
            cooldown_mono=clock.mono() + 60.0,
            cooldown_utc_ms=0,
        )
        denied = governor.admit(PollFeed.VEHICLES, "t")
        assert not denied.granted
        assert denied.gate_reason is GateReason.RATE_LIMITED

    def test_resolve_without_cooldown_reopens_completely(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_unrepresentable(PollFeed.TRIPUPDATES)
        assert governor.resolve(
            GateReason.RETRY_AFTER_UNREPRESENTABLE, PollFeed.TRIPUPDATES
        )
        assert governor.admit(PollFeed.TRIPUPDATES, "t").granted

    def test_resolve_leaves_other_gates_and_accounting(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 5, ZoneInfo("UTC"), 0, 0, clock)
        governor.note_auth_blocked(PollFeed.ALERTS)
        governor.note_auth_blocked(PollFeed.VEHICLES)
        assert governor.resolve(GateReason.AUTH_BLOCKED, PollFeed.ALERTS)
        snap = governor.snapshot()
        assert snap.auth_blocked_feeds == ("vehicles",)  # others preserved
        assert snap.charged == 0  # accounting untouched
        denied = governor.admit(PollFeed.VEHICLES, "t")
        assert not denied.granted

    def test_unresolvable_reason_rejected(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        with pytest.raises(LifecycleError):
            governor.resolve(GateReason.QUOTA_EXHAUSTED, None)


class TestGovernorValidation:
    def test_invalid_budget_and_seed_fails(self):
        clock = FakeClock()
        for budget in (0, -1, True, 1.5):
            with pytest.raises(PollConfigError):
                QuotaGovernor(budget, 0, ZoneInfo("UTC"), 0, 0, clock)
        for seed in (-1, True, 11):
            with pytest.raises(PollConfigError):
                QuotaGovernor(10, seed, ZoneInfo("UTC"), 0, 0, clock)

    def test_exhausted_never_closes_inflight(self):
        # A request already handed off can finish after exhaustion.
        clock = FakeClock()
        governor = QuotaGovernor(1, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.VEHICLES, "t")
        # The single budget unit refuses a second admission
        assert not governor.admit(PollFeed.TRIPUPDATES, "t").granted
        governor.mark_handoff(result.token)
        governor.record_response(result.token)
        governor.record_delivery(result.token)
        snap = governor.snapshot()
        assert snap.deliveries_accepted == 1


class TestConcurrency:
    def test_final_unit_race_admits_exactly_one(self):
        clock = FakeClock()
        governor = QuotaGovernor(1, 0, ZoneInfo("UTC"), 0, 0, clock)
        granted = []
        lock = threading.Lock()

        def race(feed):
            result = governor.admit(feed, "t")
            if result.granted:
                governor.mark_handoff(result.token)
                with lock:
                    granted.append(feed)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(race, FEEDS * 3))
        assert len(granted) == 1
        snap = governor.snapshot()
        assert snap.charged == 1


class TestOpeningSeedScope:
    """The opening estimate is scoped to its own reset window (review F1)."""

    def test_seed_enforces_budget_within_its_window(self):
        clock = FakeClock()
        governor = QuotaGovernor(5, 5, ZoneInfo("UTC"), 0, 0, clock)
        assert governor.current_usage() == 5
        result = governor.admit(PollFeed.ALERTS, "t")
        assert not result.granted
        assert result.gate_reason is GateReason.QUOTA_EXHAUSTED

    def test_exhausted_seed_expires_at_window_rollover(self):
        # budget 5 fully consumed by pre-runtime usage at 23:59:59 UTC: the
        # first next-day attempt must be granted (plan §7 startup policy).
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(5, 5, ZoneInfo("UTC"), 0, 0, clock)
        refused = governor.admit(PollFeed.ALERTS, "before")
        assert not refused.granted
        clock.advance(2.0)  # cross the 00:00 UTC reset
        granted = governor.admit(PollFeed.ALERTS, "after")
        assert granted.granted
        assert governor.current_usage() == 1

    def test_cooldowns_and_clock_flags_survive_seed_expiry(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(5, 5, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_cooldown(PollFeed.ALERTS, clock.mono() + 500.0, 0)
        governor.note_auth_blocked(PollFeed.VEHICLES)
        clock.advance(2.0)
        enforced = governor.admit(PollFeed.ALERTS, "t")
        assert enforced.gate_reason is GateReason.RATE_LIMITED  # survived roll
        blocked = governor.admit(PollFeed.VEHICLES, "t")
        assert blocked.gate_reason is GateReason.AUTH_BLOCKED  # survived roll

    def test_warning_does_not_refire_from_expired_seed(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(5, 4, ZoneInfo("UTC"), 0, 0, clock)
        assert governor.evaluate_warning() is True  # 4/5 = 80%
        clock.advance(2.0)
        governor.admit(PollFeed.ALERTS, "t")  # usage 1/5 in the new window
        governor.evaluate_warning()
        assert governor.snapshot().warning_fired is False


class TestDelayedHandoffReAdmission:
    """Delayed handoffs refresh their own window and honor its gates (F3)."""

    def test_delayed_handoff_readmitted_into_current_window(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(1, 0, ZoneInfo("UTC"), 0, 0, clock)
        first = governor.admit(PollFeed.ALERTS, "first")
        clock.advance(2.0)
        decision = governor.mark_handoff(first.token)
        assert decision.granted is True
        assert decision.gate_reason is GateReason.NONE
        assert governor.snapshot().charged == 1
        # The new window's single unit was consumed by the delayed handoff
        # itself; a second admission in that window is refused.
        second = governor.admit(PollFeed.VEHICLES, "second")
        assert not second.granted
        assert second.gate_reason is GateReason.QUOTA_EXHAUSTED

    def test_refused_handoff_aborts_without_transport_charge(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(1, 0, ZoneInfo("UTC"), 0, 0, clock)
        first = governor.admit(PollFeed.VEHICLES, "first")
        clock.advance(2.0)
        # Another worker consumed the new window before the delayed handoff.
        other = governor.admit(PollFeed.ALERTS, "other")
        assert other.granted
        governor.mark_handoff(other.token)
        decision = governor.mark_handoff(first.token)
        assert decision.granted is False
        assert decision.gate_reason is GateReason.QUOTA_EXHAUSTED
        assert first.token.state == "aborted"
        snap = governor.snapshot()
        assert snap.pending_admitted == 0
        assert snap.unused_admissions == 1
        assert snap.transport_handoffs == 1  # only the other feed's attempt
        assert snap.charged == 1
        assert governor.current_usage() == 1

    def test_refused_handoff_honors_rate_limit_gate(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        first = governor.admit(PollFeed.VEHICLES, "first")
        clock.advance(2.0)
        governor.declare_cooldown(
            PollFeed.VEHICLES, clock.mono() + 500.0, 0
        )
        decision = governor.mark_handoff(first.token)
        assert decision.granted is False
        assert decision.gate_reason is GateReason.RATE_LIMITED
        assert decision.next_deadline_utc_ms == 0
        assert first.token.state == "aborted"

    def test_same_window_handoff_granted(self):
        clock = FakeClock()
        governor = QuotaGovernor(2, 0, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "t")
        decision = governor.mark_handoff(result.token)
        assert decision.granted is True
        assert governor.snapshot().charged == 1


class TestRecoveryLowerBound:
    """Same-window recovery cannot discard known usage incl. the seed (F2)."""

    def test_lower_bound_includes_opening_seed(self):
        clock = FakeClock()
        governor = QuotaGovernor(100, 90, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "one")
        governor.mark_handoff(result.token)
        before = governor.current_usage()
        assert before == 91
        current = governor.snapshot().window_id
        with pytest.raises(LifecycleError):
            governor.reinitialize(1, current)
        assert governor.current_usage() == before
        epoch = governor.reinitialize(91, current)  # equal is fine
        assert epoch == 2
        assert governor.current_usage() == 91

    def test_runtime_operator_path_preserves_known_usage(self):
        # The operator-facing runtime reinitialize_quota path must reject
        # the same undercut estimates as the governor-level call.
        routes = FakeRoutes().on(
            "/ws/v2/vehicles", json_status(200, {"resultSet": {}})
        )
        runtime, clk = harness(routes, budget=100, initial_used=90)
        invoke_cycle(runtime, PollFeed.VEHICLES, clock=clk, routes=routes)
        usage = runtime._governor.current_usage()
        window_id = runtime.snapshot().quota.window_id
        with pytest.raises(LifecycleError):
            runtime.reinitialize_quota(1, window_id)
        assert runtime._governor.current_usage() == usage
        new_epoch = runtime.reinitialize_quota(usage, window_id)
        assert new_epoch == 2

    def test_cross_window_recovery_keeps_distinct_semantics(self):
        # After a legitimate rollover the seed has expired; a zero opening
        # estimate is honest there and the lower bound is zero charged.
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(100, 90, ZoneInfo("UTC"), 0, 0, clock)
        result = governor.admit(PollFeed.ALERTS, "one")
        governor.mark_handoff(result.token)
        clock.advance(2.0)
        current = governor.snapshot().window_id  # snapshot refreshes window
        epoch = governor.reinitialize(0, current)
        assert epoch == 2
        assert governor.current_usage() == 0


class TestUnrepresentableSnapshot:
    """Feed-local operator holds must stay telemetry-readable (F4)."""

    def test_local_hold_is_readable_and_immutable(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_unrepresentable(PollFeed.ALERTS)
        snap = governor.snapshot()
        assert snap.unrepresentable_scopes == ("alerts",)
        assert "alerts" in snap.unrepresentable_scopes
        # adding a second scope reorders and re-serializes fine
        governor.declare_unrepresentable(PollFeed.VEHICLES)
        governor.declare_unrepresentable(None)
        now = governor.snapshot()
        assert now.unrepresentable_scopes == ("alerts", "shared", "vehicles")
        assert isinstance(now.auth_blocked_feeds, tuple)

    def test_restart_of_same_window_after_resolution(self):
        clock = FakeClock()
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        governor.declare_unrepresentable(PollFeed.ALERTS)
        assert governor.snapshot().unrepresentable_scopes == ("alerts",)
        governor._clear_unrepresentable(PollFeed.ALERTS)
        assert governor.snapshot().unrepresentable_scopes == ()


class TestLifetimeReconciliation:
    """Per-event lifetime accounting: every event is counted exactly once,
    at rollover and under operator recovery alike (follow-up review F8)."""

    _EXPECTED_AT_ROLLOVER = {
        "charged_lifetime": 3,
        "transport_handoffs_lifetime": 2,
        "responses_received_lifetime": 1,
        "transport_failures_lifetime": 1,
        "feed_rejections_lifetime": 1,
        "deliveries_accepted_lifetime": 1,
        "retry_handoffs_lifetime": 1,
    }

    def test_one_admission_counts_once_across_rollover(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(5, 0, ZoneInfo("UTC"), 0, 0, clock)
        token = governor.admit(PollFeed.ALERTS, "before").token
        governor.mark_handoff(token)
        governor.record_response(token)
        assert governor.snapshot().lifetime_charged == 1
        clock.advance(2.0)  # cross the 00:00 UTC reset
        assert governor.snapshot().lifetime_charged == 1  # no second fold

    def test_each_event_counted_once_across_rollover(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        first = governor.admit(PollFeed.VEHICLES, "t")
        governor.mark_handoff(first.token)
        governor.record_response(first.token)
        governor.record_delivery(first.token)
        governor.record_feed_rejection(first.token)
        retry = governor.admit(PollFeed.ALERTS, "t", retry=True)
        governor.mark_handoff(retry.token)
        governor.record_transport_failure(retry.token)
        third = governor.admit(PollFeed.TRIPUPDATES, "t")  # still pending
        assert dict(governor._lifetime) == self._EXPECTED_AT_ROLLOVER
        clock.advance(2.0)  # cross the 00:00 UTC reset
        snap = governor.snapshot()
        assert snap.window_id != third.token.window_id
        assert snap.charged == 0
        assert snap.pending_admitted == 1
        assert dict(governor._lifetime) == self._EXPECTED_AT_ROLLOVER
        # The third admission is re-admitted into the new window and
        # charged exactly once more as its own event.
        decision = governor.mark_handoff(third.token)
        assert decision.granted is True
        assert governor.snapshot().charged == 1
        assert governor.snapshot().lifetime_charged == 4
        # A late completion for the second (now old-window, retry) token
        # records one additional lifetime event, never a window double
        # count: its delivery belongs to lifetime totals only.
        governor.record_delivery(retry.token)
        assert governor._lifetime["deliveries_accepted_lifetime"] == 2
        assert governor._lifetime["charged_lifetime"] == 4

    def test_recovery_keeps_lifetime_totals(self):
        clock = FakeClock()
        governor = QuotaGovernor(5, 0, ZoneInfo("UTC"), 0, 0, clock)
        token = governor.admit(PollFeed.ALERTS, "before").token
        governor.mark_handoff(token)
        current = governor.snapshot().window_id
        epoch = governor.reinitialize(1, current)
        assert epoch == 2
        assert governor.snapshot().lifetime_charged == 1  # no re-add

    def test_recovery_preserves_every_lifetime_counter(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        governor = QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, clock)
        first = governor.admit(PollFeed.VEHICLES, "t")
        governor.mark_handoff(first.token)
        governor.record_response(first.token)
        retry = governor.admit(PollFeed.ALERTS, "t", retry=True)
        governor.mark_handoff(retry.token)
        governor.record_transport_failure(retry.token)
        expected = dict(governor._lifetime)
        assert expected == {
            "charged_lifetime": 2,
            "transport_handoffs_lifetime": 2,
            "responses_received_lifetime": 1,
            "transport_failures_lifetime": 1,
            "feed_rejections_lifetime": 0,
            "deliveries_accepted_lifetime": 0,
            "retry_handoffs_lifetime": 1,
        }
        current = governor.snapshot().window_id
        governor.reinitialize(2, current)
        assert dict(governor._lifetime) == expected
