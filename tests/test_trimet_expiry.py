"""S13 atomic expiry, ingestion guard and counter tests (rows 6–11).

Store-level acceptance evidence, using the S11/S12 synthetic builders, the
deny-all socket guard and deterministic same-thread race injection (no
threads needed: the store's publication revalidation points are exercised
by interleaving a publication inside the expiry copy/filter window, which
runs outside the lock).  No sleeps, no real time, no network.

Covered rows:

- row 6: expiry threshold-1/equality/+1 across all relevance classes;
  removed IDs/counters/revision; unrelated state unchanged.
- row 7: equal/older cutoff idempotency; higher cutoff with zero removals;
  malformed/bool/out-of-range rejection; old snapshot unchanged; no store
  clock read.
- row 8: named regression ``test_expiry_state_survives_each_root_
  publication_path`` — all six baseline explicit root sites plus session
  replacement and the new cleanup path.
- row 9: cached/full/incremental/replay cannot reinsert a winner expired
  at C; retained-expired-at-T; C=None disabled; complete raw presence; no
  lower-rank fallback; genuinely eligible new expiry can return.
- row 10: exact counters per published mutation; counter-only metadata
  preserves content/receipt (named regression); no double counting;
  expiry frees capacity.
- row 11: deterministic cleanup races — rebuild from winner, three total
  attempts, exhausted operation publishes nothing; deferred cleanup
  assesses everything while filtering visibility (named regression).
"""
from __future__ import annotations

import time as _time_module

import pytest

from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    SHA_B,
    complete_evidence,
    incremental_delivery,
    make_trip_update_entity,
    open_state,
    s12_context,
    s12_world,
    tripupdates_delivery,
    vehicle_delivery,
    vehicle_position,
)
from tests._s13_helpers import (  # noqa: F401  (module-local autouse guard)
    FakeFreshnessClock,
    default_policy,
    s13_deny_all_sockets,
)
from backend.trimet.freshness import FreshnessController
from backend.trimet.freshness_errors import FreshnessInputError
from backend.trimet.freshness_types import (
    ClockAssessment,
    ClockStatus,
    ExpiryState,
    FreshnessReason,
    TimeSample,
    VehicleExpiryStatus,
)
from backend.trimet.live_state import LiveState
from backend.trimet.match_types import RelevanceClassification
from backend.trimet.poll_types import PollFeed, RequestPurpose
from backend.trimet.state_errors import StateCapacityError, StateConflictError
from backend.trimet.state_types import VehicleEntry

C = BASE_MS + 30_000  # the committed cutoff used throughout


def _activated_state(specs=None):
    """A store with one session, seeded vehicles and expiry activated at C."""
    state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
    if specs is None:
        specs = [
            (1001, BASE_MS, BASE_MS + 20_000),    # MAX, expires before C
            (1002, BASE_MS, BASE_MS + 120_000),   # MAX, unexpired
        ]
    session.accept(vehicle_delivery(
        [vehicle_position(vid, time_ms=t, expires_utc_ms=e)
         for vid, t, e in specs],
        query_time_ms=BASE_MS))
    state.expire_vehicles(C)
    return state, session


def _interleave_during_expire(monkeypatch, actions):
    """Publish one queued ``actions`` entry per expiry copy/filter window.

    Each hook fire consumes exactly one queued action.  The hook triggers
    only on the store's own ``_proxied_vehicles`` call from the expiry
    path; nested publications are marked re-entrant and skip the hook.
    Deterministic: no threads, no sleeps.
    """
    import backend.trimet.live_state as lsm

    real = lsm._proxied_vehicles
    inside = {"flag": False}

    def wrapper(mapping):
        converted = real(mapping)
        if inside["flag"] or not actions:
            return converted
        action = actions.pop(0)
        inside["flag"] = True
        try:
            action()
        finally:
            inside["flag"] = False
        return converted

    monkeypatch.setattr(lsm, "_proxied_vehicles", wrapper, raising=True)


# ---------------------------------------------------------------------------
# Row 6: expiry boundaries and untouched state
# ---------------------------------------------------------------------------


class TestExpiryBoundaries:
    def _specs_all_classes(self, *, expires_ms: int):
        # 1001 MAX (route 90), 1002 NON_MAX (bus route 1), 1003
        # UNCLASSIFIED (route absent from the world).
        return [
            (1001, BASE_MS, expires_ms),
            (1002, BASE_MS, expires_ms, 1),
            (1003, BASE_MS, expires_ms, 999),
        ]

    def _accept_all_classes(self, *, expires_ms: int, query_time_ms=BASE_MS):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        vehicles = [
            vehicle_position(1001, time_ms=BASE_MS, expires_utc_ms=expires_ms),
            vehicle_position(1002, time_ms=BASE_MS, expires_utc_ms=expires_ms,
                             route_number=1, vehicle_type="bus"),
            vehicle_position(1003, time_ms=BASE_MS, expires_utc_ms=expires_ms,
                             route_number=999),
        ]
        session.accept(vehicle_delivery(
            vehicles, query_time_ms=query_time_ms))
        snapshot = state.snapshot()
        # Verify the three S11 relevance classes are actually represented.
        classifications = {
            vid: entry.matched.classification
            for vid, entry in snapshot.vehicle_entries.items()
        }
        assert classifications[1001] is RelevanceClassification.MAX_RELEVANT
        assert classifications[1002] is RelevanceClassification.NON_MAX
        assert classifications[1003] is RelevanceClassification.UNCLASSIFIED
        return state, session, snapshot

    def test_threshold_minus_one_removes_nothing(self):
        state, _, _ = self._accept_all_classes(expires_ms=C + 1)
        result = state.expire_vehicles(C)
        assert result.changed is True  # cutoff activation is a mutation
        assert result.removed_vehicle_ids == ()
        snapshot = state.snapshot()
        assert set(snapshot.vehicle_entries) == {1001, 1002, 1003}
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.removed_entries_total == 0

    def test_equality_removes_every_relevance_class(self):
        state, _, _ = self._accept_all_classes(expires_ms=C)
        result = state.expire_vehicles(C)
        assert result.changed is True
        assert result.removed_vehicle_ids == (1001, 1002, 1003)
        snapshot = state.snapshot()
        assert set(snapshot.vehicle_entries) == set()
        assert snapshot.expiry_state.removed_entries_total == 3
        assert snapshot.max_vehicle_ids == ()
        # The MAX projection derives from retained entries: even though
        # the source batch still references the expired raw entries
        # (historical source evidence), the current projection is empty.
        assert snapshot.feed_views[
            PollFeed.VEHICLES
        ].source_content is not None

    def test_plus_one_boundary_on_fresh_store(self):
        state, _, _ = self._accept_all_classes(expires_ms=C)
        result = state.expire_vehicles(C + 1)
        assert result.removed_vehicle_ids == (1001, 1002, 1003)

    def test_unrelated_state_untouched_by_cleanup(self):
        state, session, before = self._accept_all_classes(expires_ms=C)
        state.expire_vehicles(C)
        after = state.snapshot()
        view_before = before.feed_views[PollFeed.VEHICLES]
        view_after = after.feed_views[PollFeed.VEHICLES]
        # Source/replay envelopes and high-water marks untouched.
        assert view_after.source_content is view_before.source_content
        assert (view_after.source_time_high_water_ms
                == view_before.source_time_high_water_ms)
        # Fence, session, generation, dataset identity untouched.
        assert after.absence_fence is before.absence_fence
        assert after.active_session_id == before.active_session_id
        assert after.generation == before.generation
        assert after.dataset_identity is before.dataset_identity
        # S12 decision telemetry unchanged (cleanup adds no decisions).
        assert dict(after.telemetry.reason_totals) == dict(
            before.telemetry.reason_totals
        )
        assert after.telemetry.samples == before.telemetry.samples


# ---------------------------------------------------------------------------
# Row 7: idempotency, malformed cutoffs, no clock reads
# ---------------------------------------------------------------------------


class TestExpiryIdempotencyAndValidation:
    def test_equal_cutoff_is_idempotent_noop(self):
        state, _ = _activated_state()
        before = state.snapshot()
        result = state.expire_vehicles(C)
        assert result.changed is False
        assert result.effective_cutoff_utc_ms == C
        assert result.revision_before == before.revision
        assert result.revision_after == before.revision
        assert result.removed_vehicle_ids == ()
        after = state.snapshot()
        assert after.revision == before.revision
        assert after.expiry_state.cutoff_utc_ms == C
        assert (after.expiry_state.removed_entries_total
                == before.expiry_state.removed_entries_total)

    def test_older_cutoff_never_lowers_or_revives(self):
        state, _ = _activated_state()
        result = state.expire_vehicles(BASE_MS)
        assert result.changed is False
        assert result.effective_cutoff_utc_ms == C
        assert state.snapshot().expiry_state.cutoff_utc_ms == C

    def test_higher_cutoff_zero_removals_bumps_revision_once(self):
        state, _ = _activated_state()
        before = state.snapshot()
        result = state.expire_vehicles(C + 5_000)
        assert result.changed is True
        assert result.removed_vehicle_ids == ()
        assert result.revision_after == result.revision_before + 1
        assert state.snapshot().expiry_state.cutoff_utc_ms == C + 5_000

    def test_malformed_cutoffs_rejected_before_mutation(self):
        state, _ = _activated_state()
        before = state.snapshot()
        for bad in (True, False, "abc", 1.5, None, 9e15):
            with pytest.raises(FreshnessInputError) as excinfo:
                state.expire_vehicles(bad)  # type: ignore[arg-type]
            assert excinfo.value.code == "invalid_cutoff"
        after = state.snapshot()
        assert after.revision == before.revision
        assert after.expiry_state == before.expiry_state

    def test_store_reads_no_clock_during_expiry(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 20_000)],
            query_time_ms=BASE_MS))

        def forbidden(*args: object, **kwargs: object) -> object:
            raise AssertionError("the store must not read a clock")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(_time_module, "time", forbidden)
            patch.setattr(_time_module, "monotonic", forbidden)
            patch.setattr(_time_module, "monotonic_ns", forbidden)
            state.expire_vehicles(C)
        assert state.snapshot().expiry_state.cutoff_utc_ms == C

    def test_old_snapshot_unchanged_after_expiry(self):
        state, _ = _activated_state()
        old = state.snapshot()
        state.expire_vehicles(C + 60_000)
        # The previously captured snapshot never changes.
        assert old.expiry_state.cutoff_utc_ms == C
        assert set(old.vehicle_entries) == {1002}
        fresh = state.snapshot()
        assert fresh is not old
        assert fresh.revision > old.revision


# ---------------------------------------------------------------------------
# Row 8: expiry state survives every root publication path (named)
# ---------------------------------------------------------------------------


class TestRootSitePropagation:
    def test_expiry_state_survives_each_root_publication_path(self):
        # Site 1: initial construction — expiry disabled.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        assert state.snapshot().expiry_state is None
        # Baseline vehicle data publication with expiry disabled.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS))
        assert state.snapshot().expiry_state is None
        # New expiry publication path: first activation.
        state.expire_vehicles(C)
        assert state.snapshot().expiry_state.cutoff_utc_ms == C

        # Site 2: vehicle data publication (newer queryTime, committed).
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 10_000,
                              expires_utc_ms=BASE_MS + 150_000)],
            query_time_ms=BASE_MS + 10_000, sequence=2))
        snapshot = state.snapshot()
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.removed_entries_total == 0

        # Site 3: vehicle metadata-only publication (equal queryTime,
        # newer observation, expired winner → suppression counter rides).
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 11_000,
                              expires_utc_ms=BASE_MS + 20_000)],
            query_time_ms=BASE_MS + 10_000, sequence=3))
        snapshot = state.snapshot()
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.ignored_expired_winners_total == 1

        # Sites 4/5: secondary equal-time metadata then newer content.
        session.accept(tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A", route_id="90")],
            header_timestamp_ms=BASE_MS + 5_000, sequence=1))
        session.accept(tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A", route_id="90")],
            header_timestamp_ms=BASE_MS + 5_000, sequence=2))
        snapshot = state.snapshot()
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        session.accept(tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A", route_id="90")],
            header_timestamp_ms=BASE_MS + 6_000, sequence=3))
        snapshot = state.snapshot()
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.ignored_expired_winners_total == 1

        # Site 6: static-context replacement (re-match, new generation).
        swap = state.replace_context(
            s12_context(s12_world(), sha256=SHA_B),
            expected_generation=state.snapshot().generation,
        )
        assert swap.changed is True
        snapshot = state.snapshot()
        assert snapshot.generation == swap.generation_after
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        # Re-matched retained observations survive with their expiry.
        assert set(snapshot.vehicle_entries) == {1001}

        # Session replacement: close + open (dataclasses.replace paths).
        state.close_session(session.session_id)
        session2 = state.open_session()
        snapshot = state.snapshot()
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        assert set(snapshot.vehicle_entries) == {1001}

        # The new session's first publication also preserves everything.
        session2.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 20_000,
                              expires_utc_ms=BASE_MS + 180_000)],
            query_time_ms=BASE_MS + 20_000, sequence=4))
        snapshot = state.snapshot()
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.expiry_state.ignored_expired_winners_total == 1

    def test_counters_survive_new_expiry_publication(self):
        # An ignored-winner counter accumulated before a cleanup must not
        # be reset by the cleanup publication itself.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS))
        state.expire_vehicles(C)
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2))
        assert state.snapshot().expiry_state.ignored_expired_winners_total == 1
        state.expire_vehicles(C + 60_000)
        snapshot = state.snapshot()
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        assert snapshot.expiry_state.cutoff_utc_ms == C + 60_000


# ---------------------------------------------------------------------------
# Row 9: ingestion guard against committed cutoff C
# ---------------------------------------------------------------------------


class TestIngestionGuard:
    def test_cached_reinsertion_of_expired_winner_suppressed(self):
        state, session = _activated_state()
        # Cached observation: same raw content, newer queryTime/sequence.
        result = session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 20_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2))
        snapshot = state.snapshot()
        assert set(snapshot.vehicle_entries) == {1002}
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        assert snapshot.revision == result.revision

    def test_full_snapshot_cannot_reinsert_expired_id(self):
        state, session = _activated_state()
        before = state.snapshot()
        # Authoritative full snapshot containing the expired cached ID.
        result = session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 20_000),
             vehicle_position(1002, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2,
            evidence=complete_evidence(2)))
        snapshot = state.snapshot()
        assert set(snapshot.vehicle_entries) == {1002}
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        assert snapshot.revision == before.revision + 1

    def test_incremental_delivery_cannot_reinsert_expired_id(self):
        state, session = _activated_state()
        session.accept(incremental_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 20_000)],
            query_time_ms=BASE_MS + 1_000, since_ms=BASE_MS, sequence=2))
        snapshot = state.snapshot()
        assert set(snapshot.vehicle_entries) == {1002}
        assert snapshot.expiry_state.ignored_expired_winners_total == 1

    def test_exact_replay_counts_nothing(self):
        state, session = _activated_state()
        delivery = vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 20_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2)
        session.accept(delivery)  # suppressed
        assert state.snapshot().expiry_state.ignored_expired_winners_total == 1
        result = session.accept(delivery)  # exact replay
        assert result.decision.name == "DUPLICATE_NOOP"
        assert state.snapshot().expiry_state.ignored_expired_winners_total == 1

    def test_guard_uses_committed_cutoff_not_deferred_evaluation_time(self):
        # Named regression (row 9).  C = BASE_MS + 30_000 is committed;
        # a later evaluation sample T = BASE_MS + 45_000 is NOT a guard.
        state, session = _activated_state()
        # A winner with C < expires <= T is eligible at ingestion (guard
        # uses C only) and retained under S12 until cleanup runs.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 40_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2))
        snapshot = state.snapshot()
        assert 1001 in snapshot.vehicle_entries  # C < 40_000: eligible
        assert snapshot.expiry_state.ignored_expired_winners_total == 0
        # A winner with expires <= C is suppressed even though the
        # evaluation time T has moved far past it.
        session.accept(vehicle_delivery(
            [vehicle_position(1002, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS + 1_000, sequence=3))
        snapshot = state.snapshot()
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        # No pending T-based cutoff exists: the cutoff is still C.
        assert snapshot.expiry_state.cutoff_utc_ms == C

    def test_c_none_disables_the_guard(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        # No cleanup ever ran: a winner already expired relative to any
        # future evaluation T is still retained (expiry history unknown).
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS - 5_000)],
            query_time_ms=BASE_MS))
        snapshot = state.snapshot()
        assert 1001 in snapshot.vehicle_entries
        assert snapshot.expiry_state is None

    def test_full_snapshot_raw_presence_survives_suppression(self):
        # Raw presence/completeness accounting happens before suppression:
        # the authoritative fence records the suppressed ID's presence, so
        # a cached follow-up is fence-eligible (then guard-suppressed),
        # never parser-rejected and never a missing ID.
        state, session = _activated_state()
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 20_000),
             vehicle_position(1002, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2,
            evidence=complete_evidence(2)))
        fence = state.snapshot().absence_fence
        assert fence is not None
        assert 1001 in fence.seen_ids  # raw presence preserved

    def test_no_lower_rank_fallback_for_suppressed_winner(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        # Retain 1001 unexpired (expiry activated at C removes nothing).
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS))
        state.expire_vehicles(C)
        retained_before = state.snapshot().vehicle_entries[1001]
        # A duplicate group carries a newer expired observation and an
        # older unexpired one; the winner (newest rank) is suppressed and
        # the lower-ranked record is NOT promoted.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 5_000,
                              expires_utc_ms=BASE_MS + 10_000),
             vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2))
        snapshot = state.snapshot()
        assert snapshot.vehicle_entries[1001] is retained_before
        assert snapshot.expiry_state.ignored_expired_winners_total == 1

    def test_genuinely_eligible_new_expiry_can_return(self):
        state, session = _activated_state()
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2))
        snapshot = state.snapshot()
        assert set(snapshot.vehicle_entries) == {1001, 1002}
        assert snapshot.expiry_state.ignored_expired_winners_total == 0


# ---------------------------------------------------------------------------
# Row 10: counter semantics per published mutation
# ---------------------------------------------------------------------------


class TestCounterSemantics:
    def test_suppressed_winner_metadata_only_counts_once_without_freshening_content(self):
        # Named regression (row 10).
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        seed = vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS)
        session.accept(seed)
        state.expire_vehicles(C)
        before = state.snapshot()
        source_before = before.feed_views[PollFeed.VEHICLES].source_content

        # Equal queryTime, newer observation, expired at C: the guard
        # suppresses the winner, no observation changes, and the
        # counter-only effect publishes METADATA_ONLY with one bump.
        suppressed = vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS, sequence=2)
        result = session.accept(suppressed)
        assert result.decision.name == "METADATA_ONLY"
        after = state.snapshot()
        assert after.expiry_state.ignored_expired_winners_total == 1
        assert after.revision == before.revision + 1

        # The source content (and its receipt) is NOT freshened by the
        # counter-only publication: the original envelope object survives.
        source_after = after.feed_views[PollFeed.VEHICLES].source_content
        assert source_after is source_before
        assert source_after.delivery.sequence == seed.sequence

        # Follow-ups count nothing further:
        # - exact replay of the suppressed delivery,
        assert session.accept(suppressed).decision.name == "DUPLICATE_NOOP"
        # - an older-source delivery carrying expired winners,
        old = session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 2_000,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS - 1, sequence=3))
        assert old.decision.name == "IGNORED_OLDER"
        assert state.snapshot().expiry_state.ignored_expired_winners_total == 1
        assert state.snapshot().revision == after.revision

        # - a discarded (capacity-exceeded) candidate on a store whose
        #   expiry is already active: nothing publishes, nothing counts.
        state2, session2 = open_state(
            s12_context(s12_world(), sha256=SHA_A),
            max_vehicle_entries=1)
        session2.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS))
        state2.expire_vehicles(C)
        session2.accept(vehicle_delivery(
            [vehicle_position(1002, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2))
        assert (state2.snapshot().expiry_state.ignored_expired_winners_total
                == 1)  # the suppressed 1002 winner counted once
        with pytest.raises(StateCapacityError):
            session2.accept(vehicle_delivery(
                [vehicle_position(1003, time_ms=BASE_MS + 2_000,
                                  expires_utc_ms=BASE_MS + 120_000)],
                query_time_ms=BASE_MS + 2_000, sequence=3))
        assert (state2.snapshot().expiry_state.ignored_expired_winners_total
                == 1)  # the discarded candidate counted nothing

    def test_committed_publication_counts_once(self):
        state, session = _activated_state()
        before = state.snapshot()
        result = session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 10_000),
             vehicle_position(1002, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 1_000, sequence=2))
        assert result.decision.name == "COMMITTED"
        snapshot = state.snapshot()
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        assert snapshot.revision == before.revision + 1
        # Two suppressed IDs in one delivery count twice (per ID).
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 2_000,
                              expires_utc_ms=BASE_MS + 10_000),
             vehicle_position(1002, time_ms=BASE_MS + 2_000,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS + 2_000, sequence=3))
        snapshot = state.snapshot()
        assert snapshot.expiry_state.ignored_expired_winners_total == 3

    def test_no_double_count_across_publication_retry(self):
        state, session = _activated_state()
        # An interleave publication between the suppressed delivery's
        # capture and revalidation forces a reduce rebuild; the final
        # candidate must count the suppression exactly once.
        interleave = vehicle_delivery(
            [vehicle_position(1003, time_ms=BASE_MS + 500,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 500, sequence=2)
        suppressed = vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 10_000)],
            query_time_ms=BASE_MS + 1_000, sequence=3)
        with pytest.MonkeyPatch.context() as patch:
            import backend.trimet.live_state as lsm

            real_match = lsm.match_delivery
            fired = {"once": False}

            def match_wrapper(delivery, context):
                if not fired["once"]:
                    fired["once"] = True
                    session.accept(interleave)
                return real_match(delivery, context)

            patch.setattr(lsm, "match_delivery", match_wrapper, raising=True)
            session.accept(suppressed)
        snapshot = state.snapshot()
        assert snapshot.expiry_state.ignored_expired_winners_total == 1
        assert set(snapshot.vehicle_entries) == {1002, 1003}

    def test_expiry_frees_retained_capacity(self):
        state, session = open_state(
            s12_context(s12_world(), sha256=SHA_A),
            max_vehicle_entries=2)
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 10_000),
             vehicle_position(1002, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS))
        with pytest.raises(StateCapacityError):
            session.accept(vehicle_delivery(
                [vehicle_position(1003, time_ms=BASE_MS + 1_000,
                                  expires_utc_ms=BASE_MS + 120_000)],
                query_time_ms=BASE_MS + 1_000, sequence=2))
        state.expire_vehicles(C)  # removes 1001
        result = session.accept(vehicle_delivery(
            [vehicle_position(1003, time_ms=BASE_MS + 1_000,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 1_000, sequence=3))
        assert result.decision.name == "COMMITTED"
        assert set(state.snapshot().vehicle_entries) == {1002, 1003}

    def test_no_tombstone_history_and_no_lifetime_readd(self):
        state, _ = _activated_state()
        result = state.expire_vehicles(C)  # idempotent re-request
        # No per-ID tombstone history exists anywhere on the result.
        assert result.removed_vehicle_ids == ()
        assert not hasattr(result, "tombstones")
        # A lower cutoff cannot re-add or re-count anything.
        assert state.snapshot().expiry_state.removed_entries_total == 1


# ---------------------------------------------------------------------------
# Row 11: deterministic cleanup races and deferred views
# ---------------------------------------------------------------------------


class TestExpiryRaces:
    def _racing_setup(self):
        """Store seeded with an expiring vehicle and three queued updates."""
        state, session = _activated_state()
        deliveries = [
            vehicle_delivery(
                [vehicle_position(1003, time_ms=BASE_MS + offset,
                                  expires_utc_ms=BASE_MS + 200_000)],
                query_time_ms=BASE_MS + offset, sequence=2 + index)
            for index, offset in enumerate((500, 1_500, 2_500))
        ]
        return state, session, deliveries

    def test_rebuild_from_winner_after_interleaved_publication(self):
        state, session, deliveries = self._racing_setup()
        with pytest.MonkeyPatch.context() as patch:
            _interleave_during_expire(
                patch, [lambda: session.accept(deliveries[0])])
            result = state.expire_vehicles(C + 60_000)
        assert result.changed is True
        assert result.removed_vehicle_ids == ()
        snapshot = state.snapshot()
        # The winning root (with 1003) was rebuilt from, not lost:
        assert set(snapshot.vehicle_entries) == {1002, 1003}
        assert snapshot.expiry_state.cutoff_utc_ms == C + 60_000
        assert snapshot.revision == result.revision_after

    def test_three_total_attempts_then_success(self):
        state, session, deliveries = self._racing_setup()
        with pytest.MonkeyPatch.context() as patch:
            _interleave_during_expire(
                patch,
                [lambda: session.accept(deliveries[0]),
                 lambda: session.accept(deliveries[1])],
                )
            result = state.expire_vehicles(C + 60_000)
        assert result.changed is True
        entries = set(state.snapshot().vehicle_entries)
        # Both interleaved publications survived the rebuilds (both carry
        # newer observations of ID 1003); the expiry succeeded on the
        # third total attempt.
        assert entries == {1002, 1003}

    def test_exhausted_attempts_publish_nothing_partial(self):
        state, session, deliveries = self._racing_setup()
        revision_before = state.snapshot().revision
        with pytest.MonkeyPatch.context() as patch:
            _interleave_during_expire(
                patch,
                [lambda: session.accept(deliveries[0]),
                 lambda: session.accept(deliveries[1]),
                 lambda: session.accept(deliveries[2])],
                )
            with pytest.raises(StateConflictError) as excinfo:
                state.expire_vehicles(C + 60_000)
            assert excinfo.value.code == "concurrent_change"
        snapshot = state.snapshot()
        # The winning root is intact: all three interleaved publications
        # exist (three revision bumps of ID 1003's observation), the
        # cutoff was NOT advanced and nothing was removed.
        assert set(snapshot.vehicle_entries) == {1002, 1003}
        assert snapshot.expiry_state.cutoff_utc_ms == C
        assert snapshot.revision == revision_before + 3

    def _deferred_controller_state(self):
        """Store with all three relevance classes, some expired at T."""
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        vehicles = [
            vehicle_position(1001, time_ms=BASE_MS,
                             expires_utc_ms=BASE_MS + 10_000),        # MAX expired
            vehicle_position(1002, time_ms=BASE_MS,
                             expires_utc_ms=BASE_MS + 120_000),       # MAX kept
            vehicle_position(1003, time_ms=BASE_MS, route_number=1,
                             vehicle_type="bus",
                             expires_utc_ms=BASE_MS + 120_000),       # NON_MAX
            vehicle_position(1004, time_ms=BASE_MS, route_number=999,
                             expires_utc_ms=BASE_MS + 10_000),        # UNCLASS. expired
        ]
        session.accept(vehicle_delivery(vehicles, query_time_ms=BASE_MS))
        snapshot = state.snapshot()
        classifications = {
            vid: entry.matched.classification
            for vid, entry in snapshot.vehicle_entries.items()
        }
        assert classifications == {
            1001: RelevanceClassification.MAX_RELEVANT,
            1002: RelevanceClassification.MAX_RELEVANT,
            1003: RelevanceClassification.NON_MAX,
            1004: RelevanceClassification.UNCLASSIFIED,
        }
        return state, snapshot

    def test_deferred_cleanup_assesses_all_retained_entries_but_filters_visible_max(self):
        # Named regression (row 11/§10): cleanup loses the store race, is
        # deferred, and the trusted derived view still filters expired
        # entries from visible IDs without claiming they were removed.
        state, _ = self._deferred_controller_state()
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
        assert result.sample_utc_ms == BASE_MS + 45_000
        assert FreshnessReason.CLEANUP_DEFERRED in result.top_reasons
        assert result.expiry_result is not None
        assert result.expiry_result.changed is False
        assert result.expiry_result.removed_vehicle_ids == ()
        # Every retained ID across every relevance class is assessed,
        # including the expired entries the deferred cleanup would remove.
        assert set(result.vehicle_assessments) == {1001, 1002, 1003, 1004}
        assert (result.vehicle_assessments[1001].expiry_status
                is VehicleExpiryStatus.EXPIRED)
        assert (result.vehicle_assessments[1002].expiry_status
                is VehicleExpiryStatus.UNEXPIRED)
        assert (result.vehicle_assessments[1004].expiry_status
                is VehicleExpiryStatus.EXPIRED)
        # Assessment is not visibility: only unexpired MAX IDs are visible.
        assert result.visible_max_vehicle_ids == (1002,)
        # Nothing was actually removed from the store.
        assert set(state.snapshot().vehicle_entries) == {
            1001, 1002, 1003, 1004}

    def test_successful_removal_reported_separately_from_filtering(self):
        state, _ = self._deferred_controller_state()
        clock = FakeFreshnessClock(utc_ms=BASE_MS + 45_000,
                                   monotonic_ms=1_000_000)
        controller = FreshnessController(
            state, policy=default_policy(), clock=clock)
        result = controller.refresh()
        assert result.expiry_result is not None
        assert result.expiry_result.changed is True
        assert result.expiry_result.removed_vehicle_ids == (1001, 1004)
        # Removed IDs are gone from both the store and the assessment map.
        assert set(result.vehicle_assessments) == {1002, 1003}
        assert result.visible_max_vehicle_ids == (1002,)
        assert set(state.snapshot().vehicle_entries) == {1002, 1003}
        assert state.snapshot().expiry_state.removed_entries_total == 2


def result_revision(state: LiveState, revision):
    return state.snapshot().revision
