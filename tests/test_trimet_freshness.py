"""S13 pure evaluator tests: ages, quality, visibility (rows 2–5, 12, 15, 16).

``evaluate_freshness`` is pure: it reads one immutable store snapshot plus
caller-supplied time inputs.  These tests use integer fake samples, the
S11/S12 synthetic builders and the deny-all socket guard — no real GTFS,
credentials, config loading, HTTP or clocks.

Covered rows:

- row 2: default 30/180/360 s and custom 3× threshold-1/equality/+1;
  source time (not receipt/replay) is the staleness basis.
- row 3: new-store UNKNOWN versus accepted-empty FRESH; collection
  quality attributed solely to the accepted source envelope's evidence.
- row 4: distinct source/content-receipt/replay/position/request metrics;
  session replacement clears replay; equal-time metadata, conflict and
  replay cannot freshen content; no invented API latency.
- row 5: position age is a measurement only (no position-old flag or
  warning); stale unexpired MAX stays available despite enrichment gaps.
- row 12: future-timestamp boundaries; unknown clock never freshens.
- row 15: untrusted view retains everything with unknown statuses;
  sample-before-cutoff outcome.
- row 16: pure evaluation — no store mutation, no clock reads.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    alerts_delivery,
    complete_evidence,
    make_alert,
    make_trip_update_entity,
    open_state,
    route_child,
    s12_context,
    s12_world,
    tripupdates_delivery,
    vehicle_delivery,
    vehicle_position,
)
from tests._s13_helpers import (  # noqa: F401  (module-local autouse guard)
    default_policy,
    policy_with,
    s13_deny_all_sockets,
)
from backend.trimet.freshness import evaluate_freshness
from backend.trimet.freshness_errors import FreshnessInputError
from backend.trimet.freshness_types import (
    ClockAssessment,
    ClockStatus,
    CollectionQuality,
    FeedFreshnessStatus,
    FreshnessReason,
    PositionTimeQuality,
    TimeSample,
    VehicleExpiryStatus,
)
from backend.trimet.poll_types import PollFeed

TRUSTED = ClockAssessment(status=ClockStatus.TRUSTED, epoch=1, reasons=())
UNTRUSTED = ClockAssessment(
    status=ClockStatus.UNTRUSTED, epoch=1,
    reasons=(FreshnessReason.CLOCK_DIVERGED,),
)


def _trusted_eval(state, *, utc_ms: int, policy=None, snapshot=None):
    policy = policy if policy is not None else default_policy()
    snapshot = snapshot if snapshot is not None else state.snapshot()
    return evaluate_freshness(
        snapshot,
        sample=TimeSample(utc_ms=utc_ms, monotonic_ms=1_000_000),
        policy=policy,
        clock_assessment=TRUSTED,
    )


def _seed_one(state, session, *, query_time_ms: int = BASE_MS,
              expires_ms: int | None = None, evidence=None):
    expires_ms = (
        query_time_ms + 120_000 if expires_ms is None else expires_ms
    )
    session.accept(
        vehicle_delivery(
            [vehicle_position(1001, time_ms=query_time_ms,
                              expires_utc_ms=expires_ms)],
            query_time_ms=query_time_ms,
            evidence=evidence,
        )
    )
    return state.snapshot()


# ---------------------------------------------------------------------------
# Row 2: 3× interval staleness, threshold-1/equality/+1, source-time basis
# ---------------------------------------------------------------------------


class TestThresholdBoundaries:
    def test_default_intervals_30_180_360_seconds(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(alerts_delivery(
            [make_alert(1, routes=(route_child("90"),))],
            query_time_ms=BASE_MS))
        session.accept(tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A", route_id="90")],
            header_timestamp_ms=BASE_MS))
        _seed_one(state, session)
        policy = default_policy()
        snapshot = state.snapshot()
        result = _trusted_eval(state, utc_ms=BASE_MS, policy=policy,
                               snapshot=snapshot)
        assert result.feed_assessments[PollFeed.VEHICLES].threshold_ms == 30000
        assert result.feed_assessments[PollFeed.TRIPUPDATES].threshold_ms == 180000
        assert result.feed_assessments[PollFeed.ALERTS].threshold_ms == 360000

    def test_default_boundary_minus_one_equal_plus_one(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session, query_time_ms=BASE_MS)
        policy = default_policy()  # vehicles threshold = 30000 ms
        for utc_ms, expected in (
            (BASE_MS + 29_999, FeedFreshnessStatus.FRESH),
            (BASE_MS + 30_000, FeedFreshnessStatus.FRESH),   # equality fresh
            (BASE_MS + 30_001, FeedFreshnessStatus.STALE),
        ):
            result = _trusted_eval(state, utc_ms=utc_ms, policy=policy)
            view = result.feed_assessments[PollFeed.VEHICLES]
            assert view.status is expected, utc_ms
            assert view.source_age_ms == utc_ms - BASE_MS
            assert view.threshold_ms == 30000

    def test_custom_interval_boundary(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session, query_time_ms=BASE_MS)
        policy = policy_with(vehicles_poll_ms=5000)  # threshold 15000
        for utc_ms, expected in (
            (BASE_MS + 14_999, FeedFreshnessStatus.FRESH),
            (BASE_MS + 15_000, FeedFreshnessStatus.FRESH),
            (BASE_MS + 15_001, FeedFreshnessStatus.STALE),
        ):
            result = _trusted_eval(state, utc_ms=utc_ms, policy=policy)
            assert result.feed_assessments[
                PollFeed.VEHICLES
            ].status is expected, utc_ms

    def test_tripupdates_and_alerts_use_their_own_thresholds(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(alerts_delivery(
            [make_alert(1, routes=(route_child("90"),))],
            query_time_ms=BASE_MS))
        session.accept(tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A", route_id="90")],
            header_timestamp_ms=BASE_MS))
        policy = default_policy()
        # Alerts: 3 × 120 s = 360 s threshold; +1 ms is stale.
        result = _trusted_eval(state, utc_ms=BASE_MS + 360_001, policy=policy)
        assert result.feed_assessments[PollFeed.ALERTS].status is FeedFreshnessStatus.STALE
        # TripUpdates: 3 × 60 s = 180 s; at exactly 180 s it is fresh.
        result = _trusted_eval(state, utc_ms=BASE_MS + 180_000, policy=policy)
        assert result.feed_assessments[PollFeed.TRIPUPDATES].status is FeedFreshnessStatus.FRESH

    def test_source_time_not_receipt_or_replay_is_the_basis(self):
        # Source queryTime stays old while the receipt/replay are recent:
        # the staleness classification must follow the source time.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS,
            receipt_ms=BASE_MS + 150,
        ))
        policy = policy_with(vehicles_poll_ms=1000)  # threshold 3000 ms
        # Age 5000 ms from source time → stale, although the receipt is
        # only ~4850 ms old and the replay (booked at the same delivery)
        # matches the receipt.
        result = _trusted_eval(state, utc_ms=BASE_MS + 5000, policy=policy)
        view = result.feed_assessments[PollFeed.VEHICLES]
        assert view.status is FeedFreshnessStatus.STALE
        assert view.source_age_ms == 5000


# ---------------------------------------------------------------------------
# Row 3: new store UNKNOWN, accepted-empty FRESH, feed-level quality
# ---------------------------------------------------------------------------


class TestEmptyAndQuality:
    def test_new_store_is_unknown_with_no_source_content(self):
        state, _ = open_state(s12_context(s12_world(), sha256=SHA_A))
        result = _trusted_eval(state, utc_ms=BASE_MS)
        view = result.feed_assessments[PollFeed.VEHICLES]
        assert view.status is FeedFreshnessStatus.UNKNOWN
        assert view.source_age_ms is None
        assert view.reasons == (FreshnessReason.NO_SOURCE_CONTENT,)
        assert view.collection_quality is CollectionQuality.UNKNOWN
        assert result.vehicle_assessments == {}
        assert result.visible_max_vehicle_ids == ()

    def test_accepted_empty_collection_is_fresh(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [], query_time_ms=BASE_MS, evidence=complete_evidence(0)))
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        view = result.feed_assessments[PollFeed.VEHICLES]
        # Zero MAX vehicles does not imply an unknown source or an empty
        # fleet: a timestamped accepted empty collection is FRESH.
        assert view.status is FeedFreshnessStatus.FRESH
        assert view.collection_quality is CollectionQuality.COMPLETE
        assert result.visible_max_vehicle_ids == ()

    def test_collection_quality_complete_partial_unknown(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        # Complete: rejected == 0 and raw_count == parsed.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS)],
            query_time_ms=BASE_MS, evidence=complete_evidence(1)))
        result = _trusted_eval(state, utc_ms=BASE_MS)
        assert result.feed_assessments[
            PollFeed.VEHICLES
        ].collection_quality is CollectionQuality.COMPLETE

        # Partial: rejected > 0 (raw_count = parsed + rejected).
        session2 = None
        state2, session2 = open_state(
            s12_context(s12_world(), sha256=SHA_A))
        from tests._s12_helpers import complete_evidence as ce
        from backend.trimet.poll_types import VehicleCollectionEvidence
        session2.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS)],
            query_time_ms=BASE_MS,
            evidence=VehicleCollectionEvidence(
                key_present=True, is_list=True, raw_count=3, rejected_count=2),
        ))
        result2 = _trusted_eval(state2, utc_ms=BASE_MS)
        assert result2.feed_assessments[
            PollFeed.VEHICLES
        ].collection_quality is CollectionQuality.PARTIAL

        # Unknown: evidence absent on an accepted collection.
        state3, session3 = open_state(
            s12_context(s12_world(), sha256=SHA_A))
        session3.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS)],
            query_time_ms=BASE_MS, evidence=None))
        result3 = _trusted_eval(state3, utc_ms=BASE_MS)
        assert result3.feed_assessments[
            PollFeed.VEHICLES
        ].collection_quality is CollectionQuality.UNKNOWN

    def test_quality_is_feed_level_not_per_vehicle(self):
        # Two vehicles retained from the same accepted envelope: the
        # quality label lives on the feed assessment only; per-vehicle
        # assessments carry no collection-quality field, no per-vehicle
        # evidence fabrication and no authority implication.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS),
             vehicle_position(1002, time_ms=BASE_MS, route_number=1)],
            query_time_ms=BASE_MS, evidence=complete_evidence(2)))
        result = _trusted_eval(state, utc_ms=BASE_MS)
        assert result.feed_assessments[
            PollFeed.VEHICLES
        ].collection_quality is CollectionQuality.COMPLETE
        for assessment in result.vehicle_assessments.values():
            assert not hasattr(assessment, "collection_quality")
        # Quality never creates reconciliation authority by itself: the
        # fence above (if any) came from S12's full-snapshot gates, not
        # from the quality label, and both vehicles stay retained.
        assert set(state.snapshot().vehicle_entries) == {1001, 1002}

    def test_quality_comes_from_each_envelopes_own_evidence(self):
        # Named regression support (row 5/§5): vehicles retained from
        # *different* source collections are not labelled with the newest
        # collection's quality.  The newest accepted envelope is partial;
        # the retained older observation keeps its own stamp.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS)],
            query_time_ms=BASE_MS, evidence=complete_evidence(1)))
        from backend.trimet.poll_types import VehicleCollectionEvidence
        session.accept(vehicle_delivery(
            [vehicle_position(1002, time_ms=BASE_MS + 1000)],
            query_time_ms=BASE_MS + 1000, sequence=2,
            evidence=VehicleCollectionEvidence(
                key_present=True, is_list=True, raw_count=5, rejected_count=4),
        ))
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        # The feed-level label is the *accepted source envelope's* (newest
        # collection): partial.  Both vehicles remain individually assessed.
        assert result.feed_assessments[
            PollFeed.VEHICLES
        ].collection_quality is CollectionQuality.PARTIAL
        assert set(result.vehicle_assessments) == {1001, 1002}


# ---------------------------------------------------------------------------
# Row 4: distinct metrics; replay clearing; no freshening
# ---------------------------------------------------------------------------


class TestDistinctMetrics:
    def test_source_receipt_replay_position_request_distinguished(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        # queryTime = BASE_MS, receipt = BASE_MS + 500 (explicit), elapsed 133.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS - 2000,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS,
            receipt_ms=BASE_MS + 500,
        ))
        eval_utc = BASE_MS + 10_000
        result = _trusted_eval(state, utc_ms=eval_utc)
        view = result.feed_assessments[PollFeed.VEHICLES]
        # Source age from queryTime; content receipt from the delivery's
        # receipt; replay age from the booked replay envelope's receipt.
        assert view.source_age_ms == 10_000
        assert view.source_content_receipt_age_ms == 10_000 - 500
        assert view.last_booked_delivery_age_ms == 10_000 - 500
        assert view.request_elapsed_ms == 133
        # Position age measured from the raw position time, distinct from
        # every source-side metric.
        vehicle = result.vehicle_assessments[1001]
        assert vehicle.position_age_ms == 12_000
        assert vehicle.position_time_quality is PositionTimeQuality.KNOWN

    def test_position_age_differs_from_source_age(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS - 45_000,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS))
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        view = result.feed_assessments[PollFeed.VEHICLES]
        vehicle = result.vehicle_assessments[1001]
        # Fresh source (queryTime age 1000), old position (age 46_000):
        # both measurements are reported independently.
        assert view.status is FeedFreshnessStatus.FRESH
        assert view.source_age_ms == 1000
        assert vehicle.position_age_ms == 46_000
        assert vehicle.position_time_quality is PositionTimeQuality.KNOWN
        assert vehicle.expiry_status is VehicleExpiryStatus.UNEXPIRED

    def test_session_replacement_clears_replay_not_content(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)
        before = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        assert before.feed_assessments[
            PollFeed.VEHICLES
        ].last_booked_delivery_age_ms is not None
        state.close_session(session.session_id)
        state.open_session()
        after = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        view = after.feed_assessments[PollFeed.VEHICLES]
        # Replay slots are absent after session replacement: the booked
        # delivery age is unavailable with its fixed reason — the content
        # receipt age is NOT copied into its place.
        assert view.last_booked_delivery_age_ms is None
        assert FreshnessReason.NO_REPLAY_DELIVERY in view.reasons
        assert view.source_content_receipt_age_ms == 850
        assert view.status is FeedFreshnessStatus.FRESH

    def test_equal_time_metadata_conflict_and_replay_cannot_freshen_content(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session, query_time_ms=BASE_MS)
        # Equal queryTime delivery with different content: metadata-only
        # booking with a source-timestamp conflict decision.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 5000,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS, sequence=2))
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        view = result.feed_assessments[PollFeed.VEHICLES]
        # The accepted source content is still the original envelope: its
        # source age (1000 ms) and receipt age are unchanged.
        assert view.source_age_ms == 1000
        assert view.source_content_receipt_age_ms == 850
        assert view.status is FeedFreshnessStatus.FRESH

    def test_replay_reuse_cannot_freshen_source_time(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        replay_delivery = vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS,
        )
        session.accept(replay_delivery)
        state_snapshot_before = state.snapshot()
        # Exact replay of the same delivery: duplicate no-op, nothing
        # published, source time not freshened.
        result_apply = session.accept(replay_delivery)
        assert result_apply.decision.name == "DUPLICATE_NOOP"
        assert state.snapshot().revision == state_snapshot_before.revision
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        assert result.feed_assessments[
            PollFeed.VEHICLES
        ].source_age_ms == 1000

    def test_request_duration_is_not_api_delivery_latency(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        view = result.feed_assessments[PollFeed.VEHICLES]
        # request_elapsed_ms is the recorded source-stamp request duration
        # (133 ms of synthetic elapsed_mono_ms), exposed as a separately
        # recorded fact — never relabelled as end-to-end API latency and
        # never used in any freshness classification.
        assert view.request_elapsed_ms == 133
        assert view.source_age_ms == 1000


# ---------------------------------------------------------------------------
# Row 5: position age measurement only; stale source keeps unexpired MAX
# ---------------------------------------------------------------------------


class TestPositionAgeMeasurementOnly:
    def test_position_age_does_not_create_position_stale_warning(self):
        # Named regression (row 5): a very old position produces only an
        # exact measurement — no position-old flag, no position staleness
        # threshold, no warning reason, and no visibility effect.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS - 10 * 60_000,
                              expires_utc_ms=BASE_MS + 10 * 60_000)],
            query_time_ms=BASE_MS))
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        vehicle = result.vehicle_assessments[1001]
        assert vehicle.position_age_ms == 10 * 60_000 + 1000
        assert vehicle.position_time_quality is PositionTimeQuality.KNOWN
        assert vehicle.position_ahead_by_ms is None
        # No reason in the S13 vocabulary marks a position as "old".
        assert vehicle.reasons == ()
        assert not hasattr(vehicle, "position_status")
        assert not hasattr(vehicle, "position_threshold_ms")
        # The vehicle is still visible (fresh source, unexpired).
        assert result.visible_max_vehicle_ids == (1001,)
        assert result.feed_assessments[
            PollFeed.VEHICLES
        ].status is FeedFreshnessStatus.FRESH

    def test_fresh_position_stale_source(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS - 1000,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS - 60_000))
        # Source age 61_000 > 30_000 threshold → STALE; the position is
        # only 1000 + 1000 old → KNOWN; the unexpired MAX stays visible.
        result = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        view = result.feed_assessments[PollFeed.VEHICLES]
        vehicle = result.vehicle_assessments[1001]
        assert view.status is FeedFreshnessStatus.STALE
        assert vehicle.position_time_quality is PositionTimeQuality.KNOWN
        assert vehicle.position_age_ms == 2000
        assert result.visible_max_vehicle_ids == (1001,)

    def test_stale_unexpired_max_survives_missing_enrichment(self):
        # A MAX vehicle whose trip/shape enrichment is missing (no trip_id)
        # is still tracked and still visible when the feed is stale.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS - 120_000,
                              trip_id=None,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS - 120_000))
        result = _trusted_eval(state, utc_ms=BASE_MS)
        assert result.feed_assessments[
            PollFeed.VEHICLES
        ].status is FeedFreshnessStatus.STALE
        assert result.visible_max_vehicle_ids == (1001,)
        assert result.vehicle_assessments[
            1001
        ].expiry_status is VehicleExpiryStatus.UNEXPIRED


# ---------------------------------------------------------------------------
# Row 12: future-timestamp boundaries; unknown clock never freshens
# ---------------------------------------------------------------------------


class TestFutureBoundaries:
    def test_source_ahead_within_tolerance_measures_age_zero(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session, query_time_ms=BASE_MS + 5000)
        policy = default_policy()  # tolerance 5000
        result = _trusted_eval(state, utc_ms=BASE_MS)
        view = result.feed_assessments[PollFeed.VEHICLES]
        assert view.status is FeedFreshnessStatus.FRESH
        assert view.source_age_ms == 0
        assert view.ahead_by_ms == 5000

    def test_source_ahead_beyond_tolerance_is_unknown(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session, query_time_ms=BASE_MS + 5001)
        policy = default_policy()
        result = _trusted_eval(state, utc_ms=BASE_MS)
        view = result.feed_assessments[PollFeed.VEHICLES]
        assert view.status is FeedFreshnessStatus.UNKNOWN
        assert view.source_age_ms is None
        assert view.ahead_by_ms == 5001
        # The overriding reason stays first; independent metric
        # annotations accompany the retained local measurements.
        assert view.reasons[0] is FreshnessReason.SOURCE_TIME_AHEAD
        assert view.request_elapsed_ms == 133  # still a recorded fact

    def test_source_ahead_beyond_tolerance_preserves_independent_metrics(self):
        # Permanent regression for the erased-metrics defect: a suspect
        # upstream source timestamp makes the source age UNKNOWN, but the
        # trusted local receipt metrics and the accepted collection's own
        # completeness evidence remain valid and reported.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 6000,
                              expires_utc_ms=BASE_MS + 120_000)],
            query_time_ms=BASE_MS + 6000,
            requested_at_ms=BASE_MS - 200,
            receipt_ms=BASE_MS - 100,
            evidence=complete_evidence(1),
        ))
        result = _trusted_eval(state, utc_ms=BASE_MS)
        view = result.feed_assessments[PollFeed.VEHICLES]
        assert view.status is FeedFreshnessStatus.UNKNOWN
        assert FreshnessReason.SOURCE_TIME_AHEAD in view.reasons
        assert view.ahead_by_ms == 6000
        assert view.collection_quality is CollectionQuality.COMPLETE
        assert view.source_content_receipt_age_ms == 100
        assert view.last_booked_delivery_age_ms == 100
        assert view.request_elapsed_ms == 133

    def test_untrusted_utc_preserves_recorded_request_duration(self):
        # Permanent regression: global clock distrust hides every
        # UTC-derived age but keeps the previously recorded request
        # duration — a fact whose own inputs never depended on the
        # current wall clock — and the feed-level collection quality.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [], query_time_ms=BASE_MS - 1000,
            requested_at_ms=BASE_MS - 200, receipt_ms=BASE_MS - 100,
            evidence=complete_evidence(0)))
        snapshot = state.snapshot()
        result = evaluate_freshness(
            snapshot,
            sample=None,
            policy=default_policy(),
            clock_assessment=UNTRUSTED,
        )
        view = result.feed_assessments[PollFeed.VEHICLES]
        assert view.status is FeedFreshnessStatus.UNKNOWN
        assert view.source_age_ms is None
        assert view.request_elapsed_ms == 133
        assert view.collection_quality is CollectionQuality.COMPLETE

    def test_position_ahead_boundaries(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        # Position exactly at tolerance: KNOWN with exposed skew.
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 5000,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS))
        result = _trusted_eval(state, utc_ms=BASE_MS)
        vehicle = result.vehicle_assessments[1001]
        assert vehicle.position_time_quality is PositionTimeQuality.KNOWN
        assert vehicle.position_age_ms == 0
        assert vehicle.position_ahead_by_ms == 5000

        # One ms beyond: UNKNOWN quality with its fixed reason; the
        # vehicle's expiry is still assessed normally.
        state2, session2 = open_state(s12_context(s12_world(), sha256=SHA_A))
        session2.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS + 5001,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS))
        result2 = _trusted_eval(state2, utc_ms=BASE_MS)
        vehicle2 = result2.vehicle_assessments[1001]
        assert vehicle2.position_time_quality is PositionTimeQuality.UNKNOWN
        assert vehicle2.position_age_ms is None
        assert vehicle2.position_ahead_by_ms == 5001
        assert vehicle2.reasons == (FreshnessReason.POSITION_TIME_AHEAD,)
        assert vehicle2.expiry_status is VehicleExpiryStatus.UNEXPIRED

    def test_future_receipt_makes_receipt_metric_unavailable_only(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 600_000)],
            query_time_ms=BASE_MS,
            receipt_ms=BASE_MS + 5000,  # future receipt vs evaluation UTC
        ))
        result = _trusted_eval(state, utc_ms=BASE_MS)
        view = result.feed_assessments[PollFeed.VEHICLES]
        # A future receipt does not replace the source age: the source
        # classification stands on the source time alone.
        assert view.status is FeedFreshnessStatus.FRESH
        assert view.source_age_ms == 0
        assert view.source_content_receipt_age_ms is None
        assert FreshnessReason.RECEIPT_TIME_AHEAD in view.reasons

    def test_future_expiry_deadline_is_normal_not_ahead(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session, query_time_ms=BASE_MS,
                  expires_ms=BASE_MS + 60_000)
        result = _trusted_eval(state, utc_ms=BASE_MS)
        vehicle = result.vehicle_assessments[1001]
        assert vehicle.expiry_status is VehicleExpiryStatus.UNEXPIRED
        assert vehicle.expires_in_ms == 60_000
        assert FreshnessReason.SOURCE_TIME_AHEAD not in vehicle.reasons
        assert FreshnessReason.POSITION_TIME_AHEAD not in vehicle.reasons

    def test_expired_vehicle_has_signed_negative_distance(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session, query_time_ms=BASE_MS,
                  expires_ms=BASE_MS - 1000)
        result = _trusted_eval(state, utc_ms=BASE_MS)
        vehicle = result.vehicle_assessments[1001]
        assert vehicle.expiry_status is VehicleExpiryStatus.EXPIRED
        assert vehicle.expires_in_ms == -1000
        # Still retained and assessed (cleanup is explicit, not implicit).
        assert result.visible_max_vehicle_ids == ()
        assert 1001 in result.vehicle_assessments


# ---------------------------------------------------------------------------
# Row 15: untrusted view and inconsistent sample
# ---------------------------------------------------------------------------


class TestUnavailableTime:
    def test_untrusted_retains_projection_with_unknown_statuses(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        session.accept(vehicle_delivery(
            [vehicle_position(1001, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS - 1000),
             vehicle_position(1002, time_ms=BASE_MS,
                              expires_utc_ms=BASE_MS + 60_000)],
            query_time_ms=BASE_MS, evidence=complete_evidence(2)))
        snapshot = state.snapshot()
        result = evaluate_freshness(
            snapshot,
            sample=None,
            policy=default_policy(),
            clock_assessment=UNTRUSTED,
        )
        assert result.sample_utc_ms is None
        for feed in PollFeed:
            view = result.feed_assessments[feed]
            assert view.status is FeedFreshnessStatus.UNKNOWN
            assert view.source_age_ms is None
        for assessment in result.vehicle_assessments.values():
            assert assessment.expiry_status is VehicleExpiryStatus.UNKNOWN
            assert assessment.position_time_quality is PositionTimeQuality.UNKNOWN
            assert assessment.position_age_ms is None
            assert assessment.expires_in_ms is None
        # The remaining store projection is retained without expiry claims
        # — never cleared, never claimed proven-unexpired.
        assert result.visible_max_vehicle_ids == snapshot.max_vehicle_ids
        assert set(result.vehicle_assessments) == {1001, 1002}

    def test_sample_before_snapshot_cutoff_is_bounded_unknown(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)
        state.expire_vehicles(BASE_MS + 30_000)
        snapshot = state.snapshot()
        result = evaluate_freshness(
            snapshot,
            sample=TimeSample(utc_ms=BASE_MS + 29_999, monotonic_ms=1_000_000),
            policy=default_policy(),
            clock_assessment=TRUSTED,
        )
        # The trusted sample precedes the accepted cutoff: no trusted
        # current view, no negative ages, no lowered cutoff.
        assert result.sample_utc_ms is None
        assert FreshnessReason.SAMPLE_BEFORE_SNAPSHOT_CUTOFF in result.top_reasons
        view = result.feed_assessments[PollFeed.VEHICLES]
        assert view.status is FeedFreshnessStatus.UNKNOWN
        assert view.source_age_ms is None
        assert result.expiry_state is not None
        assert result.expiry_state.cutoff_utc_ms == BASE_MS + 30_000
        # Visible = unfiltered retained projection with unknown statuses.
        assert result.visible_max_vehicle_ids == snapshot.max_vehicle_ids
        assert result.vehicle_assessments[1001].expiry_status is VehicleExpiryStatus.UNKNOWN

    def test_sample_at_cutoff_is_coherent(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)
        state.expire_vehicles(BASE_MS + 30_000)
        result = _trusted_eval(state, utc_ms=BASE_MS + 30_000)
        assert result.sample_utc_ms == BASE_MS + 30_000
        assert FreshnessReason.SAMPLE_BEFORE_SNAPSHOT_CUTOFF not in result.top_reasons


# ---------------------------------------------------------------------------
# F2 correction: the trusted sample must be an actual validated TimeSample
# ---------------------------------------------------------------------------


class TestSampleTypeBoundary:
    @pytest.mark.parametrize(
        "bad_sample",
        [True, object(), SimpleNamespace(utc_ms=BASE_MS)],
        ids=["bool", "object", "duck_typed_utc_only"],
    )
    def test_evaluator_requires_actual_validated_timesample(
        self, bad_sample,
    ):
        # evaluate_freshness is a public entry point: a trusted
        # assertion carrying anything but a validated TimeSample is a
        # typed input error — never a raw AttributeError and never a
        # duck-typed acceptance missing the monotonic field.
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)
        before = state.snapshot()
        with pytest.raises(FreshnessInputError) as excinfo:
            evaluate_freshness(
                before,
                sample=bad_sample,
                policy=default_policy(),
                clock_assessment=TRUSTED,
            )
        assert excinfo.value.code == "invalid_sample"
        assert state.snapshot() == before


# ---------------------------------------------------------------------------
# Row 16: purity — no store mutation, no clock reads
# ---------------------------------------------------------------------------


class TestEvaluatorPurity:
    def test_evaluation_does_not_mutate_the_store(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)
        before = state.snapshot()
        _trusted_eval(state, utc_ms=BASE_MS + 1000)
        _trusted_eval(state, utc_ms=BASE_MS + 2_000_000)
        after = state.snapshot()
        assert after.revision == before.revision
        assert after.expiry_state is None
        assert set(after.vehicle_entries) == set(before.vehicle_entries)

    def test_evaluator_reads_no_clock(self):
        import backend.trimet.freshness as freshness_module

        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)

        def forbidden(*args: object, **kwargs: object) -> object:
            raise AssertionError("evaluator must not read a clock")

        class _ForbiddenDatetime:
            @staticmethod
            def now(*args: object, **kwargs: object) -> object:
                raise AssertionError("evaluator must not read a clock")

        state_snapshot = state.snapshot()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(freshness_module.time, "monotonic_ns", forbidden)
            patch.setattr(freshness_module.calendar, "timegm", forbidden)
            patch.setattr(freshness_module, "datetime", _ForbiddenDatetime)
            result = _trusted_eval(state, utc_ms=BASE_MS + 1000,
                                   snapshot=state_snapshot)
        assert result.sample_utc_ms == BASE_MS + 1000

    def test_repeated_evaluation_is_deterministic(self):
        state, session = open_state(s12_context(s12_world(), sha256=SHA_A))
        _seed_one(state, session)
        first = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        second = _trusted_eval(state, utc_ms=BASE_MS + 1000)
        assert first.visible_max_vehicle_ids == second.visible_max_vehicle_ids
        assert (first.feed_assessments[PollFeed.VEHICLES].source_age_ms
                == second.feed_assessments[PollFeed.VEHICLES].source_age_ms)
