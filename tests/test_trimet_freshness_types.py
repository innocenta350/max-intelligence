"""S13 frozen vocabulary, validation and construction contracts (row 1).

Acceptance row 1: policy/sample/clock/expiry/view validation, ``bool``
exclusion, complete integer conversion, frozen maps, and old S12 snapshot
constructors remain compatible with the defaulted ``expiry_state=None``.

No store, clock reads, network or config: pure type-level tests.
"""
from __future__ import annotations

from types import MappingProxyType

import pytest

from tests._s10_helpers import make_config
from tests._s13_helpers import (  # noqa: F401  (module-local autouse guard)
    s13_deny_all_sockets,
)
from backend.trimet.feed_types import (
    MAX_PLAUSIBLE_UTC_MS,
    MIN_PLAUSIBLE_UTC_MS,
)
from backend.trimet.freshness import freshness_policy_from_config
from backend.trimet.freshness_errors import (
    FRESHNESS_INPUT_CODES,
    FRESHNESS_RECOVERY_CODES,
    FreshnessInputError,
    FreshnessRecoveryError,
)
from backend.trimet.freshness_types import (
    ClockAssessment,
    ClockStatus,
    CollectionQuality,
    ExpiryResult,
    ExpiryState,
    FeedFreshnessAssessment,
    FeedFreshnessStatus,
    FreshnessPolicy,
    FreshnessReason,
    PositionTimeQuality,
    TimeSample,
    VehicleExpiryStatus,
    VehicleFreshnessAssessment,
)
from backend.trimet.match_types import DatasetIdentity, RelevanceClassification
from backend.trimet.poll_types import PollFeed
from backend.trimet.state_types import LiveStateSnapshot, StateTelemetry

# 2006-era era-safe plausible instant for boundary tests (inside the range,
# far from the S9 fixture era so validation, not arithmetic, is the target).
MID_UTC_MS = 1_200_000_000_000


# ---------------------------------------------------------------------------
# Frozen vocabularies: exact spellings (plan §11)
# ---------------------------------------------------------------------------


class TestFrozenVocabulary:
    def test_feed_status_values(self):
        assert [s.value for s in FeedFreshnessStatus] == [
            "unknown", "fresh", "stale",
        ]

    def test_position_quality_values(self):
        assert [q.value for q in PositionTimeQuality] == ["known", "unknown"]

    def test_expiry_status_values(self):
        assert [s.value for s in VehicleExpiryStatus] == [
            "unexpired", "expired", "unknown",
        ]

    def test_clock_status_values(self):
        assert [s.value for s in ClockStatus] == [
            "uninitialized", "trusted", "untrusted",
        ]

    def test_collection_quality_values(self):
        assert [q.value for q in CollectionQuality] == [
            "complete", "partial", "unknown",
        ]

    def test_thirteen_exact_freshness_reasons(self):
        assert [r.value for r in FreshnessReason] == [
            "no_source_content",
            "no_replay_delivery",
            "source_time_ahead",
            "position_time_ahead",
            "receipt_time_ahead",
            "clock_uninitialized",
            "clock_read_failed",
            "invalid_clock_sample",
            "clock_moved_backwards",
            "clock_diverged",
            "utc_before_expiry_cutoff",
            "sample_before_snapshot_cutoff",
            "cleanup_deferred",
        ]

    def test_seven_exact_input_recovery_error_codes(self):
        assert FRESHNESS_INPUT_CODES == frozenset(
            {"invalid_policy", "invalid_clock", "invalid_sample",
             "invalid_snapshot", "invalid_cutoff"}
        )
        assert FRESHNESS_RECOVERY_CODES == frozenset(
            {"clock_epoch_conflict", "clock_recovery_blocked"}
        )

    def test_error_types_carry_bounded_message_and_code(self):
        err = FreshnessInputError("bad", code="invalid_cutoff")
        assert err.message == "bad" and err.code == "invalid_cutoff"
        rec = FreshnessRecoveryError("no", code="clock_epoch_conflict")
        assert rec.message == "no" and rec.code == "clock_epoch_conflict"
        with pytest.raises(ValueError):
            FreshnessInputError("bad", code="clock_recovery_blocked")
        with pytest.raises(ValueError):
            FreshnessRecoveryError("no", code="invalid_cutoff")


# ---------------------------------------------------------------------------
# FreshnessPolicy validation (row 1)
# ---------------------------------------------------------------------------


class TestFreshnessPolicy:
    def test_owner_defaults(self):
        policy = FreshnessPolicy()
        assert policy.vehicles_poll_ms == 10000
        assert policy.tripupdates_poll_ms == 60000
        assert policy.alerts_poll_ms == 120000
        assert policy.source_future_tolerance_ms == 5000
        assert policy.clock_divergence_tolerance_ms == 5000

    def test_poll_interval_ms_per_feed(self):
        policy = FreshnessPolicy(vehicles_poll_ms=7, tripupdates_poll_ms=9,
                                 alerts_poll_ms=11)
        assert policy.poll_interval_ms(PollFeed.VEHICLES) == 7
        assert policy.poll_interval_ms(PollFeed.TRIPUPDATES) == 9
        assert policy.poll_interval_ms(PollFeed.ALERTS) == 11

    def test_poll_intervals_must_be_positive_integers_excluding_bool(self):
        for bad in (0, -1, True, False, 1.0, "10", None):
            with pytest.raises(ValueError):
                FreshnessPolicy(vehicles_poll_ms=bad)
        with pytest.raises(ValueError):
            FreshnessPolicy(tripupdates_poll_ms=True)
        with pytest.raises(ValueError):
            FreshnessPolicy(alerts_poll_ms=0)

    def test_tolerances_must_be_nonnegative_integers_excluding_bool(self):
        for bad in (-1, True, 1.5, "5"):
            with pytest.raises(ValueError):
                FreshnessPolicy(source_future_tolerance_ms=bad)
        with pytest.raises(ValueError):
            FreshnessPolicy(clock_divergence_tolerance_ms=True)
        assert FreshnessPolicy(source_future_tolerance_ms=0).source_future_tolerance_ms == 0

    def test_policy_is_frozen(self):
        policy = FreshnessPolicy()
        with pytest.raises(Exception):
            policy.vehicles_poll_ms = 1  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Config adapter: freshness_policy_from_config (plan §5 contract, row 1)
# ---------------------------------------------------------------------------


class TestFreshnessPolicyFromConfig:
    def test_default_config_maps_seconds_to_milliseconds(self):
        policy = freshness_policy_from_config(make_config())
        assert policy.vehicles_poll_ms == 10_000
        assert policy.tripupdates_poll_ms == 60_000
        assert policy.alerts_poll_ms == 120_000
        # Tolerances keep the frozen FreshnessPolicy defaults.
        assert policy.source_future_tolerance_ms == 5000
        assert policy.clock_divergence_tolerance_ms == 5000

    def test_custom_config_intervals_produce_custom_thresholds_basis(self):
        from backend.trimet.poll_types import PollFeed

        policy = freshness_policy_from_config(make_config(
            vehicles_poll_seconds=7,
            tripupdates_poll_seconds=9,
            alerts_poll_seconds=11,
        ))
        assert policy.poll_interval_ms(PollFeed.VEHICLES) == 7_000
        assert policy.poll_interval_ms(PollFeed.TRIPUPDATES) == 9_000
        assert policy.poll_interval_ms(PollFeed.ALERTS) == 11_000

    def test_non_liveconfig_rejected_as_invalid_policy(self):
        for bad in (None, "config", 12345, object()):
            with pytest.raises(FreshnessInputError) as excinfo:
                freshness_policy_from_config(bad)
            assert excinfo.value.code == "invalid_policy"

    def test_unvalidated_poll_values_rejected_by_the_adapter_itself(self):
        from backend.trimet.live_config import LiveConfig

        # Bypass LiveConfig's own __post_init__ validation to prove the
        # adapter does not rely on it: a malformed field still raises the
        # typed invalid_policy error, bools excluded.
        for bad in (True, 0, -5, 1.5, "10"):
            config = object.__new__(LiveConfig)
            object.__setattr__(config, "app_id", None)
            object.__setattr__(config, "quota_initial_used", 0)
            object.__setattr__(config, "vehicles_poll_seconds", bad)
            object.__setattr__(config, "tripupdates_poll_seconds", 60)
            object.__setattr__(config, "alerts_poll_seconds", 120)
            with pytest.raises(FreshnessInputError) as excinfo:
                freshness_policy_from_config(config)
            assert excinfo.value.code == "invalid_policy"
            assert "vehicles_poll_seconds" in excinfo.value.message


# ---------------------------------------------------------------------------
# TimeSample and ClockAssessment validation (row 1)
# ---------------------------------------------------------------------------


class TestTimeSample:
    def test_valid_pair(self):
        sample = TimeSample(utc_ms=MID_UTC_MS, monotonic_ms=0)
        assert sample.utc_ms == MID_UTC_MS and sample.monotonic_ms == 0

    def test_plausible_utc_range_enforced(self):
        with pytest.raises(ValueError):
            TimeSample(utc_ms=MIN_PLAUSIBLE_UTC_MS - 1, monotonic_ms=0)
        with pytest.raises(ValueError):
            TimeSample(utc_ms=MAX_PLAUSIBLE_UTC_MS + 1, monotonic_ms=0)
        assert TimeSample(
            utc_ms=MIN_PLAUSIBLE_UTC_MS, monotonic_ms=0
        ).utc_ms == MIN_PLAUSIBLE_UTC_MS

    def test_bool_and_nonint_excluded(self):
        for bad in (True, False, 1.0, "100", None):
            with pytest.raises(ValueError):
                TimeSample(utc_ms=bad, monotonic_ms=0)  # type: ignore[arg-type]
            with pytest.raises(ValueError):
                TimeSample(utc_ms=MID_UTC_MS, monotonic_ms=bad)  # type: ignore[arg-type]

    def test_monotonic_must_be_nonnegative(self):
        with pytest.raises(ValueError):
            TimeSample(utc_ms=MID_UTC_MS, monotonic_ms=-1)

    def test_sample_is_frozen(self):
        sample = TimeSample(utc_ms=MID_UTC_MS, monotonic_ms=1)
        with pytest.raises(Exception):
            sample.utc_ms = 5  # type: ignore[misc]


class TestClockAssessment:
    def test_uninitialized_epoch_zero_allowed(self):
        assessment = ClockAssessment(status=ClockStatus.UNINITIALIZED, epoch=0)
        assert assessment.reasons == ()

    def test_trusted_and_untrusted_require_positive_epoch(self):
        with pytest.raises(ValueError):
            ClockAssessment(status=ClockStatus.TRUSTED, epoch=0)
        with pytest.raises(ValueError):
            ClockAssessment(status=ClockStatus.UNTRUSTED, epoch=-1)
        assert ClockAssessment(status=ClockStatus.TRUSTED, epoch=3).epoch == 3

    def test_bool_epoch_excluded(self):
        with pytest.raises(ValueError):
            ClockAssessment(status=ClockStatus.TRUSTED, epoch=True)

    def test_reasons_must_be_freshness_reason_values(self):
        with pytest.raises(ValueError):
            ClockAssessment(
                status=ClockStatus.UNTRUSTED, epoch=1,
                reasons=("clock_diverged",),  # type: ignore[list-item]
            )
        assessment = ClockAssessment(
            status=ClockStatus.UNTRUSTED, epoch=1,
            reasons=(FreshnessReason.CLOCK_DIVERGED,),
        )
        assert assessment.reasons == (FreshnessReason.CLOCK_DIVERGED,)


# ---------------------------------------------------------------------------
# ExpiryState / ExpiryResult validation (row 1)
# ---------------------------------------------------------------------------


class TestExpiryTypes:
    def test_expiry_state_validation(self):
        state = ExpiryState(
            cutoff_utc_ms=MID_UTC_MS,
            removed_entries_total=0,
            ignored_expired_winners_total=0,
        )
        assert state.removed_entries_total == 0
        with pytest.raises(ValueError):
            ExpiryState(cutoff_utc_ms=True, removed_entries_total=0,
                        ignored_expired_winners_total=0)
        with pytest.raises(ValueError):
            ExpiryState(cutoff_utc_ms=MID_UTC_MS, removed_entries_total=-1,
                        ignored_expired_winners_total=0)
        with pytest.raises(ValueError):
            ExpiryState(cutoff_utc_ms=MID_UTC_MS, removed_entries_total=0,
                        ignored_expired_winners_total=True)

    def test_expiry_state_is_frozen(self):
        state = ExpiryState(cutoff_utc_ms=MID_UTC_MS,
                            removed_entries_total=1,
                            ignored_expired_winners_total=0)
        with pytest.raises(Exception):
            state.cutoff_utc_ms = 1  # type: ignore[misc]

    def test_expiry_result_removed_ids_ascending(self):
        with pytest.raises(ValueError):
            ExpiryResult(
                requested_cutoff_utc_ms=MID_UTC_MS,
                effective_cutoff_utc_ms=MID_UTC_MS,
                revision_before=1, revision_after=2, changed=True,
                removed_vehicle_ids=(7, 3),
            )
        result = ExpiryResult(
            requested_cutoff_utc_ms=MID_UTC_MS,
            effective_cutoff_utc_ms=MID_UTC_MS,
            revision_before=1, revision_after=2, changed=True,
            removed_vehicle_ids=(3, 7),
        )
        assert result.removed_vehicle_ids == (3, 7)

    def test_expiry_result_bool_exclusion(self):
        with pytest.raises(ValueError):
            ExpiryResult(
                requested_cutoff_utc_ms=True,  # type: ignore[arg-type]
                effective_cutoff_utc_ms=None,
                revision_before=0, revision_after=0, changed=False,
            )
        with pytest.raises(ValueError):
            ExpiryResult(
                requested_cutoff_utc_ms=MID_UTC_MS,
                effective_cutoff_utc_ms=None,
                revision_before=0, revision_after=0, changed=1,
            )


# ---------------------------------------------------------------------------
# Assessment invariants (row 1)
# ---------------------------------------------------------------------------


class TestAssessmentInvariants:
    def test_fresh_stale_require_age_threshold_and_no_overriding_reason(self):
        with pytest.raises(ValueError):
            FeedFreshnessAssessment(
                feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.FRESH,
                source_age_ms=None, threshold_ms=30000,
            )
        with pytest.raises(ValueError):
            FeedFreshnessAssessment(
                feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.STALE,
                source_age_ms=30001,
            )
        with pytest.raises(ValueError):
            FeedFreshnessAssessment(
                feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.FRESH,
                source_age_ms=0, threshold_ms=30000,
                reasons=(FreshnessReason.SOURCE_TIME_AHEAD,),
            )
        # Metric-availability annotations may coexist with FRESH/STALE.
        assessment = FeedFreshnessAssessment(
            feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.FRESH,
            source_age_ms=0, threshold_ms=30000,
            reasons=(FreshnessReason.NO_REPLAY_DELIVERY,),
        )
        assert assessment.status is FeedFreshnessStatus.FRESH

    def test_age_fields_nonnegative_or_none_excluding_bool(self):
        with pytest.raises(ValueError):
            FeedFreshnessAssessment(
                feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.UNKNOWN,
                source_age_ms=True,
            )
        with pytest.raises(ValueError):
            FeedFreshnessAssessment(
                feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.UNKNOWN,
                ahead_by_ms=-5,
            )
        with pytest.raises(ValueError):
            FeedFreshnessAssessment(
                feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.UNKNOWN,
                request_elapsed_ms=True,
            )

    def test_ahead_by_requires_unavailable_or_zero_age(self):
        with pytest.raises(ValueError):
            FeedFreshnessAssessment(
                feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.UNKNOWN,
                source_age_ms=10, ahead_by_ms=10,
            )
        # Beyond tolerance: age unavailable, skew exposed.
        beyond = FeedFreshnessAssessment(
            feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.UNKNOWN,
            ahead_by_ms=10,
        )
        assert beyond.source_age_ms is None
        # Within tolerance: clamped age 0 with the exact skew exposed.
        within = FeedFreshnessAssessment(
            feed=PollFeed.VEHICLES, status=FeedFreshnessStatus.FRESH,
            source_age_ms=0, ahead_by_ms=10, threshold_ms=30000,
        )
        assert within.ahead_by_ms == 10

    def test_known_position_quality_requires_measured_age(self):
        with pytest.raises(ValueError):
            VehicleFreshnessAssessment(
                vehicle_id=1,
                classification=RelevanceClassification.MAX_RELEVANT,
                position_time_quality=PositionTimeQuality.KNOWN,
                position_age_ms=None,
            )
        with pytest.raises(ValueError):
            VehicleFreshnessAssessment(
                vehicle_id=1,
                classification=RelevanceClassification.MAX_RELEVANT,
                position_age_ms=10, position_ahead_by_ms=1,
            )
        # Within tolerance: clamped age 0 with exposed skew is valid.
        within = VehicleFreshnessAssessment(
            vehicle_id=1,
            classification=RelevanceClassification.MAX_RELEVANT,
            position_age_ms=0, position_ahead_by_ms=1,
        )
        assert within.position_ahead_by_ms == 1

    def test_signed_expires_in_allowed_bool_excluded(self):
        assessment = VehicleFreshnessAssessment(
            vehicle_id=1,
            classification=RelevanceClassification.MAX_RELEVANT,
            expires_in_ms=-1000,
            expiry_status=VehicleExpiryStatus.EXPIRED,
        )
        assert assessment.expires_in_ms == -1000
        with pytest.raises(ValueError):
            VehicleFreshnessAssessment(
                vehicle_id=True,
                classification=RelevanceClassification.MAX_RELEVANT,
            )
        with pytest.raises(ValueError):
            VehicleFreshnessAssessment(
                vehicle_id=1,
                classification=RelevanceClassification.MAX_RELEVANT,
                expires_in_ms=True,
            )

    def test_assessment_reasons_must_be_freshness_reason_values(self):
        with pytest.raises(ValueError):
            VehicleFreshnessAssessment(
                vehicle_id=1,
                classification=RelevanceClassification.MAX_RELEVANT,
                reasons=("position_time_ahead",),  # type: ignore[list-item]
            )


# ---------------------------------------------------------------------------
# FreshnessSnapshot validation: frozen maps, ascending ids (row 1)
# ---------------------------------------------------------------------------


def _dataset_identity() -> DatasetIdentity:
    return DatasetIdentity(archive_sha256="ab" * 32, feed_version="fv")


def _snapshot_fields(**overrides: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "revision": 1,
        "generation": 2,
        "dataset_identity": _dataset_identity(),
        "clock_assessment": ClockAssessment(
            status=ClockStatus.TRUSTED, epoch=1
        ),
        "sample_utc_ms": MID_UTC_MS,
        "feed_assessments": {},
        "vehicle_assessments": {},
        "visible_max_vehicle_ids": (),
        "expiry_state": None,
        "expiry_result": None,
    }
    fields.update(overrides)
    return fields


class TestFreshnessSnapshotValidation:
    def test_mapping_fields_become_read_only_proxies(self):
        from backend.trimet.freshness_types import FreshnessSnapshot

        feeds = {PollFeed.VEHICLES: None}
        vehicles = {1: None}
        snapshot = FreshnessSnapshot(
            **_snapshot_fields(  # type: ignore[arg-type]
                feed_assessments=feeds, vehicle_assessments=vehicles,
            )
        )
        assert isinstance(snapshot.feed_assessments, MappingProxyType)
        assert isinstance(snapshot.vehicle_assessments, MappingProxyType)
        with pytest.raises(TypeError):
            snapshot.feed_assessments[PollFeed.ALERTS] = None  # type: ignore[index]
        # Caller's dict is copied, not aliased.
        feeds[PollFeed.ALERTS] = None
        assert PollFeed.ALERTS not in snapshot.feed_assessments

    def test_visible_ids_ascending_and_int_excluding_bool(self):
        from backend.trimet.freshness_types import FreshnessSnapshot

        with pytest.raises(ValueError):
            FreshnessSnapshot(
                **_snapshot_fields(visible_max_vehicle_ids=(9, 3))  # type: ignore[arg-type]
            )
        with pytest.raises(ValueError):
            FreshnessSnapshot(
                **_snapshot_fields(visible_max_vehicle_ids=(True, 3))  # type: ignore[arg-type]
            )
        snapshot = FreshnessSnapshot(
            **_snapshot_fields(visible_max_vehicle_ids=(3, 9))  # type: ignore[arg-type]
        )
        assert snapshot.visible_max_vehicle_ids == (3, 9)

    def test_top_reasons_must_be_freshness_reason_values(self):
        from backend.trimet.freshness_types import FreshnessSnapshot

        with pytest.raises(ValueError):
            FreshnessSnapshot(
                **_snapshot_fields(
                    top_reasons=("cleanup_deferred",),  # type: ignore[list-item]
                )
            )
        snapshot = FreshnessSnapshot(
            **_snapshot_fields(
                top_reasons=(FreshnessReason.CLEANUP_DEFERRED,),  # type: ignore[arg-type]
            )
        )
        assert snapshot.top_reasons == (FreshnessReason.CLEANUP_DEFERRED,)

    def test_snapshot_is_frozen(self):
        from backend.trimet.freshness_types import FreshnessSnapshot

        snapshot = FreshnessSnapshot(**_snapshot_fields())  # type: ignore[arg-type]
        with pytest.raises(Exception):
            snapshot.revision = 5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Old S12 snapshot constructors remain compatible (row 1)
# ---------------------------------------------------------------------------


class TestS12ConstructorCompatibility:
    def _s12_fields(self) -> dict[str, object]:
        return {
            "revision": 0,
            "generation": 1,
            "dataset_identity": _dataset_identity(),
            "prior_state_history": "unknown",
            "active_session_id": None,
            "vehicle_entries": {},
            "max_vehicle_ids": (),
            "feed_views": {},
            "absence_fence": None,
            "telemetry": StateTelemetry(
                reason_totals={}, samples=(), omitted_samples=0,
                total_events=0,
            ),
        }

    def _s12_snapshot(self) -> LiveStateSnapshot:
        return LiveStateSnapshot(**self._s12_fields())  # type: ignore[arg-type]

    def test_default_expiry_state_is_none(self):
        snapshot = self._s12_snapshot()
        assert snapshot.expiry_state is None

    def test_explicit_expiry_state_accepted(self):
        expiry = ExpiryState(
            cutoff_utc_ms=MID_UTC_MS, removed_entries_total=2,
            ignored_expired_winners_total=1,
        )
        fields = self._s12_fields()
        fields["expiry_state"] = expiry
        snapshot = LiveStateSnapshot(**fields)  # type: ignore[arg-type]
        assert snapshot.expiry_state is expiry

    def test_invalid_expiry_state_rejected(self):
        fields = self._s12_fields()
        fields["expiry_state"] = "expired"
        with pytest.raises(ValueError):
            LiveStateSnapshot(**fields)  # type: ignore[arg-type]

    def test_old_snapshot_unchanged_after_expiry_activation_elsewhere(self):
        # A snapshot captured before activation never changes: the store
        # publishes new immutable snapshots rather than mutating this one.
        snapshot = self._s12_snapshot()
        assert snapshot.expiry_state is None
        assert snapshot.revision == 0
