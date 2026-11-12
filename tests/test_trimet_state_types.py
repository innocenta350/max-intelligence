"""S12 frozen-contract tests (plan §5, §12; implementation prompt §4).

Covers acceptance-matrix row 1 (frozen snapshot/entry/result contracts,
nested-map immutability, caller-mutation isolation, limits validation) and
the vocabulary-freezing parts of rows 12/15 (frozen reason/error sets).
"""
from __future__ import annotations

import dataclasses

import pytest

from tests._s12_helpers import s12_deny_all_sockets  # noqa: F401

from backend.trimet.feed_types import (
    VehiclesRequestKind,
    VehiclesRequestProvenance,
    VehiclesSnapshot,
    VehiclePosition,
)
from backend.trimet.match_types import (
    EnrichmentState,
    MatchedVehicle,
    RelevanceClassification,
)
from backend.trimet.poll_types import PollFeed, RequestPurpose
from backend.trimet.state_errors import (
    STATE_ERROR_CODES,
    StateCapacityError,
    StateConflictError,
    StateError,
    StateInputError,
    StateSessionError,
)
from backend.trimet.state_types import (
    PRIOR_STATE_HISTORY_UNKNOWN,
    AbsenceFence,
    ApplyDecision,
    ApplyResult,
    ContextSwapResult,
    FeedEnvelope,
    LiveStateSnapshot,
    SourceStamp,
    StateDecisionSample,
    StateLimits,
    StateReason,
    StateTelemetry,
    VehicleEntry,
)

BASE_MS = 1_794_024_790_000


def _position(vehicle_id: int = 1001) -> VehiclePosition:
    return VehiclePosition(
        vehicle_id=vehicle_id,
        vehicle_type="rail",
        route_number=90,
        latitude=45.52,
        longitude=-122.63,
        time_utc_ms=BASE_MS,
        expires_utc_ms=BASE_MS + 90_000,
    )


def _stamp(sequence: int = 1) -> SourceStamp:
    return SourceStamp(
        session_id=1,
        sequence=sequence,
        source_time_utc_ms=BASE_MS,
        purpose=RequestPurpose.STARTUP_FULL,
        request_sent_utc_ms=BASE_MS,
        receipt_utc_ms=BASE_MS + 150,
        elapsed_mono_ms=10,
        http_status=200,
        vehicles_provenance=VehiclesRequestProvenance(
            request_kind=VehiclesRequestKind.DEFAULT,
            since_ms=None,
            requested_at_utc_ms=BASE_MS,
        ),
    )


def _entry(vehicle_id: int = 1001) -> VehicleEntry:
    raw = _position(vehicle_id)
    matched = MatchedVehicle(
        classification=RelevanceClassification.MAX_RELEVANT,
        enrichment=EnrichmentState.UNMATCHED,
        vehicle=raw,
        route_id="90",
    )
    return VehicleEntry(
        vehicle_id=vehicle_id,
        raw=raw,
        matched=matched,
        stamp=_stamp(),
        matched_generation=1,
    )


# ---------------------------------------------------------------------------
# Row 1: frozen contracts, nested immutability, caller-mutation isolation
# ---------------------------------------------------------------------------


def test_snapshot_contract_is_frozen_and_caller_mutation_is_isolated():
    from backend.trimet.match_types import DatasetIdentity

    snapshot = LiveStateSnapshot(
        revision=7,
        generation=2,
        dataset_identity=DatasetIdentity(
            archive_sha256="ab" * 32, feed_version="v"
        ),
        prior_state_history=PRIOR_STATE_HISTORY_UNKNOWN,
        active_session_id=1,
        vehicle_entries={1001: _entry()},
        max_vehicle_ids=(1001,),
        feed_views={},
        absence_fence=None,
        telemetry=StateTelemetry(
            reason_totals={}, samples=(), omitted_samples=0, total_events=0
        ),
    )
    # Frozen record: field mutation is refused.
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.revision = 99
    # Nested maps are read-only proxies: caller mutation is refused.
    with pytest.raises(TypeError):
        snapshot.vehicle_entries[2002] = _entry(2002)
    with pytest.raises(TypeError):
        del snapshot.vehicle_entries[1001]
    with pytest.raises(TypeError):
        snapshot.feed_views[PollFeed.VEHICLES] = None
    # The decision-totals mapping is read-only too.
    with pytest.raises(TypeError):
        snapshot.telemetry.reason_totals[StateReason.NON_FULL_REQUEST] = 1
    # Sample collections are tuples.
    assert isinstance(snapshot.telemetry.samples, tuple)


def test_apply_result_and_context_swap_result_are_frozen():
    result = ApplyResult(
        feed=PollFeed.VEHICLES,
        sequence=1,
        decision=ApplyDecision.COMMITTED,
        reasons=(StateReason.AUTHORITATIVE_RECONCILIATION,),
        revision=2,
        authoritative=True,
        removed_vehicle_ids=(1002,),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.revision = 99
    swap = ContextSwapResult(
        changed=True,
        generation_before=1,
        generation_after=2,
        revision_after=3,
        rematched_vehicle_entries=1,
        rematched_feed_views=0,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        swap.changed = False
    # Removed IDs travel as an immutable tuple, ascending.
    assert isinstance(result.removed_vehicle_ids, tuple)
    assert result.removed_vehicle_ids == tuple(sorted(result.removed_vehicle_ids))


def test_vehicle_entry_keeps_raw_record_intact_and_validates_identity():
    entry = _entry()
    # The raw S9 record is referenced intact, never rewritten.
    assert entry.matched.vehicle is entry.raw
    with pytest.raises(dataclasses.FrozenInstanceError):
        entry.matched_generation = 9
    # vehicle_id must equal raw.vehicle_id.
    with pytest.raises(ValueError):
        VehicleEntry(
            vehicle_id=2002,
            raw=entry.raw,
            matched=entry.matched,
            stamp=entry.stamp,
            matched_generation=1,
        )
    # matched must reference the very same raw record.
    other_raw = _position(1001)
    with pytest.raises(ValueError):
        VehicleEntry(
            vehicle_id=1001,
            raw=other_raw,
            matched=entry.matched,
            stamp=entry.stamp,
            matched_generation=1,
        )


def test_source_stamp_never_retains_a_whole_delivery():
    stamp = _stamp()
    # A stamp is compact: it carries metadata, not a delivery or collection.
    assert not hasattr(stamp, "delivery") and not hasattr(stamp, "model")
    assert stamp.vehicles_provenance.requested_at_utc_ms == BASE_MS
    # Validation: bool is not an integer, sequence starts at 1, times must
    # be plausible UTC ms epochs, purpose must be a RequestPurpose.
    with pytest.raises(ValueError):
        dataclasses.replace(stamp, session_id=True)
    with pytest.raises(ValueError):
        dataclasses.replace(stamp, sequence=0)
    with pytest.raises(ValueError):
        dataclasses.replace(stamp, source_time_utc_ms=BASE_MS - 10**13)
    with pytest.raises(ValueError):
        dataclasses.replace(stamp, receipt_utc_ms=BASE_MS + 10**13)
    with pytest.raises(ValueError):
        dataclasses.replace(stamp, elapsed_mono_ms=-1)
    with pytest.raises(ValueError):
        dataclasses.replace(stamp, purpose="startup_full")
    with pytest.raises(ValueError):
        dataclasses.replace(stamp, vehicles_provenance="not-a-provenance")


def test_absence_fence_validates_and_is_frozen():
    fence = AbsenceFence(
        query_time_utc_ms=BASE_MS, seen_ids=frozenset({1001, 1002})
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        fence.query_time_utc_ms = BASE_MS + 1
    with pytest.raises(ValueError):
        AbsenceFence(query_time_utc_ms=0, seen_ids=frozenset())
    with pytest.raises(ValueError):
        # seen_ids must be a frozenset, not an arbitrary set/list.
        AbsenceFence(query_time_utc_ms=BASE_MS, seen_ids={1001})
    with pytest.raises(ValueError):
        AbsenceFence(query_time_utc_ms=BASE_MS, seen_ids=frozenset({"1001"}))


def test_feed_envelope_validates_types():
    from backend.trimet.poll_types import PollDelivery

    delivery = PollDelivery(
        feed=PollFeed.VEHICLES,
        sequence=1,
        purpose=RequestPurpose.STARTUP_FULL,
        request_sent_utc_ms=BASE_MS,
        receipt_utc_ms=BASE_MS + 1,
        elapsed_mono_ms=1,
        http_status=200,
        model=VehiclesSnapshot(
            provenance=VehiclesRequestProvenance(
                request_kind=VehiclesRequestKind.DEFAULT,
                since_ms=None,
                requested_at_utc_ms=BASE_MS,
            ),
            query_time_utc_ms=BASE_MS,
            vehicles=(),
        ),
        diagnostics=(),
        evidence=None,
    )
    with pytest.raises(ValueError):
        FeedEnvelope(
            delivery=delivery,
            batch=None,
            stamp="not-a-stamp",
            matched_generation=1,
        )
    with pytest.raises(ValueError):
        FeedEnvelope(
            delivery="not-a-delivery",
            batch=None,
            stamp=_stamp(),
            matched_generation=1,
        )
    with pytest.raises(ValueError):
        FeedEnvelope(
            delivery=delivery, batch=None, stamp=_stamp(), matched_generation=0
        )


# ---------------------------------------------------------------------------
# Row 1: validated constructor-only limits (bool excluded, positive ints)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    [
        "max_vehicle_entries",
        "max_vehicle_records_per_delivery",
        "max_tripupdate_entities",
        "max_alert_records",
        "max_nested_feed_records",
        "max_state_diagnostic_samples",
    ],
)
@pytest.mark.parametrize("bad", [True, False, 0, -1, 1.5, "10", None])
def test_state_limits_reject_bool_zero_negative_and_non_int(field, bad):
    with pytest.raises(ValueError):
        StateLimits(**{field: bad})


def test_state_limits_defaults_match_the_frozen_plan_values():
    limits = StateLimits()
    assert limits.max_vehicle_entries == 4096
    assert limits.max_vehicle_records_per_delivery == 4096
    assert limits.max_tripupdate_entities == 8192
    assert limits.max_alert_records == 4096
    assert limits.max_nested_feed_records == 100000
    assert limits.max_state_diagnostic_samples == 200


def test_state_limits_are_frozen_and_constructor_only():
    limits = StateLimits(max_vehicle_entries=8)
    with pytest.raises(dataclasses.FrozenInstanceError):
        limits.max_vehicle_entries = 16


# ---------------------------------------------------------------------------
# Frozen vocabularies (plan §12)
# ---------------------------------------------------------------------------


def test_state_reason_vocabulary_is_the_frozen_thirteen():
    expected = {
        "older_sequence",
        "duplicate_delivery",
        "older_source_timestamp",
        "source_timestamp_conflict",
        "older_vehicle_observation",
        "observation_tie_conflict",
        "duplicate_vehicle_id",
        "incomplete_collection",
        "non_full_request",
        "equal_snapshot_time",
        "authoritative_reconciliation",
        "pre_reconciliation_observation",
        "diagnostics_overflow",
    }
    assert {reason.value for reason in StateReason} == expected
    assert len(StateReason) == 13


def test_apply_decision_vocabulary_is_the_frozen_five():
    assert {decision.value for decision in ApplyDecision} == {
        "committed",
        "metadata_only",
        "duplicate_noop",
        "stale_noop",
        "ignored_older",
    }


def test_state_error_codes_are_the_frozen_nine_and_partition_by_family():
    assert STATE_ERROR_CODES == frozenset(
        {
            "invalid_delivery",
            "invalid_context",
            "invalid_limits",
            "invalid_session",
            "session_already_open",
            "sequence_collision",
            "generation_conflict",
            "concurrent_change",
            "capacity_exceeded",
        }
    )
    assert StateInputError("x", code="invalid_delivery").code == "invalid_delivery"
    assert StateSessionError("x", code="session_already_open").code == "session_already_open"
    assert StateConflictError("x", code="sequence_collision").code == "sequence_collision"
    assert StateCapacityError("x", code="capacity_exceeded").code == "capacity_exceeded"
    # Each family refuses codes outside its own subset.
    with pytest.raises(ValueError):
        StateInputError("x", code="concurrent_change")
    with pytest.raises(ValueError):
        StateCapacityError("x", code="invalid_delivery")
    # Every state error carries message + code without payload content.
    err = StateConflictError("bounded message", code="generation_conflict")
    assert isinstance(err, StateError)
    assert err.message == "bounded message"


def test_prior_state_history_constant_is_unknown():
    assert PRIOR_STATE_HISTORY_UNKNOWN == "unknown"


def test_state_telemetry_is_bounded_by_construction_contract():
    telemetry = StateTelemetry(
        reason_totals={StateReason.NON_FULL_REQUEST: 3},
        samples=(
            StateDecisionSample(
                feed=PollFeed.VEHICLES,
                reason=StateReason.NON_FULL_REQUEST,
                record_key=None,
                detail=None,
            ),
        ),
        omitted_samples=0,
        total_events=3,
    )
    with pytest.raises(TypeError):
        telemetry.reason_totals[StateReason.NON_FULL_REQUEST] = 99
    with pytest.raises(dataclasses.FrozenInstanceError):
        telemetry.total_events = 99
