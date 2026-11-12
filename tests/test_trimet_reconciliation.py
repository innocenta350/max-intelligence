"""S12 reconciliation authority matrix, omission and anti-resurrection tests.

Stage S12 (M2 plan §9; implementation prompt §6).  Covers acceptance matrix
rows 6 (every authority gate independently denied), 7 (authorized omission
and raw-ID presence), 8 (ordering safeguards) and 9 (absence fence,
anti-resurrection, and the two named fence-capacity regressions).
"""
from __future__ import annotations

import dataclasses

import pytest

from tests._s12_helpers import s12_deny_all_sockets  # noqa: F401
from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    complete_evidence,
    incremental_delivery,
    open_state,
    patch_match,
    s12_context,
    s12_world,
    vehicle_delivery,
    vehicle_position,
)
from backend.trimet.feed_types import VehiclesRequestKind
from backend.trimet.match_types import RelevanceClassification
from backend.trimet.poll_types import (
    PollFeed,
    RequestPurpose,
    VehicleCollectionEvidence,
)
from backend.trimet.state_errors import (
    StateCapacityError,
    StateConflictError,
    StateInputError,
)
from backend.trimet.state_types import ApplyDecision, StateReason

Q0 = BASE_MS
Q1 = BASE_MS + 1_000
Q2 = BASE_MS + 2_000
Q3 = BASE_MS + 3_000
Q4 = BASE_MS + 4_000


def _context():
    return s12_context(s12_world(), sha256=SHA_A)


def _full(seq, qt, vehicles, **kwargs):
    return vehicle_delivery(
        vehicles,
        query_time_ms=qt,
        sequence=seq,
        evidence=complete_evidence(len(vehicles)),
        **kwargs,
    )


def _base_state(session):
    """v1 MAX, v2 bus NON_MAX, v3 rail-on-bus-route UNCLASSIFIED, at Q1."""
    return session.accept(
        _full(
            1,
            Q1,
            [
                vehicle_position(1001, time_ms=Q1),
                vehicle_position(1002, time_ms=Q1, route_number=1,
                                 vehicle_type="bus"),
                vehicle_position(1003, time_ms=Q1, route_number=1,
                                 vehicle_type="rail"),
            ],
        )
    )


# ---------------------------------------------------------------------------
# Row 6: each authority gate independently denied; absence never runs
# ---------------------------------------------------------------------------


def test_incremental_purpose_is_never_authoritative():
    state, session = open_state(_context())
    _base_state(session)
    result = session.accept(
        incremental_delivery(
            [vehicle_position(1001, time_ms=Q2)],
            query_time_ms=Q2,
            since_ms=Q1,
            sequence=2,
        )
    )
    assert result.authoritative is False
    assert StateReason.NON_FULL_REQUEST in result.reasons
    assert 1002 in state.snapshot().vehicle_entries


def test_incremental_with_default_provenance_is_typed_invalid_input():
    state, session = open_state(_context())
    _base_state(session)
    # INCREMENTAL purpose must pair with SINCE provenance.
    delivery = vehicle_delivery(
        [vehicle_position(1001, time_ms=Q2)],
        query_time_ms=Q2,
        sequence=2,
        # request_kind defaults to VehiclesRequestKind.DEFAULT — mismatch.
        purpose=RequestPurpose.INCREMENTAL,
    )
    with pytest.raises(StateInputError) as excinfo:
        session.accept(delivery)
    assert excinfo.value.code == "invalid_delivery"


def test_full_purpose_with_since_provenance_is_typed_invalid_input():
    state, session = open_state(_context())
    _base_state(session)
    with pytest.raises(StateInputError) as excinfo:
        session.accept(
            vehicle_delivery(
                [vehicle_position(1001, time_ms=Q2)],
                query_time_ms=Q2,
                sequence=2,
                request_kind=VehiclesRequestKind.SINCE,
                since_ms=Q1,
            )
        )
    assert excinfo.value.code == "invalid_delivery"
    assert 1002 in state.snapshot().vehicle_entries


def test_provenance_timestamp_disagreement_is_typed_invalid_input():
    state, session = open_state(_context())
    _base_state(session)
    delivery = _full(2, Q2, [vehicle_position(1001, time_ms=Q2)])
    # The original request timestamp must agree with the snapshot provenance.
    mismatched = dataclasses.replace(
        delivery, request_sent_utc_ms=delivery.request_sent_utc_ms + 7
    )
    with pytest.raises(StateInputError) as excinfo:
        session.accept(mismatched)
    assert excinfo.value.code == "invalid_delivery"


def test_missing_evidence_full_is_positive_only():
    state, session = open_state(_context())
    _base_state(session)
    result = session.accept(
        vehicle_delivery(
            [vehicle_position(1001, time_ms=Q2)],
            query_time_ms=Q2,
            sequence=2,
            evidence=None,
        )
    )
    snapshot = state.snapshot()
    assert result.authoritative is False
    assert StateReason.INCOMPLETE_COLLECTION in result.reasons
    assert snapshot.absence_fence.query_time_utc_ms == Q1  # fence unmoved
    assert 1002 in snapshot.vehicle_entries and 1003 in snapshot.vehicle_entries


def test_equal_querytime_full_is_never_authoritative():
    state, session = open_state(_context())
    _base_state(session)
    # Same queryTime as the committed fence: equality can never authorize
    # another absence pass, even with a higher sequence and complete list.
    result = session.accept(
        _full(2, Q1, [vehicle_position(1001, time_ms=Q1)])
    )
    snapshot = state.snapshot()
    assert result.authoritative is False
    assert StateReason.EQUAL_SNAPSHOT_TIME in result.reasons
    assert {1001, 1002, 1003} <= set(snapshot.vehicle_entries)


def test_older_querytime_full_is_ignored_whole_including_absence():
    state, session = open_state(_context())
    _base_state(session)
    before = state.snapshot()
    result = session.accept(
        _full(2, Q0, [vehicle_position(1001, time_ms=Q0)])
    )
    after = state.snapshot()
    assert result.decision is ApplyDecision.IGNORED_OLDER
    assert result.reasons == (StateReason.OLDER_SOURCE_TIMESTAMP,)
    assert after.revision == before.revision
    assert dict(after.vehicle_entries) == dict(before.vehicle_entries)
    assert after.feed_views[PollFeed.VEHICLES].source_time_high_water_ms == Q1


def test_duplicate_raw_ids_block_the_absence_pass():
    state, session = open_state(_context())
    _base_state(session)
    result = session.accept(
        _full(
            2,
            Q2,
            [vehicle_position(1001, time_ms=Q2),
             vehicle_position(1001, time_ms=Q2)],
        )
    )
    snapshot = state.snapshot()
    assert result.authoritative is False
    assert StateReason.DUPLICATE_VEHICLE_ID in result.reasons
    assert snapshot.absence_fence.query_time_utc_ms == Q1
    assert {1002, 1003} <= set(snapshot.vehicle_entries)


# ---------------------------------------------------------------------------
# Row 7: authorized omission, complete-empty removal, raw-ID presence
# ---------------------------------------------------------------------------


def test_authorized_full_omission_removes_absent_vehicles_ascending():
    state, session = open_state(_context())
    _base_state(session)
    result = session.accept(
        _full(2, Q2, [vehicle_position(1001, time_ms=Q2)])
    )
    snapshot = state.snapshot()
    assert result.authoritative is True
    assert result.removed_vehicle_ids == (1002, 1003)  # ascending
    assert set(snapshot.vehicle_entries) == {1001}
    fence = snapshot.absence_fence
    assert fence.query_time_utc_ms == Q2
    assert fence.seen_ids == frozenset({1001})
    assert fence.authority_basis == "s12_complete_default_collection"
    assert snapshot.max_vehicle_ids == (1001,)


def test_complete_empty_authorized_full_reconciles_to_empty():
    state, session = open_state(_context())
    _base_state(session)
    result = session.accept(_full(2, Q2, []))
    snapshot = state.snapshot()
    assert result.authoritative is True
    assert result.removed_vehicle_ids == (1001, 1002, 1003)
    assert snapshot.vehicle_entries == {}
    assert snapshot.max_vehicle_ids == ()
    assert snapshot.absence_fence.seen_ids == frozenset()


def test_all_raw_ids_are_source_presence_not_just_the_max_projection():
    state, session = open_state(_context())
    _base_state(session)
    snapshot = state.snapshot()
    # All three relevance classes are retained as source observations.
    assert set(snapshot.vehicle_entries) == {1001, 1002, 1003}
    assert snapshot.max_vehicle_ids == (1001,)  # filtered projection only
    # A filtered record never becomes a false absence: the NON_MAX vehicle
    # present in the raw collection is retained and then removed only by a
    # later authorized omission.
    session.accept(
        _full(2, Q2, [vehicle_position(1002, time_ms=Q2, route_number=1,
                                       vehicle_type="bus")])
    )
    after = state.snapshot()
    assert set(after.vehicle_entries) == {1002}
    assert after.vehicle_entries[1002].matched.classification is (
        RelevanceClassification.NON_MAX
    )
    assert after.max_vehicle_ids == ()
    assert after.absence_fence.seen_ids == frozenset({1002})


# ---------------------------------------------------------------------------
# Row 8: ordering safeguards
# ---------------------------------------------------------------------------


def test_late_full_older_than_incremental_is_ignored_whole():
    state, session = open_state(_context())
    _base_state(session)  # high-water Q1
    session.accept(
        incremental_delivery(
            [vehicle_position(1001, time_ms=Q3)],
            query_time_ms=Q3,
            since_ms=Q1,
            sequence=2,
        )
    )
    before = state.snapshot()
    # A late full with queryTime Q2 (< incremental Q3), higher sequence.
    late = session.accept(
        _full(3, Q2, [vehicle_position(1004, time_ms=Q2)])
    )
    after = state.snapshot()
    assert late.decision is ApplyDecision.IGNORED_OLDER
    assert late.reasons == (StateReason.OLDER_SOURCE_TIMESTAMP,)
    assert after.revision == before.revision
    # The newer incremental state stands; 1002/1003 were not removed.
    assert after.vehicle_entries[1001].raw.time_utc_ms == Q3
    assert {1002, 1003} <= set(after.vehicle_entries)
    assert 1004 not in after.vehicle_entries
    assert after.feed_views[PollFeed.VEHICLES].source_time_high_water_ms == Q3


def test_full_with_newer_querytime_but_older_position_keeps_newer():
    state, session = open_state(_context())
    _base_state(session)
    session.accept(
        incremental_delivery(
            [vehicle_position(1001, time_ms=Q3)],
            query_time_ms=Q3,
            since_ms=Q1,
            sequence=2,
        )
    )
    # An authorized full at Q4 carrying an older position for 1001.
    result = session.accept(
        _full(3, Q4, [vehicle_position(1001, time_ms=Q2)])
    )
    entry = state.snapshot().vehicle_entries[1001]
    assert result.authoritative is True  # the collection itself is newer
    assert StateReason.OLDER_VEHICLE_OBSERVATION in result.reasons
    assert entry.raw.time_utc_ms == Q3  # newer position retained
    assert entry.stamp.source_time_utc_ms == Q3


def test_future_dated_observation_survives_authorized_omission():
    state, session = open_state(_context())
    future = BASE_MS + 60_000
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=future)]))
    # An authorized full at Q2 omits 1001, but its raw observation time is
    # later than the snapshot queryTime: the entry survives (no removal).
    result = session.accept(_full(2, Q2, [vehicle_position(1002, time_ms=Q2)]))
    snapshot = state.snapshot()
    assert result.authoritative is True
    assert result.removed_vehicle_ids == ()  # nothing was actually removed
    assert 1001 in snapshot.vehicle_entries
    assert snapshot.vehicle_entries[1001].raw.time_utc_ms == future
    assert 1002 in snapshot.vehicle_entries
    assert snapshot.max_vehicle_ids == (1001, 1002)


def test_equal_querytime_full_never_deletes_absent_vehicles():
    state, session = open_state(_context())
    _base_state(session)
    result = session.accept(
        _full(2, Q1, [vehicle_position(1004, time_ms=Q1)])
    )
    snapshot = state.snapshot()
    assert result.authoritative is False
    assert StateReason.EQUAL_SNAPSHOT_TIME in result.reasons
    assert {1001, 1002, 1003} <= set(snapshot.vehicle_entries)


def test_receipt_and_request_wall_times_never_substitute_for_source_order():
    state, session = open_state(_context())
    qa, qb = Q1, Q2
    late_wall = BASE_MS + 1_000_000_000
    early_wall = BASE_MS + 10
    # Newer queryTime arrives first with a tiny receipt time; accepted.
    session.accept(
        _full(1, qb, [vehicle_position(1001, time_ms=qb)],
              receipt_ms=early_wall)
    )
    # An older queryTime with a much later receipt time cannot win.
    stale = session.accept(
        _full(2, qa, [vehicle_position(1002, time_ms=qa)],
              receipt_ms=late_wall)
    )
    snapshot = state.snapshot()
    assert stale.decision is ApplyDecision.IGNORED_OLDER
    assert snapshot.feed_views[PollFeed.VEHICLES].source_time_high_water_ms == qb
    assert snapshot.absence_fence.query_time_utc_ms == qb
    assert 1002 not in snapshot.vehicle_entries


# ---------------------------------------------------------------------------
# Row 9: absence fence, anti-resurrection and capacity boundaries
# ---------------------------------------------------------------------------


def test_absence_fence_blocks_pre_reconciliation_resurrection():
    state, session = open_state(_context())
    _base_state(session)
    # Authorized empty full removes everyone; fence = Q2 with an empty set.
    removal = session.accept(_full(2, Q2, []))
    assert removal.removed_vehicle_ids == (1001, 1002, 1003)
    # A later incremental carrying only pre-fence positions cannot
    # resurrect the removed vehicles (raw time <= fence queryTime).
    stale = session.accept(
        incremental_delivery(
            [vehicle_position(1001, time_ms=Q1)],
            query_time_ms=Q3,
            since_ms=Q2,
            sequence=3,
        )
    )
    snapshot = state.snapshot()
    assert 1001 not in snapshot.vehicle_entries
    assert StateReason.PRE_RECONCILIATION_OBSERVATION in stale.reasons
    # A genuinely fresh observation is admitted normally.
    fresh = session.accept(
        incremental_delivery(
            [vehicle_position(1001, time_ms=Q4)],
            query_time_ms=Q4,
            since_ms=Q3,
            sequence=4,
        )
    )
    snapshot = state.snapshot()
    assert 1001 in snapshot.vehicle_entries
    assert snapshot.vehicle_entries[1001].raw.time_utc_ms == Q4
    assert StateReason.PRE_RECONCILIATION_OBSERVATION not in fresh.reasons


def test_stale_replay_after_removal_is_safe_acknowledgment():
    state, session = open_state(_context())
    _base_state(session)
    removal_delivery = _full(2, Q2, [])
    session.accept(removal_delivery)
    before = state.snapshot()
    replay = session.accept(removal_delivery)
    after = state.snapshot()
    assert replay.decision is ApplyDecision.DUPLICATE_NOOP
    assert replay.revision == before.revision
    assert after.vehicle_entries == before.vehicle_entries == {}


def test_later_authoritative_full_grants_explicit_fresh_presence():
    state, session = open_state(_context())
    _base_state(session)
    session.accept(_full(2, Q2, []))  # remove everything; fence Q2, empty
    # A later authorized full containing 1001 evaluates collection presence
    # before the introduction rule: the vehicle is explicitly present.
    result = session.accept(
        _full(3, Q3, [vehicle_position(1001, time_ms=Q1)])
    )
    snapshot = state.snapshot()
    assert result.authoritative is True
    assert 1001 in snapshot.vehicle_entries
    assert snapshot.absence_fence.query_time_utc_ms == Q3
    assert snapshot.absence_fence.seen_ids == frozenset({1001})
    # Authority still cannot override per-ID regression checks: the stale
    # position (t=Q1) was not applied over anything newer (none existed).
    assert snapshot.vehicle_entries[1001].raw.time_utc_ms == Q1


# --- The two named fence-capacity regressions (plan §9 REV 2) --------------


def test_fence_presence_exact_capacity_is_allowed():
    state, session = open_state(
        _context(), max_vehicle_entries=3, max_vehicle_records_per_delivery=10
    )
    vehicles = [vehicle_position(2000 + i, time_ms=Q1) for i in range(3)]
    result = session.accept(_full(1, Q1, vehicles))
    snapshot = state.snapshot()
    assert result.authoritative is True
    assert snapshot.absence_fence.seen_ids == frozenset({2000, 2001, 2002})
    assert set(snapshot.vehicle_entries) == {2000, 2001, 2002}


def test_fence_presence_capacity_plus_one_rejects_entire_transaction(
    monkeypatch,
):
    state, session = open_state(
        _context(), max_vehicle_entries=4096,
        max_vehicle_records_per_delivery=5000,
    )
    vehicles = [
        vehicle_position(2000 + i, time_ms=Q1) for i in range(4097)
    ]
    matching_calls: list[object] = []
    patch_match(
        monkeypatch, lambda delivery, context: matching_calls.append(delivery)
    )
    with pytest.raises(StateCapacityError) as excinfo:
        session.accept(_full(1, Q1, vehicles))
    assert excinfo.value.code == "capacity_exceeded"
    # No matching where instrumented: the rejection precedes S11 entirely.
    assert matching_calls == []
    snapshot = state.snapshot()
    # Prior fence/data/order/revision intact: the whole transaction failed.
    assert snapshot.revision == 1
    assert snapshot.vehicle_entries == {}
    assert snapshot.absence_fence is None
    assert (
        snapshot.feed_views[PollFeed.VEHICLES].source_time_high_water_ms
        is None
    )
    assert snapshot.telemetry.total_events == 0


def test_fence_capacity_boundary_exact_4096_is_allowed():
    state, session = open_state(
        _context(), max_vehicle_entries=4096,
        max_vehicle_records_per_delivery=5000,
    )
    vehicles = [
        vehicle_position(2000 + i, time_ms=Q1) for i in range(4096)
    ]
    result = session.accept(_full(1, Q1, vehicles))
    snapshot = state.snapshot()
    assert result.authoritative is True
    assert len(snapshot.absence_fence.seen_ids) == 4096
    assert len(snapshot.vehicle_entries) == 4096
    assert snapshot.revision == 2


def test_retained_map_overflow_with_future_dated_protection_is_rejected():
    """A retained-map overflow is checked independently of the fence set."""
    state, session = open_state(
        _context(), max_vehicle_entries=2, max_vehicle_records_per_delivery=5
    )
    future = BASE_MS + 60_000
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=future)]))
    # An authorized full at Q2 adds two IDs while the future-dated 1001
    # survives omission: the final candidate overflows the 2-entry map.
    with pytest.raises(StateCapacityError) as excinfo:
        session.accept(
            _full(
                2,
                Q2,
                [vehicle_position(1002, time_ms=Q2),
                 vehicle_position(1003, time_ms=Q2)],
            )
        )
    assert excinfo.value.code == "capacity_exceeded"
    snapshot = state.snapshot()
    assert snapshot.revision == 2
    assert set(snapshot.vehicle_entries) == {1001}
    assert snapshot.absence_fence.query_time_utc_ms == Q1
    # No conflict-type errors leaked into a capacity refusal.
    assert not isinstance(excinfo.value, StateConflictError)


# ---------------------------------------------------------------------------
# Review correction regressions (independent review, November 10, 2026, F1)
# ---------------------------------------------------------------------------


def _seed_one():
    """One tracked MAX vehicle 1001 committed at Q1."""
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    return state, session


@pytest.mark.parametrize("field,bad", [
    ("key_present", 1), ("is_list", 1),
    ("key_present", "false"), ("is_list", "false"),
])
def test_non_boolean_collection_flags_cannot_authorize_removal(field, bad):
    """Malformed evidence flags are a typed precommit failure (review F1).

    Plan §9's authority table requires ``key_present is True`` and
    ``is_list is True`` — actual booleans, not merely truthy values.
    A strictly newer complete-empty full with malformed flags must raise
    ``StateInputError(code='invalid_delivery')`` and change nothing:
    no removal, no fence advance, no marker movement.
    """
    state, session = _seed_one()
    before = state.snapshot()
    kwargs = dict(key_present=True, is_list=True, raw_count=0, rejected_count=0)
    kwargs[field] = bad
    with pytest.raises(StateInputError) as excinfo:
        session.accept(
            vehicle_delivery(
                [],
                query_time_ms=Q2,
                sequence=2,
                evidence=VehicleCollectionEvidence(**kwargs),
            )
        )
    assert excinfo.value.code == "invalid_delivery"
    after = state.snapshot()
    assert after.revision == before.revision
    assert after.vehicle_entries == before.vehicle_entries
    # Fence and replay/source markers are all unchanged.
    assert after.absence_fence is not None
    assert after.absence_fence.query_time_utc_ms == Q1
    assert after.feed_views == before.feed_views


def test_literal_true_complete_empty_collection_can_remove():
    """Control: literal-True zero-count evidence still authorizes (review F1)."""
    state, session = _seed_one()
    result = session.accept(_full(2, Q2, []))
    assert result.decision is ApplyDecision.COMMITTED
    assert result.authoritative is True
    assert result.removed_vehicle_ids == (1001,)
    snapshot = state.snapshot()
    assert 1001 not in snapshot.vehicle_entries
    assert snapshot.absence_fence.query_time_utc_ms == Q2
