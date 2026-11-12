"""S12 positive merge, per-feed batch storage and capacity tests.

Stage S12 (M2 plan §8, §10, §12; implementation prompt §4–§6).  Covers
acceptance matrix rows 4 (merge/reuse/ties/duplicates), 5 (non-removal on
incremental omission), 6 (evidence invariants), 10 (enrichment never
discards a MAX vehicle), 11 (secondary feeds), 15 (capacity boundaries and
diagnostics retention) and 16 (no partial publication on failure).
"""
from __future__ import annotations

import dataclasses
import weakref

import pytest

import backend.trimet.live_state as _live_state_module

from tests._s12_helpers import s12_deny_all_sockets  # noqa: F401
from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    SHA_B,
    alerts_delivery,
    complete_evidence,
    incremental_delivery,
    make_alert,
    make_stop_time_event,
    make_stop_time_update,
    make_trip_update_entity,
    open_state,
    patch_match,
    route_child,
    s12_context,
    s12_world,
    tripupdates_delivery,
    vehicle_delivery,
    vehicle_position,
)

from backend.trimet.feed_types import AlertLocation
from backend.trimet.match_types import RelevanceClassification
from backend.trimet.poll_types import PollFeed, VehicleCollectionEvidence
from backend.trimet.state_errors import (
    StateCapacityError,
    StateInputError,
)
from backend.trimet.state_types import (
    ApplyDecision,
    StateLimits,
    StateReason,
)

Q1 = BASE_MS
Q2 = BASE_MS + 1_000
Q3 = BASE_MS + 2_000
Q4 = BASE_MS + 3_000
T1 = BASE_MS + 1_000  # whole-second TripUpdates header timestamp
T2 = T1 + 1_000


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


def _incr(seq, qt, since, vehicles, **kwargs):
    return incremental_delivery(
        vehicles, query_time_ms=qt, since_ms=since, sequence=seq, **kwargs
    )


# ---------------------------------------------------------------------------
# Row 4: positive merge, reuse, ties, duplicates, source-value integrity
# ---------------------------------------------------------------------------


def test_incremental_merge_updates_entry_and_keeps_raw_values_intact():
    state, session = open_state(_context())
    original = vehicle_position(1001, time_ms=Q1)
    session.accept(_full(1, Q1, [original]))
    newer = vehicle_position(1001, time_ms=Q2, latitude=45.55)
    session.accept(_incr(2, Q2, Q1, [newer]))
    entry = state.snapshot().vehicle_entries[1001]
    # The raw S9 record is referenced intact, never rewritten.
    assert entry.raw is newer
    assert (entry.raw.latitude, entry.raw.time_utc_ms) == (45.55, Q2)
    # The stamp records the delivery that carries the accepted observation.
    assert entry.stamp.sequence == 2
    assert entry.stamp.source_time_utc_ms == Q2
    assert entry.matched.classification is RelevanceClassification.MAX_RELEVANT


def test_vehicle_id_reuse_with_newer_observation_changes_classification():
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    # The same ID reappears as a bus on route 1 (NON_MAX), genuinely newer.
    reused = vehicle_position(
        1001, time_ms=Q2, route_number=1, vehicle_type="bus"
    )
    session.accept(_incr(2, Q2, Q1, [reused]))
    snapshot = state.snapshot()
    entry = snapshot.vehicle_entries[1001]
    assert entry.raw is reused
    assert entry.matched.classification is RelevanceClassification.NON_MAX
    # The old record left the MAX projection with the replaced observation.
    assert snapshot.max_vehicle_ids == ()
    # UNCLASSIFIED stays diagnosable in the full retained map.
    assert 1001 in snapshot.vehicle_entries


def test_unclassified_newer_observation_replaces_and_stays_diagnosable():
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    # Rail reported on static bus route 1: an explicit S11 conflict record.
    conflict = vehicle_position(
        1001, time_ms=Q2, route_number=1, vehicle_type="rail"
    )
    session.accept(_incr(2, Q2, Q1, [conflict]))
    snapshot = state.snapshot()
    assert snapshot.vehicle_entries[1001].matched.classification is (
        RelevanceClassification.UNCLASSIFIED
    )
    assert snapshot.max_vehicle_ids == ()


def test_highest_rank_tie_without_retained_value_never_wins_arbitrarily():
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1002, time_ms=Q1)]))
    # Two records for the same new ID at the same rank with different content.
    a = vehicle_position(1001, time_ms=Q2, latitude=45.51)
    b = vehicle_position(1001, time_ms=Q2, latitude=45.52)
    result = session.accept(_incr(2, Q2, Q1, [a, b]))
    snapshot = state.snapshot()
    assert 1001 not in snapshot.vehicle_entries  # no arbitrary winner
    assert 1002 in snapshot.vehicle_entries  # unrelated ID still merged
    assert StateReason.OBSERVATION_TIE_CONFLICT in result.reasons
    # Source presence still includes the conflicted ID in any later fence.
    fence_snapshot = session.accept(
        _full(3, Q3, [vehicle_position(1002, time_ms=Q3)])
    )
    assert fence_snapshot.authoritative is True
    assert state.snapshot().absence_fence.seen_ids == frozenset({1002})


def test_retained_value_resolves_an_identical_rank_tie():
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    retained_b = vehicle_position(1001, time_ms=Q2, latitude=45.53)
    session.accept(_incr(2, Q2, Q1, [retained_b]))
    # Equal rank (same time, same queryTime) with conflicting raw content:
    # the retained value resolves the rank; no replacement, conflict logged.
    conflicting = vehicle_position(1001, time_ms=Q2, latitude=45.99)
    result = session.accept(
        _full(3, Q2, [vehicle_position(1001, time_ms=Q2, latitude=45.99)])
    )
    del conflicting
    entry = state.snapshot().vehicle_entries[1001]
    assert entry.raw is retained_b
    assert StateReason.OBSERVATION_TIE_CONFLICT in result.reasons
    # Equal queryTime never authorizes another absence pass.
    assert result.authoritative is False
    assert StateReason.EQUAL_SNAPSHOT_TIME in result.reasons


def test_duplicate_ids_in_one_full_delivery_suppress_all_absence_removal():
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    # A full collection with a duplicated ID: no absence pass is authorized,
    # even though both duplicates are byte-identical.
    twin = vehicle_position(1001, time_ms=Q2)
    result = session.accept(_full(2, Q2, [twin, twin]))
    snapshot = state.snapshot()
    assert result.authoritative is False
    assert StateReason.DUPLICATE_VEHICLE_ID in result.reasons
    # The fence still reflects the first authorized pass (Q1): the
    # duplicate-ID full performed no absence pass at Q2.
    assert snapshot.absence_fence is not None
    assert snapshot.absence_fence.query_time_utc_ms == Q1
    assert snapshot.absence_fence.seen_ids == frozenset({1001})
    # A positive non-conflicting update may still apply.
    assert snapshot.vehicle_entries[1001].stamp.sequence == 2


# ---------------------------------------------------------------------------
# Row 5: absence from incremental/missing-list/partial evidence is never removal
# ---------------------------------------------------------------------------


def test_incremental_omission_and_empty_incremental_remove_nothing():
    state, session = open_state(_context())
    session.accept(
        _full(1, Q1, [vehicle_position(1001, time_ms=Q1),
                      vehicle_position(1002, time_ms=Q1)])
    )
    # Incremental omits 1002 entirely.
    session.accept(_incr(2, Q2, Q1, [vehicle_position(1001, time_ms=Q2)]))
    snapshot = state.snapshot()
    assert {1001, 1002} <= set(snapshot.vehicle_entries)
    assert snapshot.vehicle_entries[1002].stamp.sequence == 1
    # A valid empty incremental never removes anything either.
    result = session.accept(_incr(3, Q3, Q2, []))
    snapshot = state.snapshot()
    assert result.decision is ApplyDecision.COMMITTED  # high-water advance
    assert {1001, 1002} <= set(snapshot.vehicle_entries)


def _missing_list_evidence() -> VehicleCollectionEvidence:
    return VehicleCollectionEvidence(
        key_present=True, is_list=False, raw_count=None, rejected_count=None
    )


def test_missing_list_full_snapshot_never_authorizes_removal():
    state, session = open_state(_context())
    session.accept(
        _full(1, Q1, [vehicle_position(1001, time_ms=Q1),
                      vehicle_position(1002, time_ms=Q1)])
    )
    partial = vehicle_delivery(
        [vehicle_position(1001, time_ms=Q2)],
        query_time_ms=Q2,
        sequence=2,
        evidence=_missing_list_evidence(),
    )
    result = session.accept(partial)
    snapshot = state.snapshot()
    assert result.authoritative is False
    assert StateReason.INCOMPLETE_COLLECTION in result.reasons
    # The fence still reflects the first authorized pass (Q1).
    assert snapshot.absence_fence.query_time_utc_ms == Q1
    assert snapshot.absence_fence.seen_ids == frozenset({1001, 1002})
    assert 1002 in snapshot.vehicle_entries  # not removed
    # A later authorized full with 1002 absent still removes it, proving
    # 1002 was retained rather than silently dropped.
    session.accept(_full(3, Q3, [vehicle_position(1001, time_ms=Q3)]))
    assert 1002 not in state.snapshot().vehicle_entries


def test_rejected_record_evidence_never_authorizes_removal():
    state, session = open_state(_context())
    session.accept(
        _full(1, Q1, [vehicle_position(1001, time_ms=Q1),
                      vehicle_position(1002, time_ms=Q1)])
    )
    evidence = VehicleCollectionEvidence(
        key_present=True, is_list=True, raw_count=2, rejected_count=1
    )
    result = session.accept(
        vehicle_delivery(
            [vehicle_position(1001, time_ms=Q2)],
            query_time_ms=Q2,
            sequence=2,
            evidence=evidence,
        )
    )
    snapshot = state.snapshot()
    assert result.authoritative is False
    assert StateReason.INCOMPLETE_COLLECTION in result.reasons
    assert 1002 in snapshot.vehicle_entries


# ---------------------------------------------------------------------------
# Row 6: structurally impossible evidence is a typed input error
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "evidence",
    [
        VehicleCollectionEvidence(
            key_present=True, is_list=True, raw_count=True, rejected_count=0
        ),
        VehicleCollectionEvidence(
            key_present=True, is_list=True, raw_count=1, rejected_count=-1
        ),
        VehicleCollectionEvidence(
            key_present=True, is_list=True, raw_count=True, rejected_count=True
        ),
    ],
)
def test_bool_or_negative_evidence_counts_are_typed_input_errors(evidence):
    state, session = open_state(_context())
    with pytest.raises(StateInputError) as excinfo:
        session.accept(
            vehicle_delivery(
                [vehicle_position(1001, time_ms=Q1)],
                query_time_ms=Q1,
                sequence=1,
                evidence=evidence,
            )
        )
    assert excinfo.value.code == "invalid_delivery"
    assert state.snapshot().revision == 1


def test_impossible_evidence_accounting_is_a_typed_input_error():
    state, session = open_state(_context())
    # raw_count must equal parsed + rejected for an explicit list.
    evidence = VehicleCollectionEvidence(
        key_present=True, is_list=True, raw_count=5, rejected_count=0
    )
    with pytest.raises(StateInputError) as excinfo:
        session.accept(
            vehicle_delivery(
                [vehicle_position(1001, time_ms=Q1)],
                query_time_ms=Q1,
                sequence=1,
                evidence=evidence,
            )
        )
    assert excinfo.value.code == "invalid_delivery"
    assert state.snapshot().vehicle_entries == {}


# ---------------------------------------------------------------------------
# Row 10: enrichment never discards a MAX vehicle
# ---------------------------------------------------------------------------


def test_visitless_max_trip_stays_tracked_through_state():
    state, session = open_state(_context())
    visitor = vehicle_position(1001, time_ms=Q1, trip_id="t90F")
    session.accept(_full(1, Q1, [visitor]))
    entry = state.snapshot().vehicle_entries[1001]
    assert entry.matched.classification is RelevanceClassification.MAX_RELEVANT
    # The visitless MAX trip itself resolves (M1-tolerated case) — the
    # vehicle is tracked with whatever enrichment S11 produced.
    assert 1001 in state.snapshot().max_vehicle_ids


def test_missing_or_conflicting_enrichment_never_discards_a_max_vehicle():
    state, session = open_state(_context())
    unknown_trip = vehicle_position(1001, time_ms=Q1, trip_id="t999")
    session.accept(_full(1, Q1, [unknown_trip]))
    snapshot = state.snapshot()
    entry = snapshot.vehicle_entries[1001]
    # Still MAX on its reported route; unmatched trip enrichment is separate.
    assert entry.matched.classification is RelevanceClassification.MAX_RELEVANT
    assert 1001 in snapshot.max_vehicle_ids
    assert entry.matched.trip is None


# ---------------------------------------------------------------------------
# Row 11: TripUpdates and alerts — newer/older/equal, empty replacement,
# metadata-only conflict booking, validation, instance fidelity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("feed_kind", ["tripupdates", "alerts"])
def test_secondary_feed_newer_older_and_equal_timestamp_rules(feed_kind):
    state, session = open_state(_context())

    def delivery(seq, source_time, entities_or_alerts, **kwargs):
        if feed_kind == "tripupdates":
            return tripupdates_delivery(
                entities_or_alerts, header_timestamp_ms=source_time,
                sequence=seq, **kwargs,
            )
        return alerts_delivery(
            entities_or_alerts, query_time_ms=source_time, sequence=seq,
            **kwargs,
        )

    def model_at(source_time, tag):
        if feed_kind == "tripupdates":
            return [make_trip_update_entity(f"e-{tag}", "t90A")]
        return [make_alert(tag)]

    feed = (
        PollFeed.TRIPUPDATES if feed_kind == "tripupdates" else PollFeed.ALERTS
    )

    first = session.accept(delivery(1, T1, model_at(T1, "a")))
    assert first.decision is ApplyDecision.COMMITTED
    view = state.snapshot().feed_views[feed]
    assert view.source_content.delivery.sequence == 1
    assert view.source_time_high_water_ms == T1

    # Older source timestamp: ignored whole; no booking, no revision change.
    before = state.snapshot()
    older = session.accept(delivery(2, T1 - 500, model_at(T1, "b")))
    after = state.snapshot()
    assert older.decision is ApplyDecision.IGNORED_OLDER
    assert older.reasons == (StateReason.OLDER_SOURCE_TIMESTAMP,)
    assert after.revision == before.revision
    assert after.feed_views[feed].source_content.delivery.sequence == 1

    # Higher sequence at the equal timestamp with conflicting content:
    # metadata-only booking; the source view and its times stay intact.
    conflict = session.accept(delivery(3, T1, model_at(T1, "c")))
    snapshot = state.snapshot()
    assert conflict.decision is ApplyDecision.METADATA_ONLY
    assert conflict.reasons == (StateReason.SOURCE_TIMESTAMP_CONFLICT,)
    assert conflict.revision == before.revision + 1
    assert snapshot.feed_views[feed].source_content.delivery.sequence == 1
    assert snapshot.feed_views[feed].source_time_high_water_ms == T1

    # An exact retry of the newly booked delivery is a duplicate no-op.
    retry = session.accept(delivery(3, T1, model_at(T1, "c")))
    assert retry.decision is ApplyDecision.DUPLICATE_NOOP
    assert retry.revision == conflict.revision


@pytest.mark.parametrize("feed_kind", ["tripupdates", "alerts"])
def test_secondary_feed_sequence_collision_and_view_integrity(feed_kind):
    state, session = open_state(_context())

    def delivery(seq, source_time, tag):
        if feed_kind == "tripupdates":
            return tripupdates_delivery(
                [make_trip_update_entity(f"e-{tag}", "t90A")],
                header_timestamp_ms=source_time, sequence=seq,
            )
        return alerts_delivery(
            [make_alert(tag)], query_time_ms=source_time, sequence=seq,
        )

    feed = (
        PollFeed.TRIPUPDATES if feed_kind == "tripupdates" else PollFeed.ALERTS
    )
    session.accept(delivery(1, T1, "a"))
    session.accept(delivery(2, T1, "b"))  # metadata-only booking at T1
    before = state.snapshot()
    with pytest.raises(Exception) as excinfo:
        session.accept(delivery(2, T1, "c"))
    assert excinfo.value.code == "sequence_collision"
    after = state.snapshot()
    assert after.revision == before.revision
    assert after.feed_views[feed].source_content.delivery.sequence == 1

    # A newer timestamp (with fresh content) replaces the prior view.
    newer = session.accept(delivery(3, T2, "d"))
    assert newer.decision is ApplyDecision.COMMITTED
    view = state.snapshot().feed_views[feed]
    assert view.source_content.delivery.sequence == 3
    assert view.source_time_high_water_ms == T2


def test_secondary_explicit_empty_replacement_takes_effect():
    """A newer timestamp with explicit empty content replaces the view."""
    state, session = open_state(_context())
    session.accept(
        tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A")],
            header_timestamp_ms=T1, sequence=1,
        )
    )
    session.accept(
        tripupdates_delivery([], header_timestamp_ms=T2, sequence=2)
    )
    view = state.snapshot().feed_views[PollFeed.TRIPUPDATES]
    assert view.source_content.delivery.sequence == 2
    assert view.source_content.delivery.model.entities == ()
    assert view.source_time_high_water_ms == T2

    session.accept(alerts_delivery([make_alert(9)], query_time_ms=T1, sequence=1))
    session.accept(alerts_delivery([], query_time_ms=T2, sequence=2))
    alert_view = state.snapshot().feed_views[PollFeed.ALERTS]
    assert alert_view.source_content.delivery.model.alerts == ()
    assert alert_view.source_time_high_water_ms == T2


def test_tripupdates_header_validation_is_typed_input():
    state, session = open_state(_context())
    # Not FULL_DATASET incrementality.
    with pytest.raises(StateInputError) as excinfo:
        session.accept(
            tripupdates_delivery(
                [make_trip_update_entity("e1", "t90A")],
                header_timestamp_ms=T1,
                sequence=1,
                incrementality_raw=1,
            )
        )
    assert excinfo.value.code == "invalid_delivery"
    # Inconsistent seconds/milliseconds pair.
    with pytest.raises(StateInputError) as excinfo:
        session.accept(
            tripupdates_delivery(
                [make_trip_update_entity("e1", "t90A")],
                header_timestamp_ms=T1,
                sequence=1,
                timestamp_pair_override=(T1 // 1000, T1 + 123),
            )
        )
    assert excinfo.value.code == "invalid_delivery"
    assert state.snapshot().feed_views[PollFeed.TRIPUPDATES].source_content is None


def test_secondary_instance_fields_flags_and_order_are_retained():
    state, session = open_state(_context())
    stop_updates = (
        make_stop_time_update(
            stop_sequence=1,
            stop_id="14000",
            arrival=make_stop_time_event(delay_seconds=-30),
        ),
        make_stop_time_update(
            stop_sequence=2,
            stop_id="14001",
            departure=make_stop_time_event(delay_seconds=45),
        ),
    )
    first = make_trip_update_entity("e1", "t90A", stop_updates=stop_updates)
    deleted = make_trip_update_entity("e2", "t90B", is_deleted=True)
    duplicate_trip = make_trip_update_entity("e3", "t90A")  # not collapsed
    session.accept(
        tripupdates_delivery(
            [first, deleted, duplicate_trip],
            header_timestamp_ms=T1,
            sequence=1,
        )
    )
    batch = state.snapshot().feed_views[PollFeed.TRIPUPDATES].source_content.batch
    records = batch.records
    assert [record.entity.entity_id for record in records] == ["e1", "e2", "e3"]
    # Instance fields, flags and distinct trips are preserved exactly.
    assert records[0].entity is first
    assert records[1].entity.is_deleted is True
    assert records[0].stop_visits[0].update.stop_id == "14000"
    assert records[0].stop_visits[0].update.arrival.delay_seconds == -30
    assert records[0].stop_visits[1].update.departure.delay_seconds == 45
    assert records[2].trip_id == "t90A"
    # Alerts: order, duplicates and child fields retained.
    alerts = [make_alert(7, routes=[route_child(90)]), make_alert(7)]
    session.accept(alerts_delivery(alerts, query_time_ms=T2, sequence=1))
    alert_batch = state.snapshot().feed_views[PollFeed.ALERTS].source_content.batch
    assert [record.alert.alert_id for record in alert_batch.records] == [7, 7]
    assert alert_batch.records[0].alert.routes[0].route_id == 90


def test_alert_nested_children_are_retained_without_inference():
    state, session = open_state(_context())
    alert = make_alert(
        5,
        routes=[route_child(90), route_child(1)],
        locations=[AlertLocation(location_id=14000)],
    )
    session.accept(alerts_delivery([alert], query_time_ms=T1, sequence=1))
    record = (
        state.snapshot()
        .feed_views[PollFeed.ALERTS]
        .source_content.batch.records[0]
    )
    assert record.alert is alert
    assert [child.route_id for child in record.alert.routes] == [90, 1]
    assert record.alert.locations[0].location_id == 14000


# ---------------------------------------------------------------------------
# Row 15: capacity boundaries; reconciliation can free capacity
# ---------------------------------------------------------------------------


def test_per_delivery_record_capacity_is_checked_before_matching(monkeypatch):
    state, session = open_state(
        _context(), max_vehicle_records_per_delivery=2
    )
    calls: list[object] = []
    patch_match(monkeypatch, lambda delivery, context: calls.append(delivery))
    with pytest.raises(StateCapacityError) as excinfo:
        session.accept(
            _full(1, Q1, [vehicle_position(1001 + i, time_ms=Q1)
                          for i in range(3)])
        )
    assert excinfo.value.code == "capacity_exceeded"
    # Expensive S11 matching never ran; nothing was published.
    assert calls == []
    snapshot = state.snapshot()
    assert snapshot.revision == 1
    assert snapshot.vehicle_entries == {}


def test_tripupdate_entity_and_nested_record_capacities():
    state, session = open_state(_context(), max_tripupdate_entities=1)
    with pytest.raises(StateCapacityError) as excinfo:
        session.accept(
            tripupdates_delivery(
                [
                    make_trip_update_entity("e1", "t90A"),
                    make_trip_update_entity("e2", "t90B"),
                ],
                header_timestamp_ms=T1,
                sequence=1,
            )
        )
    assert excinfo.value.code == "capacity_exceeded"

    state2, session2 = open_state(_context(), max_nested_feed_records=3)
    entities = [
        make_trip_update_entity(
            "e1",
            "t90A",
            stop_updates=(
                make_stop_time_update(stop_sequence=1,
                                      arrival=make_stop_time_event(0)),
                make_stop_time_update(stop_sequence=2,
                                      arrival=make_stop_time_event(1)),
            ),
        )
    ] * 2  # 4 aggregate stop updates > 3
    with pytest.raises(StateCapacityError) as excinfo:
        session2.accept(
            tripupdates_delivery(entities, header_timestamp_ms=T1, sequence=1)
        )
    assert excinfo.value.code == "capacity_exceeded"


def test_alert_record_and_nested_child_capacities():
    state, session = open_state(_context(), max_alert_records=1)
    with pytest.raises(StateCapacityError) as excinfo:
        session.accept(
            alerts_delivery(
                [make_alert(1), make_alert(2)], query_time_ms=T1, sequence=1
            )
        )
    assert excinfo.value.code == "capacity_exceeded"

    state2, session2 = open_state(_context(), max_nested_feed_records=2)
    crowded = make_alert(
        3, routes=[route_child(90), route_child(1)],
        locations=[AlertLocation(location_id=14000)],
    )
    with pytest.raises(StateCapacityError) as excinfo:
        session2.accept(
            alerts_delivery([crowded], query_time_ms=T1, sequence=1)
        )
    assert excinfo.value.code == "capacity_exceeded"  # 3 children > 2


def test_retained_map_capacity_rejected_then_reconciliation_frees_it():
    state, session = open_state(_context(), max_vehicle_entries=2)
    session.accept(_incr(1, Q1, BASE_MS, [vehicle_position(1001, time_ms=Q1)]))
    session.accept(_incr(2, Q2, Q1, [vehicle_position(1002, time_ms=Q2)]))
    before = state.snapshot()
    with pytest.raises(StateCapacityError) as excinfo:
        session.accept(_incr(3, Q3, Q2, [vehicle_position(1003, time_ms=Q3)]))
    assert excinfo.value.code == "capacity_exceeded"
    after = state.snapshot()
    # No silent eviction: data, markers and revision unchanged.
    assert after.revision == before.revision
    assert set(after.vehicle_entries) == {1001, 1002}
    # An authorized full can free capacity via valid reconciliation removals.
    result = session.accept(
        _full(4, Q4, [vehicle_position(1003, time_ms=Q4)])
    )
    snapshot = state.snapshot()
    assert result.authoritative is True
    assert result.removed_vehicle_ids == (1001, 1002)
    assert set(snapshot.vehicle_entries) == {1003}
    assert snapshot.absence_fence.seen_ids == frozenset({1003})


# ---------------------------------------------------------------------------
# Row 15: diagnostics retention 199/200/201 with exact totals
# ---------------------------------------------------------------------------


def test_state_diagnostics_cap_199_200_201_with_exact_totals():
    state, session = open_state(_context())
    events = 200
    for index in range(1, events + 1):
        session.accept(
            _incr(index, Q1 + index * 1_000, Q1 + (index - 1) * 1_000, [])
        )
    at_cap = state.snapshot()
    assert at_cap.telemetry.total_events == 200
    assert len(at_cap.telemetry.samples) == 200
    assert at_cap.telemetry.omitted_samples == 0
    assert at_cap.telemetry.reason_totals[StateReason.NON_FULL_REQUEST] == 200

    session.accept(
        _incr(201, Q1 + 201_000, Q1 + 200_000, [])
    )
    over_cap = state.snapshot()
    telemetry = over_cap.telemetry
    assert telemetry.total_events == 201
    assert len(telemetry.samples) == 200  # 199 real + 1 overflow sample
    assert telemetry.omitted_samples == 2
    assert telemetry.samples[-1].reason is StateReason.DIAGNOSTICS_OVERFLOW
    assert telemetry.samples[-1].feed is None
    assert telemetry.samples[0].reason is StateReason.NON_FULL_REQUEST
    assert telemetry.reason_totals[StateReason.NON_FULL_REQUEST] == 201
    assert sum(telemetry.reason_totals.values()) == 201


# ---------------------------------------------------------------------------
# Row 16: failures publish nothing; no anticipated error after publication
# ---------------------------------------------------------------------------


def test_matching_failure_publishes_no_partial_state(monkeypatch):
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    before = state.snapshot()

    def failing(delivery, context):
        raise RuntimeError("injected matching failure")

    patch_match(monkeypatch, failing)
    with pytest.raises(RuntimeError):
        session.accept(
            _incr(2, Q2, Q1, [vehicle_position(1002, time_ms=Q2)])
        )
    after = state.snapshot()
    assert after.revision == before.revision
    assert set(after.vehicle_entries) == {1001}
    assert after.telemetry.reason_totals == before.telemetry.reason_totals
    assert after.feed_views[PollFeed.VEHICLES].source_content.delivery.sequence == 1


def test_input_failures_leave_source_and_replay_markers_unchanged():
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    before = state.snapshot()
    with pytest.raises(StateInputError):
        session.accept(
            _full(2, Q2, [vehicle_position(1002, time_ms=Q2)],
                  http_status=201)
        )
    after = state.snapshot()
    assert after.revision == before.revision
    assert (
        after.feed_views[PollFeed.VEHICLES].source_content.delivery.sequence
        == before.feed_views[PollFeed.VEHICLES].source_content.delivery.sequence
    )
    assert (
        after.feed_views[PollFeed.VEHICLES].replay_delivery.sequence == 1
    )
    # Unused worlds keep the module honest about no other dataset use.
    assert SHA_B != SHA_A
    assert StateLimits().max_vehicle_entries == 4096


# ---------------------------------------------------------------------------
# Review correction regressions (independent review, November 10, 2026,
# F2 bounded collection + F3 envelope validation)
# ---------------------------------------------------------------------------


def test_many_decisions_are_bounded_while_being_collected(monkeypatch):
    """Per-event diagnostic objects are bounded during reduction (review F2).

    A single delivery with many per-record decision events must never
    materialize per-event diagnostic storage that scales with the delivery:
    one event object is created per counted event and either converted to a
    retained sample (under the cap) or dropped immediately.  Exact aggregate
    totals survive truncation (plan §12).  Weak-reference instrumentation
    counts simultaneously live event objects; a small constant of working
    objects above the configured sample budget is allowed.
    """
    state, session = open_state(_context(), max_state_diagnostic_samples=3)
    n = 512
    session.accept(
        _full(1, Q1, [vehicle_position(i + 1, time_ms=Q1) for i in range(n)])
    )
    live: dict[int, weakref.ref] = {}
    peak = 0
    original = _live_state_module._Decision

    def track(*args, **kwargs):
        nonlocal peak
        event = original(*args, **kwargs)
        key = id(event)
        live[key] = weakref.ref(
            event, lambda ref, key=key: live.pop(key, None)
        )
        peak = max(peak, len(live))
        return event

    monkeypatch.setattr(_live_state_module, "_Decision", track)
    session.accept(
        _incr(
            2,
            Q2,
            Q1,
            [vehicle_position(i + 1, time_ms=Q1 - 500) for i in range(n)],
        )
    )
    telemetry = state.snapshot().telemetry
    # Exact totals survive truncation: every older observation is counted.
    assert telemetry.reason_totals[StateReason.OLDER_VEHICLE_OBSERVATION] == n
    # Capped retention: 2 real samples + 1 overflow marker at cap 3.
    assert len(telemetry.samples) == 3
    assert telemetry.samples[-1].reason is StateReason.DIAGNOSTICS_OVERFLOW
    assert peak <= 3 + 4, (
        f"held {peak} live decision objects for a sample budget of 3"
    )


@pytest.mark.parametrize("changes", [
    {"elapsed_mono_ms": -1}, {"receipt_utc_ms": True},
])
def test_invalid_envelope_metadata_uses_typed_precommit_failure(changes):
    """Envelope metadata is validated before every acceptance branch (F3).

    Invalid request/receipt timing or elapsed time on a vehicle delivery
    raises the typed ``StateInputError(code='invalid_delivery')`` instead of
    leaking a raw ``ValueError`` from ``SourceStamp`` validators, and the
    rejected delivery changes nothing.
    """
    state, session = open_state(_context())
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    before = state.snapshot()
    with pytest.raises(StateInputError) as excinfo:
        session.accept(dataclasses.replace(_full(2, Q2, []), **changes))
    assert excinfo.value.code == "invalid_delivery"
    assert state.snapshot() == before


@pytest.mark.parametrize("feed", ["tripupdates", "alerts"])
def test_equal_source_time_conflict_cannot_book_invalid_envelope_metadata(
    feed,
):
    """Equal-source-time metadata-only booking validates the envelope (F3).

    A higher-sequence equal-source-time conflicting secondary delivery with
    invalid envelope metadata is a typed precommit failure: it must not be
    acknowledged on a normal return and must not book replay metadata.
    """
    state, session = open_state(_context())
    if feed == "tripupdates":
        session.accept(
            tripupdates_delivery(
                [make_trip_update_entity("first", "t90A")],
                header_timestamp_ms=T1,
                sequence=1,
            )
        )
        conflicting = tripupdates_delivery(
            [make_trip_update_entity("second", "t90B")],
            header_timestamp_ms=T1,
            sequence=2,
        )
    else:
        session.accept(
            alerts_delivery([make_alert(1)], query_time_ms=Q1, sequence=1)
        )
        conflicting = alerts_delivery(
            [make_alert(2)], query_time_ms=Q1, sequence=2
        )
    before = state.snapshot()
    with pytest.raises(StateInputError) as excinfo:
        session.accept(dataclasses.replace(conflicting, elapsed_mono_ms=-1))
    assert excinfo.value.code == "invalid_delivery"
    assert state.snapshot() == before
