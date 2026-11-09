"""S11 feed-matching tests: vehicle/TripUpdate/alert classification matrix.

Plan §13 rows 5–9 mapped to named tests through the real adapter entry
point :func:`backend.trimet.feed_match.match_delivery`, using the synthetic
world from ``tests/_s11_helpers``:

- vehicle relevance with exact ``rail``/``bus`` literals, the owner's
  rail-on-bus-rule/b bus-on-MAX conflicts evaluated before ordinary
  filtering, unknown route/types (including whitespace-only), and honest
  partial/conflicting enrichment;
- TripUpdate full-index classification, descriptor conflicts, preservation
  of instance fields and distinct trip instances;
- alert reuse of the unchanged S9 classifier with metadata that never
  changes relevance;
- exact one-classification accounting with order/duplicates preserved.
"""

from __future__ import annotations

import pytest

from backend.gtfs.types import Route, ShapeGeometry, Trip
from backend.trimet.feed_match import match_delivery
from backend.trimet.match_errors import MatchReason
from backend.trimet.match_index import build_matching_context
from backend.trimet.match_types import (
    EnrichmentState,
    RelevanceClassification,
    StopResolutionStatus,
)
from tests._s11_helpers import (
    make_stop_time_event,
    make_stop_time_update,
    make_trip_update_entity,
    make_vehicle,
    install_socket_guard,
    location_child,
    route_child,
    synthetic_identity,
    synthetic_world,
    alerts_delivery,
    make_alert,
    tripupdates_delivery,
    vehicles_delivery,
)


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    install_socket_guard(monkeypatch)


@pytest.fixture()
def context():
    world = synthetic_world()
    return build_matching_context(
        synthetic_identity(), world.network, world.route_rows, world.trip_rows
    )


MAX = RelevanceClassification.MAX_RELEVANT
NON = RelevanceClassification.NON_MAX
UNC = RelevanceClassification.UNCLASSIFIED


# ---------------------------------------------------------------------------
# Vehicle relevance (plan §7 matrix)
# ---------------------------------------------------------------------------


def _vehicle(context, **kwargs):
    return match_delivery(vehicles_delivery([make_vehicle(**kwargs)]), context).records[0]


def test_max_rail_vehicle_is_relevant_with_route_metadata(context) -> None:
    record = _vehicle(context, vehicle_id=1, vehicle_type="rail", route_number=90)
    assert record.classification is MAX
    assert record.route_id == "90"
    assert isinstance(record.max_route, Route)
    assert record.max_route.route_long_name == "MAX C41F3E Line"
    assert record.enrichment is EnrichmentState.UNMATCHED  # no trip reported
    assert record.vehicle is not None  # raw observation referenced intact


def test_max_rail_with_consistent_trip_is_matched_with_shape(context) -> None:
    record = _vehicle(
        context, vehicle_id=2, route_number=90, trip_id="t90A", direction=0,
        next_loc_id=14000, next_stop_seq=1,
    )
    assert record.enrichment is EnrichmentState.MATCHED
    assert isinstance(record.trip, Trip) and record.trip.trip_id == "t90A"
    assert isinstance(record.shape, ShapeGeometry) and record.shape.is_usable
    assert record.direction_conflict is False
    assert record.direction_source is None
    nxt = record.next_stop
    assert nxt.status is StopResolutionStatus.RESOLVED
    assert nxt.visit.stop_id == "14000"
    assert nxt.stop is not None


def test_missing_reported_direction_gets_explicit_static_provenance(context) -> None:
    record = _vehicle(
        context, vehicle_id=3, route_number=90, trip_id="t90E", direction=None
    )
    assert record.enrichment is EnrichmentState.MATCHED
    assert record.static_direction == 0
    assert record.direction_source == "static_trip"
    assert record.direction_conflict is False


def test_disagreeing_direction_keeps_reported_data_and_flags_conflict(context) -> None:
    record = _vehicle(
        context, vehicle_id=4, route_number=90, trip_id="t90A", direction=1
    )
    assert record.enrichment is EnrichmentState.PARTIAL  # honest partial result
    assert record.direction_conflict is True
    assert record.reported_direction == 1
    assert record.static_direction == 0
    assert MatchReason.DIRECTION_CONFLICT in record.reasons


@pytest.mark.parametrize(
    ("trip_id", "expected_reason", "expected_state", "trip_attached"),
    [
        ("t-nope", MatchReason.UNKNOWN_TRIP, EnrichmentState.UNMATCHED, False),
        ("t90D", MatchReason.TRIP_ROUTE_CONFLICT, EnrichmentState.CONFLICT, False),
        (None, MatchReason.TRIP_NOT_REPORTED, EnrichmentState.UNMATCHED, False),
    ],
)
def test_unavailable_trip_enrichment_stays_honorable(
    context, trip_id, expected_reason, expected_state, trip_attached
) -> None:
    record = _vehicle(context, vehicle_id=5, route_number=90, trip_id=trip_id)
    assert record.classification is MAX  # vehicle stays relevant
    assert record.enrichment is expected_state
    assert expected_reason in record.reasons
    assert (record.trip is not None) is trip_attached


def test_unusable_shape_is_partial_not_discarding(context) -> None:
    record = _vehicle(
        context, vehicle_id=6, route_number=90, trip_id="t90C", direction=1
    )
    assert record.classification is MAX
    assert record.enrichment is EnrichmentState.PARTIAL
    assert record.trip is not None
    assert record.shape is None  # unusable shape is never attached
    assert MatchReason.UNUSABLE_SHAPE in record.reasons


def test_missing_shape_is_partial(context) -> None:
    record = _vehicle(
        context, vehicle_id=7, route_number=90, trip_id="t90B"
    )
    assert record.enrichment is EnrichmentState.PARTIAL
    assert MatchReason.MISSING_SHAPE in record.reasons


def test_next_stop_resolution_failures_are_partial_annotations(context) -> None:
    record = _vehicle(
        context, vehicle_id=8, route_number=90, trip_id="t90A",
        next_loc_id=99999, next_stop_seq=None,
    )
    assert record.enrichment is EnrichmentState.PARTIAL
    assert record.next_stop.status is StopResolutionStatus.UNRESOLVED
    assert MatchReason.UNKNOWN_STOP_ID in record.reasons


def test_rail_on_bus_route_is_unclassified_owner_rule(context) -> None:
    record = _vehicle(
        context, vehicle_id=9, vehicle_type="rail", route_number=1
    )
    assert record.classification is UNC
    assert MatchReason.TYPE_ROUTE_CONFLICT in record.reasons
    assert record.enrichment is None  # filtered from MAX output, kept raw


def test_bus_on_max_route_is_unclassified(context) -> None:
    record = _vehicle(
        context, vehicle_id=10, vehicle_type="bus", route_number=200
    )
    assert record.classification is UNC
    assert MatchReason.TYPE_ROUTE_CONFLICT in record.reasons


def test_known_non_max_rail_filters_normally(context) -> None:
    # Streetcar 193 is rail-served but its static mode is 0, not bus(3).
    record = _vehicle(
        context, vehicle_id=11, vehicle_type="rail", route_number=193
    )
    assert record.classification is NON
    assert record.reasons == ()  # ordinary filtering: no diagnostic flood


def test_bus_on_bus_route_filters_normally(context) -> None:
    record = _vehicle(
        context, vehicle_id=12, vehicle_type="bus", route_number=74
    )
    assert record.classification is NON


@pytest.mark.parametrize("bad_type", ["R", "Rail", "  ", "bash", ""])
def test_unsupported_vehicle_type_strings_are_unclassified(context, bad_type) -> None:
    record = _vehicle(
        context, vehicle_id=13, vehicle_type=bad_type, route_number=90
    )
    assert record.classification is UNC
    assert MatchReason.UNKNOWN_VEHICLE_TYPE in record.reasons


def test_unknown_route_is_unclassified(context) -> None:
    record = _vehicle(
        context, vehicle_id=14, vehicle_type="rail", route_number=9999
    )
    assert record.classification is UNC
    assert MatchReason.UNKNOWN_ROUTE in record.reasons


def test_non_max_vehicle_with_max_trip_reference_is_unclassified(context) -> None:
    record = _vehicle(
        context, vehicle_id=15, vehicle_type="bus", route_number=74,
        trip_id="t90A",  # a MAX trip — contradictory enrichment
    )
    assert record.classification is UNC
    assert MatchReason.TRIP_ROUTE_CONFLICT in record.reasons


def test_non_max_vehicle_with_unknown_trip_stays_filtered(context) -> None:
    record = _vehicle(
        context, vehicle_id=16, vehicle_type="bus", route_number=74,
        trip_id="t-nope",
    )
    assert record.classification is NON


def test_non_max_vehicle_with_non_max_trip_stays_filtered(context) -> None:
    record = _vehicle(
        context, vehicle_id=17, vehicle_type="bus", route_number=1,
        trip_id="tb1",
    )
    assert record.classification is NON


# ---------------------------------------------------------------------------
# TripUpdates (plan §8–§9)
# ---------------------------------------------------------------------------


def _tripupdate(context, **entity_kwargs):
    entity = make_trip_update_entity(**entity_kwargs)
    return match_delivery(tripupdates_delivery([entity]), context).records[0]


def test_known_max_trip_update_is_relevant_and_enriched(context) -> None:
    record = _tripupdate(
        context,
        entity_id="e1",
        trip_id="t90A",
        stop_updates=[
            make_stop_time_update(1, "14000",
                                  departure=make_stop_time_event(time_raw_seconds=25230)),
        ],
    )
    assert record.classification is MAX
    assert record.static_route_id == "90"
    assert record.trip is not None
    assert record.enrichment is EnrichmentState.MATCHED
    assert record.stop_visits[0].resolution.visit.stop_sequence == 1


def test_unknown_trip_update_is_unclassified_not_non_max(context) -> None:
    record = _tripupdate(context, entity_id="e2", trip_id="unknown-t")
    assert record.classification is UNC
    assert MatchReason.UNKNOWN_TRIP in record.reasons
    assert record.static_route_id is None
    assert record.entity.trip_update.trip.trip_id == "unknown-t"


def test_known_non_max_trip_updates_filter_normally(context) -> None:
    record = _tripupdate(context, entity_id="e3", trip_id="tb1")
    assert record.classification is NON
    assert record.static_route_id == "1"
    assert record.enrichment is None


def test_descriptor_route_conflict_blocks_confident_enrichment(context) -> None:
    record = _tripupdate(
        context, entity_id="e4", trip_id="t90A", route_id="100"
    )
    assert record.classification is MAX  # classification stays trip-index based
    assert record.descriptor_route_conflict is True
    assert record.enrichment is EnrichmentState.CONFLICT
    assert record.trip is None
    assert record.stop_visits == ()


def test_descriptor_route_conflict_on_non_max_filters_normally(context) -> None:
    record = _tripupdate(
        context, entity_id="e5", trip_id="tb1", route_id="90"
    )
    assert record.classification is NON
    assert MatchReason.TRIP_ROUTE_CONFLICT in record.reasons


def test_repeated_stop_id_update_is_ambiguous_sequence_disambiguates(context) -> None:
    ambiguous = _tripupdate(
        context, entity_id="e6", trip_id="t90D",
        stop_updates=[make_stop_time_update(None, "14000")],
    )
    assert ambiguous.stop_visits[0].resolution.status is StopResolutionStatus.AMBIGUOUS
    assert ambiguous.enrichment is EnrichmentState.PARTIAL
    disambiguated = _tripupdate(
        context, entity_id="e7", trip_id="t90D",
        stop_updates=[make_stop_time_update(2, "14000")],
    )
    assert disambiguated.stop_visits[0].resolution.visit.stop_sequence == 2


def test_dangling_visit_keeps_prediction_and_marks_missing_details(context) -> None:
    record = _tripupdate(
        context, entity_id="e8", trip_id="t100A",
        stop_updates=[make_stop_time_update(5, "14024-dangling")],
    )
    sv = record.stop_visits[0].resolution
    assert sv.status is StopResolutionStatus.RESOLVED
    assert sv.stop_details_missing is True
    assert sv.stop is None
    assert record.enrichment is EnrichmentState.PARTIAL


def test_instance_fields_and_is_deleted_are_preserved_and_annotated(context) -> None:
    entity = make_trip_update_entity(
        "e9", "t90A", start_date="20261013", start_time="06:10:00",
        is_deleted=True,
    )
    record = match_delivery(tripupdates_delivery([entity]), context).records[0]
    assert record.classification is MAX
    assert record.unsupported_operational_relationship is True
    assert MatchReason.UNSUPPORTED_OPERATIONAL_RELATIONSHIP in record.reasons
    raw = record.entity.trip_update.trip
    assert raw.start_date == "20261013"
    assert raw.start_time == "06:10:00"


def test_non_scheduled_relationship_is_annotated_not_guessed(context) -> None:
    record = _tripupdate(
        context, entity_id="e10", trip_id="t90A", schedule_relationship_raw=3
    )
    assert record.unsupported_operational_relationship is True
    assert record.enrichment is EnrichmentState.MATCHED  # annotation only


def test_missing_vehicle_counterpart_retains_entity(context) -> None:
    record = _tripupdate(context, entity_id="e11", trip_id="t90A", vehicle_id=None)
    assert record.classification is MAX
    assert record.entity.trip_update.vehicle_id is None


def test_distinct_instances_of_one_trip_stay_separate(context) -> None:
    batch = match_delivery(
        tripupdates_delivery([
            make_trip_update_entity("a", "t90A", start_date="20261013"),
            make_trip_update_entity("b", "t90A", start_date="20261014"),
        ]),
        context,
    )
    assert batch.input_count == 2
    assert batch.records[0].entity is not batch.records[1].entity
    assert batch.max_relevant_count == 2


def test_stop_update_order_and_duplicates_preserved(context) -> None:
    batch = match_delivery(
        tripupdates_delivery([
            make_trip_update_entity(
                "e12", "t90A",
                stop_updates=[
                    make_stop_time_update(2, "14001"),
                    make_stop_time_update(1, "14000"),
                    make_stop_time_update(1, "14000"),
                ],
            ),
        ]),
        context,
    )
    visits = batch.records[0].stop_visits
    assert [v.resolution.visit.stop_sequence for v in visits] == [2, 1, 1]
    assert visits[1].update is not visits[2].update


# ---------------------------------------------------------------------------
# Alerts (plan §10): the S9 classifier reused unchanged
# ---------------------------------------------------------------------------


def _alert(context, **kwargs):
    return match_delivery(alerts_delivery([make_alert(**kwargs)]), context).records[0]


def test_system_wide_alert_affects_whole_context_max_set(context) -> None:
    record = _alert(context, alert_id=1, system_wide_flag=True)
    assert record.classification is MAX
    assert record.enrichment is EnrichmentState.MATCHED
    assert [route.route_id for route in record.affected_max_routes] == [
        "90", "100", "190", "200", "290"
    ]


def test_max_route_child_alert_is_relevant(context) -> None:
    record = _alert(
        context, alert_id=2, routes=[route_child(100), route_child(74)]
    )
    assert record.classification is MAX
    assert [route.route_id for route in record.affected_max_routes] == ["100"]


def test_non_max_scoped_alert_filters(context) -> None:
    record = _alert(context, alert_id=3, routes=[route_child(74)])
    assert record.classification is NON


def test_location_only_alert_stays_unclassified_with_classifier_diag(context) -> None:
    record = _alert(context, alert_id=4, locations=[location_child(5712)])
    assert record.classification is UNC
    assert record.result.classification.value == "unclassified"
    assert record.result.diagnostics != ()  # S9 diagnostic preserved
    # Location metadata can never escalate relevance: no MAX routes attached.
    assert record.affected_max_routes == ()


def test_unscoped_alert_stays_unclassified(context) -> None:
    record = _alert(context, alert_id=5)
    assert record.classification is UNC


# ---------------------------------------------------------------------------
# Accounting + order preservation (plan §6)
# ---------------------------------------------------------------------------


def test_classification_counts_sum_to_input_count(context) -> None:
    delivery = vehicles_delivery(
        [
            make_vehicle(1, route_number=90, vehicle_type="rail"),
            make_vehicle(2, route_number=74, vehicle_type="bus"),
            make_vehicle(3, route_number=1, vehicle_type="rail"),
            make_vehicle(4, route_number=9999, vehicle_type="rail"),
            make_vehicle(5, route_number=90, vehicle_type="rail", trip_id="t90A",
                         direction=0),
        ]
    )
    batch = match_delivery(delivery, context)
    assert batch.input_count == 5
    assert (
        batch.max_relevant_count + batch.non_max_count + batch.unclassified_count
        == batch.input_count
        == 5
    )
    assert batch.max_relevant_count == 2
    assert batch.non_max_count == 1
    assert batch.unclassified_count == 2


def test_repeated_vehicle_records_are_not_deduplicated(context) -> None:
    twin = make_vehicle(77, route_number=90, vehicle_type="rail")
    batch = match_delivery(vehicles_delivery([twin, twin]), context)
    assert batch.input_count == 2
    assert batch.records[0].vehicle is batch.records[1].vehicle
    assert batch.records[0] is not batch.records[1]


def test_order_preserved_and_enrichment_counts_consistent(context) -> None:
    delivery = vehicles_delivery(
        [
            make_vehicle(1, route_number=90, trip_id="t90A", direction=0),
            make_vehicle(2, route_number=90, trip_id="t90C", direction=1),
            make_vehicle(3, route_number=90),  # no trip
        ]
    )
    batch = match_delivery(delivery, context)
    assert [r.vehicle.vehicle_id for r in batch.records] == [1, 2, 3]
    assert batch.max_relevant_count == 3
    assert batch.matched_count + batch.partial_count + batch.unmatched_count == 3
    assert batch.matched_count == 1
    assert batch.partial_count == 1
    assert batch.unmatched_count == 1


def test_reason_totals_are_exact_per_feed(context) -> None:
    delivery = vehicles_delivery(
        [
            make_vehicle(1, route_number=9999, vehicle_type="rail"),
            make_vehicle(2, route_number=8888, vehicle_type="rail"),
        ]
    )
    batch = match_delivery(delivery, context)
    assert batch.reason_totals[MatchReason.UNKNOWN_ROUTE] == 2
    assert batch.omitted_match_diagnostics == 0
    assert len(batch.diagnostics) == 2


# ---------------------------------------------------------------------------
# M1-tolerated visitless MAX trips (owner review Finding 1, 2026-11-08)
# ---------------------------------------------------------------------------


@pytest.fixture()
def visitless_context():
    """Same synthetic world but ``t90A`` has no stop-time rows at all."""
    import dataclasses

    world = synthetic_world()
    stop_times = {
        trip_id: entries
        for trip_id, entries in world.network.stop_times.items()
        if trip_id != "t90A"
    }
    network = dataclasses.replace(world.network, stop_times=stop_times)
    return build_matching_context(
        synthetic_identity(), network, world.route_rows, world.trip_rows
    )


def test_visitless_trip_vehicle_stays_max_and_batch_survives(
    visitless_context,
) -> None:
    """A MAX vehicle on a visitless trip must not abort the batch.

    The known MAX classification, raw record, trip and shape references all
    survive; the requested stop enrichment resolves unresolved/partial with
    the existing reason codes; an unrelated second record still matches.
    """
    delivery = vehicles_delivery(
        [
            make_vehicle(31, route_number=90, vehicle_type="rail",
                         trip_id="t90A", direction=0,
                         next_loc_id=14000, next_stop_seq=1),
            make_vehicle(32, route_number=90, vehicle_type="rail",
                         trip_id="t90E", direction=0),
        ]
    )
    batch = match_delivery(delivery, visitless_context)
    affected, unaffected = batch.records
    assert affected.classification is RelevanceClassification.MAX_RELEVANT
    assert isinstance(affected.trip, Trip) and affected.trip.trip_id == "t90A"
    assert affected.shape.is_usable
    assert affected.enrichment is EnrichmentState.PARTIAL
    assert affected.next_stop.status is StopResolutionStatus.UNRESOLVED
    assert MatchReason.UNKNOWN_STOP_SEQUENCE in affected.reasons
    # The unrelated record in the same delivery is unaffected.
    assert unaffected.vehicle.vehicle_id == 32
    assert unaffected.enrichment is EnrichmentState.MATCHED
    assert batch.input_count == 2
    assert batch.max_relevant_count == 2


def test_visitless_trip_tripupdate_resolves_unresolved_and_survives(
    visitless_context,
) -> None:
    entity = make_trip_update_entity(
        "v1", "t90A",
        stop_updates=[make_stop_time_update(1, "14000")],
    )
    other = make_trip_update_entity(
        "v2", "t90E", stop_updates=[make_stop_time_update(0, "14001")],
    )
    batch = match_delivery(
        tripupdates_delivery([entity, other]), visitless_context
    )
    affected, unaffected = batch.records
    assert affected.classification is RelevanceClassification.MAX_RELEVANT
    assert affected.static_route_id == "90"
    assert affected.trip is not None
    sv = affected.stop_visits[0].resolution
    assert sv.status is StopResolutionStatus.UNRESOLVED
    assert MatchReason.UNKNOWN_STOP_SEQUENCE in sv.reasons
    assert affected.enrichment is EnrichmentState.PARTIAL
    assert unaffected.classification is RelevanceClassification.MAX_RELEVANT
    assert unaffected.stop_visits[0].resolution.visit.stop_sequence == 0
    assert batch.input_count == 2
