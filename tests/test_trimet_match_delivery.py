"""S11 adapter tests: PollDelivery → MatchedBatch entry-point behavior.

Plan §13 rows 10–12 mapped to named tests through the canonical
:func:`backend.trimet.feed_match.match_delivery`:

- **Typed adapter failures** — a non-``PollDelivery``, a context that is not
  a :class:`MatchingContext`, and each crosswise feed/model disagreement
  raise :class:`MatchInputError`; an empty batch is never the outcome of
  invalid programmer input.
- **Envelope preservation** — purpose, sequence, timestamps, HTTP status,
  collection evidence and the S10 source parse diagnostics travel into the
  batch unchanged and separately from the new matching diagnostics.
- **Identity honesty** — a whole A/B context-pair delivery records each
  dataset's own identity and counts; cross-template contamination is
  impossible.
- **No completeness upgrade** — carrying weak collection evidence never
  upgrades, downgrades or drops a classification; evidence passes through
  verbatim.
- **Accounting/cap** — empty feeds produce zero-everything batches;
  reason-event totals stay exact at 199/200/201/many events while samples
  cap at 200 including the overflow summary entry.
"""

from __future__ import annotations

import pytest

from backend.trimet.feed_match import MAX_DIAGNOSTIC_ENTRIES, match_delivery
from backend.trimet.feed_types import FeedDiagnostic
from backend.trimet.match_errors import ContextPreparationError, MatchInputError, MatchReason
from backend.trimet.match_index import build_matching_context
from backend.trimet.match_types import (
    EnrichmentState,
    MatchingContext,
    RelevanceClassification,
)
from backend.trimet.poll_types import PollFeed, PollDelivery, VehicleCollectionEvidence
from tests._s11_helpers import (
    install_socket_guard,
    make_alert,
    make_delivery,
    make_trip_update_entity,
    make_vehicle,
    alerts_snapshot,
    alerts_delivery,
    make_tripupdates_feed,
    synthetic_identity,
    synthetic_world,
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


# ---------------------------------------------------------------------------
# Typed adapter failures (never an empty successful batch)
# ---------------------------------------------------------------------------


def test_non_poll_delivery_raises_typed_error(context) -> None:
    with pytest.raises(MatchInputError) as exc:
        match_delivery(object(), context)
    assert exc.value.code == "delivery_type_mismatch"


def test_non_matching_context_raises_typed_error() -> None:
    with pytest.raises(MatchInputError) as exc:
        match_delivery(vehicles_delivery([make_vehicle(1)]), object())
    assert exc.value.code == "invalid_context"


def test_all_feed_model_disagreements_raise_typed_error(context) -> None:
    wrong_by_feed = (
        (PollFeed.VEHICLES, alerts_snapshot([make_alert(1)])),
        (PollFeed.TRIPUPDATES, alerts_snapshot([make_alert(1)])),
        (PollFeed.ALERTS, make_tripupdates_feed([
            make_trip_update_entity("e1", "t90A"),
        ])),
    )
    for feed, wrong_model in wrong_by_feed:
        delivery = make_delivery(feed, wrong_model)
        with pytest.raises(MatchInputError) as exc:
            match_delivery(delivery, context)
        assert exc.value.code == "feed_model_mismatch"


def test_matched_batch_never_mutates_the_delivery(context) -> None:
    vehicle = make_vehicle(1, route_number=90, vehicle_type="rail")
    delivery = vehicles_delivery([vehicle])
    before = (delivery.diagnostics, delivery.evidence, delivery.model.vehicles)
    match_delivery(delivery, context)
    assert (delivery.diagnostics, delivery.evidence, delivery.model.vehicles) == before
    assert vehicle.vehicle_id == 1


# ---------------------------------------------------------------------------
# Envelope + source-diagnostics preservation
# ---------------------------------------------------------------------------


_SOURCE_DIAGNOSTIC = FeedDiagnostic(
    feed="vehicles",
    record_key="99",
    reason="invalid_field",
    field="type",
    detail="bounded detail",
)


def test_envelope_and_source_diagnostics_preserved(context) -> None:
    delivery = vehicles_delivery(
        [make_vehicle(1, route_number=90, vehicle_type="rail")],
        sequence=4321,
        evidence=VehicleCollectionEvidence(
            key_present=True, is_list=True, raw_count=1, rejected_count=1
        ),
        diagnostics=[_SOURCE_DIAGNOSTIC],
    )
    batch = match_delivery(delivery, context)
    assert batch.feed is PollFeed.VEHICLES
    assert batch.dataset_identity is context.identity
    assert batch.sequence == 4321
    assert batch.request_sent_utc_ms == delivery.request_sent_utc_ms
    assert batch.receipt_utc_ms == delivery.receipt_utc_ms
    assert batch.elapsed_mono_ms == delivery.elapsed_mono_ms
    assert batch.http_status == 200
    assert batch.evidence is delivery.evidence
    assert batch.input_count == 1
    # Source parse diagnostics are the S9 records, referenced intact and
    # separate from the S11 matching diagnostics.
    assert batch.source_diagnostics == (_SOURCE_DIAGNOSTIC,)
    assert batch.source_diagnostics[0] is _SOURCE_DIAGNOSTIC


def test_source_and_matching_diagnostics_stay_separate(context) -> None:
    delivery = vehicles_delivery(
        [make_vehicle(9, route_number=9999, vehicle_type="rail")],
        diagnostics=[_SOURCE_DIAGNOSTIC],
    )
    batch = match_delivery(delivery, context)
    assert all(d.feed == "vehicles" for d in batch.source_diagnostics)
    assert any(
        d.reason is MatchReason.UNKNOWN_ROUTE for d in batch.diagnostics
    )
    # The matching layer only emits its own frozen vocabulary.
    assert {d.reason.value for d in batch.diagnostics} <= {
        reason.value for reason in MatchReason
    }


def test_evidence_is_carried_even_when_absent(context) -> None:
    batch = match_delivery(
        vehicles_delivery([make_vehicle(1)], evidence=None), context
    )
    assert batch.evidence is None


# ---------------------------------------------------------------------------
# No completeness upgrade (plan §11: matching never invents completeness)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "evidence",
    [
        None,
        VehicleCollectionEvidence(
            key_present=True, is_list=True, raw_count=1, rejected_count=0
        ),
        VehicleCollectionEvidence(
            key_present=True, is_list=True, raw_count=None, rejected_count=None
        ),
    ],
)
def test_weak_evidence_never_upgrades_or_downgrades(context, evidence) -> None:
    delivery = vehicles_delivery(
        [make_vehicle(1, route_number=90, vehicle_type="rail", trip_id="t90A")],
        evidence=evidence,
    )
    batch = match_delivery(delivery, context)
    record = batch.records[0]
    assert record.classification is RelevanceClassification.MAX_RELEVANT
    assert record.enrichment is EnrichmentState.MATCHED
    assert batch.max_relevant_count == 1


# ---------------------------------------------------------------------------
# A/B context identity isolation
# ---------------------------------------------------------------------------


def test_concurrent_contexts_a_and_b_report_their_own_identity() -> None:
    world_a = synthetic_world(feed_version="version-a")
    world_b = synthetic_world(feed_version="version-b")
    context_a = build_matching_context(
        synthetic_identity("version-a"), world_a.network, world_a.route_rows,
        world_a.trip_rows,
    )
    context_b = build_matching_context(
        synthetic_identity("version-b"), world_b.network, world_b.route_rows,
        world_b.trip_rows,
    )
    delivery = vehicles_delivery(
        [make_vehicle(1, route_number=90, vehicle_type="rail")]
    )
    batch_a = match_delivery(delivery, context_a)
    batch_b = match_delivery(delivery, context_b)
    assert batch_a.dataset_identity is context_a.identity
    assert batch_b.dataset_identity is context_b.identity
    assert batch_a.dataset_identity.feed_version == "version-a"
    assert batch_b.dataset_identity.feed_version == "version-b"
    assert batch_a.max_relevant_count == batch_b.max_relevant_count == 1


def test_identical_trip_ids_with_different_metadata_stay_separate() -> None:
    """A/B contexts with the same trip IDs but different trip metadata.

    World B keeps every trip id but changes t90A's static direction to 1;
    both contexts are built through the shared builder, and their honest
    direction provenance never crosses.
    """
    import dataclasses

    world_a = synthetic_world(feed_version="version-a")
    world_b = synthetic_world(feed_version="version-b")
    trips_b = {
        trip_id: dataclasses.replace(trip, direction_id=1)
        if trip_id == "t90A" else trip
        for trip_id, trip in world_b.network.trips.items()
    }
    rows_b = [
        dataclasses.replace(row, direction_id=1) if row.trip_id == "t90A" else row
        for row in world_b.trip_rows
    ]
    network_b = dataclasses.replace(world_b.network, trips=trips_b)
    context_a = build_matching_context(
        synthetic_identity("version-a"), world_a.network,
        world_a.route_rows, world_a.trip_rows,
    )
    context_b = build_matching_context(
        synthetic_identity("version-b"), network_b,
        world_b.route_rows, rows_b,
    )
    delivery = vehicles_delivery(
        [make_vehicle(7, route_number=90, vehicle_type="rail",
                      trip_id="t90A", direction=None)]
    )
    record_a = match_delivery(delivery, context_a).records[0]
    record_b = match_delivery(delivery, context_b).records[0]
    assert record_a.static_direction == 0
    assert record_b.static_direction == 1
    assert record_a.direction_source == "static_trip"
    assert record_b.direction_source == "static_trip"
    assert record_a is not record_b
    assert record_a.static_direction != record_b.static_direction


def test_failed_b_build_leaves_prepared_a_results_intact() -> None:
    """A defective B build fails whole; the already prepared A keeps serving."""
    world_a = synthetic_world(feed_version="version-a")
    world_b = synthetic_world(feed_version="version-b")
    context_a = build_matching_context(
        synthetic_identity("version-a"), world_a.network,
        world_a.route_rows, world_a.trip_rows,
    )
    delivery = vehicles_delivery(
        [make_vehicle(1, route_number=90, vehicle_type="rail",
                      trip_id="t90A", direction=0)]
    )
    batch_a = match_delivery(delivery, context_a)
    # Defective B build: a duplicated raw trip row is caught before any
    # dictionary conversion; no new context is published.
    bad_rows = list(world_b.trip_rows)
    bad_rows.append(bad_rows[0])
    with pytest.raises(ContextPreparationError):
        build_matching_context(
            synthetic_identity("version-b"), world_b.network,
            world_b.route_rows, bad_rows,
        )
    delta = match_delivery(delivery, context_a)
    assert delta.dataset_identity is context_a.identity
    assert delta.max_relevant_count == batch_a.max_relevant_count == 1
    assert delta.records[0].static_direction == batch_a.records[0].static_direction


# ---------------------------------------------------------------------------
# Empty feeds: zero-everything batches
# ---------------------------------------------------------------------------


def test_empty_vehicles_batch(context) -> None:
    batch = match_delivery(vehicles_delivery([]), context)
    assert batch.input_count == 0
    assert batch.records == ()
    assert batch.max_relevant_count == 0
    assert batch.non_max_count == 0
    assert batch.unclassified_count == 0
    assert batch.matched_count == 0
    assert batch.partial_count == 0
    assert batch.unmatched_count == 0
    assert batch.conflict_count == 0
    assert batch.reason_totals == {}
    assert batch.diagnostics == ()
    assert batch.omitted_match_diagnostics == 0


def test_empty_tripupdates_and_alerts_batches(context) -> None:
    tu = match_delivery(
        make_delivery(PollFeed.TRIPUPDATES, make_tripupdates_feed([])), context
    )
    al = match_delivery(alerts_delivery([]), context)
    for batch in (tu, al):
        assert batch.input_count == 0
        assert batch.records == ()
        assert batch.max_relevant_count == 0


# ---------------------------------------------------------------------------
# Accounting, cap, and exact totals (plan §6/§12)
# ---------------------------------------------------------------------------


def _unknown_route_delivery(count: int) -> PollDelivery:
    """Vehicles that each produce exactly one ``unknown_route`` event."""
    return vehicles_delivery(
        [
            make_vehicle(1000 + index, route_number=9999, vehicle_type="rail")
            for index in range(count)
        ]
    )


def test_diagnostic_cap_at_199_events_no_truncation(context) -> None:
    batch = match_delivery(_unknown_route_delivery(199), context)
    assert len(batch.diagnostics) == 199
    assert batch.omitted_match_diagnostics == 0
    assert batch.reason_totals[MatchReason.UNKNOWN_ROUTE] == 199


def test_diagnostic_cap_at_200_events_retains_all(context) -> None:
    batch = match_delivery(_unknown_route_delivery(MAX_DIAGNOSTIC_ENTRIES), context)
    assert len(batch.diagnostics) == MAX_DIAGNOSTIC_ENTRIES == 200
    assert batch.omitted_match_diagnostics == 0
    assert batch.reason_totals[MatchReason.UNKNOWN_ROUTE] == 200


def test_diagnostic_cap_at_201_events_keeps_199_plus_overflow(context) -> None:
    batch = match_delivery(_unknown_route_delivery(MAX_DIAGNOSTIC_ENTRIES + 1), context)
    # 201 events → first 199 samples + one diagnostics_overflow entry = 200,
    # with the exact total (201) kept in reason_totals independent of the
    # sample truncation.
    assert len(batch.diagnostics) == MAX_DIAGNOSTIC_ENTRIES
    assert batch.omitted_match_diagnostics == 2
    assert batch.diagnostics[-1].reason is MatchReason.DIAGNOSTICS_OVERFLOW
    assert "2 matching diagnostics omitted" in batch.diagnostics[-1].detail
    assert batch.reason_totals[MatchReason.UNKNOWN_ROUTE] == 201
    assert sum(
        1 for d in batch.diagnostics if d.reason is MatchReason.UNKNOWN_ROUTE
    ) == 199
    assert batch.input_count == 201
    assert batch.unclassified_count == 201


def test_diagnostic_cap_far_beyond_reports_exact_omitted_count(context) -> None:
    batch = match_delivery(_unknown_route_delivery(500), context)
    assert len(batch.diagnostics) == MAX_DIAGNOSTIC_ENTRIES
    assert batch.omitted_match_diagnostics == 301
    assert "301 matching diagnostics omitted" in batch.diagnostics[-1].detail
    assert batch.reason_totals[MatchReason.UNKNOWN_ROUTE] == 500


def test_cap_summary_entry_reports_the_cap_not_the_event_count(context) -> None:
    batch = match_delivery(_unknown_route_delivery(220), context)
    detail = batch.diagnostics[-1].detail
    assert "200-entry cap" in detail
    assert batch.reason_totals[MatchReason.UNKNOWN_ROUTE] == 220


def test_record_key_propagates_into_matching_diagnostics(context) -> None:
    batch = match_delivery(
        vehicles_delivery([make_vehicle(1234, route_number=9999)]), context
    )
    assert batch.diagnostics[0].record_key == "1234"
    assert batch.diagnostics[0].field == "route_number"
