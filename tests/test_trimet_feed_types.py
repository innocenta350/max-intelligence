"""Tests for the S9 feed data contracts (Stage S9, M2 plan REV 2).

Covers plan §5 (request provenance and snapshot authority) and the owner's
constructor-invariant watch item: ``VehiclesRequestProvenance`` and
``ReconciliationSnapshot`` validate *themselves* — every guard has a dedicated
test, including defense-in-depth re-validation of a wrapped provenance whose
own invariants were bypassed.  Also covers the immutability rules (plan §9):
frozen dataclasses, tuple-only collections.
"""

from __future__ import annotations

import dataclasses

import pytest

from backend.trimet.feed_types import (
    MAX_PLAUSIBLE_UTC_MS,
    MIN_PLAUSIBLE_UTC_MS,
    AlertsSnapshot,
    AlertRouteChild,
    AlertLocation,
    FeedDiagnostic,
    FeedParseResult,
    ReconciliationSnapshot,
    ServiceAlert,
    StopTimeEvent,
    StopTimeUpdate,
    TripDescriptor,
    TripUpdate,
    TripUpdateEntity,
    TripUpdateFeed,
    TripUpdateFeedHeader,
    VehiclePosition,
    VehiclesRequestKind,
    VehiclesRequestProvenance,
    VehiclesSnapshot,
)

PROVENANCE_MS = 1_794_024_790_000  # inside the plausibility window
SINCE_MS = 1_794_024_000_000


def make_provenance(
    request_kind: VehiclesRequestKind = VehiclesRequestKind.DEFAULT,
    since_ms: int | None = None,
    requested_at_utc_ms: int = PROVENANCE_MS,
) -> VehiclesRequestProvenance:
    return VehiclesRequestProvenance(
        request_kind=request_kind,
        since_ms=since_ms,
        requested_at_utc_ms=requested_at_utc_ms,
    )


def make_vehicle(**overrides: object) -> VehiclePosition:
    base: dict[str, object] = {
        "vehicle_id": 622,
        "vehicle_type": "rail",
        "route_number": 90,
        "latitude": 45.53200912852157,
        "longitude": -122.63347045430686,
        "time_utc_ms": 1_794_024_765_862,
        "expires_utc_ms": 1_794_025_631_000,
    }
    base.update(overrides)
    return VehiclePosition(**base)  # type: ignore[arg-type]


def make_snapshot(
    provenance: VehiclesRequestProvenance | None = None,
    vehicles: tuple[VehiclePosition, ...] | None = None,
) -> VehiclesSnapshot:
    if vehicles is None:
        vehicles = (make_vehicle(),)
    return VehiclesSnapshot(
        provenance=provenance or make_provenance(),
        query_time_utc_ms=1_794_024_790_542,
        vehicles=vehicles,
    )


# --- Plausibility window sanity ---------------------------------------------


def test_plausibility_window_constants_are_ordered() -> None:
    assert 0 < MIN_PLAUSIBLE_UTC_MS < PROVENANCE_MS < MAX_PLAUSIBLE_UTC_MS


# --- VehiclesRequestProvenance constructor invariants ------------------------


def test_provenance_default_with_since_ms_set_is_rejected() -> None:
    with pytest.raises(ValueError, match="since_ms must be None for a DEFAULT"):
        make_provenance(
            request_kind=VehiclesRequestKind.DEFAULT, since_ms=SINCE_MS
        )


def test_provenance_since_with_since_ms_none_is_rejected() -> None:
    with pytest.raises(ValueError, match="since_ms must be an integer"):
        make_provenance(
            request_kind=VehiclesRequestKind.SINCE, since_ms=None
        )


def test_provenance_since_with_non_integer_since_ms_is_rejected() -> None:
    for bad in ("1794024000000", 1_794_024_000_000.0, True):
        with pytest.raises(ValueError, match="since_ms must be an integer"):
            make_provenance(
                request_kind=VehiclesRequestKind.SINCE, since_ms=bad  # type: ignore[arg-type]
            )


def test_provenance_rejects_implausible_requested_at_utc_ms() -> None:
    for bad in (0, -1, MIN_PLAUSIBLE_UTC_MS - 1, MAX_PLAUSIBLE_UTC_MS + 1):
        with pytest.raises(ValueError, match="plausible UTC ms epoch"):
            make_provenance(requested_at_utc_ms=bad)  # type: ignore[arg-type]


def test_provenance_rejects_non_integer_requested_at_utc_ms() -> None:
    with pytest.raises(ValueError, match="plausible UTC ms epoch"):
        make_provenance(requested_at_utc_ms="1794024790000")  # type: ignore[arg-type]


def test_provenance_rejects_non_enum_request_kind() -> None:
    with pytest.raises(TypeError, match="VehiclesRequestKind"):
        make_provenance(request_kind="default")  # type: ignore[arg-type]


def test_provenance_accepts_valid_default_and_since() -> None:
    default = make_provenance()
    assert default.request_kind is VehiclesRequestKind.DEFAULT
    assert default.since_ms is None
    since = make_provenance(
        request_kind=VehiclesRequestKind.SINCE, since_ms=SINCE_MS
    )
    assert since.since_ms == SINCE_MS


# --- ReconciliationSnapshot constructor invariants ---------------------------


def test_reconciliation_rejects_empty_basis() -> None:
    with pytest.raises(ValueError, match="basis must be a non-empty string"):
        ReconciliationSnapshot(
            snapshot=make_snapshot(), basis="", established_at_utc_ms=PROVENANCE_MS
        )


def test_reconciliation_rejects_blank_basis() -> None:
    with pytest.raises(ValueError, match="basis must be a non-empty string"):
        ReconciliationSnapshot(
            snapshot=make_snapshot(),
            basis="   \t\n",
            established_at_utc_ms=PROVENANCE_MS,
        )


def test_reconciliation_rejects_non_string_basis() -> None:
    with pytest.raises(ValueError, match="basis must be a non-empty string"):
        ReconciliationSnapshot(
            snapshot=make_snapshot(),
            basis=None,  # type: ignore[arg-type]
            established_at_utc_ms=PROVENANCE_MS,
        )


def test_reconciliation_rejects_non_default_wrapped_provenance() -> None:
    since_snapshot = make_snapshot(
        provenance=make_provenance(
            request_kind=VehiclesRequestKind.SINCE, since_ms=SINCE_MS
        )
    )
    with pytest.raises(ValueError, match="since= response can never back"):
        ReconciliationSnapshot(
            snapshot=since_snapshot,
            basis="scheduled full reconciliation poll",
            established_at_utc_ms=PROVENANCE_MS,
        )


def test_reconciliation_rejects_internally_invalid_wrapped_provenance() -> None:
    # Simulate a VehiclesRequestProvenance whose __post_init__ was bypassed
    # (defense-in-depth re-validation in ReconciliationSnapshot must catch it).
    corrupt = object.__new__(VehiclesRequestProvenance)
    object.__setattr__(corrupt, "request_kind", VehiclesRequestKind.DEFAULT)
    object.__setattr__(corrupt, "since_ms", SINCE_MS)  # violates the invariant
    object.__setattr__(corrupt, "requested_at_utc_ms", PROVENANCE_MS)
    with pytest.raises(ValueError, match="internally invalid"):
        ReconciliationSnapshot(
            snapshot=make_snapshot(provenance=corrupt),
            basis="scheduled full reconciliation poll",
            established_at_utc_ms=PROVENANCE_MS,
        )


def test_reconciliation_rejects_implausible_established_at_utc_ms() -> None:
    with pytest.raises(ValueError, match="plausible UTC ms epoch"):
        ReconciliationSnapshot(
            snapshot=make_snapshot(),
            basis="scheduled full reconciliation poll",
            established_at_utc_ms=-5,
        )


def test_reconciliation_rejects_non_snapshot() -> None:
    with pytest.raises(TypeError, match="VehiclesSnapshot"):
        ReconciliationSnapshot(
            snapshot="not a snapshot",  # type: ignore[arg-type]
            basis="scheduled full reconciliation poll",
            established_at_utc_ms=PROVENANCE_MS,
        )


def test_reconciliation_accepts_valid_default_snapshot() -> None:
    reconciliation = ReconciliationSnapshot(
        snapshot=make_snapshot(),
        basis="scheduled 5-minute full reconciliation poll",
        established_at_utc_ms=PROVENANCE_MS,
    )
    assert reconciliation.snapshot.provenance.request_kind is (
        VehiclesRequestKind.DEFAULT
    )
    assert reconciliation.basis == (
        "scheduled 5-minute full reconciliation poll"
    )


# --- Immutability (plan §9) ---------------------------------------------------


def test_all_models_are_frozen() -> None:
    header = TripUpdateFeedHeader(
        gtfs_realtime_version="2.0",
        incrementality_raw=0,
        timestamp_raw_seconds=1_794_024_790,
        timestamp_utc_ms=1_794_024_790_000,
    )
    trip_update = TripUpdate(
        trip=TripDescriptor(trip_id="18330502"),
        stop_time_updates=(
            StopTimeUpdate(
                stop_sequence=12,
                arrival=StopTimeEvent(delay_seconds=-557),
            ),
        ),
    )
    instances: list[object] = [
        make_provenance(),
        make_vehicle(),
        make_snapshot(),
        ReconciliationSnapshot(
            snapshot=make_snapshot(),
            basis="basis",
            established_at_utc_ms=PROVENANCE_MS,
        ),
        AlertRouteChild(route_id=74),
        AlertLocation(location_id=4606),
        ServiceAlert(alert_id=262613),
        AlertsSnapshot(query_time_utc_ms=1_794_024_790_551, alerts=()),
        FeedDiagnostic(
            feed="vehicles", record_key="622", field="time", reason="invalid_timestamp"
        ),
        FeedParseResult(model=make_snapshot()),
        header,
        trip_update,
        TripUpdateEntity(entity_id="1", trip_update=trip_update),
        TripUpdateFeed(header=header, entities=()),
    ]
    for instance in instances:
        first_field = dataclasses.fields(instance)[0].name
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(instance, first_field, None)


def test_collection_fields_are_tuples_not_lists() -> None:
    alert = ServiceAlert(
        alert_id=262613,
        routes=(AlertRouteChild(route_id=74),),
        locations=(AlertLocation(location_id=4606),),
    )
    assert isinstance(alert.routes, tuple)
    assert isinstance(alert.locations, tuple)
    snapshot = make_snapshot()
    assert isinstance(snapshot.vehicles, tuple)
    feed = TripUpdateFeed(
        header=TripUpdateFeedHeader(
            gtfs_realtime_version=None,
            incrementality_raw=0,
            timestamp_raw_seconds=1,
            timestamp_utc_ms=1000,
        ),
        entities=(
            TripUpdateEntity(
                entity_id="1",
                trip_update=TripUpdate(
                    trip=TripDescriptor(trip_id="t"),
                    stop_time_updates=(StopTimeUpdate(),),
                ),
            ),
        ),
    )
    assert isinstance(feed.entities, tuple)
    assert isinstance(
        feed.entities[0].trip_update.stop_time_updates, tuple
    )


# --- Derived counts and header property ---------------------------------------


def test_snapshot_counts_derive_from_the_tuple() -> None:
    vehicles = (
        make_vehicle(vehicle_id=1, vehicle_type="rail"),
        make_vehicle(vehicle_id=2, vehicle_type="rail"),
        make_vehicle(vehicle_id=3, vehicle_type="bus", route_number=72),
    )
    snapshot = make_snapshot(vehicles=vehicles)
    assert snapshot.vehicle_count == 3
    assert snapshot.rail_count == 2


def test_header_is_full_dataset_property() -> None:
    full = TripUpdateFeedHeader(
        gtfs_realtime_version="2.0",
        incrementality_raw=0,
        timestamp_raw_seconds=1_794_024_790,
        timestamp_utc_ms=1_794_024_790_000,
    )
    incremental = TripUpdateFeedHeader(
        gtfs_realtime_version="2.0",
        incrementality_raw=1,
        timestamp_raw_seconds=1_794_024_790,
        timestamp_utc_ms=1_794_024_790_000,
    )
    assert full.is_full_dataset
    assert not incremental.is_full_dataset
