"""Stage S1 checks for the typed data structures (backend/gtfs/types.py)."""

import dataclasses
from pathlib import Path
from types import MappingProxyType

import pytest

from backend.gtfs.types import (
    CoverageReport,
    DirectionCoverage,
    GtfsManifest,
    ManifestAbsent,
    ManifestRead,
    ManifestUnreadable,
    ManifestValid,
    Route,
    RouteCoverage,
    ShapeGeometry,
    ShapePoint,
    Stop,
    Trip,
    TripStopEntry,
)


def test_route_keeps_direction_names_with_none_bucket() -> None:
    route = Route(
        route_id="90",
        route_short_name="90",
        route_long_name="Gresham Transit Center - Portland Transit Center",
        route_color="C41F3E",
        direction_names={1: "Gresham", 0: "Portland", None: "Unspecified"},
    )
    assert route.route_id == "90"
    assert route.route_color == "C41F3E"
    assert route.direction_names[None] == "Unspecified"
    assert route.direction_names[0] == "Portland"


def test_route_is_frozen() -> None:
    route = Route(route_id="90", direction_names={})
    with pytest.raises(dataclasses.FrozenInstanceError):
        route.route_id = "999"


def test_route_direction_names_are_enforced_immutable() -> None:
    """Review fix 2: enforcement, not a docstring promise. A plain dict must
    become read-only, and later mutation of the construction mapping must not
    leak into the record."""
    source = {1: "Gresham", 0: "Portland"}
    route = Route(route_id="90", direction_names=source)
    with pytest.raises(TypeError):
        route.direction_names[0] = "Modified"
    source[None] = "Unspecified"  # mutate the caller's dict afterwards
    assert route.direction_names.get(None) is None
    assert route.direction_names[0] == "Portland"


def test_route_direction_names_accept_read_only_proxies() -> None:
    proxy = MappingProxyType({1: "Gresham", None: "Unspecified"})
    route = Route(route_id="90", direction_names=proxy)
    assert route.direction_names[None] == "Unspecified"
    with pytest.raises(TypeError):
        route.direction_names[1] = "Changed"


def test_trip_defaults_record_the_blank_value_buckets() -> None:
    trip = Trip(trip_id="t1", route_id="290")
    assert trip.direction_id is None  # Rev. 2 change 4: blank -> None bucket
    assert trip.shape_id is None  # Rev. 2 change 1: kept, contributes no shape

    explicit = Trip(trip_id="t2", route_id="90", direction_id=1, shape_id="sh-90")
    assert (explicit.direction_id, explicit.shape_id) == (1, "sh-90")


def test_trip_frozen() -> None:
    trip = Trip(trip_id="t1", route_id="90")
    with pytest.raises(dataclasses.FrozenInstanceError):
        trip.shape_id = "changed"


def test_stop_construction_and_defaults() -> None:
    stop = Stop(stop_id="14023", stop_lat=45.5, stop_lon=-122.5)
    assert stop.stop_name is None
    assert stop.location_type is None
    assert stop.parent_station is None
    full = Stop(
        stop_id="14100",
        stop_name="Gateway Transit Center",
        stop_lat=45.528,
        stop_lon=-122.542,
        location_type=0,
        parent_station="14200",
    )
    assert full.parent_station == "14200"


def test_trip_stop_entry_optional_times() -> None:
    entry = TripStopEntry(stop_id="14023", stop_sequence=1)
    assert entry.arrival_time_secs is None
    assert entry.departure_time_secs is None
    timed = TripStopEntry(
        stop_id="14024",
        stop_sequence=2,
        arrival_time_secs=3723,
        departure_time_secs=3780,
    )
    assert timed.arrival_time_secs == 3723


def test_shape_usability_requires_two_points() -> None:
    single = ShapeGeometry("sh-1", (ShapePoint(shape_pt_sequence=0, lat=45.5, lon=-122.5),))
    assert single.is_usable is False
    doubled = ShapeGeometry(
        "sh-2",
        (
            ShapePoint(shape_pt_sequence=0, lat=45.5, lon=-122.5),
            ShapePoint(shape_pt_sequence=1, lat=45.51, lon=-122.51),
        ),
    )
    assert doubled.is_usable is True
    empty = ShapeGeometry("sh-3", ())
    assert empty.is_usable is False


def test_shape_geometry_is_frozen() -> None:
    geometry = ShapeGeometry("sh-1", ())
    with pytest.raises(dataclasses.FrozenInstanceError):
        geometry.points = ()


def test_manifest_equality_and_frozen() -> None:
    manifest = GtfsManifest(
        feed_version="20261011-20261021-0903",
        sha256="a" * 64,
        zip_size_bytes=43_467_097,
        dataset_size_bytes=1_000,
        fetched_at_utc_ms=1_760_000_000_000,
        zip_path=Path("data/gtfs/gtfs.zip"),
        dataset_path=Path("data/gtfs/feed-20261011-20261021-0903"),
    )
    identical = GtfsManifest(
        feed_version="20261011-20261021-0903",
        sha256="a" * 64,
        zip_size_bytes=43_467_097,
        dataset_size_bytes=1_000,
        fetched_at_utc_ms=1_760_000_000_000,
        zip_path=Path("data/gtfs/gtfs.zip"),
        dataset_path=Path("data/gtfs/feed-20261011-20261021-0903"),
    )
    assert manifest == identical
    different = dataclasses.replace(manifest, sha256="b" * 64)
    assert manifest != different
    with pytest.raises(dataclasses.FrozenInstanceError):
        different.sha256 = "c" * 64


def test_manifest_read_tags_cover_the_three_cases() -> None:
    # Plan Rev. 7 §2 matrix tags.
    absent = ManifestAbsent()
    assert absent == ManifestAbsent()
    assert isinstance(absent, ManifestRead)

    valid = ManifestValid("a" * 64)
    assert valid.sha256 == "a" * 64
    assert isinstance(valid, ManifestRead)

    unreadable = ManifestUnreadable("manifest.json is empty")
    assert unreadable.reason == "manifest.json is empty"
    assert isinstance(unreadable, ManifestRead)

    assert ManifestValid("a" * 64) != ManifestValid("b" * 64)
    assert ManifestAbsent() != unreadable


def test_manifest_read_tags_are_frozen() -> None:
    valid = ManifestValid("a" * 64)
    with pytest.raises(dataclasses.FrozenInstanceError):
        valid.sha256 = "b" * 64
    unreadable = ManifestUnreadable("truncated")
    with pytest.raises(dataclasses.FrozenInstanceError):
        unreadable.reason = "changed"


def test_coverage_report_wraps_route_coverage_incl_none_direction() -> None:
    route = RouteCoverage(
        route_id="100",
        directions=(
            DirectionCoverage(direction_id=0, trips_total=3, trips_with_shape=3),
            DirectionCoverage(direction_id=None, trips_total=1, trips_with_shape=0),
        ),
        trips_without_shape=frozenset({"trip-blank-shape"}),
        usable_shape_ids=frozenset({"sh-1", "sh-2"}),
        broken_shape_ids=frozenset({"sh-broken"}),
        reachable_stop_ids=frozenset({"s-1", "s-2"}),
        unmatched_stop_ids=frozenset({"14024", "14025"}),
    )
    report = CoverageReport(routes=(route,))
    assert report.routes[0].unmatched_stop_ids == {"14024", "14025"}
    assert report.routes[0].directions[1].direction_id is None
    assert report.routes[0].trips_without_shape == frozenset({"trip-blank-shape"})


def test_coverage_report_is_frozen() -> None:
    report = CoverageReport(routes=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        report.routes = ()


def test_every_collection_field_uses_an_immutable_container() -> None:
    """Bar future mutable-default regressions: container fields are tuple/
    frozenset; no field default is a list/dict/set instance."""
    for cls in (
        Route,
        Stop,
        Trip,
        TripStopEntry,
        ShapeGeometry,
        GtfsManifest,
        CoverageReport,
        RouteCoverage,
        DirectionCoverage,
        ManifestValid,
        ManifestUnreadable,
    ):
        for field_info in dataclasses.fields(cls):
            default = field_info.default
            has_container_default = isinstance(default, (list, dict, set))
            assert not has_container_default, (
                f"{cls.__name__}.{field_info.name} has a mutable default"
            )
    route = Route(route_id="90", direction_names={})
    coverage = RouteCoverage(
        route_id="90",
        directions=(),
        trips_without_shape=frozenset(),
        usable_shape_ids=frozenset(),
        broken_shape_ids=frozenset(),
        reachable_stop_ids=frozenset(),
        unmatched_stop_ids=frozenset(),
    )
    assert isinstance(coverage.trips_without_shape, frozenset)
    assert isinstance(coverage.directions, tuple)
    # direction_names is required (no default): builders always pass the
    # mapping explicitly, nothing silently shares storage.
    assert dataclasses.fields(Route)[1].default is dataclasses.MISSING
