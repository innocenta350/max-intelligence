"""Shared S11 test helpers (offline; never contacts developer.trimet.org).

Provides:

- :func:`install_socket_guard` — autouse-able guard that raises on any
  non-loopback DNS resolution or connect attempt *before* any network
  access (independent copy of the established S10 convention; the new S11
  modules never make any socket call).
- :class:`SyntheticWorld` + :func:`synthetic_world` — a small, internally
  consistent MAX network (all five whitelist routes, usable/broken shapes,
  dangling stop reference, repeated stop visits) plus the normalized
  route/trip rows that match it, for the shared builder.
- Promoted-dataset factories (``write_promoted_dataset``,
  :func:`make_load_outcome`) building tiny local ``gtfs.zip`` + manifest
  directories in pytest tmp dirs for the read-only preparation factory.
- S9 model + :class:`PollDelivery` builders for vehicles, TripUpdates and
  alerts (parsed-model level — no protobuf, no HTTP).
- :func:`make_batch` convenience for iterating many diagnostics.

Every helper is synthetic: no real machine dataset, no `.env`, no probe
imports, no campaign directories.
"""

from __future__ import annotations

import json
import socket
import zipfile
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import pytest

from backend.gtfs.promotion import PromotionOutcome
from backend.gtfs.types import (
    GtfsManifest,
    MaxNetwork,
    Route,
    ShapeGeometry,
    ShapePoint,
    Stop,
    Trip,
    TripStopEntry,
)
from backend.trimet.alerts_classify import (
    AlertClassification,  # noqa: F401  (classifier parity)
    AlertClassificationResult,  # noqa: F401  (type parity)
)
from backend.trimet.dataset import LoadOutcome
from backend.trimet.feed_types import (
    AlertLocation,
    AlertRouteChild,
    AlertsSnapshot,
    FeedDiagnostic,
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
from backend.trimet.match_types import DatasetIdentity, RouteRow, TripRow
from backend.trimet.poll_types import PollDelivery, PollFeed, RequestPurpose, VehicleCollectionEvidence

# Plausible fixed source timestamps (matching the S9/S10 fixture era).
QUERY_TIME_MS = 1_794_024_790_000
QUERY_MS = 1_794_024_790_542

SENTINEL_SHA256 = "ab" * 32

MAX_ROUTE_IDS = ("90", "100", "190", "200", "290")


# ---------------------------------------------------------------------------
# Socket guard (defense in depth, S11-scoped)
# ---------------------------------------------------------------------------

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def install_socket_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise before DNS/network access for any non-loopback destination."""
    real_getaddrinfo = socket.getaddrinfo
    real_connect = socket.socket.connect

    def guarded_getaddrinfo(host, *args, **kwargs):
        if isinstance(host, str) and host not in _LOOPBACK_HOSTS:
            raise AssertionError(
                f"S11 socket guard: blocked non-loopback resolution for {host!r}"
            )
        return real_getaddrinfo(host, *args, **kwargs)

    def guarded_connect(sock, address):
        host = address[0] if isinstance(address, tuple) else None
        if isinstance(host, str) and host not in _LOOPBACK_HOSTS:
            raise AssertionError(
                f"S11 socket guard: blocked non-loopback connect to {host!r}"
            )
        return real_connect(sock, address)

    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo, raising=True)
    monkeypatch.setattr(socket.socket, "connect", guarded_connect, raising=True)


# ---------------------------------------------------------------------------
# Synthetic identity + world (shared builder inputs)
# ---------------------------------------------------------------------------


def synthetic_identity(
    feed_version: str = "test-feed-001",
    *,
    sha256: str = SENTINEL_SHA256,
    source_directory_name: str | None = None,
) -> DatasetIdentity:
    """A clearly synthetic typed identity; makes no real-archive claim."""
    return DatasetIdentity(
        archive_sha256=sha256,
        feed_version=feed_version,
        source_directory_name=source_directory_name,
    )


@dataclass(frozen=True, slots=True)
class SyntheticWorld:
    """One internally consistent tiny MAX world + its normalized rows."""

    network: MaxNetwork
    route_rows: tuple[RouteRow, ...]
    trip_rows: tuple[TripRow, ...]

    @property
    def max_trip_ids(self) -> tuple[str, ...]:
        return tuple(self.network.trips)


def synthetic_world(
    *,
    feed_version: str = "test-feed-001",
) -> SyntheticWorld:
    """Build the reference tiny MAX world used across the S11 tests.

    Contents (deliberately exercising the plan's edge cases):

    - All five MAX whitelist routes (``route_type=0``) plus bus route ``1``
      (``route_type=3``) and Streetcar ``193`` (``route_type=0``, a known
      non-MAX rail service).
    - MAX trips: ``t90A`` (full shape/visits), ``t90B`` (blank direction +
      blank shape — M1-tolerated), ``t90C`` (broken one-point shape via
      route 90's coupon ``shBroken``), ``t90D`` (route 100, repeated stop
      ``14000`` for ambiguity), ``t100A`` (one visit whose stop is missing
      from the catalog, the M1 dangling-stop tolerance), ``t90E`` (usable
      shape, direction conflict fodder, zero sequence).
    - One non-MAX trip each on the bus and Streetcar routes.
    """
    routes: dict[str, Route] = {}
    route_rows: list[RouteRow] = []
    colors = {"90": "C41F3E", "100": "0039A6", "190": "FFD335", "200": "00A651", "290": "D05F27"}
    for route_id in MAX_ROUTE_IDS:
        routes[route_id] = Route(
            route_id=route_id,
            direction_names={0: "eastbound", 1: "westbound"},
            route_short_name=route_id,
            route_long_name=f"MAX {colors[route_id]} Line",
            route_color=colors[route_id],
        )
        route_rows.append(RouteRow(route_id=route_id, route_type=0))
    # Known non-MAX routes: a bus-mode route and a Streetcar (mode 0).
    routes_row_only = [("1", 3), ("74", 3), ("193", 0)]
    route_rows.extend(RouteRow(route_id=rid, route_type=rtype) for rid, rtype in routes_row_only)

    stops = {
        "14000": Stop("14000", "SW 1st", 45.5, -122.67, 0, None),
        "14001": Stop("14001", "SE 2nd", 45.51, -122.66, 0, None),
        "14002": Stop("14002", "Lloyd", 45.53, -122.65, 0, None),
    }
    shapes = {
        "sh90": ShapeGeometry(
            "sh90",
            (ShapePoint(1, 45.5, -122.67), ShapePoint(2, 45.51, -122.66)),
        ),
        "shBroken": ShapeGeometry("shBroken", (ShapePoint(1, 45.5, -122.67),)),
    }
    trips: dict[str, Trip] = {}
    stop_times: dict[str, tuple[TripStopEntry, ...]] = {}
    trip_rows: list[TripRow] = []

    def add_max_trip(trip_id, route_id, direction, shape, entries):
        trips[trip_id] = Trip(trip_id, route_id, direction, shape)
        stop_times[trip_id] = entries
        trip_rows.append(TripRow(trip_id, route_id, direction, shape))

    add_max_trip(
        "t90A", "90", 0, "sh90",
        (TripStopEntry("14000", 1, 25200, 25230), TripStopEntry("14001", 2, None, 25330)),
    )
    add_max_trip("t90B", "90", None, None, (TripStopEntry("14002", 7, None, None),))
    add_max_trip("t90C", "90", 1, "shBroken", (TripStopEntry("14000", 3, None, None),))
    add_max_trip(
        "t90D", "100", None, None,
        (TripStopEntry("14000", 1, None, None), TripStopEntry("14000", 2, None, None)),
    )
    add_max_trip("t90E", "90", 0, "sh90", (TripStopEntry("14001", 0, None, None),))
    add_max_trip(
        "t100A", "100", 1, "sh90",
        (TripStopEntry("14024-dangling", 5, None, None),),
    )
    # Non-MAX rows: only the two IDs matter for the classification lookup;
    # direction/shape cells stay uninterpreted (None here).
    trip_rows.extend(
        [
            TripRow("tb1", "1", None, None),
            TripRow("t193", "193", None, None),
            TripRow("t74", "74", None, None),
        ]
    )
    network = MaxNetwork(
        feed_version=feed_version,
        feed_start_date="20261011",
        feed_end_date="20261021",
        routes=routes,
        stops=stops,
        trips=trips,
        stop_times=stop_times,
        shapes=shapes,
    )
    return SyntheticWorld(
        network=network,
        route_rows=tuple(route_rows),
        trip_rows=tuple(trip_rows),
    )


# ---------------------------------------------------------------------------
# Promoted-dataset factories (tmp-dir local, offline)
# ---------------------------------------------------------------------------

MANIFEST_FIELDS = (
    "feed_version",
    "sha256",
    "zip_size_bytes",
    "dataset_size_bytes",
    "fetched_at_utc_ms",
    "zip_path",
    "dataset_path",
)


def routes_csv_text(route_rows: Iterable[RouteRow], *, extra_column: bool = False) -> str:
    header = "route_id,route_type" + (",route_note" if extra_column else "")
    lines = [header]
    for row in route_rows:
        line = f"{row.route_id},{row.route_type}"
        lines.append(line + (",note" if extra_column else ""))
    return "\n".join(lines) + "\n"


def trips_csv_text(trip_rows: Iterable[TripRow]) -> str:
    lines = ["trip_id,route_id,direction_id,shape_id"]
    for row in trip_rows:
        direction = "" if row.direction_id is None else str(row.direction_id)
        shape = "" if row.shape_id is None else row.shape_id
        lines.append(f"{row.trip_id},{row.route_id},{direction},{shape}")
    return "\n".join(lines) + "\n"


def write_promoted_dataset(
    directory: Path,
    *,
    world: SyntheticWorld,
    feed_version: str = "test-feed-001",
    route_text: str | None = None,
    trip_text: str | None = None,
    extra_route_column: bool = False,
    omit_members: tuple[str, ...] = (),
    duplicate_members: tuple[str, ...] = (),
    raw_members: dict[str, bytes] | None = None,
    overrides_sha256: str | None = None,
    manifest_record_override: dict[str, Any] | None = None,
) -> Path:
    """Create one tiny promoted dataset directory (gtfs.zip + manifest).

    The zip is written with the world's matching routes/trips texts unless
    fully overridden.  ``raw_members`` replaces/extends members with raw
    bytes (for bad-UTF-8 tests).  ``overrides_sha256`` forces the recorded
    digest (for mismatch tests); by default the real digest is written.
    ``manifest_record_override`` merges into the manifest payload last.
    """
    members: dict[str, bytes] = {
        "routes.txt": (route_text if route_text is not None else routes_csv_text(
            world.route_rows, extra_column=extra_route_column
        )).encode("utf-8"),
        "trips.txt": (trip_text if trip_text is not None else trips_csv_text(world.trip_rows)).encode("utf-8"),
    }
    for member in omit_members:
        members.pop(member, None)
    if raw_members:
        members.update(raw_members)
    # Duplicate members listed twice (zip allows repeated names).
    listed: list[tuple[str, bytes]] = []
    for name, payload in members.items():
        listed.append((name, payload))
    for member in duplicate_members:
        listed.append((member, members[member]))

    zip_path = directory / "gtfs.zip"
    directory.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as archive:
        for name, payload in listed:
            archive.writestr(name, payload)
    if overrides_sha256 is None:
        import hashlib

        real_digest = hashlib.sha256(zip_path.read_bytes()).hexdigest()
        digest = real_digest
    else:
        digest = overrides_sha256

    manifest = {
        "feed_version": feed_version,
        "sha256": digest,
        "zip_size_bytes": zip_path.stat().st_size,
        "dataset_size_bytes": 1,
        "fetched_at_utc_ms": QUERY_TIME_MS,
        "zip_path": str(zip_path),
        "dataset_path": str(directory),
    }
    if manifest_record_override:
        manifest.update(manifest_record_override)
    (directory / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8"
    )
    return directory


def make_load_outcome(dataset_dir: Path, network: MaxNetwork) -> LoadOutcome:
    """Assemble a clearly paired M1 LoadOutcome without any loader call."""
    return LoadOutcome(
        network=network,
        promotion=PromotionOutcome(
            dataset_dir=dataset_dir,
            version_name=dataset_dir.name,
            reused=True,
            notes=(),
        ),
        reused=True,
        notes=(),
    )


def promoted_world_outcome(
    tmp_path: Path,
    *,
    feed_version: str = "test-feed-001",
    **dataset_kwargs,
) -> SimpleNamespace:
    """Promoted dataset + paired network + LoadOutcome, all synthetic."""
    world = synthetic_world(feed_version=feed_version)
    dataset_dir = write_promoted_dataset(
        tmp_path / f"dataset-{feed_version}",
        world=world,
        feed_version=feed_version,
        **dataset_kwargs,
    )
    return SimpleNamespace(
        world=world,
        dataset_dir=dataset_dir,
        outcome=make_load_outcome(dataset_dir, world.network),
        digest=json.loads((dataset_dir / "manifest.json").read_text())["sha256"],
    )


# ---------------------------------------------------------------------------
# S9 parsed-model + PollDelivery builders
# ---------------------------------------------------------------------------

_PROVENANCE = VehiclesRequestProvenance(
    request_kind=VehiclesRequestKind.DEFAULT,
    since_ms=None,
    requested_at_utc_ms=QUERY_TIME_MS,
)


def make_vehicle(
    vehicle_id: int,
    *,
    vehicle_type: str = "rail",
    route_number: int = 90,
    direction: int | None = 0,
    trip_id: str | None = None,
    next_loc_id: int | None = None,
    next_stop_seq: int | None = None,
    **overrides: Any,
) -> Any:
    kwargs: dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "vehicle_type": vehicle_type,
        "route_number": route_number,
        "latitude": 45.52,
        "longitude": -122.63,
        "time_utc_ms": QUERY_TIME_MS - 30_000,
        "expires_utc_ms": QUERY_TIME_MS + 60_000,
        "direction": direction,
        "trip_id": trip_id,
        "next_loc_id": next_loc_id,
        "next_stop_seq": next_stop_seq,
    }
    kwargs.update(overrides)
    return VehiclePosition(**kwargs)


def make_vehicles_snapshot(vehicles: list | tuple) -> VehiclesSnapshot:
    return VehiclesSnapshot(
        provenance=_PROVENANCE,
        query_time_utc_ms=QUERY_MS,
        vehicles=tuple(vehicles),
    )


def make_delivery(
    feed: PollFeed,
    model: Any,
    *,
    sequence: int = 1,
    purpose: RequestPurpose = RequestPurpose.STARTUP_FULL,
    diagnostics: tuple[FeedDiagnostic, ...] | list = (),
    evidence: VehicleCollectionEvidence | None = None,
) -> PollDelivery:
    return PollDelivery(
        feed=feed,
        sequence=sequence,
        purpose=purpose,
        request_sent_utc_ms=QUERY_TIME_MS,
        receipt_utc_ms=QUERY_TIME_MS + 150,
        elapsed_mono_ms=133,
        http_status=200,
        model=model,
        diagnostics=tuple(diagnostics),
        evidence=evidence,
    )


def vehicles_delivery(
    vehicles: list | tuple,
    *,
    sequence: int = 1,
    evidence: VehicleCollectionEvidence | None = None,
    diagnostics: tuple | list = (),
) -> PollDelivery:
    return make_delivery(
        PollFeed.VEHICLES,
        make_vehicles_snapshot(vehicles),
        sequence=sequence,
        evidence=evidence,
        diagnostics=diagnostics,
    )


# --- TripUpdates ------------------------------------------------------------


def make_stop_time_update(
    stop_sequence: int | None = None,
    stop_id: str | None = None,
    arrival: StopTimeEvent | None = None,
    departure: StopTimeEvent | None = None,
    schedule_relationship_raw: int | None = None,
) -> StopTimeUpdate:
    return StopTimeUpdate(
        stop_sequence=stop_sequence,
        stop_id=stop_id,
        arrival=arrival,
        departure=departure,
        schedule_relationship_raw=schedule_relationship_raw,
    )


def make_stop_time_event(
    delay_seconds=None, time_raw_seconds=None, uncertainty_seconds=None
) -> StopTimeEvent:
    return StopTimeEvent(
        delay_seconds=delay_seconds,
        time_raw_seconds=time_raw_seconds,
        time_utc_ms=None if time_raw_seconds is None else time_raw_seconds * 1000,
        uncertainty_seconds=uncertainty_seconds,
    )


def make_trip_update_entity(
    entity_id: str,
    trip_id: str,
    *,
    route_id: str | None = None,
    direction_id: int | None = None,
    start_time: str | None = None,
    start_date: str | None = None,
    schedule_relationship_raw: int | None = None,
    stop_updates: tuple | list = (),
    vehicle_id: str | None = None,
    delay_seconds: int | None = None,
    is_deleted: bool = False,
) -> TripUpdateEntity:
    descriptor = TripDescriptor(
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        start_time=start_time,
        start_date=start_date,
        schedule_relationship_raw=schedule_relationship_raw,
    )
    body = TripUpdate(
        trip=descriptor,
        stop_time_updates=tuple(stop_updates),
        vehicle_id=vehicle_id,
        delay_seconds=delay_seconds,
        timestamp_raw_seconds=QUERY_TIME_MS // 1000,
        timestamp_utc_ms=QUERY_TIME_MS,
    )
    return TripUpdateEntity(
        entity_id=entity_id,
        trip_update=body,
        is_deleted=is_deleted,
    )


_HEADER_TIMESTAMP_S = QUERY_TIME_MS // 1000


def make_tripupdates_feed(
    entities: Iterable,
    *,
    header_timestamp_s: int = _HEADER_TIMESTAMP_S,
) -> TripUpdateFeed:
    from backend.trimet.feed_types import TripUpdateFeed, TripUpdateFeedHeader

    header = TripUpdateFeedHeader(
        gtfs_realtime_version="2.0",
        incrementality_raw=0,
        timestamp_raw_seconds=header_timestamp_s,
        timestamp_utc_ms=header_timestamp_s * 1000,
    )
    return TripUpdateFeed(header=header, entities=tuple(entities))


def tripupdates_delivery(entities: Iterable, *, sequence: int = 1) -> PollDelivery:
    return make_delivery(
        PollFeed.TRIPUPDATES, make_tripupdates_feed(entities), sequence=sequence
    )


# --- Alerts -----------------------------------------------------------------


def make_alert(
    alert_id: int,
    *,
    system_wide_flag: bool | None = None,
    routes: Iterable = (),
    locations: Iterable = (),
    begin_utc_ms: int | None = QUERY_TIME_MS,
    end_utc_ms: int | None = None,
) -> Any:
    from backend.trimet.feed_types import ServiceAlert

    return ServiceAlert(
        alert_id=alert_id,
        begin_utc_ms=begin_utc_ms,
        end_utc_ms=end_utc_ms,
        system_wide_flag=system_wide_flag,
        header_text="maintenance",
        description="bounded synthetic text",
        info_link_url=None,
        routes=tuple(routes),
        locations=tuple(locations),
    )


def route_child(route_id: int) -> AlertRouteChild:
    return AlertRouteChild(route_id=route_id, route_type="R", no_service_flag=False)


def location_child(location_id: int) -> AlertLocation:
    return AlertLocation(location_id=location_id, description="loc")


def alerts_snapshot(alerts: Iterable) -> AlertsSnapshot:
    return AlertsSnapshot(query_time_utc_ms=QUERY_MS, alerts=tuple(alerts))


def alerts_delivery(alerts: Iterable, *, sequence: int = 1) -> PollDelivery:
    return make_delivery(PollFeed.ALERTS, alerts_snapshot(alerts), sequence=sequence)

