"""Typed data structures for the MAX network dataset (Stage S1, M1 plan Rev. 9).

Covers the record-level primitives that later stages consume:

- S4 parsers produce these from the GTFS tables (plus ``csvio`` reports).
- S5 ``catalog`` joins them into the network and emits ``CoverageReport``.
- S6 ``dataset`` swaps the assembled network into application state.

``MaxNetwork`` is defined here (added in Stage S5, after the S3 live archive
inspection resolved the index shapes): the assembled, validated MAX network
dataset that S6's ``DatasetManager`` swaps in atomically. The parent-station
index (station -> platforms) lives on it, built by the S5 assembly.

All records are frozen dataclasses. Collection fields use ``tuple`` /
``frozenset`` / read-only ``Mapping`` so nothing can be mutated after
construction; there are no mutable default values.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
# Absolute import points at the *stdlib* ``types`` module, not this file
# (which is only reachable as ``backend.gtfs.types``).
from types import MappingProxyType

__all__ = [
    "CoverageReport",
    "DirectionCoverage",
    "GtfsManifest",
    "ManifestAbsent",
    "ManifestRead",
    "ManifestUnreadable",
    "ManifestValid",
    "MaxNetwork",
    "Route",
    "RouteCoverage",
    "ShapeGeometry",
    "ShapePoint",
    "Stop",
    "Trip",
    "TripStopEntry",
]


@dataclass(frozen=True, slots=True)
class Route:
    """One GTFS ``routes.txt`` row, MAX-scoped at filter time.

    ``route_color`` is the GTFS ``route_color`` hex string (M0 evidence:
    90 ``C41F3E`` … 290 ``D05F27``). ``direction_names`` maps direction id to
    the direction name; the ``None`` key is Rev. 2 change 4's "unspecified
    direction" bucket for blank ``direction_id`` values.

    Immutability is enforced, not documented-and-hoped (review fix 2): any
    incoming mapping is snapshotted once and stored as a read-only proxy, so
    both ``route.direction_names[0] = ...`` and later mutation of the
    caller's construction mapping fail / stay out, respectively.
    """

    route_id: str
    direction_names: Mapping[int | None, str]
    route_short_name: str | None = None
    route_long_name: str | None = None
    route_color: str | None = None

    def __post_init__(self) -> None:
        """Snapshot the given mapping into a truly read-only proxy."""
        object.__setattr__(
            self,
            "direction_names",
            MappingProxyType(dict(self.direction_names)),
        )


@dataclass(frozen=True, slots=True)
class Stop:
    """One GTFS ``stops.txt`` row.

    ``location_type`` splits platforms (0 default per GTFS) from stations (1);
    the parent-station index itself is assembled in S5. Coordinates stay
    ``None`` when not converted by the parser.
    """

    stop_id: str
    stop_name: str | None = None
    stop_lat: float | None = None
    stop_lon: float | None = None
    location_type: int | None = None
    parent_station: str | None = None


@dataclass(frozen=True, slots=True)
class Trip:
    """One GTFS ``trips.txt`` row for a MAX route.

    ``direction_id`` is ``None`` for blank values (Rev. 2 change 4).
    ``shape_id`` is ``None`` for trips that carry no shape link — such trips
    are kept and recorded, they simply contribute no shape (Rev. 2 change 1).
    ``service_id`` is intentionally absent: M1 ignores calendar/service-day
    data (plan §B).
    """

    trip_id: str
    route_id: str
    direction_id: int | None = None
    shape_id: str | None = None


@dataclass(frozen=True, slots=True)
class TripStopEntry:
    """One ``stop_times.txt`` row: a single ordered stop visit of a MAX trip.

    The owning ``trip_id`` is handled by the streaming grouping in S4 and the
    assembly indexes in S5 — entries deliberately stay lean. Times are
    GTFS ``HH:MM:SS`` values converted to seconds by ``csvio.get_time_secs``
    (hours beyond 23 supported); ``None`` means the cell was absent/empty.
    """

    stop_id: str
    stop_sequence: int
    arrival_time_secs: int | None = None
    departure_time_secs: int | None = None


@dataclass(frozen=True, slots=True)
class ShapePoint:
    """One ``shapes.txt`` point. Ordering by ``shape_pt_sequence`` is the
    parser's job (S4); this record stores the ordered result."""

    shape_pt_sequence: int
    lat: float
    lon: float


@dataclass(frozen=True, slots=True)
class ShapeGeometry:
    """All ordered points of one ``shape_id``.

    Usability follows Rev. 2: a shape is usable when it has at least 2 valid
    points. The rule is derived, not stored, so it can never go stale.
    """

    shape_id: str
    points: tuple[ShapePoint, ...] = ()

    @property
    def is_usable(self) -> bool:
        return len(self.points) >= 2


@dataclass(frozen=True, slots=True)
class GtfsManifest:
    """Dataset identity written by promotion and read back by reuse checks.

    ``sha256`` is the download checksum verified against the staged bytes
    (plan Revs. 6–7); ``fetched_at_utc_ms`` is UTC epoch milliseconds
    (Rev. 4 change 2). Both paths are absolute: the extracted dataset directory
    and the raw zip stored next to it.

    Note: S2 finalises the exact meaning of ``dataset_size_bytes`` when the
    write path is implemented (Rev. 9 lists "sizes" in the plural).
    """

    feed_version: str
    sha256: str
    zip_size_bytes: int
    dataset_size_bytes: int
    fetched_at_utc_ms: int
    zip_path: Path
    dataset_path: Path


# Tagged ``read_manifest()`` result (plan Rev. 7 §2). The distinction between
# "absent" and "cannot establish identity" is load-bearing: an unreadable
# manifest may never be silently promoted over (Rev. 8 §1).

@dataclass(frozen=True, slots=True)
class ManifestAbsent:
    """The dataset directory (or its ``manifest.json``) does not exist."""


@dataclass(frozen=True, slots=True)
class ManifestValid:
    """``manifest.json`` parsed; ``sha256`` is the recorded dataset identity."""

    sha256: str


@dataclass(frozen=True, slots=True)
class ManifestUnreadable:
    """The directory exists but its identity cannot be established."""

    reason: str


ManifestRead = ManifestAbsent | ManifestValid | ManifestUnreadable


@dataclass(frozen=True, slots=True)
class DirectionCoverage:
    """Per-direction coverage inside one route's report.

    ``direction_id`` is ``None`` for the unspecified-direction bucket
    (Rev. 2 change 4).
    """

    direction_id: int | None
    trips_total: int
    trips_with_shape: int


@dataclass(frozen=True, slots=True)
class RouteCoverage:
    """Everything assembly observed about one MAX route (plan §E.2–E.7).

    - ``trips_without_shape``: trips with a blank ``shape_id`` (kept +
      recorded, Rev. 2 change 1).
    - ``usable_shape_ids`` / ``broken_shape_ids``: referenced shapes split by
      the usability rule; referenced-but-broken shapes are recorded here,
      never silently discarded (Rev. 2 change 5).
    - ``reachable_stop_ids``: stop ids reached through the route's trips.
    - ``unmatched_stop_ids``: ids referenced in ``stop_times.txt`` but absent
      from ``stops.txt`` (the 14024/14025 mechanism).
    """

    route_id: str
    directions: tuple[DirectionCoverage, ...]
    trips_without_shape: frozenset[str]
    usable_shape_ids: frozenset[str]
    broken_shape_ids: frozenset[str]
    reachable_stop_ids: frozenset[str]
    unmatched_stop_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Whole-dataset coverage snapshot (plan §E.7).

    Only the six hard invariants fail the load; every other gap is reported
    here as data, so nothing in the tolerated class is ever silent. The
    ``shapes_without_reference`` field was deliberately removed in Rev. 3
    change 2 and is not present.
    """

    routes: tuple[RouteCoverage, ...]


@dataclass(frozen=True, slots=True)
class MaxNetwork:
    """The assembled, validated MAX network dataset (Stage S5, plan §E.2–E.7).

    Built by ``backend.trimet.catalog.build_network`` from a
    :class:`ParsedGtfs`: the MAX-scoped parsed tables plus the two things
    only assembly can provide — the parent-station index and the
    :class:`CoverageReport` recorded while validating the six load-failing
    invariants. Construction fails loudly (``MaxInvariantError``) rather
    than ever storing an invalid network.

    All mappings are snapshotted read-only (same rule as
    ``Route.direction_names``) and sorted by key; ``stop_times`` entries are
    in validated increasing ``stop_sequence`` order, inherited from S4.
    """

    feed_version: str
    feed_start_date: str | None = None
    feed_end_date: str | None = None
    routes: Mapping[str, Route] = MappingProxyType({})
    stops: Mapping[str, Stop] = MappingProxyType({})
    trips: Mapping[str, Trip] = MappingProxyType({})
    stop_times: Mapping[str, tuple[TripStopEntry, ...]] = MappingProxyType({})
    shapes: Mapping[str, ShapeGeometry] = MappingProxyType({})
    # station stop_id (or any referenced parent_station target) -> the stop
    # ids of its platforms. Parentless terminals appear only as plain stops.
    station_index: Mapping[str, frozenset[str]] = MappingProxyType({})
    coverage: CoverageReport = CoverageReport(routes=())
    consistency_notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Snapshot every mapping into a read-only proxy (S1 immutability rule)."""
        for name in (
            "routes",
            "stops",
            "trips",
            "stop_times",
            "shapes",
            "station_index",
        ):
            object.__setattr__(
                self, name, MappingProxyType(dict(getattr(self, name)))
            )
