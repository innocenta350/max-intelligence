"""GTFS table parsers producing the typed S1 records (Stage S4, M1 plan Rev. 9).

Reads the eight feed files whose schemas were verified against the real
TriMet feed in Stage S3 (``docs/gtfs-schema-notes.md``,
``tools/s3_schema/evidence/gtfs_schema_audit.json``) and produces the frozen
records from :mod:`backend.gtfs.types` plus per-file :class:`ParseReport`
accounting, so nothing is silently discarded. All CSV mechanics come from
:mod:`backend.gtfs.csvio` — this module adds no second CSV system.

Public surface (plan §6 function names, realized after S3):

- :func:`parse_dataset` — orchestrator; parses all files in dependency
  order and returns one :class:`ParsedGtfs`.
- :func:`parse_feed_info` — strict; dataset identity (``feed_version`` is
  load-bearing for the S6 manifest).
- :func:`parse_agency` — inspection + consistency notes only; never a hard
  requirement (plan Rev. 2 change 2): a missing, empty, or malformed
  ``agency.txt`` becomes a recorded note, never a load failure.
- :func:`parse_routes` — MAX-filter input rows (whitelist filter, never
  ``route_type`` alone — Streetcar rows also use ``route_type=0``).
- :func:`parse_route_directions` — written from S3's observed
  ``route_id,direction_id,direction_name`` header; blank ``direction_id``
  maps to the ``None`` bucket of ``Route.direction_names``.
- :func:`parse_trips` — MAX-only retention; blank ``direction_id`` -> the
  ``None`` bucket; blank ``shape_id`` tolerated and kept (Rev. 2 changes 1
  and 4).
- :func:`parse_stop_times_filtered` — single streaming pass against the
  frozenset of MAX trip ids; ``stop_sequence`` must strictly increase within
  each trip in file order (S3 verified the real feed does this); unmatched
  stop ids are kept in the entries (the 14024/14025 mechanism) for S5 to
  report.
- :func:`parse_stops` — full parse of every stop row (the table is small),
  platform/station split via ``location_type``, parent-station preserved.
- :func:`parse_shapes` — referenced shapes only (unrelated shapes are
  skipped without parsing); points ordered by ``shape_pt_sequence``; a
  referenced shape with too few points is *not* a parser error — usability
  is a derived, route-level invariant that S5 records.

Row tolerance (plan §E.3, via ``GtfsConfig.max_unrelated_malformed_rows``):
rows belonging to the MAX subset are strict — a malformed cell or a
structurally short/extra row is a hard error with file/line context. Rows
outside the MAX subset are skipped as early as practical (before field
validation) and only counted; once the running count exceeds the configured
budget, :class:`ThresholdExceededError` stops the load. A row whose scope
key is blank (no route/trip/stop/shape id) cannot be attributed to any
route and always counts toward the budget.

Determinism: every returned mapping is keyed and sorted by its identifier,
stop-time entries are in validated increasing ``stop_sequence`` order, and
shape points are sorted by ``shape_pt_sequence`` — the same input always
yields an identical :class:`ParsedGtfs`.

Performance: each file is read in exactly one streaming pass;
``stop_times.txt`` (3.4M rows) and ``shapes.txt`` (1.1M rows) do a set
membership test per row and fully validate only MAX-relevant rows, so
memory stays proportional to the MAX subset (~242k stop-time entries,
~122k shape points, 6k stops), not to the whole feed.

Error context: errors carry file name, line number, and column name only —
never absolute paths, environment data, or credentials.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from backend.gtfs.csvio import (
    GtfsCsvFile,
    RowProblem,
    get_float,
    get_int,
    get_str,
    get_time_secs,
    open_gtfs_csv,
)
from backend.gtfs.errors import (
    GtfsLoadError,
    SchemaViolationError,
    ThresholdExceededError,
)
from backend.gtfs.types import (
    Route,
    ShapeGeometry,
    ShapePoint,
    Stop,
    Trip,
    TripStopEntry,
)
from backend.trimet.config import GtfsConfig

__all__ = [
    "MAX_AGENCY_ID",
    "MAX_ROUTE_IDS",
    "FeedInfo",
    "ParsedGtfs",
    "ParseReport",
    "RouteRow",
    "StopTimesResult",
    "TripsResult",
    "parse_agency",
    "parse_dataset",
    "parse_feed_info",
    "parse_route_directions",
    "parse_routes",
    "parse_shapes",
    "parse_stop_times_filtered",
    "parse_stops",
    "parse_trips",
]

# The approved MAX route whitelist (M0 evidence:
# tools/m0_audit/evidence/routeconfig_selected.json — the five routeSubType
# "Light Rail" routes). Deliberately a constant, not a config knob: route ids
# are verified domain facts, not environment-specific settings. Filtering is
# by this whitelist alone; ``route_type == 0`` also matches Portland
# Streetcar routes 193/194/195 and must never be used on its own.
MAX_ROUTE_IDS: frozenset[str] = frozenset({"90", "100", "190", "200", "290"})

# The agency MAX routes belong to (M0 evidence). Membership is a consistency
# check recorded as a note — never a hard filter (plan Rev. 2 change 2).
MAX_AGENCY_ID: str = "TRIMET"

# The eight files S4 parses, in the order parse_dataset reads them. The
# schemas are the S3-verified ones; calendar/fare/other members of the real
# archive are deliberately not read (M1 scope).
_FILE_FEED_INFO = "feed_info.txt"
_FILE_AGENCY = "agency.txt"
_FILE_ROUTES = "routes.txt"
_FILE_ROUTE_DIRECTIONS = "route_directions.txt"
_FILE_STOPS = "stops.txt"
_FILE_SHAPES = "shapes.txt"
_FILE_TRIPS = "trips.txt"
_FILE_STOP_TIMES = "stop_times.txt"
PARSER_FILES: tuple[str, ...] = (
    _FILE_FEED_INFO,
    _FILE_AGENCY,
    _FILE_ROUTES,
    _FILE_ROUTE_DIRECTIONS,
    _FILE_STOPS,
    _FILE_SHAPES,
    _FILE_TRIPS,
    _FILE_STOP_TIMES,
)

# Columns each file must define (header presence, not value presence). These
# are the S3-verified headers; a feed that drops one fails loudly here
# instead of silently producing ``None`` fields.
_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    _FILE_FEED_INFO: ("feed_version", "feed_start_date", "feed_end_date"),
    _FILE_AGENCY: ("agency_id", "agency_name"),
    _FILE_ROUTES: ("route_id", "route_long_name", "route_color"),
    _FILE_ROUTE_DIRECTIONS: ("route_id", "direction_id", "direction_name"),
    _FILE_STOPS: (
        "stop_id", "stop_name", "stop_lat", "stop_lon",
        "location_type", "parent_station",
    ),
    _FILE_TRIPS: ("route_id", "trip_id", "direction_id", "shape_id"),
    _FILE_STOP_TIMES: (
        "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
    ),
    _FILE_SHAPES: ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
}


# ---------------------------------------------------------------------------
# Parse accounting
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParseReport:
    """Per-file parse accounting; nothing is silently discarded.

    ``row_problems`` snapshots csvio's structural findings (short/extra rows)
    with their line numbers and bounded previews. ``unrelated_malformed_rows``
    counts this file's rows that were skipped as unrelated *and* rejected
    (structurally broken or unusable scope key) — the plan §E.3 tolerance
    budget is shared across all files and enforced while parsing.
    """

    file: str
    row_problems: tuple[RowProblem, ...]
    blank_lines_skipped: int
    unrelated_malformed_rows: int
    notes: tuple[str, ...]


class _RowTolerance:
    """Shared budget for unrelated malformed rows (plan §E.3).

    Counts rows that cannot belong to the MAX subset yet are malformed, and
    fails fast with :class:`ThresholdExceededError` as soon as the configured
    maximum is exceeded — a suspicious feed stops the load instead of being
    heaped into the dataset.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self.count = 0

    def record(
        self, file: str, line_number: int | None = None, detail: str = ""
    ) -> None:
        self.count += 1
        if self.count > self._limit:
            message = (
                f"unrelated malformed rows exceed the configured tolerance of "
                f"{self._limit} (running count {self.count})"
            )
            if detail:
                message += f": {detail}"
            raise ThresholdExceededError(message, file=file, line_number=line_number)


def _open(
    dataset_dir: Path, file_name: str
) -> "GtfsCsvFile":
    """Open one required GTFS file, refusing missing files with context.

    ``agency.txt`` is the deliberate exception (never a hard requirement,
    plan Rev. 2 change 2) and handles its own absence.
    """
    path = dataset_dir / file_name
    if not path.is_file():
        raise SchemaViolationError(
            f"required GTFS file {file_name} is missing from the dataset "
            "directory",
            file=file_name,
        )
    return open_gtfs_csv(path, required_columns=_REQUIRED_COLUMNS[file_name])


def _iter_rows(gtfs: GtfsCsvFile) -> Iterator[tuple[int, dict[str, str], bool]]:
    """Yield ``(line_number, record, had_structural_problem)`` per row.

    csvio appends a row's :class:`RowProblem` immediately before yielding it,
    so growth of the problem list between rows attributes the problem to the
    row just yielded.
    """
    problems = gtfs.report.problems
    marked = len(problems)
    for line_number, record in gtfs.iter_records():
        had_problem = len(problems) > marked
        marked = len(problems)
        yield line_number, record, had_problem


def _report(
    file_name: str,
    gtfs: GtfsCsvFile,
    unrelated: int,
    notes: Sequence[str],
) -> ParseReport:
    return ParseReport(
        file=file_name,
        row_problems=tuple(gtfs.report.problems),
        blank_lines_skipped=gtfs.report.blank_lines_skipped,
        unrelated_malformed_rows=unrelated,
        notes=tuple(notes),
    )


def _empty_report(file_name: str, note: str) -> ParseReport:
    return ParseReport(
        file=file_name,
        row_problems=(),
        blank_lines_skipped=0,
        unrelated_malformed_rows=0,
        notes=(note,),
    )


# ---------------------------------------------------------------------------
# feed_info.txt — dataset identity (strict)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FeedInfo:
    """The single ``feed_info.txt`` row's identity fields.

    Dates stay raw ``YYYYMMDD`` strings: the data model has no date type and
    the manifest (S6) records them verbatim.
    """

    feed_version: str
    feed_start_date: str | None
    feed_end_date: str | None


def parse_feed_info(dataset_dir: Path) -> tuple[FeedInfo, ParseReport]:
    """Parse the dataset identity row; strict — exactly one populated row."""
    file_name = _FILE_FEED_INFO
    with _open(dataset_dir, file_name) as gtfs:
        rows: list[tuple[int, dict[str, str]]] = []
        for line_number, record in gtfs.iter_records():
            rows.append((line_number, record))
        report = _report(file_name, gtfs, 0, ())
    if len(rows) != 1:
        raise SchemaViolationError(
            f"expected exactly one data row, found {len(rows)}",
            file=file_name,
        )
    line_number, record = rows[0]
    feed_version = get_str(record, "feed_version")
    if feed_version is None:
        raise SchemaViolationError(
            "feed_version is blank; dataset identity cannot be established",
            file=file_name,
            line_number=line_number,
            column="feed_version",
        )
    return (
        FeedInfo(
            feed_version=feed_version,
            feed_start_date=get_str(record, "feed_start_date"),
            feed_end_date=get_str(record, "feed_end_date"),
        ),
        report,
    )


# ---------------------------------------------------------------------------
# agency.txt — inspection only, never a hard requirement (Rev. 2 change 2)
# ---------------------------------------------------------------------------


def parse_agency(dataset_dir: Path) -> tuple[dict[str, str], ParseReport]:
    """Inspect agencies (id -> name); every problem becomes a note.

    Missing file, empty file, undecodable content, blank ids, duplicates —
    all are recorded as notes and the load continues, because agency data
    only feeds the consistency check for MAX routes.
    """
    file_name = _FILE_AGENCY
    path = dataset_dir / file_name
    if not path.is_file():
        return {}, _empty_report(
            file_name,
            f"{file_name} is absent; agency inspection skipped (never a "
            "hard requirement)",
        )
    try:
        with open_gtfs_csv(
            path, required_columns=_REQUIRED_COLUMNS[file_name]
        ) as gtfs:
            agencies: dict[str, str] = {}
            seen: set[str] = set()
            notes: list[str] = []
            for line_number, record in gtfs.iter_records():
                agency_id = get_str(record, "agency_id")
                agency_name = get_str(record, "agency_name")
                if agency_id is None or agency_name is None:
                    notes.append(
                        f"line {line_number}: agency_id/agency_name blank; "
                        "row excluded from inspection"
                    )
                    continue
                if agency_id in seen:
                    notes.append(
                        f"line {line_number}: duplicate agency_id "
                        f"{agency_id!r}; first occurrence kept"
                    )
                    continue
                seen.add(agency_id)
                agencies[agency_id] = agency_name
            report = _report(file_name, gtfs, 0, notes)
    except SchemaViolationError as exc:
        return {}, _empty_report(
            file_name, f"{file_name} could not be inspected: {exc}"
        )
    return agencies, report


# ---------------------------------------------------------------------------
# routes.txt — MAX filter input rows
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RouteRow:
    """One ``routes.txt`` row for a MAX route, before direction merge."""

    route_id: str
    route_short_name: str | None
    route_long_name: str | None
    route_color: str | None
    agency_id: str | None


def parse_routes(
    dataset_dir: Path, *, tolerance: _RowTolerance
) -> tuple[dict[str, RouteRow], ParseReport]:
    """Parse MAX routes via the whitelist; refuse a whitelist route gone missing.

    Unrelated rows (bus, Streetcar, Tram) are skipped before field validation
    and counted only when structurally broken. A MAX route id absent from the
    file is a hard error: the approved network would be silently incomplete.
    """
    file_name = _FILE_ROUTES
    rows: dict[str, RouteRow] = {}
    notes: list[str] = []
    unrelated = 0
    with _open(dataset_dir, file_name) as gtfs:
        for line_number, record, had_problem in _iter_rows(gtfs):
            route_id = get_str(record, "route_id")
            if route_id is None or route_id not in MAX_ROUTE_IDS:
                if route_id is None or had_problem:
                    tolerance.record(
                        file_name,
                        line_number,
                        detail="unrelated routes.txt row is malformed",
                    )
                    unrelated += 1
                continue
            if had_problem:
                raise SchemaViolationError(
                    f"routes.txt row for MAX route {route_id!r} is malformed "
                    "(short or extra fields)",
                    file=file_name,
                    line_number=line_number,
                )
            if route_id in rows:
                raise SchemaViolationError(
                    f"duplicate route_id {route_id!r} for a MAX route",
                    file=file_name,
                    line_number=line_number,
                    column="route_id",
                )
            rows[route_id] = RouteRow(
                route_id=route_id,
                route_short_name=get_str(record, "route_short_name"),
                route_long_name=get_str(record, "route_long_name"),
                route_color=get_str(record, "route_color"),
                agency_id=get_str(record, "agency_id"),
            )
        report = _report(file_name, gtfs, unrelated, notes)
    missing = sorted(MAX_ROUTE_IDS - rows.keys())
    if missing:
        raise SchemaViolationError(
            "MAX route id(s) from the approved whitelist are absent from "
            f"routes.txt: {', '.join(missing)}",
            file=file_name,
        )
    return rows, report


# ---------------------------------------------------------------------------
# route_directions.txt — direction names for MAX routes (S3-verified header)
# ---------------------------------------------------------------------------


def parse_route_directions(
    dataset_dir: Path,
    route_ids: Collection[str],
    *,
    tolerance: _RowTolerance,
) -> tuple[dict[str, dict[int | None, str]], ParseReport]:
    """Parse direction names for the given (MAX) routes.

    Returns ``route_id -> {direction_id: name}``; a blank ``direction_id``
    maps to the ``None`` bucket (Rev. 2 change 4). Rows for other routes are
    skipped early. A MAX direction row with a blank name, a malformed
    direction id, or a duplicate ``(route, direction)`` pair is a hard error.
    """
    file_name = _FILE_ROUTE_DIRECTIONS
    wanted = frozenset(route_ids)
    directions: dict[str, dict[int | None, str]] = {rid: {} for rid in wanted}
    notes: list[str] = []
    unrelated = 0
    with _open(dataset_dir, file_name) as gtfs:
        for line_number, record, had_problem in _iter_rows(gtfs):
            route_id = get_str(record, "route_id")
            if route_id is None or route_id not in wanted:
                if route_id is None or had_problem:
                    tolerance.record(
                        file_name,
                        line_number,
                        detail="unrelated route_directions row is malformed",
                    )
                    unrelated += 1
                continue
            if had_problem:
                raise SchemaViolationError(
                    f"route_directions row for MAX route {route_id!r} is "
                    "malformed (short or extra fields)",
                    file=file_name,
                    line_number=line_number,
                )
            direction_id = get_int(
                record, "direction_id", file=file_name, line_number=line_number
            )
            direction_name = get_str(record, "direction_name")
            if direction_name is None:
                raise SchemaViolationError(
                    f"direction_name is blank for a direction of MAX route "
                    f"{route_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="direction_name",
                )
            bucket = directions[route_id]
            if direction_id in bucket:
                raise SchemaViolationError(
                    f"duplicate direction_id {direction_id!r} for MAX route "
                    f"{route_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="direction_id",
                )
            bucket[direction_id] = direction_name
        for route_id in sorted(wanted):
            if not directions[route_id]:
                notes.append(
                    f"MAX route {route_id} has no route_directions rows; "
                    "its direction_names mapping is empty"
                )
        report = _report(file_name, gtfs, unrelated, notes)
    return directions, report


# ---------------------------------------------------------------------------
# trips.txt — MAX-only retention
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TripsResult:
    """MAX trips plus the id sets later files filter against."""

    trips: Mapping[str, Trip]
    trip_ids: frozenset[str]
    shape_ids: frozenset[str]


def parse_trips(
    dataset_dir: Path, *, tolerance: _RowTolerance
) -> tuple[TripsResult, ParseReport]:
    """Parse MAX trips only; blank ``shape_id``/``direction_id`` stay ``None``.

    Unrelated trips are skipped before field validation. Within the MAX
    subset, ``trip_id`` is required and unique, and structural row problems
    are hard errors.
    """
    file_name = _FILE_TRIPS
    trips: dict[str, Trip] = {}
    shape_ids: set[str] = set()
    unrelated = 0
    with _open(dataset_dir, file_name) as gtfs:
        for line_number, record, had_problem in _iter_rows(gtfs):
            route_id = get_str(record, "route_id")
            if route_id is None or route_id not in MAX_ROUTE_IDS:
                if route_id is None or had_problem:
                    tolerance.record(
                        file_name,
                        line_number,
                        detail="unrelated trips.txt row is malformed",
                    )
                    unrelated += 1
                continue
            if had_problem:
                raise SchemaViolationError(
                    f"trips.txt row for MAX route {route_id!r} is malformed "
                    "(short or extra fields)",
                    file=file_name,
                    line_number=line_number,
                )
            trip_id = get_str(record, "trip_id")
            if trip_id is None:
                raise SchemaViolationError(
                    f"trip_id is blank on a trip row for MAX route "
                    f"{route_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="trip_id",
                )
            if trip_id in trips:
                raise SchemaViolationError(
                    f"duplicate trip_id {trip_id!r} within the MAX subset",
                    file=file_name,
                    line_number=line_number,
                    column="trip_id",
                )
            # Blank direction_id -> None bucket; blank shape_id -> None trip
            # with no shape link (Rev. 2 changes 1 and 4). Malformed values
            # raise CellParseError from the getter with row context.
            direction_id = get_int(
                record, "direction_id", file=file_name, line_number=line_number
            )
            shape_id = get_str(record, "shape_id")
            if shape_id is not None:
                shape_ids.add(shape_id)
            trips[trip_id] = Trip(
                trip_id=trip_id,
                route_id=route_id,
                direction_id=direction_id,
                shape_id=shape_id,
            )
        report = _report(file_name, gtfs, unrelated, ())
    return (
        TripsResult(
            trips=trips, trip_ids=frozenset(trips), shape_ids=frozenset(shape_ids)
        ),
        report,
    )


# ---------------------------------------------------------------------------
# stop_times.txt — one streaming pass against the MAX trip ids
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StopTimesResult:
    """Stop times grouped per MAX trip plus every referenced stop id."""

    by_trip: Mapping[str, tuple[TripStopEntry, ...]]
    stop_ids: frozenset[str]


def parse_stop_times_filtered(
    dataset_dir: Path,
    trip_ids: Collection[str],
    *,
    tolerance: _RowTolerance,
) -> tuple[StopTimesResult, ParseReport]:
    """Stream ``stop_times.txt`` once, keeping only MAX trips' rows.

    Every MAX trip id gets a key (empty tuple when the trip has no rows, so
    nothing disappears silently). Within a MAX trip, ``stop_sequence`` must
    be a non-negative integer that strictly increases in file order —
    duplicates and decreases are hard errors (S3 verified the real feed
    behaves this way). Unmatched stop ids are kept in the entries for S5 to
    report; times use :func:`get_time_secs` (hours beyond 23 supported,
    blank -> ``None``).
    """
    file_name = _FILE_STOP_TIMES
    wanted = frozenset(trip_ids)
    grouped: dict[str, list[TripStopEntry]] = {trip_id: [] for trip_id in wanted}
    last_sequence: dict[str, int] = {}
    stop_ids: set[str] = set()
    unrelated = 0
    with _open(dataset_dir, file_name) as gtfs:
        for line_number, record, had_problem in _iter_rows(gtfs):
            trip_id = get_str(record, "trip_id")
            if trip_id is None or trip_id not in wanted:
                if trip_id is None or had_problem:
                    tolerance.record(
                        file_name,
                        line_number,
                        detail="unrelated stop_times row is malformed",
                    )
                    unrelated += 1
                continue
            if had_problem:
                raise SchemaViolationError(
                    f"stop_times row for MAX trip {trip_id!r} is malformed "
                    "(short or extra fields)",
                    file=file_name,
                    line_number=line_number,
                )
            stop_id = get_str(record, "stop_id")
            if stop_id is None:
                raise SchemaViolationError(
                    f"stop_id is blank on a stop_times row for MAX trip "
                    f"{trip_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="stop_id",
                )
            sequence = get_int(
                record, "stop_sequence", file=file_name, line_number=line_number
            )
            if sequence is None:
                raise SchemaViolationError(
                    f"stop_sequence is blank on a stop_times row for MAX "
                    f"trip {trip_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="stop_sequence",
                )
            if sequence < 0:
                raise SchemaViolationError(
                    f"stop_sequence {sequence} is negative for MAX trip "
                    f"{trip_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="stop_sequence",
                )
            previous = last_sequence.get(trip_id)
            if previous is not None and sequence <= previous:
                raise SchemaViolationError(
                    f"stop_sequence {sequence} does not strictly increase "
                    f"(previous {previous}) for MAX trip {trip_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="stop_sequence",
                )
            last_sequence[trip_id] = sequence
            grouped[trip_id].append(
                TripStopEntry(
                    stop_id=stop_id,
                    stop_sequence=sequence,
                    arrival_time_secs=get_time_secs(
                        record,
                        "arrival_time",
                        file=file_name,
                        line_number=line_number,
                    ),
                    departure_time_secs=get_time_secs(
                        record,
                        "departure_time",
                        file=file_name,
                        line_number=line_number,
                    ),
                )
            )
            stop_ids.add(stop_id)
        by_trip = {
            trip_id: tuple(grouped[trip_id]) for trip_id in sorted(grouped)
        }
        report = _report(file_name, gtfs, unrelated, ())
    return StopTimesResult(by_trip=by_trip, stop_ids=frozenset(stop_ids)), report


# ---------------------------------------------------------------------------
# stops.txt — full parse, platform/station split, parent-station checks
# ---------------------------------------------------------------------------


def _parse_stop_row(
    record: Mapping[str, str], file_name: str, line_number: int
) -> Stop:
    """Build one :class:`Stop`; raises on malformed typed cells."""
    return Stop(
        stop_id=get_str(record, "stop_id") or "",
        stop_name=get_str(record, "stop_name"),
        stop_lat=get_float(
            record, "stop_lat", file=file_name, line_number=line_number
        ),
        stop_lon=get_float(
            record, "stop_lon", file=file_name, line_number=line_number
        ),
        location_type=get_int(
            record, "location_type", file=file_name, line_number=line_number
        ),
        parent_station=get_str(record, "parent_station"),
    )


def parse_stops(
    dataset_dir: Path,
    relevant_stop_ids: Collection[str],
    *,
    tolerance: _RowTolerance,
) -> tuple[dict[str, Stop], ParseReport]:
    """Parse every stop row; MAX-relevant stops are strict, others tolerant.

    "MAX-relevant" means referenced by a MAX trip's stop times or named as a
    parent station of such a stop. Relevant stops must carry parseable
    coordinates; malformed cells or duplicate ids on relevant stops are hard
    errors. The same defects on unreferenced stops count toward the
    tolerance budget instead (a corrupted bus stop must not fail the MAX
    load). Parent-station targets that do not resolve become consistency
    notes — the station index is S5's assembly job.
    """
    file_name = _FILE_STOPS
    relevant = frozenset(relevant_stop_ids)
    parsed: dict[str, tuple[int, Stop]] = {}
    failed: list[tuple[str, int, GtfsLoadError]] = []
    duplicates: list[tuple[str, int]] = []
    notes: list[str] = []
    unrelated = 0
    with _open(dataset_dir, file_name) as gtfs:
        for line_number, record, had_problem in _iter_rows(gtfs):
            stop_id = get_str(record, "stop_id")
            if stop_id is None:
                tolerance.record(
                    file_name,
                    line_number,
                    detail="stop_id is blank; row cannot be attributed",
                )
                unrelated += 1
                continue
            if stop_id in parsed:
                duplicates.append((stop_id, line_number))
                continue
            if had_problem:
                # Short/extra rows are structural defects: strict for
                # MAX-relevant stops, budget-counted and skipped otherwise
                # (an unmapped column must never masquerade as a None field).
                if stop_id in relevant:
                    raise SchemaViolationError(
                        f"stops.txt row for MAX-relevant stop {stop_id!r} is "
                        "malformed (short or extra fields)",
                        file=file_name,
                        line_number=line_number,
                    )
                tolerance.record(
                    file_name,
                    line_number,
                    detail="unrelated stops.txt row is malformed",
                )
                unrelated += 1
                continue
            try:
                stop = _parse_stop_row(record, file_name, line_number)
            except GtfsLoadError as exc:
                failed.append((stop_id, line_number, exc))
                continue
            parsed[stop_id] = (line_number, stop)

        # Relevance extends one level up: parent stations of MAX platforms.
        relevant_with_parents = set(relevant)
        for stop_id, (_line, stop) in parsed.items():
            if stop_id in relevant and stop.parent_station:
                relevant_with_parents.add(stop.parent_station)

        for stop_id, line_number, exc in failed:
            if stop_id in relevant_with_parents:
                raise exc
            tolerance.record(file_name, line_number, detail=str(exc))
            unrelated += 1
        for stop_id, line_number in duplicates:
            if stop_id in relevant_with_parents:
                raise SchemaViolationError(
                    f"duplicate stop_id {stop_id!r} for a MAX-relevant stop",
                    file=file_name,
                    line_number=line_number,
                    column="stop_id",
                )
            tolerance.record(
                file_name, line_number, detail=f"duplicate stop_id {stop_id!r}"
            )
            unrelated += 1
        for stop_id, (_line, stop) in parsed.items():
            if stop_id not in relevant:
                continue
            # Missing coordinates are a hard error regardless of any other
            # defect, so this check deliberately precedes the parent note.
            if stop.stop_lat is None or stop.stop_lon is None:
                raise SchemaViolationError(
                    f"MAX-relevant stop {stop_id!r} is missing coordinates",
                    file=file_name,
                    line_number=_line_of(parsed, stop_id),
                    column="stop_lat",
                )
            if stop.parent_station and stop.parent_station not in parsed:
                notes.append(
                    f"MAX-relevant stop {stop_id} references parent_station "
                    f"{stop.parent_station!r}, which is absent from stops.txt"
                )
        report = _report(file_name, gtfs, unrelated, notes)
    return {stop_id: stop for stop_id, (_line, stop) in parsed.items()}, report


def _line_of(parsed: dict[str, tuple[int, Stop]], stop_id: str) -> int:
    return parsed[stop_id][0]


# ---------------------------------------------------------------------------
# shapes.txt — referenced shapes only, points ordered by sequence
# ---------------------------------------------------------------------------


def parse_shapes(
    dataset_dir: Path,
    shape_ids: Collection[str],
    *,
    tolerance: _RowTolerance,
) -> tuple[dict[str, ShapeGeometry], ParseReport]:
    """Parse only MAX-referenced shapes; points sorted by sequence.

    Unrelated shapes are skipped without parsing their fields. For referenced
    shapes, coordinates and sequences are strict (blank coordinates, malformed
    values, negative or duplicate sequences are hard errors). A referenced
    shape with fewer than two points is deliberately *not* a parser error:
    usability is the derived route-level invariant S5 records (Rev. 2
    change 5). Every referenced shape id gets a key — a shape absent from
    ``shapes.txt`` comes back as an empty :class:`ShapeGeometry` (zero
    points, ``is_usable == False``), so the broken reference stays visible
    instead of silently disappearing.
    """
    file_name = _FILE_SHAPES
    wanted = frozenset(shape_ids)
    points: dict[str, list[ShapePoint]] = {shape_id: [] for shape_id in wanted}
    seen_sequences: set[tuple[str, int]] = set()
    unrelated = 0
    with _open(dataset_dir, file_name) as gtfs:
        for line_number, record, had_problem in _iter_rows(gtfs):
            shape_id = get_str(record, "shape_id")
            if shape_id is None or shape_id not in wanted:
                if shape_id is None or had_problem:
                    tolerance.record(
                        file_name,
                        line_number,
                        detail="unrelated shapes.txt row is malformed",
                    )
                    unrelated += 1
                continue
            if had_problem:
                raise SchemaViolationError(
                    f"shapes.txt row for MAX-referenced shape {shape_id!r} is "
                    "malformed (short or extra fields)",
                    file=file_name,
                    line_number=line_number,
                )
            lat = get_float(
                record, "shape_pt_lat", file=file_name, line_number=line_number
            )
            lon = get_float(
                record, "shape_pt_lon", file=file_name, line_number=line_number
            )
            sequence = get_int(
                record,
                "shape_pt_sequence",
                file=file_name,
                line_number=line_number,
            )
            if lat is None or lon is None:
                raise SchemaViolationError(
                    f"shape point coordinates are blank for shape {shape_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="shape_pt_lat",
                )
            if sequence is None:
                raise SchemaViolationError(
                    f"shape_pt_sequence is blank for shape {shape_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="shape_pt_sequence",
                )
            if sequence < 0:
                raise SchemaViolationError(
                    f"shape_pt_sequence {sequence} is negative for shape "
                    f"{shape_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="shape_pt_sequence",
                )
            key = (shape_id, sequence)
            if key in seen_sequences:
                raise SchemaViolationError(
                    f"duplicate shape_pt_sequence {sequence} for shape "
                    f"{shape_id!r}",
                    file=file_name,
                    line_number=line_number,
                    column="shape_pt_sequence",
                )
            seen_sequences.add(key)
            points[shape_id].append(
                ShapePoint(shape_pt_sequence=sequence, lat=lat, lon=lon)
            )
        geometries = {
            shape_id: ShapeGeometry(
                shape_id=shape_id,
                points=tuple(
                    sorted(items, key=lambda point: point.shape_pt_sequence)
                ),
            )
            for shape_id, items in sorted(points.items())
        }
        report = _report(file_name, gtfs, unrelated, ())
    return geometries, report


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParsedGtfs:
    """Everything S5's network assembly needs from one parsed dataset.

    ``routes`` contains MAX routes only; ``trips`` and ``stop_times`` are
    MAX-scoped; ``shapes`` contains only MAX-referenced shapes. ``stops``
    keeps every successfully parsed stop row (the table is small and the
    parent-station index needs stations other modes share). All mappings are
    sorted by key; ``stop_times`` entries are in validated increasing
    ``stop_sequence`` order. ``reports`` carries per-file accounting;
    ``unrelated_malformed_rows`` is the global tolerated count.
    """

    feed_version: str
    feed_start_date: str | None
    feed_end_date: str | None
    agencies: Mapping[str, str]
    routes: Mapping[str, Route]
    stops: Mapping[str, Stop]
    trips: Mapping[str, Trip]
    stop_times: Mapping[str, tuple[TripStopEntry, ...]]
    shapes: Mapping[str, ShapeGeometry]
    reports: Mapping[str, ParseReport]
    unrelated_malformed_rows: int
    consistency_notes: tuple[str, ...]


def parse_dataset(dataset_dir: Path | str, config: GtfsConfig) -> ParsedGtfs:
    """Parse an extracted GTFS dataset directory into :class:`ParsedGtfs`.

    Files are read in dependency order so each filter set is known before it
    is needed: feed_info, agency, routes (whitelist), route_directions,
    trips (MAX), stop_times (MAX trips), stops (MAX-relevant strictness),
    shapes (MAX-referenced). Raises :class:`ThresholdExceededError` when the
    tolerated unrelated-malformed-row budget is exhausted, and
    :class:`SchemaViolationError`/:class:`CellParseError` for structural or
    MAX-scope data problems, always with file/line context.
    """
    dataset = Path(dataset_dir)
    if not dataset.is_dir():
        raise SchemaViolationError(
            f"dataset directory {dataset.name!r} does not exist"
        )
    tolerance = _RowTolerance(config.max_unrelated_malformed_rows)

    feed_info, feed_report = parse_feed_info(dataset)
    agencies, agency_report = parse_agency(dataset)
    route_rows, routes_report = parse_routes(dataset, tolerance=tolerance)
    directions, directions_report = parse_route_directions(
        dataset, route_rows.keys(), tolerance=tolerance
    )
    routes = {
        route_id: Route(
            route_id=route_id,
            direction_names=directions.get(route_id, {}),
            route_short_name=row.route_short_name,
            route_long_name=row.route_long_name,
            route_color=row.route_color,
        )
        for route_id, row in sorted(route_rows.items())
    }
    trips_result, trips_report = parse_trips(dataset, tolerance=tolerance)
    stop_times_result, stop_times_report = parse_stop_times_filtered(
        dataset, trips_result.trip_ids, tolerance=tolerance
    )
    stops, stops_report = parse_stops(
        dataset, stop_times_result.stop_ids, tolerance=tolerance
    )
    shapes, shapes_report = parse_shapes(
        dataset, trips_result.shape_ids, tolerance=tolerance
    )

    # Consistency checks — recorded, never fatal (plan Rev. 2 change 2).
    notes: list[str] = []
    for route_id, row in sorted(route_rows.items()):
        if row.agency_id is None:
            notes.append(f"MAX route {route_id} has no agency_id")
        elif row.agency_id != MAX_AGENCY_ID:
            notes.append(
                f"MAX route {route_id} has agency_id {row.agency_id!r}, "
                f"expected {MAX_AGENCY_ID!r}"
            )
        elif row.agency_id not in agencies:
            notes.append(
                f"MAX route {route_id} agency {row.agency_id!r} is not "
                "defined in agency.txt"
            )
    notes.extend(directions_report.notes)

    reports = {
        report.file: report
        for report in (
            feed_report,
            agency_report,
            routes_report,
            directions_report,
            trips_report,
            stop_times_report,
            stops_report,
            shapes_report,
        )
    }
    return ParsedGtfs(
        feed_version=feed_info.feed_version,
        feed_start_date=feed_info.feed_start_date,
        feed_end_date=feed_info.feed_end_date,
        agencies=dict(sorted(agencies.items())),
        routes=routes,
        stops=dict(sorted(stops.items())),
        trips=dict(sorted(trips_result.trips.items())),
        stop_times=dict(sorted(stop_times_result.by_trip.items())),
        shapes=dict(sorted(shapes.items())),
        reports=dict(sorted(reports.items())),
        unrelated_malformed_rows=tolerance.count,
        consistency_notes=tuple(notes),
    )
