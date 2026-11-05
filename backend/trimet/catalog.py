"""MAX network assembly and validation (Stage S5, M1 plan §E.2–E.7).

Joins the S4-parsed tables (:class:`ParsedGtfs`) into the immutable
:class:`MaxNetwork` and validates the six load-failing invariants (§E.7, as
preserved in the project notes — a summary of record, not verbatim plan
text):

1. ``feed_version`` must be valid and have exactly one row.
   (Enforced authoritatively by S4's ``parse_feed_info``; re-checked here
   defensively because the network carries the identity forward.)
2. Exactly five MAX routes must survive filtering. (Enforced by
   ``maxfilter.apply_max_filter`` — whitelist + M0 Light Rail cross-check.)
3. Every MAX route must have at least one trip.
4. Every MAX route must have at least one usable shape (≥ 2 points).
5. Every MAX route must have at least one usable trip-stop list containing
   at least one valid stop — i.e. some trip of the route has stop-time
   entries of which at least one resolves to a parsed stop.
6. Every MAX route must have at least one usable direction-name source.

Everything else is *recorded, never silent* (Rev. 2 changes 1, 2 and 5):
trips with a blank ``shape_id`` (``RouteCoverage.trips_without_shape``),
broken/unusable shape references (``broken_shape_ids``), stop ids referenced
but absent from ``stops.txt`` (``unmatched_stop_ids`` — the 14024/14025
mechanism), agency mismatches and dangling parents (S4 notes), orphan trips
without stop times, and direction-name fallbacks (consistency notes).

Direction-name source selection (§E.2, decided now that S3 has run):
``route_directions.txt`` is the authoritative source (S3-verified header and
MAX mapping). If a route has no direction rows, the fallback is the M0
routeConfig evidence direction descriptions (referenced in place, Rev. 2
change 6). Trip headsigns are structurally unavailable as a source: S4
deliberately does not retain ``trip_headsign`` (``types.py``), and S3
verified it is blank feed-wide, so it could never yield names.

Determinism: trips are grouped in ``trip_id`` order (S4 emits sorted
mappings), coverage routes are sorted by route id, direction coverage is
sorted with the ``None`` bucket first, and every set is a frozenset — the
same input always yields an identical ``MaxNetwork``.
"""

from __future__ import annotations

from collections.abc import Mapping

from backend.gtfs.errors import MaxInvariantError
from backend.gtfs.types import (
    CoverageReport,
    DirectionCoverage,
    MaxNetwork,
    Route,
    RouteCoverage,
    Stop,
    Trip,
)
from backend.gtfs.parser import ParsedGtfs
from backend.trimet.config import GtfsConfig
from backend.trimet.maxfilter import (
    RouteConfigRoute,
    apply_max_filter,
    load_routeconfig_evidence,
)

__all__ = ["build_network"]

# Bound on ids quoted in a single consistency note, so a pathological feed
# cannot balloon note text (same spirit as csvio's bounded previews).
_MAX_NOTE_IDS = 5


def _bounded_ids(ids: list[str]) -> str:
    """Render up to ``_MAX_NOTE_IDS`` sorted ids, with an ellipsis if more."""
    ordered = sorted(ids)
    shown = ordered[:_MAX_NOTE_IDS]
    suffix = f" … and {len(ordered) - len(shown)} more" if len(ordered) > len(shown) else ""
    return ", ".join(shown) + suffix


def _direction_sort_key(direction_id: int | None) -> tuple[int, int]:
    """``None`` bucket first, then numeric ascending — deterministic order."""
    return (direction_id is not None, direction_id if direction_id is not None else -1)


def _route_coverage(
    route: Route,
    trips: list[Trip],
    parsed: ParsedGtfs,
) -> RouteCoverage:
    """Compute one route's :class:`RouteCoverage` (plan §E.2–E.6)."""
    totals: dict[int | None, int] = {}
    with_shape: dict[int | None, int] = {}
    trips_without_shape: list[str] = []
    referenced_shapes: set[str] = set()
    reachable: set[str] = set()
    for trip in trips:
        direction = trip.direction_id
        totals[direction] = totals.get(direction, 0) + 1
        if trip.shape_id is None:
            trips_without_shape.append(trip.trip_id)
        else:
            referenced_shapes.add(trip.shape_id)
            with_shape[direction] = with_shape.get(direction, 0) + 1
        entries = parsed.stop_times.get(trip.trip_id, ())
        reachable.update(entry.stop_id for entry in entries)

    # S4 guarantees a key for every referenced shape (empty geometry when the
    # file lacks it); the membership guard is defense in depth for directly
    # constructed inputs — an absent geometry is simply broken, never usable.
    usable = {
        shape_id
        for shape_id in referenced_shapes
        if shape_id in parsed.shapes and parsed.shapes[shape_id].is_usable
    }
    broken = referenced_shapes - usable

    direction_ids = set(totals) | set(route.direction_names)
    directions = tuple(
        DirectionCoverage(
            direction_id=direction_id,
            trips_total=totals.get(direction_id, 0),
            trips_with_shape=with_shape.get(direction_id, 0),
        )
        for direction_id in sorted(direction_ids, key=_direction_sort_key)
    )
    return RouteCoverage(
        route_id=route.route_id,
        directions=directions,
        trips_without_shape=frozenset(trips_without_shape),
        usable_shape_ids=frozenset(usable),
        broken_shape_ids=frozenset(broken),
        reachable_stop_ids=frozenset(reachable),
        unmatched_stop_ids=frozenset(reachable - parsed.stops.keys()),
    )


def _has_usable_stop_list(
    trips: list[Trip], parsed: ParsedGtfs
) -> bool:
    """§E.7 invariant 5: some trip has entries and at least one valid stop."""
    for trip in trips:
        entries = parsed.stop_times.get(trip.trip_id, ())
        if not entries:
            continue
        if any(entry.stop_id in parsed.stops for entry in entries):
            return True
    return False


def _validate_route(
    coverage: RouteCoverage,
    route: Route,
    trips: list[Trip],
    evidence: Mapping[str, RouteConfigRoute],
    parsed: ParsedGtfs,
    notes: list[str],
) -> Route:
    """Invariants 3–6 for one route; the coverage slice rides every failure.

    Returns the route to store in the network: the input route, or a rebuilt
    one when the M0 evidence was selected as the direction-name source (the
    selected source's names must actually reach downstream consumers).
    """
    route_id = route.route_id
    if not trips:  # invariant 3
        raise MaxInvariantError(
            f"MAX route {route_id} has no trips",
            route_id=route_id,
            coverage=coverage,
        )
    if not coverage.usable_shape_ids:  # invariant 4
        raise MaxInvariantError(
            f"MAX route {route_id} has no usable shape (referenced shapes: "
            f"{len(coverage.broken_shape_ids)} broken, "
            f"{len(coverage.trips_without_shape)} trips without a shape)",
            route_id=route_id,
            coverage=coverage,
        )
    if not _has_usable_stop_list(trips, parsed):  # invariant 5
        raise MaxInvariantError(
            f"MAX route {route_id} has no trip-stop list containing at "
            "least one valid stop",
            route_id=route_id,
            coverage=coverage,
        )
    if route.direction_names:  # invariant 6, primary source
        return route
    evidence_route = evidence.get(route_id)
    if evidence_route is not None and evidence_route.direction_names:
        notes.append(
            f"MAX route {route_id} has no route_directions rows; direction "
            "names sourced from the M0 routeConfig evidence"
        )
        return Route(
            route_id=route_id,
            direction_names=dict(evidence_route.direction_names),
            route_short_name=route.route_short_name,
            route_long_name=route.route_long_name,
            route_color=route.route_color,
        )
    raise MaxInvariantError(
        f"MAX route {route_id} has no usable direction-name source "
        "(no route_directions rows and no M0 routeConfig direction "
        "descriptions)",
        route_id=route_id,
        coverage=coverage,
    )


def _station_index(
    stops: Mapping[str, Stop], notes: list[str]
) -> dict[str, frozenset[str]]:
    """Build the parent-station index (station -> platform stop ids).

    Keys are every ``location_type == 1`` stop plus every referenced
    ``parent_station`` target, so a platform's parent is always findable —
    including the dangling-parent case S4 noted, which stays visible here
    instead of disappearing. Parentless terminals (28 in the audited feed)
    are simply not index keys; that is tolerated, not an error.
    """
    index: dict[str, set[str]] = {}
    for stop_id, stop in stops.items():
        if stop.location_type == 1:
            index.setdefault(stop_id, set())
        if stop.parent_station:
            index.setdefault(stop.parent_station, set()).add(stop_id)
    non_stations = [
        stop_id
        for stop_id, children in index.items()
        if stops.get(stop_id) is not None
        and stops[stop_id].location_type != 1
    ]
    if non_stations:
        notes.append(
            f"{len(non_stations)} parent_station target(s) are not station "
            f"rows: {_bounded_ids(non_stations)}"
        )
    return {
        stop_id: frozenset(children) for stop_id, children in sorted(index.items())
    }


def build_network(parsed: ParsedGtfs, config: GtfsConfig) -> MaxNetwork:
    """Assemble and validate the MAX network from a parsed dataset.

    Raises :class:`MaxInvariantError` (with the relevant
    :class:`RouteCoverage` slice where one exists) when any of the six
    load-failing invariants fails; every other gap is recorded on the
    network's coverage report and consistency notes.
    """
    # Invariant 1 (defensive re-check; S4's parse_feed_info is authoritative).
    if not parsed.feed_version.strip():
        raise MaxInvariantError(
            "feed_version is blank; dataset identity cannot be established"
        )

    # Invariant 2 lives inside apply_max_filter (§E.1).
    evidence = load_routeconfig_evidence(config.m0_routeconfig_evidence_path)
    routes, filter_notes = apply_max_filter(parsed.routes, evidence)

    notes: list[str] = list(filter_notes)

    # Group trips per route in trip_id order (parsed.trips is already sorted).
    trips_by_route: dict[str, list[Trip]] = {route_id: [] for route_id in routes}
    for trip in parsed.trips.values():
        trips_by_route[trip.route_id].append(trip)

    coverage_routes = [
        _route_coverage(routes[route_id], trips_by_route[route_id], parsed)
        for route_id in sorted(routes)
    ]
    coverage = CoverageReport(routes=tuple(coverage_routes))

    for coverage_slice in coverage_routes:
        route_id = coverage_slice.route_id
        resolved = _validate_route(
            coverage_slice,
            routes[route_id],
            trips_by_route[route_id],
            evidence,
            parsed,
            notes,
        )
        if resolved is not routes[route_id]:
            routes[route_id] = resolved
        # Orphan trips (no stop times) are recorded, never silent. S3
        # verified the real feed has none.
        orphans = [
            trip.trip_id
            for trip in trips_by_route[route_id]
            if not parsed.stop_times.get(trip.trip_id)
        ]
        if orphans:
            notes.append(
                f"MAX route {route_id}: {len(orphans)} trip(s) have no "
                f"stop_times rows: {_bounded_ids(orphans)}"
            )

    station_index = _station_index(parsed.stops, notes)
    return MaxNetwork(
        feed_version=parsed.feed_version,
        feed_start_date=parsed.feed_start_date,
        feed_end_date=parsed.feed_end_date,
        routes=routes,
        stops=parsed.stops,
        trips=parsed.trips,
        stop_times=parsed.stop_times,
        shapes=parsed.shapes,
        station_index=station_index,
        coverage=coverage,
        consistency_notes=tuple(notes),
    )
