"""Live-feed schema checks preserving the S3 findings (M1 Stage S3).

S3 inspected the real TriMet feed and recorded its schema in
``docs/gtfs-schema-notes.md`` (evidence:
``tools/s3_schema/evidence/gtfs_schema_audit.json``). These tests pin the
verified facts — exact headers, the MAX direction mapping, and the join
invariants the S4 parser may rely on — so a future feed refresh that breaks
an assumption fails loudly here instead of silently changing parser
behavior.

They are ``live``-marked: they run only when ``LIVE_FEED_TESTS=1`` is set
*and* a staged (extracted) feed exists under the configured work directory.
Otherwise every test in this module skips, leaving the default offline suite
untouched. No network access happens here — the feed must already be staged
via the S2 pipeline.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from backend.gtfs.csvio import open_gtfs_csv
from backend.gtfs.parser import parse_dataset
from backend.trimet.catalog import build_network
from backend.trimet.config import GtfsConfig

pytestmark = pytest.mark.live

MAX_ROUTES = {"90", "100", "190", "200", "290"}

# Exact header layout observed in feed_version 20261011-20261021-0903
# (docs/gtfs-schema-notes.md §2). Order-sensitive on purpose: S4's record
# mapping relies on named columns, but a renamed/reordered column is exactly
# the kind of silent change this module must surface.
EXPECTED_HEADERS = {
    "feed_info.txt": (
        "feed_publisher_name", "feed_publisher_url", "feed_lang", "default_lang",
        "feed_start_date", "feed_end_date", "feed_version", "feed_contact_email",
        "feed_contact_url",
    ),
    "agency.txt": (
        "agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang",
        "agency_phone", "agency_fare_url", "agency_email", "bikes_policy_url",
    ),
    "routes.txt": (
        "route_id", "agency_id", "route_short_name", "route_long_name", "route_type",
        "route_url", "route_color", "route_text_color", "route_sort_order",
        "network_id",
    ),
    "route_directions.txt": ("route_id", "direction_id", "direction_name"),
    "stops.txt": (
        "stop_id", "stop_code", "stop_name", "tts_stop_name", "stop_desc",
        "stop_lat", "stop_lon", "zone_id", "stop_url", "location_type",
        "parent_station", "wheelchair_boarding", "platform_code", "direction",
        "position",
    ),
    "trips.txt": (
        "route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name",
        "direction_id", "block_id", "shape_id", "trip_type", "wheelchair_accessible",
        "bikes_allowed",
    ),
    "stop_times.txt": (
        "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
        "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled",
        "timepoint",
    ),
    "shapes.txt": (
        "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
        "shape_dist_traveled",
    ),
}

# Documented MAX direction mapping (docs/gtfs-schema-notes.md §3).
EXPECTED_MAX_DIRECTIONS = {
    "90": {"0": "To Portland International Airport", "1": "To Hillsboro"},
    "100": {"0": "To Gresham", "1": "To Hillsboro"},
    "190": {"0": "To Expo Center", "1": "To Portland City Center"},
    "200": {"0": "To Clackamas Town Center", "1": "To Gateway Transit Center"},
    "290": {"0": "To Portland City Center", "1": "To Milwaukie"},
}

EXPECTED_MAX_COLORS = {
    "90": "C41F3E",
    "100": "1359AE",
    "190": "FFC52F",
    "200": "008342",
    "290": "D05F27",
}

EXPECTED_TYPE0_ROUTE_IDS = MAX_ROUTES | {"193", "194", "195"}  # + Streetcar


def _staged_dataset_dir() -> Path | None:
    """Newest S2 staging directory under the configured work dir, or None."""
    work_dir = GtfsConfig.from_env().work_dir
    candidates = [
        path for path in work_dir.glob("staging-*")
        if (path / "route_directions.txt").is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


@pytest.fixture(scope="module")
def staged_dir() -> Path:
    if os.environ.get("LIVE_FEED_TESTS") != "1":
        pytest.skip("LIVE_FEED_TESTS not enabled")
    found = _staged_dataset_dir()
    if found is None:
        pytest.skip("no staged GTFS dataset under the configured work dir")
    return found


@pytest.fixture(scope="module")
def schema(staged_dir: Path) -> dict:
    """All schema facts needed by the checks below, computed once."""
    facts: dict = {"headers": {}}
    for name, expected in EXPECTED_HEADERS.items():
        path = staged_dir / name
        if not path.is_file():
            facts["headers"][name] = None
            continue
        with open_gtfs_csv(path) as gtfs:
            facts["headers"][name] = tuple(gtfs.header.columns)

    # routes.txt
    route_ids: set[str] = set()
    type0_ids: set[str] = set()
    max_colors: dict[str, str] = {}
    with open_gtfs_csv(staged_dir / "routes.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            route_ids.add(r["route_id"])
            if r.get("route_type") == "0":
                type0_ids.add(r["route_id"])
                if r["route_id"] in MAX_ROUTES:
                    max_colors[r["route_id"]] = r.get("route_color") or ""
    facts["route_ids"] = route_ids
    facts["type0_ids"] = type0_ids
    facts["max_colors"] = max_colors

    # route_directions.txt
    pairs: Counter = Counter()
    names: dict[tuple[str, str], str] = {}
    with open_gtfs_csv(staged_dir / "route_directions.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            key = (r.get("route_id") or "", r.get("direction_id") or "")
            pairs[key] += 1
            names[key] = r.get("direction_name") or ""
    facts["rd_duplicate_pairs"] = [k for k, n in pairs.items() if n > 1]
    facts["rd_direction_values"] = {d for _r, d in pairs}
    facts["rd_orphans"] = {rid for rid, _d in pairs} - route_ids
    facts["max_direction_names"] = {
        rid: {d: names.get((rid, d)) for d in ("0", "1")}
        for rid in sorted(MAX_ROUTES)
    }

    # stops.txt (two cheap passes: MAX stops need the stop_times result first)
    stop_ids: set[str] = set()
    station_ids: set[str] = set()
    stops_by_id: dict[str, dict] = {}
    with open_gtfs_csv(staged_dir / "stops.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            stop_ids.add(r["stop_id"])
            if r.get("location_type") == "1":
                station_ids.add(r["stop_id"])
            if r["stop_id"] in {"14024", "14025"}:
                facts["m0_locid_present"] = True
    facts["stop_ids"] = stop_ids
    facts["station_ids"] = station_ids
    facts.setdefault("m0_locid_present", False)

    # trips.txt (MAX scope)
    max_trip_ids: set[str] = set()
    max_dirs: Counter = Counter()
    shape_blank = 0
    shapes_by_route: dict[str, set[str]] = defaultdict(set)
    with open_gtfs_csv(staged_dir / "trips.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            if r.get("route_id") not in MAX_ROUTES:
                continue
            max_trip_ids.add(r.get("trip_id") or "")
            max_dirs[r.get("direction_id") or "<blank>"] += 1
            if not (r.get("shape_id") or "").strip():
                shape_blank += 1
            else:
                shapes_by_route[r["route_id"]].add(r["shape_id"])
    facts["max_trip_ids"] = max_trip_ids
    facts["max_direction_ids"] = set(max_dirs)
    facts["max_shape_blank"] = shape_blank
    facts["max_shape_ids"] = set().union(*shapes_by_route.values())

    # stop_times.txt (MAX scope, streamed)
    st_trip_ids: set[str] = set()
    max_stop_ids: set[str] = set()
    unmatched: set[str] = set()
    duplicate_seqs = 0
    out_of_order = 0
    seen_seq: set[tuple[str, int]] = set()
    last_seq: dict[str, int] = {}
    with open_gtfs_csv(staged_dir / "stop_times.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            trip_id = r.get("trip_id") or ""
            if trip_id not in max_trip_ids:
                continue
            st_trip_ids.add(trip_id)
            stop_id = r.get("stop_id") or ""
            max_stop_ids.add(stop_id)
            if stop_id not in stop_ids:
                unmatched.add(stop_id)
            try:
                seq = int(r.get("stop_sequence") or "")
            except ValueError:
                duplicate_seqs += 1
                continue
            if (trip_id, seq) in seen_seq:
                duplicate_seqs += 1
            seen_seq.add((trip_id, seq))
            previous = last_seq.get(trip_id)
            if previous is not None and seq < previous:
                out_of_order += 1
            last_seq[trip_id] = seq
    facts["max_stop_ids"] = max_stop_ids
    facts["trips_without_stop_times"] = len(max_trip_ids - st_trip_ids)
    facts["unmatched_stop_ids"] = unmatched
    facts["duplicate_stop_sequences"] = duplicate_seqs
    facts["out_of_order_stop_sequences"] = out_of_order

    # stops.txt second pass: MAX stop rows (parent join)
    parents: set[str] = set()
    with open_gtfs_csv(staged_dir / "stops.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            if r["stop_id"] in max_stop_ids and (r.get("parent_station") or "").strip():
                parents.add(r["parent_station"])
    facts["max_parent_targets_missing"] = sorted(parents - stop_ids)

    # shapes.txt (MAX scope)
    points: Counter = Counter()
    with open_gtfs_csv(staged_dir / "shapes.txt") as gtfs:
        for _line, r in gtfs.iter_records():
            if r.get("shape_id") in facts["max_shape_ids"]:
                points[r["shape_id"]] += 1
    facts["missing_shapes"] = sorted(facts["max_shape_ids"] - set(points))
    facts["shapes_with_too_few_points"] = sorted(
        sid for sid, n in points.items() if n < 2
    )
    return facts


@pytest.mark.parametrize("name", sorted(EXPECTED_HEADERS))
def test_required_file_has_documented_header(staged_dir: Path, name: str) -> None:
    path = staged_dir / name
    assert path.is_file(), f"{name} missing from staged feed"
    with open_gtfs_csv(path) as gtfs:
        assert tuple(gtfs.header.columns) == EXPECTED_HEADERS[name]


def test_route_directions_pairs_unique_and_directional(schema: dict) -> None:
    assert schema["rd_duplicate_pairs"] == []
    assert schema["rd_direction_values"] == {"0", "1"}
    assert schema["rd_orphans"] == set()


def test_max_direction_names_match_documented_mapping(schema: dict) -> None:
    assert schema["max_direction_names"] == EXPECTED_MAX_DIRECTIONS


def test_max_routes_are_the_expected_type0_rows(schema: dict) -> None:
    assert schema["type0_ids"] == EXPECTED_TYPE0_ROUTE_IDS
    assert schema["max_colors"] == EXPECTED_MAX_COLORS


def test_max_trips_have_direction_and_shape(schema: dict) -> None:
    assert len(schema["max_trip_ids"]) > 0
    assert schema["max_direction_ids"] <= {"0", "1"}
    assert schema["max_shape_blank"] == 0


def test_max_stop_times_join_and_ordering(schema: dict) -> None:
    assert schema["trips_without_stop_times"] == 0
    assert schema["unmatched_stop_ids"] == set()
    assert schema["duplicate_stop_sequences"] == 0
    assert schema["out_of_order_stop_sequences"] == 0


def test_max_shapes_all_present_and_usable(schema: dict) -> None:
    assert schema["missing_shapes"] == []
    assert schema["shapes_with_too_few_points"] == []


def test_parent_station_targets_exist_and_m0_locids_absent(schema: dict) -> None:
    assert schema["max_parent_targets_missing"] == []
    assert schema["m0_locid_present"] is False  # 14024/14025 stay absent


def test_live_parse_dataset_end_to_end(staged_dir: Path) -> None:
    """S4 acceptance: the real staged feed parses through ``parse_dataset``.

    Pins the S3-verified invariants on the parser's output: the whitelist
    routes survive the filter, every MAX trip has a stop-times key with
    strictly increasing sequences, and every referenced shape is present
    and usable. No unrelated row may be malformed in the real feed.
    """
    parsed = parse_dataset(staged_dir, GtfsConfig())

    assert set(parsed.routes) == MAX_ROUTES
    assert parsed.routes["90"].route_color == "C41F3E"
    assert dict(parsed.routes["90"].direction_names) == {
        0: "To Portland International Airport",
        1: "To Hillsboro",
    }
    assert parsed.feed_version.startswith("2026")

    # Every MAX trip has a stop-times key; sequences strictly increase.
    assert set(parsed.stop_times) == set(parsed.trips)
    for entries in parsed.stop_times.values():
        sequences = [entry.stop_sequence for entry in entries]
        assert all(b > a for a, b in zip(sequences, sequences[1:]))

    # All referenced shapes present and usable (S3: 146 shapes, all >= 2 pts).
    referenced = {
        trip.shape_id for trip in parsed.trips.values() if trip.shape_id
    }
    assert referenced <= set(parsed.shapes)
    assert all(parsed.shapes[sid].is_usable for sid in referenced)

    # The real feed is clean: nothing counted toward the tolerance budget.
    assert parsed.unrelated_malformed_rows == 0
    assert parsed.agencies.get("TRIMET") == "TriMet"


def test_live_build_network(staged_dir: Path) -> None:
    """S5 acceptance: the real staged feed assembles into a valid network.

    Asserts relational facts verified in S3 — never hardcoded row counts,
    which are evidence for this build, not permanent invariants. Every
    route must satisfy the six load-failing invariants, and every tolerated
    gap class must be empty *for this verified build*.
    """
    parsed = parse_dataset(staged_dir, GtfsConfig())
    network = build_network(parsed, GtfsConfig())

    assert set(network.routes) == MAX_ROUTES
    assert {rc.route_id for rc in network.coverage.routes} == MAX_ROUTES

    for rc in network.coverage.routes:
        # Invariants 3–6 hold for every route (build_network would have
        # raised otherwise; asserted here for explicit evidence).
        assert rc.directions
        assert {dc.direction_id for dc in rc.directions} <= {0, 1}
        # S3: both declared directions have trips in this build.
        assert all(dc.trips_total > 0 for dc in rc.directions)
        # S3: every MAX trip has a shape, and every referenced shape is
        # present and usable — relational, not count-based.
        assert all(
            dc.trips_with_shape == dc.trips_total for dc in rc.directions
        )
        assert rc.trips_without_shape == frozenset()
        assert rc.usable_shape_ids and not rc.broken_shape_ids
        assert rc.reachable_stop_ids
        # S3: zero unmatched stop references inside the static feed.
        assert rc.unmatched_stop_ids == frozenset()

    # Station/platform structure (S3 §4): every MAX platform's parent is a
    # station row present in the index; parentless terminals are tolerated.
    for stop_id, stop in network.stops.items():
        if stop.parent_station:
            assert stop.parent_station in network.station_index

    # The verified build is clean: no assembly notes at all.
    assert network.consistency_notes == ()
