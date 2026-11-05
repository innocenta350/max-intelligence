"""Synthetic network-assembly tests for Stage S5 (M1 plan §E.1–E.7).

Fully offline: each test materialises a synthetic GTFS feed plus a synthetic
M0 routeConfig evidence file, parses it through the S4 ``parse_dataset``,
and assembles the network through ``build_network``. The default fixture is
a minimal *valid* MAX dataset — all five whitelist routes, each with a trip,
a usable shape, stop times resolving to parsed stops, and direction rows —
so every test isolates exactly one behaviour by overriding one file.

The six load-failing invariants implemented here are the project-notes
summary of plan §E.7 (not verbatim plan text — the Rev. 9 document is not in
the repository); each invariant has a dedicated test.

Coverage map (assignment §4 list -> tests):

- valid network construction .... ``test_valid_network_builds``
- five routes represented ....... ``test_all_five_routes_represented``
- non-MAX type-0 exclusion ...... ``test_non_max_type0_routes_excluded``
- route->trip relationships ..... ``test_route_trip_relationships``
- trip->stop relationships ...... ``test_trip_stop_relationships``
- stop-sequence preservation .... ``test_stop_sequence_ordering_preserved``
- direction mapping ............. ``test_direction_mapping_preserved``
- station/platform relationships  ``test_station_index``
- parentless terminals .......... ``test_parentless_terminal_tolerated``
- missing/unmatched references .. ``test_unmatched_stop_recorded``
- missing/empty shape geometry .. ``test_missing_shape_recorded``,
                                  ``test_single_point_shape_recorded``
- invalid network data .......... the ``test_invariant_*`` family
- empty/minimal datasets ........ ``test_invariant_exactly_five_routes``
                                  (an empty route set cannot pass)
- determinism ................... ``test_build_is_deterministic``
- immutability .................. ``test_network_mappings_are_read_only``
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.gtfs.errors import MaxInvariantError, SchemaViolationError
from backend.gtfs.parser import ParsedGtfs, parse_dataset
from backend.gtfs.types import MaxNetwork
from backend.trimet.catalog import build_network
from backend.trimet.config import GtfsConfig
from backend.trimet.maxfilter import (
    MAX_ROUTE_IDS,
    apply_max_filter,
    load_routeconfig_evidence,
)

# ---------------------------------------------------------------------------
# Synthetic fixtures
# ---------------------------------------------------------------------------


def _evidence_json(extra_routes: tuple[int, ...] = ()) -> str:
    """A minimal M0 routeConfig evidence file covering the five MAX routes.

    Direction descriptions deliberately carry trailing spaces (the documented
    M0 §2.3 finding) so the fallback path's stripping is exercised.
    """
    routes = [
        {
            "route": number,
            "routeSubType": "Light Rail",
            "direction_samples": [
                {"dir": 0, "desc": f"Route {number} Outbound "},
                {"dir": 1, "desc": f"Route {number} Inbound"},
            ],
        }
        for number in (90, 100, 190, 200, 290)
    ]
    routes.extend(
        {"route": number, "routeSubType": "Light Rail", "direction_samples": []}
        for number in extra_routes
    )
    return json.dumps({"summary": {"light_rail_routes": routes}})


_DEFAULT_FEED: dict[str, str] = {
    "feed_info.txt": (
        "feed_version,feed_start_date,feed_end_date\n"
        "20260101-20260201-0900,20260101,20260201\n"
    ),
    "agency.txt": "agency_id,agency_name\nTRIMET,TriMet\n",
    "routes.txt": (
        "route_id,agency_id,route_short_name,route_long_name,route_type,"
        "route_color\n"
        "90,TRIMET,90,Red Line,0,C41F3E\n"
        "100,TRIMET,100,Blue Line,0,1359AE\n"
        "190,TRIMET,190,Yellow Line,0,FFC52F\n"
        "200,TRIMET,200,Green Line,0,008342\n"
        "290,TRIMET,290,Orange Line,0,D05F27\n"
        # Non-MAX rows, including a Streetcar route with route_type 0.
        "1,TRIMET,1,Bus Route,3,\n"
        "193,PSC,193,A Streetcar,0,\n"
    ),
    "route_directions.txt": "route_id,direction_id,direction_name\n"
    + "".join(
        f"{route_id},{direction_id},Route {route_id} Dir {direction_id}\n"
        for route_id in ("90", "100", "190", "200", "290")
        for direction_id in (0, 1)
    ),
    "stops.txt": (
        "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,"
        "platform_code\n"
        "S1,Station One,45.5000,-122.5000,1,,\n"
        "S1P,Station One Platform,45.5001,-122.5001,0,S1,1\n"
        "S2,Terminal,45.5100,-122.5100,0,,\n"
    ),
    "trips.txt": (
        "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
        + "".join(
            f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
            for route_id in ("90", "100", "190", "200", "290")
        )
        + "1,WKDY,T1,,0,SHX\n"
    ),
    "stop_times.txt": (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        + "".join(
            f"T{route_id},08:00:00,08:00:30,S1P,0\n"
            f"T{route_id},25:10:30,25:11:00,S2,1\n"
            for route_id in ("90", "100", "190", "200", "290")
        )
    ),
    "shapes.txt": (
        "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
        + "".join(
            f"SH{route_id},45.50,-122.50,0\nSH{route_id},45.51,-122.51,1\n"
            for route_id in ("90", "100", "190", "200", "290")
        )
        + "SHX,45.60,-122.60,0\n"
    ),
}


def _write_feed(directory: Path, **overrides: str | None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    files = dict(_DEFAULT_FEED)
    files.update(overrides)
    for name, content in files.items():
        path = directory / name
        if content is None:
            path.unlink(missing_ok=True)
            continue
        path.write_text(content, encoding="utf-8", newline="")
    return directory


def _write_evidence(directory: Path, **kwargs: str) -> Path:
    path = directory / "routeconfig_selected.json"
    path.write_text(_evidence_json(**kwargs), encoding="utf-8", newline="")
    return path


def _config(
    evidence_path: Path, max_unrelated: int = 50
) -> GtfsConfig:
    return GtfsConfig(
        m0_routeconfig_evidence_path=evidence_path,
        max_unrelated_malformed_rows=max_unrelated,
    )


def _build(tmp_path: Path, **feed_overrides: str | None) -> MaxNetwork:
    """Parse + assemble in one step, using per-test feed overrides."""
    feed = _write_feed(tmp_path / "feed", **feed_overrides)
    evidence = _write_evidence(tmp_path)
    parsed = parse_dataset(feed, _config(evidence))
    return build_network(parsed, _config(evidence))


def _coverage(network: MaxNetwork, route_id: str):
    return next(rc for rc in network.coverage.routes if rc.route_id == route_id)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_valid_network_builds(tmp_path: Path) -> None:
    network = _build(tmp_path)
    assert isinstance(network, MaxNetwork)
    assert network.feed_version == "20260101-20260201-0900"
    assert network.feed_start_date == "20260101"
    assert network.feed_end_date == "20260201"
    assert len(network.coverage.routes) == 5
    assert network.consistency_notes == ()


def test_all_five_routes_represented(tmp_path: Path) -> None:
    network = _build(tmp_path)
    assert set(network.routes) == MAX_ROUTE_IDS
    assert {rc.route_id for rc in network.coverage.routes} == MAX_ROUTE_IDS


def test_non_max_type0_routes_excluded(tmp_path: Path) -> None:
    network = _build(tmp_path)
    # Streetcar 193 has route_type 0 in the fixture; the bus route 1 is
    # type 3. Neither may enter the MAX network.
    assert "193" not in network.routes
    assert "1" not in network.routes
    assert "T1" not in network.trips
    assert "SHX" not in network.shapes


def test_route_trip_relationships(tmp_path: Path) -> None:
    network = _build(tmp_path)
    assert network.trips["T90"].route_id == "90"
    assert network.trips["T290"].route_id == "290"
    assert set(network.trips) == {f"T{rid}" for rid in MAX_ROUTE_IDS}


def test_trip_stop_relationships(tmp_path: Path) -> None:
    network = _build(tmp_path)
    entries = network.stop_times["T90"]
    assert [entry.stop_id for entry in entries] == ["S1P", "S2"]
    assert entries[0].stop_id in network.stops
    assert entries[1].stop_id in network.stops


def test_stop_sequence_ordering_preserved(tmp_path: Path) -> None:
    network = _build(tmp_path)
    for entries in network.stop_times.values():
        sequences = [entry.stop_sequence for entry in entries]
        assert sequences == sorted(sequences)
    # The >24h time survives the assembly untouched.
    assert network.stop_times["T90"][1].arrival_time_secs == 90630


def test_direction_mapping_preserved(tmp_path: Path) -> None:
    network = _build(tmp_path)
    assert dict(network.routes["90"].direction_names) == {
        0: "Route 90 Dir 0",
        1: "Route 90 Dir 1",
    }


def test_station_index(tmp_path: Path) -> None:
    network = _build(tmp_path)
    assert dict(network.station_index) == {"S1": frozenset({"S1P"})}


def test_station_index_includes_childless_stations(tmp_path: Path) -> None:
    # A station row with no platforms is still a key (empty frozenset) —
    # the real feed's 68th station serves other modes and has no MAX
    # children. The contract: keys are station rows plus referenced parent
    # targets; parentless terminals are never keys.
    feed_override = {
        "stops.txt": (
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,"
            "platform_code\n"
            "S1,Station One,45.5000,-122.5000,1,,\n"
            "S1P,Station One Platform,45.5001,-122.5001,0,S1,1\n"
            "S2,Terminal,45.5100,-122.5100,0,,\n"
            "S3,Other-Mode Station,45.5200,-122.5200,1,,\n"
        ),
    }
    network = _build(tmp_path, **feed_override)
    assert network.station_index["S3"] == frozenset()
    assert network.station_index["S1"] == frozenset({"S1P"})
    assert "S2" not in network.station_index


def test_parentless_terminal_tolerated(tmp_path: Path) -> None:
    network = _build(tmp_path)
    # S2 is a parentless terminal platform: present as a stop, reachable,
    # and not a station-index key — no error, no fabrication.
    assert "S2" in network.stops
    assert network.stops["S2"].parent_station is None
    assert all("S2" not in children for children in network.station_index.values())
    assert "S2" in _coverage(network, "90").reachable_stop_ids


# ---------------------------------------------------------------------------
# Recorded (non-fatal) gaps
# ---------------------------------------------------------------------------


def test_unmatched_stop_recorded(tmp_path: Path) -> None:
    # Route 90 has one valid trip and one trip visiting a stop missing from
    # stops.txt: the unmatched id is recorded (14024/14025 mechanism) while
    # the network stays valid (invariant 5 is existential per route).
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("90", "100", "190", "200", "290")
            )
            + "90,WKDY,T90B,,1,SH90\n"
            + "1,WKDY,T1,,0,SHX\n"
        ),
        "stop_times.txt": _DEFAULT_FEED["stop_times.txt"]
        + "T90B,08:00:00,08:00:30,GHOST,0\n",
    }
    network = _build(tmp_path, **feed_override)
    coverage = _coverage(network, "90")
    assert coverage.reachable_stop_ids == frozenset({"S1P", "S2", "GHOST"})
    assert coverage.unmatched_stop_ids == frozenset({"GHOST"})
    assert "GHOST" not in network.stops


def test_missing_shape_recorded(tmp_path: Path) -> None:
    # Route 90 references a shape absent from shapes.txt (S4 hands S5 an
    # empty geometry); the route still has... no other shape — so this must
    # fail invariant 4. Use two shapes for route 90 instead: one present,
    # one missing, to prove the missing one is recorded while the network
    # still builds.
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("90", "100", "190", "200", "290")
            )
            + "90,WKDY,T90B,,1,SHMISSING\n"
            + "1,WKDY,T1,,0,SHX\n"
        ),
        "stop_times.txt": _DEFAULT_FEED["stop_times.txt"]
        + "T90B,08:00:00,08:00:30,S1P,0\n",
    }
    network = _build(tmp_path, **feed_override)
    coverage = _coverage(network, "90")
    assert coverage.usable_shape_ids == frozenset({"SH90"})
    assert coverage.broken_shape_ids == frozenset({"SHMISSING"})
    assert network.shapes["SHMISSING"].points == ()


def test_single_point_shape_recorded(tmp_path: Path) -> None:
    # A present-but-unusable shape (1 point) is broken, never silently valid.
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("90", "100", "190", "200", "290")
            )
            + "90,WKDY,T90B,,1,SHSHORT\n"
            + "1,WKDY,T1,,0,SHX\n"
        ),
        "stop_times.txt": _DEFAULT_FEED["stop_times.txt"]
        + "T90B,08:00:00,08:00:30,S1P,0\n",
        "shapes.txt": _DEFAULT_FEED["shapes.txt"] + "SHSHORT,45.70,-122.70,0\n",
    }
    network = _build(tmp_path, **feed_override)
    coverage = _coverage(network, "90")
    assert "SHSHORT" in coverage.broken_shape_ids
    assert "SH90" in coverage.usable_shape_ids


def test_trips_without_shape_recorded(tmp_path: Path) -> None:
    # A blank shape_id is tolerated and recorded (Rev. 2 change 1) as long
    # as the route still has a usable shape.
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("90", "100", "190", "200", "290")
            )
            + "90,WKDY,T90B,,1,\n"
            + "1,WKDY,T1,,0,SHX\n"
        ),
        "stop_times.txt": _DEFAULT_FEED["stop_times.txt"]
        + "T90B,08:00:00,08:00:30,S1P,0\n",
    }
    network = _build(tmp_path, **feed_override)
    coverage = _coverage(network, "90")
    assert coverage.trips_without_shape == frozenset({"T90B"})
    assert "T90B" in network.trips and network.trips["T90B"].shape_id is None


def test_orphan_trip_note(tmp_path: Path) -> None:
    # A MAX trip with no stop_times rows is recorded as a consistency note.
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("90", "100", "190", "200", "290")
            )
            + "90,WKDY,T90ORPHAN,,1,SH90\n"
            + "1,WKDY,T1,,0,SHX\n"
        ),
    }
    network = _build(tmp_path, **feed_override)
    assert any(
        "T90ORPHAN" in note and "no stop_times" in note
        for note in network.consistency_notes
    )


def test_direction_none_bucket_coverage(tmp_path: Path) -> None:
    # A trip with a blank direction_id lands in the None bucket (Rev. 2
    # change 4) and shows up in the direction coverage.
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("90", "100", "190", "200", "290")
            )
            + "90,WKDY,T90B,,,\n"
            + "1,WKDY,T1,,0,SHX\n"
        ),
        "stop_times.txt": _DEFAULT_FEED["stop_times.txt"]
        + "T90B,08:00:00,08:00:30,S1P,0\n",
    }
    network = _build(tmp_path, **feed_override)
    directions = {
        dc.direction_id: dc for dc in _coverage(network, "90").directions
    }
    assert set(directions) == {None, 0, 1}
    assert directions[None].trips_total == 1
    assert directions[None].trips_with_shape == 0  # blank shape_id trip


def test_direction_fallback_is_route_and_direction_scoped(
    tmp_path: Path,
) -> None:
    # Two routes fall back simultaneously; each receives ONLY its own
    # evidence descriptions, keyed by the matching direction id — no
    # cross-route or cross-direction bleed into the network.
    feed_override = {
        "route_directions.txt": "route_id,direction_id,direction_name\n"
        + "".join(
            f"{route_id},{direction_id},Route {route_id} Dir {direction_id}\n"
            for route_id in ("190", "200", "290")
            for direction_id in (0, 1)
        ),
    }
    feed = _write_feed(tmp_path / "feed", **feed_override)
    evidence = _write_evidence(tmp_path)
    parsed = parse_dataset(feed, _config(evidence))
    network = build_network(parsed, _config(evidence))
    assert dict(network.routes["90"].direction_names) == {
        0: "Route 90 Outbound",
        1: "Route 90 Inbound",
    }
    assert dict(network.routes["100"].direction_names) == {
        0: "Route 100 Outbound",
        1: "Route 100 Inbound",
    }
    fallback_notes = [
        note for note in network.consistency_notes if "sourced from" in note
    ]
    assert len(fallback_notes) == 2  # one per fallback route, never silent


def test_direction_fallback_to_m0_evidence(tmp_path: Path) -> None:
    # Route 90 has no route_directions rows: the M0 evidence descriptions
    # become the fallback source, recorded as a note (never silent).
    feed_override = {
        "route_directions.txt": "route_id,direction_id,direction_name\n"
        + "".join(
            f"{route_id},{direction_id},Route {route_id} Dir {direction_id}\n"
            for route_id in ("100", "190", "200", "290")
            for direction_id in (0, 1)
        ),
    }
    feed = _write_feed(tmp_path / "feed", **feed_override)
    evidence = _write_evidence(tmp_path)
    parsed = parse_dataset(feed, _config(evidence))
    network = build_network(parsed, _config(evidence))
    assert dict(network.routes["90"].direction_names) == {
        0: "Route 90 Outbound",  # trailing space stripped
        1: "Route 90 Inbound",
    }
    assert any(
        "route_directions" in note and "90" in note
        for note in network.consistency_notes
    )


# ---------------------------------------------------------------------------
# The six load-failing invariants (§E.7, project-notes summary)
# ---------------------------------------------------------------------------


def test_invariant_feed_version_blank(tmp_path: Path) -> None:
    # Invariant 1 is enforced authoritatively by S4 (exactly one row,
    # non-blank); the network-level re-check is exercised directly.
    feed = _write_feed(tmp_path / "feed")
    evidence = _write_evidence(tmp_path)
    parsed = parse_dataset(feed, _config(evidence))
    blank = ParsedGtfs(
        feed_version="",
        feed_start_date=parsed.feed_start_date,
        feed_end_date=parsed.feed_end_date,
        agencies=parsed.agencies,
        routes=parsed.routes,
        stops=parsed.stops,
        trips=parsed.trips,
        stop_times=parsed.stop_times,
        shapes=parsed.shapes,
        reports=parsed.reports,
        unrelated_malformed_rows=0,
        consistency_notes=(),
    )
    with pytest.raises(MaxInvariantError, match="feed_version"):
        build_network(blank, _config(evidence))


def test_invariant_exactly_five_routes(tmp_path: Path) -> None:
    # Invariant 2: exactly five survivors. A feed missing a whitelist route
    # already fails in S4; the filter-level check is exercised directly.
    evidence = load_routeconfig_evidence(_write_evidence(tmp_path))
    feed = _write_feed(tmp_path / "feed")
    parsed = parse_dataset(feed, _config(_write_evidence(tmp_path)))
    reduced = dict(parsed.routes)
    del reduced["290"]
    with pytest.raises(MaxInvariantError, match="290"):
        apply_max_filter(reduced, evidence)
    # A non-whitelist id reaching the filter is also a hard failure.
    inflated = dict(parsed.routes)
    inflated["999"] = parsed.routes["90"]
    with pytest.raises(MaxInvariantError, match="999"):
        apply_max_filter(inflated, evidence)


def test_invariant_route_without_trips(tmp_path: Path) -> None:
    # Invariant 3: route 290 loses its trip (and its stop times).
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("90", "100", "190", "200")
            )
            + "1,WKDY,T1,,0,SHX\n"
        ),
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + "".join(
                f"T{route_id},08:00:00,08:00:30,S1P,0\n"
                f"T{route_id},25:10:30,25:11:00,S2,1\n"
                for route_id in ("90", "100", "190", "200")
            )
        ),
    }
    with pytest.raises(MaxInvariantError) as excinfo:
        _build(tmp_path, **feed_override)
    assert excinfo.value.route_id == "290"
    assert excinfo.value.coverage is not None
    assert excinfo.value.coverage.route_id == "290"


def test_invariant_route_without_usable_shape(tmp_path: Path) -> None:
    # Invariant 4: route 90's only shape reference is missing entirely.
    feed_override = {
        "trips.txt": (
            "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
            "90,WKDY,T90,,0,SHMISSING\n"
            + "".join(
                f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
                for route_id in ("100", "190", "200", "290")
            )
            + "1,WKDY,T1,,0,SHX\n"
        ),
    }
    with pytest.raises(MaxInvariantError) as excinfo:
        _build(tmp_path, **feed_override)
    assert excinfo.value.route_id == "90"
    assert excinfo.value.coverage.broken_shape_ids == frozenset({"SHMISSING"})


def test_invariant_route_without_valid_stop(tmp_path: Path) -> None:
    # Invariant 5: route 90 has trips and stop times, but every stop
    # reference is unresolved — no usable trip-stop list.
    feed_override = {
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            + "".join(
                f"T{route_id},08:00:00,08:00:30,S1P,0\n"
                f"T{route_id},25:10:30,25:11:00,S2,1\n"
                for route_id in ("100", "190", "200", "290")
            )
            + "T90,08:00:00,08:00:30,GHOST1,0\n"
            "T90,09:00:00,09:00:30,GHOST2,1\n"
        ),
    }
    with pytest.raises(MaxInvariantError) as excinfo:
        _build(tmp_path, **feed_override)
    assert excinfo.value.route_id == "90"
    assert excinfo.value.coverage.unmatched_stop_ids == frozenset(
        {"GHOST1", "GHOST2"}
    )


def test_invariant_route_without_direction_source(tmp_path: Path) -> None:
    # Invariant 6: no route_directions rows for route 90 and no evidence
    # directions for it either (the evidence file here omits route 90's
    # direction samples but keeps the Light Rail classification).
    evidence_path = tmp_path / "routeconfig_selected.json"
    evidence_path.write_text(
        json.dumps(
            {
                "summary": {
                    "light_rail_routes": [
                        {"route": 90, "routeSubType": "Light Rail"},
                        *[
                            {
                                "route": number,
                                "routeSubType": "Light Rail",
                                "direction_samples": [
                                    {"dir": 0, "desc": "Out"},
                                    {"dir": 1, "desc": "In"},
                                ],
                            }
                            for number in (100, 190, 200, 290)
                        ],
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    feed_override = {
        "route_directions.txt": "route_id,direction_id,direction_name\n"
        + "".join(
            f"{route_id},{direction_id},Route {route_id} Dir {direction_id}\n"
            for route_id in ("100", "190", "200", "290")
            for direction_id in (0, 1)
        ),
    }
    feed = _write_feed(tmp_path / "feed", **feed_override)
    parsed = parse_dataset(feed, _config(evidence_path))
    with pytest.raises(MaxInvariantError) as excinfo:
        build_network(parsed, _config(evidence_path))
    assert excinfo.value.route_id == "90"
    assert "direction-name source" in excinfo.value.message


def test_invariant_evidence_missing_light_rail_classification(
    tmp_path: Path,
) -> None:
    # Invariant 2's cross-check: route 100 not classified Light Rail.
    evidence_path = tmp_path / "routeconfig_selected.json"
    evidence_path.write_text(
        json.dumps(
            {
                "summary": {
                    "light_rail_routes": [
                        {"route": number, "routeSubType": "Light Rail"}
                        for number in (90, 190, 200, 290)
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    feed = _write_feed(tmp_path / "feed")
    parsed = parse_dataset(feed, _config(evidence_path))
    with pytest.raises(MaxInvariantError, match="100"):
        build_network(parsed, _config(evidence_path))


def test_invariant_evidence_file_missing(tmp_path: Path) -> None:
    feed = _write_feed(tmp_path / "feed")
    parsed = parse_dataset(feed, _config(tmp_path / "absent.json"))
    with pytest.raises(SchemaViolationError, match="absent.json"):
        build_network(parsed, _config(tmp_path / "absent.json"))


def test_extra_evidence_light_rail_route_is_a_note(tmp_path: Path) -> None:
    # A sixth Light Rail route in the evidence is reported (the whitelist is
    # a deliberate, approved scope), not a failure.
    feed = _write_feed(tmp_path / "feed")
    evidence = _write_evidence(tmp_path, extra_routes=(300,))
    parsed = parse_dataset(feed, _config(evidence))
    network = build_network(parsed, _config(evidence))
    assert set(network.routes) == MAX_ROUTE_IDS
    assert any("300" in note for note in network.consistency_notes)


# ---------------------------------------------------------------------------
# Determinism and immutability
# ---------------------------------------------------------------------------


def test_build_is_deterministic(tmp_path: Path) -> None:
    feed = _write_feed(tmp_path / "feed")
    evidence = _write_evidence(tmp_path)
    first = build_network(parse_dataset(feed, _config(evidence)), _config(evidence))
    second = build_network(parse_dataset(feed, _config(evidence)), _config(evidence))
    assert first == second
    assert [rc.route_id for rc in first.coverage.routes] == sorted(MAX_ROUTE_IDS)
    for rc in first.coverage.routes:
        direction_ids = [dc.direction_id for dc in rc.directions]
        assert direction_ids == sorted(
            direction_ids, key=lambda d: (d is not None, d if d is not None else -1)
        )


def test_network_mappings_are_read_only(tmp_path: Path) -> None:
    network = _build(tmp_path)
    with pytest.raises(TypeError):
        network.routes["999"] = network.routes["90"]  # type: ignore[index]
    with pytest.raises(TypeError):
        network.station_index["S1"] = frozenset()  # type: ignore[index]
