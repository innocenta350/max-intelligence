"""Synthetic parser tests for the S4 GTFS parsers (M1 plan Rev. 9).

These fixtures are built from the S3-verified headers
(``docs/gtfs-schema-notes.md``) but are entirely synthetic: no network
access, no staged feed required. The default feed in :func:`_write_feed` is
a minimal *valid* MAX dataset; each test overrides individual files to
exercise one failure or tolerance behaviour in isolation.

Coverage map (assignment §6 list -> tests):

- minimal valid feed ............ ``test_minimal_valid_feed_parses``
- MAX route filtering ........... ``test_routes_kept_are_exactly_the_whitelist``
- Streetcar exclusion ........... ``test_streetcar_route_type0_excluded``
- direction-name mapping ........ ``test_direction_names_mapping``,
                                  ``test_blank_direction_id_maps_to_none_bucket``
- blank optional fields ......... ``test_blank_optional_fields_stay_none``
- parent / parentless stops ..... ``test_stops_with_and_without_parent``
- trip->route relationships ..... ``test_trips_scoped_to_max_routes``
- trip->shape relationships ..... ``test_shapes_scoped_to_max_trips``
- stop-time grouping/ordering ... ``test_stop_times_grouped_and_ordered``,
                                  ``test_every_max_trip_has_a_stop_times_key``
- times beyond 24h .............. ``test_times_beyond_24_hours``
- missing referenced stops ...... ``test_missing_referenced_stop_kept_not_error``
- missing referenced shapes ..... ``test_missing_referenced_shape_not_error``
- shape with too few points ..... ``test_single_point_shape_is_not_a_parser_error``
- duplicate identifiers ......... ``test_duplicate_*``
- malformed numeric fields ...... ``test_malformed_*``
- negative sequences ............ ``test_negative_stop_sequence_error``
- empty / missing files ......... ``test_missing_required_file_*``,
                                  ``test_empty_file_no_header``
- invalid coordinates ........... ``test_invalid_coords_*`` (tiering)
- tolerance budget .............. ``test_budget_exceeded_raises_threshold``
- agency never hard ............. ``test_agency_*``
- error context ................. ``test_error_context_*``
- determinism ................... ``test_parse_is_deterministic``
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.gtfs.errors import (
    CellParseError,
    GtfsLoadError,
    SchemaViolationError,
    ThresholdExceededError,
)
from backend.gtfs.parser import (
    MAX_ROUTE_IDS,
    ParsedGtfs,
    parse_dataset,
    parse_feed_info,
)
from backend.trimet.config import GtfsConfig

# ---------------------------------------------------------------------------
# Synthetic feed fixtures
# ---------------------------------------------------------------------------

# Minimal *valid* feed: all five whitelist routes present (a missing
# whitelist route is a hard error), one trip per route, two stops, two
# shapes. Headers are exactly the S3-verified required columns.
_DEFAULT_FEED: dict[str, str] = {
    "feed_info.txt": (
        "feed_version,feed_start_date,feed_end_date\n"
        "20260101-20260201-0900,20260101,20260201\n"
    ),
    "agency.txt": (
        "agency_id,agency_name\n"
        "TRIMET,TriMet\n"
    ),
    "routes.txt": (
        "route_id,agency_id,route_short_name,route_long_name,route_type,"
        "route_color\n"
        "90,TRIMET,90,Red Line,0,C41F3E\n"
        "100,TRIMET,100,Blue Line,0,1359AE\n"
        "190,TRIMET,190,Yellow Line,0,FFC52F\n"
        "200,TRIMET,200,Green Line,0,008342\n"
        "290,TRIMET,290,Orange Line,0,D05F27\n"
        # Unrelated rows: a bus route and a Streetcar route (type 0!).
        "1,TRIMET,1,Bus Route,3,\n"
        "193,PSC,193,A Streetcar,0,\n"
    ),
    "route_directions.txt": (
        "route_id,direction_id,direction_name\n"
        "90,0,To Airport\n"
        "90,1,To Hillsboro\n"
        "100,0,Outbound\n"
        "100,1,Inbound\n"
        "190,0,Outbound\n"
        "190,1,Inbound\n"
        "200,0,Outbound\n"
        "200,1,Inbound\n"
        "290,0,Outbound\n"
        "290,1,Inbound\n"
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
        # Blank headsign: S3 verified it is blank feed-wide.
        "90,WKDY,T90,,0,SH1\n"
        "100,WKDY,T100,,1,SH2\n"
        "190,WKDY,T190,,0,\n"
        # Unrelated trip on a bus route.
        "1,WKDY,T1,,0,SHX\n"
    ),
    "stop_times.txt": (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T90,08:00:00,08:00:30,S1P,0\n"
        "T90,25:10:30,25:11:00,S2,1\n"
        "T100,09:00:00,09:00:00,S2,0\n"
    ),
    "shapes.txt": (
        "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
        "SH1,45.50,-122.50,0\n"
        "SH1,45.51,-122.51,1\n"
        # SH2 deliberately out of file order: sorting is the parser's job.
        "SH2,45.52,-122.52,1\n"
        "SH2,45.53,-122.53,0\n"
        # Unrelated shape for the bus trip.
        "SHX,45.60,-122.60,0\n"
    ),
}


def _write_feed(directory: Path, **overrides: str | None) -> Path:
    """Materialise the default feed in ``directory`` with per-file overrides.

    An override value of ``None`` removes that file entirely; a string
    replaces the file's content verbatim.
    """
    files = dict(_DEFAULT_FEED)
    files.update(overrides)
    for name, content in files.items():
        path = directory / name
        if content is None:
            path.unlink(missing_ok=True)
            continue
        path.write_text(content, encoding="utf-8", newline="")
    return directory


def _config(max_unrelated: int = 50) -> GtfsConfig:
    """Direct config construction; only the tolerance budget varies here."""
    return GtfsConfig(max_unrelated_malformed_rows=max_unrelated)


# ---------------------------------------------------------------------------
# Happy path and structure
# ---------------------------------------------------------------------------


def test_minimal_valid_feed_parses(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    assert isinstance(parsed, ParsedGtfs)
    assert parsed.feed_version == "20260101-20260201-0900"
    assert parsed.feed_start_date == "20260101"
    assert parsed.feed_end_date == "20260201"
    assert set(parsed.routes) == MAX_ROUTE_IDS
    assert len(parsed.reports) == 8  # one per parsed file
    assert parsed.unrelated_malformed_rows == 0
    assert parsed.consistency_notes == ()


def test_routes_kept_are_exactly_the_whitelist(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    assert set(parsed.routes) == {"90", "100", "190", "200", "290"}
    # Metadata survives the filter.
    assert parsed.routes["90"].route_long_name == "Red Line"
    assert parsed.routes["90"].route_color == "C41F3E"


def test_streetcar_route_type0_excluded(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    # Route 193 has route_type 0 in the fixture yet must be excluded:
    # filtering is by the whitelist, never route_type alone.
    assert "193" not in parsed.routes
    assert "1" not in parsed.routes


def test_direction_names_mapping(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    assert dict(parsed.routes["90"].direction_names) == {
        0: "To Airport",
        1: "To Hillsboro",
    }


def test_blank_direction_id_maps_to_none_bucket(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "route_directions.txt": (
                "route_id,direction_id,direction_name\n"
                "90,,Unspecified\n"
                "90,0,To Airport\n"
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    assert dict(parsed.routes["90"].direction_names) == {
        None: "Unspecified",
        0: "To Airport",
    }


def test_blank_optional_fields_stay_none(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    # Blank shape_id -> None trip with no shape link (Rev. 2 change 1).
    trip = parsed.trips["T190"]
    assert trip.shape_id is None
    assert trip.direction_id == 0
    # Blank parent_station / platform_code -> None.
    assert parsed.stops["S2"].parent_station is None
    # trip_headsign is blank feed-wide and never required: the trip above
    # parsed fine with an empty headsign cell.


def test_stops_with_and_without_parent(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    assert parsed.stops["S1P"].parent_station == "S1"
    assert parsed.stops["S1"].location_type == 1
    # A parentless terminal stop is not an error.
    assert parsed.stops["S2"].parent_station is None


def test_trips_scoped_to_max_routes(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    assert set(parsed.trips) == {"T90", "T100", "T190"}
    assert parsed.trips["T90"].route_id == "90"
    assert parsed.trips["T100"].direction_id == 1


def test_shapes_scoped_to_max_trips(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    # SHX belongs to the bus trip and must not be parsed.
    assert set(parsed.shapes) == {"SH1", "SH2"}


def test_stop_times_grouped_and_ordered(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    entries = parsed.stop_times["T90"]
    assert [entry.stop_sequence for entry in entries] == [0, 1]
    assert [entry.stop_id for entry in entries] == ["S1P", "S2"]
    assert entries[0].arrival_time_secs == 8 * 3600
    assert entries[1].departure_time_secs == 25 * 3600 + 11 * 60


def test_every_max_trip_has_a_stop_times_key(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    # T190 has no stop_times rows; the key must still exist (empty tuple),
    # so nothing disappears silently.
    assert set(parsed.stop_times) == set(parsed.trips)
    assert parsed.stop_times["T190"] == ()


def test_times_beyond_24_hours(tmp_path: Path) -> None:
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    # 25:10:30 -> 25*3600 + 10*60 + 30 seconds after midnight.
    assert parsed.stop_times["T90"][1].arrival_time_secs == 90630


def test_missing_referenced_stop_kept_not_error(tmp_path: Path) -> None:
    # S9 is referenced by stop_times but absent from stops.txt: not a parser
    # error — the entry keeps the id and S5 reports it as unmatched.
    feed = _write_feed(
        tmp_path,
        **{
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T90,08:00:00,08:00:30,S9,0\n"
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    assert parsed.stop_times["T90"][0].stop_id == "S9"
    assert "S9" not in parsed.stops


def test_missing_referenced_shape_not_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "trips.txt": (
                "route_id,service_id,trip_id,trip_headsign,direction_id,"
                "shape_id\n"
                "90,WKDY,T90,,0,SHMISSING\n"
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    assert parsed.trips["T90"].shape_id == "SHMISSING"
    # The broken reference stays visible as an empty geometry (S5 records it
    # as unusable); it is not a parser error.
    assert parsed.shapes["SHMISSING"].points == ()
    assert parsed.shapes["SHMISSING"].is_usable is False


def test_single_point_shape_is_not_a_parser_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "shapes.txt": (
                "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
                "SH1,45.50,-122.50,0\n"
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    geometry = parsed.shapes["SH1"]
    assert len(geometry.points) == 1
    assert geometry.is_usable is False  # usability is S5's derived rule


# ---------------------------------------------------------------------------
# Duplicates
# ---------------------------------------------------------------------------


def test_duplicate_max_route_id_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "routes.txt": _DEFAULT_FEED["routes.txt"]
            + "90,TRIMET,90,Red Line Duplicate,0,C41F3E\n",
        },
    )
    with pytest.raises(SchemaViolationError) as excinfo:
        parse_dataset(feed, _config())
    assert excinfo.value.column == "route_id"


def test_duplicate_trip_id_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "trips.txt": _DEFAULT_FEED["trips.txt"]
            + "100,WKDY,T90,,1,SH2\n",
        },
    )
    with pytest.raises(SchemaViolationError) as excinfo:
        parse_dataset(feed, _config())
    assert excinfo.value.column == "trip_id"


def test_duplicate_stop_sequence_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T90,08:00:00,08:00:30,S1P,0\n"
                "T90,08:05:00,08:05:30,S2,0\n"
            ),
        },
    )
    with pytest.raises(SchemaViolationError) as excinfo:
        parse_dataset(feed, _config())
    assert excinfo.value.column == "stop_sequence"


def test_decreasing_stop_sequence_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T90,08:00:00,08:00:30,S1P,5\n"
                "T90,08:05:00,08:05:30,S2,3\n"
            ),
        },
    )
    with pytest.raises(SchemaViolationError, match="strictly increase"):
        parse_dataset(feed, _config())


def test_duplicate_relevant_stop_id_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stops.txt": _DEFAULT_FEED["stops.txt"]
            + "S1P,Duplicate Platform,45.5002,-122.5002,0,S1,2\n",
        },
    )
    with pytest.raises(SchemaViolationError, match="duplicate stop_id"):
        parse_dataset(feed, _config())


def test_duplicate_shape_sequence_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "shapes.txt": (
                "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
                "SH1,45.50,-122.50,0\n"
                "SH1,45.51,-122.51,0\n"
                "SH1,45.52,-122.52,1\n"
            ),
        },
    )
    with pytest.raises(SchemaViolationError, match="duplicate shape_pt_sequence"):
        parse_dataset(feed, _config())


# ---------------------------------------------------------------------------
# Malformed values in the MAX scope
# ---------------------------------------------------------------------------


def test_malformed_direction_id_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "trips.txt": (
                "route_id,service_id,trip_id,trip_headsign,direction_id,"
                "shape_id\n"
                "90,WKDY,T90,,notanumber,SH1\n"
            ),
        },
    )
    with pytest.raises(CellParseError) as excinfo:
        parse_dataset(feed, _config())
    assert excinfo.value.column == "direction_id"


def test_malformed_stop_sequence_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T90,08:00:00,08:00:30,S1P,zero\n"
            ),
        },
    )
    with pytest.raises(CellParseError) as excinfo:
        parse_dataset(feed, _config())
    assert excinfo.value.column == "stop_sequence"


def test_malformed_time_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T90,8:00,08:00:30,S1P,0\n"
            ),
        },
    )
    with pytest.raises(CellParseError) as excinfo:
        parse_dataset(feed, _config())
    assert excinfo.value.column == "arrival_time"


def test_negative_stop_sequence_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T90,08:00:00,08:00:30,S1P,-1\n"
            ),
        },
    )
    with pytest.raises(SchemaViolationError, match="negative"):
        parse_dataset(feed, _config())


def test_blank_direction_name_on_max_route_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "route_directions.txt": (
                "route_id,direction_id,direction_name\n"
                "90,0,\n"
            ),
        },
    )
    with pytest.raises(SchemaViolationError, match="direction_name"):
        parse_dataset(feed, _config())


def test_missing_whitelist_route_error(tmp_path: Path) -> None:
    # Route 290 is on the approved whitelist; dropping its row must fail.
    rows = [
        line
        for line in _DEFAULT_FEED["routes.txt"].splitlines()
        if not line.startswith("290,")
    ]
    feed = _write_feed(tmp_path, **{"routes.txt": "\n".join(rows) + "\n"})
    with pytest.raises(SchemaViolationError, match="290"):
        parse_dataset(feed, _config())


# ---------------------------------------------------------------------------
# Missing / empty files
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("file_name", ["routes.txt", "stop_times.txt", "trips.txt"])
def test_missing_required_file_error(tmp_path: Path, file_name: str) -> None:
    feed = _write_feed(tmp_path, **{file_name: None})
    with pytest.raises(SchemaViolationError, match=file_name):
        parse_dataset(feed, _config())


def test_missing_agency_file_is_note_only(tmp_path: Path) -> None:
    feed = _write_feed(tmp_path, **{"agency.txt": None})
    parsed = parse_dataset(feed, _config())
    assert parsed.agencies == {}
    report = parsed.reports["agency.txt"]
    assert any("absent" in note for note in report.notes)


def test_malformed_agency_file_is_note_only(tmp_path: Path) -> None:
    # Wrong header: agency inspection degrades to a note, never a hard error
    # (plan Rev. 2 change 2).
    feed = _write_feed(tmp_path, **{"agency.txt": "not,an,agency,header\n1,2,3\n"})
    parsed = parse_dataset(feed, _config())
    assert parsed.agencies == {}
    assert any(
        "could not be inspected" in note
        for note in parsed.reports["agency.txt"].notes
    )


def test_empty_file_no_header(tmp_path: Path) -> None:
    feed = _write_feed(tmp_path, **{"routes.txt": ""})
    with pytest.raises(SchemaViolationError, match="empty"):
        parse_dataset(feed, _config())


def test_missing_dataset_directory_error(tmp_path: Path) -> None:
    with pytest.raises(SchemaViolationError):
        parse_dataset(tmp_path / "does-not-exist", _config())


# ---------------------------------------------------------------------------
# Tolerance tiering (plan §E.3): MAX strict, unrelated budgeted
# ---------------------------------------------------------------------------


def test_invalid_coords_on_relevant_stop_hard_error(tmp_path: Path) -> None:
    # S1P is referenced by MAX stop times: a malformed coordinate is fatal.
    feed = _write_feed(
        tmp_path,
        **{
            "stops.txt": (
                "stop_id,stop_name,stop_lat,stop_lon,location_type,"
                "parent_station,platform_code\n"
                "S1,Station One,45.5000,-122.5000,1,,\n"
                "S1P,Station One Platform,notafloat,-122.5001,0,S1,1\n"
                "S2,Terminal,45.5100,-122.5100,0,,\n"
            ),
        },
    )
    with pytest.raises(CellParseError) as excinfo:
        parse_dataset(feed, _config())
    assert excinfo.value.column == "stop_lat"
    assert excinfo.value.file == "stops.txt"


def test_blank_coords_on_relevant_stop_hard_error(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stops.txt": (
                "stop_id,stop_name,stop_lat,stop_lon,location_type,"
                "parent_station,platform_code\n"
                "S1,Station One,45.5000,-122.5000,1,,\n"
                "S1P,Station One Platform,,,,S1,1\n"
                "S2,Terminal,45.5100,-122.5100,0,,\n"
            ),
        },
    )
    with pytest.raises(SchemaViolationError, match="missing coordinates"):
        parse_dataset(feed, _config())


def test_invalid_coords_on_unrelated_stop_budget(tmp_path: Path) -> None:
    # The same defect on an unreferenced stop only counts toward the budget.
    feed = _write_feed(
        tmp_path,
        **{
            "stops.txt": (
                "stop_id,stop_name,stop_lat,stop_lon,location_type,"
                "parent_station,platform_code\n"
                "S1,Station One,45.5000,-122.5000,1,,\n"
                "S1P,Station One Platform,45.5001,-122.5001,0,S1,1\n"
                "S2,Terminal,45.5100,-122.5100,0,,\n"
                "BUS1,Bus Stop,notafloat,-122.6000,0,,\n"
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    assert parsed.unrelated_malformed_rows == 1
    assert "BUS1" not in parsed.stops


def test_unrelated_short_row_counted_and_recorded(tmp_path: Path) -> None:
    # A structurally short unrelated trips row: counted toward the budget
    # and visible in the file's row problems.
    feed = _write_feed(
        tmp_path,
        **{
            "trips.txt": (
                "route_id,service_id,trip_id,trip_headsign,direction_id,"
                "shape_id\n"
                "90,WKDY,T90,,0,SH1\n"
                "100,WKDY,T100,,1,SH2\n"
                "190,WKDY,T190,,0,\n"
                "1,WKDY,T1\n"  # short unrelated row
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    assert parsed.unrelated_malformed_rows == 1
    report = parsed.reports["trips.txt"]
    assert report.unrelated_malformed_rows == 1
    assert len(report.row_problems) == 1


def test_budget_exceeded_raises_threshold(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stops.txt": (
                "stop_id,stop_name,stop_lat,stop_lon,location_type,"
                "parent_station,platform_code\n"
                "S1,Station One,45.5000,-122.5000,1,,\n"
                "S1P,Station One Platform,45.5001,-122.5001,0,S1,1\n"
                "S2,Terminal,45.5100,-122.5100,0,,\n"
                "BUS1,Bus Stop,notafloat,-122.6000,0,,\n"
                "BUS2,Bus Stop,notafloat,-122.6001,0,,\n"
                "BUS3,Bus Stop,notafloat,-122.6002,0,,\n"
            ),
        },
    )
    with pytest.raises(ThresholdExceededError):
        parse_dataset(feed, _config(2))  # third bad row exceeds budget 2


# ---------------------------------------------------------------------------
# Agency consistency (never hard, Rev. 2 change 2)
# ---------------------------------------------------------------------------


def test_agency_mismatch_is_consistency_note(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "routes.txt": _DEFAULT_FEED["routes.txt"].replace(
                "90,TRIMET,90,Red Line,0,C41F3E", "90,PSC,90,Red Line,0,C41F3E"
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    assert any(
        "PSC" in note and "TRIMET" in note
        for note in parsed.consistency_notes
    )


def test_dangling_parent_station_is_note(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stops.txt": (
                "stop_id,stop_name,stop_lat,stop_lon,location_type,"
                "parent_station,platform_code\n"
                "S1,Station One,45.5000,-122.5000,1,,\n"
                "S1P,Station One Platform,45.5001,-122.5001,0,GHOST,1\n"
                "S2,Terminal,45.5100,-122.5100,0,,\n"
            ),
        },
    )
    parsed = parse_dataset(feed, _config())
    assert any("GHOST" in note for note in parsed.reports["stops.txt"].notes)


# ---------------------------------------------------------------------------
# Error context and determinism
# ---------------------------------------------------------------------------


def test_error_context_has_file_and_line_no_tmp_path(tmp_path: Path) -> None:
    feed = _write_feed(
        tmp_path,
        **{
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T90,08:00:00,08:00:30,S1P,zero\n"
            ),
        },
    )
    with pytest.raises(GtfsLoadError) as excinfo:
        parse_dataset(feed, _config())
    error = excinfo.value
    assert error.file == "stop_times.txt"
    assert error.line_number is not None and error.line_number >= 2
    # No absolute machine paths in the message.
    assert str(tmp_path) not in str(error)
    assert str(tmp_path) not in error.message


def test_parse_is_deterministic(tmp_path: Path) -> None:
    feed = _write_feed(tmp_path)
    first = parse_dataset(feed, _config())
    second = parse_dataset(feed, _config())
    assert first == second
    # Mappings are sorted by key.
    assert list(first.routes) == sorted(first.routes)
    assert list(first.trips) == sorted(first.trips)
    assert list(first.stops) == sorted(first.stops)
    assert list(first.shapes) == sorted(first.shapes)
    assert list(first.stop_times) == sorted(first.stop_times)


# ---------------------------------------------------------------------------
# Unit-level checks on individual parsers
# ---------------------------------------------------------------------------


def test_parse_feed_info_requires_exactly_one_row(tmp_path: Path) -> None:
    _write_feed(
        tmp_path,
        **{
            "feed_info.txt": (
                "feed_version,feed_start_date,feed_end_date\n"
                "v1,20260101,20260201\n"
                "v2,20260101,20260201\n"
            ),
        },
    )
    with pytest.raises(SchemaViolationError, match="exactly one"):
        parse_feed_info(tmp_path)


def test_parse_shapes_sorts_points_by_sequence(tmp_path: Path) -> None:
    # The default feed lists SH2's points out of order (sequence 1 first).
    parsed = parse_dataset(_write_feed(tmp_path), _config())
    assert [point.shape_pt_sequence for point in parsed.shapes["SH2"].points] == [0, 1]
    assert parsed.shapes["SH2"].points[1].lat == pytest.approx(45.52)
