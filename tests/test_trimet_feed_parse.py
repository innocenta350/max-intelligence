"""Tests for the S9 malformed-data policy (Stage S9, M2 plan REV 2 §6).

Every policy row of plan §6.1 (vehicles), §6.2 (alerts), and §6.3
(TripUpdates) has at least one named test, plus the owner's final-check 2:
a ``FeedRejectedError`` is the *only* outcome of a feed-level defect — no
partially decoded or normalized feed is returned and the exception carries no
model instances.  Malformed variants are built inline (and by deep-copying
the curated fixtures for the end-to-end tests).  The protobuf byte builder is
imported from ``tests.test_trimet_rtproto`` (``tests`` is a package); no test
imports ``probe_s8`` or reads the S8 campaign directory at runtime.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
from pathlib import Path

import pytest

from backend.trimet.feed_errors import (
    FEED_ALERTS,
    FEED_TRIPUPDATES,
    FEED_VEHICLES,
    FeedRejectedError,
    ProtobufDecodeError,
)
from backend.trimet.feed_parse import (
    MAX_DIAGNOSTICS,
    parse_alerts,
    parse_trip_updates,
    parse_vehicles,
)
from backend.trimet.feed_types import (
    AlertsSnapshot,
    FeedDiagnostic,
    FeedParseResult,
    ServiceAlert,
    TripUpdate,
    TripUpdateEntity,
    TripUpdateFeed,
    VehiclePosition,
    VehiclesRequestKind,
    VehiclesRequestProvenance,
    VehiclesSnapshot,
)
from tests.test_trimet_rtproto import (
    build_entity,
    build_feed,
    build_header,
    build_stop_time_update,
    build_trip_descriptor,
    build_trip_update,
    field_fixed32,
    field_len,
    field_varint,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "s8"

PROVENANCE = VehiclesRequestProvenance(
    request_kind=VehiclesRequestKind.DEFAULT,
    since_ms=None,
    requested_at_utc_ms=1_794_024_790_000,
)

VALID_VEHICLE: dict = {
    "vehicleID": 622,
    "type": "rail",
    "routeNumber": 90,
    "latitude": 45.53200912852157,
    "longitude": -122.63347045430686,
    "time": 1_794_024_765_862,
    "expires": 1_794_025_631_000,
    "delay": -557,
    "bearing": 31,
    "tripID": "18328796",
    "garage": "ELMO",
    "source": "aim",
}

VALID_ALERT: dict = {
    "id": 262613,
    "begin": 1_754_994_600_000,
    "end": None,
    "system_wide_flag": False,
    "header_text": "",
    "desc": "No service to the eastbound stops on SE Powell at 168th.",
    "info_link_url": None,
    "routes": [{"id": 74, "type": "B", "no_service_flag": False}],
    "locations": [],
}


def vehicles_raw(vehicle: dict | None = None) -> dict:
    entry = copy.deepcopy(VALID_VEHICLE if vehicle is None else vehicle)
    return {"resultSet": {"queryTime": 1_794_024_790_542, "vehicle": [entry]}}


def alerts_raw(alert: dict | None = None) -> dict:
    entry = copy.deepcopy(VALID_ALERT if alert is None else alert)
    return {"resultSet": {"queryTime": 1_794_024_790_551, "alert": [entry]}}


def assert_no_model_in_exception(exc: BaseException) -> None:
    """Owner final-check 2: a feed rejection carries no model instances."""
    model_types = (
        VehiclesSnapshot,
        AlertsSnapshot,
        TripUpdateFeed,
        TripUpdateEntity,
        TripUpdate,
        VehiclePosition,
        ServiceAlert,
        FeedParseResult,
    )
    for value in vars(exc).values():
        if isinstance(value, tuple):
            for item in value:
                assert not isinstance(item, model_types)
        else:
            assert not isinstance(value, model_types)


# === §6.1 Vehicles ==============================================================


def test_vehicles_missing_result_set_rejects_feed() -> None:
    with pytest.raises(FeedRejectedError, match="resultSet"):
        parse_vehicles({}, PROVENANCE)


def test_vehicles_non_dict_payload_rejects_feed() -> None:
    with pytest.raises(FeedRejectedError, match="not a JSON object"):
        parse_vehicles([1, 2, 3], PROVENANCE)  # type: ignore[arg-type]


def test_vehicles_missing_query_time_rejects_feed() -> None:
    raw = vehicles_raw()
    del raw["resultSet"]["queryTime"]
    with pytest.raises(FeedRejectedError, match="queryTime"):
        parse_vehicles(raw, PROVENANCE)


def test_vehicles_invalid_query_time_rejects_feed() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["queryTime"] = "not-a-number"
    with pytest.raises(FeedRejectedError, match="queryTime"):
        parse_vehicles(raw, PROVENANCE)


def test_vehicles_non_list_vehicle_array_rejects_feed() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["vehicle"] = {"vehicleID": 622}
    with pytest.raises(FeedRejectedError, match="must be a list"):
        parse_vehicles(raw, PROVENANCE)


def test_vehicles_empty_array_yields_empty_snapshot() -> None:
    raw = {"resultSet": {"queryTime": 1_794_024_790_542}}
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics == ()


def test_vehicles_missing_vehicle_id_rejects_record() -> None:
    raw = vehicles_raw()
    del raw["resultSet"]["vehicle"][0]["vehicleID"]
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert len(result.diagnostics) == 1
    diag = result.diagnostics[0]
    assert (diag.feed, diag.field, diag.reason) == (
        FEED_VEHICLES, "vehicleID", "missing_required_field"
    )


def test_vehicles_invalid_vehicle_id_rejects_record() -> None:
    raw = vehicles_raw({"vehicleID": "622", **{
        k: v for k, v in VALID_VEHICLE.items() if k != "vehicleID"
    }})
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].reason == "invalid_field"


def test_vehicles_missing_time_rejects_record() -> None:
    raw = vehicles_raw()
    del raw["resultSet"]["vehicle"][0]["time"]
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].field == "time"
    assert result.diagnostics[0].reason == "missing_required_field"


def test_vehicles_invalid_time_rejects_record() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["vehicle"][0]["time"] = -5
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].reason == "invalid_timestamp"


def test_vehicles_missing_expires_rejects_record() -> None:
    raw = vehicles_raw()
    del raw["resultSet"]["vehicle"][0]["expires"]
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].field == "expires"


def test_vehicles_invalid_expires_rejects_record() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["vehicle"][0]["expires"] = 1.5
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].reason == "invalid_timestamp"


def test_vehicles_missing_type_rejects_record() -> None:
    raw = vehicles_raw()
    del raw["resultSet"]["vehicle"][0]["type"]
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].field == "type"


def test_vehicles_missing_route_number_rejects_record() -> None:
    raw = vehicles_raw()
    del raw["resultSet"]["vehicle"][0]["routeNumber"]
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].field == "routeNumber"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 91.0),
        ("latitude", -90.1),
        ("longitude", 181.0),
        ("longitude", -180.5),
        ("latitude", "45.5"),
        ("longitude", None),
    ],
)
def test_vehicles_invalid_coordinates_reject_record(field: str, value: object) -> None:
    raw = vehicles_raw()
    if value is None:
        del raw["resultSet"]["vehicle"][0][field]
    else:
        raw["resultSet"]["vehicle"][0][field] = value
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].field == field
    assert result.diagnostics[0].reason in (
        "coordinates_out_of_range", "missing_required_field", "invalid_field"
    )


def test_vehicles_absent_optional_fields_preserved_as_none() -> None:
    raw = vehicles_raw()
    result = parse_vehicles(raw, PROVENANCE)
    vehicle = result.model.vehicles[0]
    assert vehicle.route_color is None
    assert vehicle.sign_message_long is None
    assert vehicle.in_congestion is None
    assert vehicle.service_date is None
    assert result.diagnostics == ()


def test_vehicles_invalid_optional_field_preserved_as_none_with_diagnostic() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["vehicle"][0]["bearing"] = "north"
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 1  # record retained
    assert result.model.vehicles[0].bearing is None
    assert len(result.diagnostics) == 1
    assert result.diagnostics[0].reason == "invalid_optional_field"
    assert result.diagnostics[0].field == "bearing"


def test_vehicles_unknown_fields_ignored() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["vehicle"][0]["someFutureField"] = {"nested": [1, 2]}
    raw["resultSet"]["alsoNew"] = True
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 1
    assert result.diagnostics == ()


def test_vehicles_record_rejection_does_not_cascade() -> None:
    raw = vehicles_raw()
    bad = dict(VALID_VEHICLE, vehicleID=999)
    del bad["latitude"]
    raw["resultSet"]["vehicle"] = [
        dict(VALID_VEHICLE),
        bad,
        dict(VALID_VEHICLE, vehicleID=623),
    ]
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 2
    assert [v.vehicle_id for v in result.model.vehicles] == [622, 623]
    assert len(result.diagnostics) == 1
    assert result.diagnostics[0].record_key == "999"


def test_vehicles_time_and_expires_preserved_verbatim() -> None:
    result = parse_vehicles(vehicles_raw(), PROVENANCE)
    vehicle = result.model.vehicles[0]
    assert vehicle.time_utc_ms == 1_794_024_765_862
    assert vehicle.expires_utc_ms == 1_794_025_631_000


def test_vehicles_snapshot_carries_provenance_and_no_authority() -> None:
    result = parse_vehicles(vehicles_raw(), PROVENANCE)
    assert result.model.provenance is PROVENANCE
    assert result.model.query_time_utc_ms == 1_794_024_790_542
    assert not any(
        "authoritative" in name or "authority" in name
        for name in dir(result.model)
        if not name.startswith("_")
    )


def test_vehicles_non_dict_vehicle_entry_rejects_record() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["vehicle"] = ["not-a-vehicle"]
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 0
    assert result.diagnostics[0].reason == "invalid_record"
    assert result.diagnostics[0].record_key == "vehicle[0]"


# --- Diagnostics cap (plan §6) --------------------------------------------------


def test_diagnostics_capped_at_200_with_overflow_entry() -> None:
    raw = {"resultSet": {"queryTime": 1_794_024_790_542, "vehicle": []}}
    broken = dict(VALID_VEHICLE)
    del broken["latitude"]
    raw["resultSet"]["vehicle"] = [broken for _ in range(250)]
    result = parse_vehicles(raw, PROVENANCE)
    assert len(result.diagnostics) == MAX_DIAGNOSTICS + 1
    overflow = result.diagnostics[-1]
    assert overflow.reason == "diagnostics_overflow"
    assert overflow.detail is not None and "50 diagnostics suppressed" in overflow.detail


def test_diagnostics_are_credential_free() -> None:
    raw = vehicles_raw()
    raw["resultSet"]["vehicle"][0]["latitude"] = 999.0
    result = parse_vehicles(raw, PROVENANCE)
    for diag in result.diagnostics:
        blob = json.dumps(dataclasses.asdict(diag))
        assert "appID" not in blob
        assert "http" not in blob


# === §6.2 Alerts ================================================================


def test_alerts_missing_result_set_rejects_feed() -> None:
    with pytest.raises(FeedRejectedError, match="resultSet"):
        parse_alerts({})


def test_alerts_invalid_query_time_rejects_feed() -> None:
    raw = alerts_raw()
    raw["resultSet"]["queryTime"] = None
    with pytest.raises(FeedRejectedError, match="queryTime"):
        parse_alerts(raw)


def test_alerts_missing_id_rejects_record() -> None:
    raw = alerts_raw()
    del raw["resultSet"]["alert"][0]["id"]
    result = parse_alerts(raw)
    assert result.model.alert_count == 0
    assert result.diagnostics[0].field == "id"
    assert result.diagnostics[0].reason == "missing_required_field"


def test_alerts_invalid_id_rejects_record() -> None:
    raw = alerts_raw()
    raw["resultSet"]["alert"][0]["id"] = "262613"
    result = parse_alerts(raw)
    assert result.model.alert_count == 0
    assert result.diagnostics[0].reason == "invalid_field"


def test_alerts_absent_text_fields_are_none() -> None:
    raw = alerts_raw()
    for key in ("header_text", "desc", "info_link_url"):
        del raw["resultSet"]["alert"][0][key]
    result = parse_alerts(raw)
    alert = result.model.alerts[0]
    assert alert.header_text is None
    assert alert.description is None
    assert alert.info_link_url is None
    assert result.diagnostics == ()


def test_alerts_absent_begin_end_are_none() -> None:
    raw = alerts_raw()
    del raw["resultSet"]["alert"][0]["begin"]
    del raw["resultSet"]["alert"][0]["end"]
    result = parse_alerts(raw)
    alert = result.model.alerts[0]
    assert alert.begin_utc_ms is None
    assert alert.end_utc_ms is None
    assert result.diagnostics == ()


def test_alerts_null_end_is_not_an_error() -> None:
    # S8/A5 verified observation: null end on long-running construction alerts.
    raw = alerts_raw()
    raw["resultSet"]["alert"][0]["end"] = None
    result = parse_alerts(raw)
    assert result.model.alerts[0].end_utc_ms is None
    assert result.diagnostics == ()


def test_alerts_invalid_begin_preserved_as_none_with_diagnostic() -> None:
    raw = alerts_raw()
    raw["resultSet"]["alert"][0]["begin"] = "yesterday"
    result = parse_alerts(raw)
    assert result.model.alert_count == 1  # record retained
    assert result.model.alerts[0].begin_utc_ms is None
    assert result.diagnostics[0].field == "begin"
    assert result.diagnostics[0].reason == "invalid_timestamp"


def test_alerts_absent_system_wide_flag_is_none() -> None:
    raw = alerts_raw()
    del raw["resultSet"]["alert"][0]["system_wide_flag"]
    result = parse_alerts(raw)
    assert result.model.alerts[0].system_wide_flag is None
    assert result.diagnostics == ()


def test_alerts_route_child_without_id_rejects_whole_alert() -> None:
    raw = alerts_raw()
    raw["resultSet"]["alert"][0]["routes"] = [{"type": "B", "no_service_flag": False}]
    result = parse_alerts(raw)
    assert result.model.alert_count == 0  # whole alert rejected, conservatively
    assert result.diagnostics[0].field == "routes[].id"


def test_alerts_non_dict_route_child_rejects_whole_alert() -> None:
    raw = alerts_raw()
    raw["resultSet"]["alert"][0]["routes"] = ["74"]
    result = parse_alerts(raw)
    assert result.model.alert_count == 0
    assert result.diagnostics[0].reason == "invalid_route_child"


def test_alerts_absent_locations_empty_tuple() -> None:
    raw = alerts_raw()
    del raw["resultSet"]["alert"][0]["locations"]
    result = parse_alerts(raw)
    assert result.model.alerts[0].locations == ()


def test_alerts_absent_routes_empty_tuple() -> None:
    raw = alerts_raw()
    del raw["resultSet"]["alert"][0]["routes"]
    result = parse_alerts(raw)
    assert result.model.alerts[0].routes == ()
    assert result.diagnostics == ()


def test_alerts_malformed_location_child_dropped_alert_retained() -> None:
    # Recorded interpretation of plan §6.2: location children never drive MAX
    # classification, so a malformed entry is dropped with a diagnostic while
    # the alert is retained.
    raw = alerts_raw()
    raw["resultSet"]["alert"][0]["locations"] = [
        {"id": 4606, "desc": "SE Powell & 160th", "no_service_flag": False},
        {"desc": "no id here"},
    ]
    result = parse_alerts(raw)
    assert result.model.alert_count == 1
    assert len(result.model.alerts[0].locations) == 1
    assert result.diagnostics[0].reason == "invalid_location_child"


def test_alerts_record_rejection_does_not_cascade() -> None:
    raw = alerts_raw()
    broken = copy.deepcopy(VALID_ALERT)
    del broken["id"]
    raw["resultSet"]["alert"] = [VALID_ALERT, broken, dict(VALID_ALERT, id=270797)]
    result = parse_alerts(raw)
    assert result.model.alert_count == 2
    assert [a.alert_id for a in result.model.alerts] == [262613, 270797]
    assert len(result.diagnostics) == 1


# === §6.3 TripUpdates ===========================================================


def test_tripupdates_structural_defect_rejects_feed() -> None:
    valid = build_feed(entities=(build_entity(),))
    with pytest.raises(ProtobufDecodeError):
        parse_trip_updates(valid[:-5])


def test_tripupdates_missing_header_rejects_feed() -> None:
    with pytest.raises(FeedRejectedError):
        parse_trip_updates(field_len(2, build_entity()))


def test_tripupdates_missing_header_timestamp_rejects_feed() -> None:
    header = build_header(timestamp=None)
    with pytest.raises(FeedRejectedError, match="timestamp"):
        parse_trip_updates(build_feed(header=header))


def test_tripupdates_implausible_header_timestamp_rejects_feed() -> None:
    header = build_header(timestamp=-7)
    with pytest.raises(FeedRejectedError, match="timestamp"):
        parse_trip_updates(build_feed(header=header))


def test_tripupdates_missing_incrementality_rejects_feed() -> None:
    header = build_header(incrementality=None)
    with pytest.raises(FeedRejectedError, match="incrementality"):
        parse_trip_updates(build_feed(header=header))


@pytest.mark.parametrize("incrementality", [1, 2])
def test_tripupdates_non_full_dataset_rejects_feed(incrementality: int) -> None:
    header = build_header(incrementality=incrementality)
    with pytest.raises(FeedRejectedError, match="FULL_DATASET"):
        parse_trip_updates(build_feed(header=header, entities=(build_entity(),)))


def test_tripupdates_missing_version_tolerated_with_diagnostic() -> None:
    header = build_header(version=None)
    result = parse_trip_updates(build_feed(header=header))
    assert result.model.header.gtfs_realtime_version is None
    assert result.diagnostics[0].field == "gtfs_realtime_version"
    assert result.diagnostics[0].reason == "missing_optional_field"


def test_tripupdates_entity_without_id_rejects_record() -> None:
    entity = field_len(3, build_trip_update())  # no field 1 (id)
    result = parse_trip_updates(build_feed(entities=(entity,)))
    assert result.model.entity_count == 0
    assert result.diagnostics[0].field == "id"
    assert result.diagnostics[0].reason == "missing_required_field"


def test_tripupdates_entity_without_trip_id_rejects_record() -> None:
    trip_update = field_len(2, build_stop_time_update())  # trip descriptor absent
    entity = field_len(1, b"1") + field_len(3, trip_update)
    result = parse_trip_updates(build_feed(entities=(entity,)))
    assert result.model.entity_count == 0
    assert result.diagnostics[0].field == "trip.trip_id"


def test_tripupdates_entity_without_usable_updates_rejects_record() -> None:
    entity = build_entity(trip_update=build_trip_update(stop_updates=()))
    result = parse_trip_updates(build_feed(entities=(entity,)))
    assert result.model.entity_count == 0
    assert result.diagnostics[0].reason == "no_usable_stop_time_updates"


def test_tripupdates_unusable_stop_time_update_dropped_entity_retained() -> None:
    bare_stu = build_stop_time_update(arrival_delay=None, departure_delay=None)
    good_stu = build_stop_time_update(arrival_delay=-557)
    trip_update = build_trip_update(stop_updates=(bare_stu, good_stu))
    result = parse_trip_updates(build_feed(entities=(build_entity(trip_update=trip_update),)))
    assert result.model.entity_count == 1
    entity = result.model.entities[0]
    assert len(entity.trip_update.stop_time_updates) == 1
    assert result.diagnostics[0].reason == "no_arrival_or_departure"


def test_tripupdates_missing_vehicle_id_is_none() -> None:
    trip_update = build_trip_update(vehicle_id=None)
    result = parse_trip_updates(build_feed(entities=(build_entity(trip_update=trip_update),)))
    assert result.model.entities[0].trip_update.vehicle_id is None
    assert result.diagnostics == ()


def test_tripupdates_negative_delay_preserved() -> None:
    trip_update = build_trip_update() + field_varint(5, -1_234)
    result = parse_trip_updates(build_feed(entities=(build_entity(trip_update=trip_update),)))
    assert result.model.entities[0].trip_update.delay_seconds == -1_234


def test_tripupdates_route_id_stored_raw_never_classified() -> None:
    descriptor_with_route = build_trip_descriptor("18330502") + field_len(5, b"190")
    trip_update = (
        field_len(1, descriptor_with_route)
        + field_len(2, build_stop_time_update())
        + field_len(3, field_len(1, b"622"))
    )
    result = parse_trip_updates(build_feed(entities=(build_entity(trip_update=trip_update),)))
    trip = result.model.entities[0].trip_update.trip
    assert trip.route_id == "190"  # stored raw...
    # ...and no classification exists anywhere on the model (plan §9).
    assert not any(
        "max" in name.lower() or "route_class" in name.lower()
        for name in dir(trip) if not name.startswith("_")
    )


def test_tripupdates_unknown_fields_single_diagnostic() -> None:
    entity = build_entity(extra_fields=field_varint(99, 1))
    feed = build_feed(
        entities=(entity,),
        extra_fields=field_varint(15, 2) + field_fixed32(17, b"\x00\x00\x00\x00"),
    )
    result = parse_trip_updates(feed)
    assert result.model.entity_count == 1
    unknown_diags = [d for d in result.diagnostics if d.reason == "unknown_fields_skipped"]
    assert len(unknown_diags) == 1
    assert "3 unknown protobuf fields" in (unknown_diags[0].detail or "")


def test_tripupdates_record_rejection_does_not_cascade() -> None:
    good = build_entity(entity_id="1")
    broken = build_entity(entity_id="2", trip_update=build_trip_update(stop_updates=()))
    good2 = build_entity(entity_id="3")
    result = parse_trip_updates(build_feed(entities=(good, broken, good2)))
    assert result.model.entity_count == 2
    assert [e.entity_id for e in result.model.entities] == ["1", "3"]
    assert len(result.diagnostics) == 1
    assert result.diagnostics[0].record_key == "2"


def test_tripupdates_header_timestamp_integer_conversion() -> None:
    result = parse_trip_updates(build_feed())
    header = result.model.header
    assert header.timestamp_raw_seconds == 1_794_024_790
    assert header.timestamp_utc_ms == 1_794_024_790_000  # integer-only ×1000
    assert isinstance(header.timestamp_utc_ms, int)


# --- Owner final-check 2: no partial feed publication ---------------------------


def test_feed_rejections_carry_no_model_instances() -> None:
    # Vehicles: feed-level defect.
    with pytest.raises(FeedRejectedError) as exc_info:
        parse_vehicles({"resultSet": {"queryTime": "bad"}}, PROVENANCE)
    assert_no_model_in_exception(exc_info.value)

    # Alerts: feed-level defect.
    with pytest.raises(FeedRejectedError) as exc_info:
        parse_alerts({"resultSet": {}})
    assert_no_model_in_exception(exc_info.value)

    # TripUpdates: structural defect.
    with pytest.raises(FeedRejectedError) as exc_info:
        parse_trip_updates(b"\x0a")
    assert_no_model_in_exception(exc_info.value)


def test_tripupdates_feed_rejection_is_the_only_outcome() -> None:
    # A non-FULL_DATASET feed must raise; nothing can be captured instead.
    header = build_header(incrementality=1)
    with pytest.raises(FeedRejectedError) as exc_info:
        parse_trip_updates(build_feed(header=header, entities=(build_entity(),)))
    assert exc_info.value.reason == "non_full_dataset"
    assert_no_model_in_exception(exc_info.value)


# === Curated fixture end-to-end (fixture independence) ==========================


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_parse_curated_vehicles_fixture() -> None:
    raw = _load_fixture("vehicles_snapshot_curated.json")
    result = parse_vehicles(raw, PROVENANCE)
    assert result.model.vehicle_count == 7
    assert result.model.query_time_utc_ms == 1_794_024_790_542
    assert result.model.rail_count == 7
    assert {v.route_number for v in result.model.vehicles} == {90, 100, 190, 200, 290}
    assert result.diagnostics == ()


def test_parse_curated_alerts_fixture() -> None:
    raw = _load_fixture("alerts_snapshot_curated.json")
    result = parse_alerts(raw)
    assert result.model.alert_count == 5
    assert result.model.query_time_utc_ms == 1_794_024_790_551
    # Real observations preserved: a null end and an empty header_text exist.
    assert any(a.end_utc_ms is None for a in result.model.alerts)
    assert any(a.header_text == "" for a in result.model.alerts)
    assert result.diagnostics == ()


def test_parse_curated_tripupdate_fixture() -> None:
    payload = (FIXTURE_DIR / "tripupdate_capture_01.bin").read_bytes()
    result = parse_trip_updates(payload)
    assert result.model.header.is_full_dataset
    assert result.model.header.gtfs_realtime_version == "2.0"
    assert result.model.entity_count == 424
    assert result.model.header.timestamp_utc_ms == 1_794_024_790_000


# === Fixture integrity (plan §8) =================================================


def test_fixture_sha256sums_match() -> None:
    lines = (FIXTURE_DIR / "SHA256SUMS").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 4
    for line in lines:
        digest, name = line.split("  ", 1)
        payload = (FIXTURE_DIR / name).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == digest, name


def test_fixture_readme_documents_provenance() -> None:
    readme = (FIXTURE_DIR / "README.md").read_text(encoding="utf-8")
    assert "s8-1794024279858-afcea02f" in readme
    for name in (
        "vehicles_snapshot_curated.json",
        "alerts_snapshot_curated.json",
        "tripupdate_capture_01.bin",
        "tripupdate_capture_10.bin",
    ):
        assert name in readme


def test_json_fixtures_are_credential_free() -> None:
    for name in (
        "vehicles_snapshot_curated.json",
        "alerts_snapshot_curated.json",
    ):
        text = (FIXTURE_DIR / name).read_text(encoding="utf-8")
        assert "appid" not in text.lower()
        # A query string is the credential-bearing risk (?appID=...); plain
        # non-credential URLs (e.g. real info_link_url values) are fine.
        assert "?" not in text
