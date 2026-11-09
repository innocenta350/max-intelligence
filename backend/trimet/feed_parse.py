"""Raw-to-model normalization for the three TriMet live feeds (Stage S9, M2 plan REV 2).

Enforces the field-level malformed-data policy of plan §6.1–§6.3 exactly:

- **Record rejection** — one malformed record is dropped and diagnosed
  (:class:`~backend.trimet.feed_types.FeedDiagnostic`); every other record in
  the same response remains fully trusted.  Rejections never cascade.
- **Feed rejection** — a feed-level defect raises
  :class:`~backend.trimet.feed_errors.FeedRejectedError` and returns **no
  partial model**: no partially decoded or partially normalized feed can be
  returned through any code path, and the exception carries no model
  instances (owner final-check 2, tested).

Diagnostics identify the feed, record, field, and a stable ``reason`` code;
they never contain credentials, URLs, or raw payloads (a scalar offending
value may appear in a bounded ``detail`` only when it is a non-credential
primitive — numbers and booleans, never strings).  Diagnostics are capped at
200 per parse result; beyond the cap a single synthetic
``diagnostics_overflow`` entry records the suppressed count.

Unknown JSON fields are ignored safely.  Unknown protobuf fields are skipped
by the decoder and surfaced here as a single diagnostic.

TripUpdate feeds with ``incrementality != FULL_DATASET`` are rejected whole at
this boundary (S8/B1 safe-degradation requirement; S12 re-checks as defense
in depth).

Public surface:

- :func:`parse_vehicles` — raw vehicles JSON dict → :class:`VehiclesSnapshot`
- :func:`parse_alerts` — raw alerts JSON dict → :class:`AlertsSnapshot`
- :func:`parse_trip_updates` — raw protobuf bytes → :class:`TripUpdateFeed`
"""

from __future__ import annotations

from typing import Any

from backend.trimet import rtproto
from backend.trimet.feed_errors import (
    FEED_ALERTS,
    FEED_TRIPUPDATES,
    FEED_VEHICLES,
    FeedRejectedError,
    ProtobufDecodeError,
    RecordRejectedError,
)
from backend.trimet.feed_types import (
    INCREMENTALITY_FULL_DATASET,
    MAX_PLAUSIBLE_UTC_MS,
    MIN_PLAUSIBLE_UTC_MS,
    AlertsSnapshot,
    AlertLocation,
    AlertRouteChild,
    FeedDiagnostic,
    FeedParseResult,
    ServiceAlert,
    StopTimeEvent,
    StopTimeUpdate,
    TripDescriptor,
    TripUpdate,
    TripUpdateEntity,
    TripUpdateFeed,
    TripUpdateFeedHeader,
    VehiclePosition,
    VehiclesRequestProvenance,
    VehiclesSnapshot,
    _is_int,
    _is_number,
    _is_plausible_utc_ms,
)

MAX_DIAGNOSTICS = 200

# Seconds-domain plausibility window (derived from the millisecond window by
# integer division; the TripUpdate feed delivers header/event timestamps in
# seconds, S8/B8).
_MIN_PLAUSIBLE_UTC_S = MIN_PLAUSIBLE_UTC_MS // 1000
_MAX_PLAUSIBLE_UTC_S = MAX_PLAUSIBLE_UTC_MS // 1000


def _preview(value: object, limit: int = 60) -> str:
    """Bounded, credential-safe preview of a scalar offending value.

    Numbers and booleans are shown; strings are described by length only, so
    no URL or payload text can leak into diagnostics (plan §6).
    """
    if isinstance(value, bool) or isinstance(value, int) or isinstance(value, float):
        text = repr(value)
    else:
        text = f"<{type(value).__name__} of length {len(value) if hasattr(value, '__len__') else 'n/a'}>"
    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return text


def _cap_diagnostics(
    feed: str, diagnostics: list[FeedDiagnostic]
) -> tuple[FeedDiagnostic, ...]:
    """Cap diagnostics at ``MAX_DIAGNOSTICS`` with one synthetic overflow entry."""
    if len(diagnostics) <= MAX_DIAGNOSTICS:
        return tuple(diagnostics)
    suppressed = len(diagnostics) - MAX_DIAGNOSTICS
    overflow = FeedDiagnostic(
        feed=feed,
        record_key=None,
        field="diagnostics",
        reason="diagnostics_overflow",
        detail=f"{suppressed} diagnostics suppressed",
    )
    return tuple(diagnostics[:MAX_DIAGNOSTICS]) + (overflow,)


def _optional_str(
    entry: dict[str, Any],
    key: str,
    record_key: str | None,
    feed: str,
    diagnostics: list[FeedDiagnostic],
) -> str | None:
    """Optional string field: absent/null → None; wrong type → None + diagnostic."""
    value = entry.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        diagnostics.append(
            FeedDiagnostic(
                feed=feed,
                record_key=record_key,
                field=key,
                reason="invalid_optional_field",
                detail=f"expected string, got {_preview(value)}",
            )
        )
        return None
    return value


def _optional_int(
    entry: dict[str, Any],
    key: str,
    record_key: str | None,
    feed: str,
    diagnostics: list[FeedDiagnostic],
) -> int | None:
    """Optional integer field: absent/null → None; wrong type → None + diagnostic."""
    value = entry.get(key)
    if value is None:
        return None
    if not _is_int(value):
        diagnostics.append(
            FeedDiagnostic(
                feed=feed,
                record_key=record_key,
                field=key,
                reason="invalid_optional_field",
                detail=f"expected integer, got {_preview(value)}",
            )
        )
        return None
    return value


def _optional_bool(
    entry: dict[str, Any],
    key: str,
    record_key: str | None,
    feed: str,
    diagnostics: list[FeedDiagnostic],
) -> bool | None:
    value = entry.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        diagnostics.append(
            FeedDiagnostic(
                feed=feed,
                record_key=record_key,
                field=key,
                reason="invalid_optional_field",
                detail=f"expected boolean, got {_preview(value)}",
            )
        )
        return None
    return value


def _optional_number(
    entry: dict[str, Any],
    key: str,
    record_key: str | None,
    feed: str,
    diagnostics: list[FeedDiagnostic],
) -> int | float | None:
    value = entry.get(key)
    if value is None:
        return None
    if not _is_number(value):
        diagnostics.append(
            FeedDiagnostic(
                feed=feed,
                record_key=record_key,
                field=key,
                reason="invalid_optional_field",
                detail=f"expected number, got {_preview(value)}",
            )
        )
        return None
    return value


def _optional_str_or_int(
    entry: dict[str, Any],
    key: str,
    record_key: str | None,
    feed: str,
    diagnostics: list[FeedDiagnostic],
) -> str | int | None:
    """Optional field whose type TriMet evidence does not pin down (messageCode).

    A raw ``str`` or ``int`` is preserved verbatim; anything else (including
    ``bool``) is dropped with a diagnostic.
    """
    value = entry.get(key)
    if value is None:
        return None
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        return value
    diagnostics.append(
        FeedDiagnostic(
            feed=feed,
            record_key=record_key,
            field=key,
            reason="invalid_optional_field",
            detail=f"expected string or integer, got {_preview(value)}",
        )
    )
    return None


# --- Vehicles (plan §6.1) -----------------------------------------------------


def parse_vehicles(
    raw: dict[str, Any], provenance: VehiclesRequestProvenance
) -> FeedParseResult[VehiclesSnapshot]:
    """Normalize one vehicles response; record defects never cascade.

    Feed-level defects (missing/invalid ``resultSet`` or ``queryTime``, or a
    non-list ``vehicle`` array) raise :class:`FeedRejectedError` and produce
    no partial snapshot.  The caller supplies the request provenance; the
    parser never infers authority (plan §5).
    """
    feed = FEED_VEHICLES
    diagnostics: list[FeedDiagnostic] = []
    if not isinstance(raw, dict):
        raise FeedRejectedError(
            feed, "invalid_payload", "response is not a JSON object"
        )
    result_set = raw.get("resultSet")
    if not isinstance(result_set, dict):
        raise FeedRejectedError(
            feed,
            "missing_result_set",
            "response carries no resultSet object",
        )
    query_time = result_set.get("queryTime")
    if not _is_plausible_utc_ms(query_time):
        raise FeedRejectedError(
            feed,
            "invalid_query_time",
            f"resultSet.queryTime must be a plausible UTC ms epoch, got "
            f"{_preview(query_time)}",
        )
    vehicles_raw = result_set.get("vehicle")
    if vehicles_raw is None:
        vehicles_raw = []  # a valid empty snapshot (no vehicles in service)
    if not isinstance(vehicles_raw, list):
        raise FeedRejectedError(
            feed,
            "invalid_vehicle_array",
            f"resultSet.vehicle must be a list, got {_preview(vehicles_raw)}",
        )
    vehicles: list[VehiclePosition] = []
    for index, entry in enumerate(vehicles_raw):
        try:
            vehicles.append(_parse_vehicle(entry, index, diagnostics))
        except RecordRejectedError as exc:
            diagnostics.append(
                FeedDiagnostic(
                    feed=feed,
                    record_key=exc.record_key,
                    field=exc.field,
                    reason=exc.reason,
                    detail=exc.detail,
                )
            )
    snapshot = VehiclesSnapshot(
        provenance=provenance,
        query_time_utc_ms=query_time,
        vehicles=tuple(vehicles),
    )
    return FeedParseResult(model=snapshot, diagnostics=_cap_diagnostics(feed, diagnostics))


def _parse_vehicle(
    entry: object, index: int, diagnostics: list[FeedDiagnostic]
) -> VehiclePosition:
    """One vehicle record; raises :class:`RecordRejectedError` on policy rows."""
    if not isinstance(entry, dict):
        raise RecordRejectedError(
            FEED_VEHICLES,
            f"vehicle[{index}]",
            "vehicle",
            "invalid_record",
            "vehicle entry is not a JSON object",
        )
    vehicle_id = entry.get("vehicleID")
    record_key = str(vehicle_id) if _is_int(vehicle_id) else f"vehicle[{index}]"

    def _reject(field: str, reason: str, value: object = None, *, has_value: bool = False) -> None:
        detail = f"got {_preview(value)}" if has_value else None
        raise RecordRejectedError(
            FEED_VEHICLES, record_key, field, reason, detail
        )

    if vehicle_id is None:
        _reject("vehicleID", "missing_required_field")
    if not _is_int(vehicle_id) or vehicle_id <= 0:
        _reject("vehicleID", "invalid_field", vehicle_id, has_value=True)

    vehicle_type = entry.get("type")
    if vehicle_type is None:
        _reject("type", "missing_required_field")
    if not isinstance(vehicle_type, str) or not vehicle_type:
        _reject("type", "invalid_field", vehicle_type, has_value=True)

    route_number = entry.get("routeNumber")
    if route_number is None:
        _reject("routeNumber", "missing_required_field")
    if not _is_int(route_number):
        _reject("routeNumber", "invalid_field", route_number, has_value=True)

    time_raw = entry.get("time")
    if time_raw is None:
        _reject("time", "missing_required_field")
    if not _is_plausible_utc_ms(time_raw):
        _reject("time", "invalid_timestamp", time_raw, has_value=True)

    expires_raw = entry.get("expires")
    if expires_raw is None:
        _reject("expires", "missing_required_field")
    if not _is_plausible_utc_ms(expires_raw):
        _reject("expires", "invalid_timestamp", expires_raw, has_value=True)

    latitude = entry.get("latitude")
    longitude = entry.get("longitude")
    if latitude is None:
        _reject("latitude", "missing_required_field")
    if longitude is None:
        _reject("longitude", "missing_required_field")
    if not _is_number(latitude) or not -90.0 <= latitude <= 90.0:
        _reject("latitude", "coordinates_out_of_range", latitude, has_value=True)
    if not _is_number(longitude) or not -180.0 <= longitude <= 180.0:
        _reject("longitude", "coordinates_out_of_range", longitude, has_value=True)

    feed = FEED_VEHICLES
    return VehiclePosition(
        vehicle_id=vehicle_id,
        vehicle_type=vehicle_type,
        route_number=route_number,
        latitude=latitude,
        longitude=longitude,
        time_utc_ms=time_raw,  # raw feed ms preserved verbatim (plan §6.1)
        expires_utc_ms=expires_raw,  # raw feed ms preserved verbatim
        route_color=_optional_str(entry, "routeColor", record_key, feed, diagnostics),
        route_subtype=_optional_str(entry, "routeSubType", record_key, feed, diagnostics),
        direction=_optional_int(entry, "direction", record_key, feed, diagnostics),
        trip_id=_optional_str(entry, "tripID", record_key, feed, diagnostics),
        new_trip=_optional_bool(entry, "newTrip", record_key, feed, diagnostics),
        off_route=_optional_bool(entry, "offRoute", record_key, feed, diagnostics),
        delay=_optional_int(entry, "delay", record_key, feed, diagnostics),
        bearing=_optional_int(entry, "bearing", record_key, feed, diagnostics),
        next_loc_id=_optional_int(entry, "nextLocID", record_key, feed, diagnostics),
        next_stop_seq=_optional_int(entry, "nextStopSeq", record_key, feed, diagnostics),
        last_loc_id=_optional_int(entry, "lastLocID", record_key, feed, diagnostics),
        last_stop_seq=_optional_int(entry, "lastStopSeq", record_key, feed, diagnostics),
        sign_message=_optional_str(entry, "signMessage", record_key, feed, diagnostics),
        sign_message_long=_optional_str(entry, "signMessageLong", record_key, feed, diagnostics),
        garage=_optional_str(entry, "garage", record_key, feed, diagnostics),
        block_id=_optional_int(entry, "blockID", record_key, feed, diagnostics),
        source=_optional_str(entry, "source", record_key, feed, diagnostics),
        in_congestion=_optional_bool(entry, "inCongestion", record_key, feed, diagnostics),
        load_percentage=_optional_number(entry, "loadPercentage", record_key, feed, diagnostics),
        location_in_schedule_day=_optional_int(
            entry, "locationInScheduleDay", record_key, feed, diagnostics
        ),
        message_code=_optional_str_or_int(entry, "messageCode", record_key, feed, diagnostics),
        extra_block_id=_optional_int(entry, "extraBlockID", record_key, feed, diagnostics),
        service_date=_optional_int(entry, "serviceDate", record_key, feed, diagnostics),
    )


# --- Alerts (plan §6.2) --------------------------------------------------------


def parse_alerts(raw: dict[str, Any]) -> FeedParseResult[AlertsSnapshot]:
    """Normalize one alerts response; record defects never cascade.

    Feed-level defects (missing/invalid ``resultSet`` or ``queryTime``) raise
    :class:`FeedRejectedError`.  Null ``end`` is a verified observation
    (S8/A5), not an error.  A route child lacking a usable id rejects the
    whole alert — conservative, because dropping only the child could
    silently hide MAX relevance (plan §6.2).
    """
    feed = FEED_ALERTS
    diagnostics: list[FeedDiagnostic] = []
    if not isinstance(raw, dict):
        raise FeedRejectedError(
            feed, "invalid_payload", "response is not a JSON object"
        )
    result_set = raw.get("resultSet")
    if not isinstance(result_set, dict):
        raise FeedRejectedError(
            feed,
            "missing_result_set",
            "response carries no resultSet object",
        )
    query_time = result_set.get("queryTime")
    if not _is_plausible_utc_ms(query_time):
        raise FeedRejectedError(
            feed,
            "invalid_query_time",
            f"resultSet.queryTime must be a plausible UTC ms epoch, got "
            f"{_preview(query_time)}",
        )
    alerts_raw = result_set.get("alert")
    if alerts_raw is None:
        alerts_raw = []  # a valid empty snapshot (no active alerts)
    if not isinstance(alerts_raw, list):
        raise FeedRejectedError(
            feed,
            "invalid_alert_array",
            f"resultSet.alert must be a list, got {_preview(alerts_raw)}",
        )
    alerts: list[ServiceAlert] = []
    for index, entry in enumerate(alerts_raw):
        try:
            alerts.append(_parse_alert(entry, index, diagnostics))
        except RecordRejectedError as exc:
            diagnostics.append(
                FeedDiagnostic(
                    feed=feed,
                    record_key=exc.record_key,
                    field=exc.field,
                    reason=exc.reason,
                    detail=exc.detail,
                )
            )
    snapshot = AlertsSnapshot(
        query_time_utc_ms=query_time,
        alerts=tuple(alerts),
    )
    return FeedParseResult(model=snapshot, diagnostics=_cap_diagnostics(feed, diagnostics))


def _parse_alert(
    entry: object, index: int, diagnostics: list[FeedDiagnostic]
) -> ServiceAlert:
    """One alert record; raises :class:`RecordRejectedError` on policy rows."""
    if not isinstance(entry, dict):
        raise RecordRejectedError(
            FEED_ALERTS,
            f"alert[{index}]",
            "alert",
            "invalid_record",
            "alert entry is not a JSON object",
        )
    alert_id = entry.get("id")
    record_key = str(alert_id) if _is_int(alert_id) else f"alert[{index}]"
    if alert_id is None:
        raise RecordRejectedError(
            FEED_ALERTS, record_key, "id", "missing_required_field"
        )
    if not _is_int(alert_id) or alert_id <= 0:
        raise RecordRejectedError(
            FEED_ALERTS,
            record_key,
            "id",
            "invalid_field",
            f"got {_preview(alert_id)}",
        )

    routes_raw = entry.get("routes")
    routes: list[AlertRouteChild] = []
    if routes_raw is not None:
        if not isinstance(routes_raw, list):
            raise RecordRejectedError(
                FEED_ALERTS,
                record_key,
                "routes",
                "invalid_field",
                f"expected a list, got {_preview(routes_raw)}",
            )
        for child in routes_raw:
            if not isinstance(child, dict):
                # Conservative: reject the whole alert (plan §6.2 rationale).
                raise RecordRejectedError(
                    FEED_ALERTS,
                    record_key,
                    "routes",
                    "invalid_route_child",
                    "route child is not a JSON object",
                )
            child_id = child.get("id")
            if not _is_int(child_id) or child_id <= 0:
                # A route child without a usable id could silently hide MAX
                # relevance if only the child were dropped — reject the alert.
                raise RecordRejectedError(
                    FEED_ALERTS,
                    record_key,
                    "routes[].id",
                    "missing_required_field"
                    if child_id is None
                    else "invalid_field",
                    None if child_id is None else f"got {_preview(child_id)}",
                )
            routes.append(
                AlertRouteChild(
                    route_id=child_id,
                    route_type=_optional_str(
                        child, "type", record_key, FEED_ALERTS, diagnostics
                    ),
                    no_service_flag=_optional_bool(
                        child, "no_service_flag", record_key, FEED_ALERTS, diagnostics
                    ),
                )
            )

    locations_raw = entry.get("locations")
    locations: list[AlertLocation] = []
    if locations_raw is not None:
        if not isinstance(locations_raw, list):
            raise RecordRejectedError(
                FEED_ALERTS,
                record_key,
                "locations",
                "invalid_field",
                f"expected a list, got {_preview(locations_raw)}",
            )
        for child in locations_raw:
            if not isinstance(child, dict):
                # Location children never drive MAX classification, so a
                # malformed entry is dropped with a diagnostic while the alert
                # is retained (recorded interpretation of plan §6.2).
                diagnostics.append(
                    FeedDiagnostic(
                        feed=FEED_ALERTS,
                        record_key=record_key,
                        field="locations",
                        reason="invalid_location_child",
                        detail="location child is not a JSON object; dropped",
                    )
                )
                continue
            child_id = child.get("id")
            if not _is_int(child_id) or child_id <= 0:
                diagnostics.append(
                    FeedDiagnostic(
                        feed=FEED_ALERTS,
                        record_key=record_key,
                        field="locations[].id",
                        reason="invalid_location_child",
                        detail="location child without a usable id; dropped",
                    )
                )
                continue
            locations.append(
                AlertLocation(
                    location_id=child_id,
                    description=_optional_str(
                        child, "desc", record_key, FEED_ALERTS, diagnostics
                    ),
                    no_service_flag=_optional_bool(
                        child, "no_service_flag", record_key, FEED_ALERTS, diagnostics
                    ),
                )
            )

    begin_raw = entry.get("begin")
    begin = begin_raw if _is_plausible_utc_ms(begin_raw) else None
    if begin_raw is not None and begin is None:
        diagnostics.append(
            FeedDiagnostic(
                feed=FEED_ALERTS,
                record_key=record_key,
                field="begin",
                reason="invalid_timestamp",
                detail=f"got {_preview(begin_raw)}; preserved as None",
            )
        )
    end_raw = entry.get("end")
    end = end_raw if _is_plausible_utc_ms(end_raw) else None
    if end_raw is not None and end is None:
        # Null end is a verified observation (S8/A5) and produces no
        # diagnostic; only a present-but-invalid value does.
        diagnostics.append(
            FeedDiagnostic(
                feed=FEED_ALERTS,
                record_key=record_key,
                field="end",
                reason="invalid_timestamp",
                detail=f"got {_preview(end_raw)}; preserved as None",
            )
        )

    system_wide = entry.get("system_wide_flag")
    if system_wide is not None and not isinstance(system_wide, bool):
        diagnostics.append(
            FeedDiagnostic(
                feed=FEED_ALERTS,
                record_key=record_key,
                field="system_wide_flag",
                reason="invalid_optional_field",
                detail=f"expected boolean, got {_preview(system_wide)}; "
                "preserved as None",
            )
        )
        system_wide = None

    return ServiceAlert(
        alert_id=alert_id,
        begin_utc_ms=begin,
        end_utc_ms=end,
        system_wide_flag=system_wide,
        header_text=_optional_str(entry, "header_text", record_key, FEED_ALERTS, diagnostics),
        description=_optional_str(entry, "desc", record_key, FEED_ALERTS, diagnostics),
        info_link_url=_optional_str(entry, "info_link_url", record_key, FEED_ALERTS, diagnostics),
        routes=tuple(routes),
        locations=tuple(locations),
    )


# --- TripUpdates (plan §6.3) ---------------------------------------------------


def parse_trip_updates(payload: bytes) -> FeedParseResult[TripUpdateFeed]:
    """Normalize one TripUpdate feed; feed-level defects reject the whole feed.

    Any wire-format structural defect raises
    :class:`~backend.trimet.feed_errors.ProtobufDecodeError` (a
    :class:`FeedRejectedError` subclass) — the decoder never returns partial
    structures, so no partially decoded feed can reach normalization.  A
    missing/invalid header or a non-``FULL_DATASET`` incrementality also
    rejects the whole feed (S8/B1; S12 re-checks as defense in depth).
    """
    feed = FEED_TRIPUPDATES
    diagnostics: list[FeedDiagnostic] = []
    try:
        decoded = rtproto.decode_trip_update_feed(payload)
    except ProtobufDecodeError:
        # Feed-level by definition; re-raised unchanged, no partial model.
        raise

    header_raw = decoded.header
    assert header_raw is not None  # decoder guarantees exactly one header

    timestamp_raw = header_raw.timestamp
    if not _is_int(timestamp_raw) or not (
        _MIN_PLAUSIBLE_UTC_S <= timestamp_raw <= _MAX_PLAUSIBLE_UTC_S
    ):
        raise FeedRejectedError(
            feed,
            "invalid_header_timestamp",
            f"header.timestamp must be a plausible UTC seconds epoch, got "
            f"{_preview(timestamp_raw)}",
        )
    incrementality = header_raw.incrementality_raw
    if incrementality is None:
        raise FeedRejectedError(
            feed,
            "missing_incrementality",
            "header.incrementality is required; only FULL_DATASET feeds are "
            "accepted at this boundary",
        )
    if incrementality != INCREMENTALITY_FULL_DATASET:
        raise FeedRejectedError(
            feed,
            "non_full_dataset",
            f"header.incrementality_raw={incrementality}; only FULL_DATASET "
            "feeds are accepted at this boundary (S12 re-checks)",
        )
    if header_raw.gtfs_realtime_version is None:
        diagnostics.append(
            FeedDiagnostic(
                feed=feed,
                record_key=None,
                field="gtfs_realtime_version",
                reason="missing_optional_field",
                detail="metadata; does not affect processing correctness",
            )
        )
    header = TripUpdateFeedHeader(
        gtfs_realtime_version=header_raw.gtfs_realtime_version,
        incrementality_raw=incrementality,
        timestamp_raw_seconds=timestamp_raw,
        timestamp_utc_ms=timestamp_raw * 1000,  # integer-only conversion
    )

    if decoded.unknown_field_count > 0:
        diagnostics.append(
            FeedDiagnostic(
                feed=feed,
                record_key=None,
                field="unknown_fields",
                reason="unknown_fields_skipped",
                detail=f"{decoded.unknown_field_count} unknown protobuf "
                "fields skipped (forward compatibility)",
            )
        )

    entities: list[TripUpdateEntity] = []
    for index, entity_raw in enumerate(decoded.entities):
        try:
            entities.append(_parse_entity(entity_raw, index, diagnostics))
        except RecordRejectedError as exc:
            diagnostics.append(
                FeedDiagnostic(
                    feed=feed,
                    record_key=exc.record_key,
                    field=exc.field,
                    reason=exc.reason,
                    detail=exc.detail,
                )
            )

    trip_feed = TripUpdateFeed(header=header, entities=tuple(entities))
    return FeedParseResult(model=trip_feed, diagnostics=_cap_diagnostics(feed, diagnostics))


def _seconds_to_utc_ms(seconds: int) -> int | None:
    """Integer-only seconds→milliseconds conversion within the plausibility window."""
    if not _is_int(seconds) or not (
        _MIN_PLAUSIBLE_UTC_S <= seconds <= _MAX_PLAUSIBLE_UTC_S
    ):
        return None
    return seconds * 1000


def _parse_entity(
    entity_raw: rtproto.RawFeedEntity,
    index: int,
    diagnostics: list[FeedDiagnostic],
) -> TripUpdateEntity:
    """One FeedEntity; raises :class:`RecordRejectedError` on policy rows."""
    record_key = (
        entity_raw.id
        if isinstance(entity_raw.id, str) and entity_raw.id
        else f"entity[{index}]"
    )
    if entity_raw.id is None or not entity_raw.id:
        raise RecordRejectedError(
            FEED_TRIPUPDATES, record_key, "id", "missing_required_field"
        )
    if entity_raw.has_alert or entity_raw.has_vehicle_position:
        # Anomalous content for a TripUpdate feed; recorded, entity retained
        # (recorded interpretation of plan §6.3 — the trip_id requirement
        # below still gates the record).
        diagnostics.append(
            FeedDiagnostic(
                feed=FEED_TRIPUPDATES,
                record_key=record_key,
                field="entity",
                reason="unexpected_entity_content",
                detail="entity carries vehicle-position or alert content",
            )
        )
    if entity_raw.trip_update is None:
        raise RecordRejectedError(
            FEED_TRIPUPDATES, record_key, "trip_update", "missing_required_field"
        )
    trip_raw = entity_raw.trip_update.trip
    if trip_raw is None or not trip_raw.trip_id:
        raise RecordRejectedError(
            FEED_TRIPUPDATES, record_key, "trip.trip_id", "missing_required_field"
        )

    usable_updates: list[StopTimeUpdate] = []
    for stu_raw in entity_raw.trip_update.stop_time_updates:
        if stu_raw.arrival is None and stu_raw.departure is None:
            # Sub-record row: drop the entry, entity retained if ≥ 1 usable.
            diagnostics.append(
                FeedDiagnostic(
                    feed=FEED_TRIPUPDATES,
                    record_key=record_key,
                    field="stop_time_update",
                    reason="no_arrival_or_departure",
                    detail=(
                        f"stop_sequence={stu_raw.stop_sequence}"
                        if stu_raw.stop_sequence is not None
                        else None
                    ),
                )
            )
            continue
        usable_updates.append(_parse_stop_time_update(stu_raw, record_key, diagnostics))
    if not usable_updates:
        raise RecordRejectedError(
            FEED_TRIPUPDATES,
            record_key,
            "stop_time_update",
            "no_usable_stop_time_updates",
        )

    trip_update_raw = entity_raw.trip_update
    timestamp_raw = trip_update_raw.timestamp
    timestamp_ms = _seconds_to_utc_ms(timestamp_raw) if timestamp_raw is not None else None
    if timestamp_raw is not None and timestamp_ms is None:
        diagnostics.append(
            FeedDiagnostic(
                feed=FEED_TRIPUPDATES,
                record_key=record_key,
                field="timestamp",
                reason="invalid_timestamp",
                detail=f"got {_preview(timestamp_raw)}; preserved as None",
            )
        )

    vehicle_id: str | None = None
    if trip_update_raw.vehicle is not None:
        vehicle_id = trip_update_raw.vehicle.id

    trip_update = TripUpdate(
        trip=TripDescriptor(
            trip_id=trip_raw.trip_id,
            route_id=trip_raw.route_id,  # raw; no classification derived (plan §9)
            direction_id=trip_raw.direction_id,
            start_time=trip_raw.start_time,
            start_date=trip_raw.start_date,
            schedule_relationship_raw=trip_raw.schedule_relationship_raw,
        ),
        stop_time_updates=tuple(usable_updates),
        vehicle_id=vehicle_id,  # None when absent — coverage never assumed
        delay_seconds=trip_update_raw.delay,  # negative values valid (S8/B7)
        timestamp_raw_seconds=timestamp_raw if timestamp_ms is not None else None,
        timestamp_utc_ms=timestamp_ms,
    )
    return TripUpdateEntity(
        entity_id=entity_raw.id,
        trip_update=trip_update,
        is_deleted=entity_raw.is_deleted,
    )


def _parse_stop_time_update(
    stu_raw: rtproto.RawStopTimeUpdate,
    record_key: str,
    diagnostics: list[FeedDiagnostic],
) -> StopTimeUpdate:
    """One usable stop-time entry (arrival or departure present)."""

    def _event(raw: rtproto.RawStopTimeEvent | None) -> StopTimeEvent | None:
        if raw is None:
            return None
        time_ms = _seconds_to_utc_ms(raw.time) if raw.time is not None else None
        if raw.time is not None and time_ms is None:
            diagnostics.append(
                FeedDiagnostic(
                    feed=FEED_TRIPUPDATES,
                    record_key=record_key,
                    field="stop_time_update.time",
                    reason="invalid_timestamp",
                    detail=f"got {_preview(raw.time)}; preserved as None",
                )
            )
        return StopTimeEvent(
            delay_seconds=raw.delay,
            time_raw_seconds=raw.time if time_ms is not None else None,
            time_utc_ms=time_ms,
            uncertainty_seconds=raw.uncertainty,
        )

    return StopTimeUpdate(
        stop_sequence=stu_raw.stop_sequence,
        stop_id=stu_raw.stop_id,
        arrival=_event(stu_raw.arrival),
        departure=_event(stu_raw.departure),
        schedule_relationship_raw=stu_raw.schedule_relationship_raw,
    )
