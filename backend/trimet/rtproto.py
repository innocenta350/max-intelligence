"""Self-contained stdlib protobuf wire decoder for GTFS-Realtime TripUpdates.

Stage S9, M2 plan REV 2 (§7).  This is a port of the decoder logic verified in
Stage S8 against the official ``gtfs-realtime-bindings`` library with a
12,868/12,868 per-entity agreement cross-check (S8/B7,
``tools/s8_contract/evidence/b7_library_eval.json``).  The ported functions are
``parse_wire``/``_varint``/``submessages``/``string_field``/``int_field``/
``ste``/``decode_trip_descriptor``/``decode_tripupdate_full``/``decode_rt_feed``
from ``tools/s8_contract/probe_s8.py`` — decoder logic only.  This module never
imports the probe and carries none of its governor, HTTP, or live-probe code.

Hardening beyond the probe (plan §7: the decoder must fail safely): the probe
would raise bare ``IndexError`` on truncation and silently produce short
slices on length overruns; here every structural defect raises
:class:`~backend.trimet.feed_errors.ProtobufDecodeError` and no decoding
function ever returns a partially decoded structure:

- truncated messages (mid-field, mid-varint, mid-length-prefix),
- varints longer than 10 bytes or values exceeding 64 bits,
- unsupported wire types (3/4/6/7 — start/end group and the reserved types),
- length-delimited or fixed-width fields overrunning the remaining bytes,
- field number 0 (malformed tag),
- a FeedMessage without exactly one header field (proto2 required).

Unknown fields (valid field numbers this decoder does not consume, or known
numbers with an unexpected wire type) are skipped and counted for forward
compatibility; the count is surfaced by the parse layer as a single
diagnostic (plan §6).  Sign extension follows the verified probe semantics:
varints of ``2**63`` and above are interpreted as two's-complement negatives
(``value -= 2**64``), which is how protobuf encodes negative ``int32``/
``int64`` values such as TripUpdate delays (S8/B7: negative delays valid).

Field numbers follow the official ``gtfs-realtime.proto`` (v2.0), as verified
in S8:

- ``FeedMessage``: header=1, entity=2 (repeated)
- ``FeedHeader``: gtfs_realtime_version=1, incrementality=2, timestamp=3
- ``FeedEntity``: id=1, is_deleted=2, trip_update=3, vehicle=4, alert=5
- ``TripUpdate``: trip=1, stop_time_update=2 (repeated), vehicle=3,
  timestamp=4, delay=5
- ``TripDescriptor``: trip_id=1, start_time=2, start_date=3,
  schedule_relationship=4, route_id=5, direction_id=6
- ``VehicleDescriptor``: id=1, label=2, license_plate=3
- ``StopTimeUpdate``: stop_sequence=1, arrival=2, departure=3, stop_id=4,
  schedule_relationship=5
- ``StopTimeEvent``: delay=1, time=2, uncertainty=3

Units are preserved exactly as the feed delivers them: header and event
timestamps are **seconds** since epoch (S8/B8) and delays are seconds; the
millisecond normalization happens in :mod:`backend.trimet.feed_parse`.
"""

from __future__ import annotations

from dataclasses import dataclass

from backend.trimet.feed_errors import ProtobufDecodeError

_WIRE_VARINT = 0
_WIRE_FIXED64 = 1
_WIRE_LEN = 2
_WIRE_FIXED32 = 5

# Wire types 3/4 (start/end group) and 6/7 (reserved) are rejected outright.
_MAX_VARINT_BYTES = 10  # a 64-bit value needs at most 10 bytes
_UINT64_LIMIT = 1 << 64
_SIGN_EXTEND_THRESHOLD = 1 << 63


@dataclass(frozen=True, slots=True)
class WireField:
    """One decoded wire-format field.

    ``value`` is an ``int`` for varint fields and raw ``bytes`` for
    length-delimited and fixed-width fields (no interpretation here).
    """

    field_number: int
    wire_type: int
    value: int | bytes


# --- Raw decoded structures (feed units preserved; no normalization) ---------


@dataclass(frozen=True, slots=True)
class RawStopTimeEvent:
    delay: int | None = None
    time: int | None = None
    uncertainty: int | None = None


@dataclass(frozen=True, slots=True)
class RawStopTimeUpdate:
    stop_sequence: int | None = None
    stop_id: str | None = None
    schedule_relationship_raw: int | None = None
    arrival: RawStopTimeEvent | None = None
    departure: RawStopTimeEvent | None = None


@dataclass(frozen=True, slots=True)
class RawTripDescriptor:
    trip_id: str | None = None
    start_time: str | None = None
    start_date: str | None = None
    schedule_relationship_raw: int | None = None
    route_id: str | None = None
    direction_id: int | None = None


@dataclass(frozen=True, slots=True)
class RawVehicleDescriptor:
    id: str | None = None
    label: str | None = None


@dataclass(frozen=True, slots=True)
class RawTripUpdate:
    trip: RawTripDescriptor | None = None
    vehicle: RawVehicleDescriptor | None = None
    timestamp: int | None = None
    delay: int | None = None
    stop_time_updates: tuple[RawStopTimeUpdate, ...] = ()


@dataclass(frozen=True, slots=True)
class RawFeedEntity:
    id: str | None = None
    is_deleted: bool = False
    trip_update: RawTripUpdate | None = None
    has_vehicle_position: bool = False  # layout known nonstandard (M0 A2); unused
    has_alert: bool = False


@dataclass(frozen=True, slots=True)
class RawFeedHeader:
    gtfs_realtime_version: str | None = None
    incrementality_raw: int | None = None
    timestamp: int | None = None


@dataclass(frozen=True, slots=True)
class RawTripUpdateFeed:
    header: RawFeedHeader
    entities: tuple[RawFeedEntity, ...]
    unknown_field_count: int


# --- Wire primitives ----------------------------------------------------------


def read_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Read one base-128 varint starting at ``pos``; return ``(value, new_pos)``.

    Raises :class:`ProtobufDecodeError` on truncation, on varints longer than
    10 bytes, and on values exceeding 64 bits.
    """
    n = len(data)
    start = pos
    result = 0
    shift = 0
    while True:
        if pos >= n:
            raise ProtobufDecodeError(
                "protobuf_truncated",
                f"varint starting at byte {start} runs past the end of the "
                f"message (length {n})",
            )
        if shift >= 7 * _MAX_VARINT_BYTES:
            raise ProtobufDecodeError(
                "protobuf_varint_overflow",
                f"varint starting at byte {start} exceeds "
                f"{_MAX_VARINT_BYTES} bytes",
            )
        byte = data[pos]
        result |= (byte & 0x7F) << shift
        pos += 1
        if not byte & 0x80:
            break
        shift += 7
    if result >= _UINT64_LIMIT:
        raise ProtobufDecodeError(
            "protobuf_varint_overflow",
            f"varint starting at byte {start} encodes a value exceeding "
            "64 bits",
        )
    return result, pos


def _sign_extend(value: int) -> int:
    """Two's-complement interpretation for values with the sign bit set.

    Verified probe semantics (S8/B7): protobuf encodes negative ``int32``/
    ``int64`` values as 10-byte varints; subtracting ``2**64`` recovers the
    negative value.  Unsigned fields never reach such magnitudes in real
    feeds, and if one ever did the parse layer's plausibility checks would
    reject the feed — failing safely either way.
    """
    if value >= _SIGN_EXTEND_THRESHOLD:
        value -= _UINT64_LIMIT
    return value


def parse_wire(data: bytes) -> list[WireField]:
    """Decode a protobuf message into its top-level fields.

    Raises :class:`ProtobufDecodeError` on any structural defect; never
    returns partially decoded data.
    """
    items: list[WireField] = []
    i = 0
    n = len(data)
    while i < n:
        tag, i = read_varint(data, i)
        field_number = tag >> 3
        wire_type = tag & 7
        if field_number == 0:
            raise ProtobufDecodeError(
                "protobuf_invalid_field_number",
                f"field number 0 is invalid (tag byte at {i - 1})",
            )
        if wire_type == _WIRE_VARINT:
            value, i = read_varint(data, i)
        elif wire_type == _WIRE_LEN:
            length, i = read_varint(data, i)
            if i + length > n:
                raise ProtobufDecodeError(
                    "protobuf_length_overrun",
                    f"length-delimited field {field_number} claims {length} "
                    f"bytes at byte {i} but only {n - i} remain",
                )
            value = data[i : i + length]
            i += length
        elif wire_type == _WIRE_FIXED32:
            if i + 4 > n:
                raise ProtobufDecodeError(
                    "protobuf_truncated",
                    f"fixed32 field {field_number} at byte {i} needs 4 bytes "
                    f"but only {n - i} remain",
                )
            value = data[i : i + 4]
            i += 4
        elif wire_type == _WIRE_FIXED64:
            if i + 8 > n:
                raise ProtobufDecodeError(
                    "protobuf_truncated",
                    f"fixed64 field {field_number} at byte {i} needs 8 bytes "
                    f"but only {n - i} remain",
                )
            value = data[i : i + 8]
            i += 8
        else:
            raise ProtobufDecodeError(
                "protobuf_unsupported_wire_type",
                f"wire type {wire_type} for field {field_number} at byte {i} "
                "is not supported (types 3/4/6/7 are rejected)",
            )
        items.append(WireField(field_number, wire_type, value))
    return items


def _submessages(items: list[WireField], field_number: int) -> list[list[WireField]]:
    """Parse the length-delimited occurrences of ``field_number`` as messages."""
    return [
        parse_wire(item.value)
        for item in items
        if item.field_number == field_number and item.wire_type == _WIRE_LEN
    ]


def _string_field(items: list[WireField], field_number: int) -> str | None:
    """First length-delimited occurrence as UTF-8 text, or ``None``."""
    for item in items:
        if item.field_number == field_number and item.wire_type == _WIRE_LEN:
            assert isinstance(item.value, bytes)
            return item.value.decode("utf-8", errors="replace")
    return None


def _int_field(items: list[WireField], field_number: int) -> int | None:
    """First varint occurrence, sign-extended, or ``None``."""
    for item in items:
        if item.field_number == field_number and item.wire_type == _WIRE_VARINT:
            assert isinstance(item.value, int)
            return _sign_extend(item.value)
    return None


class _UnknownFieldCounter:
    """Counts fields this decoder does not consume (plan §6, forward compat).

    A field is unknown when its number is not part of the message schema or a
    known number arrives with an unexpected wire type.  Skipped, never fatal.
    """

    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    def count_message(
        self, items: list[WireField], known: dict[int, frozenset[int]]
    ) -> None:
        for item in items:
            if item.wire_type not in known.get(item.field_number, frozenset()):
                self.count += 1


# Per-message schemas: field number -> acceptable wire types.
_KNOWN_FEED_MESSAGE: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_LEN}),
    2: frozenset({_WIRE_LEN}),
}
_KNOWN_FEED_HEADER: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_LEN}),
    2: frozenset({_WIRE_VARINT}),
    3: frozenset({_WIRE_VARINT}),
}
_KNOWN_FEED_ENTITY: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_LEN}),
    2: frozenset({_WIRE_VARINT}),
    3: frozenset({_WIRE_LEN}),
    4: frozenset({_WIRE_LEN}),
    5: frozenset({_WIRE_LEN}),
}
_KNOWN_TRIP_UPDATE: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_LEN}),
    2: frozenset({_WIRE_LEN}),
    3: frozenset({_WIRE_LEN}),
    4: frozenset({_WIRE_VARINT}),
    5: frozenset({_WIRE_VARINT}),
}
_KNOWN_TRIP_DESCRIPTOR: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_LEN}),
    2: frozenset({_WIRE_LEN}),
    3: frozenset({_WIRE_LEN}),
    4: frozenset({_WIRE_VARINT}),
    5: frozenset({_WIRE_LEN}),
    6: frozenset({_WIRE_VARINT}),
}
_KNOWN_VEHICLE_DESCRIPTOR: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_LEN}),
    2: frozenset({_WIRE_LEN}),
    3: frozenset({_WIRE_LEN}),
}
_KNOWN_STOP_TIME_UPDATE: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_VARINT}),
    2: frozenset({_WIRE_LEN}),
    3: frozenset({_WIRE_LEN}),
    4: frozenset({_WIRE_LEN}),
    5: frozenset({_WIRE_VARINT}),
}
_KNOWN_STOP_TIME_EVENT: dict[int, frozenset[int]] = {
    1: frozenset({_WIRE_VARINT}),
    2: frozenset({_WIRE_VARINT}),
    3: frozenset({_WIRE_VARINT}),
}


# --- Schema-aware decoding (feed units preserved) -----------------------------


def _decode_stop_time_event(
    items: list[WireField], unknown: _UnknownFieldCounter
) -> RawStopTimeEvent:
    unknown.count_message(items, _KNOWN_STOP_TIME_EVENT)
    return RawStopTimeEvent(
        delay=_int_field(items, 1),
        time=_int_field(items, 2),
        uncertainty=_int_field(items, 3),
    )


def _decode_stop_time_update(
    items: list[WireField], unknown: _UnknownFieldCounter
) -> RawStopTimeUpdate:
    unknown.count_message(items, _KNOWN_STOP_TIME_UPDATE)
    arrival_items = _submessages(items, 2)
    departure_items = _submessages(items, 3)
    return RawStopTimeUpdate(
        stop_sequence=_int_field(items, 1),
        stop_id=_string_field(items, 4),
        schedule_relationship_raw=_int_field(items, 5),
        arrival=(
            _decode_stop_time_event(arrival_items[0], unknown)
            if arrival_items
            else None
        ),
        departure=(
            _decode_stop_time_event(departure_items[0], unknown)
            if departure_items
            else None
        ),
    )


def _decode_trip_descriptor(
    items: list[WireField], unknown: _UnknownFieldCounter
) -> RawTripDescriptor:
    unknown.count_message(items, _KNOWN_TRIP_DESCRIPTOR)
    return RawTripDescriptor(
        trip_id=_string_field(items, 1),
        start_time=_string_field(items, 2),
        start_date=_string_field(items, 3),
        schedule_relationship_raw=_int_field(items, 4),
        route_id=_string_field(items, 5),
        direction_id=_int_field(items, 6),
    )


def _decode_vehicle_descriptor(
    items: list[WireField], unknown: _UnknownFieldCounter
) -> RawVehicleDescriptor:
    unknown.count_message(items, _KNOWN_VEHICLE_DESCRIPTOR)
    return RawVehicleDescriptor(
        id=_string_field(items, 1),
        label=_string_field(items, 2),
    )


def _decode_trip_update(
    items: list[WireField], unknown: _UnknownFieldCounter
) -> RawTripUpdate:
    """TripUpdate with the complete stop_time_update list (verified S8/B2/B5)."""
    unknown.count_message(items, _KNOWN_TRIP_UPDATE)
    trip_items = _submessages(items, 1)
    stu_items = _submessages(items, 2)
    vehicle_items = _submessages(items, 3)
    return RawTripUpdate(
        trip=(
            _decode_trip_descriptor(trip_items[0], unknown) if trip_items else None
        ),
        vehicle=(
            _decode_vehicle_descriptor(vehicle_items[0], unknown)
            if vehicle_items
            else None
        ),
        timestamp=_int_field(items, 4),
        delay=_int_field(items, 5),
        stop_time_updates=tuple(
            _decode_stop_time_update(s, unknown) for s in stu_items
        ),
    )


def _decode_feed_entity(
    items: list[WireField], unknown: _UnknownFieldCounter
) -> RawFeedEntity:
    unknown.count_message(items, _KNOWN_FEED_ENTITY)
    trip_update_items = _submessages(items, 3)
    deleted_raw = _int_field(items, 2)
    return RawFeedEntity(
        id=_string_field(items, 1),
        is_deleted=bool(deleted_raw) if deleted_raw is not None else False,
        trip_update=(
            _decode_trip_update(trip_update_items[0], unknown)
            if trip_update_items
            else None
        ),
        has_vehicle_position=bool(_submessages(items, 4)),
        has_alert=bool(_submessages(items, 5)),
    )


def _decode_feed_header(
    items: list[WireField], unknown: _UnknownFieldCounter
) -> RawFeedHeader:
    unknown.count_message(items, _KNOWN_FEED_HEADER)
    return RawFeedHeader(
        gtfs_realtime_version=_string_field(items, 1),
        incrementality_raw=_int_field(items, 2),
        timestamp=_int_field(items, 3),
    )


def decode_trip_update_feed(body: bytes) -> RawTripUpdateFeed:
    """Decode a GTFS-Realtime FeedMessage into raw header + entities.

    Raises :class:`ProtobufDecodeError` on any wire-format structural defect
    and on a missing or duplicated header (proto2 required field).  The
    ``unknown_field_count`` aggregates every skipped unknown field across all
    message levels; the parse layer surfaces it as a single diagnostic.
    """
    if not isinstance(body, (bytes, bytearray)):
        raise ProtobufDecodeError(
            "protobuf_invalid_input",
            f"expected bytes, got {type(body).__name__}",
        )
    unknown = _UnknownFieldCounter()
    message = parse_wire(bytes(body))
    unknown.count_message(message, _KNOWN_FEED_MESSAGE)
    header_items = _submessages(message, 1)
    if not header_items:
        raise ProtobufDecodeError(
            "protobuf_missing_header",
            "FeedMessage carries no header field (field 1)",
        )
    if len(header_items) > 1:
        raise ProtobufDecodeError(
            "protobuf_duplicate_header",
            f"FeedMessage carries {len(header_items)} header fields; "
            "exactly one is required",
        )
    header = _decode_feed_header(header_items[0], unknown)
    entities = tuple(
        _decode_feed_entity(entity_items, unknown)
        for entity_items in _submessages(message, 2)
    )
    return RawTripUpdateFeed(
        header=header,
        entities=entities,
        unknown_field_count=unknown.count,
    )
