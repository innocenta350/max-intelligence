"""Tests for the S9 protobuf wire decoder (Stage S9, M2 plan REV 2 §7).

The full ten-entry failure-mode matrix is covered, plus round-trip decoding
of the two static capture fixtures.  All bytes are hand-crafted here by a
small local builder — no network, no probe import, no S8 campaign directory
access (fixture independence, owner final-check 3).  ``test_trimet_feed_parse``
reuses the builder by importing it from this module (``tests`` is a package).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.trimet.feed_errors import ProtobufDecodeError
from backend.trimet.rtproto import (
    RawTripUpdateFeed,
    decode_trip_update_feed,
    parse_wire,
    read_varint,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "s8"

# S8/B2 verified facts for the referenced captures (README documents provenance).
CAPTURE_01_ENTITY_COUNT = 424


# --- Test-local protobuf byte builder -----------------------------------------


def encode_varint(value: int) -> bytes:
    """Encode an integer as a base-128 varint (two's complement for negatives)."""
    if value < 0:
        value += 1 << 64
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def field_varint(field_number: int, value: int) -> bytes:
    return encode_varint((field_number << 3) | 0) + encode_varint(value)


def field_len(field_number: int, payload: bytes) -> bytes:
    return (
        encode_varint((field_number << 3) | 2)
        + encode_varint(len(payload))
        + payload
    )


def field_fixed32(field_number: int, raw: bytes) -> bytes:
    assert len(raw) == 4
    return encode_varint((field_number << 3) | 5) + raw


def field_fixed64(field_number: int, raw: bytes) -> bytes:
    assert len(raw) == 8
    return encode_varint((field_number << 3) | 1) + raw


def build_stop_time_event(
    delay: int | None = None, time: int | None = None
) -> bytes:
    out = b""
    if delay is not None:
        out += field_varint(1, delay)
    if time is not None:
        out += field_varint(2, time)
    return out


def build_stop_time_update(
    stop_sequence: int = 12,
    stop_id: str | None = "9822",
    arrival_delay: int | None = -557,
    departure_delay: int | None = None,
) -> bytes:
    out = field_varint(1, stop_sequence)
    if arrival_delay is not None:
        out += field_len(2, build_stop_time_event(delay=arrival_delay))
    if departure_delay is not None:
        out += field_len(3, build_stop_time_event(delay=departure_delay))
    if stop_id is not None:
        out += field_len(4, stop_id.encode())
    return out


def build_trip_descriptor(trip_id: str = "18330502") -> bytes:
    return field_len(1, trip_id.encode())


def build_trip_update(
    trip_id: str = "18330502",
    vehicle_id: str | None = "622",
    stop_updates: tuple[bytes, ...] | None = None,
    extra_fields: bytes = b"",
) -> bytes:
    if stop_updates is None:
        stop_updates = (build_stop_time_update(),)
    out = field_len(1, build_trip_descriptor(trip_id))
    for stu in stop_updates:
        out += field_len(2, stu)
    if vehicle_id is not None:
        out += field_len(3, field_len(1, vehicle_id.encode()))
    return out + extra_fields


def build_entity(
    entity_id: str = "1",
    trip_update: bytes | None = None,
    extra_fields: bytes = b"",
) -> bytes:
    if trip_update is None:
        trip_update = build_trip_update()
    return field_len(1, entity_id.encode()) + field_len(3, trip_update) + extra_fields


def build_header(
    version: str | None = "2.0",
    incrementality: int | None = 0,
    timestamp: int | None = 1_794_024_790,
) -> bytes:
    out = b""
    if version is not None:
        out += field_len(1, version.encode())
    if incrementality is not None:
        out += field_varint(2, incrementality)
    if timestamp is not None:
        out += field_varint(3, timestamp)
    return out


def build_feed(
    header: bytes | None = None,
    entities: tuple[bytes, ...] = (),
    extra_fields: bytes = b"",
) -> bytes:
    if header is None:
        header = build_header()
    return field_len(1, header) + b"".join(field_len(2, e) for e in entities) + extra_fields


# --- Matrix entry 1: truncated messages ---------------------------------------


def test_matrix_1_truncated_mid_field() -> None:
    valid = build_feed(entities=(build_entity(),))
    truncated = valid[:-5]
    with pytest.raises(ProtobufDecodeError):
        decode_trip_update_feed(truncated)


def test_matrix_1_truncated_mid_varint() -> None:
    # A tag for header field 1 followed by a length varint that never ends.
    truncated = encode_varint((1 << 3) | 2) + b"\x80"
    with pytest.raises(ProtobufDecodeError, match="truncated"):
        decode_trip_update_feed(truncated)


def test_matrix_1_truncated_mid_length_prefix() -> None:
    # Header field announces 10 payload bytes but the message ends after 3.
    truncated = (
        encode_varint((1 << 3) | 2) + encode_varint(10) + b"\x0a\x03\x02"
    )
    with pytest.raises(ProtobufDecodeError, match="overrun|truncated"):
        decode_trip_update_feed(truncated)


def test_matrix_1_truncated_empty_varint_input() -> None:
    with pytest.raises(ProtobufDecodeError, match="truncated"):
        read_varint(b"", 0)


# --- Matrix entry 2: varint overflow ------------------------------------------


def test_matrix_2_varint_longer_than_ten_bytes() -> None:
    eleven_continuation_bytes = b"\x80" * 10 + b"\x01"
    with pytest.raises(ProtobufDecodeError, match="exceeds 10 bytes"):
        read_varint(eleven_continuation_bytes, 0)


def test_matrix_2_varint_value_exceeds_64_bits() -> None:
    # 10 bytes encoding 2**64 — within the byte budget, above the value limit.
    ten_bytes_value_2_pow_64 = b"\x80" * 9 + b"\x02"
    with pytest.raises(ProtobufDecodeError, match="exceeding 64 bits"):
        read_varint(ten_bytes_value_2_pow_64, 0)


def test_matrix_2_varint_overflow_inside_message() -> None:
    overflowing = (
        encode_varint((1 << 3) | 2)
        + encode_varint(3)
        + b"\x80" * 10
        + b"\x01"
    )
    with pytest.raises(ProtobufDecodeError):
        decode_trip_update_feed(overflowing)


# --- Matrix entry 3: unsupported wire types -----------------------------------


@pytest.mark.parametrize("wire_type", [3, 4, 6, 7])
def test_matrix_3_unsupported_wire_types(wire_type: int) -> None:
    tag = encode_varint((15 << 3) | wire_type)
    with pytest.raises(ProtobufDecodeError, match="not supported"):
        parse_wire(tag)


# --- Matrix entry 4: invalid length-delimited fields --------------------------


def test_matrix_4_length_exceeds_remaining_bytes() -> None:
    # Header field announces 1,000,000 payload bytes; nothing follows.
    with pytest.raises(ProtobufDecodeError, match="overrun"):
        parse_wire(encode_varint((1 << 3) | 2) + encode_varint(1_000_000))


def test_matrix_4_oversized_length_inside_message() -> None:
    bad = encode_varint((1 << 3) | 2) + encode_varint(1 << 40)
    with pytest.raises(ProtobufDecodeError, match="overrun"):
        decode_trip_update_feed(bad)


def test_matrix_4_fixed_width_overrun() -> None:
    with pytest.raises(ProtobufDecodeError, match="needs 4 bytes"):
        parse_wire(encode_varint((15 << 3) | 5) + b"\x01\x02")
    with pytest.raises(ProtobufDecodeError, match="needs 8 bytes"):
        parse_wire(encode_varint((15 << 3) | 1) + b"\x01\x02\x03")


# --- Matrix entry 5: unknown fields skipped and counted -----------------------


def test_matrix_5_unknown_fields_at_every_level_are_skipped_and_counted() -> None:
    # Unknown fields at feed level (15 varint, 16 len, 17 fixed32, 18 fixed64),
    # inside an entity (99 varint), and inside a stop_time_update (98 varint).
    stu_with_unknown = build_stop_time_update() + field_varint(98, 5)
    entity_with_unknown = build_entity(
        trip_update=build_trip_update(stop_updates=(stu_with_unknown,)),
        extra_fields=field_varint(99, 7),
    )
    feed = build_feed(
        entities=(entity_with_unknown,),
        extra_fields=(
            field_varint(15, 1)
            + field_len(16, b"future")
            + field_fixed32(17, b"\x01\x02\x03\x04")
            + field_fixed64(18, b"\x01" * 8)
        ),
    )
    decoded = decode_trip_update_feed(feed)
    assert decoded.unknown_field_count == 6  # 4 feed-level + 1 entity + 1 STU
    # The known structure survived untouched.
    assert decoded.entities[0].trip_update is not None
    assert decoded.entities[0].trip_update.trip is not None
    assert decoded.entities[0].trip_update.trip.trip_id == "18330502"


def test_matrix_5_known_field_number_with_wrong_wire_type_is_counted() -> None:
    # Header field 3 (timestamp) arrives as length-delimited instead of varint:
    # skipped and counted, timestamp stays None (the parse layer then rejects).
    bad_header = field_len(1, b"2.0") + field_varint(2, 0) + field_len(3, b"oops")
    feed = field_len(1, bad_header)
    decoded = decode_trip_update_feed(feed)
    assert decoded.header.timestamp is None
    assert decoded.unknown_field_count == 1


# --- Matrix entry 6: negative delays preserved exactly ------------------------


@pytest.mark.parametrize("delay", [-557, -1, -2_147_483_648, 0, 2_147_483_647])
def test_matrix_6_negative_and_positive_delays_preserved(delay: int) -> None:
    stu = build_stop_time_update(arrival_delay=delay)
    feed = build_feed(entities=(build_entity(trip_update=build_trip_update(stop_updates=(stu,)),),))
    decoded = decode_trip_update_feed(feed)
    trip_update = decoded.entities[0].trip_update
    assert trip_update is not None
    assert trip_update.stop_time_updates[0].arrival is not None
    assert trip_update.stop_time_updates[0].arrival.delay == delay


def test_matrix_6_entity_level_delay_preserved() -> None:
    trip_update = build_trip_update() + field_varint(5, -1_234)
    feed = build_feed(entities=(build_entity(trip_update=trip_update),))
    decoded = decode_trip_update_feed(feed)
    assert decoded.entities[0].trip_update is not None
    assert decoded.entities[0].trip_update.delay == -1_234


# --- Matrix entry 7: missing optional fields stay None ------------------------


def test_matrix_7_missing_optional_fields_are_none() -> None:
    # Entity without a vehicle descriptor; trip descriptor without route;
    # stop_time_update without stop_id and without departure.
    stu = field_varint(1, 7) + field_len(2, build_stop_time_event(delay=-10))
    trip_update = field_len(1, build_trip_descriptor("18330502")) + field_len(2, stu)
    entity = field_len(1, b"1") + field_len(3, trip_update)
    decoded = decode_trip_update_feed(build_feed(entities=(entity,)))
    decoded_entity = decoded.entities[0]
    assert decoded_entity.trip_update is not None
    assert decoded_entity.trip_update.vehicle is None
    assert decoded_entity.trip_update.timestamp is None
    assert decoded_entity.trip_update.delay is None
    assert decoded_entity.trip_update.trip is not None
    assert decoded_entity.trip_update.trip.route_id is None
    stu_decoded = decoded_entity.trip_update.stop_time_updates[0]
    assert stu_decoded.stop_id is None
    assert stu_decoded.departure is None
    assert stu_decoded.arrival is not None
    assert stu_decoded.arrival.uncertainty is None


# --- Matrix entry 8: empty message --------------------------------------------


def test_matrix_8_empty_message_is_feed_level_rejection() -> None:
    with pytest.raises(ProtobufDecodeError, match="no header"):
        decode_trip_update_feed(b"")


# --- Matrix entry 9: minimal valid message ------------------------------------


def test_matrix_9_minimal_valid_header_only_message() -> None:
    decoded = decode_trip_update_feed(build_feed(entities=()))
    assert decoded.header is not None
    assert decoded.header.gtfs_realtime_version == "2.0"
    assert decoded.header.incrementality_raw == 0
    assert decoded.header.timestamp == 1_794_024_790
    assert decoded.entities == ()
    assert decoded.unknown_field_count == 0


def test_matrix_9_missing_header_is_rejected() -> None:
    # Entities without a header: proto2 required field missing.
    entity_only = field_len(2, build_entity())
    with pytest.raises(ProtobufDecodeError, match="no header"):
        decode_trip_update_feed(entity_only)


def test_matrix_9_duplicate_header_is_rejected() -> None:
    doubled = field_len(1, build_header()) + field_len(1, build_header())
    with pytest.raises(ProtobufDecodeError, match="exactly one is required"):
        decode_trip_update_feed(doubled)


# --- Matrix entry 10: round-trip against the static capture fixtures ----------


def test_matrix_10_round_trip_capture_01_entity_count() -> None:
    payload = (FIXTURE_DIR / "tripupdate_capture_01.bin").read_bytes()
    decoded: RawTripUpdateFeed = decode_trip_update_feed(payload)
    assert len(decoded.entities) == CAPTURE_01_ENTITY_COUNT
    assert decoded.header is not None
    assert decoded.header.incrementality_raw == 0
    assert decoded.header.gtfs_realtime_version == "2.0"


def test_matrix_10_round_trip_capture_10_decodes() -> None:
    payload = (FIXTURE_DIR / "tripupdate_capture_10.bin").read_bytes()
    decoded = decode_trip_update_feed(payload)
    assert decoded.header is not None
    assert decoded.header.timestamp is not None and decoded.header.timestamp > 0
    assert len(decoded.entities) > 0


# --- Structural defect → no partial results -----------------------------------


def test_decoder_never_returns_partial_structure_on_error() -> None:
    # A valid feed whose second entity is truncated: the error must surface,
    # never a partially filled entity tuple.
    valid = build_feed(entities=(build_entity(entity_id="1"),))
    truncated_second = build_feed(
        entities=(build_entity(entity_id="1"), build_entity(entity_id="2")),
    )[:-3]
    assert decode_trip_update_feed(valid) is not None
    with pytest.raises(ProtobufDecodeError):
        decode_trip_update_feed(truncated_second)
