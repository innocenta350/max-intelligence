"""Stage S2 checks for download/extract staging (backend/gtfs/extractor.py).

Two test layers, both fully local (no live TriMet feed, no external network):

1. Transport mechanics via a threaded loopback HTTP server (``127.0.0.1``)
   that serves fixed canned responses — real socket I/O, real header
   handling, no mocks.
2. ZIP validation/extraction via synthetic archives built with ``zipfile``.

Note: configured APIs receive deliberately constructed ``GtfsConfig``
instances (direct dataclass construction bypasses ``from_env``'s HTTPS-only
validation on purpose). Production config semantics are untouched —
``GtfsConfig.from_env`` still rejects non-HTTPS feeds — this only lets the
loopback HTTP server stand in for the real feed host.
"""

import hashlib
import io
import json
import os
import socket
import stat
import threading
import time
import warnings
import zipfile
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from backend.gtfs.errors import (
    CorruptZipError,
    ExtractionError,
    MissingRequiredFileError,
)
from backend.gtfs.extractor import (
    CHUNK_SIZE,
    _discard_if_own,
    _write_guard,
    create_staging_dir,
    download_to_staging,
    download_zip,
    safe_extract,
    stage_gtfs,
)
from backend.trimet.config import GtfsConfig


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

HTTP_BODY = bytes(range(256)) * 2048  # 512 KiB: multiple streaming chunks
BIG_BODY = bytes(range(256)) * 8192   # 2 MiB


@contextmanager
def _feed_server(scenario: dict):
    """Serve one canned scenario on loopback; yields the full request URL.

    Scenario keys: body (bytes), status (int), content_length ("auto" |
    None | str), send_bytes (int | None for "all"), hang (bool). The server
    is HTTP/1.0: each response closes the connection, so short writes read
    as clean EOF on the client.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if scenario.get("hang"):
                threading.Event().wait(5)  # stall until the client gives up
                return
            body = scenario.get("body", b"")
            self.send_response(scenario.get("status", 200))
            content_length = scenario.get("content_length", "auto")
            if content_length == "auto":
                self.send_header("Content-Length", str(len(body)))
            elif content_length is not None:
                self.send_header("Content-Length", content_length)
            self.end_headers()
            if scenario.get("status", 200) != 200:
                return
            limit = scenario.get("send_bytes")
            self.wfile.write(body if limit is None else body[:limit])

        def log_message(self, *args) -> None:  # keep test output clean
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        yield base + scenario.get("path", "/gtfs.zip")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _loopback_config(
    url: str,
    work_dir: Path,
    max_zip_bytes: int = 2**31,
    max_dataset_bytes: int = 20 * 1024**3,
) -> GtfsConfig:
    """A GtfsConfig built directly, bypassing from_env's HTTPS-only rule.

    Test-only: the loopback server speaks plain HTTP. Configuration semantics
    for real use (HTTPS enforced in ``GtfsConfig._validate``) are unchanged.
    """
    return GtfsConfig(
        feed_url=url,
        work_dir=work_dir,
        max_zip_bytes=max_zip_bytes,
        max_dataset_bytes=max_dataset_bytes,
    )


def _write_zip(
    path: Path,
    entries: dict[str, bytes] | list[tuple[str, bytes]],
    *,
    compression: int = zipfile.ZIP_STORED,
) -> Path:
    """Write a synthetic GTFS-like zip; keys ending in '/' become dir entries.

    Accepts a mapping or an ordered list of (name, content) pairs — the list
    form is needed for archives with exact duplicate member names. All members
    are stored uncompressed by default; ``ZIP_DEFLATED`` builds the realistic
    case where archive size and uncompressed corpus size differ sharply.
    """
    items = entries.items() if hasattr(entries, "items") else entries
    with zipfile.ZipFile(path, "w", compression=compression) as zf:
        for name, content in items:
            zf.writestr(name, content)
    return path


def _read_zip_bytes(
    entries: dict[str, bytes],
    *,
    compression: int = zipfile.ZIP_STORED,
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as zf:
        for name, content in entries.items():
            zf.writestr(name, content)
    return buffer.getvalue()


def _write_backdoor_zip(path: Path, entries: dict[str, bytes]) -> Path:
    """Write a zip allowing exact duplicate member names (suppresses the
    stdlib's intentional UserWarning for the duplicate)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return _write_zip(path, entries)


def _patch_member_name(zip_bytes: bytes, old_name: str, new_name: str) -> bytes:
    """Rename a member at byte level (equal-length names required).

    The ``zipfile`` reader decodes member names and normalizes '\\' to '/',
    so a crafted ``a\\b.txt`` payload always *arrives* at the validation
    layer as ``a//b.txt`` and a crafted ``..\\evil.txt`` arrives as
    ``../evil.txt`` — refused by the explicit ``..`` rule. Byte-level
    renaming of both the local header and the central directory reproduces a
    real crafted archive so the tests exercise that reader-side equivalence
    end to end.
    """
    if len(old_name) != len(new_name):
        raise AssertionError("byte-level rename needs equal-length names")
    old_b = old_name.encode("ascii")
    new_b = new_name.encode("ascii")
    out = bytearray(zip_bytes)
    patched = 0
    pos = 0
    while True:
        hit = out.find(b"PK\x03\x04", pos)
        if hit == -1:
            break
        name_len = int.from_bytes(out[hit + 26 : hit + 28], "little")
        extra_len = int.from_bytes(out[hit + 28 : hit + 30], "little")
        if bytes(out[hit + 30 : hit + 30 + name_len]) == old_b:
            out[hit + 30 : hit + 30 + name_len] = new_b
            patched += 1
        pos = hit + 30 + name_len + extra_len
    pos = 0
    while True:
        hit = out.find(b"PK\x01\x02", pos)
        if hit == -1:
            break
        name_len = int.from_bytes(out[hit + 28 : hit + 30], "little")
        tail = (
            int.from_bytes(out[hit + 30 : hit + 32], "little")
            + int.from_bytes(out[hit + 32 : hit + 34], "little")
        )
        if bytes(out[hit + 46 : hit + 46 + name_len]) == old_b:
            out[hit + 46 : hit + 46 + name_len] = new_b
            patched += 1
        pos = hit + 46 + name_len + tail
    if patched < 2:  # at least one local header + one central record
        raise AssertionError(f"member {old_name!r} not found in archive bytes")
    return bytes(out)


def _patch_central_flags(zip_bytes: bytes, mask: int) -> bytes:
    """Force flag bits on in every central-directory record.

    ``zipfile``'s writer re-derives flags at write time (dropping the crafted
    encrypted bit), so a real encrypted archive is simulated at byte level.
    """
    out = bytearray(zip_bytes)
    pos = 0
    while True:
        hit = out.find(b"PK\x01\x02", pos)
        if hit == -1:
            break
        name_len = int.from_bytes(out[hit + 28 : hit + 30], "little")
        tail = (
            int.from_bytes(out[hit + 30 : hit + 32], "little")
            + int.from_bytes(out[hit + 32 : hit + 34], "little")
        )
        flags = int.from_bytes(out[hit + 8 : hit + 10], "little") | mask
        out[hit + 8 : hit + 10] = flags.to_bytes(2, "little")
        pos = hit + 46 + name_len + tail
    return bytes(out)


def _corrupt_member_crc(zip_bytes: bytes) -> bytes:
    """Flip a data byte inside the first member so its CRC no longer matches."""
    raw = bytearray(zip_bytes)
    hit = raw.find(b"PK\x03\x04")
    name_len = int.from_bytes(zip_bytes[hit + 26 : hit + 28], "little")
    extra_len = int.from_bytes(zip_bytes[hit + 28 : hit + 30], "little")
    data_start = hit + 30 + name_len + extra_len
    raw[data_start] ^= 0xFF
    return bytes(raw)


def _spoof_compression_method(zip_bytes: bytes, method: int) -> bytes:
    """Force the central-directory compression method byte of a member."""
    raw = bytearray(zip_bytes)
    pos = 0
    while True:
        hit = raw.find(b"PK\x01\x02", pos)
        if hit == -1:
            break
        name_len = int.from_bytes(zip_bytes[hit + 28 : hit + 30], "little")
        extras = (
            int.from_bytes(zip_bytes[hit + 30 : hit + 32], "little")
            + int.from_bytes(zip_bytes[hit + 32 : hit + 34], "little")
        )
        raw[hit + 10 : hit + 12] = method.to_bytes(2, "little")
        pos = hit + 46 + name_len + extras
    return bytes(raw)


def _external_attr_member(
    path: Path,
    name: str,
    mode: int,
) -> Path:
    """Build a one-member unix-created zip with an explicit file-type mode."""
    with zipfile.ZipFile(path, "w") as zf:
        info = zipfile.ZipInfo(name)
        info.create_system = 3
        info.external_attr = mode << 16
        zf.writestr(info, b"target-content")
    return path


# ---------------------------------------------------------------------------
# Transport mechanics (raw download over real loopback sockets)
# ---------------------------------------------------------------------------


def test_streamed_download_records_sha256_and_size(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server({"body": HTTP_BODY}) as url:
        result = download_zip(url, staging / "gtfs.zip", max_bytes=2**31)
    assert result.zip_size_bytes == len(HTTP_BODY)
    assert result.sha256 == hashlib.sha256(HTTP_BODY).hexdigest()
    assert (staging / "gtfs.zip").read_bytes() == HTTP_BODY
    assert not list(staging.glob("*.part"))  # finalized, no temp leftovers


def test_missing_content_length_still_records_bytes(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server({"body": HTTP_BODY, "content_length": None}) as url:
        result = download_zip(url, staging / "gtfs.zip", max_bytes=2**31)
    assert result.zip_size_bytes == len(HTTP_BODY)  # streamed count is authority
    assert result.sha256 == hashlib.sha256(HTTP_BODY).hexdigest()


def test_content_length_limits_transfer_body_per_protocol(tmp_path) -> None:
    """A server declaring 5 bytes hands a compliant client exactly 5 bytes.

    http.client clamps reads to the declared Content-Length (RFC semantics),
    so extra bytes beyond the declared body are never delivered. The transfer
    therefore ends self-consistent (received == declared); a feed whose real
    zip is larger than what was declared simply produces a truncated file,
    which the downstream ZIP validation rejects as corrupt.
    """
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server({"body": HTTP_BODY, "content_length": "5"}) as url:
        result = download_zip(url, staging / "gtfs.zip", max_bytes=2**31)
    assert result.zip_size_bytes == 5
    assert (staging / "gtfs.zip").read_bytes() == HTTP_BODY[:5]
    assert result.sha256 == hashlib.sha256(HTTP_BODY[:5]).hexdigest()
    assert not list(staging.glob("*.part"))


def test_content_length_exceeding_cap_fails_before_writing(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server(
        {"body": HTTP_BODY, "content_length": str(len(HTTP_BODY))}
    ) as url:
        with pytest.raises(ExtractionError, match="exceeding the configured limit"):
            download_zip(url, staging / "gtfs.zip", max_bytes=1024)
    # The preliminary Content-Length gate refused before any file appeared.
    assert not (staging / "gtfs.zip").exists()
    assert not (staging / "gtfs.zip.part").exists()


def test_streamed_cap_stops_transfer_mid_flight(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    cap = int(CHUNK_SIZE * 1.25)
    with _feed_server({"body": BIG_BODY, "content_length": None}) as url:
        with pytest.raises(ExtractionError, match="exceeds the configured limit"):
            download_zip(url, staging / "gtfs.zip", max_bytes=cap)
    # Only the accepted prefix (bounded by the cap) ever reached disk.
    partial = staging / "gtfs.zip.part"
    assert partial.exists()
    assert partial.stat().st_size <= cap
    assert partial.stat().st_size < len(BIG_BODY)


def test_http_error_maps_to_extraction_error_without_query(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server(
        {"body": b"", "status": 404, "path": "/gtfs.zip?app_id=zecret"}
    ) as url:
        with pytest.raises(ExtractionError) as exc_info:
            download_zip(url, staging / "gtfs.zip", max_bytes=2**31)
    assert "HTTP 404" in str(exc_info.value)
    # Any secret-bearing query is reduced away in messages.
    assert "zecret" not in str(exc_info.value)


def test_connection_refused_maps_to_extraction_error(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    dead_port = probe.getsockname()[1]
    probe.close()
    with pytest.raises(ExtractionError, match="downloading") as exc_info:
        download_zip(
            f"http://127.0.0.1:{dead_port}/gtfs.zip",
            staging / "gtfs.zip",
            max_bytes=2**31,
        )


def test_timeout_maps_to_extraction_error(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server({"body": HTTP_BODY, "hang": True}) as url:
        with pytest.raises(ExtractionError, match="timed out"):
            download_zip(url, staging / "gtfs.zip", max_bytes=2**31, timeout=0.4)


def test_short_body_falls_short_of_content_length(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server(
        {
            "body": HTTP_BODY,
            "content_length": "10000",
            "send_bytes": 300,
        }
    ) as url:
        with pytest.raises(ExtractionError, match="incomplete"):
            download_zip(url, staging / "gtfs.zip", max_bytes=2**31)


def test_empty_body_refused(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    with _feed_server({"body": b"", "content_length": None}) as url:
        with pytest.raises(ExtractionError, match="no bytes received"):
            download_zip(url, staging / "gtfs.zip", max_bytes=2**31)


def test_non_positive_max_bytes_refused(tmp_path) -> None:
    staging = tmp_path / "stage"
    staging.mkdir()
    # No server needed: the refusal happens before any network I/O.
    with pytest.raises(ExtractionError, match="max_bytes must be a positive"):
        download_zip(
            "http://127.0.0.1:9/gtfs.zip",
            staging / "gtfs.zip",
            max_bytes=0,
        )


# ---------------------------------------------------------------------------
# Configured staging layer (download_to_staging / stage_gtfs)
# ---------------------------------------------------------------------------

_DATASET_ENTRIES = {
    "routes.txt": b"route_id,route_type,route_color\n90,0,C41F3E\n",
    "stops.txt": b"stop_id,stop_name\n14023,Gateway NE 99th\n",
    "sub/": b"",
    "sub/agency.txt": b"agency_name,agency_url\nTriMet,https://example.org\n",
}


def test_stage_gtfs_end_to_end_produces_staged_zip(tmp_path) -> None:
    work_dir = tmp_path / "work"
    zip_bytes = _read_zip_bytes(_DATASET_ENTRIES)
    with _feed_server({"body": zip_bytes}) as url:
        staged = stage_gtfs(_loopback_config(url, work_dir))
    assert isinstance(staged, object)
    assert staged.zip_size_bytes == len(zip_bytes)
    assert staged.sha256 == hashlib.sha256(zip_bytes).hexdigest()
    assert staged.dataset_size_bytes == sum(
        len(content) for name, content in _DATASET_ENTRIES.items() if not name.endswith("/")
    )
    assert staged.file_count == 3  # routes, stops, agency (dirs not counted)
    assert staged.staging_dir.parent == work_dir
    assert staged.staging_dir.name.startswith(f"staging-{os.getpid()}-")
    assert staged.zip_path == staged.staging_dir / "gtfs.zip"
    guard = json.loads((staged.staging_dir / "guard.json").read_text(encoding="utf-8"))
    assert guard["pid"] == os.getpid()
    assert guard["heartbeat_utc_ms"] >= guard["started_utc_ms"]
    # S2 must not credit promotion: no manifest exists, and the raw zip sits
    # beside the extracted members (plan Rev. 6 layout for later promotion).
    assert (staged.staging_dir / "gtfs.zip").read_bytes() == zip_bytes
    assert (staged.staging_dir / "routes.txt").exists()
    assert (staged.staging_dir / "sub" / "agency.txt").exists()
    assert not list(staged.staging_dir.glob("*.part"))
    assert not list(staged.staging_dir.glob("*.tmp"))


def test_stage_gtfs_removes_own_staging_on_http_failure(tmp_path) -> None:
    work_dir = tmp_path / "work"
    unrelated = work_dir / "keep-me"
    unrelated.mkdir(parents=True)
    (unrelated / "sibling.txt").write_bytes(b"do not touch")
    with _feed_server({"body": b"", "status": 404}) as url:
        with pytest.raises(ExtractionError, match="HTTP 404") as exc_info:
            stage_gtfs(_loopback_config(url, work_dir))
    # Own staging was disposable and fully removed (no leftovers at all);
    # the unrelated sibling directory survived untouched.
    assert not list(work_dir.glob("staging-*"))
    assert (unrelated / "sibling.txt").read_bytes() == b"do not touch"
    # Cleanup itself succeeded: no failure note attached to the raised error.
    assert getattr(exc_info.value, "cleanup_failure", None) is None


def test_stage_gtfs_removes_own_staging_on_mid_transfer_cap(tmp_path) -> None:
    work_dir = tmp_path / "work"
    # No Content-Length header: the cap must catch mid-stream, not pre-flight.
    with _feed_server({"body": BIG_BODY, "content_length": None}) as url:
        with pytest.raises(ExtractionError, match="exceeds the configured limit"):
            stage_gtfs(_loopback_config(url, work_dir, max_zip_bytes=4096))
    assert not list(work_dir.glob("staging-*"))
    assert not any(
        ".part" in p.name for p in work_dir.rglob("*")
    )


def test_download_to_staging_accepts_existing_caller_staging(tmp_path) -> None:
    work_dir = tmp_path / "work"
    staging = create_staging_dir(work_dir)
    zip_bytes = _read_zip_bytes({"routes.txt": b"route_id,route_type\n90,0\n"})
    with _feed_server({"body": zip_bytes}) as url:
        downloaded = download_to_staging(
            _loopback_config(url, work_dir), staging_dir=staging
        )
    assert downloaded.zip_path == staging / "gtfs.zip"
    assert downloaded.zip_size_bytes == len(zip_bytes)
    assert (staging / "gtfs.zip").exists()
    # Exactly this one staging directory was used, no second was invented.
    assert len(list(work_dir.glob("staging-*"))) == 1


def test_download_to_staging_refuses_nonexistent_staging(tmp_path) -> None:
    config = _loopback_config("http://127.0.0.1:9/gtfs.zip", tmp_path / "work")
    with pytest.raises(ExtractionError, match="does not exist"):
        download_to_staging(config, staging_dir=tmp_path / "missing-staging")


def test_refuses_to_replace_existing_zip_in_staging(tmp_path) -> None:
    staging = create_staging_dir(tmp_path / "work")
    (staging / "gtfs.zip").write_bytes(b"previous artifact")
    # No server needed: the refusal precedes any network I/O.
    with pytest.raises(ExtractionError, match="refusing to replace"):
        download_zip(
            "http://127.0.0.1:9/gtfs.zip",
            staging / "gtfs.zip",
            max_bytes=2**31,
        )


# ---------------------------------------------------------------------------
# guard.json ownership marker (helps S6 distinguish live vs abandoned staging)
# ---------------------------------------------------------------------------


def test_write_guard_initialises_then_preserves_started(tmp_path) -> None:
    staging = create_staging_dir(tmp_path)
    _write_guard(staging)
    first = json.loads((staging / "guard.json").read_text(encoding="utf-8"))
    assert first["pid"] == os.getpid()
    time.sleep(0.01)  # ensure the wall-clock ms tick moves forward
    _write_guard(staging)
    second = json.loads((staging / "guard.json").read_text(encoding="utf-8"))
    assert second["started_utc_ms"] == first["started_utc_ms"]
    assert second["heartbeat_utc_ms"] >= second["started_utc_ms"]


def test_write_guard_refuses_foreign_pid(tmp_path) -> None:
    staging = create_staging_dir(tmp_path)
    foreign_pid = os.getpid() + 4_000_000
    (staging / "guard.json").write_text(
        json.dumps({"pid": foreign_pid, "started_utc_ms": 1, "heartbeat_utc_ms": 1}),
        encoding="utf-8",
    )
    with pytest.raises(ExtractionError, match=f"pid {foreign_pid}"):
        _write_guard(staging)
    # The foreign record is byte-for-byte unchanged: no adoption, no rewrite.
    record = json.loads((staging / "guard.json").read_text(encoding="utf-8"))
    assert record["pid"] == foreign_pid


def test_discard_removes_own_staging(tmp_path) -> None:
    staging = create_staging_dir(tmp_path)
    _write_guard(staging)
    assert _discard_if_own(staging) is None
    assert not staging.exists()


def test_discard_never_touches_foreign_staging(tmp_path) -> None:
    staging = tmp_path / "foreign-staging"
    staging.mkdir()
    foreign_pid = os.getpid() + 4_000_000
    (staging / "guard.json").write_text(
        json.dumps({"pid": foreign_pid, "started_utc_ms": 5, "heartbeat_utc_ms": 5}),
        encoding="utf-8",
    )
    note = _discard_if_own(staging)
    assert note is not None
    assert str(foreign_pid) in note
    assert staging.exists()  # untouched: another process owns it


def test_discard_never_touches_unverifiable_staging(tmp_path) -> None:
    staging = tmp_path / "unmarked-staging"
    staging.mkdir()
    note = _discard_if_own(staging)
    assert note is not None
    assert "unreadable" in note
    assert staging.exists()  # no guard.json -> ownership cannot be proven


@pytest.mark.parametrize(
    "payload",
    [
        b"this is not json at all",
        b"",
        b"null",
        b"[]",
        b'{"started_utc_ms": 1, "heartbeat_utc_ms": 1}',  # pid missing
        b'{"pid": null, "started_utc_ms": 1, "heartbeat_utc_ms": 1}',
        b'{"pid": "not-a-number", "started_utc_ms": 1}',
        b'{"pid": 12.5, "started_utc_ms": 1}',
        b'{"pid": true, "started_utc_ms": 1}',
        b'{"pid": -1, "started_utc_ms": 1}',
        b'{"pid": "4321", "started_utc_ms": 1}',  # numeric string: not a pid
        (b'{"pid": ' + str(os.getpid()).encode() + b'.0, "started_utc_ms": 1}'),
        (b'{"pid": "' + str(os.getpid()).encode() + b'", "started_utc_ms": 1}'),
        (
            b'{"pid": '
            + str(os.getpid()).encode()
            + b', "started_utc_ms": "x", "heartbeat_utc_ms": 1}'
        ),
        (
            b'{"pid": '
            + str(os.getpid()).encode()
            + b', "started_utc_ms": 1, "heartbeat_utc_ms": null}'
        ),
        (b'{"pid": ' + str(os.getpid()).encode() + b', "started_utc_ms": 1}'),
    ],
    ids=[
        "garbage",
        "empty",
        "json-null",
        "json-list",
        "pid-missing",
        "pid-null",
        "pid-text",
        "pid-float",
        "pid-bool",
        "pid-negative",
        "pid-numeric-string",
        "pid-float-of-own-pid",
        "pid-string-of-own-pid",
        "started-not-an-int",
        "heartbeat-null",
        "heartbeat-missing",
    ],
)
def test_discard_never_touches_guard_records_it_cannot_parse(
    tmp_path, payload: bytes
) -> None:
    """Malformed guard records must never authorize deletion.

    Ownership is a security boundary: only a fully parsed record whose pid
    equals ours may delete the directory. Garbage, empty, structurally wrong,
    null-pid, and non-integer-pid records all leave the directory in place and
    report why.
    """
    staging = tmp_path / "unparseable-staging"
    staging.mkdir()
    (staging / "guard.json").write_bytes(payload)
    (staging / "keep.txt").write_bytes(b"owner data")
    note = _discard_if_own(staging)
    assert note is not None
    assert "left in place" in note
    assert staging.exists()
    assert (staging / "keep.txt").read_bytes() == b"owner data"


def test_discard_never_touches_truncated_record_naming_own_pid(tmp_path) -> None:
    """A partially written record containing our pid is still not proof.

    A substring search for the pid would delete here; parsing the record is
    what makes the ownership decision, so truncated JSON must be refused even
    though the digits of our own pid appear in it.
    """
    staging = tmp_path / "partial-staging"
    staging.mkdir()
    truncated = (
        '{"pid": ' + str(os.getpid()) + ', "started_utc_ms": 12345, "heart'
    ).encode("utf-8")
    (staging / "guard.json").write_bytes(truncated)
    note = _discard_if_own(staging)
    assert note is not None
    assert str(os.getpid()) not in note
    assert staging.exists()


@pytest.mark.parametrize(
    "payload",
    [
        b"garbage before the pid: " + str(os.getpid()).encode("utf-8"),
        b'{"pid": ' + str(os.getpid()).encode("utf-8") + b", truncated",
        b'{"pid": ' + str(os.getpid()).encode("utf-8") + b"} trailing noise",
    ],
    ids=["non-json", "truncated", "trailing-noise"],
)
def test_write_guard_refuses_to_adopt_corrupt_record(tmp_path, payload: bytes) -> None:
    """``_write_guard`` never repairs over a record it cannot parse.

    A corrupt record inside a caller-supplied staging directory could belong
    to another process whose write was interrupted; silently replacing it
    would claim ownership of someone else's directory. The refusal is an
    ``ExtractionError`` naming the unreadable guard, and the file is left
    byte-for-byte untouched.
    """
    staging = create_staging_dir(tmp_path)
    (staging / "guard.json").write_bytes(payload)
    with pytest.raises(ExtractionError, match="unreadable guard.json"):
        _write_guard(staging)
    assert (staging / "guard.json").read_bytes() == payload


def test_stage_gtfs_leaves_caller_staging_with_corrupt_guard(tmp_path) -> None:
    """End-to-end: corrupt ownership marker blocks work and blocks deletion.

    The staging directory is caller-supplied and already contains an
    unparseable guard record. ``stage_gtfs`` must fail before any network I/O
    (a dead port URL proves it never dialed), must leave the directory in
    place, and must surface why cleanup was skipped.
    """
    work_dir = tmp_path / "work"
    staging = create_staging_dir(work_dir)
    payload = b'{"pid": ' + str(os.getpid()).encode("utf-8") + b", truncated"
    (staging / "guard.json").write_bytes(payload)
    with pytest.raises(ExtractionError, match="unreadable guard.json") as exc_info:
        stage_gtfs(
            _loopback_config("http://127.0.0.1:9/gtfs.zip", work_dir),
            staging_dir=staging,
        )
    assert staging.exists()
    assert (staging / "guard.json").read_bytes() == payload
    assert not (staging / "gtfs.zip").exists()
    note = getattr(exc_info.value, "cleanup_failure", None)
    assert note is not None and "left in place" in note


def test_stage_gtfs_never_deletes_caller_staging_with_foreign_pid(tmp_path) -> None:
    """A foreign guard record blocks both adoption and cleanup."""
    work_dir = tmp_path / "work"
    staging = create_staging_dir(work_dir)
    foreign_pid = os.getpid() + 4_000_000
    record = json.dumps(
        {"pid": foreign_pid, "started_utc_ms": 7, "heartbeat_utc_ms": 8}
    )
    (staging / "guard.json").write_text(record, encoding="utf-8")
    with pytest.raises(ExtractionError, match=f"pid {foreign_pid}") as exc_info:
        stage_gtfs(
            _loopback_config("http://127.0.0.1:9/gtfs.zip", work_dir),
            staging_dir=staging,
        )
    assert staging.exists()
    assert (staging / "guard.json").read_text(encoding="utf-8") == record
    assert not (staging / "gtfs.zip").exists()
    note = getattr(exc_info.value, "cleanup_failure", None)
    assert note is not None and str(foreign_pid) in note


# ---------------------------------------------------------------------------
# ZIP validation and extraction (synthetic archives)
# ---------------------------------------------------------------------------


def test_safe_extract_valid_nested_layout(tmp_path) -> None:
    zip_path = _write_zip(tmp_path / "gtfs.zip", _DATASET_ENTRIES)
    dest = tmp_path / "staging-x"
    outcome = safe_extract(zip_path, dest)
    assert outcome.dataset_path == dest
    assert outcome.file_count == 3
    assert outcome.dataset_size_bytes == sum(
        len(content) for name, content in _DATASET_ENTRIES.items() if not name.endswith("/")
    )
    assert (dest / "routes.txt").read_bytes() == _DATASET_ENTRIES["routes.txt"]
    assert (dest / "sub" / "agency.txt").read_bytes() == _DATASET_ENTRIES["sub/agency.txt"]
    assert (dest / "sub").is_dir()


def test_directory_member_is_materialized_but_not_counted(tmp_path) -> None:
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {"sub/": b"", "sub/only.txt": b"content"},
    )
    dest = tmp_path / "staging-x"
    outcome = safe_extract(zip_path, dest)
    assert (dest / "sub").is_dir()
    assert outcome.file_count == 1
    assert outcome.dataset_size_bytes == len(b"content")


def test_garbage_bytes_rejected_as_corrupt_zip(tmp_path) -> None:
    path = tmp_path / "gtfs.zip"
    path.write_bytes(b"definitely not a zip archive")
    dest = tmp_path / "staging-x"
    with pytest.raises(CorruptZipError, match="not a readable zip archive"):
        safe_extract(path, dest)


def test_truncated_zip_rejected_as_corrupt(tmp_path) -> None:
    zip_bytes = _read_zip_bytes({"routes.txt": b"route_id\n90\n"})
    path = tmp_path / "gtfs.zip"
    path.write_bytes(zip_bytes[: len(zip_bytes) // 2])  # central directory cut off
    with pytest.raises(CorruptZipError, match="not a readable zip archive"):
        safe_extract(path, tmp_path / "staging-x")


def test_corrupt_member_crc_maps_to_corrupt_zip(tmp_path) -> None:
    zip_bytes = _read_zip_bytes({"routes.txt": b"payload for crc test" * 8})
    path = tmp_path / "gtfs.zip"
    path.write_bytes(_corrupt_member_crc(zip_bytes))
    with pytest.raises(CorruptZipError, match="integrity validation") as exc_info:
        safe_extract(path, tmp_path / "staging-x")
    assert "routes.txt" in str(exc_info.value)


def test_unsupported_compression_rejected(tmp_path) -> None:
    zip_bytes = _read_zip_bytes({"mystery.txt": b"plain"})
    path = tmp_path / "gtfs.zip"
    path.write_bytes(_spoof_compression_method(zip_bytes, method=99))
    with pytest.raises(ExtractionError, match="compression") as exc_info:
        safe_extract(path, tmp_path / "staging-x")
    assert "mystery.txt" in str(exc_info.value)


def test_traversal_member_rejected_and_no_outside_writes(tmp_path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    dest = root / "staging-x"
    zip_path = _write_zip(
        root / "gtfs.zip", {"../evil.txt": b"escaped", "routes.txt": b"route_id\n"}
    )
    with pytest.raises(ExtractionError, match="unsafe segment"):
        safe_extract(zip_path, dest)
    # Nothing escaped the staging root in either direction: no sibling file,
    # and the staging directory holds no extracted file either.
    assert not (root / "evil.txt").exists()
    assert dest.is_dir()
    assert not any(dest.iterdir())


@pytest.mark.parametrize(
    "member_name",
    [
        "/absolute.txt",
        "C:/drive.txt",
        "ev:il.txt",
        "dir/../x.txt",
    ],
)
def test_unsafe_member_names_rejected(tmp_path, member_name: str) -> None:
    zip_path = _write_zip(tmp_path / "gtfs.zip", {member_name: b"x"})
    with pytest.raises(ExtractionError, match="member name"):
        safe_extract(zip_path, tmp_path / "staging-x")


def test_backslash_member_name_cannot_escape(tmp_path) -> None:
    """A crafted ``a\\b.txt`` member decodes on the reader side to
    ``a//b.txt``; it must extract *inside* staging, never outside it."""
    raw = _patch_member_name(
        _read_zip_bytes({"aXb.txt": b"x"}), old_name="aXb.txt", new_name="a\\b.txt"
    )
    path = tmp_path / "gtfs.zip"
    path.write_bytes(raw)
    dest = tmp_path / "staging-x"
    outcome = safe_extract(path, dest)
    assert outcome.file_count == 1
    assert (dest / "a" / "b.txt").read_bytes() == b"x"


def test_backslash_prefix_traversal_member_rejected(tmp_path) -> None:
    """A crafted ``..\\evil.txt`` decodes to ``../evil.txt`` and is refused
    by the explicit dot-dot rule before any write."""
    raw = _patch_member_name(
        _read_zip_bytes({"XaXevil.txt": b"x"}),
        old_name="XaXevil.txt",
        new_name="..\\evil.txt",
    )
    path = tmp_path / "gtfs.zip"
    path.write_bytes(raw)
    dest = tmp_path / "staging-x"
    with pytest.raises(ExtractionError, match="unsafe segment"):
        safe_extract(path, dest)
    assert not (tmp_path / "evil.txt").exists()
    assert not any(dest.iterdir())


def test_duplicate_members_rejected(tmp_path) -> None:
    """Exact duplicate paths are refused outright — no first-entry-wins.

    Validation is write-free, so the rejection happens before either copy is
    materialized: the staging directory stays empty rather than containing an
    arbitrary representative of the ambiguous member.
    """
    zip_path = _write_backdoor_zip(
        tmp_path / "gtfs.zip",
        [("routes.txt", b"first"), ("routes.txt", b"second")],
    )
    dest = tmp_path / "staging-x"
    with pytest.raises(ExtractionError, match="duplicate archive member"):
        safe_extract(zip_path, dest)
    assert dest.is_dir()
    assert not any(dest.iterdir())  # neither "first" nor "second" was written


def test_duplicate_case_insensitive_members_rejected(tmp_path) -> None:
    """``Dir/x.txt`` and ``dir/X.txt`` collide on Windows-style filesystems.

    The collision is treated as a duplicate in the write-free validation
    phase, so nothing is extracted.
    """
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {"Dir/x.txt": b"one", "dir/X.txt": b"two"},
    )
    dest = tmp_path / "staging-x"
    with pytest.raises(ExtractionError, match="case-insensitive"):
        safe_extract(zip_path, dest)
    assert not any(dest.iterdir())


@pytest.mark.parametrize(
    "mode",
    [
        stat.S_IFLNK | 0o777,
        stat.S_IFIFO | 0o644,
        stat.S_IFCHR | 0o644,
        stat.S_IFBLK | 0o644,
        stat.S_IFSOCK | 0o644,
    ],
    ids=["symlink", "fifo", "chardev", "blockdev", "socket"],
)
def test_non_regular_file_members_rejected(tmp_path, mode: int) -> None:
    zip_path = _external_attr_member(tmp_path / "gtfs.zip", "link", mode)
    dest = tmp_path / "staging-x"
    with pytest.raises(ExtractionError, match="not a regular file") as exc_info:
        safe_extract(zip_path, dest)
    assert "link" in str(exc_info.value)
    assert not list(dest.glob("*"))  # nothing materialized


def test_encrypted_member_flag_refused(tmp_path) -> None:
    raw = _read_zip_bytes({"routes.txt": b"route_id\n"})
    path = tmp_path / "gtfs.zip"
    path.write_bytes(_patch_central_flags(raw, mask=0x1))
    dest = tmp_path / "staging-x"
    with pytest.raises(ExtractionError, match="encrypted"):
        safe_extract(path, dest)
    assert not list(dest.glob("*.txt"))


def test_member_declared_size_exceeds_limit_refused(tmp_path) -> None:
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {"big.txt": b"x" * 4096, "tiny.txt": b"y"},
    )
    dest = tmp_path / "staging-x"
    dest.mkdir()
    with pytest.raises(ExtractionError, match="exceeding the configured limit"):
        safe_extract(zip_path, dest, max_member_bytes=1024)
    assert not list(dest.iterdir())  # big AND tiny never written (aborted first)


def test_existing_file_never_silently_overwritten(tmp_path) -> None:
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {"routes.txt": b"malicious content", "extra.txt": b"more"},
    )
    dest = tmp_path / "staging-x"
    dest.mkdir()
    (dest / "routes.txt").write_bytes(b"pre-existing sentinel")
    with pytest.raises(ExtractionError, match="refusing to overwrite"):
        safe_extract(zip_path, dest)
    assert (dest / "routes.txt").read_bytes() == b"pre-existing sentinel"
    assert not (dest / "extra.txt").exists()  # aborted before later members


def test_required_missing_membership_reported(tmp_path) -> None:
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {"agency.txt": b"agency\n", "shapes.txt": b"none\n"},
    )
    with pytest.raises(MissingRequiredFileError) as exc_info:
        safe_extract(
            zip_path,
            tmp_path / "staging-x",
            required_members=("agency.txt", "routes.txt", "trips.txt"),
        )
    assert exc_info.value.members == ("routes.txt", "trips.txt")
    assert "routes.txt" in str(exc_info.value)


def test_empty_archive_without_requirements_is_accepted(tmp_path) -> None:
    path = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(path, "w"):
        pass
    dest = tmp_path / "staging-x"
    outcome = safe_extract(path, dest)
    assert outcome.file_count == 0
    assert outcome.dataset_size_bytes == 0
    assert not list(dest.iterdir())


def test_extract_never_writes_outside_destination(tmp_path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    pre = root / "keep.txt"
    zip_path = _write_zip(
        root / "gtfs.zip",
        {"a.txt": b"one", "sub/": b"", "sub/b.txt": b"two"},
    )
    pre.write_bytes(b"never touched")
    dest = root / "staging-x"
    outcome = safe_extract(zip_path, dest)
    assert outcome.file_count == 2
    assert sorted(p.name for p in root.iterdir()) == ["gtfs.zip", "keep.txt", "staging-x"]
    assert pre.read_bytes() == b"never touched"


def test_member_count_limit_enforced(tmp_path) -> None:
    # MAX_ARCHIVE_MEMBERS is 1000; build one member beyond it (small entries,
    # fast to construct) and expect the archive to be refused wholesale.
    path = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(path, "w") as zf:
        for index in range(1050):
            zf.writestr(f"m{index}.txt", b"a")
    with pytest.raises(ExtractionError, match="exceeding the limit"):
        safe_extract(path, tmp_path / "staging-x")


# ---------------------------------------------------------------------------
# Total uncompressed dataset cap (S2 review fix 1)
# ---------------------------------------------------------------------------


def test_total_dataset_cap_rejected_before_writes(tmp_path) -> None:
    """Many individually acceptable members can still exceed the corpus cap.

    Three 2,000-byte members each pass a 5,000-byte per-member cap; together
    they declare 6,000 bytes, which the dataset cap must refuse. Validation is
    write-free, so the refusal precedes any extraction.
    """
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {
            "a.txt": b"a" * 2000,
            "b.txt": b"b" * 2000,
            "c.txt": b"c" * 2000,
        },
    )
    dest = tmp_path / "staging-x"
    with pytest.raises(ExtractionError, match="dataset limit") as exc_info:
        safe_extract(
            zip_path,
            dest,
            max_member_bytes=5000,
            max_total_bytes=5000,
        )
    assert "6000" in str(exc_info.value)
    assert "5000" in str(exc_info.value)
    assert not any(dest.iterdir())  # zero bytes materialized


def test_total_dataset_cap_equal_boundary_is_accepted(tmp_path) -> None:
    """Exactly-at-the-cap is allowed; only *exceeding* it is refused."""
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {"a.txt": b"a" * 2000, "b.txt": b"b" * 2000, "c.txt": b"c" * 2000},
    )
    dest = tmp_path / "staging-x"
    outcome = safe_extract(zip_path, dest, max_total_bytes=6000)
    assert outcome.file_count == 3
    assert outcome.dataset_size_bytes == 6000
    assert sorted(p.name for p in dest.iterdir()) == ["a.txt", "b.txt", "c.txt"]


def test_total_dataset_cap_counts_only_regular_file_content(tmp_path) -> None:
    """Directory entries contribute nothing to the declared corpus total."""
    zip_path = _write_zip(
        tmp_path / "gtfs.zip",
        {"sub/": b"", "sub/a.txt": b"12345"},
    )
    dest = tmp_path / "staging-x"
    outcome = safe_extract(zip_path, dest, max_total_bytes=5)
    assert outcome.file_count == 1
    assert outcome.dataset_size_bytes == 5


def test_stage_gtfs_enforces_config_dataset_cap_and_cleans_up(tmp_path) -> None:
    """End-to-end proof that ``config.max_dataset_bytes`` reaches extraction.

    The archive is deflated, so its compressed download is a few hundred bytes
    (well under ``max_zip_bytes=5000``) while its members declare 6,000
    uncompressed bytes. Each member is below the per-member cap of 5,000, so
    only the total-corpus cap can reject it; the same value is configured for
    both caps, mirroring the production ``max_dataset_bytes >= max_zip_bytes``
    rule. On rejection the operation's own staging directory is removed.
    """
    work_dir = tmp_path / "work"
    entries = {"a.txt": b"a" * 2000, "b.txt": b"b" * 2000, "c.txt": b"c" * 2000}
    zip_bytes = _read_zip_bytes(entries, compression=zipfile.ZIP_DEFLATED)
    assert len(zip_bytes) < 5000  # compressed size is not what trips the cap
    with _feed_server({"body": zip_bytes}) as url:
        with pytest.raises(ExtractionError, match="dataset limit"):
            stage_gtfs(
                _loopback_config(
                    url,
                    work_dir,
                    max_zip_bytes=5000,
                    max_dataset_bytes=5000,
                )
            )
    assert not list(work_dir.glob("staging-*"))
    assert not any(".part" in path.name for path in work_dir.rglob("*"))


def test_stage_gtfs_accepts_dataset_at_the_configured_cap(tmp_path) -> None:
    """The same wiring accepts a corpus exactly equal to the cap."""
    work_dir = tmp_path / "work"
    entries = {"a.txt": b"a" * 2000, "b.txt": b"b" * 2000, "c.txt": b"c" * 2000}
    zip_bytes = _read_zip_bytes(entries, compression=zipfile.ZIP_DEFLATED)
    with _feed_server({"body": zip_bytes}) as url:
        staged = stage_gtfs(
            _loopback_config(
                url,
                work_dir,
                max_zip_bytes=5000,
                max_dataset_bytes=6000,
            )
        )
    assert staged.dataset_size_bytes == 6000
    assert staged.file_count == 3
    assert (staged.staging_dir / "c.txt").read_bytes() == b"c" * 2000
