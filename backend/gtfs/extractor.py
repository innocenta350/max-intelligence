"""Download, validation, safe extraction, and staging for gtfs.zip (Stage S2).

Stage S2 owns the download/extraction foundation the plan calls
``download_to_staging`` and ``safe_extract``; promotion (``promote``,
``promote.lock``, the checksum-suffix ladder, and ``manifest.json`` writing)
belongs to Stage S6 and is deliberately absent here.

Staging layout contract (this stage's decision, plan-faithful):

- A staging directory ``<work_dir>/staging-<pid>-<hex16>/`` is created fresh
  for every operation; it *becomes* the dataset directory upon promotion in
  S6.
- The downloaded archive lives inside it as ``gtfs.zip``, beside the
  extracted members at their archive-relative paths — so the promoted
  directory is born with the raw zip stored next to the dataset (plan Rev.
  6).
- ``guard.json`` marks the staging directory live: ``pid``,
  ``started_utc_ms``, ``heartbeat_utc_ms`` (UTC epoch milliseconds in the
  file, per Rev. 4 change 2). It authorizes nothing. S2 refreshes it at
  stage boundaries instead of running a background heartbeat thread; TTL
  semantics belong to the S6 report-only inventory, which treats age against
  these timestamps.

Why there are no download retries: the configured ``GTFS_PROMOTION_*`` retry
policy is promotion-lock contention handling (S6). A download makes exactly
one attempt; a failed download surfaces as an error immediately and never
silently retries indefinitely.

Failure cleanup ownership: on *any* terminal failure of an operation that
created the staging directory, the directory is deleted — but only after
``guard.json`` parses as a complete record attesting to the running process's
pid. Foreign, malformed, partial, or unverifiable staging directories are
never deleted; instead the reason is attached to the raised error as a
``cleanup_failure`` attribute, so cleanup problems are neither silently
swallowed nor hidden. Unrelated directories are untouched by construction.

S2 metadata responsibilities (plan §6): SHA-256 while streaming, zip size,
extracted dataset size, fetch timestamp (UTC epoch ms), and both paths are
returned in :class:`StagedZip`. ``GtfsManifest`` completion (feed_version,
``manifest.json`` persistence) belongs to S6; S2 represents the current
limitation explicitly rather than fabricating a feed version from parsing.

Exception mapping:

- ``CorruptZipError`` — not a valid ZIP, unreadable structure, CRC failure,
  or a member whose contents exceed its declared size.
- ``MissingRequiredFileError`` — caller-required (GTFS) members absent from
  the archive.
- ``ExtractionError`` — transport/service problems, size-cap violations
  (per-member declared, streamed overflow, and the total uncompressed
  corpus cap ``GTFS_MAX_DATASET_BYTES``), and safety rejections: unsafe
  member paths (traversal, absolute, drive prefix, NUL after the stdlib
  reader decodes names), unsupported entry types (symlinks, devices),
  duplicate members, unexpected pre-existing files, and unusable/foreign
  staging dirs.

No secrets appear in messages: URLs are reduced to scheme/host/path (userinfo
and query dropped), paths are file or directory *names* only, and this module
reads nothing from ``os.environ`` or ``.env``.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import shutil
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
import zlib
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from backend.gtfs.errors import (
    CorruptZipError,
    ExtractionError,
    GtfsLoadError,
    MissingRequiredFileError,
)
from backend.trimet.config import GtfsConfig

__all__ = [
    "CHUNK_SIZE",
    "DOWNLOAD_TIMEOUT_SECONDS",
    "MAX_ARCHIVE_MEMBERS",
    "DownloadedZip",
    "ExtractionOutcome",
    "StagedZip",
    "create_staging_dir",
    "download_to_staging",
    "download_zip",
    "safe_extract",
    "stage_gtfs",
]

# Socket-level timeout for connect *and* each read. A module constant rather
# than a new config knob: adding an env var would change approved S0/S1
# behavior with no plan mandate, and with no timeout a wedged connection
# hangs the pipeline forever. Smallest reasonable decision, documented.
DOWNLOAD_TIMEOUT_SECONDS: float = 60.0

# Bounded streaming granularity (chunked reads, never a whole-buffer slurp).
CHUNK_SIZE: int = 256 * 1024

# Hostile-archive DoS guard: a legitimate GTFS feed has a few dozen members.
# Fixed and documented instead of a new config knob to avoid inventing
# settings the plan did not specify.
MAX_ARCHIVE_MEMBERS: int = 1000

_GUARD_NAME = "guard.json"
_ZIP_NAME = "gtfs.zip"
_STAGING_PREFIX = "staging-"
_USER_AGENT = "MAX-Intelligence/static-gtfs"


def _parse_guard_record(record: object) -> tuple[int, int, int]:
    """Validate a complete guard record; return ``(pid, started_ms, heartbeat_ms)``.

    Deliberately strict: ``int()`` would silently coerce ``12.5``, ``True``,
    and ``"4321"`` into a pid, so a malformed or partially written record
    could be misread as naming this process. Ownership is a security boundary,
    so only a JSON object carrying genuine non-negative integers for both
    timestamps and a genuine positive integer pid qualifies; everything else
    raises and is treated as unverifiable (never deleted).
    """
    if not isinstance(record, dict):
        raise ValueError("guard record is not a JSON object")
    pid = record["pid"]
    started = record["started_utc_ms"]
    heartbeat = record["heartbeat_utc_ms"]
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError(f"guard record pid is not a positive integer: {pid!r}")
    for label, value in (
        ("started_utc_ms", started),
        ("heartbeat_utc_ms", heartbeat),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(
                f"guard record {label} is not a non-negative integer: {value!r}"
            )
    return pid, started, heartbeat


@dataclass(frozen=True, slots=True)
class DownloadedZip:
    """Result of one successful download into a staging directory."""

    staging_dir: Path
    zip_path: Path
    zip_size_bytes: int
    sha256: str
    fetched_at_utc_ms: int


@dataclass(frozen=True, slots=True)
class ExtractionOutcome:
    """Result of one successful safe extraction.

    ``dataset_size_bytes`` is the documented S2 meaning of the manifest's
    ``dataset_size_bytes``: the total size of extracted regular-file *content*
    in bytes (corpus bytes only; filesystem block overhead is excluded).
    ``file_count`` counts regular files; directory entries are not counted.
    """

    dataset_path: Path
    dataset_size_bytes: int
    file_count: int


@dataclass(frozen=True, slots=True)
class StagedZip:
    """Everything later stages need about one complete staging operation.

    ``staging_dir`` is the directory S6 promotion will turn into the dataset
    directory. ``feed_version`` is intentionally absent: S2 cannot know it
    without S3/S4 inspecting the archive, and fabricating a value from the
    URL is forbidden. ``fetched_at_utc_ms`` is the wall-clock UTC epoch-ms
    time at the start of the download (the plan's monotonic-clock rule
    applies to scheduling waits, not to recorded timestamps).
    """

    staging_dir: Path
    zip_path: Path
    zip_size_bytes: int
    sha256: str
    fetched_at_utc_ms: int
    dataset_size_bytes: int
    file_count: int


# ---------------------------------------------------------------------------
# Staging workspace and live-marker helpers
# ---------------------------------------------------------------------------


def _now_utc_ms() -> int:
    return int(time.time() * 1000)


def create_staging_dir(work_dir: Path | str) -> Path:
    """Create a fresh staging directory ``staging-<pid>-<hex16>`` under ``work_dir``.

    ``work_dir`` is created (with parents) if missing. The unique suffix makes
    collisions effectively impossible and keeps concurrent processes disjoint.
    """
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    staging = work_dir / f"{_STAGING_PREFIX}{os.getpid()}-{uuid.uuid4().hex[:16]}"
    staging.mkdir()  # exist_ok=False: a name collision is an error, not silence
    return staging


def _write_guard(staging_dir: Path) -> None:
    """Create or refresh ``guard.json``; never adopt a foreign staging dir.

    The record holds our pid plus UTC-ms started/heartbeat timestamps. An
    existing record written by a *foreign* process is never overwritten:
    refreshing only proceeds when the recorded pid matches ours, which keeps
    this process from marking someone else's staging directory as owned. A
    malformed or partially written record is likewise never adopted — it
    might be another process's interrupted write — so it raises instead of
    being repaired in place.
    """
    guard = staging_dir / _GUARD_NAME
    now = _now_utc_ms()
    our_pid = os.getpid()
    started = now
    if guard.exists():
        try:
            existing = json.loads(guard.read_text(encoding="utf-8"))
            recorded_pid, started, _ = _parse_guard_record(existing)
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise ExtractionError(
                f"unreadable {_GUARD_NAME} in staging {staging_dir.name}: "
                f"{exc.__class__.__name__}: {exc}"
            ) from exc
        if recorded_pid != our_pid:
            raise ExtractionError(
                f"staging {staging_dir.name} belongs to pid {recorded_pid}; "
                f"refusing to mark it as owned by pid {our_pid}"
            )
    payload = {
        "pid": our_pid,
        "started_utc_ms": started,
        "heartbeat_utc_ms": now,
    }
    temp_path = staging_dir / (_GUARD_NAME + ".tmp")
    try:
        # Write-then-replace keeps the record atomic for any concurrent reader.
        temp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, guard)
    except OSError as exc:
        raise ExtractionError(
            f"could not persist {_GUARD_NAME} in staging {staging_dir.name}: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc


def _discard_if_own(staging_dir: Path) -> str | None:
    """Delete ``staging_dir`` only if it verifiably belongs to this process.

    Returns ``None`` on success, otherwise a diagnostic note describing why
    the directory was left in place. The caller attaches that note to the
    raised operation error, so cleanup problems stay visible without being
    silently swallowed.
    """
    guard = staging_dir / _GUARD_NAME
    try:
        record = json.loads(guard.read_text(encoding="utf-8"))
        recorded_pid, _, _ = _parse_guard_record(record)
    except (OSError, KeyError, TypeError, ValueError):
        return (
            f"staging {staging_dir.name} left in place: {_GUARD_NAME} is missing "
            "or unreadable, so ownership cannot be verified"
        )
    if recorded_pid != os.getpid():
        return (
            f"staging {staging_dir.name} left in place: {_GUARD_NAME} records "
            f"pid {recorded_pid}, which is not this process ({os.getpid()})"
        )
    try:
        shutil.rmtree(staging_dir)
    except OSError as exc:
        return (
            f"staging {staging_dir.name} could not be removed: "
            f"{exc.__class__.__name__}: {exc}"
        )
    return None


def _checked_staging_arg(staging_dir: Path | str | None) -> Path:
    """Validate the optional caller-supplied staging directory exists."""
    if staging_dir is None:
        raise ExtractionError("staging_dir must be an existing directory")
    staging = Path(staging_dir)
    if not staging.is_dir():
        raise ExtractionError(
            f"staging {staging.name} does not exist (create it with "
            "create_staging_dir); S2 never silently mkdirs a caller-supplied "
            "staging path"
        )
    return staging


# ---------------------------------------------------------------------------
# Transport mechanics
# ---------------------------------------------------------------------------


def _display_url(url: str) -> str:
    """A URL safe for error messages: scheme, host, path — no userinfo, no query."""
    parts = urllib.parse.urlsplit(url)
    host = parts.netloc.rpartition("@")[2]  # drop any credentials block
    if not parts.scheme:
        raise ExtractionError("feed URL has no scheme")
    return f"{parts.scheme}://{host}{parts.path}"


@contextmanager
def _open_response(url: str, timeout: float) -> Iterator[http.client.HTTPResponse]:
    """Open an HTTP(S) response; urllib failures propagate to the wrapper."""
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        yield response


def _stream_to_file(
    url: str,
    dest_part: Path,
    *,
    max_bytes: int,
    timeout: float,
) -> tuple[int, str]:
    """Stream ``url`` into ``dest_part``; return ``(byte_count, sha256)``.

    Content-Length is honored as a preliminary gate only and verified at the
    end; the authoritative size is the streamed byte count. Size-cap
    violations are detected *before* an oversized chunk is written, so
    nothing beyond the limit ever lands on disk.
    """
    declared_bytes: int | None = None
    received = 0
    digest = hashlib.sha256()
    try:
        with _open_response(url, timeout) as response:
            status = getattr(response, "status", None)
            if status not in (None, 200):
                # Fail before creating any temp file: a rejected request or an
                # already-too-large declared size writes nothing to disk.
                raise ExtractionError(
                    f"unexpected HTTP status {status} from {_display_url(url)}"
                )
            declared = response.headers.get("Content-Length")
            if declared is not None:
                try:
                    declared_bytes = int(declared)
                except ValueError:
                    declared_bytes = None
            if declared_bytes is not None:
                if declared_bytes <= 0:
                    raise ExtractionError(
                        f"feed reports a non-positive Content-Length "
                        f"({declared!r}) from {_display_url(url)}"
                    )
                if declared_bytes > max_bytes:
                    raise ExtractionError(
                        f"feed reports {declared_bytes} bytes, exceeding the "
                        f"configured limit of {max_bytes} bytes "
                        f"from {_display_url(url)}"
                    )
            with dest_part.open("wb") as out:
                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    if received + len(chunk) > max_bytes:
                        raise ExtractionError(
                            f"download exceeds the configured limit of "
                            f"{max_bytes} bytes (stopped after {received} "
                            f"bytes) from {_display_url(url)}"
                        )
                    digest.update(chunk)
                    out.write(chunk)
                    received += len(chunk)
    except urllib.error.HTTPError as exc:
        raise ExtractionError(
            f"feed rejected the request: HTTP {exc.code} {exc.reason} "
            f"from {_display_url(url)}"
        ) from exc
    except (urllib.error.URLError, http.client.IncompleteRead, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        if isinstance(exc, TimeoutError) or isinstance(reason, TimeoutError):
            raise ExtractionError(
                f"connection or read timed out after {timeout} seconds while "
                f"reaching {_display_url(url)}"
            ) from exc
        raise ExtractionError(
            f"downloading {_display_url(url)}: {exc.__class__.__name__}: {exc}"
        ) from exc
    if received == 0:
        raise ExtractionError(f"no bytes received from {_display_url(url)}")
    if declared_bytes is not None and declared_bytes != received:
        raise ExtractionError(
            f"declared {declared_bytes} bytes but received {received} bytes "
            f"from {_display_url(url)} (incomplete or inconsistent download)"
        )
    return received, digest.hexdigest()


def download_zip(
    url: str,
    zip_path: Path | str,
    *,
    max_bytes: int,
    timeout: float = DOWNLOAD_TIMEOUT_SECONDS,
    fetched_at_utc_ms: int | None = None,
) -> DownloadedZip:
    """Download ``url`` into ``zip_path.parent`` as ``<name>.part``, then rename.

    Mechanics core used by :func:`download_to_staging`; it takes an explicit
    URL, destination, and size cap instead of a config object, which keeps
    transport behavior honestly testable against a local HTTP server (real
    socket I/O, real header handling — no mocks). A zero or negative
    ``max_bytes`` is refused: the configured semantics make an explicit zero
    a validation error, never a synonym for "unlimited".

    The caller owns cleanup of the ``.part`` file on failure; the configured
    staging layer (:func:`download_to_staging`, :func:`stage_gtfs`) performs
    its guarded discard, and verified in tests.
    """
    if max_bytes <= 0:
        raise ExtractionError(
            f"max_bytes must be a positive number of bytes (got {max_bytes}); "
            "explicit zero is rejected by config semantics, so 'unlimited' is "
            "not an option"
        )
    zip_path = Path(zip_path)
    part_path = zip_path.with_name(zip_path.name + ".part")
    if zip_path.exists() or part_path.exists():
        raise ExtractionError(
            f"refusing to replace existing {zip_path.name!r} or "
            f"{part_path.name!r} in staging {zip_path.parent.name}"
        )
    fetched_ms = _now_utc_ms() if fetched_at_utc_ms is None else fetched_at_utc_ms
    received, sha256 = _stream_to_file(
        url,
        part_path,
        max_bytes=max_bytes,
        timeout=timeout,
    )
    try:
        os.replace(part_path, zip_path)
    except OSError as exc:
        raise ExtractionError(
            f"could not finalize the downloaded file inside staging "
            f"{zip_path.parent.name}: {exc.__class__.__name__}: {exc}"
        ) from exc
    return DownloadedZip(
        staging_dir=zip_path.parent,
        zip_path=zip_path,
        zip_size_bytes=received,
        sha256=sha256,
        fetched_at_utc_ms=fetched_ms,
    )


def download_to_staging(
    config: GtfsConfig,
    *,
    staging_dir: Path | str | None = None,
) -> DownloadedZip:
    """Download the configured feed into a staging directory under ``work_dir``.

    Creates the staging directory (or verifies the caller-supplied one),
    marks it live with ``guard.json``, streams the configured URL with the
    configured size cap, verifies the streamed byte count and any declared
    Content-Length, then renames ``gtfs.zip.part`` to ``gtfs.zip``. On any
    failure the staging directory is removed only when ``guard.json`` still
    attests it belongs to this process; a foreign or unreadable record is
    never touched, and the reason is attached to the raised error.
    """
    staging = (
        create_staging_dir(config.work_dir)
        if staging_dir is None
        else _checked_staging_arg(staging_dir)
    )
    try:
        _write_guard(staging)
        downloaded = download_zip(
            config.feed_url,
            staging / _ZIP_NAME,
            max_bytes=config.max_zip_bytes,
        )
        _write_guard(staging)
    except BaseException as primary:
        note = _discard_if_own(staging)
        if note is not None:
            setattr(primary, "cleanup_failure", note)
        raise
    return downloaded


# ---------------------------------------------------------------------------
# Safe extraction
# ---------------------------------------------------------------------------

# Refused outright: GTFS member names never contain these, and their presence
# is exactly how crafted archives make extraction escape staging on Windows.
_UNSAFE_NAME_CHARS = ("\\", "\x00", ":")
_ZIP_UNIX_CREATE_SYSTEM = 3
_SYSTEM_FILE_TYPES_TO_REJECT = (
    stat.S_IFLNK,  # symlink: classic escape hatch for arbitrary writes
    stat.S_IFIFO,
    stat.S_IFCHR,
    stat.S_IFBLK,
    stat.S_IFSOCK,
)


def _member_segments(member_name: str) -> tuple[str, ...]:
    """Split a zip member name into safe path segments, or refuse.

    Refused: absolute paths, drive prefixes, NUL or colon anywhere, untrimmed
    leading/trailing spaces (Windows strips those), empty names, and any
    ``.``/``..`` segments. Backslashes need no rule of their own through the
    stdlib reader — ``zipfile`` decodes member names and normalizes ``\\``
    to ``/``, so crafted payloads arrive here in forward-slash form and hit
    exactly these explicit rules (the backslash reserved-char check below is
    defense-in-depth for non-zipfile constructions). Path containment is
    still re-proven by resolution after this (belt and suspenders), but these
    rules give explicit, explainable rejections instead of silent laundering.
    """
    name = member_name
    if name != name.strip(" "):
        raise ExtractionError(f"member name has untrimmed spaces: {name!r}")
    if name.startswith("/") or name.startswith("\\"):
        raise ExtractionError(f"member name is an absolute path: {name!r}")
    if any(ch in name for ch in _UNSAFE_NAME_CHARS):
        raise ExtractionError(
            f"member name contains a reserved character (backslash, NUL, or "
            f"colon): {name!r}"
        )
    segments = [segment for segment in name.split("/") if segment != ""]
    if not segments:
        raise ExtractionError(f"member name is empty: {name!r}")
    if any(segment in (".", "..") for segment in segments):
        raise ExtractionError(f"member name contains an unsafe segment: {name!r}")
    return tuple(segments)


def _entry_kind(info: zipfile.ZipInfo) -> str:
    """Classify an archive member: ``"dir"``, ``"file"``, or ``"unsupported"``.

    Unix-driven archives (``create_system == 3``) carry the file type in the
    high bits of ``external_attr``; symlink/FIFO/device/socket members are
    rejected as unsupported. Plain-file entries with mode==0 (some packers
    emit those) are the tolerated variance and count as files.
    """
    if info.is_dir():
        return "dir"
    if info.create_system == _ZIP_UNIX_CREATE_SYSTEM:
        file_type = stat.S_IFMT(info.external_attr >> 16)
        if file_type in _SYSTEM_FILE_TYPES_TO_REJECT:
            return "unsupported"
    return "file"


def _destination_within(
    root: Path,
    segments: tuple[str, ...],
    staging_display: str,
) -> Path:
    """Resolve the destination and prove it stays inside the staging root.

    Resolution — never string-prefix comparison — is authoritative here:
    after resolving, ``destination.relative_to(root)`` either succeeds or the
    member is rejected, so even a trick that survived earlier rules cannot
    escape staging.
    """
    root_resolved = root.resolve()
    destination = root_resolved.joinpath(*segments).resolve()
    try:
        destination.relative_to(root_resolved)
    except ValueError:
        raise ExtractionError(
            f"archive member would escape staging {staging_display}: "
            f"{segments!r}"
        ) from None
    return destination


def safe_extract(
    zip_path: Path | str,
    destination: Path | str | None = None,
    *,
    required_members: Sequence[str] = (),
    max_member_bytes: int | None = None,
    max_total_bytes: int | None = None,
) -> ExtractionOutcome:
    """Validate and extract a ZIP archive into ``destination``, fully guarded.

    Validation happens in a write-free first phase — before a single byte
    reaches disk — covering: member count, required members, unsafe member
    names, unsupported entry types, encrypted members, per-member declared
    sizes, **duplicate member paths** (case-insensitively; Windows treats
    ``A`` and ``a`` as the same file, so ambiguity is refused outright
    instead of a "first-entry-wins" outcome), and the total declared
    uncompressed size against ``max_total_bytes`` (S2 review fix 1: per-member limits cannot
    bound many individually-acceptable members collectively). The only
    failures after that phase are environment-shaped: a pre-existing file on
    disk and CRC/size mismatches while streaming. Only regular files and
    explicit directory entries are materialized; every regular file is
    created with exclusive mode ("xb"), so a pre-existing file is a hard
    error instead of a silent overwrite. CRC verification is performed
    inside the stdlib reader on every member read; a member whose contents
    exceed its declared size is rejected mid-stream rather than trusted, so
    the actual total can never exceed the declared total that pre-flight
    checked.

    ``required_members`` is compared against the member names exactly as
    they appear in the archive (top-level match). S2 invents no schema
    assumptions; the configured callers pass the requirements that Stage S3
    establishes after inspecting the real feed.
    """
    zip_path = Path(zip_path)
    destination_dir = Path(destination) if destination is not None else zip_path.parent
    destination_dir.mkdir(parents=True, exist_ok=True)
    staging_display = destination_dir.name
    try:
        archive = zipfile.ZipFile(zip_path)
    except FileNotFoundError as exc:
        raise ExtractionError(
            f"zip {zip_path.name!r} not found in staging {staging_display}"
        ) from exc
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError) as exc:
        raise CorruptZipError(
            f"{zip_path.name!r} is not a readable zip archive: {exc!r}"
        ) from exc
    try:
        return _extract_within(
            archive,
            destination_dir,
            staging_display=staging_display,
            required_members=required_members,
            max_member_bytes=max_member_bytes,
            max_total_bytes=max_total_bytes,
        )
    finally:
        archive.close()


def _extract_within(
    archive: zipfile.ZipFile,
    destination_dir: Path,
    *,
    staging_display: str,
    required_members: Sequence[str],
    max_member_bytes: int | None,
    max_total_bytes: int | None,
) -> ExtractionOutcome:
    """Phase 1: validate a plan with zero writes. Phase 2: materialize it."""
    infos = archive.infolist()
    if len(infos) > MAX_ARCHIVE_MEMBERS:
        raise ExtractionError(
            f"archive has {len(infos)} members, exceeding the limit of "
            f"{MAX_ARCHIVE_MEMBERS}; refusing to extract"
        )
    declared_names = {info.filename for info in infos}
    missing = sorted(set(required_members) - declared_names)
    if missing:
        raise MissingRequiredFileError(
            "archive is missing required GTFS member(s): " + ", ".join(missing),
            members=tuple(missing),
        )
    root = destination_dir.resolve()

    # ---- Phase 1: full structural validation, no filesystem writes ----
    plan: list[tuple[zipfile.ZipInfo, str, Path]] = []
    seen_members: set[str] = set()
    declared_total = 0
    for info in infos:
        member_name = info.filename
        segments = _member_segments(member_name)
        folded = "/".join(segments).casefold()
        if folded in seen_members:
            raise ExtractionError(
                f"duplicate archive member (case-insensitive): {member_name!r} "
                "(ambiguous contents rejected before anything is extracted)"
            )
        seen_members.add(folded)
        kind = _entry_kind(info)
        if kind == "unsupported":
            raise ExtractionError(
                f"archive member {member_name!r} is not a regular file or a "
                "directory (symlink/device-like entries are refused as "
                "unsafe)"
            )
        if info.flag_bits & 0x1:
            raise ExtractionError(
                f"archive member {member_name!r} is encrypted; encrypted "
                "archives cannot be integrity-verified and are refused"
            )
        if kind == "file":
            if max_member_bytes is not None and info.file_size > max_member_bytes:
                raise ExtractionError(
                    f"archive member {member_name!r} declares "
                    f"{info.file_size} bytes, exceeding the configured limit "
                    f"of {max_member_bytes} bytes"
                )
            declared_total += info.file_size
        plan.append((info, kind, _destination_within(root, segments, staging_display)))
    if max_total_bytes is not None and declared_total > max_total_bytes:
        raise ExtractionError(
            f"archive members declare {declared_total} uncompressed bytes in "
            f"total, exceeding the configured dataset limit of "
            f"{max_total_bytes} bytes; refusing to extract"
        )

    # ---- Phase 2: materialize the validated plan ----
    file_count = 0
    size_total = 0
    for info, kind, dest in plan:
        if kind == "dir":
            try:
                dest.mkdir(parents=True, exist_ok=True)
            except FileExistsError as exc:
                raise ExtractionError(
                    f"a regular file already occupies the archive directory "
                    f"path {info.filename!r} in staging {staging_display}"
                ) from exc
            continue
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
        except FileExistsError as exc:
            raise ExtractionError(
                f"a regular file blocks the parent directory needed by "
                f"archive member {info.filename!r} in staging "
                f"{staging_display}"
            ) from exc
        written = _copy_member(
            archive,
            info,
            dest,
            member_name=info.filename,
            staging_display=staging_display,
        )
        file_count += 1
        size_total += written
    return ExtractionOutcome(
        dataset_path=destination_dir,
        dataset_size_bytes=size_total,
        file_count=file_count,
    )


def _copy_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    dest: Path,
    *,
    member_name: str,
    staging_display: str,
) -> int:
    """Stream one regular-file member into ``dest`` with hard size/CRC guards.

    Returns the number of bytes actually written (the on-disk regular-file
    size, which is what ``dataset_size_bytes`` documents). The destination is
    opened exclusively ("xb"): a pre-existing file is the unexpected-file
    case, refusing to overwrite instead of silently replacing it.
    """
    try:
        out = dest.open("xb")
    except FileExistsError as exc:
        raise ExtractionError(
            f"refusing to overwrite unexpected file {member_name!r} already "
            f"present in staging {staging_display}"
        ) from exc
    written = 0
    declared = info.file_size
    try:
        with out, archive.open(info) as member_stream:
            while True:
                chunk = member_stream.read(CHUNK_SIZE)
                if not chunk:
                    break
                if written + len(chunk) > declared:
                    raise CorruptZipError(
                        f"archive member {member_name!r} in staging "
                        f"{staging_display} declares {declared} bytes but "
                        "contains more; treated as corrupt rather than "
                        "trusted"
                    )
                written += len(chunk)
                out.write(chunk)
    except GtfsLoadError:
        raise
    except (zipfile.BadZipFile, zlib.error) as exc:
        raise CorruptZipError(
            f"archive member {member_name!r} failed integrity validation: "
            f"{exc!r}"
        ) from exc
    except NotImplementedError as exc:
        raise ExtractionError(
            f"archive member {member_name!r} uses an unsupported compression "
            f"method: {exc!r}"
        ) from exc
    except OSError as exc:
        raise ExtractionError(
            f"could not read archive member {member_name!r} in staging "
            f"{staging_display}: {exc!r}"
        ) from exc
    return written


# ---------------------------------------------------------------------------
# Configured orchestrator
# ---------------------------------------------------------------------------


def stage_gtfs(
    config: GtfsConfig,
    *,
    staging_dir: Path | str | None = None,
    required_members: Sequence[str] = (),
) -> StagedZip:
    """Download and safely extract the configured feed into a staging directory.

    Returns :class:`StagedZip` with the metadata later stages need. On any
    terminal failure — including ``KeyboardInterrupt`` — the staging
    directory created for this operation is removed *only if* ``guard.json``
    still attests to our pid (foreign or unverifiable staging is never
    touched); cleanup problems are attached to the error as a
    ``cleanup_failure`` attribute so they are impossible to miss. Both
    download and extraction limits come from ``config`` (``max_zip_bytes``
    for the compressed archive and each member; ``max_dataset_bytes`` for
    the total uncompressed corpus).
    ``required_members`` is a pass-through
    for archive requirements established later (empty by default).

    ``manifest.json`` is deliberately not written here: promotion (suffix
    ladder, ``promote.lock``, manifest persistence with fsync before rename)
    is Stage S6, and this stage must not introduce any of it prematurely.
    """
    staging = (
        create_staging_dir(config.work_dir)
        if staging_dir is None
        else _checked_staging_arg(staging_dir)
    )
    try:
        _write_guard(staging)
        downloaded = download_zip(
            config.feed_url,
            staging / _ZIP_NAME,
            max_bytes=config.max_zip_bytes,
        )
        outcome = safe_extract(
            downloaded.zip_path,
            destination=staging,
            required_members=required_members,
            max_member_bytes=config.max_zip_bytes,
            max_total_bytes=config.max_dataset_bytes,
        )
        _write_guard(staging)
    except BaseException as primary:
        note = _discard_if_own(staging)
        if note is not None:
            setattr(primary, "cleanup_failure", note)
        raise
    return StagedZip(
        staging_dir=staging,
        zip_path=downloaded.zip_path,
        zip_size_bytes=downloaded.zip_size_bytes,
        sha256=downloaded.sha256,
        fetched_at_utc_ms=downloaded.fetched_at_utc_ms,
        dataset_size_bytes=outcome.dataset_size_bytes,
        file_count=outcome.file_count,
    )
