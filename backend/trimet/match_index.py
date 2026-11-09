"""S11 static-context preparation: shared builder + read-only factory.

Stage S11 (M2 plan §5; implementation prompt §4).  Two canonical entry
points, one validation path:

- :func:`build_matching_context` — the **one shared pure consistency
  builder**.  Owner of every cross-table check: unique route/trip ids caught
  *before* any dictionary construction (a dict constructor must never
  silently erase a duplicate first), nonblank identifier types, orphan
  trip→route references, complete route-mode metadata, MAX route/trip
  agreement with the immutable M1 network and the authoritative imported
  ``MAX_ROUTE_IDS``, and valid trip-local stop-sequence indexes.  Synthetic
  tests call exactly this builder with small in-memory rows and a clearly
  synthetic typed identity — there is no unchecked test-only constructor.

- :func:`prepare_matching_context` — the production factory performing the
  file/archive/manifest lineage checks a bare builder cannot: it validates
  the promoted directory from the successful M1 :class:`LoadOutcome`
  (never a guessed bare network/directory pair, never scanning for the
  newest directory), reuses ``read_manifest`` for strict shape validation,
  cross-checks the recorded manifest paths against that directory, streams
  the archive's SHA-256 via the reuse-only M1 helper ``_zip_sha256``, reads
  ``routes.txt``/``trips.txt`` directly from the verified zip (stdlib
  ZIP/text/CSV tools; never divergent extracted copies; no extraction), and
  re-verifies identity + digest again before publishing the finished
  context.  Building is private: on any failure an exception is raised and
  no partial context escapes.

Preparation is strictly read-only: it never calls ``DatasetManager.load()``
(that method downloads/promotes), never writes a file, and never mutates
``DatasetManager.current``.  Per-record matching afterwards is purely
lookup-based — no zip/CSV rescans, no clock reads.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path

from backend.gtfs.promotion import (
    MANIFEST_NAME,
    ManifestAbsent,
    ManifestUnreadableError,
    read_manifest,
)
from backend.gtfs.promotion import _zip_sha256  # reuse-only, read-only helper
from backend.gtfs.types import (
    MaxNetwork,
    ManifestValid,
    TripStopEntry,
)
from backend.trimet.dataset import LoadOutcome
from backend.trimet.maxfilter import MAX_ROUTE_IDS
from backend.trimet.match_errors import ContextPreparationError
from backend.trimet.match_types import (
    DatasetIdentity,
    MatchingContext,
    RouteRow,
    TripRow,
)

__all__ = [
    "build_matching_context",
    "prepare_matching_context",
]

ZIP_NAME = "gtfs.zip"

_ROUTES_MEMBER = "routes.txt"
_TRIPS_MEMBER = "trips.txt"

REQUIRED_ROUTE_COLUMNS: tuple[str, ...] = ("route_id", "route_type")
REQUIRED_TRIP_COLUMNS: tuple[str, ...] = (
    "trip_id",
    "route_id",
    "direction_id",
    "shape_id",
)


# ---------------------------------------------------------------------------
# Shared pure consistency builder
# ---------------------------------------------------------------------------


def _require_sequence(rows: object, row_type: type, name: str) -> Sequence:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise TypeError(f"{name} must be a sequence of {row_type.__name__}")
    items = tuple(rows)
    for item in items:
        if not isinstance(item, row_type):
            raise TypeError(
                f"{name} must contain only {row_type.__name__} entries, "
                f"got {type(item).__name__}"
            )
    return items


def _require_nonblank_id(value: object, field: str, source: str) -> str:
    if not isinstance(value, str) or value == "":
        raise ContextPreparationError(
            f"{source}: {field} must be a nonblank string",
            code="index_inconsistency",
        )
    return value


def build_matching_context(
    identity: DatasetIdentity,
    network: MaxNetwork,
    route_rows: Sequence[RouteRow],
    trip_rows: Sequence[TripRow],
) -> MatchingContext:
    """Build the immutable matching context; reject any inconsistency whole.

    This is the single canonical consistency path shared by the production
    factory and the synthetic tests (plan §5.2).  It snapshots both input
    sequences up front so later caller mutation cannot affect the build, and
    validates duplicates in the sequences before any mapping collapses them.
    """
    if not isinstance(identity, DatasetIdentity):
        raise TypeError(
            "identity must be a DatasetIdentity, "
            f"got {type(identity).__name__}"
        )
    if not isinstance(network, MaxNetwork):
        raise TypeError(f"network must be a MaxNetwork, got {type(network).__name__}")
    routes = _require_sequence(route_rows, RouteRow, "route_rows")
    trips = _require_sequence(trip_rows, TripRow, "trip_rows")

    # --- Routes: nonblank ids, complete mode metadata, no duplicates ---
    route_types: dict[str, int] = {}
    for row in routes:
        route_id = _require_nonblank_id(row.route_id, "route_id", "routes.txt row")
        route_type = row.route_type
        if (
            isinstance(route_type, bool)
            or not isinstance(route_type, int)
            or route_type < 0
        ):
            raise ContextPreparationError(
                f"route {route_id} route_type must be a non-negative integer",
                code="index_inconsistency",
            )
        if route_id in route_types:
            raise ContextPreparationError(
                f"duplicate routes.txt route_id {route_id!r}",
                code="index_inconsistency",
            )
        route_types[route_id] = route_type

    # --- Trips: nonblank ids, no duplicates, no orphan route references ---
    trip_to_route: dict[str, str] = {}
    for row in trips:
        trip_id = _require_nonblank_id(row.trip_id, "trip_id", "trips.txt row")
        route_id = _require_nonblank_id(row.route_id, "route_id", "trips.txt row")
        direction_id = row.direction_id
        if direction_id is not None and (
            isinstance(direction_id, bool) or not isinstance(direction_id, int)
        ):
            raise ContextPreparationError(
                f"trip {trip_id} direction_id must be an integer or None",
                code="index_inconsistency",
            )
        shape_id = row.shape_id
        if shape_id is not None and not isinstance(shape_id, str):
            raise ContextPreparationError(
                f"trip {trip_id} shape_id must be a string or None",
                code="index_inconsistency",
            )
        if trip_id in trip_to_route:
            raise ContextPreparationError(
                f"duplicate trips.txt trip_id {trip_id!r}",
                code="index_inconsistency",
            )
        if route_id not in route_types:
            raise ContextPreparationError(
                f"trip {trip_id} references unknown route {route_id!r} "
                "(orphan trip->route reference)",
                code="index_inconsistency",
            )
        trip_to_route[trip_id] = route_id

    # --- MAX agreement: route set, trip set, and source-row metadata ---
    network_route_ids = set(network.routes)
    if not network_route_ids <= set(MAX_ROUTE_IDS):
        raise ContextPreparationError(
            "network carries route id(s) outside the approved MAX whitelist",
            code="index_inconsistency",
        )
    index_max_routes = {rid for rid in route_types if rid in MAX_ROUTE_IDS}
    if index_max_routes != network_route_ids:
        raise ContextPreparationError(
            "prepared MAX route set disagrees with the network routes table",
            code="index_inconsistency",
        )
    network_trip_ids = set(network.trips)
    row_max_trip_ids = {
        tid for tid, rid in trip_to_route.items() if rid in MAX_ROUTE_IDS
    }
    if row_max_trip_ids != network_trip_ids:
        raise ContextPreparationError(
            "prepared MAX trip set disagrees with the network trips table "
            "(missing or extra MAX trips)",
            code="index_inconsistency",
        )
    for trip_id in network_trip_ids:
        row = _trip_row_by_id(trips, trip_id)
        static = network.trips[trip_id]
        if row.route_id != static.route_id:
            raise ContextPreparationError(
                f"MAX trip {trip_id} route disagrees with the network trips "
                "table",
                code="index_inconsistency",
            )
        if row.direction_id != static.direction_id:
            raise ContextPreparationError(
                f"MAX trip {trip_id} direction_id disagrees with the network "
                "trips table",
                code="index_inconsistency",
            )
        if row.shape_id != static.shape_id:
            raise ContextPreparationError(
                f"MAX trip {trip_id} shape_id disagrees with the network "
                "trips table",
                code="index_inconsistency",
            )

    return _build_context(
        identity=identity,
        network=network,
        trip_to_route=trip_to_route,
        route_types=route_types,
    )


def _trip_row_by_id(trips: tuple, trip_id: str) -> TripRow:
    for row in trips:
        if row.trip_id == trip_id:
            return row
    raise ContextPreparationError(  # pragma: no cover - set check above guards this
        f"MAX trip {trip_id} is absent from the trip rows",
        code="index_inconsistency",
    )


def _build_context(
    *,
    identity: DatasetIdentity,
    network: MaxNetwork,
    trip_to_route: dict[str, str],
    route_types: dict[str, int],
) -> MatchingContext:
    """Assemble the final read-only context (defensive visit checks inside)."""
    # Trips may reference their route rows only through validated ids from
    # here on; the trip-index lookups above already rejected duplicates and
    # orphans.  Defensive stop-index validation follows.
    for trip_id in network.stop_times:
        if trip_id not in network.trips:
            raise ContextPreparationError(
                "network stop_times contains a trip outside the MAX trips "
                "table",
                code="index_inconsistency",
            )
    visits_by_sequence: dict[str, dict[int, TripStopEntry]] = {}
    visits_by_stop: dict[str, dict[str, tuple[TripStopEntry, ...]]] = {}
    # Every indexed MAX trip carries trip-local indexes — M1's
    # parse_stop_times_filtered constructs by_trip only from rows seen, so a
    # known MAX trip may legitimately have no stop-time rows.  Such trips
    # get empty visit indexes instead of aborting later matching (a
    # requested stop selector then resolves unresolved/partial with the
    # existing reason codes; the whole context is never rejected for an
    # M1-tolerated visitless trip).
    for trip_id in network.trips:
        visits_by_sequence[trip_id] = {}
        visits_by_stop[trip_id] = {}
    for trip_id, entries in network.stop_times.items():
        per_sequence: dict[int, TripStopEntry] = {}
        per_stop: dict[str, list[TripStopEntry]] = {}
        for entry in entries:
            if not isinstance(entry, TripStopEntry):
                raise ContextPreparationError(
                    f"trip {trip_id} carries an invalid stop-time entry",
                    code="index_inconsistency",
                )
            sequence = entry.stop_sequence
            if isinstance(sequence, bool) or not isinstance(sequence, int):
                raise ContextPreparationError(
                    f"trip {trip_id} has a non-integer stop_sequence",
                    code="index_inconsistency",
                )
            _require_nonblank_id(entry.stop_id, "stop_id", f"trip {trip_id}")
            if sequence in per_sequence:
                raise ContextPreparationError(
                    f"trip {trip_id} has duplicate stop_sequence {sequence}",
                    code="index_inconsistency",
                )
            per_sequence[sequence] = entry
            per_stop.setdefault(entry.stop_id, []).append(entry)
        visits_by_sequence[trip_id] = per_sequence
        visits_by_stop[trip_id] = {sid: tuple(v) for sid, v in per_stop.items()}
    return MatchingContext(
        identity=identity,
        network=network,
        trip_to_route=trip_to_route,
        known_route_ids=frozenset(route_types),
        route_types=route_types,
        max_route_ids=frozenset(MAX_ROUTE_IDS),
        visits_by_sequence=visits_by_sequence,
        visits_by_stop=visits_by_stop,
    )


# ---------------------------------------------------------------------------
# Production factory (factory-only lineage and CSV syntax checks)
# ---------------------------------------------------------------------------


def prepare_matching_context(load_outcome: LoadOutcome) -> MatchingContext:
    """Prepare a provenance-checked context from a successful M1 load.

    Never a guessed bare network/directory pair: the network and the
    promoted directory travel together inside the successful M1
    :class:`~backend.trimet.dataset.LoadOutcome`.  No partial context is
    published on any failure.
    """
    if not isinstance(load_outcome, LoadOutcome):
        raise TypeError(
            "load_outcome must be a LoadOutcome, "
            f"got {type(load_outcome).__name__}"
        )
    dataset_dir = Path(load_outcome.promotion.dataset_dir)

    recorded_sha, record = _read_identity(dataset_dir)
    zip_path = dataset_dir / ZIP_NAME
    _verify_manifest_paths(record, dataset_dir, zip_path)
    if record["feed_version"] != load_outcome.network.feed_version:
        raise ContextPreparationError(
            "manifest feed_version disagrees with the loaded network",
            code="identity_mismatch",
        )
    _verify_archive_digest(zip_path, recorded_sha)

    route_rows, trip_rows = _rows_from_archive(zip_path)

    identity = DatasetIdentity(
        archive_sha256=recorded_sha,
        feed_version=record["feed_version"],  # type: ignore[index]
        source_directory_name=dataset_dir.name,
    )
    context = build_matching_context(
        identity, load_outcome.network, route_rows, trip_rows
    )

    # Re-verify identity and archive digest before returning: retention or a
    # concurrent replacement can change or remove the source while
    # preparation runs; an already prepared context has no further file
    # dependency.
    _recheck_identity(dataset_dir, record["feed_version"], recorded_sha)  # type: ignore[arg-type]
    return context


def _read_identity(dataset_dir: Path) -> tuple[str, dict]:
    """Read + strictly validate the manifest; return (sha256, full record)."""
    try:
        manifest = read_manifest(dataset_dir)
    except ManifestUnreadableError as exc:
        raise ContextPreparationError(
            f"manifest.json in {dataset_dir.name} is unreadable: {exc.message}",
            code="manifest_unreadable",
        ) from exc
    if isinstance(manifest, ManifestAbsent):
        raise ContextPreparationError(
            f"dataset directory {dataset_dir.name} has no manifest.json",
            code="manifest_absent",
        )
    if not isinstance(manifest, ManifestValid):  # pragma: no cover - narrowing
        raise ContextPreparationError(
            f"manifest.json in {dataset_dir.name} is unreadable",
            code="manifest_unreadable",
        )
    try:
        record = json.loads((dataset_dir / MANIFEST_NAME).read_text("utf-8"))
    except (OSError, ValueError) as exc:
        raise ContextPreparationError(
            f"manifest.json in {dataset_dir.name} is unreadable "
            f"({exc.__class__.__name__})",
            code="manifest_unreadable",
        ) from exc
    feed_version = record.get("feed_version")
    if not isinstance(feed_version, str) or not feed_version:
        raise ContextPreparationError(
            "manifest.json has no usable feed_version",
            code="manifest_unreadable",
        )
    return manifest.sha256, record


def _verify_manifest_paths(record: Mapping, dataset_dir: Path, zip_path: Path) -> None:
    """Recorded manifest paths must identify *this* directory and gtfs.zip."""
    recorded_zip = record.get("zip_path")
    recorded_dataset = record.get("dataset_path")
    if recorded_zip != str(zip_path) or recorded_dataset != str(dataset_dir):
        raise ContextPreparationError(
            "manifest.json recorded paths do not identify this promoted "
            "dataset directory and its gtfs.zip",
            code="manifest_paths_mismatch",
        )


def _verify_archive_digest(zip_path: Path, expected_sha256: str) -> None:
    """Stream-hash the existing archive against the manifest digest."""
    try:
        actual = _zip_sha256(zip_path)
    except Exception as exc:
        raise ContextPreparationError(
            f"gtfs.zip in {zip_path.parent.name} is missing or unreadable "
            f"({exc.__class__.__name__})",
            code="archive_unreadable",
        ) from exc
    if actual != expected_sha256:
        raise ContextPreparationError(
            "gtfs.zip does not hash to the manifest digest",
            code="archive_hash_mismatch",
        )


def _recheck_identity(dataset_dir: Path, feed_version: str, recorded_sha: str) -> None:
    """Re-read identity and re-verify the digest just before publishing."""
    sha, record = _read_identity(dataset_dir)
    if sha != recorded_sha or record["feed_version"] != feed_version:
        raise ContextPreparationError(
            "dataset identity changed during preparation",
            code="identity_mismatch",
        )
    _verify_archive_digest(dataset_dir / ZIP_NAME, recorded_sha)


def _rows_from_archive(zip_path: Path) -> tuple[tuple[RouteRow, ...], tuple[TripRow, ...]]:
    """Read and normalize routes.txt/trips.txt from the verified archive."""
    try:
        with zipfile.ZipFile(zip_path) as archive:
            route_cells = list(_member_records(archive, _ROUTES_MEMBER))
            trip_cells = list(_member_records(archive, _TRIPS_MEMBER))
    except zipfile.BadZipFile as exc:
        raise ContextPreparationError(
            f"gtfs.zip in {zip_path.parent.name} is unreadable (BadZipFile)",
            code="archive_unreadable",
        ) from exc
    except OSError as exc:
        raise ContextPreparationError(
            f"gtfs.zip in {zip_path.parent.name} is missing or unreadable "
            f"({exc.__class__.__name__})",
            code="archive_unreadable",
        ) from exc
    return (
        _normalize_route_rows(route_cells),
        _normalize_trip_rows(trip_cells),
    )


def _member_records(
    archive: zipfile.ZipFile, member: str
) -> tuple[tuple[str, ...], list[tuple[int, list[str]]], int]:
    """Validate the member, its header, and every nonblank record's width.

    Returns ``(header_columns, rows_as_lists, blank_lines_skipped)`` so the
    callers can re-index by already-validated normalized column names.  Any
    structural deviation — missing or duplicate members, a blank/duplicate/
    inapplicable header, short or long records (**including** extra trailing
    empty cells), undecodable text, or unrecoverable quoting — fails the
    whole preparation; no potentially divergent extracted copies are
    consulted.
    """
    matches = [
        name for name in archive.namelist()
        if not name.endswith("/") and name == member
    ]
    if len(matches) == 0:
        raise ContextPreparationError(
            f"{member} is absent from the verified gtfs.zip",
            code="source_member_missing",
        )
    if len(matches) > 1:
        raise ContextPreparationError(
            f"{member} appears {len(matches)} times in the verified gtfs.zip",
            code="source_member_ambiguous",
        )
    try:
        text_stream = io.TextIOWrapper(
            archive.open(matches[0], "r"),
            encoding="utf-8-sig",
            newline="",
        )
        reader = csv.reader(text_stream, strict=True)
        header: list[str] | None = None
        skipped = 0
        rows: list[tuple[int, list[str]]] = []
        try:
            for row in reader:
                line_number = reader.line_num
                if not header:
                    if not row:
                        skipped += 1
                        continue
                    header = _validated_header(row, member)
                    continue
                if not row:
                    skipped += 1
                    continue
                if len(row) != len(header):
                    raise ContextPreparationError(
                        f"{member} line {line_number}: expected "
                        f"{len(header)} fields, found {len(row)}",
                        code="csv_structure",
                    )
                rows.append((line_number, row))
            if header is None:
                raise ContextPreparationError(
                    f"{member} is empty (no header row)",
                    code="csv_structure",
                )
        finally:
            text_stream.detach()
    except UnicodeDecodeError as exc:
        raise ContextPreparationError(
            f"{member} is not valid UTF-8",
            code="csv_structure",
        ) from exc
    except csv.Error as exc:
        # csv.Error is deliberately NOT a ValueError subclass and escapes the
        # generic handler below: malformed quoting (an unterminated quoted
        # field, or data immediately after a closing quote) must still honor
        # the typed preparation-failure contract.  csv.Error messages are
        # short fixed phrases (never row content), so the detail is bounded
        # and payload-free.
        raise ContextPreparationError(
            f"{member} line {reader.line_num}: unrecoverable CSV quoting error "
            f"({exc})",
            code="csv_structure",
        ) from exc
    except zipfile.BadZipFile as exc:
        raise ContextPreparationError(
            f"{member} in gtfs.zip is unreadable (BadZipFile)",
            code="archive_unreadable",
        ) from exc
    except (OSError, EOFError, ValueError) as exc:
        raise ContextPreparationError(
            f"{member} in gtfs.zip is unreadable ({exc.__class__.__name__})",
            code="archive_unreadable",
        ) from exc
    return tuple(header) if header is not None else (), rows, skipped  # type: ignore[return-value]


def _validated_header(raw_row: Sequence[str], member: str) -> tuple[str, ...]:
    """Normalize + validate a header row exactly as csvio does."""
    columns = tuple(column.strip().lower() for column in raw_row)
    empty_positions = [
        str(index + 1) for index, column in enumerate(columns) if column == ""
    ]
    if empty_positions:
        raise ContextPreparationError(
            f"{member} header has empty column name(s) at position(s) "
            f"{', '.join(empty_positions)}",
            code="csv_structure",
        )
    seen: set[str] = set()
    for column in columns:
        if column in seen:
            raise ContextPreparationError(
                f"{member} header has duplicate column(s): {column!r}",
                code="csv_structure",
            )
        seen.add(column)
    required = (
        REQUIRED_ROUTE_COLUMNS if member == _ROUTES_MEMBER else REQUIRED_TRIP_COLUMNS
    )
    missing = sorted(set(required) - seen)
    if missing:
        raise ContextPreparationError(
            f"{member} header is missing required column(s): {', '.join(missing)}",
            code="csv_structure",
        )
    return columns


# --- Normalization into immutable shared-builder rows -----------------------


def _cell(record: Mapping[str, str], column: str) -> str | None:
    """``get_str`` semantics: empty cell → None, otherwise verbatim."""
    value = record.get(column)
    if value is None or value == "":
        return None
    return value


def _int_cell(
    record: Mapping[str, str],
    column: str,
    member: str,
    line_number: int,
    *,
    required: bool,
) -> int | None:
    """``get_int`` semantics: blank/whitespace-only → None (or required error)."""
    raw = record.get(column)
    if raw is None or raw.strip() == "":
        if required:
            raise ContextPreparationError(
                f"{member} line {line_number}: required column {column!r} is "
                "blank",
                code="index_inconsistency",
            )
        return None
    try:
        return int(raw)
    except ValueError:
        raise ContextPreparationError(
            f"{member} line {line_number}: column {column!r} has neither an "
            "integer nor a blank value",
            code="invalid_cell",
        ) from None


def _normalize_route_rows(
    member_result: tuple[tuple[str, ...], list[tuple[int, list[str]]], int],
) -> tuple[RouteRow, ...]:
    header, rows, _blank = member_result
    normalized: list[RouteRow] = []
    for line_number, cells in rows:
        record = dict(zip(header, cells))
        route_id = _cell(record, "route_id")
        if route_id is None:
            raise ContextPreparationError(
                f"routes.txt line {line_number}: blank required route_id",
                code="index_inconsistency",
            )
        route_type = _int_cell(
            record, "route_type", _ROUTES_MEMBER, line_number, required=True
        )
        normalized.append(RouteRow(route_id=route_id, route_type=route_type))
    return tuple(normalized)


def _normalize_trip_rows(
    member_result: tuple[tuple[str, ...], list[tuple[int, list[str]]], int],
) -> tuple[TripRow, ...]:
    header, rows, _blank = member_result
    normalized: list[TripRow] = []
    for line_number, cells in rows:
        record = dict(zip(header, cells))
        trip_id = _cell(record, "trip_id")
        route_id = _cell(record, "route_id")
        if trip_id is None:
            raise ContextPreparationError(
                f"trips.txt line {line_number}: blank required trip_id",
                code="index_inconsistency",
            )
        if route_id is None:
            raise ContextPreparationError(
                f"trips.txt line {line_number}: blank required route_id",
                code="index_inconsistency",
            )
        direction_id: int | None = None
        shape_id: str | None = None
        if route_id in MAX_ROUTE_IDS:
            direction_id = _int_cell(
                record, "direction_id", _TRIPS_MEMBER, line_number, required=False
            )
            shape_id = _cell(record, "shape_id")
        normalized.append(
            TripRow(
                trip_id=trip_id,
                route_id=route_id,
                direction_id=direction_id,
                shape_id=shape_id,
            )
        )
    return tuple(normalized)
