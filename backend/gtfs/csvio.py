"""Reusable GTFS CSV reading primitives (Stage S1, M1 plan Rev. 9 §3).

What belongs here: reusable CSV mechanics only — encoding tolerance (UTF-8
with or without a leading BOM), header normalisation and validation, streaming
record iteration with file line numbers, and row-problem collection so nothing
is silently discarded. Downloading files is Stage S2; GTFS table parsing is
Stage S4 — parsers express per-table tolerance on top of these primitives.

Design rules, per the plan:

- Field values stay raw strings. Cell conversion happens only through the
  explicit typed getters (which raise :class:`CellParseError` on a single bad
  cell and treat blank cells as absent), so S4 parsers control tolerance per
  column rather than this layer silently guessing.
- Structural failures raise :class:`SchemaViolationError` immediately: an
  empty file, a blank header row, duplicate header columns, missing
  caller-required columns, invalid UTF-8, or unrecoverable CSV quoting
  errors. Decoding cannot resync after such corruption.
- Rows with too few or too many fields are **still yielded** (fields that
  map are mapped) and additionally recorded as :class:`RowProblem`s on the
  report — the S4 parsers apply the plan's row tolerance (strict for MAX
  rows, capped budget for unrelated rows).
- Truly blank lines are skipped but counted on the report, so the day a
  malformed file hides data has a visible trace.
- Error context is limited to file name, line number, and column name; no
  environment data, credential material, or absolute machine paths are
  embedded.

File-handle and line-number notes: :func:`open_gtfs_csv` opens the file
inside a ``with`` block (the handle closes on normal exit and on every
exception path), and record line numbers are the *last* physical line of
each CSV record (multi-line quoted fields report their end line), matching
``csv.reader.line_num`` semantics.
"""

from __future__ import annotations

import csv
import math
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from backend.gtfs.errors import CellParseError, SchemaViolationError

__all__ = [
    "CsvReport",
    "CsvRowProblemKind",
    "GtfsCsvFile",
    "GtfsHeader",
    "RowProblem",
    "get_float",
    "get_int",
    "get_str",
    "get_time_secs",
    "open_gtfs_csv",
]

_TIME_PATTERN = re.compile(r"^([0-9]{1,4}):([0-9]{2}):([0-9]{2})$")
_MAX_ROW_PREVIEW_LENGTH = 160


def _row_preview(row: Sequence[str]) -> str:
    """Bounded preview of a raw row for row-problem diagnostics.

    Review fix 3D: previews are provably bounded, so a huge row cannot
    balloon error text, and context never carries absolute machine paths
    (``open_gtfs_csv`` passes ``Path.name`` only).
    """
    text = repr(row)
    if len(text) <= _MAX_ROW_PREVIEW_LENGTH:
        return text
    return text[: _MAX_ROW_PREVIEW_LENGTH - 3] + "..."


@dataclass(frozen=True, slots=True)
class GtfsHeader:
    """A validated, normalised CSV header row."""

    columns: tuple[str, ...]
    line_number: int

    def has(self, column: str) -> bool:
        """Whether the header defines this column (compared case-insensitively
        on trimmed names, matching the header normalisation)."""
        return column.strip().lower() in self.columns


class CsvRowProblemKind(Enum):
    """Why one record could not be mapped fully onto the header."""

    SHORT_ROW = "short_row"
    EXTRA_FIELDS = "extra_fields"


@dataclass(frozen=True)
class RowProblem:
    """One malformed row, recorded on the report instead of discarded.

    ``line_number`` is the last physical line of the record; ``detail`` is a
    compact human-readable explanation (including a raw-row preview).
    """

    line_number: int
    kind: CsvRowProblemKind
    detail: str


@dataclass
class CsvReport:
    """Row-problem accumulator for one open file.

    Mutable by design: iteration appends findings. ``blank_lines_skipped``
    keeps the documented blank-line skipping visible (plan: no silent
    discarding).
    """

    problems: list[RowProblem] = field(default_factory=list)
    blank_lines_skipped: int = 0

    def counts_by_kind(self) -> dict[CsvRowProblemKind, int]:
        """Count problems per kind (used by S4/S5 tolerance budgets)."""
        counts: dict[CsvRowProblemKind, int] = {}
        for problem in self.problems:
            counts[problem.kind] = counts.get(problem.kind, 0) + 1
        return counts


class GtfsCsvFile:
    """Validated header, report, and streaming record iterator for one file.

    Create via :func:`open_gtfs_csv`; the returned context manager owns the
    file handle, so consumers never close anything directly.
    """

    def __init__(
        self,
        *,
        path: Path,
        header: GtfsHeader,
        report: CsvReport,
        reader: Iterator[list[str]],
    ) -> None:
        self._path = path
        self.header = header
        self.report = report
        self._reader = reader

    @property
    def path(self) -> Path:
        return self._path

    def iter_records(self) -> Iterator[tuple[int, dict[str, str]]]:
        """Yield ``(file_line_number, {column: value})`` pairs, streaming.

        The record maps the validated header onto the raw row. Short rows
        simply leave trailing columns absent from the mapping; extra fields
        are not mapped. Both are recorded as :class:`RowProblem`s with a raw
        preview, so no information is silently lost. Record line numbers are
        the last physical line of the CSV record.
        """
        width = len(self.header.columns)
        try:
            for row in self._reader:
                line_number = self._reader.line_num
                if not row:
                    self.report.blank_lines_skipped += 1
                    continue
                if len(row) < width:
                    self.report.problems.append(RowProblem(
                        line_number=line_number,
                        kind=CsvRowProblemKind.SHORT_ROW,
                        detail=(
                            f"expected {width} fields, found {len(row)}; "
                            f"row {_row_preview(row)}"
                        ),
                    ))
                elif len(row) > width:
                    self.report.problems.append(RowProblem(
                        line_number=line_number,
                        kind=CsvRowProblemKind.EXTRA_FIELDS,
                        detail=(
                            f"expected {width} fields, found {len(row)}; "
                            f"row {_row_preview(row)}"
                        ),
                    ))
                yield line_number, dict(zip(self.header.columns, row))
        except csv.Error as exc:
            raise SchemaViolationError(
                f"unrecoverable CSV structure: {exc}",
                file=self._path.name,
                line_number=self._reader.line_num,
            ) from exc
        except UnicodeDecodeError as exc:
            raise SchemaViolationError(
                "file is not valid UTF-8",
                file=self._path.name,
                line_number=self._reader.line_num,
            ) from exc


def _validated_header(
    raw_header: Sequence[str],
    line_number: int,
    required_columns: Iterable[str],
    file_name: str,
) -> GtfsHeader:
    """Normalise and validate the header row.

    Normalisation is lowercase + trimmed (plan §3); duplicates are structural
    failures; every caller-required column must be present, compared in the
    same normalised form.
    """
    columns = tuple(column.strip().lower() for column in raw_header)
    if not any(columns):
        raise SchemaViolationError(
            "header row is blank",
            file=file_name,
            line_number=line_number,
        )
    # Review fix 3B: reject *individual* empty column names, not only an
    # entirely blank header row — "stop_id,,stop_name" is broken input.
    empty_positions = [
        str(index + 1)
        for index, column in enumerate(columns)
        if column == ""
    ]
    if empty_positions:
        raise SchemaViolationError(
            "empty header column name(s) at position(s) "
            f"{', '.join(empty_positions)}",
            file=file_name,
            line_number=line_number,
        )
    seen: set[str] = set()
    duplicates: list[str] = []
    for column in columns:
        if column in seen and column not in duplicates:
            duplicates.append(column)
        seen.add(column)
    if duplicates:
        raise SchemaViolationError(
            f"duplicate header column(s): {', '.join(duplicates)}",
            file=file_name,
            line_number=line_number,
        )
    required = {column.strip().lower() for column in required_columns}
    missing = sorted(required - seen)
    if missing:
        raise SchemaViolationError(
            f"missing required column(s): {', '.join(missing)}",
            file=file_name,
            line_number=line_number,
        )
    return GtfsHeader(columns=columns, line_number=line_number)


@contextmanager
def open_gtfs_csv(
    path: Path | str,
    *,
    required_columns: Sequence[str] = (),
) -> Iterator[GtfsCsvFile]:
    """Context manager opening one GTFS CSV file with safe handle management.

    - ``utf-8-sig`` decoding reads input with or without a leading BOM.
    - ``newline=""`` keeps the ``csv`` module's rule handling intact for
      quoted multi-line fields.
    - The header is normalised (lowercase, trimmed, duplicate-checked) and
      must contain every name in ``required_columns``; an empty file, blank
      header, or undecodable header raises :class:`SchemaViolationError`
      before any record is yielded.
    - The reader uses the **strict** CSV dialect: corrupted quoting (e.g. an
      unterminated final quote) is a structural failure instead of silently
      yielding a fabricated last row. Quoted multi-line fields remain legal.
    """
    file_name = Path(path).name
    with Path(path).open("rt", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, strict=True)
        try:
            header_row = next(reader)
        except StopIteration:
            raise SchemaViolationError(
                "file is empty (no header row)",
                file=file_name,
            ) from None
        except UnicodeDecodeError as exc:
            raise SchemaViolationError(
                "file is not valid UTF-8",
                file=file_name,
            ) from exc
        except csv.Error as exc:
            raise SchemaViolationError(
                f"header row is unreadable CSV: {exc}",
                file=file_name,
            ) from exc
        header = _validated_header(
            header_row, reader.line_num, required_columns, file_name
        )
        yield GtfsCsvFile(
            path=Path(path), header=header, report=CsvReport(), reader=reader
        )


# ---------------------------------------------------------------------------
# Typed cell getters (plan §3): absent/blank -> None, malformed ->
# CellParseError. Fields are read as strings until these explicit conversions.
# ---------------------------------------------------------------------------

def get_str(record: Mapping[str, str], column: str) -> str | None:
    """Return the raw string value, or ``None`` when absent or empty.

    Values are preserved verbatim: GTFS field content, including inner and
    surrounding spaces, is meaningful (headsigns, names).
    """
    value = record.get(column)
    if value is None or value == "":
        return None
    return value


def get_int(
    record: Mapping[str, str],
    column: str,
    *,
    file: str | None = None,
    line_number: int | None = None,
) -> int | None:
    """Parse an integer cell (``stop_sequence``, ``direction_id``, ...).

    Blank/whitespace-only cells and absent columns are ``None``; surrounding
    whitespace is tolerated; anything non-integer raises
    :class:`CellParseError` naming the column. ``file``/``line_number`` are
    optional context the caller attaches so the error identifies its row
    (review fix 3A). The raw cell is bounded inside ``CellParseError``.
    """
    raw = record.get(column)
    if raw is None or raw.strip() == "":
        return None
    try:
        return int(raw)
    except ValueError:
        raise CellParseError(
            "cannot parse integer value",
            column=column,
            raw_value=raw,
            file=file,
            line_number=line_number,
        ) from None


def get_float(
    record: Mapping[str, str],
    column: str,
    *,
    file: str | None = None,
    line_number: int | None = None,
) -> float | None:
    """Parse a float cell (``stop_lat``, ...).

    Blank/absent is ``None``. ``nan`` and the infinities are rejected:
    otherwise ``nan <= 0``-style checks downstream evasively pass them
    (the same lesson as the S0 config review fix 5).
    """
    raw = record.get(column)
    if raw is None or raw.strip() == "":
        return None
    try:
        value = float(raw)
    except ValueError:
        raise CellParseError(
            "cannot parse float value",
            column=column,
            raw_value=raw,
            file=file,
            line_number=line_number,
        ) from None
    if not math.isfinite(value):
        raise CellParseError(
            "float value must be finite",
            column=column,
            raw_value=raw,
            file=file,
            line_number=line_number,
        )
    return value


def get_time_secs(
    record: Mapping[str, str],
    column: str,
    *,
    file: str | None = None,
    line_number: int | None = None,
) -> int | None:
    """Parse a GTFS ``HH:MM:SS`` time into seconds after midnight.

    Hours may exceed 23 (``25:10:30`` -> 90 630 s, common in
    ``stop_times.txt``); minutes and seconds must be zero-padded two-digit
    values below 60. Blank/absent is ``None``.
    """
    raw = record.get(column)
    if raw is None or raw.strip() == "":
        return None
    match = _TIME_PATTERN.fullmatch(raw.strip())
    if match is None:
        raise CellParseError(
            "cannot parse GTFS HH:MM:SS time",
            column=column,
            raw_value=raw,
            file=file,
            line_number=line_number,
        )
    hours, minutes, seconds = (int(part) for part in match.groups())
    if minutes >= 60 or seconds >= 60:
        raise CellParseError(
            "time minutes/seconds out of range",
            column=column,
            raw_value=raw,
            file=file,
            line_number=line_number,
        )
    return hours * 3600 + minutes * 60 + seconds
