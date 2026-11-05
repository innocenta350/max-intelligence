"""Load-error hierarchy for the GTFS pipeline (Stage S1, M1 plan Rev. 9).

Every pipeline-specific failure derives from :class:`GtfsLoadError`, so later
stages can catch all planned failures with one ``except GtfsLoadError`` while
each raise site still classifies precisely:

- ``ExtractionError`` — download/extraction/staging problems (Stage S2).
  ``CorruptZipError`` and ``MissingRequiredFileError`` refine it.
- ``SchemaViolationError`` — structural violations: bad headers, duplicate
  identifiers, malformed rows beyond tolerance (raised by csvio in S1 and by
  the parsers in S4).
- ``ThresholdExceededError`` — the tolerated-unrelated-malformed-rows budget
  (plan §E.3) is exhausted (S4/S5).
- ``MaxInvariantError`` — one of the six load-failing invariants (plan §E.7)
  failed; may carry the relevant ``CoverageReport`` slice (S5).
- ``ManifestUnreadableError`` — an existing dataset directory's manifest is
  missing/truncated/unparseable so its identity cannot be established; the
  loader fails safely and never overwrites it (plan Rev. 7 §2 / Rev. 8 §1).
- ``RetryablePromoteLockError`` — another process holds ``promote.lock``; the
  foreign record is read diagnostically only, never written or deleted
  (plan Rev. 9 §2).
- ``CellParseError`` — a single cell failed a typed conversion in csvio's
  getters; row-level tolerance policy lives with the S4 callers.

Exceptions carry only parsing context (file name, row number, column name)
and the specific attributes documented per class. No environment variables,
credential material, or ``.env`` contents are ever embedded in errors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = [
    "CellParseError",
    "CorruptZipError",
    "ExtractionError",
    "GtfsLoadError",
    "ManifestUnreadableError",
    "MaxInvariantError",
    "MissingRequiredFileError",
    "RetryablePromoteLockError",
    "SchemaViolationError",
    "ThresholdExceededError",
]


# Review fix 1: foundational exceptions must not depend on the data model at
# runtime — csvio imports errors, so a live errors->types import edge would
# be a real coupling. The future-annotations import keeps ``RouteCoverage``
# available purely for type checking; annotations stay un-evaluated strings.
if TYPE_CHECKING:
    from backend.gtfs.types import RouteCoverage

_MAX_VALUE_PREVIEW_LENGTH = 64


def _truncate_cell(raw: str) -> str:
    """Bound a stored cell value so diagnostics can never balloon.

    Single source of truth for the limit: csvio getters pass raw values
    untruncated and ``CellParseError`` canonicalises whatever it receives.
    """
    if len(raw) <= _MAX_VALUE_PREVIEW_LENGTH:
        return raw
    return raw[: _MAX_VALUE_PREVIEW_LENGTH - 3] + "..."


class GtfsLoadError(Exception):
    """Base class for every MAX Intelligence GTFS pipeline failure.

    Captures optional parsing context that ``str()`` renders as a clear
    prefix: ``<file> / line <n> / column '<name>': <message>``.
    """

    def __init__(
        self,
        message: str,
        *,
        file: str | None = None,
        line_number: int | None = None,
        column: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.file = file
        self.line_number = line_number
        self.column = column

    def __str__(self) -> str:
        pieces: list[str] = []
        if self.file is not None:
            pieces.append(self.file)
        if self.line_number is not None:
            pieces.append(f"line {self.line_number}")
        if self.column is not None:
            pieces.append(f"column {self.column!r}")
        prefix = " / ".join(pieces)
        return f"{prefix}: {self.message}" if prefix else self.message


class ExtractionError(GtfsLoadError):
    """Download, extraction, or staging failure (raised from Stage S2)."""


class CorruptZipError(ExtractionError):
    """CRC mismatch or structural failure inside the gtfs.zip archive (S2)."""


class MissingRequiredFileError(ExtractionError):
    """A required GTFS member is absent from the archive (S2)."""

    def __init__(
        self,
        message: str,
        *,
        members: tuple[str, ...] = (),
        file: str | None = None,
        line_number: int | None = None,
        column: str | None = None,
    ) -> None:
        super().__init__(message, file=file, line_number=line_number, column=column)
        self.members = tuple(members)


class SchemaViolationError(GtfsLoadError):
    """Structural violation inside a GTFS input.

    Covers unreadable headers, duplicate header columns, missing required
    columns, invalid UTF-8, unrecoverable CSV quoting errors, duplicate
    identifiers, and any row malformedness the caller's tolerance refuses.
    """


class ThresholdExceededError(GtfsLoadError):
    """The tolerated-malformed-row budget (plan §E.3) was exhausted.

    Raised by S4/S5 once the counting of unrelated malformed rows exceeds
    ``GtfsConfig.max_unrelated_malformed_rows``; stops the load instead of
    heaping suspicious data into the dataset.
    """


class MaxInvariantError(GtfsLoadError):
    """One of the six load-failing network invariants (plan §E.7) failed.

    ``route_id`` scopes the failure to one MAX route when applicable, and
    ``coverage`` optionally carries the relevant :class:`RouteCoverage` slice
    so the error alone explains the observed gap (raised in S5).
    """

    def __init__(
        self,
        message: str,
        *,
        route_id: str | None = None,
        coverage: RouteCoverage | None = None,
        file: str | None = None,
        line_number: int | None = None,
        column: str | None = None,
    ) -> None:
        super().__init__(message, file=file, line_number=line_number, column=column)
        self.route_id = route_id
        self.coverage = coverage


class ManifestUnreadableError(GtfsLoadError):
    """A dataset directory's identity cannot be established (plan Rev. 7 §2).

    Raised when an existing target directory's ``manifest.json`` is missing,
    truncated, empty-sha'd, or unparseable: the loader fails safely, performs
    no promotion/reuse/rename/suffix of the staged content, and leaves the
    ambiguous directory byte-for-byte untouched. ``directory`` names it.
    """

    def __init__(
        self,
        message: str,
        *,
        directory: str | None = None,
        file: str | None = None,
        line_number: int | None = None,
        column: str | None = None,
    ) -> None:
        super().__init__(message, file=file, line_number=line_number, column=column)
        self.directory = directory


class RetryablePromoteLockError(GtfsLoadError):
    """``promote.lock`` contention during promotion (plan Rev. 9 §2).

    The foreign lock record is read diagnostically only — never written,
    recreated, or deleted. Carries the foreign owner's raw ``pid`` and
    ``acquired_utc_ms`` for the bounded retry log (disposition-table row 4).
    """

    def __init__(
        self,
        message: str,
        *,
        foreign_pid: int | None = None,
        acquired_utc_ms: int | None = None,
        file: str | None = None,
        line_number: int | None = None,
        column: str | None = None,
    ) -> None:
        super().__init__(message, file=file, line_number=line_number, column=column)
        self.foreign_pid = foreign_pid
        self.acquired_utc_ms = acquired_utc_ms


class CellParseError(GtfsLoadError):
    """A single cell failed a typed conversion (csvio getters).

    The row's file/line/column context travels in the base class; the
    offending cell itself travels in ``raw_value`` — always bounded by
    :func:`_truncate_cell` here, never by call-site convention. Messages are
    expected to stay generic in callers so cell contents are not duplicated
    into free-form text; ``str()`` renders the bounded value exactly once, as
    ``(raw: '...')``. Callers in S4 decide whether a failed cell is fatal or
    a recorded row problem.
    """

    def __init__(
        self,
        message: str,
        *,
        raw_value: str = "",
        file: str | None = None,
        line_number: int | None = None,
        column: str | None = None,
    ) -> None:
        super().__init__(message, file=file, line_number=line_number, column=column)
        self.raw_value = _truncate_cell(raw_value)

    def __str__(self) -> str:
        base = super().__str__()
        if self.raw_value:
            return f"{base} (raw: {self.raw_value!r})"
        return base
