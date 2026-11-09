"""Typed S11 errors and the frozen matching reason vocabulary.

Stage S11 (M2 plan §5–§6, §12; implementation prompt §6).  This module owns:

- :class:`MatchReason` — the **frozen** matching reason vocabulary (17 exact
  spellings, pinned in the implementation prompt §6 before implementation).
  Raw payload content is never concatenated into reason values; the exact
  per-reason totals are maintained independently of diagnostic sample
  truncation.
- :class:`MatchingError` — base for every S11 failure.  Exceptions carry a
  short, safe ``code`` drawn from a small fixed vocabulary plus a bounded
  human message: file/directory *names*, line numbers, counts — never raw
  feed bodies, URLs, credentials, absolute machine paths, or full CSV rows
  (plan §12).

Two failure levels, mirroring the S9/S10 disposition split:

- :class:`ContextPreparationError` — a defective dataset/manifest/archive/
  index/network combination fails *whole-context* preparation.  No partial
  context is ever returned; the caller keeps its previous context.
- :class:`MatchInputError` — a programmer-input defect in the
  :func:`backend.trimet.feed_match.match_delivery` adapter (wrong delivery
  type, feed/model disagreement, wrong context type).  A typed error, never
  an empty successful batch.
"""

from __future__ import annotations

import enum

__all__ = [
    "INPUT_ERROR_CODES",
    "PREPARATION_ERROR_CODES",
    "ContextPreparationError",
    "MatchInputError",
    "MatchReason",
    "MatchingError",
]


class MatchReason(enum.Enum):
    """Frozen S11 matching reason vocabulary (plan §6; prompt §6).

    Each value is a stable, spelling-pinned code.  Ordinary known non-MAX
    records produce **no** diagnostic; the vocabulary covers genuinely
    unknown/conflicting data and missing optional enrichment.  Precedence
    and severity of individual codes are established by the matching policy
    in :mod:`backend.trimet.feed_match` and :mod:`backend.trimet.stop_match`;
    this enum only fixes the vocabulary.
    """

    UNKNOWN_ROUTE = "unknown_route"
    UNKNOWN_VEHICLE_TYPE = "unknown_vehicle_type"
    TYPE_ROUTE_CONFLICT = "type_route_conflict"
    TRIP_NOT_REPORTED = "trip_not_reported"
    UNKNOWN_TRIP = "unknown_trip"
    TRIP_ROUTE_CONFLICT = "trip_route_conflict"
    DIRECTION_CONFLICT = "direction_conflict"
    MISSING_SHAPE = "missing_shape"
    UNUSABLE_SHAPE = "unusable_shape"
    MISSING_STOP_DETAILS = "missing_stop_details"
    UNKNOWN_STOP_SEQUENCE = "unknown_stop_sequence"
    UNKNOWN_STOP_ID = "unknown_stop_id"
    STOP_ID_SEQUENCE_CONFLICT = "stop_id_sequence_conflict"
    AMBIGUOUS_STOP_VISIT = "ambiguous_stop_visit"
    MISSING_STOP_SELECTORS = "missing_stop_selectors"
    UNSUPPORTED_OPERATIONAL_RELATIONSHIP = "unsupported_operational_relationship"
    DIAGNOSTICS_OVERFLOW = "diagnostics_overflow"


# ---------------------------------------------------------------------------
# Error vocabulary: small fixed codes, never raw payload content
# ---------------------------------------------------------------------------

PREPARATION_ERROR_CODES: frozenset[str] = frozenset(
    {
        "manifest_absent",  # dataset directory or manifest.json missing
        "manifest_unreadable",  # malformed/truncated/unparseable manifest
        "manifest_paths_mismatch",  # recorded paths do not identify the
        #   supplied promoted directory / gtfs.zip
        "archive_unreadable",  # gtfs.zip missing or unreadable while hashing
        "archive_hash_mismatch",  # streamed SHA-256 disagrees with manifest
        "source_member_missing",  # routes.txt/trips.txt absent from the zip
        "source_member_ambiguous",  # duplicate member name inside the zip
        "csv_structure",  # header/duplicate-column/width/UTF-8 defects
        "invalid_cell",  # a typed cell conversion failed (MAX rows)
        "identity_mismatch",  # feed version / recheck-of-identity mismatch
        "index_inconsistency",  # duplicate/orphan/blank IDs, MAX-table
        #   disagreement, invalid route types, broken stop indexes
    }
)

INPUT_ERROR_CODES: frozenset[str] = frozenset(
    {
        "delivery_type_mismatch",  # not a PollDelivery
        "feed_model_mismatch",  # PollFeed enum disagrees with the S9 model
        "invalid_context",  # not a MatchingContext
    }
)


class MatchingError(Exception):
    """Base class for every S11 failure.

    Exceptions carry only parsing/identity context: bounded message plus a
    code from the fixed vocabulary above.  No credentials, URLs, raw feed
    bodies, absolute machine paths, or full CSV rows are ever embedded.
    """

    _allowed_codes: frozenset[str] = frozenset()

    def __init__(self, message: str, *, code: str) -> None:
        if code not in self._allowed_codes:
            raise ValueError(
                f"{type(self).__name__} code {code!r} is not in its fixed "
                "vocabulary"
            )
        super().__init__(message)
        self.message = message
        self.code = code


class ContextPreparationError(MatchingError):
    """Static-context preparation failed whole; never returns a partial index.

    Raised by :func:`backend.trimet.match_index.prepare_matching_context`
    (missing manifests, unreadable archives, divergent hashes, defective CSV
    structure, inconsistent MAX tables) and by
    :func:`backend.trimet.match_index.build_matching_context` (duplicate/
    orphan/blank identifiers, invalid route modes, broken stop indexes).
    The caller (future S12) keeps its previously prepared context.
    """

    _allowed_codes = PREPARATION_ERROR_CODES


class MatchInputError(MatchingError):
    """The adapter received programmer-input it must not silently accept.

    A delivery that is not a :class:`~backend.trimet.poll_types.PollDelivery`,
    a feed/model disagreement (for example an alerts delivery carrying a
    vehicles snapshot), or a context that is not a
    :class:`~backend.trimet.match_types.MatchingContext` raises here — this
    is never degraded into an empty successful batch.
    """

    _allowed_codes = INPUT_ERROR_CODES
