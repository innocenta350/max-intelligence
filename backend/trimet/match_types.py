"""Immutable S11 contracts: identity, context, matching results and batches.

Stage S11 (M2 plan §5–§6; implementation prompt §4–§5).  Conventions mirror
``backend/trimet/feed_types.py`` (frozen dataclasses with ``slots=True``;
``tuple``-only collections) and ``backend/gtfs/types.py`` (mapping fields are
snapshotted into read-only proxies in ``__post_init__`` — immutability is
enforced, not documented-and-hoped).

Message discipline:

- **Relevance** (one primary classification per parsed input record):
  ``MAX_RELEVANT`` / ``NON_MAX`` / ``UNCLASSIFIED``.  Record-accounting
  invariant: the three counts sum to the feed's parsed input count.
- **Enrichment** is kept separate: ``matched`` / ``partial`` / ``unmatched``
  / ``conflict``.  A MAX vehicle can be relevant while its trip enrichment
  is unmatched; enrichment never upgrades relevance.
- Raw S9 models are referenced intact, never rewritten, and S10 envelope
  metadata (sequence, purpose, timestamps, evidence diagnostics) is carried
  through unchanged and separately from new matching diagnostics.
"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from backend.gtfs.types import (
    MaxNetwork,
    Route,
    ShapeGeometry,
    Stop,
    Trip,
    TripStopEntry,
)
from backend.trimet.alerts_classify import AlertClassificationResult
from backend.trimet.feed_types import (
    FeedDiagnostic,
    ServiceAlert,
    StopTimeUpdate,
    TripUpdateEntity,
    VehiclePosition,
)
from backend.trimet.match_errors import MatchReason
from backend.trimet.poll_types import PollFeed, RequestPurpose, VehicleCollectionEvidence

__all__ = [
    "DatasetIdentity",
    "EnrichmentState",
    "MatchedAlert",
    "MatchedBatch",
    "MatchedStopUpdate",
    "MatchedTripUpdate",
    "MatchedVehicle",
    "MatchingContext",
    "MatchedRecord",
    "MatchDiagnostic",
    "RelevanceClassification",
    "RouteRow",
    "StopResolutionStatus",
    "StopVisitResolution",
    "TripRow",
]

# The exact native vehicle-type strings are pinned per plan §7.  Every other
# string (including ``"R"``, ``"Rail"``, whitespace-only values) is an
# unsupported type → UNCLASSIFIED via ``unknown_vehicle_type``; no silent
# case/whitespace normalization is performed.
VEHICLE_TYPE_RAIL = "rail"
VEHICLE_TYPE_BUS = "bus"

# The agreed static mode conflict: GTFS routes.txt route_type value for bus.
ROUTE_TYPE_BUS = 3


class RelevanceClassification(enum.Enum):
    """The one primary classification per parsed input record (plan §6)."""

    MAX_RELEVANT = "max_relevant"
    NON_MAX = "non_max"
    UNCLASSIFIED = "unclassified"


class EnrichmentState(enum.Enum):
    """Enrichment state of a MAX-relevant record (plan §6).

    Kept strictly separate from relevance: ``matched`` describes a resolved
    static enrichment, ``partial`` a resolved one with recorded gaps,
    ``unmatched`` an unusable lookup, ``conflict`` an explicit contradiction
    that prevents confident enrichment.
    """

    MATCHED = "matched"
    PARTIAL = "partial"
    UNMATCHED = "unmatched"
    CONFLICT = "conflict"


class StopResolutionStatus(enum.Enum):
    """Outcome of an exact trip-local stop-visit resolution (plan §9)."""

    RESOLVED = "resolved"
    CONFLICT = "conflict"
    AMBIGUOUS = "ambiguous"
    UNRESOLVED = "unresolved"


# ---------------------------------------------------------------------------
# Dataset identity and normalized row entries
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DatasetIdentity:
    """Content identity of a prepared static dataset (plan §5.1).

    The complete archive SHA-256 is the content identity; the M1
    ``feed_version`` travels alongside it.  ``source_directory_name`` is
    **descriptive only** (observed name, never an absolute path) and carries
    no validity claim of its own.  A synthetic identity built for tests makes
    no claim that a real archive was verified.
    """

    archive_sha256: str
    feed_version: str
    source_directory_name: str | None = None  # descriptive only

    def __post_init__(self) -> None:
        if (
            not isinstance(self.archive_sha256, str)
            or len(self.archive_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.archive_sha256)
        ):
            raise ValueError(
                "archive_sha256 must be a lowercase 64-hex digest, "
                f"got {self.archive_sha256!r}"
            )
        if not isinstance(self.feed_version, str) or not self.feed_version:
            raise ValueError("feed_version must be a non-empty string")
        if self.source_directory_name is not None and (
            not isinstance(self.source_directory_name, str)
            or not self.source_directory_name
        ):
            raise ValueError("source_directory_name must be None or a name")


@dataclass(frozen=True, slots=True)
class RouteRow:
    """One normalized ``routes.txt`` row for the shared builder.

    Exact-width CSV rules are enforced by the preparation factory before
    normalization; direct (test/inputs) callers supply non-negative integer
    ``route_type`` values (booleans are rejected by the builder).
    """

    route_id: str
    route_type: int


@dataclass(frozen=True, slots=True)
class TripRow:
    """One normalized ``trips.txt`` row for the shared builder.

    On MAX rows ``direction_id`` follows the M1 ``get_int`` semantics
    (blank/whitespace → ``None``) and ``shape_id`` the ``get_str``
    semantics (empty cell → ``None``; nonempty values verbatim).  Non-MAX
    rows carry the blank-defaults; the S11 lookup only needs the two IDs.
    """

    trip_id: str
    route_id: str
    direction_id: int | None = None
    shape_id: str | None = None


@dataclass(frozen=True, slots=True)
class MatchDiagnostic:
    """One bounded S11 matching diagnostic (plan §12).

    Sized like the S9 :class:`~backend.trimet.feed_types.FeedDiagnostic`:
    a safe source identifier, field, reason and short detail.  Detail may
    carry bounded counts/scalars, never raw payloads.  Sample truncation
    never corrupts the exact per-reason totals, which are kept separately on
    the batch.
    """

    feed: str
    record_key: str | None
    reason: MatchReason
    field: str | None = None
    detail: str | None = None


# ---------------------------------------------------------------------------
# Prepared static lookup context
# ---------------------------------------------------------------------------


def _proxy(value: Mapping) -> MappingProxyType:
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class MatchingContext:
    """One prepared, immutable static matching context (plan §5.3).

    Construction is deliberately **not** part of this module's public
    surface: both the production factory
    (:func:`backend.trimet.match_index.prepare_matching_context`) and the
    synthetic tests call the one shared consistency builder
    (:func:`backend.trimet.match_index.build_matching_context`), so no
    unvalidated construction path exists.

    ``trip_to_route`` / ``known_route_ids`` / ``route_types`` cover the
    **full** static tables (all routes and trips, MAX and non-MAX); the MAX
    trip metadata and the trip-local visit indexes come from the immutable
    M1 :class:`~backend.gtfs.types.MaxNetwork`.  Every mapping is snapshotted
    read-only — deep down to the nested per-trip visit mappings — so caller
    mutation cannot alter an already prepared context.
    """

    identity: DatasetIdentity
    network: MaxNetwork  # the referenced immutable M1 network
    trip_to_route: Mapping[str, str]
    known_route_ids: frozenset[str]
    route_types: Mapping[str, int]
    max_route_ids: frozenset[str]
    visits_by_sequence: Mapping[str, Mapping[int, TripStopEntry]]
    visits_by_stop: Mapping[str, Mapping[str, tuple[TripStopEntry, ...]]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "trip_to_route", _proxy(self.trip_to_route))
        object.__setattr__(self, "route_types", _proxy(self.route_types))
        sequence_proxy = {
            trip_id: _proxy(per_trip)
            for trip_id, per_trip in self.visits_by_sequence.items()
        }
        object.__setattr__(
            self, "visits_by_sequence", _proxy(sequence_proxy)
        )
        stop_proxy = {
            trip_id: _proxy(per_trip)
            for trip_id, per_trip in self.visits_by_stop.items()
        }
        object.__setattr__(self, "visits_by_stop", _proxy(stop_proxy))


# ---------------------------------------------------------------------------
# Exact stop-visit resolution (plan §9)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StopVisitResolution:
    """The outcome of resolving one live stop selector set (plan §9).

    ``visit`` is the exact trip-local visit reference when ``status`` is
    :attr:`StopResolutionStatus.RESOLVED`; it stays ``None`` for conflicts,
    ambiguity and unresolved selector sets — no first/nearest fallback is
    ever applied.  ``stop`` is the descriptive metadata from the network's
    stops catalog, or ``None`` when the catalog lacks the referenced stop
    (in which case ``stop_details_missing`` documents the gap; never
    inventing a stop name/location).
    """

    status: StopResolutionStatus
    visit: TripStopEntry | None = None
    reasons: tuple[MatchReason, ...] = ()
    stop: Stop | None = None
    stop_details_missing: bool = False


@dataclass(frozen=True, slots=True)
class MatchedStopUpdate:
    """One preserved, trip-resolved stop-time update row (plan §9).

    ``update`` references the raw
    :class:`~backend.trimet.feed_types.StopTimeUpdate` intact; order,
    duplicates, arrival/departure events and schedule-relationship flags all
    stay exactly as the source delivered them.
    """

    update: StopTimeUpdate
    resolution: StopVisitResolution
    reasons: tuple[MatchReason, ...] = ()


# ---------------------------------------------------------------------------
# Per-record matching results (plan §6–§10)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MatchedVehicle:
    """One vehicle-position record with its primary classification.

    ``route_id`` is the canonical decimal string of the reported route
    number (JSON integers convert losslessly; GTFS route ids stay strings).
    ``max_route``/``trip``/``shape`` reference the static metadata objects
    from the captured context and are ``None`` for classifications that did
    not establish them — absence means "not established", never "fabricated
    from a fallback".

    Direction honesty (plan §7): the reported and static directions are kept
    separately.  When the reported direction is missing but the matched trip
    supplies one, ``direction_source`` records the static provenance; when
    both are present and disagree, ``direction_conflict`` is ``True``, the
    reported value is retained, and no single "certain direction" is claimed.
    """

    classification: RelevanceClassification
    enrichment: EnrichmentState | None
    vehicle: VehiclePosition
    route_id: str
    reasons: tuple[MatchReason, ...] = ()
    max_route: Route | None = None
    trip: Trip | None = None
    shape: ShapeGeometry | None = None
    reported_direction: int | None = None
    static_direction: int | None = None
    direction_source: str | None = None  # e.g. "static_trip"
    direction_conflict: bool = False
    next_stop: StopVisitResolution | None = None


@dataclass(frozen=True, slots=True)
class MatchedTripUpdate:
    """One TripUpdate entity with its primary classification (plan §8).

    ``static_route_id`` records the full-index lookup result when the trip
    id is known; ``unknown_trip`` records stay ``UNCLASSIFIED`` — the
    MAX-only lookup never re-labels unknown trips NON_MAX.  The descriptor's
    ``route_id`` (referenced intact inside ``entity``) is never a fallback
    classification source; a present-but-inconsistent descriptor route is
    flagged via ``descriptor_route_conflict`` plus the
    ``trip_route_conflict`` reason and prevents confident trip/stop
    enrichment.  Trip-instance fields (``start_date``, ``start_time``,
    schedule-relationship, ``is_deleted``) stay untouched inside the raw
    entity; ``unsupported_operational_relationship`` flags nonstandard
    operational values explicitly instead of guessing an active prediction.
    """

    classification: RelevanceClassification
    enrichment: EnrichmentState | None
    entity: TripUpdateEntity
    trip_id: str
    static_route_id: str | None = None
    trip: Trip | None = None
    stop_visits: tuple[MatchedStopUpdate, ...] = ()
    reasons: tuple[MatchReason, ...] = ()
    descriptor_route_conflict: bool = False
    unsupported_operational_relationship: bool = False


@dataclass(frozen=True, slots=True)
class MatchedAlert:
    """One service alert with its classification (plan §10).

    The S9 classifier is reused unchanged; ``result`` references its exact
    :class:`~backend.trimet.alerts_classify.AlertClassificationResult`
    (basis and classifier diagnostics preserved).  ``affected_max_routes``
    carries the MAX route metadata attached from the captured context — for
    system-wide alerts it is the context's full MAX route set; for a route
    match, the alert's MAX-scoped route children.  Location metadata is
    descriptive only and can never upgrade relevance.
    """

    classification: RelevanceClassification
    enrichment: EnrichmentState | None
    alert: ServiceAlert
    result: AlertClassificationResult
    affected_max_routes: tuple[Route, ...] = ()
    reasons: tuple[MatchReason, ...] = ()


MatchedRecord = MatchedVehicle | MatchedTripUpdate | MatchedAlert


# ---------------------------------------------------------------------------
# Matched batch (one delivery → one batch; plan §6, §11)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MatchedBatch:
    """The immutable matching outcome for one S10 ``PollDelivery``.

    The S10 envelope metadata (purpose, sequence, timestamps, HTTP status,
    vehicle-collection evidence and source parse diagnostics) is copied
    unchanged and is kept strictly separate from this stage's own matching
    diagnostics.  Classification counts sum to ``input_count`` exactly; the
    per-reason totals are exact even when the diagnostic samples are
    truncated at the plan's 200-entry cap (``omitted_match_diagnostics``
    reports the exact omitted count; with an overflow, retention keeps the
    first 199 samples plus one ``diagnostics_overflow`` summary entry).
    """

    feed: PollFeed
    dataset_identity: DatasetIdentity
    sequence: int
    purpose: RequestPurpose
    request_sent_utc_ms: int
    receipt_utc_ms: int
    elapsed_mono_ms: int
    http_status: int
    evidence: VehicleCollectionEvidence | None
    source_diagnostics: tuple[FeedDiagnostic, ...]  # referenced intact
    records: tuple[MatchedRecord, ...]
    input_count: int
    max_relevant_count: int
    non_max_count: int
    unclassified_count: int
    matched_count: int
    partial_count: int
    unmatched_count: int
    conflict_count: int
    reason_totals: Mapping[MatchReason, int]
    diagnostics: tuple[MatchDiagnostic, ...]
    omitted_match_diagnostics: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "reason_totals", MappingProxyType(dict(self.reason_totals))
        )
