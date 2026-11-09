"""Immutable data contracts for the three TriMet live feeds (Stage S9, M2 plan REV 2).

The stable data contracts that S10 (polling), S11 (matching), and S12
(TripUpdate ingestion) build against.  Investigation basis: S8 contract
verification (``docs/trimet-api-notes.md`` §11, findings A1–A6 and B1–B8) and
the M0 audit (§1–§10).  The binding specification is the owner-approved plan
REV 2 (``§5`` request provenance, ``§6`` malformed-data policy, ``§9–§10``
model rules).

Conventions (mirroring ``backend/gtfs/types.py``):

- Every record is a frozen dataclass with ``slots=True``.
- Collection fields use ``tuple`` only — no lists, dicts, or sets — so nothing
  can be mutated after construction (plan §9).
- Timestamps are UTC epoch **milliseconds** internally; where the feed
  delivers a different unit (the TripUpdate header delivers seconds) the raw
  value is preserved alongside the normalized one, and seconds→milliseconds
  conversion is integer-only (``seconds * 1000``).

Authority is unrepresentable from parser output (plan §5): a
:class:`VehiclesSnapshot` records only what was requested and what the
payload contained.  Whether a response is authoritative for reconciliation is
a caller decision expressed solely by constructing a
:class:`ReconciliationSnapshot`, whose constructor enforces the invariants
(owner watch item) — a ``since=`` response can never back full-snapshot
removal, and a blank justification is unrepresentable.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")

# --- Plausibility window for UTC epoch-millisecond timestamps ---------------
# Deterministic bounds (no wall-clock reads anywhere in S9): the floor is
# 2000-01-01T00:00:00Z and the ceiling 2200-01-01T00:00:00Z.  Values outside
# the window cannot be real TriMet epochs and are rejected per plan §6.
MIN_PLAUSIBLE_UTC_MS = 946_684_800_000
MAX_PLAUSIBLE_UTC_MS = 7_258_118_400_000

# --- gtfs-realtime.proto enum values (raw wire integers) --------------------
# FeedHeader.incrementality (S8/B1: only 0 observed across 10 captures; the
# safe-degradation requirement is that anything else is rejected whole).
INCREMENTALITY_FULL_DATASET = 0
INCREMENTALITY_INCREMENTAL = 1
INCREMENTALITY_DIFFERENTIAL = 2


def _is_int(value: object) -> bool:
    """True for real integers (``bool`` is an ``int`` subclass and excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    """True for int/float (``bool`` excluded)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_plausible_utc_ms(value: object) -> bool:
    return _is_int(value) and MIN_PLAUSIBLE_UTC_MS <= value <= MAX_PLAUSIBLE_UTC_MS


class VehiclesRequestKind(enum.Enum):
    """What was requested from the vehicles endpoint (plan §5).

    S8/A4 established that ``since=`` is not reliably incremental, so the kind
    of request is recorded fact, never inferred from the payload, and never
    conflated with snapshot authority.
    """

    DEFAULT = "default"  # plain vehicles request (no since parameter)
    SINCE = "since"  # request made with since=<ms>


@dataclass(frozen=True, slots=True)
class VehiclesRequestProvenance:
    """Records what was requested; validates itself in the constructor.

    Invariants (owner watch item, each with a dedicated test):

    - ``since_ms`` is ``None`` if and only if ``request_kind`` is
      :attr:`VehiclesRequestKind.DEFAULT`;
    - ``requested_at_utc_ms`` is a plausible UTC ms epoch.
    """

    request_kind: VehiclesRequestKind
    since_ms: int | None
    requested_at_utc_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.request_kind, VehiclesRequestKind):
            raise TypeError(
                "request_kind must be a VehiclesRequestKind, "
                f"got {type(self.request_kind).__name__}"
            )
        if self.request_kind is VehiclesRequestKind.DEFAULT:
            if self.since_ms is not None:
                raise ValueError(
                    "since_ms must be None for a DEFAULT vehicles request "
                    f"(got {self.since_ms!r})"
                )
        else:  # SINCE
            if not _is_int(self.since_ms):
                raise ValueError(
                    "since_ms must be an integer UTC ms epoch for a SINCE "
                    f"vehicles request (got {self.since_ms!r})"
                )
        if not _is_plausible_utc_ms(self.requested_at_utc_ms):
            raise ValueError(
                "requested_at_utc_ms must be a plausible UTC ms epoch in "
                f"[{MIN_PLAUSIBLE_UTC_MS}, {MAX_PLAUSIBLE_UTC_MS}] "
                f"(got {self.requested_at_utc_ms!r})"
            )


@dataclass(frozen=True, slots=True)
class VehiclePosition:
    """One vehicle from ``/ws/v2/vehicles`` (raw JSON keys, snake_case here).

    Field set is the full 30-key schema verified by M0 §2.1 and re-verified
    UNCHANGED by S8/A1.  Required fields (record rejected if missing/invalid,
    plan §6.1): ``vehicle_id``, ``vehicle_type``, ``route_number``,
    ``latitude``, ``longitude``, ``time_utc_ms``, ``expires_utc_ms``.
    Optional fields preserve ``None`` when absent or invalid in the feed.

    ``time_utc_ms`` and ``expires_utc_ms`` are the raw feed milliseconds
    preserved verbatim — S10/S12 need the raw values for the three-case
    removal rule (absence from a ``since=`` response, absence from an
    authoritative snapshot, and a passed ``expires`` are three distinct
    facts).
    """

    # Required core (parser enforces; see plan §6.1).
    vehicle_id: int
    vehicle_type: str  # raw feed key ``type``: "bus" | "rail" observed
    route_number: int
    latitude: float
    longitude: float
    time_utc_ms: int
    expires_utc_ms: int
    # Optional fields (None when absent or invalid in the feed).
    route_color: str | None = None
    route_subtype: str | None = None
    direction: int | None = None
    trip_id: str | None = None
    new_trip: bool | None = None
    off_route: bool | None = None
    # Raw feed units preserved (seconds; negative means late, M0 §2.1).
    delay: int | None = None
    bearing: int | None = None
    next_loc_id: int | None = None
    next_stop_seq: int | None = None
    last_loc_id: int | None = None
    last_stop_seq: int | None = None
    sign_message: str | None = None
    sign_message_long: str | None = None
    garage: str | None = None
    block_id: int | None = None
    source: str | None = None
    # Experimental fields also observed on rail vehicles (M0 §2.1); never relied on.
    in_congestion: bool | None = None
    load_percentage: int | float | None = None
    location_in_schedule_day: int | None = None
    # Type not pinned by M0/S8 evidence; raw value preserved when str or int.
    message_code: str | int | None = None
    extra_block_id: int | None = None
    service_date: int | None = None  # raw feed milliseconds


@dataclass(frozen=True, slots=True)
class VehiclesSnapshot:
    """One parsed vehicles response — facts only, **no authority flag**.

    The parser can never produce or imply authority (plan §5).  Whether this
    snapshot is authoritative for reconciliation is expressed solely by the
    caller wrapping it in a :class:`ReconciliationSnapshot`.
    """

    provenance: VehiclesRequestProvenance
    query_time_utc_ms: int  # validated resultSet.queryTime (raw ms)
    vehicles: tuple[VehiclePosition, ...]

    @property
    def vehicle_count(self) -> int:
        return len(self.vehicles)

    @property
    def rail_count(self) -> int:
        return sum(1 for v in self.vehicles if v.vehicle_type == "rail")


@dataclass(frozen=True, slots=True)
class ReconciliationSnapshot:
    """A snapshot the **caller** has independently established as authoritative.

    For example, the scheduled full reconciliation poll.  The parser can never
    produce one (plan §5): this wrapper is the only authority carrier, and its
    constructor enforces the invariants itself — safety never relies on
    callers following the intended construction pattern (owner watch item).

    ``DEFAULT`` provenance is necessary but **not** sufficient: the caller
    still supplies the authority decision explicitly through ``basis``.
    """

    snapshot: VehiclesSnapshot
    basis: str  # what established authority, e.g. "scheduled 5-minute full reconciliation poll"
    established_at_utc_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, VehiclesSnapshot):
            raise TypeError(
                "snapshot must be a VehiclesSnapshot, "
                f"got {type(self.snapshot).__name__}"
            )
        if not isinstance(self.basis, str) or not self.basis.strip():
            raise ValueError(
                "basis must be a non-empty string describing what established "
                "authority for this reconciliation snapshot"
            )
        provenance = self.snapshot.provenance
        if not isinstance(provenance, VehiclesRequestProvenance):
            raise TypeError(
                "snapshot.provenance must be a VehiclesRequestProvenance, "
                f"got {type(provenance).__name__}"
            )
        if provenance.request_kind is not VehiclesRequestKind.DEFAULT:
            raise ValueError(
                "a since= response can never back full-snapshot removal: "
                "only DEFAULT provenance may be wrapped in a "
                "ReconciliationSnapshot"
            )
        # Defense in depth: re-validate the wrapped provenance's own
        # invariants even though its constructor already enforced them.
        if provenance.since_ms is not None:
            raise ValueError(
                "wrapped provenance is internally invalid: since_ms must be "
                "None for a DEFAULT request"
            )
        if not _is_plausible_utc_ms(provenance.requested_at_utc_ms):
            raise ValueError(
                "wrapped provenance is internally invalid: requested_at_utc_ms "
                "is not a plausible UTC ms epoch"
            )
        if not _is_plausible_utc_ms(self.established_at_utc_ms):
            raise ValueError(
                "established_at_utc_ms must be a plausible UTC ms epoch in "
                f"[{MIN_PLAUSIBLE_UTC_MS}, {MAX_PLAUSIBLE_UTC_MS}] "
                f"(got {self.established_at_utc_ms!r})"
            )


# --- Alerts (/ws/v2/alerts) --------------------------------------------------


@dataclass(frozen=True, slots=True)
class AlertRouteChild:
    """One ``route`` child of an alert (S8/B4 verified shape).

    ``route_id`` is the raw integer feed ``id``; MAX classification joins it
    against ``MAX_ROUTE_IDS`` (string keys) via ``str(route_id)``.
    """

    route_id: int
    route_type: str | None = None  # raw feed key ``type``: "B" | "R" observed
    no_service_flag: bool | None = None


@dataclass(frozen=True, slots=True)
class AlertLocation:
    """One ``location`` child of an alert (S8/B4 verified shape)."""

    location_id: int
    description: str | None = None  # raw feed key ``desc``
    no_service_flag: bool | None = None


@dataclass(frozen=True, slots=True)
class ServiceAlert:
    """One service alert (M0 §2.2 + S8/B3–B4 verified shape).

    ``end_utc_ms`` is ``None`` when absent **or null** — null ``end`` is a
    verified observation for long-running construction alerts (S8/A5), not an
    error.  ``system_wide_flag`` is ``None`` when absent and is treated as not
    system-wide by the classifier (plan §6.2).
    """

    alert_id: int
    begin_utc_ms: int | None = None
    end_utc_ms: int | None = None
    system_wide_flag: bool | None = None
    header_text: str | None = None
    description: str | None = None  # raw feed key ``desc``
    info_link_url: str | None = None
    routes: tuple[AlertRouteChild, ...] = ()
    locations: tuple[AlertLocation, ...] = ()


@dataclass(frozen=True, slots=True)
class AlertsSnapshot:
    """One parsed alerts response (facts only)."""

    query_time_utc_ms: int
    alerts: tuple[ServiceAlert, ...]

    @property
    def alert_count(self) -> int:
        return len(self.alerts)


# --- TripUpdates (GTFS-Realtime TripUpdate feed) -----------------------------


@dataclass(frozen=True, slots=True)
class TripDescriptor:
    """Raw GTFS-Realtime TripDescriptor (plan §9: derive nothing here).

    ``route_id`` is stored raw if the feed carries it, but **no route
    classification is derived from it** — route identification happens only
    via the S11/S12 ``trip_id`` join (owner guidance).
    """

    trip_id: str
    route_id: str | None = None
    direction_id: int | None = None
    start_time: str | None = None
    start_date: str | None = None
    schedule_relationship_raw: int | None = None


@dataclass(frozen=True, slots=True)
class StopTimeEvent:
    """One arrival/departure event (raw StopTimeEvent fields).

    ``time_raw_seconds`` is the raw feed seconds; ``time_utc_ms`` is the
    integer-only conversion (``seconds * 1000``).  ``delay_seconds`` preserves
    negative values exactly (S8/B7 verified sign handling).
    """

    delay_seconds: int | None = None
    time_raw_seconds: int | None = None
    time_utc_ms: int | None = None
    uncertainty_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class StopTimeUpdate:
    """One stop-time entry of a TripUpdate.

    The parser drops entries with neither ``arrival`` nor ``departure``
    (plan §6.3, sub-record row); a surviving entry has at least one of them.
    """

    stop_sequence: int | None = None
    stop_id: str | None = None
    arrival: StopTimeEvent | None = None
    departure: StopTimeEvent | None = None
    schedule_relationship_raw: int | None = None


@dataclass(frozen=True, slots=True)
class TripUpdate:
    """One TripUpdate message body (trip + stop-level predictions)."""

    trip: TripDescriptor
    stop_time_updates: tuple[StopTimeUpdate, ...]
    # From the VehicleDescriptor; None when absent — coverage must never be
    # assumed (AGENTS §4, plan §6.3).
    vehicle_id: str | None = None
    # Raw feed units preserved (seconds); negative values are valid (S8/B7).
    delay_seconds: int | None = None
    timestamp_raw_seconds: int | None = None
    timestamp_utc_ms: int | None = None


@dataclass(frozen=True, slots=True)
class TripUpdateEntity:
    """One FeedEntity wrapper from a TripUpdate feed."""

    entity_id: str
    trip_update: TripUpdate
    is_deleted: bool = False


@dataclass(frozen=True, slots=True)
class TripUpdateFeedHeader:
    """The FeedHeader, normalized (plan §6.3 + §9).

    ``timestamp_raw_seconds`` is the raw feed seconds (S8/B8: the header
    timestamp is in **seconds**, unlike the JSON feeds' milliseconds);
    ``timestamp_utc_ms`` is the integer-only conversion.  ``incrementality_raw``
    must be present and equal ``INCREMENTALITY_FULL_DATASET`` for the feed to
    parse at all (enforced at the parse boundary; S12 re-checks as defense in
    depth).  ``gtfs_realtime_version`` is tolerated as ``None`` with a
    diagnostic (metadata; does not affect processing correctness).
    """

    gtfs_realtime_version: str | None
    incrementality_raw: int
    timestamp_raw_seconds: int
    timestamp_utc_ms: int

    @property
    def is_full_dataset(self) -> bool:
        return self.incrementality_raw == INCREMENTALITY_FULL_DATASET


@dataclass(frozen=True, slots=True)
class TripUpdateFeed:
    """One parsed TripUpdate feed (header + entities)."""

    header: TripUpdateFeedHeader
    entities: tuple[TripUpdateEntity, ...]

    @property
    def entity_count(self) -> int:
        return len(self.entities)


# --- Diagnostics and parse results -------------------------------------------


@dataclass(frozen=True, slots=True)
class FeedDiagnostic:
    """One record-level rejection or notable anomaly (plan §6).

    ``record_key`` identifies the affected record (stringified id; ``None``
    for feed-level context entries).  ``reason`` is a stable code (e.g.
    ``missing_required_field``, ``invalid_timestamp``).  ``detail`` is a short
    credential-free string and may carry a scalar offending value only when it
    is a non-credential primitive — never URLs or raw payloads.
    """

    feed: str
    record_key: str | None
    field: str
    reason: str
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class FeedParseResult(Generic[T]):
    """The outcome of one successful parse: the model plus its diagnostics.

    Feed-level defects never produce one of these — they raise
    :class:`~backend.trimet.feed_errors.FeedRejectedError` instead, so a
    caller holding a :class:`FeedParseResult` holds a fully normalized feed.
    """

    model: T
    diagnostics: tuple[FeedDiagnostic, ...] = ()
