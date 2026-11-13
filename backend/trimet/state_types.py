"""Immutable S12 state contracts: limits, stamps, entries, snapshots, results.

Stage S12 (M2 plan §5, §8, §12; implementation prompt §4, §8).  Conventions
mirror ``backend/trimet/feed_types.py`` and ``backend/trimet/match_types.py``:
frozen dataclasses with ``slots=True``, ``tuple``-only sample collections,
and mapping fields snapshotted into read-only proxies in ``__post_init__`` —
immutability is enforced, not documented-and-hoped.  Previously returned
snapshots never change.

Message discipline (mirroring S11):

- Relevance of a retained vehicle is the S11
  :class:`~backend.trimet.match_types.RelevanceClassification` of its current
  :class:`~backend.trimet.match_types.MatchedVehicle`; the MAX projection is
  a filtered *view*, never the source collection.  All three relevance
  classes are retained within ``max_vehicle_entries``.
- Enrichment stays separate: a MAX vehicle is tracked even when its trip,
  shape or stop enrichment is missing, unmatched or conflicting.
- Raw S9 records are referenced intact, never rewritten; source diagnostics,
  S11 matching diagnostics and S12 decision diagnostics remain three
  separate owners.
- The 13 decision reasons and 9 error codes are frozen vocabularies; exact
  per-reason totals travel independently of the capped sample list.
"""
from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from backend.trimet.feed_types import (
    MAX_PLAUSIBLE_UTC_MS,
    MIN_PLAUSIBLE_UTC_MS,
    VehiclesRequestProvenance,
)
from backend.trimet.freshness_types import ExpiryState
from backend.trimet.match_types import (
    DatasetIdentity,
    MatchedBatch,
    MatchedVehicle,
    VehiclePosition,
)
from backend.trimet.poll_types import PollDelivery, PollFeed, RequestPurpose

__all__ = [
    "AbsenceFence",
    "ApplyDecision",
    "ApplyResult",
    "ContextSwapResult",
    "FeedEnvelope",
    "FeedStateView",
    "LiveStateSnapshot",
    "PRIOR_STATE_HISTORY_UNKNOWN",
    "SourceStamp",
    "StateDecisionSample",
    "StateLimits",
    "StateReason",
    "StateTelemetry",
    "VehicleEntry",
]

PRIOR_STATE_HISTORY_UNKNOWN = "unknown"

MIN_SESSION_OR_SEQUENCE = 1


def _is_int(value: object) -> bool:
    """True for real integers (``bool`` is an ``int`` subclass and excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_plausible_utc_ms(value: object) -> bool:
    return _is_int(value) and MIN_PLAUSIBLE_UTC_MS <= value <= MAX_PLAUSIBLE_UTC_MS


class StateReason(enum.Enum):
    """Frozen S12 decision reason vocabulary (plan §12; 13 exact spellings).

    These document *why* a store decision was made.  They never concatenate
    payload content, and they are distinct from S9 source diagnostics and
    S11 matching reasons.  ``diagnostics_overflow`` is additionally the
    synthetic reason carried by the single overflow sample retained once
    the sample cap is exceeded.
    """

    OLDER_SEQUENCE = "older_sequence"
    DUPLICATE_DELIVERY = "duplicate_delivery"
    OLDER_SOURCE_TIMESTAMP = "older_source_timestamp"
    SOURCE_TIMESTAMP_CONFLICT = "source_timestamp_conflict"
    OLDER_VEHICLE_OBSERVATION = "older_vehicle_observation"
    OBSERVATION_TIE_CONFLICT = "observation_tie_conflict"
    DUPLICATE_VEHICLE_ID = "duplicate_vehicle_id"
    INCOMPLETE_COLLECTION = "incomplete_collection"
    NON_FULL_REQUEST = "non_full_request"
    EQUAL_SNAPSHOT_TIME = "equal_snapshot_time"
    AUTHORITATIVE_RECONCILIATION = "authoritative_reconciliation"
    PRE_RECONCILIATION_OBSERVATION = "pre_reconciliation_observation"
    DIAGNOSTICS_OVERFLOW = "diagnostics_overflow"


class ApplyDecision(enum.Enum):
    """What one :meth:`StateSession.accept` call did (plan §6).

    ``COMMITTED`` published a data change (entries, high-water mark,
    fence or feed view).  ``METADATA_ONLY`` booked the processed sequence
    and replay envelope with no content change (equal-source-time ignored
    or unchanged content, including secondary-feed timestamp conflicts).
    ``DUPLICATE_NOOP`` and ``STALE_NOOP`` and ``IGNORED_OLDER`` change
    nothing and mutate no root — they are counter-only observations.
    """

    COMMITTED = "committed"
    METADATA_ONLY = "metadata_only"
    DUPLICATE_NOOP = "duplicate_noop"
    STALE_NOOP = "stale_noop"
    IGNORED_OLDER = "ignored_older"


# ---------------------------------------------------------------------------
# Validated constructor-only limits (plan §12)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StateLimits:
    """Application safety limits; validated, constructor-only, never config.

    Every value must be a positive integer excluding ``bool``.  These are
    application safety limits, not feed measurements; there is no
    environment/config path to them (plan §12).  Defaults:

    - ``max_vehicle_entries = 4096`` retained IDs across all relevance
      classes; **also** the exact capacity of the authoritative presence
      set (no separate setting exists).
    - ``max_vehicle_records_per_delivery = 4096`` before deduplication;
      may be configured larger than ``max_vehicle_entries`` (the
      presence-set check then binds first for full collections).
    - ``max_tripupdate_entities = 8192`` / ``max_alert_records = 4096``
      per retained/latest secondary delivery.
    - ``max_nested_feed_records = 100000`` aggregate stop updates for a
      TripUpdate delivery or aggregate route/location children for an
      alert delivery.
    - ``max_state_diagnostic_samples = 200`` retained decision samples
      including the single overflow entry when one exists.
    """

    max_vehicle_entries: int = 4096
    max_vehicle_records_per_delivery: int = 4096
    max_tripupdate_entities: int = 8192
    max_alert_records: int = 4096
    max_nested_feed_records: int = 100000
    max_state_diagnostic_samples: int = 200

    def __post_init__(self) -> None:
        for field in (
            "max_vehicle_entries",
            "max_vehicle_records_per_delivery",
            "max_tripupdate_entities",
            "max_alert_records",
            "max_nested_feed_records",
            "max_state_diagnostic_samples",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(
                    f"{field} must be a positive integer excluding bool, "
                    f"got {value!r}"
                )


# ---------------------------------------------------------------------------
# Source stamps, vehicle entries, absence fence
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SourceStamp:
    """Compact immutable per-observation source metadata (plan §5).

    One stamp per retained observation — never a whole fleet delivery per
    vehicle.  ``source_time_utc_ms`` is the feed's own ordering timestamp:
    the vehicles/alerts snapshot ``queryTime`` or the TripUpdate header
    timestamp in milliseconds.  Receipt/request wall times are carried for
    provenance only and never substitute for source ordering.

    ``vehicles_provenance`` references the S9
    :class:`~backend.trimet.feed_types.VehiclesRequestProvenance` for
    vehicle observations (``None`` for secondary feeds) so re-matching can
    rebuild a one-record synthetic snapshot without the original delivery.
    """

    session_id: int
    sequence: int
    source_time_utc_ms: int
    purpose: RequestPurpose
    request_sent_utc_ms: int
    receipt_utc_ms: int
    elapsed_mono_ms: int
    http_status: int
    vehicles_provenance: VehiclesRequestProvenance | None = None

    def __post_init__(self) -> None:
        for field in ("session_id", "sequence"):
            value = getattr(self, field)
            if not _is_int(value) or value < MIN_SESSION_OR_SEQUENCE:
                raise ValueError(f"{field} must be a positive integer")
        if not _is_plausible_utc_ms(self.source_time_utc_ms):
            raise ValueError("source_time_utc_ms must be a plausible UTC ms epoch")
        for field in ("request_sent_utc_ms", "receipt_utc_ms"):
            if not _is_plausible_utc_ms(getattr(self, field)):
                raise ValueError(f"{field} must be a plausible UTC ms epoch")
        if not _is_int(self.elapsed_mono_ms) or self.elapsed_mono_ms < 0:
            raise ValueError("elapsed_mono_ms must be a non-negative integer")
        if not _is_int(self.http_status):
            raise ValueError("http_status must be an integer")
        if not isinstance(self.purpose, RequestPurpose):
            raise ValueError("purpose must be a RequestPurpose")
        if self.vehicles_provenance is not None and not isinstance(
            self.vehicles_provenance, VehiclesRequestProvenance
        ):
            raise ValueError(
                "vehicles_provenance must be a VehiclesRequestProvenance or None"
            )


@dataclass(frozen=True, slots=True)
class VehicleEntry:
    """The latest accepted observation for one vehicle ID (plan §5, §8).

    ``raw`` is the S9 :class:`~backend.trimet.match_types.VehiclePosition`
    referenced intact; ``matched`` is its current S11
    :class:`~backend.trimet.match_types.MatchedVehicle` against the store's
    current static context; ``stamp`` is the compact source metadata of the
    accepted observation; ``matched_generation`` is the static-context
    generation under which ``matched`` was produced (re-matching under a
    new context bumps it to the new generation without touching the raw
    record or the stamp).
    """

    vehicle_id: int
    raw: VehiclePosition
    matched: MatchedVehicle
    stamp: SourceStamp
    matched_generation: int

    def __post_init__(self) -> None:
        if not _is_int(self.vehicle_id):
            raise ValueError("vehicle_id must be an integer")
        if not isinstance(self.raw, VehiclePosition):
            raise ValueError("raw must be a VehiclePosition")
        if not isinstance(self.matched, MatchedVehicle):
            raise ValueError("matched must be a MatchedVehicle")
        if self.matched.vehicle is not self.raw:
            raise ValueError("matched.vehicle must reference the same raw record")
        if not isinstance(self.stamp, SourceStamp):
            raise ValueError("stamp must be a SourceStamp")
        if not _is_int(self.matched_generation) or self.matched_generation < 1:
            raise ValueError("matched_generation must be a positive integer")
        if self.raw.vehicle_id != self.vehicle_id:
            raise ValueError("vehicle_id must equal raw.vehicle_id")


@dataclass(frozen=True, slots=True)
class AbsenceFence:
    """The single bounded global absence fence (plan §9, REV 2 §9).

    ``query_time_utc_ms`` is the newest authorized full-snapshot queryTime;
    ``seen_ids`` is the **complete** raw seen-ID set of that collection,
    built before filtering and before deduplication.  Its capacity is
    exactly ``max_vehicle_entries`` — overflow rejects the whole
    transaction with ``capacity_exceeded``; no truncation, no weakened
    introduction rule, no extra reason code.

    ``authority_basis`` records the fixed credential-free basis string of
    the :class:`~backend.trimet.feed_types.ReconciliationSnapshot` the
    store constructed after all §9 gates passed (``None`` while no
    authorized pass has ever run).  The wrapper is established with the
    original receipt UTC ms — never a newly read clock — and is proof of
    the passed application gates, not a claim of globally complete
    upstream history.
    """

    query_time_utc_ms: int
    seen_ids: frozenset[int]
    authority_basis: str | None = None

    def __post_init__(self) -> None:
        if not _is_plausible_utc_ms(self.query_time_utc_ms):
            raise ValueError("query_time_utc_ms must be a plausible UTC ms epoch")
        if not isinstance(self.seen_ids, frozenset):
            raise ValueError("seen_ids must be a frozenset of vehicle ids")
        for value in self.seen_ids:
            if not _is_int(value):
                raise ValueError("seen_ids must contain only integer vehicle ids")


# ---------------------------------------------------------------------------
# Latest feed views
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FeedEnvelope:
    """One feed's retained latest source-content view (plan §5).

    ``delivery`` is the original accepted ``PollDelivery`` whose content is
    the current source view; ``batch`` is its S11 ``MatchedBatch`` against
    the context generation recorded in ``matched_generation``; ``stamp``
    repeats the compact source metadata.  This is a fixed slot per feed —
    not a delivery history — and it is never refreshed by replays or
    ignored old deliveries.
    """

    delivery: PollDelivery
    batch: MatchedBatch
    stamp: SourceStamp
    matched_generation: int

    def __post_init__(self) -> None:
        if not isinstance(self.delivery, PollDelivery):
            raise ValueError("delivery must be a PollDelivery")
        if not isinstance(self.stamp, SourceStamp):
            raise ValueError("stamp must be a SourceStamp")
        if not _is_int(self.matched_generation) or self.matched_generation < 1:
            raise ValueError("matched_generation must be a positive integer")


@dataclass(frozen=True, slots=True)
class FeedStateView:
    """One feed's published state slice (plan §5).

    ``source_content`` is the latest accepted source-content envelope
    (``None`` until a first content commit).  ``replay_delivery`` is the
    session's last accepted original delivery for exact-replay comparison —
    ``None`` whenever no session is open, because session replacement
    clears sequence/replay metadata while source-time ordering, vehicles
    and fences survive.  ``source_time_high_water_ms`` is the feed's own
    source-time high-water mark, owned by the store and always distinct
    from S10's watermark.
    """

    feed: PollFeed
    source_content: FeedEnvelope | None
    replay_delivery: PollDelivery | None
    source_time_high_water_ms: int | None


# ---------------------------------------------------------------------------
# Decision telemetry (plan §12)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StateDecisionSample:
    """One bounded, credential-free decision sample (plan §12).

    ``record_key`` is the canonical string id of the affected vehicle or
    entity (``None`` for delivery-level decisions).  ``detail`` is a short
    fixed-phrase string carrying counts only — never payloads, URLs,
    absolute paths or exception reprs.
    """

    feed: PollFeed
    reason: StateReason
    record_key: str | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class StateTelemetry:
    """Bounded published decision telemetry (plan §12).

    ``reason_totals`` are exact per-reason event counts, kept complete
    independently of sample truncation.  ``samples`` holds every event
    while at most ``max_state_diagnostic_samples`` exist; above the cap it
    holds the first ``cap - 1`` samples plus one ``diagnostics_overflow``
    sample, so at the default cap 200 a 201-event stream retains 199 real
    samples and omits 2 (``omitted_samples``).  Counter-only observations
    of replays and ignored old deliveries never appear here: they never
    change the publication revision.
    """

    reason_totals: Mapping[StateReason, int]
    samples: tuple[StateDecisionSample, ...]
    omitted_samples: int
    total_events: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "reason_totals", MappingProxyType(dict(self.reason_totals))
        )


# ---------------------------------------------------------------------------
# Results and the published snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """Outcome of one accepted delivery (plan §6).

    ``authoritative`` is ``True`` only when the delivery performed an
    authorized full-snapshot reconciliation pass.  ``removed_vehicle_ids``
    lists the IDs an authorized pass actually removed, in ascending order.
    """

    feed: PollFeed
    sequence: int
    decision: ApplyDecision
    reasons: tuple[StateReason, ...]
    revision: int
    authoritative: bool = False
    removed_vehicle_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class ContextSwapResult:
    """Outcome of one :meth:`LiveState.replace_context` call (plan §11).

    A same-content candidate (equal ``(archive_sha256, feed_version)``
    identity) is a no-op: ``changed`` is ``False``, the generation and
    revision are unchanged, and nothing was re-matched.  A genuinely new
    context publishes generation ``generation_before + 1`` and revision
    ``+1`` atomically with every re-matched entry and feed view.
    """

    changed: bool
    generation_before: int
    generation_after: int
    revision_after: int
    rematched_vehicle_entries: int
    rematched_feed_views: int


@dataclass(frozen=True, slots=True)
class LiveStateSnapshot:
    """One immutable read view of the whole store (plan §5).

    ``vehicle_entries`` is a read-only mapping copy — caller mutation can
    neither alter publication nor see later commits.  ``max_vehicle_ids``
    is the MAX projection as an ascending tuple of vehicle IDs whose
    current classification is ``MAX_RELEVANT``.  ``prior_state_history``
    is ``"unknown"`` for every fresh store object: an empty new store is
    never an authoritative upstream-empty fleet or a persisted recovery.

    ``expiry_state`` is the S13 :class:`~backend.trimet.freshness_types.
    ExpiryState` (cutoff + cumulative removal/suppression counters) once
    expiry has been explicitly activated by a first successful
    :meth:`~backend.trimet.live_state.LiveState.expire_vehicles` call;
    it is ``None`` (disabled) for every S12-era store and constructor,
    preserving all existing contracts unchanged.
    """

    revision: int
    generation: int
    dataset_identity: DatasetIdentity
    prior_state_history: str
    active_session_id: int | None
    vehicle_entries: Mapping[int, VehicleEntry]
    max_vehicle_ids: tuple[int, ...]
    feed_views: Mapping[PollFeed, FeedStateView]
    absence_fence: AbsenceFence | None
    telemetry: StateTelemetry
    expiry_state: ExpiryState | None = None

    def __post_init__(self) -> None:
        if self.expiry_state is not None and not isinstance(
            self.expiry_state, ExpiryState
        ):
            raise ValueError("expiry_state must be an ExpiryState or None")
        object.__setattr__(
            self, "vehicle_entries", MappingProxyType(dict(self.vehicle_entries))
        )
        object.__setattr__(
            self, "feed_views", MappingProxyType(dict(self.feed_views))
        )
