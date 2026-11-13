"""Immutable S13 freshness/expiry contracts (plan §5–§11; prompt §4–§6).

Conventions mirror ``backend/trimet/state_types.py``: frozen dataclasses
with ``slots=True``, ``tuple``-only collections, and mapping fields
snapshotted into read-only proxies in ``__post_init__`` — immutability is
enforced, not documented-and-hoped.

Dependency discipline (plan §8): this module must **not** import
``live_state``/``state_types`` at runtime; type-only references use
``TYPE_CHECKING``.  The store and state types may depend on this
lightweight module; the evaluator/controller may depend on the store.

Time honesty (plan §6–§7): ages are computed only from a trusted coherent
evaluation sample; nothing here reads a clock.  A future ``expires``
deadline is expected, not a bad future observation timestamp.  Signed
distances are exposed as signed values — no clamping hides skew.
"""
from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from backend.trimet.feed_types import (
    MAX_PLAUSIBLE_UTC_MS,
    MIN_PLAUSIBLE_UTC_MS,
)
from backend.trimet.match_types import DatasetIdentity, RelevanceClassification
from backend.trimet.poll_types import PollFeed

__all__ = [
    "ClockAssessment",
    "ClockStatus",
    "CollectionQuality",
    "ExpiryResult",
    "ExpiryState",
    "FeedFreshnessAssessment",
    "FeedFreshnessStatus",
    "FreshnessPolicy",
    "FreshnessReason",
    "FRESHNESS_REASONS",
    "PositionTimeQuality",
    "TimeSample",
    "VehicleExpiryStatus",
    "VehicleFreshnessAssessment",
]

# Same validated UTC-ms domain as every source model (plan §7).
_MIN_UTC_MS = MIN_PLAUSIBLE_UTC_MS
_MAX_UTC_MS = MAX_PLAUSIBLE_UTC_MS


def _is_int(value: object) -> bool:
    """True for real integers (``bool`` is an ``int`` subclass and excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_nonneg_int(value: object) -> bool:
    return _is_int(value) and value >= 0


def _is_plausible_utc_ms(value: object) -> bool:
    return _is_int(value) and _MIN_UTC_MS <= value <= _MAX_UTC_MS


# ---------------------------------------------------------------------------
# Frozen vocabularies (plan §11)
# ---------------------------------------------------------------------------


class FeedFreshnessStatus(enum.Enum):
    """One feed's upstream-data age status (plan §6).

    ``fresh`` means the accepted source-content age is at or below the
    feed's 3× poll-interval threshold (equality is fresh); ``stale`` means
    strictly beyond it.  ``unknown`` covers no accepted source content, an
    untrusted/incoherent time basis, and a source timestamp ahead of the
    trusted evaluation UTC beyond the source-future tolerance.
    """

    UNKNOWN = "unknown"
    FRESH = "fresh"
    STALE = "stale"


class PositionTimeQuality(enum.Enum):
    """Time quality of one retained raw position timestamp (plan §6).

    ``known`` means the position age could be measured against trusted
    coherent evaluation UTC (a mildly ahead timestamp still counts, with
    its exact skew exposed).  ``unknown`` covers untrusted time and a
    position timestamp ahead beyond the tolerance.  Age is a measurement
    only: no automatic position-old flag, warning threshold or
    position FRESH/STALE classification exists (plan §2, §6).
    """

    KNOWN = "known"
    UNKNOWN = "unknown"


class VehicleExpiryStatus(enum.Enum):
    """One retained vehicle's expiry eligibility (plan §10).

    Determined only from trusted coherent evaluation time and the
    required raw ``expires_utc_ms`` deadline: ``expired`` at deadline
    ``<=`` evaluation UTC, ``unexpired`` strictly after.  ``unknown``
    covers untrusted/incoherent time.  Independent of retained, assessed
    and visible membership.
    """

    UNEXPIRED = "unexpired"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class ClockStatus(enum.Enum):
    """The controller's explicit clock-trust assessment (plan §7).

    ``uninitialized``: no validated sample has ever anchored the
    controller.  ``trusted``: the current sample is coherent with the
    anchored wall/monotonic pair.  ``untrusted``: sticky distrust after a
    backwards movement, divergence, or provider failure — only explicit
    epoch-checked recovery restores trust.
    """

    UNINITIALIZED = "uninitialized"
    TRUSTED = "trusted"
    UNTRUSTED = "untrusted"


class CollectionQuality(enum.Enum):
    """Feed-level accepted-collection completeness (plan §6).

    Derived only from the accepted vehicle source-content envelope's
    original evidence paired with that envelope's original snapshot,
    using the existing literal boolean/count-consistency contract.
    Descriptive only: it never creates reconciliation authority.
    """

    COMPLETE = "complete"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class FreshnessReason(enum.Enum):
    """Frozen S13 reason vocabulary (plan §11; 13 exact spellings).

    Owned by S13 — never added to S12's frozen reason enums.
    """

    NO_SOURCE_CONTENT = "no_source_content"
    NO_REPLAY_DELIVERY = "no_replay_delivery"
    SOURCE_TIME_AHEAD = "source_time_ahead"
    POSITION_TIME_AHEAD = "position_time_ahead"
    RECEIPT_TIME_AHEAD = "receipt_time_ahead"
    CLOCK_UNINITIALIZED = "clock_uninitialized"
    CLOCK_READ_FAILED = "clock_read_failed"
    INVALID_CLOCK_SAMPLE = "invalid_clock_sample"
    CLOCK_MOVED_BACKWARDS = "clock_moved_backwards"
    CLOCK_DIVERGED = "clock_diverged"
    UTC_BEFORE_EXPIRY_CUTOFF = "utc_before_expiry_cutoff"
    SAMPLE_BEFORE_SNAPSHOT_CUTOFF = "sample_before_snapshot_cutoff"
    CLEANUP_DEFERRED = "cleanup_deferred"


FRESHNESS_REASONS: frozenset[FreshnessReason] = frozenset(FreshnessReason)


# ---------------------------------------------------------------------------
# Validated value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FreshnessPolicy:
    """Validated immutable freshness policy (plan §5, §7).

    Poll intervals are the configured polling cadences in integer
    milliseconds; the staleness rule is fixed at **3× interval**
    (owner-selected, plan §2) — the factor is not an independent setting.
    The two tolerances are explicit application policies, not measured
    provider guarantees (plan §7):

    - ``source_future_tolerance_ms``: how far ahead of trusted evaluation
      UTC a source/position timestamp may sit and still measure as age 0
      with exact exposed skew.
    - ``clock_divergence_tolerance_ms``: how far wall-clock elapsed may
      drift from monotonic elapsed before the controller distrusts its
      clock (equality is permitted; strictly greater diverges).
    """

    vehicles_poll_ms: int = 10000
    tripupdates_poll_ms: int = 60000
    alerts_poll_ms: int = 120000
    source_future_tolerance_ms: int = 5000
    clock_divergence_tolerance_ms: int = 5000

    def __post_init__(self) -> None:
        for field in (
            "vehicles_poll_ms",
            "tripupdates_poll_ms",
            "alerts_poll_ms",
        ):
            value = getattr(self, field)
            if not _is_int(value) or value < 1:
                raise ValueError(
                    f"{field} must be a positive integer excluding bool, "
                    f"got {value!r}"
                )
        for field in (
            "source_future_tolerance_ms",
            "clock_divergence_tolerance_ms",
        ):
            value = getattr(self, field)
            if not _is_nonneg_int(value):
                raise ValueError(
                    f"{field} must be a nonnegative integer excluding bool, "
                    f"got {value!r}"
                )

    def poll_interval_ms(self, feed: PollFeed) -> int:
        """The configured poll interval for one feed."""
        if feed is PollFeed.VEHICLES:
            return self.vehicles_poll_ms
        if feed is PollFeed.TRIPUPDATES:
            return self.tripupdates_poll_ms
        if feed is PollFeed.ALERTS:
            return self.alerts_poll_ms
        raise ValueError(f"unknown feed {feed!r}")


@dataclass(frozen=True, slots=True)
class TimeSample:
    """One validated wall/monotonic time pair in integer milliseconds.

    ``utc_ms`` is a plausible UTC epoch millisecond integer (``bool``
    excluded) using the source models' existing range; ``monotonic_ms``
    is a nonnegative integer (``bool`` excluded).  No floats, no
    infinity/overflow fallback.
    """

    utc_ms: int
    monotonic_ms: int

    def __post_init__(self) -> None:
        if not _is_plausible_utc_ms(self.utc_ms):
            raise ValueError(
                "utc_ms must be a plausible UTC ms epoch excluding bool, "
                f"got {self.utc_ms!r}"
            )
        if not _is_nonneg_int(self.monotonic_ms):
            raise ValueError(
                "monotonic_ms must be a nonnegative integer excluding bool, "
                f"got {self.monotonic_ms!r}"
            )


@dataclass(frozen=True, slots=True)
class ClockAssessment:
    """An explicit, validated clock-trust assertion (plan §5, §7).

    The caller/controller owns this assessment; the evaluator never infers
    trust from a plausible integer or a truthy flag.  ``epoch`` advances on
    every explicit successful recovery and is never reused.  ``reasons``
    carry the bounded fixed causes from :class:`FreshnessReason`.

    ``trusted`` requires an actual sample at evaluation time (enforced by
    the evaluator, not here — an assessment alone carries no sample).
    """

    status: ClockStatus
    epoch: int
    reasons: tuple[FreshnessReason, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, ClockStatus):
            raise ValueError("status must be a ClockStatus")
        if not _is_int(self.epoch) or self.epoch < 0:
            raise ValueError(
                "epoch must be a nonnegative integer excluding bool, "
                f"got {self.epoch!r}"
            )
        if self.status is not ClockStatus.UNINITIALIZED and self.epoch < 1:
            raise ValueError(
                "a trusted/untrusted assessment requires a positive epoch, "
                f"got {self.epoch!r}"
            )
        reasons = tuple(self.reasons)
        for reason in reasons:
            if not isinstance(reason, FreshnessReason):
                raise ValueError(
                    "reasons must contain only FreshnessReason values"
                )
        object.__setattr__(self, "reasons", reasons)


@dataclass(frozen=True, slots=True)
class ExpiryState:
    """Published expiry metadata carried on every store root (plan §8).

    ``cutoff_utc_ms`` is the newest trusted cleanup cutoff — nondecreasing
    for the store object's lifetime; it never lowers and never revives
    records.  ``removed_entries_total`` counts IDs actually removed by
    published cleanup operations (once per removal event).  Counts are
    cumulative for the store object's lifetime; a new store loses this
    history and marks prior history unknown (plan §12).
    """

    cutoff_utc_ms: int
    removed_entries_total: int
    ignored_expired_winners_total: int

    def __post_init__(self) -> None:
        if not _is_plausible_utc_ms(self.cutoff_utc_ms):
            raise ValueError(
                "cutoff_utc_ms must be a plausible UTC ms epoch excluding "
                f"bool, got {self.cutoff_utc_ms!r}"
            )
        for field in ("removed_entries_total", "ignored_expired_winners_total"):
            value = getattr(self, field)
            if not _is_nonneg_int(value):
                raise ValueError(
                    f"{field} must be a nonnegative integer excluding bool, "
                    f"got {value!r}"
                )


@dataclass(frozen=True, slots=True)
class ExpiryResult:
    """Outcome of one :meth:`~backend.trimet.live_state.LiveState.expire_vehicles`
    call (plan §8).

    ``changed`` is ``True`` exactly when the operation published a new root
    (cutoff activation or strictly increased cutoff) — including a
    zero-removal advance, which is a real root mutation.  An idempotent
    equal/older cutoff publishes nothing and reports the actual effective
    cutoff/revision.  A deferred attempt (store lost the publication race)
    also reports ``changed=False`` with the winning revision and no
    invented removals.
    """

    requested_cutoff_utc_ms: int
    effective_cutoff_utc_ms: int | None
    revision_before: int
    revision_after: int
    changed: bool
    removed_vehicle_ids: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if not _is_plausible_utc_ms(self.requested_cutoff_utc_ms):
            raise ValueError(
                "requested_cutoff_utc_ms must be a plausible UTC ms epoch"
            )
        if self.effective_cutoff_utc_ms is not None and not _is_plausible_utc_ms(
            self.effective_cutoff_utc_ms
        ):
            raise ValueError(
                "effective_cutoff_utc_ms must be a plausible UTC ms epoch "
                "or None"
            )
        for field in ("revision_before", "revision_after"):
            value = getattr(self, field)
            if not _is_nonneg_int(value):
                raise ValueError(f"{field} must be a nonnegative integer")
        if not isinstance(self.changed, bool):
            raise ValueError("changed must be a boolean")
        ids = tuple(self.removed_vehicle_ids)
        if ids != tuple(sorted(ids)):
            raise ValueError("removed_vehicle_ids must be ascending")
        for value in ids:
            if not _is_int(value):
                raise ValueError(
                    "removed_vehicle_ids must contain only integer ids"
                )
        object.__setattr__(self, "removed_vehicle_ids", ids)


# ---------------------------------------------------------------------------
# Per-feed and per-vehicle assessments
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FeedFreshnessAssessment:
    """One feed's immutable freshness assessment (plan §5–§6).

    Every age is a measurement only.  ``source_age_ms`` is nonnegative
    when known (a mildly ahead source timestamp measures as age 0 with its
    exact ``ahead_by_ms`` skew exposed); ``None`` with an explanatory
    reason otherwise.  ``threshold_ms`` repeats ``3 * poll_interval_ms``
    for the classification actually applied.  ``request_elapsed_ms`` is
    the existing source-stamp request duration — never relabelled as
    end-to-end API delivery latency.

    ``collection_quality`` is the feed-level accepted-collection
    attribute for vehicles; other feeds and missing evidence are
    ``unknown``.  It never implies per-vehicle quality or authority.
    """

    feed: PollFeed
    status: FeedFreshnessStatus
    source_age_ms: int | None = None
    source_content_receipt_age_ms: int | None = None
    last_booked_delivery_age_ms: int | None = None
    request_elapsed_ms: int | None = None
    ahead_by_ms: int | None = None
    threshold_ms: int | None = None
    collection_quality: CollectionQuality = CollectionQuality.UNKNOWN
    reasons: tuple[FreshnessReason, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.feed, PollFeed):
            raise ValueError("feed must be a PollFeed")
        if not isinstance(self.status, FeedFreshnessStatus):
            raise ValueError("status must be a FeedFreshnessStatus")
        for field in (
            "source_age_ms",
            "source_content_receipt_age_ms",
            "last_booked_delivery_age_ms",
            "ahead_by_ms",
            "threshold_ms",
        ):
            value = getattr(self, field)
            if value is not None and not _is_nonneg_int(value):
                raise ValueError(
                    f"{field} must be a nonnegative integer or None "
                    f"excluding bool, got {value!r}"
                )
        if self.request_elapsed_ms is not None and not _is_nonneg_int(
            self.request_elapsed_ms
        ):
            raise ValueError(
                "request_elapsed_ms must be a nonnegative integer or None"
            )
        if not isinstance(self.collection_quality, CollectionQuality):
            raise ValueError("collection_quality must be a CollectionQuality")
        reasons = tuple(self.reasons)
        for reason in reasons:
            if not isinstance(reason, FreshnessReason):
                raise ValueError(
                    "reasons must contain only FreshnessReason values"
                )
        object.__setattr__(self, "reasons", reasons)
        if (
            self.status is FeedFreshnessStatus.FRESH
            or self.status is FeedFreshnessStatus.STALE
        ) and (
            self.source_age_ms is None
            or self.threshold_ms is None
            or FreshnessReason.SOURCE_TIME_AHEAD in reasons
        ):
            raise ValueError(
                "a FRESH/STALE status requires a measured nonnegative age, "
                "its applied threshold and no overriding source-age reason "
                "(metric-availability annotations such as "
                "no_replay_delivery / receipt_time_ahead may coexist)"
            )
        if self.ahead_by_ms is not None and self.source_age_ms not in (
            None,
            0,
        ):
            raise ValueError(
                "ahead_by_ms may coexist only with an unavailable age "
                "(beyond tolerance) or a clamped age of exactly 0 "
                "(within tolerance); a positive age excludes exposed skew"
            )


@dataclass(frozen=True, slots=True)
class VehicleFreshnessAssessment:
    """One retained vehicle's immutable time/expiry assessment (plan §5).

    Covers exactly the IDs in the assessed snapshot's ``vehicle_entries``
    across all S11 relevance classes — assessment is not visibility.
    ``position_age_ms`` is a measurement only (no position-old flag or
    threshold); ``expires_in_ms`` is the signed deadline distance
    (``expires_utc_ms - evaluation_utc_ms``, negative when expired).
    """

    vehicle_id: int
    classification: RelevanceClassification
    position_age_ms: int | None = None
    position_time_quality: PositionTimeQuality = PositionTimeQuality.UNKNOWN
    position_ahead_by_ms: int | None = None
    expires_in_ms: int | None = None
    expiry_status: VehicleExpiryStatus = VehicleExpiryStatus.UNKNOWN
    reasons: tuple[FreshnessReason, ...] = ()

    def __post_init__(self) -> None:
        if not _is_int(self.vehicle_id):
            raise ValueError("vehicle_id must be an integer")
        if not isinstance(self.classification, RelevanceClassification):
            raise ValueError(
                "classification must be a RelevanceClassification"
            )
        if not isinstance(self.position_time_quality, PositionTimeQuality):
            raise ValueError(
                "position_time_quality must be a PositionTimeQuality"
            )
        for field in ("position_age_ms", "position_ahead_by_ms"):
            value = getattr(self, field)
            if value is not None and not _is_nonneg_int(value):
                raise ValueError(
                    f"{field} must be a nonnegative integer or None "
                    f"excluding bool, got {value!r}"
                )
        if self.expires_in_ms is not None and not _is_int(self.expires_in_ms):
            raise ValueError(
                "expires_in_ms must be a signed integer or None excluding "
                f"bool, got {self.expires_in_ms!r}"
            )
        if not isinstance(self.expiry_status, VehicleExpiryStatus):
            raise ValueError("expiry_status must be a VehicleExpiryStatus")
        reasons = tuple(self.reasons)
        for reason in reasons:
            if not isinstance(reason, FreshnessReason):
                raise ValueError(
                    "reasons must contain only FreshnessReason values"
                )
        object.__setattr__(self, "reasons", reasons)
        if (
            self.position_time_quality is PositionTimeQuality.KNOWN
            and self.position_age_ms is None
        ):
            raise ValueError(
                "known position time quality requires a measured "
                "nonnegative age"
            )
        if (
            self.position_ahead_by_ms is not None
            and self.position_age_ms not in (None, 0)
        ):
            raise ValueError(
                "position_ahead_by_ms may coexist only with an "
                "unavailable age (beyond tolerance) or a clamped age of "
                "exactly 0 (within tolerance); a positive age excludes "
                "exposed skew"
            )


@dataclass(frozen=True, slots=True)
class FreshnessSnapshot:
    """One immutable freshness evaluation of a captured store snapshot.

    ``sample_utc_ms`` is the trusted coherent evaluation instant, or
    ``None`` when time is unavailable (uninitialized/untrusted clock,
    provider failure, or a sample older than the captured cutoff — the
    bounded inconsistent-sample outcome).  ``expiry_result`` is the actual
    expiry outcome of this evaluation cycle (``None`` when no cleanup was
    attempted, e.g. untrusted time); it distinguishes real pruning from
    the pure derived ``visible_max_vehicle_ids`` filter, which never
    claims the underlying store was physically pruned.

    ``top_reasons`` carries cycle-level causes beyond the per-feed and
    per-vehicle records (``cleanup_deferred``,
    ``sample_before_snapshot_cutoff``).  Previously returned snapshots and
    the underlying store state never change.
    """

    revision: int
    generation: int
    dataset_identity: DatasetIdentity
    clock_assessment: ClockAssessment
    sample_utc_ms: int | None
    feed_assessments: Mapping[PollFeed, FeedFreshnessAssessment]
    vehicle_assessments: Mapping[int, VehicleFreshnessAssessment]
    visible_max_vehicle_ids: tuple[int, ...]
    expiry_state: ExpiryState | None
    expiry_result: ExpiryResult | None
    top_reasons: tuple[FreshnessReason, ...] = ()

    def __post_init__(self) -> None:
        for field in ("revision", "generation"):
            value = getattr(self, field)
            if not _is_int(value) or value < 0:
                raise ValueError(f"{field} must be a nonnegative integer")
        if not isinstance(self.dataset_identity, DatasetIdentity):
            raise ValueError("dataset_identity must be a DatasetIdentity")
        if not isinstance(self.clock_assessment, ClockAssessment):
            raise ValueError("clock_assessment must be a ClockAssessment")
        if self.sample_utc_ms is not None and not _is_plausible_utc_ms(
            self.sample_utc_ms
        ):
            raise ValueError(
                "sample_utc_ms must be a plausible UTC ms epoch or None"
            )
        object.__setattr__(
            self,
            "feed_assessments",
            MappingProxyType(dict(self.feed_assessments)),
        )
        object.__setattr__(
            self,
            "vehicle_assessments",
            MappingProxyType(dict(self.vehicle_assessments)),
        )
        ids = tuple(self.visible_max_vehicle_ids)
        if ids != tuple(sorted(ids)):
            raise ValueError("visible_max_vehicle_ids must be ascending")
        for value in ids:
            if not _is_int(value):
                raise ValueError(
                    "visible_max_vehicle_ids must contain only integer ids"
                )
        object.__setattr__(self, "visible_max_vehicle_ids", ids)
        if self.expiry_state is not None and not isinstance(
            self.expiry_state, ExpiryState
        ):
            raise ValueError("expiry_state must be an ExpiryState or None")
        if self.expiry_result is not None and not isinstance(
            self.expiry_result, ExpiryResult
        ):
            raise ValueError("expiry_result must be an ExpiryResult or None")
        reasons = tuple(self.top_reasons)
        for reason in reasons:
            if not isinstance(reason, FreshnessReason):
                raise ValueError(
                    "top_reasons must contain only FreshnessReason values"
                )
        object.__setattr__(self, "top_reasons", reasons)
