"""S13 pure freshness evaluator, clock boundary and controller (plan §5–§7, §10).

Layering (plan §5):

- :func:`evaluate_freshness` is **pure**: it reads one immutable store
  snapshot plus caller-supplied time inputs and returns a frozen
  assessment.  It never performs cleanup, never reads a clock and never
  mutates anything.
- :class:`SystemFreshnessClock` is the only clock-reading boundary in
  production code: integer UTC-ms wall time plus integer monotonic
  milliseconds, sampled only on explicit controller calls.
- :class:`FreshnessController` owns the explicit trust assessment and is
  the normal owner of cleanup invocation: trusted coherent time may
  advance the store's expiry cutoff; untrusted time never drives deletion
  (plan §7).

Clock honesty (plan §7): the first valid sample establishes relative
consistency under the explicit **system UTC assumption**.  A wall/monotonic
comparison cannot prove the absolute UTC is correct, detect every sleep
implementation, or undo an erroneous deletion — distrust is sticky and
only explicit epoch-checked recovery restores trust.
"""
from __future__ import annotations

import calendar
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Protocol

from backend.trimet.feed_types import VehiclesSnapshot
from backend.trimet.live_config import LiveConfig
from backend.trimet.freshness_errors import (
    FreshnessInputError,
    FreshnessRecoveryError,
)
from backend.trimet.freshness_types import (
    ClockAssessment,
    ClockStatus,
    CollectionQuality,
    ExpiryResult,
    FeedFreshnessAssessment,
    FeedFreshnessStatus,
    FreshnessPolicy,
    FreshnessReason,
    FreshnessSnapshot,
    PositionTimeQuality,
    TimeSample,
    VehicleExpiryStatus,
    VehicleFreshnessAssessment,
)
from backend.trimet.live_state import LiveState
from backend.trimet.match_types import RelevanceClassification
from backend.trimet.poll_types import PollDelivery, PollFeed, VehicleCollectionEvidence
from backend.trimet.state_errors import StateConflictError
from backend.trimet.state_types import FeedStateView, LiveStateSnapshot

__all__ = [
    "FreshnessClock",
    "FreshnessController",
    "SystemFreshnessClock",
    "evaluate_freshness",
    "freshness_policy_from_config",
]

# Fixed owner-selected staleness rule (plan §2): a source is stale when its
# accepted upstream data age exceeds 3× its configured poll interval;
# equality is fresh.  Not an independent tuning parameter.
STALE_FACTOR = 3


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


def _is_int(value: object) -> bool:
    """True for real integers (``bool`` is an ``int`` subclass and excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_nonneg_int(value: object) -> bool:
    return _is_int(value) and value >= 0


def _validate_policy(policy: object) -> None:
    if not isinstance(policy, FreshnessPolicy):
        raise FreshnessInputError(
            "policy must be a FreshnessPolicy", code="invalid_policy"
        )


def freshness_policy_from_config(config: object) -> "FreshnessPolicy":
    """Adapt existing poll-interval configuration into a policy (plan §5).

    Reads **only** the three existing integer poll-interval fields from
    the supplied :class:`~backend.trimet.live_config.LiveConfig` and
    converts seconds to milliseconds with integer arithmetic.  It never
    loads configuration, reads an environment file, touches credentials
    or changes polling; tolerance defaults come from
    :class:`~backend.trimet.freshness_types.FreshnessPolicy`.  S14 must
    compose the policy from the same validated config used by polling so
    custom intervals produce corresponding custom stale thresholds.
    """
    if not isinstance(config, LiveConfig):
        raise FreshnessInputError(
            "config must be a LiveConfig", code="invalid_policy"
        )
    intervals_ms: dict[str, int] = {}
    for field, attr in (
        ("vehicles_poll_ms", "vehicles_poll_seconds"),
        ("tripupdates_poll_ms", "tripupdates_poll_seconds"),
        ("alerts_poll_ms", "alerts_poll_seconds"),
    ):
        seconds = getattr(config, attr)
        if not _is_int(seconds) or seconds < 1:
            raise FreshnessInputError(
                f"config {attr} must be a positive integer excluding bool",
                code="invalid_policy",
            )
        intervals_ms[field] = seconds * 1000
    return FreshnessPolicy(
        vehicles_poll_ms=intervals_ms["vehicles_poll_ms"],
        tripupdates_poll_ms=intervals_ms["tripupdates_poll_ms"],
        alerts_poll_ms=intervals_ms["alerts_poll_ms"],
    )


# ---------------------------------------------------------------------------
# Clock boundary (plan §7)
# ---------------------------------------------------------------------------


class FreshnessClock(Protocol):
    """Structural provider of validated wall/monotonic time samples."""

    def sample(self) -> TimeSample: ...


class SystemFreshnessClock:
    """Production clock adapter: exact integer UTC and monotonic ms.

    UTC comes from exact ``datetime`` arithmetic (``calendar.timegm`` on
    the UTC structure plus the sub-second milliseconds) — no float
    timestamp conversion.  Monotonic time is ``monotonic_ns() //
    1_000_000`` in integer milliseconds.  Sampling happens only when
    :meth:`sample` is explicitly called — never on import or construction.
    """

    def sample(self) -> TimeSample:
        now = datetime.now(tz=timezone.utc)
        utc_ms = calendar.timegm(now.utctimetuple()) * 1000 + now.microsecond // 1000
        monotonic_ms = time.monotonic_ns() // 1_000_000
        return TimeSample(utc_ms=utc_ms, monotonic_ms=monotonic_ms)


def _validate_clock(clock: object) -> None:
    """A supplied provider must expose a callable ``.sample()`` (plan §5)."""
    if not hasattr(clock, "sample") or not callable(
        getattr(clock, "sample")
    ):
        raise FreshnessInputError(
            "clock must provide a callable sample() method",
            code="invalid_clock",
        )


def _take_sample(clock: FreshnessClock) -> tuple[TimeSample | None, FreshnessReason | None]:
    """One provider read at the explicit boundary (plan §7).

    Provider exceptions and invalid returns become bounded reasons;
    arbitrary failures are never converted into clock success.
    """
    try:
        sample = clock.sample()
    except Exception:
        return None, FreshnessReason.CLOCK_READ_FAILED
    if not isinstance(sample, TimeSample):
        return None, FreshnessReason.INVALID_CLOCK_SAMPLE
    return sample, None


# ---------------------------------------------------------------------------
# Pure evaluator (plan §5–§6)
# ---------------------------------------------------------------------------


def _coherent_time_inputs(
    snapshot: LiveStateSnapshot,
    *,
    sample: TimeSample | None,
    clock_assessment: ClockAssessment,
) -> tuple[int | None, tuple[FreshnessReason, ...]]:
    """Validate and reconcile the caller's time inputs (plan §5, §7).

    Returns ``(evaluation_utc_ms, top_reasons)``.  ``None`` means time is
    unavailable: every UTC-derived measurement is UNKNOWN and expiry
    eligibility cannot be claimed.  A trusted assessment requires a
    sample; a non-trusted assessment forbids one — a supplied sample
    cannot upgrade an untrusted assertion.  A sample older than the
    captured snapshot's expiry cutoff is the bounded
    ``sample_before_snapshot_cutoff`` outcome, never negative ages.
    """
    if not isinstance(clock_assessment, ClockAssessment):
        raise FreshnessInputError(
            "clock_assessment must be a ClockAssessment",
            code="invalid_sample",
        )
    status = clock_assessment.status
    if status is ClockStatus.TRUSTED:
        if sample is None:
            raise FreshnessInputError(
                "a trusted clock assessment requires a valid TimeSample",
                code="invalid_sample",
            )
        if not isinstance(sample, TimeSample):
            # A trusted assertion must carry an actually validated
            # TimeSample; arbitrary objects never pass (plan §5, §7).
            raise FreshnessInputError(
                "sample must be a validated TimeSample",
                code="invalid_sample",
            )
    else:
        if sample is not None:
            raise FreshnessInputError(
                "a sample is permitted only with a trusted clock assessment",
                code="invalid_sample",
            )
    top: list[FreshnessReason] = list(clock_assessment.reasons)
    if status is ClockStatus.UNINITIALIZED and FreshnessReason.CLOCK_UNINITIALIZED not in top:
        top.append(FreshnessReason.CLOCK_UNINITIALIZED)
    if sample is not None:
        expiry_state = snapshot.expiry_state
        if (
            expiry_state is not None
            and sample.utc_ms < expiry_state.cutoff_utc_ms
        ):
            # Incoherent: the evaluation instant precedes the store's
            # accepted cutoff.  No trusted current view, no negative ages,
            # no lowered cutoff, no claimed cleanup (plan §7).
            if (
                FreshnessReason.SAMPLE_BEFORE_SNAPSHOT_CUTOFF not in top
            ):
                top.append(FreshnessReason.SAMPLE_BEFORE_SNAPSHOT_CUTOFF)
            return None, tuple(top)
        return sample.utc_ms, tuple(top)
    return None, tuple(top)


def _ahead_or_age(
    source_utc_ms: int,
    evaluation_utc_ms: int,
    future_tolerance_ms: int,
) -> tuple[int | None, int | None, bool]:
    """Classify one source-side timestamp against evaluation UTC (plan §5).

    Returns ``(age_ms, ahead_by_ms, beyond_tolerance)``.  A timestamp
    ahead by at most the tolerance measures as age 0 with its exact skew
    exposed; beyond the tolerance the age is unavailable.
    """
    delta = evaluation_utc_ms - source_utc_ms
    if delta >= 0:
        return delta, None, False
    ahead = -delta
    if ahead <= future_tolerance_ms:
        return 0, ahead, False
    return None, ahead, True


def _collection_quality(delivery: PollDelivery | None) -> CollectionQuality:
    """Feed-level accepted-collection completeness (plan §6).

    Uses the existing literal boolean/count-consistency contract on the
    accepted vehicle envelope's own evidence paired with that envelope's
    original snapshot.  Descriptive only: never authority, never a
    per-vehicle claim.
    """
    if delivery is None:
        return CollectionQuality.UNKNOWN
    evidence = delivery.evidence
    if not isinstance(evidence, VehicleCollectionEvidence):
        return CollectionQuality.UNKNOWN
    if not isinstance(evidence.key_present, bool) or not isinstance(
        evidence.is_list, bool
    ):
        return CollectionQuality.UNKNOWN
    if evidence.key_present is not True or evidence.is_list is not True:
        return CollectionQuality.UNKNOWN
    model = delivery.model
    if not isinstance(model, VehiclesSnapshot):
        return CollectionQuality.UNKNOWN
    parsed = len(model.vehicles)
    raw_count = evidence.raw_count
    rejected_count = evidence.rejected_count
    if not _is_nonneg_int(raw_count) or not _is_nonneg_int(rejected_count):
        return CollectionQuality.UNKNOWN
    if rejected_count == 0 and raw_count == parsed:
        return CollectionQuality.COMPLETE
    return CollectionQuality.PARTIAL


def _receipt_ages(
    view: "FeedStateView",
    stamp,
    evaluation_utc_ms: int,
) -> tuple[int | None, int | None, list[FreshnessReason]]:
    """Local receipt metrics from already accepted provenance (plan §5).

    Returns ``(receipt_age_ms, booked_age_ms, annotation_reasons)``.
    These ages are derived from the trusted evaluation instant and each
    record's own receipt time — independent of the source-age
    classification, so a suspect upstream source timestamp does not
    erase them.  A future receipt makes only that metric unavailable
    with its fixed annotation reason.
    """
    reasons: list[FreshnessReason] = []
    receipt_raw = evaluation_utc_ms - stamp.receipt_utc_ms
    receipt_age = None
    if receipt_raw < 0:
        reasons.append(FreshnessReason.RECEIPT_TIME_AHEAD)
    else:
        receipt_age = receipt_raw
    booked_age = None
    replay = view.replay_delivery
    if replay is None:
        reasons.append(FreshnessReason.NO_REPLAY_DELIVERY)
    else:
        booked_raw = evaluation_utc_ms - replay.receipt_utc_ms
        if booked_raw < 0:
            reasons.append(FreshnessReason.RECEIPT_TIME_AHEAD)
        else:
            booked_age = booked_raw
    return receipt_age, booked_age, reasons


def evaluate_freshness(
    snapshot: LiveStateSnapshot,
    *,
    sample: TimeSample | None,
    policy: "FreshnessPolicy",
    clock_assessment: ClockAssessment,
) -> "FreshnessSnapshot":
    """Purely assess one captured store snapshot (plan §5–§6, §10).

    No cleanup, no clock reads, no mutation.  With trusted coherent time,
    source ages classify against each feed's 3× poll-interval threshold
    and expiry eligibility is exact.  With unavailable time, ages and
    eligibility are UNKNOWN and the remaining store projection is
    retained honestly — never claimed proven-unexpired and never cleared.
    """
    _validate_policy(policy)
    if not isinstance(snapshot, LiveStateSnapshot):
        raise FreshnessInputError(
            "snapshot must be a LiveStateSnapshot", code="invalid_snapshot"
        )
    evaluation_utc, top = _coherent_time_inputs(
        snapshot, sample=sample, clock_assessment=clock_assessment
    )
    trusted = evaluation_utc is not None
    tolerance = policy.source_future_tolerance_ms

    feed_assessments: dict[PollFeed, FeedFreshnessAssessment] = {}
    for feed in PollFeed:
        view = snapshot.feed_views[feed]
        if not trusted:
            # Unavailable time: report the bounded cause, retain the
            # projection without FRESH/STALE claims (plan §10).  UTC-
            # derived ages stay hidden; the recorded request duration
            # and the feed-level accepted-collection quality are facts
            # whose own inputs remain valid, so they are kept.
            source_content = view.source_content
            feed_assessments[feed] = FeedFreshnessAssessment(
                feed=feed,
                status=FeedFreshnessStatus.UNKNOWN,
                collection_quality=(
                    _collection_quality(
                        source_content.delivery
                        if source_content is not None
                        else None
                    )
                    if feed is PollFeed.VEHICLES
                    else CollectionQuality.UNKNOWN
                ),
                request_elapsed_ms=(
                    source_content.stamp.elapsed_mono_ms
                    if source_content is not None
                    else None
                ),
                reasons=tuple(top),
            )
            continue
        source_content = view.source_content
        if source_content is None:
            feed_assessments[feed] = FeedFreshnessAssessment(
                feed=feed,
                status=FeedFreshnessStatus.UNKNOWN,
                reasons=(FreshnessReason.NO_SOURCE_CONTENT,),
            )
            continue
        stamp = source_content.stamp
        threshold = STALE_FACTOR * policy.poll_interval_ms(feed)
        age, ahead, beyond = _ahead_or_age(
            stamp.source_time_utc_ms, evaluation_utc, tolerance
        )
        if beyond:
            # The upstream source timestamp is beyond tolerance: the
            # source age stays UNKNOWN with its fixed reason, but every
            # independent fact with valid inputs of its own is retained —
            # the trusted local receipt ages, the recorded request
            # duration and the accepted collection's own completeness
            # evidence (plan §5–§6).
            receipt_age, booked_age, annotations = _receipt_ages(
                view, stamp, evaluation_utc
            )
            ahead_reasons = tuple(
                dict.fromkeys(
                    (FreshnessReason.SOURCE_TIME_AHEAD, *annotations)
                )
            )
            feed_assessments[feed] = FeedFreshnessAssessment(
                feed=feed,
                status=FeedFreshnessStatus.UNKNOWN,
                ahead_by_ms=ahead,
                source_content_receipt_age_ms=receipt_age,
                last_booked_delivery_age_ms=booked_age,
                request_elapsed_ms=stamp.elapsed_mono_ms,
                collection_quality=(
                    _collection_quality(source_content.delivery)
                    if feed is PollFeed.VEHICLES
                    else CollectionQuality.UNKNOWN
                ),
                reasons=ahead_reasons,
            )
            continue
        status = (
            FeedFreshnessStatus.FRESH
            if age <= threshold
            else FeedFreshnessStatus.STALE
        )
        receipt_age, booked_age, annotations = _receipt_ages(
            view, stamp, evaluation_utc
        )
        reasons: list[FreshnessReason] = list(annotations)
        collection_quality = (
            _collection_quality(source_content.delivery)
            if feed is PollFeed.VEHICLES
            else CollectionQuality.UNKNOWN
        )
        feed_assessments[feed] = FeedFreshnessAssessment(
            feed=feed,
            status=status,
            source_age_ms=age,
            source_content_receipt_age_ms=receipt_age,
            last_booked_delivery_age_ms=booked_age,
            request_elapsed_ms=stamp.elapsed_mono_ms,
            ahead_by_ms=ahead,
            threshold_ms=threshold,
            collection_quality=collection_quality,
            reasons=tuple(reasons),
        )

    vehicle_assessments: dict[int, VehicleFreshnessAssessment] = {}
    for vehicle_id, entry in snapshot.vehicle_entries.items():
        classification = entry.matched.classification
        if not isinstance(classification, RelevanceClassification):
            classification = RelevanceClassification.UNCLASSIFIED
        if not trusted:
            vehicle_assessments[vehicle_id] = VehicleFreshnessAssessment(
                vehicle_id=vehicle_id,
                classification=classification,
            )
            continue
        pos_age, pos_ahead, pos_beyond = _ahead_or_age(
            entry.raw.time_utc_ms, evaluation_utc, tolerance
        )
        if pos_beyond:
            position_quality = PositionTimeQuality.UNKNOWN
            pos_age = None
            vehicle_reasons = [FreshnessReason.POSITION_TIME_AHEAD]
        else:
            position_quality = PositionTimeQuality.KNOWN
            vehicle_reasons = []
        expires = entry.raw.expires_utc_ms
        expires_in = expires - evaluation_utc
        expiry_status = (
            VehicleExpiryStatus.EXPIRED
            if expires <= evaluation_utc
            else VehicleExpiryStatus.UNEXPIRED
        )
        vehicle_assessments[vehicle_id] = VehicleFreshnessAssessment(
            vehicle_id=vehicle_id,
            classification=classification,
            position_age_ms=pos_age,
            position_time_quality=position_quality,
            position_ahead_by_ms=pos_ahead,
            expires_in_ms=expires_in,
            expiry_status=expiry_status,
            reasons=tuple(vehicle_reasons),
        )

    if trusted:
        visible = tuple(
            vehicle_id
            for vehicle_id in snapshot.max_vehicle_ids
            if snapshot.vehicle_entries[vehicle_id].raw.expires_utc_ms
            > evaluation_utc
        )
    else:
        # Unavailable time: retain the remaining store projection without
        # expiry claims (plan §10).
        visible = tuple(snapshot.max_vehicle_ids)
    return FreshnessSnapshot(
        revision=snapshot.revision,
        generation=snapshot.generation,
        dataset_identity=snapshot.dataset_identity,
        clock_assessment=clock_assessment,
        sample_utc_ms=evaluation_utc,
        feed_assessments=feed_assessments,
        vehicle_assessments=vehicle_assessments,
        visible_max_vehicle_ids=visible,
        expiry_state=snapshot.expiry_state,
        expiry_result=None,
        top_reasons=tuple(top),
    )


# ---------------------------------------------------------------------------
# Explicit controller (plan §7, §10)
# ---------------------------------------------------------------------------


class FreshnessController:
    """Explicit injected-clock controller over one store (plan §7, §10).

    One controller per store is the planned production composition; it
    does not register globally, own the poller or run timers.  ``refresh``
    is explicit application work: sample → assess → (trusted) cleanup →
    capture → evaluate.  ``reinitialize_clock`` is the only path back to
    trust after sticky distrust; it never clears store data, counters,
    ordering or expiry history and never calls cleanup.

    The constructor stores dependencies and samples nothing; the first
    trusted :meth:`refresh` establishes the wall/monotonic anchor.
    """

    def __init__(
        self,
        store: LiveState,
        *,
        policy: "FreshnessPolicy",
        clock: FreshnessClock | None = None,
    ) -> None:
        _validate_policy(policy)
        if not isinstance(store, LiveState):
            raise FreshnessInputError(
                "store must be a LiveState", code="invalid_snapshot"
            )
        if clock is None:
            clock = SystemFreshnessClock()
        else:
            _validate_clock(clock)
        self._store = store
        self._policy = policy
        self._clock: FreshnessClock = clock
        self._lock = threading.RLock()
        self._status = ClockStatus.UNINITIALIZED
        self._epoch = 0
        self._assessment: ClockAssessment | None = None
        self._anchor_utc_ms: int | None = None
        self._anchor_monotonic_ms: int | None = None

    # -- public surface ---------------------------------------------------

    def refresh(self) -> "FreshnessSnapshot":
        """One explicit assessment cycle (plan §10); never a timer."""
        with self._lock:
            sample, failure = _take_sample(self._clock)
            assessment = self._assess(sample, failure)
            expiry_result: ExpiryResult | None = None
            top: list[FreshnessReason] = []
            if assessment.status is ClockStatus.TRUSTED:
                assert sample is not None  # trusted ⇒ validated sample
                expiry_result, top = self._advance_expiry(sample.utc_ms)
            snapshot = self._store.snapshot()
            evaluated = evaluate_freshness(
                snapshot,
                sample=(
                    sample if assessment.status is ClockStatus.TRUSTED
                    else None
                ),
                policy=self._policy,
                clock_assessment=assessment,
            )
            merged_reasons = tuple(
                dict.fromkeys(tuple(top) + evaluated.top_reasons)
            )
            return FreshnessSnapshot(
                revision=evaluated.revision,
                generation=evaluated.generation,
                dataset_identity=evaluated.dataset_identity,
                clock_assessment=assessment,
                sample_utc_ms=evaluated.sample_utc_ms,
                feed_assessments=evaluated.feed_assessments,
                vehicle_assessments=evaluated.vehicle_assessments,
                visible_max_vehicle_ids=evaluated.visible_max_vehicle_ids,
                expiry_state=evaluated.expiry_state,
                expiry_result=expiry_result,
                top_reasons=merged_reasons,
            )

    def reinitialize_clock(
        self, *, expected_clock_epoch: int
    ) -> ClockAssessment:
        """Explicit trust recovery with an epoch check (plan §7).

        Validates the expected current epoch and one new sample, then
        establishes a new anchor and epoch.  Every refusal changes
        nothing: ``clock_epoch_conflict`` for a stale/unknown epoch and
        ``clock_recovery_blocked`` — the frozen typed refusal — when the
        corrected UTC is still below the store's accepted cutoff (the
        bounded ``utc_before_expiry_cutoff`` time-quality cause; trust is
        not restored and history is never lowered).  A provider failure
        on a never-anchored clock leaves the controller UNINITIALIZED
        with a coherent assessment instead of poisoning it: no
        controller field is mutated until a valid candidate assessment
        exists.  No cleanup runs and no S10 quota clock changes.
        """
        with self._lock:
            if not _is_int(expected_clock_epoch):
                raise FreshnessRecoveryError(
                    "expected_clock_epoch must be an integer excluding bool",
                    code="clock_epoch_conflict",
                )
            if expected_clock_epoch != self._epoch:
                raise FreshnessRecoveryError(
                    "expected_clock_epoch does not match the controller's "
                    "current clock epoch",
                    code="clock_epoch_conflict",
                )
            sample, failure = _take_sample(self._clock)
            if sample is None:
                reason = (
                    FreshnessReason.CLOCK_READ_FAILED
                    if failure is FreshnessReason.CLOCK_READ_FAILED
                    else FreshnessReason.INVALID_CLOCK_SAMPLE
                )
                if self._status is ClockStatus.UNINITIALIZED:
                    # A never-anchored clock cannot be made distrust-sticky:
                    # return the coherent bounded assessment and leave the
                    # controller exactly as it was (usable, still
                    # uninitialized; the next refresh anchors normally).
                    return ClockAssessment(
                        status=ClockStatus.UNINITIALIZED,
                        epoch=self._epoch,
                        reasons=(reason,),
                    )
                # Established clock: sticky distrust, constructed before
                # any field mutation so no invalid intermediate state.
                candidate = ClockAssessment(
                    status=ClockStatus.UNTRUSTED,
                    epoch=self._epoch,
                    reasons=(reason,),
                )
                self._status = candidate.status
                self._assessment = candidate
                return self._assessment
            expiry_state = self._store.snapshot().expiry_state
            if (
                expiry_state is not None
                and sample.utc_ms < expiry_state.cutoff_utc_ms
            ):
                # Corrected UTC is still below the accepted cutoff: the
                # frozen typed refusal — trust is not restored, the
                # anchor/epoch/history and store stay exactly as they
                # were, and no force-reset path exists (plan §7).
                raise FreshnessRecoveryError(
                    "corrected UTC is below the store's accepted expiry "
                    "cutoff (utc_before_expiry_cutoff); trust is not "
                    "restored and nothing was changed",
                    code="clock_recovery_blocked",
                )
            self._anchor_utc_ms = sample.utc_ms
            self._anchor_monotonic_ms = sample.monotonic_ms
            self._epoch += 1
            self._status = ClockStatus.TRUSTED
            self._assessment = ClockAssessment(
                status=ClockStatus.TRUSTED, epoch=self._epoch, reasons=()
            )
            return self._assessment

    # -- internals ---------------------------------------------------------

    def _assess(
        self,
        sample: TimeSample | None,
        failure: FreshnessReason | None,
    ) -> ClockAssessment:
        """Assess one sample against the anchored pair (plan §7).

        Provider failures and invalid samples create sticky distrust.
        Once anchored, negative wall/monotonic elapsed or absolute
        wall-versus-monotonic divergence **greater than** the configured
        tolerance (equality permitted) creates sticky distrust; both
        directions are checked before any destructive cleanup.
        """
        if sample is None:
            reason = (
                failure
                if failure is not None
                else FreshnessReason.INVALID_CLOCK_SAMPLE
            )
            if self._status is ClockStatus.UNINITIALIZED:
                return ClockAssessment(
                    status=ClockStatus.UNINITIALIZED,
                    epoch=self._epoch,
                    reasons=(reason,),
                )
            # Established clock: sticky distrust, constructed before any
            # field mutation so no invalid intermediate state can exist.
            candidate = ClockAssessment(
                status=ClockStatus.UNTRUSTED,
                epoch=self._epoch,
                reasons=(reason,),
            )
            self._status = candidate.status
            self._assessment = candidate
            return self._assessment
        if self._status is ClockStatus.UNINITIALIZED:
            # First valid sample anchors the pair under the explicit
            # system-UTC assumption (plan §7).
            self._anchor_utc_ms = sample.utc_ms
            self._anchor_monotonic_ms = sample.monotonic_ms
            self._status = ClockStatus.TRUSTED
            self._assessment = ClockAssessment(
                status=ClockStatus.TRUSTED, epoch=1, reasons=()
            )
            self._epoch = 1
            return self._assessment
        if self._status is ClockStatus.UNTRUSTED:
            # Sticky: only explicit recovery restores trust.
            return ClockAssessment(
                status=ClockStatus.UNTRUSTED,
                epoch=self._epoch,
                reasons=self._assessment.reasons,
            )
        assert self._anchor_utc_ms is not None
        assert self._anchor_monotonic_ms is not None
        wall_elapsed = sample.utc_ms - self._anchor_utc_ms
        mono_elapsed = sample.monotonic_ms - self._anchor_monotonic_ms
        tolerance = self._policy.clock_divergence_tolerance_ms
        if wall_elapsed < 0 or mono_elapsed < 0:
            candidate = ClockAssessment(
                status=ClockStatus.UNTRUSTED,
                epoch=self._epoch,
                reasons=(FreshnessReason.CLOCK_MOVED_BACKWARDS,),
            )
            self._status = candidate.status
            self._assessment = candidate
            return self._assessment
        if abs(wall_elapsed - mono_elapsed) > tolerance:
            candidate = ClockAssessment(
                status=ClockStatus.UNTRUSTED,
                epoch=self._epoch,
                reasons=(FreshnessReason.CLOCK_DIVERGED,),
            )
            self._status = candidate.status
            self._assessment = candidate
            return self._assessment
        return ClockAssessment(
            status=ClockStatus.TRUSTED, epoch=self._epoch, reasons=()
        )

    def _advance_expiry(
        self, cutoff_utc_ms: int
    ) -> tuple[ExpiryResult, list[FreshnessReason]]:
        """Trusted cleanup attempt (plan §10); ``concurrent_change`` defers.

        Only the store's publication-race code ``concurrent_change`` may
        take the deferred path; every other conflict (for example
        ``generation_conflict`` or ``sequence_collision``) propagates
        unchanged — an unrelated store failure is never relabelled as
        ordinary cleanup contention.
        """
        try:
            result = self._store.expire_vehicles(cutoff_utc_ms)
        except StateConflictError as exc:
            if exc.code != "concurrent_change":
                raise
            # The store's bounded publication race was lost: re-read the
            # winning snapshot and admit the deferral honestly.  The pure
            # view below still filters expired entries at the trusted
            # coherent evaluation time (plan §10).
            snapshot = self._store.snapshot()
            state = snapshot.expiry_state
            effective = state.cutoff_utc_ms if state is not None else None
            deferred = ExpiryResult(
                requested_cutoff_utc_ms=cutoff_utc_ms,
                effective_cutoff_utc_ms=effective,
                revision_before=snapshot.revision,
                revision_after=snapshot.revision,
                changed=False,
                removed_vehicle_ids=(),
            )
            return deferred, [FreshnessReason.CLEANUP_DEFERRED]
        return result, []
