"""S12 in-memory LiveState: immutable-root ingestion and safe reconciliation.

Stage S12 (M2 plan §§5–12; implementation prompt §§4–8).  One store holds the
latest accepted observation per vehicle ID (all three S11 relevance classes,
within ``max_vehicle_entries``), the latest matched TripUpdate/alert views,
per-feed source-time high-water marks, one global absence fence, and bounded
decision telemetry — all published as immutable roots under a single store
lock.

Design anchors:

- **Optimistic immutable publication** (plan §7): matching and re-matching
  run outside the publication lock; capture/validate/publish revalidate the
  captured root under the lock and publish a complete candidate or nothing.
  At most **three total attempts** per operation; exhaustion raises
  ``concurrent_change`` and preserves the winning root.
- **All-or-nothing transactions** (plan §6): a delivery is validated,
  matched through S11, reduced into a private candidate, capacity-checked,
  then published atomically.  Replay/source markers never advance before a
  successful publication, and no anticipated validation error is ever
  raised after one.
- **Authority is explicit** (plan §9): a full request's purpose is never
  authority.  The store alone evaluates the authority gates and may then
  construct the S9 :class:`~backend.trimet.feed_types.ReconciliationSnapshot`
  basis — here represented by the fence and the ``basis``-documented
  decision — and remove absent vehicles.
- **No clocks, no I/O, no network** (plan §12): the store never reads a
  clock, touches the filesystem, opens a socket, starts a runtime, or calls
  :meth:`backend.trimet.dataset.DatasetManager.load`.  Receipt/request wall
  times travel as provenance and never substitute for source ordering.

Process-local honesty (plan §12): a new ``LiveState`` object loses
observation, ordering, session, fence and generation history;
``prior_state_history`` starts ``"unknown"``.  This is duplicate tolerance,
not exactly-once delivery or durable recovery.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Mapping

from backend.trimet.feed_match import match_delivery
from backend.trimet.feed_types import (
    INCREMENTALITY_FULL_DATASET,
    AlertsSnapshot,
    MAX_PLAUSIBLE_UTC_MS,
    MIN_PLAUSIBLE_UTC_MS,
    ReconciliationSnapshot,
    TripUpdateFeed,
    VehiclesRequestKind,
    VehiclesRequestProvenance,
    VehiclesSnapshot,
)
from backend.trimet.match_types import (
    MatchedVehicle,
    MatchingContext,
    RelevanceClassification,
)
from backend.trimet.poll_types import PollDelivery, PollFeed, RequestPurpose
from backend.trimet.state_errors import (
    StateCapacityError,
    StateConflictError,
    StateInputError,
    StateSessionError,
)
from backend.trimet.state_sink import StateSession
from backend.trimet.state_types import (
    AbsenceFence,
    ApplyDecision,
    ApplyResult,
    ContextSwapResult,
    FeedEnvelope,
    FeedStateView,
    LiveStateSnapshot,
    PRIOR_STATE_HISTORY_UNKNOWN,
    SourceStamp,
    StateDecisionSample,
    StateLimits,
    StateReason,
    StateTelemetry,
    VehicleEntry,
)

__all__ = ["LiveState", "MAX_CONTEXT_SWAP_ATTEMPTS"]

# Bounded optimistic publication: three **total** attempts per operation,
# not three retries after an initial attempt (plan §7; prompt §5).
MAX_CONTEXT_SWAP_ATTEMPTS = 3

_FULL_PURPOSES = frozenset(
    {
        RequestPurpose.STARTUP_FULL,
        RequestPurpose.SCHEDULED_FULL,
        RequestPurpose.RECOVERY_FULL,
    }
)

# Fixed, credential-free basis string for the S9 authority wrapper decision
# (plan §9): proof of the store's own gates, never a claim about globally
# complete upstream history.
AUTHORITY_BASIS = "s12_complete_default_collection"


def _is_int(value: object) -> bool:
    """True for real integers (``bool`` is an ``int`` subclass and excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_plausible_utc_ms(value: object) -> bool:
    return (
        _is_int(value)
        and MIN_PLAUSIBLE_UTC_MS <= value <= MAX_PLAUSIBLE_UTC_MS
    )


def _is_count(value: object) -> bool:
    """A known nonnegative integer count, excluding ``bool``."""
    return _is_int(value) and value >= 0


# ---------------------------------------------------------------------------
# Internal immutable root pieces
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Session:
    """One open runtime session's sequence/replay namespace (plan §6).

    Session replacement clears sequence/replay metadata but never source
    ordering, vehicles, fences or the static generation, which live on the
    root itself.
    """

    session_id: int
    last_committed_sequence: Mapping[PollFeed, int]
    replay_deliveries: Mapping[PollFeed, PollDelivery]


@dataclass(frozen=True)
class _FeedSlot:
    """Root-level per-feed source content and ordering (plan §5, §6).

    Survives session replacement; the replay envelope travels with the
    session.  ``source_time_high_water_ms`` is the feed's own ordering
    timestamp of the latest accepted source content.
    """

    source_content: FeedEnvelope | None
    source_time_high_water_ms: int | None


@dataclass(frozen=True)
class _Root:
    """One complete immutable store state; publication swaps this object."""

    context: MatchingContext
    generation: int
    revision: int
    session: _Session | None
    vehicles: Mapping[int, VehicleEntry]
    feed_slots: Mapping[PollFeed, _FeedSlot]
    fence: AbsenceFence | None
    telemetry: "_TelemetryAccumulator"


@dataclass(frozen=True)
class _Decision:
    """One per-event diagnostic record (plan §12).

    Created by :meth:`_DecisionCollector.add` for every counted event and
    immediately either converted to a retained sample or discarded, so
    live per-event diagnostic objects stay bounded during reduction.
    """

    reason: StateReason
    record_key: str | None = None
    detail: str | None = None


class _TelemetryAccumulator:
    """Exact totals + capped samples for **committed** telemetry (§12).

    Never accumulates an unbounded sample list and never slices at the end:
    samples stop growing once the cap is exceeded, and the omitted count is
    exact from that point on.  A published accumulator is never mutated
    again; in-flight transactions build their telemetry on a private
    :class:`_DecisionCollector` instead.
    """

    __slots__ = ("_cap", "_omitted", "_samples", "_total", "_totals")

    def __init__(self, cap: int) -> None:
        self._cap = cap
        self._samples: list[StateDecisionSample] = []
        self._total = 0
        self._omitted = 0
        self._totals: dict[StateReason, int] = {}

    @property
    def cap(self) -> int:
        return self._cap

    def frozen(self) -> StateTelemetry:
        samples = tuple(self._samples)
        if self._omitted:
            overflow = StateDecisionSample(
                feed=None,
                reason=StateReason.DIAGNOSTICS_OVERFLOW,
                record_key=None,
                detail=(
                    f"{self._omitted} state decision samples omitted beyond "
                    f"the {self._cap}-sample cap"
                ),
            )
            samples = samples[: self._cap - 1] + (overflow,)
        return StateTelemetry(
            reason_totals=dict(self._totals),
            samples=samples,
            omitted_samples=self._omitted,
            total_events=self._total,
        )


class _DecisionCollector:
    """Per-transaction streaming decision collector (plan §12).

    Counts fixed-vocabulary events and retains capped samples **as
    reduction proceeds** — one sample object per event is created and
    either stored (while under the cap) or immediately discarded, so
    simultaneously retained diagnostic state never exceeds the sample cap
    plus the 13 fixed-vocabulary unique reasons, regardless of delivery
    size or concurrent transactions.  Nothing is stored beyond the cap: no
    per-event list is ever materialized before capping.

    The collector starts from a snapshot of the transaction's captured
    committed accumulator on its first event and is discarded with the
    transaction on any retry or failure; committed telemetry is never
    mutated before publication.
    """

    __slots__ = (
        "_base",
        "_cap",
        "_feed",
        "_omitted",
        "_samples",
        "_started",
        "_total",
        "_totals",
        "_unique",
    )

    def __init__(self, feed: PollFeed, base: _TelemetryAccumulator) -> None:
        self._feed = feed
        self._base = base
        self._cap = base.cap
        self._started = False
        self._samples: list[StateDecisionSample] = []
        self._total = 0
        self._omitted = 0
        self._totals: dict[StateReason, int] = {}
        self._unique: list[StateReason] = []

    def add(
        self,
        reason: StateReason,
        record_key: str | None = None,
        detail: str | None = None,
    ) -> None:
        """Record one decision event now; bounded by the sample cap."""
        if not self._started:
            self._started = True
            self._samples = list(self._base._samples)
            self._total = self._base._total
            self._omitted = self._base._omitted
            self._totals = dict(self._base._totals)
        self._total += 1
        self._totals[reason] = self._totals.get(reason, 0) + 1
        if reason not in self._unique:
            self._unique.append(reason)
        # One per-event record object, created now and either converted to
        # a retained sample or dropped: nothing accumulates per event
        # beyond the cap (plan §12).
        event = _Decision(reason, record_key, detail)
        if self._total <= self._cap:
            self._samples.append(
                StateDecisionSample(
                    feed=self._feed,
                    reason=event.reason,
                    record_key=event.record_key,
                    detail=event.detail,
                )
            )
        elif self._omitted == 0:
            self._omitted = self._total - (self._cap - 1)
            del self._samples[self._cap - 1 :]
        else:
            self._omitted += 1
        # The event object goes out of scope here: beyond-cap events retain
        # only exact counters (plan §12).

    @property
    def unique_reasons(self) -> tuple[StateReason, ...]:
        return tuple(self._unique)

    def merged(self) -> _TelemetryAccumulator:
        """The transaction's final accumulator, or the committed base."""
        if not self._started:
            return self._base
        acc = _TelemetryAccumulator(self._cap)
        acc._samples = self._samples
        acc._total = self._total
        acc._omitted = self._omitted
        acc._totals = self._totals
        return acc


def _booked_session(
    session: _Session, feed: PollFeed, delivery: PollDelivery
) -> _Session:
    """Copy-on-write booking of one processed sequence (plan §6)."""
    last_sequence = dict(session.last_committed_sequence)
    last_sequence[feed] = delivery.sequence
    replay = dict(session.replay_deliveries)
    replay[feed] = delivery
    return _Session(
        session_id=session.session_id,
        last_committed_sequence=MappingProxyType(last_sequence),
        replay_deliveries=MappingProxyType(replay),
    )


def _proxied_vehicles(
    vehicles: Mapping[int, VehicleEntry],
) -> Mapping[int, VehicleEntry]:
    return MappingProxyType(dict(vehicles))


def _proxied_slots(
    slots: Mapping[PollFeed, _FeedSlot],
) -> Mapping[PollFeed, _FeedSlot]:
    return MappingProxyType(dict(slots))


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------


class LiveState:
    """Thread-safe in-memory store with immutable read snapshots (plan §5).

    Public surface: :meth:`snapshot`, :meth:`open_session`,
    :meth:`close_session`, :meth:`replace_context`.  Ingestion happens
    through :class:`~backend.trimet.state_sink.StateSession`.  Store
    lifetime is one object: nothing here survives a new store or process.
    """

    def __init__(
        self,
        initial_context: MatchingContext,
        *,
        limits: StateLimits | None = None,
    ) -> None:
        if not isinstance(initial_context, MatchingContext):
            raise StateInputError(
                "initial_context must be a MatchingContext", code="invalid_context"
            )
        if limits is None:
            limits = StateLimits()
        if not isinstance(limits, StateLimits):
            raise StateInputError(
                "limits must be a StateLimits", code="invalid_limits"
            )
        self._limits = limits
        self._lock = threading.RLock()
        self._session_counter = 0
        self._noop_counters: dict[StateReason, int] = {
            reason: 0 for reason in StateReason
        }
        slots = {feed: _FeedSlot(None, None) for feed in PollFeed}
        self._root = _Root(
            context=initial_context,
            generation=1,
            revision=0,
            session=None,
            vehicles=_proxied_vehicles({}),
            feed_slots=_proxied_slots(slots),
            fence=None,
            telemetry=_TelemetryAccumulator(
                limits.max_state_diagnostic_samples
            ),
        )

    # ------------------------------------------------------------------
    # read access

    @property
    def limits(self) -> StateLimits:
        """The validated, constructor-only limits in force."""
        return self._limits

    def snapshot(self) -> LiveStateSnapshot:
        """One immutable read view; previously returned snapshots never change."""
        with self._lock:
            return self._build_snapshot(self._root)

    def _build_snapshot(self, root: _Root) -> LiveStateSnapshot:
        entries = dict(root.vehicles)
        max_ids = tuple(
            sorted(
                vehicle_id
                for vehicle_id, entry in entries.items()
                if entry.matched.classification
                is RelevanceClassification.MAX_RELEVANT
            )
        )
        views: dict[PollFeed, FeedStateView] = {}
        for feed, slot in root.feed_slots.items():
            replay = (
                root.session.replay_deliveries.get(feed)
                if root.session is not None
                else None
            )
            views[feed] = FeedStateView(
                feed=feed,
                source_content=slot.source_content,
                replay_delivery=replay,
                source_time_high_water_ms=slot.source_time_high_water_ms,
            )
        return LiveStateSnapshot(
            revision=root.revision,
            generation=root.generation,
            dataset_identity=root.context.identity,
            prior_state_history=PRIOR_STATE_HISTORY_UNKNOWN,
            active_session_id=(
                root.session.session_id if root.session is not None else None
            ),
            vehicle_entries=entries,
            max_vehicle_ids=max_ids,
            feed_views=views,
            absence_fence=root.fence,
            telemetry=root.telemetry.frozen(),
        )

    # ------------------------------------------------------------------
    # session lifecycle (plan §6)

    def open_session(self) -> StateSession:
        """Open the single active session; a second one is a typed error.

        Opening is itself a fenced root mutation and bumps the publication
        revision even when no vehicle data exists yet.
        """
        with self._lock:
            root = self._root
            if root.session is not None:
                raise StateSessionError(
                    "a session is already open", code="session_already_open"
                )
            self._session_counter += 1
            session = _Session(
                session_id=self._session_counter,
                last_committed_sequence=MappingProxyType({}),
                replay_deliveries=MappingProxyType({}),
            )
            self._root = replace(root, session=session, revision=root.revision + 1)
            return StateSession(self, session.session_id)

    def close_session(self, session_id: int) -> None:
        """Revoke the active session; wrong/unknown ids are typed errors.

        Revocation is an atomic fence: in-flight old-session candidates fail
        revalidation and publish nothing.  This does not stop or join any
        worker — real lifecycle wiring belongs to S14.
        """
        if not _is_int(session_id):
            raise StateSessionError(
                "session_id must be an integer", code="invalid_session"
            )
        with self._lock:
            root = self._root
            if root.session is None or root.session.session_id != session_id:
                raise StateSessionError(
                    "no active session with this session_id",
                    code="invalid_session",
                )
            self._root = replace(
                root, session=None, revision=root.revision + 1
            )

    # ------------------------------------------------------------------
    # ingestion entry point (called only through StateSession)

    def _accept(self, session_id: int, delivery: PollDelivery) -> ApplyResult:
        if not isinstance(delivery, PollDelivery):
            raise StateInputError(
                "delivery must be a PollDelivery", code="invalid_delivery"
            )
        if delivery.feed is PollFeed.VEHICLES:
            return self._accept_vehicles(session_id, delivery)
        return self._accept_secondary(session_id, delivery)

    # -- shared validation ------------------------------------------------

    def _validate_common(self, delivery: PollDelivery) -> None:
        """Shared envelope validation for data and metadata-only branches.

        Every field a SourceStamp (or an equal-source-time metadata-only
        booking) will carry is validated here, before sequence
        classification, matching or publication, so the typed precommit
        contract holds for every acceptance branch (plan §6, §10).
        """
        if not isinstance(delivery.feed, PollFeed):
            raise StateInputError(
                "delivery feed must be a PollFeed", code="invalid_delivery"
            )
        if not _is_int(delivery.sequence) or delivery.sequence < 1:
            raise StateInputError(
                "delivery sequence must be a positive integer excluding bool",
                code="invalid_delivery",
            )
        if not isinstance(delivery.purpose, RequestPurpose):
            raise StateInputError(
                "delivery purpose must be a RequestPurpose",
                code="invalid_delivery",
            )
        if delivery.http_status != 200:
            raise StateInputError(
                "delivery http_status must be the S10 success status 200",
                code="invalid_delivery",
            )
        for field, value in (
            ("request_sent_utc_ms", delivery.request_sent_utc_ms),
            ("receipt_utc_ms", delivery.receipt_utc_ms),
        ):
            if not _is_plausible_utc_ms(value):
                raise StateInputError(
                    f"delivery {field} must be a plausible UTC ms epoch "
                    "excluding bool",
                    code="invalid_delivery",
                )
        if not _is_int(delivery.elapsed_mono_ms) or delivery.elapsed_mono_ms < 0:
            raise StateInputError(
                "delivery elapsed_mono_ms must be a non-negative integer "
                "excluding bool",
                code="invalid_delivery",
            )

    def _validate_feed_model(
        self, delivery: PollDelivery, expected_model: type
    ) -> None:
        if not isinstance(delivery.model, expected_model):
            raise StateInputError(
                f"feed {delivery.feed.value} does not agree with the supplied "
                f"{type(delivery.model).__name__}",
                code="invalid_delivery",
            )

    def _classify_sequence(
        self, session: _Session, feed: PollFeed, delivery: PollDelivery
    ) -> tuple[ApplyDecision, StateReason] | None:
        """Sequence/replay classification under the lock (plan §6).

        Returns ``None`` when the sequence is eligible.  Stale and duplicate
        outcomes are returned as no-op decisions; a changed reuse of a
        committed sequence raises ``sequence_collision`` with no mutation.
        """
        last = session.last_committed_sequence.get(feed)
        if last is None:
            return None
        if delivery.sequence < last:
            self._noop_counters[StateReason.OLDER_SEQUENCE] += 1
            return (ApplyDecision.STALE_NOOP, StateReason.OLDER_SEQUENCE)
        if delivery.sequence == last:
            if delivery == session.replay_deliveries[feed]:
                self._noop_counters[StateReason.DUPLICATE_DELIVERY] += 1
                return (ApplyDecision.DUPLICATE_NOOP, StateReason.DUPLICATE_DELIVERY)
            raise StateConflictError(
                f"delivery sequence {delivery.sequence} was already committed "
                "with different content",
                code="sequence_collision",
            )
        return None

    def _no_op_result(
        self,
        delivery: PollDelivery,
        decision: ApplyDecision,
        reason: StateReason,
        revision: int,
    ) -> ApplyResult:
        return ApplyResult(
            feed=delivery.feed,
            sequence=delivery.sequence,
            decision=decision,
            reasons=(reason,),
            revision=revision,
        )

    def _require_active_session(
        self, root: _Root, session_id: int
    ) -> _Session:
        session = root.session
        if session is None or session.session_id != session_id:
            raise StateSessionError(
                "session is not the store's active session",
                code="invalid_session",
            )
        return session

    def _stamp_for(
        self,
        session: _Session,
        delivery: PollDelivery,
        source_time_utc_ms: int,
        *,
        vehicles_provenance=None,
    ) -> SourceStamp:
        return SourceStamp(
            session_id=session.session_id,
            sequence=delivery.sequence,
            source_time_utc_ms=source_time_utc_ms,
            purpose=delivery.purpose,
            request_sent_utc_ms=delivery.request_sent_utc_ms,
            receipt_utc_ms=delivery.receipt_utc_ms,
            elapsed_mono_ms=delivery.elapsed_mono_ms,
            http_status=delivery.http_status,
            vehicles_provenance=vehicles_provenance,
        )

    # ------------------------------------------------------------------
    # vehicles (plan §8–§9)

    def _accept_vehicles(
        self, session_id: int, delivery: PollDelivery
    ) -> ApplyResult:
        self._validate_common(delivery)
        self._validate_feed_model(delivery, VehiclesSnapshot)
        snapshot: VehiclesSnapshot = delivery.model
        limits = self._limits
        if len(snapshot.vehicles) > limits.max_vehicle_records_per_delivery:
            raise StateCapacityError(
                "vehicle delivery exceeds max_vehicle_records_per_delivery",
                code="capacity_exceeded",
            )
        provenance = snapshot.provenance
        is_full = delivery.purpose in _FULL_PURPOSES
        if is_full:
            if (
                provenance.request_kind is not VehiclesRequestKind.DEFAULT
                or provenance.since_ms is not None
            ):
                raise StateInputError(
                    "a full-purpose vehicles delivery must pair DEFAULT "
                    "provenance with no since parameter",
                    code="invalid_delivery",
                )
        else:
            if (
                delivery.purpose is not RequestPurpose.INCREMENTAL
                or provenance.request_kind is not VehiclesRequestKind.SINCE
                or not _is_int(provenance.since_ms)
            ):
                raise StateInputError(
                    "an incremental vehicles delivery must pair SINCE "
                    "provenance with an integer since value",
                    code="invalid_delivery",
                )
        if provenance.requested_at_utc_ms != delivery.request_sent_utc_ms:
            raise StateInputError(
                "delivery request timestamp disagrees with the snapshot "
                "provenance",
                code="invalid_delivery",
            )

        evidence = delivery.evidence
        if evidence is not None:
            # Defense in depth (plan §9 authority table): the collection
            # flags are annotated booleans but S10's dataclass does not
            # validate them at runtime, so the store enforces the boolean
            # domain itself before any authority decision.
            if not isinstance(evidence.key_present, bool) or not isinstance(
                evidence.is_list, bool
            ):
                raise StateInputError(
                    "vehicle collection evidence flags must be actual "
                    "booleans",
                    code="invalid_delivery",
                )
        explicit_list = (
            evidence is not None
            and evidence.key_present is True
            and evidence.is_list is True
        )
        if explicit_list:
            for name, value in (
                ("raw_count", evidence.raw_count),
                ("rejected_count", evidence.rejected_count),
            ):
                if value is not None and not _is_count(value):
                    raise StateInputError(
                        f"vehicle collection evidence {name} must be a known "
                        "nonnegative integer excluding bool",
                        code="invalid_delivery",
                    )
            if (
                evidence.raw_count is not None
                and evidence.rejected_count is not None
                and evidence.raw_count
                != len(snapshot.vehicles) + evidence.rejected_count
            ):
                raise StateInputError(
                    "vehicle collection evidence accounting is impossible: "
                    "raw_count != parsed records + rejected_count",
                    code="invalid_delivery",
                )
        complete_list_evidence = (
            explicit_list
            and _is_count(evidence.raw_count)
            and _is_count(evidence.rejected_count)
            and evidence.rejected_count == 0
            and evidence.raw_count == len(snapshot.vehicles)
        )

        raw_ids = [vehicle.vehicle_id for vehicle in snapshot.vehicles]
        distinct_ids = set(raw_ids)
        has_duplicates = len(distinct_ids) != len(raw_ids)
        query_time = snapshot.query_time_utc_ms

        for attempt in range(1, MAX_CONTEXT_SWAP_ATTEMPTS + 1):
            with self._lock:
                root = self._root
                session = self._require_active_session(root, session_id)
                classified = self._classify_sequence(
                    session, PollFeed.VEHICLES, delivery
                )
                if classified is not None:
                    decision, reason = classified
                    return self._no_op_result(
                        delivery, decision, reason, root.revision
                    )
                slot = root.feed_slots[PollFeed.VEHICLES]
                high_water = slot.source_time_high_water_ms
                if high_water is not None and query_time < high_water:
                    # An older-queryTime delivery is ignored whole, including
                    # absence removal; sequence is not booked (plan §6, §8).
                    self._noop_counters[StateReason.OLDER_SOURCE_TIMESTAMP] += 1
                    return self._no_op_result(
                        delivery,
                        ApplyDecision.IGNORED_OLDER,
                        StateReason.OLDER_SOURCE_TIMESTAMP,
                        root.revision,
                    )
                fence = root.fence
                ordering_eligible = high_water is None or query_time > high_water
                fence_eligible = fence is None or query_time > fence.query_time_utc_ms
                if (
                    is_full
                    and complete_list_evidence
                    and not has_duplicates
                    and ordering_eligible
                    and fence_eligible
                    and len(distinct_ids) > limits.max_vehicle_entries
                ):
                    # Known before matching: reject the whole transaction now
                    # (plan §9 REV 2; prompt §6.4).
                    raise StateCapacityError(
                        "authoritative presence set exceeds max_vehicle_entries",
                        code="capacity_exceeded",
                    )
            # Outside the lock: pure matching against the captured context.
            batch = match_delivery(delivery, root.context)
            (
                candidate,
                collector,
                removed_ids,
                authorized,
                committed,
            ) = self._reduce_vehicle_delivery(
                delivery=delivery,
                batch=batch,
                root=root,
                session=session,
                is_full=is_full,
                complete_list_evidence=complete_list_evidence,
                has_duplicates=has_duplicates,
            )
            with self._lock:
                if self._root is root:
                    self._root = candidate
                    if committed:
                        decision = ApplyDecision.COMMITTED
                    else:
                        decision = ApplyDecision.METADATA_ONLY
                    return self._commit_result(
                        delivery, decision, collector, candidate, authorized,
                        removed_ids,
                    )
                # Revocation always wins over an in-flight old-session
                # candidate (plan §7).
                self._require_active_session(self._root, session_id)
                if attempt == MAX_CONTEXT_SWAP_ATTEMPTS:
                    raise StateConflictError(
                        "store changed concurrently; publication retries "
                        "exhausted and nothing was published",
                        code="concurrent_change",
                    )
        raise StateConflictError(
            "store changed concurrently; publication retries exhausted",
            code="concurrent_change",
        )

    def _commit_result(
        self,
        delivery: PollDelivery,
        decision: ApplyDecision,
        collector: _DecisionCollector,
        candidate: _Root,
        authorized: bool,
        removed_ids: list[int],
    ) -> ApplyResult:
        return ApplyResult(
            feed=delivery.feed,
            sequence=delivery.sequence,
            decision=decision,
            reasons=collector.unique_reasons,
            revision=candidate.revision,
            authoritative=authorized,
            removed_vehicle_ids=tuple(sorted(removed_ids)),
        )

    def _reduce_vehicle_delivery(
        self,
        *,
        delivery: PollDelivery,
        batch,
        root: _Root,
        session: _Session,
        is_full: bool,
        complete_list_evidence: bool,
        has_duplicates: bool,
    ) -> tuple[_Root, _DecisionCollector, list[int], bool, bool]:
        """Reduce one vehicle delivery into a private candidate (plan §8–§9).

        Returns ``(candidate_root, collector, removed_ids, authorized,
        committed)``.  ``committed`` distinguishes a data transaction from a
        metadata-only one; both publish a new root and bump the revision.
        Decisions stream into the per-transaction collector as they occur,
        bounded by the sample cap during collection (plan §12).
        """
        snapshot: VehiclesSnapshot = delivery.model
        vehicles_snapshot = snapshot.vehicles
        records = batch.records
        if len(records) != len(vehicles_snapshot):
            # S11 preserves input order and count; this is a precommit
            # defensive check, never a post-publication surprise.
            raise StateInputError(
                "matching returned a record count inconsistent with the "
                "parsed vehicle collection",
                code="invalid_delivery",
            )
        query_time = snapshot.query_time_utc_ms
        collector = _DecisionCollector(PollFeed.VEHICLES, root.telemetry)
        if not is_full:
            collector.add(StateReason.NON_FULL_REQUEST)
        if is_full and complete_list_evidence and has_duplicates:
            collector.add(StateReason.DUPLICATE_VEHICLE_ID)

        slot = root.feed_slots[PollFeed.VEHICLES]
        high_water = slot.source_time_high_water_ms
        fence = root.fence
        authorized = (
            is_full
            and complete_list_evidence
            and not has_duplicates
            and (high_water is None or query_time > high_water)
            and (fence is None or query_time > fence.query_time_utc_ms)
        )
        if is_full and not authorized:
            if not complete_list_evidence:
                collector.add(StateReason.INCOMPLETE_COLLECTION)
            if high_water is not None and query_time == high_water:
                collector.add(StateReason.EQUAL_SNAPSHOT_TIME)
            elif fence is not None and query_time == fence.query_time_utc_ms:
                collector.add(StateReason.EQUAL_SNAPSHOT_TIME)

        vehicles: dict[int, VehicleEntry] = dict(root.vehicles)
        stamp = self._stamp_for(
            session, delivery, query_time, vehicles_provenance=snapshot.provenance
        )

        # Group observations by ID, preserving first-seen order and the
        # aligned matched records (plan §8 duplicate reduction).
        groups: dict[int, list[tuple[int, object, MatchedVehicle]]] = {}
        for index, vehicle in enumerate(vehicles_snapshot):
            groups.setdefault(vehicle.vehicle_id, []).append(
                (index, vehicle, records[index])
            )

        changed = False
        for vehicle_id, group in groups.items():
            if len(group) > 1:
                collector.add(
                    StateReason.DUPLICATE_VEHICLE_ID,
                    str(vehicle_id),
                    f"{len(group)} records share this vehicle id",
                )
            retained = vehicles.get(vehicle_id)
            if retained is None and not authorized and fence is not None:
                if vehicle_id not in fence.seen_ids:
                    best_time = max(item[1].time_utc_ms for item in group)
                    if best_time <= fence.query_time_utc_ms:
                        # A reappearing ID with only a pre-fence position
                        # stays withheld until a fresh position (plan §9).
                        collector.add(
                            StateReason.PRE_RECONCILIATION_OBSERVATION,
                            str(vehicle_id),
                        )
                        continue
            ranks = [(item[1].time_utc_ms, query_time) for item in group]
            max_rank = max(ranks)
            contenders = [
                item for item, rank in zip(group, ranks) if rank == max_rank
            ]
            if retained is not None:
                retained_rank = (
                    retained.raw.time_utc_ms,
                    retained.stamp.source_time_utc_ms,
                )
                if max_rank < retained_rank:
                    collector.add(
                        StateReason.OLDER_VEHICLE_OBSERVATION,
                        str(vehicle_id),
                    )
                    continue
                if max_rank == retained_rank:
                    if any(item[1] != retained.raw for item in contenders):
                        collector.add(
                            StateReason.OBSERVATION_TIE_CONFLICT,
                            str(vehicle_id),
                        )
                    # The retained value resolves the rank either way.
                    continue
            if len(contenders) > 1:
                first_value = contenders[0][1]
                if any(item[1] != first_value for item in contenders[1:]):
                    collector.add(
                        StateReason.OBSERVATION_TIE_CONFLICT,
                        str(vehicle_id),
                    )
                    # No arbitrary first/last-wins authority (plan §8).
                    continue
            # A strictly newer rank is the newest accepted observation even
            # when the raw value repeats: the entry's stamp moves to the
            # delivery that actually carries it (plan §8 rank ordering).
            _, winner, matched_record = contenders[0]
            vehicles[vehicle_id] = VehicleEntry(
                vehicle_id=vehicle_id,
                raw=winner,
                matched=matched_record,
                stamp=stamp,
                matched_generation=root.generation,
            )
            changed = True

        removed_ids: list[int] = []
        if authorized:
            # All §9 gates passed: construct the S9 authority wrapper now,
            # with the fixed basis string and the original receipt time —
            # never a newly read clock (plan §9).  Its constructor is itself
            # a final validated gate; the wrapper is retained on the fence
            # as proof of the passed application gates.
            reconciliation = ReconciliationSnapshot(
                snapshot=delivery.model,
                basis=AUTHORITY_BASIS,
                established_at_utc_ms=delivery.receipt_utc_ms,
            )
            collector.add(StateReason.AUTHORITATIVE_RECONCILIATION)
            if len(groups) > self._limits.max_vehicle_entries:
                # Re-checked at candidate construction after optimistic
                # rebuilds (plan §9; prompt §6.4).
                raise StateCapacityError(
                    "authoritative presence set exceeds max_vehicle_entries",
                    code="capacity_exceeded",
                )
            presence_ids = set(groups)
            for vehicle_id, entry in root.vehicles.items():
                if vehicle_id in presence_ids:
                    continue
                if (
                    entry.stamp.source_time_utc_ms < query_time
                    and entry.raw.time_utc_ms <= query_time
                ):
                    del vehicles[vehicle_id]
                    removed_ids.append(vehicle_id)
                    changed = True
            fence = AbsenceFence(
                query_time_utc_ms=query_time,
                seen_ids=frozenset(presence_ids),
                authority_basis=reconciliation.basis,
            )
        if len(vehicles) > self._limits.max_vehicle_entries:
            # Final-candidate retained-map capacity, checked after valid
            # reconciliation removals so a full can free space (plan §12).
            raise StateCapacityError(
                "retained vehicle map exceeds max_vehicle_entries",
                code="capacity_exceeded",
            )

        new_high_water = query_time if high_water is None else max(high_water, query_time)
        committed = changed or authorized or new_high_water != high_water
        new_session = _booked_session(session, PollFeed.VEHICLES, delivery)
        telemetry = collector.merged()
        if committed:
            envelope = FeedEnvelope(
                delivery=delivery,
                batch=batch,
                stamp=stamp,
                matched_generation=root.generation,
            )
            slots = dict(root.feed_slots)
            slots[PollFeed.VEHICLES] = _FeedSlot(
                source_content=envelope, source_time_high_water_ms=new_high_water
            )
            candidate = _Root(
                context=root.context,
                generation=root.generation,
                revision=root.revision + 1,
                session=new_session,
                vehicles=_proxied_vehicles(vehicles),
                feed_slots=_proxied_slots(slots),
                fence=fence,
                telemetry=telemetry,
            )
        else:
            # Metadata-only: book the processed sequence/replay envelope,
            # preserve the prior source-content view, its receipt time, the
            # fence and every observation (plan §6).
            candidate = _Root(
                context=root.context,
                generation=root.generation,
                revision=root.revision + 1,
                session=new_session,
                vehicles=root.vehicles,
                feed_slots=root.feed_slots,
                fence=root.fence,
                telemetry=telemetry,
            )
        return candidate, collector, removed_ids, authorized, committed

    # ------------------------------------------------------------------
    # secondary feeds (plan §10)

    def _accept_secondary(
        self, session_id: int, delivery: PollDelivery
    ) -> ApplyResult:
        self._validate_common(delivery)
        limits = self._limits
        if delivery.feed is PollFeed.TRIPUPDATES:
            self._validate_feed_model(delivery, TripUpdateFeed)
            model: TripUpdateFeed = delivery.model
            header = model.header
            if (
                isinstance(header.incrementality_raw, bool)
                or not _is_int(header.incrementality_raw)
                or header.incrementality_raw != INCREMENTALITY_FULL_DATASET
            ):
                raise StateInputError(
                    "TripUpdate header must carry the exact FULL_DATASET "
                    "incrementality value",
                    code="invalid_delivery",
                )
            if (
                not _is_int(header.timestamp_raw_seconds)
                or not _is_int(header.timestamp_utc_ms)
                or header.timestamp_utc_ms != header.timestamp_raw_seconds * 1000
                or not _is_plausible_utc_ms(header.timestamp_utc_ms)
            ):
                raise StateInputError(
                    "TripUpdate header seconds/milliseconds timestamp pair "
                    "is inconsistent or implausible",
                    code="invalid_delivery",
                )
            source_time = header.timestamp_utc_ms
            if len(model.entities) > limits.max_tripupdate_entities:
                raise StateCapacityError(
                    "TripUpdate delivery exceeds max_tripupdate_entities",
                    code="capacity_exceeded",
                )
            nested = sum(
                len(entity.trip_update.stop_time_updates)
                for entity in model.entities
            )
            if nested > limits.max_nested_feed_records:
                raise StateCapacityError(
                    "TripUpdate delivery exceeds max_nested_feed_records",
                    code="capacity_exceeded",
                )
        else:
            self._validate_feed_model(delivery, AlertsSnapshot)
            model = delivery.model
            source_time = model.query_time_utc_ms
            if not _is_plausible_utc_ms(source_time):
                raise StateInputError(
                    "alerts snapshot query time is not a plausible UTC ms "
                    "epoch",
                    code="invalid_delivery",
                )
            if len(model.alerts) > limits.max_alert_records:
                raise StateCapacityError(
                    "alerts delivery exceeds max_alert_records",
                    code="capacity_exceeded",
                )
            nested = sum(
                len(alert.routes) + len(alert.locations)
                for alert in model.alerts
            )
            if nested > limits.max_nested_feed_records:
                raise StateCapacityError(
                    "alerts delivery exceeds max_nested_feed_records",
                    code="capacity_exceeded",
                )

        feed = delivery.feed
        for attempt in range(1, MAX_CONTEXT_SWAP_ATTEMPTS + 1):
            with self._lock:
                root = self._root
                session = self._require_active_session(root, session_id)
                classified = self._classify_sequence(session, feed, delivery)
                if classified is not None:
                    decision, reason = classified
                    return self._no_op_result(
                        delivery, decision, reason, root.revision
                    )
                slot = root.feed_slots[feed]
                high_water = slot.source_time_high_water_ms
                if high_water is not None and source_time < high_water:
                    # Older source timestamp: ignored whole; no booking (§6).
                    self._noop_counters[StateReason.OLDER_SOURCE_TIMESTAMP] += 1
                    return self._no_op_result(
                        delivery,
                        ApplyDecision.IGNORED_OLDER,
                        StateReason.OLDER_SOURCE_TIMESTAMP,
                        root.revision,
                    )
                if high_water is not None and source_time == high_water:
                    # Equal source time: the content is never accepted; book
                    # the processed sequence as a metadata-only transaction.
                    # Source content, not a newer receipt, decides the tie.
                    prior = slot.source_content
                    conflict = prior is not None and prior.delivery.model != model
                    collector = _DecisionCollector(feed, root.telemetry)
                    if conflict:
                        collector.add(StateReason.SOURCE_TIMESTAMP_CONFLICT)
                    new_session = _booked_session(session, feed, delivery)
                    candidate = _Root(
                        context=root.context,
                        generation=root.generation,
                        revision=root.revision + 1,
                        session=new_session,
                        vehicles=root.vehicles,
                        feed_slots=root.feed_slots,
                        fence=root.fence,
                        telemetry=collector.merged(),
                    )
                    if self._root is root:
                        self._root = candidate
                        return self._commit_result(
                            delivery,
                            ApplyDecision.METADATA_ONLY,
                            collector,
                            candidate,
                            False,
                            [],
                        )
                    self._require_active_session(self._root, session_id)
                    if attempt == MAX_CONTEXT_SWAP_ATTEMPTS:
                        raise StateConflictError(
                            "store changed concurrently; publication retries "
                            "exhausted and nothing was published",
                            code="concurrent_change",
                        )
                    continue
            # Newer source timestamp: match outside the lock, then publish.
            batch = match_delivery(delivery, root.context)
            stamp = self._stamp_for(session, delivery, source_time)
            envelope = FeedEnvelope(
                delivery=delivery,
                batch=batch,
                stamp=stamp,
                matched_generation=root.generation,
            )
            slots = dict(root.feed_slots)
            slots[feed] = _FeedSlot(
                source_content=envelope, source_time_high_water_ms=source_time
            )
            collector = _DecisionCollector(feed, root.telemetry)
            candidate = _Root(
                context=root.context,
                generation=root.generation,
                revision=root.revision + 1,
                session=_booked_session(session, feed, delivery),
                vehicles=root.vehicles,
                feed_slots=_proxied_slots(slots),
                fence=root.fence,
                telemetry=collector.merged(),
            )
            with self._lock:
                if self._root is root:
                    self._root = candidate
                    return self._commit_result(
                        delivery, ApplyDecision.COMMITTED, collector, candidate,
                        False, [],
                    )
                self._require_active_session(self._root, session_id)
                if attempt == MAX_CONTEXT_SWAP_ATTEMPTS:
                    raise StateConflictError(
                        "store changed concurrently; publication retries "
                        "exhausted and nothing was published",
                        code="concurrent_change",
                    )
        raise StateConflictError(
            "store changed concurrently; publication retries exhausted",
            code="concurrent_change",
        )

    # ------------------------------------------------------------------
    # atomic static-context replacement (plan §11)

    def replace_context(
        self,
        candidate: MatchingContext,
        *,
        expected_generation: int,
    ) -> ContextSwapResult:
        """Atomically replace the static context and re-match everything.

        The caller prepares the candidate with S11's production factory
        outside the store; this method never loads datasets, scans files or
        mutates M1.  Identity is ``(archive_sha256, feed_version)`` — a
        descriptive-only ``source_directory_name`` difference is a no-op,
        not a content change.  Re-matching runs outside the publication
        lock with bounded rebuilds (plan §7, §11).
        """
        if not isinstance(candidate, MatchingContext):
            raise StateInputError(
                "candidate must be a MatchingContext", code="invalid_context"
            )
        if not _is_int(expected_generation) or expected_generation < 1:
            raise StateConflictError(
                "expected_generation must be a positive integer",
                code="generation_conflict",
            )
        for attempt in range(1, MAX_CONTEXT_SWAP_ATTEMPTS + 1):
            with self._lock:
                root = self._root
                if root.generation != expected_generation:
                    raise StateConflictError(
                        "expected context generation does not match the "
                        "store's current generation",
                        code="generation_conflict",
                    )
                same_content = (
                    candidate.identity.archive_sha256
                    == root.context.identity.archive_sha256
                    and candidate.identity.feed_version
                    == root.context.identity.feed_version
                )
                if same_content:
                    # Decided and returned atomically under the lock: a
                    # descriptive-only identity difference is not a content
                    # change (plan §11).
                    return ContextSwapResult(
                        changed=False,
                        generation_before=root.generation,
                        generation_after=root.generation,
                        revision_after=root.revision,
                        rematched_vehicle_entries=0,
                        rematched_feed_views=0,
                    )
            next_generation = root.generation + 1
            # Outside the lock: re-match every retained observation and
            # every latest feed view against B (plan §11).
            new_entries: dict[int, VehicleEntry] = {}
            for vehicle_id, entry in root.vehicles.items():
                matched_record = self._rematch_entry(
                    entry, candidate, next_generation
                )
                new_entries[vehicle_id] = VehicleEntry(
                    vehicle_id=vehicle_id,
                    raw=entry.raw,
                    matched=matched_record,
                    stamp=entry.stamp,
                    matched_generation=next_generation,
                )
            new_slots: dict[PollFeed, _FeedSlot] = {}
            rematched_views = 0
            for feed, slot in root.feed_slots.items():
                source_content = slot.source_content
                if source_content is None:
                    new_slots[feed] = slot
                    continue
                batch = match_delivery(source_content.delivery, candidate)
                new_slots[feed] = _FeedSlot(
                    source_content=FeedEnvelope(
                        delivery=source_content.delivery,
                        batch=batch,
                        stamp=source_content.stamp,
                        matched_generation=next_generation,
                    ),
                    source_time_high_water_ms=slot.source_time_high_water_ms,
                )
                rematched_views += 1
            with self._lock:
                if self._root is root:
                    self._root = _Root(
                        context=candidate,
                        generation=next_generation,
                        revision=root.revision + 1,
                        session=root.session,
                        vehicles=_proxied_vehicles(new_entries),
                        feed_slots=_proxied_slots(new_slots),
                        fence=root.fence,
                        telemetry=root.telemetry,
                    )
                    return ContextSwapResult(
                        changed=True,
                        generation_before=root.generation,
                        generation_after=next_generation,
                        revision_after=self._root.revision,
                        rematched_vehicle_entries=len(new_entries),
                        rematched_feed_views=rematched_views,
                    )
                if attempt == MAX_CONTEXT_SWAP_ATTEMPTS:
                    raise StateConflictError(
                        "store changed concurrently; publication retries "
                        "exhausted and the prior context was preserved",
                        code="concurrent_change",
                    )
        raise StateConflictError(
            "store changed concurrently; publication retries exhausted",
            code="concurrent_change",
        )

    def _rematch_entry(
        self,
        entry: VehicleEntry,
        candidate: MatchingContext,
        next_generation: int,
    ) -> MatchedVehicle:
        """Canonical one-record fallback re-match (plan §11, REV 2 §11).

        The original whole delivery may have been superseded or released;
        re-matching needs only the retained raw record and its stamp.  The
        synthetic one-record snapshot uses ``evidence=None`` and empty
        source diagnostics; it is an internal matching input only — never a
        new delivery, never reconciliation authority, and never published.
        """
        provenance = entry.stamp.vehicles_provenance
        if provenance is None:  # defensive; vehicle stamps always carry one
            provenance = VehiclesRequestProvenance(
                request_kind=VehiclesRequestKind.DEFAULT,
                since_ms=None,
                requested_at_utc_ms=entry.stamp.source_time_utc_ms,
            )
        subset = VehiclesSnapshot(
            provenance=provenance,
            query_time_utc_ms=entry.stamp.source_time_utc_ms,
            vehicles=(entry.raw,),
        )
        internal_delivery = PollDelivery(
            feed=PollFeed.VEHICLES,
            sequence=entry.stamp.sequence,
            purpose=entry.stamp.purpose,
            request_sent_utc_ms=entry.stamp.request_sent_utc_ms,
            receipt_utc_ms=entry.stamp.receipt_utc_ms,
            elapsed_mono_ms=entry.stamp.elapsed_mono_ms,
            http_status=entry.stamp.http_status,
            model=subset,
            diagnostics=(),
            evidence=None,
        )
        batch = match_delivery(internal_delivery, candidate)
        return batch.records[0]
