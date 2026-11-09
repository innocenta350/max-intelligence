"""S11 matching: vehicles, TripUpdates, alerts, and the delivery adapter.

Stage S11 (M2 plan §6–§11; implementation prompt §5).  Pure, deterministic,
lookup-based: one captured immutable :class:`MatchingContext` serves a whole
call — no file I/O, no archive rescans, no clock reads, and no rereading of
the active dataset between records.  Concurrent calls with contexts A and B
stay internally consistent and each reports its own dataset identity.

Policy highlights realized here:

- Exactly **one primary classification** per parsed input record; the three
  per-relevance counts sum to the feed's parsed input count, and
  enrichment warnings never double-count anything.  Input order and
  duplicate/repeated source records are preserved (no deduplication).
- Explicit conflicts are evaluated **before** the ordinary known-non-MAX
  branch: rail on a static bus route ``3``, bus on a MAX route, and any
  reported non-MAX record whose trip id maps to a MAX trip are all
  UNCLASSIFIED — nothing is silently promoted or dropped.
- The pinned native literals ``rail`` / ``bus`` are compared exactly; any
  other string (``"R"``, ``"Rail"``, whitespace-only) is UNCLASSIFIED via
  ``unknown_vehicle_type`` with no silent normalization.
- GTFS ids stay strings; JSON integer route/stop ids convert through their
  canonical decimal string only.  No trip-id repair, zero stripping,
  substring or proximity matching ever occurs.
- Diagnostic samples are capped at **200 entries per batch including the
  overflow summary**; the exact per-reason totals stay complete
  independently of truncation.
- Routine known non-MAX records are aggregated by count only — no error
  flood.
"""

from __future__ import annotations

from collections.abc import Sequence

from backend.trimet.alerts_classify import AlertClassification, classify_alert
from backend.trimet.feed_types import (
    AlertsSnapshot,
    TripUpdateEntity,
    TripUpdateFeed,
    VehiclesSnapshot,
)
from backend.trimet.match_errors import (
    ContextPreparationError,
    MatchInputError,
    MatchReason,
)
from backend.trimet.match_types import (
    ROUTE_TYPE_BUS,
    VEHICLE_TYPE_BUS,
    VEHICLE_TYPE_RAIL,
    EnrichmentState,
    MatchDiagnostic,
    MatchedAlert,
    MatchedBatch,
    MatchedStopUpdate,
    MatchedTripUpdate,
    MatchedVehicle,
    MatchingContext,
    RelevanceClassification,
)
from backend.trimet.poll_types import PollDelivery, PollFeed
from backend.trimet.stop_match import resolve_stop_visit

__all__ = [
    "MAX_DIAGNOSTIC_ENTRIES",
    "match_delivery",
]

MAX_DIAGNOSTIC_ENTRIES = 200  # total samples per batch, incl. overflow entry

_SCHEDULED = 0  # gtfs-realtime ScheduleRelationship SCHEDULED wire value


# ---------------------------------------------------------------------------
# Diagnostic collection (exact totals, capped samples; plan §12)
# ---------------------------------------------------------------------------

_REASON_FIELDS: dict[MatchReason, str] = {
    MatchReason.UNKNOWN_ROUTE: "route_number",
    MatchReason.UNKNOWN_VEHICLE_TYPE: "vehicle_type",
    MatchReason.TYPE_ROUTE_CONFLICT: "vehicle_type",
    MatchReason.TRIP_NOT_REPORTED: "tripID",
    MatchReason.UNKNOWN_TRIP: "tripID",
    MatchReason.TRIP_ROUTE_CONFLICT: "tripID",
    MatchReason.DIRECTION_CONFLICT: "direction",
    MatchReason.MISSING_SHAPE: "shape_id",
    MatchReason.UNUSABLE_SHAPE: "shape_id",
    MatchReason.MISSING_STOP_DETAILS: "stop_id",
    MatchReason.UNKNOWN_STOP_SEQUENCE: "stop_sequence",
    MatchReason.UNKNOWN_STOP_ID: "stop_id",
    MatchReason.STOP_ID_SEQUENCE_CONFLICT: "stop_sequence",
    MatchReason.AMBIGUOUS_STOP_VISIT: "stop_id",
    MatchReason.MISSING_STOP_SELECTORS: "stop_sequence",
    MatchReason.UNSUPPORTED_OPERATIONAL_RELATIONSHIP: "schedule_relationship",
}


class _DiagnosticCollector:
    """Accumulates matching diagnostics with exact totals, capped samples."""

    def __init__(self, feed: str) -> None:
        self._feed = feed
        self._events: list[MatchDiagnostic] = []
        self._totals: dict[MatchReason, int] = {}

    @property
    def event_count(self) -> int:
        return len(self._events)

    def add_record_reasons(
        self, record_key: str | None, reasons: Sequence[MatchReason]
    ) -> None:
        for reason in reasons:
            self._totals[reason] = self._totals.get(reason, 0) + 1
            self._events.append(
                MatchDiagnostic(
                    feed=self._feed,
                    record_key=record_key,
                    reason=reason,
                    field=_REASON_FIELDS.get(reason),
                )
            )

    def finish(self) -> tuple[tuple[MatchDiagnostic, ...], dict[MatchReason, int], int]:
        """Return (capped samples incl. overflow, exact totals, omitted)."""
        events = len(self._events)
        if events <= MAX_DIAGNOSTIC_ENTRIES:
            return tuple(self._events), dict(self._totals), 0
        omitted = events - (MAX_DIAGNOSTIC_ENTRIES - 1)
        samples = tuple(self._events[: MAX_DIAGNOSTIC_ENTRIES - 1]) + (
            MatchDiagnostic(
                feed=self._feed,
                record_key=None,
                reason=MatchReason.DIAGNOSTICS_OVERFLOW,
                field=None,
                detail=(
                    f"{omitted} matching diagnostics omitted beyond the "
                    f"{MAX_DIAGNOSTIC_ENTRIES}-entry cap"
                ),
            ),
        )
        return samples, dict(self._totals), omitted


# ---------------------------------------------------------------------------
# Shared derivation helpers
# ---------------------------------------------------------------------------

def _is_int(value: object) -> bool:
    """True for real integers (``bool`` is an ``int`` subclass and excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _canonical_string_id(value: object) -> str | None:
    """Canonical decimal-string id for JSON integers; strings pass verbatim."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value != "":
        return value
    return None


_CONFLICT_REASONS = frozenset({MatchReason.TRIP_ROUTE_CONFLICT})
_UNMATCHED_REASONS = frozenset(
    {MatchReason.TRIP_NOT_REPORTED, MatchReason.UNKNOWN_TRIP}
)
_PARTIAL_REASONS = frozenset(
    {
        MatchReason.DIRECTION_CONFLICT,
        MatchReason.MISSING_SHAPE,
        MatchReason.UNUSABLE_SHAPE,
        MatchReason.MISSING_STOP_DETAILS,
        MatchReason.UNKNOWN_STOP_SEQUENCE,
        MatchReason.UNKNOWN_STOP_ID,
        MatchReason.STOP_ID_SEQUENCE_CONFLICT,
        MatchReason.AMBIGUOUS_STOP_VISIT,
        MatchReason.MISSING_STOP_SELECTORS,
    }
)


def _vehicle_enrichment(reasons: frozenset[MatchReason]) -> EnrichmentState:
    """Derive an honest enrichment state from the record's annotations."""
    if reasons & _CONFLICT_REASONS:
        return EnrichmentState.CONFLICT
    if reasons & _UNMATCHED_REASONS:
        return EnrichmentState.UNMATCHED
    if reasons & _PARTIAL_REASONS:
        return EnrichmentState.PARTIAL
    return EnrichmentState.MATCHED


def _tripupdate_enrichment(
    *,
    descriptor_route_conflict: bool,
    stop_reasons: tuple[MatchReason, ...],
) -> EnrichmentState:
    if descriptor_route_conflict:
        return EnrichmentState.CONFLICT
    if any(reason in _PARTIAL_REASONS for reason in stop_reasons):
        return EnrichmentState.PARTIAL
    return EnrichmentState.MATCHED


# ---------------------------------------------------------------------------
# Vehicle matching (plan §7)
# ---------------------------------------------------------------------------


def _max_vehicle_enrichment(
    vehicle,  # VehiclePosition
    route_id: str,
    reported_trip: object,
    context: MatchingContext,
) -> tuple[
    list[MatchReason],   # enrichment annotation reasons, in occurrence order
    object | None,       # attached static Trip
    object | None,       # usable ShapeGeometry reference
    int | None,          # static direction from the matched trip
    str | None,          # provenance of a static-fallback direction
    bool,                # reported-vs-static direction conflict
    object | None,       # next-stop StopVisitResolution
]:
    """Resolve MAX-vehicle trip enrichment; annotations only, never guesses."""
    reasons: list[MatchReason] = []
    trip = None
    shape = None
    static_direction: int | None = None
    direction_source: str | None = None
    direction_conflict = False
    next_stop = None

    if not (isinstance(reported_trip, str) and reported_trip != ""):
        reasons.append(MatchReason.TRIP_NOT_REPORTED)
        return reasons, trip, shape, static_direction, direction_source, direction_conflict, next_stop
    static_route = context.trip_to_route.get(reported_trip)
    if static_route is None:
        reasons.append(MatchReason.UNKNOWN_TRIP)
        return reasons, trip, shape, static_direction, direction_source, direction_conflict, next_stop
    if static_route != route_id:
        # Conflicting trip enrichment is rejected outright; the MAX vehicle
        # stays relevant using its reported route/position.
        reasons.append(MatchReason.TRIP_ROUTE_CONFLICT)
        return reasons, None, None, static_direction, direction_source, direction_conflict, next_stop
    # build_matching_context guarantees every indexed MAX trip has its static
    # object; this is a defensive re-check of that context invariant.
    trip = context.network.trips.get(reported_trip)
    if trip is None:  # pragma: no cover - guarded by build_matching_context
        raise ContextPreparationError(
            f"indexed MAX trip {reported_trip} has no network trip object",
            code="index_inconsistency",
        )
    static_direction = trip.direction_id
    if vehicle.direction is None and static_direction is not None:
        # A missing reported direction may be filled with explicit static
        # provenance — never a silent raw-field replacement.
        direction_source = "static_trip"
    if (
        vehicle.direction is not None
        and static_direction is not None
        and vehicle.direction != static_direction
    ):
        # Disagreement: retain reported data, flag the conflict, and never
        # claim a single certain direction.
        direction_conflict = True
        reasons.append(MatchReason.DIRECTION_CONFLICT)
    if trip.shape_id is None:
        reasons.append(MatchReason.MISSING_SHAPE)
    else:
        geometry = context.network.shapes.get(trip.shape_id)
        if geometry is not None and geometry.is_usable:
            shape = geometry
        else:
            reasons.append(MatchReason.UNUSABLE_SHAPE)
    selector_sequence = vehicle.next_stop_seq if _is_int(vehicle.next_stop_seq) else None
    selector_stop = _canonical_string_id(vehicle.next_loc_id)
    if selector_sequence is not None or selector_stop is not None:
        next_stop = resolve_stop_visit(
            stop_sequence=selector_sequence,
            stop_id=selector_stop,
            visits_by_sequence=context.visits_by_sequence[reported_trip],
            visits_by_stop=context.visits_by_stop[reported_trip],
            stops_catalog=context.network.stops,
        )
        reasons.extend(next_stop.reasons)
    return (
        reasons,
        trip,
        shape,
        static_direction,
        direction_source,
        direction_conflict,
        next_stop,
    )


def _match_vehicles(
    snapshot: VehiclesSnapshot,
    context: MatchingContext,
    collector: _DiagnosticCollector,
) -> tuple[MatchedVehicle, ...]:
    records: list[MatchedVehicle] = []
    for vehicle in snapshot.vehicles:
        record_key = _canonical_string_id(vehicle.vehicle_id)
        route_id = _canonical_string_id(vehicle.route_number)
        vehicle_type = vehicle.vehicle_type
        type_ok = (
            isinstance(vehicle_type, str)
            and vehicle_type != ""
            and vehicle_type in (VEHICLE_TYPE_RAIL, VEHICLE_TYPE_BUS)
        )
        reasons: list[MatchReason] = []
        classification = RelevanceClassification.UNCLASSIFIED
        enrichment: EnrichmentState | None = None
        max_route = None
        trip = None
        shape = None
        static_direction: int | None = None
        direction_source: str | None = None
        direction_conflict = False
        next_stop = None

        if not type_ok:
            # Unsupported native-type string (including whitespace-only or
            # unknown values) — relevance was never established.
            reasons.append(MatchReason.UNKNOWN_VEHICLE_TYPE)
        elif route_id is None or route_id not in context.known_route_ids:
            reasons.append(MatchReason.UNKNOWN_ROUTE)
        else:
            is_max = route_id in context.max_route_ids
            conflict: MatchReason | None = None
            if vehicle_type == VEHICLE_TYPE_RAIL:
                if is_max:
                    classification = RelevanceClassification.MAX_RELEVANT
                    max_route = context.network.routes.get(route_id)
                elif context.route_types[route_id] == ROUTE_TYPE_BUS:
                    # Owner-selected conflict rule, evaluated before the
                    # ordinary known-non-MAX branch so a rail vehicle on a
                    # bus route can never be consumed by that filter.
                    conflict = MatchReason.TYPE_ROUTE_CONFLICT
                else:
                    # Known non-MAX rail services filter normally; rail by
                    # itself neither proves MAX relevance nor contradicts.
                    classification = RelevanceClassification.NON_MAX
            elif vehicle_type == VEHICLE_TYPE_BUS and is_max:
                conflict = MatchReason.TYPE_ROUTE_CONFLICT
            else:  # exact native "bus" on a known non-MAX route
                classification = RelevanceClassification.NON_MAX

            if conflict is not None:
                classification = RelevanceClassification.UNCLASSIFIED
                reasons.append(conflict)
            elif classification is RelevanceClassification.MAX_RELEVANT:
                enrichment_reasons, trip, shape, static_direction, direction_source, direction_conflict, next_stop = _max_vehicle_enrichment(
                    vehicle, route_id, vehicle.trip_id, context
                )
                reasons.extend(enrichment_reasons)
                enrichment = _vehicle_enrichment(frozenset(enrichment_reasons))
            else:
                # Known non-MAX with a recognized type: ordinary filtering.
                # A trip id mapping to a MAX trip stays an explicit
                # contradiction and never promotes the record to MAX.
                if isinstance(vehicle.trip_id, str) and vehicle.trip_id != "":
                    trip_route = context.trip_to_route.get(vehicle.trip_id)
                    if trip_route in context.max_route_ids:
                        classification = RelevanceClassification.UNCLASSIFIED
                        reasons.append(MatchReason.TRIP_ROUTE_CONFLICT)

        record = MatchedVehicle(
            classification=classification,
            enrichment=enrichment,
            vehicle=vehicle,
            route_id=route_id or "",
            reasons=tuple(reasons),
            max_route=max_route,
            trip=trip,
            shape=shape,
            reported_direction=vehicle.direction,
            static_direction=static_direction,
            direction_source=direction_source,
            direction_conflict=direction_conflict,
            next_stop=next_stop,
        )
        collector.add_record_reasons(record_key, record.reasons)
        records.append(record)
    return tuple(records)


# ---------------------------------------------------------------------------
# TripUpdate matching (plan §8–§9)
# ---------------------------------------------------------------------------


def _match_trip_updates(
    feed_model: TripUpdateFeed,
    context: MatchingContext,
    collector: _DiagnosticCollector,
) -> tuple[MatchedTripUpdate, ...]:
    records: list[MatchedTripUpdate] = []
    for entity in feed_model.entities:
        record_key = entity.entity_id
        trip_descriptor = entity.trip_update.trip
        trip_id = trip_descriptor.trip_id
        reasons: list[MatchReason] = []
        classification = RelevanceClassification.UNCLASSIFIED
        enrichment: EnrichmentState | None = None
        static_route_id: str | None = None
        trip = None
        stop_visits: tuple[MatchedStopUpdate, ...] = ()
        descriptor_route_conflict = False
        unsupported_operational = (
            entity.is_deleted
            or (
                trip_descriptor.schedule_relationship_raw is not None
                and trip_descriptor.schedule_relationship_raw != _SCHEDULED
            )
        )
        if unsupported_operational:
            reasons.append(MatchReason.UNSUPPORTED_OPERATIONAL_RELATIONSHIP)

        static_route = context.trip_to_route.get(trip_id)
        if static_route is None:
            # Unknown through the full static index: UNCLASSIFIED, entity
            # preserved — never relabeled NON_MAX for missing the MAX-only
            # lookup.  The unknown trip is diagnosed (reason vocabulary
            # includes unknown route/trip); nothing is silently dropped.
            reasons.append(MatchReason.UNKNOWN_TRIP)
            record = MatchedTripUpdate(
                classification=classification,
                enrichment=None,
                entity=entity,
                trip_id=trip_id,
                static_route_id=None,
                trip=None,
                stop_visits=(),
                reasons=tuple(reasons),
                descriptor_route_conflict=False,
                unsupported_operational_relationship=unsupported_operational,
            )
            collector.add_record_reasons(record_key, record.reasons)
            records.append(record)
            continue

        static_route_id = static_route
        if static_route in context.max_route_ids:
            classification = RelevanceClassification.MAX_RELEVANT
            if (
                trip_descriptor.route_id is not None
                and trip_descriptor.route_id != static_route
            ):
                # Descriptor route_id is never a fallback classification
                # source; an inconsistency prevents confident trip/stop
                # enrichment while the entity, the raw descriptor and the
                # static lookup result are all retained separately.
                descriptor_route_conflict = True
                reasons.append(MatchReason.TRIP_ROUTE_CONFLICT)
            else:
                # build_matching_context guarantees indexed MAX trips exist
                # in the network; this is a defensive re-check.
                trip = context.network.trips.get(trip_id)
                if trip is None:  # pragma: no cover - guarded by builder checks
                    raise ContextPreparationError(
                        f"indexed MAX trip {trip_id} has no network trip object",
                        code="index_inconsistency",
                    )
                stop_visits = _resolve_stop_updates(entity, trip_id, context)
                for matched_update in stop_visits:
                    reasons.extend(matched_update.reasons)
            enrichment = _tripupdate_enrichment(
                descriptor_route_conflict=descriptor_route_conflict,
                stop_reasons=tuple(
                    reason for matched_update in stop_visits for reason in matched_update.reasons
                ),
            )
        else:
            # Known non-MAX trip: ordinary filtering with enrichment kept
            # None.  A descriptor conflict is recorded as a diagnostic but
            # never changes the filtered classification.
            classification = RelevanceClassification.NON_MAX
            if (
                trip_descriptor.route_id is not None
                and trip_descriptor.route_id != static_route
            ):
                reasons.append(MatchReason.TRIP_ROUTE_CONFLICT)

        record = MatchedTripUpdate(
            classification=classification,
            enrichment=enrichment,
            entity=entity,
            trip_id=trip_id,
            static_route_id=static_route_id,
            trip=trip,
            stop_visits=stop_visits,
            reasons=tuple(reasons),
            descriptor_route_conflict=descriptor_route_conflict,
            unsupported_operational_relationship=unsupported_operational,
        )
        collector.add_record_reasons(record_key, record.reasons)
        records.append(record)
    return tuple(records)


def _resolve_stop_updates(
    entity: TripUpdateEntity,
    trip_id: str,
    context: MatchingContext,
) -> tuple[MatchedStopUpdate, ...]:
    """Resolve each stop-time update, trip-local and order-preserving."""
    matched: list[MatchedStopUpdate] = []
    for update in entity.trip_update.stop_time_updates:
        resolution = resolve_stop_visit(
            stop_sequence=update.stop_sequence,
            stop_id=update.stop_id,
            visits_by_sequence=context.visits_by_sequence[trip_id],
            visits_by_stop=context.visits_by_stop[trip_id],
            stops_catalog=context.network.stops,
        )
        matched.append(
            MatchedStopUpdate(
                update=update,
                resolution=resolution,
                reasons=resolution.reasons,
            )
        )
    return tuple(matched)


# ---------------------------------------------------------------------------
# Alert matching (plan §10): reuse the S9 classifier unchanged
# ---------------------------------------------------------------------------


def _match_alerts(
    snapshot: AlertsSnapshot,
    context: MatchingContext,
    collector: _DiagnosticCollector,
) -> tuple[MatchedAlert, ...]:
    records: list[MatchedAlert] = []
    for alert in snapshot.alerts:
        result = classify_alert(alert)
        classification = RelevanceClassification.UNCLASSIFIED
        enrichment: EnrichmentState | None = None
        if result.classification is AlertClassification.MAX_RELEVANT:
            classification = RelevanceClassification.MAX_RELEVANT
            enrichment = EnrichmentState.MATCHED
        elif result.classification is AlertClassification.NOT_MAX_RELEVANT:
            classification = RelevanceClassification.NON_MAX
        affected: list = []
        if classification is RelevanceClassification.MAX_RELEVANT:
            if result.basis == "system_wide_flag":
                # A system-wide alert affects the context's whole MAX set,
                # in the canonical numeric order of the whitelist ids.
                affected = [
                    context.network.routes[route_id]
                    for route_id in sorted(context.max_route_ids, key=int)
                    if route_id in context.network.routes
                ]
            else:
                affected = [
                    context.network.routes[str(child.route_id)]
                    for child in sorted(
                        (c for c in alert.routes
                         if str(c.route_id) in context.max_route_ids),
                        key=lambda child: int(str(child.route_id)),
                    )
                    if str(child.route_id) in context.network.routes
                ]
        record = MatchedAlert(
            classification=classification,
            enrichment=enrichment,
            alert=alert,
            result=result,
            affected_max_routes=tuple(affected),
            reasons=(),
        )
        # The classifier's own diagnostics travel inside `result` unchanged;
        # S11 adds none for alerts (plan §10).
        records.append(record)
    return tuple(records)


# ---------------------------------------------------------------------------
# Delivery adapter (plan §11)
# ---------------------------------------------------------------------------

_FEED_MODEL_TYPES: dict[PollFeed, type] = {
    PollFeed.VEHICLES: VehiclesSnapshot,
    PollFeed.TRIPUPDATES: TripUpdateFeed,
    PollFeed.ALERTS: AlertsSnapshot,
}


def match_delivery(delivery: PollDelivery, context: MatchingContext) -> MatchedBatch:
    """Match one S10 delivery against one captured immutable context.

    Validates the feed/model agreement (inconsistent types are programmer
    input errors — a typed failure, never an empty successful batch), runs
    the matching function for that feed, and returns an immutable
    :class:`MatchedBatch` without mutating either input.  The delivery's
    own envelope metadata, completeness evidence and source parse
    diagnostics are carried through unchanged and separately from the new
    matching diagnostics.
    """
    if not isinstance(delivery, PollDelivery):
        raise MatchInputError(
            "delivery must be a PollDelivery", code="delivery_type_mismatch"
        )
    if not isinstance(context, MatchingContext):
        raise MatchInputError(
            "context must be a MatchingContext", code="invalid_context"
        )
    expected_model = _FEED_MODEL_TYPES.get(delivery.feed)
    if expected_model is None:  # pragma: no cover - closed enum
        raise MatchInputError(
            f"unknown feed {delivery.feed!r}", code="feed_model_mismatch"
        )
    if not isinstance(delivery.model, expected_model):
        raise MatchInputError(
            f"feed {delivery.feed.value} does not agree with the supplied "
            f"{type(delivery.model).__name__}",
            code="feed_model_mismatch",
        )

    collector = _DiagnosticCollector(delivery.feed.value)
    if delivery.feed is PollFeed.VEHICLES:
        records = _match_vehicles(delivery.model, context, collector)
    elif delivery.feed is PollFeed.TRIPUPDATES:
        records = _match_trip_updates(delivery.model, context, collector)
    else:
        records = _match_alerts(delivery.model, context, collector)

    matched = partial = unmatched = conflict = 0
    max_relevant = non_max = unclassified = 0
    for record in records:
        if record.classification is RelevanceClassification.MAX_RELEVANT:
            max_relevant += 1
        elif record.classification is RelevanceClassification.NON_MAX:
            non_max += 1
        else:
            unclassified += 1
        if record.enrichment is EnrichmentState.MATCHED:
            matched += 1
        elif record.enrichment is EnrichmentState.PARTIAL:
            partial += 1
        elif record.enrichment is EnrichmentState.UNMATCHED:
            unmatched += 1
        elif record.enrichment is EnrichmentState.CONFLICT:
            conflict += 1

    diagnostics, reason_totals, omitted = collector.finish()
    return MatchedBatch(
        feed=delivery.feed,
        dataset_identity=context.identity,
        sequence=delivery.sequence,
        purpose=delivery.purpose,
        request_sent_utc_ms=delivery.request_sent_utc_ms,
        receipt_utc_ms=delivery.receipt_utc_ms,
        elapsed_mono_ms=delivery.elapsed_mono_ms,
        http_status=delivery.http_status,
        evidence=delivery.evidence,
        source_diagnostics=tuple(delivery.diagnostics),
        records=records,
        input_count=len(records),
        max_relevant_count=max_relevant,
        non_max_count=non_max,
        unclassified_count=unclassified,
        matched_count=matched,
        partial_count=partial,
        unmatched_count=unmatched,
        conflict_count=conflict,
        reason_totals=reason_totals,
        diagnostics=diagnostics,
        omitted_match_diagnostics=omitted,
    )
