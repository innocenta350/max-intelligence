"""Exact trip-local stop-visit resolution (Stage S11, M2 plan §9).

Realtime stop selectors resolve **within one resolved MAX trip's own**
``stop_times`` list — never globally by stop id and never by array offset.
Stop sequences are *values*, not positions: they need not start at 1 or be
contiguous, and ``0`` is handled explicitly rather than as a falsy missing
value.

Selector matrix (plan §9, one row per case — no first/nearest fallback is
ever applied):

- **sequence + stop id** — the sequence must identify a visit *and* its stop
  id must agree; otherwise the resolution is an explicit conflict.
- **sequence only** — the exact visit resolves; an unknown sequence is
  unresolved (``unknown_stop_sequence``).
- **stop id only** — resolves only when that id occurs exactly once in this
  trip; repeated visits are ambiguous (``ambiguous_stop_visit``) and require
  the sequence.
- **neither** — unresolved (``missing_stop_selectors``).

A resolved visit whose referenced stop is missing from the network's stops
catalog keeps the visit reference and raw prediction, marked with
``missing_stop_details`` — no stop name/location is invented and nothing is
discarded.
"""

from __future__ import annotations

from collections.abc import Mapping

from backend.gtfs.types import Stop, TripStopEntry
from backend.trimet.match_errors import MatchReason
from backend.trimet.match_types import (
    StopResolutionStatus,
    StopVisitResolution,
)

__all__ = [
    "resolve_stop_visit",
]


def _is_int(value: object) -> bool:
    """True for real integers (``bool`` is an ``int`` subclass and excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def resolve_stop_visit(
    *,
    stop_sequence: object,
    stop_id: object,
    visits_by_sequence: Mapping[int, TripStopEntry],
    visits_by_stop: Mapping[str, tuple[TripStopEntry, ...]],
    stops_catalog: Mapping[str, Stop] | None = None,
) -> StopVisitResolution:
    """Resolve one live stop selector set against one trip's visit indexes.

    ``stop_sequence`` must be a real integer (or ``None``/absent) — zero and
    noncontiguous values are legitimate.  ``stop_id`` must be a nonblank
    string; blank or non-string values count as absent selectors.  Neither
    selector value is normalized: unknown means unknown.
    """
    sequence_ok = _is_int(stop_sequence)
    stop_id_ok = isinstance(stop_id, str) and stop_id != ""

    if not sequence_ok and not stop_id_ok:
        return StopVisitResolution(
            status=StopResolutionStatus.UNRESOLVED,
            reasons=(MatchReason.MISSING_STOP_SELECTORS,),
        )

    if sequence_ok:
        visit = visits_by_sequence.get(stop_sequence)  # type: ignore[arg-type]
        if visit is None:
            return StopVisitResolution(
                status=StopResolutionStatus.UNRESOLVED,
                reasons=(MatchReason.UNKNOWN_STOP_SEQUENCE,),
            )
        if not stop_id_ok:
            # Sequence-only: that exact visit resolves.
            return _maybe_missing_details(visit, stops_catalog)
        if stop_id != visit.stop_id:  # type: ignore[union-attr]
            # Explicit disagreement: conflict, never a fallback guess.
            return StopVisitResolution(
                status=StopResolutionStatus.CONFLICT,
                reasons=(MatchReason.STOP_ID_SEQUENCE_CONFLICT,),
            )
        return _maybe_missing_details(visit, stops_catalog)

    # Stop id only.
    candidates = visits_by_stop.get(stop_id)  # type: ignore[arg-type]
    if not candidates:
        return StopVisitResolution(
            status=StopResolutionStatus.UNRESOLVED,
            reasons=(MatchReason.UNKNOWN_STOP_ID,),
        )
    if len(candidates) > 1:
        # Repeated visit: the sequence is required; selecting the first or
        # nearest occurrence is deliberately not attempted.
        return StopVisitResolution(
            status=StopResolutionStatus.AMBIGUOUS,
            reasons=(MatchReason.AMBIGUOUS_STOP_VISIT,),
        )
    return _maybe_missing_details(candidates[0], stops_catalog)


def _maybe_missing_details(
    visit: TripStopEntry, stops_catalog: Mapping[str, Stop] | None
) -> StopVisitResolution:
    """Attach descriptive stop metadata when the catalog carries it."""
    stop = None
    missing = False
    if stops_catalog is not None:
        stop = stops_catalog.get(visit.stop_id)
        missing = stop is None
    return StopVisitResolution(
        status=StopResolutionStatus.RESOLVED,
        visit=visit,
        reasons=(MatchReason.MISSING_STOP_DETAILS,) if missing else (),
        stop=stop,
        stop_details_missing=missing,
    )
