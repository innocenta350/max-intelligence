"""S11 stop-visit tests: the exact trip-local selector matrix (plan §9, §13 row 8).

Every row of the §9 matrix has a named test, plus the zero-sequence case
(``0`` is a legitimate sequence value, never a falsy missing value), the
repeated-visit ambiguity requirement, and the missing-stop-details
annotation path.
"""

from __future__ import annotations

import pytest

from backend.gtfs.types import Stop, TripStopEntry
from backend.trimet.match_errors import MatchReason
from backend.trimet.match_types import StopResolutionStatus
from backend.trimet.stop_match import resolve_stop_visit
from tests._s11_helpers import install_socket_guard


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    install_socket_guard(monkeypatch)


def _trip_indexes():
    """One trip's visits: sequences 9 and 12 (non-1-start, non-contiguous),
    with stop ``14000`` visited twice and ``14001`` once."""
    visits = (
        TripStopEntry("14001", 9),
        TripStopEntry("14000", 12),
        TripStopEntry("14000", 15),
        TripStopEntry("14024-dangling", 18),
    )
    by_sequence = {9: visits[0], 12: visits[1], 15: visits[2], 18: visits[3]}
    by_stop = {
        "14000": (visits[1], visits[2]),
        "14001": (visits[0],),
        "14024-dangling": (visits[3],),
    }
    stops = {
        "14000": Stop("14000", "SW Bridge", 45.5, -122.67, 0, None),
        "14001": Stop("14001", "SE Station", 45.51, -122.66, 0, None),
    }
    return visits, by_sequence, by_stop, stops


def _resolve(**selectors):
    _, by_sequence, by_stop, stops = _trip_indexes()
    return resolve_stop_visit(
        visits_by_sequence=by_sequence,
        visits_by_stop=by_stop,
        stops_catalog=stops,
        **selectors,
    )


# --- Both selectors ---------------------------------------------------------


def test_sequence_and_matching_stop_id_resolves_exact_visit() -> None:
    resolution = _resolve(stop_sequence=9, stop_id="14001")
    assert resolution.status is StopResolutionStatus.RESOLVED
    assert resolution.visit.stop_sequence == 9
    assert resolution.visit.stop_id == "14001"
    assert resolution.reasons == ()
    assert resolution.stop is not None and resolution.stop is not None
    assert resolution.stop_details_missing is False
    assert resolution.stop.stop_name == "SE Station"


def test_sequence_with_disagreeing_stop_id_is_a_conflict() -> None:
    resolution = _resolve(stop_sequence=9, stop_id="14000")
    assert resolution.status is StopResolutionStatus.CONFLICT
    assert resolution.visit is None  # no fallback guess, no enrichment
    assert resolution.reasons == (MatchReason.STOP_ID_SEQUENCE_CONFLICT,)


def test_zero_sequence_resolves_explicitly_not_falsily() -> None:
    _, by_sequence, by_stop, _stops = _trip_indexes()
    by_sequence[0] = TripStopEntry("14002", 0)
    resolution = resolve_stop_visit(
        stop_sequence=0,
        stop_id="14002",
        visits_by_sequence=by_sequence,
        visits_by_stop=by_stop,
    )
    assert resolution.status is StopResolutionStatus.RESOLVED
    assert resolution.visit.stop_sequence == 0


# --- Sequence only ----------------------------------------------------------


def test_sequence_only_resolves_that_exact_visit() -> None:
    resolution = _resolve(stop_sequence=12, stop_id=None)
    assert resolution.status is StopResolutionStatus.RESOLVED
    assert resolution.visit.stop_id == "14000"
    assert resolution.visit.stop_sequence == 12


def test_unknown_sequence_is_unresolved_not_a_stop_id_fallback() -> None:
    resolution = _resolve(stop_sequence=99, stop_id="14000")
    # The sequence *was* reported, so the stop id cannot fill in for it.
    assert resolution.status is StopResolutionStatus.UNRESOLVED
    assert resolution.reasons == (MatchReason.UNKNOWN_STOP_SEQUENCE,)
    assert resolution.visit is None


# --- Stop id only -----------------------------------------------------------


def test_unique_stop_id_only_resolves() -> None:
    resolution = _resolve(stop_sequence=None, stop_id="14001")
    assert resolution.status is StopResolutionStatus.RESOLVED
    assert resolution.visit.stop_sequence == 9


def test_repeated_stop_id_is_ambiguous_and_requires_the_sequence() -> None:
    resolution = _resolve(stop_sequence=None, stop_id="14000")
    assert resolution.status is StopResolutionStatus.AMBIGUOUS
    assert resolution.visit is None
    assert resolution.reasons == (MatchReason.AMBIGUOUS_STOP_VISIT,)


def test_repeated_stop_id_with_sequence_disambiguates() -> None:
    resolution = _resolve(stop_sequence=15, stop_id="14000")
    assert resolution.status is StopResolutionStatus.RESOLVED
    assert resolution.visit.stop_sequence == 15


def test_unknown_stop_id_is_unresolved() -> None:
    resolution = _resolve(stop_sequence=None, stop_id="not-in-this-trip")
    assert resolution.status is StopResolutionStatus.UNRESOLVED
    assert resolution.reasons == (MatchReason.UNKNOWN_STOP_ID,)


def test_blank_or_bad_selector_types_count_as_absent() -> None:
    resolution = _resolve(stop_sequence=None, stop_id="")
    assert resolution.reasons == (MatchReason.MISSING_STOP_SELECTORS,)
    resolution = _resolve(stop_sequence="12", stop_id=None)  # not an int
    assert resolution.reasons == (MatchReason.MISSING_STOP_SELECTORS,)


# --- Neither selector -------------------------------------------------------


def test_neither_selector_is_unresolved() -> None:
    resolution = _resolve(stop_sequence=None, stop_id=None)
    assert resolution.status is StopResolutionStatus.UNRESOLVED
    assert resolution.reasons == (MatchReason.MISSING_STOP_SELECTORS,)


# --- Missing stop details ---------------------------------------------------


def test_resolved_visit_with_unknown_stop_keeps_visit_and_marks_the_gap() -> None:
    resolution = _resolve(stop_sequence=18, stop_id="14024-dangling")
    assert resolution.status is StopResolutionStatus.RESOLVED
    assert resolution.visit is not None
    assert resolution.stop is None
    assert resolution.stop_details_missing is True
    assert resolution.reasons == (MatchReason.MISSING_STOP_DETAILS,)


def test_resolved_visit_with_present_stop_carries_metadata() -> None:
    resolution = _resolve(stop_sequence=9, stop_id="14001")
    assert resolution.stop_details_missing is False
    assert resolution.stop.stop_id == "14001"
    assert resolution.stop.stop_name == "SE Station"
