"""Tests for the S9 conservative alert classification (Stage S9, M2 plan REV 2 §10).

MAX-relevant iff ``system_wide_flag is True`` OR any route child id is in
``MAX_ROUTE_IDS``; location-only and ambiguous alerts are ``UNCLASSIFIED``
with diagnostics, never auto-classified.  ``MAX_ROUTE_IDS`` must be imported
exclusively from ``backend.trimet.maxfilter`` (plan §9) — verified here by
identity.
"""

from __future__ import annotations

import dataclasses

import pytest

from backend.trimet.alerts_classify import (
    MAX_ROUTE_IDS as CLASSIFIER_MAX_ROUTE_IDS,
)
from backend.trimet.alerts_classify import (
    AlertClassification,
    classify_alert,
    classify_alerts,
)
from backend.trimet.feed_types import AlertLocation, AlertRouteChild, ServiceAlert
from backend.trimet.maxfilter import MAX_ROUTE_IDS as SOURCE_MAX_ROUTE_IDS


def make_alert(**overrides: object) -> ServiceAlert:
    base: dict[str, object] = {
        "alert_id": 262613,
        "system_wide_flag": False,
        "routes": (AlertRouteChild(route_id=74, route_type="B"),),
        "locations": (),
    }
    base.update(overrides)
    return ServiceAlert(**base)  # type: ignore[arg-type]


def test_max_route_ids_imported_exclusively_from_maxfilter() -> None:
    # Plan §9: no second copy of the set anywhere.
    assert CLASSIFIER_MAX_ROUTE_IDS is SOURCE_MAX_ROUTE_IDS


def test_system_wide_flag_true_is_max_relevant() -> None:
    result = classify_alert(make_alert(system_wide_flag=True, routes=()))
    assert result.classification is AlertClassification.MAX_RELEVANT
    assert result.basis == "system_wide_flag"
    assert result.diagnostics == ()


def test_system_wide_flag_true_wins_over_route_evidence() -> None:
    result = classify_alert(make_alert(system_wide_flag=True))
    assert result.classification is AlertClassification.MAX_RELEVANT
    assert result.basis == "system_wide_flag"


@pytest.mark.parametrize("route_id", [90, 100, 190, 200, 290])
def test_max_route_child_is_max_relevant(route_id: int) -> None:
    alert = make_alert(
        routes=(AlertRouteChild(route_id=route_id, route_type="R"),)
    )
    result = classify_alert(alert)
    assert result.classification is AlertClassification.MAX_RELEVANT
    assert result.basis == f"route_match:{route_id}"


def test_non_max_route_child_with_false_flag_is_not_max_relevant() -> None:
    result = classify_alert(make_alert())
    assert result.classification is AlertClassification.NOT_MAX_RELEVANT
    assert result.basis == "no_max_route_child"
    assert result.diagnostics == ()


def test_absent_system_wide_flag_treated_as_not_system_wide() -> None:
    # Plan §6.2: absent flag → None → treated as not system-wide by the
    # classifier; the route evidence then decides.
    result = classify_alert(make_alert(system_wide_flag=None))
    assert result.classification is AlertClassification.NOT_MAX_RELEVANT


def test_absent_system_wide_flag_with_max_route_is_max_relevant() -> None:
    result = classify_alert(
        make_alert(
            system_wide_flag=None,
            routes=(AlertRouteChild(route_id=100),),
        )
    )
    assert result.classification is AlertClassification.MAX_RELEVANT


def test_location_only_alert_is_unclassified_with_diagnostic() -> None:
    alert = make_alert(
        routes=(),
        locations=(
            AlertLocation(location_id=4606, description="SE Powell & 160th"),
        ),
    )
    result = classify_alert(alert)
    assert result.classification is AlertClassification.UNCLASSIFIED
    assert result.basis == "location_only_alert"
    assert len(result.diagnostics) == 1
    diag = result.diagnostics[0]
    assert diag.reason == "unclassified_alert"
    assert diag.record_key == "262613"
    assert diag.field == "classification"


def test_unscoped_alert_is_unclassified_with_diagnostic() -> None:
    result = classify_alert(make_alert(routes=()))
    assert result.classification is AlertClassification.UNCLASSIFIED
    assert result.basis == "no_scoping_information"
    assert len(result.diagnostics) == 1


def test_unclassified_diagnostics_are_credential_free() -> None:
    result = classify_alert(make_alert(routes=()))
    blob = repr(dataclasses.asdict(result.diagnostics[0]))
    assert "appID" not in blob
    assert "http" not in blob


def test_classification_results_are_frozen() -> None:
    result = classify_alert(make_alert())
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(result, "classification", AlertClassification.MAX_RELEVANT)


def test_classify_alerts_batch_preserves_order() -> None:
    alerts = (
        make_alert(alert_id=1, routes=(AlertRouteChild(route_id=90),)),
        make_alert(alert_id=2),
        make_alert(alert_id=3, routes=()),
    )
    results = classify_alerts(alerts)
    assert [r.alert_id for r in results] == [1, 2, 3]
    assert [r.classification for r in results] == [
        AlertClassification.MAX_RELEVANT,
        AlertClassification.NOT_MAX_RELEVANT,
        AlertClassification.UNCLASSIFIED,
    ]
