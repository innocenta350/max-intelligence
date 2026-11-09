"""MAX classification for service alerts (Stage S9, M2 plan REV 2 §10).

An alert is MAX-relevant if and only if ``system_wide_flag is True`` **or**
any route child id is in ``MAX_ROUTE_IDS``.  The rule is deliberately
conservative (owner carried-forward issue #2): location-only alerts and any
alert whose classification is ambiguous come back ``UNCLASSIFIED`` with a
diagnostic — they are never auto-classified.  Rationale: S8/B4 observed no
system-wide or location-only alerts live, so those paths are evidence-limited.

``MAX_ROUTE_IDS`` is imported exclusively from
:mod:`backend.trimet.maxfilter` — never duplicated (plan §9).  The route
child ids arrive as JSON integers while ``MAX_ROUTE_IDS`` holds the GTFS
string ids (formatting preserved), so the join is ``str(child_id)``; JSON
integers cannot carry leading zeros, so the conversion is lossless.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

from backend.trimet.feed_errors import FEED_ALERTS
from backend.trimet.feed_types import FeedDiagnostic, ServiceAlert
from backend.trimet.maxfilter import MAX_ROUTE_IDS


class AlertClassification(enum.Enum):
    """Outcome of the conservative MAX rule (plan §10)."""

    MAX_RELEVANT = "max_relevant"
    NOT_MAX_RELEVANT = "not_max_relevant"
    # Ambiguous (location-only or no scoping information): recorded for later
    # review, never auto-classified.
    UNCLASSIFIED = "unclassified"


@dataclass(frozen=True, slots=True)
class AlertClassificationResult:
    """One alert's classification plus the rule that produced it."""

    alert_id: int
    classification: AlertClassification
    basis: str  # e.g. "system_wide_flag", "route_match:100", or the ambiguity rationale
    diagnostics: tuple[FeedDiagnostic, ...] = ()


def classify_alert(alert: ServiceAlert) -> AlertClassificationResult:
    """Classify one alert conservatively; ambiguous cases yield diagnostics."""
    if alert.system_wide_flag is True:
        return AlertClassificationResult(
            alert_id=alert.alert_id,
            classification=AlertClassification.MAX_RELEVANT,
            basis="system_wide_flag",
        )
    for child in alert.routes:
        if str(child.route_id) in MAX_ROUTE_IDS:
            return AlertClassificationResult(
                alert_id=alert.alert_id,
                classification=AlertClassification.MAX_RELEVANT,
                basis=f"route_match:{child.route_id}",
            )
    if alert.routes:
        # Scoped to specific routes, none of them MAX; an absent
        # system_wide_flag is treated as not system-wide (plan §6.2).
        return AlertClassificationResult(
            alert_id=alert.alert_id,
            classification=AlertClassification.NOT_MAX_RELEVANT,
            basis="no_max_route_child",
        )
    # No route children: location-only or entirely unscoped — evidence-limited
    # paths (S8/B4 observed neither live), so never auto-classified.
    basis = (
        "location_only_alert"
        if alert.locations
        else "no_scoping_information"
    )
    diagnostic = FeedDiagnostic(
        feed=FEED_ALERTS,
        record_key=str(alert.alert_id),
        field="classification",
        reason="unclassified_alert",
        detail=f"{basis}; left for later review, never auto-classified",
    )
    return AlertClassificationResult(
        alert_id=alert.alert_id,
        classification=AlertClassification.UNCLASSIFIED,
        basis=basis,
        diagnostics=(diagnostic,),
    )


def classify_alerts(
    alerts: tuple[ServiceAlert, ...],
) -> tuple[AlertClassificationResult, ...]:
    """Classify a snapshot's alerts in order."""
    return tuple(classify_alert(alert) for alert in alerts)
