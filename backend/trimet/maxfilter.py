"""MAX route identification (Stage S5, M1 plan §E.1).

Hard gates, unchanged from Rev. 2 through Rev. 9:

- ``MAX_ROUTE_IDS = frozenset({"90", "100", "190", "200", "290"})`` — string
  keys, because GTFS ids are strings and preserve formatting.
- ``apply_max_filter`` keeps a route only if (a) its id is on the approved
  whitelist and (c) the M0 routeConfig cross-check classifies it as
  ``routeSubType == "Light Rail"``. Condition (b) of the original three-part
  rule (``route_type == 0``) is enforced upstream by the S4 parser, whose
  whitelist filter is strictly stronger for MAX identification: M0/S3
  evidence shows ``route_type == 0`` matches eight routes (the five MAX
  routes *plus* Portland Streetcar 193/194/195), so the whitelist alone —
  cross-checked against the M0 evidence — is the defensible realization of
  §E.1 on the S4 interface (``Route`` carries no ``route_type``). Documented
  as a deliberate deviation, not a silent reinterpretation.
- Exactly five survivors are expected; a whitelisted id missing from the
  feed is a hard failure (TriMet renumbering detection).
- ``agency_id`` is an inspection + consistency-check value only — never an
  independent hard rule (Rev. 2 change 2).

The M0 evidence is *referenced in place* through
``GtfsConfig.m0_routeconfig_evidence_path`` (Rev. 2 change 6): this module
reads it at load time and never copies or regenerates it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from backend.gtfs.errors import MaxInvariantError, SchemaViolationError
from backend.gtfs.types import Route

__all__ = [
    "MAX_ROUTE_IDS",
    "RouteConfigRoute",
    "apply_max_filter",
    "load_routeconfig_evidence",
]

MAX_ROUTE_IDS: frozenset[str] = frozenset({"90", "100", "190", "200", "290"})


@dataclass(frozen=True, slots=True)
class RouteConfigRoute:
    """One route from the M0 routeConfig evidence, normalized for cross-checks.

    ``direction_names`` maps the routeConfig ``dir`` value to the stripped
    direction description. Trailing spaces in the raw evidence are a
    documented M0 finding (§2.3); they are display noise, so the fallback
    source strips them (the GTFS-sourced names in ``Route.direction_names``
    are preserved verbatim by S4's ``get_str``).
    """

    route_id: str
    route_subtype: str
    direction_names: Mapping[int, str]


def load_routeconfig_evidence(path: Path) -> dict[str, RouteConfigRoute]:
    """Load the M0 routeConfig evidence file (referenced, never duplicated).

    Raises :class:`SchemaViolationError` when the file is missing, unreadable,
    or does not carry the documented ``summary.light_rail_routes`` structure —
    the §E.1 cross-check is mandatory, so a missing evidence file stops the
    load instead of silently skipping the check.
    """
    file_name = path.name
    if not path.is_file():
        raise SchemaViolationError(
            "M0 routeConfig evidence file is missing; the Light Rail "
            "cross-check cannot run",
            file=file_name,
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SchemaViolationError(
            f"M0 routeConfig evidence file is unreadable ({exc.__class__.__name__})",
            file=file_name,
        ) from exc
    entries = (
        payload.get("summary", {}).get("light_rail_routes")
        if isinstance(payload, dict)
        else None
    )
    if not isinstance(entries, list):
        raise SchemaViolationError(
            "M0 routeConfig evidence has no summary.light_rail_routes list",
            file=file_name,
        )
    routes: dict[str, RouteConfigRoute] = {}
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise SchemaViolationError(
                f"M0 routeConfig evidence entry {index} is not an object",
                file=file_name,
            )
        raw_id = entry.get("route")
        subtype = entry.get("routeSubType")
        if raw_id is None or subtype is None:
            raise SchemaViolationError(
                f"M0 routeConfig evidence entry {index} lacks "
                "route/routeSubType",
                file=file_name,
            )
        directions: dict[int, str] = {}
        for sample in entry.get("direction_samples") or []:
            if not isinstance(sample, dict):
                continue
            raw_dir = sample.get("dir")
            desc = sample.get("desc")
            if raw_dir is None or desc is None:
                continue
            try:
                directions[int(raw_dir)] = desc.strip()
            except (TypeError, ValueError):
                continue
        routes[str(raw_id)] = RouteConfigRoute(
            route_id=str(raw_id),
            route_subtype=str(subtype),
            direction_names=directions,
        )
    return routes


def apply_max_filter(
    routes: Mapping[str, Route],
    evidence: Mapping[str, RouteConfigRoute],
) -> tuple[dict[str, Route], tuple[str, ...]]:
    """Apply the §E.1 MAX filter; return the survivors plus accounting notes.

    Hard failures (``MaxInvariantError`` — §E.7 invariant 2 lives here too):

    - a non-whitelist route id reached the filter (the S4 parser should have
      excluded it — defense in depth against direct construction);
    - a whitelisted id is missing (TriMet renumbering detection);
    - the M0 evidence does not classify a survivor as ``routeSubType ==
      "Light Rail"``.

    Notes (recorded, never silent): Light Rail routes in the evidence that
    are not on the approved whitelist — a new Light Rail line requires an
    approved whitelist change, so it is reported, not failed.
    """
    unexpected = sorted(set(routes) - MAX_ROUTE_IDS)
    if unexpected:
        raise MaxInvariantError(
            "non-whitelist route id(s) reached the MAX filter: "
            + ", ".join(unexpected)
        )
    missing = sorted(MAX_ROUTE_IDS - set(routes))
    if missing:
        raise MaxInvariantError(
            "whitelisted MAX route id(s) are missing from the feed "
            "(possible TriMet route renumbering): " + ", ".join(missing)
        )
    unverified = sorted(
        route_id
        for route_id in MAX_ROUTE_IDS
        if evidence.get(route_id) is None
        or evidence[route_id].route_subtype != "Light Rail"
    )
    if unverified:
        raise MaxInvariantError(
            "M0 routeConfig evidence does not classify MAX route(s) as "
            "Light Rail: " + ", ".join(unverified)
        )
    extra = sorted(set(evidence) - MAX_ROUTE_IDS)
    notes = tuple(
        f"M0 routeConfig evidence lists Light Rail route {route_id}, which "
        "is not on the approved MAX whitelist"
        for route_id in extra
    )
    return {route_id: routes[route_id] for route_id in sorted(MAX_ROUTE_IDS)}, notes
