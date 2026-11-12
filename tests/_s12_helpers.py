"""Shared S12 test helpers (offline; never contacts any host).

Provides:

- :func:`install_socket_guard` + the autouse fixture ``s12_deny_all_sockets``
  — a **deny-all** guard (no loopback exception: the S12 modules and tests
  make no socket call at all) covering ``getaddrinfo``, ``connect``,
  ``connect_ex`` and ``create_connection`` before any external access.
  Declared module-local in each collected S12 module by importing the
  fixture; compatible with pytest's function-scoped ``monkeypatch``.
- Tiny internally consistent MAX worlds (presets A/B/C) built directly for
  the S11 shared pure builder ``build_matching_context`` — no real dataset,
  no promoted directories needed for the store tests.
- Fully parameterized S9 model + ``PollDelivery`` builders (custom source
  query times, provenance pairing, purposes, sequences, receipt times,
  evidence) so ordering, replay and authority cases are deterministic.
- :func:`patch_match` — the deterministic injection point around the
  store's matching call for barrier/event concurrency tests (no sleeps).

Every helper is synthetic: no real machine GTFS, no ``.env``, no probe
imports.  Temporary artifacts belong in pytest ``tmp_path`` directories.
"""
from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import pytest

from backend.gtfs.types import (
    MaxNetwork,
    Route,
    ShapeGeometry,
    ShapePoint,
    Stop,
    Trip,
    TripStopEntry,
)
from backend.trimet.feed_types import (
    AlertLocation,
    AlertRouteChild,
    AlertsSnapshot,
    FeedDiagnostic,
    ServiceAlert,
    StopTimeEvent,
    StopTimeUpdate,
    TripDescriptor,
    TripUpdate,
    TripUpdateEntity,
    TripUpdateFeed,
    TripUpdateFeedHeader,
    VehiclesRequestKind,
    VehiclesRequestProvenance,
    VehiclesSnapshot,
    VehiclePosition,
)
from backend.trimet.live_state import LiveState
from backend.trimet.match_types import DatasetIdentity, MatchingContext, RouteRow, TripRow
from backend.trimet.match_index import build_matching_context
from backend.trimet.poll_types import PollDelivery, PollFeed, RequestPurpose, VehicleCollectionEvidence
from backend.trimet.state_sink import StateSession

# Plausible fixed source-timestamp era (same fixture era as S9–S11).
BASE_MS = 1_794_024_790_000

SHA_A = "aa" * 32
SHA_B = "bb" * 32
SHA_C = "cc" * 32


# ---------------------------------------------------------------------------
# Deny-all socket guard (row 17: no network, no DNS, no external contact)
# ---------------------------------------------------------------------------


def install_socket_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise before *any* DNS resolution or connection attempt.

    Unlike the S10/S11 loopback-tolerant convention, the S12 guard denies
    everything — the store and its tests never make a socket call, so no
    loopback exception is ever needed and a fake host is no permission.
    """

    def denied(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "S12 socket guard: network and DNS access are denied in offline "
            "state tests"
        )

    monkeypatch.setattr(socket, "getaddrinfo", denied, raising=True)
    monkeypatch.setattr(socket, "create_connection", denied, raising=True)
    monkeypatch.setattr(socket.socket, "connect", denied, raising=True)
    monkeypatch.setattr(socket.socket, "connect_ex", denied, raising=True)


@pytest.fixture(autouse=True)
def s12_deny_all_sockets(monkeypatch: pytest.MonkeyPatch):
    """Module-local autouse guard; import into every collected S12 module."""
    install_socket_guard(monkeypatch)
    yield


# ---------------------------------------------------------------------------
# Tiny synthetic MAX worlds for the shared pure builder
# ---------------------------------------------------------------------------


def _route(route_id: str) -> Route:
    return Route(
        route_id=route_id,
        direction_names={0: "eastbound", 1: "westbound"},
        route_short_name=route_id,
        route_long_name=f"MAX {route_id} Line" if route_id in ("90", "100", "190", "200", "290") else f"Service {route_id}",
        route_color=None,
    )


@dataclass(frozen=True, slots=True)
class S12World:
    """One tiny world plus the normalized rows for the shared builder."""

    network: MaxNetwork
    route_rows: tuple[RouteRow, ...]
    trip_rows: tuple[TripRow, ...]


def s12_world(
    *,
    include_route_90: bool = True,
    include_trip_t90a: bool = True,
    feed_version: str = "test-feed-001",
) -> S12World:
    """Build a small internally consistent world.

    Contents: MAX route ``90`` (optional), bus route ``1`` (mode 3),
    Streetcar ``193`` (mode 0); MAX trips ``t90A`` (full shape + two
    visits, optional), ``t90B`` (blank direction/shape, one visit), and
    ``t90F`` (**visitless** — zero stop-time rows, the M1-tolerated case);
    one non-MAX trip row each on routes 1 and 193.
    """
    routes: dict[str, Route] = {}
    route_rows: list[RouteRow] = []
    if include_route_90:
        routes["90"] = _route("90")
        route_rows.append(RouteRow("90", 0))
    # Non-MAX service exists only in the normalized rows, never in the
    # network: the builder requires network.routes ⊆ the MAX whitelist and
    # exact agreement with the MAX subset of route_rows.
    route_rows.extend([RouteRow("1", 3), RouteRow("193", 0)])

    stops = {
        "14000": Stop("14000", "SW 1st", 45.5, -122.67, 0, None),
        "14001": Stop("14001", "SE 2nd", 45.51, -122.66, 0, None),
        "14002": Stop("14002", "Lloyd", 45.53, -122.65, 0, None),
    }
    shapes = {
        "sh90": ShapeGeometry(
            "sh90",
            (ShapePoint(1, 45.5, -122.67), ShapePoint(2, 45.51, -122.66)),
        )
    }
    trips: dict[str, Trip] = {}
    stop_times: dict[str, tuple[TripStopEntry, ...]] = {}
    trip_rows: list[TripRow] = []

    if include_route_90:
        if include_trip_t90a:
            trips["t90A"] = Trip("t90A", "90", 0, "sh90")
            stop_times["t90A"] = (
                TripStopEntry("14000", 1, 25200, 25230),
                TripStopEntry("14001", 2, None, 25330),
            )
            trip_rows.append(TripRow("t90A", "90", 0, "sh90"))
        trips["t90B"] = Trip("t90B", "90", None, None)
        stop_times["t90B"] = (TripStopEntry("14002", 7, None, None),)
        trip_rows.append(TripRow("t90B", "90", None, None))
        # Visitless MAX trip: a valid trip row with zero stop-time rows.
        trips["t90F"] = Trip("t90F", "90", 1, "sh90")
        stop_times["t90F"] = ()
        trip_rows.append(TripRow("t90F", "90", 1, "sh90"))
    trip_rows.extend([TripRow("tb1", "1", None, None), TripRow("t193", "193", None, None)])

    network = MaxNetwork(
        feed_version=feed_version,
        feed_start_date="20261011",
        feed_end_date="20261021",
        routes=routes,
        stops=stops,
        trips=trips,
        stop_times=stop_times,
        shapes=shapes,
    )
    return S12World(network=network, route_rows=tuple(route_rows), trip_rows=tuple(trip_rows))


def s12_identity(
    sha256: str,
    *,
    feed_version: str = "test-feed-001",
    source_directory_name: str | None = None,
) -> DatasetIdentity:
    return DatasetIdentity(
        archive_sha256=sha256,
        feed_version=feed_version,
        source_directory_name=source_directory_name,
    )


def s12_context(
    world: S12World,
    *,
    sha256: str,
    feed_version: str = "test-feed-001",
    source_directory_name: str | None = None,
) -> MatchingContext:
    """Build one immutable context through the one shared pure builder."""
    return build_matching_context(
        s12_identity(
            sha256,
            feed_version=feed_version,
            source_directory_name=source_directory_name,
        ),
        world.network,
        world.route_rows,
        world.trip_rows,
    )


def make_state(
    context: MatchingContext,
    **limit_overrides: Any,
) -> LiveState:
    """A fresh LiveState with default or explicitly overridden limits."""
    from backend.trimet.state_types import StateLimits

    limits = StateLimits(**limit_overrides) if limit_overrides else None
    return LiveState(context, limits=limits)


def open_state(
    context: MatchingContext,
    **limit_overrides: Any,
) -> tuple[LiveState, StateSession]:
    state = make_state(context, **limit_overrides)
    return state, state.open_session()


# ---------------------------------------------------------------------------
# Deterministic matching injection for concurrency tests
# ---------------------------------------------------------------------------


def patch_match(
    monkeypatch: pytest.MonkeyPatch,
    hook: Callable[[PollDelivery, MatchingContext], None],
) -> None:
    """Wrap the store's ``match_delivery`` with a deterministic hook.

    The hook runs before the real pure matching on every call.  It may
    block on events, perform other store operations, or raise injected
    failures — barriers and events only, never sleeps.
    """
    import backend.trimet.live_state as live_state_module

    real_match = live_state_module.match_delivery

    def wrapper(delivery: PollDelivery, context: MatchingContext):
        hook(delivery, context)
        return real_match(delivery, context)

    monkeypatch.setattr(
        live_state_module, "match_delivery", wrapper, raising=True
    )


# ---------------------------------------------------------------------------
# S9 vehicle models + fully parameterized vehicle deliveries
# ---------------------------------------------------------------------------


def vehicle_position(
    vehicle_id: int,
    *,
    route_number: int = 90,
    vehicle_type: str = "rail",
    time_ms: int,
    direction: int | None = 0,
    trip_id: str | None = None,
    next_loc_id: int | None = None,
    next_stop_seq: int | None = None,
    latitude: float = 45.52,
    longitude: float = -122.63,
    **overrides: Any,
) -> VehiclePosition:
    kwargs: dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "vehicle_type": vehicle_type,
        "route_number": route_number,
        "latitude": latitude,
        "longitude": longitude,
        "time_utc_ms": time_ms,
        "expires_utc_ms": time_ms + 90_000,
        "direction": direction,
        "trip_id": trip_id,
        "next_loc_id": next_loc_id,
        "next_stop_seq": next_stop_seq,
    }
    kwargs.update(overrides)
    return VehiclePosition(**kwargs)


def vehicles_snapshot(
    vehicles: Iterable[VehiclePosition],
    *,
    query_time_ms: int,
    request_kind: VehiclesRequestKind = VehiclesRequestKind.DEFAULT,
    since_ms: int | None = None,
    requested_at_ms: int | None = None,
) -> VehiclesSnapshot:
    if requested_at_ms is None:
        requested_at_ms = query_time_ms
    return VehiclesSnapshot(
        provenance=VehiclesRequestProvenance(
            request_kind=request_kind,
            since_ms=since_ms,
            requested_at_utc_ms=requested_at_ms,
        ),
        query_time_utc_ms=query_time_ms,
        vehicles=tuple(vehicles),
    )


def complete_evidence(count: int, *, rejected: int = 0) -> VehicleCollectionEvidence:
    return VehicleCollectionEvidence(
        key_present=True, is_list=True, raw_count=count, rejected_count=rejected
    )


def vehicle_delivery(
    vehicles: Iterable[VehiclePosition],
    *,
    query_time_ms: int,
    sequence: int = 1,
    purpose: RequestPurpose = RequestPurpose.STARTUP_FULL,
    request_kind: VehiclesRequestKind = VehiclesRequestKind.DEFAULT,
    since_ms: int | None = None,
    requested_at_ms: int | None = None,
    receipt_ms: int | None = None,
    http_status: int = 200,
    evidence: VehicleCollectionEvidence | None = None,
    diagnostics: tuple[FeedDiagnostic, ...] | list = (),
) -> PollDelivery:
    if requested_at_ms is None:
        requested_at_ms = query_time_ms
    if receipt_ms is None:
        receipt_ms = requested_at_ms + 150
    return PollDelivery(
        feed=PollFeed.VEHICLES,
        sequence=sequence,
        purpose=purpose,
        request_sent_utc_ms=requested_at_ms,
        receipt_utc_ms=receipt_ms,
        elapsed_mono_ms=133,
        http_status=http_status,
        model=vehicles_snapshot(
            vehicles,
            query_time_ms=query_time_ms,
            request_kind=request_kind,
            since_ms=since_ms,
            requested_at_ms=requested_at_ms,
        ),
        diagnostics=tuple(diagnostics),
        evidence=evidence,
    )


def incremental_delivery(
    vehicles: Iterable[VehiclePosition],
    *,
    query_time_ms: int,
    since_ms: int,
    sequence: int = 1,
    receipt_ms: int | None = None,
    evidence: VehicleCollectionEvidence | None = None,
) -> PollDelivery:
    return vehicle_delivery(
        vehicles,
        query_time_ms=query_time_ms,
        sequence=sequence,
        purpose=RequestPurpose.INCREMENTAL,
        request_kind=VehiclesRequestKind.SINCE,
        since_ms=since_ms,
        receipt_ms=receipt_ms,
        evidence=evidence,
    )


# ---------------------------------------------------------------------------
# TripUpdates + alerts models and deliveries
# ---------------------------------------------------------------------------


def make_stop_time_event(
    delay_seconds: int | None = None,
    time_raw_seconds: int | None = None,
) -> StopTimeEvent:
    return StopTimeEvent(
        delay_seconds=delay_seconds,
        time_raw_seconds=time_raw_seconds,
        time_utc_ms=None if time_raw_seconds is None else time_raw_seconds * 1000,
        uncertainty_seconds=None,
    )


def make_stop_time_update(
    stop_sequence: int | None = None,
    stop_id: str | None = None,
    arrival: StopTimeEvent | None = None,
    departure: StopTimeEvent | None = None,
    schedule_relationship_raw: int | None = None,
) -> StopTimeUpdate:
    return StopTimeUpdate(
        stop_sequence=stop_sequence,
        stop_id=stop_id,
        arrival=arrival,
        departure=departure,
        schedule_relationship_raw=schedule_relationship_raw,
    )


def make_trip_update_entity(
    entity_id: str,
    trip_id: str,
    *,
    route_id: str | None = None,
    stop_updates: tuple | list = (),
    vehicle_id: str | None = None,
    is_deleted: bool = False,
    schedule_relationship_raw: int | None = None,
) -> TripUpdateEntity:
    descriptor = TripDescriptor(
        trip_id=trip_id,
        route_id=route_id,
        direction_id=None,
        start_time=None,
        start_date=None,
        schedule_relationship_raw=schedule_relationship_raw,
    )
    body = TripUpdate(
        trip=descriptor,
        stop_time_updates=tuple(stop_updates),
        vehicle_id=vehicle_id,
        delay_seconds=None,
        timestamp_raw_seconds=BASE_MS // 1000,
        timestamp_utc_ms=BASE_MS,
    )
    return TripUpdateEntity(
        entity_id=entity_id, trip_update=body, is_deleted=is_deleted
    )


def tripupdates_snapshot(
    entities: Iterable[TripUpdateEntity],
    *,
    header_timestamp_ms: int,
    incrementality_raw: int = 0,
    timestamp_pair_override: tuple[int, int] | None = None,
) -> TripUpdateFeed:
    raw_seconds, utc_ms = (
        timestamp_pair_override
        if timestamp_pair_override is not None
        # The header timestamp is in whole seconds (S8/B8): quantize to the
        # exact seconds→ms pair the S9 parser would have produced.
        else (header_timestamp_ms // 1000, (header_timestamp_ms // 1000) * 1000)
    )
    header = TripUpdateFeedHeader(
        gtfs_realtime_version="2.0",
        incrementality_raw=incrementality_raw,
        timestamp_raw_seconds=raw_seconds,
        timestamp_utc_ms=utc_ms,
    )
    return TripUpdateFeed(header=header, entities=tuple(entities))


def tripupdates_delivery(
    entities: Iterable[TripUpdateEntity],
    *,
    header_timestamp_ms: int,
    sequence: int = 1,
    receipt_ms: int | None = None,
    http_status: int = 200,
    incrementality_raw: int = 0,
    timestamp_pair_override: tuple[int, int] | None = None,
) -> PollDelivery:
    if receipt_ms is None:
        receipt_ms = header_timestamp_ms + 150
    return PollDelivery(
        feed=PollFeed.TRIPUPDATES,
        sequence=sequence,
        purpose=RequestPurpose.INCREMENTAL,
        request_sent_utc_ms=header_timestamp_ms,
        receipt_utc_ms=receipt_ms,
        elapsed_mono_ms=120,
        http_status=http_status,
        model=tripupdates_snapshot(
            entities,
            header_timestamp_ms=header_timestamp_ms,
            incrementality_raw=incrementality_raw,
            timestamp_pair_override=timestamp_pair_override,
        ),
        diagnostics=(),
        evidence=None,
    )


def make_alert(
    alert_id: int,
    *,
    system_wide_flag: bool | None = None,
    routes: Iterable[AlertRouteChild] = (),
    locations: Iterable[AlertLocation] = (),
) -> ServiceAlert:
    return ServiceAlert(
        alert_id=alert_id,
        begin_utc_ms=BASE_MS,
        end_utc_ms=None,
        system_wide_flag=system_wide_flag,
        header_text="maintenance",
        description="bounded synthetic text",
        info_link_url=None,
        routes=tuple(routes),
        locations=tuple(locations),
    )


def route_child(route_id: int) -> AlertRouteChild:
    return AlertRouteChild(route_id=route_id, route_type="R", no_service_flag=False)


def alerts_snapshot(
    alerts: Iterable[ServiceAlert],
    *,
    query_time_ms: int,
) -> AlertsSnapshot:
    return AlertsSnapshot(query_time_utc_ms=query_time_ms, alerts=tuple(alerts))


def alerts_delivery(
    alerts: Iterable[ServiceAlert],
    *,
    query_time_ms: int,
    sequence: int = 1,
    receipt_ms: int | None = None,
    http_status: int = 200,
) -> PollDelivery:
    if receipt_ms is None:
        receipt_ms = query_time_ms + 150
    return PollDelivery(
        feed=PollFeed.ALERTS,
        sequence=sequence,
        purpose=RequestPurpose.INCREMENTAL,
        request_sent_utc_ms=query_time_ms,
        receipt_utc_ms=receipt_ms,
        elapsed_mono_ms=110,
        http_status=http_status,
        model=alerts_snapshot(alerts, query_time_ms=query_time_ms),
        diagnostics=(),
        evidence=None,
    )
