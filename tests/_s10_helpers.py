"""Shared S10 test helpers (offline; never contacts developer.trimet.org).

Provides:

- :class:`FakeClock` — injected wall/monotonic pair for deterministic reset,
  cooldown and divergence testing.
- :func:`constant_jitter` — bounded deterministic retry jitter.
- :class:`FakeRoutes` — route-table based ``httpx.MockTransport`` transport
  and client factories with request capture (paths, params).
- JSON and protobuf fixture builders for all three feeds (S8/S9-compatible).
- :func:`install_socket_guard` — autouse-able guard that raises on any
  non-loopback DNS resolution or connect attempt, before any network access.
- ``FAKE_APP_ID`` sentinel credentials and test :class:`LiveConfig` factory.
- :class:`SinkRecorder` — per-feed sink callable recorder with failure mode.

Every HTTP test uses an injected transport/client factory; nothing here
performs real network I/O except the integration module's explicit 127.0.0.1
loopback server, which the socket guard permits.
"""
from __future__ import annotations

import gzip
import json
import socket
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx
import pytest

from backend.trimet.live_config import AppIDSecret, LiveConfig
from backend.trimet.poll_types import PollDelivery, PollFeed

FAKE_APP_ID = "fake-appid-sentinel"

QUERY_TIME = 1_772_000_000_000  # plausible UTC ms (March 2026)
ROUTER_TS_S = QUERY_TIME // 1000

SZ = "UTC"
LA = "America/Los_Angeles"


class FakeClock:
    """Injected clock pair under explicit test control (never auto-advances)."""

    def __init__(
        self,
        start_wall: datetime | None = None,
        start_mono: float = 1000.0,
    ) -> None:
        self._wall = start_wall or datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
        self._mono = start_mono
        self._lock = threading.Lock()

    def mono(self) -> float:
        with self._lock:
            return self._mono

    def wall_utc(self) -> datetime:
        with self._lock:
            return self._wall

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._mono += seconds
            self._wall = self._wall + timedelta(seconds=seconds)

    def jump_wall_offset(self, seconds: float) -> None:
        """Simulate a wall-clock discontinuity (mono unchanged)."""
        with self._lock:
            self._wall = self._wall + timedelta(seconds=seconds)


def constant_jitter(seconds: float) -> Callable[[], float]:
    def jitter() -> float:
        return seconds

    return jitter


def make_config(**overrides: Any) -> LiveConfig:
    """A validated test LiveConfig with the fake sentinel credential.

    Poll intervals stay at long defaults so runtime tests control cadence
    through explicit clock advances and single invoked cycles instead of
    wall-time waiting.
    """
    base: dict[str, Any] = {
        "app_id": AppIDSecret(FAKE_APP_ID),
        "quota_initial_used": 0,
        "vehicles_poll_seconds": 10,
        "tripupdates_poll_seconds": 60,
        "alerts_poll_seconds": 120,
        "vehicles_full_seconds": 300,
        "quota_daily_budget": 1_000_000,
        "quota_reset_hour": 0,
        "quota_reset_minute": 0,
        "quota_reset_zone_name": SZ,
        "max_attempts": 2,
        "backoff_min_s": 1.0,
        "backoff_max_s": 2.0,
        "attempt_deadline_s": 10.0,
        "connect_timeout_s": 3.0,
        "read_timeout_s": 5.0,
    }
    base.update(overrides)
    return LiveConfig(**base)


# -- JSON feed fixtures ----------------------------------------------------


def vehicle_record(vid: int, ts_ms: int = QUERY_TIME, minutes_ago: int = 0) -> dict:
    t = QUERY_TIME - minutes_ago * 60_000
    return {
        "vehicleID": vid,
        "type": "rail",
        "routeNumber": 90,
        "time": t,
        "expires": t + 60_000,
        "latitude": 45.52,
        "longitude": -122.63,
    }


def vehicles_payload(query_time_ms: int, vehicles: list | None) -> dict:
    """``vehicles=None`` omits the key; ``[]`` is an explicit empty list."""
    result_set: dict[str, Any] = {"queryTime": query_time_ms}
    if vehicles is not None:
        result_set["vehicle"] = vehicles
    return {"resultSet": result_set}


def alerts_payload(query_time_ms: int, alerts: list | None = None) -> dict:
    result_set: dict[str, Any] = {"queryTime": query_time_ms}
    if alerts is not None:
        result_set["alert"] = alerts
    return {"resultSet": result_set}


# -- protobuf wire fixtures (TripUpdates) -----------------------------------


def _varint(value: int) -> bytes:
    out = b""
    while True:
        b = value & 0x7F
        value >>= 7
        out += bytes([b | (0x80 if value else 0)])
        if not value:
            return out


def _ld(field_num: int, data: bytes) -> bytes:
    return _varint((field_num << 3) | 2) + _varint(len(data)) + data


def header_bytes(
    version: bytes = b"2.0",
    incrementality: int = 0,
    timestamp_s: int = ROUTER_TS_S,
) -> bytes:
    out = _ld(1, version)  # gtfs_realtime_version
    out += _varint((2 << 3) | 0) + _varint(incrementality)
    out += _varint((3 << 3) | 0) + _varint(timestamp_s)
    return out


def tripupdates_bytes(
    incrementality: int = 0, timestamp_s: int = ROUTER_TS_S
) -> bytes:
    """A header-only (zero entity) FULL_DATASET or incrementing feed."""
    return _ld(1, header_bytes(incrementality=incrementality, timestamp_s=timestamp_s))


def malformed_tripupdates() -> bytes:
    """Truncated mid-length-prefix message."""
    return _ld(1, b"")


# -- route-table transport ---------------------------------------------------


def json_response(obj: Any, status: int = 200, headers: dict | None = None) -> httpx.Response:
    merged = {"content-type": "application/json"}
    if headers:
        merged.update(headers)
    return httpx.Response(status, headers=merged, content=json.dumps(obj).encode())


def pb_response(data: bytes) -> httpx.Response:
    return httpx.Response(
        200, headers={"content-type": "application/x-protobuf"}, content=data
    )


class FakeRoutes:
    """Route table: exact path → responder; unmatched paths fail the test."""

    def __init__(self) -> None:
        self.routes: dict[str, httpx.Response] = {}
        self.requests: list[str] = []
        self.params: list[dict[str, str]] = []
        self.lock = threading.Lock()
        self.gate: threading.Event | None = None  # hold *all* responses

    def on(self, path: str, response: httpx.Response) -> "FakeRoutes":
        self.routes[path] = response
        return self

    def handler(self, request: httpx.Request) -> httpx.Response:
        with self.lock:
            self.requests.append(f"{request.method} {request.url.path}")
            self.params.append({k: v for k, v in request.url.params.items()})
        if self.gate is not None:
            self.gate.wait(5.0)
        responder = self.routes.get(request.url.path)
        assert responder is not None, f"unexpected route {request.url!s}"
        return responder

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def count(self, path: str | None = None) -> int:
        with self.lock:
            if path is None:
                return len(self.requests)
            return sum(1 for r in self.requests if r.endswith(path))

    def last_params(self, path: str | None = None) -> dict[str, str]:
        with self.lock:
            if path is None:
                return dict(self.params[-1])
            for req, p in reversed(list(zip(self.requests, self.params))):
                if req.endswith(path):
                    return dict(p)
            raise AssertionError(f"no captured request for {path}")


def make_client(
    routes: FakeRoutes,
    base_url: str = "https://developer.trimet.org",
    **client_kwargs: Any,
) -> httpx.Client:
    """Loopback-only test client pinned to the route table transport."""
    defaults: dict[str, Any] = {
        "follow_redirects": False,
        "trust_env": False,
    }
    defaults.update(client_kwargs)
    return httpx.Client(
        transport=routes.transport(),
        base_url=base_url,
        **defaults,
    )


# -- sink recorder -----------------------------------------------------------


class SinkRecorder:
    """Per-feed sink recorder with injectable failure behavior."""

    def __init__(self, *, fail_times: int = 0, block_event: threading.Event | None = None):
        self.deliveries: list[PollDelivery] = []
        self.lock = threading.Lock()
        self.fail_remaining = fail_times
        self.block_event = block_event
        self.calls = 0

    def __call__(self, delivery: PollDelivery) -> None:
        with self.lock:
            self.calls += 1
            fail = self.fail_remaining > 0
            if fail:
                self.fail_remaining -= 1
        if fail:
            raise RuntimeError("sink failure")
        if self.block_event is not None:
            self.block_event.wait(5.0)
        with self.lock:
            self.deliveries.append(delivery)

    def count(self) -> int:
        with self.lock:
            return len(self.deliveries)

    def last(self) -> PollDelivery:
        with self.lock:
            return self.deliveries[-1]


# -- governor/runtime factory ------------------------------------------------


def make_test_runtime(
    routes: FakeRoutes,
    *,
    config: LiveConfig | None = None,
    clock: FakeClock | None = None,
    jitter: Callable[[], float] | None = None,
    budget: int | None = None,
    initial_used: int | None = None,
    sinks: dict | None = None,
    lane_zone: str | None = None,
    transport_factory: Callable[[PollFeed], httpx.Client] | None = None,
    stop_join_timeout_s: float = 10.0,
):
    """Build a governor + runtime wired to the route table with recorders."""
    from types import SimpleNamespace

    from backend.trimet.polling import PollingRuntime
    from backend.trimet.quota import QuotaGovernor
    from zoneinfo import ZoneInfo

    cfg = config or make_config()
    clk = clock or FakeClock()
    zone = ZoneInfo(lane_zone or cfg.quota_reset_zone_name)
    governor = QuotaGovernor(
        budget if budget is not None else cfg.quota_daily_budget,
        initial_used if initial_used is not None else cfg.quota_initial_used,
        zone,
        cfg.quota_reset_hour,
        cfg.quota_reset_minute,
        clk,
    )
    recorders = {feed: SinkRecorder() for feed in PollFeed}
    factory = transport_factory or (lambda feed: make_client(routes))
    runtime = PollingRuntime(
        cfg,
        governor=governor,
        sinks=sinks or recorders,
        clock=clk,
        jitter=jitter,
        transport_factory=factory,
        stop_join_timeout_s=stop_join_timeout_s,
    )
    return SimpleNamespace(
        runtime=runtime,
        clock=clk,
        governor=governor,
        routes=routes,
        recorders=recorders,
        config=cfg,
    )


# -- socket guard (defense in depth) ----------------------------------------

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def install_socket_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise before DNS/network access for any non-loopback destination.

    Imported-by-test-module autouse fixtures call this; it demonstrably
    rejects a deliberate developer.trimet.org request without contacting it.
    """
    real_getaddrinfo = socket.getaddrinfo
    real_connect = socket.socket.connect

    def guarded_getaddrinfo(host, *args, **kwargs):
        if isinstance(host, str) and host not in _LOOPBACK_HOSTS:
            raise AssertionError(
                f"S10 socket guard: blocked non-loopback resolution for {host!r}"
            )
        return real_getaddrinfo(host, *args, **kwargs)

    def guarded_connect(sock, address):
        host = address[0] if isinstance(address, tuple) else None
        if isinstance(host, str) and host not in _LOOPBACK_HOSTS:
            raise AssertionError(
                f"S10 socket guard: blocked non-loopback connect to {host!r}"
            )
        return real_connect(sock, address)

    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo, raising=True)
    monkeypatch.setattr(socket.socket, "connect", guarded_connect, raising=True)


@dataclass(frozen=True, slots=True)
class Gzipped:
    """Marker for a gzip-encoded response body."""

    raw: bytes


def gzipped_json(obj: Any) -> tuple[bytes, dict[str, str]]:
    body = gzip.compress(json.dumps(obj).encode())
    return body, {"content-type": "application/json", "content-encoding": "gzip"}
