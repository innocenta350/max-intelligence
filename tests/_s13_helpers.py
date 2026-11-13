"""Shared S13 freshness/expiry test helpers (offline; never contacts a host).

Mirrors ``tests/_s12_helpers.py`` conventions:

- :func:`install_socket_guard` + the autouse fixture ``s13_deny_all_sockets``
  — a **deny-all** guard (no loopback exception: S13 modules make no socket
  call at all) covering ``getaddrinfo``, ``connect``, ``connect_ex`` and
  ``create_connection`` before any external contact.  Collected S13 modules
  import the fixture so it is declared module-local and function-scoped.
- :class:`FakeFreshnessClock` — a deterministic integer-millisecond clock
  with explicit failure modes (provider exception, non-``TimeSample``
  return) so every controller/clock test is sleep-free and exact.
- :func:`default_policy` / :func:`policy_with` — validated policy builders.
- :func:`patch_expire` / :func:`patch_sample` — deterministic injection
  points around the store's expiry publication and the controller's clock
  sampling for event/barrier race tests (no sleeps).
- :func:`seed_vehicles` — one-call store seeding across all relevance
  classes with per-vehicle ``expires_utc_ms`` control.

Every helper is synthetic: no real machine GTFS, no ``.env``, no config
loading, no HTTP, no polling runtime.  Temporary artifacts belong in pytest
``tmp_path`` directories.
"""
from __future__ import annotations

import socket
import threading
from typing import Any, Callable, Iterable

import pytest

from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    complete_evidence,
    open_state,
    s12_context,
    s12_world,
    vehicle_delivery,
    vehicle_position,
)
from backend.trimet.freshness_types import FreshnessPolicy, TimeSample
from backend.trimet.live_state import LiveState
from backend.trimet.match_types import MatchingContext
from backend.trimet.state_types import VehicleEntry, LiveStateSnapshot

__all__ = [
    "BASE_MS",
    "FakeFreshnessClock",
    "SHA_A",
    "default_policy",
    "install_socket_guard",
    "patch_expire",
    "patch_sample",
    "policy_with",
    "s13_deny_all_sockets",
    "seed_vehicles",
]


# ---------------------------------------------------------------------------
# Deny-all socket guard (row 17: no network, no DNS, no external contact)
# ---------------------------------------------------------------------------


def install_socket_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise before *any* DNS resolution or connection attempt.

    Unlike the S10/S11 loopback-tolerant convention, the S13 guard denies
    everything — the freshness evaluator, controller and store never make
    a socket call, so no loopback exception is ever needed.  A deliberate
    non-loopback attempt in a test proves the guard blocks before any
    packet can leave the process.
    """

    def denied(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "S13 socket guard: network and DNS access are denied in offline "
            "freshness tests"
        )

    monkeypatch.setattr(socket, "getaddrinfo", denied, raising=True)
    monkeypatch.setattr(socket, "create_connection", denied, raising=True)
    monkeypatch.setattr(socket.socket, "connect", denied, raising=True)
    monkeypatch.setattr(socket.socket, "connect_ex", denied, raising=True)


@pytest.fixture(autouse=True)
def s13_deny_all_sockets(monkeypatch: pytest.MonkeyPatch):
    """Module-local autouse guard; import into every collected S13 module."""
    install_socket_guard(monkeypatch)
    yield


# ---------------------------------------------------------------------------
# Policy builders
# ---------------------------------------------------------------------------


def default_policy() -> FreshnessPolicy:
    """The owner-default policy: 30/180/360 s polls, 5000 ms tolerances.

    Expressed in integer milliseconds exactly as the frozen dataclass
    defaults define them; explicit here so tests read as documentation.
    """
    return FreshnessPolicy(
        vehicles_poll_ms=10000,
        tripupdates_poll_ms=60000,
        alerts_poll_ms=120000,
        source_future_tolerance_ms=5000,
        clock_divergence_tolerance_ms=5000,
    )


def policy_with(**overrides: int) -> FreshnessPolicy:
    """A policy with explicit field overrides (validated on construction)."""
    return FreshnessPolicy(**overrides)


# ---------------------------------------------------------------------------
# Deterministic fake clock (integer ms; explicit failure modes)
# ---------------------------------------------------------------------------


class FakeFreshnessClock:
    """A controller clock with scriptable integer samples and failures.

    ``utc_ms``/``monotonic_ms`` are advanced explicitly by tests — no
    sleeps, no real time.  ``fail_with`` scripts the next ``sample()``:
    ``"raise"`` simulates a provider exception, ``"wrong_type"`` returns a
    non-``TimeSample`` object (the bounded ``invalid_clock_sample`` path),
    ``None`` restores normal behavior.
    """

    def __init__(
        self,
        *,
        utc_ms: int = BASE_MS,
        monotonic_ms: int = 1_000_000,
    ) -> None:
        self.utc_ms = utc_ms
        self.monotonic_ms = monotonic_ms
        self.fail_with: str | None = None
        self.calls = 0

    def sample(self) -> TimeSample:
        self.calls += 1
        if self.fail_with == "raise":
            raise RuntimeError("synthetic provider failure")
        if self.fail_with == "wrong_type":
            return (self.utc_ms, self.monotonic_ms)  # type: ignore[return-value]
        return TimeSample(utc_ms=self.utc_ms, monotonic_ms=self.monotonic_ms)

    def advance(
        self,
        *,
        utc_ms: int = 0,
        monotonic_ms: int = 0,
    ) -> None:
        """Move both (or diverge them) for the next sample."""
        self.utc_ms += utc_ms
        self.monotonic_ms += monotonic_ms


# ---------------------------------------------------------------------------
# Deterministic injection points for races (events/barriers; never sleeps)
# ---------------------------------------------------------------------------


def patch_expire(
    monkeypatch: pytest.MonkeyPatch,
    hook: Callable[[LiveState, int], None],
) -> None:
    """Wrap ``LiveState.expire_vehicles`` with a deterministic hook.

    The hook runs *inside* the store's capture window (before the root is
    read) so another thread can interleave a publication — the same
    convention as the S12 ``patch_match`` race helper.
    """
    import backend.trimet.live_state as live_state_module

    real_expire = live_state_module.LiveState.expire_vehicles

    def wrapper(self: LiveState, cutoff_utc_ms: int):
        hook(self, cutoff_utc_ms)
        return real_expire(self, cutoff_utc_ms)

    monkeypatch.setattr(
        live_state_module.LiveState, "expire_vehicles", wrapper, raising=True
    )


def patch_sample(
    controller: Any,
    hook: Callable[[FakeFreshnessClock], None],
) -> None:
    """Wrap a controller's clock ``sample`` with a deterministic hook.

    The hook runs before the real sample is taken, letting a test
    interleave recovery/refresh ordering effects between the two calls.
    """
    clock = controller._clock
    real_sample = clock.sample

    def wrapper():
        hook(clock)
        return real_sample()

    clock.sample = wrapper  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# Store seeding across relevance classes
# ---------------------------------------------------------------------------


def seed_vehicles(
    state: LiveState,
    session: Any,
    specs: Iterable[tuple[int, int, int]],
    *,
    query_time_ms: int = BASE_MS,
    sequence: int = 1,
    evidence: Any = None,
):
    """Accept one vehicle delivery with ``vehicle_id, time_ms, expires_ms``.

    Returns the store snapshot after publication so callers can assert on
    retained entries, the MAX projection and expiry state in one step.
    """
    vehicles = [
        vehicle_position(vehicle_id, time_ms=time_ms, expires_utc_ms=expires_ms)
        for vehicle_id, time_ms, expires_ms in specs
    ]
    session.accept(
        vehicle_delivery(
            vehicles,
            query_time_ms=query_time_ms,
            sequence=sequence,
            evidence=evidence,
        )
    )
    return state.snapshot()
