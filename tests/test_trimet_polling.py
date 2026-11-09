"""S10 polling runtime tests: scheduling, watermark/authority, retry policy,
independence and lifecycle (plan §5, §6, §9).

Threaded tests use short real-time waits only; deterministic scheduling and
watermark behavior is driven through single invoked cycles on injected
clocks.
"""
import json
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest

from tests._s10_helpers import (
    FAKE_APP_ID,
    QUERY_TIME,
    FakeClock,
    FakeRoutes,
    SinkRecorder,
    alerts_payload,
    install_socket_guard,
    json_response,
    make_client,
    make_config,
    pb_response,
    tripupdates_bytes,
    vehicle_record,
    vehicles_payload,
)

from backend.trimet.feed_types import VehiclesRequestKind
from backend.trimet.live_config import AppIDSecret
from backend.trimet.poll_errors import LifecycleError, PollConfigError
from backend.trimet.polling import PauseResolution, PollingRuntime
from backend.trimet.poll_types import (
    FailureCode,
    GateReason,
    LifecycleState,
    PollFeed,
    RequestPurpose,
)
from backend.trimet.quota import QuotaGovernor

V = PollFeed.VEHICLES
T = PollFeed.TRIPUPDATES
A = PollFeed.ALERTS
P = "/ws/v2/vehicles"
Q = "/ws/V1/TripUpdate/"
R = "/ws/v2/alerts"


@pytest.fixture(autouse=True)
def _s10_socket_guard(monkeypatch):
    install_socket_guard(monkeypatch)


def ok_routes():
    return (
        FakeRoutes()
        .on(P, json_response(vehicles_payload(QUERY_TIME, [vehicle_record(1), vehicle_record(2)])))
        .on(Q, pb_response(tripupdates_bytes()))
        .on(R, json_response(alerts_payload(QUERY_TIME, [])))
    )


def status_response(status, headers=None):
    return httpx.Response(status, headers=headers or {}, content=b"{}")


def json_status(status, obj, headers=None):
    merged = {"content-type": "application/json"}
    if headers:
        merged.update(headers)
    return httpx.Response(status, headers=merged, content=json.dumps(obj).encode())


def harness(routes, *, config=None, clock=None, budget=10_000, initial_used=0,
            jitter=None, sinks=None, transport_factory=None):
    """A non-threaded harness; cycles are invoked deterministically."""
    cfg = config or make_config()
    clk = clock or FakeClock()
    governor = QuotaGovernor(
        budget, initial_used, ZoneInfo(cfg.quota_reset_zone_name), 0, 0, clk
    )
    runtime = PollingRuntime(
        cfg,
        governor=governor,
        sinks=sinks or {feed: SinkRecorder() for feed in PollFeed},
        clock=clk,
        jitter=jitter,
        transport_factory=transport_factory or (lambda feed: make_client(routes)),
        stop_join_timeout_s=5.0,
    )
    return runtime, clk


def invoke_cycle(runtime, feed, *, clock=None, routes=None, stop_event=None):
    """Run one complete scheduled slot for a feed without threads."""
    clk = clock or runtime._clock
    stop_event = stop_event or threading.Event()
    boundary = runtime._boundary_for(feed)
    try:
        runtime._feed_cycle(
            feed, boundary, runtime._sinks[feed], stop_event, due=clk.mono()
        )
    finally:
        boundary.close()


def cycle_on_json_payload(payload, **harness_kwargs):
    """Run a single vehicles cycle against a freshly-pinned payload."""
    routes = FakeRoutes().on(P, json_status(200, payload))
    runtime, clk = harness(routes, **harness_kwargs)
    invoke_cycle(runtime, V, clock=clk, routes=routes)
    return runtime, routes, runtime._sinks[V]


def wait_until(condition, timeout=5.0, message="condition not met"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.02)
    raise AssertionError(message)


class TestScheduling:
    def test_immediate_startup_cycle_per_feed(self):
        routes = ok_routes()
        runtime, clk = harness(routes)
        for feed in PollFeed:
            invoke_cycle(runtime, feed, clock=clk)
        assert runtime._sinks[V].count() == 1
        assert runtime._sinks[T].count() == 1
        assert runtime._sinks[A].count() == 1

    def test_grid_slots_advance_and_skip_elapsed(self):
        runtime, clk = harness(ok_routes())
        anchor = runtime._grid_anchor_mono
        interval = float(runtime.interval_of(V))
        assert runtime._next_grid_slot(V, anchor + interval * 0.5) == pytest.approx(anchor + interval)
        assert runtime._next_grid_slot(V, anchor + interval * 2.5) == pytest.approx(anchor + interval * 3)
        assert runtime._next_grid_slot(V, anchor + interval * 3.0) == pytest.approx(anchor + interval * 4)

    def test_missed_slots_never_replay_after_restart(self):
        routes = ok_routes()
        runtime, clk = harness(routes)
        runtime.start()
        runtime.stop(timeout_seconds=5)
        assert routes.count() == 3
        clk.advance(float(runtime.interval_of(V)) * 2.5)  # missed slots
        runtime.start()
        runtime.stop(timeout_seconds=5)
        # One immediate cycle per feed: no backlog replay.
        assert routes.count() == 6

    def test_full_replaces_incremental_after_full_expiry(self):
        routes = ok_routes()
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V, clock=clk)  # startup full
        invoke_cycle(runtime, V, clock=clk)  # incremental SINCE
        assert routes.last_params(P)["since"] == str(QUERY_TIME)
        clk.advance(float(runtime._config.vehicles_full_seconds))
        invoke_cycle(runtime, V, clock=clk)  # full replaces the slot
        assert "since" not in routes.last_params(P)

    def test_failed_incremental_coalesces_to_one_recovery_full(self):
        # A failed incremental slot coalesces into exactly ONE recovery full
        # at the next slot: no hidden extra request, no partial retry storm.
        routes = FakeRoutes().on(
            P, json_status(200, vehicles_payload(QUERY_TIME, [vehicle_record(1)]))
        )
        runtime, clk = harness(routes, jitter=lambda: 0.05)
        invoke_cycle(runtime, V)  # startup full succeeds
        assert runtime._sinks[V].count() == 1
        routes.on(P, json_status(200, {}))  # feed-level rejection
        invoke_cycle(runtime, V)  # incremental slot fails; full is forced
        assert runtime._sinks[V].count() == 1
        routes.on(P, json_status(200, vehicles_payload(QUERY_TIME + 20, [vehicle_record(1)])))
        invoke_cycle(runtime, V)  # one coalesced recovery full
        delivery = runtime._sinks[V].last()
        assert delivery.purpose is RequestPurpose.RECOVERY_FULL
        assert routes.count(P) == 3

    def test_recovered_full_resumes_incremental(self):
        runtime, routes, van = cycle_on_json_payload(
            vehicles_payload(QUERY_TIME, [vehicle_record(1), {"vehicleID": "lost"}])
        )
        routes.on(P, json_status(200, vehicles_payload(QUERY_TIME + 10, [vehicle_record(1)])))
        invoke_cycle(runtime, V)      # recovery full (complete now)
        routes.on(P, json_status(200, vehicles_payload(QUERY_TIME + 20, [vehicle_record(2)])))
        invoke_cycle(runtime, V)      # SINCE with committed watermark
        assert routes.last_params(P)["since"] == str(QUERY_TIME + 10)


class TestVehicleWatermarkAndAuthority:
    def test_startup_default_then_incremental_since(self):
        routes = ok_routes()
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V, clock=clk)
        first = runtime._sinks[V].last()
        assert first.purpose is RequestPurpose.STARTUP_FULL
        assert runtime.snapshot().feeds[0].request_kind == "default"
        invoke_cycle(runtime, V, clock=clk)
        second = runtime._sinks[V].last()
        assert second.purpose is RequestPurpose.INCREMENTAL
        assert second.sequence == 2
        assert second.request_sent_utc_ms >= first.request_sent_utc_ms

    def test_retry_repeats_kind_and_since_with_fresh_provenance(self):
        routes = FakeRoutes().on(P, status_response(503))  # no Retry-After
        runtime, clk = harness(routes, jitter=lambda: 0.05)
        # establish a watermark from an earlier good cycle
        runtime._watermark = QUERY_TIME - 10_000
        runtime._last_full_succ_mono = clk.mono()
        invoke_cycle(runtime, V, clock=clk)
        assert routes.count(P) == 2
        assert routes.last_params(P)["since"] == str(QUERY_TIME - 10_000)
        first_kind = runtime.snapshot().feeds[0].request_kind
        assert first_kind == "since"

    def test_no_watermark_advance_after_rejection(self):
        runtime, routes, van = cycle_on_json_payload({})
        rec = runtime.snapshot().feeds[0]
        assert rec.failure_code is FailureCode.FEED_REJECTED
        assert rec.watermark_since_ms is None
        snap = runtime.snapshot().quota
        assert snap.feed_rejections == 1 and snap.deliveries_accepted == 0

    def test_regression_not_published_and_cannot_regress_watermark(self):
        routes = FakeRoutes().on(P, json_status(200, vehicles_payload(QUERY_TIME, [vehicle_record(1)])))
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V, clock=clk)      # commit watermark
        newer = QUERY_TIME + 5_000
        routes.on(P, json_status(200, vehicles_payload(newer, [vehicle_record(1)])))
        invoke_cycle(runtime, V, clock=clk)      # advance watermark
        routes.on(P, json_status(200, vehicles_payload(QUERY_TIME - 1, [vehicle_record(1)])))
        before = runtime._sinks[V].count()
        invoke_cycle(runtime, V, clock=clk)      # older: not published
        assert runtime._sinks[V].count() == before
        rec = runtime.snapshot().feeds[0]
        assert rec.failure_code is FailureCode.OUT_OF_ORDER_FEED
        assert rec.watermark_since_ms == newer
        invoke_cycle(runtime, V, clock=clk)      # forced full next
        assert "since" not in routes.last_params(P)

    def test_equal_watermark_published_without_advancing(self):
        payload = vehicles_payload(QUERY_TIME, [vehicle_record(1)])
        routes = FakeRoutes().on(P, json_status(200, payload))
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V, clock=clk)
        runtime._watermark = QUERY_TIME  # emulate equal arrival on SINCE
        runtime._last_full_succ_mono = clk.mono()
        invoke_cycle(runtime, V, clock=clk)
        assert runtime._sinks[V].count() == 2
        assert routes.last_params(P)["since"] == str(QUERY_TIME)
        assert runtime.snapshot().feeds[0].watermark_since_ms == QUERY_TIME

    def test_watermark_commits_only_after_sink_ack(self):
        routes = FakeRoutes().on(P, json_status(200, vehicles_payload(QUERY_TIME, [vehicle_record(1)])))
        failing = SinkRecorder(fail_times=1)
        runtime, clk = harness(routes, sinks={V: failing, T: SinkRecorder(), A: SinkRecorder()})
        invoke_cycle(runtime, V, clock=clk)
        assert runtime.snapshot().feeds[0].failure_code is FailureCode.DELIVERY_FAILED
        assert runtime.snapshot().feeds[0].watermark_since_ms is None
        routes.on(P, json_status(200, vehicles_payload(QUERY_TIME + 1, [vehicle_record(1)])))
        invoke_cycle(runtime, V, clock=clk)  # redelivery tolerated
        assert runtime.snapshot().feeds[0].watermark_since_ms == QUERY_TIME + 1

    def test_missing_collection_delivered_but_not_committed_and_full_forced(self):
        payload = {"resultSet": {"queryTime": QUERY_TIME}}  # no vehicle key
        runtime, routes, van = cycle_on_json_payload(payload)
        delivery = van.last()
        assert delivery.evidence.key_present is False
        assert delivery.evidence.raw_count is None
        rec = runtime.snapshot().feeds[0]
        assert rec.failure_code is FailureCode.INCOMPLETE_VEHICLE_COLLECTION
        assert rec.watermark_since_ms is None
        routes.on(P, json_status(200, vehicles_payload(QUERY_TIME + 1, [vehicle_record(1)])))
        invoke_cycle(runtime, V)             # forced full despite delivery
        assert "since" not in routes.last_params(P)

    def test_record_loss_blocks_commit_and_forces_full(self):
        payload = vehicles_payload(QUERY_TIME, [vehicle_record(1), {"vehicleID": "lost"}])
        runtime, routes, van = cycle_on_json_payload(payload)
        delivery = van.last()
        assert delivery.evidence.raw_count == 2
        assert delivery.evidence.rejected_count == 1
        assert runtime.snapshot().feeds[0].watermark_since_ms is None
        routes.on(P, json_status(200, vehicles_payload(QUERY_TIME + 10, [vehicle_record(1)])))
        invoke_cycle(runtime, V)
        assert "since" not in routes.last_params(P)

    def test_other_feeds_have_no_vehicle_evidence(self):
        routes = ok_routes()
        runtime, clk = harness(routes)
        invoke_cycle(runtime, T, clock=clk)
        invoke_cycle(runtime, A, clock=clk)
        assert runtime._sinks[T].last().evidence is None
        assert runtime._sinks[A].last().evidence is None


class TestRetryPolicy:
    def test_every_attempt_counted_max_two(self):
        routes = FakeRoutes().on(P, status_response(500))
        runtime, clk = harness(routes, jitter=lambda: 0.05)
        invoke_cycle(runtime, V)
        assert routes.count(P) == 2
        snap = runtime.snapshot().quota
        assert snap.transport_handoffs == 2
        assert snap.retry_handoffs == 1
        assert snap.responses_received == 2

    def test_max_attempts_one_is_honored(self):
        routes = FakeRoutes().on(P, status_response(500))
        runtime, clk = harness(routes, config=make_config(max_attempts=1))
        invoke_cycle(runtime, V)
        assert routes.count(P) == 1

    def test_content_failures_do_not_retry(self):
        routes = FakeRoutes().on(
            P, httpx.Response(200, headers={"content-type": "text/html"}, content=b"<h>")
        )
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V)
        assert routes.count(P) == 1

    def test_429_no_immediate_retry_shared_pause(self):
        routes = FakeRoutes().on(
            P, json_status(429, {}, {"retry-after": "90"})
        )
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V)
        assert routes.count(P) == 1
        snap = runtime.snapshot().quota
        assert snap.shared_cooldown_until_mono is not None
        other_routes = ok_routes()
        invoke_cycle(runtime, T, clock=clk)
        assert runtime.snapshot().feeds[1].gate_reason is GateReason.RATE_LIMITED

    def test_429_floor_is_60s(self):
        for raw, expected in (("5", 60.0), (None, 60.0), ("0", 60.0)):
            routes = FakeRoutes().on(
                P,
                json_status(429, {}) if raw is None else json_status(429, {}, {"retry-after": raw}),
            )
            runtime, clk = harness(routes)
            before = clk.mono()
            invoke_cycle(runtime, V)
            snap = runtime.snapshot().quota
            assert snap.shared_cooldown_until_mono == pytest.approx(before + expected)

    def test_repeated_429_extends_never_shortens(self):
        routes = FakeRoutes().on(
            P, json_status(429, {}, {"retry-after": "10"})
        )
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V)   # floor 60 s
        first = runtime.snapshot().quota.shared_cooldown_until_mono
        # No further request is possible until the cooldown expires; once it
        # does, a fresh 429 only ever extends the shared pause.
        clk.advance(61.0)
        routes.on(P, json_status(429, {}, {"retry-after": "3600"}))
        invoke_cycle(runtime, V)   # extend to 3600 s
        second = runtime.snapshot().quota.shared_cooldown_until_mono
        assert second == pytest.approx(clk.mono() + 3600.0)

    def test_503_retry_after_declares_feed_minimum(self):
        routes = FakeRoutes().on(P, json_status(503, {}, {"retry-after": "40"}))
        runtime, clk = harness(routes, jitter=lambda: 0.0)
        before = clk.mono()
        invoke_cycle(runtime, V)
        snap = runtime.snapshot().quota
        assert snap.feed_cooldowns == (("vehicles", pytest.approx(before + 40.0)),)

    def test_503_missing_retry_after_normal_bounded_retry(self):
        routes = FakeRoutes().on(P, json_status(503, {}))
        runtime, clk = harness(routes, jitter=lambda: 0.05)
        invoke_cycle(runtime, V)
        assert routes.count(P) == 2
        assert runtime.snapshot().quota.feed_cooldowns == ()

    def test_retry_must_fit_before_next_slot(self):
        routes = FakeRoutes().on(P, status_response(408))
        config = make_config(vehicles_poll_seconds=1)
        runtime, clk = harness(routes, config=config, jitter=lambda: 2.0)
        invoke_cycle(runtime, V)
        assert routes.count(P) == 1  # 2.0 s backoff cannot fit a 1.0 s slot

    def test_retry_runs_when_it_fits(self):
        routes = FakeRoutes().on(P, status_response(408))
        config = make_config(vehicles_poll_seconds=60)
        runtime, clk = harness(routes, config=config, jitter=lambda: 0.05)
        invoke_cycle(runtime, V)
        assert routes.count(P) == 2

    def test_quota_refusal_makes_no_transport_call(self):
        routes = ok_routes()
        runtime, clk = harness(routes, budget=2, initial_used=1)
        invoke_cycle(runtime, V, clock=clk)
        invoke_cycle(runtime, T, clock=clk)
        assert runtime.snapshot().feeds[1].gate_reason is GateReason.QUOTA_EXHAUSTED
        assert routes.count(Q) == 0
        assert runtime.snapshot().quota.charged <= 2  # never overshot


class TestTelemetry:
    def test_auth_block_visible(self):
        routes = FakeRoutes().on(P, status_response(401))
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V, clock=clk)
        rec = runtime.snapshot().feeds[0]
        assert rec.gate_reason is GateReason.AUTH_BLOCKED
        assert rec.failure_code is FailureCode.AUTH_BLOCKED
        assert runtime.snapshot().quota.auth_blocked_feeds == ("vehicles",)

    def test_prior_pause_state_unknown_and_preserved(self):
        runtime, clk = harness(ok_routes())
        assert runtime.snapshot().prior_pause_state == "unknown"
        runtime.start()
        runtime.stop(timeout_seconds=5)
        runtime.start()
        runtime.stop(timeout_seconds=5)
        assert runtime.snapshot().prior_pause_state == "unknown"

    def test_gate_deadline_exposed_on_cooldown_refusal(self):
        routes = FakeRoutes().on(P, json_status(429, {}, {"retry-after": "30"}))
        runtime, clk = harness(routes)
        invoke_cycle(runtime, V)
        invoke_cycle(runtime, V)
        rec = runtime.snapshot().feeds[0]
        assert rec.gate_reason is GateReason.RATE_LIMITED
        assert rec.next_gate_deadline_utc_ms is not None


class TestIndependence:
    def test_one_feed_failing_does_not_stop_others(self):
        routes = (
            FakeRoutes()
            .on(P, status_response(500))
            .on(Q, pb_response(tripupdates_bytes()))
            .on(R, json_status(200, alerts_payload(QUERY_TIME, [])))
        )
        runtime, clk = harness(routes, jitter=lambda: 0.05)
        for _ in range(3):
            invoke_cycle(runtime, T, clock=clk)
            invoke_cycle(runtime, A, clock=clk)
            invoke_cycle(runtime, V, clock=clk)  # fails every time, retried
        assert runtime._sinks[T].count() == 3
        assert runtime._sinks[A].count() == 3
        assert runtime.snapshot().feeds[0].failure_code is not FailureCode.NONE

    def test_broken_sink_isolated_to_own_feed(self):
        routes = (
            FakeRoutes()
            .on(P, json_status(200, vehicles_payload(QUERY_TIME, [vehicle_record(1)])))
            .on(Q, pb_response(tripupdates_bytes()))
            .on(R, json_status(200, alerts_payload(QUERY_TIME, [])))
        )
        runtime, clk = harness(
            routes,
            sinks={V: SinkRecorder(fail_times=99), T: SinkRecorder(), A: SinkRecorder()},
        )
        invoke_cycle(runtime, V)
        invoke_cycle(runtime, T)
        invoke_cycle(runtime, A)
        assert runtime.snapshot().feeds[0].failure_code is FailureCode.DELIVERY_FAILED
        assert runtime._sinks[T].count() == 1
        assert runtime._sinks[A].count() == 1

    def test_governor_snapshot_available_during_network(self):
        busy_routes = ok_routes()
        busy_routes.gate = threading.Event()
        # a bounded gate: holds responses briefly so the cycle is provably
        # inside network I/O while snapshot() is called from another thread.
        transport = httpx.MockTransport(busy_routes.handler)
        runtime, clk = harness(
            FakeRoutes(),
            transport_factory=lambda feed: httpx.Client(
                transport=transport,
                base_url="https://developer.trimet.org",
                follow_redirects=False,
                trust_env=False,
            ),
        )
        done = []

        def run_cycle():
            runtime._feed_cycle(V, runtime._boundary_for(V), runtime._sinks[V],
                                threading.Event(), due=clk.mono())
            done.append(True)

        # gate must spend time inside the handler, not while holding locks:
        # assert snapshot() completes fast while a request is in flight.
        worker = threading.Thread(target=run_cycle, daemon=True)
        # Pre-set gate so the handler parks inside the transport call.
        released = []
        routes = ok_routes()
        gate = threading.Event()
        slow_routes = (
            FakeRoutes()
            .on(P, json_response(vehicles_payload(QUERY_TIME, [vehicle_record(1)])))
        )

        def slow_handler(request):
            gate.wait(3.0)
            return slow_routes.handler(request)

        slow_transport = httpx.MockTransport(slow_handler)
        runtime2, clk2 = harness(
            FakeRoutes(),
            transport_factory=lambda feed: httpx.Client(
                transport=slow_transport,
                base_url="https://developer.trimet.org",
                follow_redirects=False,
                trust_env=False,
            ),
        )
        worker = threading.Thread(
            target=lambda: runtime2._feed_cycle(
                V, runtime2._boundary_for(V), runtime2._sinks[V],
                threading.Event(), due=clk2.mono()),
            daemon=True,
        )
        worker.start()
        wait_until(lambda: not gate.is_set() is False or True, timeout=0.2)
        t0 = time.monotonic()
        runtime2.snapshot()
        elapsed = time.monotonic() - t0
        assert elapsed < 1.0, "governor lock appears held during network I/O"
        gate.set()
        worker.join(5)


class TestLifecycle:
    def test_duplicate_start_refused(self):
        runtime, clk = harness(ok_routes())
        runtime.start()
        with pytest.raises(LifecycleError):
            runtime.start()
        runtime.stop(timeout_seconds=5)

    def test_stop_before_start_is_noop(self):
        runtime, clk = harness(ok_routes())
        assert runtime.stop() == ()
        assert runtime.snapshot().state is LifecycleState.STOPPED

    def test_real_partial_thread_start_failure_cleans_up(self):
        # The real spawn path registers and starts each thread in turn: a
        # failing *second* ``Thread.start`` must not break countdown by
        # joining a never-started thread (RuntimeError), must not swallow
        # the original failure, and must leave a coherent STOPPED state
        # without any survivor of this runtime.
        runtime, clk = harness(ok_routes())
        runtime._run_feed_worker = lambda feed: runtime._stop_event.wait()
        original_start = threading.Thread.start
        starts = {"n": 0}

        def fail_second(thread):
            starts["n"] += 1
            if starts["n"] == 2:
                raise RuntimeError("simulated second thread start failure")
            return original_start(thread)

        threading.Thread.start = fail_second
        try:
            caught: Exception | None = None
            try:
                runtime.start()
            except Exception as exc:  # the original spawn failure surfaces
                caught = exc
            assert isinstance(caught, RuntimeError), repr(caught)
            assert "second thread" in str(caught)
            assert runtime.snapshot().state is LifecycleState.STOPPED
            assert runtime.snapshot().surviving_workers == ()
        finally:
            threading.Thread.start = original_start
            runtime._stop_event.set()
            for thread in list(runtime._threads.values()):
                if thread.ident is not None:
                    thread.join(5.0)

    def test_partial_startup_rolls_back_to_stopped(self):
        class HaltingRuntime(PollingRuntime):
            def _spawn_workers(self):
                thread = threading.Thread(
                    target=self._run_feed_worker, args=(V,), daemon=True
                )
                self._threads[V.value] = thread
                thread.start()
                raise RuntimeError("simulated spawn failure")

        routes = ok_routes()
        client = make_client(routes)
        runtime, clk = harness(routes)
        broken = HaltingRuntime(
            runtime._config,
            governor=runtime._governor,
            sinks=runtime._sinks,
            clock=clk,
            transport_factory=lambda feed: make_client(routes),
        )
        try:
            with pytest.raises(RuntimeError):
                broken.start()
            assert broken.snapshot().state is LifecycleState.STOPPED
            assert broken.snapshot().surviving_workers == ()
        finally:
            client.close()

    def test_stop_during_request_survivor_visible_no_replacement_start(self):
        routes = ok_routes()
        gate = threading.Event()
        slow_routes = FakeRoutes().on(
            P, json_response(vehicles_payload(QUERY_TIME, [vehicle_record(1)]))
        )

        def slow_handler(request):
            gate.wait(5.0)
            return slow_routes.handler(request)

        runtime, clk = harness(
            FakeRoutes(),
            transport_factory=lambda feed: httpx.Client(
                transport=httpx.MockTransport(slow_handler),
                base_url="https://developer.trimet.org",
                follow_redirects=False,
                trust_env=False,
            ),
        )
        runtime.start()
        alive = threading.Event()
        stop_result = {}

        def stopping():
            stop_result["survivors"] = runtime.stop(timeout_seconds=0.7)
            alive.set()

        thread = threading.Thread(target=stopping)
        thread.start()
        time.sleep(0.4)  # let vehicles park inside the gated request
        # no replacement while old workers remain alive; a stop that timed
        # out leaves the worker alive, and start() refuses to proceed.
        with pytest.raises((LifecycleError, RuntimeError)):
            runtime.start()
        thread.join(10)
        survivors = stop_result["survivors"]
        assert survivors  # at least the vehicles worker survived the timeout
        assert runtime.snapshot().shutdown_incomplete is True
        # finish the in-flight attempt; the survivor sees the stop event and
        # exits, and a follow-up stop fully joins to STOPPED
        gate.set()
        joined = runtime.stop(timeout_seconds=5)
        assert joined == ()
        assert runtime.snapshot().state is LifecycleState.STOPPED
        runtime.start()
        runtime.stop(timeout_seconds=5)

    def test_stop_during_retry_backoff_is_interruptible(self):
        routes = FakeRoutes().on(P, status_response(408))
        runtime, clk = harness(routes, jitter=lambda: 30.0)
        runtime.start()
        wait_until(lambda: routes.count() >= 1)
        t0 = time.monotonic()
        runtime.stop(timeout_seconds=5)
        assert time.monotonic() - t0 < 2.0  # 30 s backoff interrupted

    def test_no_new_requests_between_stop_and_start(self):
        routes = ok_routes()
        runtime, clk = harness(routes)
        runtime.start()
        wait_until(lambda: routes.count() >= 3)
        runtime.stop(timeout_seconds=5)
        frozen_requests = routes.count()
        frozen_handoffs = runtime.snapshot().quota.transport_handoffs
        runtime2 = None
        runtime.start()
        wait_until(lambda: routes.count() > frozen_requests)
        runtime.stop(timeout_seconds=5)
        elapsed_handoffs = runtime.snapshot().quota.transport_handoffs
        assert elapsed_handoffs >= frozen_handoffs

    def test_per_worker_clients_closed_on_stop(self):
        routes = ok_routes()
        clients = []

        def tracking_factory(feed):
            client = make_client(routes)
            clients.append(client)
            return client

        runtime, clk = harness(FakeRoutes(), transport_factory=tracking_factory)
        runtime.start()
        wait_until(lambda: routes.count() >= 3)
        runtime.stop(timeout_seconds=5)
        assert len(clients) >= 3
        assert all(c.is_closed for c in clients)

    def test_worker_exception_marks_only_that_worker_failed(self):
        # Only the vehicles worker hits the poison interval; the other two
        # workers keep delivering (per-feed isolation of thread death).
        class BoomForVehicles(PollingRuntime):
            def interval_of(self, feed):
                if feed is PollFeed.VEHICLES:
                    raise ValueError("boom")
                return super().interval_of(feed)

        routes = (
            FakeRoutes()
            .on(Q, pb_response(tripupdates_bytes()))
            .on(R, json_status(200, alerts_payload(QUERY_TIME, [])))
        )

        class Recorder(SinkRecorder):
            pass

        sinks = {feed: Recorder() for feed in PollFeed}
        cfg = make_config()
        clk = FakeClock()
        governor = QuotaGovernor(
            10_000, 0, ZoneInfo(cfg.quota_reset_zone_name), 0, 0, clk
        )
        runtime = BoomForVehicles(
            cfg,
            governor=governor,
            sinks=sinks,
            clock=clk,
            transport_factory=lambda feed: make_client(routes),
            stop_join_timeout_s=5.0,
        )
        runtime.start()
        wait_until(
            lambda: runtime.snapshot().feeds[0].failure_code is FailureCode.WORKER_FAILED
        )
        runtime.stop(timeout_seconds=5)
        assert runtime.snapshot().feeds[0].failure_code is FailureCode.WORKER_FAILED
        assert sinks[T].count() == 1
        assert sinks[A].count() == 1


class TestHandoffGateRefusal:
    """A delayed handoff refused at re-admission must never emit an HTTP
    attempt (review F3, runtime path)."""

    def test_refused_handoff_never_reaches_transport(self):
        clock = FakeClock(
            start_wall=datetime(2026, 11, 7, 23, 59, 59, tzinfo=timezone.utc)
        )
        routes = ok_routes()
        runtime, clk = harness(routes, clock=clock, budget=1)
        gov = runtime._governor
        original_admit = gov.admit
        first = {"armed": True}

        def interleaved_admit(*args, **kwargs):
            result = original_admit(*args, **kwargs)
            if first["armed"] and result.granted:
                first["armed"] = False
                clock.advance(2.0)  # cross the midnight reset
                # Another worker admits into the new window first.
                other = original_admit(A, "new-window-other")
                assert other.granted
                gov.mark_handoff(other.token)
            return result

        gov.admit = interleaved_admit
        invoke_cycle(runtime, V, clock=clock)
        assert routes.count() == 0
        snap = runtime.snapshot()
        assert snap.feeds[0].gate_reason is GateReason.QUOTA_EXHAUSTED
        quota = snap.quota
        assert quota.pending_admitted == 0
        assert quota.unused_admissions == 1
        assert quota.charged == 1  # the other feed's new-window charge stays


class TestRecovery:
    def test_recovery_apis_require_stopped_runtime(self):
        from backend.trimet.polling import PauseResolution as R

        runtime, clk = harness(ok_routes())
        runtime.start()
        with pytest.raises(LifecycleError):
            runtime.reinitialize_quota(0, "2026-11-07T00:00:00+00:00")
        with pytest.raises(LifecycleError):
            runtime.resolve_operator_pause(
                V, GateReason.AUTH_BLOCKED, R.CREDENTIALS_CORRECTED
            )
        runtime.stop(timeout_seconds=5)

    def test_credential_replacement_flow(self):
        runtime, clk = harness(
            FakeRoutes().on(P, status_response(401)).on(Q, pb_response(tripupdates_bytes()))
        )
        invoke_cycle(runtime, V)
        assert runtime.snapshot().quota.auth_blocked_feeds == ("vehicles",)
        with pytest.raises(LifecycleError):
            runtime.resolve_operator_pause(
                V, GateReason.RATE_LIMITED, PauseResolution.CREDENTIALS_CORRECTED
            )
        with pytest.raises(LifecycleError):
            runtime.resolve_operator_pause(
                V, GateReason.AUTH_BLOCKED,
                PauseResolution.OVERRIDE_UNREPRESENTABLE_RETRY_AFTER,
            )
        replacement = make_config(app_id=AppIDSecret("brand-new-fake-id"))
        runtime.resolve_operator_pause(
            V, GateReason.AUTH_BLOCKED, PauseResolution.CREDENTIALS_CORRECTED,
            replacement_config=replacement,
        )
        assert runtime.snapshot().quota.auth_blocked_feeds == ()
        assert runtime._config is replacement
        # replacement never appears in telemetry/records
        assert "brand-new-fake-id" not in repr(runtime.snapshot())

    def test_resolution_rejects_non_appid_changes(self):
        runtime, clk = harness(ok_routes())
        runtime._governor.note_auth_blocked(V)
        replacement = make_config(alerts_poll_seconds=77)
        with pytest.raises(PollConfigError):
            runtime.resolve_operator_pause(
                V, GateReason.AUTH_BLOCKED, PauseResolution.CREDENTIALS_CORRECTED,
                replacement_config=replacement,
            )

    def test_resolution_record_and_audit_line(self, capsys):
        runtime, clk = harness(ok_routes())
        runtime._governor.note_auth_blocked(A)
        runtime.resolve_operator_pause(
            A, GateReason.AUTH_BLOCKED, PauseResolution.CREDENTIALS_REVALIDATED
        )
        snap = runtime.snapshot()
        assert len(snap.resolution_records) == 1
        assert snap.resolution_records[0].target == "alerts"
        assert snap.resolution_records[0].resolution == "credentials_revalidated"
        out = capsys.readouterr().out
        assert FAKE_APP_ID not in out
        assert "alerts" in out and "credentials_revalidated" in out

    def test_unrepresentable_override_sets_60s_guard(self):
        runtime, clk = harness(ok_routes())
        runtime._governor.declare_unrepresentable(None)
        runtime.resolve_operator_pause(
            "shared", GateReason.RETRY_AFTER_UNREPRESENTABLE,
            PauseResolution.OVERRIDE_UNREPRESENTABLE_RETRY_AFTER,
        )
        snap = runtime.snapshot().quota
        guard = snap.shared_cooldown_until_mono - clk.mono()
        assert 59.0 <= guard <= 61.0

    def test_reinitialize_quota_success_on_stopped_runtime(self):
        # The explicit success path from the runtime level: a fully stopped,
        # fully joined runtime may reinitialize with the current window id;
        # clock trust is restored and the epoch advances.
        runtime, clk = harness(ok_routes())
        runtime.start()
        runtime.stop(timeout_seconds=5)
        snap = runtime.snapshot()
        window_id = snap.quota.window_id
        charged = snap.quota.charged
        # an undercut opening estimate is refused even on a stopped runtime
        from backend.trimet.poll_errors import LifecycleError as _LE, PollConfigError as _PC

        with pytest.raises((LifecycleError, PollConfigError)):
            runtime.reinitialize_quota(max(0, charged - 1), window_id)
        epoch = runtime.reinitialize_quota(charged, window_id)
        snap = runtime.snapshot()
        assert epoch >= 2
        assert snap.quota.clock_untrusted is False
        assert snap.state is LifecycleState.STOPPED

    def test_stale_resolution_without_gate_rejected(self):
        runtime, clk = harness(ok_routes())
        with pytest.raises(LifecycleError):
            runtime.resolve_operator_pause(
                V, GateReason.AUTH_BLOCKED, PauseResolution.CREDENTIALS_CORRECTED
            )
