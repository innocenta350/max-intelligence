"""S10 offline integration tests over a real 127.0.0.1 loopback server.

Everything here obeys the plan §11 constraints: injected client factory with
an exact loopback origin, the socket guard installed for the duration, fake
sentinel credentials only, and no contact with developer.trimet.org (a
deliberate attempt is proven to be refused before DNS/network access).
"""
import gzip
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from tests._s10_helpers import (
    FAKE_APP_ID,
    QUERY_TIME,
    FakeClock,
    SinkRecorder,
    vehicle_record,
    vehicles_payload,
)

from backend.trimet.feed_http import build_production_client
from backend.trimet.live_config import AppIDSecret
from backend.trimet.polling import PauseResolution, PollingRuntime
from backend.trimet.poll_types import FailureCode, GateReason, PollFeed, RequestPurpose
from backend.trimet.quota import QuotaGovernor

from tests._s10_helpers import alerts_payload

V = PollFeed.VEHICLES
T = PollFeed.TRIPUPDATES
A = PollFeed.ALERTS

PROD_PATH_V = "/ws/v2/vehicles"
PROD_PATH_T = "/ws/V1/TripUpdate/"
PROD_PATH_A = "/ws/v2/alerts"


def _pb_header_only(timestamp_s: int = QUERY_TIME // 1000) -> bytes:
    def varint(v):
        out = b""
        while True:
            b = v & 0x7F
            v >>= 7
            out += bytes([b | (0x80 if v else 0)])
            if not v:
                return out

    def ld(num, data):
        return varint((num << 3) | 2) + varint(len(data)) + data

    header = ld(1, b"2.0")
    header += varint((2 << 3) | 0) + b"\x00"      # incrementality FULL_DATASET
    header += varint((3 << 3) | 0) + varint(timestamp_s)
    return ld(1, header)


@pytest.fixture(autouse=True)
def _s10_socket_guard(monkeypatch):
    from tests._s10_helpers import install_socket_guard

    install_socket_guard(monkeypatch)


class LoopbackTriMet:
    """A ThreadingHTTPServer bound to 127.0.0.1:0 with swappable routes."""

    def __init__(self) -> None:
        self.routes: dict[str, tuple[int, bytes, dict[str, str]]] = {}
        self.delay_s = 0.0
        self.requests: list[tuple[str, str, str]] = []  # (method, path, appID)
        self.lock = threading.Lock()
        server = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self.server = server
        self.base_url = f"http://127.0.0.1:{server.server_address[1]}"
        self.thread = threading.Thread(target=server.serve_forever, daemon=True)

    def start(self) -> "LoopbackTriMet":
        self.thread.start()
        return self

    def on(self, path: str, status: int, body: bytes, headers: dict | None = None):
        self.routes[path] = (status, body, headers or {"content-type": "application/json"})
        return self

    def _make_handler(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                split = urlsplit(self.path)
                params = parse_qs(split.query)
                app_id = (params.get("appID") or [None])[0]
                with outer.lock:
                    outer.requests.append((self.command, split.path, app_id or ""))
                route = outer.routes.get(split.path)
                if route is None:
                    self.send_response(404)
                    self.send_header("content-length", "0")
                    self.end_headers()
                    return
                if outer.delay_s:
                    time.sleep(outer.delay_s)
                status, body, headers = route
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # silence stderr per-request noise
                pass

        return Handler

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


def harness(server, *, budget=10_000, initial_used=0, config_overrides=None):
    from tests._s10_helpers import make_config

    config = make_config(**(config_overrides or {}))
    clk = FakeClock()
    governor = QuotaGovernor(budget, initial_used, __import__("zoneinfo").ZoneInfo("UTC"), 0, 0, clk)
    recorders = {feed: SinkRecorder() for feed in PollFeed}
    runtime = PollingRuntime(
        config,
        governor=governor,
        sinks=recorders,
        clock=clk,
        transport_factory=lambda feed: httpx.Client(
            base_url=server.base_url,
            timeout=httpx.Timeout(5.0),
            follow_redirects=False,
            trust_env=False,
        ),
    )
    return runtime, clk, recorders


def invoke_cycle(runtime, feed):
    clk = runtime._clock
    boundary = runtime._boundary_for(feed)
    try:
        runtime._feed_cycle(feed, boundary, runtime._sinks[feed], threading.Event(), due=clk.mono())
    finally:
        boundary.close()


class TestRealLoopbackDelivery:
    def test_three_feeds_deliver_over_real_http(self):
        server = (
            LoopbackTriMet()
            .on(PROD_PATH_V, 200, json.dumps(vehicles_payload(QUERY_TIME, [vehicle_record(1), vehicle_record(2)])).encode())
            .on(PROD_PATH_T, 200, _pb_header_only(), {"content-type": "application/x-protobuf"})
            .on(PROD_PATH_A, 200, json.dumps(alerts_payload(QUERY_TIME, [])).encode())
            .start()
        )
        try:
            runtime, clk, recorders = harness(server)
            for feed in PollFeed:
                invoke_cycle(runtime, feed)
            assert recorders[V].count() == 1
            assert recorders[T].count() == 1
            assert recorders[A].count() == 1
            delivery = recorders[V].last()
            assert delivery.evidence.raw_count == 2
            assert delivery.evidence.rejected_count == 0
            assert delivery.purpose is RequestPurpose.STARTUP_FULL
            snap = runtime.snapshot().quota
            assert snap.deliveries_accepted == 3
            assert snap.responses_received == 3
        finally:
            server.close()

    def test_gzip_and_incremental_watermark_over_real_http(self):
        body = gzip.compress(json.dumps(vehicles_payload(QUERY_TIME, [vehicle_record(1)])).encode())
        server = (
            LoopbackTriMet()
            .on(PROD_PATH_V, 200, body, {"content-type": "application/json", "content-encoding": "gzip"})
            .start()
        )
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)   # startup full, gzip decoded
            assert recorders[V].count() == 1
            assert runtime.snapshot().feeds[0].watermark_since_ms == QUERY_TIME
            server.on(
                PROD_PATH_V,
                200,
                json.dumps(vehicles_payload(QUERY_TIME + 5_000, [vehicle_record(1)])).encode(),
            )
            invoke_cycle(runtime, V)   # incremental SINCE=watermark
            assert recorders[V].count() == 2
            request_paths = server.requests
            assert request_paths[-1][0] == "GET"
            assert recorders[V].last().purpose is RequestPurpose.INCREMENTAL
        finally:
            server.close()


class TestOriginGuardAndDeliberateExternalAttempt:
    def test_deliberate_developer_trimet_request_refused_before_dns(self):
        # No injected factory: attempt the production origin directly.
        # The socket guard must refuse it before any DNS or TCP work, and
        # nothing about the production origin may be contacted.
        client = build_production_client(V, __import__("tests._s10_helpers", fromlist=["make_config"]).make_config())
        try:
            with pytest.raises(AssertionError) as excinfo:
                client.get(PROD_PATH_V, params={"appID": FAKE_APP_ID})
            assert "blocked" in str(excinfo.value)
        finally:
            client.close()

    def test_runtime_factory_cannot_reach_other_origins(self):
        # Even a test-transplanted factory pinning loopback cannot be
        # redirected: production origins/paths remain constant.
        server = LoopbackTriMet().on(PROD_PATH_V, 200, json.dumps(vehicles_payload(QUERY_TIME, [])).encode()).start()
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)
            assert recorders[V].count() == 1
            # the transport saw exactly the production path with a sentinel
            _, path, app_id = server.requests[-1]
            assert path == PROD_PATH_V
            assert app_id == FAKE_APP_ID
        finally:
            server.close()


class TestCredentialSafetyUnderDebugLogging:
    def test_sentinel_absent_from_debug_logs_and_telemetry(self, caplog):
        import json as _json

        caplog.set_level(logging.DEBUG)  # root DEBUG per plan §10
        server = (
            LoopbackTriMet()
            .on(PROD_PATH_V, 200, _json.dumps(vehicles_payload(QUERY_TIME, [vehicle_record(1)])).encode())
            .on(PROD_PATH_T, 200, _pb_header_only(), {"content-type": "application/x-protobuf"})
            .on(PROD_PATH_A, 200, _json.dumps(alerts_payload(QUERY_TIME, [])).encode())
            .start()
        )
        try:
            # production logging setup path (idempotent §10 guard)
            build_production_client(V, __import__("tests._s10_helpers", fromlist=["make_config"]).make_config()).close()
            runtime, clk, recorders = harness(server)
            for feed in PollFeed:
                invoke_cycle(runtime, feed)
            logs = caplog.text
            assert FAKE_APP_ID not in logs
            assert "appID" not in logs
            assert FAKE_APP_ID not in repr(runtime.snapshot())
        finally:
            server.close()


class TestRateLimitAndOverrideOverRealHttp:
    def test_unrepresentable_hold_then_operator_override_60s(self):
        server = LoopbackTriMet()  # everything 429 with an unrepresentable Retry-After
        server.on(PROD_PATH_V, 429, b"{}", {"retry-after": "9" * 400})
        server.start()
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)
            assert recorders[V].count() == 0
            snap = runtime.snapshot()
            assert snap.quota.unrepresentable_scopes == ("shared",)
            # operator override -> 60 s guard cooldown, runtime stopped state
            runtime.resolve_operator_pause(
                "shared",
                GateReason.RETRY_AFTER_UNREPRESENTABLE,
                PauseResolution.OVERRIDE_UNREPRESENTABLE_RETRY_AFTER,
            )
            quota = runtime.snapshot().quota
            guard = quota.shared_cooldown_until_mono - clk.mono()
            assert 59.0 <= guard <= 61.0
        finally:
            server.close()

    def test_429_90s_shared_pause_blocks_all_feeds(self):
        server = LoopbackTriMet().on(PROD_PATH_V, 429, b"{}", {"retry-after": "90"})
        server.start()
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)
            snap = runtime.snapshot()
            assert snap.quota.shared_cooldown_until_mono == pytest.approx(
                clk.mono() + 90.0
            )
            invoke_cycle(runtime, T)
            assert runtime.snapshot().feeds[1].gate_reason is GateReason.RATE_LIMITED
            assert recorders[T].count() == 0
        finally:
            server.close()

    @pytest.mark.parametrize("status,scope", [(429, ("shared",)), (503, ("vehicles",))])
    def test_finite_deadline_overflowing_server_wait_becomes_hold(self, status, scope):
        # A finite float (1e306 s) the parser accepts, but whose duration
        # overflows the runtime's checked ms-deadline conversion.  The whole
        # runtime path must classify it into the operator-only hold — no
        # exception, no worker failure, readable telemetry, correct scope
        # (review, follow-up finding 2).
        value = "1" + "0" * 306
        server = LoopbackTriMet().on(PROD_PATH_V, status, b"{}", {"retry-after": value})
        server.start()
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)  # must not raise into the worker
            assert recorders[V].count() == 0  # no automatic retry either way
            snap = runtime.snapshot()
            assert snap.quota.unrepresentable_scopes == scope
            assert (
                snap.feeds[0].gate_reason is GateReason.RETRY_AFTER_UNREPRESENTABLE
            )
            assert snap.feeds[0].failure_code is not FailureCode.WORKER_FAILED
        finally:
            server.close()

    def test_finite_deadline_overflowing_hold_operator_resolution(self):
        value = "1" + "0" * 306
        server = LoopbackTriMet().on(PROD_PATH_V, 429, b"{}", {"retry-after": value})
        server.start()
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)
            assert runtime.snapshot().quota.unrepresentable_scopes == ("shared",)
            # Operator resolution on the stopped runtime, with the existing
            # 60-second guard cooldown afterwards (plan §9, §8).
            runtime.resolve_operator_pause(
                "shared",
                GateReason.RETRY_AFTER_UNREPRESENTABLE,
                PauseResolution.OVERRIDE_UNREPRESENTABLE_RETRY_AFTER,
            )
            quota = runtime.snapshot().quota
            guard = quota.shared_cooldown_until_mono - clk.mono()
            assert 59.0 <= guard <= 61.0
            assert quota.unrepresentable_scopes == ()
        finally:
            server.close()


class TestAuthReplacementAcrossRestart:
    def test_pause_survives_ordinary_restart(self):
        server = (
            LoopbackTriMet()
            .on(PROD_PATH_V, 401, b"{}", {"content-type": "application/json"})
            .on(PROD_PATH_T, 200, _pb_header_only(), {"content-type": "application/x-protobuf"})
            .on(PROD_PATH_A, 200, json.dumps(alerts_payload(QUERY_TIME, [])).encode())
            .start()
        )
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)
            assert runtime.snapshot().quota.auth_blocked_feeds == ("vehicles",)
            # pause gates survive ordinary restart/reset (plan §9)
            runtime.start()
            time.sleep(0.05)
            runtime.stop(timeout_seconds=5)
            assert runtime.snapshot().quota.auth_blocked_feeds == ("vehicles",)
        finally:
            server.close()

    def test_full_flow_stopped_resolution_rebuilds_credential(self):
        server = (
            LoopbackTriMet()
            .on(PROD_PATH_V, 401, b"{}", {"content-type": "application/json"})
            .on(PROD_PATH_T, 200, _pb_header_only(), {"content-type": "application/x-protobuf"})
            .on(PROD_PATH_A, 200, json.dumps(alerts_payload(QUERY_TIME, [])).encode())
            .start()
        )
        try:
            runtime, clk, recorders = harness(server)
            invoke_cycle(runtime, V)
            assert runtime.snapshot().quota.auth_blocked_feeds == ("vehicles",)
            from tests._s10_helpers import make_config

            replacement = make_config(app_id=AppIDSecret(FAKE_APP_ID + "-rotated"))
            runtime.resolve_operator_pause(
                V,
                GateReason.AUTH_BLOCKED,
                PauseResolution.CREDENTIALS_CORRECTED,
                replacement_config=replacement,
            )
            assert runtime.snapshot().quota.auth_blocked_feeds == ()
            assert FAKE_APP_ID + "-rotated" not in repr(runtime.snapshot())
            # only the vehicles feed needed a new credential; the runtime is
            # deliberately stopped here; delivering again is covered by the
            # scheduling tests (restart rebuilds credential clients only on
            # explicit start).
        finally:
            server.close()


class TestStopDuringInFlightRequest:
    def test_stop_times_out_with_survivor_then_join_completes(self):
        server = (
            LoopbackTriMet()
            .on(PROD_PATH_V, 200, json.dumps(vehicles_payload(QUERY_TIME, [vehicle_record(1)])).encode())
            .on(PROD_PATH_T, 200, _pb_header_only(), {"content-type": "application/x-protobuf"})
            .on(PROD_PATH_A, 200, json.dumps(alerts_payload(QUERY_TIME, [])).encode())
            .start()
        )
        try:
            runtime, clk, recorders = harness(server)
            runtime.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and recorders[V].count() == 0:
                time.sleep(0.02)
            assert recorders[V].count() == 1
            # slow the server so some worker's next attempt hangs in flight
            server.delay_s = 5.0
            survivors_result = {}
            stopper = threading.Thread(
                target=lambda: survivors_result.setdefault(
                    "survivors", runtime.stop(timeout_seconds=0.5)
                )
            )
            stopper.start()
            stopper.join(10)
            survivors = survivors_result["survivors"]
            # vehicles may still be inside its (slow) request at timeout
            assert isinstance(survivors, tuple)
            # whatever survived, release and fully join to STOPPED
            joined = runtime.stop(timeout_seconds=10)
            assert joined == ()
            assert runtime.snapshot().state.name == "STOPPED"
            runtime.start()
            runtime.stop(timeout_seconds=5)
        finally:
            server.close()


class TestStartupOrdering:
    def test_immediate_first_cycle_and_no_double_slots(self):
        server = (
            LoopbackTriMet()
            .on(PROD_PATH_V, 200, json.dumps(vehicles_payload(QUERY_TIME, [vehicle_record(1)])).encode())
            .on(PROD_PATH_T, 200, _pb_header_only(), {"content-type": "application/x-protobuf"})
            .on(PROD_PATH_A, 200, json.dumps(alerts_payload(QUERY_TIME, [])).encode())
            .start()
        )
        try:
            runtime, clk, recorders = harness(server)
            runtime.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and sum(
                r.count() for r in recorders.values()
            ) < 3:
                time.sleep(0.02)
            runtime.stop(timeout_seconds=5)
            # one immediate cycle per feed; nothing replayed
            assert recorders[V].count() == 1
            assert recorders[T].count() == 1
            assert recorders[A].count() == 1
            assert runtime.snapshot().state.name == "STOPPED"
        finally:
            server.close()
