"""S10 one-shot HTTP boundary tests (plan §8)."""
import gzip
import json
import threading
import time
from datetime import datetime, timezone

import httpx
import pytest

from tests._s10_helpers import (
    FAKE_APP_ID,
    QUERY_TIME,
    FakeRoutes,
    FakeClock,
    alerts_payload,
    gzipped_json,
    install_socket_guard,
    json_response,
    make_client,
    make_config,
    malformed_tripupdates,
    pb_response,
    tripupdates_bytes,
    vehicle_record,
    vehicles_payload,
)

from backend.trimet.feed_http import (
    AuthRejected,
    BoundaryFailure,
    Delivered,
    FeedRejected,
    FeedHttpBoundary,
    HttpStatusFailure,
    RateLimited,
    ResponseMeta,
    TransportFailed,
    build_production_client,
    capture_vehicle_evidence,
    parse_retry_after,
)
from backend.trimet.feed_types import VehiclesRequestKind, VehiclesRequestProvenance
from backend.trimet.poll_errors import ProgrammerStateError
from backend.trimet.poll_types import (
    PollFeed,
    RequestPurpose,
    SystemClock,
    VehicleCollectionEvidence,
)

V = PollFeed.VEHICLES
T = PollFeed.TRIPUPDATES
A = PollFeed.ALERTS
P = "/ws/v2/vehicles"
Q = "/ws/V1/TripUpdate/"
R = "/ws/v2/alerts"


@pytest.fixture(autouse=True)
def _s10_socket_guard(monkeypatch):
    install_socket_guard(monkeypatch)


def default_provenance(kind=VehiclesRequestKind.DEFAULT, since=None):
    return VehiclesRequestProvenance(
        request_kind=kind,
        since_ms=since,
        requested_at_utc_ms=QUERY_TIME,
    )


def attempt(feed, routes, *, purpose=RequestPurpose.STARTUP_FULL, provenance=None,
            clock=None, config=None, client_kwargs=None):
    clock = clock or SystemClock()
    config = config or make_config()
    boundary = FeedHttpBoundary(
        feed,
        config,
        clock,
        client_factory=lambda: make_client(routes, **(client_kwargs or {})),
    )
    try:
        return boundary.execute_attempt(purpose=purpose, provenance=provenance)
    finally:
        boundary.close()


def attempt_vehicles(routes, **kw):
    kwargs = {"purpose": kw.pop("purpose", RequestPurpose.STARTUP_FULL)}
    kwargs["provenance"] = kw.pop(
        "provenance", default_provenance(VehiclesRequestKind.DEFAULT)
    )
    return attempt(V, routes, **kw, **kwargs)


class TestRequestConstruction:
    def test_three_paths_and_casing(self):
        routes = (
            FakeRoutes()
            .on(P, json_response(vehicles_payload(QUERY_TIME, [])))
            .on(Q, pb_response(tripupdates_bytes()))
            .on(R, json_response(alerts_payload(QUERY_TIME)))
        )
        attempt_vehicles(routes)
        attempt(T, routes)
        attempt(A, routes)
        assert routes.requests == [
            f"GET {P}", f"GET {Q}", f"GET {R}",
        ]

    def test_credential_travels_only_as_param(self):
        routes = FakeRoutes().on(P, json_response(vehicles_payload(QUERY_TIME, [])))
        attempt_vehicles(routes)
        params = routes.last_params(P)
        assert params["appID"] == FAKE_APP_ID

    def test_since_param_for_incremental_vehicles(self):
        routes = FakeRoutes().on(P, json_response(vehicles_payload(QUERY_TIME, [])))
        since = QUERY_TIME - 10_000
        attempt(
            V,
            routes,
            purpose=RequestPurpose.INCREMENTAL,
            provenance=default_provenance(VehiclesRequestKind.SINCE, since),
        )
        params = routes.last_params(P)
        assert params["since"] == str(since)

    def test_vehicles_attempt_requires_provenance(self):
        routes = FakeRoutes().on(P, json_response(vehicles_payload(QUERY_TIME, [])))
        with pytest.raises(ProgrammerStateError):
            attempt(V, routes, provenance=None)

    def test_since_kind_rejects_null_watermark(self):
        routes = FakeRoutes().on(P, json_response(vehicles_payload(QUERY_TIME, [])))
        with pytest.raises(ValueError):
            attempt(
                V,
                routes,
                provenance=default_provenance(VehiclesRequestKind.SINCE, None),
            )

    def test_production_client_hardening(self):
        config = make_config()
        client = build_production_client(V, config)
        try:
            assert str(client.base_url) == "https://developer.trimet.org"
            assert client.follow_redirects is False
            assert client.trust_env is False
            assert client.headers["Accept-Encoding"] == "gzip, identity"
            assert client.timeout.connect == config.connect_timeout_s
            assert client.timeout.read == config.read_timeout_s
            assert client.timeout.write == config.write_timeout_s
            assert client.timeout.pool == config.pool_timeout_s
        finally:
            client.close()


class TestValidDeliveries:
    def test_vehicles_delivered_with_exact_evidence(self):
        routes = FakeRoutes().on(
            P, json_response(vehicles_payload(QUERY_TIME, [vehicle_record(1), vehicle_record(2)]))
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, Delivered)
        assert out.meta.status == 200
        assert out.result.model.query_time_utc_ms == QUERY_TIME
        assert len(out.result.model.vehicles) == 2
        ev = out.evidence
        assert ev.key_present and ev.is_list
        assert ev.raw_count == 2 and ev.rejected_count == 0

    def test_explicit_empty_array_is_valid(self):
        routes = FakeRoutes().on(P, json_response(vehicles_payload(QUERY_TIME, [])))
        out = attempt_vehicles(routes)
        assert isinstance(out, Delivered)
        assert out.evidence.raw_count == 0 and out.evidence.rejected_count == 0
        assert len(out.result.model.vehicles) == 0

    def test_missing_array_is_valid_empty_with_unknown_counts(self):
        routes = FakeRoutes().on(
            P, json_response(vehicles_payload(QUERY_TIME, None))
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, Delivered)
        ev = out.evidence
        assert not ev.key_present and not ev.is_list
        assert ev.raw_count is None and ev.rejected_count is None

    def test_explicit_null_array_has_unknown_counts(self):
        routes = FakeRoutes().on(
            P,
            json_response(
                {"resultSet": {"queryTime": QUERY_TIME, "vehicle": None}},
            ),
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, Delivered)
        # REV 3 §6: an explicit null differs from a missing key by
        # key_present=True (review F6).
        assert out.evidence == VehicleCollectionEvidence(
            True, False, None, None
        )

    def test_record_rejection_preserves_diagnostics_and_exact_count(self):
        routes = FakeRoutes().on(
            P,
            json_response(
                vehicles_payload(
                    QUERY_TIME,
                    [vehicle_record(1), {"vehicleID": "not-an-int"}],
                ),
            ),
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, Delivered)
        ev = out.evidence
        assert ev.raw_count == 2 and ev.rejected_count == 1
        assert len(out.result.diagnostics) == 1
        assert not out.result.model.vehicles[0].vehicle_id == "not-an-int"

    def test_tripupdates_delivered(self):
        routes = FakeRoutes().on(Q, pb_response(tripupdates_bytes()))
        out = attempt(T, routes)
        assert isinstance(out, Delivered)
        assert out.evidence is None
        assert out.result.model is not None

    def test_alerts_delivered(self):
        routes = FakeRoutes().on(R, json_response(alerts_payload(QUERY_TIME, [])))
        out = attempt(A, routes)
        assert isinstance(out, Delivered)
        assert out.evidence is None

    def test_gzip_delivery_decoded(self):
        body, headers = gzipped_json(vehicles_payload(QUERY_TIME, [vehicle_record(1)]))
        routes = FakeRoutes().on(
            P, httpx.Response(200, headers=headers, content=body)
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, Delivered)
        assert len(out.result.model.vehicles) == 1


class TestRejections:
    def test_malformed_json(self):
        routes = FakeRoutes().on(
            P, httpx.Response(200, headers={"content-type": "application/json"}, content=b"{nope")
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, BoundaryFailure)
        assert out.kind == "invalid_json"

    def test_malformed_protobuf(self):
        routes = FakeRoutes().on(Q, pb_response(malformed_tripupdates()))
        out = attempt(T, routes)
        assert isinstance(out, FeedRejected)
        assert out.meta.status == 200

    def test_non_full_dataset_rejected(self):
        routes = FakeRoutes().on(Q, pb_response(tripupdates_bytes(incrementality=1)))
        out = attempt(T, routes)
        assert isinstance(out, FeedRejected)

    def test_html_content_type_rejected(self):
        routes = FakeRoutes().on(
            P, httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>")
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, BoundaryFailure)
        assert out.kind == "invalid_content_type"

    def test_json_media_type_required_for_alerts(self):
        routes = FakeRoutes().on(
            R, httpx.Response(200, headers={"content-type": "text/plain"}, content=b"hi")
        )
        out = attempt(A, routes)
        assert isinstance(out, BoundaryFailure)
        assert out.kind == "invalid_content_type"

    def test_protobuf_aliases_accepted(self):
        for media in ("application/grtfeed", "application/protobuf", "application/octet-stream"):
            routes = FakeRoutes().on(
                Q, httpx.Response(200, headers={"content-type": media},
                                  content=tripupdates_bytes())
            )
            out = attempt(T, routes)
            assert isinstance(out, Delivered), media

    def test_unsupported_content_encoding_rejected(self):
        routes = FakeRoutes().on(
            P,
            httpx.Response(
                200,
                headers={
                    "content-type": "application/json",
                    "content-encoding": "br",
                },
                content=b"\x00\x01",
            ),
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, BoundaryFailure)
        assert out.kind == "unsupported_content_encoding"

    def test_content_length_above_cap_rejected_before_read(self):
        routes = FakeRoutes().on(
            P,
            httpx.Response(
                200,
                headers={"content-type": "application/json", "content-length": "999999999"},
                content=b"{}",
            ),
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, BoundaryFailure)
        assert out.kind == "response_too_large"

    def test_lying_content_length_cannot_hide_decode_overflow(self):
        big = json.dumps(vehicles_payload(QUERY_TIME, [vehicle_record(i) for i in range(200)])).encode()
        routes = FakeRoutes().on(
            P,
            httpx.Response(
                200,
                headers={"content-type": "application/json", "content-length": "10"},
                content=b"x" * (2 * 1024 * 1024 + 1),
            ),
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, BoundaryFailure)
        assert out.kind == "response_too_large"
        assert len(big) > 0  # fixture sanity

    def test_gzip_decoded_overflow_rejected(self):
        inner = json.dumps(vehicles_payload(QUERY_TIME, [vehicle_record(i) for i in range(400)])).encode()
        big_inner = inner + b" " * (3 * 1024 * 1024)
        body = __import__("gzip").compress(big_inner)
        routes = FakeRoutes().on(
            P,
            httpx.Response(
                200,
                headers={
                    "content-type": "application/json",
                    "content-encoding": "gzip",
                    "content-length": str(len(body)),
                },
                content=body,
            ),
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, BoundaryFailure)
        assert out.kind == "response_too_large"

    def test_processed_status_204_not_sniffed_as_empty_feed(self):
        routes = FakeRoutes().on(P, httpx.Response(204))
        out = attempt_vehicles(routes)
        assert isinstance(out, HttpStatusFailure)
        assert out.retryable is False

    def test_redirects_are_not_followed(self):
        routes = FakeRoutes().on(
            P, httpx.Response(301, headers={"location": "http://127.0.0.1:2/other"})
        )
        out = attempt_vehicles(routes)
        assert isinstance(out, HttpStatusFailure)
        assert out.meta.status == 301
        assert out.retryable is False

    def test_other_4xx_non_retryable(self):
        routes = FakeRoutes().on(P, httpx.Response(404))
        out = attempt_vehicles(routes)
        assert isinstance(out, HttpStatusFailure)
        assert out.retryable is False


class TestVehicleEvidenceFacts:
    """Full evidence records for every resultSet.vehicle shape (review F6)."""

    def test_missing_key_and_missing_resultset(self):
        assert capture_vehicle_evidence(
            {"resultSet": {"queryTime": QUERY_TIME}}
        ) == VehicleCollectionEvidence(False, False, None, None)
        assert capture_vehicle_evidence({}) == VehicleCollectionEvidence(
            False, False, None, None
        )
        assert capture_vehicle_evidence("not a dict") == VehicleCollectionEvidence(
            False, False, None, None
        )

    def test_explicit_null_is_present_but_unknown(self):
        assert capture_vehicle_evidence(
            {"resultSet": {"vehicle": None}}
        ) == VehicleCollectionEvidence(True, False, None, None)

    def test_list_values_carry_exact_lengths(self):
        empty = capture_vehicle_evidence({"resultSet": {"vehicle": []}})
        assert empty == VehicleCollectionEvidence(True, True, 0, None)
        two = capture_vehicle_evidence({"resultSet": {"vehicle": [1, 2]}})
        assert two == VehicleCollectionEvidence(True, True, 2, None)

    def test_non_list_container_is_recorded_as_fact(self):
        odd = capture_vehicle_evidence({"resultSet": {"vehicle": {"a": 1}}})
        assert odd == VehicleCollectionEvidence(True, False, None, None)
        stringy = capture_vehicle_evidence({"resultSet": {"vehicle": "x"}})
        assert stringy == VehicleCollectionEvidence(True, False, None, None)


class TestRateLimited:
    def _with_retry_after(self, status, raw):
        headers = {"content-type": "application/json"}
        if raw is not None:
            headers["retry-after"] = raw
        routes = FakeRoutes().on(P, httpx.Response(status, headers=headers, content=b"{}"))
        return attempt_vehicles(routes)

    def test_429_positive_delay_shared(self):
        out = self._with_retry_after(429, "90")
        assert isinstance(out, RateLimited)
        assert out.scope_shared is True
        assert out.parsed_delay_s == 90.0
        assert out.unrepresentable is False

    def test_503_positive_delay_feed_scoped(self):
        out = self._with_retry_after(503, "5")
        assert isinstance(out, RateLimited)
        assert out.scope_shared is False
        assert out.parsed_delay_s == 5.0

    def test_503_missing_retry_after_is_normal_retry(self):
        out = self._with_retry_after(503, None)
        assert isinstance(out, RateLimited)
        assert out.scope_shared is False
        assert out.parsed_delay_s is None
        assert out.unrepresentable is False

    def test_invalid_retry_after_yields_no_parsed_delay(self):
        for raw in ("-30", "+90", "90 s", "", " ゲーム".encode("utf-8")):
            out = self._with_retry_after(429, raw)
            assert out.parsed_delay_s is None, raw
            assert out.unrepresentable is False

    def test_zero_and_past_dates_contribute_zero(self):
        out = self._with_retry_after(429, "0")
        assert out.parsed_delay_s == 0.0
        out = self._with_retry_after(503, "Wed, 21 Oct 2020 07:28:00 GMT")
        assert out.parsed_delay_s == 0.0

    def test_multiday_waits_are_representable(self):
        out = self._with_retry_after(429, str(60 * 60 * 24 * 5))
        assert out.parsed_delay_s == 60.0 * 60 * 24 * 5
        assert out.unrepresentable is False

    def test_unrepresentable_429_shared(self):
        out = self._with_retry_after(429, "9" * 400)  # beyond float range
        assert isinstance(out, RateLimited)
        assert out.unrepresentable is True
        assert out.scope_shared is True

    def test_unrepresentable_503_feed_scoped(self):
        out = self._with_retry_after(503, "1" + "0" * 400)  # beyond float range
        assert out.unrepresentable is True
        assert out.scope_shared is False


class TestRetryAfterParser:
    @pytest.mark.parametrize(
        "raw,want",
        [
            (None, (None, False)),
            ("", (None, False)),
            ("120", (120.0, False)),
            ("0", (0.0, False)),
            ("10000001", (10000001.0, False)),  # finite waits stay exact
            ("9" * 400, (None, True)),          # beyond float → operator hold
            ("1" + "0" * 306, (10.0 ** 306, False)),  # finite float; the
            # runtime classifies its own deadline conversion for this range
            ("9" * 4301, (None, True)),   # interpreter digit safeguard →
            # operator hold, never a malformed-header short-retry fallback
            ("garbage!!", (None, False)),
            ("Wed, 21 Oct 2020 07:28:00 GMT", (0.0, False)),
        ],
    )
    def test_cases(self, raw, want):
        assert parse_retry_after(
            raw, datetime(2026, 11, 7, tzinfo=timezone.utc)
        ) == want

    def test_long_representable_wait_is_preserved(self):
        # Review F7: no numeric policy cutoff — 10,000,001 seconds is a
        # finite, safely representable server minimum and must not become
        # an operator hold.
        result = parse_retry_after(
            str(10_000_001), datetime(2026, 11, 7, tzinfo=timezone.utc)
        )
        assert result == (10000001.0, False)

    def test_future_http_date_remaining(self):
        delay, unrep = parse_retry_after(
            "Fri, 01 Jan 2027 00:00:00 GMT",
            datetime(2026, 11, 7, tzinfo=timezone.utc),
        )
        assert unrep is False
        assert 0 < delay < 60 * 60 * 24 * 60


class TestTransportFailures:
    def test_connection_refused_to_loopback_is_transport_error(self):
        transport = httpx.MockTransport(
            lambda request: (_ for _ in ()).throw(httpx.ConnectError("boom"))
        )
        boundary = FeedHttpBoundary(
            V,
            make_config(),
            SystemClock(),
            client_factory=lambda: httpx.Client(
                transport=transport,
                base_url="http://127.0.0.1:1",
                follow_redirects=False,
                trust_env=False,
            ),
        )
        try:
            out = boundary.execute_attempt(
                purpose=RequestPurpose.STARTUP_FULL,
                provenance=default_provenance(),
            )
        finally:
            boundary.close()
        assert isinstance(out, TransportFailed)
        assert out.timeout is False

    def test_read_timeout_flagged_timeout(self):
        # MockTransport bypasses httpx's network timeouts, so use a real
        # stalled loopback socket (allowed by the socket guard).
        import socket

        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]

        def serve():
            conn, _ = server.accept()
            time.sleep(0.5)  # accept, then never respond
            conn.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()

        boundary = FeedHttpBoundary(
            T,
            make_config(),
            SystemClock(),
            client_factory=lambda: httpx.Client(
                base_url=f"http://127.0.0.1:{port}",
                timeout=httpx.Timeout(connect=3, read=0.1, write=3, pool=1),
                follow_redirects=False,
                trust_env=False,
            ),
        )
        try:
            out = boundary.execute_attempt(purpose=RequestPurpose.INCREMENTAL)
        finally:
            boundary.close()
            server.close()
            thread.join(2)
        assert isinstance(out, TransportFailed)
        assert out.timeout is True


class TestAttemptDeadline:
    def test_cooperative_deadline_checked_between_chunks(self):
        clock = FakeClock()

        class SlowChunks:
            headers = {"content-type": "application/json"}
            status_code = 200

            def iter_bytes(self, chunk_size=65536):
                yield b"x"
                clock.advance(99.0)
                yield b"y"

        config = make_config()
        boundary = FeedHttpBoundary(V, config, clock,
                                    client_factory=lambda: httpx.MockTransport(None))
        meta = ResponseMeta(
            status=200, content_type="application/json", content_length=2,
            content_encoding=None, retry_after_raw=None, date_utc_ms=None,
        )
        out = boundary._read_payload_decoded(SlowChunks(), meta, compressed=False)
        assert out.kind == "transport_timeout"


class TestCredentialSafety:
    @pytest.mark.parametrize("path", [P, Q, R])
    def test_outcomes_never_carry_url_or_credential(self, path):
        feed = {P: V, Q: T, R: A}[path]
        routes = FakeRoutes().on(path, httpx.Response(500, content=b"secret leak"))
        if feed is V:
            out = attempt_vehicles(routes)
        else:
            out = attempt(feed, routes)
        text = repr(out) + type(out).__name__
        assert FAKE_APP_ID not in text
        assert "appID" not in text
        assert "developer.trimet.org" not in text
