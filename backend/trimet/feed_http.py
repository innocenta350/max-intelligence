"""One-shot HTTP boundary: HTTPX clients, request construction, bounded
streaming reads and S9 parser dispatch (REV 3 plan §8).

Rules implemented here:

- Production destinations/paths are the three S8-verified services; the app
  credential travels only as query parameters and never enters repr or error
  text (tests inject the destination via ``origin_override`` only).
- Clients: TLS verified, ``follow_redirects=False``, transport retries at
  zero, ``trust_env=False``, explicit phase timeouts, and
  ``Accept-Encoding: gzip, identity``.
- Streaming reads cap **decoded** bytes independently of Content-Length;
  Content-Length itself is only used for a conservative over-large rejection.
- An additional cooperative attempt deadline is checked between decode
  chunks; synchronous phases can still block until their phase timeout.
- Every response context is closed on success and on every failure path
  (exception semantics guaranteed by the ``with`` form).
- One attempt = one admission; retry decisions live in :mod:`polling`.
"""
from __future__ import annotations

import json as _json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable

import httpx

from backend.trimet.poll_errors import (
    ProgrammerStateError,
)
from backend.trimet.poll_types import (
    ClockSource,
    PollFeed,
    RequestPurpose,
    VehicleCollectionEvidence,
)
from backend.trimet import feed_parse
from backend.trimet.feed_errors import FeedRejectedError
from backend.trimet.live_config import LiveConfig

# Reserved for genuine source-representation limits only (plan §8): a value
# whose seconds figure cannot even be converted into a float (int overflow)
# becomes the named operator-only hold.  All remaining representable values
# are honored exactly as the server stated them.
PRODUCTION_ORIGINS: dict[PollFeed, tuple[str, str]] = {
    PollFeed.VEHICLES: ("https://developer.trimet.org", "/ws/v2/vehicles"),
    PollFeed.TRIPUPDATES: ("https://developer.trimet.org", "/ws/V1/TripUpdate/"),
    PollFeed.ALERTS: ("https://developer.trimet.org", "/ws/v2/alerts"),
}

_ACCEPTED_TYPES: dict[PollFeed, frozenset[str]] = {
    PollFeed.VEHICLES: frozenset({"application/json"}),
    PollFeed.ALERTS: frozenset({"application/json"}),
    PollFeed.TRIPUPDATES: frozenset(
        {"application/grtfeed", "application/protobuf",
         "application/x-protobuf", "application/octet-stream"}
    ),
}

_KNOWN_ENCODINGS = frozenset(
    {"gzip", "x-gzip", "deflate", "identity"}
)


@dataclass(frozen=True, slots=True)
class ResponseMeta:
    """Safe subset of HTTP response headers (no bodies, no URLs, no creds)."""

    status: int
    content_type: str | None
    content_length: int | None
    content_encoding: str | None
    retry_after_raw: str | None
    date_utc_ms: int | None


@dataclass(frozen=True, slots=True)
class Delivered:
    kind: str
    meta: ResponseMeta
    result: object              # S9 FeedParseResult
    evidence: VehicleCollectionEvidence | None
    payload_bytes: int


@dataclass(frozen=True, slots=True)
class BoundaryFailure:
    """Terminal content-level failure (content type/encoding/size/JSON)."""

    kind: str                   # FailureCode value
    meta: ResponseMeta


@dataclass(frozen=True, slots=True)
class HttpStatusFailure:
    kind: str
    meta: ResponseMeta
    retryable: bool


@dataclass(frozen=True, slots=True)
class RateLimited:
    kind: str
    meta: ResponseMeta
    scope_shared: bool
    parsed_delay_s: float | None
    unrepresentable: bool


@dataclass(frozen=True, slots=True)
class AuthRejected:
    kind: str
    meta: ResponseMeta


@dataclass(frozen=True, slots=True)
class TransportFailed:
    kind: str
    timeout: bool
    meta: ResponseMeta | None


@dataclass(frozen=True, slots=True)
class FeedRejected:
    kind: str
    meta: ResponseMeta
    reason: str


AttemptOutcome = (
    Delivered | BoundaryFailure | HttpStatusFailure | RateLimited
    | AuthRejected | TransportFailed | FeedRejected
)


def parse_retry_after(raw: str | None, now_utc: datetime) -> tuple[float | None, bool]:
    """Parse a Retry-After header value (plan §8, RFC 9110 §10.2.3).

    Returns ``(delay_seconds, unrepresentable)``:

    - ``(None, False)``: missing or invalid → caller applies its fallback;
    - ``(0.0, False)``: zero or a date in the past contributes no additional
      server delay;
    - ``(None, True)``: a value whose delay genuinely cannot be represented
      as a float at all (e.g. an integer beyond ``float``'s range) →
      operator-only hold, never shortened into an automatic retry.  All
      finite representable delays are preserved exactly as stated.
    """
    if raw is None:
        return None, False
    candidate = raw.strip()
    if not candidate or not candidate.isascii():
        return None, False
    if candidate.isdigit():
        try:
            return float(int(candidate)), False
        except (OverflowError, ValueError):
            # Genuine unsupported representations: beyond the float range
            # (OverflowError) or beyond the interpreter's integer-string
            # conversion safeguard (ValueError) — never disable the
            # safeguard.  Both become the operator-only hold, never a
            # short-retry fallback.
            return None, True
    try:
        when = parsedate_to_datetime(candidate)
    except (TypeError, ValueError):
        return None, False
    if when is None:
        return None, False
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    remaining = (when - now_utc).total_seconds()
    if remaining < 0:
        remaining = 0.0
    return remaining, False


def capture_vehicle_evidence(payload: Any) -> VehicleCollectionEvidence:
    """Observe the ``resultSet.vehicle`` collection before S9 parsing.

    Facts only (REV 3 plan §6).  Absent and explicit-``null`` collections
    carry unknown counts; explicit lists carry exact lengths; a non-list
    non-null container is recorded as a non-list fact (S9 then rejects the
    whole feed, producing no delivery).
    """
    result_set = payload.get("resultSet") if isinstance(payload, dict) else None
    if not isinstance(result_set, dict):
        return VehicleCollectionEvidence(False, False, None, None)
    if "vehicle" not in result_set:
        return VehicleCollectionEvidence(False, False, None, None)
    raw = result_set["vehicle"]
    if not isinstance(raw, list):
        return VehicleCollectionEvidence(True, False, None, None)
    return VehicleCollectionEvidence(True, True, len(raw), None)


def finalize_vehicle_evidence(
    evidence: VehicleCollectionEvidence, delivered_count: int
) -> VehicleCollectionEvidence:
    """Attach the exact rejected count for an explicit list collection.

    A negative result is an explicit runtime ``ProgrammerStateError`` — not
    an optimization-removable ``assert`` (REV 3 plan §6).
    """
    if not evidence.is_list or evidence.raw_count is None:
        return evidence
    rejected = evidence.raw_count - delivered_count
    if rejected < 0:
        raise ProgrammerStateError(
            f"vehicle evidence rejected count must be non-negative (raw="
            f"{evidence.raw_count}, delivered={delivered_count})"
        )
    return VehicleCollectionEvidence(
        evidence.key_present, evidence.is_list, evidence.raw_count, rejected
    )


_logging_guard_applied = False


class _DropTransportRecords(logging.Filter):
    """Plan §10: credentials travel as URL query parameters, and httpx/
    httpcore emit full URLs (including INFO request logs).  Records from
    those loggers are dropped outright and their level is pinned to WARNING;
    the application must never rely on the operator's log level."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name not in ("httpx", "httpcore")


def apply_credential_safe_logging() -> None:
    """Suppress transport-library logging for the production runtime.

    Idempotent.  Retained application warning/error paths carry reason
    codes and numeric metadata only (audited: no URL interpolation).
    """
    global _logging_guard_applied
    if _logging_guard_applied:
        return
    _logging_guard_applied = True
    for name in ("httpx", "httpcore"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.WARNING)
        logger.addFilter(_DropTransportRecords())


def build_production_client(feed: PollFeed, config: LiveConfig) -> httpx.Client:
    """Production client against the fixed S8-verified origin (plan §4/§8)."""
    apply_credential_safe_logging()
    origin, _path = PRODUCTION_ORIGINS[feed]
    timeout = httpx.Timeout(
        connect=config.connect_timeout_s,
        read=config.read_timeout_s,
        write=config.write_timeout_s,
        pool=config.pool_timeout_s,
    )
    return httpx.Client(
        base_url=origin,
        timeout=timeout,
        verify=True,
        follow_redirects=False,
        trust_env=False,
        headers={"Accept-Encoding": "gzip, identity"},
    )


def _media_type(value: str | None) -> str | None:
    if not value:
        return None
    return value.split(";", 1)[0].strip().lower()


def _int_header(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


def _date_header_ms(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        when = parsedate_to_datetime(value.strip())
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return int(when.timestamp() * 1000)


def _meta_from_response(response: httpx.Response) -> ResponseMeta:
    headers = response.headers
    return ResponseMeta(
        status=response.status_code,
        content_type=_media_type(headers.get("content-type")),
        content_length=_int_header(headers.get("content-length")),
        content_encoding=headers.get("content-encoding"),
        retry_after_raw=headers.get("retry-after"),
        date_utc_ms=_date_header_ms(headers.get("date")),
    )


_JSON_FEEDS = frozenset({PollFeed.VEHICLES, PollFeed.ALERTS})


class FeedHttpBoundary:
    """One client + one bounded-read boundary for one feed's worker.

    Admissions, retries and cooldowns are the polling layer's job; this
    boundary performs exactly one transport attempt per call.
    """

    def __init__(
        self,
        feed: PollFeed,
        config: LiveConfig,
        clock: ClockSource,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        origin_override: str | None = None,
    ) -> None:
        self._feed = feed
        self._config = config
        self._clock = clock
        self._origin_override = origin_override
        if client_factory is None:
            def client_factory():
                return build_production_client(feed, config)
        self._client_builder = client_factory
        self._client: httpx.Client | None = None
        self._max_bytes = {
            PollFeed.VEHICLES: config.vehicles_max_bytes,
            PollFeed.TRIPUPDATES: config.tripupdates_max_bytes,
            PollFeed.ALERTS: config.alerts_max_bytes,
        }[feed]

    def _client_ready(self) -> httpx.Client:
        if self._client is None:
            self._client = self._client_builder()
        return self._client

    def close(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def execute_attempt(
        self,
        *,
        purpose: RequestPurpose,
        provenance: Any | None = None,
    ) -> AttemptOutcome:
        feed = self._feed
        params: dict[str, str] = {"appID": self._config.app_id.value}
        if feed is PollFeed.VEHICLES:
            if provenance is None:
                raise ProgrammerStateError(
                    "vehicles attempts require VehiclesRequestProvenance"
                )
            if provenance.request_kind.name == "SINCE":
                if provenance.since_ms is None:
                    raise ProgrammerStateError(
                        "SINCE vehicles attempts require a watermark since_ms"
                    )
                params["since"] = str(provenance.since_ms)
        client = self._client_ready()
        attempt_deadline = self._clock.mono() + self._config.attempt_deadline_s
        try:
            with client.stream(
                "GET", _request_path(feed), params=params
            ) as response:
                meta = _meta_from_response(response)
                if response.status_code in (401, 403):
                    return AuthRejected("auth_blocked", meta)
                if response.status_code == 429:
                    return self._rate_limited_outcome(meta, shared=True)
                if response.status_code == 503:
                    return self._rate_limited_outcome(meta, shared=False)
                if response.status_code in (408, 500, 502, 504):
                    return HttpStatusFailure("http_status_error", meta, retryable=True)
                if response.status_code != 200:
                    return HttpStatusFailure("http_status_error", meta, retryable=False)

                if meta.content_type not in _ACCEPTED_TYPES[feed]:
                    return BoundaryFailure(
                        "invalid_content_type",
                        meta,
                    )
                encoding = meta.content_encoding
                if encoding is not None:
                    enc = encoding.strip().lower()
                    if enc not in _KNOWN_ENCODINGS:
                        return BoundaryFailure(
                            "unsupported_content_encoding", meta
                        )
                    if enc == "identity":
                        encoding = None

                payload_or_failure = self._read_payload_decoded(
                    response, meta, encoding is not None
                )
                if isinstance(payload_or_failure, BoundaryFailure):
                    return payload_or_failure
                payload = payload_or_failure

                return self._parse_payload(payload, meta, provenance)
        except httpx.TimeoutException:
            return TransportFailed("transport_timeout", timeout=True, meta=None)
        except httpx.TransportError:
            return TransportFailed("transport_error", timeout=False, meta=None)

    def _rate_limited_outcome(self, meta: ResponseMeta, *, shared: bool) -> RateLimited:
        delay, unrepresentable = parse_retry_after(
            meta.retry_after_raw, self._clock.wall_utc()
        )
        return RateLimited(
            "rate_limited",
            meta,
            scope_shared=shared,
            parsed_delay_s=None if unrepresentable else delay,
            unrepresentable=unrepresentable,
        )

    def _read_payload_decoded(
        self,
        response: httpx.Response,
        meta: ResponseMeta,
        compressed: bool,
    ) -> "bytes | BoundaryFailure":
        """Bounded streaming read; returns bytes or a terminal failure."""
        cap = self._max_bytes
        cl = meta.content_length
        if cl is not None:
            # Conservative rejection only; never used to bound decoded size.
            if not compressed and cl > cap:
                return BoundaryFailure(
                    "response_too_large",
                    meta,
                )
            if compressed and cl > cap * 8:
                return BoundaryFailure(
                    "response_too_large",
                    meta,
                )
        deadline = self._clock.mono() + self._config.attempt_deadline_s
        buf = bytearray()
        chunks = response.iter_bytes(chunk_size=64 * 1024)
        while True:
            if self._clock.mono() > deadline:
                return BoundaryFailure("transport_timeout", meta)
            chunk = next(chunks, None)
            if chunk is None:
                break
            buf.extend(chunk)
            if len(buf) > cap:
                return BoundaryFailure(
                    "response_too_large",
                    meta,
                )
        return bytes(buf)

    def _parse_payload(
        self,
        payload: bytes,
        meta: ResponseMeta,
        provenance: Any | None,
    ) -> AttemptOutcome:
        feed = self._feed
        if feed in _JSON_FEEDS:
            try:
                raw_obj = _json.loads(payload)
            except _json.JSONDecodeError:
                return BoundaryFailure("invalid_json", meta)
            if feed is PollFeed.VEHICLES:
                evidence = capture_vehicle_evidence(raw_obj)
                try:
                    result = feed_parse.parse_vehicles(raw_obj, provenance)
                except FeedRejectedError as exc:
                    return FeedRejected("feed_rejected", meta, reason=exc.reason)
                evidence = finalize_vehicle_evidence(
                    evidence, len(result.model.vehicles)
                )
                return Delivered(
                    "delivered", meta, result, evidence, len(payload)
                )
            assert feed is PollFeed.ALERTS
            try:
                result = feed_parse.parse_alerts(raw_obj)
            except FeedRejectedError as exc:
                return FeedRejected("feed_rejected", meta, reason=exc.reason)
            return Delivered(
                "delivered", meta, result, None, len(payload)
            )
        assert feed is PollFeed.TRIPUPDATES
        try:
            result = feed_parse.parse_trip_updates(payload)
        except FeedRejectedError as exc:
            return FeedRejected("feed_rejected", meta, reason=exc.reason)
        return Delivered("delivered", meta, result, None, len(payload))


def _request_path(feed: PollFeed) -> str:
    """Relative request path for one feed (production-constant for M2).

    Test deployments never rewrite this path; they redirect the client's
    base origin instead (the credential never appears in a loggable URL).
    """
    _origin, path = PRODUCTION_ORIGINS[feed]
    return path
