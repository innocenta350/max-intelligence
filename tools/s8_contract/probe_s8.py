"""
S8 Feed Contract Verification probe (MAX Intelligence M2).

Purpose:
    Investigation-only stage S8. Re-verifies M0's recorded API findings
    against the current live feeds, answers the open contract questions
    (B1-B8), and produces AppID-scrubbed evidence files plus a budget
    reconciliation. No production code, no pipeline changes.

Security:
    - TRIMET_APP_ID is loaded from .env at the repo root.
    - The AppID is NEVER printed, logged, or stored. All output and evidence
      pass through redact(); evidence files are scrubbed before write.
    - Request URLs are logged with the query string stripped entirely.

Usage:
    python tools/s8_contract/probe_s8.py <subcommand>

Subcommands (run step by step; each conserves requests):
    envcheck        Load .env, confirm AppID present without revealing it.
    dryrun          Verify the RequestBudget governor against a loopback
                    HTTP server (10 checks, zero live requests).
    parta           Part A live re-verification: A1+A2, A3, A4.
    window          Coordinated 10-minute observation window (B1-B5 captures).
    partb           B6 showStale pairs + B8 HTTP mechanics.
    analyze         Offline analyses: A5/A6, B1-B4 (no live requests).
    b7              gtfs-realtime-bindings evaluation in an isolated venv.
    report          Budget reconciliation + AppID scan (no live requests).

Only the Python standard library is used so S8 installs no project dependencies.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

# ---------------------------------------------------------------------------
# Paths, constants
# ---------------------------------------------------------------------------

REPO = Path(__file__).resolve().parents[2]      # .../max-intelligence/max-intelligence
ENV_PATH = REPO / ".env"
EVIDENCE_DIR = Path(__file__).resolve().parent / "evidence"
BUDGET_STATE_PATH = EVIDENCE_DIR / "budget_state.json"
M1_DATASET_DIR = REPO / "data" / "gtfs" / "dataset-e642a21ab45100a7"

APP_ID = ""
CAMPAIGN_ID = ""                                # set from budget state (stable across phases)

USER_AGENT = "MAXIntelligenceS8Probe/0.1 (+transit research audit)"
CTX = ssl.create_default_context()

# Approved budget table (S8 plan REV 2 / implementation prompt section 6.3)
CAP = 150
BASE_MAX = 110
RETRY_POOL = 20
RESERVE_MAX = 20
MAX_RETRIES_PER_REQUEST = 1

TAG_MAX = {
    "A1A2": 2,
    "A3": 12,
    "A4": 3,
    "B5-vehicles": 60,
    "B5-tripupdates": 10,
    "B5-alerts": 5,
    "B6": 10,
    "B8": 8,
}
BASE_TAGS = [t for t in TAG_MAX]                # tags that consume the base allocation

MAX_ROUTE_IDS = {"90", "100", "190", "200", "290"}

# M0-observed vehicle key list (docs/trimet-api-notes.md section 2.1) for A1 comparison
M0_VEHICLE_KEYS = [
    "vehicleID", "type", "blockID", "bearing", "delay", "direction", "expires",
    "extraBlockID", "garage", "inCongestion", "lastLocID", "lastStopSeq",
    "latitude", "loadPercentage", "locationInScheduleDay", "longitude",
    "messageCode", "newTrip", "nextLocID", "nextStopSeq", "offRoute",
    "routeColor", "routeNumber", "routeSubType", "serviceDate", "signMessage",
    "signMessageLong", "source", "time", "tripID",
]

URL_VEHICLES = "https://developer.trimet.org/ws/v2/vehicles?appID={AID}"
URL_VEHICLES_STALE = "https://developer.trimet.org/ws/v2/vehicles?showStale=true&appID={AID}"
URL_ALERTS = "https://developer.trimet.org/ws/v2/alerts?appID={AID}"
URL_TRIPUPDATES = "https://developer.trimet.org/ws/V1/TripUpdate?appID={AID}"


# ---------------------------------------------------------------------------
# Env loading, redaction, evidence helpers
# ---------------------------------------------------------------------------

def load_env() -> dict[str, str]:
    """Parse .env without ever printing values."""
    values: dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        values[key.strip()] = val.strip().strip('"').strip("'")
    return values


def init_appid() -> bool:
    global APP_ID
    if APP_ID:
        return True
    APP_ID = load_env().get("TRIMET_APP_ID", "")
    return bool(APP_ID)


def redact(text: str) -> str:
    """Remove any trace of the AppID from strings before display/storage."""
    if APP_ID and text:
        return text.replace(APP_ID, "***REDACTED***")
    return text


def _scrub(obj):
    """Recursively redact strings inside evidence payloads (defense in depth)."""
    if isinstance(obj, str):
        return redact(obj)
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_scrub(v) for v in obj]
    return obj


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_iso_z(ms: int | None) -> str | None:
    """ISO-8601 UTC with explicit Z suffix (secondary representation only)."""
    if ms is None:
        return None
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def display_url(url: str) -> str:
    """scheme://host/path with the query string stripped entirely."""
    try:
        s = urlsplit(url)
        return f"{s.scheme}://{s.netloc}{s.path}"
    except Exception:  # noqa: BLE001
        return "<unparseable-url>"


def artifact_header(tags: list[str], requests_consumed: int | None = None) -> dict:
    """Self-identifying block required at the top of every evidence artifact."""
    hdr = {
        "artifact_tags": tags,
        "campaign_id": CAMPAIGN_ID or "unknown",
        "tool": "tools/s8_contract/probe_s8.py",
        "captured_at_utc_ms": now_ms(),
        "captured_at_utc": utc_iso_z(now_ms()),
        "timestamp_convention": "UTC epoch milliseconds primary; ISO-8601 Z secondary",
    }
    if requests_consumed is not None:
        hdr["requests_consumed_by_this_artifact"] = requests_consumed
    return hdr


def save_evidence(name: str, data: dict) -> Path:
    """Write a scrubbed evidence JSON file."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / name
    text = json.dumps(_scrub(data), indent=2, ensure_ascii=False, default=str)
    path.write_text(redact(text), encoding="utf-8")
    return path


def out(msg: str = "") -> None:
    print(redact(msg), flush=True)


def date_header_ms(headers: dict) -> int | None:
    """Parse the HTTP Date header into UTC epoch ms (second resolution)."""
    raw = headers.get("Date") or headers.get("date")
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Request budget governor (atomic accounting)
# ---------------------------------------------------------------------------

class Reservation:
    __slots__ = ("unit", "tag", "kind", "justification")

    def __init__(self, unit: int, tag: str, kind: str, justification: str | None):
        self.unit = unit
        self.tag = tag
        self.kind = kind
        self.justification = justification


class RequestBudget:
    """Atomic request-budget governor.

    Six counters (reserved / transmitted / succeeded / failed / retried /
    aborted_pre_transmission), three separate allocations (base / retry /
    reserve) under one hard cap, per-tag maxima, stop-all semantics, and
    conservative consumption: every granted reservation is permanently
    consumed whether or not transmission follows.
    """

    def __init__(self, base_max: int = BASE_MAX, retry_max: int = RETRY_POOL,
                 reserve_max: int = RESERVE_MAX, cap: int = CAP,
                 tag_max: dict[str, int] | None = None,
                 stop_event: threading.Event | None = None):
        self._lock = threading.Lock()
        self.stop_event = stop_event or threading.Event()
        self.base_max = base_max
        self.retry_max = retry_max
        self.reserve_max = reserve_max
        self.cap = cap
        self.tag_max = dict(tag_max) if tag_max else {}
        self.base_used = 0
        self.retry_used = 0
        self.reserve_used = 0
        self.reserved = 0
        self.transmitted = 0
        self.succeeded = 0
        self.failed = 0
        self.retried = 0
        self.aborted_pre_transmission = 0
        self.tags: dict[str, dict[str, int]] = {}
        self.reserve_draws: list[dict] = []
        self.stop_log: list[dict] = []
        self.refusals: list[dict] = []
        self._next_unit = 1
        self.granted_count = 0
        self.max_total_used = 0

    # -- internal helpers ---------------------------------------------------

    def _tag(self, tag: str) -> dict[str, int]:
        return self.tags.setdefault(tag, {
            "reserved": 0, "transmitted": 0, "succeeded": 0,
            "failed": 0, "retried": 0, "aborted_pre_transmission": 0,
        })

    def _log_stop(self, reason: str, tag: str | None = None) -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()
        self.stop_log.append({
            "reason": reason, "tag": tag, "at_utc_ms": now_ms(),
            "at_utc": utc_iso_z(now_ms()),
            "total_used": self.base_used + self.retry_used + self.reserve_used,
        })

    def _total(self) -> int:
        return self.base_used + self.retry_used + self.reserve_used

    # -- reservation ----------------------------------------------------------

    def reserve(self, tag: str, kind: str = "base",
                justification: str | None = None):
        """Atomically reserve one unit before transmission. Returns a
        Reservation, or None when refused (reason recorded)."""
        with self._lock:
            if self.stop_event.is_set():
                self.refusals.append({"tag": tag, "kind": kind,
                                      "reason": "stop event set",
                                      "at_utc_ms": now_ms()})
                return None
            total = self._total()
            if total >= self.cap:
                self._log_stop("hard cap reached", tag)
                self.refusals.append({"tag": tag, "kind": kind,
                                      "reason": "hard cap reached",
                                      "at_utc_ms": now_ms()})
                return None
            if kind == "base":
                if self.base_used >= self.base_max:
                    self.refusals.append({"tag": tag, "kind": kind,
                                          "reason": "base allocation exhausted",
                                          "at_utc_ms": now_ms()})
                    return None
                tmax = self.tag_max.get(tag)
                if tmax is not None and self._tag(tag).get("base_grants", 0) >= tmax:
                    self.refusals.append({"tag": tag, "kind": kind,
                                          "reason": f"tag max {tmax} reached",
                                          "at_utc_ms": now_ms()})
                    return None
            elif kind == "retry":
                if self.retry_used >= self.retry_max:
                    self._log_stop("retry pool exhausted", tag)
                    self.refusals.append({"tag": tag, "kind": kind,
                                          "reason": "retry pool exhausted",
                                          "at_utc_ms": now_ms()})
                    return None
            elif kind == "reserve":
                if not justification:
                    self.refusals.append({"tag": tag, "kind": kind,
                                          "reason": "reserve draw requires justification",
                                          "at_utc_ms": now_ms()})
                    return None
                if self.reserve_used >= self.reserve_max:
                    self.refusals.append({"tag": tag, "kind": kind,
                                          "reason": "reserve exhausted",
                                          "at_utc_ms": now_ms()})
                    return None
            else:
                self.refusals.append({"tag": tag, "kind": kind,
                                      "reason": f"unknown kind {kind!r}",
                                      "at_utc_ms": now_ms()})
                return None

            # Grant: permanently consumed from this instant.
            unit = self._next_unit
            self._next_unit += 1
            self.granted_count += 1
            if kind == "base":
                self.base_used += 1
            elif kind == "retry":
                self.retry_used += 1
            else:
                self.reserve_used += 1
                self.reserve_draws.append({"tag": tag, "unit": unit,
                                           "justification": justification,
                                           "at_utc_ms": now_ms(),
                                           "at_utc": utc_iso_z(now_ms())})
            self.reserved += 1
            t = self._tag(tag)
            t["reserved"] += 1
            if kind == "base":
                t["base_grants"] = t.get("base_grants", 0) + 1
            if kind == "retry":
                t["retried_grants"] = t.get("retried_grants", 0) + 1
            self.max_total_used = max(self.max_total_used, self._total())
            if self._total() >= self.cap:
                self._log_stop("hard cap reached after grant", tag)
            return Reservation(unit, tag, kind, justification)

    # -- lifecycle markers ----------------------------------------------------

    def mark_transmitted(self, res: Reservation) -> None:
        with self._lock:
            self.transmitted += 1
            self._tag(res.tag)["transmitted"] += 1
            if res.kind == "retry":
                self.retried += 1

    def mark_success(self, res: Reservation) -> None:
        with self._lock:
            self.succeeded += 1
            self._tag(res.tag)["succeeded"] += 1

    def mark_failure(self, res: Reservation) -> None:
        with self._lock:
            self.failed += 1
            self._tag(res.tag)["failed"] += 1

    def mark_aborted(self, res: Reservation) -> None:
        with self._lock:
            self.aborted_pre_transmission += 1
            self._tag(res.tag)["aborted_pre_transmission"] += 1

    # -- invariants / persistence ---------------------------------------------

    def _check_invariants_locked(self) -> dict:
        """Caller must already hold self._lock."""
        problems = []
        if self.transmitted != self.succeeded + self.failed:
            problems.append("transmitted != succeeded + failed")
        if self.reserved != self.transmitted + self.aborted_pre_transmission:
            problems.append("reserved != transmitted + aborted_pre_transmission")
        if self.reserved > self.cap:
            problems.append("reserved exceeds cap")
        if self.retry_used > self.retry_max or self.retried > self.retry_max:
            problems.append("retry pool exceeded")
        if self._total() > self.cap:
            problems.append("total allocation exceeds cap")
        return {"ok": not problems, "problems": problems}

    def check_invariants(self) -> dict:
        with self._lock:
            return self._check_invariants_locked()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "campaign_id": CAMPAIGN_ID or None,
                "cap": self.cap,
                "allocations": {
                    "base": {"max": self.base_max, "used": self.base_used},
                    "retry_pool": {"max": self.retry_max, "used": self.retry_used},
                    "reserve": {"max": self.reserve_max, "used": self.reserve_used},
                },
                "counters": {
                    "reserved": self.reserved,
                    "transmitted": self.transmitted,
                    "succeeded": self.succeeded,
                    "failed": self.failed,
                    "retried": self.retried,
                    "aborted_pre_transmission": self.aborted_pre_transmission,
                },
                "per_tag": {k: dict(v) for k, v in sorted(self.tags.items())},
                "reserve_draws": list(self.reserve_draws),
                "stop_log": list(self.stop_log),
                "refusals": list(self.refusals),
                "granted_count": self.granted_count,
                "max_total_used": self.max_total_used,
                "invariants": self._check_invariants_locked(),
            }

    # State persistence so the campaign count survives across phase processes.

    def save_state(self, path: Path = BUDGET_STATE_PATH) -> None:
        with self._lock:
            data = {
                "campaign_id": CAMPAIGN_ID or None,
                "cap": self.cap,
                "base_max": self.base_max,
                "retry_max": self.retry_max,
                "reserve_max": self.reserve_max,
                "base_used": self.base_used,
                "retry_used": self.retry_used,
                "reserve_used": self.reserve_used,
                "counters": {
                    "reserved": self.reserved,
                    "transmitted": self.transmitted,
                    "succeeded": self.succeeded,
                    "failed": self.failed,
                    "retried": self.retried,
                    "aborted_pre_transmission": self.aborted_pre_transmission,
                },
                "per_tag": {k: dict(v) for k, v in sorted(self.tags.items())},
                "reserve_draws": list(self.reserve_draws),
                "stop_log": list(self.stop_log),
                "refusals": list(self.refusals),
                "next_unit": self._next_unit,
                "granted_count": self.granted_count,
                "max_total_used": self.max_total_used,
                "saved_at_utc_ms": now_ms(),
                "saved_at_utc": utc_iso_z(now_ms()),
            }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    @classmethod
    def load_state(cls, path: Path = BUDGET_STATE_PATH) -> "RequestBudget":
        data = json.loads(path.read_text(encoding="utf-8"))
        b = cls(base_max=data["base_max"], retry_max=data["retry_max"],
                reserve_max=data["reserve_max"], cap=data["cap"],
                tag_max=TAG_MAX)
        b.base_used = data["base_used"]
        b.retry_used = data["retry_used"]
        b.reserve_used = data["reserve_used"]
        c = data["counters"]
        b.reserved = c["reserved"]
        b.transmitted = c["transmitted"]
        b.succeeded = c["succeeded"]
        b.failed = c["failed"]
        b.retried = c["retried"]
        b.aborted_pre_transmission = c["aborted_pre_transmission"]
        b.tags = {k: dict(v) for k, v in data["per_tag"].items()}
        b.reserve_draws = data["reserve_draws"]
        b.stop_log = data["stop_log"]
        b.refusals = data["refusals"]
        b._next_unit = data["next_unit"]
        b.granted_count = data["granted_count"]
        b.max_total_used = data["max_total_used"]
        if data["stop_log"]:
            b.stop_event.set()
        return b


def load_campaign_budget() -> RequestBudget:
    """Load the campaign budget state, creating it (with a stable campaign id)
    on first use."""
    global CAMPAIGN_ID
    if BUDGET_STATE_PATH.exists():
        b = RequestBudget.load_state()
        CAMPAIGN_ID = json.loads(BUDGET_STATE_PATH.read_text(encoding="utf-8"))["campaign_id"]
        return b
    CAMPAIGN_ID = f"s8-{now_ms()}-{uuid.uuid4().hex[:8]}"
    b = RequestBudget(tag_max=TAG_MAX)
    b.save_state()
    return b


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

def fetch_raw(url: str, timeout: float = 30.0, headers: dict | None = None):
    """GET a URL (already containing credentials if any).
    Returns (status, headers, body). Network errors propagate."""
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "*/*", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as resp:
            return resp.status, dict(resp.headers.items()), resp.read()
    except urllib.error.HTTPError as e:  # non-2xx is still evidence
        return e.code, dict(e.headers.items()) if e.headers else {}, e.read()


def budgeted_fetch(budget: RequestBudget, tag: str, url: str, *,
                   kind: str = "base", justification: str | None = None,
                   timeout: float = 30.0, headers: dict | None = None,
                   allow_retry: bool = True, return_body: bool = False) -> dict:
    """One budgeted request lifecycle: reserve -> transmit -> record.

    Follows the conservative consumption rule: a granted reservation is
    consumed even if transmission fails ambiguously. At most
    MAX_RETRIES_PER_REQUEST retries per failed request; each retry reserves
    its own unit from the retry pool.
    """
    result: dict = {"tag": tag, "kind": kind,
                    "url_redacted": display_url(url),
                    "started_at_utc_ms": now_ms(),
                    "started_at_utc": utc_iso_z(now_ms())}
    res = budget.reserve(tag, kind, justification)
    if res is None:
        result["outcome"] = "refused"
        return result
    if budget.stop_event.is_set():
        budget.mark_aborted(res)
        result["outcome"] = "aborted_pre_transmission"
        budget.save_state()
        return result
    budget.mark_transmitted(res)
    result["transmitted_at_utc_ms"] = now_ms()
    try:
        status, resp_headers, body = fetch_raw(url, timeout, headers)
    except Exception as exc:  # noqa: BLE001
        budget.mark_failure(res)
        result["outcome"] = "failed"
        result["error"] = redact(repr(exc))
        result["finished_at_utc_ms"] = now_ms()
        budget.save_state()
        # Retry path: each retry is a new reservation from the retry pool.
        if allow_retry and kind == "base":
            return _retry_path(budget, tag, url, result, timeout, headers,
                               return_body)
        return result
    budget.mark_success(res)
    result["outcome"] = "succeeded"
    result["http_status"] = status
    result["bytes"] = len(body)
    result["date_header"] = resp_headers.get("Date") or resp_headers.get("date")
    result["date_header_ms"] = date_header_ms(resp_headers)
    result["content_type"] = resp_headers.get("Content-Type") or resp_headers.get("content-type")
    result["content_encoding"] = resp_headers.get("Content-Encoding") or resp_headers.get("content-encoding")
    result["response_headers_sanitized"] = {
        k: redact(str(v)) for k, v in resp_headers.items()}
    if return_body:
        result["body"] = body
    result["finished_at_utc_ms"] = now_ms()
    budget.save_state()
    return result


def _retry_path(budget: RequestBudget, tag: str, url: str, first: dict,
                timeout: float, headers: dict | None,
                return_body: bool) -> dict:
    """Retry a failed request once, from the retry-pool allocation."""
    first["initial_outcome"] = "failed"
    res = budget.reserve(tag, "retry")
    if res is None:
        first["retry_outcome"] = "refused"
        first["item_incomplete"] = True
        return first
    if budget.stop_event.is_set():
        budget.mark_aborted(res)
        first["retry_outcome"] = "aborted_pre_transmission"
        first["item_incomplete"] = True
        budget.save_state()
        return first
    budget.mark_transmitted(res)
    try:
        status, resp_headers, body = fetch_raw(url, timeout, headers)
    except Exception as exc:  # noqa: BLE001
        budget.mark_failure(res)
        first["retry_outcome"] = "failed"
        first["retry_error"] = redact(repr(exc))
        first["item_incomplete"] = True
        budget.save_state()
        return first
    budget.mark_success(res)
    first["outcome"] = "succeeded"          # the request ultimately succeeded
    first["retry_outcome"] = "succeeded"
    first["http_status"] = status
    first["bytes"] = len(body)
    first["date_header"] = resp_headers.get("Date") or resp_headers.get("date")
    first["date_header_ms"] = date_header_ms(resp_headers)
    first["content_type"] = resp_headers.get("Content-Type") or resp_headers.get("content-type")
    first["response_headers_sanitized"] = {
        k: redact(str(v)) for k, v in resp_headers.items()}
    if return_body:
        first["body"] = body
    budget.save_state()
    return first


# ---------------------------------------------------------------------------
# Feed summarizers (JSON feeds)
# ---------------------------------------------------------------------------

def jv(obj, name):
    """TriMet JSON keys carry no '@' prefix; be defensive and accept both."""
    if obj is None:
        return None
    if name in obj:
        return obj[name]
    return obj.get("@" + name)


def compact_vehicle(v: dict) -> dict:
    return {
        "vehicleID": jv(v, "vehicleID"),
        "type": jv(v, "type"),
        "routeNumber": jv(v, "routeNumber"),
        "routeSubType": jv(v, "routeSubType"),
        "routeColor": jv(v, "routeColor"),
        "tripID": jv(v, "tripID"),
        "newTrip": jv(v, "newTrip"),
        "offRoute": jv(v, "offRoute"),
        "direction": jv(v, "direction"),
        "time": jv(v, "time"),
        "expires": jv(v, "expires"),
        "delay": jv(v, "delay"),
        "latitude": jv(v, "latitude"),
        "longitude": jv(v, "longitude"),
        "bearing": jv(v, "bearing"),
        "nextLocID": jv(v, "nextLocID"),
        "lastLocID": jv(v, "lastLocID"),
    }


def vehicle_change_key(v: dict) -> tuple:
    """Fields whose change counts as a vehicle-state change for B5."""
    return (jv(v, "time"), jv(v, "latitude"), jv(v, "longitude"),
            jv(v, "bearing"), jv(v, "tripID"), jv(v, "nextLocID"),
            jv(v, "routeNumber"), jv(v, "delay"))


def summarize_vehicles_full(parsed: dict) -> dict:
    """Full A1-style analysis of one vehicles snapshot."""
    rs = (parsed or {}).get("resultSet", {})
    vehicles = rs.get("vehicle", [])
    if isinstance(vehicles, dict):
        vehicles = [vehicles]
    query_time = jv(rs, "queryTime")
    key_presence: dict[str, int] = {}
    at_prefixed: set[str] = set()
    routes_seen: dict = {}
    rails = []
    for v in vehicles:
        for k in v:
            if k.startswith("@"):
                at_prefixed.add(k)
            key_presence[k] = key_presence.get(k, 0) + 1
        r = jv(v, "routeNumber")
        routes_seen[r] = routes_seen.get(r, 0) + 1
        if str(jv(v, "type")).lower() == "rail":
            rails.append(compact_vehicle(v))
    margins = sorted((v["expires"] - v["time"]) / 1000.0
                     for v in rails if v["expires"] and v["time"])
    q_margins = sorted((v["expires"] - query_time) / 1000.0
                       for v in rails if v["expires"] and query_time)
    return {
        "queryTime": query_time,
        "queryTime_iso": utc_iso_z(query_time),
        "vehicle_count_total": len(vehicles),
        "rail_count": len(rails),
        "keys_first_vehicle": sorted(vehicles[0].keys()) if vehicles else [],
        "keys_union_all_vehicles": sorted(key_presence),
        "at_prefixed_keys": sorted(at_prefixed),
        "key_presence_counts": dict(sorted(key_presence.items())),
        "route_counts": dict(sorted(((str(k), n) for k, n in routes_seen.items()),
                                    key=lambda kv: str(kv[0]))),
        "rail_vehicles": rails,
        "expires_minus_time_seconds_min": margins[0] if margins else None,
        "expires_minus_time_seconds_median": margins[len(margins) // 2] if margins else None,
        "expires_minus_time_seconds_max": margins[-1] if margins else None,
        "expires_minus_queryTime_seconds_min": q_margins[0] if q_margins else None,
        "expires_minus_queryTime_seconds_max": q_margins[-1] if q_margins else None,
    }


def vehicle_ids(parsed: dict) -> list:
    rs = (parsed or {}).get("resultSet", {})
    vehicles = rs.get("vehicle", [])
    if isinstance(vehicles, dict):
        vehicles = [vehicles]
    return [jv(v, "vehicleID") for v in vehicles]


def summarize_alerts(parsed: dict) -> dict:
    """Compact, credential-free summary of one alerts snapshot."""
    rs = (parsed or {}).get("resultSet", {})
    alerts = rs.get("alert", []) if isinstance(rs, dict) else []
    if isinstance(alerts, dict):
        alerts = [alerts]
    out_alerts = []
    for a in alerts:
        route_val = a.get("route", [])
        routes = route_val if isinstance(route_val, list) else ([route_val] if isinstance(route_val, dict) else [])
        loc_val = a.get("location", [])
        locs = loc_val if isinstance(loc_val, list) else ([loc_val] if isinstance(loc_val, dict) else [])
        out_alerts.append({
            "id": jv(a, "id"),
            "begin": jv(a, "begin"),
            "end": jv(a, "end"),
            "system_wide_flag": jv(a, "system_wide_flag"),
            "header_text": jv(a, "header_text"),
            "desc_first160": (jv(a, "desc") or "")[:160],
            "info_link_url": jv(a, "info_link_url"),
            "routes": [{"id": jv(r, "id"), "type": jv(r, "type"),
                        "no_service_flag": jv(r, "no_service_flag")} for r in routes],
            "locations": [{"id": jv(l, "id"), "desc": (jv(l, "desc") or "")[:80],
                           "no_service_flag": jv(l, "no_service_flag")} for l in locs],
        })
    return {"queryTime": jv(rs, "queryTime") if isinstance(rs, dict) else None,
            "alert_count": len(alerts), "alerts": out_alerts}


# ---------------------------------------------------------------------------
# Minimal protobuf wire-format decoder (stdlib only; follows audit_m0.py).
# Field numbers follow the official gtfs-realtime.proto (v2.0).
# ---------------------------------------------------------------------------

def parse_wire(data: bytes):
    items = []
    i = 0
    n = len(data)
    while i < n:
        tag, i = _varint(data, i)
        field_number = tag >> 3
        wire_type = tag & 7
        if wire_type == 0:
            val, i = _varint(data, i)
        elif wire_type == 2:
            ln, i = _varint(data, i)
            val = data[i:i + ln]
            i += ln
        elif wire_type == 5:
            val = data[i:i + 4]
            i += 4
        elif wire_type == 1:
            val = data[i:i + 8]
            i += 8
        else:
            raise ValueError(f"unsupported wire type {wire_type} at byte {i}")
        items.append((field_number, wire_type, val))
    return items


def _varint(data: bytes, i: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        b = data[i]
        result |= (b & 0x7F) << shift
        i += 1
        if not b & 0x80:
            return result, i
        shift += 7


def submessages(items, field_no):
    return [parse_wire(v) for f, w, v in items if f == field_no and w == 2]


def string_field(items, field_no):
    vals = [v.decode("utf-8", errors="replace") for f, w, v in items if f == field_no and w == 2]
    return vals[0] if vals else None


def int_field(items, field_no):
    """Varint fields can carry negative int32/int64 as sign-extended values."""
    vals = [v for f, w, v in items if f == field_no and w == 0]
    if not vals:
        return None
    x = vals[0]
    if x >= 1 << 63:
        x -= 1 << 64
    return x


def ste(sub_items):
    return {"delay": int_field(sub_items, 1), "time": int_field(sub_items, 2),
            "uncertainty": int_field(sub_items, 3)}


def decode_trip_descriptor(sub_items):
    return {
        "trip_id": string_field(sub_items, 1),
        "start_time": string_field(sub_items, 2),
        "start_date": string_field(sub_items, 3),
        "schedule_relationship_raw": int_field(sub_items, 4),
        "route_id": string_field(sub_items, 5),
        "direction_id": int_field(sub_items, 6),
    }


def decode_tripupdate_full(sub_items):
    """TripUpdate with the complete stop_time_update list (needed for B2/B5)."""
    trip_items = submessages(sub_items, 1)
    stu_items = submessages(sub_items, 2)
    veh_items = submessages(sub_items, 3)
    trip = decode_trip_descriptor(trip_items[0]) if trip_items else None
    vehicle = None
    if veh_items:
        vehicle = {"id": string_field(veh_items[0], 1),
                   "label": string_field(veh_items[0], 2)}
    stus = []
    for s in stu_items:
        arrival_items = submessages(s, 2)
        departure_items = submessages(s, 3)
        stus.append({
            "stop_sequence": int_field(s, 1),
            "stop_id": string_field(s, 4),
            "schedule_relationship_raw": int_field(s, 5),
            "arrival": ste(arrival_items[0]) if arrival_items else None,
            "departure": ste(departure_items[0]) if departure_items else None,
        })
    return {
        "trip": trip,
        "vehicle": vehicle,
        "timestamp": int_field(sub_items, 4),
        "delay": int_field(sub_items, 5),
        "stop_time_updates": stus,
    }


def decode_rt_feed(body: bytes):
    """Parse a GTFS-RT FeedMessage: header + entities."""
    msg = parse_wire(body)
    header_items = submessages(msg, 1)
    entity_items = submessages(msg, 2)
    header = None
    if header_items:
        header = {
            "gtfs_realtime_version": string_field(header_items[0], 1),
            "incrementality_raw": int_field(header_items[0], 2),
            "timestamp": int_field(header_items[0], 3),
        }
    entities = []
    for ent in entity_items:
        rec = {"id": string_field(ent, 1), "is_deleted": int_field(ent, 2)}
        tu_items = submessages(ent, 3)
        vp_items = submessages(ent, 4)
        al_items = submessages(ent, 5)
        if tu_items:
            rec["trip_update"] = decode_tripupdate_full(tu_items[0])
        if vp_items:
            rec["vehicle_position"] = True   # layout known nonstandard; not used in S8
        if al_items:
            rec["alert"] = True
        entities.append(rec)
    return header, entities


def stu_signature(tu: dict) -> tuple:
    """Comparable signature of a TripUpdate's stop_time_updates (for B5)."""
    return tuple(
        (s["stop_sequence"],
         (s["arrival"] or {}).get("delay") if s["arrival"] else None,
         (s["arrival"] or {}).get("time") if s["arrival"] else None,
         (s["departure"] or {}).get("delay") if s["departure"] else None,
         (s["departure"] or {}).get("time") if s["departure"] else None)
        for s in tu["stop_time_updates"])


# ---------------------------------------------------------------------------
# Dry-run: governor verification against a loopback HTTP server (no live reqs)
# ---------------------------------------------------------------------------

class _LoopbackHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # silence per-request logging
        pass

    def do_GET(self):
        if self.path == "/ok":
            body = b'{"resultSet":{"queryTime":1,"vehicle":[]}}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/incomplete":
            # Headers promise 1000 bytes; deliver 11 then drop the connection
            # so the client sees an ambiguous mid-response failure.
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "1000")
            self.end_headers()
            try:
                self.wfile.write(b'{"partial":')
                self.wfile.flush()
            finally:
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()
                self.close_connection = True
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()


def _start_loopback():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _LoopbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def _fetch_loopback(url: str, timeout: float = 10.0):
    """Bare fetch used inside dry-run scenarios (no budget inside)."""
    req = urllib.request.Request(url, headers={"User-Agent": "s8-dryrun"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers.items()), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers.items()) if e.headers else {}, e.read()


def _dry_fetch(budget: RequestBudget, tag: str, url: str, *,
               kind: str = "base", justification: str | None = None,
               allow_retry: bool = True) -> dict:
    """budgeted_fetch without save_state churn (dry-run only)."""
    result: dict = {"tag": tag, "kind": kind}
    res = budget.reserve(tag, kind, justification)
    if res is None:
        result["outcome"] = "refused"
        return result
    if budget.stop_event.is_set():
        budget.mark_aborted(res)
        result["outcome"] = "aborted_pre_transmission"
        return result
    budget.mark_transmitted(res)
    try:
        status, _h, _b = _fetch_loopback(url)
    except Exception as exc:  # noqa: BLE001
        budget.mark_failure(res)
        result["outcome"] = "failed"
        result["error"] = type(exc).__name__
        if allow_retry and kind == "base":
            r = budget.reserve(tag, "retry")
            if r is None:
                result["retry_outcome"] = "refused"
                result["item_incomplete"] = True
                return result
            if budget.stop_event.is_set():
                budget.mark_aborted(r)
                result["retry_outcome"] = "aborted_pre_transmission"
                result["item_incomplete"] = True
                return result
            budget.mark_transmitted(r)
            try:
                _fetch_loopback(url)
                budget.mark_success(r)
                result["retry_outcome"] = "succeeded"
            except Exception as exc2:  # noqa: BLE001
                budget.mark_failure(r)
                result["retry_outcome"] = "failed"
                result["retry_error"] = type(exc2).__name__
                result["item_incomplete"] = True
        return result
    budget.mark_success(res)
    result["outcome"] = "succeeded"
    result["http_status"] = status
    return result


def cmd_dryrun(_args):
    """All ten section-6.4 checks against a loopback server. Zero live requests."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    server, base = _start_loopback()
    results: dict[str, dict] = {}
    try:
        # -- Check 1: concurrent reservations, exact count --------------------
        b = RequestBudget(base_max=75, retry_max=0, reserve_max=0, cap=75)
        errs: list[str] = []
        def worker(n):
            for _ in range(n):
                r = _dry_fetch(b, "DRY1", base + "/ok", allow_retry=False)
                if r["outcome"] not in ("succeeded", "aborted_pre_transmission"):
                    errs.append(json.dumps(r))
        threads = [threading.Thread(target=worker, args=(25,)) for _ in range(3)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        snap = b.snapshot()
        out("dryrun: check1 done")
        c = snap["counters"]
        ok1 = (not errs
               and c["reserved"] == 75
               and len(snap["refusals"]) == 0
               and c["transmitted"] + c["aborted_pre_transmission"] == 75
               and c["succeeded"] == c["transmitted"]
               and c["failed"] == 0)
        results["check1_concurrent_reservations"] = {
            "pass": ok1, "detail": {"counters": snap["counters"],
                                    "unexpected": errs[:5],
                                    "note": ("granted units are fully accounted: "
                                             "transmitted + aborted_pre_transmission == reserved")}}

        # -- Check 2: boundary at 149/150/151 ----------------------------------
        b = RequestBudget(base_max=150, retry_max=0, reserve_max=0, cap=150,
                          tag_max=None)
        grants = []
        for i in range(151):
            res = b.reserve("DRY2", "base")
            grants.append((i + 1, res is not None))
        ok2 = (all(g for n, g in grants if n <= 150)
               and grants[148][1] and grants[149][1] and not grants[150][1]
               and b.snapshot()["counters"]["reserved"] == 150)
        out("dryrun: check2 done")
        results["check2_boundary_149_150_151"] = {
            "pass": ok2,
            "detail": {"unit_149_granted": grants[148][1],
                       "unit_150_granted": grants[149][1],
                       "unit_151_granted": grants[150][1],
                       "refusal_reasons": [r["reason"] for r in b.refusals]}}

        # -- Checks 3 + 7: retry-pool contention and bound ---------------------
        b = RequestBudget(base_max=25, retry_max=20, reserve_max=0, cap=60)
        incomplete_items: list[dict] = []

        def failing_worker():
            r = _dry_fetch(b, "DRY3", base + "/incomplete", allow_retry=True)
            if r.get("item_incomplete"):
                incomplete_items.append(r)
        barrier = threading.Barrier(25)

        def failing_worker_sync():
            barrier.wait()
            failing_worker()
        threads = [threading.Thread(target=failing_worker_sync) for _ in range(25)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        snap = b.snapshot()
        out("dryrun: check3/7 done")
        retry_grants = snap["counters"]["retried"]
        retry_refusals = [r for r in snap["refusals"] if r["reason"] == "retry pool exhausted"]
        ok37 = (snap["allocations"]["retry_pool"]["used"] == 20
                and retry_grants == 20
                and len(retry_refusals) >= 1
                and snap["allocations"]["base"]["used"] == 25
                and len(incomplete_items) >= 1)
        results["check3_retry_pool_contention"] = {
            "pass": ok37,
            "detail": {"retry_pool_used": snap["allocations"]["retry_pool"]["used"],
                       "retry_grants": retry_grants,
                       "retry_pool_exhausted_refusals": len(retry_refusals),
                       "other_retry_refusals": len([r for r in snap["refusals"]
                                                    if r["kind"] == "retry"]) - len(retry_refusals),
                       "items_flagged_incomplete": len(incomplete_items)}}
        results["check7_retry_pool_bound"] = {
            "pass": retry_grants <= 20 and snap["allocations"]["retry_pool"]["used"] <= 20,
            "detail": {"retried_counter": retry_grants,
                       "retry_allocation_used": snap["allocations"]["retry_pool"]["used"]}}

        # -- Check 4: stop-event propagation to three poller threads -----------
        b = RequestBudget(base_max=10, retry_max=0, reserve_max=0, cap=10)
        stop_points: list[dict] = {}

        def poller_worker(name):
            iterations = 0
            while not b.stop_event.is_set():
                r = _dry_fetch(b, f"DRY4-{name}", base + "/ok", allow_retry=False)
                if r["outcome"] in ("refused", "aborted_pre_transmission"):
                    stop_points[name] = {"iterations": iterations,
                                         "stopped_on": r["outcome"]}
                    return
                iterations += 1
            stop_points[name] = {"iterations": iterations,
                                 "stopped_on": "stop_event_observed"}
        threads = [threading.Thread(target=poller_worker, args=(n,)) for n in ("A", "B", "C")]
        [t.start() for t in threads]
        [t.join() for t in threads]
        out("dryrun: check4 done")
        snap = b.snapshot()
        total_iterations = sum(s["iterations"] for s in stop_points.values())
        c = snap["counters"]
        ok4 = (len(stop_points) == 3
               and c["reserved"] == 10
               and c["transmitted"] + c["aborted_pre_transmission"] == 10
               and c["failed"] == 0
               and all(s.get("stopped_on") for s in stop_points.values()))
        results["check4_stop_event_propagation"] = {
            "pass": ok4,
            "detail": {"stop_points": stop_points,
                       "total_successful_iterations": total_iterations,
                       "counters": snap["counters"],
                       "stop_log": snap["stop_log"],
                       "note": ("the final grant reaches the cap and sets the stop "
                                "event; a just-granted reservation may legitimately "
                                "abort pre-transmission and is consumed")}}

        # -- Check 5: ambiguous-failure consumption ----------------------------
        b = RequestBudget(base_max=5, retry_max=0, reserve_max=0, cap=5)
        r = _dry_fetch(b, "DRY5", base + "/incomplete", allow_retry=False)
        out("dryrun: check5 done")
        snap = b.snapshot()
        ok5 = (r["outcome"] == "failed"
               and snap["counters"]["reserved"] == 1
               and snap["counters"]["transmitted"] == 1
               and snap["counters"]["failed"] == 1
               and snap["counters"]["succeeded"] == 0)
        results["check5_ambiguous_failure_consumes"] = {
            "pass": ok5, "detail": {"outcome": r["outcome"],
                                    "counters": snap["counters"]}}

        # -- Check 6: allocation isolation --------------------------------------
        b = RequestBudget(base_max=30, retry_max=5, reserve_max=5, cap=40)
        for _ in range(30):
            _dry_fetch(b, "DRY6", base + "/ok", allow_retry=False)
        out("dryrun: check6 done")
        snap = b.snapshot()
        ok6 = (snap["allocations"]["base"]["used"] == 30
               and snap["allocations"]["retry_pool"]["used"] == 0
               and snap["allocations"]["reserve"]["used"] == 0)
        results["check6_allocation_isolation"] = {
            "pass": ok6, "detail": {"allocations": snap["allocations"]}}

        # -- Check 8: reserve discipline ----------------------------------------
        b = RequestBudget(base_max=2, retry_max=0, reserve_max=3, cap=5)
        _dry_fetch(b, "DRY8", base + "/ok", allow_retry=False)
        _dry_fetch(b, "DRY8", base + "/ok", allow_retry=False)
        r_implicit = _dry_fetch(b, "DRY8", base + "/ok", allow_retry=False)  # base exhausted
        r_nogood = b.reserve("DRY8", "reserve", justification=None)           # no justification
        r_good = b.reserve("DRY8", "reserve", justification="dry-run test draw")
        out("dryrun: check8 done")
        snap = b.snapshot()
        ok8 = (r_implicit["outcome"] == "refused"
               and snap["allocations"]["reserve"]["used"] == 1  # only the justified draw
               and r_nogood is None and r_good is not None
               and len(snap["reserve_draws"]) == 1
               and snap["reserve_draws"][0]["justification"] == "dry-run test draw")
        results["check8_reserve_discipline"] = {
            "pass": ok8,
            "detail": {"implicit_draw_refused": r_implicit["outcome"] == "refused",
                       "no_justification_refused": r_nogood is None,
                       "justified_draw_granted": r_good is not None,
                       "reserve_draws": snap["reserve_draws"]}}

        # -- Check 9: global cap under any interleaving -------------------------
        # Base + reserve draws only: retry-pool exhaustion legitimately sets the
        # stop event (section 6.3.4), so retries cannot participate in a test
        # that drives consumption to the cap.
        b = RequestBudget(base_max=50, retry_max=20, reserve_max=20, cap=60)
        def mixed_worker(idx):
            kind = "base" if idx % 2 == 0 else "reserve"
            while not b.stop_event.is_set():
                res = b.reserve(f"DRY9-{kind}", kind,
                                justification="dry-run global-cap test" if kind == "reserve" else None)
                if res is None:
                    return
                if b.stop_event.is_set():
                    b.mark_aborted(res)
                    return
                b.mark_transmitted(res)
                b.mark_success(res)
        threads = [threading.Thread(target=mixed_worker, args=(i,)) for i in range(6)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        out("dryrun: check9 done")
        snap = b.snapshot()
        al = snap["allocations"]
        ok9 = (snap["max_total_used"] <= 60
               and snap["counters"]["reserved"] == 60
               and al["base"]["used"] + al["retry_pool"]["used"] + al["reserve"]["used"] == 60
               and al["base"]["used"] <= 50
               and al["reserve"]["used"] >= 1
               and al["reserve"]["used"] <= 20)
        results["check9_global_cap_any_interleaving"] = {
            "pass": ok9,
            "detail": {"max_total_used": snap["max_total_used"],
                       "reserved": snap["counters"]["reserved"],
                       "allocations": snap["allocations"],
                       "note": ("base + reserve draws drive the cap; retry-pool "
                                "exhaustion sets stop-all by design and is "
                                "covered by checks 3/7")}}

        # -- Check 10: no double-grant ------------------------------------------
        b = RequestBudget(base_max=40, retry_max=10, reserve_max=10, cap=60)
        def granting_worker():
            while not b.stop_event.is_set():
                res = b.reserve("DRY10", "base")
                if res is None:
                    return
        threads = [threading.Thread(target=granting_worker) for _ in range(5)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        out("dryrun: check10 done")
        snap = b.snapshot()
        ok10 = (snap["granted_count"] == snap["counters"]["reserved"]
                and snap["counters"]["reserved"] == 40
                and snap["max_total_used"] <= 60)
        results["check10_no_double_grant"] = {
            "pass": ok10,
            "detail": {"granted_count": snap["granted_count"],
                       "reserved": snap["counters"]["reserved"]}}

        # -- invariant sweep across all budgets exercised ------------------------
        # (each scenario already asserted its own invariants via snapshot())
    finally:
        server.shutdown()
        server.server_close()

    all_pass = all(v["pass"] for v in results.values())
    rec = artifact_header(["DRYRUN"], requests_consumed=0)
    rec["note"] = ("Loopback-only verification; zero live requests emitted. "
                   "Governor instances here are throwaway test budgets, "
                   "separate from the campaign budget.")
    rec["checks"] = results
    rec["all_pass"] = all_pass
    rec["result"] = "PASS" if all_pass else "FAIL"
    path = save_evidence("governor_dryrun.json", rec)
    for name, r in results.items():
        out(f"{'PASS' if r['pass'] else 'FAIL'}  {name}")
    out(f"DRYRUN result: {rec['result']} -> {path}")


# ---------------------------------------------------------------------------
# Part A: live re-verification (A1+A2, A3, A4)
# ---------------------------------------------------------------------------

def _skew_record(result: dict, parsed: dict | None) -> dict:
    """A2: timestamp-unit and clock-skew facts for one vehicles response."""
    rs = (parsed or {}).get("resultSet", {})
    q = jv(rs, "queryTime")
    local_at_finish = result.get("finished_at_utc_ms")
    dh = result.get("date_header_ms")
    rec = {
        "queryTime": q,
        "queryTime_iso": utc_iso_z(q),
        "queryTime_magnitude_is_ms": bool(q and q > 10**12),
        "local_minus_queryTime_ms": (local_at_finish - q) if (local_at_finish and q) else None,
        "date_header_minus_queryTime_ms": (dh - q) if (dh and q) else None,
    }
    vehicles = rs.get("vehicle", []) if isinstance(rs, dict) else []
    if isinstance(vehicles, dict):
        vehicles = [vehicles]
    t_vals = [jv(v, "time") for v in vehicles if jv(v, "time")]
    e_vals = [jv(v, "expires") for v in vehicles if jv(v, "expires")]
    rec["time_values_all_ms_magnitude"] = bool(t_vals) and all(t > 10**12 for t in t_vals)
    rec["expires_values_all_ms_magnitude"] = bool(e_vals) and all(e > 10**12 for e in e_vals)
    rec["time_min"] = min(t_vals) if t_vals else None
    rec["time_max"] = max(t_vals) if t_vals else None
    return rec


def cmd_parta(_args):
    budget = load_campaign_budget()
    out(f"campaign {CAMPAIGN_ID}; budget state loaded: "
        f"reserved={budget.reserved} cap={budget.cap} stop={budget.stop_event.is_set()}")
    rec = artifact_header(["A1", "A2", "A3", "A4"])
    rec["a1"] = {}
    rec["a2"] = {}
    rec["a3"] = {"status": "NOT_RUN"}
    rec["a4"] = {"status": "NOT_RUN"}

    # -- A1 + A2 sample 1 ---------------------------------------------------
    r1 = budgeted_fetch(budget, "A1A2", URL_VEHICLES.format(AID=APP_ID), return_body=True)
    if r1["outcome"] != "succeeded":
        rec["a1"] = {"status": "INCOMPLETE", "reason": f"first request {r1['outcome']}",
                     "detail": {k: v for k, v in r1.items() if k != "body"}}
        save_evidence("a_reverify.json", rec)
        out("A1/A2 INCOMPLETE: first vehicles request did not succeed; stopping Part A.")
        return
    p1 = json.loads(r1["body"].decode("utf-8", errors="replace"))
    s1 = summarize_vehicles_full(p1)
    rec["a1"]["request1"] = {
        "observation": {k: v for k, v in r1.items() if k not in ("body",)},
        "summary_counts": {"total": s1["vehicle_count_total"], "rail": s1["rail_count"],
                           "queryTime": s1["queryTime"]},
        "keys_first_vehicle": s1["keys_first_vehicle"],
        "keys_union_all_vehicles": s1["keys_union_all_vehicles"],
        "at_prefixed_keys": s1["at_prefixed_keys"],
        "key_presence_counts": s1["key_presence_counts"],
        "route_counts": s1["route_counts"],
    }
    m0_keys = set(M0_VEHICLE_KEYS)
    s8_keys = set(s1["keys_union_all_vehicles"])
    rec["a1"]["comparison_vs_m0"] = {
        "m0_key_count": len(m0_keys),
        "s8_key_count": len(s8_keys),
        "unchanged": sorted(m0_keys & s8_keys),
        "new_vs_m0": sorted(s8_keys - m0_keys),
        "missing_vs_m0": sorted(m0_keys - s8_keys),
        "plain_keys_no_at_prefix": not s1["at_prefixed_keys"],
    }
    rec["a2"]["sample1"] = _skew_record(r1, p1)
    rec["a2"]["sample1"]["expires_minus_time_seconds"] = {
        "min": s1["expires_minus_time_seconds_min"],
        "median": s1["expires_minus_time_seconds_median"],
        "max": s1["expires_minus_time_seconds_max"]}
    out(f"A1 req1: total={s1['vehicle_count_total']} rail={s1['rail_count']} "
        f"keys={len(s8_keys)} at_prefixed={sorted(s1['at_prefixed_keys'])}")

    # Select the 3 rail vehicles with the earliest expires for A3.
    candidates = sorted(
        [v for v in s1["rail_vehicles"] if v["expires"]],
        key=lambda v: v["expires"])[:3]
    selected_records = [
        {"vehicleID": v["vehicleID"], "expires": v["expires"],
         "expires_iso": utc_iso_z(v["expires"]), "time": v["time"],
         "routeNumber": v["routeNumber"]} for v in candidates]

    # -- A1/A2 request 2: skew re-check ~30 s later --------------------------
    time.sleep(30)
    r2 = budgeted_fetch(budget, "A1A2", URL_VEHICLES.format(AID=APP_ID), return_body=True)
    if r2["outcome"] == "succeeded":
        p2 = json.loads(r2["body"].decode("utf-8", errors="replace"))
        s2 = summarize_vehicles_full(p2)
        rec["a2"]["sample2"] = _skew_record(r2, p2)
        rec["a2"]["sample2"]["expires_minus_time_seconds"] = {
            "min": s2["expires_minus_time_seconds_min"],
            "median": s2["expires_minus_time_seconds_median"],
            "max": s2["expires_minus_time_seconds_max"]}
        out(f"A1/A2 req2 (skew recheck): total={s2['vehicle_count_total']} rail={s2['rail_count']}")
    else:
        rec["a2"]["sample2"] = {"status": "INCOMPLETE",
                                "detail": {k: v for k, v in r2.items() if k != "body"}}

    # -- A3: expires study around the earliest expirations -------------------
    a3 = {"plan": "3 earliest-expiring rail vehicles; default poll pre-expiry, "
                  "default + showStale around each expiry",
          "polls": [], "status": "INCOMPLETE",
          "selected_vehicles_at_a1": selected_records}
    rec["a3"] = a3
    try:
        targets = {v["vehicleID"]: dict(v) for v in candidates}
        retargets = 0
        min_exp = min(v["expires"] for v in targets.values())
        # Pre-expiry poll: wait until min_expires - 45 s.
        while True:
            wait_s = (min_exp - 45000 - now_ms()) / 1000.0
            if wait_s <= 0:
                break
            out(f"A3: waiting {wait_s:.0f}s until pre-expiry poll (min expires "
                f"{utc_iso_z(min_exp)})")
            time.sleep(min(wait_s, 60.0))
        p_pre = budgeted_fetch(budget, "A3", URL_VEHICLES.format(AID=APP_ID),
                               return_body=True)
        a3["polls"].append({"purpose": "pre_expiry_default",
                            "result": {k: v for k, v in p_pre.items() if k != "body"}})
        if p_pre["outcome"] != "succeeded":
            raise RuntimeError(f"pre-expiry poll {p_pre['outcome']}")
        pp = json.loads(p_pre["body"].decode("utf-8", errors="replace"))
        ps = summarize_vehicles_full(pp)
        pre_by_id = {v["vehicleID"]: v for v in ps["rail_vehicles"]}
        a3["pre_expiry_snapshot"] = {
            "queryTime": ps["queryTime"], "queryTime_iso": utc_iso_z(ps["queryTime"]),
            "total": ps["vehicle_count_total"], "rail": ps["rail_count"],
            "expires_minus_time_seconds": {
                "min": ps["expires_minus_time_seconds_min"],
                "median": ps["expires_minus_time_seconds_median"],
                "max": ps["expires_minus_time_seconds_max"]},
            "targets_observed": {
                str(vid): {"present": vid in pre_by_id,
                           "expires": pre_by_id[vid]["expires"] if vid in pre_by_id else None,
                           "expires_iso": utc_iso_z(pre_by_id[vid]["expires"]) if vid in pre_by_id else None}
                for vid in targets}}
        # Re-target if any expires moved; extend the study if needed.
        new_exps = {vid: (pre_by_id[vid]["expires"] if vid in pre_by_id else targets[vid]["expires"])
                    for vid in targets}
        changed = {vid for vid in targets if new_exps[vid] != targets[vid]["expires"]}
        if changed:
            a3["expires_changed_pre_expiry"] = {str(v): [targets[v]["expires"], new_exps[v]] for v in changed}
            for vid in changed:
                targets[vid]["expires"] = new_exps[vid]
            retargets += 1
        min_exp = min(v["expires"] for v in targets.values())

        # Post-expiry default poll at min_exp + 4 s.
        wait_s = (min_exp + 4000 - now_ms()) / 1000.0
        if wait_s > 0:
            out(f"A3: waiting {wait_s:.0f}s until post-expiry default poll")
            time.sleep(wait_s)
        p_post = budgeted_fetch(budget, "A3", URL_VEHICLES.format(AID=APP_ID),
                                return_body=True)
        a3["polls"].append({"purpose": "post_expiry_default",
                            "result": {k: v for k, v in p_post.items() if k != "body"}})
        if p_post["outcome"] != "succeeded":
            raise RuntimeError(f"post-expiry default poll {p_post['outcome']}")
        pq = json.loads(p_post["body"].decode("utf-8", errors="replace"))
        pqs = summarize_vehicles_full(pq)
        post_ids = set(vehicle_ids(pq))
        a3["post_expiry_default"] = {
            "queryTime": pqs["queryTime"], "queryTime_iso": utc_iso_z(pqs["queryTime"]),
            "total": pqs["vehicle_count_total"], "rail": pqs["rail_count"],
            "targets_present": {str(vid): vid in post_ids for vid in targets},
            "targets_expired_by_ms_at_queryTime": {
                str(vid): (pqs["queryTime"] or 0) - targets[vid]["expires"] for vid in targets},
        }

        # showStale poll ~3 s later.
        time.sleep(3.0)
        p_stale = budgeted_fetch(budget, "A3", URL_VEHICLES_STALE.format(AID=APP_ID),
                                 return_body=True)
        a3["polls"].append({"purpose": "post_expiry_showStale",
                            "result": {k: v for k, v in p_stale.items() if k != "body"}})
        if p_stale["outcome"] != "succeeded":
            raise RuntimeError(f"showStale poll {p_stale['outcome']}")
        pst = json.loads(p_stale["body"].decode("utf-8", errors="replace"))
        psts = summarize_vehicles_full(pst)
        stale_by_id = {v["vehicleID"]: v for v in psts["rail_vehicles"]}
        a3["post_expiry_showStale"] = {
            "queryTime": psts["queryTime"], "queryTime_iso": utc_iso_z(psts["queryTime"]),
            "total": psts["vehicle_count_total"], "rail": psts["rail_count"],
            "targets_present": {str(vid): vid in set(vehicle_ids(pst)) for vid in targets},
            "target_details_when_present": {
                str(vid): {"expires": stale_by_id[vid]["expires"],
                           "expires_iso": utc_iso_z(stale_by_id[vid]["expires"]),
                           "time": stale_by_id[vid]["time"],
                           "expired_by_ms_at_queryTime": (psts["queryTime"] or 0) - stale_by_id[vid]["expires"]}
                for vid in targets if vid in stale_by_id},
        }

        # Vehicles among the targets that had NOT expired at the shared poll:
        # one targeted default poll each around their own expiry (budget-capped).
        not_yet = [vid for vid in targets
                   if (pqs["queryTime"] or 0) < targets[vid]["expires"]]
        extra = []
        for vid in not_yet:
            if budget.base_used >= budget.base_max or budget.stop_event.is_set():
                break
            exp = targets[vid]["expires"]
            wait_s = (exp + 4000 - now_ms()) / 1000.0
            if wait_s > 0:
                time.sleep(wait_s)
            px = budgeted_fetch(budget, "A3", URL_VEHICLES.format(AID=APP_ID),
                                return_body=True)
            entry = {"purpose": f"post_expiry_default_vehicle_{vid}",
                     "result": {k: v for k, v in px.items() if k != "body"}}
            if px["outcome"] == "succeeded":
                pxj = json.loads(px["body"].decode("utf-8", errors="replace"))
                pxs = summarize_vehicles_full(pxj)
                entry["queryTime"] = pxs["queryTime"]
                entry["vehicle_present"] = vid in set(vehicle_ids(pxj))
                entry["expired_by_ms_at_queryTime"] = (pxs["queryTime"] or 0) - exp
            extra.append(entry)
            a3["polls"].append(entry)
        a3["targeted_extra_polls"] = extra

        # Verdicts.
        expired_at_shared = [vid for vid in targets
                             if (pqs["queryTime"] or 0) >= targets[vid]["expires"]]
        s1_rail_expires = {v["vehicleID"]: v["expires"] for v in s1["rail_vehicles"]}
        a3["verdicts"] = {
            "default_omits_expired": all(vid not in post_ids for vid in expired_at_shared) if expired_at_shared else None,
            "showStale_includes_expired": any(vid in set(vehicle_ids(pst)) for vid in expired_at_shared) if expired_at_shared else None,
            "expires_non_extension_pre_expiry": {
                str(vid): pre_by_id[vid]["expires"] == s1_rail_expires.get(vid)
                for vid in targets if vid in pre_by_id
            },
            "retargets": retargets,
        }
        a3["status"] = "COMPLETE"
    except Exception as exc:  # noqa: BLE001
        a3["status"] = "INCOMPLETE"
        a3["error"] = redact(repr(exc))
    rec["a3"] = a3

    # -- A4: since= probe -----------------------------------------------------
    a4 = {"status": "NOT_RUN"}
    rec["a4"] = a4
    try:
        rb = budgeted_fetch(budget, "A4", URL_VEHICLES.format(AID=APP_ID), return_body=True)
        if rb["outcome"] != "succeeded":
            raise RuntimeError(f"base snapshot {rb['outcome']}")
        pb = json.loads(rb["body"].decode("utf-8", errors="replace"))
        psb = summarize_vehicles_full(pb)
        q1 = psb["queryTime"]
        a4["base"] = {"queryTime": q1, "queryTime_iso": utc_iso_z(q1),
                      "total": psb["vehicle_count_total"], "rail": psb["rail_count"]}
        time.sleep(2.0)
        fu = []
        since_val = q1
        for i in range(2):
            ru = budgeted_fetch(budget, "A4",
                                URL_VEHICLES.format(AID=APP_ID) + f"&since={since_val}",
                                return_body=True)
            if ru["outcome"] != "succeeded":
                raise RuntimeError(f"follow-up {i + 1} {ru['outcome']}")
            pu = json.loads(ru["body"].decode("utf-8", errors="replace"))
            rsu = pu.get("resultSet", {})
            vs = rsu.get("vehicle", [])
            if isinstance(vs, dict):
                vs = [vs]
            qu = jv(rsu, "queryTime")
            t_gt = sum(1 for v in vs if (jv(v, "time") or 0) > since_val)
            t_le = sum(1 for v in vs if (jv(v, "time") or 0) <= since_val)
            fu.append({"since": since_val,
                       "queryTime": qu, "queryTime_iso": utc_iso_z(qu),
                       "returned": len(vs),
                       "time_gt_since": t_gt, "time_le_since": t_le,
                       "sample_ids": [jv(v, "vehicleID") for v in vs[:8]]})
            since_val = qu
            time.sleep(2.0)
        a4["follow_ups"] = fu
        a4["status"] = "COMPLETE"
    except Exception as exc:  # noqa: BLE001
        a4["status"] = "INCOMPLETE"
        a4["error"] = redact(repr(exc))
    rec["a4"] = a4

    rec["budget_snapshot_at_end"] = budget.snapshot()["counters"]
    save_evidence("a_reverify.json", rec)
    out(f"Part A done: A1/A2 collected; A3 {a3['status']}; A4 {a4['status']}; "
        f"budget reserved={budget.reserved}")


# ---------------------------------------------------------------------------
# Coordinated 10-minute observation window (B1-B5 captures)
# ---------------------------------------------------------------------------

WINDOW_DURATION_S = 600.0

def _scheduled_loop(count: int, interval_s: float, fn, feed_name: str,
                    budget: RequestBudget, tag: str) -> list[dict]:
    """Monotonic-clock polling loop for one feed inside the window.

    Runs sequentially in one thread, so polls for a feed can never overlap;
    if a poll overruns its interval the next poll starts immediately and the
    lag is recorded explicitly.
    """
    start = time.monotonic()
    records = []
    for i in range(count):
        target = start + i * interval_s
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        actual_offset = time.monotonic() - start
        rec = {"poll_index": i,
               "scheduled_offset_s": round(i * interval_s, 3),
               "actual_offset_s": round(actual_offset, 3),
               "started_late_s": round(max(0.0, actual_offset - i * interval_s), 3)}
        r = fn()
        rec.update(r)
        records.append(rec)
        if r.get("outcome") in ("refused", "aborted_pre_transmission"):
            rec["window_stopped"] = True
            out(f"window: {feed_name} stopped at poll {i} ({r.get('outcome')})")
            break
        if r.get("item_incomplete"):
            rec["item_incomplete"] = True
    skipped = count - len(records)
    if skipped:
        for j in range(len(records), count):
            records.append({"poll_index": j, "skipped": True,
                            "reason": "window stopped early"})
    return records


def cmd_window(_args):
    budget = load_campaign_budget()
    out(f"campaign {CAMPAIGN_ID}; budget reserved={budget.reserved}; "
        f"starting coordinated 10-minute window (75 requests planned)")
    rec = artifact_header(["B1", "B2", "B3", "B4", "B5"])
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

    # -- vehicles thread -----------------------------------------------------
    veh_state: dict = {"prev": None, "records": [], "id_lists": [],
                       "first_compact": None, "last_compact": None,
                       "stopped": None}

    def poll_vehicles() -> dict:
        r = budgeted_fetch(budget, "B5-vehicles", URL_VEHICLES.format(AID=APP_ID),
                           return_body=True)
        entry = {k: v for k, v in r.items() if k != "body"}
        if r["outcome"] != "succeeded":
            return entry
        p = json.loads(r["body"].decode("utf-8", errors="replace"))
        s = summarize_vehicles_full(p)
        ids = vehicle_ids(p)
        cur = {jv(v, "vehicleID"): vehicle_change_key(v) for v in _iter_raw_vehicles(p)}
        changed = new = gone = 0
        if veh_state["prev"] is not None:
            prev = veh_state["prev"]
            changed = sum(1 for k, sig in cur.items() if k in prev and prev[k] != sig)
            new = sum(1 for k in cur if k not in prev)
            gone = sum(1 for k in prev if k not in cur)
        veh_state["prev"] = cur
        veh_state["id_lists"].append(ids)
        if veh_state["first_compact"] is None:
            veh_state["first_compact"] = s["rail_vehicles"][:50]
        veh_state["last_compact"] = s["rail_vehicles"][:50]
        entry.update({
            "queryTime": s["queryTime"], "queryTime_iso": utc_iso_z(s["queryTime"]),
            "total": s["vehicle_count_total"], "rail": s["rail_count"],
            "changed_since_prev": changed, "new_since_prev": new,
            "gone_since_prev": gone,
            "queryTime_lag_ms": (entry.get("finished_at_utc_ms") - s["queryTime"])
            if s["queryTime"] else None,
            "expires_minus_time_s_min": s["expires_minus_time_seconds_min"],
            "expires_minus_time_s_median": s["expires_minus_time_seconds_median"],
            "expires_minus_time_s_max": s["expires_minus_time_seconds_max"],
        })
        return entry

    def _iter_raw_vehicles(p: dict):
        vs = (p.get("resultSet") or {}).get("vehicle", [])
        if isinstance(vs, dict):
            vs = [vs]
        return vs

    # -- TripUpdates thread ----------------------------------------------------
    tu_state: dict = {"prev_sigs": None, "records": [], "bins": [], "stopped": None}

    def poll_tripupdates() -> dict:
        r = budgeted_fetch(budget, "B5-tripupdates", URL_TRIPUPDATES.format(AID=APP_ID),
                           return_body=True)
        entry = {k: v for k, v in r.items() if k != "body"}
        if r["outcome"] != "succeeded":
            return entry
        body = r["body"]
        idx = len(tu_state["bins"]) + 1
        bin_path = EVIDENCE_DIR / f"tripupdate_capture_{idx:02d}.bin"
        bin_path.write_bytes(body)
        tu_state["bins"].append({"index": idx, "file": bin_path.name,
                                 "bytes": len(body),
                                 "captured_at_utc_ms": entry.get("finished_at_utc_ms"),
                                 "captured_at_utc": utc_iso_z(entry.get("finished_at_utc_ms"))})
        try:
            header, entities = decode_rt_feed(body)
        except Exception as exc:  # noqa: BLE001
            entry["decode_error"] = redact(repr(exc))
            return entry
        tus = [e["trip_update"] for e in entities if e.get("trip_update")]
        sigs = {}
        for tu in tus:
            tid = (tu["trip"] or {}).get("trip_id")
            if tid is not None:
                sigs[tid] = stu_signature(tu)
        changed = 0
        if tu_state["prev_sigs"] is not None:
            prev = tu_state["prev_sigs"]
            changed = sum(1 for k, sig in sigs.items() if k in prev and prev[k] != sig)
        tu_state["prev_sigs"] = sigs
        entry.update({
            "header": header,
            "entities_total": len(entities),
            "trip_update_entities": len(tus),
            "distinct_trip_ids": len(sigs),
            "changed_trip_ids_since_prev": changed,
            "stop_time_updates_total": sum(len(t["stop_time_updates"]) for t in tus),
            "vehicle_id_present": sum(1 for t in tus if (t["vehicle"] or {}).get("id")),
            "trip_id_present": sum(1 for t in tus if (t["trip"] or {}).get("trip_id")),
            "trip_route_id_present": sum(1 for t in tus if (t["trip"] or {}).get("route_id")),
            "entity_summaries": [
                {"id": e.get("id"),
                 "trip_id": (t["trip"] or {}).get("trip_id"),
                 "vehicle_id": (t["vehicle"] or {}).get("id"),
                 "stu_count": len(t["stop_time_updates"])}
                for e, t in [(e, e["trip_update"]) for e in entities if e.get("trip_update")]],
        })
        return entry

    # -- alerts thread -----------------------------------------------------------
    al_state: dict = {"records": [], "stopped": None}

    def poll_alerts() -> dict:
        r = budgeted_fetch(budget, "B5-alerts", URL_ALERTS.format(AID=APP_ID),
                           return_body=True)
        entry = {k: v for k, v in r.items() if k != "body"}
        if r["outcome"] != "succeeded":
            return entry
        p = json.loads(r["body"].decode("utf-8", errors="replace"))
        s = summarize_alerts(p)
        sig = json.dumps(s["alerts"], sort_keys=True, default=str)
        changed_alerts = 0
        if al_state["records"]:
            prev_sig = al_state["records"][-1].get("content_signature")
            prev_map = al_state["records"][-1].get("alert_sigs", {})
            cur_map = {a["id"]: json.dumps(a, sort_keys=True, default=str) for a in s["alerts"]}
            changed_alerts = sum(1 for k, v in cur_map.items()
                                 if k in prev_map and prev_map[k] != v)
            entry["alert_sigs"] = cur_map
        else:
            entry["alert_sigs"] = {a["id"]: json.dumps(a, sort_keys=True, default=str)
                                   for a in s["alerts"]}
        entry["content_signature"] = sig
        entry.update({"queryTime": s["queryTime"],
                      "alert_count": s["alert_count"],
                      "changed_alerts_since_prev": changed_alerts,
                      "alerts": s["alerts"]})
        return entry

    # -- run the three threads concurrently ------------------------------------
    t0 = time.monotonic()
    th_veh = threading.Thread(
        target=lambda: veh_state.update(
            records=_scheduled_loop(60, 10.0, poll_vehicles, "vehicles", budget, "B5-vehicles")))
    th_tu = threading.Thread(
        target=lambda: tu_state.update(
            records=_scheduled_loop(10, 60.0, poll_tripupdates, "tripupdates", budget, "B5-tripupdates")))
    th_al = threading.Thread(
        target=lambda: al_state.update(
            records=_scheduled_loop(5, 120.0, poll_alerts, "alerts", budget, "B5-alerts")))
    th_veh.start(); th_tu.start(); th_al.start()
    th_veh.join(); th_tu.join(); th_al.join()
    elapsed = time.monotonic() - t0
    out(f"window finished in {elapsed:.0f}s")

    # -- assemble evidence --------------------------------------------------------
    def feed_summary(records: list[dict], name: str) -> dict:
        ok = [r for r in records if r.get("outcome") == "succeeded"]
        clean = [{k: v for k, v in r.items() if k not in ("alert_sigs",)}
                 for r in records]
        return {
            "planned_polls": len(records),
            "succeeded": len(ok),
            "failed": len([r for r in records if r.get("outcome") == "failed"]),
            "refused_or_aborted": len([r for r in records
                                       if r.get("outcome") in ("refused", "aborted_pre_transmission")]),
            "status": "COMPLETE" if len(ok) == len(records) else "INCOMPLETE",
            "polls": clean,
        }

    rec["b5"] = {
        "window_duration_s_planned": WINDOW_DURATION_S,
        "window_duration_s_actual": round(elapsed, 1),
        "vehicles": feed_summary(veh_state["records"], "vehicles"),
        "tripupdates": feed_summary(tu_state["records"], "tripupdates"),
        "alerts": feed_summary(al_state["records"], "alerts"),
        "vehicle_first_window_rail_sample": veh_state["first_compact"],
        "vehicle_last_window_rail_sample": veh_state["last_compact"],
        "vehicle_id_lists_per_poll": veh_state["id_lists"],
    }
    rec["b2_capture_manifest"] = tu_state["bins"]
    rec["alert_captures"] = [
        {k: v for k, v in r.items() if k != "alert_sigs"}
        for r in al_state["records"] if r.get("alerts") is not None]
    save_evidence("b5_cadence.json", rec)
    save_evidence("window_alert_captures.json",
                  artifact_header(["B3", "B4"]) | {"captures": rec["alert_captures"]})
    save_evidence("b2_tripupdate_manifest.json",
                  artifact_header(["B2"]) | {"captures": tu_state["bins"]})
    out(f"window: vehicles ok={len([r for r in veh_state['records'] if r.get('outcome') == 'succeeded'])}/60, "
        f"tripupdates ok={len([r for r in tu_state['records'] if r.get('outcome') == 'succeeded'])}/10, "
        f"alerts ok={len([r for r in al_state['records'] if r.get('outcome') == 'succeeded'])}/5; "
        f"budget reserved={budget.reserved}")


# ---------------------------------------------------------------------------
# Part B live probes: B6 showStale pairs, B8 HTTP mechanics
# ---------------------------------------------------------------------------

def cmd_partb(_args):
    budget = load_campaign_budget()
    out(f"campaign {CAMPAIGN_ID}; budget reserved={budget.reserved}")
    rec = artifact_header(["B6", "B8"])

    # -- B6: five paired polls (default vs showStale=true) --------------------
    b6 = {"pairs": [], "status": "NOT_RUN"}
    rec["b6"] = b6
    try:
        for i in range(5):
            if budget.stop_event.is_set():
                break
            rd = budgeted_fetch(budget, "B6", URL_VEHICLES.format(AID=APP_ID),
                                return_body=True)
            pair = {"pair_index": i,
                    "default": {k: v for k, v in rd.items() if k != "body"}}
            if rd["outcome"] != "succeeded":
                pair["default"]["outcome"] = rd["outcome"]
                b6["pairs"].append(pair)
                break
            pd = json.loads(rd["body"].decode("utf-8", errors="replace"))
            sd = summarize_vehicles_full(pd)
            ids_default = set(vehicle_ids(pd))
            rs = budgeted_fetch(budget, "B6", URL_VEHICLES_STALE.format(AID=APP_ID),
                                return_body=True)
            pair["showStale"] = {k: v for k, v in rs.items() if k != "body"}
            if rs["outcome"] != "succeeded":
                pair["showStale"]["outcome"] = rs["outcome"]
                b6["pairs"].append(pair)
                break
            ps = json.loads(rs["body"].decode("utf-8", errors="replace"))
            ss = summarize_vehicles_full(ps)
            ids_stale = set(vehicle_ids(ps))
            stale_only = ids_stale - ids_default
            stale_detail = []
            stale_by_id = {v["vehicleID"]: v for v in ss["rail_vehicles"]}
            all_stale_vehicles = {
                jv(v, "vehicleID"): v
                for v in ((ps.get("resultSet") or {}).get("vehicle", []))}
            for vid in sorted(stale_only)[:20]:
                v = all_stale_vehicles.get(vid) or stale_by_id.get(vid)
                if v is None:
                    continue
                exp = jv(v, "expires")
                t = jv(v, "time")
                stale_detail.append({
                    "vehicleID": vid, "type": jv(v, "type"),
                    "routeNumber": jv(v, "routeNumber"),
                    "expires": exp, "expires_iso": utc_iso_z(exp),
                    "time": t,
                    "expired_by_ms_at_queryTime": (ss["queryTime"] or 0) - exp if exp else None,
                    "time_age_ms": (ss["queryTime"] or 0) - t if t else None,
                })
            pair.update({
                "default_summary": {"queryTime": sd["queryTime"],
                                    "total": sd["vehicle_count_total"],
                                    "rail": sd["rail_count"]},
                "showStale_summary": {"queryTime": ss["queryTime"],
                                      "total": ss["vehicle_count_total"],
                                      "rail": ss["rail_count"]},
                "stale_only_count": len(stale_only),
                "stale_only_sample": stale_detail,
            })
            b6["pairs"].append(pair)
            out(f"B6 pair {i}: default total={sd['vehicle_count_total']} rail={sd['rail_count']}; "
                f"showStale total={ss['vehicle_count_total']} rail={ss['rail_count']}; "
                f"stale_only={len(stale_only)}")
            if i < 4:
                time.sleep(20.0)
        b6["status"] = "COMPLETE" if len(b6["pairs"]) == 5 and all(
            p.get("default", {}).get("outcome") == "succeeded"
            and p.get("showStale", {}).get("outcome") == "succeeded"
            for p in b6["pairs"]) else "INCOMPLETE"
    except Exception as exc:  # noqa: BLE001
        b6["status"] = "INCOMPLETE"
        b6["error"] = redact(repr(exc))
    rec["b6"] = b6

    # -- B8: HTTP mechanics (8 requests max) -----------------------------------
    b8 = {"probes": [], "status": "NOT_RUN"}
    rec["b8"] = b8
    feeds = [
        ("vehicles", URL_VEHICLES.format(AID=APP_ID),
         "https://developer.trimet.org/ws/v2/vehicles/?appID={AID}"),
        ("alerts", URL_ALERTS.format(AID=APP_ID), None),
        ("tripupdates", URL_TRIPUPDATES.format(AID=APP_ID),
         "https://developer.trimet.org/ws/V1/TripUpdate/?appID={AID}"),
    ]
    try:
        for name, url, slash_url in feeds:
            for enc in ("gzip", "identity"):
                if budget.stop_event.is_set():
                    break
                r = budgeted_fetch(budget, "B8", url, headers={"Accept-Encoding": enc})
                entry = {"feed": name, "accept_encoding": enc,
                         "outcome": r.get("outcome"), "http_status": r.get("http_status"),
                         "bytes": r.get("bytes"), "content_type": r.get("content_type"),
                         "content_encoding": r.get("content_encoding"),
                         "date_header": r.get("date_header")}
                b8["probes"].append(entry)
                out(f"B8 {name} Accept-Encoding={enc}: status={entry['http_status']} "
                    f"bytes={entry['bytes']} content-encoding={entry['content_encoding']}")
            if slash_url:
                if budget.stop_event.is_set():
                    break
                r = budgeted_fetch(budget, "B8", slash_url.format(AID=APP_ID))
                entry = {"feed": name, "variant": "trailing_slash",
                         "outcome": r.get("outcome"), "http_status": r.get("http_status"),
                         "bytes": r.get("bytes"), "content_type": r.get("content_type"),
                         "date_header": r.get("date_header")}
                b8["probes"].append(entry)
                out(f"B8 {name} trailing-slash: status={entry['http_status']} bytes={entry['bytes']}")
        b8["status"] = "COMPLETE" if not budget.stop_event.is_set() else "INCOMPLETE"
    except Exception as exc:  # noqa: BLE001
        b8["status"] = "INCOMPLETE"
        b8["error"] = redact(repr(exc))
    rec["b8"] = b8

    rec["budget_snapshot_at_end"] = budget.snapshot()["counters"]
    save_evidence("b6_showstale.json", rec)
    save_evidence("b8_http_details.json",
                  artifact_header(["B8"]) | {"probes": b8["probes"],
                                             "status": b8["status"]})
    out(f"Part B done: B6 {b6['status']}; B8 {b8['status']}; budget reserved={budget.reserved}")


# ---------------------------------------------------------------------------
# Offline analyses (no live requests): A5/A6, B1-B4
# ---------------------------------------------------------------------------

def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def cmd_analyze(_args):
    rec = artifact_header(["A5", "A6", "B1", "B2", "B3", "B4"])
    rec["requests_consumed"] = 0  # offline phase

    # ---- A5: alerts schema from the first window capture --------------------
    a5: dict = {"status": "NOT_RUN"}
    try:
        caps = _load_json(EVIDENCE_DIR / "window_alert_captures.json")["captures"]
        if not caps:
            raise RuntimeError("no alert captures found")
        first = caps[0]
        alerts = first["alerts"]
        key_union: set[str] = set()
        end_null = end_present = 0
        route_child_keys: set[str] = set()
        location_child_keys: set[str] = set()
        for a in alerts:
            key_union |= set(a.keys())
            if "end" in a:
                if a["end"] is None:
                    end_null += 1
                else:
                    end_present += 1
        for a in alerts:
            for r in a.get("routes", []):
                route_child_keys |= set(r.keys())
            for l in a.get("locations", []):
                location_child_keys |= set(l.keys())
        a5 = {
            "status": "COMPLETE",
            "source": "window alert capture 1",
            "alert_count": len(alerts),
            "field_keys_union": sorted(key_union),
            "end_null_count": end_null,
            "end_present_count": end_present,
            "route_child_keys": sorted(route_child_keys),
            "location_child_keys": sorted(location_child_keys),
            "system_wide_flag_values": sorted({str(a.get("system_wide_flag")) for a in alerts}),
        }
    except Exception as exc:  # noqa: BLE001
        a5 = {"status": "INCOMPLETE", "error": redact(repr(exc))}
    rec["a5"] = a5

    # ---- A6: TripUpdate schema from the first raw capture --------------------
    a6: dict = {"status": "NOT_RUN"}
    try:
        manifest = _load_json(EVIDENCE_DIR / "b2_tripupdate_manifest.json")["captures"]
        if not manifest:
            raise RuntimeError("no TripUpdate captures found")
        bin_path = EVIDENCE_DIR / manifest[0]["file"]
        body = bin_path.read_bytes()
        header, entities = decode_rt_feed(body)
        tus = [e["trip_update"] for e in entities if e.get("trip_update")]
        a6 = {
            "status": "COMPLETE",
            "source": manifest[0]["file"],
            "bytes": len(body),
            "header": header,
            "header_timestamp_magnitude_is_seconds": bool(
                header and header.get("timestamp") and 10**9 < header["timestamp"] < 10**10),
            "entities_total": len(entities),
            "trip_update_entities": len(tus),
            "other_entity_types": sorted({k for e in entities for k in e if k in ("vehicle_position", "alert")}),
            "trip_route_id_present_count": sum(1 for t in tus if (t["trip"] or {}).get("route_id")),
            "trip_id_present_count": sum(1 for t in tus if (t["trip"] or {}).get("trip_id")),
            "stop_time_updates_total": sum(len(t["stop_time_updates"]) for t in tus),
            "schedule_relationship_values": sorted({str((t["trip"] or {}).get("schedule_relationship_raw")) for t in tus}),
            "negative_delay_samples": [
                {"trip_id": (t["trip"] or {}).get("trip_id"),
                 "delay": t["delay"]}
                for t in tus if (t["delay"] or 0) < 0][:5],
        }
    except Exception as exc:  # noqa: BLE001
        a6 = {"status": "INCOMPLETE", "error": redact(repr(exc))}
    rec["a6"] = a6

    # ---- B1: incrementality across all captures (qualified conclusion) -------
    b1: dict = {"status": "NOT_RUN"}
    try:
        manifest = _load_json(EVIDENCE_DIR / "b2_tripupdate_manifest.json")["captures"]
        if not manifest:
            raise RuntimeError("no TripUpdate captures found")
        obs = []
        for m in manifest:
            header, _ = decode_rt_feed((EVIDENCE_DIR / m["file"]).read_bytes())
            obs.append({"capture": m["index"],
                        "captured_at_utc_ms": m["captured_at_utc_ms"],
                        "gtfs_realtime_version": header.get("gtfs_realtime_version") if header else None,
                        "incrementality_raw": header.get("incrementality_raw") if header else None,
                        "header_timestamp": header.get("timestamp") if header else None})
        counts: dict[str, int] = {}
        for o in obs:
            counts[str(o["incrementality_raw"])] = counts.get(str(o["incrementality_raw"]), 0) + 1
        duration_ms = (obs[-1]["captured_at_utc_ms"] - obs[0]["captured_at_utc_ms"]) if len(obs) >= 2 else 0
        all_full = all(o["incrementality_raw"] == 0 for o in obs) and obs
        b1 = {
            "status": "COMPLETE",
            "observations": obs,
            "incrementality_value_counts": counts,
            "incrementality_names": {"0": "FULL_DATASET", "1": "DIFFERENTIAL"},
            "sampling_duration_ms": duration_ms,
            "sampling_duration_minutes": round(duration_ms / 60000.0, 1),
            "successful_responses": len(obs),
            "qualified_conclusion": (
                "FULL_DATASET-only observed across this sample; sufficient for "
                "initial implementation PROVIDED S12 rejects any non-FULL_DATASET "
                "response whole and preserves previous state (safe degradation is "
                "mandatory in either case)." if all_full else
                "DIFFERENTIAL observed - entity-level merging required in S12."),
            "sampling_caveat": ("A finite sample cannot prove DIFFERENTIAL never "
                                "occurs; the safe-degradation path must exist "
                                "regardless of this observation."),
        }
    except Exception as exc:  # noqa: BLE001
        b1 = {"status": "INCOMPLETE", "error": redact(repr(exc))}
    rec["b1"] = b1

    # ---- B2: TripUpdate -> static GTFS matching (expanded report) -------------
    b2: dict = {"status": "NOT_RUN"}
    try:
        trips_by_id: dict[str, str] = {}
        with (M1_DATASET_DIR / "trips.txt").open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                trips_by_id[row["trip_id"]] = row.get("route_id", "")
        routes_meta: dict[str, dict] = {}
        with (M1_DATASET_DIR / "routes.txt").open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                routes_meta[row["route_id"]] = {
                    "short": row.get("route_short_name", ""),
                    "long": row.get("route_long_name", ""),
                    "type": row.get("route_type", ""),
                }
        manifest = _load_json(EVIDENCE_DIR / "b2_tripupdate_manifest.json")["captures"]
        if not manifest:
            raise RuntimeError("no TripUpdate captures found")
        cadence = _load_json(EVIDENCE_DIR / "b5_cadence.json")
        vehicle_id_union = {str(x)
                            for lst in cadence["b5"]["vehicle_id_lists_per_poll"]
                            for x in lst}
        totals = {"entities": 0, "with_usable_trip_id": 0, "invalid_or_incomplete": 0,
                  "vehicle_id_present": 0, "vehicle_id_joined": 0}
        unmatched_samples: list[str] = []
        unmatched_format: dict[str, int] = {}
        matched_route_counts: dict[str, int] = {}
        max_matched = non_max_matched = 0
        distinct_unmatched: set[str] = set()
        distinct_matched: set[str] = set()
        invalid_kinds: dict[str, int] = {}
        for m in manifest:
            header, entities = decode_rt_feed((EVIDENCE_DIR / m["file"]).read_bytes())
            for e in entities:
                totals["entities"] += 1
                tu = e.get("trip_update")
                if tu is None:
                    invalid_kinds["non_trip_update_entity"] = invalid_kinds.get("non_trip_update_entity", 0) + 1
                    totals["invalid_or_incomplete"] += 1
                    continue
                trip = tu.get("trip") or {}
                tid = trip.get("trip_id")
                stus = tu.get("stop_time_updates") or []
                if not tid:
                    invalid_kinds["missing_trip_id"] = invalid_kinds.get("missing_trip_id", 0) + 1
                    totals["invalid_or_incomplete"] += 1
                    continue
                if not stus:
                    invalid_kinds["no_stop_time_updates"] = invalid_kinds.get("no_stop_time_updates", 0) + 1
                    totals["invalid_or_incomplete"] += 1
                totals["with_usable_trip_id"] += 1
                vid = (tu.get("vehicle") or {}).get("id")
                if vid:
                    totals["vehicle_id_present"] += 1
                    if str(vid) in vehicle_id_union:
                        totals["vehicle_id_joined"] += 1
                rid = trips_by_id.get(str(tid))
                if rid is None:
                    distinct_unmatched.add(str(tid))
                    if len(unmatched_samples) < 25:
                        unmatched_samples.append(str(tid))
                    kind = "numeric" if str(tid).isdigit() else "non_numeric"
                    unmatched_format[kind] = unmatched_format.get(kind, 0) + 1
                else:
                    distinct_matched.add(str(tid))
                    matched_route_counts[rid] = matched_route_counts.get(rid, 0) + 1
                    if rid in MAX_ROUTE_IDS:
                        max_matched += 1
                    else:
                        non_max_matched += 1
        b2 = {
            "status": "COMPLETE",
            "captures_analyzed": len(manifest),
            "m1_dataset": str(M1_DATASET_DIR.relative_to(REPO)),
            "m1_trips_loaded": len(trips_by_id),
            "totals": totals,
            "distinct_trip_ids": {
                "matched": len(distinct_matched),
                "unmatched": len(distinct_unmatched),
            },
            "matched_to_MAX": max_matched,
            "matched_to_non_MAX": non_max_matched,
            "matched_route_breakdown": dict(sorted(matched_route_counts.items(),
                                                   key=lambda kv: -kv[1])[:20]),
            "unmatched_samples": unmatched_samples,
            "unmatched_format": unmatched_format,
            "invalid_kinds": invalid_kinds,
            "vehicle_id_join_note": "vehicle.id (TripUpdate) vs vehicles-feed vehicleID, string-compared",
        }
    except Exception as exc:  # noqa: BLE001
        b2 = {"status": "INCOMPLETE", "error": redact(repr(exc))}
    rec["b2"] = b2
    save_evidence("b2_match_report.json", artifact_header(["B2"]) | {"report": b2})

    # ---- B3: alert id stability ----------------------------------------------
    b3: dict = {"status": "NOT_RUN"}
    try:
        caps = _load_json(EVIDENCE_DIR / "window_alert_captures.json")["captures"]
        id_sets = [{a["id"] for a in c["alerts"]} for c in caps]
        all_ids: set = set().union(*id_sets) if id_sets else set()
        recurring = set.intersection(*id_sets) if id_sets else set()
        # changed content among recurring ids
        changed = set()
        for i in range(1, len(caps)):
            prev = {a["id"]: a for a in caps[i - 1]["alerts"]}
            cur = {a["id"]: a for a in caps[i]["alerts"]}
            for k in set(prev) & set(cur):
                if prev[k] != cur[k]:
                    changed.add(k)
        b3 = {
            "status": "COMPLETE" if len(caps) == 5 else "INCOMPLETE (fewer than 5 captures)",
            "capture_count": len(caps),
            "id_counts_per_capture": [len(s) for s in id_sets],
            "distinct_ids_overall": len(all_ids),
            "recurring_ids": len(recurring),
            "recurring_with_changed_content": sorted(changed),
            "ids_appearing_disappearing": sorted(
                all_ids - recurring)[:20],
        }
    except Exception as exc:  # noqa: BLE001
        b3 = {"status": "INCOMPLETE", "error": redact(repr(exc))}
    rec["b3"] = b3

    # ---- B4: alert route namespace ---------------------------------------------
    b4: dict = {"status": "NOT_RUN"}
    try:
        caps = _load_json(EVIDENCE_DIR / "window_alert_captures.json")["captures"]
        if not caps:
            raise RuntimeError("no alert captures found")
        gtfs_route_ids: set[str] = set()
        gtfs_short_names: set[str] = set()
        with (M1_DATASET_DIR / "routes.txt").open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                gtfs_route_ids.add(row["route_id"])
                if row.get("route_short_name"):
                    gtfs_short_names.add(row["route_short_name"])
        route_child_ids: set = set()
        location_only = []
        system_wide = []
        route_scoped = 0
        for a in caps[0]["alerts"]:
            ids = [str(r.get("id")) for r in a.get("routes", [])]
            route_child_ids |= set(ids)
            if a.get("system_wide_flag"):
                system_wide.append(a["id"])
            elif ids:
                route_scoped += 1
            elif a.get("locations"):
                location_only.append({"id": a["id"],
                                      "location_count": len(a["locations"]),
                                      "header": a.get("header_text")})
        in_gtfs_id = sorted(i for i in route_child_ids if i in gtfs_route_ids)
        in_short = sorted(i for i in route_child_ids if i in gtfs_short_names)
        not_matched = sorted(i for i in route_child_ids
                             if i not in gtfs_route_ids and i not in gtfs_short_names)
        max_ids = sorted(i for i in route_child_ids if i in MAX_ROUTE_IDS)
        b4 = {
            "status": "COMPLETE",
            "distinct_route_child_ids": sorted(route_child_ids),
            "route_child_ids_in_gtfs_route_id": in_gtfs_id,
            "route_child_ids_in_gtfs_short_name": in_short,
            "route_child_ids_unmatched": not_matched,
            "route_child_ids_that_are_MAX": max_ids,
            "route_scoped_alerts": route_scoped,
            "location_only_alerts": location_only,
            "system_wide_alerts": system_wide,
            "proposed_MAX_classification_rule": (
                "An alert is MAX-relevant if system_wide_flag is true OR any "
                "route child id is in {90,100,190,200,290}. Location-only alerts "
                "are inventoried as diagnostics and require desc inspection to "
                "classify; they are NOT classified MAX by route children alone."),
        }
    except Exception as exc:  # noqa: BLE001
        b4 = {"status": "INCOMPLETE", "error": redact(repr(exc))}
    rec["b4"] = b4

    # ---- update a_reverify.json with A5/A6 --------------------------------------
    a_path = EVIDENCE_DIR / "a_reverify.json"
    if a_path.exists():
        a_rec = _load_json(a_path)
        a_rec["a5"] = a5
        a_rec["a6"] = a6
        save_evidence("a_reverify.json", a_rec)
    save_evidence("b1_incrementality.json", artifact_header(["B1"]) | {"b1": b1})
    save_evidence("b3_alert_stability.json", artifact_header(["B3"]) | {"b3": b3})
    save_evidence("b4_alert_routes.json", artifact_header(["B4"]) | {"b4": b4})
    out(f"analyze done: A5 {a5['status']}, A6 {a6['status']}, B1 {b1['status']}, "
        f"B2 {b2['status']}, B3 {b3['status']}, B4 {b4['status']}")


# ---------------------------------------------------------------------------
# B7: gtfs-realtime-bindings evaluation in an isolated venv (no adoption)
# ---------------------------------------------------------------------------

B7_VENV_PARENT = Path(tempfile.gettempdir()) / "opencode"
B7_VENV_DIR = B7_VENV_PARENT / "s8_b7_venv"
B7_PIP_CACHE = B7_VENV_PARENT / "s8_b7_pip_cache"

B7_DECODE_SCRIPT = r'''
import json, sys
sys.path.insert(0, r"{S8_DIR}")
import probe_s8

bin_path = sys.argv[1]
body = open(bin_path, "rb").read()

# --- stdlib wire-parser decode (reference) ---
header, entities = probe_s8.decode_rt_feed(body)
tus_std = [e["trip_update"] for e in entities if e.get("trip_update")]
std_delays = []
for t in tus_std:
    for s in t["stop_time_updates"]:
        for ev in (s.get("arrival"), s.get("departure")):
            if ev and ev.get("delay") is not None and ev["delay"] < 0:
                std_delays.append(ev["delay"])
std_neg_count = len(std_delays)

# --- gtfs-realtime-bindings decode ---
from google.transit import gtfs_realtime_pb2
from google.protobuf import __version__ as protobuf_version
import importlib.metadata

msg = gtfs_realtime_pb2.FeedMessage()
msg.ParseFromString(body)
lib_header = {
    "gtfs_realtime_version": msg.header.gtfs_realtime_version,
    "incrementality": gtfs_realtime_pb2.FeedHeader.Incrementality.Name(msg.header.incrementality),
    "timestamp": msg.header.timestamp,
}
lib_tus = [e.trip_update for e in msg.entity if e.HasField("trip_update")]
lib_neg = []
for tu in lib_tus:
    for stu in tu.stop_time_update:
        if stu.HasField("arrival") and stu.arrival.delay < 0:
            lib_neg.append(stu.arrival.delay)
        if stu.HasField("departure") and stu.departure.delay < 0:
            lib_neg.append(stu.departure.delay)

# --- cross-check: per-entity delay agreement between the two decoders ---
std_by_trip = {}
for t in tus_std:
    tid = (t["trip"] or {}).get("trip_id")
    if tid:
        std_by_trip[str(tid)] = [
            ((s.get("arrival") or {}).get("delay"),
             (s.get("departure") or {}).get("delay"))
            for s in t["stop_time_updates"]]
agree = 0
disagree = 0
checked = 0
for tu in lib_tus[:200]:
    tid = tu.trip.trip_id
    ref = std_by_trip.get(tid)
    if ref is None:
        continue
    lib_pairs = []
    for stu in tu.stop_time_update:
        lib_pairs.append((stu.arrival.delay if stu.HasField("arrival") else None,
                          stu.departure.delay if stu.HasField("departure") else None))
    for a, b in zip(ref, lib_pairs):
        checked += 1
        if a == b:
            agree += 1
        else:
            disagree += 1

print(json.dumps({
    "library": "gtfs-realtime-bindings",
    "library_version": importlib.metadata.version("gtfs-realtime-bindings"),
    "protobuf_version": protobuf_version,
    "decoded_ok": True,
    "header": lib_header,
    "entities": len(msg.entity),
    "trip_updates": len(lib_tus),
    "stdlib_negative_delays": std_neg_count,
    "library_negative_delays": len(lib_neg),
    "negative_delay_sample_stdlib": std_delays[:5],
    "negative_delay_sample_library": lib_neg[:5],
    "crosscheck_pairs_checked": checked,
    "crosscheck_agree": agree,
    "crosscheck_disagree": disagree,
    "stdlib_header_for_comparison": header,
}))
'''.replace("{S8_DIR}", str(Path(__file__).resolve().parent))


def cmd_b7(_args):
    rec = artifact_header(["B7"])
    rec["requests_consumed"] = 0  # decodes already-captured protobuf only
    rec["venv_dir"] = str(B7_VENV_DIR)
    rec["venv_outside_repo"] = not str(B7_VENV_DIR).startswith(str(REPO))
    rec["pip_cache_dir"] = str(B7_PIP_CACHE)
    rec["status"] = "NOT_RUN"
    try:
        manifest = _load_json(EVIDENCE_DIR / "b2_tripupdate_manifest.json")["captures"]
        if not manifest:
            raise RuntimeError("no captured FeedMessage found; run the window phase first")
        bin_path = EVIDENCE_DIR / manifest[0]["file"]

        if not (B7_VENV_DIR / "Scripts" / "python.exe").exists():
            out("B7: creating isolated venv (outside the repository)...")
            r = subprocess.run([sys.executable, "-m", "venv", str(B7_VENV_DIR)],
                               capture_output=True, text=True, timeout=300)
            if r.returncode != 0:
                raise RuntimeError(f"venv creation failed: {r.stderr[-500:]}")
        venv_py = B7_VENV_DIR / "Scripts" / "python.exe"
        out("B7: installing gtfs-realtime-bindings into the isolated venv...")
        r = subprocess.run(
            [str(venv_py), "-m", "pip", "install", "--cache-dir", str(B7_PIP_CACHE),
             "gtfs-realtime-bindings"],
            capture_output=True, text=True, timeout=600,
            env={**os.environ, "PIP_CACHE_DIR": str(B7_PIP_CACHE)})
        rec["pip_returncode"] = r.returncode
        rec["pip_output_tail"] = redact((r.stdout or "")[-800:])
        if r.returncode != 0:
            raise RuntimeError(f"pip install failed: {(r.stderr or '')[-500:]}")

        script_path = B7_VENV_PARENT / "s8_b7_decode.py"
        script_path.write_text(B7_DECODE_SCRIPT, encoding="utf-8")
        out("B7: decoding the captured FeedMessage with the library (offline)...")
        r = subprocess.run([str(venv_py), str(script_path), str(bin_path)],
                           capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            raise RuntimeError(f"decode script failed: {(r.stderr or '')[-800:]}")
        decoded = json.loads(r.stdout)
        rec["decoded"] = decoded
        ok = (decoded.get("decoded_ok")
              and decoded.get("crosscheck_disagree") == 0
              and decoded.get("crosscheck_pairs_checked", 0) > 0)
        rec["verdict"] = ("SUITABLE" if ok else "NOT_SUITABLE")
        rec["verdict_basis"] = {
            "decoded_full_message": decoded.get("decoded_ok"),
            "crosscheck_agreement": f"{decoded.get('crosscheck_agree')}/{decoded.get('crosscheck_pairs_checked')}",
            "negative_delay_handling_verified": (
                decoded.get("stdlib_negative_delays", 0) > 0
                and decoded.get("stdlib_negative_delays") == decoded.get("library_negative_delays")),
        }
        rec["adoption_note"] = ("Suitability verdict only. Adoption into the project "
                                "dependency manifest is a separate, later, explicitly "
                                "authorized decision. The project venv and manifest "
                                "are untouched by this phase.")
        rec["status"] = "COMPLETE"
    except Exception as exc:  # noqa: BLE001
        rec["status"] = "INCOMPLETE"
        rec["error"] = redact(repr(exc))
    save_evidence("b7_library_eval.json", rec)
    out(f"B7 {rec['status']}: verdict={rec.get('verdict')} "
        f"(library {rec.get('decoded', {}).get('library_version')}, "
        f"protobuf {rec.get('decoded', {}).get('protobuf_version')})")


# ---------------------------------------------------------------------------
# Report: budget reconciliation + AppID scan
# ---------------------------------------------------------------------------

def cmd_report(_args):
    budget = load_campaign_budget()
    snap = budget.snapshot()
    rec = artifact_header(["BUDGET"], requests_consumed=budget.reserved)
    rec["budget"] = snap
    rec["reconciliation"] = {
        "approved_table": {**TAG_MAX, "retry_pool": RETRY_POOL, "reserve": RESERVE_MAX,
                           "hard_cap": CAP},
        "actual_per_tag": snap["per_tag"],
        "allocations": snap["allocations"],
        "counters": snap["counters"],
        "total_reserved": budget.reserved,
        "within_cap": budget.reserved <= CAP,
        "reserve_draws": snap["reserve_draws"],
        "stop_events": snap["stop_log"],
        "refusals_summary": len(snap["refusals"]),
    }
    save_evidence("request_budget.json", rec)
    out(f"budget: reserved={budget.reserved}/{CAP} "
        f"(base={budget.base_used}/{budget.base_max}, "
        f"retry={budget.retry_used}/{budget.retry_max}, "
        f"reserve={budget.reserve_used}/{budget.reserve_max}) "
        f"transmitted={budget.transmitted} succeeded={budget.succeeded} "
        f"failed={budget.failed} retried={budget.retried} "
        f"aborted_pre_transmission={budget.aborted_pre_transmission}")

    # AppID scan over every file under tools/s8_contract/ (never prints the value)
    if not init_appid():
        out("AppID scan: SKIPPED (no AppID loaded)")
        return
    hits = {}
    for p in sorted(Path(__file__).resolve().parent.rglob("*")):
        if p.is_file():
            data = p.read_bytes()
            n = data.count(APP_ID.encode("utf-8"))
            if n:
                hits[str(p.relative_to(REPO))] = n
    out(f"AppID scan: {'FAIL - occurrences found: ' + json.dumps(hits) if hits else 'PASS - zero occurrences across all files under tools/s8_contract/'}")
    rec["appid_scan"] = {"files_scanned": len(list(Path(__file__).resolve().parent.rglob('*'))),
                         "occurrences": hits,
                         "result": "FAIL" if hits else "PASS"}
    save_evidence("request_budget.json", rec)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cmd_envcheck(_args):
    if not init_appid():
        out("FAIL: TRIMET_APP_ID not found in .env")
        return
    out(f"PASS: .env loaded. TRIMET_APP_ID present: length={len(APP_ID)}, "
        f"alphanumeric={APP_ID.isalnum()} (value never displayed)")


def main(argv=None):
    parser = argparse.ArgumentParser(description="S8 feed contract verification probe")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("envcheck")
    sub.add_parser("dryrun")
    sub.add_parser("parta")
    sub.add_parser("window")
    sub.add_parser("partb")
    sub.add_parser("analyze")
    sub.add_parser("b7")
    sub.add_parser("report")
    args = parser.parse_args(argv)

    global CAMPAIGN_ID
    if args.cmd in ("parta", "window", "partb", "report"):
        if not init_appid():
            out("FAIL: TRIMET_APP_ID not found in .env")
            return 2
        load_campaign_budget()
    elif args.cmd in ("analyze", "b7"):
        load_campaign_budget()   # campaign id only; no live requests in these phases
    elif args.cmd == "envcheck":
        pass
    handler = {
        "envcheck": cmd_envcheck,
        "dryrun": cmd_dryrun,
        "parta": cmd_parta,
        "window": cmd_window,
        "partb": cmd_partb,
        "analyze": cmd_analyze,
        "b7": cmd_b7,
        "report": cmd_report,
    }[args.cmd]
    handler(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())








