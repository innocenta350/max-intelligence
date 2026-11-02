"""
M0 Feed Audit utility (MAX Intelligence).

Purpose:
    One-off, step-wise audit tool for Milestone M0. It tests TriMet endpoints,
    verifies assumptions A1-A5, studies the `expires` field (E1), and observes
    quota-related behavior. Findings are saved as sanitized evidence JSON files
    so docs/trimet-api-notes.md can cite them.

Security:
    - TRIMET_APP_ID is loaded from .env at the repo root.
    - The AppID is NEVER printed. All output and evidence pass through redact().
    - Evidence files are additionally scrubbed for the AppID substring.

Usage:
    python tools/m0_audit/audit_m0.py <subcommand>

Subcommands (each conserves requests; run them step by step):
    envcheck        Load .env, confirm AppID present without revealing it.
    vehicles        One full /ws/v2/vehicles snapshot (JSON).
    alerts          One /ws/v2/alerts snapshot (JSON).
    routeconfig     /ws/V1/routeConfig (json=true), all routes, no stops.
    routeconfig --routes 90,100,190,200,290
                    Same endpoint for specific routes, with stops and directions.
    tripupdate      /ws/V1/TripUpdate protobuf; decode with stdlib wire parser.
    rtpositions     /ws/V1/VehiclePositions protobuf; decode with stdlib wire parser.
    a2-compare      RT VehiclePositions + JSON snapshot back-to-back coverage compare.
    a4-cadence      5 JSON snapshots spaced ~4s; queryTime deltas + freshness stats.
    e1-study        Follow a vehicle until expires passes; test showStale behavior.

Only the Python standard library is used so M0 installs no dependencies.
"""

from __future__ import annotations

import argparse
import json
import ssl
import struct
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------
# Paths, env loading, redaction
# --------------------------------------------------------------------------

REPO = Path(__file__).resolve().parents[2]     # .../max-intelligence/max-intelligence
ENV_PATH = REPO / ".env"
EVIDENCE_DIR = Path(__file__).resolve().parent / "evidence"

APP_ID = ""


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


def redact(text: str) -> str:
    """Remove any trace of the AppID from strings before display/storage."""
    if APP_ID:
        text = text.replace(APP_ID, "***REDACTED***")
    return text


def now_ms() -> int:
    return int(time.time() * 1000)


def expiry_unit_ratio(expires_ms: int) -> float:
    """Sanity ratio: close to 1.0 means both clocks are epoch-milliseconds."""
    return expires_ms / max(now_ms(), 1)


def utc_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")


def save_evidence(name: str, data: dict) -> Path:
    """Write a sanitized evidence JSON file; scrub any accidental AppID."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / name
    text = json.dumps(_scrub(data), indent=2, ensure_ascii=False, default=str)
    path.write_text(redact(text), encoding="utf-8")
    return path


def _scrub(obj):
    """Recursively redact strings inside evidence payloads (defense in depth)."""
    if isinstance(obj, str):
        return redact(obj)
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_scrub(v) for v in obj]
    return obj


def out(msg: str = "") -> None:
    print(redact(msg))


def note(section: str, url: str, status: int, content_type: str, **extra) -> dict:
    """Standard evidence block for one HTTP observation."""
    return {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "section": section,
        "url_redacted": redact(url),
        "http_status": status,
        "content_type": content_type,
        **extra,
    }


# --------------------------------------------------------------------------
# HTTP helpers (stdlib only).
# URLS_ below are templates: the "{AID}" placeholder is filled with the AppID
# at run time and always immediately redacted from any output.
# --------------------------------------------------------------------------

CTX = ssl.create_default_context()
USER_AGENT = "MAXIntelligenceM0Audit/0.1 (+transit research audit)"

URL_VEHICLES = "https://developer.trimet.org/ws/v2/vehicles?appID={AID}"
URL_ALERTS = "https://developer.trimet.org/ws/v2/alerts?appID={AID}"
URL_ROUTECONFIG = "https://developer.trimet.org/ws/V1/routeConfig?appID={AID}&json=true"
URL_ROUTECONFIG_STALE = "https://developer.trimet.org/ws/v2/vehicles?showStale=true&appID={AID}"
URL_RT_TRIPUPDATES = "https://developer.trimet.org/ws/V1/TripUpdate?appID={AID}"
URL_RT_TRIPUPDATES_SLASH = "https://developer.trimet.org/ws/V1/TripUpdate/?appID={AID}"
URL_RT_POSITIONS = "https://developer.trimet.org/ws/V1/VehiclePositions?appID={AID}"
URL_GTFS_ZIP = "https://developer.trimet.org/schedule/gtfs.zip"


def fill(url: str) -> str:
    return url.replace("{AID}", APP_ID)


def fetch_raw(url: str, timeout: float = 30.0):
    """GET a URL (already containing credentials if any). Returns (status, headers, body)."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as resp:
            return resp.status, dict(resp.headers.items()), resp.read()
    except urllib.error.HTTPError as e:  # non-2xx is still evidence
        return e.code, dict(e.headers.items()) if e.headers else {}, e.read()


def fetch_json(url: str, timeout: float = 30.0):
    """GET a JSON TriMet endpoint. Returns (evidence, parsed_json_or_None, status, ctype, body)."""
    status, headers, body = fetch_raw(url, timeout)
    ctype = headers.get("Content-Type", "") or headers.get("content-type", "")
    parsed = None
    err = None
    try:
        parsed = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as exc:  # noqa: BLE001
        err = f"json parse failed: {exc!r}; first bytes: {body[:120]!r}"
    ev = note(
        "probe",
        url,
        status,
        ctype,
        bytes=len(body),
        parse_error=err,
        response_headers_sanitized={k: redact(str(v)) for k, v in headers.items()},
    )
    return ev, parsed, status, ctype, body


# --------------------------------------------------------------------------
# Minimal protobuf wire-format decoder (for GTFS-Realtime) - stdlib only.
# Field numbers follow the official gtfs-realtime.proto (v2.0):
#   FeedMessage: header=1, entity=2
#   FeedHeader:  gtfs_realtime_version=1, incrementality=2, timestamp=3
#   FeedEntity:  id=1, is_deleted=2, trip_update=3, vehicle=4, alert=5
#   TripUpdate:  trip=1, stop_time_update=2, vehicle=3, timestamp=4, delay=5
#   TripDescriptor: trip_id=1, start_time=2, start_date=3,
#                   schedule_relationship=4, route_id=5, direction_id=6
#   VehicleDescriptor: id=1, label=2, license_plate=3
#   StopTimeUpdate: stop_sequence=1, arrival=2, departure=3, stop_id=4,
#                   schedule_relationship=5
#   StopTimeEvent: delay=1, time=2, uncertainty=3
#   VehiclePosition: trip=1, vehicle=2, position=3, current_stop_sequence=4,
#                    stop_id=5, current_status=6, timestamp=7
#   Position: latitude=1, longitude=2, bearing=3, odometer=4, speed=5
#   Alert: active_period=1, informed_entity=2, cause=4, effect=5, url=6,
#          header_text=8, description_text=9
# --------------------------------------------------------------------------

def parse_wire(data: bytes):
    """Generic protobuf parse: list of (field_number, wire_type, value).
    wire_type 0: varint; 2: length-delimited bytes; 5: fixed32; 1: fixed64."""
    items = []
    i = 0
    n = len(data)
    while i < n:
        tag, i = varint(data, i)
        field_number = tag >> 3
        wire_type = tag & 7
        if wire_type == 0:
            val, i = varint(data, i)
        elif wire_type == 2:
            ln, i = varint(data, i)
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


def varint(data: bytes, i: int) -> tuple[int, int]:
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
    """Parse submessages of a given field."""
    return [parse_wire(v) for f, w, v in items if f == field_no and w == 2]


def string_field(items, field_no):
    vals = [v.decode("utf-8", errors="replace") for f, w, v in items if f == field_no and w == 2]
    return vals[0] if vals else None


def int_field(items, field_no):
    """Varint fields can carry negative int64/int32 as 10-byte sign-extended values.
    Convert two's-complement so delay/time come out sensible."""
    vals = [v for f, w, v in items if f == field_no and w == 0]
    if not vals:
        return None
    x = vals[0]
    if x >= 1 << 63:          # negative signed value encoded as huge unsigned
        x -= 1 << 64
    return x


def float32_field(items, field_no):
    vals = [struct.unpack("<f", v)[0] for f, w, v in items if f == field_no and w == 5]
    return vals[0] if vals else None


def ste(sub_items):
    """StopTimeEvent: delay=1, time=2, uncertainty=3."""
    return {
        "delay": int_field(sub_items, 1),
        "time": int_field(sub_items, 2),
        "uncertainty": int_field(sub_items, 3),
    }


def decode_mystery(vp_items, field_no):
    """Decode a VP submessage conservatively: report nested field numbers/types,
    string candidates, float32 candidates, and varint values. Never raises."""
    subs = [v for f, w, v in vp_items if f == field_no and w == 2]
    if not subs:
        return None
    raw = subs[0]
    try:
        items = parse_wire(raw)
    except Exception as exc:  # noqa: BLE001
        return {"parse_error": repr(exc), "hex_head": raw[:48].hex(), "len": len(raw)}
    out_items = []
    for f, w, v in items:
        rec = {"field": f, "wire": w}
        try:
            if w == 2:
                utf = v.decode("utf-8", errors="replace")
                rec["utf8_candidate"] = utf if all(32 <= ord(c) < 127 or c in "\r\n\t" for c in utf) else None
                rec["utf8_len"] = len(v)
            elif w == 5:
                rec["f32"] = struct.unpack("<f", v)[0]
                rec["hex"] = v.hex()
            elif w == 0:
                rec["varint"] = v
            elif w == 1:
                rec["f64"] = struct.unpack("<d", v)[0] if len(v) == 8 else None
        except Exception as exc:  # noqa: BLE001
            rec["decode_error"] = repr(exc)
        out_items.append(rec)
    return out_items


def decode_trip_descriptor(sub_items):
    return {
        "trip_id": string_field(sub_items, 1),
        "start_time": string_field(sub_items, 2),
        "start_date": string_field(sub_items, 3),
        "schedule_relationship_raw": int_field(sub_items, 4),
        "route_id": string_field(sub_items, 5),
        "direction_id": int_field(sub_items, 6),
    }


def decode_tripupdate(sub_items):
    trip_items = submessages(sub_items, 1)
    stu_items = submessages(sub_items, 2)
    veh_items = submessages(sub_items, 3)
    trip = decode_trip_descriptor(trip_items[0]) if trip_items else None
    vehicle = None
    if veh_items:
        vehicle = {"id": string_field(veh_items[0], 1), "label": string_field(veh_items[0], 2)}
    stu_summary = []
    for s in stu_items:
        arrival_items = submessages(s, 2)
        departure_items = submessages(s, 3)
        stu_summary.append({
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
        "stop_time_update_count": len(stu_summary),
        "stop_time_updates_first3": stu_summary[:3],
    }


def decode_vehicleposition(sub_items):
    trip_items = submessages(sub_items, 1)
    veh_items = submessages(sub_items, 2)
    pos_items = submessages(sub_items, 3)
    trip = decode_trip_descriptor(trip_items[0]) if trip_items else None
    vehicle = None
    if veh_items:
        vehicle = {"id": string_field(veh_items[0], 1), "label": string_field(veh_items[0], 2)}
    pos = None
    if pos_items:
        pos = {"latitude": float32_field(pos_items[0], 1), "longitude": float32_field(pos_items[0], 2)}
    return {
        "trip": trip,
        "vehicle": vehicle,
        "position": pos,
        "current_stop_sequence": int_field(sub_items, 4),
        "stop_id": string_field(sub_items, 5),
        "timestamp": int_field(sub_items, 7),
    }


def decode_rt_feed(body: bytes):
    """Parse a GTFS-RT FeedMessage: header + entities (trip_update / vehicle_position / alert)."""
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
            rec["trip_update"] = decode_tripupdate(tu_items[0])
        if vp_items:
            rec["vehicle_position"] = decode_vehicleposition(vp_items[0])
        if al_items:
            rec["alert"] = {
                "cause_raw": int_field(al_items[0], 4),
                "effect_raw": int_field(al_items[0], 5),
                "header_text": string_field(al_items[0], 8),
                "description_first120": (string_field(al_items[0], 9) or "")[:120],
            }
        entities.append(rec)
    return header, entities


# --------------------------------------------------------------------------
# Subcommands
# --------------------------------------------------------------------------

def cmd_envcheck(_args):
    out(f"PASS: .env loaded. TRIMET_APP_ID present: length={len(APP_ID)}, alphanumeric={APP_ID.isalnum()}")
    for k, v in load_env().items():
        if k != "TRIMET_APP_ID":
            out(f"  {k} = {v}  (non-secret config; value shown intentionally)")


def jv(obj, name):
    """TriMet JSON keys carry no '@' prefix, but be defensive and accept both forms."""
    if obj is None:
        return None
    if name in obj:
        return obj[name]
    return obj.get("@" + name)


def summarize_vehicles(parsed):
    """Extract sanitized facts from a parsed /ws/v2/vehicles payload."""
    rs = (parsed or {}).get("resultSet", {})
    query_time = jv(rs, "queryTime")
    vehicles = rs.get("vehicle", [])
    sum_v = 0
    routes_seen: dict = {}
    rails = []
    attr_freq: dict = {}
    for v in vehicles:
        sum_v += 1
        r = jv(v, "routeNumber")
        routes_seen[r] = routes_seen.get(r, 0) + 1
        if str(jv(v, "type")).lower() == "rail":
            rails.append({
                "vehicleID": jv(v, "vehicleID"),
                "routeNumber": r,
                "routeColor": jv(v, "routeColor"),
                "routeSubType": jv(v, "routeSubType"),
                "direction": jv(v, "direction"),
                "tripID": jv(v, "tripID"),
                "newTrip": jv(v, "newTrip"),
                "offRoute": jv(v, "offRoute"),
                "time": jv(v, "time"),
                "time_iso": utc_iso(jv(v, "time")),
                "expires": jv(v, "expires"),
                "expires_iso": utc_iso(jv(v, "expires")),
                "expires_minus_queryTime_s": (jv(v, "expires") - query_time) / 1000 if jv(v, "expires") and query_time else None,
                "delay": jv(v, "delay"),
                "lat": jv(v, "latitude"),
                "lon": jv(v, "longitude"),
                "bearing": jv(v, "bearing"),
                "nextLocID": jv(v, "nextLocID"),
                "nextStopSeq": jv(v, "nextStopSeq"),
                "lastLocID": jv(v, "lastLocID"),
                "lastStopSeq": jv(v, "lastStopSeq"),
                "signMessage": jv(v, "signMessage"),
                "signMessageLong": jv(v, "signMessageLong"),
                "garage": jv(v, "garage"),
                "blockID": jv(v, "blockID"),
                "source": jv(v, "source"),
            })
        for k in v:
            attr_freq[k] = attr_freq.get(k, 0) + 1
    margins = sorted((r["expires_minus_queryTime_s"] for r in rails if r["expires_minus_queryTime_s"] is not None))
    return {
        "queryTime": query_time,
        "queryTime_iso": utc_iso(query_time),
        "vehicle_count_total": len(vehicles),
        "rail_count": len(rails),
        "rail_vehicles": rails,
        "route_counts": dict(sorted(((str(k), v) for k, v in routes_seen.items()), key=lambda kv: str(kv[0]))),
        "attribute_frequency_counts": dict(sorted(attr_freq.items())),
        "rail_expires_margin_seconds_min": margins[0] if margins else None,
        "rail_expires_margin_seconds_max": margins[-1] if margins else None,
        "attribute_names_first_vehicle": sorted(vehicles[0].keys()) if vehicles else [],
    }


def cmd_vehicles(_args):
    url = fill(URL_VEHICLES)
    ev, parsed, status, ctype, _body = fetch_json(url)
    s = summarize_vehicles(parsed)
    rec = {
        "observation": ev,
        "summary_counts": {
            "total": s["vehicle_count_total"],
            "rail": s["rail_count"],
            "queryTime": s["queryTime"],
            "queryTime_iso": s["queryTime_iso"],
        },
        "rail_sample_first5": s["rail_vehicles"][:5],
        "rail_expires_margin_seconds_min": s["rail_expires_margin_seconds_min"],
        "rail_expires_margin_seconds_max": s["rail_expires_margin_seconds_max"],
        "route_counts": s["route_counts"],
        "attribute_names_first_vehicle": s["attribute_names_first_vehicle"],
        "attribute_frequency_counts_full": s["attribute_frequency_counts"],
        "result": "HTTP OK" if 200 <= status < 300 else "HTTP ERROR",
    }
    path = save_evidence("vehicles_snapshot.json", rec)
    out(f"HTTP {status} content-type={ctype} queryTime={s['queryTime']} -> {s['queryTime_iso']}")
    out(f"vehicles total={s['vehicle_count_total']} rail={s['rail_count']}")
    out(f"rail expires margin (queryTime-relative) min..max seconds: {s['rail_expires_margin_seconds_min']}..{s['rail_expires_margin_seconds_max']}")
    out(f"first rail sample: {json.dumps(_scrub(s['rail_vehicles'][0]), ensure_ascii=False)[:400]}" if s['rail_vehicles'] else "no rail vehicles in response")
    out(f"attribute names (first vehicle): {s['attribute_names_first_vehicle']}")
    out(f"top routes by vehicle count: {dict(list(s['route_counts'].items())[:8])}")
    out(f"evidence: {path}")


def cmd_alerts(_args):
    url = fill(URL_ALERTS)
    ev, parsed, status, ctype, _body = fetch_json(url)
    rs = (parsed or {}).get("resultSet", {})
    alert_list = rs.get("alert", []) if isinstance(rs, dict) else []
    summaries = []
    for a in alert_list[:30]:
        route_val = a.get("route", [])
        routes = route_val if isinstance(route_val, list) else ([route_val] if isinstance(route_val, dict) else [])
        loc_val = a.get("location", [])
        locs = loc_val if isinstance(loc_val, list) else ([loc_val] if isinstance(loc_val, dict) else [])
        summaries.append({
            "id": jv(a, "id"),
            "begin": jv(a, "begin"),
            "begin_iso": utc_iso(jv(a, "begin")),
            "end": jv(a, "end"),
            "end_iso": utc_iso(jv(a, "end")),
            "system_wide_flag": jv(a, "system_wide_flag"),
            "header_text": jv(a, "header_text"),
            "desc_first160": (jv(a, "desc") or "")[:160],
            "info_link_url": jv(a, "info_link_url"),
            "routes": [{"id": jv(r, "id"), "type": jv(r, "type"), "no_service_flag": jv(r, "no_service_flag")} for r in routes],
            "locations_count": len(locs),
            "location_sample": [{"id": jv(l, "id"), "desc": (jv(l, "desc") or "")[:60], "no_service_flag": jv(l, "no_service_flag")} for l in locs[:2]],
        })
    rec = {
        "observation": ev,
        "summary": {"alert_count": len(alert_list), "alerts_sample": summaries},
        "result": "HTTP OK" if 200 <= status < 300 else "HTTP ERROR",
    }
    path = save_evidence("alerts_snapshot.json", rec)
    out(f"HTTP {status} content-type={ctype}")
    out(f"alerts count={len(alert_list)}")
    for s in summaries[:6]:
        out("  " + json.dumps(_scrub(s), ensure_ascii=False)[:300])
    out(f"evidence: {path}")


def cmd_routeconfig(args):
    extra = ""
    if getattr(args, "routes", None):
        extra = "&routes=" + args.routes + "&dir=true&stops=true"
    url = fill(URL_ROUTECONFIG) + extra
    ev, parsed, status, ctype, body = fetch_json(url)
    url_used = url
    if not (200 <= status < 300):
        # retry with path-style parameters documented in the service examples
        routes = getattr(args, "routes", None)
        if routes:
            path_style = "https://developer.trimet.org/ws/V1/routeConfig/route/" + routes + "/dir/true/stops/true?appID=" + APP_ID + "&json=true"
            ev2, parsed2, status2, ctype2, body2 = fetch_json(path_style)
            ev["retry_url_redacted"] = redact(path_style)
            ev["retry_http_status"] = status2
            ev["retry_content_type"] = ctype2
            if 200 <= status2 < 300:
                url_used, parsed, status, ctype, body = path_style, parsed2, status2, ctype2, body2
                ev["retry_result"] = "OK"
            else:
                ev["retry_result"] = "ALSO FAILED"
                ev["retry_parse_error"] = None
                ev["error_body_first200"] = redact(body2[:200].decode("utf-8", errors="replace"))
                ev["error_body_first200_original"] = redact(body[:200].decode("utf-8", errors="replace"))
    rs = (parsed or {}).get("resultSet", {})
    routes_list = rs.get("route", []) if isinstance(rs, dict) else []
    routes = routes_list
    route_rows = []
    rail_routes = []
    for r in routes:
        dir_val = r.get("dir", [])
        dirs = dir_val if isinstance(dir_val, list) else ([dir_val] if isinstance(dir_val, dict) else [])
        row = {
            "route": jv(r, "route"),
            "type": jv(r, "type"),
            "routeSubType": jv(r, "routeSubType"),
            "routeColor": jv(r, "routeColor"),
            "desc": (jv(r, "desc") or "")[:60],
            "frequentService": jv(r, "frequentService"),
            "routeSortOrder": jv(r, "routeSortOrder"),
            "direction_count": len(dirs),
            "stops_total": 0,
            "direction_samples": [],
        }
        stop_total = 0
        for d in dirs:
            stops = d.get("stop", [])
            stops = stops if isinstance(stops, list) else ([stops] if isinstance(stops, dict) else [])
            stop_total += len(stops)
            row["direction_samples"].append({"dir": jv(d, "dir"), "desc": jv(d, "desc"), "stops": len(stops)})
        row["stops_total"] = stop_total
        route_rows.append(row)
        if (jv(r, "routeSubType") or "").strip() == "Light Rail":
            rail_routes.append(row)
    rec = {
        "observation": ev,
        "summary": {
            "route_count": len(routes),
            "light_rail_routes": rail_routes,
            "subtype_values_observed": sorted({(jv(r, "routeSubType") or "") for r in routes}),
            "type_values_observed": sorted({str(jv(r, "type")) for r in routes}),
        },
        "result": "HTTP OK" if 200 <= status < 300 else "HTTP ERROR",
    }
    name = "routeconfig_all.json" if not extra else "routeconfig_selected.json"
    path = save_evidence(name, rec)
    out(f"HTTP {status} content-type={ctype}")
    out(f"routes returned: {len(routes)}")
    out("routeSubType values observed: " + ", ".join({r for r in rec["summary"]["subtype_values_observed"] if r}))
    out("type values observed: " + ", ".join(rec["summary"]["type_values_observed"]))
    if rail_routes:
        out("Light Rail routes:")
        for r in rail_routes:
            out(f"  route={r['route']} color={r['routeColor']} desc={r['desc']} dirs={r['direction_count']} stops={r['stops_total']} samples={r['direction_samples']}")
    out(f"evidence: {path}")


def rt_url_probe(base_url: str, alt_url: str | None = None):
    """Try base_url; if non-2xx and an alt is given, try that (e.g. with trailing slash).
    Returns (status, headers, body, url_used, alt_tried)."""
    url_used = base_url
    status, headers, body = fetch_raw(fill(base_url))
    alt_tried = False
    if alt_url and not (200 <= status < 300):
        alt_tried = True
        url_used = alt_url
        status, headers, body = fetch_raw(fill(alt_url))
    return status, headers, body, url_used, alt_tried


def cmd_tripupdate(_args):
    status, headers, body, url_used, alt_tried = rt_url_probe(URL_RT_TRIPUPDATES, URL_RT_TRIPUPDATES_SLASH)
    ctype = headers.get("Content-Type", "") or ""
    rec = note("probe", url_used, status, ctype, bytes=len(body), alternate_url_tried=alt_tried)
    try:
        header, entities = decode_rt_feed(body)
        tu_only = [e for e in entities if e.get("trip_update")]
        vp_like = [e for e in entities if e.get("vehicle_position")]
        stop_updates = sum(e["trip_update"]["stop_time_update_count"] for e in tu_only)
        route_ids = sorted(str((e["trip_update"]["trip"] or {}).get("route_id")) for e in tu_only if e.get("trip_update"))
        sched_rel = sorted({(e["trip_update"]["trip"] or {}).get("schedule_relationship_raw") for e in tu_only if e.get("trip_update")})
        rec["decode"] = {
            "header": header,
            "entities_total": len(entities),
            "trip_update_entities": len(tu_only),
            "vehicle_position_entities": len(vp_like),
            "alert_entities": len([e for e in entities if e.get("alert")]),
            "total_stop_time_updates": stop_updates,
            "tripupdate_route_ids": route_ids,
            "schedule_relationship_values_dr": sched_rel,
            "entities_sample_first3": entities[:3],
        }
        rec["result"] = "DECODED OK"
        out(f"HTTP {status} content-type={ctype} bytes={len(body)} url_used={redact(url_used)}")
        out(f"header: {header}")
        out(f"entities: total={len(entities)} trip_update={len(tu_only)} vehicle_position={len(vp_like)} alert={len([e for e in entities if e.get('alert')])}")
        out(f"stop_time_updates across entities: {stop_updates}")
        out(f"distinct route_ids in trip updates: {sorted(set(route_ids))}")
        out(f"schedule_relationship raw values: {sched_rel}")
        out("sample entity[0]: " + json.dumps(_scrub(entities[0]), ensure_ascii=False, default=str)[:500] if entities else "(no entities)")
    except Exception as exc:  # noqa: BLE001
        rec["result"] = f"DECODE FAILED: {exc!r}"
        rec["body_head_hex"] = body[:64].hex()
        out(f"decode failed: {exc!r}")
    path = save_evidence("tripupdate_snapshot.json", rec)
    out(f"evidence: {path}")


def cmd_rtpositions(_args):
    url_used = URL_RT_POSITIONS
    status, headers, body = fetch_raw(fill(url_used))
    ctype = headers.get("Content-Type", "") or ""
    rec = {"observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
           "url_redacted": redact(url_used), "http_status": status, "content_type": ctype, "bytes": len(body)}
    try:
        header, entities = decode_rt_feed(body)
        vps = [e["vehicle_position"] for e in entities if e.get("vehicle_position")]
        route_ids = sorted({str((e.get("trip") or {}).get("route_id")) for e in vps})
        veh_ids = [(e.get("vehicle") or {}).get("id") for e in vps]
        rec["decode"] = {
            "header": header,
            "entities_total": len(entities),
            "vehicle_position_entities": len(vps),
            "distinct_route_ids": route_ids,
            "vehicle_ids_count": len([v for v in veh_ids if v]),
            "samples_first5": vps[:5],
        }
        rec["result"] = "DECODED OK"
        out(f"HTTP {status} content-type={ctype} bytes={len(body)}")
        out(f"header: {header}")
        out(f"entities total={len(entities)} vehicle_positions={len(vps)}")
        out(f"distinct route_ids: {route_ids}")
        for s2 in vps[:5]:
            out("sample VP: " + json.dumps(_scrub(s2), ensure_ascii=False, default=str)[:300])
    except Exception as exc:  # noqa: BLE001
        rec["result"] = f"DECODE FAILED: {exc!r}"
        rec["body_head_hex"] = body[:64].hex()
        out(f"decode failed: {exc!r}")
    path = save_evidence("rtpositions_snapshot.json", rec)
    out(f"evidence: {path}")


def cmd_a2_compare(_args):
    """A2: RT VehiclePositions coverage vs. native JSON endpoint, same minute."""
    ev, parsed, status, ctype, _ = fetch_json(fill(URL_VEHICLES))
    js = summarize_vehicles(parsed)
    url_rt = URL_RT_POSITIONS
    status2, headers2, body = fetch_raw(fill(url_rt))
    ctype2 = headers2.get("Content-Type", "") or ""
    ev2 = note("probe", url_rt, status2, ctype2, bytes=len(body))
    rec = {
        "json_observation": ev,
        "json_summary": {
            "total": js["vehicle_count_total"],
            "rail": js["rail_count"],
            "queryTime": js["queryTime"],
            "queryTime_iso": js["queryTime_iso"],
        },
        "rt_observation": ev2,
    }
    try:
        header, entities = decode_rt_feed(body)
        vps = [e["vehicle_position"] for e in entities if e.get("vehicle_position")]
        rt_trip_ids = {(e.get("trip") or {}).get("trip_id") for e in vps}
        rt_trip_ids.discard(None)
        rt_route_ids = sorted({str((e.get("trip") or {}).get("route_id")) for e in vps if e.get("trip")})
        with_vehicle_descriptor = len([e for e in vps if e.get("vehicle")])
        with_position = len([e for e in vps if e.get("position") and e["position"]["latitude"] is not None])
        with_timestamp = len([e for e in vps if e.get("timestamp")])
        json_trip_ids = {jv(v, "tripID") for v in (parsed or {}).get("resultSet", {}).get("vehicle", [])}
        json_trip_ids.discard(None)
        json_rail_trip_ids = {r["tripID"] for r in js["rail_vehicles"]}
        both = sorted(json_trip_ids & rt_trip_ids)
        rail_in_both = sorted(json_rail_trip_ids & rt_trip_ids)
        rail_missing = sorted(json_rail_trip_ids - rt_trip_ids)
        rec["rt_header"] = header
        rec["rt_totals"] = {
            "vehicle_position_entities": len(vps),
            "entities_with_vehicle_descriptor": with_vehicle_descriptor,
            "entities_with_position": with_position,
            "entities_with_timestamp": with_timestamp,
            "distinct_trip_ids": len(rt_trip_ids),
        }
        rec["comparison"] = {
            "identity_criterion": "trip_id (RT VehiclePosition lacks VehicleDescriptor)",
            "json_all_trip_ids": len(json_trip_ids),
            "json_rail_trip_id_count": len(json_rail_trip_ids),
            "json_rail_trip_ids": sorted(json_rail_trip_ids),
            "rt_route_ids_observed": rt_route_ids,
            "tripids_in_both": both,
            "rail_tripids_in_both": rail_in_both,
            "rail_tripids_only_in_json": rail_missing,
            "rt_tripids_not_in_json": sorted(rt_trip_ids - json_trip_ids),
        }
        rec["result"] = "DECODED OK"
        out(f"JSON snapshot: status={status} ctype={ctype} total={js['vehicle_count_total']} rail={len(json_rail_trip_ids)}")
        out(f"RT snapshot:   status={status2} ctype={ctype2} vehicle_positions={len(vps)} distinct trip_ids={len(rt_trip_ids)}")
        out(f"RT field presence: vehicle_descriptor={with_vehicle_descriptor} position={with_position} timestamp={with_timestamp} of {len(vps)}")
        out(f"trip_ids in both feeds: {len(both)}")
        out(f"rail trip_ids in both: {len(rail_in_both)} ; rail trip_ids only in JSON: {len(rail_missing)} {rail_missing[:8]}")
        out(f"RT distinct route_ids observed: {rt_route_ids}")
    except Exception as exc:  # noqa: BLE001
        rec["result"] = f"DECODE FAILED: {exc!r}"
        out(f"decode failed: {exc!r}")
    path = save_evidence("a2_compare.json", rec)
    out(f"evidence: {path}")


def cmd_a4_cadence(_args):
    """A4: measure queryTime deltas across 5 snapshots spaced ~4s."""
    samples = []
    for i in range(5):
        ev, parsed, status, ctype, _ = fetch_json(fill(URL_VEHICLES))
        s = summarize_vehicles(parsed)
        rail_time_ages = [
            (s["queryTime"] - r["time"]) / 1000.0
            for r in s["rail_vehicles"] if r["time"] and s["queryTime"]
        ]
        samples.append({
            "i": i,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "http_status": status,
            "queryTime": s["queryTime"],
            "queryTime_iso": s["queryTime_iso"],
            "total": s["vehicle_count_total"],
            "rail_count": s["rail_count"],
            "rail_time_age_seconds_min": min(rail_time_ages) if rail_time_ages else None,
            "rail_time_age_seconds_max": max(rail_time_ages) if rail_time_ages else None,
        })
        if i < 4:
            time.sleep(4)
    deltas = [(samples[k + 1]["queryTime"] - samples[k]["queryTime"]) / 1000.0 for k in range(len(samples) - 1)]
    rec = {
        "samples": samples,
        "queryTime_deltas_seconds": deltas,
        "rail_counts_over_time": [s["rail_count"] for s in samples],
        "result": "COLLECTED",
    }
    path = save_evidence("a4_cadence.json", rec)
    out(f"queryTime deltas across ~4s gaps (seconds): {deltas}")
    out(f"rail counts over time: {rec['rail_counts_over_time']}")
    for s in samples:
        out(f"  sample {s['i']}: status={s['http_status']} rail={s['rail_count']} railTimeAge(min..max)={s['rail_time_age_seconds_min']}..{s['rail_time_age_seconds_max']}s")
    out(f"evidence: {path}")


def cmd_gtfs_audit(_args):
    """A5 + A3-part: GTFS zip accessibility, file list, feed_version, and a
    limited join-key probe against the live vehicle feed (full join work is M1).
    Downloads gtfs.zip once with stdlib only, then parses locally and deletes it.
    """
    rec: dict = {"result": "INCOMPLETE"}
    import io
    import os
    import tempfile
    import zipfile

    # Step 1: HEAD to check availability and metadata
    head_url = URL_GTFS_ZIP
    req = urllib.request.Request(head_url, headers={"User-Agent": USER_AGENT}, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30, context=CTX) as resp:
            h = dict(resp.headers.items())
            rec["head"] = {
                "url": head_url,
                "status": resp.status,
                "content_length": h.get("Content-Length"),
                "last_modified": h.get("Last-Modified"),
                "accept_ranges": h.get("Accept-Ranges"),
                "content_type": h.get("Content-Type"),
            }
            out(f"HEAD gtfs.zip: {rec['head']}")
    except Exception as exc:  # noqa: BLE001
        rec["head"] = {"url": head_url, "error": repr(exc)}
        out(f"HEAD gtfs.zip failed: {exc!r}")

    # Step 2: full GET once, parse locally, delete the file afterwards
    status, headers, body = fetch_raw(head_url, timeout=120)
    rec["get"] = {
        "url": head_url,
        "status": status,
        "bytes": len(body),
        "content_type": headers.get("Content-Type"),
        "last_modified": headers.get("Last-Modified"),
    }
    out(f"GET gtfs.zip: status={status} bytes={len(body)}")
    if not (200 <= status < 300):
        rec["result"] = "FAIL: gtfs.zip not retrievable"
        path = save_evidence("gtfs_audit.json", rec)
        out(f"evidence: {path}")
        return

    tmp_dir = tempfile.gettempdir()
    tmp_zip = Path(tmp_dir) / "maxi-m0-gtfs.zip"
    tmp_zip.write_bytes(body)
    try:
        with zipfile.ZipFile(tmp_zip) as zf:
            names = zf.namelist()
            rec["zip_member_files"] = names
            out(f"zip members: {names}")

            def read_text(member):
                return zf.read(member).decode("utf-8-sig", errors="replace").splitlines()

            static: dict = {}
            if "feed_info.txt" in names:
                lines = read_text("feed_info.txt")
                static["feed_info"] = lines[:5]
                out(f"feed_info.txt rows: {lines}")
            if "agency.txt" in names:
                lines = read_text("agency.txt")
                static["agency"] = lines[:6]
                out(f"agency.txt rows: {lines[:4]}")
            rail_route_rows = []
            if "routes.txt" in names:
                lines = read_text("routes.txt")
                hdr = lines[0].split(",")
                try:
                    ti = hdr.index("route_type")
                    ri = hdr.index("route_id")
                    sn = hdr.index("route_short_name") if "route_short_name" in hdr else None
                    ln = hdr.index("route_long_name") if "route_long_name" in hdr else None
                    rc = hdr.index("route_color") if "route_color" in hdr else None
                except ValueError as e:
                    static["routes_header"] = lines[0]
                    raise
                for line in lines[1:]:
                    if not line.strip():
                        continue
                    parts = line.split(",")
                    if len(parts) > ti and parts[ti].strip() == "0":
                        row = {
                            "route_id": parts[ri],
                            "route_type": parts[ti],
                        }
                        if sn is not None:
                            row["route_short_name"] = parts[sn]
                        if ln is not None:
                            row["route_long_name"] = parts[ln]
                        if rc is not None:
                            row["route_color"] = parts[rc]
                        rail_route_rows.append(row)
                static["gtfs_route_type0_rows"] = rail_route_rows
                out(f"route_type=0 rows: {rail_route_rows}")
            # A3 join probes
            a3: dict = {}
            if "stops.txt" in names:
                lines = read_text("stops.txt")
                hdr = lines[0].split(",")
                si = hdr.index("stop_id")
                a3["stops_file_rows"] = len(lines) - 1
                stop_ids = set()
                for line in lines[1:]:
                    parts = line.split(",")
                    if len(parts) > si:
                        stop_ids.add(parts[si].strip())
                static["stop_id_count"] = len(stop_ids)
                a3["gtfs_stop_id_count"] = len(stop_ids)
                out(f"stops.txt rows={len(lines)-1} distinct stop_ids={len(stop_ids)}")
            ev, parsed, vstatus, ctype, _ = fetch_json(fill(URL_VEHICLES))
            js = summarize_vehicles(parsed)
            rec["vehicles_probe"] = {"observation": ev, "rail": js["rail_count"], "total": js["vehicle_count_total"]}
            a3["vehicle_probe_rail_count"] = js["rail_count"]
            loc_samples = []
            for r in js["rail_vehicles"][:60]:
                if r["nextLocID"]:
                    loc_samples.append(str(r["nextLocID"]))
                if r["lastLocID"]:
                    loc_samples.append(str(r["lastLocID"]))
            if "stops.txt" in names:
                lines = read_text("stops.txt")
                hdr = lines[0].split(",")
                si = hdr.index("stop_id")
                stop_ids2 = set()
                for line in lines[1:]:
                    parts = line.split(",")
                    if len(parts) > si:
                        stop_ids2.add(parts[si].strip())
                hits = [l for l in loc_samples if l in stop_ids2]
                misses = [l for l in loc_samples if l not in stop_ids2]
                a3["locid_samples_from_vehicles"] = sorted(set(loc_samples))[:20]
                a3["locids_found_in_gtfs_stop_id"] = sorted(set(hits))
                a3["locids_not_found"] = sorted(set(misses))
                out(f"A3 locID probe: samples={len(set(loc_samples))} found={len(set(hits))} missing={sorted(set(misses))}")
            rail_numbers_from_routeconfig = {"90", "100", "190", "200", "290"}
            gtfs_rail_ids = {r["route_id"] for r in rail_route_rows}
            short_names = {str(r.get("route_short_name")) for r in rail_route_rows}
            a3["gtfs_rail_route_ids"] = sorted(gtfs_rail_ids)
            a3["gtfs_rail_short_names"] = sorted(short_names)
            a3["max_numbers_from_routeconfig_in_gtfs_route_id"] = sorted(rail_numbers_from_routeconfig & gtfs_rail_ids)
            a3["max_numbers_from_routeconfig_in_gtfs_short_name"] = sorted(rail_numbers_from_routeconfig & short_names)
            out(f"A3 route join: MAX numbers in gtfs route_id={a3['max_numbers_from_routeconfig_in_gtfs_route_id']}; in short_name={a3['max_numbers_from_routeconfig_in_gtfs_short_name']}")
            rec["static_extract"] = static
            rec["a3_probe"] = a3
            rec["result"] = "COLLECTED"
    finally:
        tmp_zip.unlink(missing_ok=True)
    path = save_evidence("gtfs_audit.json", rec)
    out(f"evidence: {path}")


def cmd_e1_study(_args):
    """E1: follow a vehicle until its expires passes, observing feed behavior.

    Bounded follow-loop (max 10 cycles): each cycle re-reads the vehicle's
    current expires value (it may be extended by newer updates) and waits
    until it passes, then polls once plainly and once with showStale=true.
    """
    max_cycles = 10
    max_cycle_wait_s = 150.0
    history = []
    # Step 1: initial snapshot
    ev, parsed, status, ctype, _ = fetch_json(fill(URL_VEHICLES))
    js = summarize_vehicles(parsed)
    qtime = js["queryTime"] or now_ms()
    local_ms = now_ms()
    clock_offset_ms = local_ms - qtime  # how far local clock is AHEAD of TriMet report time
    steps = {
        "initial": {
            "observation": ev,
            "queryTime": qtime,
            "queryTime_iso": utc_iso(qtime),
            "local_ms": local_ms,
            "local_clock_minus_queryTime_ms": clock_offset_ms,
            "total": js["vehicle_count_total"],
            "rail": js["rail_count"],
        },
    }
    candidates = sorted(
        [r for r in js["rail_vehicles"] if r["expires"] is not None],
        key=lambda r: r["expires"],
    )
    vehicle = candidates[0] if candidates else None
    if not vehicle:
        rec = {"steps": steps, "result": "UNVERIFIED: no vehicles with an expires value observed"}
        path = save_evidence("e1_study.json", rec)
        out("no vehicle with expires found; evidence saved")
        out(f"evidence: {path}")
        return
    vid = vehicle["vehicleID"]
    expires = vehicle["expires"]
    margins = [(r["expires"] - r["time"]) / 1000.0 for r in js["rail_vehicles"] if r["time"] and r["expires"]]
    steps["rail_expires_minus_time_seconds"] = {
        "min": min(margins) if margins else None,
        "median": sorted(margins)[len(margins) // 2] if margins else None,
        "max": max(margins) if margins else None,
        "count": len(margins),
    }
    steps["selected_vehicle_first"] = vehicle
    out(f"selected vehicle {vid} expires={expires} ({vehicle['expires_iso']})")
    out(f"unit check: expires/localNow ratio={expires / max(local_ms, 1):.6f} (~1 => both epoch-ms)")
    out(f"expires - queryTime = {vehicle['expires_minus_queryTime_s']} s ; local clock ahead by {clock_offset_ms} ms")
    out(f"all-rail (expires - time) seconds: {steps['rail_expires_minus_time_seconds']}")

    # Step 2: bounded follow loop until expires passes (or vehicle disappears)
    current = vehicle
    expiry_passed = False
    absence_observed_at = None
    for cycle in range(max_cycles):
        remaining_s = (current["expires"] - now_ms()) / 1000.0
        if remaining_s <= 0:
            expiry_passed = True
            break
        wait_s = min(max(remaining_s + 3.0, 3.0), max_cycle_wait_s)
        out(f"cycle {cycle}: expires in {remaining_s:.1f}s; waiting {wait_s:.1f}s")
        time.sleep(wait_s)
        evc, parsedc, statusc, ctypec, _ = fetch_json(fill(URL_VEHICLES))
        jsc = summarize_vehicles(parsedc)
        current = next((r for r in jsc["rail_vehicles"] if r["vehicleID"] == vid), None)
        history.append({
            "cycle": cycle,
            "queryTime": jsc["queryTime"],
            "queryTime_iso": jsc["queryTime_iso"],
            "present": current is not None,
            "expires_now": current["expires"] if current else None,
            "expires_now_iso": current["expires_iso"] if current else None,
            "expires_extended": bool(current and current["expires"] != expires),
            "expires_passed_by_ms_at_queryTime": (jsc["queryTime"] or 0) - expires,
        })
        out(f"  cycle {cycle}: present={current is not None} expires_now={current['expires_iso'] if current else None} passedByMs={(jsc['queryTime'] or 0) - expires}")
        if current is None:
            absence_observed_at = history[-1]
            break
        if current["expires"] != expires:
            expires = current["expires"]
    if absence_observed_at is None and not expiry_passed:
        rec = {"steps": steps, "history": history, "result": "PARTIAL: expiry not observed within bounded window (expires kept extending)"}
        path = save_evidence("e1_study.json", rec)
        out("E1 PARTIAL: vehicle's expires kept extending; evidence saved")
        out(f"evidence: {path}")
        return

    # Step 3: default (no showStale) poll right now, presence check
    ev2, parsed2, status2, ctype2, _ = fetch_json(fill(URL_VEHICLES))
    js2 = summarize_vehicles(parsed2)
    all_ids2 = {jv(v, "vehicleID") for v in (parsed2 or {}).get("resultSet", {}).get("vehicle", [])}
    steps["after_expiry_default"] = {
        "observation": ev2,
        "queryTime": js2["queryTime"],
        "queryTime_iso": js2["queryTime_iso"],
        "expires_last_seen": expires,
        "expires_passed_by_ms_at_queryTime": (js2["queryTime"] or 0) - expires,
        "status": status2,
        "total_rail": js2["rail_count"],
        "vehicle_still_anywhere": vid in all_ids2,
    }
    out(f"default poll: present={vid in all_ids2} (our last-seen expires passed by {steps['after_expiry_default']['expires_passed_by_ms_at_queryTime']} ms at queryTime); rail total={js2['rail_count']}")

    # Step 4: showStale=true poll
    time.sleep(3.0)
    ev3, parsed3, status3, ctype3, _ = fetch_json(fill(URL_ROUTECONFIG_STALE))
    js3 = summarize_vehicles(parsed3)
    all_ids3 = {jv(v, "vehicleID") for v in (parsed3 or {}).get("resultSet", {}).get("vehicle", [])}
    stale_present = vid in all_ids3
    steps["after_expiry_showStale_true"] = {
        "observation": ev3,
        "queryTime": js3["queryTime"],
        "queryTime_iso": js3["queryTime_iso"],
        "expires_last_seen": expires,
        "expires_passed_by_ms_at_queryTime": (js3["queryTime"] or 0) - expires,
        "status": status3,
        "total_vehicles": js3["vehicle_count_total"],
        "vehicle_still_anywhere": stale_present,
    }
    out(f"showStale=true poll: present={stale_present} (passed by {steps['after_expiry_showStale_true']['expires_passed_by_ms_at_queryTime']} ms); total returned={js3['vehicle_count_total']}")

    conclusion = {
        "vehicleID": vid,
        "unit_check_ratio": expiry_unit_ratio(expires),
        "expires_epoch_ms_rounded_to_seconds_observed": steps["selected_vehicle_first"]["expires"] % 1000 == 0,
        "absence_observed": absence_observed_at is not None,
        "absence_observation": absence_observed_at,
        "present_after_expiry_default": steps["after_expiry_default"]["vehicle_still_anywhere"],
        "present_after_expiry_showStale": stale_present,
        "follow_history": history,
    }
    rec = {"steps": steps, "conclusion": conclusion, "result": "COLLECTED"}
    path = save_evidence("e1_study.json", rec)
    out("conclusion: " + json.dumps(_scrub(conclusion), ensure_ascii=False, default=str)[:700])
    out(f"evidence: {path}")


def cmd_since_probe(_args):
    """A4 support: verify `since=` returns only vehicles refreshed after queryTime."""
    # snapshot 1
    ev1, p1, s1, c1, _ = fetch_json(fill(URL_VEHICLES))
    js1 = summarize_vehicles(p1)
    q1 = js1["queryTime"]
    out(f"snapshot1: queryTime={q1} -> {js1['queryTime_iso']} total={js1['vehicle_count_total']} rail={js1['rail_count']}")
    time.sleep(2.0)
    # snapshot 2 with since=q1
    url2 = fill(URL_VEHICLES) + "&since=" + str(q1)
    ev2, p2, s2, c2, _ = fetch_json(url2)
    rs2 = (p2 or {}).get("resultSet", {})
    vehicles2 = rs2.get("vehicle", [])
    q2 = jv(rs2, "queryTime")
    times_after = 0
    times_stale = 0
    max_time = 0
    for v in vehicles2:
        t = jv(v, "time") or 0
        if t > q1:
            times_after += 1
        else:
            times_stale += 1
        max_time = max(max_time, t)
    ids2 = [jv(v, "vehicleID") for v in vehicles2]
    rec = {
        "since_parameter": "tested",
        "snapshot1": {"observation": ev1, "queryTime": q1, "queryTime_iso": js1["queryTime_iso"],
                      "total": js1["vehicle_count_total"], "rail": js1["rail_count"]},
        "snapshot2": {"observation": ev2, "queryTime": q2, "queryTime_iso": utc_iso(q2),
                      "total": len(vehicles2), "sample_ids": ids2[:8],
                      "vehicle_time_gt_since_count": times_after,
                      "vehicle_time_le_since_count": times_stale,
                      "max_time": max_time},
        "result": "COLLECTED",
    }
    path = save_evidence("since_probe.json", rec)
    out(f"snapshot2 with since={q1}: HTTP {s2} returned {len(vehicles2)} vehicles")
    out(f"  vehicles with time>since: {times_after}; time<=since: {times_stale} (0 expected for true incremental)")
    out(f"  queryTime2={q2} ({utc_iso(q2)}) ; sample ids: {ids2[:8]}")
    out(f"evidence: {path}")


def cmd_rt_raw(_args):
    """Dump raw GTFS-RT VehiclePositions field layout for the first entities,
    to confirm whether lat/lon/timestamp are truly absent (not a decode artifact)."""
    status, headers, body = fetch_raw(fill(URL_RT_POSITIONS))
    ctype = headers.get("Content-Type", "") or ""
    rec = {"observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
           "url_redacted": redact(URL_RT_POSITIONS.replace("{AID}", APP_ID)),
           "http_status": status, "content_type": ctype, "bytes": len(body)}
    try:
        msg = parse_wire(body)
        entity_items = submessages(msg, 2)
        entity_layouts = [{"entity_field_wires": sorted({(f, w) for f, w, _ in ent})} for ent in entity_items[:4]]
        first_vp = None
        for ent in entity_items:
            vp = submessages(ent, 4)
            if not vp:
                continue
            trip = submessages(vp[0], 1)
            vehicle = submessages(vp[0], 2)
            position = submessages(vp[0], 3)
            first_vp = {
                "vp_field_wires": sorted({(f, w) for f, w, _ in vp[0]}),
                "trip_field_wires": sorted({(f, w) for f, w, _ in trip[0]}) if trip else [],
                "trip_route_id": string_field(trip[0], 5) if trip else None,
                "trip_trip_id": string_field(trip[0], 1) if trip else None,
                "field2_submessage_decoded": decode_mystery(vp[0], 2),
                "vehicle_id_raw": string_field(vehicle[0], 1) if vehicle else None,
                "position_field_wires": sorted({(f, w) for f, w, _ in position[0]}) if position else None,
                "top_level_timestamp_field7": int_field(vp[0], 7),
                "field7_submessage_decoded": decode_mystery(vp[0], 7),
                "field8_submessage_decoded": decode_mystery(vp[0], 8),
            }
            break
        rec["entities_total_raw"] = len(entity_items)
        rec["entity_layouts_first4"] = entity_layouts
        rec["first_vehicle_position_layout"] = first_vp
        rec["result"] = "COLLECTED"
        out(f"HTTP {status} ctype={ctype} entities={len(entity_items)}")
        out(f"entity field/wire layouts (first 4): {entity_layouts}")
        out(f"first VP layout: {json.dumps(first_vp)}")
    except Exception as exc:  # noqa: BLE001
        rec["result"] = f"FAILED: {exc!r}"
        out(f"failed: {exc!r}")
    path = save_evidence("rt_raw_layout.json", rec)
    out(f"evidence: {path}")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main():
    global APP_ID
    APP_ID = load_env().get("TRIMET_APP_ID", "")
    if not APP_ID:
        out("FAIL: TRIMET_APP_ID missing/empty in .env; refusing to run.")
        sys.exit(2)
    parser = argparse.ArgumentParser(description="M0 TriMet feed audit")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("envcheck", help="Verify .env and AppID presence (no network).")
    sub.add_parser("vehicles", help="One /ws/v2/vehicles snapshot.")
    sub.add_parser("alerts", help="One /ws/v2/alerts snapshot.")
    rc = sub.add_parser("routeconfig", help="routeConfig, all routes; add --routes for a second stops pass.")
    rc.add_argument("--routes", help="Comma list of route numbers for a targeted stops run.")
    sub.add_parser("tripupdate", help="Decode /ws/V1/TripUpdate protobuf.")
    sub.add_parser("rtpositions", help="Decode /ws/V1/VehiclePositions protobuf.")
    sub.add_parser("a2-compare", help="Compare RT positions vs JSON rail coverage.")
    sub.add_parser("a4-cadence", help="Measure queryTime cadence (5 samples, ~20s).")
    sub.add_parser("since-probe", help="Verify since= returns only refreshed vehicles (2 requests).")
    sub.add_parser("rt-raw", help="Raw field-layout dump of first RT VehiclePositions entities.")
    sub.add_parser("gtfs-audit", help="HEAD + one GET of gtfs.zip; feed_version, file list, A3 join probe.")
    sub.add_parser("e1-study", help="Follow a vehicle through its expires moment.")
    args = parser.parse_args()
    {
        "envcheck": cmd_envcheck,
        "vehicles": cmd_vehicles,
        "alerts": cmd_alerts,
        "routeconfig": cmd_routeconfig,
        "tripupdate": cmd_tripupdate,
        "rtpositions": cmd_rtpositions,
        "a2-compare": cmd_a2_compare,
        "a4-cadence": cmd_a4_cadence,
        "since-probe": cmd_since_probe,
        "rt-raw": cmd_rt_raw,
        "gtfs-audit": cmd_gtfs_audit,
        "e1-study": cmd_e1_study,
    }[args.cmd](args)


if __name__ == "__main__":
    main()
