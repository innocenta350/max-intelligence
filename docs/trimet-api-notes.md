# TriMet API Audit Notes (M0)

## 1. Audit Information

- Project: MAX Intelligence
- Milestone: M0 - Feed Audit & Access
- Audit status: COMPLETE — approved by owner
- AppID configured: YES (loaded from `.env`; never displayed; length 25, alphanumeric — matches TriMet's documented AppID format)
- Audit date: 2026-11-02 (main live window 03:34–03:55 UTC = 20:34–20:55 PDT Saturday evening)
- Tested by: agent executing M0, using `tools/m0_audit/audit_m0.py`
- Total TriMet HTTP requests issued by this audit: ~35 (exactly: 22 calls to the vehicles endpoint across all probes, 1 alerts, 4 routeConfig incl. failed-alias attempts, 1 TripUpdate, 4 VehiclePositions RT, 2 gtfs.zip incl. HEAD; retries were counted because they were observed request attempts; zero load-style testing against quota)
- All evidence files: `tools/m0_audit/evidence/*.json` (AppID-scrubbed; URL values redacted)

**Security note:** No AppID appears anywhere in this document, in the audit tool output, or in the
evidence files (verified by a content scan against the real AppID value). `.env` is Git-ignored
(verified; see §8).

## 2. Endpoint Verification

Summary of probe results (all request URLs were of the form `https://developer.trimet.org/<path>?appID=***REDACTED***&<params>`):

| Endpoint | HTTP Status | Content-Type (actual) | Params used (excluding credentials) | Result | Evidence |
|---|---|---|---|---|---|
| `/ws/v2/vehicles` | 200 | `application/json` | *(none)* | PASS | `vehicles_snapshot.json` |
| `/ws/v2/alerts` | 200 | `application/json` | *(none)* | PASS | `alerts_snapshot.json` |
| `/ws/V1/routeConfig` (all routes) | 200 | `application/json` | `json=true` | PASS | `routeconfig_all.json` |
| `/ws/V1/routeConfig` (routes=90,100,190,200,290 + dir=true + stops=true) | 200 | `application/json` | `routes=90,100,190,200,290&dir=true&stops=true&json=true` | PASS | `routeconfig_selected.json` |
| `/ws/V1/routeConfig` with `route=<comma list>` (documented alias) | 400 | `application/json` | `route=90,100,190,200,290&dir=true&stops=true` | FAIL (see note) | `routeconfig_selected.json` |
| `/ws/V1/TripUpdate` | 200 | `application/grtfeed` | *(none)* | PASS (decodes; see notes) | `tripupdate_snapshot.json` |
| `/ws/V1/VehiclePositions` | 200 | `application/grtfeed` | *(none)* | PASS w/ major caveat (nonstandard layout; see A2) | `a2_compare.json`, `rt_raw_layout.json` |
| `https://developer.trimet.org/schedule/gtfs.zip` | 200 (GET and HEAD) | `application/zip` | *(none)* | PASS | `gtfs_audit.json` |

**Key format discrepancy discovered (JSON key naming):** The official documentation describes response
fields using XML attribute syntax (`vehicle[@routeNumber]`). In the actual **JSON** responses, none of
the `@` prefixes are present: keys are plain (`queryTime`, `vehicleID`, `routeNumber`, `routeColor`,
`routeSubType`, ...). The audit tool therefore accepts both notations. Future M2 normalizers must use
the plain JSON keys observed here.

**HTTP 400 error body shape observed** (`route=`-comma-list case):
`{"resultSet":{"errorMessage":{"content":"Requested route was not a number: 90,100,190,200,290"}}}` —
useful for M7 error handling.

### 2.1 Vehicles `/ws/v2/vehicles` (PASS)

- Full snapshot at one instant: 287–289 vehicles total; rail vehicles 84–90 during the audit window.
- Response root: `resultSet` with `queryTime` (**milliseconds since epoch**; verified against HTTP
  `Date` header and wall clock — observed local-clock lead of 155–339 ms only).
- Observed keys on every vehicle (presence confirmed on all vehicles in the capture):
  `vehicleID, type ("bus"|"rail"), blockID, bearing, delay, direction, expires, extraBlockID,
  garage, inCongestion, lastLocID, lastStopSeq, latitude, loadPercentage, locationInScheduleDay,
  longitude, messageCode, newTrip, nextLocID, nextStopSeq, offRoute, routeColor, routeNumber,
  routeSubType, serviceDate, signMessage, signMessageLong, source, time, tripID`.
- Notable undocumented-in-docs fields observed and recorded: `routeColor` (hex string, e.g. `1359AE`
  for the Blue line), `routeSubType` (`Light Rail` — present directly on rail vehicles), `source`.
- Documented-as-bus-only-experimental fields `inCongestion` and `loadPercentage` **also appear on rail
  vehicles** in this feed (presence on every vehicle; values may be null/zero). Do not rely on them.
- Timestamp units: `queryTime`, `time`, `expires`, and alert/service times are all **ms since epoch**
  (see E1 for the `expires` study; `queryTime` verified against wall clock and the HTTP `Date` header).
- `delay` observed negative values (e.g. `-557` — per docs, negative means *late*). Sign convention
  verified against GTFS-RT in §3.
- MAX presence confirmed: rail vehicles on routes 100 (Blue; 32 vehicles at 03:35:56Z),
  190 (Yellow; 12), and others; `signMessageLong` carries headsign/full destination text.
- Route color/subtype carried on the vehicle itself permits MAX filtering without a second call.

### 2.2 Alerts `/ws/v2/alerts` (PASS)

- 38 alerts currently in effect at audit time.
- Fields observed: integer `id`, `begin` (ms epoch), `end` (ms epoch or `null` — null observed for
  long-running construction alerts), `system_wide_flag` (observed `false` in sample; no system-wide
  alert present in this window), `header_text` (empty string in several entries), `desc` (long text,
  includes "Stop ID 4612"-style references), `info_link_url` (null for many), child `route` objects
  (`id`, `type`, `no_service_flag`), child `location` objects (`id`, `desc`, `no_service_flag`).
- Sampling: first 6 alerts all construction-related, beginning dates 2025-08 through 2026-04.
- Note: the audit's sample list shows `system_wide_flag=false` for all sampled alerts; MAX scoping is
  via `route` child entries (type `R`) or `location` entries. Full MAX/PS/Tram route-type breakdown is
  deferred to M5 alerts filtering (approved scope).

### 2.3 RouteConfig `/ws/V1/routeConfig` (PASS)

- All-routes call returned 82 routes.
- `routeSubType` values observed: `Commuter Rail`, `Bus`, `BRT`, `Shuttle`, `Streetcar`, `Aerial Tram`, `Light Rail`.
- `type` values observed: only `B` and `R` — confirming that `type=R` alone is insufficient for MAX
  (it also covers Aerial Tram and Commuter Rail/WES).
- **Light Rail routes (exactly 5):**

| route | color | desc | directions (dir 0 / dir 1) | stops |
|---|---|---|---|---|
| 90 | `C41F3E` | MAX Red Line | To Portland International Airport / To Hillsboro | 63 |
| 100 | `1359AE` | MAX Blue Line | To Gresham / To Hillsboro | 88 |
| 190 | `FFC52F` | MAX Yellow Line | To Expo Center / To Portland City Center | 28 |
| 200 | `008342` | MAX Green Line | To Clackamas Town Center / To Gateway Transit Center | 18 |
| 290 | `D05F27` | MAX Orange Line | To Portland City Center / To Milwaukie | 27 |

- Direction descriptions include trailing spaces in some values (e.g. `"To Hillsboro "`); M1 should trim.
- `stops=true` returns per-direction stop lists with (`locid`, `desc`, `lat`, `lng`, `dir`, `seq`) —
  this is the planned M1/M4 stop source.
- **Parameter discrepancy vs. docs:** the documented alias `route=<comma list>` returns HTTP 400
  ("Requested route was not a number") for comma lists, while `routes=<comma list>` works. Use
  `routes=` in the future client.

### 2.4 GTFS-Realtime TripUpdate `/ws/V1/TripUpdate` (PASS)

- Content-Type `application/grtfeed`; ~208 KB body; decodes as GTFS-Realtime protobuf.
- Header: `gtfs_realtime_version "2.0"`, `incrementality 0 (FULL_DATASET)`, `timestamp` in **seconds**
  (10-digit; e.g. 1793590703). Note: TriMet JSON feeds use ms; GTFS-RT protobuf uses s (per spec). The
  M5 normalizer must handle the 1000x unit difference.
- 379 FeedEntities, all `trip_update`; 21,845 `stop_time_update` entries total in this capture.
- TripUpdate entity fields observed: `trip.trip_id` (e.g. `18309493`), `vehicle.id` (e.g. `4518`),
  `timestamp` (seconds), per-stop updates with `stop_sequence`, `arrival.delay`/`arrival.time`,
  `departure.delay`/`departure.time`, `schedule_relationship` (unspecified = SCHEDULED in this capture).
- **`trip.route_id` is ABSENT** from TripUpdate entities in this capture (also `start_time`,
  `start_date`, `schedule_relationship`). Consequence: TripUpdates cannot be scoped to MAX by
  `route_id` alone; M5 must join `trip_id` against GTFS `trips.txt` (M1 loads trips/stop_times).
- Negative delays appear as expected int32-negative encodings (two's-complement varint);
  a naive unsigned parse reads them as ~2^64-scale numbers (audit tool handles sign-extension).

### 2.5 GTFS-Realtime VehiclePositions `/ws/V1/VehiclePositions` (PASS-with-major-caveat)

- Content-Type `application/grtfeed`; ~270–284 entities; each FeedEntity carries only `id` (LEN) +
  `vehicle` (field 4, LEN). No trip_update/alert entities in the feed.
- At the trip level the feed matches the native JSON endpoint (see A2) — all RT `trip_id` values were
  also present in JSON, including all rail trip ids.
- **Wire-layout deviation from the official `gtfs-realtime.proto` (verified from raw bytes):**
  VehiclePosition messages carry, in order:
  - field 1 = TripDescriptor (fields 1 = trip_id, 5 = route_id) — standard;
  - field 2 = a submessage of **three fixed32 floats** (observed values: lat `45.5042`, lon `−122.5442`,
    bearing `270.0`) — this is a `Position`, but the official spec puts *VehicleDescriptor* (strings)
    at field 2 and *Position* at field 3;
  - fields 3/4/5 = varints (chose not to guess meanings);
  - field 7 = a submessage (inner field 6, fixed64) where the spec requires a `uint64` timestamp varint;
  - field 8 = a submessage of strings (observed `"4529"`, `"FX2 To Portland"` — block-id/headsign-like).
  - No standard `VehicleDescriptor` string, no spec-compliant `timestamp` varint at field 7, and no
    spec position at field 3 were observed.
- Consequence (architecture-relevant): a standards-compliant decoder (`gtfs-realtime-bindings` or
  generated `_pb2`) successfully decodes `trip` but yields NO usable position/timestamp for TriMet's
  VehiclePositions feed. **RT VehiclePositions is therefore excluded as a position source.** The
  approved hybrid architecture already relies on the native JSON endpoint for positions; this finding
  ratifies that decision and must be recorded for M5 (use RT only for TripUpdate; apply sign-extension
  care for negative delays).

## 3. Assumption Verification

### A1 - Endpoint URLs and Authentication

**Status: PASS**

Evidence:
- All four endpoints (`/ws/v2/vehicles`, `/ws/v2/alerts`, `/ws/V1/routeConfig`, `/ws/V1/TripUpdate`)
  returned HTTP 200 over HTTPS with `appID` passed as a URL query parameter.
- URL casing behaved as documented: lowercase `/ws/v2/...` for JSON services, capital `/ws/V1/...`
  for the legacy GTFS-RT and routeConfig paths. The GTFS-RT probe also exercised the documented
  trailing-slash variant `/ws/V1/TripUpdate/` path form; first-request validation used the non-slash
  path `/ws/V1/TripUpdate` (200). No redirect or casing mismatch was observed.
- See `fetch_json`/`fetch_raw` evidence blocks in every evidence file (all contain
  `url_redacted` entries with the same HTTPS host and an AppID placeholder).

### A2 - VehiclePositions Coverage

**Status: PASS (trip-level) — with an important sub-finding**

Evidence (`a2_compare.json`, `rt_raw_layout.json`):
- Native JSON snapshot: 284 vehicles (44 rail trip_ids at that instant); RT VehiclePositions feed: 284
  VehiclePosition entities; **240 distinct RT trip_ids, all of which were also present in the JSON
  snapshot** (240/240 trip-id match; rail trip-id match 44/44 — zero JSON rail trips missing from RT).
- RT trip-level `route_id` values include the rail routes `90`, `100`, `190`, `200`, `290`, plus bus
  route ids (see `rt_route_ids_observed` in the evidence file).
- Sub-finding on RT usability: as detailed in §2.5, the RT VehiclePositions protobuf layout deviates
  from the official specification (position encoded at field 2, non-spec fields 3-8). Standard GTFS-RT
  tooling cannot extract positions/timestamps from this feed. **Decision ratified:** keep native JSON
  `/ws/v2/vehicles` as the position source (approved architecture); do not use RT VehiclePositions in
  any milestone.

### A3 - GTFS Join Keys

**Status: PASS (routes) / PASS-with-limits (stops); full joining verified in M1**

Evidence (`gtfs_audit.json`):
- `routes.txt` rows with `route_type=0` include exactly `90`, `100`, `190`, `200`, `290` with
  `route_long_name` "MAX Red/Blue/Yellow/Green/Orange Line" and route colors matching routeConfig
  (`C41F3E`, `1359AE`, `FFC52F`, `008342`, `D05F27`). Route-id ↔ route-number join verified.
- Because GTFS `route_type=0` also includes **Portland Streetcar lines 193/194/195 (agency `PSC`)**,
  the M1 MAX filter must additionally exclude non-TriMet (Streetcar) rows; "route_type=0" alone is not
  sufficient (this supersedes the naive cross-check in the architecture doc).
- `stops.txt`: 6,031 rows / 6,031 distinct `stop_id`. From a live vehicle snapshot, 53 distinct
  locID samples (`nextLocID`/`lastLocID` of rail vehicles) were checked: 51 were found in GTFS
  `stop_id` (96.2%); 2 missing: `14024`, `14025` (consecutive ids; likely yard/monitor locations).
  Consequence: the M1/M2 join must tolerate locIDs without GTFS stop rows.

### A4 - Vehicle Update Cadence

**Status: PASS (per-request cadence), UNVERIFIED (long-run statistical behavior)**

Evidence (`a4_cadence.json`, `since_probe.json`, `vehicles_snapshot.json`, `e1_study.json`):
- `queryTime` advances with each request in near-real-time: deltas 4.26–4.45 s per ~4.0 s sleep across
  5 consecutive samples (the extra ~0.3 s per sample matches per-request latency). `queryTime` is a
  server-side generation timestamp, not a cached snapshot time.
- Rail count was stable at 90 across one ~20 s run; vehicle-level `time` freshness ranged from
  <1 s to **915 s** (one long-stopped train). Most vehicles' `time` is seconds-old; stale outliers exist.
- `expires − time` for all rail vehicles at one instant: min 600.0 s, median ~827–840 s, max ~1844 s —
  i.e. a 10–31 minute expiry horizon varies per vehicle. 10 s polling + 5 min full reconciliation
  remains reasonable (verdict supported; not "proven" for long-term behavior — the M2 live soak is the
  real check).
- `since=` is **not strictly incremental**: an immediate follow-up request with `since=<queryTime>`
  returned 48 vehicles, of which 37 had `time > since` and **11 did not** (fed additional state changes
  or re-sent entries). Implementation guideline confirmed for M2: merge by `vehicleID`; treat absence
  from an incremental response as non-removal; rely on reconciliation for removals.
- Watch item (anomaly): rail count fluctuated once between successive snapshots during the audit
  (88 → 44 → 88/90 across a few minutes). Samples came from the same endpoint with identical params.
  Not conclusive (could be genuine end-of-evening-service transitions or a feed-side transient); the
  M2 soak test will quantify whether such swings recur.

### A5 - Static GTFS Refresh

**Status: PASS (version metadata + accessibility); UNVERIFIED (actual refresh cadence)**

Evidence (`gtfs_archive`):
- `https://developer.trimet.org/schedule/gtfs.zip`: HTTP 200, `Content-Length 43,467,097`,
  `Last-Modified: Wed, 21 Oct 2026 16:31:08 GMT`, `Accept-Ranges: bytes`, `application/zip`.
  The version stamp inside is reproducible: `feed_info.feed_version = 20261011-20261021-0903`
  (service period 2026-10-11 → 2027-02-27). The version string embeds the build date (2026-10-21),
  which can be compared against `Last-Modified` to detect refreshes in the M2/M5 cache logic.
- Zip member files (20): `agency.txt, feed_info.txt, calendar.txt, calendar_dates.txt,
  fare_*.txt (several), routes.txt, route_directions.txt, shapes.txt, stop_features.txt, trips.txt,
  stop_times.txt, transfers.txt, stops.txt, linked_datasets.txt` — both unplanned
  `route_directions.txt` and `linked_datasets.txt` unofficial files exist, as documented.
- Refresh cadence itself cannot be measured inside a single audit window (we only see one
  `Last-Modified` snapshot at ~16 days old). The feed-version caching approach in the plan covers this;
  actual refresh behavior will be observed during M2/M5 operation.

## 4. E1 - Vehicle Expiration Verification

Objective: determine the units and actual feed behavior of `expires`.

**Status: PASS — all six sub-checks verified live** (evidence: `e1_study.json`;
follow-up observation window 03:49–03:55 UTC 2026-11-02).

1. **Units:** `expires` is **milliseconds since epoch** (same clock as `queryTime`/`time`).
   Ratio check `expires/localNow ≈ 1.0000000` across two independent runs; observed values include
   both second-aligned (…975000) and ms-precision (…9386) values — treat as plain ms.
2. **Wall-clock comparison:** selected vehicle 231 `expires=1793591639386` (03:53:59.386Z) vs local
   now: local clock lead 237/240/339 ms in different samples — negligible; both clocks are UTC epoch
   ms. Reference clock for removal logic: **UTC epoch milliseconds**.
3. **Does the default feed stop returning a vehicle after expiration?** YES.
   - Pre-expiry polls (−248 s, −97 s): present.
   - Poll at queryTime = expires **+3.163 s**: absent (cycle 2).
   - Supplementary default poll at expires **+3.433 s**: absent.
   - During the same window the client's own rail count ratio stayed ~86–90 vehicles, so this was not
     a bulk feed outage; the individual entry disappeared at expiry.
4. **Does `showStale=true` return expired entries?** YES.
   - Poll at expires **+6.811 s** with `showStale=true`: vehicle 231 present again; the returned
     vehicle count also grew substantially (295 total vs ~86 rail/271 default at the same period),
     consistent with the documented "expired entries are also included when showStale=true" behavior.
   - Also observed: `expires` did NOT extend during two pre-expiry polls (decoded `expires_extended:
     false`), i.e. this vehicle's expiry window does not slide as newer position updates arrive
     (hypothesis: `expires = last_report_time + fixed horizon`, with horizon varying 600–1844 s by
     vehicle — see A4 stats; not probed further).
5. **Documented vs observed:** TriMet's docs say the entry "should be discarded if no new position
   information is received" and that the default feed omits entries whose `expires` has passed
   (with `showStale=true` re-including them). Live behavior matched exactly, within ~3 seconds of the
   expiry moment in both default and stale probes.
6. **Removal logic reference clock and unit for M2:** UTC epoch **milliseconds**; drop a vehicle when
   `now ≥ expires` (with `queryTime > expires` as the server-side equivalent), and treat consistency
   with reconciliation as the second removal path (ghost-vehicle veto). `showStale=true` may be used
   by the audit tooling for diagnostics but is NOT used in the live pipeline.

Sub-check summary (from the successful full run, vehicle 231):

| Check | Result | Detail |
|---|---|---|
| 1. ms vs s | PASS | ratio 0.9999999961; observed ms-precision value |
| 2. wall-clock compare | PASS | clock lead 237–339 ms |
| 3. post-expiry default behavior | PASS | absent at +3.163 s / +3.433 s (repeated) |
| 4. showStale=true behavior | PASS | present again at +6.811 s; totals grow 271→295 |
| 5. docs vs observed | PASS | matches, within seconds |
| 6. removal clock | PASS | UTC epoch ms; use now ≥ expires; reconcile also required |

## 5. AppID Quota Verification

Documented default allowance (per TriMet's public docs and the approved plan): **1,000,000 requests
per day**.

### Observed live behavior (evidence: all response_headers_sanitized blocks)

- The working AppID is accepted on every endpoint with status 200 (see §2).
- **No quota-related response headers were present** in any observed response. Headers seen across
  all captures: `Date, Server (Apache), Content-Type, Content-Length / Transfer-Encoding, Vary,
  Via, Connection, Keep-Alive, X-Frame-Options, Access-Control-Allow-Origin ( * ),
  Strict-Transport-Security`. No `X-RateLimit`, `X-Quota`, or similar.
- No HTTP 429 / quota-error bodies were observed (audit never approached quota scale).
- The audit made ~35 requests in ~25 minutes — three orders of magnitude below any plausible limit.

### Statuses

| Quota sub-question | Status | Basis |
|---|---|---|
| AppID accepted | PASS | direct observation (§2, all endpoints 200) |
| Quota headers exposed | FAIL-observed (none present) | response headers captured |
| Counting across endpoints shares one allowance | UNVERIFIED | not observable without load or TriMet-side data |
| Retries count toward allowance | UNVERIFIED | not observable without triggering a retry burst |
| Daily reset time | UNVERIFIED | not documented on recordable pages; not observable in one day |
| 1,000,000/day default documented | Verified as documented | approved plan records it as documented |

### Conclusion

Counting/retry-share/reset behavior cannot be verified ethically or practically within M0. The plan's
conservative self-throttling (3 sources ≈ 26,000 req/day at 10 s polling ≈ 2.6% of the documented
default) with a configurable budget/reset already encodes the safe assumptions. M7's quota unit tests
must test OUR counting behavior (including retries) against a configurable budget — not against an
assumed TriMet-side behavior.

## 6. Additional Observations

- **JSON key naming:** docs' `@` prefixes are absent in the actual JSON (all four JSON endpoints).
- **`route=` parameter alias fails** for comma lists (HTTP 400); `routes=` works — client must use `routes=`.
- Standardized failure body shape: `{"resultSet":{"errorMessage":{"content":"..."}}}`.
- Unplanned-in-docs but useful fields in vehicle responses: `routeColor`, `routeSubType`, `source`.
- `extraBlockID` (docs name it `extrablockID`) appears in vehicle JSON; treat naming as
  case-preserved `extraBlockID`.
- Timestamp unit map (verified): JSON feeds ms epoch; GTFS-RT protobuf seconds epoch (per spec);
  alert `begin`/`end` ms epoch with `end` often `null`.
- GTFS-RT TripUpdate lacks `route_id` in TripDescriptor in this capture → MAX scoping requires a
  trips.txt join (M1/M5 order note).
- GTFS `route_type=0` includes Portland Streetcar (193/194/195, agency `PSC`): M1's MAX filter must
  use routeConfig `routeSubType=Light Rail` and/or GTFS route_id whitelist {90,100,190,200,290}.
- Route direction descriptions contain trailing spaces; normalize when displaying.
- Two locIDs not matched in GTFS stops (14024, 14025): tolerate unknown locIDs in joins.
- Audit tool history: three E1 evidence-capture runs crashed or short-circuited before completing
  (first run: audit-script source file was modified under a running process; second run: a `None`
  handling bug; third run: flow-order design issue). The fourth run completed the full checklist.
  These were audit-tool defects, not feed issues. Earlier partial E1 runs still produced valid
  evidence (unit ratio, one expiry-absence observation each).

## 7. Final M0 Checklist

| Requirement | Status | Evidence |
|---|---|---|
| AppID configured securely | PASS | `.env` (25-char alphanumeric, not displayed); `audit_m0.py envcheck` |
| Vehicles endpoint tested | PASS | `vehicles_snapshot.json` |
| Alerts endpoint tested | PASS | `alerts_snapshot.json` |
| routeConfig endpoint tested | PASS | `routeconfig_all.json`, `routeconfig_selected.json` |
| TripUpdate endpoint tested | PASS | `tripupdate_snapshot.json` |
| A1 verified | PASS | all probes 200 HTTPS, AppID as URL param |
| A2 verified | PASS (trip-level) + sub-finding | `a2_compare.json` |
| A3 verified | PASS (routes; locIDs 51/53) | `gtfs_audit.json` |
| A4 verified | PASS (request cadence) | `a4_cadence.json`, `since_probe.json` |
| A5 verified | PASS (version metadata); refresh cadence UNVERIFIED | `gtfs_audit.json` (+HEAD) |
| E1 verified | PASS (all six sub-checks) | `e1_study.json` |
| Quota behavior investigated | PARTIAL (documented=1M/day; counting/retry/reset UNVERIFIED) | header evidence, §5 |
| `.env` Git-ignored | PASS | `git check-ignore .env` → `.gitignore:2`; `git status --ignored` shows `!! .env` |
| No credentials exposed | PASS | content scan of all evidence files + docs: no AppID matches |

## 8. Security Summary

- `.env` is ignored by Git (verified with `git check-ignore -v .env` and `git status --ignored`).
- The audit tool never prints the AppID: a single `redact()`/`***REDACTED***` path guards all output;
  every evidence write goes through `_scrub()`.
- A content scan of all `tools/m0_audit/evidence/*.json` files against the real AppID value found
  zero matches.
- No credentials appear in this document (§1 records only the length/alphabet-class of the AppID).

## 9. Unresolved Questions / Watch Items

1. Quota: counting semantics, retry counting, cross-endpoint sharing, and the actual daily reset
   clock — UNVERIFIED (not ethically observable in M0). Mitigation: conservative self-throttle +
   configurable budget/reset in the app.
2. The rail-vehicle count dip anomaly (88 → 44 → 90 within minutes) — cause unknown; watch during the
   M2 live soak.
3. RT VehiclePositions nonstandard protobuf layout — does TriMet publish their actual .proto anywhere?
   (Not in the audited docs.) No follow-up needed for the approved architecture (RT positions are
   excluded), but it would clarify if this ever changes.
4. Exact `expires` computation (fixed vs refreshed horizon per vehicle) — not fully probed; M2 may
   optionally log observed `expires − time` distributions to test the stability of the 600–1844 s range.
5. locIDs `14024` / `14025` — semantic role unknown (yard/monitor?); confirm during M1 stop loading.
6. `systemWideOnly` and system-wide alerts were not exercised (no system-wide alert was live during
   the audit window) — alerts filtering tests in M5 should use recorded payload mocks where possible.

## 10. M0 Completion Decision

**Status: COMPLETE — approved by the owner.** The findings above (exclusive JSON position source; routeSubType/whitelist MAX filter; ms-epoch `expires`; non-strict `since=`; `trips.txt` join for TripUpdates; `routes=` parameter) are now implementation constraints for M1–M7.

All four endpoints verified working with the configured AppID; all five plan assumptions resolved
(3 PASS / 1 PASS-with-limits / 1 partially verified for refresh cadence); E1 fully verified with live
behavioral evidence; quota documented-vs-unverified separation recorded; `.env` and evidence security
verified. No later-milestone work was started (no FastAPI app, no React frontend, no pipeline, no GTFS
loader implementation — the audit only read gtfs.zip to verify metadata and join keys).
