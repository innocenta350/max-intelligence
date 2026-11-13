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

## 11. M2 contract verification (S8)

- Milestone: M2, Stage S8 - Feed Contract Verification (investigation-only)
- Status: COMPLETE — evidence collected and analyzed; owner decisions pending
- Verification date: 2026-11-07 (Part A 03:51-04:13 UTC; coordinated window 04:16-04:26 UTC; Part B 04:27-04:31 UTC)
- Executed by: agent executing S8, using `tools/s8_contract/probe_s8.py`
- Total TriMet HTTP requests issued by S8: **101** (hard cap 150; base allocation 101/110 used,
  retry pool 0/20, reserve 0/20; every request transmitted exactly once — zero failures, zero
  retries, zero ambiguous outcomes). Per-investigation reconciliation is in
  `tools/s8_contract/evidence/request_budget.json`.
- All evidence files: `tools/s8_contract/evidence/*.json` plus raw TripUpdate protobuf captures
  (`tripupdate_capture_01..10.bin`, credential-free by construction). AppID scan of every file
  under `tools/s8_contract/`: zero occurrences.
- Governor verification: all ten dry-run checks passed against a loopback server before any live
  request (`evidence/governor_dryrun.json`); zero live requests were used for verification.

### 11.1 Part A — re-verification of M0 findings

| # | Item | Verdict | Evidence / notes |
|---|---|---|---|
| A1 | Vehicles schema | **UNCHANGED** | 30 keys, identical set to M0 (`a_reverify.json`); plain keys, zero `@`-prefixed keys; key presence confirmed across all 323 vehicles |
| A2 | Timestamp units | **UNCHANGED** | `queryTime`/`time`/`expires` all ms-since-epoch; clock skew: local − queryTime +25/+48 ms; HTTP `Date` − queryTime −163/−458 ms (Date has 1 s resolution) |
| A3 | `expires` semantics | **UNCHANGED** | 3 rail vehicles followed through expiry: default feed omitted all three at +3.7-4.3 s past expiry; `showStale=true` re-included them (rail 79→99); `expires` did not extend pre-expiry (3/3); horizon (`expires − time`) 600.0-1885.6 s across the campaign (M0: 600-1844 s) |
| A4 | `since=` behavior | **UNCHANGED (stronger)** | Not strictly incremental, more pronounced than M0: follow-up 1 returned 195 vehicles of which **106 had `time ≤ since`** (M0: 11/48); follow-up 2: 9/137. Merge-by-`vehicleID` design confirmed; absence from an incremental response must never cause removal |
| A5 | Alerts schema | **UNCHANGED** | Field set unchanged; `end` null observed; route children carry `id`/`type`/`no_service_flag`; location children present on some alerts |
| A6 | TripUpdate feed | **UNCHANGED** | Decodes as GTFS-RT 2.0; `incrementality 0 (FULL_DATASET)`; `trip.route_id` absent (0 of 424 entities); header `timestamp` in seconds; 424 entities / 24,244 stop_time_updates in capture 1 |

### 11.2 Part B — new investigations

**B1 — TripUpdate incrementality (qualified conclusion).** 10 successful responses spanning
9.0 minutes (one per minute in the coordinated window): `incrementality_raw = 0 (FULL_DATASET)`
in 10/10 responses; `gtfs_realtime_version "2.0"` throughout. **A finite sample cannot prove
DIFFERENTIAL never occurs.** Qualified conclusion (a): FULL_DATASET-only observed, sufficient for
initial implementation **provided** S12 rejects any non-FULL_DATASET response whole and preserves
previous state (safe degradation mandatory in either case). Evidence: `b1_incrementality.json`.

**B2 — TripUpdate → static GTFS matching (all 10 captures, M1 dataset `dataset-e642a21ab45100a7`).**

| Measure | Value |
|---|---|
| Total TripUpdate entities across 10 captures | 4,166 |
| Entities with usable `trip.trip_id` | 4,166 (100%) |
| Invalid or incomplete entities | 0 |
| Distinct trip ids | 451 |
| Distinct trip ids matching M1 `trips.txt` (67,957 trips loaded) | **451 (100%)** |
| Unmatched ids | 0 (no samples; no format anomaly) |
| Entity-instances matched to MAX routes (90/100/190/200/290) | 713 |
| Entity-instances matched to non-MAX routes | 3,453 |
| Entities with `vehicle.id` | 4,166 (100%) |
| `vehicle.id` joining to vehicles-feed `vehicleID`s | 4,162/4,166 (99.9%) |

TripUpdate→GTFS matching is a pure `trip_id` join with a 100% hit rate against the M1 dataset;
MAX filtering needs only the route whitelist. Evidence: `b2_match_report.json`.

**B3 — Alert id stability.** 32 alerts in every one of 5 captures (spaced 2 min); identical id
set throughout (0 appear/disappear); zero content changes among recurring ids. Alert `id` is a
stable dedup key over the observed window. Evidence: `b3_alert_stability.json`.

**B4 — Alert route namespace.** 29 distinct route-child ids across capture 1; **all 29 match
GTFS `route_id` values** (27 also match `route_short_name`); zero unmatched. 31 route-scoped
alerts, 0 system-wide, 0 location-only in this window. Proposed MAX-classification rule: an alert
is MAX-relevant if `system_wide_flag` is true OR any route child id ∈ {90,100,190,200,290};
location-only alerts (none observed; M0 saw location children exist) go to diagnostics with
`desc` inspection, never auto-classified. Evidence: `b4_alert_routes.json`.

**B5 — Cadence and freshness (coordinated 10-minute window; monotonic scheduling, no overlap —
max schedule lateness 0.016 s).**

| Feed | Polls | Response size | Change fraction per interval | `queryTime` lag behind poll |
|---|---|---|---|---|
| Vehicles @10 s | 60/60 ok | 192-199 KB | 241-268 of ~280 vehicles (~90%) changed per 10 s; new 0.14/poll, gone 0.31/poll | −34 to +273 ms (avg 70 ms) |
| TripUpdates @60 s | 10/10 ok | 217-232 KB | 302-332 of ~415 trips (~75%) changed per 60 s; header `timestamp` advanced exactly 60 s per poll | fresh each poll |
| Alerts @120 s | 5/5 ok | ~24 KB | 0 changes across 10 min (32 alerts constant) | n/a |

`expires − time` horizon re-measured across the window: 600.0-1885.6 s (median ~799 s) —
consistent with M0's 600-1844 s. Evidence: `b5_cadence.json`.

**B6 — `showStale=true` (5 paired polls).** Default vs showStale totals: ~301-305 vs ~331-335
(rail 79 vs 99); stale-only set consistently **30 vehicles, all rail**, expired by 88 s to
**18.5 hours** (time age up to 18.7 h) — end-of-service rail vehicles parked for the night.
Recommendation: **do not adopt `showStale=true` in the live pipeline**; it would re-introduce
precisely the ghost vehicles the M2 removal hierarchy exists to prevent. Keep it audit/diagnostic
only (as M0 concluded). Evidence: `b6_showstale.json`.

**B7 — `gtfs-realtime-bindings` evaluation (isolated throwaway venv outside the repository;
project venv and manifest untouched).** Library 3.0.0 with protobuf 7.36.2 decoded the captured
FeedMessage (424 entities); 2,702 negative delays decoded identically by library and stdlib
parser (sign-extension verified); cross-check of 12,868 arrival/departure delay pairs:
12,868/12,868 agreement. Verdict: **SUITABLE**. Adoption into the project dependency manifest is
a separate, later, explicitly authorized decision. Evidence: `b7_library_eval.json`.

**B8 — HTTP mechanics.** Vehicles and alerts honor `Accept-Encoding: gzip` (vehicles 190,198 →
24,242 bytes; alerts 23,673 → 4,733 bytes); **TripUpdate does not compress** (219,823 bytes
identical with gzip and identity). Trailing-slash variants (`/ws/v2/vehicles/`,
`/ws/V1/TripUpdate/`) return 200. Evidence: `b8_http_details.json`.

### 11.3 Proposed polling intervals and freshness thresholds (owner decides)

| Feed | Proposed poll interval | Proposed freshness thresholds |
|---|---|---|
| Vehicles | 10 s (incremental `since=` + periodic full reconciliation per architecture) | HEALTHY if last success ≤ 30 s; DELAYED if `queryTime` lag > 5 s; STALE if last success > 30 s |
| TripUpdates | 60 s (feed regenerates per minute; ~75% of trips change per interval) | HEALTHY if last success ≤ 120 s; STALE if > 120 s |
| Alerts | 120 s (static over the observed window) | HEALTHY if last success ≤ 360 s; STALE if > 360 s |

Basis: B5 measurements (section 11.2). These are proposals; the owner approves final values.

### 11.4 Decisions recorded for the owner

1. **`showStale=true`: do not adopt** for the live pipeline (B6: stale entries up to 18.5 h
   expired, all rail end-of-service vehicles).
2. **TripUpdate incrementality:** qualified conclusion (a) — FULL_DATASET-only observed (10/10,
   9.0 min); S12 must implement whole-response rejection of non-FULL_DATASET responses;
   entity-level merging deferred with this risk recorded.
3. **`gtfs-realtime-bindings`: SUITABLE** (3.0.0; protobuf 7.36.2; 12,868/12,868 cross-check;
   negative delays verified). Adoption is a separate, later, explicitly authorized decision.
4. **Polling intervals / freshness thresholds:** proposals in section 11.3, pending owner approval.
5. **Alert MAX-classification rule:** `system_wide_flag` OR route child id ∈
   {90,100,190,200,290}; location-only alerts to diagnostics (rule evidence-limited for
   location-only and system-wide cases — none were live in the window).

### 11.5 S8 limitations

- The B1 sample spans 9 minutes of one late-evening window; DIFFERENTIAL cannot be ruled out
  (safe degradation in S12 is mandatory regardless).
- No system-wide or location-only alert was live during the window; the B4 rule's edge cases
  rest on M0 observations plus conservative design, not S8 live evidence.
- B2's 100% match rate reflects late-evening service (451 active trips); a daytime window would
  exercise more trips, but the join mechanism itself is id-proven.
- TripUpdate captures are ~10 minutes apart at 60 s granularity; sub-minute cadence behavior
  was not measured.

## 12. S10 — Feed Polling & Quota Management (M2)

Stage S10 implements the REV 3 polling plan on top of the S8-verified endpoint facts and the
S9 normalizers. All work below was verified offline (injected clocks, `httpx.MockTransport`, a
real 127.0.0.1 loopback server); **no live TriMet request was issued during S10**.

### 12.1 Modules added

| Module | Role |
|---|---|
| `backend/trimet/poll_types.py` | Frozen types: feeds, purposes, gate reasons, failure codes, `PollDelivery`, `VehicleCollectionEvidence`, quota/runtime telemetry snapshots |
| `backend/trimet/poll_errors.py` | Error taxonomy (`PollConfigError`, `LifecycleError`, `ProgrammerStateError`) |
| `backend/trimet/live_config.py` | `LiveConfig` (frozen, secret-redacting) + `load_live_config` (env > `.env` > defaults) |
| `backend/trimet/quota.py` | Shared request-budget governor: accounting, reset windows (DST-aware), cooldowns, clock trust, operator resolution, reinitialization |
| `backend/trimet/feed_http.py` | One-shot HTTP boundary per attempt: request construction, bounded streaming reads (decoded-byte caps), gzip/`identity` only, content-type checks, Retry-After parsing, credential-safe logging suppression |
| `backend/trimet/polling.py` | `PollingRuntime`: three worker threads, monotonic grid scheduling, vehicles watermark/authority logic, retry policy, delivery + telemetry, lifecycle, operator recovery |

Three workers share one governor; every transport handoff consumes one counted, single-use
admission. There are no refunds: abandoned in-flight admissions stay charged and are recorded
as unused.

### 12.2 Scheduling and vehicle authority (plan §5–§6)

- Monotonic grid slots anchored at runtime construction; elapsed slots are skipped, never replayed.
- Vehicle request purposes: `STARTUP_FULL` (first ever), `RECOVERY_FULL`, `SCHEDULED_FULL`
  (every `TRIMET_VEHICLES_FULL_SECONDS`), `INCREMENTAL` with `since=<queryTime watermark>`.
- The watermark commits only after an acknowledged sink delivery with a complete vehicle
  collection (key present, is a list, zero rejected records). Missing/null collections deliver
  an empty snapshot but do not commit; any record loss blocks the commit and forces a full.
  The evidence record distinguishes an absent `vehicle` key (`key_present=false`) from an
  explicit `vehicle: null` (`key_present=true, is_list=false`); both carry unknown counts.
- Out-of-order responses (older `queryTime`) are detected pre-sink: not published, watermark
  untouched, full forced. Equal `queryTime` is published without advancing (max merge).

### 12.3 Quota accounting and resets (plan §7)

Counters per window and lifetime: charged admissions, transport handoffs (retries as a subset),
responses, transport failures, feed rejections, accepted deliveries, unused admissions, and
admitted-but-not-yet-settled (`pending_admitted`). Lifetime counters use a strict per-event
convention: each event increments its lifetime counter exactly once when it happens, and
rollover/recovery clear only the current window without re-adding it, so lifetime totals equal
their event counts at all times. Refusal examples verified by tests:

- Budget 5, opening 0: the 5th admission is granted, the 6th is refused `quota_exhausted`.
- Opening 3 of 5: two grantable; `quota_initial_used = budget` refuses startup entirely.
- Warning fires at `ceil(0.8 × budget)` — including a high opening estimate.
- The opening estimate is scoped to its construction window: a nonzero, fully exhausted
  opening seed stops contributing at the next legitimate reset rollover, so an unknown-usage
  startup cannot pause polling beyond one reset (`quota_initial_used = budget` pauses until
  the first reset, not forever); server cooldowns, auth blocks and clock flags survive the
  rollover.
- Delayed handoffs re-admit into the refreshed window under its *current* gates and budget
  and return an explicit granted/refused decision; the runtime must not emit the HTTP attempt
  on a refused re-admission (the charge stays conservative and unused admissions are counted).
- Same-window recovery (`reinitialize_quota`) cannot undercut known current-window usage
  (opening-estimate contribution + charged admissions); cross-window recovery after a
  legitimate rollover correctly accepts a zero opening estimate.
- Reset boundary crossing resets window counters once; lifecycle counters never roll backward
  (a 40-day backward wall-clock jump cannot reopen a consumed window); a ~5-minute suspend
  with matching wall advance stays trusted; divergent movement marks `clock_untrusted` and
  halts admission until an operator `reinitialize_quota` (validates the expected UTC window id
  and does not undercut known current-window usage).
- Cooldowns: shared (429) gates all three feeds; per-feed (503) gates only that feed; a later
  declaration extends a wait, never shortens it; named unrepresentable holds are operator-only.
- Acknowledged limitation: the governor is in-memory per process, so quota accounting and pause
  state do **not** survive a restart or span multiple processes. Restart reconciliation is by
  operator-provided opening estimate (`TRIMET_QUOTA_INITIAL_USED` / `reinitialize_quota`).

### 12.4 Reset windows and DST (plan §7)

`TRIMET_QUOTA_RESET_TIME`/`TRIMET_QUOTA_RESET_ZONE` are application policy — TriMet's actual
reset clock is not verified. Verified tests: a 23 h spring-forward window (2026-03-08, where a
02:30 reset instant does not exist and the first valid local minute is used), a 25 h fall-back
window (2026-11-01 with an ambiguous 01:30 reset having two UTC occurrence boundaries), and
non-midnight local resets.

### 12.5 Retry policy (plan §8)

- Every attempt is counted; at most one bounded in-cycle retry per slot (configurable 1–2).
- 429: never an immediate retry; shared AppID pause at `max(60 s policy floor, valid server
  delay)`; missing/malformed/negative/zero values use the floor; repeated refusals only extend.
- 503: a valid positive Retry-After is a per-feed minimum delay; missing/invalid/zero values
  fall back to the normal bounded retry.
- A server wait is accepted only when it survives the *complete* conversion path: the parser
  must produce a finite float (integer figures beyond the float range, or beyond the
  interpreter's 4300-digit integer-string safeguard — which is never disabled — become the
  operator-only hold), and the runtime's checked duration/deadline arithmetic must accept it
  (`int(delay_s * 1000)` cannot overflow into infinity). A finite float such as 1e306 seconds
  that overflows the deadline forms is also an operator-only hold — never clipped to a shorter
  cooldown and never a worker crash; the hold is visible in feed telemetry and
  operator-resolvable with the existing 60-second guard.
- In-cycle retries only run when the wait fits before the next regular slot; waits are
  stop-event interruptible.
- `Retry-After` parsing tests: seconds, zero, negative, signed, non-ASCII bytes, past HTTP
  dates (→ 0), future dates, multi-day waits, and unrepresentable values.

### 12.6 Credential safety (plan §10)

- The AppID travels only as a query parameter; it is excluded from `LiveConfig` repr, telemetry
  snapshots, audit lines and error text; failure outcomes carry reason codes and numeric
  metadata, never `str(httpx_exception)` or response bodies.
- `feed_http.apply_credential_safe_logging()` (applied by `build_production_client`) pins
  `httpx`/`httpcore` loggers to WARNING and drops their records outright — their INFO request
  logs carry full URLs including `appID`, so the application must not rely on log level. Verified
  by a root-DEBUG capture: the fake sentinel appears nowhere in logs, output or telemetry.
- S10 test suites install a socket guard that refuses any non-loopback DNS/connect before
  contact; a deliberate `developer.trimet.org` request is demonstrably refused (`appID` guard
  assertion) in `tests/test_trimet_polling_integration.py`.

### 12.7 Verification evidence

- Full suite: **685 passed, 17 skipped, 0 failed** (baseline before S10 stayed 483 passed /
  17 skipped / 0 failed — reproduced unchanged after the correction pass; the 17 gated
  live-feed tests remain skipped).
- S10 test files collect **202** tests: `test_live_config.py` (31), `test_trimet_quota.py`
  (56), `test_trimet_feed_http.py` (55), `test_trimet_polling.py` (49),
  `test_trimet_polling_integration.py` (11 real-loopback tests). (The first-pass completion
  report's per-file list summed to 180 and its total read 663; the review's verified first
  pass was 664. Both reporting slips are corrected here. This list reflects final counts
  including the correction-pass regressions.)
- Dependencies installed in the existing venv (reviewed versions only):
  `httpx==0.28.1` (+ `httpcore 1.0.9`, `anyio 4.16.0`, `idna`, `certifi` transitives) and
  `tzdata==2026.6`; pinned in `requirements.txt` / `requirements-dev.txt`
  (`pytest==9.2.0` pre-existing version). No protobuf package is required — the GTFS-Realtime
  decoder is in-project (`backend/trimet/rtproto.py`).

### 12.8 S10 limitations

- Governor state is per-process, in memory: quota counters, cooldowns, auth blocks and clock
  distrust do not survive a process restart; after a restart the governor begins from the
  operator-supplied opening estimate, and any prior operator pause must be re-entered by the
  operator (the runtime keeps a `prior_pause_state: unknown` marker for the first cycle).
- Multi-process deployments would each keep their own budget counters — not safe for a shared
  AppID without external coordination (Phase 1 runs one process).
- No live TriMet soak has been run against the new polling code; the real-feed soak remains
  deferred (S14 work).

### 12.9 S10 correction pass (acceptance review, November 7 2026)

The owner's acceptance review reproduced seven defects with an offline check runner
(`s10-review-checks.py`, 0/8 before, 8/8 after). All fixes stayed inside the S10-scoped files:

1. **Opening seed retention across rollovers** (`quota.py`): the opening estimate is now
   scoped to `_seed_window_id` and contributes only while the current window is its
   construction window; a legitimate forward rollover stops the seed without touching
   cooldowns/auth blocks/clock trust.
2. **Recovery lower bound** (`quota.py`): same-window `reinitialize` compares the new
   estimate against the complete known current-window usage (seed contribution + charged);
   cross-window recovery semantics unchanged.
3. **Handoff re-admission** (`quota.py`, `polling.py`): `mark_handoff` refreshes its own
   reset window, re-applies the current gates and budget to a stale token, and returns a
   typed `HandoffDecision` (`granted` / `gate_reason` / deadline); the runtime never calls
   the transport boundary for a refused re-admission and records the gate telemetry.
4. **Feed-local operator holds in telemetry** (`quota.py`): `snapshot()` serializes
   `unrepresentable_scopes` from the stored string keys and returns immutable tuples;
   `declare_unrepresentable(PollFeed.ALERTS)` no longer raises `AttributeError`.
5. **Partial thread-start failure** (`polling.py`): `_spawn_workers` starts each thread
   inline (instead of registering all three first) and drops a never-started thread from
   `_threads` on `start()` failure; `_join_workers` additionally skips `ident is None`
   entries. The original spawn failure is preserved and the runtime returns to `STOPPED`
   with the recorded survivors.
6. **Explicit-null vehicle evidence** (`feed_http.py`): `capture_vehicle_evidence` tests
   `"vehicle"` membership instead of `.get() .. is None`; explicit null keeps
   `key_present=True, is_list=False, counts=None`.
7. **Retry-After representation limit** (`feed_http.py`, `polling.py`): the `10 000 000 s`
   policy cutoff was removed; the operator-only path now triggers only on genuine
   representation limits (see 12.5). `10 000 001 s` etc. are preserved exactly.

Regressions added for each finding (opening-seed expiry, recovery lower bound incl. the
runtime operator path, both handoff interleavings, local-hold snapshot readability, real
second-start failure, full evidence records, preserved long waits + overflow cases).

### 12.10 S10 follow-up correction pass (acceptance review, November 8 2026)

The review's follow-up runner reproduced two further defects (0/4 before, 4/4 after);
fixes again stayed inside the S10-scoped files:

8. **Lifetime totals double-counted at rollover/recovery** (`quota.py`): lifetime counters
   were incremented per event *and* had the whole current window added again at reset — one
   admission became `lifetime_charged=2` at midnight and at same-window recovery. The fold
   was removed: lifetime counters are strictly per-event, totals equal event counts, late
   (old-window) completions and separately charged re-admissions keep counting once each as
   their own events. Exact-value assertions added for charged, handoffs, responses,
   transport failures, feed rejections, deliveries and retry handoffs across rollover and
   recovery (`TestLifetimeReconciliation`).
9. **Retry-After conversion path incomplete** (`feed_http.py`, `polling.py`): the parser
   also classifies the interpreter's integer-string-digit-safeguard `ValueError` (e.g. a
   4,301-digit header) into the operator hold instead of a malformed-header short-retry
   fallback; and the runtime classifies a finite parsed delay whose `int(delay_s * 1000)`
   deadline conversion would overflow (e.g. `1e306` seconds) into the same hold *before* any
   failing conversion, so a 429 or feed-local 503 can no longer crash a worker with
   `OverflowError`. The hold now also appears in the feed's own telemetry gate reason, and
   both full runtime paths (429 shared / 503 feed-scoped) are covered over the real loopback
   server, including stopped-runtime operator resolution with the existing 60-second guard.


## 13. S11 - Static GTFS Matching (M2)

Implemented November 8, 2026 (plan REV 2, SHA-256 `5a40c480...1002087`). S11 attaches the
promoted static dataset to the S10 deliveries: route identity, a matching scheduled trip where
available, its direction/shape, and exact trip-local stop visits. It also separates MAX data from
other transit data without hiding genuine matching problems. Everything is lookup-based against
one captured immutable context per call - no file I/O, no archive rescans, no clock reads inside
matching, and no conversations with developer.trimet.org (all verification offline over synthetic
worlds; the not-yet-run live system stays exactly as M1/S10 left it).

### 13.1 Canonical public surface

- `prepare_matching_context(load_outcome: LoadOutcome) -> MatchingContext` (backend/trimet/match_index.py):
  the only production factory. It takes a *successful* M1 `LoadOutcome` (never a guessed bare
  network/directory pair and never a directory scan), reuses M1 `read_manifest` + the reuse-only
  `_zip_sha256` helper, cross-checks manifest paths against the supplied directory, streams the
  archive digest, reads `routes.txt`/`trips.txt` directly from the verified zip (stdlib zip/text/CSV;
  no extraction, no divergent extracted copies), and re-verifies identity + digest again just before
  publishing. Whole-context failure only: any defect raises `ContextPreparationError` and no partial
  context escapes (codes: `manifest_absent`, `manifest_unreadable`, `manifest_paths_mismatch`,
  `archive_unreadable`, `archive_hash_mismatch`, `source_member_missing`, `source_member_ambiguous`,
  `csv_structure`, `invalid_cell`, `identity_mismatch`, `index_inconsistency`).
- `build_matching_context(identity, network, route_rows, trip_rows) -> MatchingContext` - the one
  shared pure consistency builder used by *both* the production factory and the synthetic tests, so
  no unchecked test-only constructor exists. It validates duplicates before dictionary conversion
  (a dict constructor never silently erases a duplicate first), orphan trip-to-route references,
  invalid route types, MAX route/trip/direction/shape agreement against the M1 network and the
  authoritative `MAX_ROUTE_IDS`, and trip-local stop-sequence indexes (deep read-only proxies).
- `match_delivery(delivery: PollDelivery, context: MatchingContext) -> MatchedBatch` (backend/trimet/feed_match.py):
  the single adapter entry point for the three feeds.

### 13.2 Owner decisions realized

- All-trip lookup from the promoted dataset: the static trip-id lookup covers the full tables (MAX
  and non-MAX), so a known non-MAX trip classifies NON_MAX while an unknown trip stays UNCLASSIFIED -
  the MAX-only network lookup never re-labels anything on its own.
- Native `"rail"` on a static bus route (`route_type == 3`) is UNCLASSIFIED (`type_route_conflict`),
  retained for diagnosis and excluded from MAX output; known non-MAX rail services (Streetcar 193)
  filter normally with no diagnostic flood. Conflicts are evaluated **before** the ordinary
  known-non-MAX branch, so a rail vehicle on a bus route is never consumed by that filter.
- Vehicle type uses the exact literals `rail` / `bus` only; `R`, `Rail`, whitespace-only values and
  other strings are UNCLASSIFIED via `unknown_vehicle_type`. GTFS ids stay strings; JSON integers
  convert through their canonical decimal string only (lossless; no zero stripping, no repair,
  no proximity fallbacks).

### 13.3 Frozen vocabulary and records

`MatchReason` freezes the 17 exact spellings (pinned in the implementation prompt before code):
`unknown_route`, `unknown_vehicle_type`, `type_route_conflict`, `trip_not_reported`, `unknown_trip`,
`trip_route_conflict`, `direction_conflict`, `missing_shape`, `unusable_shape`,
`missing_stop_details`, `unknown_stop_sequence`, `unknown_stop_id`, `stop_id_sequence_conflict`,
`ambiguous_stop_visit`, `missing_stop_selectors`, `unsupported_operational_relationship`,
`diagnostics_overflow`. Raw payload content is never concatenated into reason values.

Exactly one primary classification per parsed record (`max_relevant` / `non_max` / `unclassified`);
the three counts always sum to the input count. Enrichment is kept strictly separate
(`matched` / `partial` / `unmatched` / `conflict`) and never upgrades relevance: a MAX vehicle with
an unavailable trip stays a MAX observation (`trip_not_reported`/`unknown_trip` -> unmatched;
trip-route disagreement -> conflict with the trip enrichment rejected); direction disagreement is an
*annotation* (`direction_conflict` field flag plus the `direction_conflict` reason) under PARTIAL, and
a missing reported direction filled from the matched trip carries explicit `direction_source =
"static_trip"` provenance. Annotation-only `unsupported_operational_relationship` (non-SCHEDULED
descriptor relationship or `is_deleted`) never downgrades a resolved MAX TripUpdate.

### 13.4 Trip-local stop resolution (backend/trimet/stop_match.py)

`resolve_stop_visit` implements the plan §9 matrix inside one resolved trip's own `stop_times` list
(never globally by stop id, never by array offset):

| Selectors | Outcome |
| --- | --- |
| sequence + stop ID | visit resolves only when the sequence exists and its stop ID agrees; otherwise explicit conflict `stop_id_sequence_conflict`, no fallback guess |
| sequence only | that exact visit resolves; unknown sequence is `unknown_stop_sequence` (the reported sequence is never backfilled by a stop ID) |
| stop ID only | resolves only when the id occurs exactly once in this trip; repeated visits are `ambiguous_stop_visit` and require the sequence |
| neither | `missing_stop_selectors` |

Sequence values are values, not positions: noncontiguous lists and explicit `0` resolve normally.
A resolved visit whose stop is missing from the catalog keeps the visit and prediction and is
annotated `missing_stop_details` - no invented stop metadata, nothing discarded.

### 13.5 Diagnostics cap with exact totals (plan §12)

Per batch, matching diagnostic samples cap at 200 entries including one `diagnostics_overflow`
summary; 201 events become 199 samples + the overflow reporting *2 omitted*. The per-reason
`reason_totals` remain exact independent of truncation (verified at 199/200/201/220/500 events;
e.g. 500 events -> 500 exact total, 301 omitted, samples `...`+overflow `301 matching diagnostics
omitted beyond the 200-entry cap`). Source parse diagnostics from S10 travel in the batch
*further separately, referenced intact* (`source_diagnostics`). Routine known non-MAX records
aggregate as counts only.

### 13.6 Files added (11) and the single docs append

Modules: `backend/trimet/match_errors.py`, `backend/trimet/match_types.py`,
`backend/trimet/match_index.py`, `backend/trimet/stop_match.py`, `backend/trimet/feed_match.py`.
Tests: `tests/_s11_helpers.py` (offline synthetic world + promoted-dataset factories + S9/delivery
builders + socket guard), `tests/test_trimet_match_types.py`,
`tests/test_trimet_match_index.py`, `tests/test_trimet_stop_match.py`,
`tests/test_trimet_feed_match.py`, `tests/test_trimet_match_delivery.py` (136 tests total). Only
existing file modified: this section's append to `docs/trimet-api-notes.md` (this document).

### 13.7 Limits

- The reason-code totals are exact per batch, but diagnostic *samples* truncate at the 200-entry cap
  by design; consumers needing every occurrence must read `reason_totals`.
- Synthesis-only identities (`DatasetIdentity` built directly) are clearly synthetic and make no
  claim that a real archive was verified; `source_directory_name` is descriptive only.
- Direction provenance fills only fill `direction_source` when the static trip actually supplies a
  direction; blank static direction plus missing reported direction stays honest (both None).
- Alert matching adds no S11 diagnostics of its own; the unchanged S9 classifier verdict and its
  diagnostics carry inside the referenced classification result.

### 13.8 Correction pass (owner review, November 8, 2026)

Two P2 findings from the independent owner review were reproduced and corrected; both fixes stay
inside the same S11 file scope, and every check the review reported failing now passes:

- **Visitless MAX trips no longer abort matching.** M1's `parse_stop_times_filtered` builds
  `by_trip` only from rows seen, so a known MAX trip can legitimately have zero stop-time rows;
  such a trip is accepted by preparation and carried with **empty trip-local indexes**
  (`match_index._build_context` seeds an index for every indexed trip). A requested stop selector
  against that empty index resolves UNRESOLVED with the existing reason codes
  (`unknown_stop_sequence` / `unknown_stop_id`), keeping the known MAX classification, raw record
  and valid trip/shape references; that record's enrichment is PARTIAL while unrelated records in
  the same delivery are untouched. Regression tests cover both feed paths and same-batch survival
  (`test_visitless_*` in `tests/test_trimet_match_index.py` and `tests/test_trimet_feed_match.py`).
- **Malformed CSV quoting now honors the typed contract.** `csv.Error` is not a `ValueError`
  subclass, so unterminated quoted fields (`unexpected end of data`) and data immediately after a
  closing quote (`',' expected after '"'`) escaped the earlier handlers and surfaced as raw
  `_csv.Error`. `_member_records` now translates any `csv.Error` into
  `ContextPreparationError(code='csv_structure')` with bounded member/line context (the messages
  are short fixed phrases; no raw row content). Both named regressions live in
  `tests/test_trimet_match_index.py`; the valid quoted-comma behavior is unchanged.
## 14. S12 - In-memory LiveState and safe full-snapshot reconciliation (M2)

Stage S12 adds the in-memory state store that the S10 polling runtime will
deliver into and the S13 API surface will read from. Binding plan:
`s12-livestate-plan-rev2.md` (SHA-256 `0d3d517d19f8d4e9de6067d149c5e8573bc0495e4cf158d156ba39ecd59b5aff`),
executed per the owner-approved implementation prompt at `main` checkout
`6dfde7f1b75a37f74c155f051d0c2bbfd0b45d4a`. This section documents the
implemented behavior, the frozen limits and the actual verification results.

### 14.1 What S12 adds

Eleven scoped paths: five new backend/trimet state modules
(`state_types.py`, `state_errors.py`, `live_state.py`, `state_sink.py`),
five new collected test modules plus one non-collected helper
(`tests/_s12_helpers.py`, denied collection by its underscore name), and
this one documentation append. No conftest, `pytest.ini`, dependency or
existing-file change; no commit, push or S13/S14 work.

The store (`LiveState`) holds the latest accepted observation per vehicle
ID across **all three** S11 relevance classes (NON_MAX and UNCLASSIFIED
records are retained source observations; the MAX projection is a filtered
view, never the source collection), the latest matched TripUpdates and
alerts views, per-feed source-time high-water marks, one global absence
fence, and bounded decision telemetry — published as immutable roots under
one store lock. Reading is snapshot-based: previously returned snapshots
never change.

### 14.2 Transactions, sessions and publication

- One store lock serializes capture/validate/publish. S11 matching and
  context re-matching run **outside** the lock; publication uses bounded
  optimistic retry — exactly **3 total attempts** per operation, after
  which `concurrent_change` is raised and the winning root is preserved.
- Every delivery is all-or-nothing: validate, match, reduce into a private
  candidate, capacity-check, then publish atomically or raise a typed
  precommit error with nothing published. No anticipated validation error
  can occur after publication.
- Sessions: one active session per store (`session_already_open` on a
  second). A session owns the per-feed sequence/replay namespace; opening
  and revoking are revision-bumping root mutations. Revocation is an
  atomic fence — an old-session candidate blocked in matching fails
  revalidation (`invalid_session`) and publishes nothing, even though its
  matching began earlier.
- Sequence rules per session/feed: lower sequence is a stale no-op; equal
  sequence with byte-equal delivery is a duplicate no-op; equal sequence
  with different content is `sequence_collision`; higher sequence is
  eligible. No-ops (replay, stale, ignored-older) never mutate the root or
  revision — they are counter-only observations. Session replacement
  clears sequences/replay but never source-time ordering, vehicles,
  fences or the static generation.
- Higher-sequence deliveries whose equal-source-time content is ignored or
  unchanged commit a **metadata-only** transaction: the processed sequence
  and replay envelope are booked (revision +1) while the source-content
  view and its receipt time stay intact, so a later conflicting reuse of
  that sequence is still detected.

### 14.3 Vehicle merge and explicit reconciliation authority

- Ordering uses the feed's own source `queryTime`; receipt/request wall
  times travel as provenance and never substitute for source order. An
  older queryTime delivery is ignored whole, including absence removal.
- Per vehicle ID, observations rank lexicographically by
  `(vehicle.time_utc_ms, source queryTime)`. A lower rank never replaces
  the retained record; an identical rank with conflicting raw content
  retains the existing value and records `observation_tie_conflict`;
  duplicate IDs within one delivery reduce by the same rank with no
  arbitrary first/last-wins, and the ID stays in source presence.
- Authority gates (all required for an absence pass): full-purpose enum +
  DEFAULT provenance + `since_ms is None` with exact request-timestamp
  correspondence, HTTP 200, explicit list evidence with known nonnegative
  integer counts excluding bool, `rejected_count == 0` and
  `raw_count == len(vehicles)`, no duplicate raw IDs, queryTime strictly
  newer than both the vehicle high-water mark and the prior fence, and
  publication-time session/generation/limits validity. Any incompleteness
  commits safe positive updates with `incomplete_collection`; structurally
  impossible evidence (bool/negative counts, impossible accounting,
  purpose/provenance disagreement, non-200) is typed `invalid_delivery`.
- After all gates pass the store constructs the S9
  `ReconciliationSnapshot` wrapper with the fixed credential-free basis
  `s12_complete_default_collection`, established at the original receipt
  time — never a newly read clock — and retains its basis on the fence.
  Removal of an absent ID requires its stored source queryTime older than
  the snapshot queryTime **and** its raw observation time not later than
  that queryTime, so future-dated observations survive authorized omission
  and a vehicle updated while the snapshot was in flight is never removed.
- One bounded global absence fence carries the newest authorized queryTime
  and its **complete** raw seen-ID set. An ID absent from both the
  retained map and the fence set needs `vehicle.time_utc_ms >
  absence_fence_query_ms` to be introduced afterwards; older observations
  are withheld as `pre_reconciliation_observation` (deliberately
  conservative anti-resurrection policy; counted in diagnostics). IDs
  present in a later authorized full gain fresh explicit presence first.

### 14.4 Secondary feeds and context replacement

- TripUpdates order by the header `timestamp_utc_ms` (raw seconds pair
  rechecked, FULL_DATASET rechecked as an exact integer); alerts order by
  `AlertsSnapshot.query_time_utc_ms`. Newer timestamps replace the whole
  view, including explicit empties; older timestamps are ignored; equal
  timestamp with differing content books the sequence metadata-only and
  records `source_timestamp_conflict` — the prior view, its timestamp and
  its receipt stay intact; an exact retry of that booking is a duplicate;
  changed content reusing the booked sequence is a collision. Entity/alert
  order, duplicates, flags and instance fields are preserved without any
  active-prediction inference. Missing TripUpdates never block vehicles.
- `LiveState.replace_context(candidate, expected_generation=g)` compares
  authoritative identity `(archive_sha256, feed_version)`;
  `source_directory_name` is descriptive only. Same content is an atomic
  no-op; a different candidate re-matches every retained entry and every
  latest feed view outside the lock and publishes generation `g+1` and
  revision +1 together. Stale expected generation is an immediate
  `generation_conflict`. Re-matching uses the **canonical one-record
  synthetic `VehiclesSnapshot` per retained entry** (raw record + compact
  stamp, `evidence=None`, empty diagnostics) — internal matching inputs
  only, never new deliveries or authority — so re-matching never depends
  on the original whole collection still being retained. A removed trip
  leaves its vehicle position-only MAX while the route stays MAX; old
  snapshots remain valid immutable A views.

### 14.5 Frozen limits and diagnostics

All limits are validated constructor-only (positive integers excluding
bool; no environment/config path): `max_vehicle_entries = 4096` (also the
exact fence presence-set capacity), `max_vehicle_records_per_delivery =
4096`, `max_tripupdate_entities = 8192`, `max_alert_records = 4096`,
`max_nested_feed_records = 100000`, `max_state_diagnostic_samples = 200`.
Per-delivery record/nested bounds are checked **before** expensive S11
matching. The fence set overflow rejects the **entire transaction** with
`capacity_exceeded` (REV 2: no truncation, no weakened introduction rule,
no extra reason code). Retained-map capacity is checked on the final
candidate after valid reconciliation removals, so an authorized full can
free space. Decision telemetry keeps exact per-reason totals plus at most
200 samples including one overflow entry: 201 events means the first 199
real samples plus one overflow sample and 2 omitted, collected without
ever accumulating an unbounded list. The 13 reason codes and 9 error codes
are the frozen vocabularies pinned by the plan (§12); exceptions carry
short bounded messages plus a code — never payloads, URLs, paths,
credentials or exception reprs.

Failure-path honesty: a synthetic non-200 `PollDelivery` reaching the
adapter is S12 `invalid_delivery`; a real upstream HTTP/transport failure
is S10's domain before a successful delivery exists; an exception raised
by the S12 sink inside S10's `_invoke_sink` is reported by S10 as
`DELIVERY_FAILED` and advances no watermark. The S12 store never reads a
clock, touches the filesystem, opens a socket, starts a runtime, or loads
datasets; tests patch `time`/`open` and deny all sockets while a full
store lifecycle runs. A fresh store object reports
`prior_state_history = "unknown"` — duplicate tolerance, not
exactly-once delivery, durability, or an upstream-empty fleet.

### 14.6 S12 verification results (actual)

Preflight baseline (original file set, LIVE_FEED_TESTS unset,
PYTHONDONTWRITEBYTECODE=1): **838 passed / 17 skipped / 0 failed** in
68.30 s with the one known intentional duplicate-ZIP-member warning
(`test_missing_and_duplicate_zip_members_fail`). After implementation:

- Five new collected test modules: **131 passed** in 0.62 s
  (`test_trimet_state_types.py` 55, `test_trimet_state_sink.py` 14,
  `test_trimet_live_state.py` 30, `test_trimet_reconciliation.py` 23,
  `test_trimet_state_context.py` 9).
- Full offline suite: **969 passed / 17 skipped / 0 failed** in 58.41 s
  with the same single expected warning.
- Original test-file set re-run with the five new modules excluded:
  **838 passed / 17 skipped / 0 failed** in 56.87 s (baseline unchanged).

Named regressions required by the plan: exact fence capacity allowed /
capacity+1 rejects the whole transaction with no matching where
instrumented (`test_fence_presence_exact_capacity_is_allowed`,
`test_fence_presence_capacity_plus_one_rejects_entire_transaction`, plus
the 4096-boundary variant); secondary equal-timestamp conflict metadata
booking parameterized over TripUpdates and alerts
(`test_secondary_feed_newer_older_and_equal_timestamp_rules`,
`test_secondary_feed_sequence_collision_and_view_integrity`); context
re-match without the original delivery envelopes
(`test_context_rematch_without_original_delivery_envelopes`); revocation
during matching, retry exhaustion, second-session rejection
(`test_revocation_during_matching_fences_the_survivor`,
`test_retry_exhaustion_leaves_the_winning_root_intact`,
`test_second_active_session_is_rejected`); concurrent vehicle/context
publication losing no accepted update
(`test_concurrent_vehicle_and_context_publication_loses_no_update`); and
the deliberate blocked-network guard proof inside
`test_store_makes_no_clock_file_or_network_calls`. Concurrency tests use
events and bounded waits only — no sleeps, no real poller startup (that
integration is S14).

Documented interpretation (for owner review): per plan §8 the vehicle
high-water mark advances after any committed non-older delivery, including
incomplete collections; a consequence is that a corrected full carrying
the *same* queryTime as such an incomplete commit is equality-denied
authority until a strictly newer queryTime arrives. Equal-source-time
content that is unchanged is booked as a metadata-only transaction (§6)
also when the source model is identical, keeping later reuse of that
sequence detectable as a collision.

### 14.7 S12 completion status

S12 implementation and verification are complete and stop here for
independent owner review. Not performed: staging, commit, push, branch
change, S13/S14 work, any dependency change, any `.env`/credential access.
Checkout remained `main` at `6dfde7f1b75a37f74c155f051d0c2bbfd0b45d4a`
with clean tracked state; the complete pre-existing API-notes prefix above
(60,039 bytes) is preserved byte-for-byte, and this §14 is the single
append made to this document.

### 14.8 Correction pass (independent review, November 10, 2026)

An independent implementation review returned three P2 findings, all inside
the already approved S12 paths. The targeted corrections below were made
without changing any documented acceptance behavior, and each finding now
has a permanent regression test. The S12 completion status above still
holds: nothing staged, committed or pushed, and no S13/S14 work.

- **F1 — literal boolean collection evidence before authority.**
  `_accept_vehicles` now enforces the boolean domain of
  `VehicleCollectionEvidence.key_present` and `is_list` before any
  authority decision: a non-boolean flag (`1`, `"false"`, …) raises
  `StateInputError(code='invalid_delivery')` and the whole transaction
  fails precommit with no removal, fence advance or marker movement.
  Authority additionally requires both flags to be literally `True`
  (plan §9). Regressions: `test_non_boolean_collection_flags_cannot_authorize_removal`
  (4 malformed cases) and `test_literal_true_complete_empty_collection_can_remove`
  (control) in `tests/test_trimet_reconciliation.py`.

- **F2 — diagnostics bounded during reduction, not only at publication.**
  The per-transaction reduction no longer materializes a full
  `list[_Decision]`. A private streaming `_DecisionCollector` counts each
  fixed-vocabulary event and retains capped samples as reduction proceeds:
  one `_Decision` event object per event is created and either converted
  to a retained sample or dropped, so simultaneously retained per-event
  diagnostic objects stay bounded by the sample cap regardless of delivery
  size. Exact totals, overflow arithmetic and bounded unique `ApplyResult`
  reasons are unchanged; committed telemetry is never mutated before
  publication and the collector is discarded on retry or failure.
  Regression: `test_many_decisions_are_bounded_while_being_collected`
  (sample budget 3, 512 retained vehicles, one newer-queryTime incremental
  with older positions for all 512; exact total 512
  `older_vehicle_observation`, weakref-instrumented peak live event
  objects within the budget) in `tests/test_trimet_live_state.py`.

- **F3 — envelope metadata validated before every acceptance branch.**
  `_validate_common` now also validates the feed domain (`PollFeed`),
  request/receipt UTC-ms plausibility and non-negative integer
  `elapsed_mono_ms` (int, `bool` excluded) for every acceptance branch,
  including the equal-source-time metadata-only booking for both secondary
  feeds. Malformed envelope metadata raises the typed
  `StateInputError(code='invalid_delivery')` before sequence
  classification, matching or publication instead of leaking a raw
  `ValueError` from `SourceStamp` validators or acknowledging an invalid
  metadata-only delivery. Regressions:
  `test_invalid_envelope_metadata_uses_typed_precommit_failure` and
  `test_equal_source_time_conflict_cannot_book_invalid_envelope_metadata`
  (both secondary feeds) in `tests/test_trimet_live_state.py`.

Verification after the corrections: the full offline suite passes with
979 tests (969 prior + 10 new parametrized review regressions), 17 skipped,
the single expected warning; the review's own 13-case runner passes.

## 15. S13 - Freshness assessment and explicit expiry removal (M2)

### 15.1 What S13 adds

S13 completes the M2 store's time story on top of the S12 in-memory
`LiveState`. It adds three backend modules and three edits:

- `backend/trimet/freshness_types.py` — the frozen S13 vocabulary:
  five enums (`FeedFreshnessStatus` unknown/fresh/stale,
  `PositionTimeQuality` known/unknown, `VehicleExpiryStatus`
  unexpired/expired/unknown, `ClockStatus` uninitialized/trusted/
  untrusted, `CollectionQuality` complete/partial/unknown), the 13 fixed
  `FreshnessReason` strings, the owner-fixed `FreshnessPolicy`
  (10/60/120 s poll defaults → 30/180/360 s staleness thresholds; both
  future/clock tolerances 5000 ms), validated `TimeSample`,
  `ClockAssessment`, `FeedFreshnessAssessment`,
  `VehicleFreshnessAssessment`, `ExpiryState`, `ExpiryResult` and the
  derived read-only `FreshnessSnapshot`.
- `backend/trimet/freshness_errors.py` — the typed error family:
  `FreshnessInputError` with the five fixed codes
  (`invalid_policy`, `invalid_clock`, `invalid_sample`,
  `invalid_snapshot`, `invalid_cutoff`) and `FreshnessRecoveryError`
  with the two fixed recovery codes (`clock_epoch_conflict`,
  `clock_recovery_blocked`). No other error codes exist.
- `backend/trimet/freshness.py` — the pure evaluator
  `evaluate_freshness` (one immutable snapshot plus caller-supplied
  time inputs; no clocks, no I/O), the validated `SystemFreshnessClock`
  adapter, `FreshnessController` with `refresh()` and the explicit
  `reinitialize_clock(expected_clock_epoch=...)` recovery call, and the
  `freshness_policy_from_config` adapter that converts the existing
  validated `LiveConfig` poll-interval seconds into a policy in
  milliseconds (no environment loading, no credential access).
- `backend/trimet/state_types.py` (edit) — the optional validated
  `expiry_state` field on `LiveStateSnapshot`, defaulted `None` so every
  existing S12 constructor call remains compatible.
- `backend/trimet/live_state.py` (edit) — the public atomic
  `expire_vehicles(cutoff_utc_ms)` publication, the ingestion guard
  against the committed cutoff, `ExpiryState`/counter propagation across
  every root publication path, and snapshot exposure of the expiry
  state. The store still reads no clock and makes no I/O: expiry is
  always explicit, never scheduled by the store (S14 owns scheduling).
- `docs/trimet-api-notes.md` (edit) — this §15, the single append.

Staleness classification is bounded and source-time based: a feed is
FRESH while its accepted source time is at most 3× its poll interval
old (equality is FRESH, threshold+1 is STALE) and STALE past it, with
the source `queryTime`/header timestamp — never the receipt time, the
replay booking or any request duration — as the basis. Position age is
a measurement only: it is reported exactly (`PositionTimeQuality`
known/unknown with the raw age or an exposed `ahead_by_ms`) but creates
no position-staleness threshold, no warning reason and no visibility
effect. A stale-source, unexpired MAX vehicle therefore stays visible
despite missing enrichment.

### 15.2 Frozen vocabulary and the pure evaluator contract

The evaluator exposes five distinct, never conflated metrics per feed:
source age, source-content receipt age, last booked (replay) delivery
age, position age (per vehicle) and the recorded request elapsed
duration. Session replacement clears replay slots only — the booked
delivery age becomes unavailable with the fixed `no_replay_delivery`
reason and the content receipt age is never copied into its place.
Equal-queryTime metadata bookings, source-timestamp conflicts and exact
replays cannot freshen accepted content. Collection quality
(complete/partial/unknown) is a feed-level label derived solely from
the accepted source envelope's own evidence; it implies no per-vehicle
authority and never creates reconciliation authority by itself.

Boundary rules that the tests pin exactly: a source/position timestamp
ahead of the sample within the 5000 ms tolerance clamps the measured
age to 0 while exposing the exact `ahead_by_ms`; one millisecond beyond
the tolerance makes the age unavailable with the fixed
`source_time_ahead`/`position_time_ahead` reason (so `ahead_by_ms` may
coexist only with age exactly 0 or `None` — a positive age excludes
exposed skew). A future receipt or replay timestamp affects only its
own metric (`receipt_time_ahead` annotation) and never replaces the
source classification. A future vehicle `expires` deadline is a normal
unexpired state, not clock skew. FRESH/STALE statuses require a
measured age plus the applied threshold and forbid only the overriding
`source_time_ahead` reason; the metric annotations
(`no_replay_delivery`, `receipt_time_ahead`) may coexist.

Untrusted or inconsistent time never invents a view: with an untrusted
clock or a sample older than the snapshot's accepted cutoff
(`sample_before_snapshot_cutoff`), the evaluation returns unknown
statuses for every feed and vehicle, retains the whole projection with
no expiry claims, and never reports a negative age or a lowered cutoff.
A trusted evaluation requires an actually validated `TimeSample` — any
other object is the typed `FreshnessInputError(code='invalid_sample')`,
never a raw attribute failure or a duck-typed acceptance. Unknown
source/time branches erase nothing whose own inputs remain valid: a
source timestamp beyond tolerance stays UNKNOWN with its ahead-of-clock
reason while the trusted local receipt ages, the recorded request
duration and the accepted collection's own completeness evidence are
still reported; global clock distrust still hides every UTC-derived age
but keeps the previously recorded request duration. Evaluation is
pure: repeated runs are deterministic, the store is never mutated and
the evaluator reads no clock of any kind.

### 15.3 Expiry publication and the ingestion guard

`LiveState.expire_vehicles(cutoff_utc_ms)` is the only removal path.
The cutoff must be a plausible UTC millisecond instant excluding
`bool` (`FreshnessInputError(code='invalid_cutoff')` otherwise) and is
applied monotonically: an equal cutoff is an honest idempotent no-op
(`changed=False`, no revision bump), an older cutoff never lowers the
accepted cutoff or revives anything, and a higher cutoff publishes even
with zero removals (one revision bump, `ExpiryState.cutoff_utc_ms`
advances). Removal is the physical predicate: entries whose verified
`expires` timestamp is at or before the cutoff are removed across all
three S11 relevance classes (MAX, NON_MAX, UNCLASSIFIED) in one atomic
transaction. The publication runs its copy/filter window outside the
store lock and revalidates before committing, with three total attempts
bounded — interleaved publications force an exact candidate rebuild
from the winning root (never from stale state), and exhausted retries
raise `StateConflictError(code='concurrent_change')` with the prior
root fully preserved and nothing partially applied. Expiry frees
retained capacity, touches no source/replay envelope, no high-water
mark, no fence, no session/generation/identity and no S12 decision
telemetry, and previously captured snapshots never change.

The ingestion guard makes removed vehicles non-resurrectable without
tombstones: once a cutoff C is committed, a vehicle observation whose
expiry winner is at or before C is suppressed at ingestion time —
cached reinsertions, authoritative full snapshots and incrementals
alike — counted once per suppressed winner ID on the published
transaction only. A suppressed equal-queryTime winner publishes a
counter-only `METADATA_ONLY` transaction (one revision bump, the
suppression counter rides along) without freshening the accepted
source content or its receipt; exact replays, ignored-older deliveries
and discarded (capacity-exceeded) candidates count nothing. The guard
uses the committed cutoff only — never a deferred or evaluation-time
clock reading — and lower-ranked records are never promoted to replace
a suppressed winner. A genuinely newer observation with a later expiry
returns normally; with no cleanup yet performed (`C=None`) the guard
is disabled and nothing is preemptively suppressed. Raw presence
accounting for authoritative full snapshots happens before
suppression, so the fence records the suppressed ID's presence
honestly. `ExpiryState` (cutoff plus both counters,
`removed_entries_total` and `ignored_expired_winners_total`)
propagates through every root publication path — the six baseline
explicit sites (initial construction, committed vehicle publication,
metadata-only vehicle publication, both secondary-feed paths and
static-context replacement with its generation bump) plus the new
cleanup path and session close/reopen — and the accepted cutoff is
never lowered by any of them.

### 15.4 Clock protocol, distrust and epoch-checked recovery

The clock protocol is explicit: nothing samples at import, construction
or injection; `SystemFreshnessClock` and any injected clock are
validated (`FreshnessInputError(code='invalid_clock')` for a
non-provider, `code='invalid_snapshot'` for a non-store), a falsy but
valid provider is used rather than silently replaced, and sampling
happens only inside explicit `refresh()`/`reinitialize_clock()` calls.
`refresh()` is the single destructive-cleanup entry point and it never
runs without a trust decision first: the first valid wall/monotonic
sample anchors epoch 1; a negative monotonic or wall elapsed delta, or
an absolute wall-versus-monotonic divergence strictly greater than the
5000 ms tolerance (equality allowed), makes the clock sticky-UNTRUSTED
with the fixed `clock_moved_backwards` / `clock_diverged` reasons, and
provider failures or non-`TimeSample` returns are bounded
(`clock_read_failed` / `invalid_clock_sample`) rather than fatal. A
distrusted `refresh()` performs no cleanup at all — no expiry call
reaches the store before a trust decision exists, so a forward jump can
never delete vehicles and only then be diagnosed
(`test_clock_jump_is_distrusted_before_any_expiry_removal`).

Recovery is explicit and epoch-checked:
`reinitialize_clock(expected_clock_epoch=...)` refuses a stale or
unknown epoch with `FreshnessRecoveryError(code='clock_epoch_conflict')`
(changing nothing), and otherwise samples and validates one corrected
pair and establishes a new anchor with epoch+1. Recovery re-anchors the
clock only: it never expires, never clears store data, counters,
source ordering or expiry history, never calls cleanup and never
touches S10's quota clock. Recovery may not lower an accepted expiry
cutoff: corrected UTC still below the store cutoff raises the frozen
typed refusal `FreshnessRecoveryError(code='clock_recovery_blocked')` —
trust is not restored, the anchor/epoch/history and store stay exactly
as they were, no force-reset escape hatch exists, and the bounded
`utc_before_expiry_cutoff` time-quality cause travels in the refusal
message. A recovery attempt on a never-anchored clock whose provider
fails or returns a non-`TimeSample` is bounded too: it returns a
coherent UNINITIALIZED assessment and leaves the controller fully
usable — no controller field is mutated until a valid candidate
assessment exists, so no failed recovery can poison later refreshes. A
`refresh()` whose own assessment is untrusted passes
`sample=None` to the evaluator, so trust and time inputs can never
disagree. `refresh()` and recovery are serialized inside the
controller, so an obsolete clock assessment cannot race a recovery
into destructive cleanup (verified with a real-thread, event-bounded
serialization test — the only threaded test in the S13 set). When the
store loses the publication race against a cleanup, the cycle reports
the fixed `cleanup_deferred` reason with a `changed=False`
`ExpiryResult` — distinct from a successful removal — and the derived
view still filters expired entries from visible MAX IDs while every
retained entry remains assessed. Only the publication-race code
`concurrent_change` takes that deferred path: any other
`StateConflictError` code propagates unchanged, never relabelled as
ordinary cleanup contention.

### 15.5 Files added (8) and edited (3), the single docs append

Added: `backend/trimet/freshness_types.py`,
`backend/trimet/freshness_errors.py`, `backend/trimet/freshness.py`,
`tests/_s13_helpers.py` (shared offline helpers; collects no tests),
`tests/test_trimet_freshness_types.py`,
`tests/test_trimet_freshness.py`, `tests/test_trimet_expiry.py` and
`tests/test_trimet_freshness_clock.py`. Edited:
`backend/trimet/state_types.py`, `backend/trimet/live_state.py` and
this document. No other file was touched — no conftest, no dependency
change, no `.env` or credential access, no live request, no S14 work.

### 15.6 S13 verification results (actual)

The four new collected modules hold 146 tests (44 vocabulary,
validation and config-adapter, 36 evaluator, 31 expiry/guard/races,
35 clock controller), all passing offline against the synthetic S11/S12
worlds with a deny-all socket guard (a deliberate non-loopback attempt
is proven blocked), integer-millisecond fake clocks and deterministic
same-thread race injection (no sleeps; one bounded real-thread
serialization test as documented above). The full offline suite passes
with 1125 tests (979 prior + 146 new), 17 skipped, the single expected
duplicate-ZIP warning; the pre-existing 35-file set alone still passes
979/17/0, and the S12 review's own 13-case runner passes (exit 0).

Five implementation defects were found and fixed during verification,
before any completion report, each now pinned by regression tests: (1)
FRESH/STALE reason validation rejected legitimate metric annotations —
only `source_time_ahead` is forbidden; (2) the `ahead_by_ms`/age
invariant wrongly forbade the within-tolerance coexistence with a
clamped age of exactly 0; (3) `refresh()` passed its real sample to the
evaluator together with an untrusted assessment — it now passes
`sample=None` whenever its own assessment is untrusted; (4)
`clock_epoch_conflict` was raised as `FreshnessInputError` instead of
`FreshnessRecoveryError`; (5) `freshness_policy_from_config` referenced
`LiveConfig` in a runtime `isinstance` check that the type-checking-only
import never provided, making the documented config adapter unusable —
the runtime import was added. The named acceptance regressions all exist
and pass: `test_expiry_state_survives_each_root_publication_path`,
`test_deferred_cleanup_assesses_all_retained_entries_but_filters_visible_max`,
`test_ingestion_guard_uses_committed_cutoff_not_deferred_evaluation_time`,
`test_suppressed_winner_metadata_only_counts_once_without_freshening_content`,
`test_clock_jump_is_distrusted_before_any_expiry_removal`,
`test_clock_recovery_preserves_store_and_does_not_run_cleanup` and
`test_position_age_does_not_create_position_stale_warning`.

### 15.7 S13 completion status

S13 implementation and verification are complete and stop here for
independent owner review. Not performed: staging, commit, push, branch
change, S14 work, any live feed request, any dependency change, any
`.env`/credential access. Checkout remained `main` at
`b229b000965d38fa1fbe927cedd87e9d9ab29e6b` with clean tracked state
apart from the eleven authorized paths above. The complete pre-existing
API-notes prefix above (75,626 bytes through §14.8) is preserved
byte-for-byte, and this §15 is the single append made to this document.

### 15.8 Correction pass (independent review, November 11, 2026)

An independent implementation review reproduced the repository totals
and every preservation check, and returned four P2 findings inside the
already approved S13 paths. The finalized independent runner (14 cases,
SHA-256
`4AA0968D4B522E88B4CDE5354BD6166B1E7DE07ECFC4482BEF083ED6ED0E810F`)
executed unchanged before and after the corrections: 14 failures
before, 14 passes after, its assertions never weakened or altered. Each
finding also gained permanent regressions inside the authorized S13
test files.

- **F1 — recovery poisoning and the typed refusal contract.**
  `reinitialize_clock` previously mutated controller fields before a
  valid candidate assessment existed: on a never-anchored controller
  (`epoch 0`) a failing provider attempted an illegal
  `ClockAssessment(UNTRUSTED, epoch=0)`, left the controller marked
  untrusted with no assessment, and made the next `refresh()` raise
  `AttributeError`. Recovery now constructs the candidate assessment
  first and mutates no controller field until it is valid; a provider
  failure on a never-anchored clock returns a coherent UNINITIALIZED
  assessment and leaves the controller fully usable (the next refresh
  anchors normally), while an established clock keeps its sticky
  distrust. Separately, recovery whose corrected UTC is still below the
  store's accepted cutoff now raises the frozen typed refusal
  `FreshnessRecoveryError(code='clock_recovery_blocked')` — trust not
  restored, anchor/epoch/history/store unchanged, no force-reset path,
  the bounded `utc_before_expiry_cutoff` cause in the message — instead
  of returning an assessment. Regressions in
  `tests/test_trimet_freshness_clock.py`: the rewritten
  `test_recovery_below_cutoff_raises_frozen_typed_refusal`,
  `test_recovery_provider_failure_before_first_anchor_is_bounded` (both
  failure modes) and
  `test_failed_initial_recovery_does_not_poison_subsequent_refresh`
  (both failure modes, mirroring the runner's both-contracts tolerance).

- **F2 — the pure evaluator accepts unvalidated samples.**
  `evaluate_freshness` checked only the None/trust pairing and then
  accessed sample fields directly: `True`, bare objects or a
  duck-typed UTC-only namespace either leaked a raw `AttributeError` or
  passed as trusted input. A trusted assessment now requires an
  actually validated `TimeSample`; anything else is the typed
  `FreshnessInputError(code='invalid_sample')`. The None/untrusted
  pairing and purity are unchanged. Regressions in
  `tests/test_trimet_freshness.py`:
  `TestSampleTypeBoundary::test_evaluator_requires_actual_validated_
  timesample` (bool / plain object / duck-typed UTC-only).

- **F3 — unrelated store conflicts mislabelled as deferred cleanup.**
  The controller caught the whole `StateConflictError` class and turned
  every code into `cleanup_deferred`. Only the publication-race code
  `concurrent_change` may take that path; `generation_conflict` and
  `sequence_collision` now propagate with their original exception and
  code, and nothing is published or fabricated for them. Regressions in
  `tests/test_trimet_freshness_clock.py`:
  `test_controller_propagates_non_publication_conflicts` (both codes),
  alongside the existing deferred-cleanup control.

- **F4 — UNKNOWN source/time branches erased independent metrics.**
  The source-beyond-tolerance branch returned early, dropping the
  trusted local receipt ages, the recorded request duration and the
  accepted collection's completeness evidence; the global-untrusted
  branch dropped the recorded request duration, which is a previously
  recorded fact unrelated to the current wall clock. Receipt/replay age
  computation is now shared (`_receipt_ages`) and the beyond-tolerance
  branch retains all three independent facts while the source age stays
  UNKNOWN with its fixed reason; the untrusted branch retains the
  request duration (UTC-derived ages stay hidden there). No duration is
  relabelled as API publication latency and collection quality remains
  feed-level only. Regressions in `tests/test_trimet_freshness.py`:
  `test_source_ahead_beyond_tolerance_preserves_independent_metrics`
  and `test_untrusted_utc_preserves_recorded_request_duration`.

Verification after the corrections: the unchanged independent runner
passes 14/14; the four S13 modules hold 146 tests (44 + 36 + 31 + 35),
all passing; the full offline suite passes with 1125 tests (979 prior +
146 new), 17 skipped, the single expected duplicate-ZIP warning; the
pre-existing 35-file set alone still passes 979/17/0; and the S12
review's own 13-case runner passes (exit 0). The 75,626-byte pre-existing
docs prefix remains byte-for-byte identical; the corrections amended
only §15 text and appended this subsection. Nothing staged, committed
or pushed; checkout remained `main` at HEAD
`b229b000965d38fa1fbe927cedd87e9d9ab29e6b` with the unchanged preflight
index hash.
