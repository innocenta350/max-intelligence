# TriMet Static GTFS Schema Notes (M1 Stage S3)

Evidence-based schema findings for the static GTFS feed, established by
inspecting the real archive before designing the S4 parser. Every claim below
was measured from the staged feed, not assumed from the GTFS specification.

## 1. Provenance

- Inspected: `route_directions.txt` **first**, then the remaining files (S3 assignment).
- Feed: `https://developer.trimet.org/schedule/gtfs.zip` (configured `GTFS_FEED_URL` default).
- `feed_version`: `20261011-20261021-0903` (same build M0 audited; zip size
  43,467,097 bytes matches M0's `Content-Length` exactly).
- Service period: `feed_start_date` 2026-10-11 → `feed_end_date` 2027-02-27.
- Download SHA-256: `e642a21ab45100a77824c5f6c2960dc37b632b8191feefb9dcfc2d94fe4d662d`.
- Extracted via the approved S2 pipeline (`stage_gtfs`): 20 members,
  276,605,163 uncompressed bytes.
- Reproduce with `tools/s3_schema/inspect_gtfs.py <staging-dir>`; full
  measurements live in `tools/s3_schema/evidence/gtfs_schema_audit.json`.
- All eight files parse cleanly through the S1 `csvio` primitives
  (`open_gtfs_csv` with the S4 required columns): zero row problems, zero
  blank lines, no header names needing trim, no duplicate header names.

## 2. Per-file findings

| File | Rows | Blank columns (rows affected) | Key facts |
|---|---|---|---|
| `feed_info.txt` | 1 | `feed_contact_email` (1) | Single row; `feed_version`, `feed_start_date`, `feed_end_date` populated. |
| `agency.txt` | 3 | `bikes_policy_url` (2) | `agency_id` unique: `PSC`, `TRAM`, `TRIMET`. |
| `routes.txt` | 82 | `route_short_name` (8), `network_id` (1) | `route_id` unique. `route_type`: 72×`3` (bus), 8×`0`, 1×`6` (Aerial Tram), 1×`2` (WES). |
| `route_directions.txt` | 161 | none | `(route_id, direction_id)` unique; `direction_id` ∈ {`0`,`1`} only (82/79 split); `direction_name` never blank; every `route_id` exists in `routes.txt`. |
| `stops.txt` | 6,031 | `platform_code` (all 6,031), `parent_station` (5,895), `direction` (75), `stop_code`/`stop_desc`/`zone_id`/`stop_url`/`position` (68 each) | `stop_id` unique. `location_type`: 5,963×`0` (platforms), 68×`1` (stations, `stop_id` = `station-<n>`). `stop_lat`/`stop_lon` fully populated. `14024`/`14025` absent (M0 A3 confirmed). |
| `trips.txt` | 67,957 | `trip_headsign`, `trip_short_name`, `trip_type` (all rows) | MAX trips: 11,602. `direction_id` fully populated, values {`0`,`1`} only. `shape_id` fully populated — **every MAX trip has a shape**. `trip_id` unique within MAX. `service_id` format `A.737` (16 distinct). |
| `stop_times.txt` | 3,407,790 | none | MAX rows: 241,920 covering all 11,602 MAX trips. `stop_sequence` strictly increasing within every MAX trip, no duplicates. `arrival_time`/`departure_time` never blank for MAX rows. `stop_headsign`, `pickup_type`, `drop_off_type`, `shape_dist_traveled`, `timepoint` fully populated. |
| `shapes.txt` | 1,095,877 | none | MAX trips reference 146 distinct `shape_id`s — all present, 121,509 points (84–1,933 per shape), coordinates all numeric, `shape_pt_sequence` unique per shape. No `shape_id` is shared between two MAX routes. |

## 3. `route_directions.txt` schema and MAX direction mapping

Header (exact): `route_id,direction_id,direction_name`

This file is TriMet-specific (not part of the GTFS standard) and is the
authoritative source for the direction names in the planned data model
(`Route.direction_names`, keyed by direction id with `None` reserved for
unspecified directions — currently unused because `direction_id` is always
populated).

MAX rows, verified against M0 routeConfig evidence
(`tools/m0_audit/evidence/routeconfig_selected.json`):

| route_id | direction_id | direction_name | routeConfig `dir`/`desc` match |
|---|---|---|---|
| 90 | 0 | To Portland International Airport | dir 0 ✓ |
| 90 | 1 | To Hillsboro | dir 1 ✓ |
| 100 | 0 | To Gresham | dir 0 ✓ |
| 100 | 1 | To Hillsboro | dir 1 ✓ |
| 190 | 0 | To Expo Center | dir 0 ✓ |
| 190 | 1 | To Portland City Center | dir 1 ✓ |
| 200 | 0 | To Clackamas Town Center | dir 0 ✓ |
| 200 | 1 | To Gateway Transit Center | dir 1 ✓ |
| 290 | 0 | To Portland City Center | dir 0 ✓ |
| 290 | 1 | To Milwaukie | dir 1 ✓ |

GTFS `direction_id` 0/1 aligns with routeConfig `dir` 0/1 for all five MAX
routes. routeConfig descriptions carry trailing spaces (M0 §2.3); the GTFS
names here do not. Per `csvio.get_str`, values are preserved verbatim — any
trimming is a display concern, not a parser concern.

## 4. MAX-specific relationship verification

- **routes ⇄ routeConfig:** `route_type=0` yields exactly 8 rows: MAX
  `90/100/190/200/290` (agency `TRIMET`, blank short names, colors `C41F3E`/
  `1359AE`/`FFC52F`/`008342`/`D05F27`) plus Streetcar `193/194/195`
  (agency `PSC`). Confirms M0 A3: `route_type=0` alone is not a MAX filter;
  the route-id whitelist is required.
- **trips ⇄ route_directions:** all 11,602 MAX trips carry `direction_id`
  ∈ {0,1}, and both directions exist per route in `route_directions.txt`.
- **trips ⇄ stop_times:** every MAX trip has stop times (0 orphans);
  241,920 MAX rows.
- **stop_times ⇄ stops:** all 162 distinct MAX stop ids exist in
  `stops.txt` — zero unmatched references *inside the static feed*.
- **stop_times ordering:** `stop_sequence` is strictly increasing within
  every MAX trip; no duplicates; no unparseable values.
- **trips ⇄ shapes:** every MAX trip has a `shape_id`; all 146 referenced
  shapes exist in `shapes.txt` with ≥ 2 points (usability rule met for all).
- **stops parent/station structure:** 134 of 162 MAX platforms carry
  `parent_station` pointing at 67 distinct `station-<n>` rows — all present
  in `stops.txt` as `location_type=1`. The other 28 MAX stops (e.g. PDX
  airport, Expo Center, Clackamas TC) have no parent: they are their own
  station. `stop_times.txt` never references station rows directly. The S5
  parent-station index must therefore tolerate platforms without a parent.
- **M0 locID note:** `14024`/`14025` are live-vehicle locIDs with no GTFS
  stop rows; they remain an M2 join concern (`RouteCoverage.unmatched_stop_ids`
  mechanism), not an S4 parsing concern.

## 5. Parser requirements for S4 (evidence-based)

Required columns (all verified present; enforced via `open_gtfs_csv`):

- `feed_info.txt`: `feed_version`, `feed_start_date`, `feed_end_date`
- `agency.txt`: `agency_id`, `agency_name`
- `routes.txt`: `route_id`, `route_type`, `route_long_name`, `route_color`
- `route_directions.txt`: `route_id`, `direction_id`, `direction_name`
- `stops.txt`: `stop_id`, `stop_name`, `stop_lat`, `stop_lon`,
  `location_type`, `parent_station`
- `trips.txt`: `route_id`, `trip_id`, `direction_id`, `shape_id`
- `stop_times.txt`: `trip_id`, `arrival_time`, `departure_time`, `stop_id`,
  `stop_sequence`
- `shapes.txt`: `shape_id`, `shape_pt_lat`, `shape_pt_lon`, `shape_pt_sequence`

Optional / tolerated-absent columns (may be read but never required):

- `routes.txt`: `route_short_name` (blank on all 8 type-0 rows),
  `route_url`, `route_text_color`, `route_sort_order`, `network_id`
  (blank on the TRAM row).
- `stops.txt`: `stop_code`, `tts_stop_name`, `stop_desc`, `zone_id`,
  `stop_url`, `wheelchair_boarding`, `platform_code` (always blank),
  `direction`, `position`, `parent_station` (28 MAX stops lack it).
- `trips.txt`: `trip_headsign` (always blank — headsigns live in
  `stop_times.stop_headsign`), `trip_short_name`, `trip_type`, `block_id`,
  `wheelchair_accessible`, `bikes_allowed`. `service_id` exists but M1
  ignores calendar data (plan §B).
- `stop_times.txt`: `stop_headsign`, `pickup_type`, `drop_off_type`,
  `shape_dist_traveled`, `timepoint` (populated today, but not needed by the
  data model — read only if a future stage requires them).
- `shapes.txt`: `shape_dist_traveled`.
- `feed_info.txt`: `feed_contact_email`, `feed_contact_url`.

Type conversions (via the S1 getters): `direction_id`/`stop_sequence`/
`location_type`/`shape_pt_sequence` → `get_int`; `stop_lat`/`stop_lon` →
`get_float`; `arrival_time`/`departure_time` → `get_time_secs` (hours > 23
supported); everything else stays raw strings (`get_str`).

Validation the real feed supports (safe to enforce as hard invariants in
S4/S5, with evidence above): unique `route_id`/`stop_id`; unique
`(route_id, direction_id)` in `route_directions.txt`; `direction_id` ∈ {0,1};
unique `trip_id` within MAX scope; strictly increasing `stop_sequence` per
trip; every MAX trip has a shape; every referenced shape exists with ≥ 2
points; parent-station targets exist.

## 6. Discrepancies and watch items

- **No architectural conflicts found.** The Rev. 9 data model
  (`backend/gtfs/types.py`) matches the observed feed: `Trip.direction_id`/
  `shape_id` optionality is still correct as written (blank values are
  tolerated even though this build never emits them), and
  `Route.direction_names`' `None` bucket remains a harmless generality.
- `trips.trip_headsign` is entirely blank in this build. Nothing in the
  planned model depends on it; if a future stage needs headsigns, use
  `stop_times.stop_headsign` (populated).
- `stops.platform_code` is entirely blank; do not require it.
- One `location_type=1` station row is not the parent of any MAX platform
  (68 station rows vs 67 MAX parents) — expected, stations also serve other
  modes.
- Feed version/format is stable against the M0 audit (same build). The
  live-marked schema test (`tests/test_gtfs_schema_live.py`) re-verifies
  these facts against any future staged feed so a refresh that breaks an S4
  assumption fails loudly instead of silently.
