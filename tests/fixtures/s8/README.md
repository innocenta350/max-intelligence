# S8 Fixtures — Immutable Test Inputs (Stage S9, M2 plan REV 2 §8)

Static, curated test inputs created **once** during S9 implementation. They
are never generated at test time and never produced by importing the probe
tool. All values are real observations from the S8 verification campaign;
nothing is synthesized except the file structure itself.

- Source campaign: `s8-1794024279858-afcea02f` (tool:
  `tools/s8_contract/probe_s8.py`)
- Timestamp convention: UTC epoch milliseconds primary; ISO-8601 Z secondary
- Integrity: `SHA256SUMS` covers **all four** fixture files; the fixture
  integrity test in `tests/test_trimet_feed_parse.py` verifies every checksum
  before use, so corruption or drift fails loudly.
- Credential safety: the JSON fixtures contain **no AppID and no
  credential-bearing metadata**; an automated test asserts there is no
  `appID` substring and no URL (hence no URL query string) in any JSON
  fixture.
- Fixture independence: tests read these files directly. They never import
  `probe_s8.py` and never read `tools/s8_contract/` (or any S8 campaign path)
  at runtime — the provenance references below are documentation only.

## `vehicles_snapshot_curated.json`

- Source artifact: `tools/m0_audit/evidence/vehicles_snapshot.json` (schema
  basis, re-verified UNCHANGED by S8/A1) + S8 campaign evidence
  `tools/s8_contract/evidence/b5_cadence.json`, section
  `b5.vehicle_first_window_rail_sample` (campaign id above; B5 window poll 0,
  started `2026-11-07T04:13:10.256Z`).
- `resultSet.queryTime` = `1794024790542` (poll 0, `2026-11-07T04:13:10.542Z`).
- 7 real rail vehicle records, verbatim from the B5 sample: vehicleIDs
  314, 324, 537, 617, 620, 627, 629 — covering all five MAX routes
  (90, 100, 190, 200, 290), both directions, negative and positive delays.
- Curation note (honest scope): the B5 sample carries the **17 raw keys the
  S8 summarizer preserved** (`vehicleID, type, routeNumber, routeSubType,
  routeColor, tripID, newTrip, offRoute, direction, time, expires, delay,
  latitude, longitude, bearing, nextLocID, lastLocID`). The remaining 13 keys
  of the verified 30-key schema (`blockID, extraBlockID, garage,
  inCongestion, lastStopSeq, loadPercentage, locationInScheduleDay,
  messageCode, nextStopSeq, serviceDate, signMessage, signMessageLong,
  source`) were present on all 287 vehicles in the M0 snapshot
  (`attribute_frequency_counts_full`) but are absent from this curated copy;
  parser tests exercise those keys with inline records. Absent keys parse as
  `None` per plan §6.1.

## `alerts_snapshot_curated.json`

- Source artifact: `tools/s8_contract/evidence/window_alert_captures.json`,
  capture index 0 (tag `B5-alerts`, `poll_index` 0), started
  `2026-11-07T04:13:10.257Z`.
- `resultSet.queryTime` = `1794024790551` (`2026-11-07T04:13:10.551Z`).
- 5 real alerts selected from the 32 in the capture, covering every alert
  shape observed in the window: route-only, route+location, multi-route, and
  unscoped. Alert `293963` carries a real MAX route child (`200`).
- Curation notes (honest scope): the capture summarizer stored description
  text under `desc_first160`, truncated to 160 characters (location
  descriptions truncated to 80); curation maps that value back to the real
  feed key `desc` verbatim — the truncation is inherent to the source
  evidence, not introduced here. `header_text` empty strings, null `end`,
  and null `info_link_url` are real observations (S8/A5), preserved as-is.

## `tripupdate_capture_01.bin`

- Source artifact: `tools/s8_contract/evidence/tripupdate_capture_01.bin`
  (static copy, byte-identical; capture index 1, captured
  `2026-11-07T04:13:10.580Z`, 231,638 bytes).
- Expected decode (S8/B2 manifest + verified by the S9 decoder round-trip
  test): 424 entities, header `gtfs_realtime_version` `"2.0"`,
  `incrementality` FULL_DATASET (raw 0), header timestamp `1794024790`
  (seconds).

## `tripupdate_capture_10.bin`

- Source artifact: `tools/s8_contract/evidence/tripupdate_capture_10.bin`
  (static copy, byte-identical; capture index 10, captured
  `2026-11-07T04:22:10.532Z`, 217,278 bytes).
- Expected decode (verified by the S9 decoder round-trip test): 411 entities,
  FULL_DATASET, non-empty entity list.
