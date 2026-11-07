# M1 Final Report — Static GTFS Pipeline

**Status: COMPLETE.** Permanent checkpoint: `073fe79` on `main` (pushed to
`origin/main`). Working tree clean at closure; tests 347 passed, 17 skipped,
0 failed.

This report is the milestone-closure record for M1. It summarizes what was
built, how it was verified, and what was deliberately left open. It is a
factual record of verified results, not a plan document.

## 1. Milestone history

| Milestone / stage | Commit | Content |
|---|---|---|
| M0 | `daa1cc4` | TriMet feed audit and access verification |
| S0–S5 | `953f764` | Scaffold/config, CSV infrastructure, download & safe extraction, schema inspection, GTFS parser, MAX network construction |
| S6 | `b86cbbf` | Dataset promotion and loading (manifests, `promote.lock`, suffix ladder, retention, `DatasetManager`, CLI `load`) |
| S7 | `073fe79` | Final integration verification; one integration defect found and fixed with regression coverage |

## 2. What M1 delivers

The complete static GTFS pipeline, end to end:

```
GtfsConfig.from_env()
    ↓
download (streaming, size-capped, credential-safe URLs)
    ↓
safe ZIP extraction into owned staging (staging-<pid>-<hex16>, guard.json)
    ↓
GTFS CSV parsing (strict MAX rows, capped unrelated-row tolerance)
    ↓
MAX-only filtering + network construction (six load-failing invariants)
    ↓
manifest.json write + fsync + verify
    ↓
promote.lock (O_CREAT|O_EXCL, immutable 4-field record, token-verified release)
    ↓
checksum-suffix ladder → atomic rename → dataset-<sha256[:16]>
    ↓
retention (keep last 2 valid versions; never delete uncertain data)
    ↓
DatasetManager atomic in-memory swap
```

Operator entry point:

```powershell
python -m backend.trimet.load_cli        # load runs by default
python -m backend.trimet.load_cli load   # explicit form
```

### Key guarantees

- The active in-memory dataset is only ever replaced by a fully validated,
  verifiably promoted version. Failed loads never disturb the previous
  dataset; a failed first load leaves `current is None`.
- Repeated loads of identical content are idempotent (verified offline and
  against the live feed).
- Corrupt, incomplete, foreign, or uncertain state fails loudly and is never
  adopted, overwritten, or auto-deleted.
- Ownership is enforced everywhere: `guard.json` pid checks for staging, the
  immutable `(pid, token, acquired_utc_ms, repo_paths_hint)` record for the
  promotion lock.
- CLI failures exit nonzero and never expose URLs or credentials.

## 3. Verification record

### Offline (default suite, no network)

- **347 passed, 17 skipped, 0 failed** at closure.
- Synthetic feeds built from a deterministic 8-file fixture; transport tests
  use a real loopback HTTP server (no mocks in the I/O layer).
- Coverage includes: failure injection at every pipeline boundary, promotion
  locking (contention, malformed/partial records, token-verified release,
  retry exhaustion), manifest validation (absent/malformed/type-broken never
  treated as valid), ladder collisions, retention safety, foreign-artifact
  preservation, concurrent readers, and CLI exit codes.

### Live (opt-in via `LIVE_FEED_TESTS=1`, run during S7 verification)

- 17/17 live schema tests passed against both a previously staged feed and a
  freshly downloaded feed.
- Fresh download: 43,467,097 bytes — exactly the M0-audited size; feed
  version `20261011-20261021-0903` unchanged since S3.
- Real CLI loads via both documented invocation forms exited 0; the second
  load confirmed real-feed idempotence (`reused existing: yes`); no URL or
  credential leakage in any output.
- Activated live network: 5 MAX routes (90 Red, 100 Blue, 190 Yellow,
  200 Green, 290 Orange), 6,031 stops, 11,602 trips, 146 shapes.

### S7 integration defect (found and fixed)

A reused promotion short-circuits without consuming the staging directory,
and nothing else cleaned it up — every reused load leaked one owned
`staging-*` directory (observed live). Fixed in `DatasetManager.load`
(ownership-verified discard after reuse), regression-tested, and verified
live. This is the only post-S6 production change; S0–S5 behavior was never
modified during S6/S7 apart from one additive public wrapper
(`discard_staging_if_own`).

## 4. Accepted limitations (documented, deliberate)

1. Residual read-verify-delete race in `promote.lock` release — accepted and
   documented for M1.
2. Directory fsync is best-effort (Windows cannot fsync directory handles).
3. No cross-process garbage collection: stale/foreign/orphaned staging
   directories are report-only (inventory classifies ACTIVE/STALE/UNREADABLE
   and never deletes).
4. The original M1 Rev. 9 plan text was not available in the repository
   during S5–S7. Stage requirements were reconstructed from the committed
   implementation, approved decisions, and owner direction; every
   reconstruction is labeled in code docstrings and stage reports. The S7
   prompt was treated as the requirement of record for S7.

## 5. Parked items (not authorized; awaiting explicit owner direction)

1. **S8/M2 — live vehicle-data pipeline** (incremental polling, full-snapshot
   reconciliation, expiry removal, in-memory LiveState).
2. **Live-test fixture decoupling** — the S3 live tests locate their feed via
   incidental `staging-*` directories, which S6 promotion consumes; a fresh
   environment's `load` leaves no staging dir, so live tests would skip.
   Recommended follow-up: obtain the feed from an explicit fixture path or a
   controlled staging operation.
3. **Manual cleanup of orphaned staging directories** — three `staging-*`
   directories exist under `data/gtfs/` (one S3-era, one pre-fix leak, one
   from a crashed verification run). Left untouched by design; deletable only
   after independent ownership and contents verification.

## 6. Operational notes

- Configuration comes from environment / `.env` (git-ignored); secrets are
  never read by M1 code paths (`TRIMET_APP_ID` belongs to M2+).
- Runtime data lives under `data/` (git-ignored): promoted versions,
  manifests, and staging directories.
- The staging inventory (`backend.trimet.dataset.inventory_staging`) is the
  diagnostic tool for inspecting staging directories before any manual
  cleanup decision.
