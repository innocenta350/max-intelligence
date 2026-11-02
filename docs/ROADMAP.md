# MAX Intelligence - Phase 1 Roadmap

## Overview

Phase 1 establishes the live-data foundation of MAX Intelligence.

The primary objective is to deliver a working live MAX tracking map by completing M4.

## Milestone Sequence

M0 → M1 → M2 → M3 → M4 → M5 → M6 → M7 → M8 → M9

M8 and M9 are optional.

---

## M0 - Feed Audit & Access

**Objective:** Verify TriMet API access and actual feed behavior.

### Scope
- Configure TriMet AppID.
- Test vehicles, alerts, routeConfig, and TripUpdate endpoints.
- Verify assumptions A1-A5.
- Verify `expires` timestamp units and semantics.
- Investigate quota counting and reset behavior.
- Document findings in `docs/trimet-api-notes.md`.

### Acceptance Criteria
- All four endpoints tested.
- A1-A5 marked PASS, FAIL, or UNVERIFIED with evidence.
- E1 expiration behavior investigated and documented.
- Quota counting behavior investigated.
- No credentials exposed.

## M1 - Static GTFS

**Objective:** Load the static MAX network data.

### Scope
Parse:
- `agency.txt`
- `routes.txt`
- `route_directions.txt`
- `stops.txt`
- `shapes.txt`
- `feed_info.txt`

Filter MAX routes using TriMet routeConfig and GTFS route metadata.

### Acceptance Criteria
- All required files parse successfully.
- Only Light Rail routes pass the filter.
- No buses or Aerial Tram routes included.
- Every MAX route has at least one shape and one stop.
- GTFS feed version recorded.

## M2 - Vehicle Pipeline

**Objective:** Build reliable live vehicle ingestion.

### Scope
- Incremental polling every 10 seconds.
- Full reconciliation every 5 minutes.
- Vehicle normalization.
- Expiry-based removal.
- Full-snapshot removal.
- Fallback to full polling after incremental failure.
- In-memory LiveState.

### Acceptance Criteria
- Critical vehicle pipeline tests pass.
- Full-snapshot comparison works under concurrent updates.
- Ghost vehicles are removed.
- No ghost vehicles remain after a 10-minute live soak.

## M3 - REST API

**Objective:** Expose backend data to the frontend.

### Endpoints
- `/api/vehicles`
- `/api/routes`
- `/api/alerts`
- `/api/network/shapes`
- `/api/health`

### Acceptance Criteria
- OpenAPI specification exported.
- API contract tests pass.
- Health endpoint reports source freshness and quota usage.
- No SSE implementation yet.

## M4 - MVP Live MAX Map

**Objective:** Deliver the first usable MAX Intelligence product.

### Scope
- Interactive Leaflet map.
- MAX route shapes.
- MAX stops.
- Live train markers.
- Vehicle selection and information panel.
- Freshness banner.
- Route and direction coloring.

### Acceptance Criteria
- All in-service MAX trains visible, cross-checked against a same-instant raw feed capture.
- Train markers reposition as new data arrives.
- Selecting a train displays vehicle ID, route, direction, delay, sign message, next stop, and data timestamp.
- Freshness banner correctly displays LIVE or STALE.
- Layout works between 320 and 1920 pixels wide.

**M4 is the MVP gate. No M5 or later work may delay it.**

## M5 - Alerts & Predictions

**Objective:** Add service alerts and stop-level predictions.

### Scope
- MAX service alerts panel.
- GTFS-Realtime TripUpdate decoder.
- Prediction display.
- Evaluate standard `gtfs-realtime-bindings` first.

### Acceptance Criteria
- MAX alerts displayed with data age.
- TripUpdates decoded successfully.
- Missing predictions displayed as "No prediction."
- Sparse feeds do not cause errors.
- Live vehicle tracking remains functional without predictions.

## M6 - Server-Sent Events

**Objective:** Improve frontend update delivery.

### Scope
- Add `/api/events`.
- Implement SSE.
- Automatic reconnection.
- Polling fallback.

### Acceptance Criteria
- State changes delivered within one second internally.
- Automatic reconnection works.
- Polling fallback activates within three seconds of SSE failure.
- User experience remains consistent.

## M7 - Hardening

**Objective:** Improve reliability and error handling.

### Scope
- Timeouts.
- Retries with jittered backoff.
- Circuit breakers.
- Structured logging.
- Corrupted-payload handling.
- Quota protection.

### Acceptance Criteria
- One-hour live soak without unhandled exceptions.
- Simulated outages recover automatically.
- Corrupt payloads are safely skipped.
- Retry requests count toward quota.
- Configured quota reset behavior tested.
- Polling halts when the budget is exceeded.

## M8 - Optional Historical Data

**Objective:** Introduce lightweight historical data collection.

### Scope
- Compressed NDJSON archival.
- Append-only raw-feed storage.
- Configurable feature flag.

### Acceptance Criteria
- Raw payloads and capture timestamps recorded.
- Recording is asynchronous and fail-open.
- No more than 50 ms p95 added to the polling cycle.

M8 is optional and must not delay the MVP.

## M9 - Optional Deployment

**Objective:** Make MAX Intelligence deployable through Docker Compose.

### Scope
- Backend container.
- Frontend container.
- Nginx configuration.
- Deployment runbook.

### Acceptance Criteria
- Clean-clone Docker Compose startup works.
- Health endpoint usable for uptime monitoring.
- Secrets never baked into images or logs.
- Deployment instructions documented.

M9 is optional and must not delay the MVP.

---

## Development Rules

1. Complete milestones in order.
2. Verify acceptance criteria before advancing.
3. Do not introduce PostgreSQL during Phase 1.
4. Do not let optional features delay M4.
5. Do not claim completion without evidence.
6. Obtain owner approval before architectural changes, commits, or pushes.