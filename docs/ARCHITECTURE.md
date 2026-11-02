# MAX Intelligence - Architecture

## 1. Overview

MAX Intelligence is an independent analytics platform for Portland's TriMet MAX light rail system.

Phase 1 focuses on building a reliable live-data platform that can later support historical analysis and transit performance analytics.

## 2. Technology Stack

### Backend
- Python
- FastAPI
- HTTPX
- Pydantic Settings
- Pytest

### Frontend
- React
- TypeScript
- Vite
- Leaflet / React-Leaflet
- Zustand

### Data
- TriMet native JSON APIs.
- GTFS-Realtime protobuf feeds.
- Static GTFS datasets.

## 3. Data Sources

| Source | Purpose |
|---|---|
| `/ws/v2/vehicles` | Live vehicle positions |
| `/ws/v2/alerts` | Service alerts |
| `/ws/V1/TripUpdate/` | Stop-level arrival predictions |
| Static GTFS | Routes, stops, shapes, and direction names |
| `/ws/V1/routeConfig` | Route metadata and route classification |

### MAX Route Identification

MAX routes must be identified using:

1. TriMet routeConfig `@routeSubType == "Light Rail"`.
2. Cross-checking against GTFS `routes.txt` with `route_type = 0`.

Do not rely on `type=R` alone because it also includes the Aerial Tram.

## 4. System Architecture

The application uses a backend-driven architecture.

### Data Flow

1. The backend requests data from TriMet.
2. The backend parses and normalizes the responses.
3. Vehicle information is stored in an in-memory LiveState.
4. FastAPI exposes normalized data through REST endpoints.
5. The React frontend requests data from the backend.
6. Leaflet displays vehicle positions, routes, and stops.

The frontend must never call TriMet directly. All TriMet API communication goes through the backend.

## 5. Vehicle Data Management

### Incremental Polling

- Poll `/ws/v2/vehicles` every 10 seconds.
- Use the `since` parameter with the last successful `queryTime`.
- Merge updates using `vehicleID`.
- Do not remove vehicles simply because they are absent from an incremental response.

### Full-Snapshot Reconciliation

- Request a full vehicle snapshot every 5 minutes.
- Compare the snapshot against LiveState.
- Remove vehicles missing from the authoritative snapshot.
- Ensure reconciliation cannot overwrite newer incremental updates.

Use serialization or timestamp/version safeguards to prevent stale snapshots from deleting newer vehicle updates.

### Expiry

Use the documented `expires` field to remove expired vehicles.

Its timestamp unit and actual feed behavior must first be verified during M0. Removal logic must use the verified unit and reference clock.

### Failure Handling

If incremental polling fails, temporarily fall back to full-snapshot polling until incremental requests succeed again.

## 6. Data Normalization

TriMet and GTFS-Realtime data must be normalized before entering the application state.

Important normalization requirements:

- Convert timestamps into consistent UTC representations.
- Normalize delay conventions.
- Preserve vehicle identifiers and trip information.
- Handle missing fields safely.
- Distinguish stale data from missing data.

TriMet JSON vehicle delay uses negative values for late vehicles and positive values for early vehicles. Normalize this to the GTFS convention, where positive values indicate lateness.

## 7. Freshness and Reliability

The application must distinguish between:

**Upstream data age:** How old the information received from TriMet is.

**Internal delivery latency:** How long the backend takes to expose updated information through its API.

A source is considered stale when its data age exceeds three times its configured polling interval.

Missing predictions must never prevent vehicle tracking from working.

## 8. API Design

The backend initially exposes these REST endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /api/vehicles` | Current MAX vehicle positions |
| `GET /api/routes` | MAX route information |
| `GET /api/alerts` | Current service alerts |
| `GET /api/network/shapes` | Route geometry |
| `GET /api/health` | Data freshness and system health |

Server-Sent Events will be introduced later in M6, after the REST API is stable.

## 9. Quota Management

The documented default TriMet AppID allowance is 1,000,000 requests per day.

The backend must:

- Count every emitted HTTP request, including retries.
- Track requests across all polled sources.
- Warn at 80% of the configured budget.
- Halt polling if the budget is exceeded.
- Keep reset timezone and time configurable.

The actual quota reset behavior must be verified during M0.

## 10. Storage

Phase 1 uses in-memory state.

PostgreSQL is explicitly excluded from Phase 1.

Optional historical data collection may be introduced in M8 using compressed, append-only NDJSON files.

## 11. Frontend

The frontend is responsible for:

- Displaying MAX routes and stops.
- Displaying live train markers.
- Showing vehicle information when selected.
- Displaying freshness indicators.
- Showing service alerts and predictions in later milestones.

Leaflet is the approved mapping library.

## 12. Deployment

Docker Compose deployment is planned for M9.

Deployment must not block the first usable MVP.

## 13. Architectural Constraints

The following decisions are approved:

1. Hybrid TriMet JSON and GTFS-Realtime data sources.
2. Python and FastAPI backend.
3. React and TypeScript frontend.
4. Leaflet mapping.
5. REST before SSE.
6. No PostgreSQL in Phase 1.
7. Standard `gtfs-realtime-bindings` evaluated before custom protobuf code generation.
8. M4 is the MVP gate.

Architecture changes require explicit owner approval.