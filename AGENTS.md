# MAX Intelligence - AI Agent Instructions

## 1. Project Overview

MAX Intelligence is an independent analytics platform for Portland's TriMet MAX light rail system.

The goal is to improve visibility into MAX operations through:
- Live vehicle tracking.
- Service alert monitoring.
- Historical transit data collection.
- Schedule adherence analysis.
- Headway consistency analysis.
- Transit reliability and performance analytics.

The project is being developed incrementally, with a working live MAX map as the first major product milestone.

## 2. Developer Experience

The project owner is an intermediate programmer and relies on AI-assisted development.

All agents must:
- Explain important technical decisions in understandable language.
- Explain terminal commands before asking the owner to run them.
- Avoid assuming the owner understands advanced programming concepts.
- Clearly explain errors, test results, and implementation changes.
- Distinguish between completed work and proposed work.
- Never claim something works without testing or other supporting evidence.

The owner makes final decisions about architecture, features, and implementation.

## 3. Approved Technology Stack

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

### Data Sources
- TriMet native JSON Vehicle Location Service.
- TriMet native JSON Service Alerts.
- TriMet GTFS-Realtime feeds.
- TriMet static GTFS data.

### Infrastructure
- Git and GitHub for version control.
- Docker Compose for optional deployment.

Do not introduce additional frameworks, databases, or infrastructure without approval.

## 4. Approved Architecture Decisions

The following decisions are already approved and must not be reconsidered without new evidence or explicit owner approval.

1. Use TriMet's native JSON vehicle endpoint for live vehicle positions.
2. Use TriMet's native JSON alerts endpoint for service alerts.
3. Use GTFS-Realtime TripUpdates for stop-level predictions.
4. Use static GTFS for routes, stops, shapes, and direction names.
5. Use Python and FastAPI for the backend.
6. Use React and TypeScript for the frontend.
7. Use Leaflet instead of MapLibre GL.
8. Use REST endpoints before implementing Server-Sent Events (SSE).
9. Do not introduce PostgreSQL during Phase 1.
10. Lightweight compressed NDJSON history is optional and belongs in M8.
11. Evaluate the standard `gtfs-realtime-bindings` library before considering custom protobuf code generation.

### Vehicle Data Correctness

Vehicle tracking must support:
- Incremental polling every 10 seconds using `since`.
- Full-snapshot reconciliation every 5 minutes.
- Vehicle updates merged using `vehicleID`.
- Removal of expired vehicles using verified `expires` semantics.
- Removal of vehicles missing from authoritative full snapshots.
- Fallback to full polling when incremental polling fails.

Reconciliation must not overwrite newer incremental updates or incorrectly remove vehicles updated while a snapshot is being fetched. Use serialization or version/timestamp safeguards.

### Quota Management

- Default documented AppID quota: 1,000,000 requests per day.
- Count every emitted HTTP request, including retries.
- Warn at 80% of the configured budget.
- Halt polling if the budget is exceeded.
- Keep the reset timezone and time configurable.
- Do not claim TriMet's actual quota reset behavior is verified until M0 provides evidence.

### Data Integrity

- Normalize TriMet and GTFS-Realtime timestamps into consistent UTC representations.
- Normalize delay conventions before exposing them to the frontend.
- Distinguish upstream data age from internal API delivery latency.
- Never assume TripUpdate coverage is complete.
- Missing predictions must not break live vehicle tracking.

## 5. Development Workflow

Development must follow the approved Phase 1 roadmap in `docs/ROADMAP.md`.

Milestone order:

M0 → M1 → M2 → M3 → M4 → M5 → M6 → M7 → M8 (optional) → M9 (optional).

Rules:
- Work on one milestone at a time.
- Do not skip milestones without owner approval.
- Do not begin later milestones while earlier acceptance criteria remain unmet.
- M4 is the first usable product milestone and must not be delayed by optional features.
- M8 and M9 are optional and must not block the MVP.
- Do not introduce scope creep.

## 6. Implementation Rules

Before making changes:
1. Read this file.
2. Read the relevant sections of `docs/ARCHITECTURE.md`.
3. Read the relevant milestone in `docs/ROADMAP.md`.
4. Inspect the existing codebase.
5. Explain the proposed implementation if it involves significant architectural changes.

While implementing:
- Make small, focused changes.
- Follow existing naming conventions and project structure.
- Avoid unnecessary dependencies.
- Keep TriMet-specific API logic isolated in the backend's `trimet/` package.
- Use configuration variables instead of hardcoding credentials or environment-specific settings.
- Handle missing, stale, malformed, and incomplete data safely.
- Do not silently discard important errors.

Do not rewrite working code simply to improve style or personal preference.

## 7. Testing and Verification

Testing is required for every meaningful implementation.

Prioritize:
- Unit tests for data normalization.
- Tests for incremental vehicle updates.
- Tests for expiry and reconciliation removal.
- Tests for concurrent vehicle updates.
- Tests for quota accounting and reset behavior.
- API contract tests.
- Integration tests using mocked TriMet responses.

Live API tests must be explicitly enabled using `LIVE_FEED_TESTS=1`.

Never:
- Claim tests passed if they were not executed.
- Hide failing tests.
- Replace real verification with assumptions.
- Use arbitrary test coverage percentages as a substitute for testing critical behavior.

Explain what was tested and report any failures.

## 8. Security Rules

- Never expose the actual TriMet AppID in source code, documentation, logs, screenshots, or Git commits.
- Load credentials from environment variables.
- Keep `.env` in `.gitignore`.
- Commit `.env.example` with placeholders only.
- Never hardcode secrets.
- Do not weaken security settings to make tests pass.

If a credential is accidentally exposed, immediately notify the owner.

## 9. Git and GitHub Rules

GitHub is the project's source of truth.

- Make changes on the existing project branch unless instructed otherwise.
- Do not overwrite or discard existing owner work.
- Do not commit unfinished or unapproved work.
- Do not push to GitHub without explicit owner approval.
- Before committing, summarize changed files and test results.
- Only commit and push after the owner explicitly approves the completed changes.

Use clear, descriptive commit messages.

## 10. Communication and Reporting

At the end of each task, report:

1. What was completed.
2. Which files were created or modified.
3. What tests or verification were performed.
4. Whether acceptance criteria were met.
5. Any remaining problems or unresolved questions.
6. What should happen next.

Use these statuses:
- **PASS:** Verified and working.
- **FAIL:** Tested and not working.
- **UNVERIFIED:** Not yet tested or insufficient evidence.
- **BLOCKED:** Cannot proceed because of an external dependency or unresolved issue.

Never present assumptions as verified facts.

## 11. Scope and Approval

The project owner has final authority over:
- Architecture changes.
- Technology choices.
- Feature prioritization.
- New dependencies.
- Milestone completion.
- Git commits and pushes.

If an implementation requires changing an approved architectural decision, stop and explain why before proceeding.

When requirements are ambiguous, ask for clarification rather than making major assumptions.

**Primary objective: Build a reliable, accurate, and maintainable MAX Intelligence MVP before expanding its capabilities.**