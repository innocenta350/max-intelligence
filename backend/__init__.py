"""MAX Intelligence backend.

Phase 1 packages (see docs/ARCHITECTURE.md and docs/ROADMAP.md):

- ``backend/gtfs``  — generic GTFS machinery: CSV input/output, typed data
  structures, archive extraction, file parsing.
- ``backend/trimet`` — TriMet-specific logic: pipeline configuration, MAX
  route filtering, network assembly, and the dataset holder.

Created as Milestone M1 (Static GTFS) Stage S0 per the approved Rev. 9 plan.
All modules except ``backend/trimet/config.py`` are intentional placeholders
until their own stage begins.
"""
