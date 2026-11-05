"""TriMet-specific pipeline logic.

Per AGENTS.md §6, TriMet implementation detail stays isolated in the backend's
``trimet/`` package; generic machinery lives in ``backend/gtfs``.

Modules (M1 plan Rev. 9, §B): ``config`` (implemented in S0), ``maxfilter`` (S5),
``catalog`` (S5), ``dataset`` (S6), ``load_cli`` (S3/S6 CLI entry points).
"""
