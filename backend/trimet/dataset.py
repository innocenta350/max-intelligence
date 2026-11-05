"""Dataset holder with atomic replacement (planned Stage S6, M1 plan §5/§B).

Planned responsibilities:

- ``DatasetManager.current: MaxNetwork | None``; ``load()`` runs
  download -> extract -> parse -> build -> validate -> single locked reference
  swap. A failed load never disturbs ``current`` (first-load failure leaves
  ``current is None``).
- Manifest with ``feed_version``, download metadata, and ``CoverageReport``.
- Startup inventory of staging dirs (ACTIVE/STALE/UNREADABLE classification
  from persisted UTC heartbeats) — report-only, never deletes foreign dirs
  (plan Rev. 4 change 1, Rev. 9 §3).

Intentionally empty in Stage S0.
"""
