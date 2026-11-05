"""CLI entry point: ``python -m backend.trimet.load_cli`` (planned Stages S3/S6).

Planned subcommands (M1 plan Rev. 2 §A change 3 and §C):

- ``inspect`` — Stage S3: one real gtfs.zip download into staging, then dump
  every member's header rows, sample rows, and row counts to
  ``data/gtfs/inspection.json`` plus a printed summary. Used to design the
  ``route_directions.txt`` / ``linked_datasets.txt`` parsers from observed
  reality before any parser code exists.
- ``load``     — Stage S6: full download->extract->parse->build->validate->
  promote->swap pipeline; prints the dataset summary and writes the manifest.
- ``--verify-live`` — optional single live routeConfig cross-check (plan D4).

Intentionally empty in Stage S0.
"""
