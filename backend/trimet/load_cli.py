"""CLI entry point: ``python -m backend.trimet.load_cli`` (Stage S6).

Subcommands (M1 plan Rev. 2 §A change 3 and §C):

- ``load`` — the full S6 pipeline: read configuration, download and safely
  extract the feed, parse it, build and validate the MAX network, promote
  the dataset version, and activate it in the :class:`DatasetManager`.
  Invoked bare (no subcommand), ``load`` runs by default. Prints a summary
  of the activated dataset and exits 0, or prints the failure reason and
  exits 1.

Output hygiene: diagnostics name files and directories only — never the
feed URL, never environment contents, never credentials.
"""

from __future__ import annotations

import argparse
import sys

from backend.gtfs.errors import GtfsLoadError
from backend.trimet.config import GtfsConfig
from backend.trimet.dataset import DatasetManager

_PROGRAM = "python -m backend.trimet.load_cli"


def _print_load_summary(outcome) -> None:  # type: ignore[no-untyped-def]
    """Print the activated dataset summary (names only, never URLs/secrets)."""
    network = outcome.network
    promotion = outcome.promotion
    print("Dataset loaded and activated.")
    print(f"  feed version:    {network.feed_version}")
    print(f"  dataset version: {promotion.version_name}")
    print(f"  reused existing: {'yes' if outcome.reused else 'no'}")
    print(f"  routes:          {len(network.routes)}")
    print(f"  stops:           {len(network.stops)}")
    print(f"  trips:           {len(network.trips)}")
    print(f"  shapes:          {len(network.shapes)}")
    if network.feed_start_date is not None:
        print(f"  feed start date: {network.feed_start_date}")
    if network.feed_end_date is not None:
        print(f"  feed end date:   {network.feed_end_date}")
    for note in outcome.notes:
        print(f"  note: {note}")


def _run_load(config: GtfsConfig) -> int:
    """Run the full load pipeline; return the process exit code."""
    manager = DatasetManager(config)
    try:
        outcome = manager.load()
    except GtfsLoadError as exc:
        cleanup = getattr(exc, "cleanup_failure", None)
        print(f"Load failed: {exc}", file=sys.stderr)
        if cleanup:
            print(f"Cleanup problem: {cleanup}", file=sys.stderr)
        return 1
    _print_load_summary(outcome)
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(
        prog=_PROGRAM,
        description="Load and activate the TriMet static GTFS MAX dataset.",
    )
    subparsers = parser.add_subparsers(dest="command")
    # `load` is the only subcommand; bare invocation runs it by default.
    subparsers.add_parser(
        "load",
        help="Download, extract, parse, validate, promote, and activate "
        "the dataset (the default action).",
    )
    args = parser.parse_args(argv)
    command = args.command or "load"
    if command != "load":
        parser.error(f"unknown command {command!r}")  # exits 2

    try:
        config = GtfsConfig.from_env()
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1
    return _run_load(config)


if __name__ == "__main__":
    raise SystemExit(main())
