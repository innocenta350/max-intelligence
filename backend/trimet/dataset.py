"""Dataset holder with atomic replacement and staging inventory (Stage S6).

Responsibilities (M1 plan §5/§B, as reconstructed from the approved S0–S5
implementation and recorded decisions — the Rev. 9 text is not in the
repository; see the S6 report for the documented reconstruction list):

- :class:`DatasetManager` holds the currently active, validated
  :class:`~backend.gtfs.types.MaxNetwork` in ``current``. ``load()`` runs the
  full pipeline — download/extract (S2) → parse (S4) → build/validate (S5) →
  promote (S6) — and only then swaps the new network into place. The swap is
  a single attribute assignment under a lock, so concurrent readers see
  either the whole previous network or the whole new one, never a partially
  constructed state. A failed load never disturbs ``current`` (a first-load
  failure leaves ``current is None``).
- Loads are single-flight: a ``threading.Lock`` is held for the entire
  pipeline, so two concurrent ``load()`` calls in one process serialize
  rather than racing two promotions.
- :func:`inventory_staging` classifies staging directories as ACTIVE, STALE,
  or UNREADABLE from their ``guard.json`` records. It is strictly
  report-only: foreign, stale, and unreadable staging directories are never
  adopted, refreshed, or deleted here (plan Rev. 4 change 1, Rev. 9 §3).
  STALE means the recorded heartbeat is older than
  ``GtfsConfig.stale_staging_ttl_minutes``; UNREADABLE means the guard record
  is missing, malformed, or type-broken.

The manifest's ``CoverageReport`` prose in the S0 placeholder is superseded
by the approved S1 design: coverage lives on :class:`MaxNetwork.coverage`
(§E.7), while the promoted manifest is the approved
:class:`~backend.gtfs.types.GtfsManifest` format.

No secrets in messages: file and directory *names* only, never absolute
paths, URLs, or credentials.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from backend.gtfs.errors import GtfsLoadError
from backend.gtfs.extractor import (
    StagedZip,
    _now_utc_ms,
    discard_staging_if_own,
    stage_gtfs,
)
from backend.gtfs.parser import PARSER_FILES, parse_dataset
from backend.gtfs.promotion import PromotionOutcome, promote_staged_dataset
from backend.gtfs.types import MaxNetwork
from backend.trimet.catalog import build_network
from backend.trimet.config import GtfsConfig

__all__ = [
    "STAGING_PREFIX",
    "STATUS_ACTIVE",
    "STATUS_STALE",
    "STATUS_UNREADABLE",
    "DatasetManager",
    "LoadOutcome",
    "StagingEntry",
    "inventory_staging",
]

STAGING_PREFIX = "staging-"
STATUS_ACTIVE = "ACTIVE"
STATUS_STALE = "STALE"
STATUS_UNREADABLE = "UNREADABLE"

_GUARD_NAME = "guard.json"


# ---------------------------------------------------------------------------
# Staging inventory (report-only)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StagingEntry:
    """One staging directory's diagnostic classification (report-only)."""

    name: str
    path: Path
    status: str  # STATUS_ACTIVE | STATUS_STALE | STATUS_UNREADABLE
    pid: int | None
    started_utc_ms: int | None
    heartbeat_utc_ms: int | None
    note: str


def inventory_staging(
    work_dir: Path | str, config: GtfsConfig, *, now_utc_ms: int | None = None
) -> tuple[StagingEntry, ...]:
    """Classify every staging directory under ``work_dir`` (diagnostic only).

    ACTIVE: a readable ``guard.json`` whose heartbeat is within
    ``stale_staging_ttl_minutes`` (regardless of which pid owns it — a live
    foreign staging directory is still active and is reported as such).
    STALE: a readable record whose heartbeat is older than the TTL.
    UNREADABLE: the directory exists but its guard record is missing,
    malformed, or type-broken.

    This function never adopts, refreshes, or deletes anything.
    """
    work_dir = Path(work_dir)
    now = _now_utc_ms() if now_utc_ms is None else now_utc_ms
    ttl_ms = config.stale_staging_ttl_minutes * 60 * 1000
    entries: list[StagingEntry] = []
    if not work_dir.is_dir():
        return tuple(entries)
    for entry in sorted(work_dir.iterdir()):
        if not entry.is_dir() or not entry.name.startswith(STAGING_PREFIX):
            continue
        guard = entry / _GUARD_NAME
        try:
            record = json.loads(guard.read_text(encoding="utf-8"))
            pid = record["pid"]
            started = record["started_utc_ms"]
            heartbeat = record["heartbeat_utc_ms"]
            if (
                isinstance(pid, bool)
                or not isinstance(pid, int)
                or isinstance(started, bool)
                or not isinstance(started, int)
                or isinstance(heartbeat, bool)
                or not isinstance(heartbeat, int)
            ):
                raise TypeError("guard fields are not integers")
        except (OSError, ValueError, KeyError, TypeError):
            entries.append(
                StagingEntry(
                    name=entry.name,
                    path=entry,
                    status=STATUS_UNREADABLE,
                    pid=None,
                    started_utc_ms=None,
                    heartbeat_utc_ms=None,
                    note=(
                        f"{_GUARD_NAME} is missing, malformed, or unreadable; "
                        "ownership and liveness cannot be determined"
                    ),
                )
            )
            continue
        if now - heartbeat > ttl_ms:
            entries.append(
                StagingEntry(
                    name=entry.name,
                    path=entry,
                    status=STATUS_STALE,
                    pid=pid,
                    started_utc_ms=started,
                    heartbeat_utc_ms=heartbeat,
                    note=(
                        f"heartbeat is older than the configured "
                        f"{config.stale_staging_ttl_minutes}-minute TTL; "
                        "reported only, never deleted here"
                    ),
                )
            )
        else:
            entries.append(
                StagingEntry(
                    name=entry.name,
                    path=entry,
                    status=STATUS_ACTIVE,
                    pid=pid,
                    started_utc_ms=started,
                    heartbeat_utc_ms=heartbeat,
                    note="guard heartbeat is within the TTL",
                )
            )
    return tuple(entries)


# ---------------------------------------------------------------------------
# DatasetManager
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LoadOutcome:
    """What one completed load did (diagnostics for CLI/reporting)."""

    network: MaxNetwork
    promotion: PromotionOutcome
    reused: bool
    notes: tuple[str, ...]


class DatasetManager:
    """Holds the active :class:`MaxNetwork`; loads are atomic and single-flight.

    ``current`` is safe to read from any thread at any time: it returns
    either the previous fully validated network or the new one, never a
    partially constructed object. The load pipeline itself runs under a
    separate single-flight lock so concurrent loads serialize instead of
    racing. On any pipeline failure the previous network is untouched, and
    the staging directory this load created is removed only when its
    ``guard.json`` still attests to this process's pid.
    """

    def __init__(self, config: GtfsConfig) -> None:
        self._config = config
        self._network: MaxNetwork | None = None
        self._swap_lock = threading.Lock()
        self._load_lock = threading.Lock()

    @property
    def current(self) -> MaxNetwork | None:
        """The active network, or ``None`` before the first successful load."""
        with self._swap_lock:
            return self._network

    def load(self) -> LoadOutcome:
        """Run the full pipeline and atomically activate the new network.

        Sequence: download/extract into owned staging (with the parser's
        required members enforced at extraction time) → parse → build and
        validate the network → promote under the immutable ``promote.lock`` →
        swap. Only a successful promotion activates the dataset. On failure
        the previous ``current`` is preserved, this load's staging directory
        is discarded if it is still verifiably ours, and the pipeline error
        propagates.

        Staging hygiene (S7 integration fix): a fresh promotion renames the
        staging directory into the dataset version, but an *idempotent
        reuse* short-circuit leaves it in place — so the manager discards it
        (ownership-verified) whenever promotion reports reuse. Without this,
        every reused load leaked one owned staging directory (observed live
        during S7 verification).
        """
        with self._load_lock:
            staged: StagedZip | None = None
            try:
                staged = stage_gtfs(
                    self._config, required_members=PARSER_FILES
                )
                parsed = parse_dataset(staged.staging_dir, self._config)
                network = build_network(parsed, self._config)
                promotion = promote_staged_dataset(
                    staged, network.feed_version, self._config
                )
            except GtfsLoadError as exc:
                if staged is not None:
                    note = discard_staging_if_own(staged.staging_dir)
                    if note is not None:
                        exc.cleanup_failure = note
                raise
            notes = list(promotion.notes)
            if promotion.reused:
                # The reuse short-circuit did not consume the staging
                # directory; clean up our own now (S7 integration fix).
                note = discard_staging_if_own(staged.staging_dir)
                if note is not None:
                    notes.append(note)
            with self._swap_lock:
                self._network = network
            return LoadOutcome(
                network=network,
                promotion=promotion,
                reused=promotion.reused,
                notes=tuple(notes),
            )
