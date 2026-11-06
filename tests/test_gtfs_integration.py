"""Stage S7 integration tests: the complete pipeline as one system.

These tests exercise *interactions* between the S0–S6 modules rather than
repeating their unit tests: full loads through :class:`DatasetManager` over
a loopback HTTP server (the S2/S6 test convention), failure injection at
pipeline boundaries, promotion-state corruption encountered through the
manager, retention with an active dataset attached, and foreign-artifact
preservation.

All offline: synthetic feeds built from
``tests.test_gtfs_catalog._DEFAULT_FEED``, loopback HTTP only. Live-feed
verification is performed separately (see the S7 report); the live tests
remain opt-in via ``LIVE_FEED_TESTS=1``.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from backend.gtfs.errors import (
    ExtractionError,
    GtfsLoadError,
    ManifestUnreadableError,
    RetryablePromoteLockError,
)
from backend.gtfs.promotion import LOCK_NAME, MANIFEST_NAME, read_manifest
from backend.gtfs.types import ManifestValid
from backend.trimet.config import GtfsConfig
from backend.trimet.dataset import DatasetManager, inventory_staging
from backend.trimet.maxfilter import MAX_ROUTE_IDS
from tests.test_gtfs_dataset import (
    _config,
    _feed_server,
    _feed_zip_bytes,
    _make_guard,
    _versions,
)
from tests.test_gtfs_catalog import _DEFAULT_FEED


# ---------------------------------------------------------------------------
# A. End-to-end successful load: exact network content
# ---------------------------------------------------------------------------


def test_reused_load_leaves_no_staging_behind(tmp_path: Path) -> None:
    """S7 regression: an idempotent reuse must not leak its staging dir.

    Observed live during S7 verification: a reused promotion short-circuits
    without consuming the staging directory, and nothing else cleaned it up,
    so every reused load left one owned ``staging-*`` directory behind.
    """
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        first = manager.load()
        assert inventory_staging(config.work_dir, config) == ()
        second = manager.load()
    assert second.reused is True
    assert second.promotion.dataset_dir == first.promotion.dataset_dir
    # No staging directory remains after either load.
    assert inventory_staging(config.work_dir, config) == ()
    assert not list(config.work_dir.glob("staging-*"))


def test_e2e_activated_network_has_exactly_the_five_max_routes(
    tmp_path: Path,
) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        manager = DatasetManager(_config(tmp_path, url))
        manager.load()
    network = manager.current
    assert network is not None
    # Exactly the five MAX routes, by id and color.
    assert set(network.routes) == MAX_ROUTE_IDS
    assert set(network.routes) == {"90", "100", "190", "200", "290"}
    colors = {
        route_id: network.routes[route_id].route_color
        for route_id in network.routes
    }
    assert colors == {
        "90": "C41F3E",
        "100": "1359AE",
        "190": "FFC52F",
        "200": "008342",
        "290": "D05F27",
    }
    # Portland Streetcar (route 193, type 0) and the bus route are excluded.
    assert "193" not in network.routes
    assert "1" not in network.routes
    # Every MAX route has at least one trip and stop times (M1 criterion).
    for route_id in MAX_ROUTE_IDS:
        trips = [
            trip
            for trip in network.trips.values()
            if trip.route_id == route_id
        ]
        assert trips, f"route {route_id} has no trips"
        for trip in trips:
            assert network.stop_times[trip.trip_id]


# ---------------------------------------------------------------------------
# B/C. Promotion-state corruption encountered through the manager
# ---------------------------------------------------------------------------


def test_reload_with_tampered_existing_dataset_fails_loudly(
    tmp_path: Path,
) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        manager.load()
        previous = manager.current
        assert previous is not None

    # Corrupt the promoted version's archived zip: its manifest still reads
    # valid, but the bytes no longer hash to the recorded digest. The next
    # load must refuse loudly (never reuse) and preserve the active network.
    version_dir = config.work_dir / _versions(config.work_dir)[0]
    (version_dir / "gtfs.zip").write_bytes(b"tampered contents")
    with _feed_server(body) as url:
        # Re-point the manager at the *live* server; the load must reach the
        # download and fail at reuse verification, not at connection setup.
        manager._config = _config(tmp_path, url).__class__(
            feed_url=url,
            work_dir=config.work_dir,
            m0_routeconfig_evidence_path=config.m0_routeconfig_evidence_path,
            promotion_max_attempts=2,
            promotion_backoff_seconds=(0.001, 0.001),
        )
        with pytest.raises(ExtractionError):
            manager.load()
    assert manager.current is previous
    # The corrupt version was neither reused nor deleted.
    assert version_dir.is_dir()


def test_reload_with_corrupted_manifest_fails_loudly(tmp_path: Path) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        manager.load()
        previous = manager.current

    # Shred the promoted version's manifest: the name is occupied by a
    # directory without a valid manifest, so promotion must fail safe.
    version_dir = config.work_dir / _versions(config.work_dir)[0]
    (version_dir / MANIFEST_NAME).write_text("{broken", encoding="utf-8")
    with _feed_server(body) as url:
        manager._config = GtfsConfig(
            feed_url=url,
            work_dir=config.work_dir,
            m0_routeconfig_evidence_path=config.m0_routeconfig_evidence_path,
            promotion_max_attempts=2,
            promotion_backoff_seconds=(0.001, 0.001),
        )
        with pytest.raises(ManifestUnreadableError):
            manager.load()
    assert manager.current is previous
    assert version_dir.is_dir()
    # Our new staging directory was cleaned up under the ownership rules.
    assert inventory_staging(config.work_dir, config) == ()


# ---------------------------------------------------------------------------
# C. Distinct feeds, retention, and the active dataset
# ---------------------------------------------------------------------------


def test_three_distinct_feeds_retain_two_and_keep_current(
    tmp_path: Path,
) -> None:
    feeds = [
        _feed_zip_bytes(),
        _feed_zip_bytes(
            {
                "feed_info.txt": (
                    "feed_version,feed_start_date,feed_end_date\n"
                    "20260201-20260301-0900,20260201,20260301\n"
                )
            }
        ),
        _feed_zip_bytes(
            {
                "feed_info.txt": (
                    "feed_version,feed_start_date,feed_end_date\n"
                    "20260301-20260401-0900,20260301,20260401\n"
                )
            }
        ),
    ]
    versions_seen: list[str] = []
    manager: DatasetManager | None = None
    config: GtfsConfig | None = None
    for body in feeds:
        with _feed_server(body) as url:
            # One shared work_dir across all three loads.
            if manager is None:
                config = _config(tmp_path, url)
                manager = DatasetManager(config)
            else:
                manager._config = _config(tmp_path, url).__class__(
                    feed_url=url,
                    work_dir=config.work_dir,
                    m0_routeconfig_evidence_path=(
                        config.m0_routeconfig_evidence_path
                    ),
                    promotion_max_attempts=2,
                    promotion_backoff_seconds=(0.001, 0.001),
                )
            outcome = manager.load()
            versions_seen.append(outcome.promotion.version_name)
    assert len(set(versions_seen)) == 3  # three distinct versions were used
    # Retention kept exactly two valid versions...
    surviving = _versions(config.work_dir)
    assert len(surviving) == 2
    # ...including the one backing the active network.
    current_version = _versions(config.work_dir)
    assert manager.current is not None
    # The current network's feed version matches the newest promotion.
    assert manager.current.feed_version == "20260301-20260401-0900"
    for name in current_version:
        assert isinstance(
            read_manifest(config.work_dir / name), ManifestValid
        )


# ---------------------------------------------------------------------------
# Foreign artifacts are never auto-removed
# ---------------------------------------------------------------------------


def test_foreign_promote_lock_blocks_load_and_survives(
    tmp_path: Path,
) -> None:
    body = _feed_zip_bytes()
    foreign_lock = {
        "pid": 999_999_999,
        "token": "f" * 32,
        "acquired_utc_ms": 1,
        "repo_paths_hint": ["<foreign>"],
    }
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        config.work_dir.mkdir(parents=True, exist_ok=True)
        (config.work_dir / LOCK_NAME).write_text(
            json.dumps(foreign_lock), encoding="utf-8"
        )
        manager = DatasetManager(config)
        with pytest.raises(RetryablePromoteLockError):
            manager.load()
    # The foreign lock was never removed and never rewritten.
    assert (
        json.loads((config.work_dir / LOCK_NAME).read_text(encoding="utf-8"))
        == foreign_lock
    )
    assert manager.current is None


def test_foreign_staging_directory_survives_a_load(tmp_path: Path) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        config.work_dir.mkdir(parents=True, exist_ok=True)
        foreign = config.work_dir / "staging-999999999-foreign"
        foreign.mkdir()
        _make_guard(foreign, pid=999_999_999)
        manager = DatasetManager(config)
        manager.load()
    # The foreign staging directory is untouched and still classified.
    assert foreign.is_dir()
    entries = inventory_staging(config.work_dir, config)
    assert any(
        entry.name == foreign.name and entry.pid == 999_999_999
        for entry in entries
    )


def test_uncertain_version_survives_retention_with_current_attached(
    tmp_path: Path,
) -> None:
    # First load activates a dataset; then a foreign, manifest-less
    # "dataset-*" directory appears; a second, different load must promote
    # past it (ladder), keep it (retention never deletes uncertain dirs),
    # and keep the newly activated version.
    first = _feed_zip_bytes()
    with _feed_server(first) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        manager.load()
    uncertain = config.work_dir / "dataset-ffffffffffffffff"
    uncertain.mkdir()
    (uncertain / "some-file.txt").write_text("not a dataset", encoding="utf-8")
    second = _feed_zip_bytes(
        {
            "feed_info.txt": (
                "feed_version,feed_start_date,feed_end_date\n"
                "20260201-20260301-0900,20260201,20260301\n"
            )
        }
    )
    with _feed_server(second) as url:
        manager._config = GtfsConfig(
            feed_url=url,
            work_dir=config.work_dir,
            m0_routeconfig_evidence_path=config.m0_routeconfig_evidence_path,
            promotion_max_attempts=2,
            promotion_backoff_seconds=(0.001, 0.001),
        )
        outcome = manager.load()
    assert manager.current.feed_version == "20260201-20260301-0900"
    assert uncertain.is_dir()  # uncertain: never deleted
    assert outcome.promotion.dataset_dir.is_dir()


# ---------------------------------------------------------------------------
# D. CLI module entry point (subprocess level)
# ---------------------------------------------------------------------------


def test_cli_module_entry_point_help(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``python -m backend.trimet.load_cli --help`` works as documented."""
    result = subprocess.run(
        [sys.executable, "-m", "backend.trimet.load_cli", "--help"],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    assert result.returncode == 0
    assert "load" in result.stdout
    # No credentials or URLs in help output.
    assert "http" not in result.stdout
    assert "appid" not in result.stdout.lower()
