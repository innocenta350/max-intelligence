"""Stage S6 checks for DatasetManager, staging inventory, CLI, and e2e.

Three layers, all offline:

1. Staging inventory classification against hand-built staging directories.
2. :class:`DatasetManager` lifecycle: initial ``None``, successful load,
   failed load preserving the previous dataset, single-flight loads.
3. End-to-end pipeline through a threaded loopback HTTP server (the S2 test
   convention: real socket I/O, no mocks) serving a synthetic 8-file GTFS
   zip built from ``tests.test_gtfs_catalog._DEFAULT_FEED``.

Configs are constructed directly (bypassing ``from_env``'s HTTPS-only rule)
so the loopback HTTP server can stand in for the real feed host; production
config semantics are unchanged. The CLI tests monkeypatch
``GtfsConfig.from_env`` inside the CLI module for the same reason.
"""

from __future__ import annotations

import io
import json
import threading
import zipfile
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from backend.gtfs.errors import GtfsLoadError, MaxInvariantError
from backend.gtfs.extractor import _now_utc_ms, create_staging_dir
from backend.gtfs.parser import PARSER_FILES
from backend.gtfs.promotion import MANIFEST_NAME, VERSION_PREFIX, read_manifest
from backend.gtfs.types import ManifestValid
from backend.trimet.config import GtfsConfig
from backend.trimet.dataset import (
    STATUS_ACTIVE,
    STATUS_STALE,
    STATUS_UNREADABLE,
    DatasetManager,
    inventory_staging,
)
from backend.trimet.load_cli import main as cli_main
from tests.test_gtfs_catalog import _DEFAULT_FEED, _evidence_json


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def _feed_zip_bytes(overrides: dict[str, str | None] | None = None) -> bytes:
    """Build a synthetic GTFS zip from the default valid feed."""
    files = dict(_DEFAULT_FEED)
    if overrides:
        files.update(overrides)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            if content is None:
                continue
            archive.writestr(name, content)
    return buffer.getvalue()


@contextmanager
def _feed_server(body: bytes):
    """Serve one canned zip body on loopback; yields the request URL."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:  # keep test output clean
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/gtfs.zip"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _config(
    tmp_path: Path, url: str, *, reuse_promoted: bool = True
) -> GtfsConfig:
    evidence = tmp_path / "routeconfig_selected.json"
    evidence.write_text(_evidence_json(), encoding="utf-8", newline="")
    return GtfsConfig(
        feed_url=url,
        work_dir=tmp_path / "work",
        m0_routeconfig_evidence_path=evidence,
        promotion_max_attempts=2,
        promotion_backoff_seconds=(0.001, 0.001),
        reuse_promoted=reuse_promoted,
    )


def _make_guard(staging_dir: Path, *, pid: int | None = None) -> None:
    record = {
        "pid": pid if pid is not None else 4_000_000,
        "started_utc_ms": _now_utc_ms(),
        "heartbeat_utc_ms": _now_utc_ms(),
    }
    (staging_dir / "guard.json").write_text(
        json.dumps(record), encoding="utf-8"
    )


def _versions(work_dir: Path) -> list[str]:
    return sorted(
        p.name
        for p in work_dir.iterdir()
        if p.is_dir() and p.name.startswith(VERSION_PREFIX)
    )


# ---------------------------------------------------------------------------
# Staging inventory (report-only)
# ---------------------------------------------------------------------------


def test_inventory_empty_work_dir(tmp_path: Path) -> None:
    config = GtfsConfig(work_dir=tmp_path)
    assert inventory_staging(tmp_path, config) == ()


def test_inventory_ignores_non_staging_entries(tmp_path: Path) -> None:
    (tmp_path / "dataset-abc").mkdir()
    (tmp_path / "promote.lock").write_text("{}", encoding="utf-8")
    config = GtfsConfig(work_dir=tmp_path)
    assert inventory_staging(tmp_path, config) == ()


def test_inventory_classifies_active(tmp_path: Path) -> None:
    staging = create_staging_dir(tmp_path)
    _make_guard(staging)
    config = GtfsConfig(work_dir=tmp_path)
    entries = inventory_staging(tmp_path, config)
    assert len(entries) == 1
    assert entries[0].status == STATUS_ACTIVE
    assert entries[0].pid == 4_000_000


def test_inventory_classifies_stale_beyond_ttl(tmp_path: Path) -> None:
    staging = create_staging_dir(tmp_path)
    old = _now_utc_ms() - 61 * 60 * 1000  # 61 minutes old
    (staging / "guard.json").write_text(
        json.dumps(
            {
                "pid": 4_000_000,
                "started_utc_ms": old,
                "heartbeat_utc_ms": old,
            }
        ),
        encoding="utf-8",
    )
    config = GtfsConfig(work_dir=tmp_path, stale_staging_ttl_minutes=30)
    entries = inventory_staging(tmp_path, config)
    assert entries[0].status == STATUS_STALE


def test_inventory_classifies_unreadable_guard(tmp_path: Path) -> None:
    staging = create_staging_dir(tmp_path)
    (staging / "guard.json").write_text("{broken", encoding="utf-8")
    config = GtfsConfig(work_dir=tmp_path)
    entries = inventory_staging(tmp_path, config)
    assert entries[0].status == STATUS_UNREADABLE


def test_inventory_missing_guard_is_unreadable(tmp_path: Path) -> None:
    create_staging_dir(tmp_path)  # no guard written
    config = GtfsConfig(work_dir=tmp_path)
    entries = inventory_staging(tmp_path, config)
    assert entries[0].status == STATUS_UNREADABLE


def test_inventory_never_modifies_entries(tmp_path: Path) -> None:
    staging = create_staging_dir(tmp_path)
    _make_guard(staging)
    before = (staging / "guard.json").read_text(encoding="utf-8")
    config = GtfsConfig(work_dir=tmp_path)
    inventory_staging(tmp_path, config)
    assert (staging / "guard.json").read_text(encoding="utf-8") == before
    assert staging.is_dir()


# ---------------------------------------------------------------------------
# DatasetManager lifecycle (pure in-memory; no pipeline)
# ---------------------------------------------------------------------------


def test_manager_initial_current_is_none(tmp_path: Path) -> None:
    manager = DatasetManager(GtfsConfig(work_dir=tmp_path))
    assert manager.current is None


def test_manager_successful_load_activates(tmp_path: Path) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        outcome = manager.load()
    assert manager.current is outcome.network
    assert manager.current.feed_version == "20260101-20260201-0900"
    assert outcome.reused is False
    assert len(manager.current.routes) == 5
    # Exactly one promoted version, with a valid manifest.
    versions = _versions(config.work_dir)
    assert len(versions) == 1
    assert isinstance(
        read_manifest(config.work_dir / versions[0]), ManifestValid
    )


def test_manager_failed_load_preserves_previous(tmp_path: Path) -> None:
    good = _feed_zip_bytes()
    with _feed_server(good) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        manager.load()
    previous = manager.current
    assert previous is not None

    # A broken feed (routes.txt dropped -> parse failure) must not disturb
    # the activated network. The manager holds one config, so the test
    # re-points it at the bad URL (private attribute, test-only).
    bad = _feed_zip_bytes({"routes.txt": None})
    with _feed_server(bad) as url:
        bad_config = _config(tmp_path, url)
        bad_config = GtfsConfig(
            feed_url=url,
            work_dir=config.work_dir,
            m0_routeconfig_evidence_path=config.m0_routeconfig_evidence_path,
            promotion_max_attempts=2,
            promotion_backoff_seconds=(0.001, 0.001),
        )
        manager._config = bad_config  # test-only re-point at the broken feed
        with pytest.raises(GtfsLoadError):
            manager.load()
    assert manager.current is previous


def test_manager_failed_first_load_leaves_none(tmp_path: Path) -> None:
    bad = _feed_zip_bytes({"routes.txt": None})
    with _feed_server(bad) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        with pytest.raises(GtfsLoadError):
            manager.load()
    assert manager.current is None


def test_manager_failure_cleans_own_staging(tmp_path: Path) -> None:
    bad = _feed_zip_bytes({"routes.txt": None})
    with _feed_server(bad) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        with pytest.raises(GtfsLoadError):
            manager.load()
    # Our staging directory was discarded; nothing lingers.
    assert inventory_staging(config.work_dir, config) == ()
    assert _versions(config.work_dir) == []


def test_manager_loads_are_single_flight(tmp_path: Path) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        results: list = []
        errors: list = []

        def run() -> None:
            try:
                results.append(manager.load())
            except GtfsLoadError as exc:  # pragma: no cover - unexpected
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert not errors
        assert len(results) == 3
    # All three loads ran serialized (single-flight) and each swap replaced
    # the whole network; the final activation is the last load's network.
    assert manager.current is results[-1].network
    assert len(_versions(config.work_dir)) == 1


def test_invariant_failure_prevents_promotion(tmp_path: Path) -> None:
    # Route 290 loses its trip and stop times: parsing succeeds, but the S5
    # six-invariant validation refuses the network — so nothing is promoted.
    trips = (
        "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
        + "".join(
            f"{route_id},WKDY,T{route_id},,0,SH{route_id}\n"
            for route_id in ("90", "100", "190", "200")
        )
        + "1,WKDY,T1,,0,SHX\n"
    )
    stop_times = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        + "".join(
            f"T{route_id},08:00:00,08:00:30,S1P,0\n"
            f"T{route_id},25:10:30,25:11:00,S2,1\n"
            for route_id in ("90", "100", "190", "200")
        )
    )
    feed = _feed_zip_bytes(
        {"trips.txt": trips, "stop_times.txt": stop_times}
    )
    with _feed_server(feed) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        with pytest.raises(MaxInvariantError):
            manager.load()
    assert _versions(config.work_dir) == []


def test_repeated_identical_feeds_are_idempotent(tmp_path: Path) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        first = manager.load()
        second = manager.load()
    assert first.reused is False
    assert second.reused is True
    # Reuse happens at promotion: the second load re-parses and rebuilds a
    # fresh (equal) network, but activates the same promoted version.
    assert second.network == first.network
    assert second.promotion.dataset_dir == first.promotion.dataset_dir
    assert len(_versions(config.work_dir)) == 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_load_success_exit_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        monkeypatch.setattr(
            "backend.trimet.load_cli.GtfsConfig.from_env",
            classmethod(lambda cls, env_file=None: config),
        )
        code = cli_main(["load"])
    assert code == 0
    output = capsys.readouterr().out
    assert "20260101-20260201-0900" in output
    assert "reused existing: no" in output
    assert url not in output  # no feed URL in output
    assert "http" not in output


def test_cli_bare_invocation_defaults_to_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = _feed_zip_bytes()
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        monkeypatch.setattr(
            "backend.trimet.load_cli.GtfsConfig.from_env",
            classmethod(lambda cls, env_file=None: config),
        )
        assert cli_main([]) == 0


def test_cli_failure_exit_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    bad = _feed_zip_bytes({"routes.txt": None})
    with _feed_server(bad) as url:
        config = _config(tmp_path, url)
        monkeypatch.setattr(
            "backend.trimet.load_cli.GtfsConfig.from_env",
            classmethod(lambda cls, env_file=None: config),
        )
        code = cli_main(["load"])
    assert code == 1
    assert "Load failed" in capsys.readouterr().err


def test_cli_config_error_exit_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    def broken_from_env(cls, env_file=None):  # type: ignore[no-untyped-def]
        raise ValueError("GTFS_MAX_ZIP_BYTES must be a positive number of bytes")

    monkeypatch.setattr(
        "backend.trimet.load_cli.GtfsConfig.from_env", classmethod(broken_from_env)
    )
    assert cli_main(["load"]) == 1
    assert "Configuration error" in capsys.readouterr().err


def test_cli_required_members_enforced(tmp_path: Path) -> None:
    # Sanity: the pipeline requests the parser's required members at
    # extraction time, so an archive missing one fails before parsing.
    body = _feed_zip_bytes({"shapes.txt": None})
    with _feed_server(body) as url:
        config = _config(tmp_path, url)
        manager = DatasetManager(config)
        with pytest.raises(GtfsLoadError):
            manager.load()
    assert manager.current is None
    assert PARSER_FILES  # the requirement is wired from the parser module
