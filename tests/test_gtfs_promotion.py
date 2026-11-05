"""Stage S6 checks for promotion, locking, manifests, and retention.

Fully offline: every test builds a synthetic staged archive with
``zipfile``/bytes and drives ``backend/gtfs/promotion.py`` directly against
per-test temporary work directories. No network, no live feed.

Lock contention across *processes* is approximated in-process by planting a
foreign ``promote.lock`` record by hand (the acquisition path is
``O_CREAT | O_EXCL`` against the filesystem, so a pre-existing file is
exactly what a foreign holder produces).

Note: configs are constructed directly (bypassing ``from_env``'s HTTPS-only
validation), the same convention the S2/S5 test modules use.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from backend.gtfs.errors import (
    ExtractionError,
    GtfsLoadError,
    ManifestUnreadableError,
    RetryablePromoteLockError,
)
from backend.gtfs.extractor import (
    StagedZip,
    _now_utc_ms,
    _write_guard,
    create_staging_dir,
)
from backend.gtfs.promotion import (
    LOCK_NAME,
    MANIFEST_NAME,
    RETAINED_VERSIONS,
    VERSION_PREFIX,
    acquire_promote_lock,
    promote_staged_dataset,
    prune_old_versions,
    read_manifest,
    write_manifest,
)
from backend.gtfs.types import GtfsManifest, ManifestAbsent, ManifestValid
from backend.trimet.config import GtfsConfig


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

_ZIP_A = b"synthetic gtfs archive A" * 64
_ZIP_B = b"synthetic gtfs archive B" * 64


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _config(tmp_path: Path, *, attempts: int = 3) -> GtfsConfig:
    return GtfsConfig(
        work_dir=tmp_path,
        promotion_max_attempts=attempts,
        promotion_backoff_seconds=(0.001,) * attempts,
    )


def _stage(work_dir: Path, content: bytes = _ZIP_A) -> StagedZip:
    """Create an owned staging directory holding one synthetic archive."""
    staging = create_staging_dir(work_dir)
    _write_guard(staging)  # the real stage_gtfs always leaves a live guard
    zip_path = staging / "gtfs.zip"
    zip_path.write_bytes(content)
    # A sibling "extracted" file so the staged dir is not just the zip.
    (staging / "feed_info.txt").write_text(
        "feed_version,feed_start_date,feed_end_date\nT1,20260101,20260201\n",
        encoding="utf-8",
        newline="",
    )
    return StagedZip(
        staging_dir=staging,
        zip_path=zip_path,
        zip_size_bytes=len(content),
        sha256=_sha(content),
        fetched_at_utc_ms=_now_utc_ms(),
        dataset_size_bytes=len(content) + 64,
        file_count=2,
    )


def _plant_version(
    work_dir: Path, name: str, content: bytes, *, fetched: int = 1_000
) -> Path:
    """Plant a complete, valid dataset version directory."""
    version = work_dir / name
    version.mkdir(parents=True)
    (version / "gtfs.zip").write_bytes(content)
    manifest = GtfsManifest(
        feed_version="T0",
        sha256=_sha(content),
        zip_size_bytes=len(content),
        dataset_size_bytes=len(content),
        fetched_at_utc_ms=fetched,
        zip_path=version / "gtfs.zip",
        dataset_path=version,
    )
    write_manifest(version, manifest)
    return version


def _promote(staged: StagedZip, config: GtfsConfig):
    return promote_staged_dataset(staged, "T1", config)


# ---------------------------------------------------------------------------
# promote.lock acquisition, content, contention, release
# ---------------------------------------------------------------------------


def test_lock_acquire_creates_immutable_record(tmp_path: Path) -> None:
    lock = acquire_promote_lock(tmp_path)
    try:
        record = json.loads(lock.path.read_text(encoding="utf-8"))
        assert set(record) == {
            "pid",
            "token",
            "acquired_utc_ms",
            "repo_paths_hint",
        }
        assert record["pid"] == os.getpid()
        assert isinstance(record["acquired_utc_ms"], int)
        # The ownership token is a random UUID hex, mirrored on the handle.
        assert record["token"] == lock.token
        assert len(lock.token) == 32
        int(lock.token, 16)  # parses as hex
        assert record["repo_paths_hint"] == [str(tmp_path)]
    finally:
        lock.release()


def test_lock_release_refuses_token_mismatch(tmp_path: Path) -> None:
    lock = acquire_promote_lock(tmp_path)
    # Same pid and timestamp, but the record's token was replaced: this is
    # not our acquisition any more and the file must survive.
    replaced = {
        "pid": lock.pid,
        "token": "0" * 32,
        "acquired_utc_ms": lock.acquired_utc_ms,
        "repo_paths_hint": [str(tmp_path)],
    }
    lock.path.write_text(json.dumps(replaced), encoding="utf-8")
    note = lock.release()
    assert note is not None
    assert lock.path.exists()
    assert json.loads(lock.path.read_text(encoding="utf-8")) == replaced


def test_lock_contention_reports_foreign_diagnostics(tmp_path: Path) -> None:
    foreign = tmp_path / LOCK_NAME
    foreign.write_text(
        json.dumps({"pid": 999999, "acquired_utc_ms": 12345}),
        encoding="utf-8",
    )
    with pytest.raises(RetryablePromoteLockError) as excinfo:
        acquire_promote_lock(tmp_path)
    assert excinfo.value.foreign_pid == 999999
    assert excinfo.value.acquired_utc_ms == 12345


def test_lock_contention_with_malformed_record_is_foreign(tmp_path: Path) -> None:
    (tmp_path / LOCK_NAME).write_text("{not json", encoding="utf-8")
    with pytest.raises(RetryablePromoteLockError) as excinfo:
        acquire_promote_lock(tmp_path)
    # Conservative: unknown ownership, still retryable, still held.
    assert excinfo.value.foreign_pid is None
    assert excinfo.value.acquired_utc_ms is None


def test_lock_contention_with_partial_record_is_foreign(tmp_path: Path) -> None:
    # A partially written record (valid JSON so far, missing the second field).
    (tmp_path / LOCK_NAME).write_text('{"pid": 42', encoding="utf-8")
    with pytest.raises(RetryablePromoteLockError) as excinfo:
        acquire_promote_lock(tmp_path)
    assert excinfo.value.foreign_pid is None


def test_lock_release_removes_own_record(tmp_path: Path) -> None:
    lock = acquire_promote_lock(tmp_path)
    assert lock.release() is None
    assert not lock.path.exists()


def test_lock_release_never_removes_foreign_record(tmp_path: Path) -> None:
    lock = acquire_promote_lock(tmp_path)
    # Simulate the record being replaced by another process after acquisition.
    foreign = {"pid": os.getpid() + 1, "acquired_utc_ms": lock.acquired_utc_ms + 1}
    lock.path.write_text(json.dumps(foreign), encoding="utf-8")
    note = lock.release()
    assert note is not None
    assert lock.path.exists()  # foreign record untouched
    assert json.loads(lock.path.read_text(encoding="utf-8")) == foreign


def test_lock_release_never_removes_unreadable_record(tmp_path: Path) -> None:
    lock = acquire_promote_lock(tmp_path)
    lock.path.write_text("garbage", encoding="utf-8")
    note = lock.release()
    assert note is not None
    assert lock.path.exists()


def test_retry_exhaustion_raises_last_contention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / LOCK_NAME).write_text(
        json.dumps({"pid": 1, "acquired_utc_ms": 2}), encoding="utf-8"
    )
    sleeps: list[float] = []
    monkeypatch.setattr("backend.gtfs.promotion.time.sleep", sleeps.append)
    config = _config(tmp_path, attempts=3)
    with pytest.raises(RetryablePromoteLockError):
        from backend.gtfs.promotion import _acquire_with_retries

        _acquire_with_retries(config)
    # One configured wait precedes each attempt (len(backoffs) == attempts).
    assert sleeps == [0.001, 0.001, 0.001]


def test_promote_releases_lock_after_success(tmp_path: Path) -> None:
    config = _config(tmp_path)
    staged = _stage(tmp_path)
    _promote(staged, config)
    assert not (tmp_path / LOCK_NAME).exists()


def test_promote_failure_leaves_staging_for_diagnosis(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    staged = _stage(tmp_path)
    # Occupied rung with an unreadable manifest -> fail-safe error.
    bad = tmp_path / f"{VERSION_PREFIX}{staged.sha256[:16]}"
    bad.mkdir()
    (bad / MANIFEST_NAME).write_text("{broken", encoding="utf-8")
    with pytest.raises(ManifestUnreadableError):
        _promote(staged, config)
    # The staging directory (ours, guarded) stays for diagnosis.
    assert staged.staging_dir.is_dir()
    # The foreign/unreadable occupant is untouched.
    assert bad.is_dir()
    # And the lock was still released in the finally block.
    assert not (tmp_path / LOCK_NAME).exists()


# ---------------------------------------------------------------------------
# Manifest write/read/verify
# ---------------------------------------------------------------------------


def test_manifest_roundtrip(tmp_path: Path) -> None:
    version = _plant_version(tmp_path, "dataset-x", _ZIP_A)
    manifest = read_manifest(version)
    assert isinstance(manifest, ManifestValid)
    assert manifest.sha256 == _sha(_ZIP_A)


def test_manifest_absent_when_directory_missing(tmp_path: Path) -> None:
    assert isinstance(read_manifest(tmp_path / "nope"), ManifestAbsent)


def test_manifest_absent_without_file(tmp_path: Path) -> None:
    (tmp_path / "d").mkdir()
    assert isinstance(read_manifest(tmp_path / "d"), ManifestAbsent)


@pytest.mark.parametrize(
    "content",
    [
        "{not json",
        "[]",  # not an object
        json.dumps({"feed_version": "T"}),  # missing everything else
        json.dumps(
            {
                "feed_version": "T",
                "sha256": "zz",  # not 64-hex
                "zip_size_bytes": 1,
                "dataset_size_bytes": 1,
                "fetched_at_utc_ms": 1,
                "zip_path": "p",
                "dataset_path": "d",
            }
        ),
        json.dumps(
            {
                "feed_version": "T",
                "sha256": "a" * 64,
                "zip_size_bytes": -1,  # negative
                "dataset_size_bytes": 1,
                "fetched_at_utc_ms": 1,
                "zip_path": "p",
                "dataset_path": "d",
            }
        ),
        json.dumps(
            {
                "feed_version": "T",
                "sha256": "a" * 64,
                "zip_size_bytes": True,  # bool is not an int here
                "dataset_size_bytes": 1,
                "fetched_at_utc_ms": 1,
                "zip_path": "p",
                "dataset_path": "d",
            }
        ),
    ],
)
def test_malformed_manifest_is_never_valid(
    tmp_path: Path, content: str
) -> None:
    directory = tmp_path / "d"
    directory.mkdir()
    (directory / MANIFEST_NAME).write_text(content, encoding="utf-8")
    with pytest.raises(ManifestUnreadableError):
        read_manifest(directory)


def test_verify_refuses_tampered_dataset(tmp_path: Path) -> None:
    version = _plant_version(tmp_path, "dataset-x", _ZIP_A)
    (version / "gtfs.zip").write_bytes(_ZIP_B)  # bytes no longer match manifest
    from backend.gtfs.promotion import _verify_dataset_version

    with pytest.raises(ExtractionError):
        _verify_dataset_version(version, _sha(_ZIP_A))


# ---------------------------------------------------------------------------
# Promotion lifecycle
# ---------------------------------------------------------------------------


def test_promote_happy_path(tmp_path: Path) -> None:
    config = _config(tmp_path)
    staged = _stage(tmp_path)
    outcome = _promote(staged, config)
    assert outcome.reused is False
    assert outcome.dataset_dir == tmp_path / f"{VERSION_PREFIX}{staged.sha256[:16]}"
    assert outcome.dataset_dir.is_dir()
    assert not staged.staging_dir.exists()  # renamed, not copied
    manifest = read_manifest(outcome.dataset_dir)
    assert isinstance(manifest, ManifestValid)
    assert manifest.sha256 == staged.sha256
    record = json.loads((outcome.dataset_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert record["feed_version"] == "T1"
    assert not (outcome.dataset_dir / "guard.json").exists()


def test_promote_is_idempotent_on_identical_feed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first = _promote(_stage(tmp_path), config)
    second = _promote(_stage(tmp_path), config)
    assert second.reused is True
    assert second.dataset_dir == first.dataset_dir
    # Only one version directory exists.
    versions = [p for p in tmp_path.iterdir() if p.name.startswith(VERSION_PREFIX)]
    assert len(versions) == 1


def test_pre_lock_reuse_short_circuits_without_lock(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _promote(_stage(tmp_path), config)
    # Second promotion returns before acquiring the lock: no lock residue.
    second = _promote(_stage(tmp_path), config)
    assert second.reused is True
    assert not (tmp_path / LOCK_NAME).exists()


def test_ladder_collision_steps_to_next_rung(tmp_path: Path) -> None:
    config = _config(tmp_path)
    # Rung 1 occupied by a *different* valid version.
    _plant_version(tmp_path, f"{VERSION_PREFIX}{_sha(_ZIP_A)[:16]}", _ZIP_B)
    staged = _stage(tmp_path, _ZIP_A)
    outcome = _promote(staged, config)
    assert outcome.reused is False
    assert outcome.version_name == f"{VERSION_PREFIX}{_sha(_ZIP_A)[:16]}-2"
    assert outcome.dataset_dir.is_dir()
    # The different-hash occupant was never touched.
    assert (tmp_path / f"{VERSION_PREFIX}{_sha(_ZIP_A)[:16]}" / "gtfs.zip").read_bytes() == _ZIP_B


def test_reuse_disabled_promotes_a_second_rung(tmp_path: Path) -> None:
    config = GtfsConfig(
        work_dir=tmp_path,
        promotion_max_attempts=3,
        promotion_backoff_seconds=(0.001,) * 3,
        reuse_promoted=False,
    )
    first = _promote(_stage(tmp_path), config)
    second = _promote(_stage(tmp_path), config)
    assert first.reused is False
    assert second.reused is False
    assert second.version_name == f"{VERSION_PREFIX}{_sha(_ZIP_A)[:16]}-2"


def test_unreadable_occupant_is_fail_safe(tmp_path: Path) -> None:
    config = _config(tmp_path)
    rung = tmp_path / f"{VERSION_PREFIX}{_sha(_ZIP_A)[:16]}"
    rung.mkdir()
    (rung / "somefile.txt").write_text("not a dataset", encoding="utf-8")
    staged = _stage(tmp_path)
    with pytest.raises(ManifestUnreadableError):
        _promote(staged, config)
    # Nothing was adopted, overwritten, or deleted.
    assert rung.is_dir()
    assert not list(rung.glob("gtfs.zip"))
    assert staged.staging_dir.is_dir()


def test_rename_failure_leaves_staging_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    staged = _stage(tmp_path)

    def broken_rename(src, dst):  # type: ignore[no-untyped-def]
        raise OSError(5, "simulated I/O failure")

    monkeypatch.setattr("backend.gtfs.promotion.os.rename", broken_rename)
    with pytest.raises(GtfsLoadError):
        _promote(staged, config)
    assert staged.staging_dir.is_dir()
    # The guard survived the failed rename: the staging directory is still
    # verifiably ours, so inventory reports it ACTIVE and cleanup works.
    assert (staged.staging_dir / "guard.json").is_file()
    from backend.gtfs.extractor import discard_staging_if_own

    assert discard_staging_if_own(staged.staging_dir) is None
    assert not staged.staging_dir.exists()
    assert not (tmp_path / LOCK_NAME).exists()


def test_promote_records_guard_removal_and_post_rename_manifest(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    staged = _stage(tmp_path)
    assert (staged.staging_dir / "guard.json").is_file()
    outcome = _promote(staged, config)
    manifest = read_manifest(outcome.dataset_dir)
    assert isinstance(manifest, ManifestValid)
    # Manifest paths are the final (post-rename) absolute locations.
    record = json.loads((outcome.dataset_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert Path(record["dataset_path"]) == outcome.dataset_dir
    assert Path(record["zip_path"]) == outcome.dataset_dir / "gtfs.zip"


def test_foreign_guard_refuses_promotion(tmp_path: Path) -> None:
    config = _config(tmp_path)
    staging = create_staging_dir(tmp_path)
    (staging / "gtfs.zip").write_bytes(_ZIP_A)
    # Overwrite the guard with a foreign pid.
    guard = staging / "guard.json"
    guard.write_text(
        json.dumps(
            {
                "pid": os.getpid() + 1,
                "started_utc_ms": _now_utc_ms(),
                "heartbeat_utc_ms": _now_utc_ms(),
            }
        ),
        encoding="utf-8",
    )
    staged = StagedZip(
        staging_dir=staging,
        zip_path=staging / "gtfs.zip",
        zip_size_bytes=len(_ZIP_A),
        sha256=_sha(_ZIP_A),
        fetched_at_utc_ms=_now_utc_ms(),
        dataset_size_bytes=len(_ZIP_A),
        file_count=1,
    )
    with pytest.raises(ExtractionError, match="foreign"):
        _promote(staged, config)
    assert staging.is_dir()


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------


def test_prune_keeps_two_newest_valid_versions(tmp_path: Path) -> None:
    _plant_version(tmp_path, "dataset-a", _ZIP_A, fetched=1_000)
    _plant_version(tmp_path, "dataset-b", _ZIP_B, fetched=2_000)
    third = b"third archive" * 32
    _plant_version(tmp_path, "dataset-c", third, fetched=3_000)
    notes = prune_old_versions(tmp_path, keep=RETAINED_VERSIONS)
    assert any("dataset-a" in note for note in notes)
    assert not (tmp_path / "dataset-a").exists()
    assert (tmp_path / "dataset-b").exists()
    assert (tmp_path / "dataset-c").exists()


def test_prune_never_deletes_unreadable_or_foreign(tmp_path: Path) -> None:
    _plant_version(tmp_path, "dataset-a", _ZIP_A, fetched=1_000)
    _plant_version(tmp_path, "dataset-b", _ZIP_B, fetched=2_000)
    unreadable = tmp_path / "dataset-c"
    unreadable.mkdir()
    (unreadable / MANIFEST_NAME).write_text("{broken", encoding="utf-8")
    prune_old_versions(tmp_path, keep=1)
    assert unreadable.is_dir()  # uncertain: never deleted
    assert not (tmp_path / "dataset-a").exists()  # keep=1 prunes the older one
    assert (tmp_path / "dataset-b").exists()  # the newest survives


def test_prune_never_deletes_hash_mismatched_version(tmp_path: Path) -> None:
    _plant_version(tmp_path, "dataset-a", _ZIP_A, fetched=1_000)
    _plant_version(tmp_path, "dataset-b", _ZIP_B, fetched=2_000)
    # Corrupt the older version's zip: retention cannot prove it is what its
    # manifest says, so it must be left in place.
    (tmp_path / "dataset-a" / "gtfs.zip").write_bytes(b"tampered")
    notes = prune_old_versions(tmp_path, keep=1)
    assert (tmp_path / "dataset-a").exists()
    assert any("dataset-a" in note for note in notes)


def test_prune_protects_the_just_promoted_version(tmp_path: Path) -> None:
    _plant_version(tmp_path, "dataset-a", _ZIP_A, fetched=1_000)
    _plant_version(tmp_path, "dataset-b", _ZIP_B, fetched=2_000)
    third = b"third archive" * 32
    _plant_version(tmp_path, "dataset-c", third, fetched=3_000)
    prune_old_versions(tmp_path, keep=1, protect="dataset-c")
    assert (tmp_path / "dataset-c").exists()


def test_retention_runs_after_promotion(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _plant_version(tmp_path, "dataset-old1", _ZIP_A, fetched=1_000)
    _plant_version(tmp_path, "dataset-old2", _ZIP_B, fetched=2_000)
    # Promoting a third distinct version should prune down to two valid ones.
    third = b"third archive" * 32
    outcome = _promote(_stage(tmp_path, third), config)
    names = {p.name for p in tmp_path.iterdir() if p.name.startswith(VERSION_PREFIX)}
    assert outcome.version_name in names
    assert len(names) == 2
    assert "dataset-old1" not in names
