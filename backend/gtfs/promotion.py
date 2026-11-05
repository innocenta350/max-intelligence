"""Dataset promotion: manifests, promote.lock, and the version ladder (S6).

Stage S6 owns what S2 deliberately deferred (extractor.py docstring):
``manifest.json`` persistence, the immutable ``promote.lock`` record, the
checksum-suffix ladder, and the atomic staging->version rename.

Version naming (documented reconstruction — the Rev. 9 text is not in the
repository; this is the smallest defensible reading of the recorded
"checksum-suffix ladder"):

- A promoted version directory is ``dataset-<sha256[:16]>`` — content-
  addressed by the *full archive hash*, so the name alone identifies the
  dataset and identical feeds promote idempotently.
- The ladder exists for occupied names: rung 1 is the plain name, then
  ``...-2``, ``...-3``, ... At an occupied rung, a *valid* manifest whose
  recorded sha256 matches the staged archive — and whose ``gtfs.zip`` bytes
  re-verify against it — means the dataset already exists: reuse it
  (idempotence, the double-checked short-circuit behind
  ``GtfsConfig.reuse_promoted``). A valid manifest with a *different* hash
  is a collision: step to the next rung. Anything else (missing manifest in
  an existing directory, malformed manifest) is fail-safe: refuse loudly,
  never adopt, overwrite, or delete.

Locking (plan Revs. 5–9): ``<work_dir>/promote.lock`` is acquired with
``O_CREAT | O_EXCL`` and written once as ``{"pid", "token",
"acquired_utc_ms", "repo_paths_hint"}`` — never heartbeated, never
rewritten. ``token`` is a random UUID hex generated at acquisition and is
the ownership token verified at release; ``repo_paths_hint`` names the
directory tree the lock governs (diagnostic). Contention is diagnostic-only:
the foreign record is read (best-effort) to enrich
:class:`RetryablePromoteLockError`, and a malformed or partially written
record is treated conservatively as foreign. Release verifies the recorded
``(pid, token, acquired_utc_ms)`` tuple against what this process wrote and
never removes another process's lock. The residual read-verify-delete race
(another process replacing the lock between verification and unlink) is
accepted and documented for M1.

Durability: the manifest is written into staging, fsynced, and re-read for
verification *before* the atomic ``os.rename``; the staging directory gets a
best-effort fsync (best-effort because Windows cannot reliably fsync
directory handles). Success is reported only after the rename and a
post-rename manifest re-read both succeed.

Retention (plan: keep the last two valid versions, no cross-process GC in
M1): :func:`prune_old_versions` runs under the promotion lock, keeps the two
newest *valid* versions, and deletes an older version only after its
``gtfs.zip`` re-verifies against its own manifest. Foreign, unreadable, and
uncertain directories are never deleted — only noted.

No secrets in messages: file and directory *names* only, never absolute
paths, environment data, or credentials.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from backend.gtfs.errors import (
    ExtractionError,
    GtfsLoadError,
    ManifestUnreadableError,
    RetryablePromoteLockError,
)
from backend.gtfs.extractor import StagedZip, _now_utc_ms
from backend.gtfs.types import (
    GtfsManifest,
    ManifestAbsent,
    ManifestRead,
    ManifestValid,
)
from backend.trimet.config import GtfsConfig

__all__ = [
    "LOCK_NAME",
    "MANIFEST_NAME",
    "RETAINED_VERSIONS",
    "VERSION_PREFIX",
    "PromoteLock",
    "PromotionOutcome",
    "acquire_promote_lock",
    "promote_staged_dataset",
    "prune_old_versions",
    "read_manifest",
    "write_manifest",
]

LOCK_NAME = "promote.lock"
MANIFEST_NAME = "manifest.json"
VERSION_PREFIX = "dataset-"
# Ladder bound: far beyond any plausible collision count; a permanent guard
# against a pathological work_dir filling up with same-hash rungs.
_MAX_LADDER_RUNGS = 100
# Plan: retain the last two valid dataset versions (M1, no cross-process GC).
RETAINED_VERSIONS = 2

_SHA256_LENGTH = 64
_HASH_CHUNK = 256 * 1024


# ---------------------------------------------------------------------------
# Manifest persistence and verification
# ---------------------------------------------------------------------------


def _manifest_payload(manifest: GtfsManifest) -> dict[str, object]:
    """JSON-safe mapping for a :class:`GtfsManifest`; paths stored absolute."""
    return {
        "feed_version": manifest.feed_version,
        "sha256": manifest.sha256,
        "zip_size_bytes": manifest.zip_size_bytes,
        "dataset_size_bytes": manifest.dataset_size_bytes,
        "fetched_at_utc_ms": manifest.fetched_at_utc_ms,
        "zip_path": str(manifest.zip_path),
        "dataset_path": str(manifest.dataset_path),
    }


def _fsync_dir_best_effort(path: Path) -> None:
    """Best-effort directory fsync; Windows cannot fsync directory handles."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write_manifest(dataset_dir: Path | str, manifest: GtfsManifest) -> None:
    """Write ``manifest.json`` atomically (temp file + fsync + replace)."""
    dataset_dir = Path(dataset_dir)
    temp_path = dataset_dir / (MANIFEST_NAME + ".tmp")
    payload = json.dumps(_manifest_payload(manifest), sort_keys=True, indent=2)
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as out:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp_path, dataset_dir / MANIFEST_NAME)
    except OSError as exc:
        raise ExtractionError(
            f"could not persist {MANIFEST_NAME} in {dataset_dir.name}: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc
    _fsync_dir_best_effort(dataset_dir)


def read_manifest(dataset_dir: Path | str) -> ManifestRead:
    """Read and strictly validate ``manifest.json`` (plan Rev. 7 §2).

    Returns :class:`ManifestAbsent` when the directory or the manifest file
    does not exist, :class:`ManifestValid` when the record parses and every
    field has the documented type and shape, and raises
    :class:`ManifestUnreadableError` otherwise — a malformed, truncated, or
    type-broken manifest can never be mistaken for a valid dataset.
    """
    dataset_dir = Path(dataset_dir)
    manifest_path = dataset_dir / MANIFEST_NAME
    if not dataset_dir.is_dir() or not manifest_path.is_file():
        return ManifestAbsent()
    try:
        record = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ManifestUnreadableError(
            f"{MANIFEST_NAME} in {dataset_dir.name} is unreadable "
            f"({exc.__class__.__name__})",
            directory=dataset_dir.name,
            file=MANIFEST_NAME,
        ) from exc

    def fail(reason: str) -> ManifestUnreadableError:
        return ManifestUnreadableError(
            f"{MANIFEST_NAME} in {dataset_dir.name} is invalid: {reason}",
            directory=dataset_dir.name,
            file=MANIFEST_NAME,
        )

    if not isinstance(record, dict):
        raise fail("record is not a JSON object")
    required = (
        "feed_version",
        "sha256",
        "zip_size_bytes",
        "dataset_size_bytes",
        "fetched_at_utc_ms",
        "zip_path",
        "dataset_path",
    )
    for key in required:
        if key not in record:
            raise fail(f"missing field {key!r}")
    for key in ("feed_version", "sha256", "zip_path", "dataset_path"):
        value = record[key]
        if not isinstance(value, str) or not value:
            raise fail(f"{key} is not a non-empty string")
    sha256 = record["sha256"]
    if len(sha256) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in sha256
    ):
        raise fail("sha256 is not a lowercase 64-hex digest")
    for key in ("zip_size_bytes", "dataset_size_bytes", "fetched_at_utc_ms"):
        value = record[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise fail(f"{key} is not a non-negative integer")
    if record["zip_size_bytes"] <= 0:
        raise fail("zip_size_bytes must be positive")
    return ManifestValid(sha256=sha256)


def _zip_sha256(zip_path: Path) -> str:
    """Stream-hash a zip file; absence/unreadability raises ExtractionError."""
    digest_state = hashlib.sha256()
    try:
        with zip_path.open("rb") as handle:
            while True:
                chunk = handle.read(_HASH_CHUNK)
                if not chunk:
                    break
                digest_state.update(chunk)
    except OSError as exc:
        raise ExtractionError(
            f"could not hash {zip_path.name} in {zip_path.parent.name}: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc
    return digest_state.hexdigest()


def _verify_dataset_version(
    dataset_dir: Path, expected_sha256: str
) -> ManifestValid:
    """Return the valid manifest, proving the version's zip bytes match it.

    The double-checked reuse rule: a name match alone proves nothing — the
    manifest must parse as valid *and* the archived ``gtfs.zip`` must hash
    to the manifest's recorded digest. Any mismatch is a loud fail-safe
    error (a tampered or truncated dataset is never reused).
    """
    manifest = read_manifest(dataset_dir)
    if isinstance(manifest, ManifestAbsent):
        raise ManifestUnreadableError(
            f"dataset version {dataset_dir.name} has no {MANIFEST_NAME}; "
            "treating it as incomplete rather than valid",
            directory=dataset_dir.name,
            file=MANIFEST_NAME,
        )
    if not isinstance(manifest, ManifestValid):  # pragma: no cover - narrowing
        raise ManifestUnreadableError(
            f"dataset version {dataset_dir.name} has an unreadable manifest",
            directory=dataset_dir.name,
            file=MANIFEST_NAME,
        )
    if manifest.sha256 != expected_sha256:
        raise ManifestUnreadableError(
            f"dataset version {dataset_dir.name} records a different archive "
            "hash than the staged feed; refusing to reuse it",
            directory=dataset_dir.name,
            file=MANIFEST_NAME,
        )
    actual = _zip_sha256(dataset_dir / "gtfs.zip")
    if actual != expected_sha256:
        raise ExtractionError(
            f"dataset version {dataset_dir.name} is corrupt: its gtfs.zip "
            "does not hash to the manifest's recorded digest"
        )
    return manifest


# ---------------------------------------------------------------------------
# promote.lock — immutable record, O_CREAT | O_EXCL, diagnostic contenders
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PromoteLock:
    """An acquired promotion lock; ``release()`` is token-verified.

    ``token`` is a random UUID hex generated at acquisition and written into
    the immutable record; release removes the file only when the on-disk
    record still carries *this* token (and pid). The token makes ownership
    verification unambiguous even if a recycled pid reacquires within the
    same millisecond.
    """

    path: Path
    pid: int
    token: str
    acquired_utc_ms: int
    repo_paths_hint: tuple[str, ...]

    def release(self) -> str | None:
        """Remove the lock only if it still records *this* acquisition.

        Returns ``None`` on success, otherwise a diagnostic note and the file
        is left untouched (foreign, malformed, or already-replaced records
        are never deleted). The residual read-verify-delete race — another
        process replacing the lock between verification and unlink — is
        accepted and documented for M1.
        """
        try:
            record = json.loads(self.path.read_text(encoding="utf-8"))
            recorded_pid = record["pid"]
            recorded_token = record["token"]
            recorded_acquired = record["acquired_utc_ms"]
        except (OSError, ValueError, KeyError, TypeError):
            return (
                f"{LOCK_NAME} left in place: its record is missing or "
                "unreadable, so ownership cannot be verified"
            )
        if (
            recorded_pid != self.pid
            or recorded_token != self.token
            or recorded_acquired != self.acquired_utc_ms
        ):
            return (
                f"{LOCK_NAME} left in place: its record does not match this "
                f"acquisition (pid {self.pid}, token {self.token[:8]}…, "
                f"acquired {self.acquired_utc_ms})"
            )
        try:
            self.path.unlink()
        except OSError as exc:
            return (
                f"{LOCK_NAME} could not be removed: "
                f"{exc.__class__.__name__}: {exc}"
            )
        return None


def _foreign_lock_diagnostics(path: Path) -> tuple[int | None, int | None]:
    """Best-effort read of a foreign lock record for error diagnostics."""
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
        pid = record["pid"]
        acquired = record["acquired_utc_ms"]
    except (OSError, ValueError, KeyError, TypeError):
        return None, None
    pid = pid if isinstance(pid, int) and not isinstance(pid, bool) else None
    acquired = (
        acquired
        if isinstance(acquired, int) and not isinstance(acquired, bool)
        else None
    )
    return pid, acquired


def acquire_promote_lock(work_dir: Path | str) -> PromoteLock:
    """Acquire ``promote.lock`` exclusively, or raise a retryable error.

    ``O_CREAT | O_EXCL`` makes creation atomic across processes. The record
    is written exactly once and fsynced — it is immutable by design. Its
    fields (approved Rev. 9 shape):

    - ``pid``: the acquiring process id.
    - ``token``: a random UUID hex generated at acquisition; the ownership
      token verified at release time.
    - ``acquired_utc_ms``: UTC epoch milliseconds at acquisition.
    - ``repo_paths_hint``: diagnostic hint naming the directory tree this
      lock governs (the work_dir), so an orphaned lock can be traced to its
      pipeline without reading any other state.

    An existing file (whatever its contents) is another process's business:
    it is read diagnostically only and reported through
    :class:`RetryablePromoteLockError`; a malformed or partial record is
    conservatively treated as foreign with unknown ownership.
    """
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock_path = work_dir / LOCK_NAME
    pid = os.getpid()
    token = uuid.uuid4().hex
    acquired_ms = _now_utc_ms()
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        foreign_pid, foreign_acquired = _foreign_lock_diagnostics(lock_path)
        raise RetryablePromoteLockError(
            f"{LOCK_NAME} is held by another process"
            + (f" (pid {foreign_pid})" if foreign_pid is not None else "")
            + "; promotion is retryable",
            foreign_pid=foreign_pid,
            acquired_utc_ms=foreign_acquired,
            file=LOCK_NAME,
        ) from None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "pid": pid,
                        "token": token,
                        "acquired_utc_ms": acquired_ms,
                        "repo_paths_hint": [str(work_dir)],
                    },
                    sort_keys=True,
                )
            )
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        # Best-effort removal of our own half-written record: it is ours by
        # construction (O_EXCL just created it), so cleanup is safe here.
        try:
            lock_path.unlink()
        except OSError:
            pass
        raise ExtractionError(
            f"could not write {LOCK_NAME}: {exc.__class__.__name__}: {exc}"
        ) from exc
    return PromoteLock(
        path=lock_path,
        pid=pid,
        token=token,
        acquired_utc_ms=acquired_ms,
        repo_paths_hint=(str(work_dir),),
    )


def _acquire_with_retries(config: GtfsConfig) -> PromoteLock:
    """Bounded in-process retry around :func:`acquire_promote_lock`.

    The config validation pins ``len(backoffs) == max_attempts`` with every
    value > 0, so each attempt is preceded by its own configured wait:
    attempt ``i`` sleeps ``backoff[i]`` first (monotonic-clock scheduling).
    Exhausting the attempts re-raises the last contention error — lock
    contention is the *only* thing retried; validation and corruption
    failures propagate immediately.
    """
    last_error: RetryablePromoteLockError | None = None
    for attempt in range(config.promotion_max_attempts):
        delay = config.promotion_backoff_seconds[attempt]
        if delay > 0:
            time.sleep(delay)
        try:
            return acquire_promote_lock(config.work_dir)
        except RetryablePromoteLockError as exc:
            last_error = exc
    assert last_error is not None  # the loop only ends after a contention
    raise last_error


# ---------------------------------------------------------------------------
# Promotion
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PromotionOutcome:
    """Result of one promotion attempt (idempotent reuse included)."""

    dataset_dir: Path
    version_name: str
    reused: bool
    notes: tuple[str, ...]


def _ladder_names(sha256: str) -> list[str]:
    """Rung 1 is the content-addressed name; later rungs append ``-N``."""
    short = sha256[:16]
    return [f"{VERSION_PREFIX}{short}"] + [
        f"{VERSION_PREFIX}{short}-{run}" for run in range(2, _MAX_LADDER_RUNGS + 1)
    ]


def _try_reuse(
    dataset_dir: Path, sha256: str, notes: list[str]
) -> PromotionOutcome | None:
    """Reuse ``dataset_dir`` when it verifiably holds this exact dataset.

    ``None`` means "not a reuse situation": the directory does not exist, or
    it holds a *valid* manifest recording a different archive hash (a ladder
    collision, resolved by the caller). Anything else — absent or unreadable
    manifest in an existing directory, or a zip that does not re-verify —
    raises (fail-safe, never adopted).
    """
    if not dataset_dir.is_dir():
        return None
    manifest = read_manifest(dataset_dir)
    if isinstance(manifest, ManifestAbsent):
        raise ManifestUnreadableError(
            f"dataset version {dataset_dir.name} has no {MANIFEST_NAME}; "
            "treating it as incomplete rather than valid",
            directory=dataset_dir.name,
            file=MANIFEST_NAME,
        )
    if manifest.sha256 != sha256:
        # A different, valid version: a collision for the ladder, not an error.
        return None
    _verify_dataset_version(dataset_dir, sha256)
    notes.append(f"reused existing dataset version {dataset_dir.name}")
    return PromotionOutcome(
        dataset_dir=dataset_dir,
        version_name=dataset_dir.name,
        reused=True,
        notes=tuple(notes),
    )


def _promote_into(
    staging_dir: Path, dataset_dir: Path, manifest: GtfsManifest
) -> tuple[str, ...]:
    """Write+verify the manifest in staging, then atomically rename.

    The staging guard marker ends its lifecycle *after* the rename: it is
    kept on the staging directory until the rename has actually succeeded,
    so a failed or interrupted promotion leaves the staging directory fully
    owned (inventory reads it as ACTIVE for our pid) and cleanable via
    ``discard_staging_if_own``. Removing the guard before the rename would
    strand an ownership-unverifiable directory on every rename failure.
    The guard is then deleted from the promoted directory; if that unlink
    fails the promotion still stands (the manifest defines validity, and
    inventory only examines ``staging-*`` directories) and the problem is
    returned as a diagnostic note.

    A foreign or unreadable guard refuses promotion before anything is
    touched.
    """
    notes: list[str] = []
    guard = staging_dir / "guard.json"
    if guard.is_file():
        try:
            record = json.loads(guard.read_text(encoding="utf-8"))
            recorded_pid = record["pid"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ExtractionError(
                f"staging {staging_dir.name} has an unreadable guard.json; "
                "refusing to promote a staging directory whose ownership "
                "cannot be verified"
            ) from exc
        if recorded_pid != os.getpid():
            raise ExtractionError(
                f"staging {staging_dir.name} belongs to pid {recorded_pid}; "
                "refusing to promote a foreign staging directory"
            )

    write_manifest(staging_dir, manifest)
    written = read_manifest(staging_dir)
    if not isinstance(written, ManifestValid) or written.sha256 != manifest.sha256:
        raise ManifestUnreadableError(
            f"manifest verification failed in staging {staging_dir.name}; "
            "refusing to promote an unverifiable dataset",
            directory=staging_dir.name,
            file=MANIFEST_NAME,
        )
    _fsync_dir_best_effort(staging_dir)
    try:
        os.rename(staging_dir, dataset_dir)
    except FileExistsError:
        raise  # the ladder caller steps to the next rung
    except OSError as exc:
        raise ExtractionError(
            f"could not promote staging {staging_dir.name} to "
            f"{dataset_dir.name}: {exc.__class__.__name__}: {exc}"
        ) from exc
    # Post-rename verification: the promoted version must read back valid.
    final = read_manifest(dataset_dir)
    if not isinstance(final, ManifestValid) or final.sha256 != manifest.sha256:
        raise ManifestUnreadableError(
            f"promoted dataset {dataset_dir.name} failed post-rename "
            "manifest verification",
            directory=dataset_dir.name,
            file=MANIFEST_NAME,
        )
    # The rename succeeded: end the guard's staging lifecycle in the
    # promoted directory. Best-effort — a leftover guard.json here is a
    # cosmetic wart, never a validity problem (see docstring).
    try:
        (dataset_dir / "guard.json").unlink()
    except OSError as exc:
        notes.append(
            f"guard.json could not be removed from promoted dataset "
            f"{dataset_dir.name}: {exc.__class__.__name__}: {exc}"
        )
    return tuple(notes)


def promote_staged_dataset(
    staged: StagedZip,
    feed_version: str,
    config: GtfsConfig,
) -> PromotionOutcome:
    """Promote a staged, parsed, validated dataset into an immutable version.

    Sequence (plan Revs. 5–9): pre-lock idempotence check; bounded lock
    acquisition; state recheck under the lock; reuse of a verified existing
    version; checksum-suffix ladder for occupied names; manifest write +
    fsync + verify in staging; atomic rename; post-rename verification;
    retention pruning; ownership-verified lock release. A failure leaves the
    staging directory in place (it is owned and guarded) for diagnosis and
    never disturbs existing versions.
    """
    work_dir = Path(config.work_dir)
    notes: list[str] = []
    rung_one = work_dir / _ladder_names(staged.sha256)[0]

    # 1. Pre-lock idempotence: a verified existing version short-circuits
    #    the whole promotion (double-checked short-circuit, reuse_promoted).
    if config.reuse_promoted:
        outcome = _try_reuse(rung_one, staged.sha256, notes)
        if outcome is not None:
            return outcome

    # 2–3. Acquire the lock (bounded retries), then recheck under it.
    lock = _acquire_with_retries(config)
    try:
        if config.reuse_promoted:
            outcome = _try_reuse(rung_one, staged.sha256, notes)
            if outcome is not None:
                return outcome

        # 4–6. Walk the ladder.
        for rung_name in _ladder_names(staged.sha256):
            destination = work_dir / rung_name
            if not destination.exists():
                final_manifest = GtfsManifest(
                    feed_version=feed_version,
                    sha256=staged.sha256,
                    zip_size_bytes=staged.zip_size_bytes,
                    dataset_size_bytes=staged.dataset_size_bytes,
                    fetched_at_utc_ms=staged.fetched_at_utc_ms,
                    zip_path=destination / "gtfs.zip",
                    dataset_path=destination,
                )
                notes.extend(
                    _promote_into(staged.staging_dir, destination, final_manifest)
                )
                notes.extend(
                    prune_old_versions(work_dir, keep=RETAINED_VERSIONS)
                )
                return PromotionOutcome(
                    dataset_dir=destination,
                    version_name=rung_name,
                    reused=False,
                    notes=tuple(notes),
                )
            existing = read_manifest(destination)
            if isinstance(existing, ManifestValid):
                if existing.sha256 == staged.sha256 and config.reuse_promoted:
                    # Full-hash byte verification before declaring reuse.
                    _verify_dataset_version(destination, staged.sha256)
                    notes.append(
                        f"reused existing dataset version {destination.name} "
                        "(verified under the promotion lock)"
                    )
                    notes.extend(
                        prune_old_versions(work_dir, keep=RETAINED_VERSIONS)
                    )
                    return PromotionOutcome(
                        dataset_dir=destination,
                        version_name=destination.name,
                        reused=True,
                        notes=tuple(notes),
                    )
                notes.append(
                    f"dataset name {rung_name!r} is occupied by a "
                    + (
                        "different version; stepping to the next ladder rung"
                        if existing.sha256 != staged.sha256
                        else "verified copy while reuse is disabled; "
                        "stepping to the next ladder rung"
                    )
                )
                continue
            # Absent manifest in an existing directory, or unreadable
            # manifest: fail-safe (Rev. 8 §1) — never adopt or overwrite.
            raise ManifestUnreadableError(
                f"dataset name {rung_name!r} is occupied by a directory "
                "without a valid manifest; refusing to promote over it",
                directory=rung_name,
                file=MANIFEST_NAME,
            )
        raise ExtractionError(
            "promotion ladder exhausted without a free destination name"
        )
    finally:
        note = lock.release()
        if note is not None:
            notes.append(note)


# ---------------------------------------------------------------------------
# Retention (plan: keep the last two valid versions; no cross-process GC)
# ---------------------------------------------------------------------------


def prune_old_versions(
    work_dir: Path | str,
    *,
    keep: int = RETAINED_VERSIONS,
    protect: str | None = None,
) -> tuple[str, ...]:
    """Delete verifiably-old valid versions beyond the newest ``keep``.

    Runs under the promotion lock. A version directory is deleted only when
    its manifest reads back valid *and* its ``gtfs.zip`` re-hashes to the
    manifest digest — anything foreign, unreadable, or uncertain is left in
    place and reported. ``protect`` names a version that must survive this
    pass (the just-promoted one). Returns diagnostic notes.
    """
    work_dir = Path(work_dir)
    notes: list[str] = []
    versions: list[tuple[int, str, Path, ManifestValid]] = []
    for entry in work_dir.iterdir():
        if not entry.is_dir() or not entry.name.startswith(VERSION_PREFIX):
            continue
        try:
            manifest = read_manifest(entry)
        except ManifestUnreadableError:
            manifest = None
        if isinstance(manifest, ManifestValid):
            try:
                fetched = int(
                    json.loads(
                        (entry / MANIFEST_NAME).read_text(encoding="utf-8")
                    )["fetched_at_utc_ms"]
                )
            except (OSError, ValueError, KeyError, TypeError):
                notes.append(
                    f"dataset version {entry.name} left in place: its "
                    "manifest could not be re-read for retention ordering"
                )
                continue
            versions.append((fetched, entry.name, entry, manifest))
        else:
            notes.append(
                f"dataset version {entry.name} left in place: it has no "
                "valid manifest, so retention cannot classify it"
            )
    versions.sort(key=lambda item: (item[0], item[1]), reverse=True)
    for _fetched, name, path, manifest in versions[keep:]:
        if protect is not None and name == protect:
            continue
        try:
            actual = _zip_sha256(path / "gtfs.zip")
        except GtfsLoadError as exc:
            notes.append(
                f"dataset version {name} left in place: its archive could "
                f"not be verified ({exc.message})"
            )
            continue
        if actual != manifest.sha256:
            notes.append(
                f"dataset version {name} left in place: its archive does "
                "not match its manifest, so it is uncertain"
            )
            continue
        try:
            shutil.rmtree(path)
        except OSError as exc:
            notes.append(
                f"dataset version {name} could not be removed: "
                f"{exc.__class__.__name__}: {exc}"
            )
            continue
        notes.append(f"pruned old dataset version {name}")
    return tuple(notes)
