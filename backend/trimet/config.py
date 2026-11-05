"""Configuration foundation for the MAX Intelligence static-GTFS pipeline.

Stage S0 of M1 (Static GTFS) per the approved Rev. 9 plan. This is the only
M1 module that contains code so far; every other module is a placeholder.

Precedence for every setting (plan Rev. 3, §B):

    process environment  >  .env file at the repo root  >  defaults below

All defaults resolve against ``REPO_ROOT`` — this file's own location — and
never against the process working directory, so the loader behaves identically
however it is launched (plan Rev. 3 change 3 / Rev. 9).

Environment variables recognised (all optional, all non-secret):

==============================  ===================================================
``GTFS_FEED_URL``               static GTFS zip URL (default: TriMet's)
``GTFS_WORK_DIR``               working dir (default: ``<repo>/data/gtfs``);
                                relative values resolve against REPO_ROOT
``GTFS_MAX_ZIP_BYTES``           download size cap (default: 2 GiB)
``GTFS_MAX_DATASET_BYTES``       extracted-dataset total size cap, uncompressed
                                 corpus bytes across all members (default:
                                 20 GiB = 10x the zip cap); must be >= the
                                 per-member cap so one knob never traps the
                                 other
``GTFS_MAX_UNRELATED_MALFORMED_ROWS`` parse tolerance for non-MAX rows (default: 50)
``GTFS_STALE_WARNING_DAYS``     stale-static-dataset warning age (default: 45)
``GTFS_M0_ROUTECONFIG_EVIDENCE_PATH`` M0 routeConfig evidence referenced in place
                                (default: ``tools/m0_audit/evidence/
                                routeconfig_selected.json``); never copied (D5
                                replacement, Rev. 2 change 6)
``GTFS_STALE_STAGING_TTL_MINUTES`` staging-age threshold used ONLY by the report-only
                                startup inventory (Rev. 4 change 1; default: 30)
``GTFS_PROMOTION_MAX_ATTEMPTS`` bounded promotion retry attempts (default: 3)
``GTFS_PROMOTION_BACKOFF_SECONDS`` comma-separated backoff seconds (default: 0.5,1,2);
                                must have exactly as many entries as attempts
``GTFS_REUSE_PROMOTED``         allow the double-checked promote short-circuit
                                (default: true; accepts true/1/yes or false/0/no,
                                case-insensitive — any other value is an error)

Secrets (M1 scope): only ``GTFS_*`` keys are consulted. ``TRIMET_APP_ID``
belongs to the M2+ vehicle code paths and is intentionally not read, logged,
or stored here.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, fields
from pathlib import Path
from urllib.parse import urlparse

# Repo root is fixed by this module's location, per plan Rev. 9 (flaw-3 fix):
# backend/trimet/config.py -> parents[2] == repository root.
REPO_ROOT: Path = Path(__file__).resolve().parents[2]

# M0-verified feed location (tools evidence: gtfs_audit.json; docs §A5).
DEFAULT_FEED_URL = "https://developer.trimet.org/schedule/gtfs.zip"

_ENV_FILE = REPO_ROOT / ".env"


def _load_env_file(path: Path) -> dict[str, str]:
    """Read a ``KEY=VALUE`` .env file. Values are never printed or logged.

    Same parsing technique proven by ``tools/m0_audit/audit_m0.py`` in M0.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _env_value(name: str, env: dict[str, str]) -> str | None:
    """First non-empty value: process environment, then the .env file."""
    from_env = os.environ.get(name)
    if from_env is not None and from_env.strip():
        return from_env.strip()
    from_file = env.get(name)
    if from_file is not None and from_file.strip():
        return from_file.strip()
    return None


def _resolve_path(raw: str | Path) -> Path:
    """Absolute paths pass through; relative paths resolve against REPO_ROOT."""
    candidate = Path(raw).expanduser()
    return candidate if candidate.is_absolute() else (REPO_ROOT / candidate).resolve()


def _require_https(url: str) -> None:
    """The feed is downloaded unauthenticated; HTTPS is mandatory (M0 posture)."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(
            "GTFS_FEED_URL must be an https:// URL for an unauthenticated "
            f"third-party download (got {url!r})"
        )


def _parse_bool(name: str, raw: str) -> bool:
    """Strict boolean parsing: ``true/1/yes`` and ``false/0/no`` only.

    Case and surrounding whitespace are tolerated (``_env_value`` strips, and
    this strips again for safety). Everything else is an actionable error, so
    values such as ``maybe`` can no longer be silently read as ``true``
    (review fix 2).
    """
    value = raw.strip().lower()
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    raise ValueError(
        f"{name} expects true/1/yes or false/0/no (case-insensitive), got {raw!r}"
    )


def _parse_backoff_list(name: str, raw: str) -> tuple[float, ...]:
    """Parse a comma-separated backoff list; every entry must be finite and > 0.

    Empty entries such as ``0.5,,2`` are rejected as malformed instead of being
    silently discarded (review fix 4). Non-finite values are rejected because
    ``float("nan")`` and ``nan <= 0`` are both falsy-evasive traps: NaN and the
    infinities would previously slip past the numeric validation
    (review fix 5).
    """
    entries = raw.split(",")
    for index, entry in enumerate(entries):
        if not entry.strip():
            raise ValueError(
                f"{name} contains an empty entry at position {index + 1} "
                f"(malformed comma-separated list), got {raw!r}"
            )
    try:
        values = tuple(float(entry) for entry in entries)
    except ValueError:
        raise ValueError(f"{name} expects comma-separated numbers, got {raw!r}")
    for value in values:
        if not math.isfinite(value):
            raise ValueError(f"{name} entries must be finite numbers, got {raw!r}")
        if value <= 0:
            raise ValueError(f"{name} entries must be > 0, got {raw!r}")
    return values


@dataclass(frozen=True)
class GtfsConfig:
    """Static-GTFS pipeline settings (M1 plan Rev. 9, §B and §H).

    Field docs mirror the env-var table in this module's docstring. Defaults
    live here so that a single source of truth exists for both the config file
    and ``.env.example`` documentation.
    """

    feed_url: str = DEFAULT_FEED_URL
    work_dir: Path = REPO_ROOT / "data" / "gtfs"
    # Download/extraction guards (plan §5: caps checked before parsing starts).
    max_zip_bytes: int = 2 * 1024**3  # real dataset was 43,467,097 bytes (M0)
    # Uncompressed-corpus cap for extraction (S2 review fix 1): the download
    # cap bounds the *compressed* archive; many individually small members
    # could still decompress enormously. Default is 10x the zip cap, the
    # documented typical text-compression ratio of GTFS corpora.
    max_dataset_bytes: int = 20 * 1024**3
    # Parse tolerance (plan §E.3): MAX-subset rows are strict; unrelated rows capped.
    max_unrelated_malformed_rows: int = 50
    # Stale static-data watch (plan §E.6; surfaced later via M3 /health).
    stale_warning_days: int = 45
    # M0 routeConfig evidence referenced in place, never duplicated
    # (plan Rev. 2 change 6, overriding the original D5 proposal).
    m0_routeconfig_evidence_path: Path = (
        REPO_ROOT / "tools" / "m0_audit" / "evidence" / "routeconfig_selected.json"
    )
    # Staging hygiene (plan Rev. 4 change 1: report-only inventory, no deletion).
    stale_staging_ttl_minutes: int = 30
    # Promotion under promote.lock (plan Revs. 5–9: double-checked locking with
    # a write-once immutable record; here only the *retry window* is configured).
    promotion_max_attempts: int = 3
    promotion_backoff_seconds: tuple[float, ...] = (0.5, 1.0, 2.0)
    reuse_promoted: bool = True

    @classmethod
    def from_env(cls, env_file: Path | None = None) -> "GtfsConfig":
        """Build a config from the process env, an optional .env file, and defaults.

        ``env_file`` exists mainly for tests: passing a temporary file controls
        the file layer without touching the repository's real ``.env``.
        """
        env = _load_env_file(env_file or _ENV_FILE)
        defaults = {f.name: f.default for f in fields(cls)}

        def optional_int(name: str) -> int | None:
            raw = _env_value(name, env)
            if raw is None:
                return None
            try:
                return int(raw)
            except ValueError:
                raise ValueError(f"{name} expects an integer value, got {raw!r}")

        def optional_bool(name: str) -> bool | None:
            raw = _env_value(name, env)
            return None if raw is None else _parse_bool(name, raw)

        def optional_backoff() -> tuple[float, ...] | None:
            raw = _env_value("GTFS_PROMOTION_BACKOFF_SECONDS", env)
            if raw is None:
                return None
            return _parse_backoff_list("GTFS_PROMOTION_BACKOFF_SECONDS", raw)

        # Each value is parsed exactly once into a local; ``None`` is the only
        # signal that a setting is absent (review fixes 1 and 3). In particular
        # an explicit ``GTFS_MAX_ZIP_BYTES=0`` must reach _validate() and fail
        # there, never be masked away by truthiness (``0 or default``).
        feed_url_raw = _env_value("GTFS_FEED_URL", env)
        work_dir_raw = _env_value("GTFS_WORK_DIR", env)
        evidence_raw = _env_value("GTFS_M0_ROUTECONFIG_EVIDENCE_PATH", env)
        max_zip_bytes = optional_int("GTFS_MAX_ZIP_BYTES")
        max_dataset_bytes = optional_int("GTFS_MAX_DATASET_BYTES")
        max_unrelated_malformed_rows = optional_int("GTFS_MAX_UNRELATED_MALFORMED_ROWS")
        stale_warning_days = optional_int("GTFS_STALE_WARNING_DAYS")
        stale_staging_ttl_minutes = optional_int("GTFS_STALE_STAGING_TTL_MINUTES")
        promotion_max_attempts = optional_int("GTFS_PROMOTION_MAX_ATTEMPTS")
        promotion_backoff_seconds = optional_backoff()
        reuse_promoted = optional_bool("GTFS_REUSE_PROMOTED")

        config = cls(
            feed_url=(
                defaults["feed_url"] if feed_url_raw is None else feed_url_raw
            ),
            work_dir=_resolve_path(work_dir_raw) if work_dir_raw else defaults["work_dir"],
            max_zip_bytes=(
                defaults["max_zip_bytes"] if max_zip_bytes is None else max_zip_bytes
            ),
            max_dataset_bytes=(
                defaults["max_dataset_bytes"]
                if max_dataset_bytes is None
                else max_dataset_bytes
            ),
            max_unrelated_malformed_rows=(
                defaults["max_unrelated_malformed_rows"]
                if max_unrelated_malformed_rows is None
                else max_unrelated_malformed_rows
            ),
            m0_routeconfig_evidence_path=(
                _resolve_path(evidence_raw)
                if evidence_raw
                else defaults["m0_routeconfig_evidence_path"]
            ),
            stale_warning_days=(
                defaults["stale_warning_days"]
                if stale_warning_days is None
                else stale_warning_days
            ),
            stale_staging_ttl_minutes=(
                defaults["stale_staging_ttl_minutes"]
                if stale_staging_ttl_minutes is None
                else stale_staging_ttl_minutes
            ),
            promotion_max_attempts=(
                defaults["promotion_max_attempts"]
                if promotion_max_attempts is None
                else promotion_max_attempts
            ),
            promotion_backoff_seconds=(
                defaults["promotion_backoff_seconds"]
                if promotion_backoff_seconds is None
                else promotion_backoff_seconds
            ),
            reuse_promoted=(
                defaults["reuse_promoted"] if reuse_promoted is None else reuse_promoted
            ),
        )
        _validate(config)
        return config


def _validate(config: GtfsConfig) -> None:
    """Fail fast on misconfiguration with actionable messages."""
    _require_https(config.feed_url)
    if config.max_zip_bytes <= 0:
        raise ValueError("GTFS_MAX_ZIP_BYTES must be a positive number of bytes")
    if config.max_dataset_bytes <= 0:
        raise ValueError("GTFS_MAX_DATASET_BYTES must be a positive number of bytes")
    if config.max_dataset_bytes < config.max_zip_bytes:
        raise ValueError(
            "GTFS_MAX_DATASET_BYTES must be >= GTFS_MAX_ZIP_BYTES so the "
            "per-member cap can never exceed the whole-dataset cap "
            f"(got dataset {config.max_dataset_bytes} < zip "
            f"{config.max_zip_bytes})"
        )
    if config.max_unrelated_malformed_rows < 0:
        raise ValueError("GTFS_MAX_UNRELATED_MALFORMED_ROWS must be >= 0")
    if config.stale_warning_days < 0:
        raise ValueError("GTFS_STALE_WARNING_DAYS must be >= 0")
    if config.stale_staging_ttl_minutes <= 0:
        raise ValueError("GTFS_STALE_STAGING_TTL_MINUTES must be > 0")
    if config.promotion_max_attempts <= 0:
        raise ValueError("GTFS_PROMOTION_MAX_ATTEMPTS must be >= 1")
    if len(config.promotion_backoff_seconds) != config.promotion_max_attempts:
        raise ValueError(
            "GTFS_PROMOTION_BACKOFF_SECONDS must have exactly as many comma-"
            "separated entries as GTFS_PROMOTION_MAX_ATTEMPTS "
            f"(got {len(config.promotion_backoff_seconds)} vs "
            f"{config.promotion_max_attempts})"
        )
    if any(
        not math.isfinite(value) or value <= 0
        for value in config.promotion_backoff_seconds
    ):
        raise ValueError(
            "GTFS_PROMOTION_BACKOFF_SECONDS entries must be finite and > 0"
        )
