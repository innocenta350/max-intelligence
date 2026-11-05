"""Stage S0 checks for the configuration foundation (backend/trimet/config.py).

Focus: repo-root path resolution (plan Rev. 9 flaw-3 fix), environment
precedence, rejection of insecure feed URLs, and the strict parsing /
validation corrections from the review: explicit zero never masked by the
default, strict booleans, single-parse semantics, malformed backoff lists,
and finite/positive backoff values.
"""

import pytest

from backend.trimet.config import GtfsConfig, REPO_ROOT


def _missing_env_file(tmp_path):
    """A nonexistent file makes from_env() read defaults only, deterministically."""
    return tmp_path / "this-env-file-does-not-exist.env"


def _fake_env_file(tmp_path, content: str = ""):
    """A present-but-empty .env so only the process environment applies."""
    path = tmp_path / ".env"
    path.write_text(content, encoding="utf-8")
    return path


def test_defaults_resolve_from_repo_root(tmp_path) -> None:
    cfg = GtfsConfig.from_env(env_file=_missing_env_file(tmp_path))
    assert cfg.feed_url == "https://developer.trimet.org/schedule/gtfs.zip"
    assert cfg.work_dir.is_absolute()
    assert cfg.work_dir == REPO_ROOT / "data" / "gtfs"
    # Plan Rev. 9 (flaw-3 fix): the M0 evidence default must resolve from the
    # repo root — independent of the process working directory — and the file
    # is committed by M0, so it exists right now.
    assert cfg.m0_routeconfig_evidence_path.exists()
    assert cfg.promotion_max_attempts == 3
    assert cfg.promotion_backoff_seconds == (0.5, 1.0, 2.0)
    assert cfg.reuse_promoted is True


def test_env_overrides_apply(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path, "GTFS_FEED_URL=https://example.test/gtfs.zip\n")
    monkeypatch.setenv("GTFS_WORK_DIR", str(tmp_path / "custom-data"))
    monkeypatch.setenv("GTFS_PROMOTION_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("GTFS_PROMOTION_BACKOFF_SECONDS", "0.1,0.2")
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.feed_url == "https://example.test/gtfs.zip"
    # Relative and absolute override paths both resolve absolutely.
    assert cfg.work_dir == (tmp_path / "custom-data").resolve()
    assert cfg.promotion_max_attempts == 2
    assert cfg.promotion_backoff_seconds == (0.1, 0.2)


def test_relative_paths_resolve_against_repo_root(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_WORK_DIR", "data/gtfs-alternate")
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.work_dir == (REPO_ROOT / "data" / "gtfs-alternate").resolve()


def test_http_feed_url_is_rejected(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_FEED_URL", "http://insecure.example/gtfs.zip")
    with pytest.raises(ValueError, match="https"):
        GtfsConfig.from_env(env_file=fake_env)


def test_backoff_attempt_count_mismatch_is_rejected(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_PROMOTION_MAX_ATTEMPTS", "4")
    monkeypatch.setenv("GTFS_PROMOTION_BACKOFF_SECONDS", "0.1,0.2,0.4")
    with pytest.raises(ValueError, match="GTFS_PROMOTION_BACKOFF_SECONDS"):
        GtfsConfig.from_env(env_file=fake_env)


# ---------------------------------------------------------------------------
# Review fix 1: GTFS_MAX_ZIP_BYTES=0 must be rejected by validation, never
# silently replaced by the 2 GiB default; None (absent) is the sole "unset".
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["0", "-1024"])
def test_zero_or_negative_max_zip_bytes_is_rejected(monkeypatch, tmp_path, raw) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_MAX_ZIP_BYTES", raw)
    with pytest.raises(ValueError, match="GTFS_MAX_ZIP_BYTES"):
        GtfsConfig.from_env(env_file=fake_env)


def test_nonzero_explicit_max_zip_bytes_is_preserved(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_MAX_ZIP_BYTES", "1000")
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.max_zip_bytes == 1000


# ---------------------------------------------------------------------------
# Review fix 2: strict boolean parsing — true/1/yes vs false/0/no, tolerant
# of case and surrounding whitespace, everything else rejected.
# ---------------------------------------------------------------------------

TRUE_VARIANTS = ["true", "TRUE", "True", "1", "yes", "YES", "  yes  "]
FALSE_VARIANTS = ["false", "FALSE", "False", "0", "no", "No", "  no  "]
INVALID_BOOLS = ["maybe", "off", "on", "2", "-1", "enabled", "yes no"]


@pytest.mark.parametrize("raw", TRUE_VARIANTS)
def test_bool_true_variants(monkeypatch, tmp_path, raw) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_REUSE_PROMOTED", raw)
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.reuse_promoted is True


@pytest.mark.parametrize("raw", FALSE_VARIANTS)
def test_bool_false_variants(monkeypatch, tmp_path, raw) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_REUSE_PROMOTED", raw)
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.reuse_promoted is False


@pytest.mark.parametrize("raw", INVALID_BOOLS)
def test_bool_invalid_values_are_rejected(monkeypatch, tmp_path, raw) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_REUSE_PROMOTED", raw)
    with pytest.raises(ValueError, match="GTFS_REUSE_PROMOTED"):
        GtfsConfig.from_env(env_file=fake_env)


def test_bool_empty_value_still_means_unset(monkeypatch, tmp_path) -> None:
    """Blank/whitespace values remain the documented 'absent' form -> default."""
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_REUSE_PROMOTED", "   ")
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.reuse_promoted is True


# ---------------------------------------------------------------------------
# Review fix 4: empty entries in the backoff list are malformed, not silently
# discarded (``0.5,,2`` used to parse as ``(0.5, 2.0)``).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["0.5,,2", "0.5, ,2", ",,", "0.5,2,"])
def test_backoff_empty_entries_are_rejected(monkeypatch, tmp_path, raw) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_PROMOTION_BACKOFF_SECONDS", raw)
    with pytest.raises(ValueError, match="empty entry"):
        GtfsConfig.from_env(env_file=fake_env)


# ---------------------------------------------------------------------------
# Review fix 5: backoff entries must be finite (NaN and the infinities used to
# slip past the ``<= 0`` check) and positive (zero/negative rejected).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["nan", "inf", "-inf", "Infinity", "nan,1"])
def test_backoff_non_finite_entries_are_rejected(monkeypatch, tmp_path, raw) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_PROMOTION_BACKOFF_SECONDS", raw)
    with pytest.raises(ValueError, match="finite"):
        GtfsConfig.from_env(env_file=fake_env)


@pytest.mark.parametrize("raw", ["0", "0.0", "-0.5", "-1"])
def test_backoff_zero_or_negative_entries_are_rejected(monkeypatch, tmp_path, raw) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_PROMOTION_BACKOFF_SECONDS", raw)
    with pytest.raises(ValueError, match="must be > 0"):
        GtfsConfig.from_env(env_file=fake_env)


# ---------------------------------------------------------------------------
# S2 review fix 1: GTFS_MAX_DATASET_BYTES — the total uncompressed corpus cap
# for extraction. The zip cap bounds the compressed archive only; many
# individually small members could still decompress enormously.
# ---------------------------------------------------------------------------


def test_default_dataset_cap_is_ten_x_zip_cap(tmp_path) -> None:
    cfg = GtfsConfig.from_env(env_file=_missing_env_file(tmp_path))
    assert cfg.max_dataset_bytes == 10 * cfg.max_zip_bytes


@pytest.mark.parametrize("raw", ["0", "-1", "-4096"])
def test_zero_or_negative_max_dataset_bytes_is_rejected(
    monkeypatch, tmp_path, raw
) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_MAX_DATASET_BYTES", raw)
    with pytest.raises(ValueError, match="GTFS_MAX_DATASET_BYTES"):
        GtfsConfig.from_env(env_file=fake_env)


def test_dataset_cap_below_zip_cap_is_rejected(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_MAX_ZIP_BYTES", "100000")
    monkeypatch.setenv("GTFS_MAX_DATASET_BYTES", "99999")
    with pytest.raises(ValueError, match="must be >= GTFS_MAX_ZIP_BYTES"):
        GtfsConfig.from_env(env_file=fake_env)


def test_dataset_cap_equal_to_zip_cap_is_accepted(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_MAX_ZIP_BYTES", "100000")
    monkeypatch.setenv("GTFS_MAX_DATASET_BYTES", "100000")
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.max_dataset_bytes == 100000


def test_explicit_dataset_cap_is_preserved(monkeypatch, tmp_path) -> None:
    fake_env = _fake_env_file(tmp_path)
    monkeypatch.setenv("GTFS_MAX_ZIP_BYTES", "1000")
    monkeypatch.setenv("GTFS_MAX_DATASET_BYTES", "2048")
    cfg = GtfsConfig.from_env(env_file=fake_env)
    assert cfg.max_dataset_bytes == 2048
