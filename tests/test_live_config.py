"""S10 configuration contract tests (plan §4)."""
import dataclasses
from pathlib import Path

import pytest
import zoneinfo

from tests._s10_helpers import FAKE_APP_ID, install_socket_guard, make_config
from backend.trimet.live_config import AppIDSecret, LiveConfig, load_live_config
from backend.trimet.poll_errors import PollConfigError
from backend.trimet.quota import compute_reset_window
from zoneinfo import ZoneInfo


@pytest.fixture(autouse=True)
def _s10_socket_guard(monkeypatch):
    install_socket_guard(monkeypatch)


ENV_FILE = """\
TRIMET_APP_ID= {sentinel}
TRIMET_QUOTA_INITIAL_USED=42
TRIMET_VEHICLES_POLL_SECONDS=11
TRIMET_QUOTA_RESET_TIME=04:30
TRIMET_QUOTA_RESET_ZONE={zone}
"""


def _write_env(tmp_path: Path, content: str, sentinel: str = FAKE_APP_ID) -> Path:
    path = tmp_path / "env"
    path.write_text(
        content.format(sentinel=sentinel, zone="America/Los_Angeles"),
        encoding="utf-8",
    )
    return path


class TestPrecedenceAndRequired:
    def test_env_overrides_env_file_overrides_defaults(self, tmp_path):
        path = _write_env(tmp_path, ENV_FILE)
        cfg = load_live_config(
            env_file=path,
            environ={"TRIMET_VEHICLES_POLL_SECONDS": "13"},
        )
        assert cfg.quota_initial_used == 42
        assert cfg.quota_reset_hour == 4 and cfg.quota_reset_minute == 30
        assert cfg.quota_reset_zone_name == "America/Los_Angeles"
        assert cfg.vehicles_poll_seconds == 13  # environment beats the file

    def test_process_environment_wins_over_file(self, tmp_path):
        path = _write_env(tmp_path, ENV_FILE)
        cfg = load_live_config(
            env_file=path,
            environ={"TRIMET_QUOTA_INITIAL_USED": "900"},
        )
        assert cfg.quota_initial_used == 900

    def test_valid_defaults_when_only_required_given(self, tmp_path):
        path = tmp_path / "env"
        path.write_text(
            f"TRIMET_APP_ID={FAKE_APP_ID}\nTRIMET_QUOTA_INITIAL_USED=0\n",
            encoding="utf-8",
        )
        cfg = load_live_config(env_file=path)
        assert cfg.vehicles_poll_seconds == 10
        assert cfg.tripupdates_poll_seconds == 60
        assert cfg.alerts_poll_seconds == 120
        assert cfg.vehicles_full_seconds == 300
        assert cfg.quota_daily_budget == 1_000_000
        assert cfg.quota_reset_hour == 0 and cfg.quota_reset_minute == 0
        assert cfg.quota_reset_zone_name == "UTC"
        assert cfg.max_attempts == 2

    def test_missing_app_id_fails(self, tmp_path):
        path = tmp_path / "env"
        path.write_text("TRIMET_QUOTA_INITIAL_USED=1\n", encoding="utf-8")
        with pytest.raises(PollConfigError):
            load_live_config(env_file=path)

    def test_blank_app_id_fails(self, tmp_path):
        path = tmp_path / "env"
        path.write_text(
            "TRIMET_APP_ID=\nTRIMET_QUOTA_INITIAL_USED=1\n", encoding="utf-8"
        )
        with pytest.raises(PollConfigError):
            load_live_config(env_file=path)

    def test_whitespace_only_app_id_fails(self, tmp_path):
        path = tmp_path / "env"
        path.write_text(
            "TRIMET_APP_ID=   \nTRIMET_QUOTA_INITIAL_USED=1\n", encoding="utf-8"
        )
        with pytest.raises(PollConfigError):
            load_live_config(env_file=path)

    def test_appid_is_trimmed_and_no_format_invented(self, tmp_path):
        path = _write_env(
            tmp_path,
            "TRIMET_APP_ID=  {sentinel}  \nTRIMET_QUOTA_INITIAL_USED=1\n",
            sentinel="an arcane & unusual credential string",
        )
        cfg = load_live_config(env_file=path)
        assert cfg.app_id.value == "an arcane & unusual credential string"

    def test_missing_initial_used_fails(self, tmp_path):
        path = tmp_path / "env"
        path.write_text(f"TRIMET_APP_ID={FAKE_APP_ID}\n", encoding="utf-8")
        with pytest.raises(PollConfigError):
            load_live_config(env_file=path)


class TestNumericAndValidation:
    @pytest.mark.parametrize("raw", ["abc", "1.5", "-3", "1e4"])
    def test_invalid_numeric_seed_fails(self, tmp_path, raw):
        path = tmp_path / "env"
        path.write_text(
            f"TRIMET_APP_ID={FAKE_APP_ID}\nTRIMET_QUOTA_INITIAL_USED={raw}\n",
            encoding="utf-8",
        )
        with pytest.raises(PollConfigError):
            load_live_config(env_file=path)

    def test_initial_used_above_budget_fails(self, tmp_path):
        path = tmp_path / "env"
        path.write_text(
            f"TRIMET_APP_ID={FAKE_APP_ID}\nTRIMET_QUOTA_INITIAL_USED=1000001\n",
            encoding="utf-8",
        )
        with pytest.raises(PollConfigError):
            load_live_config(env_file=path)

    def test_numeric_full_budget_seed_is_valid(self, tmp_path):
        path = tmp_path / "env"
        path.write_text(
            f"TRIMET_APP_ID={FAKE_APP_ID}\nTRIMET_QUOTA_INITIAL_USED=1000000\n",
            encoding="utf-8",
        )
        cfg = load_live_config(env_file=path)
        assert cfg.quota_initial_used == cfg.quota_daily_budget

    def test_bool_seed_forbidden(self):
        with pytest.raises(PollConfigError):
            LiveConfig(app_id=AppIDSecret(FAKE_APP_ID), quota_initial_used=True)

    def test_non_int_seed_forbidden(self):
        with pytest.raises(PollConfigError):
            LiveConfig(app_id=AppIDSecret(FAKE_APP_ID), quota_initial_used=1.0)

    @pytest.mark.parametrize(
        "field", ["connect_timeout_s", "read_timeout_s", "attempt_deadline_s"]
    )
    def test_non_finite_durations_fail(self, field):
        for bad in (float("nan"), float("inf")):
            with pytest.raises(PollConfigError):
                LiveConfig(
                    app_id=AppIDSecret(FAKE_APP_ID),
                    quota_initial_used=0,
                    **{field: bad},
                )

    def test_non_positive_durations_fail(self):
        with pytest.raises(PollConfigError):
            LiveConfig(
                app_id=AppIDSecret(FAKE_APP_ID),
                quota_initial_used=0,
                connect_timeout_s=0.0,
            )

    def test_invalid_zone_fails(self):
        with pytest.raises(PollConfigError):
            LiveConfig(
                app_id=AppIDSecret(FAKE_APP_ID),
                quota_initial_used=0,
                quota_reset_zone_name="Mars/Olympus",
            )

    def test_invalid_reset_time_format_fails(self, tmp_path):
        path = tmp_path / "env"
        path.write_text(
            f"TRIMET_APP_ID={FAKE_APP_ID}\nTRIMET_QUOTA_INITIAL_USED=0\n"
            "TRIMET_QUOTA_RESET_TIME=2:0 O\n",
            encoding="utf-8",
        )
        with pytest.raises(PollConfigError):
            load_live_config(env_file=path)

    def test_invalid_reset_time_values_fail(self, tmp_path):
        for bad_time in ("24:00", "12:60", "12:5", "7 30"):
            path = tmp_path / "env"
            path.write_text(
                f"TRIMET_APP_ID={FAKE_APP_ID}\nTRIMET_QUOTA_INITIAL_USED=0\n"
                f"TRIMET_QUOTA_RESET_TIME={bad_time}\n",
                encoding="utf-8",
            )
            with pytest.raises(PollConfigError):
                load_live_config(env_file=path)

    def test_max_attempts_must_be_one_or_two(self):
        for bad in (0, 3, -1):
            with pytest.raises(PollConfigError):
                LiveConfig(
                    app_id=AppIDSecret(FAKE_APP_ID),
                    quota_initial_used=0,
                    max_attempts=bad,
                )
        for good in (1, 2):
            make_config(max_attempts=good)

    def test_full_interval_below_vehicle_interval_fails(self):
        with pytest.raises(PollConfigError):
            make_config(vehicles_poll_seconds=40, vehicles_full_seconds=30)

    def test_byte_limits_must_be_positive_ints(self):
        with pytest.raises(PollConfigError):
            make_config(alerts_max_bytes=0)
        with pytest.raises(PollConfigError):
            make_config(vehicles_max_bytes=True)


class TestTzdataFallback:
    def test_configured_zones_resolve_with_system_tzpath_disabled(self, monkeypatch):
        # The reviewed tzdata package must supply IANA data when no system
        # timezone database path is configured.
        zoneinfo.reset_tzpath([])
        try:
            zoneinfo.ZoneInfo.clear_cache()
            assert ZoneInfo("America/Los_Angeles").key == "America/Los_Angeles"
            assert ZoneInfo("UTC").key == "UTC"
        finally:
            zoneinfo.reset_tzpath()
            zoneinfo.ZoneInfo.clear_cache()


class TestCredentialRedaction:
    def test_repr_of_config_hides_credential(self):
        cfg = make_config()
        r = repr(cfg)
        assert FAKE_APP_ID not in r

    def test_appid_secret_repr_is_redacted(self):
        secret = AppIDSecret(FAKE_APP_ID)
        assert FAKE_APP_ID not in repr(secret)
        assert str(secret) == repr(secret)

    def test_appid_field_excluded_from_dataclass_repr_fields(self):
        cfg = make_config()
        for fld in dataclasses.fields(cfg):
            if isinstance(getattr(cfg, fld.name), AppIDSecret):
                assert fld.repr is False, "app_id must be repr-excluded"

    def test_config_snapshot_never_carries_credential(self):
        from tests._s10_helpers import FakeClock
        from backend.trimet.poll_types import PollFeed
        from backend.trimet.polling import PollingRuntime
        from backend.trimet.quota import QuotaGovernor

        cfg = make_config()
        runtime = PollingRuntime(
            cfg,
            governor=QuotaGovernor(10, 0, ZoneInfo("UTC"), 0, 0, FakeClock()),
            sinks={feed: lambda d: None for feed in PollFeed},
        )
        snap = runtime.snapshot()
        assert FAKE_APP_ID not in repr(snap)
