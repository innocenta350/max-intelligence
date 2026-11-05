"""Stage S1 checks for the GTFS load-error hierarchy (backend/gtfs/errors.py)."""

import pytest

from backend.gtfs.errors import (
    CellParseError,
    CorruptZipError,
    ExtractionError,
    GtfsLoadError,
    ManifestUnreadableError,
    MaxInvariantError,
    MissingRequiredFileError,
    RetryablePromoteLockError,
    SchemaViolationError,
    ThresholdExceededError,
)
from backend.gtfs.types import RouteCoverage

ALL_ERROR_CLASSES = [
    ExtractionError,
    CorruptZipError,
    MissingRequiredFileError,
    SchemaViolationError,
    ThresholdExceededError,
    MaxInvariantError,
    ManifestUnreadableError,
    RetryablePromoteLockError,
    CellParseError,
]


@pytest.mark.parametrize("error_class", ALL_ERROR_CLASSES)
def test_every_error_derives_from_base(error_class: type[Exception]) -> None:
    assert issubclass(error_class, GtfsLoadError)


def test_zip_failures_refine_extraction_error() -> None:
    assert issubclass(CorruptZipError, ExtractionError)
    assert issubclass(MissingRequiredFileError, ExtractionError)
    assert issubclass(ExtractionError, GtfsLoadError)


def test_error_is_catchable_as_base() -> None:
    with pytest.raises(GtfsLoadError):
        raise SchemaViolationError("duplicate trip_id", file="trips.txt", line_number=7)


def test_context_is_rendered_into_str() -> None:
    error = SchemaViolationError(
        "duplicate header columns", file="stops.txt", line_number=12, column="stop_id"
    )
    text = str(error)
    assert "stops.txt" in text
    assert "line 12" in text
    assert "column 'stop_id'" in text
    assert "duplicate header columns" in text


def test_context_is_optional() -> None:
    plain = ThresholdExceededError("malformed-row budget exhausted")
    assert str(plain) == "malformed-row budget exhausted"
    assert plain.file is None
    assert plain.line_number is None
    assert plain.column is None


def test_missing_required_file_records_members() -> None:
    error = MissingRequiredFileError(
        "archive is missing required members", members=("shapes.txt", "feed_info.txt")
    )
    assert error.members == ("shapes.txt", "feed_info.txt")
    assert error.members is not None  # tuple, immutable


def test_manifest_unreadable_names_directory() -> None:
    error = ManifestUnreadableError(
        "manifest.json is truncated", directory="feed-20261011-20261021-0903"
    )
    assert error.directory == "feed-20261011-20261021-0903"
    # Plan Rev. 7 §2 / Rev. 8 §1: the error must survive catchability checks
    # as a plain GtfsLoadError for the cleanup path in S2.
    assert isinstance(error, GtfsLoadError)


def test_promote_lock_error_carries_foreign_diagnostics() -> None:
    error = RetryablePromoteLockError(
        "promote.lock held by another process",
        foreign_pid=4242,
        acquired_utc_ms=1_760_000_000_000,
    )
    assert error.foreign_pid == 4242
    assert error.acquired_utc_ms == 1_760_000_000_000


def test_max_invariant_error_carries_route_and_coverage() -> None:
    coverage = RouteCoverage(
        route_id="90",
        directions=(),
        trips_without_shape=frozenset(),
        usable_shape_ids=frozenset(),
        broken_shape_ids=frozenset({"sh-broken"}),
        reachable_stop_ids=frozenset(),
        unmatched_stop_ids=frozenset(),
    )
    error = MaxInvariantError(
        "route has no usable shape", route_id="90", coverage=coverage
    )
    assert error.route_id == "90"
    assert error.coverage is coverage
    assert MaxInvariantError("no usable shape").coverage is None


def test_cell_parse_error_keeps_raw_context() -> None:
    error = CellParseError(
        "cannot parse integer value",
        raw_value="abc",
        column="stop_sequence",
        file="stop_times.txt",
        line_number=3,
    )
    assert error.raw_value == "abc"
    assert error.column == "stop_sequence"
    assert error.line_number == 3
    assert "stop_times.txt" in str(error)
    assert "line 3" in str(error)


def test_error_messages_do_not_embed_env_state() -> None:
    """The context contract: only parsing context, never environment dumps."""
    error = CellParseError("bad cell", raw_value="x", file="trips.txt", line_number=9)
    rendered = str(error) + repr(error.args[0])
    for forbidden in ("GTFS_", "TRIMET_APP_ID", "os.environ", ".env"):
        assert forbidden not in rendered


def test_cell_parse_error_bounds_stored_raw_value() -> None:
    """Review fix 1: enforcement lives in the exception, not call convention."""
    error = CellParseError("bad cell", raw_value="a" * 500)
    assert len(error.raw_value) == 64
    assert error.raw_value.endswith("...")
    rendered = str(error)
    assert "a" * 100 not in rendered  # nothing beyond the bound leaks
    assert "(raw: 'aaaa" in rendered  # bounded value rendered exactly once
