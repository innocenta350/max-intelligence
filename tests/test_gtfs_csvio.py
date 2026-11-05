"""Stage S1 checks for the CSV primitives (backend/gtfs/csvio.py).

Synthetic files only; no live feed, no network access. Handle-safety is
verified Windows-style: renaming (os.replace) a file that is still open
raises PermissionError, so a successful rename after the context manager
proves the handle was released.
"""

import os
from pathlib import Path

import pytest

from backend.gtfs.csvio import (
    CsvRowProblemKind,
    get_float,
    get_int,
    get_str,
    get_time_secs,
    open_gtfs_csv,
)
from backend.gtfs.errors import CellParseError, SchemaViolationError


def write_csv(tmp_path: Path, name: str, text: str, *, bom: bool = False) -> Path:
    """Write a synthetic CSV as raw bytes (deterministic newlines, no CRLF)."""
    path = tmp_path / name
    raw = text.encode("utf-8")
    if bom:
        raw = b"\xef\xbb\xbf" + raw
    path.write_bytes(raw)
    return path


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

def test_valid_file_roundtrip_with_line_numbers(tmp_path) -> None:
    path = write_csv(
        tmp_path,
        "routes.txt",
        "route_id,route_short_name,route_color\n90,90,C41F3E\n100,100,1359AE\n",
    )
    with open_gtfs_csv(path) as table:
        assert table.header.columns == ("route_id", "route_short_name", "route_color")
        assert table.header.has("route_color") is True
        assert table.header.has("ROUTE_ID") is True  # normalised lookup
        records = list(table.iter_records())
        assert [values["route_id"] for _, values in records] == ["90", "100"]
        assert [line for line, _ in records] == [2, 3]
        assert records[0][1]["route_color"] == "C41F3E"
        assert table.report.problems == []
        assert table.report.blank_lines_skipped == 0


def test_field_values_preserved_verbatim(tmp_path) -> None:
    path = write_csv(
        tmp_path,
        "stops.txt",
        'stop_id,stop_name\n14023, Gateway / NE 122nd Ave \n',
    )
    with open_gtfs_csv(path) as table:
        records = list(table.iter_records())
        assert records[0][1]["stop_name"] == " Gateway / NE 122nd Ave "


def test_unicode_values_survive(tmp_path) -> None:
    path = write_csv(
        tmp_path,
        "stops.txt",
        'stop_id,stop_name\n14023,Süd–Grand Ave / Longfellow Ø\n',
    )
    with open_gtfs_csv(path) as table:
        records = list(table.iter_records())
        assert records[0][1]["stop_name"] == "Süd–Grand Ave / Longfellow Ø"


def test_utf8_bom_is_handled(tmp_path) -> None:
    path = write_csv(
        tmp_path,
        "stops.txt",
        "stop_id,stop_name\n14023,Grand Ave\n",
        bom=True,
    )
    with open_gtfs_csv(path, required_columns=("stop_id",)) as table:
        assert table.header.columns[0] == "stop_id"  # no \ufeff prefix
        assert not any(col.startswith("\ufeff") for col in table.header.columns)
        records = list(table.iter_records())
        assert records[0][1]["stop_name"] == "Grand Ave"


def test_multiline_quoted_field(tmp_path) -> None:
    path = write_csv(
        tmp_path,
        "stops.txt",
        'stop_id,stop_name\n14023,"Gateway\nPlatform 2"\n',
    )
    with open_gtfs_csv(path) as table:
        records = list(table.iter_records())
        assert records[0][1]["stop_name"] == "Gateway\nPlatform 2"
        # Line numbers are the last physical line of the record.
        assert [line for line, _ in records] == [3]


# ---------------------------------------------------------------------------
# Structural failures
# ---------------------------------------------------------------------------

def test_empty_file_raises(tmp_path) -> None:
    path = write_csv(tmp_path, "empty.txt", "")
    with pytest.raises(SchemaViolationError, match="no header row"):
        with open_gtfs_csv(path):
            pass


def test_blank_header_row_raises(tmp_path) -> None:
    path = write_csv(tmp_path, "odd.txt", "\nstop_id\n14023\n")
    with pytest.raises(SchemaViolationError, match="header row is blank"):
        with open_gtfs_csv(path):
            pass


def test_header_only_file_is_valid_but_empty(tmp_path) -> None:
    path = write_csv(tmp_path, "routes.txt", "route_id,route_short_name\n")
    with open_gtfs_csv(path, required_columns=("route_id",)) as table:
        assert list(table.iter_records()) == []
        assert table.report.problems == []


def test_missing_required_column_raises(tmp_path) -> None:
    path = write_csv(tmp_path, "routes.txt", "route_id\n90\n")
    with pytest.raises(SchemaViolationError, match="missing required column") as exc_info:
        with open_gtfs_csv(path, required_columns=("route_id", "route_type")):
            pass
    assert "route_type" in str(exc_info.value)


def test_individual_empty_header_column_names_are_rejected(tmp_path) -> None:
    """Review fix 3B: "stop_id,,stop_name" is broken input, not a spare slot."""
    path = write_csv(tmp_path, "weird-header.txt", "stop_id,,stop_name\n14023,x,A\n")
    with pytest.raises(SchemaViolationError, match="empty header column") as exc_info:
        with open_gtfs_csv(path):
            pass
    assert "2" in str(exc_info.value)  # names the 1-based position


def test_duplicate_header_column_raises(tmp_path) -> None:
    path = write_csv(tmp_path, "routes.txt", "route_id,route_id,route_color\n90,90,C41F3E\n")
    with pytest.raises(SchemaViolationError, match="duplicate header column"):
        with open_gtfs_csv(path):
            pass


def test_invalid_utf8_raises_schema_violation(tmp_path) -> None:
    path = tmp_path / "bad-early.txt"
    path.write_bytes(b"stop_id,stop_name\n1,\xff\xfe\n")
    with pytest.raises(SchemaViolationError, match="UTF-8"):
        with open_gtfs_csv(path) as table:
            list(table.iter_records())


def test_invalid_utf8_late_in_file_raises_schema_violation(tmp_path) -> None:
    """The invalid byte lands beyond the first buffered chunk, so the decode
    failure surfaces during record iteration (same wrapping either way)."""
    filler = ("1," + "x" * 40 + "\n") * 210
    path = tmp_path / "bad-late.txt"
    # The invalid byte must be a raw 0xFF byte in the file, not the character
    # U+00FF encoded as text (which would be the valid UTF-8 pair c3 bf).
    path.write_bytes(
        b"stop_id,stop_name\n" + filler.encode("utf-8") + b"1002,\xff\n"
    )
    with pytest.raises(SchemaViolationError, match="UTF-8"):
        with open_gtfs_csv(path) as table:
            list(table.iter_records())


def test_unterminated_quote_raises_schema_violation(tmp_path) -> None:
    path = write_csv(tmp_path, "broken.txt", 'a,b\n"unterminated value\n')
    with pytest.raises(SchemaViolationError, match="CSV structure"):
        with open_gtfs_csv(path) as table:
            list(table.iter_records())


# ---------------------------------------------------------------------------
# Row problems: recorded, never silently discarded
# ---------------------------------------------------------------------------

def test_short_row_yielded_and_recorded(tmp_path) -> None:
    path = write_csv(tmp_path, "odd-rows.txt", "a,b,c\n1,2\n3,4,5\n")
    with open_gtfs_csv(path) as table:
        records = list(table.iter_records())
        assert records[0][1] == {"a": "1", "b": "2"}
        assert "c" not in records[0][1]
        assert records[1][1] == {"a": "3", "b": "4", "c": "5"}
        problems = table.report.problems
        assert len(problems) == 1
        assert problems[0].line_number == 2
        assert problems[0].kind is CsvRowProblemKind.SHORT_ROW
        assert "found 2" in problems[0].detail
        assert table.report.counts_by_kind() == {CsvRowProblemKind.SHORT_ROW: 1}


def test_extra_fields_yielded_and_recorded(tmp_path) -> None:
    path = write_csv(tmp_path, "wide-rows.txt", "a,b\n1,2\n3,4,5\n")
    with open_gtfs_csv(path) as table:
        records = list(table.iter_records())
        assert records[1][1] == {"a": "3", "b": "4"}  # the "5" is not mapped
        problems = table.report.problems
        assert len(problems) == 1
        assert problems[0].line_number == 3
        assert problems[0].kind is CsvRowProblemKind.EXTRA_FIELDS
        assert "found 3" in problems[0].detail
        assert "'5'" in problems[0].detail  # raw preview keeps the evidence


def test_interior_blank_line_skipped_but_counted(tmp_path) -> None:
    path = write_csv(tmp_path, "blanks.txt", "a\n1\n\n2\n")
    with open_gtfs_csv(path) as table:
        records = list(table.iter_records())
        assert [line for line, _ in records] == [2, 4]
        assert [values["a"] for _, values in records] == ["1", "2"]
        assert table.report.blank_lines_skipped == 1
        assert table.report.problems == []


def test_trailing_newline_is_not_a_blank_row(tmp_path) -> None:
    path = write_csv(tmp_path, "scoped.txt", "a\n1\n")
    with open_gtfs_csv(path) as table:
        records = list(table.iter_records())
        assert len(records) == 1
        assert table.report.blank_lines_skipped == 0


# ---------------------------------------------------------------------------
# File-handle cleanup on success and on failure (Windows-specific: an open
# handle makes os.replace raise PermissionError).
# ---------------------------------------------------------------------------

def test_file_handle_released_after_success(tmp_path) -> None:
    path = write_csv(tmp_path, "routes.txt", "route_id\n90\n")
    with open_gtfs_csv(path) as table:
        list(table.iter_records())
    os.replace(path, path.with_suffix(".renamed"))  # raises if still open
    assert path.with_suffix(".renamed").exists()


def test_file_handle_released_after_failure(tmp_path) -> None:
    path = write_csv(tmp_path, "broken.txt", 'a,b\n"unterminated value\n')
    with pytest.raises(SchemaViolationError):
        with open_gtfs_csv(path) as table:
            list(table.iter_records())
    os.replace(path, path.with_suffix(".renamed"))  # raises if still open
    assert path.with_suffix(".renamed").exists()


@pytest.mark.parametrize(
    "build",
    [
        ("missing_required", True),
        ("invalid_utf8_header", False),
    ],
    ids=["validation", "decode"],
)
def test_file_handle_released_after_header_stage_failure(
    tmp_path, build: tuple[str, bool]
) -> None:
    """Review fix 3C: header-stage failures (validation and decode) must also
    release the handle."""
    kind, needs_columns = build
    path = tmp_path / "header-fail.txt"
    if kind == "invalid_utf8_header":
        path.write_bytes(b"\xff\xfe no utf8 header here\n")
    else:
        path.write_text("route_id\n90\n", encoding="utf-8")
    kwargs = {"required_columns": ("route_id", "route_type")} if needs_columns else {}
    with pytest.raises(SchemaViolationError):
        with open_gtfs_csv(path, **kwargs):
            pass
    os.replace(path, path.with_suffix(".renamed"))  # raises if still open
    assert path.with_suffix(".renamed").exists()


def test_error_context_never_exposes_absolute_paths(tmp_path) -> None:
    """Review fix 3D: errors carry the file name, never machine paths."""
    path = write_csv(tmp_path, "routes.txt", "")  # empty -> header failure
    with pytest.raises(SchemaViolationError) as exc_info:
        with open_gtfs_csv(path, required_columns=("route_id",)):
            pass
    text = str(exc_info.value)
    assert str(tmp_path) not in text
    assert "\\" not in text and "/" not in text  # no path separators anywhere


# ---------------------------------------------------------------------------
# Typed getters
# ---------------------------------------------------------------------------

def test_get_str_absent_and_blank() -> None:
    assert get_str({}, "x") is None
    assert get_str({"x": ""}, "x") is None
    assert get_str({"x": "  padded  "}, "x") == "  padded  "
    assert get_str({"x": "90"}, "x") == "90"


def test_get_int_convert_missing_and_rejects() -> None:
    assert get_int({"n": "3"}, "n") == 3
    assert get_int({"n": " 3 "}, "n") == 3
    assert get_int({"n": "0"}, "n") == 0
    assert get_int({"n": " "}, "n") is None  # whitespace-only == blank
    assert get_int({}, "n") is None  # absent column
    with pytest.raises(CellParseError, match="integer"):
        get_int({"n": "abc"}, "n")
    with pytest.raises(CellParseError, match="column") as exc_info:
        get_int({"n": "1.5"}, "n")
    assert exc_info.value.column == "n"


def test_get_float_convert_missing_and_rejects_non_finite() -> None:
    assert get_float({"f": "1.5"}, "f") == 1.5
    assert get_float({"f": "-2.25"}, "f") == -2.25
    assert get_float({"f": ""}, "f") is None
    with pytest.raises(CellParseError) as nan_info:
        get_float({"f": "nan"}, "f")
    assert nan_info.value.column == "f"
    with pytest.raises(CellParseError):
        get_float({"f": "inf"}, "f")
    with pytest.raises(CellParseError):
        get_float({"f": "-inf"}, "f")
    with pytest.raises(CellParseError):
        get_float({"f": "Infinity"}, "f")
    with pytest.raises(CellParseError):
        get_float({"f": "abc"}, "f")


def test_get_time_secs_hours_beyond_23(tmp_path) -> None:
    assert get_time_secs({"t": "00:00:00"}, "t") == 0
    assert get_time_secs({"t": "25:10:30"}, "t") == 25 * 3600 + 10 * 60 + 30
    assert get_time_secs({"t": "24:00:00"}, "t") == 86_400
    assert get_time_secs({"t": " 01:02:03 "}, "t") == 3723
    assert get_time_secs({"t": ""}, "t") is None
    assert get_time_secs({}, "t") is None


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("12:60:00", "out of range"),
        ("01:02:60", "out of range"),
        ("1:2:3", "HH:MM:SS"),
        ("00:00", "HH:MM:SS"),
        ("abc", "HH:MM:SS"),
        ("-1:00:00", "HH:MM:SS"),
    ],
)
def test_get_time_secs_rejects_malformed(raw: str, reason: str) -> None:
    with pytest.raises(CellParseError, match=reason or "HH:MM:SS"):
        get_time_secs({"t": raw}, "t")


def test_getters_accept_optional_file_and_line_context() -> None:
    """Review fix 3A: context fields exist in the hierarchy, so getters let
    callers attach the row they were converting."""
    with pytest.raises(CellParseError) as int_info:
        get_int({"n": "abc"}, "n", file="stop_times.txt", line_number=7)
    text = str(int_info.value)
    assert "stop_times.txt" in text
    assert "line 7" in text
    assert "(raw: 'abc')" in text  # bounded cell content, rendered once

    with pytest.raises(CellParseError) as time_info:
        get_time_secs({"t": "25:70:00"}, "t", file="stop_times.txt", line_number=8)
    assert "line 8" in str(time_info.value)
    assert time_info.value.column == "t"
