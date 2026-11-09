"""S11 index tests: complete full-table index, lineage and failure cases.

Plan §13 rows 2–4 (lineage, identity, full index) mapped to named tests:

- successful ``LoadOutcome`` supplies the paired network/directory and no
  loader call or bare guessed pair is involved;
- missing/malformed manifests, wrong digests/feed versions/paths, source
  change/disappearance during preparation and inconsistent MAX tables fail
  whole preparation;
- the exact CSV rules (widths, extra columns, trailing empties, quoted
  fields, blank direction/shape semantics, bad UTF-8, missing/duplicate
  members) are enforced on a new valid version without hardcoding a
  checksum.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from backend.trimet.dataset import DatasetManager
from backend.trimet.match_errors import ContextPreparationError
from backend.trimet.match_index import build_matching_context, prepare_matching_context
from backend.trimet.match_types import RouteRow, TripRow
from tests._s11_helpers import (
    SyntheticWorld,
    install_socket_guard,
    promoted_world_outcome,
    routes_csv_text,
    synthetic_identity,
    synthetic_world,
    trips_csv_text,
)


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    install_socket_guard(monkeypatch)


@pytest.fixture()
def promoted(tmp_path: Path):
    return promoted_world_outcome(tmp_path)


def _rewrite_manifest(dataset_dir: Path, **changes) -> None:
    payload = json.loads((dataset_dir / "manifest.json").read_text("utf-8"))
    payload.update(changes)
    (dataset_dir / "manifest.json").write_text(
        json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8"
    )


def _rewrite_archive(bundle, **member_texts: str) -> None:
    """Rewrite the dataset zip with replacement member texts + fix manifest."""
    world: SyntheticWorld = bundle.world
    members = {
        "routes.txt": world.route_rows,
        "trips.txt": world.trip_rows,
    }
    texts = {
        name: routes_csv_text(members[name]) if name == "routes.txt" else trips_csv_text(members[name])
        for name in members
    }
    texts.update(member_texts)
    zip_path = bundle.dataset_dir / "gtfs.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as archive:
        for name, text in texts.items():
            archive.writestr(name, text.encode("utf-8"))
    _rewrite_manifest(
        bundle.dataset_dir,
        sha256=hashlib.sha256(zip_path.read_bytes()).hexdigest(),
        zip_size_bytes=zip_path.stat().st_size,
    )


# ---------------------------------------------------------------------------
# Builder failure matrix (synthetic path; the shared validation)
# ---------------------------------------------------------------------------


def test_duplicate_route_id_fails_before_dictionary_conversion() -> None:
    world = synthetic_world()
    route_rows = list(world.route_rows)
    route_rows.insert(0, RouteRow("90", 0))
    with pytest.raises(ContextPreparationError, match="duplicate") as exc:
        build_matching_context(
            synthetic_identity(), world.network, route_rows, list(world.trip_rows)
        )
    assert exc.value.code == "index_inconsistency"


def test_duplicate_trip_id_fails() -> None:
    world = synthetic_world()
    trip_rows = list(world.trip_rows)
    trip_rows.append(TripRow("t90A", "90"))
    with pytest.raises(ContextPreparationError, match="duplicate") as exc:
        build_matching_context(
            synthetic_identity(), world.network, list(world.route_rows), trip_rows
        )
    assert exc.value.code == "index_inconsistency"


@pytest.mark.parametrize(
    "bad_row",
    [
        RouteRow("", 0),            # blank id
        RouteRow("rt", True),       # boolean mode
        RouteRow("rt", -1),         # negative mode
        RouteRow("rt", "0"),        # non-int mode
    ],
)
def test_invalid_route_entries_fail_whole(bad_row: RouteRow) -> None:
    world = synthetic_world()
    route_rows = list(world.route_rows) + [bad_row]
    with pytest.raises(ContextPreparationError):
        build_matching_context(
            synthetic_identity(), world.network, route_rows, list(world.trip_rows)
        )


def test_whitespace_only_ids_are_preserved_verbatim() -> None:
    # GTFS cell values are never normalized: a whitespace-only identifier is
    # a legal (if unusual) id string, exactly like csvio's get_str keeps it.
    world = synthetic_world()
    route_rows = list(world.route_rows) + [RouteRow("   ", 3)]
    trip_rows = list(world.trip_rows) + [TripRow(" ", "   ")]
    context = build_matching_context(
        synthetic_identity(), world.network, route_rows, trip_rows
    )
    assert context.trip_to_route[" "] == "   "
    assert context.route_types["   "] == 3


def test_blank_trip_id_fails() -> None:
    world = synthetic_world()
    trip_rows = list(world.trip_rows) + [TripRow("", "90")]
    with pytest.raises(ContextPreparationError, match="blank"):
        build_matching_context(
            synthetic_identity(), world.network, list(world.route_rows), trip_rows
        )


def test_orphan_route_reference_fails_whole() -> None:
    world = synthetic_world()
    trip_rows = list(world.trip_rows) + [TripRow("tnx", "555-not-a-route")]
    with pytest.raises(ContextPreparationError, match="orphan"):
        build_matching_context(
            synthetic_identity(), world.network, list(world.route_rows), trip_rows
        )


def test_missing_max_trip_fails_whole_preparation() -> None:
    world = synthetic_world()
    trip_rows = [row for row in world.trip_rows if row.trip_id != "t90B"]
    with pytest.raises(ContextPreparationError, match="MAX trip"):
        build_matching_context(
            synthetic_identity(), world.network, list(world.route_rows), trip_rows
        )


def test_extra_max_trip_in_rows_fails() -> None:
    world = synthetic_world()
    trip_rows = list(world.trip_rows) + [TripRow("t90Z", "90")]
    with pytest.raises(ContextPreparationError, match="MAX trip"):
        build_matching_context(
            synthetic_identity(), world.network, list(world.route_rows), trip_rows
        )


def test_max_route_set_disagreement_fails() -> None:
    world = synthetic_world()
    route_rows = [row for row in world.route_rows if row.route_id != "290"]
    with pytest.raises(ContextPreparationError, match="MAX route set"):
        build_matching_context(
            synthetic_identity(), world.network, route_rows, list(world.trip_rows)
        )


def test_direction_and_shape_disagreement_fails() -> None:
    world = synthetic_world()

    def rewrite(**changes):
        return [
            TripRow(
                row.trip_id,
                row.route_id,
                changes.get("direction_id", row.direction_id),
                changes.get("shape_id", row.shape_id),
            )
            for row in world.trip_rows
        ]

    with pytest.raises(ContextPreparationError, match="direction_id"):
        build_matching_context(
            synthetic_identity(),
            world.network,
            list(world.route_rows),
            rewrite(direction_id=7),
        )
    with pytest.raises(ContextPreparationError, match="shape_id"):
        build_matching_context(
            synthetic_identity(),
            world.network,
            list(world.route_rows),
            rewrite(shape_id="sh-other"),
        )


def test_duplicate_stop_sequence_in_network_fails_whole() -> None:
    world = synthetic_world()
    stop_times = dict(world.network.stop_times)
    stop_times["t90A"] = stop_times["t90A"] + (stop_times["t90A"][0],)
    network = dataclasses.replace(world.network, stop_times=stop_times)
    with pytest.raises(ContextPreparationError, match="duplicate stop_sequence"):
        build_matching_context(
            synthetic_identity(), network, list(world.route_rows), list(world.trip_rows)
        )


def test_non_sequence_input_rejected_as_programmer_error() -> None:
    world = synthetic_world()
    with pytest.raises(TypeError):
        build_matching_context(synthetic_identity(), world.network, ["nope"], [])
    with pytest.raises(TypeError):
        build_matching_context(
            synthetic_identity(),
            world.network,
            list(world.route_rows),
            ["nope"],
        )


# ---------------------------------------------------------------------------
# Factory: successful lineage
# ---------------------------------------------------------------------------


def test_successful_load_outcome_prepares_full_context(promoted) -> None:
    context = prepare_matching_context(promoted.outcome)
    assert context.identity.feed_version == "test-feed-001"
    assert context.identity.archive_sha256 == promoted.digest
    assert context.identity.source_directory_name == promoted.dataset_dir.name
    # Full all-trip lookup distinguishes known MAX/non-MAX/unknown.
    assert context.trip_to_route["t90A"] == "90"
    assert context.trip_to_route["tb1"] == "1"
    assert context.trip_to_route["t193"] == "193"
    assert context.route_types["1"] == 3
    assert context.route_types["193"] == 0
    # MAX lookups from M1 are preserved verbatim.
    assert context.visits_by_sequence["t90A"][1].stop_id == "14000"
    assert context.max_route_ids == frozenset({"90", "100", "190", "200", "290"})


def test_preparation_does_not_call_the_download_loader(promoted, monkeypatch) -> None:
    def _no_load(self):  # pragma: no cover - must never run
        raise AssertionError("DatasetManager.load must never be called by S11")

    monkeypatch.setattr(DatasetManager, "load", _no_load)
    prepare_matching_context(promoted.outcome)


def test_bare_mismatched_network_directory_pair_fails_cross_checks(
    promoted, tmp_path
) -> None:
    # No public "bare pair" entry point exists: the only factory always
    # re-verifies lineage.  Pairing the network with another version's
    # directory fails the identity cross-check.
    other = promoted_world_outcome(tmp_path / "second", feed_version="test-feed-002")
    swapped = dataclasses.replace(
        other.outcome,
        network=promoted.world.network,
    )
    with pytest.raises(ContextPreparationError):
        prepare_matching_context(swapped)


# ---------------------------------------------------------------------------
# Factory: identity defects
# ---------------------------------------------------------------------------


def test_missing_manifest_fails_whole(promoted) -> None:
    (promoted.dataset_dir / "manifest.json").unlink()
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(promoted.outcome)
    assert exc.value.code == "manifest_absent"


def test_malformed_manifest_fails_whole(promoted) -> None:
    (promoted.dataset_dir / "manifest.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(promoted.outcome)
    assert exc.value.code == "manifest_unreadable"


def test_manifest_paths_mismatch_fails_whole(promoted, tmp_path) -> None:
    _rewrite_manifest(promoted.dataset_dir, zip_path=str(tmp_path / "elsewhere.zip"))
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(promoted.outcome)
    assert exc.value.code == "manifest_paths_mismatch"


def test_wrong_digest_in_manifest_fails_whole(promoted) -> None:
    _rewrite_manifest(promoted.dataset_dir, sha256="cd" * 32)
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(promoted.outcome)
    assert exc.value.code == "archive_hash_mismatch"


def test_feed_version_mismatch_with_network_fails_whole(promoted) -> None:
    import json as _json

    _rewrite_manifest(promoted.dataset_dir, feed_version="different")
    assert _json.loads(
        (promoted.dataset_dir / "manifest.json").read_text("utf-8")
    )["feed_version"] == "different"
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(promoted.outcome)
    assert exc.value.code == "identity_mismatch"


def test_missing_and_duplicate_zip_members_fail(tmp_path) -> None:
    missing = promoted_world_outcome(tmp_path / "m1", omit_members=("trips.txt",))
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(missing.outcome)
    assert exc.value.code == "source_member_missing"
    duplicated = promoted_world_outcome(
        tmp_path / "m2", duplicate_members=("routes.txt",)
    )
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(duplicated.outcome)
    assert exc.value.code == "source_member_ambiguous"


def test_bad_utf8_member_fails_whole(tmp_path) -> None:
    bundle = promoted_world_outcome(
        tmp_path,
        raw_members={"routes.txt": b"route_id,route_type\n90,\xff\xfe0\n"},
    )
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(bundle.outcome)
    assert exc.value.code == "csv_structure"


def test_source_change_during_preparation_fails(promoted, monkeypatch) -> None:
    calls = {"n": 0}
    from backend.trimet import match_index

    real_hash = match_index._zip_sha256

    def hash_that_diverges_second_time(path):
        calls["n"] += 1
        if calls["n"] == 1:
            return real_hash(path)
        return "ef" * 32

    monkeypatch.setattr(match_index, "_zip_sha256", hash_that_diverges_second_time)
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(promoted.outcome)
    assert exc.value.code == "archive_hash_mismatch"


def test_source_disappears_during_preparation_fails(promoted, monkeypatch) -> None:
    from backend.trimet import match_index

    real_hash = match_index._zip_sha256
    zip_path = promoted.dataset_dir / "gtfs.zip"
    calls = {"n": 0}

    def hash_then_disappearing(path):
        calls["n"] += 1
        if calls["n"] == 1:
            value = real_hash(path)
            zip_path.unlink()
            return value
        raise FileNotFoundError(zip_path.name)

    monkeypatch.setattr(match_index, "_zip_sha256", hash_then_disappearing)
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(promoted.outcome)
    assert exc.value.code == "archive_unreadable"


# ---------------------------------------------------------------------------
# Factory: exact CSV rules
# ---------------------------------------------------------------------------


def test_new_valid_version_accepted_without_hardcoded_checksum(tmp_path) -> None:
    bundle = promoted_world_outcome(tmp_path, feed_version="next-version")
    context = prepare_matching_context(bundle.outcome)
    assert context.identity.feed_version == "next-version"
    assert context.identity.archive_sha256 == bundle.digest


def test_extra_named_header_column_accepted(promoted) -> None:
    first = prepare_matching_context(promoted.outcome)
    extra_text = routes_csv_text(promoted.world.route_rows, extra_column=True)
    _rewrite_archive(promoted, routes_txt=extra_text)
    second = prepare_matching_context(promoted.outcome)
    assert second.trip_to_route == first.trip_to_route
    assert second.route_types == first.route_types


def test_short_and_long_rows_fail_whole(tmp_path) -> None:
    complete = trips_csv_text(synthetic_world().trip_rows)
    short = complete.replace("t90A,90,0,sh90\n", "t90A,90,0\n")
    long = complete.replace("t90A,90,0,sh90\n", "t90A,90,0,sh90,\n")
    for label, text in (("short", short), ("long", long)):
        bundle = promoted_world_outcome(tmp_path / label, trip_text=text)
        with pytest.raises(ContextPreparationError) as exc:
            prepare_matching_context(bundle.outcome)
        assert exc.value.code == "csv_structure", label


def test_trailing_extra_empty_cells_fail_whole(tmp_path) -> None:
    complete = trips_csv_text(synthetic_world().trip_rows)
    sabotaged = complete.replace(
        "t90B,90,,\n", "t90B,90,,,,\n"  # two extra trailing empties
    )
    assert sabotaged != complete
    bundle = promoted_world_outcome(tmp_path, trip_text=sabotaged)
    with pytest.raises(ContextPreparationError):
        prepare_matching_context(bundle.outcome)


def test_correct_position_empty_cells_and_quoted_fields_accepted(tmp_path) -> None:
    complete = trips_csv_text(synthetic_world().trip_rows)
    bundle = promoted_world_outcome(tmp_path, trip_text=complete)
    context = prepare_matching_context(bundle.outcome)
    assert context.trip_to_route["t90B"] == "90"  # explicit empty cells


def test_blank_records_skipped(tmp_path) -> None:
    complete = trips_csv_text(synthetic_world().trip_rows)
    padded = complete + "\n\n"  # trailing blank physical records
    bundle = promoted_world_outcome(tmp_path, trip_text=padded)
    context = prepare_matching_context(bundle.outcome)
    assert len(context.trip_to_route) == len(synthetic_world().trip_rows)


def test_duplicate_header_columns_fail_whole(tmp_path) -> None:
    bundle = promoted_world_outcome(
        tmp_path,
        trip_text="trip_id,route_id,route_id,shape_id\nt90A,90,90,sh90\n",
    )
    with pytest.raises(ContextPreparationError, match="duplicate"):
        prepare_matching_context(bundle.outcome)


def test_missing_required_columns_fail_whole(tmp_path) -> None:
    routes_only_id = "route_id\n90\n"
    bundle = promoted_world_outcome(tmp_path / "r", route_text=routes_only_id)
    with pytest.raises(ContextPreparationError, match="missing required"):
        prepare_matching_context(bundle.outcome)
    trips_without_shape = "trip_id,route_id,direction_id\ntz9,100,0\n"
    bundle = promoted_world_outcome(tmp_path / "t", trip_text=trips_without_shape)
    with pytest.raises(ContextPreparationError, match="missing required"):
        prepare_matching_context(bundle.outcome)


def test_max_row_direction_whitespace_maps_to_none(tmp_path) -> None:
    world = synthetic_world()
    text = trips_csv_text(world.trip_rows)
    # t90B is the blank direction/shape MAX row; spaces still map to None.
    spacey = text.replace("t90B,90,,\n", "t90B,90,   ,\n")
    assert spacey != text
    bundle = promoted_world_outcome(tmp_path, trip_text=spacey)
    context = prepare_matching_context(bundle.outcome)
    assert context.network.trips["t90B"].direction_id is None


def test_nonempty_shape_cell_stays_verbatim(tmp_path) -> None:
    world = synthetic_world()
    text = trips_csv_text(world.trip_rows)
    verbatim = text.replace("t90B,90,,\n", "t90B,90,, \n")  # whitespace-only shape
    assert verbatim != text
    bundle = promoted_world_outcome(tmp_path, trip_text=verbatim)
    with pytest.raises(ContextPreparationError, match="shape_id"):
        # A nonempty (whitespace-only) shape string is kept verbatim, so it
        # now disagrees with the network's blank (None) shape value.
        prepare_matching_context(bundle.outcome)


def test_malformed_nonempty_max_direction_fails_cell(tmp_path) -> None:
    text = trips_csv_text(synthetic_world().trip_rows)
    broken = text.replace("t90B,90,,\n", "t90B,90,east,\n")
    assert broken != text
    bundle = promoted_world_outcome(tmp_path, trip_text=broken)
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(bundle.outcome)
    assert exc.value.code == "invalid_cell"


def test_non_max_unused_cells_not_converted(tmp_path) -> None:
    text = trips_csv_text(synthetic_world().trip_rows)
    junk = text + "tz99,555,not-an-int,broken-shape-<>\n"
    routes = routes_csv_text(
        list(synthetic_world().route_rows) + [RouteRow("555", 3)]
    )
    bundle = promoted_world_outcome(tmp_path, trip_text=junk, route_text=routes)
    context = prepare_matching_context(bundle.outcome)
    # The non-MAX row's junk direction/shape never entered any lookup; the
    # two canonical ids still classify the trip.
    assert context.trip_to_route["tz99"] == "555"
    assert context.route_types["555"] == 3


def test_noninteger_route_type_fails_whole(tmp_path) -> None:
    bundle = promoted_world_outcome(
        tmp_path, route_text="route_id,route_type\n90,zero\n"
    )
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(bundle.outcome)
    assert exc.value.code == "invalid_cell"


def test_blank_required_route_type_fails_whole(tmp_path) -> None:
    bundle = promoted_world_outcome(
        tmp_path, route_text="route_id,route_type\n90,\n"
    )
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(bundle.outcome)
    assert exc.value.code == "index_inconsistency"


# ---------------------------------------------------------------------------
# M1-tolerated visitless MAX trips (owner review Finding 1, 2026-11-08)
# ---------------------------------------------------------------------------


def test_visitless_max_trip_prepares_with_empty_indexes() -> None:
    """A known MAX trip may have no stop-time rows (M1 ``by_trip`` rule).

    The context must prepare, carry empty trip-local indexes for that trip,
    and leave every other trip's indexes untouched.
    """
    world = synthetic_world()
    stop_times = {
        trip_id: entries
        for trip_id, entries in world.network.stop_times.items()
        if trip_id != "t90A"
    }
    network = dataclasses.replace(world.network, stop_times=stop_times)
    context = build_matching_context(
        synthetic_identity(), network, world.route_rows, world.trip_rows
    )
    assert dict(context.visits_by_sequence["t90A"]) == {}
    assert dict(context.visits_by_stop["t90A"]) == {}
    # Unaffected trips keep their real visits.
    assert context.visits_by_sequence["t90D"][1].stop_id == "14000"
    assert set(context.visits_by_sequence) == set(network.trips)


def test_factory_accepts_network_with_visitless_max_trip(promoted) -> None:
    """The production factory must not reject an M1-tolerated visitless trip."""
    stop_times = {
        trip_id: entries
        for trip_id, entries in promoted.world.network.stop_times.items()
        if trip_id != "t90A"
    }
    visitless_network = dataclasses.replace(
        promoted.world.network, stop_times=stop_times
    )
    outcome = dataclasses.replace(promoted.outcome, network=visitless_network)
    context = prepare_matching_context(outcome)
    assert dict(context.visits_by_sequence["t90A"]) == {}
    assert context.trip_to_route["t90A"] == "90"


# ---------------------------------------------------------------------------
# CSV quoting errors escape as the typed contract (owner review F2)
# ---------------------------------------------------------------------------


def test_unterminated_quoted_field_fails_whole(tmp_path) -> None:
    """An unterminated quoted field raises _csv.Error, not ValueError; it
    must still honor the typed ``csv_structure`` preparation contract."""
    trips = (
        "trip_id,route_id,direction_id,shape_id\n"
        't90A,90,0,"sh90\n'  # quote never closes
        "tb1,1,,\n"
    )
    bundle = promoted_world_outcome(tmp_path, trip_text=trips)
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(bundle.outcome)
    assert exc.value.code == "csv_structure"
    assert isinstance(exc.value.__cause__, __import__("csv").Error)


def test_data_after_closing_quote_fails_whole(tmp_path) -> None:
    trips = (
        "trip_id,route_id,direction_id,shape_id\n"
        't90A,90,0,"sh90"junk\n'  # data immediately after a closing quote
        "tb1,1,,\n"
    )
    bundle = promoted_world_outcome(tmp_path, trip_text=trips)
    with pytest.raises(ContextPreparationError) as exc:
        prepare_matching_context(bundle.outcome)
    assert exc.value.code == "csv_structure"
    assert isinstance(exc.value.__cause__, __import__("csv").Error)
