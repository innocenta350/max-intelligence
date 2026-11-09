"""S11 contract tests: frozen vocabulary, identity, immutability (plan §13 row 1).

Covers the frozen 17-spelling reason vocabulary, :class:`DatasetIdentity`
validation, deep read-only behavior of :class:`MatchingContext` (caller
mutation of the construction inputs cannot affect the context), and the
synthetic/production contexts sharing one builder (malformed rows fail in
the same path for both).
"""

from __future__ import annotations

import dataclasses

import pytest

from backend.trimet.match_errors import (
    INPUT_ERROR_CODES,
    PREPARATION_ERROR_CODES,
    ContextPreparationError,
    MatchInputError,
    MatchReason,
    MatchingError,
)
from backend.trimet.match_index import build_matching_context
from backend.trimet.match_types import (
    DatasetIdentity,
    EnrichmentState,
    RelevanceClassification,
    RouteRow,
    TripRow,
)
from tests._s11_helpers import install_socket_guard, synthetic_identity, synthetic_world


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    install_socket_guard(monkeypatch)


# ---------------------------------------------------------------------------
# Frozen reason vocabulary (prompt §6: exact spelling pinned before code)
# ---------------------------------------------------------------------------


def test_match_reason_has_exactly_the_seventeen_frozen_spellings() -> None:
    expected = [
        "unknown_route",
        "unknown_vehicle_type",
        "type_route_conflict",
        "trip_not_reported",
        "unknown_trip",
        "trip_route_conflict",
        "direction_conflict",
        "missing_shape",
        "unusable_shape",
        "missing_stop_details",
        "unknown_stop_sequence",
        "unknown_stop_id",
        "stop_id_sequence_conflict",
        "ambiguous_stop_visit",
        "missing_stop_selectors",
        "unsupported_operational_relationship",
        "diagnostics_overflow",
    ]
    assert [reason.value for reason in MatchReason] == expected
    assert len({reason.value for reason in MatchReason}) == 17


def test_error_code_vocabularies_are_small_and_fixed() -> None:
    assert len(PREPARATION_ERROR_CODES) == 11
    assert set(INPUT_ERROR_CODES) == {
        "delivery_type_mismatch",
        "feed_model_mismatch",
        "invalid_context",
    }


def test_exception_codes_are_vocabulary_checked() -> None:
    exc = ContextPreparationError("x", code="archive_hash_mismatch")
    assert exc.code == "archive_hash_mismatch"
    assert isinstance(exc, MatchingError)
    with pytest.raises(ValueError):
        MatchInputError("x", code="manifest_absent")  # cross-vocabulary misuse


def test_classification_and_enrichment_enums() -> None:
    assert [c.value for c in RelevanceClassification] == [
        "max_relevant",
        "non_max",
        "unclassified",
    ]
    assert [e.value for e in EnrichmentState] == [
        "matched",
        "partial",
        "unmatched",
        "conflict",
    ]


# ---------------------------------------------------------------------------
# DatasetIdentity
# ---------------------------------------------------------------------------


def test_dataset_identity_holds_complete_digest_and_version() -> None:
    identity = synthetic_identity("20261011-20261021-0903")
    assert identity.archive_sha256 == "ab" * 32
    assert identity.feed_version == "20261011-20261021-0903"
    assert identity.source_directory_name is None


@pytest.mark.parametrize(
    "bad_sha",
    ["", "ab" * 31 + "g", "AB" * 32, "ab" * 33],
)
def test_dataset_identity_rejects_non_canonical_digests(bad_sha: str) -> None:
    with pytest.raises(ValueError):
        synthetic_identity(sha256=bad_sha)


def test_dataset_identity_rejects_blank_feed_version() -> None:
    with pytest.raises(ValueError):
        synthetic_identity(feed_version="")


def test_dataset_identity_rejects_junk_directory_name() -> None:
    with pytest.raises(ValueError):
        synthetic_identity(source_directory_name="")


@pytest.mark.parametrize(
    "record_type", [DatasetIdentity, RelevanceClassification, EnrichmentState]
)
def test_identity_like_records_are_frozen(record_type) -> None:
    if record_type is DatasetIdentity:
        instance = synthetic_identity()
        with pytest.raises(dataclasses.FrozenInstanceError):
            instance.feed_version = "changed"
    else:
        instance = next(iter(record_type))
        with pytest.raises(AttributeError):
            instance.name = "changed"


# ---------------------------------------------------------------------------
# MatchingContext deep immutability through the shared builder
# ---------------------------------------------------------------------------


def _built_context():
    world = synthetic_world()
    return build_matching_context(
        synthetic_identity(), world.network, list(world.route_rows), list(world.trip_rows)
    )


def test_caller_list_mutation_cannot_affect_context() -> None:
    world = synthetic_world()
    route_rows = list(world.route_rows)
    trip_rows = list(world.trip_rows)
    context = build_matching_context(synthetic_identity(), world.network, route_rows, trip_rows)
    route_rows.append(RouteRow("999-example", 3))
    trip_rows.append(TripRow("tz", "999-example"))
    assert context.known_route_ids == frozenset(
        row.route_id for row in world.route_rows
    )
    assert "tz" not in context.trip_to_route
    assert "999-example" not in context.known_route_ids


def test_nested_visit_mappings_are_read_only() -> None:
    context = _built_context()
    with pytest.raises(TypeError):
        context.trip_to_route["injected"] = "90"
    with pytest.raises(TypeError):
        context.visits_by_sequence["t90A"][1] = None
    with pytest.raises(TypeError):
        context.visits_by_stop["t90D"]["14000"] = ()
    with pytest.raises(TypeError):
        context.route_types["90"] = 3


def test_context_fields_are_frozen() -> None:
    context = _built_context()
    with pytest.raises(dataclasses.FrozenInstanceError):
        context.known_route_ids = frozenset()


def test_synthetic_and_production_paths_share_one_builder() -> None:
    # The exact-malformed-row rejection must live in the shared builder, so
    # a synthetic construction fails identically to a production one.
    world = synthetic_world()
    rows = list(world.route_rows)
    rows.append(RouteRow(world.route_rows[0].route_id, 0))
    with pytest.raises(ContextPreparationError) as exc:
        build_matching_context(synthetic_identity(), world.network, rows, [])
    assert exc.value.code == "index_inconsistency"


def test_builder_indexes_cover_max_and_non_max_rows() -> None:
    context = _built_context()
    assert context.trip_to_route["tb1"] == "1"
    assert context.trip_to_route["t193"] == "193"
    assert context.trip_to_route["t90A"] == "90"
    assert context.route_types["1"] == 3
    assert context.known_route_ids >= {"1", "74", "193"}
    # Repeated stop visits survive as a tuple in stop-id order.
    visits = context.visits_by_stop["t90D"]["14000"]
    assert len(visits) == 2
    assert context.visits_by_sequence["t90D"][1] is visits[0]
    assert context.visits_by_sequence["t90D"][2] is visits[1]
