"""S12 atomic static-context replacement, re-matching and preservation tests.

Stage S12 (M2 plan §11; implementation prompt §5).  Covers acceptance
matrix rows 12 (A→B re-matching including the named one-record fallback
regression and same-content no-op), 13 (failure preservation and
concurrent vehicle/context publication) and the generation-conflict part
of row 14's retry discipline.
"""
from __future__ import annotations

import threading

import pytest

from tests._s12_helpers import s12_deny_all_sockets  # noqa: F401
from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    SHA_B,
    SHA_C,
    complete_evidence,
    incremental_delivery,
    make_trip_update_entity,
    open_state,
    patch_match,
    s12_context,
    s12_world,
    tripupdates_delivery,
    vehicle_delivery,
    vehicle_position,
)

from backend.trimet.match_index import build_matching_context
from backend.trimet.match_types import (
    DatasetIdentity,
    RelevanceClassification,
    RouteRow,
)
from backend.trimet.poll_types import PollFeed
from backend.trimet.state_errors import StateConflictError, StateInputError
from backend.trimet.state_types import PRIOR_STATE_HISTORY_UNKNOWN

Q1 = BASE_MS
Q2 = BASE_MS + 1_000
T1 = BASE_MS + 1_000
T2 = T1 + 1_000


def _state_a():
    return open_state(s12_context(s12_world(), sha256=SHA_A))


def _full(seq, qt, vehicles):
    return vehicle_delivery(
        vehicles,
        query_time_ms=qt,
        sequence=seq,
        evidence=complete_evidence(len(vehicles)),
    )


# ---------------------------------------------------------------------------
# Row 12: A→B re-matching, one-record fallback, same-content no-op
# ---------------------------------------------------------------------------


def test_context_rematch_without_original_delivery_envelopes(monkeypatch):
    """Named regression (REV 2 §11): both retained envelope slots have been
    superseded, yet every retained entry re-matches canonically from its
    one-record synthetic snapshot."""
    state, session = _state_a()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1,
                                                 trip_id="t90A")]))
    # The vehicles slot is superseded by an incremental that does not even
    # carry 1001; the TripUpdates slot is superseded too.
    session.accept(
        incremental_delivery(
            [vehicle_position(1002, time_ms=Q2)],
            query_time_ms=Q2,
            since_ms=Q1,
            sequence=2,
        )
    )
    session.accept(
        tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A")],
            header_timestamp_ms=T1,
            sequence=1,
        )
    )
    session.accept(
        tripupdates_delivery(
            [make_trip_update_entity("e2", "t90B")],
            header_timestamp_ms=T2,
            sequence=2,
        )
    )
    before = state.snapshot()
    assert before.vehicle_entries[1001].matched_generation == 1

    calls: list[tuple[object, str]] = []

    def hook(delivery, context):
        calls.append((delivery, context.identity.archive_sha256))

    patch_match(monkeypatch, hook)
    context_b = s12_context(s12_world(), sha256=SHA_B)
    result = state.replace_context(context_b, expected_generation=1)

    assert result.changed is True
    assert result.generation_before == 1
    assert result.generation_after == 2
    assert result.rematched_vehicle_entries == 2
    assert result.rematched_feed_views == 2
    assert result.revision_after == before.revision + 1

    vehicles_calls = [
        delivery for delivery, sha in calls if delivery.feed is PollFeed.VEHICLES
    ]
    # The vehicles feed view is re-matched from its exact stored delivery.
    stored_view_delivery = (
        before.feed_views[PollFeed.VEHICLES].source_content.delivery
    )
    view_calls = [
        delivery for delivery in vehicles_calls if delivery is stored_view_delivery
    ]
    assert len(view_calls) == 1
    # Every other vehicle call is the internal one-record synthetic input
    # with evidence=None and empty diagnostics — no original whole
    # collection was consumed or reconstructed.
    synthetic = [
        delivery for delivery in vehicles_calls if delivery is not stored_view_delivery
    ]
    assert len(synthetic) == 2
    for synthetic_delivery in synthetic:
        snapshot_model = synthetic_delivery.model
        assert len(snapshot_model.vehicles) == 1
        assert snapshot_model.provenance.request_kind is not None
        assert synthetic_delivery.diagnostics == ()
        assert synthetic_delivery.evidence is None
        assert synthetic_delivery.sequence in (1, 2)  # from the retained stamp
    # The superseded originals were never re-delivered as whole collections.
    assert before.feed_views[PollFeed.VEHICLES].source_content.delivery.sequence == 2

    snapshot = state.snapshot()
    for entry in snapshot.vehicle_entries.values():
        # Enrichment is B-shaped; the raw record and stamp are preserved.
        assert entry.matched_generation == 2
        assert entry.stamp.source_time_utc_ms in (Q1, Q2)
    assert snapshot.vehicle_entries[1001].raw.time_utc_ms == Q1
    assert snapshot.generation == 2
    assert snapshot.prior_state_history == PRIOR_STATE_HISTORY_UNKNOWN


def test_context_swap_rematches_entries_and_views_under_b():
    state, session = _state_a()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1,
                                                 trip_id="t90A")]))
    session.accept(
        tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A")],
            header_timestamp_ms=T1,
            sequence=1,
        )
    )
    raw_before = state.snapshot().vehicle_entries[1001].raw
    stamp_before = state.snapshot().vehicle_entries[1001].stamp

    context_b = s12_context(s12_world(), sha256=SHA_B)
    result = state.replace_context(context_b, expected_generation=1)
    assert result.changed is True
    assert result.generation_after == 2
    assert result.rematched_vehicle_entries == 1
    assert result.rematched_feed_views == 2  # vehicles view + TripUpdates view

    snapshot = state.snapshot()
    entry = snapshot.vehicle_entries[1001]
    assert entry.raw is raw_before
    assert entry.stamp is stamp_before
    assert entry.matched_generation == 2
    assert snapshot.dataset_identity.archive_sha256 == SHA_B
    view = snapshot.feed_views[PollFeed.TRIPUPDATES]
    assert view.source_content.matched_generation == 2
    assert view.source_time_high_water_ms == T1  # source times preserved


def test_same_content_context_is_an_atomic_noop():
    state, session = _state_a()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    before = state.snapshot()
    # Same (archive_sha256, feed_version): a descriptive-only naming
    # difference must not cause a content change.
    same = s12_context(
        s12_world(),
        sha256=SHA_A,
        source_directory_name="differently-named-copy",
    )
    result = state.replace_context(same, expected_generation=1)
    after = state.snapshot()
    assert result.changed is False
    assert result.generation_before == result.generation_after == 1
    assert result.rematched_vehicle_entries == 0
    assert result.rematched_feed_views == 0
    assert result.revision_after == before.revision
    assert after.revision == before.revision
    assert after.dataset_identity.archive_sha256 == SHA_A


def test_removed_trip_leaves_position_only_max_vehicle():
    state, session = _state_a()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1,
                                                 trip_id="t90A")]))
    matched_before = state.snapshot().vehicle_entries[1001].matched
    assert matched_before.trip is not None  # t90A enrichment exists under A

    context_b = s12_context(
        s12_world(include_trip_t90a=False), sha256=SHA_B
    )
    state.replace_context(context_b, expected_generation=1)
    snapshot = state.snapshot()
    entry = snapshot.vehicle_entries[1001]
    # The removed trip loses enrichment; the still-known MAX route keeps
    # the vehicle MAX and tracked — nothing was cleared.
    assert entry.matched.classification is RelevanceClassification.MAX_RELEVANT
    assert entry.matched.trip is None
    assert entry.matched.max_route is not None
    assert entry.raw.time_utc_ms == Q1
    assert snapshot.max_vehicle_ids == (1001,)
    # The reported raw id is preserved; no A-shaped trip object survived.
    assert entry.raw.trip_id == "t90A"
    assert entry.matched.trip is None


def test_changed_relevance_is_reflected_and_old_snapshots_stay_unchanged():
    state, session = _state_a()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    snapshot_a = state.snapshot()
    assert snapshot_a.max_vehicle_ids == (1001,)
    assert snapshot_a.generation == 1

    context_b = s12_context(
        s12_world(include_route_90=False), sha256=SHA_B
    )
    state.replace_context(context_b, expected_generation=1)
    snapshot_b = state.snapshot()
    # Route 90 left the network: the vehicle becomes UNCLASSIFIED and
    # leaves the MAX projection, but stays retained and diagnosable.
    assert snapshot_b.vehicle_entries[1001].matched.classification is (
        RelevanceClassification.UNCLASSIFIED
    )
    assert snapshot_b.max_vehicle_ids == ()
    assert 1001 in snapshot_b.vehicle_entries
    assert snapshot_b.generation == 2
    # The previously returned A snapshot is still a valid immutable A view.
    assert snapshot_a.generation == 1
    assert snapshot_a.max_vehicle_ids == (1001,)
    assert snapshot_a.vehicle_entries[1001].matched.classification is (
        RelevanceClassification.MAX_RELEVANT
    )


# ---------------------------------------------------------------------------
# Row 13: failure preservation and concurrent publication
# ---------------------------------------------------------------------------


def test_stale_expected_generation_preserves_the_current_root():
    state, session = _state_a()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    state.replace_context(
        s12_context(s12_world(), sha256=SHA_B), expected_generation=1
    )
    before = state.snapshot()
    with pytest.raises(StateConflictError) as excinfo:
        state.replace_context(
            s12_context(s12_world(), sha256=SHA_C), expected_generation=1
        )
    assert excinfo.value.code == "generation_conflict"
    after = state.snapshot()
    assert after.generation == 2
    assert after.revision == before.revision
    assert after.dataset_identity.archive_sha256 == SHA_B


def test_invalid_context_candidate_is_typed_input_and_preserves_root():
    state, session = _state_a()
    before = state.snapshot()
    with pytest.raises(StateInputError) as excinfo:
        state.replace_context("not-a-context", expected_generation=1)
    assert excinfo.value.code == "invalid_context"
    assert state.snapshot().revision == before.revision
    assert state.snapshot().generation == before.generation


def test_failed_b_preparation_never_reaches_replacement():
    state, session = _state_a()
    before = state.snapshot()
    # A defective candidate fails in S11's shared builder, before the store.
    broken_world = s12_world()
    with pytest.raises(Exception) as excinfo:
        build_matching_context(
            DatasetIdentity(archive_sha256=SHA_B, feed_version="v"),
            broken_world.network,
            (RouteRow("90", 0), RouteRow("90", 0)),  # duplicate route row
            broken_world.trip_rows,
        )
    assert "duplicate" in str(excinfo.value).lower()
    assert state.snapshot().revision == before.revision
    assert state.snapshot().generation == before.generation


def test_concurrent_vehicle_and_context_publication_loses_no_update(
    monkeypatch,
):
    state, session = _state_a()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1,
                                                 trip_id="t90A")]))

    entered = threading.Event()
    release = threading.Event()

    def hook(delivery, context):
        # Block only the concurrent vehicle candidate, once.
        if (
            delivery.feed is PollFeed.VEHICLES
            and delivery.sequence == 2
            and not release.is_set()
        ):
            entered.set()
            assert release.wait(timeout=30), "test orchestrator stalled"

    patch_match(monkeypatch, hook)
    failures: list[Exception] = []

    def worker():
        try:
            session.accept(
                incremental_delivery(
                    [vehicle_position(1002, time_ms=Q2)],
                    query_time_ms=Q2,
                    since_ms=Q1,
                    sequence=2,
                )
            )
        except Exception as exc:
            failures.append(exc)

    thread = threading.Thread(target=worker, name="s12-concurrent-vehicle")
    thread.start()
    assert entered.wait(timeout=30)

    # The context swap wins while the vehicle candidate is matching: the
    # candidate must rebuild and re-match against B, never publish A-shaped
    # enrichment under B's identity.
    swap = state.replace_context(
        s12_context(s12_world(), sha256=SHA_B), expected_generation=1
    )
    assert swap.changed is True
    release.set()
    thread.join(timeout=30)
    assert not thread.is_alive()
    assert failures == []

    snapshot = state.snapshot()
    # No lost update: both accepted observations exist under generation 2.
    assert set(snapshot.vehicle_entries) == {1001, 1002}
    assert snapshot.generation == 2
    for entry in snapshot.vehicle_entries.values():
        assert entry.matched_generation == 2
    assert snapshot.vehicle_entries[1002].raw.time_utc_ms == Q2
    # The winning revision reflects both publications after the swap.
    assert snapshot.revision == swap.revision_after + 1
