"""S12 session fencing, acknowledgment and concurrent sink behavior.

Stage S12 (M2 plan §6, §7; implementation prompt §4).  Covers acceptance
matrix rows 2 (S10-compatible callback signatures and the two failure
paths), 3 (replay/older/collision/new-runtime reset), 14 (fencing and
second-session rejection with barrier/event synchronization — no sleeps),
16 (safe replay acknowledgment after a caller retry) and 17 (no clock,
filesystem or network calls in the store; no real poller startup).
"""
from __future__ import annotations

import builtins
import inspect
import socket
import threading
import time as time_module

import pytest

from tests._s12_helpers import s12_deny_all_sockets  # noqa: F401
from tests._s12_helpers import (
    BASE_MS,
    SHA_A,
    SHA_B,
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

from backend.trimet.poll_types import PollFeed
from backend.trimet.state_errors import (
    StateConflictError,
    StateInputError,
    StateSessionError,
)
from backend.trimet.state_types import ApplyDecision, StateReason

Q1 = BASE_MS
Q2 = BASE_MS + 1_000
Q3 = BASE_MS + 2_000
T_HDR1 = BASE_MS + 100


def _world_and_state():
    context = s12_context(s12_world(), sha256=SHA_A)
    return open_state(context)


def _full(sequence, query_time, vehicles, **kwargs):
    return vehicle_delivery(
        vehicles,
        query_time_ms=query_time,
        sequence=sequence,
        evidence=complete_evidence(len(vehicles)),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Row 2: S10-compatible callback signatures and two distinct failure paths
# ---------------------------------------------------------------------------


def test_sinks_are_one_signature_compatible_callback_per_feed():
    state, session = _world_and_state()
    sinks = session.sinks
    assert set(sinks) == {PollFeed.VEHICLES, PollFeed.TRIPUPDATES, PollFeed.ALERTS}
    for feed, sink in sinks.items():
        parameters = inspect.signature(sink).parameters
        assert list(parameters) == ["delivery"]
        # One positional PollDelivery parameter, no extra state threaded in.
        assert parameters["delivery"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        # A stable, payload-free wrapper name for observability.
        assert sink.__name__ == f"s12_state_sink_{feed.value}"


def test_normal_acknowledgment_only_after_commit_or_noop():
    state, session = _world_and_state()
    delivery = _full(1, Q1, [vehicle_position(1001, time_ms=Q1)])
    # The wrapper returns None on a commit — S10 acknowledges by return.
    assert session.sinks[PollFeed.VEHICLES](delivery) is None
    result = session.accept(delivery)  # direct accept of the same delivery
    # Replaying through the wrapper is also a normal no-op acknowledgment.
    assert session.sinks[PollFeed.VEHICLES](delivery) is None
    assert result.decision is ApplyDecision.DUPLICATE_NOOP


def test_typed_precommit_failure_propagates_from_the_sink():
    state, session = _world_and_state()
    bad = _full(1, Q1, [vehicle_position(1001, time_ms=Q1)], http_status=500)
    with pytest.raises(StateInputError) as excinfo:
        session.sinks[PollFeed.VEHICLES](bad)
    assert excinfo.value.code == "invalid_delivery"
    # Nothing was published for the failed callback.
    assert state.snapshot().revision == 1  # open-session revision only


def test_s12_input_error_is_distinct_from_s10_upstream_failure(monkeypatch):
    """A synthetic non-200 delivery is S12 invalid_delivery, never an
    upstream HTTP/transport failure, and never an S10 error type."""
    from backend.trimet import feed_http, poll_errors  # noqa: F401  (S10 owners)

    state, session = _world_and_state()
    bad = _full(1, Q1, [vehicle_position(1001, time_ms=Q1)], http_status=503)
    with pytest.raises(StateInputError) as excinfo:
        session.accept(bad)
    error = excinfo.value
    for s10_error_type in (
        getattr(poll_errors, name)
        for name in dir(poll_errors)
        if name.startswith("Poll")
    ):
        if isinstance(s10_error_type, type) and issubclass(
            s10_error_type, Exception
        ):
            assert not isinstance(error, s10_error_type)
    # The code vocabulary carries no HTTP status content.
    assert error.code == "invalid_delivery"


def test_s10_ack_pattern_advances_watermark_only_on_normal_return():
    """Replicates S10's acknowledgment rule locally (real runtime startup is
    S14): watermark commits only after the sink returns normally."""
    state, session = _world_and_state()

    watermark = {"since_ms": None}

    def s10_style_invoke(delivery):
        try:
            session.sinks[PollFeed.VEHICLES](delivery)
        except Exception:
            return False  # S10 reports DELIVERY_FAILED; no watermark move
        query_time = delivery.model.query_time_utc_ms
        watermark["since_ms"] = (
            query_time
            if watermark["since_ms"] is None
            else max(watermark["since_ms"], query_time)
        )
        return True

    good = _full(1, Q1, [vehicle_position(1001, time_ms=Q1)])
    assert s10_style_invoke(good) is True
    assert watermark["since_ms"] == Q1
    # A synthetic non-200 delivery fails the callback; watermark untouched.
    bad = _full(2, Q2, [vehicle_position(1002, time_ms=Q2)], http_status=404)
    assert s10_style_invoke(bad) is False
    assert watermark["since_ms"] == Q1
    # An exact replay acks normally; the watermark cannot regress or jump.
    assert s10_style_invoke(good) is True
    assert watermark["since_ms"] == Q1
    assert state.snapshot().revision == 2


# ---------------------------------------------------------------------------
# Row 3: replay, older sequence, collision and new-runtime reset
# ---------------------------------------------------------------------------


def test_exact_replay_is_duplicate_noop_without_root_mutation():
    state, session = _world_and_state()
    delivery = _full(1, Q1, [vehicle_position(1001, time_ms=Q1)])
    first = session.accept(delivery)
    before = state.snapshot()
    second = session.accept(delivery)
    after = state.snapshot()
    assert first.decision is ApplyDecision.COMMITTED
    assert second.decision is ApplyDecision.DUPLICATE_NOOP
    assert second.reasons == (StateReason.DUPLICATE_DELIVERY,)
    assert second.revision == before.revision
    # Safe acknowledgment after a caller retry (row 16): no visible change.
    assert after.revision == before.revision
    assert dict(after.vehicle_entries) == dict(before.vehicle_entries)
    assert after.telemetry.reason_totals == before.telemetry.reason_totals


def test_older_sequence_is_stale_noop_without_root_mutation():
    state, session = _world_and_state()
    session.accept(_full(2, Q2, [vehicle_position(1001, time_ms=Q2)]))
    before = state.snapshot()
    result = session.accept(
        _full(1, Q1, [vehicle_position(1002, time_ms=Q1)])
    )
    after = state.snapshot()
    assert result.decision is ApplyDecision.STALE_NOOP
    assert result.reasons == (StateReason.OLDER_SEQUENCE,)
    assert result.revision == before.revision
    assert dict(after.vehicle_entries) == dict(before.vehicle_entries)
    assert 1002 not in after.vehicle_entries


def test_sequence_collision_reuses_committed_sequence_with_new_content():
    state, session = _world_and_state()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    before = state.snapshot()
    with pytest.raises(StateConflictError) as excinfo:
        session.accept(_full(1, Q1, [vehicle_position(1002, time_ms=Q1)]))
    assert excinfo.value.code == "sequence_collision"
    after = state.snapshot()
    # All-or-nothing: the failed transaction published nothing.
    assert after.revision == before.revision
    assert dict(after.vehicle_entries) == dict(before.vehicle_entries)
    assert set(after.vehicle_entries) == {1001}
    assert after.telemetry.reason_totals == before.telemetry.reason_totals


def test_new_runtime_session_resets_sequences_but_keeps_source_ordering():
    state, session = _world_and_state()
    committed = session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    assert committed.authoritative is True
    state.close_session(session.session_id)
    session2 = state.open_session()
    assert session2.session_id != session.session_id
    # A replacement runtime may restart at sequence 1.
    reset = session2.accept(_full(1, Q2, [vehicle_position(1001, time_ms=Q2)]))
    assert reset.decision is ApplyDecision.COMMITTED
    # Source-time ordering survived session replacement: an older queryTime
    # in the new namespace is still ignored whole.
    stale = session2.accept(
        _full(2, Q2 - 500, [vehicle_position(1002, time_ms=Q2 - 500)])
    )
    assert stale.decision is ApplyDecision.IGNORED_OLDER
    assert stale.reasons == (StateReason.OLDER_SOURCE_TIMESTAMP,)
    snapshot = state.snapshot()
    assert 1002 not in snapshot.vehicle_entries
    assert snapshot.vehicle_entries[1001].stamp.source_time_utc_ms == Q2


def test_closed_session_clears_replay_envelope_but_keeps_source_view():
    state, session = _world_and_state()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    while_open = state.snapshot()
    view = while_open.feed_views[PollFeed.VEHICLES]
    assert view.replay_delivery is not None
    assert view.source_content is not None
    state.close_session(session.session_id)
    after_close = state.snapshot()
    closed_view = after_close.feed_views[PollFeed.VEHICLES]
    # Replay envelope travels with the session; source content survives.
    assert closed_view.replay_delivery is None
    assert closed_view.source_content is not None
    assert closed_view.source_time_high_water_ms == Q1


# ---------------------------------------------------------------------------
# Row 14: fencing with barrier/event synchronization (no sleeps)
# ---------------------------------------------------------------------------


def test_second_active_session_is_rejected():
    state, session = _world_and_state()
    with pytest.raises(StateSessionError) as excinfo:
        state.open_session()
    assert excinfo.value.code == "session_already_open"
    # After revocation a replacement session may be opened.
    state.close_session(session.session_id)
    replacement = state.open_session()
    assert replacement.session_id == session.session_id + 1


def test_revocation_during_matching_fences_the_survivor(monkeypatch):
    state, session = _world_and_state()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))
    revision_before_block = state.snapshot().revision

    started = threading.Event()
    release = threading.Event()

    def hook(delivery, context):
        if delivery.feed is PollFeed.VEHICLES and delivery.sequence == 2:
            started.set()
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
        except Exception as exc:  # captured for exact assertions
            failures.append(exc)

    thread = threading.Thread(target=worker, name="s12-fenced-candidate")
    thread.start()
    assert started.wait(timeout=30)
    # Revocation is an atomic fence that wins over the in-flight candidate.
    state.close_session(session.session_id)
    release.set()
    thread.join(timeout=30)
    assert not thread.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], StateSessionError)
    assert failures[0].code == "invalid_session"
    # Nothing from the fenced candidate was published.
    snapshot = state.snapshot()
    assert snapshot.active_session_id is None
    assert 1002 not in snapshot.vehicle_entries
    assert snapshot.revision == revision_before_block + 1  # close_session only


def test_retry_exhaustion_leaves_the_winning_root_intact(monkeypatch):
    state, session = _world_and_state()
    session.accept(_full(1, Q1, [vehicle_position(1001, time_ms=Q1)]))

    attempts = iter(range(1, 10))
    attempt_entered = [threading.Event() for _ in range(3)]
    attempt_release = [threading.Event() for _ in range(3)]

    def hook(delivery, context):
        if delivery.feed is PollFeed.VEHICLES and delivery.sequence == 2:
            index = next(attempts) - 1
            attempt_entered[index].set()
            assert attempt_release[index].wait(timeout=30)

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

    thread = threading.Thread(target=worker, name="s12-exhausted-candidate")
    thread.start()

    # While the vehicle candidate is matching, three secondary commits win
    # the publication race, one per optimistic attempt.
    for index in range(3):
        assert attempt_entered[index].wait(timeout=30)
        session.accept(
            tripupdates_delivery(
                [make_trip_update_entity(f"e{index}", "t90A")],
                header_timestamp_ms=T_HDR1 + index * 1_000,
                sequence=index + 1,
            )
        )
        attempt_release[index].set()

    thread.join(timeout=30)
    assert not thread.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], StateConflictError)
    assert failures[0].code == "concurrent_change"
    # The winning root is intact: three secondary commits stand, the
    # exhausted vehicle candidate published nothing.
    snapshot = state.snapshot()
    assert 1002 not in snapshot.vehicle_entries
    assert dict(snapshot.vehicle_entries) == {1001: snapshot.vehicle_entries[1001]}
    tu_view = snapshot.feed_views[PollFeed.TRIPUPDATES]
    assert tu_view.source_content is not None
    assert tu_view.source_content.delivery.sequence == 3
    assert snapshot.revision == 2 + 3  # vehicle commit + open + 3 secondary


# ---------------------------------------------------------------------------
# Row 17: no clocks, no timers, no filesystem, no network, no poller
# ---------------------------------------------------------------------------


def test_store_makes_no_clock_file_or_network_calls(monkeypatch):
    # Deliberate blocked-network attempt: the deny-all guard fires without
    # any external contact (plan §13 requirement).
    with pytest.raises(AssertionError, match="S12 socket guard"):
        socket.getaddrinfo("example.invalid", 80)

    # The S12 store modules bind neither the time nor datetime modules.
    import sys

    for module_name in (
        "backend.trimet.live_state",
        "backend.trimet.state_sink",
        "backend.trimet.state_types",
        "backend.trimet.state_errors",
    ):
        module_globals = vars(sys.modules[module_name])
        assert "time" not in module_globals, module_name
        assert "datetime" not in module_globals, module_name

    def denied(what):
        def raiser(*args, **kwargs):
            raise AssertionError(f"S12 guard: {what} is denied in the store")

        return raiser

    monkeypatch.setattr(time_module, "time", denied("time.time"))
    monkeypatch.setattr(time_module, "monotonic", denied("time.monotonic"))
    monkeypatch.setattr(time_module, "perf_counter", denied("time.perf_counter"))
    monkeypatch.setattr(time_module, "time_ns", denied("time.time_ns"))
    # ``datetime.datetime.now`` is C-implemented and immutable; the time
    # module patches above already prove the store reads no wall clock —
    # the S12 store modules import neither ``time`` nor ``datetime``.
    monkeypatch.setattr(builtins, "open", denied("open"))

    threads_before = threading.active_count()
    state, session = _world_and_state()
    committed = session.accept(
        _full(1, Q1, [vehicle_position(1001, time_ms=Q1)])
    )
    assert committed.decision is ApplyDecision.COMMITTED
    session.accept(
        incremental_delivery(
            [vehicle_position(1001, time_ms=Q2)],
            query_time_ms=Q2,
            since_ms=Q1,
            sequence=2,
        )
    )
    session.accept(
        tripupdates_delivery(
            [make_trip_update_entity("e1", "t90A")],
            header_timestamp_ms=T_HDR1,
            sequence=1,
        )
    )
    snapshot = state.snapshot()
    assert snapshot.prior_state_history == "unknown"
    state.close_session(session.session_id)
    # No timers, workers or poller threads were started by the store.
    assert threading.active_count() == threads_before
    assert not any(
        "s12" in thread.name.lower()
        for thread in threading.enumerate()
        if thread is not threading.current_thread()
    )
