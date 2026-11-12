"""S12 session handle, revocation fencing, and the S10-compatible adapter.

Stage S12 (M2 plan §§5–7; implementation prompt §4).  A
:class:`StateSession` represents one ``PollingRuntime`` object's
delivery-sequence namespace.  Only one session may be open per store; a
replacement runtime gets a new session after the previous one is revoked.

Adapter contract (plan §6; prompt §4):

- Each sink wrapper invokes :meth:`StateSession.accept` and returns
  ``None`` normally, after an accepted or no-op decision; the wrapper
  discards the :class:`~backend.trimet.state_types.ApplyResult` because S10
  sinks acknowledge by returning.
- A precommit typed failure propagates: S10's
  ``_invoke_sink`` reports it as ``DELIVERY_FAILED`` and does not advance
  that delivery's watermark.  This is deliberately distinct from a real
  upstream HTTP/transport failure, which S10 owns before a successful
  ``PollDelivery`` exists.
- The adapter does not stop or join workers and does not claim hard
  cancellation.  Revocation is an atomic fence at the store: an old
  session's in-flight candidates fail revalidation and publish nothing.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType

from backend.trimet.poll_types import PollDelivery, PollFeed
from backend.trimet.state_types import ApplyResult

__all__ = ["StateSession"]

_Sink = Callable[[PollDelivery], None]


class StateSession:
    """One open session bound to a :class:`~backend.trimet.live_state.LiveState`.

    The session identity is an opaque store-local integer.  Callbacks
    capture it; the store revalidates it under the lock at capture and at
    publication, so a revoked session can never publish even when matching
    began earlier.
    """

    def __init__(self, store, session_id: int) -> None:
        # ``store`` is the owning LiveState (duck-typed to keep the modules
        # acyclic); every call revalidates the session against the store's
        # current root.
        self._store = store
        self._session_id = session_id
        self._sinks: Mapping[PollFeed, _Sink] = MappingProxyType(
            {feed: self._make_sink(feed) for feed in PollFeed}
        )

    @property
    def session_id(self) -> int:
        """The opaque store-local session identity."""
        return self._session_id

    @property
    def sinks(self) -> Mapping[PollFeed, _Sink]:
        """Read-only mapping of one S10-compatible sink per feed.

        Signature-compatible with ``PollingRuntime``'s ``sinks`` argument:
        ``Callable[[PollDelivery], None]``.  Normal return acknowledges the
        delivery (accepted or no-op); a raised typed precommit failure is
        reported by S10 as ``DELIVERY_FAILED``.
        """
        return self._sinks

    def accept(self, delivery: PollDelivery) -> ApplyResult:
        """Offer one S10 delivery to the store (plan §6).

        All-or-nothing: either a complete candidate publishes (data or
        metadata-only) or a typed precommit failure raises with nothing
        published.  Exact replays and stale deliveries return normally as
        no-ops; changed content reusing a committed sequence raises
        ``sequence_collision``.
        """
        return self._store._accept(self._session_id, delivery)

    def _make_sink(self, feed: PollFeed) -> _Sink:
        def sink(delivery: PollDelivery) -> None:
            # Discard the ApplyResult: S10 acknowledges by normal return.
            self._store._accept(self._session_id, delivery)

        # Give the wrapper a stable, inspectable name without payloads.
        sink.__name__ = f"s12_state_sink_{feed.value}"
        return sink
