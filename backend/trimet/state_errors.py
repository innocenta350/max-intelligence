"""Typed credential-free S12 state errors (plan §12; prompt §8).

One failure family with a fixed nine-code vocabulary, frozen before
implementation.  Exceptions carry a short bounded message plus a ``code``
from the closed set below — never payloads, URLs, absolute paths,
credentials, or raw exception reprs (plan §12).  The codes partition into
four dispositions:

- :class:`StateInputError` — the caller handed the store inconsistent or
  malformed programmer input (``invalid_delivery``, ``invalid_context``,
  ``invalid_limits``).  Nothing was published.
- :class:`StateSessionError` — session fencing (``invalid_session``,
  ``session_already_open``).
- :class:`StateConflictError` — optimistic-publication and ordering
  conflicts (``sequence_collision``, ``generation_conflict``,
  ``concurrent_change``).  The winning committed root is intact.
- :class:`StateCapacityError` — a validated application bound would be
  exceeded (``capacity_exceeded``).  Nothing was published; no silent
  truncation or eviction ever disguises data loss as success.

Every failure is a precommit failure: the store never raises an anticipated
validation error after publication, so a raised error always means the
previous root is unchanged.
"""
from __future__ import annotations

__all__ = [
    "STATE_ERROR_CODES",
    "StateCapacityError",
    "StateConflictError",
    "StateError",
    "StateInputError",
    "StateSessionError",
]

STATE_ERROR_CODES: frozenset[str] = frozenset(
    {
        "invalid_delivery",  # inconsistent/malformed PollDelivery input
        "invalid_context",  # not a prepared MatchingContext
        "invalid_limits",  # StateLimits values outside their validated domain
        "invalid_session",  # unknown/revoked session identity
        "session_already_open",  # a second active session was requested
        "sequence_collision",  # a committed sequence reused with new content
        "generation_conflict",  # stale expected static-context generation
        "concurrent_change",  # optimistic retries exhausted; winner preserved
        "capacity_exceeded",  # a validated bound would be exceeded
    }
)


class StateError(Exception):
    """Base class for every S12 failure.

    Exceptions carry only a bounded message plus a ``code`` from the fixed
    vocabulary above.  ``message`` is exposed explicitly (mirroring the S11
    error convention) so callers never parse exception strings.
    """

    _allowed_codes: frozenset[str] = STATE_ERROR_CODES

    def __init__(self, message: str, *, code: str) -> None:
        if code not in self._allowed_codes:
            raise ValueError(
                f"{type(self).__name__} code {code!r} is not in its fixed "
                "vocabulary"
            )
        super().__init__(message)
        self.message = message
        self.code = code


class StateInputError(StateError):
    """Inconsistent programmer input reached the store; nothing published.

    Covers malformed deliveries (feed/model disagreement, a synthetic
    non-200 status, purpose/provenance disagreement, impossible collection
    evidence), a non-context candidate for :meth:`LiveState.replace_context`,
    and invalid ``StateLimits`` values.  This is deliberately distinct from
    a real upstream HTTP/transport failure, which S10 owns before a
    successful ``PollDelivery`` ever exists.
    """

    _allowed_codes = frozenset(
        {"invalid_delivery", "invalid_context", "invalid_limits"}
    )


class StateSessionError(StateError):
    """Session fencing refused an operation.

    Raised when a session identity is unknown or revoked, when a second
    active session is requested, or when revocation fences an in-flight
    candidate out of publication.  Revocation always wins: an old-session
    candidate can never publish, even if its matching began earlier.
    """

    _allowed_codes = frozenset({"invalid_session", "session_already_open"})


class StateConflictError(StateError):
    """A sequence/generation/publication conflict; the winner is preserved.

    ``sequence_collision`` rejects changed content reusing a committed
    sequence; ``generation_conflict`` rejects a stale expected generation;
    ``concurrent_change`` reports bounded optimistic-retry exhaustion.  In
    every case the currently committed root — which may legitimately be a
    newer one than the operation captured — remains intact and the failed
    operation published nothing.
    """

    _allowed_codes = frozenset(
        {"sequence_collision", "generation_conflict", "concurrent_change"}
    )


class StateCapacityError(StateError):
    """A validated application bound would be exceeded; nothing published.

    Raised for per-delivery record/nested-record bounds, the retained
    vehicle-map capacity on the final candidate, and the authoritative
    presence-set capacity.  Over-capacity operations fail atomically: data,
    context, fences, source/replay markers and revision are all preserved,
    and the oversized delivery is never downgraded to a successful partial
    update.
    """

    _allowed_codes = frozenset({"capacity_exceeded"})
