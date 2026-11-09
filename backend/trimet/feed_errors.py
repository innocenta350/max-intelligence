"""Rejection vocabulary for the TriMet live feeds (Stage S9, M2 plan REV 2).

Two rejection levels, never mixed (plan §6):

- **Record rejection** — one malformed record is dropped and diagnosed; every
  other record in the same response remains fully trusted.  The parse layer
  (:mod:`backend.trimet.feed_parse`) converts :class:`RecordRejectedError`
  into a :class:`~backend.trimet.feed_types.FeedDiagnostic`; this exception is
  internal control-flow and must never escape a parse call.

- **Feed rejection** — the whole feed is untrusted and the caller keeps its
  previous state untouched.  :class:`FeedRejectedError` is the *only* outcome
  of a feed-level defect: no partially decoded or partially normalized feed is
  ever returned through any code path, and the exception carries no model
  instances (owner final-check 2, tested).

Exception messages follow the ``backend.gtfs.errors`` convention: actionable
text with bounded previews.  Details never contain credentials, URLs, or raw
payloads (plan §6).

Feed-name constants are defined here because they are part of the rejection
vocabulary used by diagnostics and errors across the feed modules; the model
module (:mod:`backend.trimet.feed_types`) stays free of local imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from backend.trimet.feed_types import FeedDiagnostic

FEED_VEHICLES = "vehicles"
FEED_ALERTS = "alerts"
FEED_TRIPUPDATES = "tripupdates"


class RecordRejectedError(Exception):
    """One record failed a required-field check (internal control-flow only).

    Raised by per-record helpers inside :mod:`backend.trimet.feed_parse` and
    caught at the record loop boundary, where it becomes a
    :class:`~backend.trimet.feed_types.FeedDiagnostic`.  Never let this escape
    a parse call — callers should only ever see models, diagnostics, or
    :class:`FeedRejectedError`.
    """

    def __init__(
        self,
        feed: str,
        record_key: str | None,
        field: str,
        reason: str,
        detail: str | None = None,
    ) -> None:
        self.feed = feed
        self.record_key = record_key
        self.field = field
        self.reason = reason
        self.detail = detail
        message = f"{feed}: record {record_key!r} field {field!r}: {reason}"
        if detail:
            message += f" ({detail})"
        super().__init__(message)


class FeedRejectedError(Exception):
    """The whole feed is untrusted; no partial model is returned.

    Carries the stable ``reason`` code, an optional credential-free ``detail``,
    and the diagnostics that had accumulated *before* the feed-level defect
    was detected.  It never carries model instances — a rejected feed must not
    leak partially normalized data (plan §5.2).
    """

    def __init__(
        self,
        feed: str,
        reason: str,
        detail: str | None = None,
        diagnostics: tuple["FeedDiagnostic", ...] = (),
    ) -> None:
        self.feed = feed
        self.reason = reason
        self.detail = detail
        self.diagnostics = tuple(diagnostics)
        message = f"{feed}: {reason}"
        if detail:
            message += f" ({detail})"
        super().__init__(message)


class ProtobufDecodeError(FeedRejectedError):
    """A GTFS-Realtime wire-format structural defect (plan §6.3, row 1).

    Raised by :mod:`backend.trimet.rtproto` for truncation, varint overflow,
    unsupported wire types, and length overruns.  The decoder never returns
    partially decoded structures, so this error is the only possible outcome
    of a malformed byte stream.  ``detail`` records byte offsets and the
    defect — never payload bytes.
    """

    def __init__(
        self,
        reason: str,
        detail: str | None = None,
        diagnostics: tuple["FeedDiagnostic", ...] = (),
    ) -> None:
        super().__init__(
            feed=FEED_TRIPUPDATES,
            reason=reason,
            detail=detail,
            diagnostics=diagnostics,
        )
