"""Safe transport/configuration/lifecycle error vocabulary (REV 3 plan §9-§10).

Message-safety rules enforced by structure:

- Every message is built from stable reason codes and numeric metadata.
- Exception messages never include URL text, raw HTTPX exception ``str``,
  response bodies, headers, or raw parser exception detail.
- ``raise ... from None`` in constructors-callers keeps original text out of
  the chain.  Type information of the cause is preserved as a short code.
"""
from __future__ import annotations


class PollError(Exception):
    """Base class for S10 errors; carries a stable ``code``."""

    code = "poll_error"


class PollConfigError(PollError):
    """Invalid or missing configuration.  Never echoes credential values."""

    code = "invalid_config"


class LifecycleError(PollError):
    """Invalid programmer/lifecycle state; not retried as upstream problems."""

    code = "invalid_lifecycle"


class ProgrammerStateError(PollError):
    """A violated invariant that must surface as ``worker_failed``."""

    code = "worker_failed"


class AttemptError(PollError):
    """Base class for per-attempt failures with a stable safe code."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


class TransportTimeoutError(AttemptError):
    def __init__(self, detail: str) -> None:
        super().__init__("transport_timeout", detail)


class TransportFailureError(AttemptError):
    def __init__(self, detail: str) -> None:
        super().__init__("transport_error", detail)


class HttpStatusError(AttemptError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__("http_status_error", f"status={status} {detail}")
        self.status = status


class AuthBlockedError(AttemptError):
    """HTTP 401/403 — stable code only; never echoes the credential."""

    def __init__(self, status: int) -> None:
        super().__init__("auth_blocked", f"status={status} credentials rejected")
        self.status = status


class UnsupportedContentTypeError(AttemptError):
    def __init__(self, status: int) -> None:
        super().__init__(
            "invalid_content_type", f"status={status} unsupported media type"
        )
        self.status = status


class UnsupportedContentEncodingError(AttemptError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__("unsupported_content_encoding", f"status={status} {detail}")
        self.status = status


class ResponseTooLargeError(AttemptError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__("response_too_large", f"status={status} {detail}")
        self.status = status


class InvalidJsonError(AttemptError):
    def __init__(self, status: int) -> None:
        super().__init__("invalid_json", f"status={status} payload is not valid JSON")
        self.status = status


class FeedRejectedBubble(PollError):
    """S9 whole-feed rejection surfaced to the polling layer.

    ``reason`` is S9's own stable code; the S9 message text is intentionally
    not used (it may embed bounded previews of feed data, which is fine for
    S9-level diagnostics but must not be quoted into S10 error text).
    """

    def __init__(self, feed_name: str, reason: str) -> None:
        super().__init__(f"({feed_name}) feed_rejected: reason={reason}")
        self.code = "feed_rejected"
        self.reason = reason
