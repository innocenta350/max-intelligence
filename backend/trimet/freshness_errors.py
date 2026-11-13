"""Typed credential-free S13 freshness errors (plan §11; prompt §4).

One failure family with a fixed seven-code input/recovery vocabulary,
frozen before implementation.  Exceptions carry a short bounded message
plus a ``code`` from the closed set below — never payloads, URLs, absolute
paths, credentials, or raw exception reprs.

The codes partition into two dispositions:

- :class:`FreshnessInputError` — malformed caller input reached the pure
  evaluator, the policy adapter, the clock boundary or the store's expiry
  operation.  Nothing was mutated or published.
- :class:`FreshnessRecoveryError` — an explicit clock recovery was refused
  by the epoch check or by the store's accepted expiry cutoff.  No anchor,
  epoch or store state changed.

Runtime clock-provider failures are **not** exceptions: they become bounded
``ClockStatus.UNTRUSTED`` assessments with fixed
:class:`~backend.trimet.freshness_types.FreshnessReason` values at the
explicit clock boundary (plan §7).  Existing S12
:class:`~backend.trimet.state_errors.StateError` codes keep their exact
meanings; S13 never adds codes to that frozen vocabulary.
"""
from __future__ import annotations

from backend.trimet.state_errors import StateError

__all__ = [
    "FRESHNESS_INPUT_CODES",
    "FRESHNESS_RECOVERY_CODES",
    "FreshnessError",
    "FreshnessInputError",
    "FreshnessRecoveryError",
]

FRESHNESS_INPUT_CODES: frozenset[str] = frozenset(
    {
        "invalid_policy",  # FreshnessPolicy/freshness_policy_from_config input
        "invalid_clock",  # supplied provider without a callable .sample()
        "invalid_sample",  # not a validated TimeSample / incoherent pairing
        "invalid_snapshot",  # not a LiveStateSnapshot for the evaluator
        "invalid_cutoff",  # expire_vehicles cutoff outside its validated domain
    }
)

FRESHNESS_RECOVERY_CODES: frozenset[str] = frozenset(
    {
        "clock_epoch_conflict",  # reinitialize under an obsolete/unknown epoch
        "clock_recovery_blocked",  # corrected UTC is below the store cutoff
    }
)

_FRESHNESS_CODES = FRESHNESS_INPUT_CODES | FRESHNESS_RECOVERY_CODES


class FreshnessError(StateError):
    """Base class for every S13 freshness/expiry failure.

    Mirrors :class:`~backend.trimet.state_errors.StateError`: a bounded
    message plus a ``code`` from the fixed S13 vocabulary, exposed
    explicitly so callers never parse exception strings.
    """

    _allowed_codes: frozenset[str] = _FRESHNESS_CODES


class FreshnessInputError(FreshnessError):
    """Malformed freshness/expiry input; nothing was mutated or published.

    Covers policy construction and configuration adaptation, clock
    provider/object validation, time-sample validation and trusted-pairing
    coherence, non-snapshot evaluator input, and an
    :meth:`~backend.trimet.live_state.LiveState.expire_vehicles` cutoff
    outside the plausible UTC-ms domain (including ``bool``).
    """

    _allowed_codes = FRESHNESS_INPUT_CODES


class FreshnessRecoveryError(FreshnessError):
    """An explicit clock recovery was refused; nothing changed.

    ``clock_epoch_conflict`` rejects recovery under an obsolete or
    unknown epoch; ``clock_recovery_blocked`` refuses to restore trust
    when the corrected UTC is below the store's accepted expiry cutoff
    (plan §7).  No anchor, epoch, counter or store state changes.
    """

    _allowed_codes = FRESHNESS_RECOVERY_CODES
