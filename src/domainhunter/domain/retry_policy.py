"""Deterministic retry decisions for typed observation outcomes."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from .observations import OutcomeCode


@dataclass(frozen=True, slots=True)
class RetryDecision:
    """The next action for an observation, with no hidden retry behavior."""

    next_check_at: datetime | None
    terminal_status: str | None


_DNS_SCHEDULE = (timedelta(hours=6), timedelta(hours=24), timedelta(hours=72))
_NETWORK_SCHEDULE = (
    timedelta(hours=1),
    timedelta(hours=6),
    timedelta(hours=24),
    timedelta(hours=72),
)
_CONTENT_SCHEDULE = (
    timedelta(hours=24),
    timedelta(hours=72),
    timedelta(days=7),
)


def _scheduled(
    schedule: tuple[timedelta, ...], attempt_number: int, now: datetime, terminal: str
) -> RetryDecision:
    if attempt_number < 1:
        raise ValueError("attempt_number must be positive")
    if attempt_number <= len(schedule):
        return RetryDecision(next_check_at=now + schedule[attempt_number - 1], terminal_status=None)
    return RetryDecision(next_check_at=None, terminal_status=terminal)


def decide_next_action(
    outcome_code: OutcomeCode, *, attempt_number: int, now: datetime
) -> RetryDecision:
    """Return the approved bounded retry or terminal decision."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if attempt_number < 1:
        raise ValueError("attempt_number must be positive")

    if outcome_code is OutcomeCode.DNS_NOT_FOUND:
        return _scheduled(_DNS_SCHEDULE, attempt_number, now, "inactive")
    if outcome_code in {OutcomeCode.HTTP_4XX, OutcomeCode.CONTENT_INSUFFICIENT}:
        return _scheduled(_CONTENT_SCHEDULE, attempt_number, now, "not_ready")
    if outcome_code in {
        OutcomeCode.DNS_TIMEOUT,
        OutcomeCode.TLS_ERROR,
        OutcomeCode.CONNECT_TIMEOUT,
        OutcomeCode.HTTP_5XX,
        OutcomeCode.RENDER_TIMEOUT,
        OutcomeCode.REDIRECT_LOOP,
    }:
        return _scheduled(_NETWORK_SCHEDULE, attempt_number, now, "unreachable")
    if outcome_code in {OutcomeCode.ROBOTS_DISALLOWED, OutcomeCode.HTTP_429}:
        return RetryDecision(next_check_at=None, terminal_status="access_restricted")
    if outcome_code is OutcomeCode.BLOCKED_SSRF:
        return RetryDecision(next_check_at=None, terminal_status="blocked_network")
    if outcome_code is OutcomeCode.SUCCESS:
        return RetryDecision(next_check_at=None, terminal_status=None)
    raise ValueError(f"unsupported outcome code: {outcome_code}")
