from datetime import UTC, datetime, timedelta

import pytest

from webradar_v2.domain.observations import OutcomeCode
from webradar_v2.domain.retry_policy import decide_next_action


NOW = datetime(2026, 8, 16, tzinfo=UTC)


def test_dns_not_found_retries_after_six_hours() -> None:
    decision = decide_next_action(OutcomeCode.DNS_NOT_FOUND, attempt_number=1, now=NOW)
    assert decision.next_check_at == NOW + timedelta(hours=6)
    assert decision.terminal_status is None


def test_not_ready_becomes_terminal_after_seven_days() -> None:
    decision = decide_next_action(
        OutcomeCode.CONTENT_INSUFFICIENT,
        attempt_number=3,
        now=NOW,
    )
    assert decision.next_check_at == NOW + timedelta(days=7)

    terminal = decide_next_action(
        OutcomeCode.CONTENT_INSUFFICIENT,
        attempt_number=4,
        now=NOW + timedelta(days=7),
    )
    assert terminal.next_check_at is None
    assert terminal.terminal_status == "not_ready"


def test_robots_is_never_retried_automatically() -> None:
    decision = decide_next_action(OutcomeCode.ROBOTS_DISALLOWED, attempt_number=1, now=NOW)
    assert decision.next_check_at is None
    assert decision.terminal_status == "access_restricted"


def test_http_rate_limit_is_not_retried_automatically() -> None:
    decision = decide_next_action(OutcomeCode.HTTP_429, attempt_number=1, now=NOW)

    assert decision.next_check_at is None
    assert decision.terminal_status == "access_restricted"


def test_rejects_zero_attempt_even_for_terminal_outcomes() -> None:
    with pytest.raises(ValueError, match="attempt_number must be positive"):
        decide_next_action(OutcomeCode.ROBOTS_DISALLOWED, attempt_number=0, now=NOW)
