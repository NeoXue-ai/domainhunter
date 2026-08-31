from datetime import UTC, datetime

import pytest

from webradar_v2.domain.reviews import ReasonTag, ReviewAction, build_review_decision


NOW = datetime(2026, 8, 16, tzinfo=UTC)


def test_builds_a_replay_safe_review_decision_for_one_candidate_version() -> None:
    first = build_review_decision(
        request_id="review-request-42",
        candidate_id="candidate-1",
        candidate_version=2,
        action=ReviewAction.APPROVE,
        actor_id="reviewer@example.com",
        decided_at=NOW,
    )
    replay = build_review_decision(
        request_id="review-request-42",
        candidate_id="candidate-1",
        candidate_version=2,
        action=ReviewAction.APPROVE,
        actor_id="reviewer@example.com",
        decided_at=NOW,
    )

    assert first.decision_id == replay.decision_id
    assert first.action is ReviewAction.APPROVE


def test_reason_tags_must_be_enum_members() -> None:
    with pytest.raises(ValueError, match="reason_tags must be ReasonTag members"):
        build_review_decision(
            request_id="review-request-bad-tags",
            candidate_id="candidate-1",
            candidate_version=1,
            action=ReviewAction.REJECT,
            actor_id="reviewer@example.com",
            decided_at=NOW,
            reason_tags=("not_a_real_tag",),
        )


def test_review_decision_stores_reason_tag_values() -> None:
    decision = build_review_decision(
        request_id="review-request-tags",
        candidate_id="candidate-1",
        candidate_version=1,
        action=ReviewAction.REJECT,
        actor_id="reviewer@example.com",
        decided_at=NOW,
        reason_tags=(ReasonTag.BLOG, ReasonTag.BROKEN_SITE),
    )

    assert decision.reason_tags == (ReasonTag.BLOG, ReasonTag.BROKEN_SITE)
    assert all(isinstance(tag, ReasonTag) for tag in decision.reason_tags)
