from datetime import UTC, datetime

from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.review_queue import ReviewQueueItem
from domainhunter.domain.review_priority import ReviewPriorityInputs, calculate_review_priority
from domainhunter.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def _draft(domain: str) -> CandidateVersionDraft:
    return CandidateVersionDraft(
        author_kind="rule",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.8,
        name_suggestion=domain,
        description_suggestion="AI workflow automation",
        evidence=(Evidence(EvidenceType.TITLE, domain, f"https://{domain}"),),
    )


def test_orders_review_queue_by_latest_persisted_priority(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    lower = store.create_candidate("lower-example.com", created_at=OBSERVED_AT)
    higher = store.create_candidate("higher-example.com", created_at=OBSERVED_AT)
    for candidate in (lower, higher):
        store.append_candidate_version(candidate.candidate_id, _draft(candidate.domain), created_at=OBSERVED_AT)
    store.append_review_priority(
        lower.candidate_id,
        calculate_review_priority(ReviewPriorityInputs(0.5, 0.5, None, 0.5)),
        calculated_at=OBSERVED_AT,
    )
    store.append_review_priority(
        higher.candidate_id,
        calculate_review_priority(ReviewPriorityInputs(0.9, 0.9, 1.0, 0.9)),
        calculated_at=OBSERVED_AT,
    )

    queue = store.list_review_queue()

    assert [item.candidate.domain for item in queue] == [
        "higher-example.com",
        "lower-example.com",
    ]
    assert all(isinstance(item, ReviewQueueItem) for item in queue)
    assert queue[0].latest_version.draft.author_kind == "rule"
