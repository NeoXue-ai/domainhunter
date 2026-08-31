from datetime import UTC, datetime, timedelta

import pytest

from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.reviews import ReviewAction, build_review_decision
from webradar_v2.publish.claim_service import ClaimNotApproved, ClaimService
from webradar_v2.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 16, tzinfo=UTC)


def _human_version(store: SQLiteStore):
    candidate = store.create_candidate("example.com", created_at=NOW)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="human",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example AI",
            description_suggestion="AI workflow automation",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
        created_at=NOW,
    )
    return candidate, version


def test_issues_claim_token_only_for_an_approved_human_version(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate, version = _human_version(store)
    service = ClaimService(store=store)

    with pytest.raises(ClaimNotApproved):
        service.issue_approved_claim(
            candidate.candidate_id,
            version.version,
            created_at=NOW,
            expires_at=NOW + timedelta(days=7),
        )

    store.append_review_decision(
        build_review_decision(
            request_id="approve-claim",
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=NOW,
        )
    )
    raw_token = service.issue_approved_claim(
        candidate.candidate_id,
        version.version,
        created_at=NOW,
        expires_at=NOW + timedelta(days=7),
    )

    assert len(raw_token) >= 32
