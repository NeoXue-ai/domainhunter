from datetime import UTC, datetime

from fastapi.testclient import TestClient

from domainhunter.api import create_app
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.review_priority import (
    ReviewPriorityInputs,
    calculate_review_priority,
)
from domainhunter.storage.sqlite import SQLiteStore


NOW = datetime(2026, 9, 3, tzinfo=UTC)


def _reviewable_client(tmp_path) -> tuple[TestClient, str]:
    database = tmp_path / "ui.db"
    store = SQLiteStore(database)
    candidate = store.create_candidate("newsite.ai", created_at=NOW)
    store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.8,
            name_suggestion="Newsite AI",
            description_suggestion="A real AI workflow product",
            evidence=(
                Evidence(
                    EvidenceType.TITLE,
                    "Newsite AI automates workflows",
                    "https://newsite.ai/",
                ),
            ),
        ),
        created_at=NOW,
    )
    store.append_review_priority(
        candidate.candidate_id,
        calculate_review_priority(ReviewPriorityInputs(0.9, 0.8, None, 0.8)),
        calculated_at=NOW,
    )
    return TestClient(create_app(database)), candidate.candidate_id


def test_home_is_inbox_not_dashboard(tmp_path) -> None:
    response = TestClient(create_app(tmp_path / "ui.db")).get("/")

    assert response.status_code == 200
    assert 'id="root"' in response.text
    assert '/assets/' in response.text


def test_inbox_renders_a_linked_candidate_card(tmp_path) -> None:
    client, candidate_id = _reviewable_client(tmp_path)

    response = client.get("/")

    assert response.status_code == 200
    assert 'id="root"' in response.text
    assert '/assets/' in response.text
