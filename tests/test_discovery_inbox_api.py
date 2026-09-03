from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from domainhunter.api import create_app
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.reviews import ReviewAction, build_review_decision
from domainhunter.domain.review_priority import (
    ReviewPriorityInputs,
    calculate_review_priority,
)
from domainhunter.domain.verification import CandidateVerification
from domainhunter.storage.sqlite import SQLiteStore


NOW = datetime(2026, 9, 3, tzinfo=UTC)


def _candidate(
    store: SQLiteStore,
    domain: str,
    *,
    with_verification: bool,
) -> tuple[str, int]:
    candidate = store.create_candidate(domain, created_at=NOW)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.82,
            name_suggestion="Newsite AI",
            description_suggestion="An AI workflow product",
            evidence=(
                Evidence(
                    EvidenceType.TITLE,
                    "Newsite AI automates workflows",
                    f"https://{domain}/",
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
    if with_verification:
        store.append_candidate_verification(
            CandidateVerification(
                candidate_id=candidate.candidate_id,
                candidate_version=version.version,
                checked_at=NOW,
                ct_first_seen_at=NOW - timedelta(hours=1),
                rdap_tier="tier1",
                rdap_age_days=2,
                rdap_registration_at=NOW - timedelta(days=2),
                dns_has_a=True,
                http_status_code=200,
                final_url=f"https://{domain}/",
                canonical_url=f"https://{domain}/",
                final_root_matches=True,
            )
        )
    return candidate.candidate_id, version.version


def _seeded_client(tmp_path) -> tuple[TestClient, str, str, int]:
    database = tmp_path / "inbox.db"
    store = SQLiteStore(database)
    pending_id, _pending_version = _candidate(
        store, "pending.ai", with_verification=True
    )
    approved_id, approved_version = _candidate(
        store, "approved.ai", with_verification=True
    )
    store.append_review_decision(
        build_review_decision(
            request_id="approved-once",
            candidate_id=approved_id,
            candidate_version=approved_version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=NOW,
        )
    )
    return TestClient(create_app(database)), pending_id, approved_id, approved_version


def test_inbox_returns_only_pending_candidates_with_truthful_summaries(tmp_path) -> None:
    client, pending_id, _approved_id, _approved_version = _seeded_client(tmp_path)

    response = client.get("/v1/review-queue")

    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["candidate_id"] for item in items] == [pending_id]
    item = items[0]
    assert item["review_state"] == "pending"
    assert item["newness"]["status"] == "passed"
    assert item["newness"]["rdap_age_days"] == 2
    assert item["reachability"] == {
        "status": "passed",
        "http_status_code": 200,
        "final_url": "https://pending.ai/",
        "canonical_url": "https://pending.ai/",
        "same_root": True,
    }


def test_candidate_context_marks_missing_verification_unknown(tmp_path) -> None:
    database = tmp_path / "unknown.db"
    store = SQLiteStore(database)
    candidate_id, version = _candidate(
        store, "unknown.ai", with_verification=False
    )
    client = TestClient(create_app(database))

    response = client.get(
        f"/v1/candidates/{candidate_id}/versions/{version}/review-context"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["review_state"] == "pending"
    assert payload["newness"]["status"] == "unknown"
    assert payload["reachability"]["status"] == "unknown"
    assert payload["evidence"] == [
        {
            "type": "title",
            "quote": "Newsite AI automates workflows",
            "url": "https://unknown.ai/",
        }
    ]
    assert payload["audit"]["decisions"] == []


def test_candidate_context_returns_not_found_for_an_unknown_version(tmp_path) -> None:
    client, pending_id, _approved_id, _approved_version = _seeded_client(tmp_path)

    response = client.get(
        f"/v1/candidates/{pending_id}/versions/99/review-context"
    )

    assert response.status_code == 404
