from datetime import UTC, datetime

from fastapi.testclient import TestClient

from webradar_v2.api import create_app
from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.events import SourceEvent
from webradar_v2.domain.observations import Observation, OutcomeCode
from webradar_v2.domain.reviews import ReasonTag
from webradar_v2.domain.review_priority import ReviewPriorityInputs, calculate_review_priority
from webradar_v2.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def _reviewable_candidate(store: SQLiteStore):
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.8,
            name_suggestion="Example AI",
            description_suggestion="AI workflow automation",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
        created_at=OBSERVED_AT,
    )
    store.append_review_priority(
        candidate.candidate_id,
        calculate_review_priority(ReviewPriorityInputs(0.8, 0.8, None, 0.8)),
        calculated_at=OBSERVED_AT,
    )
    return candidate, version


def test_lists_a_reviewable_candidate_with_its_evidence_and_score(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, version = _reviewable_candidate(store)

    response = TestClient(create_app(database)).get("/v1/review-queue")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["candidate_id"] == candidate.candidate_id
    assert item["version"] == version.version
    assert item["priority"]["score"] == 0.64
    assert item["evidence"][0]["quote"] == "Example AI"


def test_review_queue_projects_canonical_and_internal_links_when_present(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, _version = _reviewable_candidate(store)
    event = SourceEvent("ct_log", "argon:43", "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    observation = Observation(
        "example.com",
        OutcomeCode.SUCCESS,
        OBSERVED_AT,
        1,
        canonical_url="https://example.com/",
        internal_links=(
            "https://example.com/about",
            "https://example.com/pricing",
        ),
    )
    store.append_observation(observation)

    response = TestClient(create_app(database)).get("/v1/review-queue")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["canonical_url"] == "https://example.com/"
    assert item["internal_links"] == [
        "https://example.com/about",
        "https://example.com/pricing",
    ]


def test_review_queue_omits_canonical_and_internal_links_when_absent(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    _reviewable_candidate(SQLiteStore(database))

    response = TestClient(create_app(database)).get("/v1/review-queue")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["canonical_url"] is None
    assert item["internal_links"] == []


def test_appends_idempotent_review_decisions_with_an_explicit_actor(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, version = _reviewable_candidate(store)
    client = TestClient(create_app(database))
    body = {
        "request_id": "decision-1",
        "action": "approve",
        "reason_tags": [ReasonTag.INSUFFICIENT_EVIDENCE.value],
    }

    first = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json=body,
        headers={"X-Actor-ID": "reviewer-1"},
    )
    replay = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json=body,
        headers={"X-Actor-ID": "reviewer-1"},
    )

    assert first.status_code == 201
    assert first.json()["created"] is True
    assert replay.status_code == 200
    assert replay.json()["created"] is False
    assert store.is_version_approved(candidate.candidate_id, version.version) is True


def test_serves_a_local_review_console_with_keyboard_actions(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, _version = _reviewable_candidate(store)

    response = TestClient(create_app(database)).get(f"/review/{candidate.candidate_id}")

    assert response.status_code == 200
    assert 'id="candidate-card"' in response.text
    assert "['approve','reject','defer','blocklist']" in response.text
    assert "keydown" in response.text
    assert "/v1/review-queue" in response.text


def test_exposes_an_operational_funnel_snapshot(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    _reviewable_candidate(SQLiteStore(database))

    response = TestClient(create_app(database)).get("/v1/metrics")

    assert response.status_code == 200
    assert response.json()["candidates"] == 1
    assert response.json()["candidate_versions"] == 1


def test_appends_decision_with_known_reason_tag_values(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, version = _reviewable_candidate(store)
    client = TestClient(create_app(database))

    response = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json={
            "request_id": "decision-tagged",
            "action": "reject",
            "reason_tags": [ReasonTag.BLOG.value, ReasonTag.BROKEN_SITE.value],
        },
        headers={"X-Actor-ID": "reviewer-1"},
    )

    assert response.status_code == 201
    decisions = store.list_review_decisions(candidate.candidate_id)
    assert decisions[0].reason_tags == (ReasonTag.BLOG, ReasonTag.BROKEN_SITE)


def test_rejects_decision_with_unknown_reason_tag_value(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    candidate, version = _reviewable_candidate(SQLiteStore(database))
    client = TestClient(create_app(database))

    response = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json={
            "request_id": "decision-bad-tag",
            "action": "reject",
            "reason_tags": ["not_a_real_tag"],
        },
        headers={"X-Actor-ID": "reviewer-1"},
    )

    assert response.status_code == 422
