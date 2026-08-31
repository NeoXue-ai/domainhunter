from datetime import UTC, datetime

from fastapi.testclient import TestClient

from webradar_v2.api import create_app
from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.reviews import ReviewAction, build_review_decision
from webradar_v2.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 16, tzinfo=UTC)


class FakeContactPageFetcher:
    """Return a fixed HTML body for any URL the outreach endpoint requests."""

    def __init__(self, html: str, *, url: str = "https://example.com/contact") -> None:
        self._html = html
        self._url = url
        self.calls: list[str] = []

    def fetch(self, url: str) -> str:
        self.calls.append(url)
        return self._html


def _approved_human_version(store: SQLiteStore) -> tuple[str, int]:
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
    store.append_review_decision(
        build_review_decision(
            request_id="approve-outreach",
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=NOW,
        )
    )
    return candidate.candidate_id, version.version


def _rule_version(store: SQLiteStore) -> tuple[str, int]:
    candidate = store.create_candidate("example.com", created_at=NOW)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.5,
            name_suggestion="Example",
            description_suggestion="auto",
            evidence=(Evidence(EvidenceType.TITLE, "Example", "https://example.com"),),
        ),
        created_at=NOW,
    )
    return candidate.candidate_id, version.version


def test_outreach_requires_actor_id(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_human_version(store)
    client = TestClient(create_app(database))

    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/outreach",
        json={"request_id": "no-actor", "dry_run": True},
    )

    assert response.status_code == 401


def test_outreach_requires_approval(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_human_version(store)
    # Revoke the approval before recording the conflicting reject; spec §11
    # forbids appending a second decision with a different request_id while an
    # active one is still in place.
    approve = store.list_review_decisions(candidate_id)[0]
    assert store.revoke_review_decision(
        approve.decision_id,
        actor_id="reviewer-1",
        revoked_at=NOW,
        reason="reconsider",
    )
    store.append_review_decision(
        build_review_decision(
            request_id="reject-after-approve",
            candidate_id=candidate_id,
            candidate_version=version,
            action=ReviewAction.REJECT,
            actor_id="reviewer-1",
            decided_at=NOW,
        )
    )
    client = TestClient(create_app(database))

    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/outreach",
        json={"request_id": "not-approved", "dry_run": True},
        headers={"X-Actor-ID": "actor-1"},
    )

    assert response.status_code == 403


def test_outreach_requires_human_version(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _rule_version(store)
    store.append_review_decision(
        build_review_decision(
            request_id="approve-rule",
            candidate_id=candidate_id,
            candidate_version=version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=NOW,
        )
    )
    client = TestClient(create_app(database))

    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/outreach",
        json={"request_id": "not-human", "dry_run": True},
        headers={"X-Actor-ID": "actor-1"},
    )

    assert response.status_code == 403


def test_outreach_dry_run_returns_preview_without_persisting_token(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_human_version(store)
    fetcher = FakeContactPageFetcher(
        "<p>Reach founders@example.com</p>",
        url="https://example.com/contact",
    )
    client = TestClient(create_app(database, contact_fetcher=fetcher))

    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/outreach",
        json={
            "request_id": "dry-run-1",
            "dry_run": True,
            "recipient_source_url": "https://example.com/contact",
        },
        headers={"X-Actor-ID": "actor-1"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "dry_run"
    assert body["recipient_source_url"] == "https://example.com/contact"
    assert body["issued_token_hash_preview"]
    assert len(body["contact_preview"]) == 1
    assert body["contact_preview"][0]["redacted_address"] == "f*******@example.com"
    events = store.list_outreach_events(candidate_id)
    assert len(events) == 1
    assert events[0].dry_run is True
    assert events[0].claim_tokens_issued == 0


def test_outreach_real_run_persists_claim_tokens_and_outreach_event(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_human_version(store)
    fetcher = FakeContactPageFetcher(
        "<p>founders@example.com sales@example.com</p>",
        url="https://example.com/contact",
    )
    client = TestClient(create_app(database, contact_fetcher=fetcher))

    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/outreach",
        json={
            "request_id": "real-run-1",
            "dry_run": False,
            "recipient_source_url": "https://example.com/contact",
        },
        headers={"X-Actor-ID": "actor-1"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "real_run"
    assert body["tokens_issued"] == 2
    assert len(body["contact_preview"]) == 2
    events = store.list_outreach_events(candidate_id)
    assert len(events) == 1
    assert events[0].dry_run is False
    assert events[0].claim_tokens_issued == 2
    assert events[0].contact_count == 2


def test_outreach_extracts_and_redacts_contacts_from_source_url(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_human_version(store)
    fetcher = FakeContactPageFetcher(
        "<p>founders@example.com</p><p>support@example.com</p>",
        url="https://example.com/about",
    )
    client = TestClient(create_app(database, contact_fetcher=fetcher))

    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/outreach",
        json={
            "request_id": "redact-1",
            "dry_run": True,
            "recipient_source_url": "https://example.com/about",
        },
        headers={"X-Actor-ID": "actor-1"},
    )

    assert response.status_code == 201
    body = response.json()
    redacted = [entry["redacted_address"] for entry in body["contact_preview"]]
    assert redacted == ["f*******@example.com", "s******@example.com"]
    assert fetcher.calls == ["https://example.com/about"]
