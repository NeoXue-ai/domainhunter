"""Regression tests for QA-flagged bugs.

Each test ties to an issue in
    .gstack/qa-reports/qa-report-127-0-0-1-8000-2026-08-31.md
and pins the fix so it cannot silently break again.
"""

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from domainhunter.api import create_app
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.outreach import OutreachEvent
from domainhunter.domain.reviews import ReviewAction, build_review_decision
from domainhunter.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 16, tzinfo=UTC)


# Regression: ISSUE-001 — outreach stage strip was wired to candidate_versions
# Found by /qa on 2026-08-31
# Report: .gstack/qa-reports/qa-report-127-0-0-1-8000-2026-08-31.md


def _seed_approved_human_version(store: SQLiteStore) -> tuple[str, int]:
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
            request_id="approve-regression",
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=NOW,
        )
    )
    return candidate.candidate_id, version.version


def test_funnel_metrics_exposes_outreach_events_count(tmp_path) -> None:
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    candidate_id, version = _seed_approved_human_version(store)

    store.append_outreach_event(
        OutreachEvent(
            candidate_id=candidate_id,
            candidate_version=version,
            actor_id="reviewer-1",
            triggered_at=NOW,
            dry_run=True,
            recipient_source_url="https://example.com/contact",
            claim_tokens_issued=0,
            contact_count=1,
            contact_preview_json="[]",
        )
    )
    store.append_outreach_event(
        OutreachEvent(
            candidate_id=candidate_id,
            candidate_version=version,
            actor_id="reviewer-1",
            triggered_at=NOW,
            dry_run=False,
            recipient_source_url="https://example.com/contact",
            claim_tokens_issued=1,
            contact_count=1,
            contact_preview_json="[]",
        )
    )

    metrics = store.funnel_metrics()

    assert metrics.outreach_events == 2


def test_metrics_endpoint_exposes_outreach_events_field(tmp_path) -> None:
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    candidate_id, version = _seed_approved_human_version(store)
    # Second candidate so candidate_versions and outreach_events diverge.
    other = store.create_candidate("other.com", created_at=NOW)
    store.append_candidate_version(
        other.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.5,
            name_suggestion="Other",
            description_suggestion="auto",
            evidence=(Evidence(EvidenceType.TITLE, "Other", "https://other.com"),),
        ),
        created_at=NOW,
    )

    store.append_outreach_event(
        OutreachEvent(
            candidate_id=candidate_id,
            candidate_version=version,
            actor_id="reviewer-1",
            triggered_at=NOW,
            dry_run=True,
            recipient_source_url="https://example.com/contact",
            claim_tokens_issued=0,
            contact_count=1,
            contact_preview_json="[]",
        )
    )

    client = TestClient(create_app(database))
    response = client.get("/v1/metrics")

    assert response.status_code == 200
    payload = response.json()
    assert payload["outreach_events"] == 1
    assert payload["candidate_versions"] == 2
    # Candidate versions must not bleed into outreach reporting.
    assert payload["outreach_events"] != payload["candidate_versions"]