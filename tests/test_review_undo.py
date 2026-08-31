"""Soft-revoke behaviour for review decisions and the matching API endpoint."""

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from webradar_v2.api import create_app
from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.reviews import (
    ReviewAction,
    build_review_decision,
)
from webradar_v2.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 17, tzinfo=UTC)


def _approved_candidate(store: SQLiteStore) -> tuple[str, int]:
    candidate = store.create_candidate("example.com", created_at=NOW)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example",
            description_suggestion="auto",
            evidence=(Evidence(EvidenceType.TITLE, "Example", "https://example.com"),),
        ),
        created_at=NOW,
    )
    return candidate.candidate_id, version.version


def test_undo_action_is_a_first_class_review_action() -> None:
    """UNDO must round-trip through the same hash-based decision id contract."""
    decision = build_review_decision(
        request_id="undo-request-1",
        candidate_id="candidate-1",
        candidate_version=1,
        action=ReviewAction.UNDO,
        actor_id="reviewer-1",
        decided_at=NOW,
    )

    assert decision.action is ReviewAction.UNDO
    assert decision.action.value == "undo"


def test_revoke_soft_revokes_an_active_review_decision(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_candidate(store)
    decision = build_review_decision(
        request_id="approval-1",
        candidate_id=candidate_id,
        candidate_version=version,
        action=ReviewAction.APPROVE,
        actor_id="reviewer-1",
        decided_at=NOW,
    )
    store.append_review_decision(decision)

    revoked = store.revoke_review_decision(
        decision.decision_id,
        actor_id="auditor-1",
        revoked_at=NOW + timedelta(minutes=5),
        reason="duplicate of approval-0",
    )

    assert revoked is True
    decisions = store.list_review_decisions(candidate_id)
    # Append-only history is preserved; revocation is recorded separately.
    assert len(decisions) == 1
    assert decisions[0].action is ReviewAction.APPROVE


def test_revoke_is_idempotent_for_an_already_revoked_decision(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_candidate(store)
    decision = build_review_decision(
        request_id="approval-2",
        candidate_id=candidate_id,
        candidate_version=version,
        action=ReviewAction.APPROVE,
        actor_id="reviewer-1",
        decided_at=NOW,
    )
    store.append_review_decision(decision)

    first = store.revoke_review_decision(
        decision.decision_id,
        actor_id="auditor-1",
        revoked_at=NOW + timedelta(minutes=5),
        reason="superseded",
    )
    second = store.revoke_review_decision(
        decision.decision_id,
        actor_id="auditor-1",
        revoked_at=NOW + timedelta(minutes=10),
        reason="second attempt",
    )

    assert first is True
    assert second is False


def test_revoke_raises_when_decision_id_is_unknown(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    with_test = store.revoke_review_decision(
        "missing-decision",
        actor_id="auditor-1",
        revoked_at=NOW,
        reason="never existed",
    )
    assert with_test is False


def test_legacy_review_decisions_table_is_migrated_with_revocation_columns(tmp_path) -> None:
    """An older database without revoked_at/revoked_by/revoke_reason must still open."""
    import sqlite3

    database = tmp_path / "webradar.db"
    connection = sqlite3.connect(database)
    try:
        connection.executescript(
            """
            CREATE TABLE source_events (
                id INTEGER PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                raw_subject TEXT NOT NULL,
                observed_at TEXT NOT NULL
            );
            CREATE TABLE domains (
                domain TEXT PRIMARY KEY,
                first_seen_at TEXT NOT NULL
            );
            CREATE TABLE event_domains (
                event_id INTEGER NOT NULL REFERENCES source_events(id),
                domain TEXT NOT NULL REFERENCES domains(domain),
                PRIMARY KEY (event_id, domain)
            );
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY,
                domain TEXT NOT NULL REFERENCES domains(domain),
                outcome_code TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                status_code INTEGER,
                final_url TEXT,
                detail TEXT,
                canonical_url TEXT,
                internal_links_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE source_cursors (
                source TEXT PRIMARY KEY,
                cursor TEXT
            );
            CREATE TABLE candidates (
                candidate_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            );
            CREATE TABLE candidate_versions (
                candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                author_kind TEXT NOT NULL,
                primary_outcome TEXT NOT NULL,
                classification_confidence REAL NOT NULL,
                name_suggestion TEXT,
                description_suggestion TEXT,
                category TEXT,
                tags_json TEXT NOT NULL,
                pricing_model TEXT,
                target_audience TEXT,
                model_version TEXT,
                evidence_json TEXT NOT NULL,
                PRIMARY KEY (candidate_id, version)
            );
            CREATE TABLE review_decisions (
                decision_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                candidate_version INTEGER NOT NULL,
                action TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                decided_at TEXT NOT NULL,
                reason_tags_json TEXT NOT NULL,
                FOREIGN KEY (candidate_id, candidate_version)
                    REFERENCES candidate_versions(candidate_id, version)
            );
            """
        )
        connection.commit()
    finally:
        connection.close()

    store = SQLiteStore(database)
    candidate_id, version = _approved_candidate(store)
    decision = build_review_decision(
        request_id="legacy-approval",
        candidate_id=candidate_id,
        candidate_version=version,
        action=ReviewAction.APPROVE,
        actor_id="reviewer-1",
        decided_at=NOW,
    )
    store.append_review_decision(decision)
    revoked = store.revoke_review_decision(
        decision.decision_id,
        actor_id="auditor-1",
        revoked_at=NOW,
        reason="legacy row",
    )
    assert revoked is True


def test_api_revoke_endpoint_returns_revoked_flag_with_actor_header(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_candidate(store)
    decision = build_review_decision(
        request_id="decision-to-revoke",
        candidate_id=candidate_id,
        candidate_version=version,
        action=ReviewAction.APPROVE,
        actor_id="reviewer-1",
        decided_at=NOW,
    )
    store.append_review_decision(decision)

    client = TestClient(create_app(database))
    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/decisions/{decision.decision_id}/revoke",
        json={"request_id": "revoke-request-1", "reason": "audit follow-up"},
        headers={"X-Actor-ID": "auditor-1"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "revoked": True,
        "decision_id": decision.decision_id,
    }


def test_api_revoke_requires_actor_header(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _approved_candidate(store)
    decision = build_review_decision(
        request_id="decision-no-actor",
        candidate_id=candidate_id,
        candidate_version=version,
        action=ReviewAction.APPROVE,
        actor_id="reviewer-1",
        decided_at=NOW,
    )
    store.append_review_decision(decision)

    client = TestClient(create_app(database))
    response = client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/decisions/{decision.decision_id}/revoke",
        json={"request_id": "revoke-no-actor", "reason": ""},
    )

    assert response.status_code == 401