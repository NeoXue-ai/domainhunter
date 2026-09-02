"""AIKnows audit log + per-second token-bucket rate limiting."""

import asyncio
import time
from datetime import UTC, datetime

import httpx

from domainhunter.domain.audit import AIKnowsAuditEntry
from domainhunter.publish.aiknows_client import AIKnowsClient
from domainhunter.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 17, tzinfo=UTC)


def _payload() -> dict[str, object]:
    return {
        "external_entry_id": "aik-42",
        "external_version": "3",
        "publication_status": "draft",
    }


def test_append_audit_writes_one_row_per_call(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    entry = AIKnowsAuditEntry(
        method="POST",
        url="/v1/domainhunter/drafts",
        status_code=201,
        latency_ms=42.0,
        candidate_id="candidate-1",
        candidate_version=1,
        occurred_at=NOW,
    )

    assert store.append_audit(entry) is True
    assert store.append_audit(entry) is True  # audit rows are append-only
    rows = store.list_audit(candidate_id="candidate-1")
    assert len(rows) == 2
    assert rows[0].method == "POST"
    assert rows[0].status_code == 201


def test_list_audit_filters_by_candidate_id(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    for cid, version in (("candidate-1", 1), ("candidate-1", 2), ("candidate-2", 1)):
        store.append_audit(
            AIKnowsAuditEntry(
                method="POST",
                url="/v1/domainhunter/drafts",
                status_code=201,
                latency_ms=10.0,
                candidate_id=cid,
                candidate_version=version,
                occurred_at=NOW,
            )
        )

    rows = store.list_audit(candidate_id="candidate-1")
    assert {row.candidate_version for row in rows} == {1, 2}
    assert all(row.candidate_id == "candidate-1" for row in rows)


def test_token_bucket_sleeps_between_consecutive_calls() -> None:
    """Two sync calls must be spaced by at least the configured interval."""
    timestamps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        timestamps.append(time.monotonic())
        return httpx.Response(201, json=_payload())

    async def run() -> None:
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
            requests_per_second=5.0,  # 200ms interval
        ) as client:
            from domainhunter.domain.candidates import (
                Candidate,
                CandidateOutcome,
                CandidateVersion,
                CandidateVersionDraft,
                Evidence,
                EvidenceType,
            )

            candidate = Candidate("candidate-1", "example.com", NOW)
            version = CandidateVersion(
                candidate_id=candidate.candidate_id,
                version=1,
                created_at=NOW,
                draft=CandidateVersionDraft(
                    author_kind="human",
                    primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
                    classification_confidence=0.9,
                    name_suggestion="Example AI",
                    description_suggestion="An AI workflow helper.",
                    evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
                ),
            )
            await client.sync_draft(candidate, version)
            await client.sync_draft(candidate, version)

    start = time.monotonic()
    asyncio.run(run())
    assert len(timestamps) == 2
    gap = timestamps[1] - timestamps[0]
    # 200ms interval minus a small jitter tolerance.
    assert gap >= 0.15, f"expected a >=150ms gap, got {gap:.3f}s"
    # And not absurdly slow (the bucket should not stall beyond 1s for 5 rps).
    assert gap < 1.0, f"expected the bucket to stay under 1s, got {gap:.3f}s"
    assert timestamps[1] - start < 1.0


def test_rate_limit_does_not_apply_when_unset() -> None:
    """When requests_per_second is None, no sleep is injected."""
    timestamps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        timestamps.append(time.monotonic())
        return httpx.Response(204)

    async def run() -> None:
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
        ) as client:
            await client.unpublish("aik-42", "3")
            await client.unpublish("aik-43", "4")

    asyncio.run(run())
    assert len(timestamps) == 2
    # Two consecutive DELETE calls must be back-to-back.
    assert (timestamps[1] - timestamps[0]) < 0.05


def test_aiknows_audit_endpoint_returns_recent_rows_for_a_candidate(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from domainhunter.api import create_app

    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    for i in range(3):
        store.append_audit(
            AIKnowsAuditEntry(
                method="POST",
                url="/v1/domainhunter/drafts",
                status_code=201,
                latency_ms=10.0 + i,
                candidate_id="candidate-1",
                candidate_version=1,
                occurred_at=NOW,
            )
        )
    store.append_audit(
        AIKnowsAuditEntry(
            method="POST",
            url="/v1/domainhunter/drafts",
            status_code=201,
            latency_ms=99.0,
            candidate_id="candidate-2",
            candidate_version=1,
            occurred_at=NOW,
        )
    )

    response = TestClient(create_app(database)).get(
        "/v1/audit/aiknows", params={"candidate_id": "candidate-1"}
    )

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 3
    assert len(body["entries"]) == 3
    assert all(entry["candidate_id"] == "candidate-1" for entry in body["entries"])