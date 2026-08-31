"""Unpublish flow: AIKnows client → PublicationService → API endpoint."""

import asyncio
from datetime import UTC, datetime

import httpx

from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.reviews import ReviewAction, build_review_decision
from webradar_v2.publish.aiknows_client import AIKnowsClient, SyncStatus
from webradar_v2.publish.service import PublicationNotApproved, PublicationService
from webradar_v2.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 17, tzinfo=UTC)


def _seed_human_approved_version(store: SQLiteStore) -> tuple[str, int]:
    """Create the candidate + human version + approval inside one store."""
    candidate = store.create_candidate("example.com", created_at=NOW)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="human",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example AI",
            description_suggestion="An AI workflow helper.",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
        created_at=NOW,
    )
    store.append_review_decision(
        build_review_decision(
            request_id="approve-base",
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=NOW,
        )
    )
    return candidate.candidate_id, version.version


def test_aiknows_unpublish_deletes_the_external_draft_and_returns_revoked() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async def run() -> None:
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await client.unpublish(
                external_entry_id="aik-42",
                external_version="3",
            )

        assert result.status is SyncStatus.REVOKED
        assert requests[0].method == "DELETE"
        assert requests[0].url.path == "/v1/webradar/drafts/aik-42"

    asyncio.run(run())


def test_aiknows_unpublish_marks_timeout_as_reconciliation_required() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("unknown", request=request)

    async def run() -> None:
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await client.unpublish(
                external_entry_id="aik-42",
                external_version=None,
            )

        assert result.status is SyncStatus.RECONCILIATION_REQUIRED

    asyncio.run(run())


def test_publication_service_unpublish_calls_aiknows_and_appends_record(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate_id, version = _seed_human_approved_version(store)

    class FakeClient:
        def __init__(self) -> None:
            self.unpublish_calls: list[tuple[str, str | None]] = []

        async def unpublish(self, external_entry_id, external_version):
            self.unpublish_calls.append((external_entry_id, external_version))
            from webradar_v2.publish.aiknows_client import SyncResult
            return SyncResult(
                status=SyncStatus.REVOKED,
                external_entry_id=external_entry_id,
                external_version=external_version,
                publication_status="revoked",
            )

    client = FakeClient()

    async def run() -> None:
        service = PublicationService(store=store, client=client)
        result = await service.unpublish(
            candidate_id,
            version,
            external_entry_id="aik-42",
            external_version="3",
            requested_at=NOW,
        )
        assert result.status is SyncStatus.REVOKED

    asyncio.run(run())

    assert client.unpublish_calls == [("aik-42", "3")]
    publications = store.list_publications(candidate_id)
    assert len(publications) == 1
    assert publications[0].sync_status is SyncStatus.REVOKED


def test_publication_service_unpublish_requires_approved_human_version(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    # Seed without approval
    candidate = store.create_candidate("example.com", created_at=NOW)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="human",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example AI",
            description_suggestion="An AI workflow helper.",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
        created_at=NOW,
    )

    class FakeClient:
        async def unpublish(self, external_entry_id, external_version):
            raise AssertionError("AIKnows should not be called for an unapproved version")

    client = FakeClient()
    service = PublicationService(store=store, client=client)

    async def run() -> None:
        try:
            await service.unpublish(
                candidate.candidate_id,
                version.version,
                external_entry_id="aik-42",
                external_version="3",
                requested_at=NOW,
            )
        except PublicationNotApproved:
            return
        raise AssertionError("expected PublicationNotApproved")

    asyncio.run(run())


def test_publication_service_unpublish_with_reconciliation_is_recorded(tmp_path) -> None:
    """A timeout during unpublish is recorded as RECONCILIATION_REQUIRED."""
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate_id, version = _seed_human_approved_version(store)

    class ReconClient:
        async def unpublish(self, external_entry_id, external_version):
            from webradar_v2.publish.aiknows_client import SyncResult
            return SyncResult(
                status=SyncStatus.RECONCILIATION_REQUIRED,
                detail="timeout",
                external_entry_id=external_entry_id,
            )

    async def run() -> None:
        service = PublicationService(store=store, client=ReconClient())
        await service.unpublish(
            candidate_id,
            version,
            external_entry_id="aik-42",
            external_version=None,
            requested_at=NOW,
        )

    asyncio.run(run())
    publications = store.list_publications(candidate_id)
    assert publications[0].sync_status is SyncStatus.RECONCILIATION_REQUIRED
    assert publications[0].detail == "timeout"


def test_api_unpublish_endpoint_returns_revoked_status(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from webradar_v2.api import create_app

    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, version = _seed_human_approved_version(store)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    transport = httpx.MockTransport(handler)
    aiknows_client = AIKnowsClient(
        base_url="https://aiknows.example.test",
        token="service-token",
        transport=transport,
    )
    publish_service = PublicationService(store=store, client=aiknows_client)

    app = create_app(database, publish_service=publish_service)
    test_client = TestClient(app)
    response = test_client.post(
        f"/v1/candidates/{candidate_id}/versions/{version}/unpublish",
        json={
            "request_id": "unpublish-1",
            "external_entry_id": "aik-42",
            "external_version": "3",
            "reason": "no longer applies",
        },
        headers={"X-Actor-ID": "auditor-1"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["revoked"] is True
    assert body["external_entry_id"] == "aik-42"