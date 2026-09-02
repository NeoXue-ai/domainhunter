import asyncio
from datetime import UTC, datetime

import httpx

from domainhunter.domain.candidates import (
    Candidate,
    CandidateOutcome,
    CandidateVersion,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.publish.aiknows_client import AIKnowsClient, SyncStatus


NOW = datetime(2026, 8, 16, tzinfo=UTC)


def _candidate_version() -> tuple[Candidate, CandidateVersion]:
    candidate = Candidate("candidate-1", "example.com", NOW)
    version = CandidateVersion(
        candidate_id=candidate.candidate_id,
        version=2,
        created_at=NOW,
        draft=CandidateVersionDraft(
            author_kind="human",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example AI",
            description_suggestion="An AI writing assistant.",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
    )
    return candidate, version


def test_syncs_a_human_approved_draft_with_candidate_version_idempotency() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            201,
            json={
                "external_entry_id": "aik-42",
                "external_version": "3",
                "publication_status": "draft",
            },
        )

    async def run() -> None:
        candidate, version = _candidate_version()
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await client.sync_draft(candidate, version)

        assert result.status is SyncStatus.SYNCED
        assert result.external_entry_id == "aik-42"
        assert requests[0].headers["idempotency-key"] == "candidate-1:2"
        assert requests[0].headers["authorization"] == "Bearer service-token"
        assert requests[0].url.path == "/v1/domainhunter/drafts"

    asyncio.run(run())


def test_marks_timeout_for_reconciliation_instead_of_blind_retry() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("unknown result", request=request)

    async def run() -> None:
        candidate, version = _candidate_version()
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await client.sync_draft(candidate, version)

        assert result.status is SyncStatus.RECONCILIATION_REQUIRED
        assert result.external_entry_id is None

    asyncio.run(run())


def test_returns_field_validation_errors_without_creating_a_public_entry() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"field_errors": ["category is invalid"]})

    async def run() -> None:
        candidate, version = _candidate_version()
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await client.sync_draft(candidate, version)

        assert result.status is SyncStatus.VALIDATION_ERROR
        assert result.field_errors == ("category is invalid",)

    asyncio.run(run())


def test_handles_a_non_json_validation_response_without_losing_the_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, text="invalid request")

    async def run() -> None:
        candidate, version = _candidate_version()
        async with AIKnowsClient(
            base_url="https://aiknows.example.test",
            token="service-token",
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await client.sync_draft(candidate, version)

        assert result.status is SyncStatus.VALIDATION_ERROR
        assert result.field_errors == ()

    asyncio.run(run())
