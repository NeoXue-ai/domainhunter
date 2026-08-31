import asyncio
from datetime import UTC, datetime

import pytest

from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.reviews import ReviewAction, build_review_decision
from webradar_v2.publish.aiknows_client import SyncResult, SyncStatus
from webradar_v2.publish.service import PublicationNotApproved, PublicationService
from webradar_v2.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


class FakeAIKnowsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    async def sync_draft(self, candidate, version) -> SyncResult:
        self.calls.append((candidate.candidate_id, version.version))
        return SyncResult(
            status=SyncStatus.SYNCED,
            external_entry_id="aik-42",
            external_version="3",
            publication_status="draft",
        )


def _human_version(store: SQLiteStore):
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
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
        created_at=OBSERVED_AT,
    )
    return candidate, version


def test_rejects_unapproved_or_nonhuman_versions_without_calling_aiknows(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate, version = _human_version(store)
    client = FakeAIKnowsClient()

    async def run() -> None:
        service = PublicationService(store=store, client=client)
        with pytest.raises(PublicationNotApproved):
            await service.sync_approved_version(
                candidate.candidate_id, version.version, requested_at=OBSERVED_AT
            )

    asyncio.run(run())
    assert client.calls == []


def test_syncs_an_approved_human_version_and_appends_the_external_result(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate, version = _human_version(store)
    store.append_review_decision(
        build_review_decision(
            request_id="approve-1",
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=OBSERVED_AT,
        )
    )
    client = FakeAIKnowsClient()

    async def run() -> SyncResult:
        service = PublicationService(store=store, client=client)
        return await service.sync_approved_version(
            candidate.candidate_id, version.version, requested_at=OBSERVED_AT
        )

    result = asyncio.run(run())

    assert result.status is SyncStatus.SYNCED
    assert client.calls == [(candidate.candidate_id, version.version)]
    assert store.list_publications(candidate.candidate_id)[0].external_entry_id == "aik-42"
