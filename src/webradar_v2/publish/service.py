"""Authorization boundary for explicit AIKnows draft synchronization."""

from datetime import datetime
from typing import Protocol

from webradar_v2.publish.aiknows_client import SyncResult
from webradar_v2.storage.sqlite import SQLiteStore
from webradar_v2.domain.publications import PublicationRecord


class DraftSyncClient(Protocol):
    async def sync_draft(self, candidate: object, version: object) -> SyncResult: ...

    async def unpublish(
        self, external_entry_id: str, external_version: str | None
    ) -> SyncResult: ...


class PublicationNotApproved(PermissionError):
    """Raised when a candidate version has not received an active human approval."""


class PublicationService:
    """Sync only the explicitly approved, human-authored candidate version."""

    def __init__(self, *, store: SQLiteStore, client: DraftSyncClient) -> None:
        self._store = store
        self._client = client

    async def sync_approved_version(
        self, candidate_id: str, candidate_version: int, *, requested_at: datetime
    ) -> SyncResult:
        candidate = self._store.get_candidate(candidate_id)
        version = self._store.get_candidate_version(candidate_id, candidate_version)
        if candidate is None or version is None:
            raise ValueError("candidate version does not exist")
        if version.draft.author_kind != "human" or not self._store.is_version_approved(
            candidate_id, candidate_version
        ):
            raise PublicationNotApproved("only an approved human version may be synchronized")

        result = await self._client.sync_draft(candidate, version)
        self._store.append_publication(
            PublicationRecord.from_sync_result(
                candidate_id=candidate_id,
                candidate_version=candidate_version,
                requested_at=requested_at,
                result=result,
            )
        )
        return result

    async def unpublish(
        self,
        candidate_id: str,
        candidate_version: int,
        *,
        external_entry_id: str,
        external_version: str | None,
        requested_at: datetime,
    ) -> SyncResult:
        """Withdraw a previously synced draft and append one immutable record."""
        version = self._store.get_candidate_version(candidate_id, candidate_version)
        if version is None:
            raise ValueError("candidate version does not exist")
        if version.draft.author_kind != "human" or not self._store.is_version_approved(
            candidate_id, candidate_version
        ):
            raise PublicationNotApproved(
                "only an approved human version may be unpublished",
            )

        result = await self._client.unpublish(external_entry_id, external_version)
        self._store.append_publication(
            PublicationRecord.from_sync_result(
                candidate_id=candidate_id,
                candidate_version=candidate_version,
                requested_at=requested_at,
                result=result,
            )
        )
        return result
