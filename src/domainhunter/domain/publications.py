"""Append-only records for explicit external draft synchronization."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from domainhunter.publish.aiknows_client import SyncResult, SyncStatus


class PublicationStatus(StrEnum):
    SYNCED = "synced"
    VALIDATION_ERROR = "validation_error"
    RECONCILIATION_REQUIRED = "sync_reconciliation_required"
    REVOKED = "revoked"


@dataclass(frozen=True, slots=True)
class PublicationRecord:
    """One external synchronization attempt, including unknown timeout outcomes."""

    candidate_id: str
    candidate_version: int
    requested_at: datetime
    sync_status: SyncStatus
    external_entry_id: str | None = None
    external_version: str | None = None
    publication_status: str | None = None
    field_errors: tuple[str, ...] = ()
    detail: str | None = None

    def __post_init__(self) -> None:
        if not self.candidate_id or self.candidate_version < 1:
            raise ValueError("candidate version must be specified")
        if self.requested_at.tzinfo is None:
            raise ValueError("requested_at must be timezone-aware")
        if any(not error.strip() for error in self.field_errors):
            raise ValueError("field_errors must not contain empty values")

    @classmethod
    def from_sync_result(
        cls,
        *,
        candidate_id: str,
        candidate_version: int,
        requested_at: datetime,
        result: SyncResult,
    ) -> "PublicationRecord":
        return cls(
            candidate_id=candidate_id,
            candidate_version=candidate_version,
            requested_at=requested_at,
            sync_status=result.status,
            external_entry_id=result.external_entry_id,
            external_version=result.external_version,
            publication_status=result.publication_status,
            field_errors=result.field_errors,
            detail=result.detail,
        )
