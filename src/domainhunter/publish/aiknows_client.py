"""Versioned, explicit AIKnows draft synchronization contract."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
import json
from time import monotonic
from typing import TYPE_CHECKING, Any

import httpx

from domainhunter.domain.audit import AIKnowsAuditEntry
from domainhunter.domain.candidates import Candidate, CandidateVersion

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from domainhunter.storage.sqlite import SQLiteStore


class SyncStatus(StrEnum):
    SYNCED = "synced"
    VALIDATION_ERROR = "validation_error"
    RECONCILIATION_REQUIRED = "sync_reconciliation_required"
    REVOKED = "revoked"


@dataclass(frozen=True, slots=True)
class SyncResult:
    """Result of one explicit AIKnows draft request, including unknown outcomes."""

    status: SyncStatus
    external_entry_id: str | None = None
    external_version: str | None = None
    publication_status: str | None = None
    field_errors: tuple[str, ...] = ()
    detail: str | None = None


class AIKnowsClient:
    """Send a human-approved candidate version to AIKnows without direct DB access."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        transport: httpx.AsyncBaseTransport | None = None,
        requests_per_second: float | None = None,
        audit_store: SQLiteStore | None = None,
    ) -> None:
        if not base_url.strip() or not token.strip():
            raise ValueError("AIKnows base_url and token must not be empty")
        if requests_per_second is not None and requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive when set")
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=10.0,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )
        self._audit_store = audit_store
        self._rate_lock = asyncio.Lock()
        self._min_interval = (
            1.0 / requests_per_second if requests_per_second is not None else 0.0
        )
        self._last_call_at: float = 0.0

    async def sync_draft(self, candidate: Candidate, version: CandidateVersion) -> SyncResult:
        """Create or update an AIKnows draft exactly once for a human version."""
        if version.candidate_id != candidate.candidate_id:
            raise ValueError("candidate version does not belong to candidate")
        if version.draft.author_kind != "human":
            raise ValueError("only human candidate versions may be published")
        payload = {
            "candidate_id": candidate.candidate_id,
            "candidate_version": version.version,
            "domain": candidate.domain,
            "name": version.draft.name_suggestion,
            "description": version.draft.description_suggestion,
            "category": version.draft.category,
            "tags": list(version.draft.tags),
            "pricing_model": version.draft.pricing_model,
            "target_audience": version.draft.target_audience,
            "evidence": [
                {
                    "type": evidence.evidence_type.value,
                    "quote": evidence.quote,
                    "url": evidence.url,
                }
                for evidence in version.draft.evidence
            ],
        }
        await self._await_token_bucket()
        start = monotonic()
        status_code: int | None = None
        try:
            response = await self._client.post(
                "/v1/domainhunter/drafts",
                json=payload,
                headers={"Idempotency-Key": f"{candidate.candidate_id}:{version.version}"},
            )
            status_code = response.status_code
        except httpx.TimeoutException as error:
            self._record_audit(
                method="POST",
                url="/v1/domainhunter/drafts",
                status_code=None,
                latency_ms=(monotonic() - start) * 1000.0,
                candidate_id=candidate.candidate_id,
                candidate_version=version.version,
            )
            return SyncResult(status=SyncStatus.RECONCILIATION_REQUIRED, detail=str(error))

        latency_ms = (monotonic() - start) * 1000.0
        if response.status_code == 422:
            self._record_audit(
                method="POST",
                url="/v1/domainhunter/drafts",
                status_code=status_code,
                latency_ms=latency_ms,
                candidate_id=candidate.candidate_id,
                candidate_version=version.version,
            )
            return SyncResult(
                status=SyncStatus.VALIDATION_ERROR,
                field_errors=self._field_errors(response),
                detail=response.text,
            )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("external_entry_id"), str):
            self._record_audit(
                method="POST",
                url="/v1/domainhunter/drafts",
                status_code=status_code,
                latency_ms=latency_ms,
                candidate_id=candidate.candidate_id,
                candidate_version=version.version,
            )
            raise ValueError("AIKnows response must contain external_entry_id")
        self._record_audit(
            method="POST",
            url="/v1/domainhunter/drafts",
            status_code=status_code,
            latency_ms=latency_ms,
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
        )
        return SyncResult(
            status=SyncStatus.SYNCED,
            external_entry_id=payload["external_entry_id"],
            external_version=str(payload["external_version"])
            if payload.get("external_version") is not None
            else None,
            publication_status=payload.get("publication_status"),
        )

    async def unpublish(
        self,
        external_entry_id: str,
        external_version: str | None,
    ) -> SyncResult:
        """Delete the AIKnows draft entry and report a single, explicit outcome."""
        if not external_entry_id or not external_entry_id.strip():
            raise ValueError("external_entry_id must not be empty")
        await self._await_token_bucket()
        start = monotonic()
        try:
            response = await self._client.delete(
                f"/v1/domainhunter/drafts/{external_entry_id}",
                headers={
                    "Idempotency-Key": f"unpublish:{external_entry_id}:{external_version or ''}",
                },
            )
        except httpx.TimeoutException as error:
            self._record_audit(
                method="DELETE",
                url=f"/v1/domainhunter/drafts/{external_entry_id}",
                status_code=None,
                latency_ms=(monotonic() - start) * 1000.0,
                candidate_id=None,
                candidate_version=None,
            )
            return SyncResult(
                status=SyncStatus.RECONCILIATION_REQUIRED,
                external_entry_id=external_entry_id,
                external_version=external_version,
                detail=str(error),
            )

        latency_ms = (monotonic() - start) * 1000.0
        self._record_audit(
            method="DELETE",
            url=f"/v1/domainhunter/drafts/{external_entry_id}",
            status_code=response.status_code,
            latency_ms=latency_ms,
            candidate_id=None,
            candidate_version=None,
        )

        if response.status_code == 204 or response.status_code == 200:
            return SyncResult(
                status=SyncStatus.REVOKED,
                external_entry_id=external_entry_id,
                external_version=external_version,
                publication_status="revoked",
            )
        return SyncResult(
            status=SyncStatus.RECONCILIATION_REQUIRED,
            external_entry_id=external_entry_id,
            external_version=external_version,
            detail=f"unpublish returned HTTP {response.status_code}",
        )

    @staticmethod
    def _field_errors(response: httpx.Response) -> tuple[str, ...]:
        try:
            payload: Any = response.json()
        except json.JSONDecodeError:
            return ()
        if not isinstance(payload, dict) or not isinstance(payload.get("field_errors"), list):
            return ()
        return tuple(str(error) for error in payload["field_errors"])

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "AIKnowsClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def _await_token_bucket(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._rate_lock:
            now = monotonic()
            wait = self._min_interval - (now - self._last_call_at)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call_at = monotonic()

    def _record_audit(
        self,
        *,
        method: str,
        url: str,
        status_code: int | None,
        latency_ms: float,
        candidate_id: str | None,
        candidate_version: int | None,
    ) -> None:
        if self._audit_store is None:
            return
        entry = AIKnowsAuditEntry(
            method=method,
            url=url,
            status_code=status_code,
            latency_ms=latency_ms,
            candidate_id=candidate_id,
            candidate_version=candidate_version,
            occurred_at=datetime.now(UTC),
        )
        self._audit_store.append_audit(entry)
