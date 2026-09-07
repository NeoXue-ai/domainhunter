"""Small local FastAPI surface for the human review workflow."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from dataclasses import asdict
from hashlib import sha256
import json
from pathlib import Path
from typing import Protocol

from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from domainhunter.domain.claims import build_claim_token
from domainhunter.domain.contacts import extract_public_contacts
from domainhunter.domain.observations import Observation, OutcomeCode
from domainhunter.domain.outreach import OutreachEvent
from domainhunter.domain.reopens import build_reopen_event
from domainhunter.domain.reviews import ReasonTag, ReviewAction, build_review_decision
from domainhunter.domain.verification import CandidateVerification
from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.ingest.ct_log_adapter import CTLogFetcher, DEFAULT_LOG
from domainhunter.ingest.ct_orchestrator import CTIngestOrchestrator
from domainhunter.ingest.ct_poller import CTPoller
from domainhunter.pipeline import DomainHunterPipeline
from domainhunter.publish.claim_service import ClaimService
from domainhunter.publish.service import PublicationService
from domainhunter.storage.sqlite import ConcurrentDecisionError, SQLiteStore


_FRONTEND_DIR = Path(__file__).with_name("static")


class ReviewDecisionRequest(BaseModel):
    request_id: str = Field(min_length=1)
    action: ReviewAction
    reason_tags: tuple[ReasonTag, ...] = ()


class RevokeRequest(BaseModel):
    request_id: str = Field(min_length=1)
    reason: str = ""


class UnpublishRequest(BaseModel):
    request_id: str = Field(min_length=1)
    external_entry_id: str = Field(min_length=1)
    external_version: str | None = None
    reason: str = ""


class OutreachRequest(BaseModel):
    request_id: str = Field(min_length=1)
    dry_run: bool = True
    recipient_source_url: str | None = None


class ReopenRequest(BaseModel):
    request_id: str = Field(min_length=1)
    trigger_source_event_id: str = Field(min_length=1)
    new_outcome: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class BudgetUpdateRequest(BaseModel):
    daily_limit: float = Field(gt=0.0)


class StagePauseRequest(BaseModel):
    stage: str = Field(min_length=1)
    paused: bool
    reason: str = Field(min_length=1)



class DiscoveryRunRequest(BaseModel):
    max_probes: int = Field(default=20, ge=1, le=200)


class ContactPageFetcher(Protocol):
    """Fetch the HTML body for a public contact/about page."""

    def fetch(self, url: str) -> str: ...


class _BuiltinContactPageFetcher:
    """Default fetcher that blocks the explicit no-network policy in tests.

    Real HTTP is intentionally avoided; a fetcher is injected in tests.
    """

    def fetch(self, url: str) -> str:
        raise RuntimeError(
            "no live contact fetcher is configured; inject one through "
            "create_app(..., contact_fetcher=...)"
        )


def build_default_contact_fetcher() -> ContactPageFetcher:
    """Return the conservative no-network default used in production."""
    return _BuiltinContactPageFetcher()


def _newness_payload(
    verification: CandidateVerification | None,
) -> dict[str, object]:
    """Expose only the persisted CT/RDAP facts needed to assess newness."""
    if verification is None:
        return {
            "status": "unknown",
            "checked_at": None,
            "ct_first_seen_at": None,
            "rdap_tier": None,
            "rdap_age_days": None,
            "rdap_registration_at": None,
        }
    is_proven = (
        verification.ct_first_seen_at is not None
        and verification.rdap_tier in {"tier1", "tier2"}
    )
    is_failed = verification.rdap_tier == "unknown"
    return {
        "status": "passed" if is_proven else "failed" if is_failed else "unknown",
        "checked_at": verification.checked_at.isoformat(),
        "ct_first_seen_at": (
            verification.ct_first_seen_at.isoformat()
            if verification.ct_first_seen_at is not None
            else None
        ),
        "rdap_tier": verification.rdap_tier,
        "rdap_age_days": verification.rdap_age_days,
        "rdap_registration_at": (
            verification.rdap_registration_at.isoformat()
            if verification.rdap_registration_at is not None
            else None
        ),
    }


def _reachability_payload(
    verification: CandidateVerification | None,
) -> dict[str, object]:
    """Expose final-route facts without treating an absent check as success."""
    if verification is None:
        return {
            "status": "unknown",
            "http_status_code": None,
            "final_url": None,
            "canonical_url": None,
            "same_root": None,
        }
    return {
        "status": (
            "passed"
            if verification.final_root_matches is True
            else "failed"
            if verification.final_root_matches is False
            else "unknown"
        ),
        "http_status_code": verification.http_status_code,
        "final_url": verification.final_url,
        "canonical_url": verification.canonical_url,
        "same_root": verification.final_root_matches,
    }


def _review_projection(
    *,
    candidate: object,
    version: object,
    priority: object | None,
    verification: CandidateVerification | None,
    review_state: str,
    canonical_url: str | None = None,
    internal_links: tuple[str, ...] = (),
) -> dict[str, object]:
    """Build the shared inbox/detail representation from stored facts only."""
    draft = version.draft
    score = priority.priority if priority is not None else None
    final_canonical_url = (
        verification.canonical_url
        if verification is not None and verification.canonical_url is not None
        else canonical_url
    )
    return {
        "candidate_id": candidate.candidate_id,
        "domain": candidate.domain,
        "version": version.version,
        "author_kind": draft.author_kind,
        "primary_outcome": draft.primary_outcome.value,
        "classification_confidence": draft.classification_confidence,
        "name_suggestion": draft.name_suggestion,
        "description_suggestion": draft.description_suggestion,
        "evidence": [
            {
                "type": evidence.evidence_type.value,
                "quote": evidence.quote,
                "url": evidence.url,
            }
            for evidence in draft.evidence
        ],
        "canonical_url": final_canonical_url,
        "internal_links": list(internal_links),
        "priority": (
            {
                "score": score.score,
                "formula_version": score.formula_version,
                "product_evidence_contribution": score.product_evidence_contribution,
                "early_presence_contribution": score.early_presence_contribution,
                "low_exposure_contribution": score.low_exposure_contribution,
                "data_completeness_contribution": score.data_completeness_contribution,
            }
            if score is not None
            else None
        ),
        "review_state": review_state,
        "newness": _newness_payload(verification),
        "reachability": _reachability_payload(verification),
    }


def create_app(
    database_path: str | Path,
    *,
    contact_fetcher: ContactPageFetcher | None = None,
    now: Callable[[], datetime] | None = None,
    publish_service: PublicationService | None = None,
) -> FastAPI:
    """Create a process-local review API backed by the supplied SQLite database."""
    store = SQLiteStore(database_path)
    app = FastAPI(title="DomainHunter Review API", version="0.1.0")
    app.mount("/assets", StaticFiles(directory=_FRONTEND_DIR / "assets"), name="assets")
    fetcher = contact_fetcher or build_default_contact_fetcher()
    claim_service = ClaimService(store=store)
    clock = now or (lambda: datetime.now(UTC))
    publish = publish_service

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def queue_console() -> FileResponse:
        """Serve the focused candidate inbox."""
        return FileResponse(_FRONTEND_DIR / "index.html")

    @app.get("/review/{candidate_id}", response_class=HTMLResponse, include_in_schema=False)
    def review_console(candidate_id: str) -> FileResponse:
        """Single-candidate review page: hero, evidence, gauge, decision buttons."""
        return FileResponse(_FRONTEND_DIR / "index.html")

    @app.get("/discovery", response_class=HTMLResponse, include_in_schema=False)
    def discovery_console() -> RedirectResponse:
        """Preserve old bookmarks while keeping the inbox as the only entry point."""
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    @app.get("/ops", response_class=HTMLResponse, include_in_schema=False)
    def ops_console() -> RedirectResponse:
        """Preserve old bookmarks while keeping the inbox as the only entry point."""
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/v1/run/discovery")
    async def run_discovery(payload: DiscoveryRunRequest) -> dict[str, object]:
        """Run one CT log discovery pass end-to-end."""
        try:
            async with CTLogFetcher(logs=(DEFAULT_LOG,)) as fetcher:
                poller = CTPoller(store=store, fetch_page=fetcher)
                async with HTTPProbe() as probe:
                    pipeline = DomainHunterPipeline(store=store, probe=probe)
                    strict_filter = FilterPipeline(
                        tier1_days=30,
                        tier2_days=90,
                        require_dns=True,
                        drop_unknown_rdap=True,
                    )
                    orchestrator = CTIngestOrchestrator(
                        store=store,
                        poller=poller,
                        pipeline=pipeline,
                        probe_limit=payload.max_probes,
                        filter_pipeline=strict_filter,
                        require_first_seen=True,
                    )
                    summary = await orchestrator.run_once()
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"discovery run failed: {error}",
            ) from error
        return {
            "status": (
                "completed"
                if summary.candidates_created > 0
                else "queued"
                if summary.pending_work > 0
                else "no_candidates"
            ),
            "next_cursor": summary.next_cursor,
            "certificates_seen": summary.certificates_seen,
            "events_added": summary.events_added,
            "roots_observed": summary.roots_observed,
            "strict_rejections": summary.strict_rejections,
            "probes_run": summary.probes_run,
            "candidates_created": summary.candidates_created,
            "source_errors": list(summary.source_errors),
            "pending_work": summary.pending_work,
        }

    @app.get("/v1/discovery/overview")
    def discovery_overview() -> dict[str, object]:
        """Return a compact view of discovered domains and their latest probe state."""
        metrics = store.funnel_metrics()
        domains: list[dict[str, object]] = []
        for domain in store.list_domains():
            observations = store.list_observations(domain)
            latest = observations[-1] if observations else None
            with store._connection() as connection:  # noqa: SLF001 — local read-only view
                row = connection.execute(
                    "SELECT first_seen_at FROM domains WHERE domain = ?",
                    (domain,),
                ).fetchone()
            domains.append(
                {
                    "domain": domain,
                    "first_seen_at": row["first_seen_at"] if row else None,
                    "observed_at": (
                        latest.observed_at.isoformat() if latest is not None else None
                    ),
                    "outcome_code": (
                        latest.outcome_code.value if latest is not None else None
                    ),
                    "attempt_number": (
                        latest.attempt_number if latest is not None else None
                    ),
                    "status_code": latest.status_code if latest is not None else None,
                    "final_url": latest.final_url if latest is not None else None,
                    "detail": latest.detail if latest is not None else None,
                }
            )
        return {
            "cursor": store.get_source_cursor("ct_log"),
            "domains": domains,
            "counts": {
                "source_events": metrics.source_events,
                "domains": metrics.domains,
                "observations": metrics.observations,
                "candidates": metrics.candidates,
                "candidate_versions": metrics.candidate_versions,
                "review_queue": len(store.list_review_queue()),
            },
        }

    @app.get("/v1/review-queue")
    def list_review_queue() -> dict[str, object]:
        """List only candidate versions that have no active human decision."""
        items: list[dict[str, object]] = []
        for item in store.list_review_queue():
            candidate_id = item.candidate.candidate_id
            candidate_version = item.latest_version.version
            if store.active_review_action(candidate_id, candidate_version) is not None:
                continue
            items.append(
                _review_projection(
                    candidate=item.candidate,
                    version=item.latest_version,
                    priority=item.priority,
                    verification=store.get_candidate_verification(
                        candidate_id, candidate_version
                    ),
                    review_state="pending",
                    canonical_url=item.canonical_url,
                    internal_links=item.internal_links,
                )
            )
        return {"items": items}

    @app.get("/v1/candidates/{candidate_id}/versions/{candidate_version}/review-context")
    def candidate_review_context(
        candidate_id: str, candidate_version: int
    ) -> dict[str, object]:
        """Return the immutable facts and decision history for one review page."""
        candidate = store.get_candidate(candidate_id)
        version = store.get_candidate_version(candidate_id, candidate_version)
        if candidate is None or version is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="candidate version not found",
            )
        active_action = store.active_review_action(candidate_id, candidate_version)
        payload = _review_projection(
            candidate=candidate,
            version=version,
            priority=store.latest_review_priority(candidate_id),
            verification=store.get_candidate_verification(candidate_id, candidate_version),
            review_state=active_action.value if active_action is not None else "pending",
        )
        payload["audit"] = {
            "decisions": [
                {
                    "decision_id": decision.decision_id,
                    "action": decision.action.value,
                    "actor_id": decision.actor_id,
                    "decided_at": decision.decided_at.isoformat(),
                    "reason_tags": [tag.value for tag in decision.reason_tags],
                    "revoked": store.is_decision_revoked(decision.decision_id),
                }
                for decision in store.list_review_decisions(candidate_id)
                if decision.candidate_version == candidate_version
            ]
        }
        return payload

    @app.get("/v1/candidates/{candidate_id}/review-context")
    def latest_candidate_review_context(candidate_id: str) -> dict[str, object]:
        """Return the latest version's context for the independent detail route."""
        candidate = store.get_candidate(candidate_id)
        versions = store.list_candidate_versions(candidate_id)
        if candidate is None or not versions:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="candidate not found",
            )
        version = versions[-1]
        active_action = store.active_review_action(candidate_id, version.version)
        latest_observation = store._latest_observation(candidate.domain)  # noqa: SLF001
        payload = _review_projection(
            candidate=candidate,
            version=version,
            priority=store.latest_review_priority(candidate_id),
            verification=store.get_candidate_verification(candidate_id, version.version),
            review_state=active_action.value if active_action is not None else "pending",
            canonical_url=(
                latest_observation.canonical_url if latest_observation is not None else None
            ),
            internal_links=(
                latest_observation.internal_links if latest_observation is not None else ()
            ),
        )
        payload["audit"] = {
            "decisions": [
                {
                    "decision_id": decision.decision_id,
                    "action": decision.action.value,
                    "actor_id": decision.actor_id,
                    "decided_at": decision.decided_at.isoformat(),
                    "reason_tags": [tag.value for tag in decision.reason_tags],
                    "revoked": store.is_decision_revoked(decision.decision_id),
                }
                for decision in store.list_review_decisions(candidate_id)
                if decision.candidate_version == version.version
            ]
        }
        return payload

    @app.get("/v1/audit/aiknows")
    def list_aiknows_audit(
        candidate_id: str | None = None,
        limit: int = 200,
    ) -> dict[str, object]:
        rows = store.list_audit(candidate_id=candidate_id, limit=limit)
        return {
            "count": len(rows),
            "entries": [
                {
                    "method": row.method,
                    "url": row.url,
                    "status_code": row.status_code,
                    "latency_ms": row.latency_ms,
                    "candidate_id": row.candidate_id,
                    "candidate_version": row.candidate_version,
                    "occurred_at": row.occurred_at.isoformat(),
                }
                for row in rows
            ],
        }

    @app.post(
        "/v1/candidates/{candidate_id}/versions/{candidate_version}/decisions",
        status_code=status.HTTP_201_CREATED,
    )
    def append_review_decision(
        candidate_id: str,
        candidate_version: int,
        payload: ReviewDecisionRequest,
        response: Response,
        actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    ) -> dict[str, object]:
        if not actor_id or not actor_id.strip():
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-Actor-ID is required")
        if store.get_candidate_version(candidate_id, candidate_version) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="candidate version not found")
        decision = build_review_decision(
            request_id=payload.request_id,
            candidate_id=candidate_id,
            candidate_version=candidate_version,
            action=payload.action,
            actor_id=actor_id,
            decided_at=datetime.now(UTC),
            reason_tags=payload.reason_tags,
        )
        try:
            created = store.append_review_decision(decision)
        except ConcurrentDecisionError as conflict:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "detail": "concurrent decision conflict",
                    "active_decision_id": conflict.active_decision_id,
                    "active_request_id": conflict.active_request_id,
                },
            ) from conflict
        if not created:
            response.status_code = status.HTTP_200_OK
        return {"created": created, "decision_id": decision.decision_id}

    @app.post(
        "/v1/candidates/{candidate_id}/versions/{candidate_version}/decisions/{decision_id}/revoke"
    )
    def revoke_review_decision(
        candidate_id: str,
        candidate_version: int,
        decision_id: str,
        payload: RevokeRequest,
        actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    ) -> dict[str, object]:
        if not actor_id or not actor_id.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-Actor-ID is required",
            )
        if store.get_candidate_version(candidate_id, candidate_version) is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="candidate version not found",
            )
        revoked = store.revoke_review_decision(
            decision_id,
            actor_id=actor_id,
            revoked_at=datetime.now(UTC),
            reason=payload.reason,
        )
        return {"revoked": revoked, "decision_id": decision_id}

    @app.post("/v1/candidates/{candidate_id}/versions/{candidate_version}/unpublish")
    def unpublish_endpoint(
        candidate_id: str,
        candidate_version: int,
        payload: UnpublishRequest,
        actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    ) -> dict[str, object]:
        if not actor_id or not actor_id.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-Actor-ID is required",
            )
        if publish is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="AIKnows publication service is not configured",
            )
        from domainhunter.publish.aiknows_client import SyncStatus

        try:
            import asyncio

            result = asyncio.run(
                publish.unpublish(
                    candidate_id,
                    candidate_version,
                    external_entry_id=payload.external_entry_id,
                    external_version=payload.external_version,
                    requested_at=clock(),
                )
            )
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"unpublish failed: {error}",
            ) from error

        return {
            "revoked": result.status is SyncStatus.REVOKED,
            "sync_status": result.status.value,
            "external_entry_id": result.external_entry_id,
            "external_version": result.external_version,
            "detail": result.detail,
        }

    @app.post(
        "/v1/candidates/{candidate_id}/versions/{candidate_version}/outreach",
        status_code=status.HTTP_201_CREATED,
    )
    def trigger_outreach(
        candidate_id: str,
        candidate_version: int,
        payload: OutreachRequest,
        actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    ) -> dict[str, object]:
        if not actor_id or not actor_id.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-Actor-ID is required",
            )
        version = store.get_candidate_version(candidate_id, candidate_version)
        if version is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="candidate version not found",
            )
        if version.draft.author_kind != "human" or not store.is_version_approved(
            candidate_id, candidate_version
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="only an approved human version may trigger outreach",
            )

        triggered_at = clock()
        recipient_url = payload.recipient_source_url
        contact_extraction = None
        if recipient_url is not None:
            try:
                html = fetcher.fetch(recipient_url)
            except Exception as error:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"contact fetch failed: {error}",
                ) from error
            contact_extraction = extract_public_contacts(html, source_url=recipient_url)

        preview_payload: dict[str, object]
        tokens_issued = 0

        if payload.dry_run:
            if contact_extraction is None:
                _preview_raw, preview_record = build_claim_token(
                    candidate_id=candidate_id,
                    created_at=triggered_at,
                    expires_at=triggered_at + timedelta(days=7),
                )
                preview_payload = {
                    "issued_token_hash_preview": preview_record.token_hash,
                    "recipient_source_url": None,
                    "contact_preview": [],
                    "status": "dry_run",
                }
            else:
                _preview_raw, preview_record = build_claim_token(
                    candidate_id=candidate_id,
                    created_at=triggered_at,
                    expires_at=triggered_at + timedelta(days=7),
                )
                preview_payload = {
                    "issued_token_hash_preview": preview_record.token_hash,
                    "recipient_source_url": recipient_url,
                    "contact_preview": [
                        {
                            "redacted_address": contact.redacted_address,
                            "source_url": contact.source_url,
                        }
                        for contact in contact_extraction.contacts
                    ],
                    "status": "dry_run",
                }
        else:
            if contact_extraction is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="recipient_source_url is required for a non-dry-run trigger",
                )
            for _ in contact_extraction.contacts:
                claim_service.issue_token(
                    candidate_id,
                    candidate_version,
                    actor_id=actor_id,
                    ttl_days=7,
                    now=triggered_at,
                )
                tokens_issued += 1
            preview_payload = {
                "tokens_issued": tokens_issued,
                "contact_preview": [
                    {
                        "redacted_address": contact.redacted_address,
                        "source_url": contact.source_url,
                    }
                    for contact in contact_extraction.contacts
                ],
                "status": "real_run",
            }

        contact_count = 0 if contact_extraction is None else len(contact_extraction.contacts)
        contact_preview_json = json.dumps(
            preview_payload["contact_preview"], ensure_ascii=False
        )
        store.append_outreach_event(
            OutreachEvent(
                candidate_id=candidate_id,
                candidate_version=candidate_version,
                actor_id=actor_id,
                triggered_at=triggered_at,
                dry_run=payload.dry_run,
                recipient_source_url=recipient_url,
                claim_tokens_issued=tokens_issued,
                contact_count=contact_count,
                contact_preview_json=contact_preview_json,
            )
        )
        return preview_payload

    @app.post(
        "/v1/candidates/{candidate_id}/reopen",
        status_code=status.HTTP_201_CREATED,
    )
    def reopen_candidate(
        candidate_id: str,
        payload: ReopenRequest,
        response: Response,
        actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    ) -> dict[str, object]:
        if not actor_id or not actor_id.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-Actor-ID is required",
            )

        try:
            new_outcome = OutcomeCode(payload.new_outcome)
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"unknown outcome_code: {payload.new_outcome}",
            ) from error

        candidate = store.get_candidate(candidate_id)
        if candidate is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="candidate not found",
            )

        if not store.source_event_links_to_domain(payload.trigger_source_event_id, candidate.domain):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "trigger_source_event_id does not exist for this candidate's domain"
                ),
            )

        candidate_reopen_id = sha256(
            f"reopen\0{candidate_id}\0{payload.trigger_source_event_id}\0{payload.request_id}".encode(
                "utf-8"
            )
        ).hexdigest()
        existing = next(
            (
                item
                for item in store.list_reopen_events(candidate_id)
                if item.reopen_id == candidate_reopen_id
            ),
            None,
        )
        if existing is not None:
            response.status_code = status.HTTP_200_OK
            return {
                "created": False,
                "reopen_id": existing.reopen_id,
                "candidate_id": existing.candidate_id,
                "trigger_source_event_id": existing.trigger_source_event_id,
                "previous_outcome": existing.previous_outcome.value,
                "new_outcome": existing.new_outcome.value,
                "actor_id": existing.actor_id,
                "reason": existing.reason,
                "reopened_at": existing.reopened_at.isoformat(),
            }

        latest_observations = store.list_observations(candidate.domain)
        latest_observation = latest_observations[-1] if latest_observations else None
        if latest_observation is None or not store.is_reopenable(candidate.domain):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="candidate is not in a terminal state",
            )

        reopened_at = clock()
        event = build_reopen_event(
            request_id=payload.request_id,
            candidate_id=candidate_id,
            trigger_source_event_id=payload.trigger_source_event_id,
            previous_outcome=latest_observation.outcome_code,
            new_outcome=new_outcome,
            actor_id=actor_id,
            reason=payload.reason,
            reopened_at=reopened_at,
        )
        created = store.append_reopen_event(event)
        if created:
            new_observation = Observation(
                domain=candidate.domain,
                outcome_code=new_outcome,
                observed_at=reopened_at,
                attempt_number=1,
                detail=f"reopen:{event.reopen_id}",
            )
            store.append_observation(new_observation)
        return {
            "created": created,
            "reopen_id": event.reopen_id,
            "candidate_id": event.candidate_id,
            "trigger_source_event_id": event.trigger_source_event_id,
            "previous_outcome": event.previous_outcome.value,
            "new_outcome": event.new_outcome.value,
            "actor_id": event.actor_id,
            "reason": event.reason,
            "reopened_at": event.reopened_at.isoformat(),
        }

    return app
