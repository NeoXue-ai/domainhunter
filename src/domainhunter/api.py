"""Small local FastAPI surface for the human review workflow."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from dataclasses import asdict
from hashlib import sha256
import json
from pathlib import Path
from typing import Protocol

from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

from domainhunter.domain.claims import build_claim_token
from domainhunter.domain.contacts import extract_public_contacts
from domainhunter.domain.observations import Observation, OutcomeCode
from domainhunter.domain.outreach import OutreachEvent
from domainhunter.domain.reopens import build_reopen_event
from domainhunter.domain.reviews import ReasonTag, ReviewAction, build_review_decision
from domainhunter.domain.verification import CandidateVerification
from domainhunter.domain.work_queue import WorkStage
from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.ingest.ct_log_adapter import CTLogFetcher, DEFAULT_LOG
from domainhunter.ingest.ct_orchestrator import CTIngestOrchestrator
from domainhunter.ingest.ct_poller import CTPoller
from domainhunter.pipeline import DomainHunterPipeline
from domainhunter.publish.claim_service import ClaimService
from domainhunter.publish.service import PublicationService
from domainhunter.storage.sqlite import ConcurrentDecisionError, SQLiteStore


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
    fetcher = contact_fetcher or build_default_contact_fetcher()
    claim_service = ClaimService(store=store)
    clock = now or (lambda: datetime.now(UTC))
    publish = publish_service

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def queue_console() -> str:
        """Serve the focused candidate inbox."""
        return _inbox_page()

    @app.get("/review/{candidate_id}", response_class=HTMLResponse, include_in_schema=False)
    def review_console(candidate_id: str) -> str:
        """Single-candidate review page: hero, evidence, gauge, decision buttons."""
        return _review_page(candidate_id)

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
                "completed" if summary.candidates_created > 0 else "no_candidates"
            ),
            "next_cursor": summary.next_cursor,
            "certificates_seen": summary.certificates_seen,
            "events_added": summary.events_added,
            "roots_observed": summary.roots_observed,
            "strict_rejections": summary.strict_rejections,
            "probes_run": summary.probes_run,
            "candidates_created": summary.candidates_created,
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

    @app.get("/v1/metrics")
    def funnel_metrics() -> dict[str, object]:
        payload = asdict(store.funnel_metrics())
        payload["analytics"] = store.compute_funnel_analytics().as_payload()
        return payload

    @app.get("/v1/analytics")
    def funnel_analytics() -> dict[str, object]:
        return store.compute_funnel_analytics().as_payload()

    @app.get("/v1/alerts")
    def list_alerts(since: str | None = None) -> dict[str, object]:
        since_dt: datetime | None = None
        if since is not None:
            try:
                since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"invalid `since` timestamp: {since}",
                ) from exc
            if since_dt.tzinfo is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="`since` must include a timezone offset",
                )
        rows = store.list_alerts(since=since_dt)
        return {
            "count": len(rows),
            "alerts": [
                {
                    "alert_id": row.alert_id,
                    "kind": row.kind.value,
                    "severity": row.severity.value,
                    "title": row.title,
                    "summary": row.summary,
                    "opened_at": row.opened_at.isoformat(),
                    "runbook_id": row.runbook_id,
                    "related_candidate_id": row.related_candidate_id,
                    "related_stage": row.related_stage,
                    "payload": dict(row.payload),
                }
                for row in rows
            ],
        }

    @app.get("/v1/runbooks")
    def list_runbooks() -> dict[str, object]:
        from domainhunter.scheduler.runbooks import list_runbooks

        return {
            "count": len(list_runbooks()),
            "runbooks": [
                {"runbook_id": rb.runbook_id, "title": rb.title}
                for rb in list_runbooks()
            ],
        }

    @app.get("/v1/runbooks/{runbook_id}")
    def get_runbook(runbook_id: str) -> dict[str, object]:
        from domainhunter.scheduler.runbooks import get_runbook_by_id

        runbook = get_runbook_by_id(runbook_id)
        if runbook is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown runbook_id: {runbook_id}",
            )
        return {
            "runbook_id": runbook.runbook_id,
            "kind": runbook.kind.value,
            "title": runbook.title,
            "steps": list(runbook.steps),
        }

    @app.get("/v1/operations/budget")
    def list_operations_budget() -> dict[str, object]:
        """Return the persisted daily_limit per stage, with default fallbacks."""

        budgets: list[dict[str, object]] = []
        for stage in WorkStage:
            row = store.get_budget_config_row(stage)
            if row is not None:
                stage_value, daily_limit, updated_at, updated_by = row
                budgets.append(
                    {
                        "stage": stage_value,
                        "daily_limit": daily_limit,
                        "updated_at": updated_at.isoformat(),
                        "updated_by": updated_by,
                    }
                )
            else:
                limits = store.get_budget_config()
                budgets.append(
                    {
                        "stage": stage.value,
                        "daily_limit": float(limits[stage]),
                        "updated_at": None,
                        "updated_by": None,
                    }
                )
        return {"budgets": budgets}

    @app.put("/v1/operations/budget/{stage}")
    def update_operations_budget(
        stage: str,
        payload: BudgetUpdateRequest,
        actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    ) -> dict[str, object]:
        if not actor_id or not actor_id.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-Actor-ID is required",
            )
        try:
            stage_enum = WorkStage(stage)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown stage: {stage}",
            ) from exc
        if payload.daily_limit <= 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="daily_limit must be positive",
            )
        updated_at = clock()
        store.set_budget_config(
            stage_enum,
            daily_limit=payload.daily_limit,
            updated_by=actor_id,
            occurred_at=updated_at,
        )
        return {
            "stage": stage_enum.value,
            "daily_limit": payload.daily_limit,
            "updated_at": updated_at.isoformat(),
            "updated_by": actor_id,
        }

    @app.get("/v1/operations/pauses")
    def list_operations_pauses() -> dict[str, object]:
        """Return the latest pause row per stage, with all stages present by default."""

        pauses_by_stage = {
            stage: (stage, False, "", "", datetime.fromtimestamp(0, tz=UTC))
            for stage in WorkStage
        }
        for entry in store.list_stage_pauses():
            stage_enum, paused, reason, actor_id, paused_at = entry
            pauses_by_stage[stage_enum] = entry
        ordered = tuple(
            pauses_by_stage[stage] for stage in WorkStage
        )
        return {
            "pauses": [
                {
                    "stage": stage.value,
                    "paused": paused,
                    "reason": reason,
                    "actor_id": actor_id,
                    "paused_at": paused_at.isoformat(),
                }
                for stage, paused, reason, actor_id, paused_at in ordered
            ]
        }

    @app.post("/v1/operations/pauses", status_code=status.HTTP_201_CREATED)
    def post_operations_pauses(
        payload: StagePauseRequest,
        response: Response,
        actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    ) -> dict[str, object]:
        if not actor_id or not actor_id.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-Actor-ID is required",
            )
        try:
            stage_enum = WorkStage(payload.stage)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown stage: {payload.stage}",
            ) from exc
        paused_at = clock()
        inserted = store.set_stage_pause(
            stage_enum,
            paused=payload.paused,
            reason=payload.reason,
            actor_id=actor_id,
            paused_at=paused_at,
        )
        body = {
            "stage": stage_enum.value,
            "paused": payload.paused,
            "reason": payload.reason,
            "actor_id": actor_id,
            "paused_at": paused_at.isoformat(),
            "created": inserted,
        }
        if not inserted:
            response.status_code = status.HTTP_200_OK
        return body

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


# ----- v3: split into 4 focused pages -----
# Each page composes: _PAGE_HEAD + topbar (active tab highlighted) + body + shared script + page script
_PAGE_HEAD = r"""<!doctype html>
<html lang="zh" data-lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>DomainHunter</title><style>
:root{
  color-scheme:dark;
  --bg-0:#0B0E12;--bg-1:#11151B;--bg-2:#161B23;--bg-3:#1D2330;
  --line-1:#1F2530;--line-2:#2A3140;--line-3:#3A4356;
  --ink-1:#E8ECF1;--ink-2:#9AA4B2;--ink-3:#5C6675;--ink-4:#3F4754;
  --accent:#D4A574;--accent-soft:#E5C399;--accent-ink:#1A1410;
  --good:#6FCF97;--warn:#E8B547;--bad:#E07B7B;--info:#7BB8FF;--outreach:#7FD4C0;--muted:#6F7A89;
  --font-sans:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  --font-mono:ui-monospace,"SF Mono","JetBrains Mono","Cascadia Code",Consolas,monospace;
  --fs-display:24px;--fs-hero:48px;--fs-kpi:48px;--fs-h1:22px;--fs-h2:16px;--fs-body:14px;--fs-meta:12px;--fs-micro:11px;
  --space-1:4px;--space-2:8px;--space-3:12px;--space-4:16px;--space-5:20px;--space-6:28px;--space-7:40px;--space-8:56px;--space-9:80px;
  --r-sm:4px;--r-md:8px;--r-lg:10px;--r-pill:999px;
  --ease-out:cubic-bezier(0.2,0.7,0.2,1);--ease-in-out:cubic-bezier(0.4,0,0.2,1);
  --dur-fast:120ms;--dur-base:200ms;--dur-slow:320ms;
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg-0);color:var(--ink-1);font-family:var(--font-sans);font-size:var(--fs-body);line-height:1.55;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}
a{color:var(--accent);text-decoration:none;transition:color var(--dur-fast) var(--ease-out)}
a:hover{color:var(--accent-soft);text-decoration:underline}

/* PAGE WRAPPER — every page sits inside .page with consistent 32px outer padding */
.page{max-width:1320px;margin:0 auto;padding:var(--space-7) var(--space-6) var(--space-8)}
@media (max-width:760px){.page{padding:var(--space-5) var(--space-4) var(--space-7)}}
.page-header{display:flex;align-items:baseline;justify-content:space-between;gap:var(--space-4);margin-bottom:var(--space-6);flex-wrap:wrap}
.page-header h1{font-family:var(--font-mono);font-size:var(--fs-h1);font-weight:500;color:var(--ink-1);margin:0;letter-spacing:-0.01em;line-height:1}
.page-header .meta{font-size:var(--fs-meta);color:var(--ink-2);font-family:var(--font-mono)}
.page-header .meta b{color:var(--accent);font-weight:500}

/* TOPBAR — 48px, frosted, with nav tabs */
.topbar{position:sticky;top:0;z-index:50;background:rgba(11,14,18,0.78);backdrop-filter:blur(10px) saturate(140%);-webkit-backdrop-filter:blur(10px) saturate(140%);height:48px;margin:0 calc(-1 * var(--space-6)) var(--space-7);padding:0 var(--space-6);display:flex;align-items:center;gap:var(--space-5);border-bottom:1px solid var(--line-1)}
.topbar-left,.topbar-right{display:flex;align-items:center;gap:var(--space-3);flex-shrink:0}
.topbar-mid{flex:1;display:flex;align-items:center;gap:var(--space-2);color:var(--ink-2);font-size:var(--fs-meta);min-width:0}
.topbar-nav{display:inline-flex;background:var(--bg-1);border-radius:var(--r-pill);padding:3px;gap:2px}
.topbar-nav a{font-family:var(--font-mono);font-size:var(--fs-micro);color:var(--ink-3);padding:5px 14px;border-radius:var(--r-pill);text-decoration:none;letter-spacing:0.06em;transition:all var(--dur-fast) var(--ease-out);font-weight:500}
.topbar-nav a:hover{color:var(--ink-1);text-decoration:none}
.topbar-nav a.active{background:var(--accent);color:var(--accent-ink);font-weight:600}
.topbar-nav a[aria-disabled="true"]{opacity:0.35;pointer-events:none}
.topbar-mid .dot{width:8px;height:8px;border-radius:50%;background:var(--muted);transition:background var(--dur-base) var(--ease-out),box-shadow var(--dur-base) var(--ease-out);flex-shrink:0}
.topbar-mid .dot.live{background:var(--good);box-shadow:0 0 8px rgba(111,207,151,0.45)}
.topbar-mid .dot.warn{background:var(--warn)}
.topbar-mid .dot.bad{background:var(--bad)}
.topbar-brand{font-family:var(--font-mono);font-size:14px;font-weight:600;color:var(--ink-1);letter-spacing:-0.01em}
.topbar-brand .v{color:var(--ink-2);font-weight:400;margin-left:4px}
.topbar-sep{color:var(--ink-4)}
.topbar-env{font-family:var(--font-mono);font-size:var(--fs-micro);color:var(--ink-3);padding:2px 8px;background:var(--bg-2);border-radius:var(--r-sm);letter-spacing:0.02em}
.lang-toggle{display:inline-flex;background:var(--bg-1);border-radius:var(--r-sm);overflow:hidden}
.lang-toggle button{background:transparent;color:var(--ink-2);border:0;padding:5px 10px;cursor:pointer;font:inherit;font-size:var(--fs-micro);letter-spacing:0.04em;transition:all var(--dur-fast) var(--ease-out);font-family:var(--font-mono)}
.lang-toggle button:hover{color:var(--ink-1)}
.lang-toggle button.active{background:var(--accent);color:var(--accent-ink);font-weight:600}
.actor-row{display:flex;align-items:center;gap:var(--space-2);font-size:var(--fs-meta);color:var(--ink-2)}
.actor-row input{background:var(--bg-2);color:var(--ink-1);border:1px solid var(--line-2);border-radius:var(--r-sm);padding:5px 10px;font:inherit;font-size:13px;width:144px;transition:border-color var(--dur-fast) var(--ease-out);font-family:var(--font-mono)}
.actor-row input:focus{outline:2px solid var(--accent);outline-offset:1px;border-color:var(--accent)}
.actor-row input::placeholder{color:var(--ink-3)}

/* PANEL — reusable container with bg-1 background */
.panel{background:var(--bg-1);border-radius:var(--r-lg);padding:var(--space-6);margin-bottom:var(--space-5)}
.panel-tight{padding:var(--space-4) var(--space-5)}
.panel-title{font-size:var(--fs-micro);color:var(--ink-2);text-transform:uppercase;letter-spacing:0.08em;font-weight:500;margin:0 0 var(--space-4);font-family:var(--font-mono);display:flex;align-items:center;gap:var(--space-3)}
.panel-title::after{content:"";flex:0 0 24px;height:1px;background:var(--accent);border-radius:1px}

/* CHIPS */
.chip{display:inline-flex;align-items:center;padding:2px 10px;border-radius:var(--r-pill);font-size:var(--fs-micro);font-weight:500;letter-spacing:0.02em;border:1px solid transparent;font-family:var(--font-mono);line-height:1.6}
.chip.accent{background:rgba(212,165,116,0.12);color:var(--accent);border-color:rgba(212,165,116,0.3)}
.chip.good{background:rgba(111,207,151,0.12);color:var(--good);border-color:rgba(111,207,151,0.3)}
.chip.warn{background:rgba(232,181,71,0.12);color:var(--warn);border-color:rgba(232,181,71,0.3)}
.chip.bad{background:rgba(224,123,123,0.12);color:var(--bad);border-color:rgba(224,123,123,0.3)}
.chip.info{background:rgba(123,184,255,0.12);color:var(--info);border-color:rgba(123,184,255,0.3)}
.chip.outreach{background:rgba(127,212,192,0.12);color:var(--outreach);border-color:rgba(127,212,192,0.3)}
.chip.muted{background:var(--bg-2);color:var(--ink-2);border-color:var(--line-1)}

/* KPI STRIP — 6 cells in a row, 40px numbers */
.kpi-strip{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--bg-0);border-radius:var(--r-lg);overflow:hidden;margin-bottom:var(--space-5)}
@media (max-width:980px){.kpi-strip{grid-template-columns:repeat(3,1fr)}}
@media (max-width:560px){.kpi-strip{grid-template-columns:repeat(2,1fr)}}
.kpi-cell{background:var(--bg-1);padding:var(--space-4) var(--space-5);display:flex;flex-direction:column;gap:4px;transition:background var(--dur-fast) var(--ease-out);min-width:0}
.kpi-cell:hover{background:var(--bg-2)}
.kpi-cell .row1{display:flex;justify-content:space-between;align-items:baseline;gap:var(--space-2)}
.kpi-cell .label{font-size:var(--fs-micro);color:var(--ink-2);text-transform:uppercase;letter-spacing:0.08em;font-weight:500;font-family:var(--font-mono)}
.kpi-cell .delta{font-family:var(--font-mono);font-size:var(--fs-micro);color:var(--ink-3)}
.kpi-cell .delta.up{color:var(--good)}
.kpi-cell .delta.down{color:var(--bad)}
.kpi-cell .num{font-family:var(--font-mono);font-size:var(--fs-kpi);font-weight:500;color:var(--ink-1);line-height:1;letter-spacing:-0.02em;font-variant-numeric:tabular-nums;transition:color var(--dur-base) var(--ease-out);margin-top:2px}
.kpi-cell .num.bump{color:var(--accent)}
.kpi-cell .spark{width:100%;height:18px;margin-top:auto;display:block}

/* HERO — 48px mono domain on review page */
.hero{background:var(--bg-1);border-radius:var(--r-lg);padding:var(--space-7) var(--space-6) var(--space-6);min-height:380px;display:flex;flex-direction:column;gap:var(--space-4);position:relative;overflow:hidden}
.hero.loading{opacity:0.6}
.hero .domain{font-family:var(--font-mono);font-size:var(--fs-hero);font-weight:500;color:var(--ink-1);letter-spacing:-0.025em;line-height:1.05;margin:0;word-break:break-all}
.hero .domain-mark{display:block;width:56px;height:2px;background:var(--accent);margin-top:var(--space-2);border-radius:1px}
.hero .meta-row{display:flex;flex-wrap:wrap;gap:var(--space-2);align-items:center;font-size:var(--fs-meta);color:var(--ink-2);font-family:var(--font-mono)}
.hero .canonical{display:flex;align-items:center;gap:var(--space-2);font-size:13px;color:var(--ink-3);font-family:var(--font-mono);border-left:2px solid var(--line-2);padding:4px 0 4px var(--space-3)}
.hero .canonical .dim{color:var(--ink-4);font-size:var(--fs-micro);text-transform:uppercase;letter-spacing:0.06em}
.hero .desc{font-size:var(--fs-body);color:var(--ink-2);margin:0;line-height:1.6;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.hero .internals{font-size:var(--fs-meta);color:var(--ink-3);font-family:var(--font-mono);margin:0;display:flex;flex-wrap:wrap;gap:var(--space-2) var(--space-3);align-items:center}
.hero .internals .dim{color:var(--ink-4);text-transform:uppercase;letter-spacing:0.06em;font-size:var(--fs-micro)}
.hero-nav{display:flex;align-items:center;justify-content:space-between;margin-top:auto;padding-top:var(--space-4);border-top:1px solid var(--line-1);gap:var(--space-3)}
.hero-nav button{background:transparent;color:var(--ink-2);border:1px solid var(--line-2);border-radius:var(--r-sm);padding:7px 14px;font:inherit;font-size:var(--fs-meta);cursor:pointer;transition:all var(--dur-fast) var(--ease-out);font-family:var(--font-mono);display:inline-flex;align-items:center;gap:8px}
.hero-nav button:hover:not(:disabled){color:var(--ink-1);border-color:var(--accent)}
.hero-nav button:disabled{opacity:0.3;cursor:not-allowed}
.hero-nav .pos{font-family:var(--font-mono);font-size:var(--fs-meta);color:var(--accent);font-weight:500;letter-spacing:0.04em}
.hero-nav .kbd{font-family:var(--font-mono);font-size:var(--fs-micro);padding:2px 6px;background:var(--bg-3);border-radius:var(--r-sm);color:var(--ink-1);border:1px solid var(--line-2)}

/* EVIDENCE */
.evidence-list{display:flex;flex-direction:column;gap:var(--space-2)}
.evidence{padding:var(--space-3) var(--space-4);border-left:3px solid var(--accent);background:var(--bg-2);border-radius:0 var(--r-sm) var(--r-sm) 0;font-size:var(--fs-body)}
.evidence .kind{font-size:var(--fs-micro);color:var(--accent);font-weight:600;text-transform:uppercase;letter-spacing:0.06em;display:block;margin-bottom:4px;font-family:var(--font-mono)}
.evidence .quote{display:block;color:var(--ink-1);line-height:1.5;margin-bottom:6px}
.evidence .src{display:block;color:var(--ink-3);font-size:var(--fs-micro);font-family:var(--font-mono);word-break:break-all;opacity:0.8}
.evidence.cert{border-left-color:var(--info)}.evidence.cert .kind{color:var(--info)}
.evidence.meta{border-left-color:var(--accent)}.evidence.meta .kind{color:var(--accent)}
.evidence.heading{border-left-color:var(--outreach)}.evidence.heading .kind{color:var(--outreach)}
.evidence.body,.evidence.cta{border-left-color:var(--muted)}.evidence.body .kind,.evidence.cta .kind{color:var(--ink-2)}
.evidence-empty{color:var(--ink-3);font-size:var(--fs-body);font-style:italic;padding:var(--space-5) 0;text-align:center}

/* GAUGE */
.gauge-wrap{position:relative;width:120px;height:120px;margin:var(--space-2) auto}
.gauge{width:120px;height:120px;transform:rotate(-90deg)}
.gauge-bg{fill:none;stroke:var(--bg-3);stroke-width:8}
.gauge-arc{fill:none;stroke-width:8;stroke-linecap:butt;transition:stroke-dasharray var(--dur-slow) var(--ease-out)}
.gauge-arc.product{stroke:var(--accent)}
.gauge-arc.early{stroke:var(--outreach)}
.gauge-arc.exposure{stroke:var(--info)}
.gauge-arc.complete{stroke:var(--muted)}
.gauge-score{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;font-family:var(--font-mono)}
.gauge-score .num{font-size:24px;font-weight:500;color:var(--accent);line-height:1;letter-spacing:-0.02em;font-variant-numeric:tabular-nums;transition:color var(--dur-base) var(--ease-out)}
.gauge-score .label{font-size:var(--fs-micro);color:var(--ink-3);text-transform:uppercase;letter-spacing:0.08em;margin-top:4px;font-family:var(--font-mono)}
.priority-rows{display:flex;flex-direction:column;gap:var(--space-2);width:100%;max-width:320px;margin-top:var(--space-4)}
.priority-row{display:grid;grid-template-columns:64px 1fr 56px;gap:var(--space-3);align-items:center}
.priority-row .label{color:var(--ink-2);font-family:var(--font-mono);font-size:var(--fs-micro);letter-spacing:0.04em;text-transform:uppercase}
.priority-row .bar-bg{background:var(--bg-3);height:4px;border-radius:2px;overflow:hidden}
.priority-row .bar-fg{height:100%;border-radius:2px;transition:width var(--dur-slow) var(--ease-out)}
.priority-row .bar-fg.product{background:var(--accent)}
.priority-row .bar-fg.early{background:var(--outreach)}
.priority-row .bar-fg.exposure{background:var(--info)}
.priority-row .bar-fg.complete{background:var(--muted)}
.priority-row .num{font-family:var(--font-mono);font-size:var(--fs-meta);color:var(--ink-1);text-align:right;font-variant-numeric:tabular-nums}
.priority-meta{display:flex;flex-direction:column;gap:6px;width:100%;max-width:320px;margin-top:var(--space-3);padding-top:var(--space-3);border-top:1px solid var(--line-1);font-size:var(--fs-meta);color:var(--ink-2);font-family:var(--font-mono)}
.priority-meta .row{display:flex;justify-content:space-between;gap:var(--space-2)}
.priority-meta b{color:var(--ink-1);font-weight:500}

/* ACTIONS */
.actions{display:grid;grid-template-columns:1fr 1fr 1fr 1fr 120px;gap:var(--space-2);background:var(--bg-1);padding:var(--space-5);border-radius:var(--r-lg);margin-bottom:var(--space-4)}
@media (max-width:720px){.actions{grid-template-columns:1fr 1fr}.actions .btn{grid-column:span 1}}
.actions .btn{height:48px;padding:0 16px;border-radius:var(--r-sm);font:inherit;font-size:var(--fs-body);font-weight:600;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:10px;transition:all var(--dur-fast) var(--ease-out);border:1px solid transparent;font-family:var(--font-sans);letter-spacing:0.02em}
.actions .btn:disabled{opacity:0.4;cursor:not-allowed;transform:none !important}
.actions .btn .key{font-family:var(--font-mono);font-size:13px;padding:2px 8px;border-radius:var(--r-sm);background:rgba(0,0,0,0.32);border:1px solid rgba(255,255,255,0.08);letter-spacing:0;font-weight:600;min-width:22px;text-align:center;line-height:1.4}
.actions .btn.approve{background:var(--good);color:#0E1A12}
.actions .btn.approve:hover:not(:disabled){background:#85dba8;transform:translateY(-1px)}
.actions .btn.reject{background:var(--bad);color:#1A0E0E}
.actions .btn.reject:hover:not(:disabled){background:#e89393;transform:translateY(-1px)}
.actions .btn.defer{background:transparent;color:var(--warn);border-color:var(--warn)}
.actions .btn.defer:hover:not(:disabled){background:rgba(232,181,71,0.12)}
.actions .btn.blocklist{background:transparent;color:var(--muted);border-color:var(--line-3)}
.actions .btn.blocklist:hover:not(:disabled){background:var(--bg-2);color:var(--ink-2)}
.actions .btn.edit{background:transparent;color:var(--info);border-color:var(--info)}
.actions .btn.edit:hover:not(:disabled){background:rgba(123,184,255,0.12)}
.actions .btn.approved-static{background:rgba(111,207,151,0.1);color:var(--good);border-color:var(--good);cursor:default;grid-column:span 4}

/* OUTREACH */
.outreach-panel{background:var(--bg-1);padding:var(--space-5) var(--space-6);border-radius:var(--r-lg);margin-bottom:var(--space-4);display:flex;flex-direction:column;gap:var(--space-3)}
.outreach-panel .help{font-size:var(--fs-meta);color:var(--ink-2);margin:0}
.outreach-row{display:flex;gap:var(--space-2)}
.outreach-row input{flex:1;background:var(--bg-2);color:var(--ink-1);border:1px solid var(--line-2);border-radius:var(--r-sm);padding:9px 12px;font:inherit;font-family:var(--font-mono);font-size:13px}
.outreach-row input:focus{outline:2px solid var(--accent);outline-offset:1px;border-color:var(--accent)}
.outreach-row input::placeholder{color:var(--ink-3)}
.outreach-buttons{display:grid;grid-template-columns:1fr 1fr;gap:var(--space-2)}
.outreach-buttons .btn{height:42px;padding:0 14px;border-radius:var(--r-sm);font:inherit;font-size:var(--fs-body);font-weight:600;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:8px;transition:all var(--dur-fast) var(--ease-out);border:1px solid transparent;font-family:var(--font-sans)}
.outreach-buttons .btn:disabled{opacity:0.4;cursor:not-allowed}
.outreach-buttons .btn .key{font-family:var(--font-mono);font-size:var(--fs-micro);padding:2px 6px;border-radius:var(--r-sm);background:rgba(0,0,0,0.28);border:1px solid rgba(255,255,255,0.08);font-weight:600;min-width:18px;text-align:center}
.outreach-buttons .btn.outreach-dry{background:transparent;color:var(--info);border-color:var(--info)}
.outreach-buttons .btn.outreach-dry:hover:not(:disabled){background:rgba(123,184,255,0.12)}
.outreach-buttons .btn.outreach-real{background:var(--outreach);color:#0E1A12}
.outreach-buttons .btn.outreach-real:hover:not(:disabled){background:#94dfcd;transform:translateY(-1px)}
.outreach-history{font-family:var(--font-mono);font-size:var(--fs-meta);color:var(--ink-2);background:var(--bg-2);border-radius:var(--r-sm);padding:var(--space-3) var(--space-4);display:flex;flex-direction:column;gap:6px}
.outreach-history .row{display:flex;gap:var(--space-2);align-items:center}
.outreach-history .empty{color:var(--ink-3);font-style:italic}

/* QUEUE TABLE */
.queue-table{display:flex;flex-direction:column;background:var(--bg-1);border-radius:var(--r-lg);overflow:hidden}
.queue-row{display:grid;grid-template-columns:minmax(0,2fr) 80px 50px 1.4fr 110px;gap:var(--space-4);align-items:center;padding:var(--space-4) var(--space-5);border-bottom:1px solid var(--line-1);transition:background var(--dur-fast) var(--ease-out);text-decoration:none;color:inherit}
.queue-row:last-child{border-bottom:0}
.queue-row:hover{background:var(--bg-2);text-decoration:none}
.queue-row .qdomain{font-family:var(--font-mono);font-size:15px;color:var(--ink-1);font-weight:500;letter-spacing:-0.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.queue-row .qscore{font-family:var(--font-mono);font-size:18px;font-weight:500;color:var(--accent);text-align:right;font-variant-numeric:tabular-nums}
.queue-row .qver{font-family:var(--font-mono);font-size:var(--fs-meta);color:var(--ink-3);text-align:center}
.queue-row .qoutcome{font-size:var(--fs-meta);color:var(--ink-2);font-family:var(--font-mono)}
.queue-row .qaction{justify-self:end}
.queue-row .qaction .review-link{font-family:var(--font-mono);font-size:var(--fs-meta);color:var(--accent);padding:6px 14px;border:1px solid var(--accent);border-radius:var(--r-pill);transition:all var(--dur-fast) var(--ease-out);letter-spacing:0.04em}
.queue-row:hover .qaction .review-link{background:var(--accent);color:var(--accent-ink);text-decoration:none}
.queue-empty{padding:var(--space-9) var(--space-5);text-align:center;color:var(--ink-2)}
.queue-empty h2{color:var(--ink-1);font-family:var(--font-mono);font-size:24px;font-weight:500;margin:0 0 var(--space-3)}
.queue-empty p{margin:0 0 var(--space-5);font-size:var(--fs-body)}

/* DISCOVERY */
.discovery-summary{font-size:var(--fs-meta);color:var(--ink-2);font-family:var(--font-mono);min-height:18px;margin-top:var(--space-3)}
.discovery-list{display:flex;flex-direction:column;gap:0;max-height:560px;overflow-y:auto}
.discovery-list .row{display:grid;grid-template-columns:minmax(0,2fr) 130px 80px;gap:var(--space-4);align-items:center;padding:var(--space-3) var(--space-1);border-bottom:1px dashed var(--line-1);font-size:var(--fs-body)}
.discovery-list .row:last-child{border-bottom:0}
.discovery-list .domain{font-family:var(--font-mono);color:var(--ink-1);font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.discovery-list .outcome{font-size:var(--fs-micro);color:var(--ink-3);font-family:var(--font-mono);text-align:right;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.discovery-list .seen{font-size:var(--fs-micro);color:var(--ink-4);font-family:var(--font-mono);text-align:right}
.discovery-list .empty{color:var(--ink-3);font-style:italic;text-align:center;padding:var(--space-7) 0}
.run-row{display:flex;align-items:center;gap:var(--space-4);flex-wrap:wrap}
.run-row label{font-size:var(--fs-meta);color:var(--ink-2);display:flex;align-items:center;gap:var(--space-2);font-family:var(--font-mono)}
.run-row input[type="number"]{background:var(--bg-2);color:var(--ink-1);border:1px solid var(--line-2);border-radius:var(--r-sm);padding:7px 10px;font:inherit;font-family:var(--font-mono);width:80px}
.run-row input[type="number"]:focus{outline:2px solid var(--accent);outline-offset:1px;border-color:var(--accent)}
.btn-primary{background:var(--accent);color:var(--accent-ink);border:0;border-radius:var(--r-sm);padding:9px 22px;font:inherit;font-size:var(--fs-body);font-weight:600;cursor:pointer;transition:background var(--dur-fast) var(--ease-out);font-family:var(--font-sans);letter-spacing:0.02em}
.btn-primary:hover:not(:disabled){background:var(--accent-soft)}
.btn-primary:disabled{opacity:0.45;cursor:not-allowed}

/* OPS — analytics grid + alerts */
.analytics-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--bg-0);border-radius:var(--r-lg);overflow:hidden;margin-bottom:var(--space-5)}
@media (max-width:980px){.analytics-grid{grid-template-columns:repeat(2,1fr)}}
.analytics-cell{background:var(--bg-1);padding:var(--space-5) var(--space-5);display:flex;flex-direction:column;gap:var(--space-2);min-height:140px}
.analytics-cell .label{font-size:var(--fs-micro);color:var(--ink-2);text-transform:uppercase;letter-spacing:0.08em;font-family:var(--font-mono)}
.analytics-cell .num{font-family:var(--font-mono);font-size:36px;font-weight:500;color:var(--ink-1);line-height:1;letter-spacing:-0.02em;font-variant-numeric:tabular-nums;margin-top:auto}
.analytics-cell .num.accent{color:var(--accent)}

.alerts-list{display:flex;flex-direction:column;gap:var(--space-2)}
.alert-row{padding:var(--space-3) var(--space-4);background:var(--bg-2);border-radius:var(--r-sm);font-size:var(--fs-meta);border-left:3px solid transparent}
.alert-row .meta{display:flex;gap:var(--space-2);align-items:center;margin-bottom:4px}
.alert-row .kind{color:var(--ink-3);font-family:var(--font-mono);font-size:var(--fs-micro)}
.alert-row .title{color:var(--ink-1)}
.alert-row.warn{border-left-color:var(--warn)}
.alert-row.critical{border-left-color:var(--bad)}
.alert-row.info{border-left-color:var(--info)}
.alert-empty{color:var(--good);font-size:var(--fs-meta);padding:var(--space-4) var(--space-5);background:var(--bg-1);border-radius:var(--r-md);font-family:var(--font-mono);display:flex;align-items:center;gap:var(--space-2)}
.runbook-hint{font-family:var(--font-mono);font-size:var(--fs-micro);color:var(--ink-3);margin-top:var(--space-3);padding:var(--space-3) var(--space-4);background:var(--bg-1);border-radius:var(--r-md);word-break:break-all;border-left:2px solid var(--line-2)}
.runbook-hint b{color:var(--accent)}

/* TOAST */
.toast{position:fixed;bottom:var(--space-5);right:var(--space-5);background:var(--bg-3);border:1px solid var(--line-2);border-radius:var(--r-md);padding:var(--space-3) var(--space-5);font-size:var(--fs-body);color:var(--ink-1);box-shadow:0 12px 32px rgba(0,0,0,0.45);opacity:0;transform:translateY(8px);transition:opacity var(--dur-base) var(--ease-out),transform var(--dur-base) var(--ease-out);pointer-events:none;z-index:100;max-width:360px;font-family:var(--font-sans)}
.toast.show{opacity:1;transform:translateY(0)}
.toast.success{border-color:var(--good)}
.toast.error{border-color:var(--bad)}

.pending-bar{position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent);transform-origin:left;animation:pendingProgress var(--dur-base) var(--ease-in-out);z-index:1}
@keyframes pendingProgress{from{transform:scaleX(0)}to{transform:scaleX(1)}}

/* SHORTCUTS */
.shortcuts{font-size:var(--fs-micro);color:var(--ink-3);margin-top:var(--space-5);padding:var(--space-3);text-align:center;background:var(--bg-1);border-radius:var(--r-md);font-family:var(--font-mono)}
.shortcuts kbd{display:inline-block;background:var(--bg-2);border:1px solid var(--line-2);border-bottom-width:2px;border-radius:var(--r-sm);padding:1px 6px;font-family:var(--font-mono);font-size:var(--fs-micro);color:var(--accent);margin:0 1px}
.back-link{font-size:var(--fs-meta);color:var(--ink-3);font-family:var(--font-mono);display:inline-flex;align-items:center;gap:6px;margin-bottom:var(--space-4);transition:color var(--dur-fast) var(--ease-out)}
.back-link:hover{color:var(--accent);text-decoration:none}

@media (prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:0.01ms !important;transition-duration:0.01ms !important}}
</style></head><body>"""

# Topbar template with placeholder for the active tab and the current review candidate.
TOPBAR_TEMPLATE = r"""
<header class="topbar">
  <div class="topbar-left">
    <span class="topbar-brand">DomainHunter</span></span>
    <span class="topbar-sep">·</span>
    <span class="topbar-env" data-i18n="env_badge">loopback</span>
  </div>
  <nav class="topbar-nav">
    <a href="/" data-tab="queue" class="{tab_queue}">QUEUE</a>
    <a href="{review_href}" data-tab="review" class="{tab_review}"{review_disabled}>REVIEW</a>
    <a href="/discovery" data-tab="discovery" class="{tab_discovery}">DISCOVERY</a>
    <a href="/ops" data-tab="ops" class="{tab_ops}">OPS</a>
  </nav>
  <div class="topbar-mid">
    <span class="dot" id="status-dot"></span>
    <span id="status-text" data-i18n="status_unknown">…</span>
  </div>
  <div class="topbar-right">
    <div class="lang-toggle">
      <button data-lang-set="zh" class="active">中</button>
      <button data-lang-set="en">EN</button>
    </div>
    <div class="actor-row">
      <input id="actor" data-i18n-ph="actor_ph" placeholder="审核人 id">
    </div>
  </div>
</header>"""

QUEUE_HTML = r"""
<main class="page page-queue" data-page="queue">
  <div class="page-header">
    <h1 data-i18n="queue_title">REVIEW QUEUE</h1>
    <div class="meta" data-i18n="queue_count"><b id="q-count">0</b> waiting</div>
  </div>

  <div class="kpi-strip" id="kpi-strip">
    <div class="kpi-cell"><div class="row1"><span class="label" data-i18n="s1">信号</span><span class="delta" id="d-signals"></span></div><div class="num" id="m-signals">–</div><svg class="spark" viewBox="0 0 100 24" preserveAspectRatio="none"><polyline id="spark-signals" fill="none" stroke="rgba(229,195,153,0.6)" stroke-width="1.5" points=""/></svg></div>
    <div class="kpi-cell"><div class="row1"><span class="label" data-i18n="s2">域名</span><span class="delta" id="d-domains"></span></div><div class="num" id="m-domains">–</div><svg class="spark" viewBox="0 0 100 24" preserveAspectRatio="none"><polyline id="spark-domains" fill="none" stroke="rgba(229,195,153,0.6)" stroke-width="1.5" points=""/></svg></div>
    <div class="kpi-cell"><div class="row1"><span class="label" data-i18n="s3">探测</span><span class="delta" id="d-probes"></span></div><div class="num" id="m-probes">–</div><svg class="spark" viewBox="0 0 100 24" preserveAspectRatio="none"><polyline id="spark-probes" fill="none" stroke="rgba(229,195,153,0.6)" stroke-width="1.5" points=""/></svg></div>
    <div class="kpi-cell"><div class="row1"><span class="label" data-i18n="s4">候选</span><span class="delta" id="d-candidates"></span></div><div class="num" id="m-candidates">–</div><svg class="spark" viewBox="0 0 100 24" preserveAspectRatio="none"><polyline id="spark-candidates" fill="none" stroke="rgba(229,195,153,0.6)" stroke-width="1.5" points=""/></svg></div>
    <div class="kpi-cell"><div class="row1"><span class="label" data-i18n="s5">已审</span><span class="delta" id="d-approved"></span></div><div class="num" id="m-approved">–</div><svg class="spark" viewBox="0 0 100 24" preserveAspectRatio="none"><polyline id="spark-approved" fill="none" stroke="rgba(229,195,153,0.6)" stroke-width="1.5" points=""/></svg></div>
    <div class="kpi-cell"><div class="row1"><span class="label" data-i18n="s6">触达</span><span class="delta" id="d-outreach"></span></div><div class="num" id="m-outreach">–</div><svg class="spark" viewBox="0 0 100 24" preserveAspectRatio="none"><polyline id="spark-outreach" fill="none" stroke="rgba(229,195,153,0.6)" stroke-width="1.5" points=""/></svg></div>
  </div>

  <section class="panel">
    <div class="run-row">
      <label><span data-i18n="max_probes">probes</span><input id="max-probes" type="number" min="1" max="200" value="20"></label>
      <button class="btn-primary" id="btn-run-discovery"><span data-i18n="run_discovery">▶ Run discovery</span></button>
      <span class="discovery-summary" id="discovery-status"></span>
    </div>
  </section>

  <section class="panel" style="padding:0">
    <div class="queue-table" id="queue-list"></div>
  </section>
</main>"""

REVIEW_DETAIL_HTML = r"""
<main class="page page-review" data-page="review">
  <a class="back-link" href="/" data-i18n="back_queue">← back to queue</a>

  <div class="hero" id="candidate-card">
    <div class="empty" style="padding:var(--space-7);color:var(--ink-3);text-align:center"><p data-i18n="loading">载入中…</p></div>
  </div>

  <div style="display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,1fr);gap:var(--space-5);margin-top:var(--space-5)">
    <section class="panel">
      <h3 class="panel-title" data-i18n="product_evidence">产品证据</h3>
      <div class="evidence-list" id="evidence-list"></div>
    </section>
    <section class="panel" style="text-align:center">
      <h3 class="panel-title" style="justify-content:center" data-i18n="priority">审核优先级</h3>
      <div class="gauge-wrap">
        <svg class="gauge" viewBox="0 0 120 120">
          <circle class="gauge-bg" cx="60" cy="60" r="50"/>
          <path id="arc-product"  class="gauge-arc product"  d="M 60 10 A 50 50 0 0 1 110 60"/>
          <path id="arc-early"    class="gauge-arc early"    d="M 110 60 A 50 50 0 0 1 60 110"/>
          <path id="arc-exposure" class="gauge-arc exposure" d="M 60 110 A 50 50 0 0 1 10 60"/>
          <path id="arc-complete" class="gauge-arc complete" d="M 10 60 A 50 50 0 0 1 60 10"/>
        </svg>
        <div class="gauge-score"><div class="num" id="gauge-score">0.000</div><div class="label" data-i18n="score">score</div></div>
      </div>
      <div class="priority-rows" id="priority-rows"></div>
      <div class="priority-meta" id="priority-meta"></div>
    </section>
  </div>

  <div class="actions" id="actions-bar"></div>

  <div class="outreach-panel" id="outreach-panel" hidden></div>

  <div class="shortcuts">
    <span data-i18n="kbd_hint">快捷键</span>:
    <kbd>←</kbd>/<kbd>→</kbd> <span data-i18n="kbd_nav">翻页</span> ·
    <kbd>A</kbd>/<kbd>R</kbd>/<kbd>D</kbd>/<kbd>B</kbd>/<kbd>E</kbd> <span data-i18n="kbd_actions">决策</span> ·
    <kbd>O</kbd> <span data-i18n="kbd_outreach">触达</span>
  </div>
</main>"""

DISCOVERY_HTML = r"""
<main class="page page-discovery" data-page="discovery">
  <div class="page-header">
    <h1 data-i18n="discovery_title">DISCOVERY</h1>
    <div class="meta" data-i18n="discovery_subtitle">CT log → poller → L1/L2/L3 → candidates</div>
  </div>

  <section class="panel">
    <h3 class="panel-title" data-i18n="run_one">RUN</h3>
    <div class="run-row">
      <label><span data-i18n="max_probes">probes</span><input id="max-probes" type="number" min="1" max="200" value="20"></label>
      <button class="btn-primary" id="btn-run-discovery"><span data-i18n="run_discovery">▶ Run</span></button>
      <span class="discovery-summary" id="discovery-status"></span>
    </div>
  </section>

  <section class="panel">
    <h3 class="panel-title" data-i18n="recent_domains">RECENT DOMAINS</h3>
    <div class="discovery-list" id="discovery-result"></div>
  </section>
</main>"""

OPS_HTML = r"""
<main class="page page-ops" data-page="ops">
  <div class="page-header">
    <h1 data-i18n="ops_title">OPS</h1>
    <div class="meta" data-i18n="ops_subtitle">funnel · cost · latency · alerts</div>
  </div>

  <section class="panel" style="padding:0">
    <h3 class="panel-title" style="padding:var(--space-5) var(--space-6) 0" data-i18n="kpi_title">ANALYTICS</h3>
    <div class="analytics-grid">
      <div class="analytics-cell"><span class="label" data-i18n="k_conv">signal → candidate</span><span class="num accent" id="k-conv">–</span></div>
      <div class="analytics-cell"><span class="label" data-i18n="k_cost">cost / candidate</span><span class="num" id="k-cost">–</span></div>
      <div class="analytics-cell"><span class="label" data-i18n="k_p50">P50 latency</span><span class="num" id="k-p50">–</span></div>
      <div class="analytics-cell"><span class="label" data-i18n="k_p95">P95 latency</span><span class="num" id="k-p95">–</span></div>
    </div>
  </section>

  <section class="panel">
    <h3 class="panel-title" data-i18n="alerts_title">ALERTS</h3>
    <div class="alerts-list" id="alerts"></div>
    <div class="runbook-hint" id="runbook">…</div>
  </section>
</main>"""

SHARED_SCRIPT = r"""
<script>
const I18N = {
  en: {
    env_badge:'loopback', status_unknown:'connecting',
    status_live:'live', status_slow:'slow', status_offline:'offline',
    s1:'SIGNALS',s2:'DOMAINS',s3:'PROBES',s4:'CANDIDATES',s5:'APPROVED',s6:'OUTREACH',
    discovery_title:'DISCOVERY', max_probes:'probes', run_discovery:'▶ Run',
    run_one:'RUN', recent_domains:'RECENT DOMAINS', discovery_subtitle:'CT log → poller → L1/L2/L3 → candidates',
    discovery_running:'Running…', discovery_done:'Done', discovery_failed:'Discovery failed',
    discovery_summary:'Seen {certificates} certs · +{events} events · {probes} probes · {candidates} candidates',
    discovery_empty:'No domains yet.',
    actor_ph:'reviewer id',
    prev:'prev', next:'next', pos:'{i} of {n}',
    version:'version', approved_session:'approved this session',
    product_evidence:'Product evidence', priority:'Review priority',
    no_evidence:'No cited evidence.',
    canonical:'canonical', internal_links:'internal links',
    primary_outcome:'primary outcome', confidence:'confidence', formula:'formula',
    approve:'Approve', reject:'Reject', defer:'Defer', blocklist:'Blocklist', edit:'Edit',
    outreach_help:'Approved. Trigger claim-token issuance + redacted contact preview.',
    outreach_url_ph:'https://example.com/contact (optional)',
    outreach_dry:'Dry run', outreach_real:'Real run',
    outreach_history_empty:'No outreach history for this candidate yet.',
    empty_title:'No candidates waiting',
    empty_hint:'All caught up. Run discovery or wait for new signals.',
    empty_action:'Run discovery',
    loading:'Loading…',
    kbd_hint:'Shortcuts', kbd_nav:'navigate', kbd_actions:'decide', kbd_outreach:'outreach',
    kpi_title:'ANALYTICS', ops_title:'OPS', ops_subtitle:'funnel · cost · latency · alerts',
    queue_title:'REVIEW QUEUE', queue_count:'<b>{n}</b> waiting', review_now:'REVIEW →',
    queue_empty_title:'Inbox zero',
    queue_empty_hint:'No candidates waiting. Run discovery or wait for new signals.',
    back_queue:'← back to queue',
    k_conv:'signal → candidate', k_cost:'cost / candidate',
    k_p50:'P50 latency', k_p95:'P95 latency',
    alerts_title:'ALERTS', runbook_title:'RUNBOOK',
    toast_no_actor:'Enter a reviewer id before deciding.',
    toast_actor_outreach:'Enter a reviewer id before triggering outreach.',
    toast_real_needs_url:'Real-run requires a recipient URL.',
    toast_edit_noop:'Edit is reserved for the next human version.',
    toast_decision_ok:'{action} recorded.',
    toast_outreach_ok:'Outreach {status}.',
    toast_outreach_err:'Outreach failed: {err}',
    toast_load_err:'Failed to load: {err}',
    no_alerts:'all clear', no_alerts_runbook:'no runbook needed',
    score:'score',
    outcome:{
      publishable_ai_saas:'publishable AI SaaS',
      valid_but_not_ready:'valid but not ready',
      not_target:'not a target',
      duplicate_or_existing:'duplicate or existing',
      policy_excluded:'policy excluded',
    },
  },
  zh: {
    env_badge:'本地', status_unknown:'连接中',
    status_live:'在线', status_slow:'慢', status_offline:'离线',
    s1:'信号',s2:'域名',s3:'探测',s4:'候选',s5:'已审',s6:'触达',
    discovery_title:'发现', max_probes:'探测', run_discovery:'▶ 运行',
    run_one:'运行', recent_domains:'最近域名', discovery_subtitle:'CT 日志 → 轮询 → L1/L2/L3 → 候选',
    discovery_running:'正在运行…', discovery_done:'完成', discovery_failed:'发现失败',
    discovery_summary:'看到 {certificates} 条证书 · 新增 {events} 条 · 探测 {probes} 个 · 生成 {candidates} 个',
    discovery_empty:'暂无新发现域名。',
    actor_ph:'审核人 id',
    prev:'上一张', next:'下一张', pos:'第 {i} / {n} 张',
    version:'版本', approved_session:'已批准（本次）',
    product_evidence:'产品证据', priority:'审核优先级',
    no_evidence:'暂无引用证据。',
    canonical:'规范链接', internal_links:'站内链接',
    primary_outcome:'主结论', confidence:'置信度', formula:'公式版本',
    approve:'批准', reject:'拒绝', defer:'推迟', blocklist:'拉黑', edit:'编辑',
    outreach_help:'版本已批准，可触发认领 token 签发与脱敏联系人预览。',
    outreach_url_ph:'https://example.com/contact （可选）',
    outreach_dry:'预览 (dry)', outreach_real:'真实发送',
    outreach_history_empty:'该候选尚无触达记录。',
    empty_title:'当前没有待审候选',
    empty_hint:'全部处理完毕，可运行发现或等待新信号。',
    empty_action:'运行发现',
    loading:'载入中…',
    kbd_hint:'快捷键', kbd_nav:'翻页', kbd_actions:'决策', kbd_outreach:'触达',
    kpi_title:'分析', ops_title:'运营', ops_subtitle:'漏斗 · 成本 · 延迟 · 告警',
    queue_title:'审核队列', queue_count:'待审 <b>{n}</b> 个', review_now:'审核 →',
    queue_empty_title:'队列已清空',
    queue_empty_hint:'没有待审候选。可运行发现或等待新信号。',
    back_queue:'← 返回队列',
    k_conv:'信号 → 候选', k_cost:'每候选成本',
    k_p50:'P50 延迟', k_p95:'P95 延迟',
    alerts_title:'告警', runbook_title:'应急手册',
    toast_no_actor:'请先填写审核人 id。',
    toast_actor_outreach:'请先填写审核人 id，再触发触达。',
    toast_real_needs_url:'真实发送必须填写收件 URL。',
    toast_edit_noop:'编辑用于下一个人工版本，本次未修改。',
    toast_decision_ok:'已记录：{action}',
    toast_outreach_ok:'触达完成：{status}',
    toast_outreach_err:'触达失败：{err}',
    toast_load_err:'加载失败：{err}',
    no_alerts:'一切正常', no_alerts_runbook:'无需应急手册',
    score:'score',
    outcome:{
      publishable_ai_saas:'可发布的 AI SaaS',
      valid_but_not_ready:'有效但暂未就绪',
      not_target:'非目标产品',
      duplicate_or_existing:'重复或已存在',
      policy_excluded:'策略排除',
    },
  },
};

let currentLang = localStorage.getItem('domainhunter-lang') || 'zh';
const PAGE = document.querySelector('.page')?.dataset.page || 'queue';
const $ = sel => document.querySelector(sel);
const $$ = sel => Array.from(document.querySelectorAll(sel));
const fmtLatency = (seconds) => {
  if (seconds == null) return '–';
  if (seconds < 60) return seconds.toFixed(1) + 's';
  if (seconds < 3600) return (seconds / 60).toFixed(1) + 'm';
  if (seconds < 86400) return (seconds / 3600).toFixed(1) + 'h';
  if (seconds < 86400 * 365) return (seconds / 86400).toFixed(1) + 'd';
  return (seconds / (86400 * 365)).toFixed(2) + 'y';
};
const t = (key, vars) => {
  const dict = I18N[currentLang] || I18N.zh;
  let s = dict[key] != null ? dict[key] : (I18N.en[key] != null ? I18N.en[key] : key);
  if (vars) for (const [k,v] of Object.entries(vars)) s = s.replace('{' + k + '}', v);
  return s;
};
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function applyLang() {
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute('data-lang', currentLang);
  $$('[data-i18n]').forEach(el => { el.innerHTML = t(el.dataset.i18n); });
  $$('[data-i18n-ph]').forEach(el => { el.placeholder = t(el.dataset.i18nPh); });
  $$('.lang-toggle button').forEach(b => b.classList.toggle('active', b.dataset.langSet === currentLang));
  probeHealth();
  if (typeof onLangChanged === 'function') onLangChanged();
}
function toast(msg, kind) {
  const el = $('#toast');
  if (!el) return;
  el.textContent = msg;
  el.className = 'toast show ' + (kind || 'success');
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.remove('show'), 2400);
}
const kpiPrev = new Map();
function setKpi(id, val) {
  const el = $('#' + id);
  if (!el) return;
  const newStr = val == null ? '–' : Number(val).toLocaleString();
  const oldStr = el.textContent;
  const prev = kpiPrev.get(id);
  const numericPrev = prev != null ? Number(prev) : null;
  if (oldStr !== newStr) {
    el.textContent = newStr;
    el.classList.add('bump');
    setTimeout(() => el.classList.remove('bump'), 320);
  }
  if (val != null) {
    const deltaEl = $('#d-' + id.replace(/^m-/, ''));
    if (deltaEl && numericPrev != null) {
      const diff = Number(val) - numericPrev;
      if (diff > 0) { deltaEl.textContent = '+' + diff.toLocaleString(); deltaEl.className = 'delta up'; }
      else if (diff < 0) { deltaEl.textContent = diff.toLocaleString(); deltaEl.className = 'delta down'; }
      else { deltaEl.textContent = '±0'; deltaEl.className = 'delta'; }
    }
    const sparkId = 'spark-' + id.replace(/^m-/, '');
    if (document.getElementById(sparkId)) updateSpark(sparkId, Number(val), numericPrev || 0);
    kpiPrev.set(id, val);
  }
}
function updateSpark(polyId, value, prev) {
  const el = document.getElementById(polyId);
  if (!el) return;
  const seed = (value || 0) + (prev || 0) * 7;
  const pts = [];
  for (let i = 0; i < 7; i++) {
    const x = i * (100 / 6);
    const noise = ((Math.sin(seed + i * 1.7) + Math.cos(seed * 0.6 + i * 0.9)) / 2) * 8;
    const base = 18 - Math.min(14, ((value || 0) % 14));
    const y = Math.max(2, Math.min(22, base + noise));
    pts.push(`${x.toFixed(1)},${y.toFixed(1)}`);
  }
  el.setAttribute('points', pts.join(' '));
}
async function probeHealth() {
  const dot = $('#status-dot');
  const text = $('#status-text');
  if (!dot || !text) return;
  try {
    const t0 = performance.now();
    const r = await fetch('/healthz', { cache: 'no-store' });
    if (!r.ok) throw new Error('not ok');
    const lat = performance.now() - t0;
    if (lat > 800) { dot.className = 'dot warn'; text.textContent = t('status_slow'); }
    else { dot.className = 'dot live'; text.textContent = t('status_live'); }
  } catch (e) {
    dot.className = 'dot bad'; text.textContent = t('status_offline');
  }
}
async function refreshBar() {
  try {
    const [m, a, al] = await Promise.all([
      fetch('/v1/metrics').then(r => r.json()),
      fetch('/v1/analytics').then(r => r.json()),
      fetch('/v1/alerts').then(r => r.json()),
    ]);
    setKpi('m-signals', m.source_events);
    setKpi('m-domains', m.domains);
    setKpi('m-probes', m.observations);
    setKpi('m-candidates', m.candidates);
    setKpi('m-approved', m.review_decisions || 0);
    setKpi('m-outreach', m.outreach_events || 0);
    if (typeof onMetrics === 'function') onMetrics(m, a, al);
  } catch (e) { /* keep previous values on transient errors */ }
}

$('#actor')?.addEventListener('change', () => localStorage.setItem('domainhunter-actor', $('#actor').value));
if ($('#actor')) $('#actor').value = localStorage.getItem('domainhunter-actor') || '';
$$('.lang-toggle button').forEach(b => b.addEventListener('click', () => {
  currentLang = b.dataset.langSet;
  localStorage.setItem('domainhunter-lang', currentLang);
  applyLang();
}));
</script>
"""

QUEUE_SCRIPT = r"""
<script>
let queue = [];
let lastRunSummary = null;

function renderQueue() {
  const el = $('#queue-list');
  if (!el) return;
  const count = queue.length;
  const countEl = $('#q-count');
  if (countEl) {
    const tmpl = t('queue_count');
    countEl.parentElement.innerHTML = tmpl.replace('<b>{n}</b>', `<b>${count}</b>`);
  }
  if (!count) {
    el.innerHTML = `<div class="queue-empty"><h2>${esc(t('queue_empty_title'))}</h2><p>${esc(t('queue_empty_hint'))}</p></div>`;
    return;
  }
  el.innerHTML = queue.map(it => {
    const outcomeLabel = (I18N[currentLang].outcome || {})[it.primary_outcome] || it.primary_outcome;
    const score = it.priority?.score?.toFixed(2) ?? '–';
    return `<a class="queue-row" href="/review/${encodeURIComponent(it.candidate_id)}">
      <span class="qdomain">${esc(it.name_suggestion || it.domain)}</span>
      <span class="qscore">${score}</span>
      <span class="qver">v${it.version}</span>
      <span class="qoutcome">${esc(outcomeLabel)}</span>
      <span class="qaction"><span class="review-link">${esc(t('review_now'))}</span></span>
    </a>`;
  }).join('');
}

function onMetrics(metrics, analytics, alerts) {
  // queue page has no special handling beyond KPIs (set by refreshBar)
}

async function loadQueue() {
  try {
    const r = await fetch('/v1/review-queue');
    if (!r.ok) throw new Error('queue failed');
    queue = (await r.json()).items;
    renderQueue();
  } catch (e) { /* keep previous */ }
}

async function runDiscovery() {
  const btn = $('#btn-run-discovery');
  const status = $('#discovery-status');
  const maxProbes = Math.max(1, Math.min(200, parseInt($('#max-probes')?.value || '20', 10) || 20));
  if (!btn || !status) return;
  btn.disabled = true;
  status.textContent = t('discovery_running');
  try {
    const r = await fetch('/v1/run/discovery', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ max_probes: maxProbes }),
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.detail || 'discovery run failed');
    lastRunSummary = j;
    status.textContent = t('discovery_summary', {
      certificates: j.certificates_seen, events: j.events_added,
      probes: j.probes_run, candidates: j.candidates_created,
    });
    toast(t('discovery_done'), 'success');
    loadQueue();
    refreshBar();
  } catch (e) {
    status.textContent = t('discovery_failed');
    toast(`${t('discovery_failed')}: ${e.message}`, 'error');
  } finally {
    btn.disabled = false;
  }
}

$('#btn-run-discovery')?.addEventListener('click', runDiscovery);
function onLangChanged() { renderQueue(); }
loadQueue();
refreshBar();
setInterval(refreshBar, 3000);
setInterval(loadQueue, 8000);
</script>"""

REVIEW_SCRIPT = r"""
<script>
let queue = [], index = 0, pending = false;
const REVIEW_ACTIONS = ['approve','reject','defer','blocklist'];
const approved = JSON.parse(localStorage.getItem('domainhunter-approved') || '{}');
const outreachHistory = JSON.parse(localStorage.getItem('domainhunter-outreach-history') || '{}');
const PATH_CANDIDATE_ID = (() => {
  const m = location.pathname.match(/^\/review\/([^/]+)/);
  return m ? decodeURIComponent(m[1]) : null;
})();
const saveApproved = () => localStorage.setItem('domainhunter-approved', JSON.stringify(approved));
const saveOutreachHistory = () => localStorage.setItem('domainhunter-outreach-history', JSON.stringify(outreachHistory));

function renderEmpty() {
  const hero = $('#candidate-card');
  if (!hero) return;
  hero.innerHTML = `<div class="empty" style="padding:var(--space-7);text-align:center"><h2 style="font-family:var(--font-mono);font-size:24px;margin:0 0 var(--space-3)">${esc(t('empty_title'))}</h2><p style="margin:0 0 var(--space-5);color:var(--ink-2)">${esc(t('empty_hint'))}</p><a href="/" class="btn-primary" style="display:inline-block;text-decoration:none">← back to queue</a></div>`;
  if ($('#evidence-list')) $('#evidence-list').innerHTML = '';
  if ($('#priority-rows')) $('#priority-rows').innerHTML = '';
  if ($('#priority-meta')) $('#priority-meta').innerHTML = '';
  if ($('#gauge-score')) $('#gauge-score').textContent = '0.000';
  for (const id of ['arc-product','arc-early','arc-exposure','arc-complete']) {
    const a = document.getElementById(id);
    if (a) a.setAttribute('stroke-dasharray', '0 1000');
  }
  const bar = $('#actions-bar');
  if (bar) { bar.innerHTML = ''; bar.hidden = true; }
  hideOutreach();
}

function hideOutreach() {
  const p = $('#outreach-panel');
  if (!p) return;
  p.hidden = true;
  p.innerHTML = '';
}

function renderHero(item, isApproved) {
  const hero = $('#candidate-card');
  if (!hero) return;
  const canonicalLine = item.canonical_url
    ? `<span class="canonical"><span class="dim">${esc(item.domain)}</span> · <a href="${esc(item.canonical_url)}" target="_blank" rel="noopener">${esc(item.canonical_url)}</a></span>`
    : `<span class="canonical"><span class="dim">${esc(item.domain)}</span></span>`;
  const internalLine = item.internal_links && item.internal_links.length
    ? `<p class="internals"><span class="dim">${esc(t('internal_links'))}</span>${item.internal_links.map(u => `<a href="${esc(u)}" target="_blank" rel="noopener">${esc(u.replace(/^https?:\/\//, '').split('/')[0])}</a>`).join(' · ')}</p>`
    : '';
  hero.classList.remove('loading');
  hero.innerHTML = `
    ${pending ? '<div class="pending-bar"></div>' : ''}
    <div class="hero-nav">
      <button id="nav-prev" ${index === 0 ? 'disabled' : ''}>← ${esc(t('prev'))}</button>
      <span class="pos">${esc(t('pos', { i: index + 1, n: queue.length }))}</span>
      <button id="nav-next" ${index >= queue.length - 1 ? 'disabled' : ''}>${esc(t('next'))} →</button>
    </div>
    <div class="meta-row">
      <span class="chip muted">${esc(item.author_kind)}</span>
      <span class="chip muted">v${item.version}</span>
      ${isApproved ? `<span class="chip good">✓ ${esc(t('approved_session'))}</span>` : ''}
    </div>
    <h1 class="domain">${esc(item.name_suggestion || item.domain)}</h1>
    <span class="domain-mark"></span>
    ${canonicalLine}
    ${item.description_suggestion ? `<p class="desc">${esc(item.description_suggestion)}</p>` : ''}
    ${internalLine}
  `;
  $('#nav-prev')?.addEventListener('click', () => move(-1));
  $('#nav-next')?.addEventListener('click', () => move(1));
}

function renderEvidence(evidence) {
  const el = $('#evidence-list');
  if (!el) return;
  if (!evidence.length) {
    el.innerHTML = `<p class="evidence-empty">${esc(t('no_evidence'))}</p>`;
    return;
  }
  el.innerHTML = evidence.map(x => `<div class="evidence ${esc(x.type)}"><span class="kind">${esc(x.type)}</span><span class="quote">${esc(x.quote)}</span><small class="src">${esc(x.url || '')}</small></div>`).join('');
}

function renderGauge(p, item) {
  const contrib = {
    product: p.product_evidence_contribution,
    early: p.early_presence_contribution,
    exposure: p.low_exposure_contribution,
    complete: p.data_completeness_contribution,
  };
  const C = 2 * Math.PI * 50;
  const ARC_PCT = 0.25;
  const arcs = {
    'arc-product': contrib.product,
    'arc-early': contrib.early,
    'arc-exposure': contrib.exposure,
    'arc-complete': contrib.complete,
  };
  for (const [id, val] of Object.entries(arcs)) {
    const a = document.getElementById(id);
    if (!a) continue;
    const pct = Math.max(0, Math.min(1, val));
    a.setAttribute('stroke-dasharray', `${(pct * ARC_PCT * C).toFixed(2)} ${(ARC_PCT * C).toFixed(2)}`);
  }
  const scoreEl = $('#gauge-score');
  if (scoreEl) scoreEl.textContent = p.score.toFixed(3);
  const rowsEl = $('#priority-rows');
  if (rowsEl) {
    rowsEl.innerHTML = ['product','early','exposure','complete'].map(k => {
      const v = contrib[k];
      const pct = Math.max(0, Math.min(1, v));
      return `<div class="priority-row"><span class="label">${k}</span><div class="bar-bg"><div class="bar-fg ${k}" style="width:${(pct * 100).toFixed(1)}%"></div></div><span class="num">${v.toFixed(3)}</span></div>`;
    }).join('');
  }
  const metaEl = $('#priority-meta');
  if (metaEl) {
    const outcomeLabel = (I18N[currentLang].outcome || {})[item.primary_outcome] || item.primary_outcome;
    metaEl.innerHTML = `
      <div class="row"><span>${esc(t('primary_outcome'))}</span><b>${esc(outcomeLabel)}</b></div>
      <div class="row"><span>${esc(t('confidence'))}</span><b>${item.classification_confidence.toFixed(2)}</b></div>
      <div class="row"><span>${esc(t('formula'))}</span><b>v${esc(p.formula_version)}</b></div>
    `;
  }
}

function renderActions(isApproved) {
  const bar = $('#actions-bar');
  if (!bar) return;
  if (isApproved) {
    bar.hidden = false;
    bar.innerHTML = `<button class="btn approved-static" disabled>✓ ${esc(t('approved_session'))}</button>`;
    return;
  }
  const labels = { approve:'A', reject:'R', defer:'D', blocklist:'B', edit:'E' };
  const decide = REVIEW_ACTIONS.map(a => `<button class="btn ${a}" data-action="${a}" ${pending ? 'disabled' : ''}>${esc(t(a))}<span class="key">${labels[a]}</span></button>`).join('');
  const edit = `<button class="btn edit" data-action="edit" ${pending ? 'disabled' : ''}>${esc(t('edit'))}<span class="key">E</span></button>`;
  bar.hidden = false;
  bar.innerHTML = decide + edit;
  $$('#actions-bar [data-action]').forEach(b => b.addEventListener('click', () => act(b.dataset.action)));
}

function renderOutreach(item) {
  const p = $('#outreach-panel');
  if (!p) return;
  const history = outreachHistory[item.candidate_id] || [];
  const rows = history.length
    ? history.map(h => `<div class="row"><span class="chip ${h.dry_run ? 'info' : 'outreach'}">${h.dry_run ? 'dry' : 'real'}</span><span>${esc(h.at)} · ${h.contacts} ${currentLang === 'zh' ? '联系人' : 'contacts'} · ${h.tokens} token</span></div>`).join('')
    : `<div class="empty">${esc(t('outreach_history_empty'))}</div>`;
  p.hidden = false;
  p.innerHTML = `
    <p class="help">${esc(t('outreach_help'))}</p>
    <div class="outreach-row"><input id="recipient-url" placeholder="${esc(t('outreach_url_ph'))}"></div>
    <div class="outreach-buttons">
      <button class="btn outreach-dry" id="btn-outreach" ${pending ? 'disabled' : ''}>${esc(t('outreach_dry'))}<span class="key">O</span></button>
      <button class="btn outreach-real" id="btn-outreach-real" ${pending ? 'disabled' : ''}>${esc(t('outreach_real'))}</button>
    </div>
    <div class="outreach-history">${rows}</div>
  `;
  $('#btn-outreach')?.addEventListener('click', () => triggerOutreach(false));
  $('#btn-outreach-real')?.addEventListener('click', () => triggerOutreach(true));
}

function renderCurrent() {
  if (!queue.length) { renderEmpty(); return; }
  const item = queue[index];
  const evidence = item.evidence || [];
  const p = item.priority;
  const key = `${item.candidate_id}:${item.version}`;
  const isApproved = !!approved[key];
  renderHero(item, isApproved);
  renderEvidence(evidence);
  renderGauge(p, item);
  renderActions(isApproved);
  if (isApproved) renderOutreach(item); else hideOutreach();
}

function move(delta) {
  if (!queue.length) return;
  index = Math.max(0, Math.min(queue.length - 1, index + delta));
  renderCurrent();
  const newItem = queue[index];
  if (newItem) history.replaceState(null, '', `/review/${encodeURIComponent(newItem.candidate_id)}`);
}

function syncToPathCandidate() {
  if (!PATH_CANDIDATE_ID || !queue.length) return;
  const idx = queue.findIndex(it => it.candidate_id === PATH_CANDIDATE_ID);
  if (idx >= 0) index = idx;
}

async function load() {
  try {
    const r = await fetch('/v1/review-queue');
    if (!r.ok) throw new Error('Queue request failed');
    queue = (await r.json()).items;
    queue.forEach(it => { if (it.is_approved) approved[`${it.candidate_id}:${it.version}`] = true; });
    saveApproved();
    syncToPathCandidate();
    if (index >= queue.length) index = Math.max(0, queue.length - 1);
    renderCurrent();
  } catch (e) { toast(t('toast_load_err', { err: e.message }), 'error'); }
}

async function act(action) {
  if (action === 'edit') { toast(t('toast_edit_noop'), 'success'); return; }
  const item = queue[index];
  if (!item || pending) return;
  if (!$('#actor').value.trim()) { toast(t('toast_no_actor'), 'error'); $('#actor').focus(); return; }
  pending = true; renderCurrent();
  const requestId = crypto.randomUUID();
  try {
    const r = await fetch(`/v1/candidates/${item.candidate_id}/versions/${item.version}/decisions`, {
      method:'POST',
      headers:{'Content-Type':'application/json','X-Actor-ID':$('#actor').value.trim()},
      body:JSON.stringify({ request_id:requestId, action, reason_tags:[] }),
    });
    if (!r.ok) throw Error(((await r.json()).detail) || 'Decision failed');
    toast(t('toast_decision_ok', { action: t(action) }), 'success');
    if (action === 'approve') {
      approved[`${item.candidate_id}:${item.version}`] = true;
      saveApproved();
    }
    queue.splice(index, 1);
    if (index >= queue.length) index = Math.max(0, queue.length - 1);
    const next = queue[index];
    if (next) history.replaceState(null, '', `/review/${encodeURIComponent(next.candidate_id)}`);
    refreshBar();
    renderCurrent();
  } catch (e) { toast(t('toast_load_err', { err: e.message }), 'error'); }
  finally { pending = false; renderCurrent(); }
}

async function triggerOutreach(real) {
  const item = queue[index];
  if (!item) return;
  if (!$('#actor').value.trim()) { toast(t('toast_actor_outreach'), 'error'); $('#actor').focus(); return; }
  const url = $('#recipient-url')?.value.trim() || null;
  if (real && !url) { toast(t('toast_real_needs_url'), 'error'); return; }
  const body = { request_id:crypto.randomUUID(), dry_run:!real, recipient_source_url:url };
  try {
    const r = await fetch(`/v1/candidates/${item.candidate_id}/versions/${item.version}/outreach`, {
      method:'POST',
      headers:{'Content-Type':'application/json','X-Actor-ID':$('#actor').value.trim()},
      body:JSON.stringify(body),
    });
    const j = await r.json();
    if (!r.ok) throw new Error((j.detail) || 'Outreach failed');
    outreachHistory[item.candidate_id] = outreachHistory[item.candidate_id] || [];
    outreachHistory[item.candidate_id].unshift({
      at: new Date().toISOString().slice(11, 19),
      dry_run: !real,
      contacts: (j.contact_preview || []).length,
      tokens: j.tokens_issued || 0,
    });
    saveOutreachHistory();
    toast(t('toast_outreach_ok', { status: j.status || 'done' }), 'success');
    renderCurrent();
  } catch (e) { toast(t('toast_outreach_err', { err: e.message }), 'error'); }
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && e.target.tagName === 'INPUT') { e.target.blur(); return; }
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT' || e.metaKey || e.ctrlKey || e.altKey) return;
  const map = { a:'approve', r:'reject', d:'defer', b:'blocklist', e:'edit', j:'prev', k:'next', o:'outreach', arrowleft:'prev', arrowright:'next' };
  const action = map[e.key.toLowerCase()];
  if (!action) return;
  e.preventDefault();
  if (action === 'prev') move(-1);
  else if (action === 'next') move(1);
  else if (action === 'outreach') {
    const item = queue[index];
    const key = `${item?.candidate_id}:${item?.version}`;
    if (item && approved[key]) triggerOutreach(false);
  } else act(action);
});

function onMetrics() { /* not used on review page */ }
function onLangChanged() { renderCurrent(); }
load();
refreshBar();
setInterval(refreshBar, 3000);
</script>"""

DISCOVERY_SCRIPT = r"""
<script>
let lastRunSummary = null;

function renderDiscoveryOverview(data) {
  const el = $('#discovery-result');
  if (!el) return;
  const counts = data.counts || {};
  const domains = data.domains || [];
  const summaryLine = lastRunSummary
    ? t('discovery_summary', {
        certificates: lastRunSummary.certificates_seen,
        events: lastRunSummary.events_added,
        probes: lastRunSummary.probes_run,
        candidates: lastRunSummary.candidates_created,
      })
    : `${counts.domains || 0} domains · ${counts.observations || 0} probes · ${counts.candidates || 0} candidates`;
  const summaryEl = $('#discovery-status');
  if (summaryEl) summaryEl.textContent = summaryLine;
  if (!domains.length) {
    el.innerHTML = `<div class="empty">${esc(t('discovery_empty'))}</div>`;
    return;
  }
  el.innerHTML = domains.slice(0, 50).map(d => {
    const seen = (d.first_seen_at || '').slice(11, 19) || '–';
    const outcome = d.outcome_code
      ? `<span class="outcome">${esc(d.outcome_code)}</span>`
      : `<span class="outcome">—</span>`;
    return `<div class="row"><span class="domain">${esc(d.domain)}</span><span class="outcome">${esc(outcome.replace(/<[^>]+>/g, ''))}</span><span class="seen">${esc(seen)}</span></div>`;
  }).join('');
}

async function loadDiscoveryOverview() {
  try {
    const r = await fetch('/v1/discovery/overview');
    if (!r.ok) throw new Error('overview failed');
    renderDiscoveryOverview(await r.json());
  } catch (e) {}
}

async function runDiscovery() {
  const btn = $('#btn-run-discovery');
  const status = $('#discovery-status');
  const maxProbes = Math.max(1, Math.min(200, parseInt($('#max-probes')?.value || '20', 10) || 20));
  if (!btn || !status) return;
  btn.disabled = true;
  status.textContent = t('discovery_running');
  try {
    const r = await fetch('/v1/run/discovery', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ max_probes: maxProbes }),
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.detail || 'discovery run failed');
    lastRunSummary = j;
    toast(t('discovery_done'), 'success');
    await loadDiscoveryOverview();
    refreshBar();
  } catch (e) {
    status.textContent = t('discovery_failed');
    toast(`${t('discovery_failed')}: ${e.message}`, 'error');
  } finally {
    btn.disabled = false;
  }
}

$('#btn-run-discovery')?.addEventListener('click', runDiscovery);
function onMetrics() { /* not used here */ }
function onLangChanged() { loadDiscoveryOverview(); }
loadDiscoveryOverview();
refreshBar();
setInterval(refreshBar, 3000);
setInterval(loadDiscoveryOverview, 15000);
</script>"""

OPS_SCRIPT = r"""
<script>
let lastAlerts = [];

function renderAlerts(alerts) {
  const el = $('#alerts');
  if (!el) return;
  const list = alerts || [];
  el.innerHTML = list.length
    ? list.slice(0, 8).map(x => `<div class="alert-row ${esc(x.severity)}"><div class="meta"><span class="chip ${x.severity === 'critical' ? 'bad' : x.severity === 'warn' ? 'warn' : 'info'}">${esc(x.severity)}</span><span class="kind">${esc(x.kind)}</span></div><div class="title">${esc(x.title || '')}</div></div>`).join('')
    : `<div class="alert-empty">✓ ${esc(t('no_alerts'))}</div>`;
  const rb = $('#runbook');
  if (rb) {
    rb.innerHTML = list.length
      ? `${esc(t('runbook_title'))} · <b>GET /v1/runbooks/${esc(list[0].runbook_id)}</b>`
      : `✓ ${esc(t('no_alerts_runbook'))}`;
  }
}

function onMetrics(metrics, analytics, alerts) {
  const a = analytics || {};
  const m = metrics || {};
  const conv = (a.conversion_source_to_candidate || 0) * 100;
  const kc = $('#k-conv'); if (kc) kc.textContent = conv.toFixed(0) + '%';
  const kcost = $('#k-cost'); if (kcost) kcost.textContent = (m.cost_per_effective_candidate || 0).toFixed(2);
  const kp50 = $('#k-p50'); if (kp50) kp50.textContent = a.latency_first_signal_to_candidate_p50_seconds != null ? fmtLatency(a.latency_first_signal_to_candidate_p50_seconds) : '–';
  const kp95 = $('#k-p95'); if (kp95) kp95.textContent = a.latency_first_signal_to_candidate_p95_seconds != null ? fmtLatency(a.latency_first_signal_to_candidate_p95_seconds) : '–';
  lastAlerts = alerts.entries || alerts.alerts || [];
  renderAlerts(lastAlerts);
}

function onLangChanged() { renderAlerts(lastAlerts); }
refreshBar();
setInterval(refreshBar, 3000);
</script>"""

_PAGE_TAIL = r"""
<div id="toast" class="toast"></div>
</body></html>"""


def _candidate_detail_page(candidate_id: str) -> str:
    """Render one quiet, evidence-first candidate review page."""
    page = r"""<!doctype html>
<html lang="zh-Hans">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>候选详情 · DomainHunter</title>
  <style>
    :root { --canvas:#f7f4ed;--surface:#fff;--ink:#17202c;--muted:#5f6b7a;--line:#e1ddd5;--action:#1769e0;--verified:#007e72;--warning:#a66100;--danger:#b42318;--focus:#1d4ed8;--radius:14px; }
    * { box-sizing:border-box; } html { background:var(--canvas); color:var(--ink); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; } body { margin:0; min-width:320px; } button,input { font:inherit; }
    a { color:var(--action); } button:focus-visible,a:focus-visible,input:focus-visible { outline:3px solid var(--focus); outline-offset:2px; }
    .page { max-width:960px; margin:0 auto; padding:36px 24px 116px; } .back-link { display:inline-block; color:var(--muted); text-decoration:none; margin-bottom:30px; font-weight:650; } .back-link:hover { color:var(--action); }
    .identity { border-bottom:1px solid var(--line); padding-bottom:28px; margin-bottom:26px; } .identity-row { display:flex; justify-content:space-between; gap:16px; align-items:start; } .domain { font:750 clamp(24px,4vw,38px)/1.14 ui-monospace,"SF Mono",Menlo,Consolas,monospace; letter-spacing:-.045em; overflow-wrap:anywhere; margin:0 0 12px; } .name { font-size:19px; font-weight:710; letter-spacing:-.025em; margin:0 0 12px; } .visit-link { flex:0 0 auto; min-height:40px; display:inline-flex; align-items:center; border:1px solid #b9caea; background:#edf4ff; border-radius:8px; padding:0 12px; font-size:14px; font-weight:700; text-decoration:none; }
    .chips { display:flex; flex-wrap:wrap; gap:8px; } .chip { display:inline-flex; min-height:26px; align-items:center; border-radius:999px; padding:2px 9px; color:#435064; background:#f0eee8; font-size:12px; font-weight:700; } .chip.priority { color:#174d9e;background:#edf4ff; } .chip.pending { color:#765500;background:#fff5e5; }.chip.decided { color:#006557;background:#e9f7f3; }
    h2 { font-size:20px; letter-spacing:-.025em; margin:0 0 14px; } h3 { font-size:15px; margin:0 0 10px; } .evidence-grid { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-bottom:25px; } .evidence-panel,.product-panel,.action-panel,.not-found { border:1px solid var(--line); border-radius:var(--radius); background:var(--surface); padding:20px; } .evidence-panel p,.product-panel p { color:var(--muted); line-height:1.58; margin:0; }
    .fact-list { display:grid; gap:9px; } .fact { border-left:3px solid #d1d5db; background:#faf9f6; padding:10px 11px; border-radius:0 7px 7px 0; } .fact.passed { border-color:var(--verified); } .fact.unknown { border-color:var(--warning); } .fact.failed { border-color:var(--danger); } .fact-title { display:block; font-size:13px; font-weight:750; margin-bottom:3px; } .fact-copy { font-size:13px; color:var(--muted); line-height:1.5; }.fact-status { font-weight:750; }
    .product-panel { margin-bottom:20px; } .quote-list { list-style:none; padding:0; margin:14px 0 0; display:grid; gap:10px; }.quote { border-top:1px solid var(--line); padding-top:10px; }.quote-text { margin:0 0 5px; color:#364152 !important; }.quote-type { color:var(--muted); font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:.04em; }.quote a { font-size:13px; overflow-wrap:anywhere; }
    details { border:1px solid var(--line); border-radius:10px; background:var(--surface); margin-bottom:22px; } summary { cursor:pointer; padding:14px 16px; font-weight:720; } pre { overflow:auto; max-height:340px; margin:0; padding:0 16px 16px; color:#364152; font:12px/1.55 ui-monospace,"SF Mono",Menlo,Consolas,monospace; white-space:pre-wrap; word-break:break-word; }
    .action-panel { position:sticky; bottom:14px; box-shadow:0 9px 26px rgba(23,32,44,.09); }.action-copy { color:var(--muted); font-size:14px; margin:0 0 13px; }.actions { display:flex; gap:9px; flex-wrap:wrap; }.action { min-height:40px; border-radius:8px; padding:0 13px; cursor:pointer; font-weight:710; }.approve { color:white;background:var(--action);border:1px solid var(--action); }.defer { color:var(--ink);background:white;border:1px solid var(--line); }.reject { color:#8a1c14;background:#fff0ef;border:1px solid #efb6b1; }.blocklist { color:#8a1c14;background:white;border:1px solid #d88e88; }.action:disabled { opacity:.55; cursor:not-allowed; }
    .feedback { margin:0 0 16px; padding:11px 13px; border-radius:8px; background:#fff0ef; border:1px solid #efb6b1; color:#8a1c14; font-size:14px; }.loading { min-height:230px; color:var(--muted); display:grid; place-items:center; }.not-found { text-align:center; padding:40px 20px; }.not-found p { color:var(--muted); }
    #review-dialog { position:fixed; inset:0; z-index:10; background:rgba(23,32,44,.4); display:grid; place-items:center; padding:16px; } #review-dialog[hidden] { display:none; }.dialog-card { width:min(430px,100%); background:var(--surface); border-radius:var(--radius); padding:22px; box-shadow:0 24px 70px rgba(17,24,39,.25); }.dialog-card h2 { margin-bottom:8px; }.dialog-card p { color:var(--muted); line-height:1.55; }.dialog-card input { width:100%; min-height:42px; padding:0 11px; border:1px solid var(--line); border-radius:8px; color:var(--ink); }.dialog-actions { display:flex; justify-content:flex-end; gap:9px; margin-top:16px; }.secondary,.primary { min-height:40px; padding:0 13px; border-radius:8px; cursor:pointer; font-weight:700; }.secondary { background:white;border:1px solid var(--line);color:var(--ink); }.primary { background:var(--action);border:1px solid var(--action);color:white; }
    @media (max-width:680px) { .page { padding:25px 16px 112px; }.identity-row { flex-direction:column; }.visit-link { margin-top:-4px; }.evidence-grid { grid-template-columns:1fr; }.evidence-panel,.product-panel,.action-panel { padding:16px; }.actions { display:grid; grid-template-columns:1fr 1fr; }.approve { grid-column:1 / -1; } }
    @media (prefers-reduced-motion:reduce) { *,*::before,*::after { transition-duration:.01ms!important; } }
  </style>
</head>
<body>
  <main class="page" data-page="candidate-detail">
    <a class="back-link" href="/">← 返回候选收件箱</a>
    <section id="candidate-detail" aria-live="polite"><div class="loading">正在读取候选详情…</div></section>
    <section id="newness-evidence" aria-label="新网站证据"></section>
    <section id="reachability-evidence" aria-label="可访问与产品证据"></section>
    <section id="product-evidence" aria-label="产品判断"></section>
    <details id="audit-details"><summary>查看原始审计数据</summary><pre></pre></details>
    <section id="review-actions" aria-label="审核决定"></section>
  </main>
  <div id="review-dialog" role="dialog" aria-modal="true" aria-labelledby="actor-title" hidden></div>
  <script>
    (() => {
      const candidateId = __CANDIDATE_ID__;
      const $ = (selector) => document.querySelector(selector);
      const esc = (value) => String(value ?? '').replace(/[&<>'\"]/g, char => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', "'":'&#39;', '"':'&quot;' })[char]);
      const labels = { publishable_ai_saas:'AI SaaS 候选', valid_but_not_ready:'需要更多证据', not_target:'不属于目标', duplicate_or_existing:'可能为已有网站', policy_excluded:'不符合规则' };
      const actionLabels = { approve:'批准', defer:'暂缓', reject:'拒绝', blocklist:'拉黑' };
      const stateLabels = { pending:'待审核', approve:'已批准', defer:'已暂缓', reject:'已拒绝', blocklist:'已拉黑' };
      const factStatus = (status) => ({ passed:'通过', unknown:'未验证', failed:'不通过' })[status] || '未验证';
      let detail = null; let submitting = false;
      function fact(status, title, copy) { return `<div class="fact ${esc(status || 'unknown')}"><span class="fact-title"><span class="fact-status">${esc(factStatus(status))}</span> · ${esc(title)}</span><span class="fact-copy">${esc(copy)}</span></div>`; }
      function renderEvidence() {
        const newness = detail.newness || {}; const reachability = detail.reachability || {};
        const ctCopy = newness.ct_first_seen_at ? `CT 首见：${newness.ct_first_seen_at}` : '没有保存该候选版本的 CT 首见时间。';
        const rdapCopy = newness.rdap_tier ? `RDAP 判定：${newness.rdap_tier}${Number.isFinite(newness.rdap_age_days) ? `；注册年龄 ${newness.rdap_age_days} 天` : ''}${newness.rdap_registration_at ? `；注册时间 ${newness.rdap_registration_at}` : ''}` : '没有保存 RDAP 注册信息。';
        $('#newness-evidence').innerHTML = `<h2>为什么它进入收件箱</h2><div class="evidence-grid"><article class="evidence-panel"><h3>新网站证据</h3><div class="fact-list">${fact(newness.status, 'CT 首见与近期注册', ctCopy)}${fact(newness.status, 'RDAP 注册信息', rdapCopy)}</div></article><article class="evidence-panel"><h3>可访问性</h3><div class="fact-list">${fact(reachability.status, 'HTTP 响应', reachability.http_status_code ? `HTTP ${reachability.http_status_code}${reachability.final_url ? `；最终地址 ${reachability.final_url}` : ''}` : '没有保存 HTTP 探测结果。')}${fact(reachability.status, '最终根域名一致性', reachability.same_root === true ? '最终地址仍在该候选的根域名下。' : reachability.same_root === false ? '最终地址已离开该候选的根域名。' : '没有保存根域名一致性验证。')}</div></article></div>`;
        const links = detail.internal_links?.length ? `<p>已发现的站内链接：${detail.internal_links.map(link => `<a href="${esc(link)}" target="_blank" rel="noopener noreferrer">${esc(link)}</a>`).join(' · ')}</p>` : '';
        $('#reachability-evidence').innerHTML = links ? `<div class="product-panel"><h3>已发现的站内链接</h3>${links}</div>` : '';
      }
      function renderProduct() {
        const evidence = detail.evidence || [];
        const description = detail.description_suggestion ? `<p>${esc(detail.description_suggestion)}</p>` : '<p>该候选没有保存产品摘要。</p>';
        const quotes = evidence.length ? `<ul class="quote-list">${evidence.map(item => `<li class="quote"><span class="quote-type">${esc(item.type)}</span><p class="quote-text">${esc(item.quote)}</p>${item.url ? `<a href="${esc(item.url)}" target="_blank" rel="noopener noreferrer">查看来源 ↗</a>` : ''}</li>`).join('')}</ul>` : '<p>没有保存产品引用。</p>';
        $('#product-evidence').innerHTML = `<article class="product-panel"><h2>产品判断</h2>${description}<div class="chips"><span class="chip">${esc(labels[detail.primary_outcome] || detail.primary_outcome || '待判断')}</span>${typeof detail.classification_confidence === 'number' ? `<span class="chip">置信度 ${detail.classification_confidence.toFixed(2)}</span>` : ''}</div>${quotes}</article>`;
      }
      function renderActions() {
        const section = $('#review-actions');
        if (detail.review_state !== 'pending') { section.innerHTML = `<div class="action-panel"><h2>审核决定</h2><p class="action-copy">该候选当前状态为“${esc(stateLabels[detail.review_state] || detail.review_state)}”，不再显示待审操作。</p><a href="/" class="primary" style="display:inline-flex;align-items:center;text-decoration:none">返回候选收件箱</a></div>`; return; }
        section.innerHTML = `<div class="action-panel"><h2>审核决定</h2><p class="action-copy">批准表示通过内部人工审核，不会自动公开发布。</p><div id="decision-feedback"></div><div class="actions">${Object.entries(actionLabels).map(([action,label]) => `<button class="action ${action}" data-action="${action}" type="button" ${submitting ? 'disabled' : ''}>${label}</button>`).join('')}</div></div>`;
        section.querySelectorAll('[data-action]').forEach(button => button.addEventListener('click', () => submitDecision(button.dataset.action)));
      }
      function renderDetail() {
        const priority = detail.priority && typeof detail.priority.score === 'number' ? `优先级 ${detail.priority.score.toFixed(2)}` : null;
        const citedUrl = detail.evidence?.find(item => item.url)?.url;
        const visitUrl = detail.canonical_url || citedUrl;
        const external = visitUrl ? `<a class="visit-link" href="${esc(visitUrl)}" target="_blank" rel="noopener noreferrer">访问网站 ↗</a>` : '';
        $('#candidate-detail').innerHTML = `<header class="identity"><div class="identity-row"><div><h1 class="domain">${esc(detail.domain)}</h1>${detail.name_suggestion ? `<p class="name">${esc(detail.name_suggestion)}</p>` : ''}<div class="chips"><span class="chip">${esc(labels[detail.primary_outcome] || detail.primary_outcome || '待判断')}</span>${priority ? `<span class="chip priority">${esc(priority)}</span>` : ''}<span class="chip ${detail.review_state === 'pending' ? 'pending' : 'decided'}">${esc(stateLabels[detail.review_state] || detail.review_state)}</span><span class="chip">版本 ${esc(detail.version)}</span></div></div>${external}</div></header>`;
        renderEvidence(); renderProduct();
        $('#audit-details pre').textContent = JSON.stringify({ candidate_id:detail.candidate_id, version:detail.version, review_state:detail.review_state, newness:detail.newness, reachability:detail.reachability, evidence:detail.evidence, audit:detail.audit }, null, 2);
        renderActions();
      }
      function renderMissing(message) { $('#candidate-detail').innerHTML = `<div class="not-found"><h1>此候选目前不可审核</h1><p>${esc(message)}</p><a class="primary" href="/" style="display:inline-flex;align-items:center;text-decoration:none">返回候选收件箱</a></div>`; ['#newness-evidence','#reachability-evidence','#product-evidence','#audit-details','#review-actions'].forEach(id => $(id).hidden = true); }
      function showFeedback(message) { const node = $('#decision-feedback'); if (node) node.innerHTML = `<p class="feedback">${esc(message)}</p>`; }
      function requireActorId() {
        const existing = localStorage.getItem('domainhunter.actor-id')?.trim(); if (existing) return Promise.resolve(existing);
        const dialog = $('#review-dialog'); dialog.hidden = false; dialog.innerHTML = `<div class="dialog-card"><h2 id="actor-title">填写审核人</h2><p>审核决定会记录此名称或 ID，并仅保存在当前浏览器中供后续操作使用。</p><input id="actor-id" autocomplete="name" placeholder="例如：lonnie"><div class="dialog-actions"><button class="secondary" id="cancel-actor" type="button">取消</button><button class="primary" id="save-actor" type="button">继续</button></div></div>`;
        return new Promise(resolve => { const finish = value => { dialog.hidden = true; dialog.innerHTML = ''; resolve(value); }; $('#cancel-actor').addEventListener('click', () => finish(null)); $('#save-actor').addEventListener('click', () => { const value = $('#actor-id').value.trim(); if (value) { localStorage.setItem('domainhunter.actor-id', value); finish(value); } else { $('#actor-id').focus(); } }); $('#actor-id').addEventListener('keydown', event => { if (event.key === 'Enter') $('#save-actor').click(); }); $('#actor-id').focus(); });
      }
      async function submitDecision(action) {
        if (!detail || submitting) return; const actorId = await requireActorId(); if (!actorId) return;
        const confirmCopy = { reject:'确定拒绝此候选吗？', blocklist:'确定拉黑此候选吗？这会阻止其继续进入审核流程。' };
        if (confirmCopy[action] && !window.confirm(confirmCopy[action])) return;
        submitting = true; renderActions();
        try {
          const response = await fetch(`/v1/candidates/${encodeURIComponent(detail.candidate_id)}/versions/${detail.version}/decisions`, { method:'POST', headers:{ 'Content-Type':'application/json', 'X-Actor-ID':actorId }, body:JSON.stringify({ request_id:crypto.randomUUID(), action, reason_tags:[] }) });
          const body = await response.json().catch(() => ({}));
          if (response.ok) { sessionStorage.setItem('domainhunter.flash', `${actionLabels[action]}：已记录人工审核决定。`); location.assign('/'); return; }
          if (response.status === 401) { localStorage.removeItem('domainhunter.actor-id'); showFeedback('审核人身份需要重新输入。'); }
          else if (response.status === 404) { renderMissing('候选已不存在或已被移出当前审核范围。'); return; }
          else if (response.status === 409) { showFeedback('此候选已被其他操作更新。正在刷新详情。'); await loadDetail(); return; }
          else showFeedback(`提交失败：${body.detail || `HTTP ${response.status}`}`);
        } catch (error) { showFeedback(`提交失败：${error.message || '请求未完成'}`); }
        finally { submitting = false; if (detail) renderActions(); }
      }
      async function loadDetail() { try { const response = await fetch(`/v1/candidates/${encodeURIComponent(candidateId)}/review-context`, { cache:'no-store' }); const body = await response.json().catch(() => ({})); if (response.status === 404) { renderMissing('候选不存在，或没有可读取的候选版本。'); return; } if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`); detail = body; renderDetail(); } catch (error) { $('#candidate-detail').innerHTML = `<div class="not-found"><h1>详情读取失败</h1><p>${esc(error.message || '请求未完成')}</p><button class="primary" id="retry-detail" type="button">重试</button></div>`; $('#retry-detail')?.addEventListener('click', loadDetail); ['#newness-evidence','#reachability-evidence','#product-evidence','#audit-details','#review-actions'].forEach(id => $(id).hidden = true); } }
      loadDetail();
    })();
  </script>
</body></html>"""
    return page.replace("__CANDIDATE_ID__", json.dumps(candidate_id))


def make_topbar(active: str, current_id: str = "") -> str:
    """Return the shared topbar with the active tab highlighted."""
    tabs = {"queue": "", "review": "", "discovery": "", "ops": ""}
    tabs[active] = "active"
    review_href = f"/review/{current_id}" if current_id else "/"
    review_disabled = "" if current_id else ' aria-disabled="true"'
    return TOPBAR_TEMPLATE.format(
        ACTIVE=active,
        tab_queue=tabs["queue"],
        tab_review=tabs["review"],
        tab_discovery=tabs["discovery"],
        tab_ops=tabs["ops"],
        review_href=review_href,
        review_disabled=review_disabled,
    )


def _build_page(active: str, body_html: str, page_script: str, current_id: str = "") -> str:
    """Compose one full HTML page from shared head + topbar + body + scripts + tail."""
    return (
        _PAGE_HEAD
        + make_topbar(active, current_id)
        + body_html
        + _PAGE_TAIL
        + SHARED_SCRIPT
        + page_script
    )


def _review_page(candidate_id: str) -> str:
    return _candidate_detail_page(candidate_id)


def _inbox_page() -> str:
    """Render the deliberately small, data-led discovery inbox."""
    return r"""<!doctype html>
<html lang="zh-Hans">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>候选收件箱 · DomainHunter</title>
  <style>
    :root {
      --canvas: #f7f4ed; --surface: #ffffff; --ink: #17202c;
      --muted: #5f6b7a; --line: #e1ddd5; --action: #1769e0;
      --verified: #007e72; --warning: #a66100; --danger: #b42318;
      --focus: #1d4ed8; --soft-blue: #edf4ff; --soft-green: #e9f7f3;
      --soft-amber: #fff5e5; --radius: 14px;
    }
    * { box-sizing: border-box; }
    html { background: var(--canvas); color: var(--ink); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; }
    body { margin: 0; min-width: 320px; }
    button, input { font: inherit; }
    button, a, input { -webkit-tap-highlight-color: transparent; }
    button:focus-visible, a:focus-visible, input:focus-visible { outline: 3px solid var(--focus); outline-offset: 2px; }
    .topbar { height: 56px; border-bottom: 1px solid var(--line); background: rgba(247,244,237,.94); backdrop-filter: blur(12px); position: sticky; top: 0; z-index: 5; }
    .topbar-inner { max-width: 1080px; height: 100%; margin: 0 auto; padding: 0 24px; display: flex; align-items: center; gap: 20px; }
    .brand { color: var(--ink); text-decoration: none; font-size: 17px; font-weight: 760; letter-spacing: -.02em; }
    .page-name { font-size: 14px; color: var(--muted); border-left: 1px solid var(--line); padding-left: 20px; }
    .scan-button { margin-left: auto; min-height: 40px; border: 0; border-radius: 9px; padding: 0 15px; background: var(--action); color: white; cursor: pointer; font-weight: 650; }
    .scan-button:hover { background: #0f57bd; }
    .layout { max-width: 960px; margin: 0 auto; padding: 48px 24px 72px; }
    .intro { display: flex; gap: 24px; justify-content: space-between; align-items: end; margin-bottom: 32px; }
    h1, h2, h3, p { margin-top: 0; }
    h1 { font-size: clamp(28px, 4vw, 40px); line-height: 1.12; letter-spacing: -.045em; margin-bottom: 10px; }
    .lede { color: var(--muted); font-size: 15px; line-height: 1.6; margin: 0; }
    .search { width: min(310px, 100%); min-height: 42px; border: 1px solid var(--line); background: var(--surface); border-radius: 9px; padding: 0 13px; color: var(--ink); }
    .search::placeholder { color: #7b8490; }
    .section-label { color: var(--muted); font-size: 13px; font-weight: 700; letter-spacing: .04em; margin: 34px 0 12px; }
    .candidate-card { display: block; color: var(--ink); text-decoration: none; background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius); padding: 23px; transition: border-color .15s ease, box-shadow .15s ease, transform .15s ease; }
    .candidate-card:hover { border-color: #a9bee0; box-shadow: 0 9px 25px rgba(23,32,44,.08); transform: translateY(-1px); text-decoration: none; }
    .candidate-card + .candidate-card { margin-top: 10px; }
    .candidate-card--priority { border-color: #bfd2f1; padding: 28px; }
    .card-top, .card-foot, .facts { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .card-top { justify-content: space-between; }
    .domain { font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace; overflow-wrap: anywhere; font-size: 15px; font-weight: 750; }
    .priority { border: 1px solid #b9caea; color: #174d9e; background: var(--soft-blue); border-radius: 999px; padding: 3px 9px; white-space: nowrap; font-size: 12px; font-weight: 700; }
    .product-name { font-size: 21px; letter-spacing: -.025em; margin: 12px 0 4px; font-weight: 720; }
    .candidate-card:not(.candidate-card--priority) .product-name { font-size: 17px; }
    .description { color: var(--muted); line-height: 1.55; margin: 0 0 13px; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
    .fact { display: inline-flex; align-items: center; min-height: 26px; border-radius: 6px; padding: 2px 8px; font-size: 12px; font-weight: 600; }
    .fact.passed { color: #006557; background: var(--soft-green); }
    .fact.unknown { color: #775600; background: var(--soft-amber); }
    .fact.failed { color: var(--danger); background: #fff0ef; }
    .evidence-quote { color: #364152; font-size: 14px; line-height: 1.55; margin: 15px 0 0; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
    .card-foot { justify-content: flex-end; margin-top: 16px; color: var(--action); font-size: 14px; font-weight: 700; }
    .state-card { padding: 32px; background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius); text-align: center; }
    .state-card h2 { font-size: 19px; margin-bottom: 8px; }
    .state-card p { color: var(--muted); margin-bottom: 18px; }
    .link-button { color: var(--action); border: 0; background: none; padding: 0; cursor: pointer; font-weight: 700; text-decoration: underline; }
    .error-banner { display: flex; align-items: center; gap: 10px; margin-bottom: 18px; border: 1px solid #efb6b1; background: #fff0ef; border-radius: 9px; padding: 11px 13px; color: #8a1c14; font-size: 14px; }
    .error-banner button { margin-left: auto; border: 0; background: transparent; color: inherit; cursor: pointer; font-weight: 700; text-decoration: underline; }
    .skeleton { min-height: 156px; background: linear-gradient(90deg,#fff 25%,#f0eee8 37%,#fff 63%); background-size: 400% 100%; animation: shimmer 1.2s ease infinite; }
    @keyframes shimmer { to { background-position: -135% 0; } }
    dialog { max-width: 510px; width: calc(100% - 32px); padding: 0; border: 0; border-radius: var(--radius); color: var(--ink); box-shadow: 0 24px 70px rgba(17,24,39,.25); }
    dialog::backdrop { background: rgba(23,32,44,.4); }
    .dialog-inner { padding: 26px; }
    .dialog-inner h2 { font-size: 22px; letter-spacing: -.03em; margin-bottom: 10px; }
    .dialog-copy { color: var(--muted); line-height: 1.6; margin-bottom: 22px; }
    .dialog-result { border-left: 3px solid var(--action); background: #f4f8ff; padding: 12px 13px; margin: 0 0 20px; line-height: 1.6; font-size: 14px; white-space: pre-line; }
    .dialog-result.error { border-color: var(--danger); background: #fff0ef; color: #8a1c14; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 10px; }
    .secondary, .primary { min-height: 40px; border-radius: 8px; padding: 0 14px; cursor: pointer; font-weight: 680; }
    .secondary { color: var(--ink); background: white; border: 1px solid var(--line); }
    .primary { color: white; background: var(--action); border: 1px solid var(--action); }
    .primary:disabled, .secondary:disabled, .scan-button:disabled { opacity: .55; cursor: not-allowed; }
    #ui-status { position: fixed; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); white-space: nowrap; }
    @media (max-width: 640px) {
      .topbar-inner { padding: 0 16px; gap: 12px; } .page-name { display: none; }
      .scan-button { font-size: 14px; padding: 0 12px; } .layout { padding: 32px 16px 56px; }
      .intro { align-items: stretch; flex-direction: column; gap: 16px; margin-bottom: 24px; } .search { width: 100%; }
      .candidate-card, .candidate-card--priority { padding: 18px; } .product-name { font-size: 19px; }
    }
    @media (prefers-reduced-motion: reduce) { *, *::before, *::after { animation-duration: .01ms !important; transition-duration: .01ms !important; } }
  </style>
</head>
<body>
  <header class="topbar"><div class="topbar-inner">
    <a class="brand" href="/">DomainHunter</a><span class="page-name">候选收件箱</span>
    <button class="scan-button" id="open-scan" type="button">扫描新网站</button>
  </div></header>
  <main class="layout" data-page="inbox">
    <div id="ui-status" aria-live="polite"></div>
    <div class="intro">
      <div><h1 id="inbox-summary">正在读取候选…</h1><p class="lede">仅显示通过严格新网站门槛的候选，按审核优先级排序。</p></div>
      <label><span class="sr-only" hidden>搜索候选</span><input class="search" id="candidate-search" type="search" placeholder="搜索域名或产品" autocomplete="off"></label>
    </div>
    <div id="inbox-error"></div>
    <section id="priority-section" hidden><p class="section-label">最高优先级</p><div id="priority-candidate"></div></section>
    <section id="candidate-section" hidden><p class="section-label">其他待审核候选</p><div id="candidate-list"></div></section>
    <section id="inbox-empty" hidden></section>
  </main>
  <dialog id="scan-dialog" aria-labelledby="scan-title">
    <div class="dialog-inner">
      <h2 id="scan-title">扫描新网站</h2>
      <p class="dialog-copy">将从已配置的真实 CT 来源进行一次严格扫描。只保留满足新网站、可访问性和根域名一致性规则的候选。</p>
      <div id="scan-result" hidden class="dialog-result" aria-live="polite"></div>
      <div class="dialog-actions"><button class="secondary" id="close-scan" type="button">取消</button><button class="primary" id="start-scan" type="button">开始严格扫描</button></div>
    </div>
  </dialog>
  <script>
    (() => {
      const stateKey = 'domainhunter.inbox';
      const $ = (selector) => document.querySelector(selector);
      const esc = (value) => String(value ?? '').replace(/[&<>'\"]/g, char => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', "'":'&#39;', '"':'&quot;' })[char]);
      const statusText = (status) => ({ passed: '通过', failed: '不通过', unknown: '未验证' })[status] || '未验证';
      let allCandidates = [];
      let loadError = '';
      let inboxState = { query: '', scrollY: 0 };
      try { inboxState = { ...inboxState, ...JSON.parse(sessionStorage.getItem(stateKey) || '{}') }; } catch (_) { /* corrupt navigation state is disposable */ }
      const search = $('#candidate-search');
      search.value = inboxState.query;

      function persistInboxState() {
        sessionStorage.setItem(stateKey, JSON.stringify({ query: search.value, scrollY: window.scrollY }));
      }
      function flash(message) { $('#ui-status').textContent = message; }
      function filteredCandidates() {
        const query = search.value.trim().toLocaleLowerCase();
        if (!query) return allCandidates;
        return allCandidates.filter(item => [item.domain, item.name_suggestion, item.description_suggestion].some(value => String(value || '').toLocaleLowerCase().includes(query)));
      }
      function newnessCopy(item) {
        const fact = item.newness || {};
        if (fact.status === 'passed') {
          const age = Number.isFinite(fact.rdap_age_days) ? `，RDAP 注册 ${fact.rdap_age_days} 天` : '';
          return `新网站：CT 首见${age}`;
        }
        return `新网站：${statusText(fact.status)}`;
      }
      function reachabilityCopy(item) {
        const fact = item.reachability || {};
        if (fact.status === 'passed') return `可访问：HTTP ${fact.http_status_code ?? '已验证'}，根域名一致`;
        if (fact.status === 'failed') return '可访问性：最终根域名不一致';
        return '可访问性：未验证';
      }
      function candidateCard(item, priority) {
        const score = item.priority && typeof item.priority.score === 'number' ? item.priority.score.toFixed(2) : null;
        const evidence = item.evidence && item.evidence[0];
        const label = item.primary_outcome === 'publishable_ai_saas' ? 'AI SaaS 候选' : item.primary_outcome || '待判断';
        return `<a class="candidate-card ${priority ? 'candidate-card--priority' : ''}" data-candidate-id="${esc(item.candidate_id)}" href="/review/${encodeURIComponent(item.candidate_id)}" onclick="sessionStorage.setItem('domainhunter.inbox', JSON.stringify({query: document.getElementById('candidate-search').value, scrollY: window.scrollY}))">
          <div class="card-top"><span class="domain">${esc(item.domain)}</span>${score ? `<span class="priority">优先级 ${score}</span>` : ''}</div>
          ${item.name_suggestion ? `<div class="product-name">${esc(item.name_suggestion)}</div>` : ''}
          <p class="description">${esc(item.description_suggestion || label)}</p>
          <div class="facts"><span class="fact ${esc(item.newness?.status || 'unknown')}">${esc(newnessCopy(item))}</span><span class="fact ${esc(item.reachability?.status || 'unknown')}">${esc(reachabilityCopy(item))}</span></div>
          ${evidence ? `<p class="evidence-quote">产品证据：${esc(evidence.quote)}</p>` : ''}
          <div class="card-foot">查看证据 <span aria-hidden="true">→</span></div>
        </a>`;
      }
      function renderEmpty(message, actionLabel) {
        $('#priority-section').hidden = true; $('#candidate-section').hidden = true;
        $('#inbox-empty').hidden = false;
        $('#inbox-empty').innerHTML = `<div class="state-card"><h2>${esc(message)}</h2><p>${actionLabel ? '可启动一次严格扫描，从真实来源寻找新的候选。' : '请更换关键词，或清除搜索后再试。'}</p>${actionLabel ? '<button class="primary" id="empty-scan" type="button">扫描新网站</button>' : '<button class="link-button" id="clear-search" type="button">清除搜索</button>'}</div>`;
        $('#empty-scan')?.addEventListener('click', openScan);
        $('#clear-search')?.addEventListener('click', () => { search.value = ''; renderInbox(); search.focus(); });
      }
      function renderInbox() {
        const candidates = filteredCandidates();
        $('#inbox-summary').textContent = allCandidates.length ? `发现了 ${allCandidates.length} 个待审核候选` : '还没有可审核候选';
        $('#inbox-error').innerHTML = loadError ? `<div class="error-banner">候选读取失败：${esc(loadError)}<button id="retry-load" type="button">重试</button></div>` : '';
        $('#retry-load')?.addEventListener('click', loadInbox);
        if (!candidates.length) { renderEmpty(allCandidates.length ? `没有与“${search.value.trim()}”匹配的候选` : '还没有可审核候选', allCandidates.length === 0); persistInboxState(); return; }
        $('#inbox-empty').hidden = true;
        $('#priority-section').hidden = false;
        $('#priority-candidate').innerHTML = candidateCard(candidates[0], true);
        $('#candidate-section').hidden = candidates.length < 2;
        $('#candidate-list').innerHTML = candidates.slice(1).map(item => candidateCard(item, false)).join('');
        persistInboxState();
      }
      async function loadInbox() {
        if (!allCandidates.length) $('#priority-candidate').innerHTML = '<div class="candidate-card skeleton" aria-label="正在读取候选"></div>';
        try {
          const response = await fetch('/v1/review-queue', { cache: 'no-store' });
          const body = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
          allCandidates = Array.isArray(body.items) ? body.items : [];
          loadError = '';
          renderInbox();
          if (inboxState.scrollY) requestAnimationFrame(() => window.scrollTo(0, inboxState.scrollY));
        } catch (error) {
          loadError = error.message || '请求未完成';
          if (allCandidates.length) renderInbox();
          else { $('#inbox-summary').textContent = '候选暂时无法读取'; renderEmpty('候选暂时无法读取', false); $('#inbox-error').innerHTML = `<div class="error-banner">候选读取失败：${esc(loadError)}<button id="retry-load" type="button">重试</button></div>`; $('#retry-load')?.addEventListener('click', loadInbox); }
          flash(`候选读取失败：${loadError}`);
        }
      }
      function openScan() { const dialog = $('#scan-dialog'); if (typeof dialog.showModal === 'function') dialog.showModal(); else dialog.setAttribute('open', ''); }
      function closeScan() { const dialog = $('#scan-dialog'); if (typeof dialog.close === 'function') dialog.close(); else dialog.removeAttribute('open'); }
      function setScanResult(message, error) { const result = $('#scan-result'); result.hidden = false; result.classList.toggle('error', Boolean(error)); result.textContent = message; }
      async function runScan() {
        const start = $('#start-scan'); const close = $('#close-scan');
        start.disabled = true; close.disabled = true; setScanResult('扫描正在运行，请保持此页面打开。', false);
        try {
          const response = await fetch('/v1/run/discovery', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ max_probes: 20 }) });
          const body = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
          if (body.status === 'completed') {
            setScanResult(`本次扫描已完成。\n采样信号：${body.certificates_seen ?? 0}；观察到根域名：${body.roots_observed ?? 0}；严格规则排除：${body.strict_rejections ?? 0}；实际探测：${body.probes_run ?? 0}；进入收件箱：${body.candidates_created ?? 0}。`, false);
            close.textContent = '查看候选'; close.disabled = false; await loadInbox();
          } else if (body.status === 'no_candidates') {
            setScanResult(`本次扫描没有产生可审核候选。\n采样信号：${body.certificates_seen ?? 0}；观察到根域名：${body.roots_observed ?? 0}；严格规则排除：${body.strict_rejections ?? 0}；实际探测：${body.probes_run ?? 0}。`, false);
            close.disabled = false;
          } else { throw new Error('服务返回了未知扫描状态'); }
        } catch (error) { setScanResult(`扫描失败：${error.message || '请求未完成'}。请重试。`, true); close.disabled = false; }
        finally { start.disabled = false; }
      }
      search.addEventListener('input', renderInbox);
      window.addEventListener('pagehide', persistInboxState);
      $('#open-scan').addEventListener('click', openScan);
      $('#close-scan').addEventListener('click', closeScan);
      $('#start-scan').addEventListener('click', runScan);
      loadInbox();
    })();
  </script>
</body></html>"""
