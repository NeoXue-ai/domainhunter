"""Small local FastAPI surface for the human review workflow."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from domainhunter.domain.reviews import ReasonTag, ReviewAction, build_review_decision
from domainhunter.domain.verification import CandidateVerification
from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.ingest.ct_log_adapter import CTLogFetcher, DEFAULT_LOG
from domainhunter.ingest.ct_orchestrator import CTIngestOrchestrator
from domainhunter.ingest.ct_poller import CTPoller
from domainhunter.pipeline import DomainHunterPipeline
from domainhunter.storage.sqlite import ConcurrentDecisionError, SQLiteStore


_FRONTEND_DIR = Path(__file__).with_name("static")


class ReviewDecisionRequest(BaseModel):
    request_id: str = Field(min_length=1)
    action: ReviewAction
    reason_tags: tuple[ReasonTag, ...] = ()


class RevokeRequest(BaseModel):
    request_id: str = Field(min_length=1)
    reason: str = ""


class DiscoveryRunRequest(BaseModel):
    max_probes: int = Field(default=20, ge=1, le=200)


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
    now: Callable[[], datetime] | None = None,
) -> FastAPI:
    """Create a process-local review API backed by the supplied SQLite database."""
    store = SQLiteStore(database_path)
    app = FastAPI(title="DomainHunter Review API", version="0.1.0")
    app.mount("/assets", StaticFiles(directory=_FRONTEND_DIR / "assets"), name="assets")
    clock = now or (lambda: datetime.now(UTC))
    discover_log_path = Path(database_path).with_suffix(".log")

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

    @app.get("/v1/discovery/log")
    def discovery_log(tail: int = 200) -> dict[str, object]:
        """Return the tail of the discover daemon's log file (sibling of the DB)."""
        tail = max(1, min(tail, 1000))
        if not discover_log_path.exists():
            return {"exists": False, "lines": []}
        with discover_log_path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - 262_144))
            chunk = handle.read().decode("utf-8", errors="replace")
        lines = chunk.splitlines()[-tail:]
        return {"exists": True, "lines": lines}

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


    return app
