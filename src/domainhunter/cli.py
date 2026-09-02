"""Command-line entrypoint for local DomainHunter operation and acceptance."""

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn

from domainhunter.api import create_app
from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.domain.work_queue import WorkStage
from domainhunter.ingest.ct_log_adapter import DEFAULT_LOG, CTLogFetcher, CTLogTarget
from domainhunter.ingest.ct_orchestrator import CTIngestOrchestrator
from domainhunter.ingest.ct_poller import CTCertificate, CTPage, CTPoller
from domainhunter.llm.provider import MockLLMProvider, OpenAICompatibleProvider
from domainhunter.pipeline import DomainHunterPipeline, enrich_candidate_with_llm
from domainhunter.scheduler.daemon import WorkerDaemon
from domainhunter.storage.sqlite import SQLiteStore


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timestamp must be ISO-8601") from error
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed


def _load_json(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError("input JSON must be an object")
    return value


def _print_json(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _init(args: argparse.Namespace) -> int:
    SQLiteStore(args.database)
    _print_json({"database": str(args.database), "initialized": True})
    return 0


async def _run_orchestrator(args: argparse.Namespace, fetcher: object) -> object:
    store = SQLiteStore(args.database)
    poller = CTPoller(store=store, fetch_page=fetcher)  # type: ignore[arg-type]
    async with HTTPProbe() as probe:
        pipeline = DomainHunterPipeline(store=store, probe=probe)
        orchestrator = CTIngestOrchestrator(
            store=store,
            poller=poller,
            pipeline=pipeline,
            probe_limit=args.max_probes,
        )
        return await orchestrator.run_once()


def _poll_ct_log(args: argparse.Namespace) -> int:
    logs = tuple(args.logs) if args.logs else (DEFAULT_LOG,)

    async def run() -> object:
        fetcher = CTLogFetcher(
            logs=logs,
            catchup_entries=args.catchup,
            max_entries_per_page=args.page_size,
        )
        async with fetcher:
            return await _run_orchestrator(args, fetcher)

    summary = asyncio.run(run())
    _print_json(
        {
            "next_cursor": summary.next_cursor,
            "certificates_seen": summary.certificates_seen,
            "events_added": summary.events_added,
            "probes_run": summary.probes_run,
            "candidates_created": summary.candidates_created,
        }
    )
    return 0


def _ingest_ct_page(args: argparse.Namespace) -> int:
    payload = _load_json(args.input)
    entries = tuple(
        CTCertificate(
            source_event_id=item["source_event_id"],
            certificate=item["certificate"],
            observed_at=_parse_datetime(item["observed_at"]),
        )
        for item in payload.get("entries", [])
    )
    page = CTPage(entries=entries, next_cursor=payload.get("next_cursor"))

    async def fetch(_cursor: str | None) -> CTPage:
        return page

    async def run() -> object:
        poller = CTPoller(store=SQLiteStore(args.database), fetch_page=fetch)
        return await poller.poll()

    result = asyncio.run(run())
    _print_json(
        {
            "next_cursor": result.next_cursor,
            "certificates_seen": result.certificates_seen,
            "events_seen": result.events_seen,
            "events_added": result.events_added,
        }
    )
    return 0


def _status(args: argparse.Namespace) -> int:
    store = SQLiteStore(args.database)
    observed_at = args.at or datetime.now(UTC)
    _print_json(
        {
            "database": str(args.database),
            "domains": list(store.list_domains()),
            "due_domains": list(store.due_domains(observed_at)),
        }
    )
    return 0


def _probe_due(args: argparse.Namespace) -> int:
    observed_at = args.at or datetime.now(UTC)

    async def run() -> tuple[dict[str, Any], ...]:
        async with HTTPProbe() as probe:
            pipeline = DomainHunterPipeline(store=SQLiteStore(args.database), probe=probe)
            runs = await pipeline.probe_due_domains(observed_at=observed_at, limit=args.limit)
        return tuple(
            {
                "domain": run.observation.domain,
                "outcome_code": run.observation.outcome_code.value,
                "attempt_number": run.observation.attempt_number,
                "status_code": run.observation.status_code,
                "final_url": run.observation.final_url,
                "next_check_at": (
                    run.retry_decision.next_check_at.isoformat()
                    if run.retry_decision.next_check_at
                    else None
                ),
                "terminal_status": run.retry_decision.terminal_status,
            }
            for run in runs
        )

    _print_json({"observed_at": observed_at.isoformat(), "runs": asyncio.run(run())})
    return 0


def _filter_domains(args: argparse.Namespace) -> int:
    """Run the S1→S2→S3 filter funnel over a JSON list of registrable domains.

    Input JSON: ``{"domains": ["a.com", "b.io", ...]}``.
    Output: layer-by-layer reduction report plus the surviving candidates
    with their full evidence chain (S1 score, S2 age verdict, S3 DNS).
    """
    payload = _load_json(args.input)
    raw_domains = payload.get("domains")
    if not isinstance(raw_domains, list) or not raw_domains:
        raise ValueError("input JSON must contain a non-empty 'domains' list")
    from domainhunter.filter.pipeline import FilterPipeline

    pipeline = FilterPipeline(
        tier1_days=args.tier1_days,
        tier2_days=args.tier2_days,
        require_dns=args.require_dns,
        drop_unknown_rdap=args.drop_unknown_rdap,
    )
    domains = [str(d) for d in raw_domains]
    candidates = pipeline.run(domains)
    report = {
        "input": len(domains),
        "kept": len(candidates),
        "dropped": len(domains) - len(candidates),
        "tier1": sum(1 for c in candidates if c.final_tier == "tier1"),
        "tier2": sum(1 for c in candidates if c.final_tier == "tier2"),
    }
    _print_json(
        {
            "report": report,
            "candidates": [c.as_payload() for c in candidates],
        }
    )
    return 0


def _domainhunter_probe(store: SQLiteStore, observed_at: datetime):
    """Build the S4 probe callback wired to the DomainHunter pipeline.

    Each candidate is first persisted as a ``filter`` source event so
    the append-only invariant holds (a domain must be seen before it
    can be observed), then probed with the full L1 pipeline.
    """
    from domainhunter.crawler.http_probe import HTTPProbe
    from domainhunter.domain.events import SourceEvent
    from domainhunter.pipeline import DomainHunterPipeline

    async def probe(domain: str, *, observed_at: datetime) -> object:
        store.append_source_event(
            SourceEvent(
                source="filter",
                source_event_id=f"filter:{domain}",
                raw_subject=domain,
                observed_at=observed_at,
            ),
            hostname=domain,
        )
        async with HTTPProbe() as http_probe:
            domainhunter_pipeline = DomainHunterPipeline(store=store, probe=http_probe)
            run = await domainhunter_pipeline.probe_domain(domain, observed_at=observed_at)
        return {
            "domain": run.observation.domain,
            "outcome_code": run.observation.outcome_code.value,
            "status_code": run.observation.status_code,
            "final_url": run.observation.final_url,
            "attempt_number": run.observation.attempt_number,
            "terminal_status": run.retry_decision.terminal_status,
        }

    return probe


def _filter_probe(args: argparse.Namespace) -> int:
    payload = _load_json(args.input)
    raw_domains = payload.get("domains")
    if not isinstance(raw_domains, list) or not raw_domains:
        raise ValueError("input JSON must contain a non-empty 'domains' list")
    from domainhunter.filter.pipeline import FilterPipeline
    from domainhunter.storage.sqlite import SQLiteStore

    store = SQLiteStore(args.database)
    domains = [str(d) for d in raw_domains]
    pipeline = FilterPipeline(
        tier1_days=args.tier1_days,
        tier2_days=args.tier2_days,
        require_dns=args.require_dns,
        drop_unknown_rdap=args.drop_unknown_rdap,
    )

    async def run() -> dict[str, object]:
        observed_at = datetime.now(UTC)
        candidates = await pipeline.run_with_probe(
            domains,
            observed_at=observed_at,
            probe=_domainhunter_probe(store, observed_at),
        )
        return {
            "observed_at": observed_at.isoformat(),
            "candidates": [c.as_payload() for c in candidates],
        }

    _print_json(asyncio.run(run()))
    return 0


def _filter_enrich(args: argparse.Namespace) -> int:
    """Run the full S1→S5 funnel: filter, probe, then LLM-classify each survivor.

    Every candidate that survives the age gate is probed (L1) and then
    classified by the configured LLM (S5). Both the rule draft and the
    LLM draft are persisted as candidate versions, feeding the review
    queue. Requires an OpenAI-compatible endpoint (--base-url/--model
    plus --token or --token-env).
    """
    payload = _load_json(args.input)
    raw_domains = payload.get("domains")
    if not isinstance(raw_domains, list) or not raw_domains:
        raise ValueError("input JSON must contain a non-empty 'domains' list")
    from domainhunter.filter.enrich import build_openai_provider, run_batch
    from domainhunter.llm.provider import MockLLMProvider
    from domainhunter.storage.sqlite import SQLiteStore

    store = SQLiteStore(args.database)
    domains = [str(d) for d in raw_domains]
    fresh_info: dict[str, object] | None = None

    if args.fresh_only:
        seen_count = store.seen_domain_count()
        domains = list(store.filter_new(tuple(domains)))
        fresh_info = {"seen_before": seen_count, "fresh": len(domains)}
        if not domains:
            _print_json({"fresh_only": fresh_info, "outcomes": []})
            return 0

    if args.provider == "mock":
        provider = MockLLMProvider()
    else:
        token = args.token or (
            os.environ.get(args.token_env) if args.token_env else None
        )
        if not token:
            raise ValueError(
                "an OpenAI-compatible --token or --token-env is required"
            )
        provider = build_openai_provider(
            base_url=args.base_url, token=token, model=args.model
        )

    async def run() -> dict[str, object]:
        observed_at = datetime.now(UTC)

        async def _batch() -> object:
            return await run_batch(
                store=store,
                domains=domains,
                provider=provider,
                tier1_days=args.tier1_days,
                tier2_days=args.tier2_days,
                require_dns=args.require_dns,
                drop_unknown_rdap=args.drop_unknown_rdap,
                observed_at=observed_at,
            )

        if hasattr(provider, "aclose"):
            async with provider:  # type: ignore[union-attr]
                outcomes = await _batch()
        else:
            outcomes = await _batch()
        if args.fresh_only:
            # Everything we looked at is now "seen", including dropped ones.
            store.mark_seen(tuple(domains), at=observed_at, source="filter")
        payload: dict[str, object] = {
            "observed_at": observed_at.isoformat(),
            "outcomes": [o.as_payload() for o in outcomes],
        }
        if fresh_info is not None:
            payload["fresh_only"] = fresh_info
        return payload

    _print_json(asyncio.run(run()))
    return 0


def _discover(args: argparse.Namespace) -> int:
    """Run the long-running discovery daemon (CT collect → enrich) on a loop."""

    from domainhunter.filter.enrich import build_openai_provider
    from domainhunter.llm.provider import MockLLMProvider
    from domainhunter.scheduler.discovery import DiscoveryDaemon
    from domainhunter.storage.sqlite import SQLiteStore

    store = SQLiteStore(args.database)
    if args.provider == "mock":
        provider = MockLLMProvider()
    else:
        token = args.token or (
            os.environ.get(args.token_env) if args.token_env else None
        )
        if not token:
            raise ValueError(
                "an OpenAI-compatible --token or --token-env is required"
            )
        provider = build_openai_provider(
            base_url=args.base_url, token=token, model=args.model
        )

    def monitor_factory(callback):
        from ct_moniteur import CTMoniteur

        # First yield waits one poll_interval, so keep it well under the
        # collect window to guarantee at least one batch in that window.
        poll_interval = max(2.0, min(args.collect_seconds / 2, 10.0))
        return CTMoniteur(
            callback=callback,
            poll_interval=poll_interval,
            timeout=20,
            max_retries=2,
        )

    daemon = DiscoveryDaemon(
        store=store,
        provider=provider,
        monitor_factory=monitor_factory,
        collect_seconds=args.collect_seconds,
        round_seconds=args.round_seconds,
        max_domains_per_round=args.max_domains_per_round,
        tier1_days=args.tier1_days,
        tier2_days=args.tier2_days,
        require_dns=args.require_dns,
        drop_unknown_rdap=args.drop_unknown_rdap,
    )

    async def run() -> None:
        if hasattr(provider, "aclose"):
            async with provider:  # type: ignore[union-attr]
                await daemon.run(max_rounds=args.max_rounds)
        else:
            await daemon.run(max_rounds=args.max_rounds)

    asyncio.run(run())
    _print_json(
        {
            "rounds": daemon.round,
            "database": str(args.database),
        }
    )
    return 0


def _serve(args: argparse.Namespace) -> int:
    """Run the local review API against one explicit SQLite database."""
    uvicorn.run(create_app(args.database), host=args.host, port=args.port)
    return 0


def _outreach(args: argparse.Namespace) -> int:
    """Dry-run outreach trigger for one approved human candidate version.

    The CLI bypasses the HTTP server and reuses the FastAPI app via its
    TestClient so the local endpoint remains the single source of truth.
    """
    from fastapi.testclient import TestClient

    app = create_app(args.database)
    payload: dict[str, object] = {
        "request_id": args.request_id or f"cli-outreach-{datetime.now(UTC).timestamp()}",
        "dry_run": True,
    }
    if args.recipient_source_url:
        payload["recipient_source_url"] = args.recipient_source_url
    response = TestClient(app).post(
        f"/v1/candidates/{args.candidate_id}/versions/{args.version}/outreach",
        json=payload,
        headers={"X-Actor-ID": args.actor_id},
    )
    _print_json({"status_code": response.status_code, "body": response.json()})
    return 0 if response.status_code < 400 else 1


def _reopen(args: argparse.Namespace) -> int:
    """Reopen one terminal candidate via the local FastAPI app.

    The CLI bypasses the HTTP server and reuses the FastAPI app via its
    TestClient so the local endpoint remains the single source of truth.
    """
    from fastapi.testclient import TestClient

    app = create_app(args.database)
    payload: dict[str, object] = {
        "request_id": args.request_id or f"cli-reopen-{datetime.now(UTC).timestamp()}",
        "trigger_source_event_id": args.trigger_source_event_id,
        "new_outcome": args.new_outcome,
        "reason": args.reason,
    }
    response = TestClient(app).post(
        f"/v1/candidates/{args.candidate_id}/reopen",
        json=payload,
        headers={"X-Actor-ID": args.actor_id},
    )
    _print_json({"status_code": response.status_code, "body": response.json()})
    return 0 if response.status_code < 400 else 1


def _daemon(args: argparse.Namespace) -> int:
    """Run the long-running scheduler daemon against one explicit database."""

    async def run() -> None:
        async with HTTPProbe() as probe:
            pipeline = DomainHunterPipeline(store=SQLiteStore(args.database), probe=probe)
            store = SQLiteStore(args.database)
            from domainhunter.scheduler.alerts import AlertEngine

            alert_engine = AlertEngine(
                store=store,
                daily_budget_per_stage={
                    WorkStage.L1: args.budget_per_stage_l1,
                    WorkStage.L2: args.budget_per_stage_l2,
                    WorkStage.LLM: args.budget_per_stage_llm,
                },
            )
            daemon = WorkerDaemon(
                store=store,
                pipeline=pipeline,
                tick_seconds=args.tick_seconds,
                lease_seconds=args.lease_seconds,
                budget_per_tick=args.budget_per_tick,
                worker_id=args.worker_id,
                daily_budget_per_stage={
                    WorkStage.L1: args.budget_per_stage_l1,
                    WorkStage.L2: args.budget_per_stage_l2,
                    WorkStage.LLM: args.budget_per_stage_llm,
                },
                alert_engine=alert_engine,
                budget_loader=lambda: store.get_budget_config(),
                pause_checker=lambda stage: (
                    store.is_stage_paused(stage),
                    _latest_pause_reason(store, stage),
                ),
            )
            await daemon.run(max_ticks=args.max_ticks)

    asyncio.run(run())
    return 0


def _latest_pause_reason(store: SQLiteStore, stage: WorkStage) -> str:
    entry = store.latest_stage_pause(stage)
    return entry[2] if entry is not None else ""


def _enrich_llm(args: argparse.Namespace) -> int:
    """Run one LLM enrichment pass against the latest rule-authored version."""
    store = SQLiteStore(args.database)
    if args.provider == "mock":
        # Mock provider without a canned draft yields NEEDS_REVIEW, surfacing the gate.
        from domainhunter.domain.candidates import (
            CandidateOutcome,
            CandidateVersionDraft,
            Evidence,
            EvidenceType,
        )

        draft = CandidateVersionDraft(
            author_kind="llm",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion=args.name or "Mock Draft",
            description_suggestion=args.description or "Mock description.",
            category=args.category or "automation",
            tags=tuple(args.tag or ("mock",)),
            pricing_model=args.pricing_model,
            target_audience=args.target_audience,
            evidence=tuple(
                Evidence(EvidenceType.H1, quote, args.url or "https://example.com")
                for quote in (args.evidence_quote or ("Automate your AI workflows",))
            ),
            model_version=args.model_version or "mock-cli",
        )
        provider = MockLLMProvider(draft=draft)
    else:
        token = args.token or (
            os.environ.get(args.token_env) if args.token_env else None
        )
        if not token:
            raise ValueError(
                "an OpenAI-compatible --token or --token-env is required"
            )

        async def _run() -> object:
            async with OpenAICompatibleProvider(
                base_url=args.base_url,
                token=token,
                model=args.model,
            ) as provider:
                return await enrich_candidate_with_llm(
                    store=store,
                    candidate_id=args.candidate_id,
                    candidate_version=args.version,
                    provider=provider,
                    observed_at=datetime.now(UTC),
                )

        result = asyncio.run(_run())
        _print_json(
            {
                "candidate_id": args.candidate_id,
                "version": args.version,
                "persisted": result is not None,
                "model_version": getattr(result, "model_version", None),
            }
        )
        return 0

    async def _mock_run() -> object:
        return await enrich_candidate_with_llm(
            store=store,
            candidate_id=args.candidate_id,
            candidate_version=args.version,
            provider=provider,
            observed_at=datetime.now(UTC),
        )

    result = asyncio.run(_mock_run())
    _print_json(
        {
            "candidate_id": args.candidate_id,
            "version": args.version,
            "persisted": result is not None,
            "model_version": getattr(result, "model_version", None),
        }
    )
    return 0


def _alerts(args: argparse.Namespace) -> int:
    """Print persisted alerts (optionally filtered by ``--since``)."""
    store = SQLiteStore(args.database)
    since = args.since
    rows = store.list_alerts(since=since)
    _print_json(
        {
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
    )
    return 0


def _analytics(args: argparse.Namespace) -> int:
    """Print funnel analytics as one human-readable line per metric."""
    store = SQLiteStore(args.database)
    analytics = store.compute_funnel_analytics()
    payload = analytics.as_payload()
    lines = []
    for key, value in payload.items():
        if key == "backlog_by_stage" or key == "backlog_over_1h_by_stage":
            rendered = (
                " ".join(f"{stage}={count}" for stage, count in sorted(value.items()))
                if value
                else "(empty)"
            )
        elif value is None:
            rendered = "n/a"
        elif isinstance(value, float):
            rendered = f"{value:.4f}"
        else:
            rendered = str(value)
        lines.append(f"{key}: {rendered}")
    print("\n".join(lines))
    return 0


def _runbook(args: argparse.Namespace) -> int:
    """Print the curated runbook text for ``--id`` (exit 1 if not found)."""
    from domainhunter.scheduler.runbooks import get_runbook_by_id

    runbook = get_runbook_by_id(args.id)
    if runbook is None:
        _print_json({"error": "unknown runbook_id", "runbook_id": args.id})
        return 1
    _print_json(
        {
            "runbook_id": runbook.runbook_id,
            "kind": runbook.kind.value,
            "title": runbook.title,
            "steps": list(runbook.steps),
        }
    )
    return 0


def _budget_show(args: argparse.Namespace) -> int:
    """Print the persisted per-stage daily_limit table."""

    store = SQLiteStore(args.database)
    limits = store.get_budget_config()
    rows: list[dict[str, object]] = []
    for stage in WorkStage:
        row = store.get_budget_config_row(stage)
        rows.append(
            {
                "stage": stage.value,
                "daily_limit": float(limits[stage]),
                "updated_at": row[2].isoformat() if row is not None else None,
                "updated_by": row[3] if row is not None else None,
            }
        )
    _print_json({"budgets": rows})
    return 0


def _budget_set(args: argparse.Namespace) -> int:
    """Upsert one stage's daily_limit; exit 2 on validation failure."""

    try:
        stage_enum = WorkStage(args.stage)
    except ValueError:
        _print_json({"error": "unknown stage", "stage": args.stage})
        return 2
    if args.daily_limit is None or args.daily_limit <= 0:
        _print_json({"error": "daily_limit must be positive"})
        return 2
    store = SQLiteStore(args.database)
    occurred_at = datetime.now(UTC)
    inserted = store.set_budget_config(
        stage_enum,
        daily_limit=args.daily_limit,
        updated_by=args.actor_id,
        occurred_at=occurred_at,
    )
    _print_json(
        {
            "stage": stage_enum.value,
            "daily_limit": args.daily_limit,
            "updated_at": occurred_at.isoformat(),
            "updated_by": args.actor_id,
            "changed": inserted,
        }
    )
    return 0


def _pause_show(args: argparse.Namespace) -> int:
    """Print the latest pause state per stage."""

    store = SQLiteStore(args.database)
    rows = store.list_stage_pauses()
    pauses_by_stage = {
        stage: (stage.value, False, "", "", datetime.fromtimestamp(0, tz=UTC))
        for stage in WorkStage
    }
    for entry in rows:
        stage_enum, paused, reason, actor_id, paused_at = entry
        pauses_by_stage[stage_enum] = (
            stage_enum.value,
            paused,
            reason,
            actor_id,
            paused_at,
        )
    ordered = [pauses_by_stage[stage] for stage in WorkStage]
    _print_json(
        {
            "pauses": [
                {
                    "stage": stage_value,
                    "paused": paused,
                    "reason": reason,
                    "actor_id": actor_id,
                    "paused_at": paused_at.isoformat(),
                }
                for stage_value, paused, reason, actor_id, paused_at in ordered
            ]
        }
    )
    return 0


def _pause_set(args: argparse.Namespace) -> int:
    """Append one pause/unpause audit row for ``--stage``."""

    try:
        stage_enum = WorkStage(args.stage)
    except ValueError:
        _print_json({"error": "unknown stage", "stage": args.stage})
        return 2
    paused_value = _parse_bool(args.paused)
    if paused_value is None:
        _print_json({"error": "paused must be 'true' or 'false'"})
        return 2
    store = SQLiteStore(args.database)
    paused_at = datetime.now(UTC)
    inserted = store.set_stage_pause(
        stage_enum,
        paused=paused_value,
        reason=args.reason,
        actor_id=args.actor_id,
        paused_at=paused_at,
    )
    _print_json(
        {
            "stage": stage_enum.value,
            "paused": paused_value,
            "reason": args.reason,
            "actor_id": args.actor_id,
            "paused_at": paused_at.isoformat(),
            "created": inserted,
        }
    )
    return 0


def _parse_bool(value: str) -> bool | None:
    lowered = value.lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def _parse_log_spec(value: str) -> CTLogTarget:
    """Parse a ``--log`` value of the form ``log_id=base_url``."""
    if "=" not in value:
        raise argparse.ArgumentTypeError("--log must be of the form log_id=base_url")
    log_id, base_url = value.split("=", 1)
    return CTLogTarget(log_id=log_id.strip(), base_url=base_url.strip())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="domainhunter", description="Operate the DomainHunter local pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="create or open a SQLite database")
    init.add_argument("--database", required=True, type=Path)
    init.set_defaults(handler=_init)

    for name, handler, help_text in (
        ("ingest-ct-page", _ingest_ct_page, "ingest one CT JSON page"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--database", required=True, type=Path)
        command.add_argument("--input", required=True, type=Path)
        command.set_defaults(handler=handler)

    poll_ct_log = subparsers.add_parser(
        "poll-ct-log",
        help="poll a live RFC 6962 CT log, probe each new hostname, persist candidates",
    )
    poll_ct_log.add_argument("--database", required=True, type=Path)
    poll_ct_log.add_argument(
        "--log",
        action="append",
        dest="logs",
        type=_parse_log_spec,
        default=None,
        help=(
            "log spec of the form log_id=base_url; repeat for multiple logs "
            f"(default: {DEFAULT_LOG.log_id}={DEFAULT_LOG.base_url})"
        ),
    )
    poll_ct_log.add_argument("--max-probes", type=int, default=50)
    poll_ct_log.add_argument("--catchup", type=int, default=1000)
    poll_ct_log.add_argument("--page-size", type=int, default=500)
    poll_ct_log.set_defaults(handler=_poll_ct_log)

    for name, handler, help_text in (("status", _status, "show known and due domains"),):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--database", required=True, type=Path)
        command.add_argument("--at", type=_parse_datetime)
        command.set_defaults(handler=handler)

    probe = subparsers.add_parser("probe-due", help="probe due domains with bounded L1 HTTP")
    probe.add_argument("--database", required=True, type=Path)
    probe.add_argument("--at", type=_parse_datetime)
    probe.add_argument("--limit", type=int)
    probe.set_defaults(handler=_probe_due)

    filter_cmd = subparsers.add_parser(
        "filter",
        help="run the S1→S2→S3 newborn-domain filter funnel over a JSON domain list",
    )
    filter_cmd.add_argument("--input", required=True, type=Path)
    filter_cmd.add_argument("--tier1-days", type=int, default=30)
    filter_cmd.add_argument("--tier2-days", type=int, default=90)
    filter_cmd.add_argument("--require-dns", action="store_true")
    filter_cmd.add_argument("--drop-unknown-rdap", action="store_true")
    filter_cmd.set_defaults(handler=_filter_domains)

    filter_probe_cmd = subparsers.add_parser(
        "filter-probe",
        help="run the S1→S2→S3 funnel then S4-probe survivors against the database",
    )
    filter_probe_cmd.add_argument("--database", required=True, type=Path)
    filter_probe_cmd.add_argument("--input", required=True, type=Path)
    filter_probe_cmd.add_argument("--tier1-days", type=int, default=30)
    filter_probe_cmd.add_argument("--tier2-days", type=int, default=90)
    filter_probe_cmd.add_argument("--require-dns", action="store_true")
    filter_probe_cmd.add_argument("--drop-unknown-rdap", action="store_true")
    filter_probe_cmd.set_defaults(handler=_filter_probe)

    filter_enrich_cmd = subparsers.add_parser(
        "filter-enrich",
        help="run the full S1→S5 funnel: filter, probe, then LLM-classify survivors",
    )
    filter_enrich_cmd.add_argument("--database", required=True, type=Path)
    filter_enrich_cmd.add_argument("--input", required=True, type=Path)
    filter_enrich_cmd.add_argument(
        "--provider",
        choices=("mock", "openai-compatible"),
        default="openai-compatible",
    )
    filter_enrich_cmd.add_argument("--base-url")
    filter_enrich_cmd.add_argument("--model")
    filter_enrich_cmd.add_argument("--token")
    filter_enrich_cmd.add_argument("--token-env")
    filter_enrich_cmd.add_argument("--tier1-days", type=int, default=30)
    filter_enrich_cmd.add_argument("--tier2-days", type=int, default=90)
    filter_enrich_cmd.add_argument("--require-dns", action="store_true")
    filter_enrich_cmd.add_argument("--drop-unknown-rdap", action="store_true")
    filter_enrich_cmd.add_argument(
        "--fresh-only",
        action="store_true",
        help="only enrich domains never seen in a CT log before; mark inputs as seen",
    )
    filter_enrich_cmd.set_defaults(handler=_filter_enrich)

    serve = subparsers.add_parser("serve", help="run the local human-review API")
    serve.add_argument("--database", required=True, type=Path)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", default=8000, type=int)
    serve.set_defaults(handler=_serve)

    daemon = subparsers.add_parser("daemon", help="run the long-running scheduler daemon")
    daemon.add_argument("--database", required=True, type=Path)
    daemon.add_argument("--tick-seconds", type=float, default=5.0)
    daemon.add_argument("--lease-seconds", type=float, default=30.0)
    daemon.add_argument("--budget-per-tick", type=float, default=10.0)
    daemon.add_argument("--max-ticks", type=int)
    daemon.add_argument("--worker-id", default="scheduler-worker")
    daemon.add_argument("--budget-per-stage-l1", type=float, default=100.0)
    daemon.add_argument("--budget-per-stage-l2", type=float, default=10.0)
    daemon.add_argument("--budget-per-stage-llm", type=float, default=5.0)
    daemon.set_defaults(handler=_daemon)

    discover = subparsers.add_parser(
        "discover",
        help="long-running discovery daemon: CT collect → first-seen → S1-S5 enrich",
    )
    discover.add_argument("--database", required=True, type=Path)
    discover.add_argument("--collect-seconds", type=float, default=60.0)
    discover.add_argument("--round-seconds", type=float, default=120.0)
    discover.add_argument("--max-rounds", type=int)
    discover.add_argument("--max-domains-per-round", type=int, default=200)
    discover.add_argument(
        "--provider",
        choices=("mock", "openai-compatible"),
        default="openai-compatible",
    )
    discover.add_argument("--base-url")
    discover.add_argument("--model")
    discover.add_argument("--token")
    discover.add_argument("--token-env")
    discover.add_argument("--tier1-days", type=int, default=30)
    discover.add_argument("--tier2-days", type=int, default=90)
    discover.add_argument("--require-dns", action="store_true")
    discover.add_argument("--drop-unknown-rdap", action="store_true")
    discover.set_defaults(handler=_discover)

    outreach = subparsers.add_parser(
        "outreach", help="dry-run outreach for an approved candidate version"
    )
    outreach.add_argument("--database", required=True, type=Path)
    outreach.add_argument("--candidate-id", required=True)
    outreach.add_argument("--version", required=True, type=int)
    outreach.add_argument("--actor-id", required=True)
    outreach.add_argument("--recipient-source-url")
    outreach.add_argument("--request-id")
    outreach.set_defaults(handler=_outreach)

    reopen = subparsers.add_parser(
        "reopen", help="reopen one terminal candidate and reset its retry counter"
    )
    reopen.add_argument("--database", required=True, type=Path)
    reopen.add_argument("--candidate-id", required=True)
    reopen.add_argument("--trigger-source-event-id", required=True)
    reopen.add_argument("--new-outcome", required=True)
    reopen.add_argument("--reason", required=True)
    reopen.add_argument("--actor-id", required=True)
    reopen.add_argument("--request-id")
    reopen.set_defaults(handler=_reopen)

    enrich_llm = subparsers.add_parser(
        "enrich-llm", help="enrich one candidate version through an LLM provider"
    )
    enrich_llm.add_argument("--database", required=True, type=Path)
    enrich_llm.add_argument("--candidate-id", required=True)
    enrich_llm.add_argument("--version", required=True, type=int)
    enrich_llm.add_argument(
        "--provider",
        choices=("mock", "openai-compatible"),
        default="mock",
    )
    enrich_llm.add_argument("--base-url")
    enrich_llm.add_argument("--model")
    enrich_llm.add_argument("--token")
    enrich_llm.add_argument("--token-env")
    enrich_llm.add_argument("--name")
    enrich_llm.add_argument("--description")
    enrich_llm.add_argument("--category")
    enrich_llm.add_argument("--tag", action="append")
    enrich_llm.add_argument("--pricing-model")
    enrich_llm.add_argument("--target-audience")
    enrich_llm.add_argument("--url")
    enrich_llm.add_argument("--evidence-quote", action="append")
    enrich_llm.add_argument("--model-version")
    enrich_llm.set_defaults(handler=_enrich_llm)

    alerts_cmd = subparsers.add_parser(
        "alerts", help="list persisted operator alerts from the local store"
    )
    alerts_cmd.add_argument("--database", required=True, type=Path)
    alerts_cmd.add_argument("--since", type=_parse_datetime, default=None)
    alerts_cmd.set_defaults(handler=_alerts)

    analytics_cmd = subparsers.add_parser(
        "analytics", help="print funnel conversion / latency / backlog analytics"
    )
    analytics_cmd.add_argument("--database", required=True, type=Path)
    analytics_cmd.set_defaults(handler=_analytics)

    runbook_cmd = subparsers.add_parser(
        "runbook", help="print the curated runbook text for a known anomaly class"
    )
    runbook_cmd.add_argument("--id", required=True)
    runbook_cmd.set_defaults(handler=_runbook)

    budget_cmd = subparsers.add_parser("budget", help="manage per-stage daily budget limits")
    budget_subparsers = budget_cmd.add_subparsers(dest="budget_command", required=True)

    budget_show_cmd = budget_subparsers.add_parser(
        "show", help="print the per-stage daily_limit table"
    )
    budget_show_cmd.add_argument("--database", required=True, type=Path)
    budget_show_cmd.set_defaults(handler=_budget_show)

    budget_set_cmd = budget_subparsers.add_parser(
        "set", help="upsert one stage's daily_limit"
    )
    budget_set_cmd.add_argument("--database", required=True, type=Path)
    budget_set_cmd.add_argument("--stage", required=True)
    budget_set_cmd.add_argument("--daily-limit", type=float, required=True)
    budget_set_cmd.add_argument("--actor-id", required=True)
    budget_set_cmd.set_defaults(handler=_budget_set)

    pause_cmd = subparsers.add_parser("pause", help="manage per-stage pause/resume state")
    pause_subparsers = pause_cmd.add_subparsers(dest="pause_command", required=True)

    pause_show_cmd = pause_subparsers.add_parser(
        "show", help="print the latest pause row per stage"
    )
    pause_show_cmd.add_argument("--database", required=True, type=Path)
    pause_show_cmd.set_defaults(handler=_pause_show)

    pause_set_cmd = pause_subparsers.add_parser(
        "set", help="append one pause/unpause audit row"
    )
    pause_set_cmd.add_argument("--database", required=True, type=Path)
    pause_set_cmd.add_argument("--stage", required=True)
    pause_set_cmd.add_argument("--paused", required=True)
    pause_set_cmd.add_argument("--reason", required=True)
    pause_set_cmd.add_argument("--actor-id", required=True)
    pause_set_cmd.set_defaults(handler=_pause_set)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
