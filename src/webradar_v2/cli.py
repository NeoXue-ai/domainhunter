"""Command-line entrypoint for local WebRadar v2 operation and acceptance."""

import argparse
import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from typing import Any

import uvicorn

from webradar_v2.api import create_app
from webradar_v2.crawler.http_probe import HTTPProbe
from webradar_v2.domain.work_queue import WorkStage
from webradar_v2.ingest.certstream import CertStreamListener, DEFAULT_CERTSTREAM_URL
from webradar_v2.ingest.certstream_latest import (
    CertStreamLatestFetcher,
    DEFAULT_CERTSTREAM_LATEST_URL,
)
from webradar_v2.ingest.crt_sh import CrtShFetcher
from webradar_v2.ingest.ct_orchestrator import CTIngestOrchestrator
from webradar_v2.ingest.github_api import GitHubSearchFetcher
from webradar_v2.ingest.ct_poller import CTCertificate, CTPage, CTPoller
from webradar_v2.ingest.github_poller import GitHubPage, GitHubPoller, GitHubRepository
from webradar_v2.llm.provider import MockLLMProvider, OpenAICompatibleProvider
from webradar_v2.pipeline import WebRadarPipeline, enrich_candidate_with_llm
from webradar_v2.scheduler.daemon import WorkerDaemon
from webradar_v2.storage.sqlite import SQLiteStore


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("timestamp must be ISO-8601") from error
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed


def _load_json(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("input JSON must be an object")
    return value


def _print_json(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _init(args: argparse.Namespace) -> int:
    SQLiteStore(args.database)
    _print_json({"database": str(args.database), "initialized": True})
    return 0


def _ingest_github_page(args: argparse.Namespace) -> int:
    payload = _load_json(args.input)
    repositories = tuple(
        GitHubRepository(
            repository_id=item["repository_id"],
            homepage=item["homepage"],
            observed_at=_parse_datetime(item["observed_at"]),
        )
        for item in payload.get("repositories", [])
    )
    page = GitHubPage(repositories=repositories, next_cursor=payload.get("next_cursor"))

    async def fetch(_cursor: str | None) -> GitHubPage:
        return page

    async def run() -> object:
        poller = GitHubPoller(store=SQLiteStore(args.database), fetch_page=fetch)
        return await poller.poll()

    result = asyncio.run(run())
    _print_json(
        {
            "next_cursor": result.next_cursor,
            "repositories_seen": result.repositories_seen,
            "invalid_homepages": result.invalid_homepages,
            "events_added": result.events_added,
        }
    )
    return 0


def _poll_github_api(args: argparse.Namespace) -> int:
    token = args.token or (os.environ.get(args.token_env) if args.token_env else None)

    async def run() -> object:
        async with GitHubSearchFetcher(query=args.query, token=token) as fetcher:
            poller = GitHubPoller(store=SQLiteStore(args.database), fetch_page=fetcher)
            return await poller.poll()

    result = asyncio.run(run())
    _print_json(
        {
            "next_cursor": result.next_cursor,
            "repositories_seen": result.repositories_seen,
            "invalid_homepages": result.invalid_homepages,
            "events_added": result.events_added,
        }
    )
    return 0


def _poll_crt_sh(args: argparse.Namespace) -> int:
    async def run() -> object:
        transport = None
        if args.dry_run:
            import json as _json
            import httpx as _httpx

            from tests.support.fake_http import FakeHTTPServer

            fixture = _json.loads(args.dry_run_path.read_text(encoding="utf-8"))
            server = FakeHTTPServer()
            server.start()
            try:
                server.add_route(
                    "/",
                    body=_json.dumps(fixture["entries"]).encode("utf-8"),
                    content_type="application/json",
                )

                def handler(request: _httpx.Request) -> _httpx.Response:
                    return server.as_httpx_handler()(request)

                transport = _httpx.MockTransport(handler)
                fetcher = CrtShFetcher(query=args.query, transport=transport)
                return await _run_orchestrator(args, fetcher)
            finally:
                server.stop()
        async with CrtShFetcher(query=args.query) as fetcher:
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


async def _run_orchestrator(args: argparse.Namespace, fetcher: object) -> object:
    store = SQLiteStore(args.database)
    poller = CTPoller(store=store, fetch_page=fetcher)  # type: ignore[arg-type]
    async with HTTPProbe() as probe:
        pipeline = WebRadarPipeline(store=store, probe=probe)
        orchestrator = CTIngestOrchestrator(
            store=store,
            poller=poller,
            pipeline=pipeline,
            probe_limit=args.max_probes,
        )
        return await orchestrator.run_once()


def _listen_certstream(args: argparse.Namespace) -> int:
    async def run() -> object:
        listener = CertStreamListener(store=SQLiteStore(args.database), url=args.url)
        return await listener.listen(max_messages=args.max_messages)

    result = asyncio.run(run())
    _print_json(
        {
            "messages_seen": result.messages_seen,
            "certificate_updates": result.certificate_updates,
            "invalid_messages": result.invalid_messages,
            "events_added": result.events_added,
        }
    )
    return 0


def _poll_certstream_latest(args: argparse.Namespace) -> int:
    async def run() -> object:
        async with CertStreamLatestFetcher(base_url=args.url) as fetcher:
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
            pipeline = WebRadarPipeline(store=SQLiteStore(args.database), probe=probe)
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
            pipeline = WebRadarPipeline(store=SQLiteStore(args.database), probe=probe)
            store = SQLiteStore(args.database)
            from webradar_v2.scheduler.alerts import AlertEngine

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
        from webradar_v2.domain.candidates import (
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
    from webradar_v2.scheduler.runbooks import get_runbook_by_id

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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="webradar", description="Operate the WebRadar v2 local pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="create or open a SQLite database")
    init.add_argument("--database", required=True, type=Path)
    init.set_defaults(handler=_init)

    for name, handler, help_text in (
        ("ingest-ct-page", _ingest_ct_page, "ingest one CT JSON page"),
        ("ingest-github-page", _ingest_github_page, "ingest one GitHub JSON page"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--database", required=True, type=Path)
        command.add_argument("--input", required=True, type=Path)
        command.set_defaults(handler=handler)

    github_api = subparsers.add_parser(
        "poll-github-api", help="poll one live GitHub repository-search page"
    )
    github_api.add_argument("--database", required=True, type=Path)
    github_api.add_argument("--query", required=True)
    credentials = github_api.add_mutually_exclusive_group()
    credentials.add_argument("--token")
    credentials.add_argument("--token-env")
    github_api.set_defaults(handler=_poll_github_api)

    crt_sh = subparsers.add_parser(
        "poll-crt-sh",
        help="poll one crt.sh query, probe each new hostname, persist real candidates",
    )
    crt_sh.add_argument("--database", required=True, type=Path)
    crt_sh.add_argument("--query", default="ai")
    crt_sh.add_argument("--max-probes", type=int, default=50)
    crt_sh.add_argument("--dry-run", action="store_true", help="ingest from a local JSON fixture instead of crt.sh")
    crt_sh.add_argument("--dry-run-path", type=Path, default=Path("tests/fixtures/ct/crt_sh/crt_sh_sample.json"))
    crt_sh.set_defaults(handler=_poll_crt_sh)

    certstream = subparsers.add_parser(
        "listen-certstream", help="listen to a live CertStream certificate-update feed"
    )
    certstream.add_argument("--database", required=True, type=Path)
    certstream.add_argument("--url", default=DEFAULT_CERTSTREAM_URL)
    certstream.add_argument("--max-messages", type=int)
    certstream.set_defaults(handler=_listen_certstream)

    certstream_latest = subparsers.add_parser(
        "poll-certstream-latest",
        help="poll CertStream latest.json snapshot, probe each new hostname, persist candidates",
    )
    certstream_latest.add_argument("--database", required=True, type=Path)
    certstream_latest.add_argument("--url", default=DEFAULT_CERTSTREAM_LATEST_URL)
    certstream_latest.add_argument("--max-probes", type=int, default=50)
    certstream_latest.set_defaults(handler=_poll_certstream_latest)


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
