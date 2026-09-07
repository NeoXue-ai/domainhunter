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
from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.ingest.ct_log_adapter import DEFAULT_LOG, CTLogFetcher, CTLogTarget
from domainhunter.ingest.ct_orchestrator import CTIngestOrchestrator
from domainhunter.ingest.ct_poller import CTCertificate, CTPage, CTPoller
from domainhunter.llm.provider import MockLLMProvider, OpenAICompatibleProvider
from domainhunter.pipeline import DomainHunterPipeline, enrich_candidate_with_llm
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
            probe_limit=args.max_probes,
            filter_pipeline=strict_filter,
            require_first_seen=True,
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
            "roots_observed": summary.roots_observed,
            "strict_rejections": summary.strict_rejections,
            "probes_run": summary.probes_run,
            "candidates_created": summary.candidates_created,
            "source_errors": list(summary.source_errors),
            "pending_work": summary.pending_work,
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
            run = await domainhunter_pipeline.probe_domain(
                domain,
                observed_at=observed_at,
                require_same_final_root=True,
            )
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
    """Run direct strict CT polling and optional LLM enrichment on a loop."""

    from domainhunter.filter.enrich import build_openai_provider
    from domainhunter.llm.provider import MockLLMProvider
    from domainhunter.scheduler.ct_discovery import CTDiscoveryDaemon
    from domainhunter.storage.sqlite import SQLiteStore

    store = SQLiteStore(args.database)
    logs = tuple(args.logs) if args.logs else (DEFAULT_LOG,)
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

    async def run() -> CTDiscoveryDaemon:
        async with CTLogFetcher(
            logs=logs,
            catchup_entries=args.catchup,
            max_entries_per_page=args.page_size,
        ) as fetcher:
            poller = CTPoller(store=store, fetch_page=fetcher)
            async with HTTPProbe() as probe:
                pipeline = DomainHunterPipeline(store=store, probe=probe)
                strict_filter = FilterPipeline(
                    tier1_days=args.tier1_days,
                    tier2_days=args.tier2_days,
                    require_dns=True,
                    drop_unknown_rdap=True,
                )
                orchestrator = CTIngestOrchestrator(
                    store=store,
                    poller=poller,
                    pipeline=pipeline,
                    filter_pipeline=strict_filter,
                    require_first_seen=True,
                    probe_limit=args.max_domains_per_round,
                    provider=provider,
                )
                daemon = CTDiscoveryDaemon(
                    run_once=orchestrator.run_once,
                    round_seconds=args.round_seconds,
                )
                if hasattr(provider, "aclose"):
                    async with provider:  # type: ignore[union-attr]
                        await daemon.run(max_rounds=args.max_rounds)
                else:
                    await daemon.run(max_rounds=args.max_rounds)
                return daemon

    daemon = asyncio.run(run())
    _print_json(
        {
            "rounds": daemon.round,
            "successful_rounds": daemon.successful_rounds,
            "failed_rounds": daemon.failed_rounds,
            "last_error": daemon.last_error,
            "source_errors": list(daemon.last_source_errors),
            "database": str(args.database),
        }
    )
    # Bounded invocations are commonly used by launchd/CI.  Surface an
    # unavailable source as a real failure rather than a successful empty run.
    return 1 if args.max_rounds is not None and daemon.failed_rounds else 0


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
        help="poll a live RFC 6962 CT log and strictly probe verified newborn domains",
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
    filter_cmd.add_argument("--require-dns", action="store_true", default=True)
    filter_cmd.add_argument("--drop-unknown-rdap", action="store_true", default=True)
    filter_cmd.set_defaults(handler=_filter_domains)

    filter_probe_cmd = subparsers.add_parser(
        "filter-probe",
        help="run the S1→S2→S3 funnel then S4-probe survivors against the database",
    )
    filter_probe_cmd.add_argument("--database", required=True, type=Path)
    filter_probe_cmd.add_argument("--input", required=True, type=Path)
    filter_probe_cmd.add_argument("--tier1-days", type=int, default=30)
    filter_probe_cmd.add_argument("--tier2-days", type=int, default=90)
    filter_probe_cmd.add_argument("--require-dns", action="store_true", default=True)
    filter_probe_cmd.add_argument("--drop-unknown-rdap", action="store_true", default=True)
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
    filter_enrich_cmd.add_argument("--require-dns", action="store_true", default=True)
    filter_enrich_cmd.add_argument("--drop-unknown-rdap", action="store_true", default=True)
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

    discover = subparsers.add_parser(
        "discover",
        help="long-running strict RFC 6962 polling → first-seen → S1-S5 enrich",
    )
    discover.add_argument("--database", required=True, type=Path)
    discover.add_argument("--round-seconds", type=float, default=120.0)
    discover.add_argument("--max-rounds", type=int)
    discover.add_argument("--max-domains-per-round", type=int, default=200)
    discover.add_argument(
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
    discover.add_argument("--catchup", type=int, default=1000)
    discover.add_argument("--page-size", type=int, default=500)
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
