"""Command-line entrypoint for local DomainHunter operation and acceptance."""

import argparse
import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import uvicorn

from domainhunter.api import create_app
from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.ingest.ct_log_adapter import DEFAULT_LOG, CTLogFetcher, CTLogTarget
from domainhunter.ingest.ct_orchestrator import CTIngestOrchestrator
from domainhunter.ingest.ct_poller import CTPoller
from domainhunter.llm.provider import MockLLMProvider, OpenAICompatibleProvider
from domainhunter.pipeline import DomainHunterPipeline
from domainhunter.storage.sqlite import SQLiteStore

_LOGGER = logging.getLogger("domainhunter.cli")


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timestamp must be ISO-8601") from error
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed


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


def _discover(args: argparse.Namespace) -> int:
    """Run direct strict CT polling and optional LLM enrichment on a loop."""

    import logging

    from domainhunter.scheduler.ct_discovery import CTDiscoveryDaemon

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _LOGGER.info(
        "discover starting: database=%s logs=%s round_seconds=%s provider=%s",
        args.database,
        [t.log_id for t in (args.logs or (DEFAULT_LOG,))],
        args.round_seconds,
        args.provider,
    )
    _LOGGER.info(
        "each round polls new CT entries, filters, probes, and queues "
        "candidates; Ctrl-C stops"
    )

    store = SQLiteStore(args.database)
    logs = tuple(args.logs) if args.logs else (DEFAULT_LOG,)
    provider = None
    if args.provider == "mock":
        provider = MockLLMProvider()
    elif args.provider == "openai-compatible":
        token = args.token or (
            os.environ.get(args.token_env) if args.token_env else None
        )
        if not token:
            raise ValueError(
                "an OpenAI-compatible --token or --token-env is required"
            )
        provider = OpenAICompatibleProvider(
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

    try:
        daemon = asyncio.run(run())
    except KeyboardInterrupt:
        print("\ndiscover: stopped by Ctrl-C", flush=True)
        return 0
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

    for name, handler, help_text in (("status", _status, "show known and due domains"),):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--database", required=True, type=Path)
        command.add_argument("--at", type=_parse_datetime)
        command.set_defaults(handler=handler)

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
        choices=("none", "mock", "openai-compatible"),
        default="none",
        help="S5 LLM classification: none = rules-only (default), "
        "mock = deterministic test provider, openai-compatible = any "
        "OpenAI-compatible endpoint (requires --token or --token-env)",
    )
    discover.add_argument("--base-url")
    discover.add_argument("--model")
    discover.add_argument("--token")
    discover.add_argument("--token-env")
    discover.add_argument("--tier1-days", type=int, default=30)
    discover.add_argument("--tier2-days", type=int, default=90)
    discover.set_defaults(handler=_discover)

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
