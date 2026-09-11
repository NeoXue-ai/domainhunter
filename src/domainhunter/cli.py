"""Command-line entrypoint for local DomainHunter operation and acceptance."""

import argparse
import asyncio
import json
import logging
import os
from datetime import UTC, datetime, timedelta
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


def _start(args: argparse.Namespace) -> int:
    """Continuous discovery: sweep new CT entries at max speed until Ctrl-C."""

    import logging

    from domainhunter.filter.pipeline import FilterPipeline
    from domainhunter.ingest.runner import StartConfig, run_start

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log_path = Path(args.database).with_suffix(".log")
    logging.getLogger().addHandler(logging.FileHandler(log_path, encoding="utf-8"))
    _LOGGER.info(
        "start: database=%s hours=%s entries=%s claim_limit=%s rdap=%s probe=%s",
        args.database,
        args.hours,
        args.entries,
        args.claim_limit,
        args.rdap_concurrency,
        args.probe_concurrency,
    )

    provider = None
    if args.provider == "mock":
        provider = MockLLMProvider()
    elif args.provider == "openai-compatible":
        token = args.token or (
            os.environ.get(args.token_env) if args.token_env else None
        )
        if not token:
            raise ValueError("an OpenAI-compatible --token or --token-env is required")
        provider = OpenAICompatibleProvider(
            base_url=args.base_url, token=token, model=args.model
        )

    async def run() -> dict[str, object]:
        config = StartConfig(
            hours=args.hours,
            entries=args.entries,
            follow=True,
            idle_seconds=args.idle_seconds,
            page_delay_seconds=args.page_delay,
            claim_limit=args.claim_limit,
            round_delay_seconds=args.round_delay,
            rdap_concurrency=args.rdap_concurrency,
            dns_concurrency=args.dns_concurrency,
            probe_concurrency=args.probe_concurrency,
            page_fetch_concurrency=args.page_fetch_concurrency,
            max_rounds=args.max_rounds,
        )
        store = SQLiteStore(args.database)
        logs = tuple(args.logs) if args.logs else (DEFAULT_LOG,)
        async with CTLogFetcher(
            logs=logs,
            catchup_entries=0,
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
                    rdap_concurrency=config.rdap_concurrency,
                    dns_concurrency=config.dns_concurrency,
                )
                orchestrator = CTIngestOrchestrator(
                    store=store,
                    poller=poller,
                    pipeline=pipeline,
                    filter_pipeline=strict_filter,
                    require_first_seen=True,
                    probe_limit=config.claim_limit,
                    provider=provider,
                    probe_concurrency=config.probe_concurrency,
                    filter_retry_delay=timedelta(
                        seconds=(
                            args.filter_retry_delay
                            if args.filter_retry_delay is not None
                            else 20.0
                        )
                    ),
                )

                def progress(event: dict[str, object]) -> None:
                    _LOGGER.info("start %s", event)

                return await run_start(
                    store=store,
                    fetcher=fetcher,
                    poller=poller,
                    digest_once=orchestrator.run_once,
                    config=config,
                    progress=progress,
                )

    try:
        summary = asyncio.run(run())
    except KeyboardInterrupt:
        print("\nstart: stopped by Ctrl-C (progress is saved; rerun to continue)", flush=True)
        return 0
    _print_json(summary.as_payload())
    return 0


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

    start_cmd = subparsers.add_parser(
        "start",
        help="run continuous max-speed CT discovery until Ctrl-C",
    )
    start_cmd.add_argument("--database", required=True, type=Path)
    start_cmd.add_argument(
        "--hours", type=float, default=None,
        help="optional: on the FIRST run, start from the past N hours",
    )
    start_cmd.add_argument(
        "--entries", type=int, default=None,
        help="optional: on the FIRST run, start from the last N log entries "
        "(immune to STH cache lag; recommended for a fresh database)",
    )
    start_cmd.add_argument(
        "--idle-seconds", type=float, default=15.0,
        help="in --follow mode, pause between sweep cycles (default 15)",
    )
    start_cmd.add_argument(
        "--log", action="append", dest="logs", type=_parse_log_spec, default=None,
        help="log spec of the form log_id=base_url; repeat for multiple logs",
    )
    start_cmd.add_argument("--page-size", type=int, default=500)
    start_cmd.add_argument("--page-delay", type=float, default=0.25)
    start_cmd.add_argument("--claim-limit", type=int, default=400)
    start_cmd.add_argument("--round-delay", type=float, default=10.0)
    start_cmd.add_argument("--rdap-concurrency", type=int, default=6)
    start_cmd.add_argument("--dns-concurrency", type=int, default=30)
    start_cmd.add_argument("--probe-concurrency", type=int, default=16)
    start_cmd.add_argument("--page-fetch-concurrency", type=int, default=8)
    start_cmd.add_argument(
        "--filter-retry-delay", type=float, default=None,
        help="seconds before failed DNS/RDAP checks are retried "
        "(default 300, or 20 in --follow mode)",
    )
    start_cmd.add_argument("--max-rounds", type=int)
    start_cmd.add_argument("--tier1-days", type=int, default=30)
    start_cmd.add_argument("--tier2-days", type=int, default=90)
    start_cmd.add_argument(
        "--provider", choices=("none", "mock", "openai-compatible"), default="none"
    )
    start_cmd.add_argument("--base-url")
    start_cmd.add_argument("--model")
    start_cmd.add_argument("--token")
    start_cmd.add_argument("--token-env")
    start_cmd.set_defaults(handler=_start)

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
