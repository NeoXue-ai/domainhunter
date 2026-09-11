"""Continuous CT discovery runner for ``domainhunter start``.

One command, started once, runs at capacity until Ctrl-C:

1. **Sweep**: pull every page from the stored cursor to the tree head
   with bounded parallelism (servers truncate pages, so the stride is
   discovered from real responses and shortfalls are repaired in place).
   Idempotent events, first-seen baseline and one work item per new root.
2. **Digest**: run funnel rounds (RDAP age, DNS, HTTP probe, optional
   LLM) until the work queue drains, then pause ``--idle-seconds`` and
   sweep again.  A fresh database starts at the tree head; ``--entries``
   or ``--hours`` can seed a one-time historical window on first run.
3. **Resume**: the cursor is checkpointed every ~50 pages, so a killed
   run continues where it stopped instead of rescanning.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from domainhunter.ingest.ct_log_adapter import (
    CTLogFetcher,
    CTLogTarget,
    leaf_cert_envelope,
    parse_leaf_input,
)
from domainhunter.ingest.ct_poller import CTCertificate, CTPoller
from domainhunter.storage.sqlite import SQLiteStore

_LOGGER = logging.getLogger("domainhunter.start")


@dataclass(frozen=True, slots=True)
class StartConfig:
    """Knobs for one backfill run. All bounded on purpose."""

    hours: float | None = None
    entries: int | None = None
    follow: bool = True
    idle_seconds: float = 15.0
    page_delay_seconds: float = 0.25
    claim_limit: int = 400
    round_delay_seconds: float = 10.0
    rdap_concurrency: int = 6
    dns_concurrency: int = 30
    probe_concurrency: int = 16
    page_fetch_concurrency: int = 8
    max_rounds: int | None = None

    def __post_init__(self) -> None:
        if self.hours is not None and self.hours <= 0:
            raise ValueError("hours must be positive")
        if self.entries is not None and self.entries <= 0:
            raise ValueError("entries must be positive")
        if self.hours is not None and self.entries is not None:
            raise ValueError("choose at most one of hours/entries")
        if self.page_delay_seconds < 0:
            raise ValueError("page_delay_seconds must be non-negative")
        if self.claim_limit < 1:
            raise ValueError("claim_limit must be positive")
        if self.round_delay_seconds < 0:
            raise ValueError("round_delay_seconds must be non-negative")
        if self.page_fetch_concurrency < 1:
            raise ValueError("page_fetch_concurrency must be positive")
        if self.follow and self.idle_seconds < 0:
            raise ValueError("idle_seconds must be non-negative")
        if min(
            self.rdap_concurrency, self.dns_concurrency, self.probe_concurrency
        ) < 1:
            raise ValueError("concurrency knobs must be positive")


@dataclass(frozen=True, slots=True)
class StartSummary:
    """Counts observable at the end of one backfill run."""

    ingested_entries: int
    queued_domains: int
    probes_run: int
    candidates_created: int
    rounds: int
    pending_work: int
    start_indices: dict[str, int]

    def as_payload(self) -> dict[str, object]:
        return {
            "ingested_entries": self.ingested_entries,
            "queued_domains": self.queued_domains,
            "probes_run": self.probes_run,
            "candidates_created": self.candidates_created,
            "rounds": self.rounds,
            "pending_work": self.pending_work,
            "start_indices": dict(self.start_indices),
        }


async def locate_start_index(
    fetcher: CTLogFetcher,
    *,
    log_id: str,
    since: datetime,
    tree_size: int,
) -> int:
    """Binary-search the smallest entry index whose ``notBefore`` >= ``since``.

    Unparseable entries are treated as "older than since" (the search walks
    right past them), which biases the window slightly larger — safe for a
    filter that drops over-age domains anyway.
    """
    if since.tzinfo is None:
        raise ValueError("since must be timezone-aware")
    lo, hi = 0, max(tree_size - 1, 0)
    answer = tree_size
    while lo <= hi:
        mid = (lo + hi) // 2
        stamp = await fetcher.leaf_timestamp(log_id, mid)
        if stamp is None:
            # Walk right past unparseable entries; give up after 10.
            parsed_stamp: datetime | None = None
            for probe_index in range(mid + 1, min(mid + 11, tree_size)):
                parsed_stamp = await fetcher.leaf_timestamp(log_id, probe_index)
                if parsed_stamp is not None:
                    break
            if parsed_stamp is None:
                lo = mid + 10
            elif parsed_stamp < since:
                lo = probe_index + 1
            else:
                hi = mid - 1
                answer = probe_index
            continue
        if stamp < since:
            lo = mid + 1
        else:
            hi = mid - 1
            answer = mid
    return max(0, min(answer, tree_size))


async def run_start(
    *,
    store: SQLiteStore,
    fetcher: CTLogFetcher,
    poller: CTPoller,
    digest_once,
    config: StartConfig,
    progress=None,
) -> StartSummary:
    """Run one backfill: rewind, ingest the window, then digest the queue.

    Ingest fetches pages with bounded concurrency (servers cap page sizes,
    so the stride is discovered from the first real response). If the store
    cursor already sits inside the located window, ingest resumes from the
    cursor instead of rewinding — repeated runs are incremental and safe.
    ``digest_once()`` must run one orchestrator round.
    """
    report = progress or (lambda event: _LOGGER.info("%s", event))
    if config.follow:
        return await _run_follow(
            store=store,
            fetcher=fetcher,
            poller=poller,
            digest_once=digest_once,
            config=config,
            progress=report,
        )
    since = (
        datetime.now(UTC) - timedelta(hours=config.hours)
        if config.hours is not None
        else None
    )

    stored_map = _parse_stored_cursor(store.get_source_cursor(poller.source_name))
    tree_sizes = await fetcher.tree_sizes()
    start_indices: dict[str, int] = {}
    resume_indices: dict[str, int] = {}
    for target in fetcher.logs:
        if config.entries is not None:
            located = max(0, tree_sizes[target.log_id] - config.entries)
        else:
            located = await locate_start_index(
                fetcher,
                log_id=target.log_id,
                since=since,  # type: ignore[arg-type]
                tree_size=tree_sizes[target.log_id],
            )
            if located >= tree_sizes[target.log_id]:
                report(
                    {
                        "event": "start.window_empty",
                        "log": target.log_id,
                        "hint": (
                            "the log tail lags its public STH by tens of minutes; "
                            "use --entries N instead of --hours for count-based windows"
                        ),
                    }
                )
        stored = stored_map.get(target.log_id)
        # Resume when the stored cursor already sits inside this window.
        resume = located
        if stored is not None and located <= stored <= tree_sizes[target.log_id]:
            resume = stored
        start_indices[target.log_id] = located
        resume_indices[target.log_id] = resume
        report(
            {
                "event": "start.located_start",
                "log": target.log_id,
                "tree_size": tree_sizes[target.log_id],
                "start_index": located,
                "resume_index": resume,
                "window_entries": tree_sizes[target.log_id] - located,
            }
        )

    ingested, queued_before = await _ingest_window(
        store=store,
        fetcher=fetcher,
        poller=poller,
        resume_indices=resume_indices,
        tree_sizes=tree_sizes,
        config=config,
        progress=report,
    )
    report({"event": "start.ingest_done", "ingested": ingested, "pending_work": queued_before})
    rounds = 0
    probes_run = 0
    candidates_created = 0
    while True:
        summary = await digest_once()
        rounds += 1
        probes_run += summary.probes_run
        candidates_created += summary.candidates_created
        pending = store.pending_ct_discovery_work_count()
        report(
            {
                "event": "start.digest_round",
                "round": rounds,
                "probes": summary.probes_run,
                "candidates": summary.candidates_created,
                "pending_work": pending,
            }
        )
        if pending == 0:
            break
        if config.max_rounds is not None and rounds >= config.max_rounds:
            report({"event": "start.round_budget_exhausted", "rounds": rounds})
            break
        await asyncio.sleep(config.round_delay_seconds)

    return StartSummary(
        ingested_entries=ingested,
        queued_domains=queued_before,
        probes_run=probes_run,
        candidates_created=candidates_created,
        rounds=rounds,
        pending_work=store.pending_ct_discovery_work_count(),
        start_indices=start_indices,
    )


async def _run_follow(
    *,
    store: SQLiteStore,
    fetcher: CTLogFetcher,
    poller: CTPoller,
    digest_once,
    config: StartConfig,
    progress,
) -> StartSummary:
    """Continuous max-speed mode: sweep cursor→head, drain the queue, repeat.

    This is ``discover`` with backfill's parallel ingest: started once, it
    keeps catching up new entries and digesting candidates until Ctrl-C.
    An empty store starts at the current tree head (future-only); a stored
    cursor resumes wherever the last run stopped.
    """
    cycle = 0
    total_ingested = 0
    total_probes = 0
    total_candidates = 0
    total_rounds = 0
    first_cycle = True
    since = (
        datetime.now(UTC) - timedelta(hours=config.hours)
        if config.hours is not None
        else None
    )
    while True:
        cycle += 1
        stored = _parse_stored_cursor(store.get_source_cursor(poller.source_name))
        tree_sizes = await fetcher.tree_sizes()
        resume_indices: dict[str, int] = {}
        for target in fetcher.logs:
            log_id = target.log_id
            cursor = stored.get(log_id)
            if cursor is not None:
                resume_indices[log_id] = cursor
            elif first_cycle and config.entries is not None:
                resume_indices[log_id] = max(0, tree_sizes[log_id] - config.entries)
            elif first_cycle and since is not None:
                located = await locate_start_index(
                    fetcher, log_id=log_id, since=since, tree_size=tree_sizes[log_id]
                )
                resume_indices[log_id] = min(located, tree_sizes[log_id])
                if located >= tree_sizes[log_id]:
                    progress({
                        "event": "start.window_empty",
                        "log": log_id,
                        "hint": "log tail lags its public STH; use --entries N for a count-based first window",
                    })
            else:
                resume_indices[log_id] = tree_sizes[log_id]
        first_cycle = False
        ingested, queued = await _ingest_window(
            store=store,
            fetcher=fetcher,
            poller=poller,
            resume_indices=resume_indices,
            tree_sizes=tree_sizes,
            config=config,
            progress=progress,
        )
        total_ingested += ingested

        rounds = 0
        while True:
            summary = await digest_once()
            rounds += 1
            total_rounds += 1
            total_probes += summary.probes_run
            total_candidates += summary.candidates_created
            pending = store.pending_ct_discovery_work_count()
            if config.max_rounds is not None and rounds >= config.max_rounds:
                break
            if pending == 0:
                break
            await asyncio.sleep(config.round_delay_seconds)
        progress(
            {
                "event": "start.cycle",
                "cycle": cycle,
                "cycle_ingested": ingested,
                "cycle_digest_rounds": rounds,
                "cycle_candidates": summary.candidates_created,
                "queued_domains": queued,
                "pending_work": store.pending_ct_discovery_work_count(),
                "total_ingested": total_ingested,
                "total_candidates": total_candidates,
            }
        )
        await asyncio.sleep(config.idle_seconds)


def _parse_stored_cursor(cursor: str | None) -> dict[str, int]:
    if not cursor:
        return {}
    try:
        raw = json.loads(cursor)
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}
    return {k: v for k, v in raw.items() if isinstance(v, int)}


async def _ingest_window(
    *,
    store: SQLiteStore,
    fetcher: CTLogFetcher,
    poller: CTPoller,
    resume_indices: dict[str, int],
    tree_sizes: dict[str, int],
    config: StartConfig,
    progress,
) -> tuple[int, int]:
    """Ingest [resume_index, tree_size) for every log with bounded parallelism."""
    total_ingested = 0
    for target in fetcher.logs:
        log_id = target.log_id
        start = resume_indices[log_id]
        tree_size = tree_sizes[log_id]
        if start >= tree_size:
            continue
        total_ingested += await _ingest_log_window(
            store=store,
            fetcher=fetcher,
            poller=poller,
            log_id=log_id,
            start=start,
            tree_size=tree_size,
            config=config,
            progress=progress,
        )
        store.set_source_cursor(poller.source_name, json.dumps({log_id: tree_size}))
    queued = store.pending_ct_discovery_work_count()
    return total_ingested, queued


async def _ingest_log_window(
    *,
    store: SQLiteStore,
    fetcher: CTLogFetcher,
    poller: CTPoller,
    log_id: str,
    start: int,
    tree_size: int,
    config: StartConfig,
    progress,
) -> int:
    """One log's window: parallel fixed-stride page sweep, truncation-aware.

    The first probe page reveals the server's effective batch size; spans
    are swept with ``page_fetch_concurrency`` workers. Responses shorter
    than their span enqueue a repair range, so no entry is ever skipped.
    The store cursor is advanced to the highest contiguous covered index
    every ~50 pages, so an interrupted run resumes cheaply.
    """
    probe_raw, _ = await fetcher.fetch_entries(log_id, start, min(start + 499, tree_size - 1))
    state = {"stride": max(len(probe_raw), 1)}
    probe_covered = start + len(probe_raw)

    spans: list[tuple[int, int]] = []
    first_span_end = min(start + 499, tree_size - 1)
    if probe_covered <= first_span_end:
        spans.append((probe_covered, first_span_end))
    cursor_pos = first_span_end + 1
    while cursor_pos < tree_size:
        spans.append((cursor_pos, min(cursor_pos + state["stride"] - 1, tree_size - 1)))
        cursor_pos += state["stride"]

    queue: asyncio.Queue[tuple[int, tuple[tuple[str, tuple[str, ...], str | None], int, int]] | None] = asyncio.Queue()
    pages_done = 1
    covered: dict[int, int] = {start: probe_covered}
    contiguous = start
    while contiguous in covered:
        contiguous = covered.pop(contiguous)
    certs: list[tuple[str, tuple[str, ...], str | None]] = []
    for offset, entry in enumerate(probe_raw):
        hostnames, issuer = parse_leaf_input(entry.get("leaf_input", ""))
        if not hostnames:
            continue
        certs.append((f"{log_id}:{start + offset}", hostnames, issuer))
    probe_entries = tuple(
        CTCertificate(
            source_event_id=cert_id,
            certificate=leaf_cert_envelope(hosts, issuer),
            observed_at=datetime.now(UTC),
        )
        for cert_id, hosts, issuer in certs
    )
    probe_seen, _ = await poller.ingest_entries(probe_entries)
    total_ingested = probe_seen
    pages_done = 1

    async def worker() -> None:
        while spans:
            span_start, span_end = spans.pop(0)
            try:
                raw, _ = await fetcher.fetch_entries(log_id, span_start, span_end)
            except Exception as error:  # noqa: BLE001 - a failed page is retried at the tail
                _LOGGER.warning("backfill page fetch failed [%s:%s]: %s", span_start, span_end, error)
                spans.insert(0, (span_start, span_end))
                await asyncio.sleep(config.page_delay_seconds)
                continue
            certs: list[tuple[str, tuple[str, ...], str | None]] = []
            for offset, entry in enumerate(raw):
                hostnames, issuer = parse_leaf_input(entry.get("leaf_input", ""))
                if not hostnames:
                    continue
                certs.append((f"{log_id}:{span_start + offset}", hostnames, issuer))
            await queue.put((span_start, (tuple(certs), span_start + len(raw), span_end)))
            if len(raw) < (span_end - span_start + 1):
                # Server shorted this span — repair immediately (front of the
                # queue) so the contiguous cursor pointer keeps advancing,
                # and shrink the stride to the observed response size.
                state["stride"] = min(state["stride"], max(len(raw), 1))
                spans.insert(0, (span_start + len(raw), span_end))

    async def consumer() -> None:
        nonlocal total_ingested, pages_done, contiguous
        while True:
            item = await queue.get()
            if item is None:
                return
            span_start, (certs, returned_end, _span_end) = item
            entries = tuple(
                CTCertificate(
                    source_event_id=cert_id,
                    certificate=leaf_cert_envelope(hosts, issuer),
                    observed_at=datetime.now(UTC),
                )
                for cert_id, hosts, issuer in certs
            )
            seen, _added = await poller.ingest_entries(entries)
            total_ingested += seen
            pages_done += 1
            covered[span_start] = returned_end
            while contiguous in covered:
                contiguous = covered.pop(contiguous)
            if pages_done % 50 == 0:
                store.set_source_cursor(poller.source_name, json.dumps({log_id: contiguous}))
                progress(
                    {
                        "event": "start.ingest_progress",
                        "log": log_id,
                        "pages": pages_done,
                        "entries": total_ingested,
                        "covered_until": contiguous,
                        "window_entries": tree_size - start,
                    }
                )
            await asyncio.sleep(0)

    workers = [
        asyncio.create_task(worker()) for _ in range(min(config.page_fetch_concurrency, max(len(spans), 1)))
    ]
    consumer_task = asyncio.create_task(consumer())
    await asyncio.gather(*workers)
    await queue.put(None)
    await consumer_task
    store.set_source_cursor(poller.source_name, json.dumps({log_id: tree_size}))
    return total_ingested
