"""Manual backfill: replay a past time window of CT log entries.

Realtime discovery only sees what arrives while the daemon runs.  This
module replays an arbitrary past window (``--hours N``) through the
exact same funnel:

1. **Locate** the start index per log via binary search on the leaf
   certificate's ``notBefore`` (CT logs carry no time index; issuance
   order tracks submission order closely enough for day granularity).
2. **Ingest**: pull every page from the start index to the tree head
   back-to-back through the ordinary ``CTPoller`` — idempotent events,
   first-seen baseline, and one ``ct_discovery_work`` item per new root.
   At ~500 entries/page a 24h window (~350k entries) takes minutes.
3. **Digest**: run orchestrator rounds until the work queue drains.
   Bounded concurrency knobs keep the fan-out polite: RDAP servers
   rate-limit aggressively, and HTTP probes are capped per host.
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

_LOGGER = logging.getLogger("domainhunter.backfill")


@dataclass(frozen=True, slots=True)
class BackfillConfig:
    """Knobs for one backfill run. All bounded on purpose."""

    hours: float
    page_delay_seconds: float = 0.25
    claim_limit: int = 400
    round_delay_seconds: float = 10.0
    rdap_concurrency: int = 6
    dns_concurrency: int = 30
    probe_concurrency: int = 16
    page_fetch_concurrency: int = 8
    max_rounds: int | None = None

    def __post_init__(self) -> None:
        if self.hours <= 0:
            raise ValueError("hours must be positive")
        if self.page_delay_seconds < 0:
            raise ValueError("page_delay_seconds must be non-negative")
        if self.claim_limit < 1:
            raise ValueError("claim_limit must be positive")
        if self.round_delay_seconds < 0:
            raise ValueError("round_delay_seconds must be non-negative")
        if self.page_fetch_concurrency < 1:
            raise ValueError("page_fetch_concurrency must be positive")
        if min(
            self.rdap_concurrency, self.dns_concurrency, self.probe_concurrency
        ) < 1:
            raise ValueError("concurrency knobs must be positive")


@dataclass(frozen=True, slots=True)
class BackfillSummary:
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


async def run_backfill(
    *,
    store: SQLiteStore,
    fetcher: CTLogFetcher,
    poller: CTPoller,
    digest_once,
    config: BackfillConfig,
    progress=None,
) -> BackfillSummary:
    """Run one backfill: rewind, ingest the window, then digest the queue.

    Ingest fetches pages with bounded concurrency (servers cap page sizes,
    so the stride is discovered from the first real response). If the store
    cursor already sits inside the located window, ingest resumes from the
    cursor instead of rewinding — repeated runs are incremental and safe.
    ``digest_once()`` must run one orchestrator round.
    """
    report = progress or (lambda event: _LOGGER.info("%s", event))
    since = datetime.now(UTC) - timedelta(hours=config.hours)

    stored_map = _parse_stored_cursor(store.get_source_cursor(poller.source_name))
    tree_sizes = await fetcher.tree_sizes()
    start_indices: dict[str, int] = {}
    resume_indices: dict[str, int] = {}
    for target in fetcher.logs:
        located = await locate_start_index(
            fetcher, log_id=target.log_id, since=since, tree_size=tree_sizes[target.log_id]
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
                "event": "backfill.located_start",
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
    report({"event": "backfill.ingest_done", "ingested": ingested, "pending_work": queued_before})
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
                "event": "backfill.digest_round",
                "round": rounds,
                "probes": summary.probes_run,
                "candidates": summary.candidates_created,
                "pending_work": pending,
            }
        )
        if pending == 0:
            break
        if config.max_rounds is not None and rounds >= config.max_rounds:
            report({"event": "backfill.round_budget_exhausted", "rounds": rounds})
            break
        await asyncio.sleep(config.round_delay_seconds)

    return BackfillSummary(
        ingested_entries=ingested,
        queued_domains=queued_before,
        probes_run=probes_run,
        candidates_created=candidates_created,
        rounds=rounds,
        pending_work=store.pending_ct_discovery_work_count(),
        start_indices=start_indices,
    )


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
    config: BackfillConfig,
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
    config: BackfillConfig,
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
    stride = max(len(probe_raw), 1)

    spans: list[tuple[int, int]] = []
    probe_covered = start + len(probe_raw)
    if probe_covered < start + 500 and tree_size - 1 >= probe_covered:
        spans.append((probe_covered, min(start + 499, tree_size - 1)))
    first_span_end = min(start + 499, tree_size - 1)
    cursor_pos = first_span_end + 1
    while cursor_pos < tree_size:
        spans.append((cursor_pos, min(cursor_pos + stride - 1, tree_size - 1)))
        cursor_pos += stride

    queue: asyncio.Queue[tuple[int, tuple[tuple[str, tuple[str, ...], str | None], int, int]] | None] = asyncio.Queue()
    total_ingested = 0
    pages_done = 0
    covered: dict[int, int] = {}
    contiguous = start

    async def worker() -> None:
        while spans:
            span_start, span_end = spans.pop(0)
            try:
                raw, _ = await fetcher.fetch_entries(log_id, span_start, span_end)
            except Exception as error:  # noqa: BLE001 - a failed page is retried at the tail
                _LOGGER.warning("backfill page fetch failed [%s:%s]: %s", span_start, span_end, error)
                spans.append((span_start, span_end))
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
                # Server shorted this span — queue a repair range.
                spans.append((span_start + len(raw), span_end))

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
                        "event": "backfill.ingest_progress",
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


def build_backfill_config(args: Any) -> BackfillConfig:
    """Build a BackfillConfig from parsed CLI arguments."""
    return BackfillConfig(
        hours=args.hours,
        page_delay_seconds=args.page_delay,
        claim_limit=args.claim_limit,
        round_delay_seconds=args.round_delay,
        rdap_concurrency=args.rdap_concurrency,
        dns_concurrency=args.dns_concurrency,
        probe_concurrency=args.probe_concurrency,
        max_rounds=args.max_rounds,
    )
