"""Long-running discovery daemon: CT collect → first-seen → S1-S5 enrich.

The discovery daemon automates the whole newborn-domain pipeline on a
loop, so no manual ``filter-enrich`` runs are needed in production:

1. Collect domains from an injected CT source for a bounded window.
2. Deduplicate against the ``ct_seen_domains`` table (first-seen).
3. Run the S1→S5 funnel (filter → probe → LLM classify) on fresh domains.
4. Mark everything collected as seen, so the next round only processes
   genuinely new domains.

The daemon is deliberately simple: one bounded collect window per
round, no work-queue/lease machinery (that's the legacy WorkerDaemon's
job). Failures in one round never kill the loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from domainhunter.filter.enrich import run_batch
from domainhunter.llm.provider import LLMProvider
from domainhunter.storage.sqlite import SQLiteStore

_LOGGER = logging.getLogger("domainhunter.discovery")


class DiscoveryDaemon:
    """Run CT collection + first-seen filtering + S1-S5 enrichment on a loop."""

    def __init__(
        self,
        *,
        store: SQLiteStore,
        provider: LLMProvider,
        monitor_factory: Callable[..., object],
        collect_seconds: float = 60.0,
        round_seconds: float = 120.0,
        max_domains_per_round: int = 200,
        tier1_days: int = 30,
        tier2_days: int = 90,
        require_dns: bool = False,
        drop_unknown_rdap: bool = False,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._provider = provider
        self._monitor_factory = monitor_factory
        self._collect_seconds = collect_seconds
        self._round_seconds = round_seconds
        self._max_domains_per_round = max_domains_per_round
        self._tier1_days = tier1_days
        self._tier2_days = tier2_days
        self._require_dns = require_dns
        self._drop_unknown_rdap = drop_unknown_rdap
        self._clock = clock or (lambda: datetime.now(UTC))
        self._round = 0

    @property
    def round(self) -> int:
        return self._round

    async def run(
        self,
        *,
        max_rounds: int | None = None,
        signal: asyncio.Event | None = None,
    ) -> None:
        """Loop until ``max_rounds`` or ``signal`` is set."""
        while True:
            if signal is not None and signal.is_set():
                return
            if max_rounds is not None and self._round >= max_rounds:
                return

            self._round += 1
            at_time = self._clock()
            try:
                await self._round_once(at_time=at_time)
            except Exception as error:  # noqa: BLE001 - never kill the loop
                _LOGGER.exception(
                    "discovery.round.failed [round=%d, error=%s]",
                    self._round,
                    str(error) or type(error).__name__,
                )

            if max_rounds is not None and self._round >= max_rounds:
                return
            if signal is not None:
                try:
                    await asyncio.wait_for(signal.wait(), timeout=self._round_seconds)
                except TimeoutError:
                    pass
                if signal.is_set():
                    return
            else:
                await asyncio.sleep(self._round_seconds)

    async def _round_once(self, *, at_time: datetime) -> dict[str, object]:
        """One bounded round: collect → first-seen → enrich → mark seen."""
        domains = await self._collect_domains()
        _LOGGER.info(
            "discovery.collected",
            extra={"round": self._round, "domains": len(domains)},
        )
        if not domains:
            return {"collected": 0, "fresh": 0, "enriched": 0}

        fresh = self._store.filter_new(tuple(domains))
        _LOGGER.info(
            "discovery.first_seen",
            extra={"round": self._round, "fresh": len(fresh)},
        )
        # Bound the per-round probe/LLM budget: process the first N fresh
        # domains only. Everything collected is marked seen (idempotent,
        # avoids re-collecting the same tail next round); a domain that
        # missed the budget is simply not enriched this round — raise
        # ``max_domains_per_round`` to keep more of them.
        to_enrich = list(fresh)[: self._max_domains_per_round]
        enriched = 0
        if to_enrich:
            outcomes = await run_batch(
                store=self._store,
                domains=to_enrich,
                provider=self._provider,
                tier1_days=self._tier1_days,
                tier2_days=self._tier2_days,
                require_dns=self._require_dns,
                drop_unknown_rdap=self._drop_unknown_rdap,
                observed_at=at_time,
            )
            enriched = sum(1 for o in outcomes if o.stage == "enriched")
            _LOGGER.info(
                "discovery.enriched",
                extra={"round": self._round, "enriched": enriched},
            )

        self._store.mark_seen(tuple(domains), at=at_time, source="discovery")
        return {
            "collected": len(domains),
            "fresh": len(fresh),
            "enriched": enriched,
        }

    async def _collect_domains(self) -> tuple[str, ...]:
        """Watch CT logs for ``collect_seconds`` and return all seen domains.

        Uses the injected ``monitor_factory`` (an object with ``start`` and
        ``stop`` coroutines that calls the supplied callback).
        """
        seen: set[str] = set()

        def callback(entry) -> None:  # type: ignore[no-untyped-def]
            for domain in entry.domains or []:
                domain = domain.strip().lower()
                if domain and not domain.startswith("*."):
                    seen.add(domain)

        monitor = self._monitor_factory(callback=callback)
        await monitor.start()
        try:
            await asyncio.sleep(self._collect_seconds)
        finally:
            await monitor.stop()
        return tuple(sorted(seen))
