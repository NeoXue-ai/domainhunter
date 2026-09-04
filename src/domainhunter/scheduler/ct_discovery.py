"""Dependency-free loop for strict, direct CT discovery.

The source adapter is intentionally injected.  The CLI wires it to the
project's RFC 6962 poller and strict ingestion orchestrator, while tests can
provide a deterministic ``run_once`` coroutine without touching a CT log.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from domainhunter.ingest.ct_orchestrator import CTIngestRunSummary

_LOGGER = logging.getLogger("domainhunter.ct_discovery")

RunOnce = Callable[[], Awaitable[CTIngestRunSummary]]


class CTDiscoveryDaemon:
    """Run one strict CT ingestion pass repeatedly without vendor dependencies."""

    def __init__(
        self,
        *,
        run_once: RunOnce,
        round_seconds: float = 120.0,
    ) -> None:
        if round_seconds < 0:
            raise ValueError("round_seconds must be non-negative")
        self._run_once = run_once
        self._round_seconds = round_seconds
        self._round = 0
        self._successful_rounds = 0
        self._failed_rounds = 0
        self._last_error: str | None = None
        self._last_source_errors: tuple[str, ...] = ()

    @property
    def round(self) -> int:
        """Number of attempted rounds, including failed ones."""
        return self._round

    @property
    def successful_rounds(self) -> int:
        return self._successful_rounds

    @property
    def failed_rounds(self) -> int:
        return self._failed_rounds

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def last_source_errors(self) -> tuple[str, ...]:
        """Latest non-fatal source failures from a partial successful round."""
        return self._last_source_errors

    async def run(
        self,
        *,
        max_rounds: int | None = None,
        signal: asyncio.Event | None = None,
    ) -> None:
        """Run until stopped, while keeping one failed upstream poll retryable.

        The exception remains visible in the structured log record and is
        retained for bounded CLI output.  This avoids presenting an upstream
        outage as a successful zero-candidate scan.
        """
        if max_rounds is not None and max_rounds < 1:
            raise ValueError("max_rounds must be positive")

        while True:
            if signal is not None and signal.is_set():
                return
            if max_rounds is not None and self._round >= max_rounds:
                return

            self._round += 1
            try:
                summary = await self._run_once()
            except Exception as error:  # noqa: BLE001 - a transient log outage is retryable
                self._failed_rounds += 1
                self._last_error = str(error) or type(error).__name__
                _LOGGER.exception(
                    "discovery.round.failed [round=%d, error=%s]",
                    self._round,
                    self._last_error,
                )
            else:
                self._successful_rounds += 1
                self._last_source_errors = summary.source_errors
                _LOGGER.info(
                    "discovery.round.completed "
                    "[round=%d, certificates=%d, events=%d, roots=%d, "
                    "strict_rejections=%d, probes=%d, candidates=%d, llm_enriched=%d, "
                    "source_failures=%d]",
                    self._round,
                    summary.certificates_seen,
                    summary.events_added,
                    summary.roots_observed,
                    summary.strict_rejections,
                    summary.probes_run,
                    summary.candidates_created,
                    summary.llm_enriched,
                    len(summary.source_errors),
                )

            if max_rounds is not None and self._round >= max_rounds:
                return
            if signal is None:
                await asyncio.sleep(self._round_seconds)
                continue
            try:
                await asyncio.wait_for(signal.wait(), timeout=self._round_seconds)
            except TimeoutError:
                pass
