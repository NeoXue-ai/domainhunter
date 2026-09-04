"""Lag monitoring for CT log ingestion.

A CT log grows continuously; a poller that cannot keep up falls
permanently behind the tail (``tree_size`` advances faster than the
consumer index). This module watches every active log's gap and
surfaces the ones that are falling behind, so operators can
rebalance poll intervals or probe the log's health.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LagAlert:
    """One log is further behind the tree tail than the threshold."""

    log_url: str
    log_name: str
    tree_size: int
    consumed_index: int
    lag_entries: int


class _LogClientLike(Protocol):
    log_meta: Any

    async def fetch_tree_size(self) -> int: ...


class LogLagMonitor:
    """Periodically compare each log's tree size with its consumed index.

    The consumed index comes from the direct poller's persisted state (the
    last entry index processed per log URL). The tree size comes from a
    fresh ``fetch_tree_size()`` call per client. ``lag`` is the number
    of entries between the tail and the consumer.

    Args:
        clients: The active log clients.
        state: Callable returning the ``{log_url: last_index}`` dict.
        threshold: Lag (in entries) above which an alert is raised.
        interval: Seconds between checks.
    """

    def __init__(
        self,
        *,
        clients: list[_LogClientLike],
        state: Any,
        threshold: int = 50_000,
        interval: float = 60.0,
        timeout: float = 15.0,
    ) -> None:
        self._clients = clients
        self._state = state
        self._threshold = threshold
        self._interval = interval
        self._timeout = timeout
        self._last_alerts: tuple[LagAlert, ...] = ()
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def check_once(self) -> tuple[LagAlert, ...]:
        """Fetch tree sizes and return alerts for logs that are behind."""
        consumed = dict(self._state())
        alerts: list[LagAlert] = []
        for client in self._clients:
            url = client.log_meta.url
            last_index = consumed.get(url)
            if last_index is None:
                # No state yet (client hasn't consumed anything) — not an alert.
                continue
            try:
                tree_size = await asyncio.wait_for(
                    client.fetch_tree_size(), timeout=self._timeout
                )
            except Exception as exc:  # noqa: BLE001 - network errors are heterogeneous
                logger.warning("lag check failed for %s: %s", url, exc)
                continue
            lag = max(0, tree_size - last_index - 1)
            if lag > self._threshold:
                alerts.append(
                    LagAlert(
                        log_url=url,
                        log_name=client.log_meta.name,
                        tree_size=tree_size,
                        consumed_index=last_index,
                        lag_entries=lag,
                    )
                )
        self._last_alerts = tuple(alerts)
        return self._last_alerts

    async def run_forever(self) -> None:
        """Run periodic checks until cancelled."""
        self._running = True
        try:
            while self._running:
                try:
                    alerts = await self.check_once()
                    if alerts:
                        for alert in alerts:
                            logger.warning(
                                "CT log %s (%s) is %d entries behind the tail "
                                "(tree=%d consumed=%d)",
                                alert.log_name,
                                alert.log_url,
                                alert.lag_entries,
                                alert.tree_size,
                                alert.consumed_index,
                            )
                    else:
                        logger.debug("all CT logs within lag threshold")
                except Exception as exc:  # noqa: BLE001 - keep the loop alive at all costs
                    logger.error("lag monitor check failed: %s", exc)
                await asyncio.sleep(self._interval)
        finally:
            self._running = False

    def start(self) -> None:
        """Start the periodic loop as a background task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run_forever())

    async def stop(self) -> None:
        """Cancel the background loop."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    @property
    def last_alerts(self) -> tuple[LagAlert, ...]:
        return self._last_alerts
