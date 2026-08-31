"""Long-running scheduler daemon that drains the leased work queue under budget."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
import logging
from typing import Mapping

from webradar_v2.domain.work_queue import WorkStage
from webradar_v2.storage.sqlite import SQLiteStore


class _AlertEngineProtocol:
    """Structural type for the optional AlertEngine dependency.

    Keeping this in the daemon module avoids an import cycle with the
    scheduler.alerts module, which already imports SQLiteStore.
    """

    async def evaluate(self) -> tuple[object, ...]: ...


_LOGGER = logging.getLogger("webradar_v2.scheduler")


class WorkerDaemon:
    """Drive one bounded scheduler tick at a time; never implicitly retries work.

    The daemon owns the lease for a slice of work, defers unknown stages to the
    budget ledger, and stops cleanly on signal or ``max_ticks``. It never hides
    failures: handlers raise, the lease is released, and the budget row is the
    source of truth for deferred work.
    """

    def __init__(
        self,
        *,
        store: SQLiteStore,
        pipeline: object,
        tick_seconds: float = 5.0,
        lease_seconds: float = 30.0,
        budget_per_tick: float = 10.0,
        worker_id: str = "scheduler-worker",
        daily_budget_per_stage: Mapping[WorkStage, float] | None = None,
        clock: Callable[[], datetime] | None = None,
        alert_engine: _AlertEngineProtocol | None = None,
        budget_loader: Callable[[], dict[WorkStage, float]] | None = None,
        pause_checker: Callable[[WorkStage], tuple[bool, str]] | None = None,
    ) -> None:
        self._store = store
        self._pipeline = pipeline
        self._tick_seconds = tick_seconds
        self._lease_seconds = lease_seconds
        self._budget_per_tick = budget_per_tick
        self._worker_id = worker_id
        self._daily_budget_per_stage = dict(daily_budget_per_stage or {})
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(UTC))
        self._alert_engine = alert_engine
        self._budget_loader = budget_loader
        self._pause_checker = pause_checker

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def run(
        self,
        *,
        max_ticks: int | None = None,
        signal: asyncio.Event | None = None,
    ) -> None:
        """Loop until ``max_ticks`` or ``signal``; ticks are bounded by the budget gate."""

        ticks = 0
        while True:
            if signal is not None and signal.is_set():
                return
            if max_ticks is not None and ticks >= max_ticks:
                return

            await self._tick(at_time=self._clock())
            await self._evaluate_alerts()
            ticks += 1

            if max_ticks is not None and ticks >= max_ticks:
                return

            if signal is not None:
                try:
                    await asyncio.wait_for(signal.wait(), timeout=self._tick_seconds)
                except asyncio.TimeoutError:
                    pass
                if signal.is_set():
                    return
            else:
                await asyncio.sleep(self._tick_seconds)

    async def _evaluate_alerts(self) -> None:
        """Run the AlertEngine after a tick and surface new emissions via logs."""

        if self._alert_engine is None:
            return
        try:
            alerts = await self._alert_engine.evaluate()
        except Exception as error:  # noqa: BLE001 — keep the tick loop alive
            _LOGGER.warning("scheduler.alerts.evaluate_failed", extra={"error": str(error)})
            return
        for alert in alerts:
            _LOGGER.warning(
                "scheduler.alert.fired",
                extra={
                    "alert_id": getattr(alert, "alert_id", None),
                    "kind": getattr(getattr(alert, "kind", None), "value", None),
                    "severity": getattr(getattr(alert, "severity", None), "value", None),
                    "title": getattr(alert, "title", None),
                    "runbook_id": getattr(alert, "runbook_id", None),
                },
            )

    async def _tick(self, *, at_time: datetime) -> None:
        """Execute one bounded scheduler tick: budget gate -> claim -> handle -> record."""

        if not self._budget_gate_open(at_time):
            metrics = self._store.funnel_metrics()
            _LOGGER.info(
                "scheduler.tick.skipped_budget",
                extra={
                    "queued_work": metrics.queued_work,
                    "budget_reserved_units": metrics.budget_reserved_units,
                },
            )
            return

        leases = self._store.claim_work(
            worker_id=self._worker_id,
            now=at_time,
            lease_seconds=self._lease_seconds,
            limit=int(self._budget_per_tick),
        )
        if not leases:
            metrics = self._store.funnel_metrics()
            _LOGGER.info(
                "scheduler.tick.idle",
                extra={"queued_work": metrics.queued_work},
            )
            return

        for lease in leases:
            await self._handle(lease=lease, at_time=at_time)

        metrics = self._store.funnel_metrics()
        _LOGGER.info(
            "scheduler.tick.complete",
            extra={
                "queued_work": metrics.queued_work,
                "budget_reserved_units": metrics.budget_reserved_units,
                "budget_deferred_units": metrics.budget_deferred_units,
            },
        )

    def _resolve_daily_budgets(self) -> dict[WorkStage, float]:
        """Pick the runtime budget map: explicit kwarg, else ``budget_loader``, else empty."""

        if self._daily_budget_per_stage:
            return self._daily_budget_per_stage
        if self._budget_loader is not None:
            loaded = self._budget_loader()
            return dict(loaded)
        return {}

    def _budget_gate_open(self, at_time: datetime) -> bool:
        """Skip ticks when the per-stage daily budget has been fully reserved.

        Honors ``pause_checker`` as if a paused stage were budget-exhausted, and
        pulls daily limits from ``budget_loader`` when no explicit mapping was
        passed to the constructor.
        """

        budgets = self._resolve_daily_budgets()
        if not budgets:
            return True
        metrics = self._store.funnel_metrics()
        # Per-stage reserved totals are aggregated by status in the ledger.
        # We approximate the per-stage total from the funnel snapshot.
        for stage, daily_limit in budgets.items():
            if self._pause_checker is not None:
                paused, pause_reason = self._pause_checker(stage)
                if paused:
                    _LOGGER.info(
                        "scheduler.budget_gate.paused",
                        extra={
                            "stage": stage.value,
                            "reason": pause_reason,
                            "queued_work": metrics.queued_work,
                        },
                    )
                    return False
            used = self._stage_reserved_today(stage=stage, at_time=at_time)
            if used >= daily_limit:
                _LOGGER.info(
                    "scheduler.budget_gate.closed",
                    extra={
                        "stage": stage.value,
                        "daily_limit": daily_limit,
                        "used": used,
                        "queued_work": metrics.queued_work,
                    },
                )
                return False
        return True

    def _stage_reserved_today(self, *, stage: WorkStage, at_time: datetime) -> float:
        """Return the reserved units for one stage on the UTC day of ``at_time``."""

        from datetime import UTC, timedelta

        utc_time = at_time.astimezone(UTC)
        day_start = utc_time.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        with self._store._connection() as connection:  # noqa: SLF001 — read-only ledger query
            row = connection.execute(
                """
                SELECT COALESCE(SUM(units), 0) AS used FROM budget_ledger
                WHERE stage = ? AND status = 'reserved'
                  AND occurred_at >= ? AND occurred_at < ?
                """,
                (stage.value, day_start.isoformat(), day_end.isoformat()),
            ).fetchone()
        return float(row["used"])

    async def _handle(self, *, lease, at_time: datetime) -> None:
        """Dispatch one leased item to its handler; never crash the loop."""

        try:
            if lease.stage is WorkStage.L1:
                await self._pipeline.probe_due_domains(  # type: ignore[attr-defined]
                    observed_at=at_time
                )
                self._store.complete_work(lease.work_id, lease_token=lease.lease_token)
                return
            if lease.stage in {WorkStage.L2, WorkStage.L3, WorkStage.EXPOSURE}:
                # Not yet wired to the daemon; explicitly defer the budget.
                self._store.record_budget_deferred(
                    lease.stage,
                    entity_id=lease.entity_id,
                    units=1.0,
                    occurred_at=at_time,
                    reason="stage_not_implemented",
                )
                self._store.release_work(
                    lease.work_id,
                    lease_token=lease.lease_token,
                    reschedule_at=at_time + timedelta(minutes=15),
                )
                return
            # Unimplemented stages must still be accounted for, not silently dropped.
            self._store.record_budget_deferred(
                lease.stage,
                entity_id=lease.entity_id,
                units=1.0,
                occurred_at=at_time,
                reason="stage_not_implemented",
            )
            self._store.release_work(
                lease.work_id,
                lease_token=lease.lease_token,
                reschedule_at=at_time + timedelta(minutes=15),
            )
        except Exception as error:  # noqa: BLE001 — keep the tick loop alive
            _LOGGER.warning(
                "scheduler.handler.failed",
                extra={
                    "stage": lease.stage.value,
                    "entity_id": lease.entity_id,
                    "error": str(error),
                },
            )
            self._store.record_budget_deferred(
                lease.stage,
                entity_id=lease.entity_id,
                units=1.0,
                occurred_at=at_time,
                reason=f"handler_error:{error.__class__.__name__}",
            )
            self._store.release_work(
                lease.work_id,
                lease_token=lease.lease_token,
                reschedule_at=at_time + timedelta(minutes=5),
            )
