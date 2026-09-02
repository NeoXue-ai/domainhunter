"""Contract tests for the long-running scheduler daemon (spec §14 operations)."""

import asyncio
from datetime import UTC, datetime

from domainhunter.domain.work_queue import WorkStage
from domainhunter.scheduler.daemon import WorkerDaemon
from domainhunter.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)


class _StubPipeline:
    """Minimal pipeline stand-in so the daemon never touches the network."""

    def __init__(self) -> None:
        self.probed_at: list[datetime] = []

    async def probe_due_domains(self, *, observed_at, limit=None):
        self.probed_at.append(observed_at)
        return ()


def test_daemon_runs_one_tick_and_invokes_probe_handler(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    pipeline = _StubPipeline()
    assert store.enqueue_work(WorkStage.L1, "trigger-probe", scheduled_at=NOW) is True
    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=5.0,
        budget_per_tick=2.0,
        worker_id="test-worker",
        daily_budget_per_stage={WorkStage.L1: 100.0},
    )

    asyncio.run(daemon.run(max_ticks=1))

    metrics = store.funnel_metrics()
    assert metrics.source_events == 0
    assert len(pipeline.probed_at) == 1
    # The daemon completes the L1 work item after the handler runs.
    leases = store.claim_work(
        worker_id="post", now=NOW, lease_seconds=5, limit=5
    )
    assert all(lease.entity_id != "trigger-probe" for lease in leases)


def test_daemon_respects_budget_gate(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    pipeline = _StubPipeline()
    # Saturate the daily L1 budget before the daemon starts.
    store.reserve_budget(
        WorkStage.L1, units=10.0, daily_limit=10.0, occurred_at=NOW, entity_id="seed"
    )
    assert store.enqueue_work(WorkStage.L1, "trigger-probe", scheduled_at=NOW) is True
    # Pin the daemon's clock to NOW so the budget ledger's day window matches the seed.
    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=5.0,
        budget_per_tick=2.0,
        worker_id="test-worker",
        daily_budget_per_stage={WorkStage.L1: 10.0},
        clock=lambda: NOW,
    )

    asyncio.run(daemon.run(max_ticks=1))

    metrics = store.funnel_metrics()
    assert metrics.budget_reserved_units == 10.0
    assert pipeline.probed_at == []


def test_daemon_claims_and_releases_leases(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    pipeline = _StubPipeline()
    assert store.enqueue_work(WorkStage.L1, "domain-a", scheduled_at=NOW) is True

    observed_atoms: list[datetime] = []

    class _InspectingPipeline(_StubPipeline):
        async def probe_due_domains(self, *, observed_at, limit=None):
            self.probed_at.append(observed_at)
            observed_atoms.append(observed_at)
            # Inspect the leased work by querying the queue directly.
            with store._connection() as connection:  # noqa: SLF001
                rows = connection.execute(
                    "SELECT id, lease_token, completed_at FROM work_queue"
                ).fetchall()
            for row in rows:
                if row["completed_at"] is None:
                    raise AssertionError("daemon did not complete the L1 work item")
            return ()

    inspecting = _InspectingPipeline()
    daemon = WorkerDaemon(
        store=store,
        pipeline=inspecting,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=5.0,
        budget_per_tick=4.0,
        worker_id="test-worker",
        daily_budget_per_stage={WorkStage.L1: 100.0},
    )

    asyncio.run(daemon.run(max_ticks=1))

    assert len(inspecting.probed_at) == 1
    assert len(observed_atoms) == 1


def test_daemon_stops_on_max_ticks(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    pipeline = _StubPipeline()
    # Three triggers for three ticks.
    for i in range(3):
        assert store.enqueue_work(WorkStage.L1, f"t-{i}", scheduled_at=NOW) is True
    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.001,
        lease_seconds=1.0,
        budget_per_tick=10.0,
        worker_id="test-worker",
        daily_budget_per_stage={WorkStage.L1: 100.0},
    )

    asyncio.run(daemon.run(max_ticks=3))

    assert len(pipeline.probed_at) == 3


def test_daemon_handles_signal_shutdown(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    pipeline = _StubPipeline()
    assert store.enqueue_work(WorkStage.L1, "trigger-probe", scheduled_at=NOW) is True
    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=1.0,
        budget_per_tick=1.0,
        worker_id="test-worker",
        daily_budget_per_stage={WorkStage.L1: 100.0},
    )

    shutdown = asyncio.Event()

    async def scenario() -> None:
        async def trigger() -> None:
            await asyncio.sleep(0.05)
            shutdown.set()

        trigger_task = asyncio.create_task(trigger())
        await daemon.run(signal=shutdown)
        await trigger_task

    asyncio.run(scenario())

    assert len(pipeline.probed_at) >= 1


def test_daemon_emits_budget_deferred_when_stage_not_implemented(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    pipeline = _StubPipeline()
    assert store.enqueue_work(WorkStage.L2, "domain-b", scheduled_at=NOW) is True
    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=5.0,
        budget_per_tick=4.0,
        worker_id="test-worker",
        daily_budget_per_stage={WorkStage.L2: 100.0},
    )

    asyncio.run(daemon.run(max_ticks=1))

    metrics = store.funnel_metrics()
    assert metrics.budget_deferred_units >= 1.0
    # The L2 item was released for retry; verify the row is still uncompleted.
    with store._connection() as connection:  # noqa: SLF001
        row = connection.execute(
            "SELECT completed_at FROM work_queue WHERE entity_id = 'domain-b'"
        ).fetchone()
    assert row is not None
    assert row["completed_at"] is None


def test_daemon_handles_unsupported_kind_without_crashing(tmp_path) -> None:
    """An unknown stage must not crash the loop; it is budget-deferred."""

    store = SQLiteStore(tmp_path / "domainhunter.db")
    pipeline = _StubPipeline()
    # L3 is also unimplemented in the daemon and should be deferred gracefully.
    assert store.enqueue_work(WorkStage.L3, "domain-c", scheduled_at=NOW) is True
    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=5.0,
        budget_per_tick=4.0,
        worker_id="test-worker",
        daily_budget_per_stage={WorkStage.L3: 100.0},
    )

    asyncio.run(daemon.run(max_ticks=1))
    metrics = store.funnel_metrics()
    assert metrics is not None
    assert metrics.budget_deferred_units >= 1.0


def test_daemon_signature_uses_typed_store_and_pipeline() -> None:
    """Static guard so accidental signature drift fails the test suite loudly."""

    import inspect

    signature = inspect.signature(WorkerDaemon.__init__)
    assert "store" in signature.parameters
    assert "pipeline" in signature.parameters
    assert "tick_seconds" in signature.parameters
    assert "lease_seconds" in signature.parameters
    assert "budget_per_tick" in signature.parameters
