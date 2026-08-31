from datetime import UTC, datetime, timedelta

from webradar_v2.domain.work_queue import WorkStage
from webradar_v2.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 16, tzinfo=UTC)


def test_leases_due_work_once_then_allows_reclaim_after_expiry(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    assert store.enqueue_work(WorkStage.L2, "candidate-1", scheduled_at=NOW) is True
    first = store.claim_work(
        worker_id="worker-a", now=NOW, lease_seconds=60, limit=1
    )
    unavailable = store.claim_work(
        worker_id="worker-b", now=NOW + timedelta(seconds=1), lease_seconds=60, limit=1
    )
    reclaimed = store.claim_work(
        worker_id="worker-b", now=NOW + timedelta(seconds=61), lease_seconds=60, limit=1
    )

    assert len(first) == 1
    assert first[0].lease_owner == "worker-a"
    assert unavailable == ()
    assert len(reclaimed) == 1
    assert reclaimed[0].lease_owner == "worker-b"


def test_defers_expensive_work_when_the_daily_budget_is_exhausted(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    first = store.reserve_budget(
        WorkStage.LLM, units=2, daily_limit=3, occurred_at=NOW, entity_id="candidate-1"
    )
    deferred = store.reserve_budget(
        WorkStage.LLM, units=2, daily_limit=3, occurred_at=NOW, entity_id="candidate-2"
    )

    assert first.allowed is True
    assert deferred.allowed is False
    assert deferred.reason == "budget_deferred"
