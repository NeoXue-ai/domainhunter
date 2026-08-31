"""Contract tests for runtime-configurable budget + pause operations (spec §14)."""

import asyncio
from datetime import UTC, datetime, timedelta
import json

import pytest
from fastapi.testclient import TestClient

from webradar_v2 import cli
from webradar_v2.api import create_app
from webradar_v2.domain.work_queue import WorkStage
from webradar_v2.scheduler.daemon import WorkerDaemon
from webradar_v2.storage.sqlite import DEFAULT_DAILY_BUDGET, SQLiteStore


NOW = datetime(2026, 8, 17, 12, 0, 0, tzinfo=UTC)


class _StubPipeline:
    """Minimal pipeline stand-in so the daemon never touches the network."""

    def __init__(self) -> None:
        self.probed_at: list[datetime] = []

    async def probe_due_domains(self, *, observed_at, limit=None):
        self.probed_at.append(observed_at)
        return ()


# ---------------------------------------------------------------------------
# budget_config persistence
# ---------------------------------------------------------------------------


def test_budget_config_default_returns_hardcoded_fallbacks(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    budgets = store.get_budget_config()

    assert budgets == dict(DEFAULT_DAILY_BUDGET)
    # Setting a row must not be implicit; the DB is still empty until upsert.
    with store._connection() as connection:  # noqa: SLF001
        assert connection.execute("SELECT COUNT(*) AS c FROM budget_config").fetchone()["c"] == 0


def test_set_budget_config_creates_row_and_get_returns_it(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    assert store.set_budget_config(
        WorkStage.L1, daily_limit=42.0, updated_by="alice", occurred_at=NOW
    ) is True

    budgets = store.get_budget_config()
    assert budgets[WorkStage.L1] == 42.0
    # Unrelated stages keep their hardcoded defaults.
    assert budgets[WorkStage.L2] == DEFAULT_DAILY_BUDGET[WorkStage.L2]

    row = store.get_budget_config_row(WorkStage.L1)
    assert row is not None
    assert row[1] == 42.0
    assert row[2] == NOW
    assert row[3] == "alice"


def test_set_budget_config_rejects_zero_or_negative(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    with pytest.raises(ValueError):
        store.set_budget_config(
            WorkStage.L1, daily_limit=0.0, updated_by="alice", occurred_at=NOW
        )
    with pytest.raises(ValueError):
        store.set_budget_config(
            WorkStage.L1, daily_limit=-1.0, updated_by="alice", occurred_at=NOW
        )


def test_set_budget_config_idempotent_on_identical_update_within_window(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    assert store.set_budget_config(
        WorkStage.L2, daily_limit=10.0, updated_by="bob", occurred_at=NOW
    ) is True
    # Same value + same timestamp must be a no-op.
    assert store.set_budget_config(
        WorkStage.L2, daily_limit=10.0, updated_by="bob", occurred_at=NOW
    ) is False
    # Different value flips the row.
    assert store.set_budget_config(
        WorkStage.L2, daily_limit=12.0, updated_by="bob", occurred_at=NOW
    ) is True
    assert store.get_budget_config()[WorkStage.L2] == 12.0


# ---------------------------------------------------------------------------
# stage_pauses persistence
# ---------------------------------------------------------------------------


def test_pause_stage_creates_pause_row(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    assert store.set_stage_pause(
        WorkStage.LLM,
        paused=True,
        reason="maintenance",
        actor_id="operator-1",
        paused_at=NOW,
    ) is True

    latest = store.latest_stage_pause(WorkStage.LLM)
    assert latest is not None
    stage, paused, reason, actor_id, paused_at = latest
    assert stage is WorkStage.LLM
    assert paused is True
    assert reason == "maintenance"
    assert actor_id == "operator-1"
    assert paused_at == NOW


def test_is_stage_paused_returns_latest_state(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    assert store.is_stage_paused(WorkStage.LLM) is False

    store.set_stage_pause(
        WorkStage.LLM,
        paused=True,
        reason="incident",
        actor_id="operator",
        paused_at=NOW,
    )
    assert store.is_stage_paused(WorkStage.LLM) is True

    store.set_stage_pause(
        WorkStage.LLM,
        paused=False,
        reason="resolved",
        actor_id="operator",
        paused_at=NOW + timedelta(minutes=5),
    )
    assert store.is_stage_paused(WorkStage.LLM) is False


def test_unpause_stage_clears_state(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    store.set_stage_pause(
        WorkStage.PUBLICATION,
        paused=True,
        reason="freeze",
        actor_id="ops",
        paused_at=NOW,
    )
    assert store.is_stage_paused(WorkStage.PUBLICATION) is True

    store.set_stage_pause(
        WorkStage.PUBLICATION,
        paused=False,
        reason="thawed",
        actor_id="ops",
        paused_at=NOW + timedelta(minutes=2),
    )
    assert store.is_stage_paused(WorkStage.PUBLICATION) is False

    listing = store.list_stage_pauses()
    assert (WorkStage.PUBLICATION, False, "thawed", "ops", NOW + timedelta(minutes=2)) in listing


def test_set_stage_pause_idempotent_within_one_minute(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")

    assert store.set_stage_pause(
        WorkStage.CLAIM,
        paused=True,
        reason="rate limit",
        actor_id="ops",
        paused_at=NOW,
    ) is True
    # Identical within 1 minute window is a no-op.
    assert store.set_stage_pause(
        WorkStage.CLAIM,
        paused=True,
        reason="rate limit",
        actor_id="ops",
        paused_at=NOW + timedelta(seconds=10),
    ) is False


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


def test_api_get_budget_returns_default_when_unset(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    SQLiteStore(database)
    client = TestClient(create_app(database))

    response = client.get("/v1/operations/budget")

    assert response.status_code == 200
    body = response.json()
    by_stage = {entry["stage"]: entry["daily_limit"] for entry in body["budgets"]}
    assert by_stage == {stage.value: DEFAULT_DAILY_BUDGET[stage] for stage in WorkStage}
    # No row persisted yet, so updated_at/updated_by stay None.
    for entry in body["budgets"]:
        assert entry["updated_at"] is None
        assert entry["updated_by"] is None


def test_api_put_budget_updates_and_returns_200(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    client = TestClient(create_app(database))

    response = client.put(
        "/v1/operations/budget/l1",
        json={"daily_limit": 25.0},
        headers={"X-Actor-ID": "alice"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["stage"] == "l1"
    assert body["daily_limit"] == 25.0
    assert body["updated_by"] == "alice"
    assert body["updated_at"] is not None
    assert store.get_budget_config()[WorkStage.L1] == 25.0


def test_api_put_budget_rejects_negative(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    client = TestClient(create_app(database))

    response = client.put(
        "/v1/operations/budget/l1",
        json={"daily_limit": -5.0},
        headers={"X-Actor-ID": "alice"},
    )

    assert response.status_code == 422


def test_api_put_budget_unknown_stage_returns_404(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    client = TestClient(create_app(database))

    response = client.put(
        "/v1/operations/budget/not-a-stage",
        json={"daily_limit": 10.0},
        headers={"X-Actor-ID": "alice"},
    )

    assert response.status_code == 404


def test_api_post_pauses_pauses_stage(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    client = TestClient(create_app(database))

    response = client.post(
        "/v1/operations/pauses",
        json={"stage": "llm", "paused": True, "reason": "rate limit"},
        headers={"X-Actor-ID": "alice"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["stage"] == "llm"
    assert body["paused"] is True
    assert body["reason"] == "rate limit"
    assert body["actor_id"] == "alice"
    assert store.is_stage_paused(WorkStage.LLM) is True

    # Replaying the same payload within the 1-minute window is idempotent.
    replay = client.post(
        "/v1/operations/pauses",
        json={"stage": "llm", "paused": True, "reason": "rate limit"},
        headers={"X-Actor-ID": "alice"},
    )
    assert replay.status_code == 200
    assert replay.json()["created"] is False


def test_api_get_pauses_lists_current_state(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    client = TestClient(create_app(database))

    store.set_stage_pause(
        WorkStage.LLM,
        paused=True,
        reason="ops freeze",
        actor_id="ops",
        paused_at=NOW,
    )

    response = client.get("/v1/operations/pauses")
    assert response.status_code == 200
    body = response.json()
    by_stage = {entry["stage"]: entry for entry in body["pauses"]}
    assert by_stage["llm"]["paused"] is True
    assert by_stage["llm"]["reason"] == "ops freeze"
    assert by_stage["llm"]["actor_id"] == "ops"
    # Stages with no pause row default to paused=False.
    assert by_stage["l1"]["paused"] is False


# ---------------------------------------------------------------------------
# CLI subcommands (subprocess-free via cli.main)
# ---------------------------------------------------------------------------


def test_cli_budget_show_subcommand(tmp_path, capsys) -> None:
    database = tmp_path / "webradar.db"

    assert cli.main(["budget", "show", "--database", str(database)]) == 0
    body = json.loads(capsys.readouterr().out)
    by_stage = {entry["stage"]: entry for entry in body["budgets"]}
    assert by_stage["l1"]["daily_limit"] == DEFAULT_DAILY_BUDGET[WorkStage.L1]


def test_cli_budget_set_subcommand(tmp_path, capsys) -> None:
    database = tmp_path / "webradar.db"

    assert (
        cli.main(
            [
                "budget",
                "set",
                "--database",
                str(database),
                "--stage",
                "l1",
                "--daily-limit",
                "77.0",
                "--actor-id",
                "cli-user",
            ]
        )
        == 0
    )
    capsys.readouterr()
    store = SQLiteStore(database)
    assert store.get_budget_config()[WorkStage.L1] == 77.0


def test_cli_budget_set_rejects_zero(tmp_path, capsys) -> None:
    database = tmp_path / "webradar.db"

    assert cli.main(
        [
            "budget",
            "set",
            "--database",
            str(database),
            "--stage",
            "l1",
            "--daily-limit",
            "0",
            "--actor-id",
            "cli-user",
        ]
    ) == 2


def test_cli_pause_set_subcommand(tmp_path, capsys) -> None:
    database = tmp_path / "webradar.db"

    assert (
        cli.main(
            [
                "pause",
                "set",
                "--database",
                str(database),
                "--stage",
                "llm",
                "--paused",
                "true",
                "--reason",
                "cli-test",
                "--actor-id",
                "cli-user",
            ]
        )
        == 0
    )
    capsys.readouterr()

    store = SQLiteStore(database)
    assert store.is_stage_paused(WorkStage.LLM) is True
    latest = store.latest_stage_pause(WorkStage.LLM)
    assert latest is not None
    assert latest[2] == "cli-test"


def test_cli_pause_show_subcommand(tmp_path, capsys) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    store.set_stage_pause(
        WorkStage.SIGNAL_INGEST,
        paused=True,
        reason="throttle",
        actor_id="ops",
        paused_at=NOW,
    )

    assert cli.main(["pause", "show", "--database", str(database)]) == 0
    body = json.loads(capsys.readouterr().out)
    by_stage = {entry["stage"]: entry for entry in body["pauses"]}
    assert by_stage["signal_ingest"]["paused"] is True
    assert by_stage["signal_ingest"]["reason"] == "throttle"


# ---------------------------------------------------------------------------
# Daemon integration with pause_checker
# ---------------------------------------------------------------------------


def test_daemon_skips_paused_stage(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    store.set_stage_pause(
        WorkStage.L1,
        paused=True,
        reason="ops freeze",
        actor_id="operator",
        paused_at=NOW,
    )
    assert store.enqueue_work(WorkStage.L1, "trigger-probe", scheduled_at=NOW) is True
    pipeline = _StubPipeline()

    def pause_checker(stage: WorkStage) -> tuple[bool, str]:
        return store.is_stage_paused(stage), "ops freeze"

    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=5.0,
        budget_per_tick=2.0,
        worker_id="test-worker",
        budget_loader=lambda: store.get_budget_config(),
        pause_checker=pause_checker,
        clock=lambda: NOW,
    )

    asyncio.run(daemon.run(max_ticks=1))

    # Probe handler must not have run because the stage was paused.
    assert pipeline.probed_at == []


def test_daemon_uses_budget_loader_when_no_explicit_budget(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    store.set_budget_config(
        WorkStage.L1, daily_limit=10.0, updated_by="operator", occurred_at=NOW
    )
    # Saturate the L1 budget so the loader-derived limit closes the gate.
    store.reserve_budget(
        WorkStage.L1, units=10.0, daily_limit=10.0, occurred_at=NOW, entity_id="seed"
    )
    assert store.enqueue_work(WorkStage.L1, "trigger-probe", scheduled_at=NOW) is True
    pipeline = _StubPipeline()

    daemon = WorkerDaemon(
        store=store,
        pipeline=pipeline,  # type: ignore[arg-type]
        tick_seconds=0.01,
        lease_seconds=5.0,
        budget_per_tick=2.0,
        worker_id="test-worker",
        budget_loader=lambda: store.get_budget_config(),
        clock=lambda: NOW,
    )

    asyncio.run(daemon.run(max_ticks=1))

    # Budget gate closed -> probe never invoked.
    assert pipeline.probed_at == []