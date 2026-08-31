"""Spec §14 AlertEngine + alert store contract tests."""

import asyncio
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from webradar_v2 import cli
from webradar_v2.api import create_app
from webradar_v2.domain.alerts import Alert, AlertKind, AlertSeverity
from webradar_v2.domain.audit import AIKnowsAuditEntry
from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.events import SourceEvent
from webradar_v2.domain.observations import Observation, OutcomeCode
from webradar_v2.domain.work_queue import WorkStage
from webradar_v2.scheduler.alerts import AlertEngine
from webradar_v2.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 17, 12, 0, 0, tzinfo=UTC)


def _hour_ago(hours: float = 1.0) -> datetime:
    return NOW - timedelta(hours=hours)


def _seed_source_events(store: SQLiteStore, *, count: int, observed_at: datetime) -> None:
    for index in range(count):
        event = SourceEvent(
            source="ct_log",
            source_event_id=f"seed:{observed_at.timestamp()}:{index}",
            raw_subject=f"example{index}.com",
            observed_at=observed_at,
        )
        store.append_source_event(event, hostname=f"example{index}.com")


def _seed_observations(
    store: SQLiteStore,
    *,
    outcomes: list[tuple[str, OutcomeCode]],
    observed_at: datetime,
    domain_prefix: str = "example",
) -> None:
    for index, (domain, code) in enumerate(outcomes):
        hostname = f"{domain_prefix}{index}.{domain}"
        # The store requires a domain row first.
        event = SourceEvent(
            source="ct_log",
            source_event_id=f"seed-obs:{observed_at.timestamp()}:{index}",
            raw_subject=hostname,
            observed_at=observed_at,
        )
        store.append_source_event(event, hostname=hostname)
        store.append_observation(
            Observation(
                domain=hostname,
                outcome_code=code,
                observed_at=observed_at,
                attempt_number=1,
            )
        )


def _make_llm_version(
    store: SQLiteStore, *, outcome: CandidateOutcome, created_at: datetime
) -> str:
    candidate = store.create_candidate("example.com", created_at=created_at)
    store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example",
            description_suggestion="Example desc",
            evidence=(Evidence(EvidenceType.TITLE, "Example", "https://example.com"),),
        ),
        created_at=created_at,
    )
    store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="llm",
            primary_outcome=outcome,
            classification_confidence=0.4,
            name_suggestion="Example (LLM)",
            description_suggestion="Needs review",
            evidence=(Evidence(EvidenceType.TITLE, "Example", "https://example.com"),),
            model_version="mock-1",
        ),
        created_at=created_at,
    )
    return candidate.candidate_id


def _make_engine(store: SQLiteStore, **overrides) -> AlertEngine:
    defaults: dict[str, object] = {
        "clock": lambda: NOW,
        "daily_budget_per_stage": {},
        "window": timedelta(hours=1),
        "baseline_window": timedelta(hours=24),
    }
    defaults.update(overrides)
    return AlertEngine(store, **defaults)  # type: ignore[arg-type]


def _run(engine: AlertEngine):
    return asyncio.run(engine.evaluate())


# ---------------------------------------------------------------------
# AlertStore — append + dedup
# ---------------------------------------------------------------------


def test_append_alert_is_idempotent_by_alert_id(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    alert = Alert(
        alert_id="abc123",
        kind=AlertKind.ZERO_INPUT,
        severity=AlertSeverity.CRITICAL,
        title="x",
        summary="y",
        opened_at=NOW,
        runbook_id="rb.zero_input",
        related_candidate_id=None,
        related_stage=None,
        payload={},
    )

    assert store.append_alert(alert) is True
    # Second append of the exact same alert_id (same fingerprint window) must
    # not insert a duplicate.
    assert store.append_alert(alert) is False
    assert len(store.list_alerts()) == 1


def test_alert_idempotent_within_window(tmp_path) -> None:
    """Re-running AlertEngine twice must not duplicate the same fingerprint."""
    store = SQLiteStore(tmp_path / "webradar.db")
    _seed_source_events(store, count=20, observed_at=_hour_ago(3.0))
    engine = _make_engine(store)

    first = _run(engine)
    second = _run(engine)

    assert len(first) >= 1  # baseline drift or zero_input, depending on seeding
    assert second == ()
    assert len(store.list_alerts()) == len(first)


# ---------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------


def test_zero_input_emits_critical_when_sources_dry_up(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    _seed_source_events(store, count=8, observed_at=_hour_ago(3.0))  # baseline
    engine = _make_engine(store)

    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.ZERO_INPUT in kinds
    zero_input = next(alert for alert in emitted if alert.kind is AlertKind.ZERO_INPUT)
    assert zero_input.severity is AlertSeverity.CRITICAL


def test_baseline_drift_warns_when_rate_drops_below_half(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    # Seed ~120 events across 24h (5/h), but with all events strictly older
    # than the 1h window so recent_rate is 0.
    for hour in range(0, 24, 2):
        _seed_source_events(store, count=10, observed_at=_hour_ago(hour + 2.0))
    engine = _make_engine(store)

    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.BASELINE_DRIFT in kinds
    drift = next(alert for alert in emitted if alert.kind is AlertKind.BASELINE_DRIFT)
    assert drift.severity is AlertSeverity.WARN


def test_queue_backlog_warns_after_1h_of_stuck_work(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    store.enqueue_work(WorkStage.L1, "stuck-1", scheduled_at=_hour_ago(2.0))
    store.enqueue_work(WorkStage.L1, "stuck-2", scheduled_at=_hour_ago(3.0))
    engine = _make_engine(store)

    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.QUEUE_BACKLOG in kinds
    backlog = next(alert for alert in emitted if alert.kind is AlertKind.QUEUE_BACKLOG)
    assert backlog.severity is AlertSeverity.WARN


def test_budget_exhausted_when_80pct_consumed(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    store.reserve_budget(
        WorkStage.L1,
        units=9.0,
        daily_limit=10.0,
        occurred_at=NOW,
        entity_id="candidate-1",
    )
    engine = _make_engine(
        store,
        daily_budget_per_stage={WorkStage.L1: 10.0, WorkStage.L2: 10.0, WorkStage.LLM: 10.0},
    )

    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.BUDGET_EXHAUSTED in kinds
    budget = next(alert for alert in emitted if alert.kind is AlertKind.BUDGET_EXHAUSTED)
    assert budget.severity is AlertSeverity.CRITICAL
    assert budget.related_stage == WorkStage.L1.value


def test_error_rate_spike_when_5xx_doubles(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    # 24h baseline: 5% error rate (1/20).
    baseline_outcomes: list[tuple[str, OutcomeCode]] = []
    for index in range(20):
        baseline_outcomes.append(
            ("baseline.com", OutcomeCode.SUCCESS if index != 0 else OutcomeCode.HTTP_5XX)
        )
    _seed_observations(store, outcomes=baseline_outcomes, observed_at=_hour_ago(12.0))

    # Recent (last 1h): 50% error rate (5/10) → 10x baseline.
    recent_outcomes = []
    for index in range(10):
        recent_outcomes.append(
            ("recent.com", OutcomeCode.SUCCESS if index < 5 else OutcomeCode.HTTP_5XX)
        )
    _seed_observations(store, outcomes=recent_outcomes, observed_at=_hour_ago(0.5))

    engine = _make_engine(store)
    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.ERROR_RATE_SPIKE in kinds
    spike = next(alert for alert in emitted if alert.kind is AlertKind.ERROR_RATE_SPIKE)
    assert spike.severity is AlertSeverity.CRITICAL


def test_ssrf_interception_spike_when_blocked_doubles(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    # Baseline: 1 blocked out of 100.
    baseline_outcomes = [
        ("base.com", OutcomeCode.BLOCKED_SSRF if index == 0 else OutcomeCode.SUCCESS)
        for index in range(100)
    ]
    _seed_observations(store, outcomes=baseline_outcomes, observed_at=_hour_ago(12.0))

    # Recent: 5 blocked out of 50 (10%).
    recent_outcomes = [
        ("rec.com", OutcomeCode.BLOCKED_SSRF if index < 5 else OutcomeCode.SUCCESS)
        for index in range(50)
    ]
    _seed_observations(store, outcomes=recent_outcomes, observed_at=_hour_ago(0.5))

    engine = _make_engine(store)
    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.SSRF_INTERCEPTION_SPIKE in kinds
    spike = next(
        alert for alert in emitted if alert.kind is AlertKind.SSRF_INTERCEPTION_SPIKE
    )
    assert spike.severity is AlertSeverity.WARN


def test_schema_failure_on_llm_valid_but_not_ready(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    _make_llm_version(
        store, outcome=CandidateOutcome.VALID_BUT_NOT_READY, created_at=_hour_ago(0.2)
    )

    engine = _make_engine(store)
    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.SCHEMA_FAILURE in kinds
    schema = next(alert for alert in emitted if alert.kind is AlertKind.SCHEMA_FAILURE)
    assert schema.severity is AlertSeverity.WARN
    assert schema.payload["llm_needs_review_count"] >= 1


def test_external_sync_failure_warns_on_audit_activity(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate_id = "candidate-external"
    store.append_audit(
        AIKnowsAuditEntry(
            method="POST",
            url="/v1/webradar/drafts",
            status_code=503,
            latency_ms=100.0,
            candidate_id=candidate_id,
            candidate_version=1,
            occurred_at=_hour_ago(0.3),
        )
    )

    engine = _make_engine(store)
    emitted = _run(engine)

    kinds = {alert.kind for alert in emitted}
    assert AlertKind.EXTERNAL_SYNC_FAILURE in kinds
    sync = next(
        alert for alert in emitted if alert.kind is AlertKind.EXTERNAL_SYNC_FAILURE
    )
    assert sync.severity is AlertSeverity.WARN
    assert sync.related_candidate_id == candidate_id


# ---------------------------------------------------------------------
# API + CLI surface
# ---------------------------------------------------------------------


def test_api_lists_alerts(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    store.append_alert(
        Alert(
            alert_id="listed-1",
            kind=AlertKind.ZERO_INPUT,
            severity=AlertSeverity.CRITICAL,
            title="x",
            summary="y",
            opened_at=NOW,
            runbook_id="rb.zero_input",
            related_candidate_id=None,
            related_stage=None,
            payload={"k": "v"},
        )
    )

    response = TestClient(create_app(database)).get("/v1/alerts")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    item = payload["alerts"][0]
    assert item["alert_id"] == "listed-1"
    assert item["kind"] == "zero_input"
    assert item["severity"] == "critical"
    assert item["payload"] == {"k": "v"}


def test_api_lists_alerts_since_filter(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    store.append_alert(
        Alert(
            alert_id="old",
            kind=AlertKind.ZERO_INPUT,
            severity=AlertSeverity.CRITICAL,
            title="old",
            summary="y",
            opened_at=_hour_ago(48.0),
            runbook_id="rb.zero_input",
            related_candidate_id=None,
            related_stage=None,
            payload={},
        )
    )
    store.append_alert(
        Alert(
            alert_id="new",
            kind=AlertKind.QUEUE_BACKLOG,
            severity=AlertSeverity.WARN,
            title="new",
            summary="y",
            opened_at=NOW,
            runbook_id="rb.queue_backlog",
            related_candidate_id=None,
            related_stage=None,
            payload={},
        )
    )

    response = TestClient(create_app(database)).get(
        "/v1/alerts", params={"since": (_hour_ago(2.0)).isoformat()}
    )

    assert response.status_code == 200
    payload = response.json()
    assert [item["alert_id"] for item in payload["alerts"]] == ["new"]


def test_api_get_runbook_returns_200_and_404(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    client = TestClient(create_app(database))

    ok = client.get("/v1/runbooks/rb.zero_input")
    assert ok.status_code == 200
    body = ok.json()
    assert body["runbook_id"] == "rb.zero_input"
    assert body["kind"] == "zero_input"
    assert any("先看什么" in step for step in body["steps"])

    missing = client.get("/v1/runbooks/rb.does_not_exist")
    assert missing.status_code == 404


def test_api_lists_runbooks_summary(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    response = TestClient(create_app(database)).get("/v1/runbooks")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == len(AlertKind)
    ids = {item["runbook_id"] for item in payload["runbooks"]}
    assert "rb.zero_input" in ids
    assert "rb.external_sync_failure" in ids


def test_cli_alerts_subcommand(tmp_path, capsys) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    store.append_alert(
        Alert(
            alert_id="cli-1",
            kind=AlertKind.BUDGET_EXHAUSTED,
            severity=AlertSeverity.CRITICAL,
            title="x",
            summary="y",
            opened_at=NOW,
            runbook_id="rb.budget_exhausted",
            related_candidate_id=None,
            related_stage=None,
            payload={},
        )
    )

    import json

    exit_code = cli.main(["alerts", "--database", str(database)])
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["count"] == 1
    assert payload["alerts"][0]["alert_id"] == "cli-1"


def test_cli_runbook_subcommand(tmp_path, capsys) -> None:
    import json

    exit_code = cli.main(["runbook", "--id", "rb.zero_input"])
    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["runbook_id"] == "rb.zero_input"
    assert payload["kind"] == "zero_input"

    missing = cli.main(["runbook", "--id", "rb.unknown"])
    assert missing == 1