"""Tier 2 ops polish — FunnelAnalytics: conversion rates, latency percentiles,
backlog shape. These tests pin the new analytics feature without touching
budget config, pause, alerts, or other features."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from domainhunter.api import create_app
from domainhunter.cli import main as cli_main
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.events import SourceEvent
from domainhunter.domain.metrics import FunnelAnalytics, percentile
from domainhunter.domain.reviews import ReviewAction, build_review_decision
from domainhunter.domain.work_queue import WorkStage
from domainhunter.storage.sqlite import SQLiteStore


T0 = datetime(2026, 8, 17, 12, 0, 0, tzinfo=UTC)


def _event(key: str, *, hostname: str, observed_at: datetime) -> SourceEvent:
    return SourceEvent(
        source="ct",
        source_event_id=key,
        raw_subject=f"CN={key}",
        observed_at=observed_at,
    )


def _seed_source(store: SQLiteStore, key: str, *, hostname: str, observed_at: datetime) -> None:
    store.append_source_event(_event(key, hostname=hostname, observed_at=observed_at), hostname=hostname)


def _seed_candidate(
    store: SQLiteStore,
    *,
    hostname: str,
    created_at: datetime,
    approved: bool = False,
    decided_at: datetime | None = None,
    requested_at: datetime | None = None,
) -> None:
    candidate = store.create_candidate(hostname, created_at=created_at)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="human",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion=f"Example {hostname}",
            description_suggestion="AI workflow automation",
            evidence=(Evidence(EvidenceType.TITLE, "Example", f"https://{hostname}"),),
        ),
        created_at=created_at,
    )
    if approved:
        decision = build_review_decision(
            request_id=f"req-{candidate.candidate_id}",
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            actor_id="actor-1",
            action=ReviewAction.APPROVE,
            reason_tags=(),
            decided_at=decided_at or (created_at + timedelta(minutes=10)),
        )
        store.append_review_decision(decision)
    if requested_at is not None:
        from domainhunter.domain.publications import PublicationRecord
        from domainhunter.publish.aiknows_client import SyncResult, SyncStatus

        sync = SyncResult(
            status=SyncStatus.SYNCED,
            external_entry_id=f"ext-{candidate.candidate_id}",
            external_version="1",
            detail=None,
        )
        record = PublicationRecord.from_sync_result(
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            requested_at=requested_at,
            result=sync,
        )
        store.append_publication(record)


# ---------------------------------------------------------------------------
# Pure helper
# ---------------------------------------------------------------------------


def test_percentile_helper_picks_nearest_rank() -> None:
    assert percentile([10.0, 20.0, 30.0, 40.0], 50.0) == 20.0
    assert percentile([10.0, 20.0, 30.0, 40.0], 95.0) == 40.0
    assert percentile([10.0, 20.0, 30.0, 40.0], 0.0) == 10.0
    assert percentile([10.0, 20.0, 30.0, 40.0], 100.0) == 40.0


def test_percentile_helper_handles_empty() -> None:
    assert percentile([], 50.0) is None


# ---------------------------------------------------------------------------
# Conversion rates
# ---------------------------------------------------------------------------


def test_funnel_analytics_conversion_rates_compute(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "funnel.db")
    # 4 source events across 4 distinct registrable domains; 2 yield candidates,
    # 1 of those yields an approved version, and 1 publication lands against it.
    _seed_source(store, "evt-1", hostname="alpha.com", observed_at=T0)
    _seed_source(store, "evt-2", hostname="beta.io", observed_at=T0 + timedelta(seconds=5))
    _seed_source(store, "evt-3", hostname="gamma.ai", observed_at=T0 + timedelta(seconds=10))
    _seed_source(store, "evt-4", hostname="delta.app", observed_at=T0 + timedelta(seconds=15))

    _seed_candidate(
        store,
        hostname="alpha.com",
        created_at=T0 + timedelta(minutes=2),
        approved=True,
        decided_at=T0 + timedelta(minutes=10),
        requested_at=T0 + timedelta(minutes=12),
    )
    _seed_candidate(
        store,
        hostname="beta.io",
        created_at=T0 + timedelta(minutes=3),
        approved=False,
    )

    analytics = store.compute_funnel_analytics(now=T0 + timedelta(hours=1))

    assert isinstance(analytics, FunnelAnalytics)
    assert analytics.conversion_source_to_candidate == 0.5  # 2 / 4
    assert analytics.conversion_candidate_to_approved == 0.5  # 1 approved / 2 versions
    assert analytics.conversion_source_to_published == 0.25  # 1 / 4


def test_funnel_analytics_conversion_rates_clamp_when_empty(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "empty.db")
    analytics = store.compute_funnel_analytics(now=T0)
    assert analytics.conversion_source_to_candidate == 0.0
    assert analytics.conversion_candidate_to_approved == 0.0
    assert analytics.conversion_source_to_published == 0.0
    assert analytics.backlog_by_stage == {}
    assert analytics.backlog_over_1h_by_stage == {}


# ---------------------------------------------------------------------------
# Latency percentiles
# ---------------------------------------------------------------------------


def test_funnel_analytics_latency_p50_p95_with_two_data_points(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "latency.db")
    # Distinct registrable domains so each candidate has its own event-domain row.
    _seed_source(store, "evt-1", hostname="alpha.com", observed_at=T0)
    _seed_source(store, "evt-2", hostname="beta.io", observed_at=T0 + timedelta(seconds=30))
    # 60s and 120s deltas → P50 picks 60s, P95 picks 120s (2-element nearest rank).
    _seed_candidate(
        store,
        hostname="alpha.com",
        created_at=T0 + timedelta(seconds=60),
        approved=True,
        decided_at=T0 + timedelta(seconds=120),
    )
    _seed_candidate(
        store,
        hostname="beta.io",
        created_at=T0 + timedelta(seconds=150),
        approved=True,
        decided_at=T0 + timedelta(seconds=300),
    )

    analytics = store.compute_funnel_analytics(now=T0 + timedelta(hours=1))

    assert analytics.latency_first_signal_to_candidate_p50_seconds == 60.0
    assert analytics.latency_first_signal_to_candidate_p95_seconds == 120.0
    # candidate-to-decision deltas: 60s and 150s
    assert analytics.latency_candidate_to_decision_p50_seconds == 60.0
    assert analytics.latency_candidate_to_decision_p95_seconds == 150.0


def test_funnel_analytics_latency_returns_none_when_empty(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "nolat.db")
    analytics = store.compute_funnel_analytics(now=T0)
    assert analytics.latency_first_signal_to_candidate_p50_seconds is None
    assert analytics.latency_first_signal_to_candidate_p95_seconds is None
    assert analytics.latency_candidate_to_decision_p50_seconds is None
    assert analytics.latency_candidate_to_decision_p95_seconds is None


# ---------------------------------------------------------------------------
# Backlog
# ---------------------------------------------------------------------------


def test_funnel_analytics_backlog_by_stage(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.db")
    now = T0 + timedelta(hours=2)
    store.enqueue_work(WorkStage.L1, "alpha.example", scheduled_at=now - timedelta(minutes=10))
    store.enqueue_work(WorkStage.L1, "beta.example", scheduled_at=now - timedelta(minutes=20))
    store.enqueue_work(WorkStage.LLM, "gamma.example", scheduled_at=now - timedelta(minutes=5))

    analytics = store.compute_funnel_analytics(now=now)
    assert analytics.backlog_by_stage == {"l1": 2, "llm": 1}


def test_funnel_analytics_backlog_over_1h_filters_by_age(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "stale.db")
    now = T0 + timedelta(hours=2)
    store.enqueue_work(WorkStage.L1, "alpha.example", scheduled_at=now - timedelta(minutes=10))
    store.enqueue_work(WorkStage.L1, "beta.example", scheduled_at=now - timedelta(hours=2))
    store.enqueue_work(WorkStage.LLM, "gamma.example", scheduled_at=now - timedelta(hours=3))

    analytics = store.compute_funnel_analytics(now=now)
    # Only items scheduled_at < now - 1h qualify. min:10 is too fresh.
    assert analytics.backlog_over_1h_by_stage == {"l1": 1, "llm": 1}
    assert analytics.backlog_by_stage == {"l1": 2, "llm": 1}


# ---------------------------------------------------------------------------
# API + CLI
# ---------------------------------------------------------------------------


def test_metrics_endpoint_includes_analytics(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "api-metrics.db")
    app = create_app(tmp_path / "api-metrics.db")
    response = TestClient(app).get("/v1/metrics")
    assert response.status_code == 200
    body = response.json()
    # FunnelMetrics fields still present (backwards compat).
    assert "source_events" in body
    assert body["source_events"] == 0
    assert "analytics" in body
    analytics = body["analytics"]
    assert analytics["conversion_source_to_candidate"] == 0.0
    assert analytics["backlog_by_stage"] == {}
    assert "computed_at" in analytics  # ISO string
    # Ensure ISO-8601 round-trip parses.
    datetime.fromisoformat(analytics["computed_at"])


def test_analytics_endpoint_returns_just_analytics(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "api-analytics.db")
    app = create_app(tmp_path / "api-analytics.db")
    response = TestClient(app).get("/v1/analytics")
    assert response.status_code == 200
    body = response.json()
    # Must NOT include FunnelMetrics fields.
    assert "source_events" not in body
    assert "analytics" not in body
    assert body["conversion_source_to_candidate"] == 0.0
    assert body["backlog_by_stage"] == {}
    assert datetime.fromisoformat(body["computed_at"]).tzinfo is not None


def test_cli_analytics_subcommand(tmp_path: Path, capsys: pytest) -> None:
    db_path = tmp_path / "cli-analytics.db"
    SQLiteStore(db_path)
    exit_code = cli_main(["analytics", "--database", str(db_path)])
    assert exit_code == 0
    output = capsys.readouterr().out
    lines = output.strip().splitlines()
    keys = {line.split(":")[0] for line in lines}
    assert "conversion_source_to_candidate" in keys
    assert "latency_first_signal_to_candidate_p50_seconds" in keys
    assert "backlog_by_stage" in keys
    assert "backlog_over_1h_by_stage" in keys
    assert "computed_at" in keys
    # Latency fields without data must print the n/a sentinel.
    p50_line = next(
        line for line in lines if line.startswith("latency_first_signal_to_candidate_p50_seconds")
    )
    assert p50_line.endswith(": n/a")


# ---------------------------------------------------------------------------
# Smoke test ensuring FunnelAnalytics rejects naive datetimes
# ---------------------------------------------------------------------------


def test_funnel_analytics_rejects_naive_datetime(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "naive.db")
    with pytest.raises(ValueError):
        store.compute_funnel_analytics(now=datetime(2026, 8, 17))  # noqa: DTZ001


# Mark this module as testable for a json smoke import (keep coverage honest).
_ = json
_ = sys