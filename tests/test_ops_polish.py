"""Tier 2 ops polish: concurrent-decision 409, cost_per_effective_candidate,
per-hostname rate limiting.

These tests pin the three small features requested by the Tier 2 ops polish
spec: (1) spec §11 concurrent-decision conflict feedback, (2) the
``cost_per_effective_candidate`` funnel metric, and (3) a per-hostname
token-bucket rate limit applied to outbound HTTP probes.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import pytest
from fastapi.testclient import TestClient

from domainhunter.api import create_app
from domainhunter.crawler.http_probe import HostRateLimiter
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.reviews import ReviewAction, build_review_decision
from domainhunter.domain.review_priority import ReviewPriorityInputs, calculate_review_priority
from domainhunter.domain.work_queue import WorkStage
from domainhunter.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 17, tzinfo=UTC)


def _seed_candidate(store: SQLiteStore, hostname: str = "example.com"):
    """Append one reviewable candidate + version, and return its identifiers."""
    candidate = store.create_candidate(hostname, created_at=OBSERVED_AT)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="human",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example AI",
            description_suggestion="AI workflow automation",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", f"https://{hostname}"),),
        ),
        created_at=OBSERVED_AT,
    )
    store.append_review_priority(
        candidate.candidate_id,
        calculate_review_priority(ReviewPriorityInputs(0.9, 0.9, None, 0.9)),
        calculated_at=OBSERVED_AT,
    )
    return candidate, version


# ---------------------------------------------------------------------------
# Feature 1: Concurrent decision conflict feedback
# ---------------------------------------------------------------------------


def test_concurrent_decision_conflict_returns_409(tmp_path) -> None:
    """Two reviewers with different request_ids on the same version → 409."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    candidate, version = _seed_candidate(store)
    client = TestClient(create_app(database))

    first = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json={"request_id": "reviewer-a", "action": "approve", "reason_tags": []},
        headers={"X-Actor-ID": "reviewer-a"},
    )
    assert first.status_code == 201
    first_decision_id = first.json()["decision_id"]

    second = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json={"request_id": "reviewer-b", "action": "reject", "reason_tags": []},
        headers={"X-Actor-ID": "reviewer-b"},
    )

    assert second.status_code == 409
    detail = second.json()["detail"]
    assert detail["detail"] == "concurrent decision conflict"
    assert detail["active_decision_id"] == first_decision_id
    assert detail["active_request_id"] == "reviewer-a"
    # Only the first decision is persisted.
    decisions = store.list_review_decisions(candidate.candidate_id)
    assert len(decisions) == 1
    assert decisions[0].request_id == "reviewer-a"


def test_concurrent_decision_same_request_id_is_idempotent(tmp_path) -> None:
    """Replay with the same request_id must remain 200, not 409."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    candidate, version = _seed_candidate(store)
    client = TestClient(create_app(database))
    body = {"request_id": "replay-1", "action": "approve", "reason_tags": []}
    headers = {"X-Actor-ID": "reviewer-x"}

    first = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json=body,
        headers=headers,
    )
    replay = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json=body,
        headers=headers,
    )

    assert first.status_code == 201
    assert first.json()["created"] is True
    assert replay.status_code == 200
    assert replay.json()["created"] is False
    assert replay.json()["decision_id"] == first.json()["decision_id"]


def test_revoked_decision_does_not_block_new_decision(tmp_path) -> None:
    """Once the active decision is revoked, a fresh request_id may decide again."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    candidate, version = _seed_candidate(store)
    client = TestClient(create_app(database))

    first = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json={"request_id": "reviewer-a", "action": "approve", "reason_tags": []},
        headers={"X-Actor-ID": "reviewer-a"},
    )
    assert first.status_code == 201
    first_decision_id = first.json()["decision_id"]

    revoked = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions/{first_decision_id}/revoke",
        json={"request_id": "reviewer-a-revoke", "reason": "reconsider"},
        headers={"X-Actor-ID": "reviewer-a"},
    )
    assert revoked.status_code == 200
    assert revoked.json()["revoked"] is True

    second = client.post(
        f"/v1/candidates/{candidate.candidate_id}/versions/{version.version}/decisions",
        json={"request_id": "reviewer-b", "action": "reject", "reason_tags": []},
        headers={"X-Actor-ID": "reviewer-b"},
    )

    assert second.status_code == 201, second.text
    assert second.json()["created"] is True
    decisions = store.list_review_decisions(candidate.candidate_id)
    assert len(decisions) == 2
    assert store.is_decision_revoked(first_decision_id) is True


# ---------------------------------------------------------------------------
# Feature 2: cost_per_effective_candidate metric
# ---------------------------------------------------------------------------


def test_metrics_exposes_cost_per_effective_candidate(tmp_path) -> None:
    """cost_per_effective_candidate = reserved_units / max(1, approved_count)."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    approved_a, version_a = _seed_candidate(store, hostname="approved-a.example")
    approved_b, version_b = _seed_candidate(store, hostname="approved-b.example")
    rejected, version_c = _seed_candidate(store, hostname="rejected.example")

    store.reserve_budget(
        WorkStage.L1, units=12.0, daily_limit=100.0, occurred_at=OBSERVED_AT, entity_id="seed"
    )
    store.append_review_decision(
        build_review_decision(
            request_id="approve-a",
            candidate_id=approved_a.candidate_id,
            candidate_version=version_a.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer",
            decided_at=OBSERVED_AT,
        )
    )
    store.append_review_decision(
        build_review_decision(
            request_id="approve-b",
            candidate_id=approved_b.candidate_id,
            candidate_version=version_b.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer",
            decided_at=OBSERVED_AT,
        )
    )
    store.append_review_decision(
        build_review_decision(
            request_id="reject-c",
            candidate_id=rejected.candidate_id,
            candidate_version=version_c.version,
            action=ReviewAction.REJECT,
            actor_id="reviewer",
            decided_at=OBSERVED_AT,
        )
    )

    metrics = store.funnel_metrics()

    assert metrics.budget_reserved_units == 12.0
    assert metrics.cost_per_effective_candidate == pytest.approx(12.0 / 2)
    # The rejected decision must not count toward the divisor.
    assert store.count_approved_versions() == 2


def test_metrics_zero_approved_returns_cost_based_on_max_1(tmp_path) -> None:
    """No approvals → use max(1, 0) as the divisor; no division by zero."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    store.reserve_budget(
        WorkStage.L1, units=4.0, daily_limit=100.0, occurred_at=OBSERVED_AT, entity_id="seed"
    )

    metrics = store.funnel_metrics()

    assert metrics.budget_reserved_units == 4.0
    assert metrics.cost_per_effective_candidate == pytest.approx(4.0)
    assert store.count_approved_versions() == 0


# ---------------------------------------------------------------------------
# Feature 3: per-hostname rate limiter in HTTPProbe
# ---------------------------------------------------------------------------


def test_host_rate_limiter_slows_rapid_probes() -> None:
    """Two consecutive ``acquire()`` calls on the same hostname must wait."""
    # 5 rps ⇒ minimum 0.2s per acquire. We do 3 acquires to ~0.4s of expected
    # wait, large enough to dominate async scheduling noise.
    limiter = HostRateLimiter(requests_per_second_per_host=5.0)

    async def run() -> float:
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(3):
            await limiter.acquire("example.com")
        return loop.time() - start

    elapsed = asyncio.run(run())

    assert elapsed >= 0.35, f"3 acquires against a 5rps limiter took only {elapsed:.3f}s"


async def _async_resolver(hostname: str) -> tuple[str, ...]:
    return ("1.1.1.1",)


def test_host_rate_limiter_zero_rate_disables_limiting() -> None:
    """``requests_per_second_per_host=0`` makes ``acquire()`` a no-op."""
    limiter = HostRateLimiter(requests_per_second_per_host=0.0)

    async def run() -> float:
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(5):
            await limiter.acquire("example.com")
        return loop.time() - start

    elapsed = asyncio.run(run())
    assert elapsed < 0.05, f"disabled limiter still slept: {elapsed:.4f}s"


def test_host_rate_limiter_isolates_per_hostname() -> None:
    """Acquires on different hostnames do not block each other."""
    limiter = HostRateLimiter(requests_per_second_per_host=20.0)

    async def run() -> float:
        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(limiter.acquire("a.example"), limiter.acquire("b.example"))
        return loop.time() - start

    elapsed = asyncio.run(run())
    # Each hostname has its own bucket; running two in parallel should be
    # effectively instantaneous rather than serialized.
    assert elapsed < 0.04, f"per-host isolation broke: {elapsed:.4f}s"