"""Tests for the manual CT backfill: start-index search and bounded fan-out."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.filter.rdap_age import Registration
from domainhunter.ingest.backfill import BackfillConfig, locate_start_index


class _FakeTimestampFetcher:
    """Answers tree_sizes/leaf_timestamp from a monotonic timestamp table."""

    def __init__(self, stamps: dict[str, list[datetime]]) -> None:
        self._stamps = stamps

    @property
    def logs(self):  # noqa: ANN201 - duck-typed for the search
        return tuple(
            type("_Log", (), {"log_id": log_id})() for log_id in self._stamps
        )

    async def tree_sizes(self) -> dict[str, int]:
        return {log_id: len(stamps) for log_id, stamps in self._stamps.items()}

    async def leaf_timestamp(self, log_id: str, index: int) -> datetime | None:
        stamps = self._stamps[log_id]
        if index < 0 or index >= len(stamps):
            return None
        return stamps[index]


_BASE = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)


def _stamps(count: int, gap_seconds: int = 60) -> list[datetime]:
    return [_BASE + timedelta(seconds=gap_seconds * i) for i in range(count)]


def test_locate_start_index_finds_hour_boundary() -> None:
    fetcher = _FakeTimestampFetcher({"log": _stamps(600)})  # 600 minutes of entries
    since = _BASE + timedelta(minutes=240)

    result = _run_search(fetcher, "log", since, 600)

    assert result == 240


def _run_search(fetcher, log_id: str, since: datetime, tree_size: int) -> int:
    import asyncio

    return asyncio.run(
        locate_start_index(fetcher, log_id=log_id, since=since, tree_size=tree_size)
    )


def test_locate_start_index_clamps_to_bounds() -> None:
    fetcher = _FakeTimestampFetcher({"log": _stamps(100)})

    before_all = _run_search(fetcher, "log", _BASE - timedelta(days=1), 100)
    after_all = _run_search(fetcher, "log", _BASE + timedelta(days=1), 100)

    assert before_all == 0
    assert after_all == 100


def test_locate_start_index_survives_unparseable_entries() -> None:
    stamps = _stamps(50)
    fetcher = _FakeTimestampFetcher({"log": stamps})

    class _HoleyFetcher(_FakeTimestampFetcher):
        async def leaf_timestamp(self, log_id: str, index: int) -> datetime | None:
            if index in (23, 24):
                return None  # simulate unparseable leaves
            return stamps[index]

    result = _run_search(_HoleyFetcher({"log": stamps}), "log", stamps[30], 50)

    assert result == 30


def test_backfill_config_rejects_bad_knobs() -> None:
    with pytest.raises(ValueError):
        BackfillConfig(hours=0)
    with pytest.raises(ValueError):
        BackfillConfig(hours=1, probe_concurrency=0)
    with pytest.raises(ValueError):
        BackfillConfig(hours=1, claim_limit=-1)


def test_filter_pipeline_parallel_rdap_matches_sequential() -> None:
    seen: list[str] = []
    observed_peak = {"concurrent": 0, "current": 0}

    def slow_fetcher(domain: str) -> Registration | None:
        import threading
        import time

        with threading.Lock():
            observed_peak["current"] += 1
            observed_peak["concurrent"] = max(
                observed_peak["concurrent"], observed_peak["current"]
            )
        time.sleep(0.02)
        seen.append(domain)
        with threading.Lock():
            observed_peak["current"] -= 1
        return Registration(
            domain=domain,
            registration_date=datetime.now(UTC) - timedelta(days=2),
            registrar="Test",
            statuses=(),
        )

    domains = [f"d{i}.com" for i in range(12)]
    pipeline = FilterPipeline(
        rdap_fetcher=slow_fetcher,
        dns_checker=lambda kept: {},
        rdap_concurrency=4,
        require_dns=False,
    )
    decisions = pipeline.evaluate(domains)

    assert [d.domain for d in decisions] == domains
    assert all(d.candidate is not None for d in decisions)
    assert len(seen) == 12
    assert observed_peak["concurrent"] <= 4
    assert observed_peak["concurrent"] >= 2  # actually parallel, not accidental serial
