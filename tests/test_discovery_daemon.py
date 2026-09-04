"""Tests for the long-running discovery daemon."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.llm.provider import MockLLMProvider
from domainhunter.scheduler.discovery import DiscoveryDaemon
from domainhunter.storage.sqlite import SQLiteStore

NOW = datetime(2026, 8, 17, tzinfo=UTC)

EVIDENCE = (
    Evidence(EvidenceType.H1, "AI workflow automation", "https://new.com"),
)


def _llm_draft() -> CandidateVersionDraft:
    return CandidateVersionDraft(
        author_kind="llm",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.9,
        name_suggestion="New AI",
        description_suggestion="AI automation",
        category="automation",
        tags=("ai",),
        pricing_model=None,
        target_audience=None,
        evidence=EVIDENCE,
        model_version="mock-1",
    )


class FakeMonitor:
    """Fake callback monitor: yields fixed domains after start."""

    def __init__(self, callback, *, domains: tuple[str, ...]) -> None:
        self._callback = callback
        self._domains = domains
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True
        for domain in self._domains:
            self._callback(SimpleNamespace(domains=(domain,)))

    async def stop(self) -> None:
        self.stopped = True


def test_daemon_round_collects_enriches_and_marks_seen(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")

    def monitor_factory(callback):
        return FakeMonitor(callback, domains=("fresh.com", "seen.com"))

    daemon = DiscoveryDaemon(
        store=store,
        provider=MockLLMProvider(draft=_llm_draft()),
        monitor_factory=monitor_factory,
        collect_seconds=0.01,
        round_seconds=0.01,
        clock=lambda: NOW,
    )

    result = asyncio.run(daemon._round_once(at_time=NOW))
    assert result["collected"] == 2
    assert result["fresh"] == 2  # first round: everything is fresh
    # Both are now marked seen.
    assert store.is_seen("fresh.com")
    assert store.is_seen("seen.com")

    # Second round: nothing fresh → nothing enriched.
    result2 = asyncio.run(daemon._round_once(at_time=NOW))
    assert result2["fresh"] == 0


def test_daemon_round_skips_previously_seen(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    store.mark_seen(("already.com",), at=NOW, source="prior")

    def monitor_factory(callback):
        return FakeMonitor(callback, domains=("already.com", "brandnew.com"))

    daemon = DiscoveryDaemon(
        store=store,
        provider=MockLLMProvider(draft=_llm_draft()),
        monitor_factory=monitor_factory,
        collect_seconds=0.01,
        round_seconds=0.01,
        clock=lambda: NOW,
    )

    result = asyncio.run(daemon._round_once(at_time=NOW))
    assert result["collected"] == 2
    assert result["fresh"] == 1  # only brandnew.com
    assert store.is_seen("brandnew.com") is True


def test_daemon_run_obeys_max_rounds(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    calls = 0

    def monitor_factory(callback):
        return FakeMonitor(callback, domains=())

    daemon = DiscoveryDaemon(
        store=store,
        provider=MockLLMProvider(draft=_llm_draft()),
        monitor_factory=monitor_factory,
        collect_seconds=0.01,
        round_seconds=0.01,
        clock=lambda: NOW,
    )

    async def run() -> None:
        nonlocal calls
        original = daemon._round_once

        async def counting_round(*, at_time: datetime) -> dict[str, object]:
            nonlocal calls
            calls += 1
            return await original(at_time=at_time)

        daemon._round_once = counting_round  # type: ignore[method-assign]
        await daemon.run(max_rounds=2)

    asyncio.run(run())
    assert calls == 2


def test_daemon_run_stops_on_signal(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    signal = asyncio.Event()

    def monitor_factory(callback):
        return FakeMonitor(callback, domains=())

    daemon = DiscoveryDaemon(
        store=store,
        provider=MockLLMProvider(draft=_llm_draft()),
        monitor_factory=monitor_factory,
        collect_seconds=0.01,
        round_seconds=0.01,
        clock=lambda: NOW,
    )

    async def run() -> None:
        task = asyncio.create_task(daemon.run(signal=signal))
        await asyncio.sleep(0.05)
        signal.set()
        await task

    asyncio.run(run())
    assert daemon.round >= 1


def test_daemon_round_failure_includes_the_cause_and_traceback(tmp_path, caplog) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")

    def monitor_factory(*, callback):
        del callback
        raise RuntimeError("monitor import failed")

    daemon = DiscoveryDaemon(
        store=store,
        provider=MockLLMProvider(draft=_llm_draft()),
        monitor_factory=monitor_factory,
        collect_seconds=0.01,
        round_seconds=0.01,
        clock=lambda: NOW,
    )

    import logging

    with caplog.at_level(logging.ERROR, logger="domainhunter.discovery"):
        asyncio.run(daemon.run(max_rounds=1))

    record = next(record for record in caplog.records if record.message.startswith("discovery.round.failed"))
    assert "monitor import failed" in record.getMessage()
    assert record.exc_info is not None
