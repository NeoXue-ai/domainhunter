"""Tests for the dependency-free strict CT discovery loop."""

import asyncio
import logging

from domainhunter.ingest.ct_orchestrator import CTIngestRunSummary
from domainhunter.scheduler.ct_discovery import CTDiscoveryDaemon


def _summary(*, source_errors: tuple[str, ...] = ()) -> CTIngestRunSummary:
    return CTIngestRunSummary(
        certificates_seen=3,
        events_added=2,
        probes_run=1,
        candidates_created=1,
        next_cursor='{"nimbus": 9}',
        roots_observed=2,
        strict_rejections=1,
        source_errors=source_errors,
    )


def test_ct_discovery_daemon_runs_the_requested_number_of_rounds() -> None:
    calls = 0

    async def run_once() -> CTIngestRunSummary:
        nonlocal calls
        calls += 1
        return _summary()

    daemon = CTDiscoveryDaemon(run_once=run_once, round_seconds=0)

    asyncio.run(daemon.run(max_rounds=2))

    assert calls == 2
    assert daemon.round == 2
    assert daemon.successful_rounds == 2
    assert daemon.failed_rounds == 0
    assert daemon.last_error is None


def test_ct_discovery_daemon_keeps_running_but_logs_the_full_round_error(caplog) -> None:
    async def run_once() -> CTIngestRunSummary:
        raise RuntimeError("nimbus CT log is unavailable")

    daemon = CTDiscoveryDaemon(run_once=run_once, round_seconds=0)

    with caplog.at_level(logging.ERROR, logger="domainhunter.ct_discovery"):
        asyncio.run(daemon.run(max_rounds=1))

    assert daemon.round == 1
    assert daemon.successful_rounds == 0
    assert daemon.failed_rounds == 1
    assert daemon.last_error == "nimbus CT log is unavailable"
    record = next(record for record in caplog.records if record.message.startswith("discovery.round.failed"))
    assert "nimbus CT log is unavailable" in record.getMessage()
    assert record.exc_info is not None


def test_ct_discovery_daemon_retains_an_earlier_failure_for_bounded_reporting() -> None:
    calls = 0

    async def run_once() -> CTIngestRunSummary:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first CT poll failed")
        return _summary()

    daemon = CTDiscoveryDaemon(run_once=run_once, round_seconds=0)

    asyncio.run(daemon.run(max_rounds=2))

    assert daemon.successful_rounds == 1
    assert daemon.failed_rounds == 1
    assert daemon.last_error == "first CT poll failed"


def test_ct_discovery_daemon_retains_partial_source_failures() -> None:
    async def run_once() -> CTIngestRunSummary:
        return _summary(source_errors=("failing log timed out",))

    daemon = CTDiscoveryDaemon(run_once=run_once, round_seconds=0)

    asyncio.run(daemon.run(max_rounds=1))

    assert daemon.successful_rounds == 1
    assert daemon.failed_rounds == 0
    assert daemon.last_source_errors == ("failing log timed out",)
