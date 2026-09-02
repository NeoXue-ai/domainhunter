"""Tests for the CT log lag monitor."""

import asyncio
from types import SimpleNamespace

from domainhunter.scheduler.lag_monitor import LogLagMonitor


class FakeClient:
    def __init__(self, url: str, name: str, tree_size: int, fail: bool = False):
        self.log_meta = SimpleNamespace(url=url, name=name)
        self._tree_size = tree_size
        self._fail = fail

    async def fetch_tree_size(self) -> int:
        if self._fail:
            raise OSError("boom")
        return self._tree_size


def _state(mapping: dict[str, int]):
    return lambda: dict(mapping)


def test_alert_when_log_is_behind_threshold() -> None:
    async def run() -> None:
        monitor = LogLagMonitor(
            clients=[FakeClient("https://a.example/", "Alpha", tree_size=200_000)],
            state=_state({"https://a.example/": 100_000}),
            threshold=50_000,
        )
        alerts = await monitor.check_once()
        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.log_url == "https://a.example/"
        assert alert.lag_entries == 99_999  # tree - consumed - 1
        assert alert.tree_size == 200_000

    asyncio.run(run())


def test_no_alert_within_threshold() -> None:
    async def run() -> None:
        monitor = LogLagMonitor(
            clients=[FakeClient("https://a.example/", "Alpha", tree_size=100_000)],
            state=_state({"https://a.example/": 99_900}),
            threshold=50_000,
        )
        assert await monitor.check_once() == ()

    asyncio.run(run())


def test_no_alert_when_no_state_yet() -> None:
    async def run() -> None:
        monitor = LogLagMonitor(
            clients=[FakeClient("https://a.example/", "Alpha", tree_size=200_000)],
            state=_state({}),
            threshold=50_000,
        )
        assert await monitor.check_once() == ()

    asyncio.run(run())


def test_failed_tree_size_fetch_is_skipped() -> None:
    async def run() -> None:
        monitor = LogLagMonitor(
            clients=[
                FakeClient("https://bad.example/", "Bad", tree_size=200_000, fail=True),
                FakeClient("https://ok.example/", "Ok", tree_size=100_000),
            ],
            state=_state({"https://bad.example/": 10, "https://ok.example/": 99_900}),
            threshold=50_000,
        )
        assert await monitor.check_once() == ()

    asyncio.run(run())


def test_lag_zero_when_fully_caught_up() -> None:
    async def run() -> None:
        monitor = LogLagMonitor(
            clients=[FakeClient("https://a.example/", "Alpha", tree_size=100_000)],
            state=_state({"https://a.example/": 99_999}),
            threshold=50_000,
        )
        alerts = await monitor.check_once()
        assert alerts == ()

    asyncio.run(run())


def test_start_stop_background_loop() -> None:
    async def run() -> None:
        monitor = LogLagMonitor(
            clients=[FakeClient("https://a.example/", "Alpha", tree_size=200_000)],
            state=_state({"https://a.example/": 100_000}),
            threshold=50_000,
            interval=0.01,
        )
        monitor.start()
        await asyncio.sleep(0.05)
        await monitor.stop()
        assert monitor.last_alerts, "expected at least one alert run"

    asyncio.run(run())