"""Bounded same-host subpage probing selected from L1 link facts."""

from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Protocol
from urllib.parse import urlparse

from webradar_v2.crawler.http_probe import ProbeResult
from webradar_v2.crawler.l1_analysis import L1Analysis


_PATH_PRIORITIES = ("pricing", "features", "about", "contact")


class URLProbe(Protocol):
    async def probe_url(self, url: str) -> ProbeResult: ...


@dataclass(frozen=True, slots=True)
class L3Result:
    """The bounded list of selected URLs and their typed probe results."""

    attempted_urls: tuple[str, ...]
    results: tuple[ProbeResult, ...]


def select_l3_urls(analysis: L1Analysis, *, max_pages: int) -> tuple[str, ...]:
    """Select only L1-discovered same-host links, prioritizing product pages."""
    if max_pages < 1:
        raise ValueError("max_pages must be positive")
    base_host = urlparse(analysis.final_url).hostname
    candidates: list[tuple[int, int, str]] = []
    for index, url in enumerate(analysis.internal_links):
        parsed = urlparse(url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.hostname.lower() != (base_host or "").lower()
        ):
            continue
        path = f"{parsed.path}?{parsed.query}".lower()
        priority = next(
            (rank for rank, keyword in enumerate(_PATH_PRIORITIES) if keyword in path),
            len(_PATH_PRIORITIES),
        )
        candidates.append((priority, index, url))
    candidates.sort(key=lambda candidate: candidate[:2])
    return tuple(url for _, _, url in candidates[:max_pages])


class L3Crawler:
    """Probe a small, deadline-bound set of L1-discovered product subpages."""

    def __init__(
        self,
        *,
        probe: URLProbe,
        max_pages: int = 4,
        max_total_seconds: float = 20.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        if max_total_seconds <= 0:
            raise ValueError("max_total_seconds must be positive")
        self._probe = probe
        self._max_pages = max_pages
        self._max_total_seconds = max_total_seconds
        self._clock = clock

    async def crawl(self, analysis: L1Analysis) -> L3Result:
        """Run selected L3 probes until the page limit or total deadline is reached."""
        started = self._clock()
        attempted: list[str] = []
        results: list[ProbeResult] = []
        for url in select_l3_urls(analysis, max_pages=self._max_pages):
            if self._clock() - started >= self._max_total_seconds:
                break
            attempted.append(url)
            results.append(await self._probe.probe_url(url))
        return L3Result(attempted_urls=tuple(attempted), results=tuple(results))
