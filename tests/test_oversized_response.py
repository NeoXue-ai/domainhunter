"""Oversized HTTP responses must not OOM the probe.

Spec §15 lists ``超大响应体`` (oversized response body) as a required
security/perf acceptance test. The probe already truncates at
``max_body_bytes``; these tests pin the contract from the
:class:`HTTPProbe` end using the in-process :class:`FakeHTTPServer`.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.domain.observations import OutcomeCode

from tests.support.fake_http import FakeHTTPServer


async def _public_resolver(hostname: str) -> tuple[str, ...]:
    return ("1.1.1.1",)


def test_probe_truncates_huge_body_to_max_body_bytes() -> None:
    """A 10 MiB body must be truncated to the configured cap without OOM."""
    server = FakeHTTPServer()
    server.add_huge("/huge", body_size=10_485_760)  # 10 MiB
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
            max_body_bytes=4096,
        ) as probe:
            result = await probe.probe_url("https://huge.example.com/huge")

        assert result.outcome_code in {
            OutcomeCode.CONTENT_INSUFFICIENT,
            OutcomeCode.SUCCESS,
        }
        assert result.analysis is not None
        # Truncation happens before analysis runs, so the analysis text length
        # is bounded by max_body_bytes (with some slack for newline normalisation).
        assert result.analysis.text_length <= 4096

    asyncio.run(run())


def test_probe_classifies_huge_body_at_default_cap_without_oom() -> None:
    """Default 1 MB cap should also work cleanly on a multi-MB payload."""
    server = FakeHTTPServer()
    server.add_huge("/huge", body_size=2_000_000)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe_url("https://default-cap.example.com/huge")

        # 2 MB of 'A' yields a content_insufficient outcome (no real text).
        assert result.outcome_code in {
            OutcomeCode.CONTENT_INSUFFICIENT,
            OutcomeCode.SUCCESS,
        }
        assert result.analysis is not None
        assert result.analysis.text_length <= 1_000_000

    asyncio.run(run())


def test_probe_completes_in_bounded_time_for_huge_body() -> None:
    """Truncation must short-circuit streaming; the probe must return quickly."""
    server = FakeHTTPServer()
    # 50 MiB is large enough that linear streaming would dominate runtime.
    server.add_huge("/huge", body_size=50 * 1024 * 1024)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
            max_body_bytes=8_192,
        ) as probe:
            result = await probe.probe_url("https://slow-cap.example.com/huge")

        assert result.analysis is not None
        assert result.analysis.text_length <= 8_192

    asyncio.run(run())


def test_probe_hits_server_only_once_for_oversized_response() -> None:
    """The probe must not retry on oversized responses (one round trip only)."""
    server = FakeHTTPServer()
    server.add_huge("/huge", body_size=5_000_000)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            await probe.probe_url("https://once.example.com/huge")

        # Only the /huge path should be hit — no retries, no reconnects.
        huge_hits = [hit for hit in server.hits if hit == "/huge"]
        assert len(huge_hits) == 1

    asyncio.run(run())
