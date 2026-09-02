"""End-to-end tests that drive :class:`HTTPProbe` against a fake route table.

The fake server's :meth:`FakeHTTPServer.as_httpx_handler` exposes the same
route table through ``httpx.MockTransport``, so the probe exercises the full
code path (DNS pin, analysis, redirects, robots, oversized bodies) without
making a real socket connect. The SSRF guard correctly rejects loopback;
mounting through ``MockTransport`` is the canonical way to fake a public
origin in tests.
"""

import asyncio

import httpx

from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.domain.observations import OutcomeCode

from tests.support.fake_http import FakeHTTPServer


PRICING_HTML = (
    "<html><head><title>Acme AI Studio</title>"
    "<meta name='description' content='AI workflow automation platform.'>"
    "</head><body>"
    "<h1>Acme AI Studio</h1>"
    "<p>AI workflow automation platform for marketing teams. "
    + ("Sign up free. Trusted by enterprise teams. " * 20)
    + "</p>"
    "<a href='/pricing'>Pricing</a><a href='/about'>About</a>"
    "</body></html>"
)


def _probe_html(html: str, *, transport: httpx.MockTransport | None = None) -> None:
    """Drive the fake server's HTML body through HTTPProbe."""
    server = FakeHTTPServer()
    server.add_html("/", html)
    transport = transport or httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe("ai-saas.example.com")
        assert result.outcome_code is OutcomeCode.SUCCESS
        assert result.analysis is not None
        assert result.analysis.title == "Acme AI Studio"

    asyncio.run(run())


async def _public_resolver(hostname: str) -> tuple[str, ...]:
    return ("1.1.1.1",)


def test_probe_classifies_rich_product_page_as_success() -> None:
    _probe_html(PRICING_HTML)


def test_probe_classifies_renewal_as_success_on_second_observation() -> None:
    """Renewal scenario — same hostname, same content, two probes in a row."""
    server = FakeHTTPServer()
    server.add_html("/", PRICING_HTML)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            first = await probe.probe("renew.example.com")
            second = await probe.probe("renew.example.com")
        assert first.outcome_code is OutcomeCode.SUCCESS
        assert second.outcome_code is OutcomeCode.SUCCESS

    asyncio.run(run())


def test_probe_handles_wildcard_redirect() -> None:
    """A redirect from a wildcard cert subject to the apex must succeed."""
    server = FakeHTTPServer()
    server.add_redirect("/", "/about", status=302)
    server.add_html("/about", PRICING_HTML)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe("wildcard.example.com")
        assert result.outcome_code is OutcomeCode.SUCCESS
        assert result.final_url == "https://wildcard.example.com/about"

    asyncio.run(run())


def test_probe_serves_idn_label_through_fake_server() -> None:
    server = FakeHTTPServer()
    server.add_html("/", PRICING_HTML)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe("xn--bcher-kva.example.com")
        assert result.outcome_code is OutcomeCode.SUCCESS

    asyncio.run(run())


def test_probe_classifies_huge_body_as_content_insufficient_or_success() -> None:
    server = FakeHTTPServer()
    server.add_huge("/huge", body_size=10_485_760)  # 10 MiB
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
            max_body_bytes=8192,
        ) as probe:
            result = await probe.probe_url("https://huge.example.com/huge")
        # We must not OOM and we must not return more than max_body_bytes of analysis.
        assert result.outcome_code in {
            OutcomeCode.CONTENT_INSUFFICIENT,
            OutcomeCode.SUCCESS,
        }
        assert result.analysis is not None
        assert (result.analysis.text_length or 0) <= 8192

    asyncio.run(run())


def test_probe_respects_robots_disallow_before_fetching_page() -> None:
    server = FakeHTTPServer()
    server.add_text("/robots.txt", "User-agent: *\nDisallow: /\n")
    server.add_html("/", PRICING_HTML)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=True,
        ) as probe:
            result = await probe.probe("robots.example.com")
        assert result.outcome_code is OutcomeCode.ROBOTS_DISALLOWED
        assert any(hit == "/robots.txt" for hit in server.hits)
        assert "/" not in server.hits

    asyncio.run(run())


def test_probe_blocks_redirect_to_metadata_ip() -> None:
    server = FakeHTTPServer()
    server.add_metadata_redirect("/metadata")
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe_url("https://metadata.example.com/metadata")
        assert result.outcome_code is OutcomeCode.BLOCKED_SSRF

    asyncio.run(run())


def test_probe_classifies_5xx_as_http_5xx() -> None:
    server = FakeHTTPServer()
    server.add_html("/oops", "<p>boom</p>", status=503)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe_url("https://err.example.com/oops")
        assert result.outcome_code is OutcomeCode.HTTP_5XX

    asyncio.run(run())


def test_probe_detects_redirect_loop() -> None:
    server = FakeHTTPServer()
    server.add_redirect("/loop", "/loop", status=302)
    transport = httpx.MockTransport(server.as_httpx_handler())

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
            max_redirects=3,
        ) as probe:
            result = await probe.probe_url("https://loop.example.com/loop")
        assert result.outcome_code is OutcomeCode.REDIRECT_LOOP

    asyncio.run(run())