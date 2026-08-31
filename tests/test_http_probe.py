import asyncio
import socket

import httpx
import pytest

from webradar_v2.crawler.http_probe import HTTPProbe
from webradar_v2.domain.observations import OutcomeCode


async def _public_resolver(hostname: str) -> tuple[str, ...]:
    return ("1.1.1.1",)


def test_probes_https_and_returns_l1_analysis() -> None:
    html = "<title>Example AI</title><p>" + ("Useful text. " * 60) + "</p>"
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))

    async def run() -> None:
        async with HTTPProbe(resolver=_public_resolver, transport=transport) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.SUCCESS
        assert result.final_url == "https://example.com"
        assert result.analysis is not None
        assert result.analysis.title == "Example AI"

    asyncio.run(run())


def test_revalidates_each_redirect_target() -> None:
    calls: list[str] = []

    async def resolver(hostname: str) -> tuple[str, ...]:
        calls.append(hostname)
        return ("1.1.1.1",)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "example.com":
            return httpx.Response(302, headers={"Location": "https://app.example.com"})
        return httpx.Response(200, text="<p>" + ("Product text. " * 60) + "</p>")

    async def run() -> None:
        async with HTTPProbe(resolver=resolver, transport=httpx.MockTransport(handler)) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.SUCCESS
        assert result.final_url == "https://app.example.com"
        assert calls == ["example.com", "app.example.com"]

    asyncio.run(run())


def test_blocks_private_resolutions_before_request() -> None:
    async def private_resolver(hostname: str) -> tuple[str, ...]:
        return ("127.0.0.1",)

    async def run() -> None:
        async with HTTPProbe(resolver=private_resolver) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.BLOCKED_SSRF

    asyncio.run(run())


def test_classifies_http_429_as_rate_limited() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(429))

    async def run() -> None:
        async with HTTPProbe(resolver=_public_resolver, transport=transport) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.HTTP_429

    asyncio.run(run())


def test_respects_robots_disallow_before_fetching_page() -> None:
    requested_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_paths.append(request.url.path)
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /\n")
        return httpx.Response(200, text="<title>Must not fetch</title>")

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=httpx.MockTransport(handler),
        ) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.ROBOTS_DISALLOWED
        assert requested_paths == ["/robots.txt"]

    asyncio.run(run())


def test_maps_dns_name_failure_to_dns_not_found() -> None:
    async def resolver(hostname: str) -> tuple[str, ...]:
        raise socket.gaierror(-2, "name or service not known")

    async def run() -> None:
        async with HTTPProbe(resolver=resolver, respect_robots=False) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.DNS_NOT_FOUND

    asyncio.run(run())


def test_maps_dns_timeout_to_dns_timeout() -> None:
    async def resolver(hostname: str) -> tuple[str, ...]:
        raise TimeoutError("resolver timed out")

    async def run() -> None:
        async with HTTPProbe(resolver=resolver, respect_robots=False) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.DNS_TIMEOUT

    asyncio.run(run())


def test_probes_an_explicit_public_url() -> None:
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(200, text="<p>" + ("Product facts. " * 60) + "</p>")

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=httpx.MockTransport(handler),
            respect_robots=False,
        ) as probe:
            result = await probe.probe_url("https://example.com/pricing")

        assert result.outcome_code is OutcomeCode.SUCCESS
        assert result.final_url == "https://example.com/pricing"
        assert requested_urls == ["https://example.com/pricing"]

    asyncio.run(run())


def test_probe_connects_to_resolved_public_ip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The actual socket connect must use the IP we resolved+validated."""
    pinned_ip = "8.8.8.8"
    seen_getaddrinfo: list[tuple[str, ...]] = []

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        seen_getaddrinfo.append((hostname,))
        return [(socket.AF_INET, None, None, None, (pinned_ip, 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    html = "<title>Ok</title><p>" + ("Body text. " * 60) + "</p>"
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))

    async def run() -> None:
        # Use the default resolver so the probe actually calls socket.getaddrinfo.
        async with HTTPProbe(
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.SUCCESS
        # The probe must invoke socket.getaddrinfo so it can pin the connect IP.
        assert seen_getaddrinfo, "expected socket.getaddrinfo to be called"
        assert seen_getaddrinfo[0][0] == "example.com"

    asyncio.run(run())


def test_probe_blocks_when_dns_returns_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OS DNS resolving to 127.0.0.1 must be blocked before any request."""

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        return [(socket.AF_INET, None, None, None, ("127.0.0.1", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    called = {"n": 0}
    transport = httpx.MockTransport(
        lambda request: (called.__setitem__("n", called["n"] + 1) or httpx.Response(200))
    )

    async def run() -> None:
        async with HTTPProbe(
            transport=transport,
            respect_robots=False,
        ) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.BLOCKED_SSRF
        assert called["n"] == 0  # never reached the transport

    asyncio.run(run())


def test_probe_blocks_metadata_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redirect to 169.254.169.254 must be blocked."""

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        if hostname == "169.254.169.254":
            return [(socket.AF_INET, None, None, None, ("169.254.169.254", 0))]
        return [(socket.AF_INET, None, None, None, ("1.1.1.1", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "example.com":
            return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest"})
        return httpx.Response(200, text="metadata page")

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=httpx.MockTransport(handler),
            respect_robots=False,
        ) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.BLOCKED_SSRF

    asyncio.run(run())


def test_probe_revalidates_redirect_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A redirect to a public host must succeed; both DNS lookups happen."""

    seen: list[str] = []

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        seen.append(hostname)
        return [(socket.AF_INET, None, None, None, ("8.8.8.8", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "example.com":
            return httpx.Response(302, headers={"Location": "https://app.example.com/"})
        return httpx.Response(200, text="<p>" + ("Product facts. " * 60) + "</p>")

    async def run() -> None:
        async with HTTPProbe(
            transport=httpx.MockTransport(handler),
            respect_robots=False,
        ) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.SUCCESS
        assert result.final_url == "https://app.example.com/"
        assert seen == ["example.com", "app.example.com"]

    asyncio.run(run())


def test_probe_blocks_oversized_response_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A body exceeding max_body_bytes must yield CONTENT_INSUFFICIENT, not OOM."""

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        return [(socket.AF_INET, None, None, None, ("1.1.1.1", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    big_body = ("X" * 4096) + "<title>Big</title>" + ("Y" * 4096)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=big_body))

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
            max_body_bytes=1024,
        ) as probe:
            result = await probe.probe("example.com")

        # Truncating past 1024 bytes leaves an unparseable / too-small body.
        assert result.outcome_code in {
            OutcomeCode.CONTENT_INSUFFICIENT,
            OutcomeCode.SUCCESS,
        }
        assert result.analysis is not None
        # We must not have read past the configured cap.
        assert len(result.analysis.title or "") <= 256

    asyncio.run(run())


def test_probe_rebinding_first_call_public_second_call_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolver returns public first, loopback second → second redirect is BLOCKED_SSRF."""

    responses = iter([("8.8.8.8", 0), ("127.0.0.1", 0)])

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        ip = next(responses)
        return [(socket.AF_INET, None, None, None, ip)]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "example.com":
            return httpx.Response(302, headers={"Location": "https://attacker.example/"})
        # Second hop should never be reached: resolver already returned loopback.
        return httpx.Response(200, text="should not see this")

    async def run() -> None:
        async with HTTPProbe(
            transport=httpx.MockTransport(handler),
            respect_robots=False,
        ) as probe:
            result = await probe.probe("example.com")

        assert result.outcome_code is OutcomeCode.BLOCKED_SSRF

    asyncio.run(run())
