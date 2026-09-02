"""Bounded HTTP probing with manual redirect and address validation."""

import asyncio
import socket
from dataclasses import dataclass
from time import monotonic
from typing import Awaitable, Callable
from urllib.parse import urljoin, urlparse

import httpx

from domainhunter.crawler.l1_analysis import L1Analysis, analyze_http_document
from domainhunter.crawler.pinned_transport import PinnedTransport
from domainhunter.domain.normalization import InvalidHostname, normalize_hostname
from domainhunter.domain.observations import OutcomeCode
from domainhunter.network_safety import (
    BlockedNetworkTarget,
    resolve_public_addresses,
    validate_public_addresses,
)


Resolver = Callable[[str], Awaitable[tuple[str, ...]]]


async def _default_resolver(hostname: str) -> tuple[str, ...]:
    return await asyncio.to_thread(resolve_public_addresses, hostname)


# Backwards-compatible alias for callers (e.g. l2_renderer) that imported the
# legacy helper before pinned-transport support landed.
_resolve_public_addresses = _default_resolver


class HostRateLimiter:
    """Per-hostname async token-bucket limiter.

    A new bucket is allocated lazily on the first ``acquire`` call for each
    hostname. Setting ``requests_per_second_per_host`` to ``0`` (or negative)
    disables the limiter entirely, preserving the original unbounded behavior.
    """

    def __init__(self, requests_per_second_per_host: float = 1.0) -> None:
        self._rate = float(requests_per_second_per_host)
        self._min_interval = 0.0 if self._rate <= 0 else 1.0 / self._rate
        self._buckets: dict[str, tuple[asyncio.Lock, float]] = {}
        self._buckets_lock = asyncio.Lock()

    async def acquire(self, hostname: str) -> None:
        """Block until one token is available for ``hostname``."""
        if self._min_interval <= 0:
            return
        async with self._buckets_lock:
            entry = self._buckets.get(hostname)
            if entry is None:
                entry = (asyncio.Lock(), 0.0)
                self._buckets[hostname] = entry
        lock, last_call = entry
        async with lock:
            now = monotonic()
            wait = self._min_interval - (now - last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._buckets[hostname] = (lock, monotonic())


def _robots_disallows_root(robots_text: str, user_agent: str) -> bool:
    """Return whether the matching robots group disallows the site's root path."""
    groups: dict[str, list[str]] = {}
    active_agents: list[str] = []
    has_directive = False

    for raw_line in robots_text.splitlines():
        line = raw_line.split("#", maxsplit=1)[0].strip()
        if not line or ":" not in line:
            continue
        field, value = (part.strip() for part in line.split(":", maxsplit=1))
        field = field.lower()
        if field == "user-agent":
            if has_directive:
                active_agents = []
                has_directive = False
            if value:
                active_agents.append(value.lower())
            continue
        if field == "disallow" and active_agents:
            has_directive = True
            for agent in active_agents:
                groups.setdefault(agent, []).append(value)

    agent_name = user_agent.split("/", maxsplit=1)[0].split(maxsplit=1)[0].lower()
    rules = groups.get(agent_name, groups.get("*", []))
    return "/" in rules


@dataclass(frozen=True, slots=True)
class ProbeResult:
    outcome_code: OutcomeCode
    final_url: str | None = None
    analysis: L1Analysis | None = None
    detail: str | None = None
    response_time_ms: int | None = None


class HTTPProbe:
    """Probe a public host with bounded response and redirect behavior.

    Every TCP connect is pinned to an IP that has already passed
    :func:`domainhunter.network_safety.resolve_public_addresses`, closing the
    DNS rebinding window between pre-validation and ``socket.connect``.
    """

    def __init__(
        self,
        *,
        resolver: Resolver = _default_resolver,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 5.0,
        max_redirects: int = 5,
        max_body_bytes: int = 1_000_000,
        user_agent: str = "DomainHunter/2.0 (+public-research)",
        respect_robots: bool = True,
        host_rate_per_second: float | None = None,
        host_rate_limiter: HostRateLimiter | None = None,
    ) -> None:
        self._resolver = resolver
        self._max_redirects = max_redirects
        self._max_body_bytes = max_body_bytes
        self._respect_robots = respect_robots
        self._user_agent = user_agent
        if host_rate_limiter is not None:
            self._rate_limiter = host_rate_limiter
        elif host_rate_per_second is None:
            self._rate_limiter = HostRateLimiter(requests_per_second_per_host=0.0)
        else:
            self._rate_limiter = HostRateLimiter(
                requests_per_second_per_host=host_rate_per_second
            )
        self._client = httpx.AsyncClient(
            follow_redirects=False,
            timeout=timeout,
            headers={"User-Agent": user_agent},
            transport=transport,
        )

    async def probe(self, hostname: str) -> ProbeResult:
        """Probe the HTTPS root for one public hostname."""
        normalized = normalize_hostname(hostname)
        return await self.probe_url(f"https://{normalized.hostname}")

    async def probe_url(self, url: str) -> ProbeResult:
        """Probe one public HTTP(S) URL with the same bounds as root probing."""
        initial = urlparse(url)
        if initial.scheme not in {"http", "https"} or not initial.hostname:
            raise ValueError("url must be an HTTP(S) URL with a hostname")
        normalize_hostname(initial.hostname)
        current_url = url
        visited: set[str] = set()
        robots_checked: set[tuple[str, str, int | None]] = set()
        pinned_map: dict[str, str] = {}
        started = monotonic()

        for _ in range(self._max_redirects + 1):
            parsed = urlparse(current_url)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                return self._result(OutcomeCode.REDIRECT_LOOP, current_url, started, "invalid redirect")

            try:
                redirect_host = normalize_hostname(parsed.hostname).hostname
            except InvalidHostname as error:
                # IP-literal redirects (e.g. 169.254.169.254) cannot be normalized
                # as hostnames; treat them as SSRF blocks.
                return self._result(OutcomeCode.BLOCKED_SSRF, current_url, started, str(error))

            await self._rate_limiter.acquire(redirect_host)

            try:
                addresses = await self._resolver(redirect_host)
            except BlockedNetworkTarget as error:
                return self._result(OutcomeCode.BLOCKED_SSRF, current_url, started, str(error))
            except socket.gaierror as error:
                return self._result(OutcomeCode.DNS_NOT_FOUND, current_url, started, str(error))
            except TimeoutError as error:
                return self._result(OutcomeCode.DNS_TIMEOUT, current_url, started, str(error))

            # Defense-in-depth: even when a caller-supplied resolver returns
            # addresses directly, refuse to pin anything that is not globally
            # routable.
            try:
                validate_public_addresses(addresses)
            except BlockedNetworkTarget as error:
                return self._result(OutcomeCode.BLOCKED_SSRF, current_url, started, str(error))

            pinned_ip = pinned_map.setdefault(redirect_host, addresses[0])

            if current_url in visited:
                return self._result(OutcomeCode.REDIRECT_LOOP, current_url, started, "redirect loop")
            visited.add(current_url)

            robots_key = (parsed.scheme, redirect_host, parsed.port)
            if self._respect_robots and robots_key not in robots_checked:
                robots_checked.add(robots_key)
                if await self._robots_disallows(robots_key, pinned_map):
                    return self._result(
                        OutcomeCode.ROBOTS_DISALLOWED,
                        current_url,
                        started,
                        "robots.txt disallows the root path",
                    )

            pin = PinnedTransport({redirect_host: pinned_ip})
            try:
                with pin.patch():
                    async with self._client.stream("GET", current_url) as response:
                        if response.status_code in {301, 302, 303, 307, 308}:
                            location = response.headers.get("location")
                            if not location:
                                analysis = analyze_http_document(
                                    status_code=response.status_code,
                                    final_url=current_url,
                                    html="",
                                )
                                return self._result(
                                    analysis.outcome_code, current_url, started, analysis=analysis
                                )
                            current_url = urljoin(current_url, location)
                            continue

                        body = bytearray()
                        async for chunk in response.aiter_bytes():
                            body.extend(chunk)
                            if len(body) >= self._max_body_bytes:
                                del body[self._max_body_bytes :]
                                break
                        analysis = analyze_http_document(
                            status_code=response.status_code,
                            final_url=current_url,
                            html=bytes(body).decode("utf-8", errors="replace"),
                        )
                        return self._result(
                            analysis.outcome_code, current_url, started, analysis=analysis
                        )
            except httpx.TimeoutException as error:
                return self._result(OutcomeCode.CONNECT_TIMEOUT, current_url, started, str(error))
            except httpx.TransportError as error:
                return self._result(OutcomeCode.CONNECT_TIMEOUT, current_url, started, str(error))

        return self._result(OutcomeCode.REDIRECT_LOOP, current_url, started, "redirect limit")

    async def _robots_disallows(
        self,
        robots_key: tuple[str, str, int | None],
        pinned_map: dict[str, str],
    ) -> bool:
        scheme, hostname, port = robots_key
        authority = hostname if port is None else f"{hostname}:{port}"
        robots_url = f"{scheme}://{authority}/robots.txt"
        pin = PinnedTransport({hostname: pinned_map[hostname]})
        try:
            with pin.patch():
                async with self._client.stream("GET", robots_url) as response:
                    if response.status_code != 200:
                        return False
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) >= self._max_body_bytes:
                            del body[self._max_body_bytes :]
                            break
        except httpx.HTTPError:
            return False
        return _robots_disallows_root(bytes(body).decode("utf-8", errors="replace"), self._user_agent)

    def _result(
        self,
        outcome_code: OutcomeCode,
        final_url: str,
        started: float,
        detail: str | None = None,
        analysis: L1Analysis | None = None,
    ) -> ProbeResult:
        return ProbeResult(
            outcome_code=outcome_code,
            final_url=final_url,
            analysis=analysis,
            detail=detail,
            response_time_ms=int((monotonic() - started) * 1000),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "HTTPProbe":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
