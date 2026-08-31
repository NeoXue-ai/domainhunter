"""Bounded Playwright rendering after public-address prevalidation.

The browser provides JavaScript-rendered facts for human review. It does not
replace the HTTP probe and must never be used as an unrestricted crawler.
"""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncContextManager, Callable, Protocol
from urllib.parse import urlparse

from webradar_v2.crawler.http_probe import Resolver, _resolve_public_addresses
from webradar_v2.crawler.l2_analysis import L2Facts, extract_rendered_facts
from webradar_v2.domain.normalization import normalize_hostname
from webradar_v2.domain.observations import OutcomeCode
from webradar_v2.network_safety import BlockedNetworkTarget, validate_public_addresses


class RenderPage(Protocol):
    url: str

    async def goto(self, url: str, **kwargs: object) -> object: ...

    async def content(self) -> str: ...

    async def screenshot(self, *, path: str, full_page: bool) -> object: ...


PageFactory = Callable[[], AsyncContextManager[RenderPage]]


@dataclass(frozen=True, slots=True)
class L2RenderResult:
    outcome_code: OutcomeCode
    final_url: str | None = None
    facts: L2Facts | None = None
    screenshot_path: Path | None = None
    detail: str | None = None


@asynccontextmanager
async def _playwright_page_factory() -> RenderPage:
    """Create one short-lived headless Chromium page only when rendering is used."""
    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            yield page
        finally:
            await browser.close()


class L2Renderer:
    """Render one already-scoped public URL into compact, citeable L2 facts."""

    def __init__(
        self,
        *,
        resolver: Resolver = _resolve_public_addresses,
        page_factory: PageFactory = _playwright_page_factory,
        timeout_seconds: float = 15.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._resolver = resolver
        self._page_factory = page_factory
        self._timeout_seconds = timeout_seconds

    async def render(
        self, url: str, *, screenshot_path: Path | None = None
    ) -> L2RenderResult:
        """Prevalidate a public HTTP(S) target, render it, and extract review facts."""
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("url must be an HTTP(S) URL with a hostname")

        try:
            hostname = normalize_hostname(parsed.hostname).hostname
            addresses = await self._resolver(hostname)
            validate_public_addresses(addresses)
        except BlockedNetworkTarget as error:
            return L2RenderResult(OutcomeCode.BLOCKED_SSRF, final_url=url, detail=str(error))
        except TimeoutError as error:
            return L2RenderResult(OutcomeCode.DNS_TIMEOUT, final_url=url, detail=str(error))
        except OSError as error:
            return L2RenderResult(OutcomeCode.DNS_NOT_FOUND, final_url=url, detail=str(error))

        try:
            async with self._page_factory() as page:
                await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=int(self._timeout_seconds * 1000),
                )
                html = await page.content()
                if screenshot_path is not None:
                    await page.screenshot(path=str(screenshot_path), full_page=True)
                return L2RenderResult(
                    OutcomeCode.SUCCESS,
                    final_url=page.url,
                    facts=extract_rendered_facts(html),
                    screenshot_path=screenshot_path,
                )
        except asyncio.TimeoutError as error:
            return L2RenderResult(OutcomeCode.RENDER_TIMEOUT, final_url=url, detail=str(error))
        except Exception as error:
            if error.__class__.__name__ == "TimeoutError":
                return L2RenderResult(OutcomeCode.RENDER_TIMEOUT, final_url=url, detail=str(error))
            return L2RenderResult(OutcomeCode.CONNECT_TIMEOUT, final_url=url, detail=str(error))
