import asyncio
from contextlib import asynccontextmanager

from webradar_v2.crawler.l2_renderer import L2Renderer
from webradar_v2.domain.observations import OutcomeCode


class FakePage:
    url = "https://example.com/app"

    async def goto(self, url: str, **kwargs: object) -> None:
        assert url == "https://example.com/app"

    async def content(self) -> str:
        return "<h1>Example AI</h1><button>Start free trial</button><p>Plans start at $29.</p>"

    async def screenshot(self, *, path: str, full_page: bool) -> None:
        with open(path, "wb") as output:
            output.write(b"fake screenshot")


async def _public_resolver(hostname: str) -> tuple[str, ...]:
    return ("1.1.1.1",)


def test_renders_a_prevalidated_page_into_l2_facts_and_screenshot(tmp_path) -> None:
    @asynccontextmanager
    async def page_factory():
        yield FakePage()

    async def run() -> None:
        renderer = L2Renderer(resolver=_public_resolver, page_factory=page_factory)
        screenshot = tmp_path / "l2.png"

        result = await renderer.render("https://example.com/app", screenshot_path=screenshot)

        assert result.outcome_code is OutcomeCode.SUCCESS
        assert result.final_url == "https://example.com/app"
        assert result.facts is not None
        assert result.facts.headings == ("Example AI",)
        assert screenshot.read_bytes() == b"fake screenshot"

    asyncio.run(run())
