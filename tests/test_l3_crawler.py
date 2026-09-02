import asyncio

from domainhunter.crawler.http_probe import ProbeResult
from domainhunter.crawler.l1_analysis import L1Analysis
from domainhunter.crawler.l3_crawler import L3Crawler, select_l3_urls
from domainhunter.domain.observations import OutcomeCode


ANALYSIS = L1Analysis(
    outcome_code=OutcomeCode.SUCCESS,
    final_url="https://example.com",
    title="Example",
    meta_description=None,
    text_length=600,
    is_parking_page=False,
    status_code=200,
    internal_links=(
        "https://example.com/blog",
        "https://example.com/contact",
        "https://example.com/features",
        "https://example.com/pricing",
        "https://example.com/about",
    ),
)


def test_prioritizes_product_paths_from_l1_same_host_links() -> None:
    assert select_l3_urls(ANALYSIS, max_pages=3) == (
        "https://example.com/pricing",
        "https://example.com/features",
        "https://example.com/about",
    )


def test_probes_only_the_configured_number_of_l3_urls() -> None:
    class FakeProbe:
        def __init__(self) -> None:
            self.urls: list[str] = []

        async def probe_url(self, url: str) -> ProbeResult:
            self.urls.append(url)
            return ProbeResult(OutcomeCode.SUCCESS, final_url=url)

    async def run() -> None:
        probe = FakeProbe()
        crawler = L3Crawler(probe=probe, max_pages=2, max_total_seconds=10)

        result = await crawler.crawl(ANALYSIS)

        assert result.attempted_urls == (
            "https://example.com/pricing",
            "https://example.com/features",
        )
        assert probe.urls == list(result.attempted_urls)
        assert len(result.results) == 2

    asyncio.run(run())
