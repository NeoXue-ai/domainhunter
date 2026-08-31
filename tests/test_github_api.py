import asyncio
from datetime import UTC, datetime

import httpx

from webradar_v2.ingest.github_api import GitHubSearchFetcher


def test_fetches_a_github_search_page_with_cursor_and_optional_token() -> None:
    observed_at = datetime(2026, 8, 16, tzinfo=UTC)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "total_count": 101,
                "items": [
                    {
                        "id": 123,
                        "homepage": "https://app.example.com",
                    }
                ],
            },
        )

    async def run() -> None:
        async with GitHubSearchFetcher(
            query="topic:artificial-intelligence",
            token="secret-token",
            page_size=1,
            observed_at=lambda: observed_at,
            transport=httpx.MockTransport(handler),
        ) as fetcher:
            page = await fetcher(None)

        assert page.next_cursor == "2"
        assert page.repositories[0].repository_id == "123"
        assert page.repositories[0].homepage == "https://app.example.com"
        assert page.repositories[0].observed_at == observed_at
        assert requests[0].url.params["q"] == "topic:artificial-intelligence"
        assert requests[0].url.params["page"] == "1"
        assert requests[0].headers["authorization"] == "Bearer secret-token"

    asyncio.run(run())
