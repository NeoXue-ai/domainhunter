"""Small GitHub repository-search fetcher that feeds the vendor-neutral poller."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from webradar_v2.ingest.github_poller import GitHubPage, GitHubRepository


class GitHubRateLimitError(RuntimeError):
    """Raised when GitHub asks the caller to cool down instead of retrying immediately."""


class GitHubSearchFetcher:
    """Fetch public GitHub repository search pages with explicit pagination."""

    def __init__(
        self,
        *,
        query: str,
        token: str | None = None,
        page_size: int = 100,
        observed_at: Callable[[], datetime] = lambda: datetime.now(UTC),
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not query.strip():
            raise ValueError("query must not be empty")
        if not 1 <= page_size <= 100:
            raise ValueError("page_size must be between 1 and 100")
        self._query = query
        self._page_size = page_size
        self._observed_at = observed_at
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "WebRadar/2.0 (+public-research)",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.AsyncClient(
            base_url="https://api.github.com",
            headers=headers,
            timeout=10.0,
            transport=transport,
        )

    async def __call__(self, cursor: str | None) -> GitHubPage:
        page_number = 1 if cursor is None else self._parse_cursor(cursor)
        response = await self._client.get(
            "/search/repositories",
            params={
                "q": self._query,
                "page": page_number,
                "per_page": self._page_size,
                "sort": "updated",
                "order": "desc",
            },
        )
        if response.status_code in {403, 429}:
            raise GitHubRateLimitError(
                f"GitHub rate limit response: {response.status_code}"
            )
        response.raise_for_status()
        payload: Any = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise ValueError("GitHub response must contain an items array")
        repositories = tuple(
            GitHubRepository(
                repository_id=str(item["id"]),
                homepage=item.get("homepage") or "",
                observed_at=self._observed_at(),
            )
            for item in payload["items"]
            if isinstance(item, dict) and "id" in item
        )
        total_count = payload.get("total_count", 0)
        next_cursor = (
            str(page_number + 1)
            if isinstance(total_count, int) and page_number * self._page_size < total_count
            else None
        )
        return GitHubPage(repositories=repositories, next_cursor=next_cursor)

    @staticmethod
    def _parse_cursor(cursor: str) -> int:
        try:
            page_number = int(cursor)
        except ValueError as error:
            raise ValueError("GitHub cursor must be a positive page number") from error
        if page_number < 1:
            raise ValueError("GitHub cursor must be a positive page number")
        return page_number

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "GitHubSearchFetcher":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
