import asyncio
from datetime import UTC, datetime

from webradar_v2.ingest.github_poller import GitHubPage, GitHubPoller, GitHubRepository
from webradar_v2.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def test_polls_homepages_and_counts_invalid_and_duplicate_repositories(tmp_path) -> None:
    page = GitHubPage(
        repositories=(
            GitHubRepository("123", "https://example.com", OBSERVED_AT),
            GitHubRepository("456", "not-a-url", OBSERVED_AT),
        ),
        next_cursor="page-2",
    )

    async def fetch(cursor: str | None) -> GitHubPage:
        return page

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = GitHubPoller(store=store, fetch_page=fetch)

        first = await poller.poll()
        replay = await poller.poll()

        assert first.repositories_seen == 2
        assert first.invalid_homepages == 1
        assert first.events_added == 1
        assert replay.events_added == 0
        assert store.list_domains() == ("example.com",)
        assert store.get_source_cursor("github") == "page-2"

    asyncio.run(run())
