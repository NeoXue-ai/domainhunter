"""Cursor-based GitHub homepage ingestion without API client coupling."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from webradar_v2.ingest.github_events import (
    InvalidGitHubHomepage,
    build_github_homepage_event,
)
from webradar_v2.storage.sqlite import SQLiteStore


@dataclass(frozen=True, slots=True)
class GitHubRepository:
    """The homepage fields needed from one GitHub repository response."""

    repository_id: str
    homepage: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class GitHubPage:
    """A replayable cursor page from a GitHub source adapter."""

    repositories: tuple[GitHubRepository, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class GitHubPollResult:
    """Counts that make a single GitHub polling run observable."""

    next_cursor: str | None
    repositories_seen: int
    invalid_homepages: int
    events_added: int


GitHubPageFetcher = Callable[[str | None], Awaitable[GitHubPage]]


class GitHubPoller:
    """Convert GitHub homepage pages to idempotent events in the local store."""

    source_name = "github"

    def __init__(self, *, store: SQLiteStore, fetch_page: GitHubPageFetcher) -> None:
        self._store = store
        self._fetch_page = fetch_page

    async def poll(self, *, cursor: str | None = None) -> GitHubPollResult:
        """Fetch one cursor page and append its valid public homepages once."""
        requested_cursor = (
            cursor if cursor is not None else self._store.get_source_cursor(self.source_name)
        )
        page = await self._fetch_page(requested_cursor)
        invalid_homepages = 0
        events_added = 0
        for repository in page.repositories:
            try:
                event = build_github_homepage_event(
                    repository_id=repository.repository_id,
                    homepage=repository.homepage,
                    observed_at=repository.observed_at,
                )
            except (InvalidGitHubHomepage, ValueError):
                invalid_homepages += 1
                continue
            if self._store.append_source_event(event, hostname=event.raw_subject):
                events_added += 1
        self._store.set_source_cursor(self.source_name, page.next_cursor)
        return GitHubPollResult(
            next_cursor=page.next_cursor,
            repositories_seen=len(page.repositories),
            invalid_homepages=invalid_homepages,
            events_added=events_added,
        )
