"""End-to-end tests for the GitHub fixture suite."""

import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from webradar_v2.ingest.github_poller import (
    GitHubPage,
    GitHubPoller,
    GitHubRepository,
)
from webradar_v2.storage.sqlite import SQLiteStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "github"


def _load(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())


def _make_repositories(payload: dict) -> tuple[GitHubRepository, ...]:
    return tuple(
        GitHubRepository(
            repository_id=r["repository_id"],
            homepage=r["homepage"],
            observed_at=datetime.fromisoformat(r["observed_at"].replace("Z", "+00:00")),
        )
        for r in payload["repositories"]
    )


async def _page_async(repositories, next_cursor) -> GitHubPage:
    return GitHubPage(repositories=repositories, next_cursor=next_cursor)


def test_valid_homepage_fixture_appends_one_event_per_repo(tmp_path) -> None:
    fixture = _load("valid_homepage.json")

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = GitHubPoller(
            store=store,
            fetch_page=lambda cursor: _page_async(_make_repositories(fixture), None),
        )
        result = await poller.poll()

        assert result.repositories_seen == fixture["expected_repositories"]
        assert result.invalid_homepages == fixture["expected_invalid_homepages"]
        assert result.events_added == fixture["expected_events_added"]

    asyncio.run(run())


def test_duplicate_fixture_is_idempotent_across_replays(tmp_path) -> None:
    fixture = _load("duplicate.json")

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = GitHubPoller(
            store=store,
            fetch_page=lambda cursor: _page_async(_make_repositories(fixture), None),
        )

        first = await poller.poll()
        replay = await poller.poll()

        assert first.events_added == fixture["expected_events_added"]
        assert replay.events_added == fixture["expected_replay_added_count"]

    asyncio.run(run())


def test_invalid_homepage_fixture_rejects_unsafe_inputs(tmp_path) -> None:
    fixture = _load("invalid_homepage.json")

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = GitHubPoller(
            store=store,
            fetch_page=lambda cursor: _page_async(_make_repositories(fixture), None),
        )
        result = await poller.poll()

        assert result.invalid_homepages == fixture["expected_invalid_homepages"]
        assert result.events_added == fixture["expected_events_added"]
        assert store.list_domains() == tuple(fixture["expected_domains"])

    asyncio.run(run())


def test_pagination_fixture_walks_pages_until_next_cursor_is_none(tmp_path) -> None:
    fixture = _load("pagination.json")
    pages = fixture["pages"]

    async def fetch(cursor):
        if cursor is None:
            payload = pages[0]
            index = 1
        else:
            index = int(cursor.split("-")[-1])
            payload = pages[index - 1]
        repositories = _make_repositories(payload)
        return await _page_async(repositories, payload["next_cursor"])

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = GitHubPoller(store=store, fetch_page=fetch)
        events_added = 0
        cursor = None
        for _ in range(len(pages) + 1):
            result = await poller.poll(cursor=cursor)
            events_added += result.events_added
            cursor = result.next_cursor
            if cursor is None:
                break

        assert events_added == fixture["expected_events_added"]
        # PSL collapses subdomains — both startup.example.io and example.io map to example.io.
        assert store.list_domains() == ("example.com", "example.io")

    asyncio.run(run())


def test_all_github_fixtures_load_as_valid_json() -> None:
    for path in FIXTURE_DIR.glob("*.json"):
        payload = json.loads(path.read_text())
        assert "repositories" in payload or "pages" in payload