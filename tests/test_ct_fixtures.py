"""End-to-end tests for the CT fixture suite."""

import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from webradar_v2.ingest.ct_events import build_ct_events, extract_certificate_hostnames
from webradar_v2.ingest.ct_poller import CTCertificate, CTPage, CTPoller
from webradar_v2.storage.sqlite import SQLiteStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ct"


def _load(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())


def _make_entries(payload: dict) -> tuple[CTCertificate, ...]:
    return tuple(
        CTCertificate(
            source_event_id=entry["source_event_id"],
            certificate=entry["certificate"],
            observed_at=datetime.fromisoformat(entry["observed_at"].replace("Z", "+00:00")),
        )
        for entry in payload["entries"]
    )


async def _page_async(entries: tuple[CTCertificate, ...], next_cursor: str | None) -> CTPage:
    return CTPage(entries=entries, next_cursor=next_cursor)


def test_renewal_fixture_appends_once_and_replay_is_noop(tmp_path) -> None:
    fixture = _load("renewal.json")

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = CTPoller(store=store, fetch_page=lambda cursor: _page_async(_make_entries(fixture), None))

        first = await poller.poll()
        replay = await poller.poll()

        assert first.events_added == fixture["expected_events_count"]
        assert replay.events_added == fixture["expected_replay_added_count"]

    asyncio.run(run())


def test_wildcard_fixture_strips_prefix_and_expands_san() -> None:
    fixture = _load("wildcard.json")
    certificate = fixture["entries"][0]["certificate"]

    hostnames = extract_certificate_hostnames(certificate)

    assert list(hostnames) == fixture["expected_hostnames"]
    assert all("*" not in hostname for hostname in hostnames)


def test_wildcard_fixture_appends_one_event_per_hostname(tmp_path) -> None:
    fixture = _load("wildcard.json")

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = CTPoller(store=store, fetch_page=lambda cursor: _page_async(_make_entries(fixture), None))
        result = await poller.poll()

        assert result.events_added == fixture["expected_events_count"]
        assert store.list_domains() == ("example.com",)

    asyncio.run(run())


def test_duplicate_fixture_is_idempotent_across_replays(tmp_path) -> None:
    fixture = _load("duplicate.json")

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        poller = CTPoller(store=store, fetch_page=lambda cursor: _page_async(_make_entries(fixture), None))

        first = await poller.poll()
        replay = await poller.poll()

        assert first.events_added == fixture["expected_events_count"]
        assert replay.events_added == fixture["expected_replay_added_count"]

    asyncio.run(run())


def test_idn_fixture_normalizes_unicode_to_punycode() -> None:
    fixture = _load("idn.json")
    hostnames: set[str] = set()

    for entry in fixture["entries"]:
        hostnames.update(extract_certificate_hostnames(entry["certificate"]))

    assert hostnames == set(fixture["expected_hostnames"])
    assert all(h.startswith("xn--") for h in hostnames)


def test_malformed_fixture_skips_bad_entries_without_crashing(tmp_path) -> None:
    fixture = _load("malformed.json")

    async def run() -> None:
        store = SQLiteStore(tmp_path / "webradar.db")
        events_seen = 0
        for entry in fixture["entries"]:
            if not entry["source_event_id"]:
                with pytest.raises(ValueError):
                    build_ct_events(
                        entry["certificate"],
                        entry["source_event_id"],
                        datetime.fromisoformat(entry["observed_at"].replace("Z", "+00:00")),
                    )
                continue
            try:
                events = build_ct_events(
                    entry["certificate"],
                    entry["source_event_id"],
                    datetime.fromisoformat(entry["observed_at"].replace("Z", "+00:00")),
                )
            except ValueError:
                continue
            events_seen += len(events)
            for event in events:
                store.append_source_event(event, hostname=event.raw_subject)

        # valid.example.com normalizes to the registrable domain example.com.
        assert store.list_domains() == ("example.com",)
        assert events_seen == fixture["expected_events_count"]

    asyncio.run(run())


def test_all_ct_fixtures_load_as_valid_json() -> None:
    for path in FIXTURE_DIR.glob("*.json"):
        payload = json.loads(path.read_text())
        assert "entries" in payload
        for entry in payload["entries"]:
            assert "source_event_id" in entry
            assert "certificate" in entry