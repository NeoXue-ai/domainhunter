import asyncio
from datetime import UTC, datetime

from domainhunter.ingest.ct_poller import CTCertificate, CTPage, CTPoller
from domainhunter.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def test_polls_ct_pages_with_cursor_and_idempotent_event_counts(tmp_path) -> None:
    calls: list[str | None] = []
    page = CTPage(
        entries=(
            CTCertificate(
                source_event_id="argon:42",
                certificate={
                    "leaf_cert": {
                        "subject": {"CN": "example.com"},
                        "all_domains": ["app.example.com"],
                    }
                },
                observed_at=OBSERVED_AT,
            ),
        ),
        next_cursor="argon:43",
    )

    async def fetch(cursor: str | None) -> CTPage:
        calls.append(cursor)
        return page

    async def run() -> None:
        store = SQLiteStore(tmp_path / "domainhunter.db")
        poller = CTPoller(store=store, fetch_page=fetch)

        first = await poller.poll()
        replay = await poller.poll()

        assert first.certificates_seen == 1
        assert first.events_added == 2
        assert replay.events_added == 0
        assert first.next_cursor == "argon:43"
        assert calls == [None, "argon:43"]
        assert store.get_source_cursor("ct_log") == "argon:43"
        assert store.list_domains() == ("example.com",)

    asyncio.run(run())
