"""Cursor-based Certificate Transparency ingestion without vendor coupling."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from domainhunter.ingest.ct_events import build_ct_events
from domainhunter.storage.sqlite import SQLiteStore


@dataclass(frozen=True, slots=True)
class CTCertificate:
    """One certificate update returned by a CT source adapter."""

    source_event_id: str
    certificate: Mapping[str, Any]
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class CTPage:
    """A replayable cursor page from a CT source adapter."""

    entries: tuple[CTCertificate, ...]
    next_cursor: str | None
    source_errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CTPollResult:
    """Counts that make a single CT polling run observable."""

    next_cursor: str | None
    certificates_seen: int
    events_seen: int
    events_added: int
    source_errors: tuple[str, ...] = ()


CTPageFetcher = Callable[[str | None], Awaitable[CTPage]]


class CTPoller:
    """Convert CT pages to idempotent events in the local store."""

    source_name = "ct_log"

    def __init__(self, *, store: SQLiteStore, fetch_page: CTPageFetcher) -> None:
        self._store = store
        self._fetch_page = fetch_page

    async def poll(self, *, cursor: str | None = None) -> CTPollResult:
        """Fetch one cursor page and append every valid hostname event exactly once."""
        requested_cursor = (
            cursor if cursor is not None else self._store.get_source_cursor(self.source_name)
        )
        page = await self._fetch_page(requested_cursor)
        events_seen = 0
        events_added = 0
        for entry in page.entries:
            events = build_ct_events(
                entry.certificate,
                entry.source_event_id,
                entry.observed_at,
            )
            events_seen += len(events)
            for event in events:
                if self._store.append_source_event(event, hostname=event.raw_subject):
                    events_added += 1
        self._store.set_source_cursor(self.source_name, page.next_cursor)
        return CTPollResult(
            next_cursor=page.next_cursor,
            certificates_seen=len(page.entries),
            events_seen=events_seen,
            events_added=events_added,
            source_errors=page.source_errors,
        )
