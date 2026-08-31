"""Configurable CertStream listener that feeds immutable CT source events."""

from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any, AsyncContextManager

from websockets.asyncio.client import connect as websocket_connect

from webradar_v2.ingest.ct_events import build_ct_events
from webradar_v2.storage.sqlite import SQLiteStore


DEFAULT_CERTSTREAM_URL = "wss://certstream.calidog.io/"
CertStreamConnection = Callable[[str], AsyncContextManager[AsyncIterator[str | bytes]]]


def _default_certstream_connect(url: str) -> AsyncContextManager[AsyncIterator[str | bytes]]:
    """Connect to CertStream without inheriting the machine's SOCKS proxy.

    ``websockets`` auto-detects system proxies by default. On developer
    machines this often points at a local SOCKS proxy (for example Clash) and
    fails unless ``python-socks`` is installed. CertStream is a public WSS
    endpoint, so we disable proxy auto-detection unless a caller injects a
    custom ``connect``.
    """
    return websocket_connect(url, proxy=None)


@dataclass(frozen=True, slots=True)
class CertStreamResult:
    """Observable counts for a bounded or long-running CertStream session."""

    messages_seen: int
    certificate_updates: int
    invalid_messages: int
    events_added: int


class CertStreamListener:
    """Listen for certificate updates and append their normalized source events."""

    def __init__(
        self,
        *,
        store: SQLiteStore,
        url: str = DEFAULT_CERTSTREAM_URL,
        connect: CertStreamConnection = _default_certstream_connect,
        observed_at: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not url.strip():
            raise ValueError("CertStream URL must not be empty")
        self._store = store
        self._url = url
        self._connect = connect
        self._observed_at = observed_at

    async def listen(self, *, max_messages: int | None = None) -> CertStreamResult:
        """Read a session, accepting only well-formed certificate-update messages."""
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be positive")
        messages_seen = 0
        certificate_updates = 0
        invalid_messages = 0
        events_added = 0
        async with self._connect(self._url) as messages:
            async for raw_message in messages:
                messages_seen += 1
                if max_messages is not None and messages_seen > max_messages:
                    break
                try:
                    payload: Any = json.loads(raw_message)
                except (TypeError, json.JSONDecodeError):
                    invalid_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    invalid_messages += 1
                    continue
                if payload.get("message_type") != "certificate_update":
                    continue
                certificate_updates += 1
                data = payload.get("data")
                if not isinstance(data, Mapping) or data.get("cert_index") is None:
                    invalid_messages += 1
                    continue
                events = build_ct_events(
                    data,
                    source_event_id=str(data["cert_index"]),
                    observed_at=self._observed_at(),
                    parser_version="certstream-v1",
                )
                for event in events:
                    if self._store.append_source_event(event, hostname=event.raw_subject):
                        events_added += 1
                if max_messages is not None and messages_seen == max_messages:
                    break
        return CertStreamResult(
            messages_seen=messages_seen,
            certificate_updates=certificate_updates,
            invalid_messages=invalid_messages,
            events_added=events_added,
        )
