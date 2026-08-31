import asyncio
import json
from datetime import UTC, datetime

from webradar_v2.ingest.certstream import CertStreamListener
from webradar_v2.storage.sqlite import SQLiteStore


class FakeSocket:
    def __init__(self, messages: tuple[str, ...]) -> None:
        self._messages = messages

    async def __aenter__(self) -> "FakeSocket":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self._messages:
            yield message


def test_listens_to_certstream_and_replays_certificate_updates_idempotently(tmp_path) -> None:
    message = json.dumps(
        {
            "message_type": "certificate_update",
            "data": {
                "cert_index": 42,
                "leaf_cert": {
                    "subject": {"CN": "example.com"},
                    "all_domains": ["example.com"],
                },
            },
        }
    )
    now = datetime(2026, 8, 16, tzinfo=UTC)

    def connect(url: str) -> FakeSocket:
        assert url == "wss://ct.example.test"
        return FakeSocket((message, message))

    async def run() -> None:
        listener = CertStreamListener(
            store=SQLiteStore(tmp_path / "webradar.db"),
            url="wss://ct.example.test",
            connect=connect,
            observed_at=lambda: now,
        )
        result = await listener.listen(max_messages=2)

        assert result.messages_seen == 2
        assert result.certificate_updates == 2
        assert result.events_added == 1

    asyncio.run(run())
