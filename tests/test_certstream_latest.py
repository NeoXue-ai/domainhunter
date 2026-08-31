"""Unit tests for the CertStream latest.json REST fetcher."""

import asyncio
from datetime import UTC, datetime
import json

import httpx
import pytest

from tests.support.fake_http import FakeHTTPServer
from webradar_v2.ingest.certstream_latest import (
    CertStreamLatestFetchError,
    CertStreamLatestFetcher,
)


def _payload_bytes() -> bytes:
    messages = [
        {
            "message_type": "certificate_update",
            "data": {
                "cert_index": 9001,
                "seen": 1780000000.0,
                "leaf_cert": {
                    "subject": {"CN": "nova-ai.com"},
                    "all_domains": ["nova-ai.com", "www.nova-ai.com"],
                },
            },
        },
        {
            "message_type": "certificate_update",
            "data": {
                "cert_index": 9002,
                "seen": 1780000001.0,
                "leaf_cert": {
                    "subject": {"CN": "brightcanvas.io"},
                    "all_domains": ["brightcanvas.io", "app.brightcanvas.io"],
                },
            },
        },
    ]
    return json.dumps({"messages": messages}).encode("utf-8")


def _ok_route(server: FakeHTTPServer) -> None:
    server.add_route("/latest.json", body=_payload_bytes(), content_type="application/json")


def _make_fetcher(
    *,
    server: FakeHTTPServer | None = None,
    transport: httpx.MockTransport | None = None,
    **overrides: object,
) -> CertStreamLatestFetcher:
    if transport is None:
        if server is None:
            raise ValueError("either server or transport is required")
        transport = httpx.MockTransport(server.as_httpx_handler())
    kwargs: dict[str, object] = {
        "transport": transport,
        "sleep": lambda _seconds: asyncio.sleep(0),
        "max_retries": 3,
    }
    kwargs.update(overrides)
    return CertStreamLatestFetcher(**kwargs)  # type: ignore[arg-type]


async def _drive(fetcher: CertStreamLatestFetcher, cursor: str | None) -> object:
    async with fetcher:
        return await fetcher(cursor)


def test_returns_ct_page_from_live_json() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        _ok_route(server)

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            page = await _drive(fetcher, None)

            assert page.next_cursor == "9002"
            assert len(page.entries) == 2

            first = page.entries[0]
            assert first.source_event_id == "9001"
            assert first.certificate["leaf_cert"]["all_domains"] == [
                "nova-ai.com",
                "www.nova-ai.com",
            ]
            assert first.observed_at == datetime.fromtimestamp(
                1780000000.0, tz=UTC
            )

        asyncio.run(run())
    finally:
        server.stop()


def test_filters_by_cursor() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        _ok_route(server)

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            page = await _drive(fetcher, "9001")

            assert page.next_cursor == "9002"
            ids = tuple(entry.source_event_id for entry in page.entries)
            assert ids == ("9002",)

        asyncio.run(run())
    finally:
        server.stop()


def test_advances_cursor_when_no_new_entries() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        _ok_route(server)

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            page = await _drive(fetcher, "9002")
            assert page.entries == ()
            assert page.next_cursor == "9002"

        asyncio.run(run())
    finally:
        server.stop()


def test_retries_on_502_then_succeeds() -> None:
    payload = _payload_bytes()
    responses = iter(
        [
            httpx.Response(502, content=b"upstream busy"),
            httpx.Response(
                200,
                content=payload,
                headers={"Content-Type": "application/json"},
            ),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    transport = httpx.MockTransport(handler)

    async def run() -> None:
        fetcher = _make_fetcher(server=None, transport=transport)  # type: ignore[arg-type]
        page = await _drive(fetcher, None)
        assert len(page.entries) == 2

    asyncio.run(run())


def test_raises_after_persistent_502() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, content=b"upstream busy")

    transport = httpx.MockTransport(handler)

    async def run() -> None:
        fetcher = _make_fetcher(
            server=None, transport=transport, max_retries=2  # type: ignore[arg-type]
        )
        with pytest.raises(CertStreamLatestFetchError):
            await _drive(fetcher, None)

    asyncio.run(run())


def test_skips_non_certificate_messages_and_bad_entries() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        payload = {
            "messages": [
                {"message_type": "heartbeat", "data": {}},
                {
                    "message_type": "certificate_update",
                    "data": {
                        "cert_index": "not-an-int",
                        "leaf_cert": {"subject": {"CN": "bad.example"}},
                    },
                },
                {
                    "message_type": "certificate_update",
                    "data": {
                        "cert_index": 42,
                        "leaf_cert": {"subject": {"CN": "good.example"}},
                    },
                },
            ]
        }
        server.add_route(
            "/latest.json",
            body=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
        )

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            page = await _drive(fetcher, None)
            assert page.next_cursor == "42"
            assert len(page.entries) == 1
            assert page.entries[0].source_event_id == "42"

        asyncio.run(run())
    finally:
        server.stop()


def test_raises_on_non_object_payload() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        server.add_route(
            "/latest.json",
            body=b'["not", "an", "object"]',
            content_type="application/json",
        )

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            with pytest.raises(CertStreamLatestFetchError):
                await _drive(fetcher, None)

        asyncio.run(run())
    finally:
        server.stop()
