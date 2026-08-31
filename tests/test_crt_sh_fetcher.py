"""Unit tests for the crt.sh JSON fetcher against an in-process MockTransport."""

import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path

import httpx
import pytest

from tests.support.fake_http import FakeHTTPServer
from webradar_v2.ingest.crt_sh import CrtShFetchError, CrtShFetcher


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ct" / "crt_sh" / "crt_sh_sample.json"


def _payload_bytes() -> bytes:
    data = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return json.dumps(data["entries"], ensure_ascii=False).encode("utf-8")


def _ok_route(server: FakeHTTPServer) -> None:
    server.add_route("/", body=_payload_bytes(), content_type="application/json")


def _make_fetcher(
    *,
    server: FakeHTTPServer | None = None,
    transport: httpx.MockTransport | None = None,
    **overrides: object,
) -> CrtShFetcher:
    if transport is None:
        if server is None:
            raise ValueError("either server or transport is required")
        transport = httpx.MockTransport(server.as_httpx_handler())
    kwargs: dict[str, object] = {
        "query": "ai",
        "transport": transport,
        "sleep": lambda _seconds: asyncio.sleep(0),  # skip backoff in tests
        "max_retries": 3,
    }
    kwargs.update(overrides)
    return CrtShFetcher(**kwargs)  # type: ignore[arg-type]


async def _drive(fetcher: CrtShFetcher, cursor: str | None) -> object:
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

            assert page.next_cursor == "9000000004"
            assert len(page.entries) == 4

            first = page.entries[0]
            assert first.source_event_id == "9000000001"
            assert first.certificate["leaf_cert"]["all_domains"] == [
                "nova-ai.com",
                "www.nova-ai.com",
            ]
            assert first.certificate["leaf_cert"]["issuer"] == "Let's Encrypt"

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
            page = await _drive(fetcher, "9000000002")

            assert page.next_cursor == "9000000004"
            ids = tuple(entry.source_event_id for entry in page.entries)
            assert ids == ("9000000003", "9000000004")

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
            page = await _drive(fetcher, "9000000004")
            assert page.entries == ()
            assert page.next_cursor == "9000000004"

        asyncio.run(run())
    finally:
        server.stop()


def test_retries_on_502_then_succeeds() -> None:
    payload = _payload_bytes()
    responses = iter(
        [
            httpx.Response(502, content=b"upstream busy"),
            httpx.Response(200, content=payload, headers={"Content-Type": "application/json"}),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    transport = httpx.MockTransport(handler)

    async def run() -> None:
        fetcher = _make_fetcher(server=None, transport=transport)  # type: ignore[arg-type]
        page = await _drive(fetcher, None)
        assert len(page.entries) == 4

    asyncio.run(run())


def test_raises_after_persistent_502() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, content=b"upstream busy")

    transport = httpx.MockTransport(handler)

    async def run() -> None:
        fetcher = _make_fetcher(
            server=None, transport=transport, max_retries=2  # type: ignore[arg-type]
        )
        with pytest.raises(CrtShFetchError):
            await _drive(fetcher, None)

    asyncio.run(run())


def test_splits_name_value_and_dedupes() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        _ok_route(server)

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            page = await _drive(fetcher, "9000000001")  # skip past first entry
            brightcanvas = next(
                entry for entry in page.entries if entry.source_event_id == "9000000002"
            )
            sans = brightcanvas.certificate["leaf_cert"]["all_domains"]
            assert sans == [
                "*.brightcanvas.io",
                "brightcanvas.io",
                "app.brightcanvas.io",
            ]
            assert len(sans) == len(set(sans))

        asyncio.run(run())
    finally:
        server.stop()


def test_maps_common_name_and_san_into_envelope() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        _ok_route(server)

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            page = await _drive(fetcher, "9000000002")

            third = next(
                entry for entry in page.entries if entry.source_event_id == "9000000003"
            )
            leaf = third.certificate["leaf_cert"]
            assert leaf["subject"]["CN"] == "bücher-tts.de"
            assert "bücher-tts.de" in leaf["all_domains"]
            assert "xn--bcher-kva-tts.de" in leaf["all_domains"]

        asyncio.run(run())
    finally:
        server.stop()


def test_skips_entry_without_id() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        body = json.dumps(
            [
                {"common_name": "no-id.example", "name_value": "no-id.example"},
                {
                    "id": 42,
                    "common_name": "good.example",
                    "name_value": "good.example",
                    "issuer_name": "Test CA",
                },
            ]
        ).encode("utf-8")
        server.add_route("/", body=body, content_type="application/json")

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            page = await _drive(fetcher, None)
            assert page.next_cursor == "42"
            assert len(page.entries) == 1
            assert page.entries[0].source_event_id == "42"

        asyncio.run(run())
    finally:
        server.stop()


def test_raises_on_non_array_payload() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        server.add_route(
            "/",
            body=b'{"oops": "not a list"}',
            content_type="application/json",
        )

        async def run() -> None:
            fetcher = _make_fetcher(server=server)
            with pytest.raises(CrtShFetchError):
                await _drive(fetcher, None)

        asyncio.run(run())
    finally:
        server.stop()


def test_clock_is_used_for_observed_at() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        _ok_route(server)
        stamp = datetime(2026, 8, 20, 6, 30, tzinfo=UTC)

        async def run() -> None:
            fetcher = _make_fetcher(server=server, clock=lambda: stamp)
            page = await _drive(fetcher, None)
            assert all(entry.observed_at == stamp for entry in page.entries)

        asyncio.run(run())
    finally:
        server.stop()