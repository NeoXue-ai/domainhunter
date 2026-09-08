"""Unit tests for the RFC 6962 CT log adapter against an in-process MockTransport."""

import base64
import struct
from datetime import UTC, datetime

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from domainhunter.ingest.ct_log_adapter import (
    DEFAULT_LOG,
    CTLogFetcher,
    CTLogTarget,
    _format_cursor,
    _parse_cursors,
    parse_leaf_input,
)


def _make_cert(common_name: str = "app.example.com") -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, common_name)]
    )
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now.replace(year=now.year + 1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("app.example.com"), x509.DNSName("example.com")]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.DER)


def _x509_leaf_input(cert_der: bytes) -> str:
    timestamp = struct.pack(">Q", 0)
    cert_len = struct.pack(">I", len(cert_der))[1:]
    raw = b"\x00\x00" + timestamp + b"\x00\x00" + cert_len + cert_der
    return base64.b64encode(raw).decode("ascii")


def _handler(size: int, leaf: str):
    """A realistic single-log MockTransport: get-entries returns a contiguous
    slice of `leaf` for every index in the requested [start, end] range."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/ct/v1/get-sth"):
            return httpx.Response(200, json={"tree_size": size})
        if "/ct/v1/get-entries" in url:
            start = int(request.url.params["start"])
            end = int(request.url.params["end"])
            batch = [{"leaf_input": leaf} for _ in range(start, min(end, size - 1) + 1)]
            return httpx.Response(200, json={"entries": batch})
        return httpx.Response(404)

    return handler


def _log() -> CTLogTarget:
    return CTLogTarget("test", "https://ct.example.test")


def test_default_log_uses_cloudflare_nimbus() -> None:
    assert DEFAULT_LOG == CTLogTarget(
        "cloudflare-nimbus2026",
        "https://ct.cloudflare.com/logs/nimbus2026",
    )


def test_parse_leaf_input_extracts_cn_and_san() -> None:
    leaf = _x509_leaf_input(_make_cert())
    hostnames, issuer = parse_leaf_input(leaf)
    assert set(hostnames) == {"app.example.com", "example.com"}
    assert issuer == "app.example.com"


def test_parse_leaf_input_rejects_garbage() -> None:
    assert parse_leaf_input("not-base64!!") == ((), None)
    assert parse_leaf_input(base64.b64encode(b"\x00").decode()) == ((), None)


def test_cursor_round_trip() -> None:
    cursor = _format_cursor({"a": 5, "b": 12345})
    assert _parse_cursors(cursor) == {"a": 5, "b": 12345}
    assert _parse_cursors(None) == {}


def test_fetcher_returns_new_entries_and_advances_cursor() -> None:
    leaf = _x509_leaf_input(_make_cert())
    transport = httpx.MockTransport(_handler(1000, leaf))
    fetcher = CTLogFetcher(
        logs=(_log(),),
        transport=transport,
        catchup_entries=3,
        sleep=lambda _: None,
    )

    import asyncio

    async def run() -> None:
        first = await fetcher(None)
        assert len(first.entries) == 3
        assert {e.source_event_id for e in first.entries} == {
            "test:997",
            "test:998",
            "test:999",
        }
        for entry in first.entries:
            assert "example.com" in entry.certificate["leaf_cert"]["all_domains"]
        assert _parse_cursors(first.next_cursor) == {"test": 1000}

        # Re-polling from the returned cursor yields nothing new.
        second = await fetcher(first.next_cursor)
        assert second.entries == ()

    asyncio.run(run())


def test_fetcher_cursor_survives_server_truncation() -> None:
    """A server may return fewer entries than requested (RFC 6962 allows it).

    The cursor must advance by the number of entries actually returned,
    never by the requested range — otherwise truncated entries are
    silently skipped forever.
    """
    leaf = _x509_leaf_input(_make_cert())
    returned_per_page = {"count": 2}

    def truncating_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("get-sth"):
            return httpx.Response(200, json={"tree_size": 10_000})
        start = int(httpx.QueryParams(request.url.query)["start"])
        count = min(returned_per_page["count"], 10_000 - start)
        return httpx.Response(
            200, json={"entries": [{"leaf_input": leaf} for _ in range(count)]}
        )

    fetcher = CTLogFetcher(
        logs=(_log(),),
        transport=httpx.MockTransport(truncating_handler),
        catchup_entries=10,
        max_entries_per_page=500,
        sleep=lambda _: None,
    )

    import asyncio

    async def run() -> None:
        first = await fetcher(None)
        # Requested 10 (catchup), server returned only 2.
        assert len(first.entries) == 2
        assert {e.source_event_id for e in first.entries} == {
            "test:9990",
            "test:9991",
        }
        assert _parse_cursors(first.next_cursor) == {"test": 9992}

        # Next page continues right after what was actually returned.
        second = await fetcher(first.next_cursor)
        assert {e.source_event_id for e in second.entries} == {
            "test:9992",
            "test:9993",
        }

    asyncio.run(run())


def test_fetcher_catchup_pulls_only_the_tail() -> None:
    leaf = _x509_leaf_input(_make_cert())
    transport = httpx.MockTransport(_handler(2000, leaf))
    fetcher = CTLogFetcher(
        logs=(_log(),),
        transport=transport,
        catchup_entries=3,
        max_entries_per_page=500,
        sleep=lambda _: None,
    )

    import asyncio

    async def run() -> None:
        page = await fetcher(None)
        assert len(page.entries) == 3
        assert {e.source_event_id for e in page.entries} == {
            "test:1997",
            "test:1998",
            "test:1999",
        }
        assert _parse_cursors(page.next_cursor) == {"test": 2000}

    asyncio.run(run())


def test_fetcher_requires_unique_log_ids() -> None:
    import pytest

    with pytest.raises(ValueError):
        CTLogFetcher(logs=(_log(), _log()))


def test_fetcher_keeps_healthy_logs_moving_when_another_source_fails() -> None:
    leaf = _x509_leaf_input(_make_cert())
    healthy = CTLogTarget("healthy", "https://healthy.example.test")
    failing = CTLogTarget("failing", "https://failing.example.test")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "failing.example.test":
            return httpx.Response(503)
        return _handler(1, leaf)(request)

    fetcher = CTLogFetcher(
        logs=(healthy, failing),
        transport=httpx.MockTransport(handler),
        catchup_entries=1,
        max_retries=1,
        sleep=lambda _: None,
    )

    import asyncio

    async def run() -> None:
        page = await fetcher(None)
        assert [entry.source_event_id for entry in page.entries] == ["healthy:0"]
        assert page.source_errors == (
            "failing: /ct/v1/get-sth unreachable after 1 attempts: "
            "failing: /ct/v1/get-sth retryable status=503",
        )

    asyncio.run(run())
