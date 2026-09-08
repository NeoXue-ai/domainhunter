"""Direct Certificate Transparency log polling (RFC 6962) as a ``CTPageFetcher``.

This is the single source of truth for CT discovery. It replaces the
previous CertStream (WebSocket + latest.json) and crt.sh adapters with a
thin, pure-HTTP poller that talks straight to a CT log's public API:

* ``GET /ct/v1/get-sth``         — signed tree head, current tree size
* ``GET /ct/v1/get-entries``     — raw ``leaf_input`` entries by index range

A single HTTP ``CTPageFetcher`` adapter keeps the rest of the pipeline
(``CTPoller`` → ``CTIngestOrchestrator`` → ``DomainHunterPipeline``) unchanged.

Cursor format (opaque to :class:`domainhunter.ingest.ct_poller.CTPoller`) is the
JSON-encoded map ``{ <log_id>: <last_consumed_tree_index> }`` so a store can
track several logs independently with one cursor string.
"""

from __future__ import annotations

import base64
import json
import struct
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self

import httpx
from cryptography import x509
from cryptography.x509.oid import ExtensionOID, NameOID

from domainhunter.ingest.ct_poller import CTCertificate, CTPage


@dataclass(frozen=True, slots=True)
class CTLogTarget:
    """One CT log to poll. ``log_id`` must be unique and stable across runs."""

    log_id: str
    base_url: str

    def __post_init__(self) -> None:
        if not self.log_id.strip():
            raise ValueError("log_id must not be empty")
        if not self.base_url.strip():
            raise ValueError("base_url must not be empty")
        if not self.base_url.endswith("/"):
            object.__setattr__(self, "base_url", self.base_url.rstrip("/"))


DEFAULT_LOG = CTLogTarget(
    log_id="cloudflare-nimbus2026",
    base_url="https://ct.cloudflare.com/logs/nimbus2026",
)

RETRY_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 2.0, 4.0)
RETRYABLE_STATUSES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})


class CTLogFetchError(RuntimeError):
    """Raised when a CT log cannot be reached or its payload cannot be parsed."""


def encode_length(length: int) -> bytes:
    if length < 128:
        return bytes([length])
    if length < 256:
        return bytes([0x81, length])
    if length < 65536:
        return bytes([0x82, (length >> 8) & 0xFF, length & 0xFF])
    return bytes([0x83, (length >> 16) & 0xFF, (length >> 8) & 0xFF, length & 0xFF])


def encode_bitstring(data: bytes) -> bytes:
    payload = b"\x00" + data
    return b"\x03" + encode_length(len(payload)) + payload


def encode_sequence(contents: bytes) -> bytes:
    return b"\x30" + encode_length(len(contents)) + contents


def _encode_oid_component(value: int) -> bytes:
    if value < 128:
        return bytes([value])
    parts: list[int] = []
    while value > 0:
        parts.insert(0, (value & 0x7F) | 0x80)
        value >>= 7
    parts[-1] &= 0x7F
    return bytes(parts)


def encode_oid(oid_str: str) -> bytes:
    parts = [int(part) for part in oid_str.split(".")]
    result = bytes([40 * parts[0] + parts[1]])
    for part in parts[2:]:
        result += _encode_oid_component(part)
    return b"\x06" + encode_length(len(result)) + result


def encode_null() -> bytes:
    return b"\x05\x00"


def wrap_tbs_as_certificate(tbs_der: bytes) -> bytes:
    """Reconstruct a certificate envelope around a precert's TBS so
    ``cryptography`` can load it and read CN/SAN."""
    sig_algo = encode_sequence(encode_oid("1.2.840.113549.1.1.11") + encode_null())
    sig_value = encode_bitstring(b"")
    return encode_sequence(tbs_der + sig_algo + sig_value)


def _cert_hostnames(cert: x509.Certificate) -> tuple[str, ...]:
    hostnames: set[str] = set()
    for attr in cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME):
        if isinstance(attr.value, str) and attr.value.strip():
            hostnames.add(attr.value)
    try:
        san = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
        for name in san.value:
            if isinstance(name, x509.DNSName):
                hostnames.add(name.value)
    except x509.ExtensionNotFound:
        pass
    return tuple(hostnames)


def _parse_leaf_cert(leaf_input_b64: str) -> x509.Certificate | None:
    """Decode one RFC 6962 ``leaf_input`` into a certificate (or None)."""
    try:
        data = base64.b64decode(leaf_input_b64)
    except (ValueError, TypeError):
        return None
    if len(data) < 12:
        return None

    version, leaf_type = data[0], data[1]
    if version != 0 or leaf_type != 0:
        return None
    entry_type = struct.unpack(">H", data[10:12])[0]
    payload = data[12:]

    try:
        if entry_type == 0:
            cert_len = struct.unpack(">I", b"\x00" + payload[:3])[0]
            return x509.load_der_x509_certificate(payload[3 : 3 + cert_len])
        if entry_type == 1:
            tbs_len = struct.unpack(">I", b"\x00" + payload[32:35])[0]
            tbs_der = payload[35 : 35 + tbs_len]
            return x509.load_der_x509_certificate(wrap_tbs_as_certificate(tbs_der))
    except (ValueError, struct.error):
        return None
    return None


def parse_leaf_input(leaf_input_b64: str) -> tuple[tuple[str, ...], str | None]:
    """Decode one RFC 6962 ``leaf_input`` and return (hostnames, issuer CN|O) or empty.

    Mirrors the parsing verified in the v1 poller: full certs are loaded
    directly; precert entries have their TBS wrapped back into a
    certificate so CN and SAN can be read uniformly.
    """
    cert = _parse_leaf_cert(leaf_input_b64)
    if cert is None:
        return (), None

    issuer: str | None = None
    try:
        for attr in cert.issuer.get_attributes_for_oid(NameOID.ORGANIZATION_NAME):
            if isinstance(attr.value, str) and attr.value.strip():
                issuer = attr.value.strip()
                break
        if issuer is None:
            for attr in cert.issuer.get_attributes_for_oid(NameOID.COMMON_NAME):
                if isinstance(attr.value, str) and attr.value.strip():
                    issuer = attr.value.strip()
                    break
    except (ValueError, AttributeError):
        issuer = None

    return _cert_hostnames(cert), issuer


def parse_leaf_timestamp(leaf_input_b64: str) -> datetime | None:
    """Return the leaf's log-submission timestamp (UTC) or None if unparseable.

    RFC 6962 v1 MerkleTreeLeaf: ``version(1) + leaf_type(1) + timestamp(8, ms)``.
    The log assigns this when it accepts the entry, so it is monotonically
    non-decreasing with tree index — the only reliable time axis for
    backfill range selection. (Certificate ``notBefore`` is NOT usable:
    CAs backdate and batch-issue, so it is wildly non-monotonic.)
    """
    try:
        data = base64.b64decode(leaf_input_b64)
    except (ValueError, TypeError):
        return None
    if len(data) < 12:
        return None
    version, leaf_type = data[0], data[1]
    if version != 0 or leaf_type != 0:
        return None
    timestamp_ms = struct.unpack(">Q", data[2:10])[0]
    if timestamp_ms <= 0:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)


def leaf_cert_envelope(hostnames: tuple[str, ...], issuer: str | None) -> Mapping[str, Any]:
    """Normalize parsed hosts into the ``leaf_cert`` shape ``build_ct_events`` expects."""
    leaf_cert: dict[str, Any] = {"all_domains": list(hostnames)}
    if hostnames:
        leaf_cert["subject"] = {"CN": hostnames[0]}
    if issuer:
        leaf_cert["issuer"] = {"O": issuer}
    return {"leaf_cert": leaf_cert}


def _parse_cursors(cursor: str | None) -> dict[str, int]:
    if cursor is None:
        return {}
    try:
        raw = json.loads(cursor)
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid CT log cursor {cursor!r}") from error
    if not isinstance(raw, dict):
        raise TypeError("CT log cursor must be a JSON object")
    parsed: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"CT log cursor index for {key!r} must be a non-negative int")
        parsed[key] = value
    return parsed


def _format_cursor(state: Mapping[str, int]) -> str:
    return json.dumps(dict(state), sort_keys=True)


class CTLogFetcher:
    """Fetch new entries from one or more CT logs as a single ``CTPage``."""

    def __init__(
        self,
        *,
        logs: tuple[CTLogTarget, ...] = (DEFAULT_LOG,),
        user_agent: str = "DomainHunter/2.0 (+public-research)",
        timeout: float = 20.0,
        max_retries: int = len(RETRY_BACKOFF_SECONDS),
        catchup_entries: int = 1000,
        max_entries_per_page: int = 500,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], object] | None = None,
    ) -> None:
        if not logs:
            raise ValueError("logs must not be empty")
        duplicates = {log.log_id for log in logs}
        if len(duplicates) != len(logs):
            raise ValueError("log_id values must be unique")
        if catchup_entries < 0:
            raise ValueError("catchup_entries must be non-negative")
        if max_entries_per_page < 1:
            raise ValueError("max_entries_per_page must be positive")
        self._logs = tuple(logs)
        self._catchup = catchup_entries
        self._page_size = max_entries_per_page
        self._max_retries = max_retries
        self._clock = clock
        self._sleep = sleep or _noop_sleep
        self._client = httpx.AsyncClient(
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=timeout,
            transport=transport,
        )

    async def __call__(self, cursor: str | None) -> CTPage:
        state = _parse_cursors(cursor)
        entries: list[CTCertificate] = []
        source_errors: list[str] = []
        successful_logs = 0
        for log in self._logs:
            try:
                entries.extend(await self._poll_log(log, state))
            except CTLogFetchError as error:
                source_errors.append(str(error))
            else:
                successful_logs += 1
        if successful_logs == 0:
            raise CTLogFetchError("; ".join(source_errors))
        return CTPage(
            entries=tuple(entries),
            next_cursor=_format_cursor(state),
            source_errors=tuple(source_errors),
        )

    async def _poll_log(self, log: CTLogTarget, state: dict[str, int]) -> list[CTCertificate]:
        tree_size = await self._get_sth(log)
        last = state.get(log.log_id)
        if last is None:
            last = max(0, tree_size - self._catchup)
        if tree_size <= last:
            return []
        start = last
        end = min(start + self._page_size - 1, tree_size - 1)
        entries = await self._get_entries(log, start, end)
        # RFC 6962 allows servers to return FEWER entries than requested
        # (Nimbus caps at ~50-150). The cursor must advance by the number
        # of entries actually returned — advancing to ``end + 1`` would
        # silently skip every truncated entry.
        state[log.log_id] = min(start + len(entries), tree_size)
        certificates: list[CTCertificate] = []
        for index, entry in enumerate(entries):
            leaf_input = entry.get("leaf_input", "")
            hostnames, issuer = parse_leaf_input(leaf_input)
            if not hostnames:
                continue
            certificates.append(
                CTCertificate(
                    source_event_id=f"{log.log_id}:{start + index}",
                    certificate=leaf_cert_envelope(hostnames, issuer),
                    observed_at=self._clock(),
                )
            )
        return certificates

    @property
    def logs(self) -> tuple[CTLogTarget, ...]:
        """The configured CT log targets, in polling order."""
        return self._logs

    async def fetch_entries(
        self, log_id: str, start: int, end: int
    ) -> tuple[list[dict[str, Any]], CTLogTarget]:
        """Fetch a raw entry range for one log (backfill's range-based page API)."""
        for log in self._logs:
            if log.log_id == log_id:
                if end < start:
                    raise ValueError("end must be >= start")
                return await self._get_entries(log, start, end), log
        raise ValueError(f"unknown log_id {log_id!r}")

    async def tree_sizes(self) -> dict[str, int]:
        """Return the current tree size of every configured log."""
        return {log.log_id: await self._get_sth(log) for log in self._logs}

    async def leaf_timestamp(self, log_id: str, index: int) -> datetime | None:
        """Fetch one entry and return its certificate ``notBefore`` (UTC)."""
        for log in self._logs:
            if log.log_id == log_id:
                entries = await self._get_entries(log, index, index)
                if not entries:
                    return None
                return parse_leaf_timestamp(entries[0].get("leaf_input", ""))
        raise ValueError(f"unknown log_id {log_id!r}")

    async def _get_sth(self, log: CTLogTarget) -> int:
        payload = await self._request(log, "/ct/v1/get-sth")
        tree_size = payload.get("tree_size") if isinstance(payload, dict) else None
        if not isinstance(tree_size, int) or tree_size < 0:
            raise CTLogFetchError(f"{log.log_id}: get-sth missing valid tree_size")
        return tree_size

    async def _get_entries(self, log: CTLogTarget, start: int, end: int) -> list[dict[str, Any]]:
        payload = await self._request(
            log, "/ct/v1/get-entries", params={"start": start, "end": end}
        )
        if not isinstance(payload, dict):
            raise CTLogFetchError(f"{log.log_id}: get-entries payload must be an object")
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list):
            raise CTLogFetchError(f"{log.log_id}: get-entries missing entries array")
        return [entry for entry in raw_entries if isinstance(entry, dict)]

    async def _request(
        self,
        log: CTLogTarget,
        path: str,
        *,
        params: Mapping[str, int] | None = None,
    ) -> dict[str, Any]:
        url = f"{log.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = await self._client.get(url, params=params)
            except httpx.TimeoutException as error:
                last_error = CTLogFetchError(f"{log.log_id}: {path} timeout: {error}")
            except httpx.HTTPError as error:
                last_error = CTLogFetchError(f"{log.log_id}: {path} transport error: {error}")
            else:
                if response.status_code in RETRYABLE_STATUSES:
                    last_error = CTLogFetchError(
                        f"{log.log_id}: {path} retryable status={response.status_code}"
                    )
                else:
                    if response.status_code >= 400:
                        raise CTLogFetchError(
                            f"{log.log_id}: {path} returned status={response.status_code}"
                        )
                    try:
                        payload: Any = response.json()
                    except (TypeError, ValueError) as error:
                        raise CTLogFetchError(
                            f"{log.log_id}: {path} payload is not JSON: {error}"
                        ) from error
                    if not isinstance(payload, dict):
                        raise CTLogFetchError(f"{log.log_id}: {path} payload must be an object")
                    return payload
            if attempt < self._max_retries - 1:
                await self._sleep(RETRY_BACKOFF_SECONDS[attempt])
        raise CTLogFetchError(
            f"{log.log_id}: {path} unreachable after {self._max_retries} attempts: {last_error}"
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()


async def _noop_sleep(_seconds: float) -> None:
    return None
