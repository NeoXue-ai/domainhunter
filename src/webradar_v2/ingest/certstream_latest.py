"""REST poller for CertStream's latest.json feed.

CertStream normally provides a WebSocket stream at ``wss://certstream.calidog.io/``.
The same service also publishes a small JSON snapshot at ``/latest.json`` which is
useful as a lightweight fallback when the WebSocket is unavailable or stalled.
This module adapts that snapshot to the existing cursor-based ``CTPage`` contract
so it can be fed into ``CTPoller`` and the normal CT -> candidate pipeline.
"""

from collections.abc import Callable
from datetime import UTC, datetime
import asyncio
from typing import Any

import httpx

from webradar_v2.ingest.ct_poller import CTCertificate, CTPage


DEFAULT_CERTSTREAM_LATEST_URL = "https://certstream.calidog.io/latest.json"
DEFAULT_USER_AGENT = "WebRadar/2.0 (+public-research)"
RETRYABLE_STATUSES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
RETRY_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 2.0, 4.0)


class CertStreamLatestFetchError(RuntimeError):
    """Raised when the CertStream latest.json endpoint cannot be consumed."""


class CertStreamLatestFetcher:
    """Fetch CertStream's latest certificate snapshot as a ``CTPage``.

    Cursor format is the decimal ``str`` of the largest ``cert_index`` already
    observed. ``None`` means "first fetch" and accepts every entry on the page.
    Subsequent calls only keep entries with ``cert_index > cursor``.
    """

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_CERTSTREAM_LATEST_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = 15.0,
        max_retries: int = len(RETRY_BACKOFF_SECONDS),
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], asyncio.Future[None]] = asyncio.sleep,
    ) -> None:
        if not base_url.strip():
            raise ValueError("base_url must not be empty")
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        self._base_url = base_url
        self._clock = clock
        self._sleep = sleep
        self._max_retries = max_retries
        self._client = httpx.AsyncClient(
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=timeout,
            transport=transport,
        )

    async def __call__(self, cursor: str | None) -> CTPage:
        payload = await self._fetch_json()
        previous_id = self._parse_cursor(cursor) if cursor is not None else 0
        entries: list[CTCertificate] = []
        max_id = previous_id
        for raw_message in payload.get("messages", []):
            entry = self._normalize_message(raw_message)
            if entry is None:
                continue
            cert_id = int(entry.source_event_id)
            if cert_id <= previous_id:
                continue
            entries.append(entry)
            if cert_id > max_id:
                max_id = cert_id
        next_cursor = str(max_id) if max_id > previous_id else cursor
        return CTPage(entries=tuple(entries), next_cursor=next_cursor)

    async def _fetch_json(self) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = await self._client.get(self._base_url)
            except httpx.TimeoutException as error:
                last_error = error
            except httpx.HTTPError as error:
                raise CertStreamLatestFetchError(
                    f"CertStream latest.json transport error: {error}"
                ) from error
            else:
                if response.status_code in RETRYABLE_STATUSES:
                    last_error = CertStreamLatestFetchError(
                        f"CertStream latest.json retryable status={response.status_code}"
                    )
                else:
                    if response.status_code >= 400:
                        raise CertStreamLatestFetchError(
                            f"CertStream latest.json returned status={response.status_code}"
                        )
                    try:
                        payload = response.json()
                    except (TypeError, ValueError) as error:
                        raise CertStreamLatestFetchError(
                            f"CertStream latest.json payload is not JSON: {error}"
                        ) from error
                    if not isinstance(payload, dict):
                        raise CertStreamLatestFetchError(
                            "CertStream latest.json payload must be a JSON object"
                        )
                    messages = payload.get("messages")
                    if messages is not None and not isinstance(messages, list):
                        raise CertStreamLatestFetchError(
                            "CertStream latest.json 'messages' must be an array"
                        )
                    return payload
            if attempt < self._max_retries - 1:
                await self._sleep(RETRY_BACKOFF_SECONDS[attempt])
        raise CertStreamLatestFetchError(
            "CertStream latest.json unreachable after "
            f"{self._max_retries} attempts: {last_error}"
        )

    def _normalize_message(self, raw: Any) -> CTCertificate | None:
        if not isinstance(raw, dict):
            return None
        if raw.get("message_type") != "certificate_update":
            return None
        data = raw.get("data")
        if not isinstance(data, dict):
            return None
        cert_index = data.get("cert_index")
        if isinstance(cert_index, str):
            try:
                cert_index = int(cert_index)
            except ValueError:
                return None
        if not isinstance(cert_index, int) or cert_index <= 0:
            return None
        leaf_cert = data.get("leaf_cert")
        if not isinstance(leaf_cert, dict):
            return None
        seen = data.get("seen")
        if isinstance(seen, (int, float)):
            try:
                observed_at = datetime.fromtimestamp(float(seen), tz=UTC)
            except (OverflowError, OSError, ValueError):
                observed_at = self._clock()
        else:
            observed_at = self._clock()
        return CTCertificate(
            source_event_id=str(cert_index),
            certificate={"leaf_cert": leaf_cert},
            observed_at=observed_at,
        )

    @staticmethod
    def _parse_cursor(cursor: str) -> int:
        try:
            value = int(cursor)
        except ValueError as error:
            raise ValueError(
                "CertStream latest cursor must be a positive integer: %r" % (cursor,)
            ) from error
        if value < 0:
            raise ValueError(
                "CertStream latest cursor must be non-negative: %r" % (cursor,)
            )
        return value

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "CertStreamLatestFetcher":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
