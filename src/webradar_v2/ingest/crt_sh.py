"""Live crt.sh JSON fetcher that feeds the cursor-based CT poller.

crt.sh is a free Certificate Transparency search engine. Its ``?q=...&output=json``
endpoint returns a top-level JSON array of certificate entries, each with
``id``, ``not_before``, ``not_after``, ``common_name``, ``name_value``
(newline-joined SAN list), and ``issuer_name``. There is no server-side
``id > X`` filter, so we fetch the full query window and filter client-side.

This module only adapts the wire format. The cursor-based idempotency contract
(``CTPoller``) and the certificate → event mapping (``build_ct_events``) live
elsewhere; this fetcher just turns one crt.sh entry into the ``leaf_cert``
envelope they expect.
"""

from collections.abc import Callable
from datetime import UTC, datetime
import asyncio
from typing import Any

import httpx

from webradar_v2.ingest.ct_poller import CTCertificate, CTPage


DEFAULT_BASE_URL = "https://crt.sh/"
DEFAULT_USER_AGENT = "WebRadar/2.0 (+public-research)"
RETRYABLE_STATUSES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
RETRY_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 2.0, 4.0)


class CrtShFetchError(RuntimeError):
    """Raised when crt.sh cannot be reached or its payload cannot be parsed."""


class CrtShFetcher:
    """Fetch crt.sh JSON entries and normalize them to ``CTPage`` records.

    Cursor format: a decimal ``str`` of the largest cert id already observed.
    ``None`` means "first page ever" and returns every entry on the page; the
    next cursor is set to the page's maximum ``id``. On subsequent pages we
    only retain entries with ``id > cursor`` and advance the cursor to either
    the new max or the previous value (whichever is larger).
    """

    def __init__(
        self,
        *,
        query: str,
        base_url: str = DEFAULT_BASE_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = 15.0,
        max_retries: int = len(RETRY_BACKOFF_SECONDS),
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], asyncio.Future[None]] = asyncio.sleep,
    ) -> None:
        if not query.strip():
            raise ValueError("query must not be empty")
        if not base_url.strip():
            raise ValueError("base_url must not be empty")
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        self._query = query
        self._base_url = base_url.rstrip("/") + "/"
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
        observed_at = self._clock()
        entries: list[CTCertificate] = []
        max_id = previous_id
        for raw_entry in payload:
            entry = self._normalize_entry(raw_entry)
            if entry is None:
                continue
            if entry["id"] <= previous_id:
                continue
            entries.append(
                CTCertificate(
                    source_event_id=str(entry["id"]),
                    certificate={"leaf_cert": entry["leaf_cert"]},
                    observed_at=observed_at,
                )
            )
            if entry["id"] > max_id:
                max_id = entry["id"]
        next_cursor = str(max_id) if max_id > previous_id else cursor
        return CTPage(entries=tuple(entries), next_cursor=next_cursor)

    async def _fetch_json(self) -> list[Any]:
        """GET the JSON endpoint with bounded retry on transient failures."""
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = await self._client.get(
                    self._base_url,
                    params={"q": self._query, "output": "json"},
                )
            except httpx.TimeoutException as error:
                last_error = error
            except httpx.HTTPError as error:
                raise CrtShFetchError(f"crt.sh transport error: {error}") from error
            else:
                if response.status_code in RETRYABLE_STATUSES:
                    last_error = CrtShFetchError(
                        f"crt.sh retryable status={response.status_code}"
                    )
                else:
                    if response.status_code >= 400:
                        raise CrtShFetchError(
                            f"crt.sh returned status={response.status_code}"
                        )
                    try:
                        payload = response.json()
                    except (TypeError, ValueError) as error:
                        raise CrtShFetchError(
                            f"crt.sh payload is not JSON: {error}"
                        ) from error
                    if not isinstance(payload, list):
                        raise CrtShFetchError(
                            "crt.sh payload must be a top-level JSON array"
                        )
                    return payload
            if attempt < self._max_retries - 1:
                await self._sleep(RETRY_BACKOFF_SECONDS[attempt])
        raise CrtShFetchError(
            f"crt.sh unreachable after {self._max_retries} attempts: {last_error}"
        )

    @staticmethod
    def _normalize_entry(raw: Any) -> dict[str, Any] | None:
        """Convert one raw crt.sh entry into the ``leaf_cert`` envelope.

        Returns ``None`` when the entry lacks a usable ``id`` or hostname
        signal, so the caller can skip it without raising.
        """
        if not isinstance(raw, dict):
            return None
        cert_id = raw.get("id")
        if not isinstance(cert_id, int) or cert_id <= 0:
            return None
        common_name = raw.get("common_name")
        name_value = raw.get("name_value")
        all_domains: list[str] = []
        if isinstance(name_value, str):
            for chunk in name_value.split("\n"):
                cleaned = chunk.strip()
                if cleaned and cleaned not in all_domains:
                    all_domains.append(cleaned)
        if isinstance(common_name, str):
            common = common_name.strip()
            if common and common not in all_domains:
                all_domains.append(common)
        if not all_domains:
            return None
        issuer = raw.get("issuer_name")
        leaf_cert: dict[str, Any] = {
            "subject": {"CN": common_name if isinstance(common_name, str) else ""},
            "all_domains": all_domains,
        }
        if isinstance(issuer, str) and issuer.strip():
            leaf_cert["issuer"] = issuer.strip()
        return {"id": cert_id, "leaf_cert": leaf_cert}

    @staticmethod
    def _parse_cursor(cursor: str) -> int:
        try:
            value = int(cursor)
        except ValueError as error:
            raise ValueError(f"crt.sh cursor must be a positive integer: {cursor!r}") from error
        if value < 0:
            raise ValueError(f"crt.sh cursor must be non-negative: {cursor!r}")
        return value

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "CrtShFetcher":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()