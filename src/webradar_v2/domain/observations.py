"""Typed outcomes for append-only domain observations."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class OutcomeCode(StrEnum):
    DNS_NOT_FOUND = "dns_not_found"
    DNS_TIMEOUT = "dns_timeout"
    TLS_ERROR = "tls_error"
    CONNECT_TIMEOUT = "connect_timeout"
    HTTP_4XX = "http_4xx"
    HTTP_429 = "http_429"
    HTTP_5XX = "http_5xx"
    REDIRECT_LOOP = "redirect_loop"
    ROBOTS_DISALLOWED = "robots_disallowed"
    RENDER_TIMEOUT = "render_timeout"
    CONTENT_INSUFFICIENT = "content_insufficient"
    BLOCKED_SSRF = "blocked_ssrf"
    BUDGET_DEFERRED = "budget_deferred"
    SUCCESS = "success"


@dataclass(frozen=True, slots=True)
class Observation:
    """One immutable result from a DNS, HTTP, render, or evidence check."""

    domain: str
    outcome_code: OutcomeCode
    observed_at: datetime
    attempt_number: int
    status_code: int | None = None
    final_url: str | None = None
    detail: str | None = None
    canonical_url: str | None = None
    internal_links: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.domain.strip():
            raise ValueError("domain must not be empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.attempt_number < 1:
            raise ValueError("attempt_number must be positive")
