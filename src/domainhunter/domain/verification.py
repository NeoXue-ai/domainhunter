"""Immutable strict-scan facts attached to a candidate version."""

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class CandidateVerification:
    """Evidence produced by the strict CT → filter → HTTP path."""

    candidate_id: str
    candidate_version: int
    checked_at: datetime
    ct_first_seen_at: datetime | None
    rdap_tier: str | None
    rdap_age_days: int | None
    rdap_registration_at: datetime | None
    dns_has_a: bool | None
    http_status_code: int | None
    final_url: str | None
    canonical_url: str | None
    final_root_matches: bool | None

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ValueError("candidate_id must not be empty")
        if self.candidate_version < 1:
            raise ValueError("candidate_version must be positive")
        for value in (
            self.checked_at,
            self.ct_first_seen_at,
            self.rdap_registration_at,
        ):
            if value is not None and value.tzinfo is None:
                raise ValueError("verification timestamps must be timezone-aware")
        if self.rdap_tier is not None and self.rdap_tier not in {
            "tier1",
            "tier2",
            "unknown",
        }:
            raise ValueError("rdap_tier must be tier1, tier2, unknown, or None")
        for value in (self.final_url, self.canonical_url):
            if value is None:
                continue
            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValueError("verification URLs must be HTTP(S)")
