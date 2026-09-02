"""Immutable source events and deterministic idempotency keys."""

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256


@dataclass(frozen=True, slots=True)
class SourceEvent:
    """A single, append-only signal received from an external source."""

    source: str
    source_event_id: str
    raw_subject: str
    observed_at: datetime
    source_timestamp: datetime | None = None
    evidence_summary: str | None = None
    parser_version: str | None = None
    issuer: str | None = None

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise ValueError("source must not be empty")
        if not self.source_event_id.strip():
            raise ValueError("source_event_id must not be empty")
        if not self.raw_subject.strip():
            raise ValueError("raw_subject must not be empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.source_timestamp is not None and self.source_timestamp.tzinfo is None:
            raise ValueError("source_timestamp must be timezone-aware")

    @property
    def idempotency_key(self) -> str:
        """Return a stable key for replaying a source event safely."""
        material = f"{self.source}\0{self.source_event_id}".encode("utf-8")
        return sha256(material).hexdigest()
