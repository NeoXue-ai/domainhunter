"""Append-only audit log entries for the AIKnows HTTP integration."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class AIKnowsAuditEntry:
    """One durable HTTP call record emitted by the AIKnows client wrapper."""

    method: str
    url: str
    status_code: int | None
    latency_ms: float
    candidate_id: str | None
    candidate_version: int | None
    occurred_at: datetime

    def __post_init__(self) -> None:
        if not self.method or not self.method.strip():
            raise ValueError("method must not be empty")
        if not self.url or not self.url.strip():
            raise ValueError("url must not be empty")
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")
        if self.candidate_version is not None and self.candidate_version < 1:
            raise ValueError("candidate_version must be positive when set")
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
