"""Append-only record of an explicit, dry-run outreach trigger."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class OutreachEvent:
    """One manual outreach trigger that issues (or previews) claim tokens.

    The event records who triggered the action, against which approved version,
    whether it was a dry-run preview, and a redacted contact preview that is
    safe to expose back through the API.
    """

    candidate_id: str
    candidate_version: int
    actor_id: str
    triggered_at: datetime
    dry_run: bool
    recipient_source_url: str | None
    claim_tokens_issued: int
    contact_count: int
    contact_preview_json: str

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if not self.actor_id:
            raise ValueError("actor_id must not be empty")
        if self.triggered_at.tzinfo is None:
            raise ValueError("triggered_at must be timezone-aware")
        if self.candidate_version < 1:
            raise ValueError("candidate_version must be positive")
        if self.claim_tokens_issued < 0 or self.contact_count < 0:
            raise ValueError("token and contact counts must be non-negative")
