"""Immutable human review actions, kept separate from publication."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256


class ReviewAction(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"
    DEFER = "defer"
    BLOCKLIST = "blocklist"
    EDIT = "edit"
    UNDO = "undo"


class ReasonTag(StrEnum):
    BLOG = "blog"
    PROMPT_DIRECTORY = "prompt_directory"
    OPEN_SOURCE_DEMO = "open_source_demo"
    AGENCY = "agency"
    BROKEN_SITE = "broken_site"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


@dataclass(frozen=True, slots=True)
class ReviewDecision:
    """One replay-safe, append-only human decision for a specific candidate version."""

    decision_id: str
    request_id: str
    candidate_id: str
    candidate_version: int
    action: ReviewAction
    actor_id: str
    decided_at: datetime
    reason_tags: tuple[ReasonTag, ...] = ()

    def __post_init__(self) -> None:
        if not self.decision_id or not self.request_id.strip():
            raise ValueError("review identifiers must not be empty")
        if not self.candidate_id or self.candidate_version < 1:
            raise ValueError("candidate version must be specified")
        if not self.actor_id.strip():
            raise ValueError("actor_id must not be empty")
        if self.decided_at.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        if any(not isinstance(tag, ReasonTag) for tag in self.reason_tags):
            raise ValueError("reason_tags must be ReasonTag members")


def build_review_decision(
    *,
    request_id: str,
    candidate_id: str,
    candidate_version: int,
    action: ReviewAction,
    actor_id: str,
    decided_at: datetime,
    reason_tags: tuple[ReasonTag, ...] = (),
) -> ReviewDecision:
    """Build an idempotent review event from the client action request ID."""
    decision_id = sha256(f"review\0{request_id}".encode("utf-8")).hexdigest()
    return ReviewDecision(
        decision_id=decision_id,
        request_id=request_id,
        candidate_id=candidate_id,
        candidate_version=candidate_version,
        action=action,
        actor_id=actor_id,
        decided_at=decided_at,
        reason_tags=tuple(reason_tags),
    )
