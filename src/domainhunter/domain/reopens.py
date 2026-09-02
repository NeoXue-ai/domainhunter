"""Immutable reopen events that reset a terminal candidate's retry counter."""

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from .observations import OutcomeCode


@dataclass(frozen=True, slots=True)
class ReopenEvent:
    """One replay-safe, append-only human trigger that resets a terminal candidate."""

    reopen_id: str
    candidate_id: str
    trigger_source_event_id: str
    previous_outcome: OutcomeCode
    new_outcome: OutcomeCode
    actor_id: str
    reason: str
    reopened_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.previous_outcome, OutcomeCode):
            raise ValueError("previous_outcome must be an OutcomeCode member")
        if not isinstance(self.new_outcome, OutcomeCode):
            raise ValueError("new_outcome must be an OutcomeCode member")
        if not self.reason or not self.reason.strip():
            raise ValueError("reason must not be empty")
        if self.reopened_at.tzinfo is None:
            raise ValueError("reopened_at must be timezone-aware")


def build_reopen_event(
    *,
    request_id: str,
    candidate_id: str,
    trigger_source_event_id: str,
    previous_outcome: OutcomeCode,
    new_outcome: OutcomeCode,
    actor_id: str,
    reason: str,
    reopened_at: datetime,
) -> ReopenEvent:
    """Build an idempotent reopen event from a client-supplied request id."""
    if not candidate_id.strip():
        raise ValueError("candidate_id must not be empty")
    if not trigger_source_event_id.strip():
        raise ValueError("trigger_source_event_id must not be empty")
    if not actor_id.strip():
        raise ValueError("actor_id must not be empty")
    if not reason.strip():
        raise ValueError("reason must not be empty")
    reopen_id = sha256(
        f"reopen\0{candidate_id}\0{trigger_source_event_id}\0{request_id}".encode("utf-8")
    ).hexdigest()
    return ReopenEvent(
        reopen_id=reopen_id,
        candidate_id=candidate_id,
        trigger_source_event_id=trigger_source_event_id,
        previous_outcome=previous_outcome,
        new_outcome=new_outcome,
        actor_id=actor_id,
        reason=reason,
        reopened_at=reopened_at,
    )
