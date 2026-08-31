"""Typed work stages, leases, and budget-reservation decisions."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class WorkStage(StrEnum):
    L1 = "l1"
    L2 = "l2"
    L3 = "l3"
    EXPOSURE = "exposure"
    LLM = "llm"
    PUBLICATION = "publication"
    SIGNAL_INGEST = "signal_ingest"
    CLAIM = "claim"


@dataclass(frozen=True, slots=True)
class WorkLease:
    work_id: int
    stage: WorkStage
    entity_id: str
    scheduled_at: datetime
    lease_token: str
    lease_owner: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class BudgetReservation:
    allowed: bool
    remaining_units: float
    reason: str | None = None
