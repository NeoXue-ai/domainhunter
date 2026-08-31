"""Typed alert + runbook domain primitives for spec §14 operations."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class AlertKind(StrEnum):
    """The exhaustive list of operator-actionable anomalies from spec §14."""

    ZERO_INPUT = "zero_input"
    BASELINE_DRIFT = "baseline_drift"
    QUEUE_BACKLOG = "queue_backlog"
    BUDGET_EXHAUSTED = "budget_exhausted"
    ERROR_RATE_SPIKE = "error_rate_spike"
    SSRF_INTERCEPTION_SPIKE = "ssrf_interception_spike"
    SCHEMA_FAILURE = "schema_failure"
    EXTERNAL_SYNC_FAILURE = "external_sync_failure"


class AlertSeverity(StrEnum):
    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class Alert:
    """One operator-actionable anomaly with a deterministic identity."""

    alert_id: str
    kind: AlertKind
    severity: AlertSeverity
    title: str
    summary: str
    opened_at: datetime
    runbook_id: str
    related_candidate_id: str | None
    related_stage: str | None
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.alert_id or not self.alert_id.strip():
            raise ValueError("alert_id must not be empty")
        if not self.title.strip():
            raise ValueError("title must not be empty")
        if not self.summary.strip():
            raise ValueError("summary must not be empty")
        if self.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        if not self.runbook_id.strip():
            raise ValueError("runbook_id must not be empty")


@dataclass(frozen=True, slots=True)
class Runbook:
    """Curated operator text for one anomaly class."""

    runbook_id: str
    kind: AlertKind
    title: str
    steps: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.runbook_id.strip():
            raise ValueError("runbook_id must not be empty")
        if not self.title.strip():
            raise ValueError("title must not be empty")
        if not self.steps:
            raise ValueError("steps must not be empty")