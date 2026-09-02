"""Read-only operational funnel snapshot."""

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
import math


@dataclass(frozen=True, slots=True)
class FunnelMetrics:
    source_events: int
    domains: int
    observations: int
    candidates: int
    candidate_versions: int
    review_decisions: int
    publications: int
    outreach_events: int
    queued_work: int
    budget_reserved_units: float
    budget_deferred_units: float
    cost_per_effective_candidate: float
    observation_outcomes: dict[str, int]


def percentile(values: Sequence[float], pct: float) -> float | None:
    """Return the nearest-rank ``pct`` percentile of ``values``.

    ``values`` is expected to be sorted ascending; it is re-sorted defensively
    because a mis-ordered input would silently return a wrong number. Nearest
    rank is used deliberately: with the handful of samples a local funnel
    produces, interpolation invents precision the data does not have.

    Returns ``None`` for an empty sequence so callers can distinguish
    "no samples yet" from "the measured latency was zero".
    """
    if not 0.0 <= pct <= 100.0:
        raise ValueError("pct must be between 0 and 100")
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    # Nearest rank: ceil(pct/100 * N), 1-based, clamped into range. The epsilon
    # absorbs binary-float drift so pct=50 over 4 samples lands on rank 2, not 3.
    rank = math.ceil(pct * len(ordered) / 100.0 - 1e-9)
    index = min(max(rank, 1), len(ordered)) - 1
    return ordered[index]


@dataclass(frozen=True, slots=True)
class FunnelAnalytics:
    """Derived funnel health: conversion, latency percentiles, backlog shape.

    Every rate is bounded to ``0..1`` and every latency is expressed in
    seconds. Latency fields are ``None`` when no sample exists rather than
    ``0.0``, so an empty pipeline never reads as an instantaneous one.
    """

    conversion_source_to_candidate: float
    conversion_candidate_to_approved: float
    conversion_source_to_published: float
    latency_first_signal_to_candidate_p50_seconds: float | None
    latency_first_signal_to_candidate_p95_seconds: float | None
    latency_candidate_to_decision_p50_seconds: float | None
    latency_candidate_to_decision_p95_seconds: float | None
    backlog_by_stage: dict[str, int]
    backlog_over_1h_by_stage: dict[str, int]
    computed_at: datetime

    def __post_init__(self) -> None:
        if self.computed_at.tzinfo is None:
            raise ValueError("computed_at must be timezone-aware")

    def as_payload(self) -> dict[str, object]:
        """Return a JSON-safe dict with ``computed_at`` rendered as ISO-8601."""
        payload = asdict(self)
        payload["computed_at"] = self.computed_at.isoformat()
        return payload
