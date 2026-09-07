"""Read-only operational funnel snapshot."""


from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FunnelMetrics:
    source_events: int
    domains: int
    observations: int
    candidates: int
    candidate_versions: int
    review_decisions: int
    observation_outcomes: dict[str, int]
