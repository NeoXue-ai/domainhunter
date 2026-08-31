"""Offline Precision@K evaluator for candidate classification models.

This module is intentionally pure: it takes a sequence of ``(prediction, expected)``
pairs and returns deterministic metrics, so the same redacted HTML corpus can be
replayed against rule, LLM, and human versions of the classifier and the
results compared side-by-side.

It does **not** call any external model or network. Model authors run their
classifier offline, pass the predictions in, and report the metrics back.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from webradar_v2.domain.candidates import Candidate, CandidateOutcome


@dataclass(frozen=True, slots=True)
class ExpectedOutcome:
    """One expected outcome attached to a known candidate."""

    candidate: Candidate
    primary_outcome: CandidateOutcome
    confidence: float = 1.0

    def __post_init__(self) -> None:
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        if not self.candidate.candidate_id:
            raise ValueError("candidate must have a candidate_id")


@dataclass(frozen=True, slots=True)
class PrecisionMetrics:
    """A snapshot of Precision@K results for one model run."""

    p_at_1: float
    p_at_3: float
    p_at_5: float
    p_at_10: float
    total: int
    hits: dict[int, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "p_at_1": self.p_at_1,
            "p_at_3": self.p_at_3,
            "p_at_5": self.p_at_5,
            "p_at_10": self.p_at_10,
            "total": self.total,
            "hits": dict(self.hits),
        }


def evaluate_predictions(
    predictions: Sequence[tuple[Candidate, CandidateOutcome]],
    *,
    expected: Sequence[ExpectedOutcome],
    k_values: tuple[int, ...] = (1, 3, 5, 10),
) -> PrecisionMetrics:
    """Compute Precision@K for a batch of predictions against the expected set.

    A prediction is a *hit* at rank ``k`` when the predicted outcome for that
    candidate matches the expected outcome. Each candidate is counted exactly
    once at the first K where it appears; missing candidates count as misses.

    Returns a :class:`PrecisionMetrics` snapshot with the precision at each
    requested K plus raw hit counts.
    """
    if not k_values:
        raise ValueError("k_values must contain at least one K")
    if any(k < 1 for k in k_values):
        raise ValueError("K values must be positive")

    by_candidate: dict[str, ExpectedOutcome] = {
        item.candidate.candidate_id: item for item in expected
    }
    hits_by_k: dict[int, int] = {k: 0 for k in k_values}
    seen: set[str] = set()

    for index, (candidate, predicted_outcome) in enumerate(predictions, start=1):
        if not candidate.candidate_id:
            raise ValueError("prediction candidates must have a candidate_id")
        if candidate.candidate_id in seen:
            continue  # duplicates do not multiply the denominator
        expected_outcome = by_candidate.get(candidate.candidate_id)
        if expected_outcome is None:
            continue
        seen.add(candidate.candidate_id)
        for k in k_values:
            if index <= k and predicted_outcome == expected_outcome.primary_outcome:
                hits_by_k[k] += 1

    total = len(by_candidate)
    safe_total = total if total else 1
    p_at = {
        k: hits_by_k[k] / safe_total for k in k_values
    }

    return PrecisionMetrics(
        p_at_1=p_at[k_values[0]] if len(k_values) >= 1 else 0.0,
        p_at_3=p_at[3] if 3 in p_at else (p_at[max(k_values)] if k_values else 0.0),
        p_at_5=p_at[5] if 5 in p_at else (p_at[max(k_values)] if k_values else 0.0),
        p_at_10=p_at[10] if 10 in p_at else (p_at[max(k_values)] if k_values else 0.0),
        total=total,
        hits=hits_by_k,
    )