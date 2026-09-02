"""Versioned, explainable ordering for human candidate review."""

from dataclasses import dataclass
from datetime import datetime


FORMULA_VERSION = "review-priority-v1"


def _validate_score(name: str, value: float | None, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if not isinstance(value, int | float) or not 0 <= value <= 1:
        raise ValueError(f"{name} must be a number between 0 and 1")


@dataclass(frozen=True, slots=True)
class ReviewPriorityInputs:
    """Normalized evidence components for the approved review-ordering formula."""

    product_evidence: float
    early_presence: float
    low_exposure: float | None
    data_completeness: float

    def __post_init__(self) -> None:
        _validate_score("product_evidence", self.product_evidence)
        _validate_score("early_presence", self.early_presence)
        _validate_score("low_exposure", self.low_exposure, optional=True)
        _validate_score("data_completeness", self.data_completeness)


@dataclass(frozen=True, slots=True)
class ReviewPriority:
    """A score plus each saved formula contribution for later audit."""

    score: float
    product_evidence_contribution: float
    early_presence_contribution: float
    low_exposure_contribution: float
    data_completeness_contribution: float
    formula_version: str = FORMULA_VERSION


@dataclass(frozen=True, slots=True)
class ReviewPrioritySnapshot:
    """An append-only persisted calculation used to order human review."""

    calculated_at: datetime
    priority: ReviewPriority

    def __post_init__(self) -> None:
        if self.calculated_at.tzinfo is None:
            raise ValueError("calculated_at must be timezone-aware")


def calculate_review_priority(inputs: ReviewPriorityInputs) -> ReviewPriority:
    """Calculate the fixed human-review ordering score; unknown exposure is neutral."""
    product_evidence = round(inputs.product_evidence * 0.35, 4)
    early_presence = round(inputs.early_presence * 0.30, 4)
    low_exposure = round((inputs.low_exposure or 0.0) * 0.20, 4)
    data_completeness = round(inputs.data_completeness * 0.15, 4)
    return ReviewPriority(
        score=round(product_evidence + early_presence + low_exposure + data_completeness, 4),
        product_evidence_contribution=product_evidence,
        early_presence_contribution=early_presence,
        low_exposure_contribution=low_exposure,
        data_completeness_contribution=data_completeness,
    )
