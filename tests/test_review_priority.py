import pytest

from domainhunter.domain.review_priority import ReviewPriorityInputs, calculate_review_priority


def test_calculates_the_versioned_review_priority_breakdown() -> None:
    result = calculate_review_priority(
        ReviewPriorityInputs(
            product_evidence=0.8,
            early_presence=0.9,
            low_exposure=0.7,
            data_completeness=0.6,
        )
    )

    assert result.score == 0.78
    assert result.product_evidence_contribution == 0.28
    assert result.early_presence_contribution == 0.27
    assert result.low_exposure_contribution == 0.14
    assert result.data_completeness_contribution == 0.09
    assert result.formula_version == "review-priority-v1"


def test_unknown_exposure_neither_adds_nor_subtracts_priority() -> None:
    result = calculate_review_priority(
        ReviewPriorityInputs(
            product_evidence=0.8,
            early_presence=0.9,
            low_exposure=None,
            data_completeness=0.6,
        )
    )

    assert result.low_exposure_contribution == 0.0
    assert result.score == 0.64


@pytest.mark.parametrize("value", [-0.1, 1.1])
def test_rejects_out_of_range_component_scores(value: float) -> None:
    with pytest.raises(ValueError):
        ReviewPriorityInputs(
            product_evidence=value,
            early_presence=0.5,
            low_exposure=0.5,
            data_completeness=0.5,
        )
