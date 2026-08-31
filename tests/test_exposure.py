from datetime import UTC, datetime

import pytest

from webradar_v2.domain.exposure import ExposureChannel, ExposureCheck, ExposureStatus


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def test_scores_explicit_exposure_results_without_assuming_unknown() -> None:
    not_observed = ExposureCheck(
        channel=ExposureChannel.PRODUCT_HUNT,
        checked_at=OBSERVED_AT,
        status=ExposureStatus.NOT_OBSERVED,
        query="Example AI",
    )
    observed = ExposureCheck(
        channel=ExposureChannel.HACKER_NEWS,
        checked_at=OBSERVED_AT,
        status=ExposureStatus.OBSERVED,
        query="Example AI",
        evidence_url="https://news.ycombinator.com/item?id=1",
    )

    assert not_observed.low_exposure_score == 1.0
    assert observed.low_exposure_score == 0.0
    assert (
        ExposureCheck.unknown(ExposureChannel.LINKEDIN, OBSERVED_AT).low_exposure_score is None
    )


def test_requires_evidence_url_only_when_exposure_is_observed() -> None:
    with pytest.raises(ValueError, match="evidence_url"):
        ExposureCheck(
            channel=ExposureChannel.PRODUCT_HUNT,
            checked_at=OBSERVED_AT,
            status=ExposureStatus.OBSERVED,
            query="Example AI",
        )
