"""Contract tests for the immutable Observation outcome record."""

from datetime import UTC, datetime

from domainhunter.domain.observations import Observation, OutcomeCode


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def test_observation_defaults_canonical_and_internal_links_to_none_and_empty_tuple() -> None:
    observation = Observation(
        domain="example.com",
        outcome_code=OutcomeCode.SUCCESS,
        observed_at=OBSERVED_AT,
        attempt_number=1,
    )

    assert observation.canonical_url is None
    assert observation.internal_links == ()


def test_observation_carries_canonical_url_and_internal_links() -> None:
    observation = Observation(
        domain="example.com",
        outcome_code=OutcomeCode.SUCCESS,
        observed_at=OBSERVED_AT,
        attempt_number=1,
        canonical_url="https://example.com/",
        internal_links=("https://example.com/about", "https://example.com/pricing"),
    )

    assert observation.canonical_url == "https://example.com/"
    assert observation.internal_links == (
        "https://example.com/about",
        "https://example.com/pricing",
    )


def test_observation_internal_links_is_exposed_as_tuple() -> None:
    observation = Observation(
        domain="example.com",
        outcome_code=OutcomeCode.SUCCESS,
        observed_at=OBSERVED_AT,
        attempt_number=1,
        internal_links=("https://example.com/about", "https://example.com/pricing"),
    )

    assert isinstance(observation.internal_links, tuple)