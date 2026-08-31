from datetime import UTC, datetime

import pytest

from webradar_v2.ingest.github_events import InvalidGitHubHomepage, build_github_homepage_event


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def test_builds_a_replay_safe_event_from_a_public_repository_homepage() -> None:
    event = build_github_homepage_event(
        repository_id="12345",
        homepage="HTTPS://App.Example.COM:443/pricing?ref=github",
        observed_at=OBSERVED_AT,
    )

    assert event.source == "github"
    assert event.source_event_id == "12345:app.example.com"
    assert event.raw_subject == "app.example.com"
    assert event.observed_at == OBSERVED_AT
    assert event.evidence_summary == "repository_homepage"
    assert event.parser_version == "github-v1"


@pytest.mark.parametrize(
    "homepage",
    ["", "example.com", "ftp://example.com", "https://127.0.0.1", "https://localhost"],
)
def test_rejects_non_public_or_non_http_homepages(homepage: str) -> None:
    with pytest.raises(InvalidGitHubHomepage):
        build_github_homepage_event(
            repository_id="12345",
            homepage=homepage,
            observed_at=OBSERVED_AT,
        )
