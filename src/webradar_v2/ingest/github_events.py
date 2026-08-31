"""GitHub repository homepage parsing without API access or persistence."""

from datetime import datetime
from urllib.parse import urlparse

from webradar_v2.domain.events import SourceEvent
from webradar_v2.domain.normalization import InvalidHostname, normalize_hostname


class InvalidGitHubHomepage(ValueError):
    """Raised when a repository homepage is not a public HTTP(S) URL."""


def build_github_homepage_event(
    *,
    repository_id: str,
    homepage: str,
    observed_at: datetime,
    parser_version: str = "github-v1",
) -> SourceEvent:
    """Create one source event for a repository's declared public homepage."""
    if not repository_id.strip():
        raise ValueError("repository_id must not be empty")
    if not isinstance(homepage, str) or not homepage.strip():
        raise InvalidGitHubHomepage("homepage must be a non-empty HTTP(S) URL")

    try:
        parsed = urlparse(homepage)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise InvalidGitHubHomepage("homepage must use HTTP(S) and include a hostname")
        if parsed.username is not None or parsed.password is not None:
            raise InvalidGitHubHomepage("homepage must not contain credentials")
        hostname = normalize_hostname(parsed.hostname).hostname
    except (InvalidHostname, ValueError) as error:
        if isinstance(error, InvalidGitHubHomepage):
            raise
        raise InvalidGitHubHomepage("homepage hostname must be public") from error

    return SourceEvent(
        source="github",
        source_event_id=f"{repository_id}:{hostname}",
        raw_subject=hostname,
        observed_at=observed_at,
        evidence_summary="repository_homepage",
        parser_version=parser_version,
    )
