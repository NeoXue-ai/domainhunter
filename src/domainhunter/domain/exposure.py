"""Explicit public-channel exposure observations for review ordering."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from urllib.parse import urlparse


class ExposureChannel(StrEnum):
    PRODUCT_HUNT = "product_hunt"
    HACKER_NEWS = "hacker_news"
    X = "x"
    LINKEDIN = "linkedin"
    AI_DIRECTORY = "ai_directory"


class ExposureStatus(StrEnum):
    OBSERVED = "observed"
    NOT_OBSERVED = "not_observed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ExposureCheck:
    """One channel result; unknown is intentionally neutral rather than negative evidence."""

    channel: ExposureChannel
    checked_at: datetime
    status: ExposureStatus
    query: str
    evidence_url: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if self.checked_at.tzinfo is None:
            raise ValueError("checked_at must be timezone-aware")
        if not self.query.strip():
            raise ValueError("query must not be empty")
        if self.status is ExposureStatus.OBSERVED and not self.evidence_url:
            raise ValueError("observed exposure requires evidence_url")
        if self.status is not ExposureStatus.OBSERVED and self.evidence_url:
            raise ValueError("evidence_url is only allowed for observed exposure")
        if self.evidence_url:
            parsed = urlparse(self.evidence_url)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValueError("evidence_url must be an HTTP(S) URL")

    @property
    def low_exposure_score(self) -> float | None:
        if self.status is ExposureStatus.NOT_OBSERVED:
            return 1.0
        if self.status is ExposureStatus.OBSERVED:
            return 0.0
        return None

    @classmethod
    def unknown(cls, channel: ExposureChannel, checked_at: datetime) -> "ExposureCheck":
        return cls(
            channel=channel,
            checked_at=checked_at,
            status=ExposureStatus.UNKNOWN,
            query="not_checked",
        )
