"""Read-only projection for the human candidate-review queue."""

from dataclasses import dataclass

from domainhunter.domain.candidates import Candidate, CandidateVersion
from domainhunter.domain.review_priority import ReviewPrioritySnapshot


@dataclass(frozen=True, slots=True)
class ReviewQueueItem:
    """A candidate ready for review with its latest cited version and saved score."""

    candidate: Candidate
    latest_version: CandidateVersion
    priority: ReviewPrioritySnapshot
    canonical_url: str | None = None
    internal_links: tuple[str, ...] = ()
