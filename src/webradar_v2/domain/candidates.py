"""Immutable candidate identities, version drafts, and cited evidence."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from urllib.parse import urlparse

from webradar_v2.domain.normalization import normalize_hostname


class CandidateOutcome(StrEnum):
    PUBLISHABLE_AI_SAAS = "publishable_ai_saas"
    VALID_BUT_NOT_READY = "valid_but_not_ready"
    NOT_TARGET = "not_target"
    DUPLICATE_OR_EXISTING = "duplicate_or_existing"
    POLICY_EXCLUDED = "policy_excluded"


class EvidenceType(StrEnum):
    SOURCE_EVENT = "source_event"
    TITLE = "title"
    META_DESCRIPTION = "meta_description"
    VISIBLE_TEXT = "visible_text"
    H1 = "h1"
    CTA = "cta"
    PRICING = "pricing"
    EXPOSURE_CHECK = "exposure_check"


@dataclass(frozen=True, slots=True)
class Evidence:
    """A quote that supports one candidate claim without relying on model memory."""

    evidence_type: EvidenceType
    quote: str
    url: str | None = None

    def __post_init__(self) -> None:
        if not self.quote.strip():
            raise ValueError("evidence quote must not be empty")
        if self.url is not None:
            parsed = urlparse(self.url)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValueError("evidence url must be an HTTP(S) URL")


@dataclass(frozen=True, slots=True)
class Candidate:
    """A stable product-review identity currently anchored to one root domain."""

    candidate_id: str
    domain: str
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class CandidateVersionDraft:
    """One proposed rule, model, or human view that must cite its evidence."""

    author_kind: str
    primary_outcome: CandidateOutcome
    classification_confidence: float
    name_suggestion: str | None
    description_suggestion: str | None
    evidence: tuple[Evidence, ...]
    model_version: str | None = None
    category: str | None = None
    tags: tuple[str, ...] = ()
    pricing_model: str | None = None
    target_audience: str | None = None

    def __post_init__(self) -> None:
        if self.author_kind not in {"rule", "llm", "human"}:
            raise ValueError("author_kind must be rule, llm, or human")
        if not 0 <= self.classification_confidence <= 1:
            raise ValueError("classification_confidence must be between 0 and 1")
        if not self.evidence:
            raise ValueError("candidate version requires evidence")
        if self.author_kind == "llm" and not (self.model_version and self.model_version.strip()):
            raise ValueError("llm candidate version requires model_version")
        if any(not tag.strip() for tag in self.tags):
            raise ValueError("tags must not contain empty values")


@dataclass(frozen=True, slots=True)
class CandidateVersion:
    """One append-only saved candidate interpretation with its original author payload."""

    candidate_id: str
    version: int
    created_at: datetime
    draft: CandidateVersionDraft

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.version < 1:
            raise ValueError("candidate version must be positive")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")


def build_candidate(hostname: str, *, created_at: datetime) -> Candidate:
    """Build the stable candidate identity for a normalized registrable domain."""
    domain = normalize_hostname(hostname).registrable_domain
    candidate_id = sha256(f"candidate\0{domain}".encode("utf-8")).hexdigest()
    return Candidate(candidate_id=candidate_id, domain=domain, created_at=created_at)
