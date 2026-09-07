"""Conservative candidate suggestions from deterministic L1 facts only."""

import re

from domainhunter.crawler.l1_analysis import L1Analysis
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.observations import OutcomeCode


_PRODUCT_TERMS = (
    "assistant",
    "automation",
    "free trial",
    "get started",
    "platform",
    "pricing",
    "workflow",
    "sign up",
    "try free",
)


def build_rule_candidate_draft(analysis: L1Analysis) -> CandidateVersionDraft | None:
    """Suggest a cited candidate only when L1 contains at least one usable fact."""
    if analysis.outcome_code not in {OutcomeCode.SUCCESS, OutcomeCode.CONTENT_INSUFFICIENT}:
        return None

    evidence: list[Evidence] = []
    if analysis.title:
        evidence.append(Evidence(EvidenceType.TITLE, analysis.title, analysis.final_url))
    if analysis.meta_description:
        evidence.append(
            Evidence(EvidenceType.META_DESCRIPTION, analysis.meta_description, analysis.final_url)
        )
    if not evidence:
        return None

    combined = " ".join(item.quote for item in evidence).lower()
    has_ai_signal = bool(re.search(r"\bai\b", combined)) or "artificial intelligence" in combined
    has_product_signal = any(term in combined for term in _PRODUCT_TERMS)
    strong = analysis.outcome_code is OutcomeCode.SUCCESS and has_ai_signal and has_product_signal
    return CandidateVersionDraft(
        author_kind="rule",
        primary_outcome=(
            CandidateOutcome.PUBLISHABLE_AI_SAAS
            if strong
            else CandidateOutcome.VALID_BUT_NOT_READY
        ),
        classification_confidence=0.78 if strong else 0.42,
        name_suggestion=analysis.title,
        description_suggestion=analysis.meta_description,
        evidence=tuple(evidence),
    )
