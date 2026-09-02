from datetime import UTC, datetime

import pytest

from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
    build_candidate,
)


NOW = datetime(2026, 8, 16, tzinfo=UTC)


def test_builds_a_stable_candidate_for_a_registrable_domain() -> None:
    first = build_candidate("app.example.co.uk", created_at=NOW)
    replay = build_candidate("www.example.co.uk", created_at=NOW)

    assert first.domain == "example.co.uk"
    assert first.candidate_id == replay.candidate_id


def test_version_draft_keeps_evidence_and_requires_llm_model_version() -> None:
    evidence = Evidence(
        evidence_type=EvidenceType.TITLE,
        quote="Example AI — Write better content",
        url="https://example.com",
    )
    draft = CandidateVersionDraft(
        author_kind="llm",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.82,
        name_suggestion="Example AI",
        description_suggestion="An AI writing assistant.",
        evidence=(evidence,),
        model_version="gpt-5.6",
    )

    assert draft.evidence == (evidence,)

    with pytest.raises(ValueError, match="model_version"):
        CandidateVersionDraft(
            author_kind="llm",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.82,
            name_suggestion="Example AI",
            description_suggestion="An AI writing assistant.",
            evidence=(evidence,),
        )


def test_version_draft_rejects_unsupported_claims_without_evidence() -> None:
    with pytest.raises(ValueError, match="evidence"):
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.VALID_BUT_NOT_READY,
            classification_confidence=0.5,
            name_suggestion=None,
            description_suggestion=None,
            evidence=(),
        )
