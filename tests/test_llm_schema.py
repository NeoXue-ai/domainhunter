import json

from webradar_v2.domain.candidates import CandidateOutcome, Evidence, EvidenceType
from webradar_v2.llm.schema import LLMResultState, parse_llm_candidate_output


EVIDENCE = (
    Evidence(EvidenceType.H1, "Automate your AI workflows", "https://example.com"),
    Evidence(EvidenceType.PRICING, "Plans start at $29 per month", "https://example.com"),
)


def _payload(**overrides: object) -> str:
    payload: dict[str, object] = {
        "is_candidate": True,
        "classification_confidence": 0.88,
        "primary_outcome_suggestion": "publishable_ai_saas",
        "rejection_reasons": [],
        "name_suggestion": "Example AI",
        "description_suggestion": "Automates AI workflows for operations teams.",
        "category": "automation",
        "tags": ["workflow", "operations"],
        "pricing_model": "paid",
        "target_audience": "operations teams",
        "evidence": [
            {"type": "h1", "url": "https://example.com", "quote": "Automate your AI workflows"}
        ],
        "model_version": "gpt-test-1",
    }
    payload.update(overrides)
    return json.dumps(payload)


def test_builds_a_cited_llm_draft_only_from_existing_evidence() -> None:
    result = parse_llm_candidate_output(_payload(), allowed_evidence=EVIDENCE)

    assert result.state is LLMResultState.READY
    assert result.draft is not None
    assert result.draft.author_kind == "llm"
    assert result.draft.primary_outcome is CandidateOutcome.PUBLISHABLE_AI_SAAS
    assert result.draft.category == "automation"
    assert result.draft.evidence == (EVIDENCE[0],)


def test_sends_schema_or_hallucinated_evidence_failures_to_manual_review() -> None:
    result = parse_llm_candidate_output(
        _payload(evidence=[{"type": "h1", "url": "https://example.com", "quote": "Invented claim"}]),
        allowed_evidence=EVIDENCE,
    )

    assert result.state is LLMResultState.NEEDS_REVIEW
    assert result.draft is None
    assert "allowed evidence" in result.reason
