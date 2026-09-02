from domainhunter.crawler.l1_analysis import analyze_http_document
from domainhunter.crawler.l2_analysis import extract_rendered_facts
from domainhunter.domain.candidates import CandidateOutcome, EvidenceType
from domainhunter.domain.rule_candidates import build_rule_candidate_draft


def test_builds_a_publishable_rule_draft_from_strong_product_metadata() -> None:
    analysis = analyze_http_document(
        status_code=200,
        final_url="https://example.com",
        html=(
            "<title>Example AI — Automate workflows</title>"
            '<meta name="description" content="AI workflow automation with a free trial">'
            "<p>" + ("Automate your team workflows. " * 30) + "</p>"
        ),
    )

    draft = build_rule_candidate_draft(analysis)

    assert draft is not None
    assert draft.author_kind == "rule"
    assert draft.primary_outcome is CandidateOutcome.PUBLISHABLE_AI_SAAS
    assert draft.name_suggestion == "Example AI — Automate workflows"
    assert {evidence.evidence_type for evidence in draft.evidence} == {
        EvidenceType.TITLE,
        EvidenceType.META_DESCRIPTION,
    }


def test_returns_not_ready_for_weak_metadata_and_none_without_evidence() -> None:
    weak = analyze_http_document(
        status_code=200,
        final_url="https://example.com",
        html="<title>Coming Soon</title><p>Launching shortly.</p>",
    )
    empty = analyze_http_document(status_code=200, final_url="https://example.com", html="<p>Hi</p>")

    weak_draft = build_rule_candidate_draft(weak)

    assert weak_draft is not None
    assert weak_draft.primary_outcome is CandidateOutcome.VALID_BUT_NOT_READY
    assert build_rule_candidate_draft(empty) is None


def test_adds_cited_l2_product_evidence_to_rule_draft() -> None:
    analysis = analyze_http_document(
        status_code=200,
        final_url="https://example.com",
        html=(
            "<title>Example AI — Automate workflows</title>"
            '<meta name="description" content="AI workflow automation">'
            "<p>" + ("Automate your team workflows. " * 30) + "</p>"
        ),
    )
    facts = extract_rendered_facts(
        "<h1>Example AI</h1><button>Start free trial</button><p>Plans start at $29.</p>"
    )

    draft = build_rule_candidate_draft(analysis, l2_facts=facts)

    assert draft is not None
    assert {item.evidence_type for item in draft.evidence} >= {
        EvidenceType.H1,
        EvidenceType.CTA,
        EvidenceType.PRICING,
    }
