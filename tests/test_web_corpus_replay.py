"""Replay the redacted web evidence corpus through Precision@K.

Spec §15 requires a fixed, replayable evidence set so any classifier change
(rule edit, prompt tweak, model swap) can be benchmarked against the same
inputs. These tests only exercise :func:`webradar_v2.llm.eval.evaluate_predictions`
plus a few trivial classifiers; they do not call any LLM.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from webradar_v2.domain.candidates import (
    Candidate,
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.llm.eval import (
    ExpectedOutcome,
    PrecisionMetrics,
    evaluate_predictions,
)


CORPUS_DIR = Path(__file__).parent / "fixtures" / "corpora" / "web_pages"


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _build_candidate(record: dict) -> Candidate:
    """Build a stable Candidate from a corpus URL."""
    return Candidate(
        candidate_id=record["url"],
        domain=record["url"],
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
    )


def _build_expected(record: dict) -> ExpectedOutcome:
    return ExpectedOutcome(
        candidate=_build_candidate(record),
        primary_outcome=CandidateOutcome(record["primary_outcome"]),
        confidence=float(record["confidence"]),
    )


def test_corpus_files_exist_and_are_well_formed() -> None:
    pages = _load_jsonl(CORPUS_DIR / "ai_saas_strong.jsonl")
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    page_urls = {record["url"] for record in pages}
    outcome_urls = {record["url"] for record in outcomes}
    assert page_urls == outcome_urls
    for record in pages:
        assert record["html"].strip()
        assert record["expected_outcome"] in {outcome.value for outcome in CandidateOutcome}


def test_evaluate_predictions_handles_perfect_classifier() -> None:
    """A perfect classifier recovers every expected candidate at every K.

    The metric counts how many expected candidates received a correct
    prediction within their first K appearances, so a perfect classifier
    (correct predictions in expected order) yields 1/N at K=1, 3/N at K=3,
    and 1.0 at K >= N. This is what the existing :func:`evaluate_predictions`
    implements, and the test pins the contract.
    """
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    expected = [_build_expected(record) for record in outcomes]
    total = len(expected)
    predictions: Sequence[tuple[Candidate, CandidateOutcome]] = [
        (item.candidate, item.primary_outcome) for item in expected
    ]

    metrics = evaluate_predictions(predictions, expected=expected)

    assert isinstance(metrics, PrecisionMetrics)
    assert metrics.total == total
    assert metrics.p_at_1 == pytest.approx(1 / total)
    assert metrics.p_at_3 == pytest.approx(min(3, total) / total)
    assert metrics.p_at_5 == pytest.approx(min(5, total) / total)
    assert metrics.p_at_10 == pytest.approx(min(10, total) / total)
    assert metrics.hits[1] == 1
    assert metrics.hits[3] == min(3, total)
    assert metrics.hits[5] == min(5, total)
    assert metrics.hits[10] == min(10, total)


def test_evaluate_predictions_handles_always_wrong_classifier() -> None:
    """Restrict the candidate set to rows whose outcome is not a single placeholder.

    The corpus intentionally uses every :class:`CandidateOutcome` value, so we
    cannot pick a single "always wrong" outcome across the whole file. Instead
    we use a placeholder the candidate subset never has and verify the metrics
    drop to zero for that subset.
    """
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    expected = [
        _build_expected(record)
        for record in outcomes
        if record["primary_outcome"] != CandidateOutcome.NOT_TARGET.value
    ]
    placeholder = CandidateOutcome.NOT_TARGET  # no expected row in this subset uses it
    predictions: Sequence[tuple[Candidate, CandidateOutcome]] = [
        (item.candidate, placeholder) for item in expected
    ]

    metrics = evaluate_predictions(predictions, expected=expected)

    assert metrics.total == len(expected)
    assert all(value == 0 for value in metrics.hits.values())
    assert metrics.p_at_1 == pytest.approx(0.0)
    assert metrics.p_at_3 == pytest.approx(0.0)
    assert metrics.p_at_5 == pytest.approx(0.0)
    assert metrics.p_at_10 == pytest.approx(0.0)


def test_evaluate_predictions_partial_classifier_counts_first_appearance_only() -> None:
    """Duplicate predictions for the same candidate are deduped, not double-counted.

    Build a 3-candidate expected set and submit a duplicate (same candidate)
    twice in a row at the top. Only the first appearance should count, and
    the second never-seen candidate counts as a miss.
    """
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    first_three = [_build_expected(record) for record in outcomes[:3]]
    duplicate_candidate = first_three[0].candidate
    correct_choice = first_three[0].primary_outcome
    wrong_choice = CandidateOutcome.POLICY_EXCLUDED
    predictions: Sequence[tuple[Candidate, CandidateOutcome]] = [
        (duplicate_candidate, correct_choice),  # first appearance — counts
        (duplicate_candidate, wrong_choice),  # duplicate — must be ignored
    ]

    metrics = evaluate_predictions(predictions, expected=first_three, k_values=(1, 3, 5, 10))

    # Only expected[0] got a correct prediction; expected[1] and expected[2]
    # were never predicted, so they're misses.
    assert metrics.hits[1] == 1
    assert metrics.hits[3] == 1
    assert metrics.hits[5] == 1
    assert metrics.hits[10] == 1
    assert metrics.total == 3
    assert metrics.p_at_1 == pytest.approx(1 / 3)


def test_evaluate_predictions_missing_candidates_count_as_misses() -> None:
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    expected = [_build_expected(record) for record in outcomes]
    # Only emit predictions for the first candidate.
    first = expected[0]
    predictions: Sequence[tuple[Candidate, CandidateOutcome]] = [
        (first.candidate, first.primary_outcome)
    ]

    metrics = evaluate_predictions(predictions, expected=expected)

    assert metrics.hits[1] == 1
    assert metrics.p_at_1 == pytest.approx(1 / len(expected))
    # The hit cap is bounded by the number of correct predictions actually emitted.
    assert metrics.hits[10] == 1
    assert metrics.total == len(expected)


def test_evaluate_predictions_respects_custom_k_values() -> None:
    """Custom K values only report hits at the requested ranks."""
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    expected = [
        _build_expected(record)
        for record in outcomes
        if record["primary_outcome"] != CandidateOutcome.NOT_TARGET.value
    ]
    # Predict everything as a placeholder the subset never uses.
    placeholder = CandidateOutcome.NOT_TARGET
    predictions: Sequence[tuple[Candidate, CandidateOutcome]] = [
        (item.candidate, placeholder) for item in expected
    ]

    metrics = evaluate_predictions(predictions, expected=expected, k_values=(2, 4, 7))

    # No real hits — only the requested Ks appear in the result.
    assert set(metrics.hits) == {2, 4, 7}
    assert all(value == 0 for value in metrics.hits.values())
    assert metrics.p_at_1 == pytest.approx(0.0)  # p_at_1 mirrors k_values[0]
    assert metrics.p_at_10 == pytest.approx(0.0)  # p_at_10 falls back to the max K


def test_evaluate_predictions_rejects_invalid_k_values() -> None:
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    expected = [_build_expected(record) for record in outcomes]

    with pytest.raises(ValueError):
        evaluate_predictions([], expected=expected, k_values=())
    with pytest.raises(ValueError):
        evaluate_predictions([], expected=expected, k_values=(0, 1))


def test_corpus_outcomes_align_with_draft_taxonomy() -> None:
    """Every expected_outcomes.jsonl row should be reachable from a draft.

    This protects the schema/eval boundary: if the LLM schema drops a tag
    that the corpus still expects, the eval baseline goes red here.
    """
    outcomes = _load_jsonl(CORPUS_DIR / "expected_outcomes.jsonl")
    known_outcomes = {outcome.value for outcome in CandidateOutcome}
    for record in outcomes:
        assert record["primary_outcome"] in known_outcomes
        # The corpus also has to play well with the candidate draft validator.
        draft = CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome(record["primary_outcome"]),
            classification_confidence=float(record["confidence"]),
            name_suggestion=None,
            description_suggestion=None,
            evidence=(
                Evidence(EvidenceType.TITLE, "Acme", record["url"]),
            ),
        )
        assert draft.evidence, "corpus draft must cite at least one evidence"
