"""Validate untrusted LLM JSON before it can become a candidate version."""

from dataclasses import dataclass
from enum import StrEnum
import json
from typing import Any

from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)


TAXONOMY_VERSION = "webradar-taxonomy-v1"
ALLOWED_CATEGORIES = frozenset(
    {
        "ai_assistant",
        "automation",
        "content_generation",
        "customer_support",
        "data_analysis",
        "developer_tools",
        "other",
    }
)
_REQUIRED_KEYS = frozenset(
    {
        "is_candidate",
        "classification_confidence",
        "primary_outcome_suggestion",
        "rejection_reasons",
        "name_suggestion",
        "description_suggestion",
        "category",
        "tags",
        "pricing_model",
        "target_audience",
        "evidence",
        "model_version",
    }
)


class LLMResultState(StrEnum):
    READY = "ready"
    NEEDS_REVIEW = "llm_needs_review"


@dataclass(frozen=True, slots=True)
class LLMParseResult:
    """A verified draft or a reason why model output requires manual inspection."""

    state: LLMResultState
    draft: CandidateVersionDraft | None = None
    reason: str | None = None


def _needs_review(reason: str) -> LLMParseResult:
    return LLMParseResult(state=LLMResultState.NEEDS_REVIEW, reason=reason)


def validate_llm_output(
    raw_output: str, *, allowed_evidence: tuple[Evidence, ...]
) -> LLMParseResult:
    """Validate raw model JSON against the fixed candidate schema and allowed evidence.

    This is the public entry point used by :class:`LLMProvider` adapters. It
    is intentionally a thin alias of :func:`parse_llm_candidate_output` so
    callers can depend on a stable contract that is documented separately
    from the parser's implementation details.
    """
    return parse_llm_candidate_output(raw_output, allowed_evidence=allowed_evidence)


def _string(value: object, field: str, *, nullable: bool = False) -> str | None:
    if nullable and value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _allowed_evidence(
    items: object, allowed: tuple[Evidence, ...]
) -> tuple[Evidence, ...]:
    if not isinstance(items, list) or not items:
        raise ValueError("evidence must be a non-empty list")
    selected: list[Evidence] = []
    for item in items:
        if not isinstance(item, dict) or set(item) != {"type", "url", "quote"}:
            raise ValueError("each evidence item must contain only type, url, and quote")
        try:
            candidate = Evidence(
                evidence_type=EvidenceType(item["type"]),
                quote=_string(item["quote"], "evidence.quote") or "",
                url=_string(item["url"], "evidence.url"),
            )
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid evidence: {error}") from error
        if candidate not in allowed:
            raise ValueError("model evidence must exactly match allowed evidence")
        if candidate not in selected:
            selected.append(candidate)
    return tuple(selected)


def parse_llm_candidate_output(
    raw_output: str, *, allowed_evidence: tuple[Evidence, ...]
) -> LLMParseResult:
    """Accept only fixed-schema output that cites facts already observed by WebRadar."""
    try:
        payload: Any = json.loads(raw_output)
    except (TypeError, json.JSONDecodeError) as error:
        return _needs_review(f"invalid JSON: {error}")
    if not isinstance(payload, dict) or set(payload) != _REQUIRED_KEYS:
        return _needs_review("output must match the fixed LLM schema")

    try:
        if not isinstance(payload["is_candidate"], bool):
            raise ValueError("is_candidate must be boolean")
        confidence = payload["classification_confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, int | float):
            raise ValueError("classification_confidence must be a number")
        outcome = CandidateOutcome(payload["primary_outcome_suggestion"])
        if not isinstance(payload["rejection_reasons"], list) or any(
            not isinstance(reason, str) or not reason.strip()
            for reason in payload["rejection_reasons"]
        ):
            raise ValueError("rejection_reasons must be a list of non-empty strings")
        category = _string(payload["category"], "category")
        if category not in ALLOWED_CATEGORIES:
            raise ValueError(f"category must use {TAXONOMY_VERSION}")
        tags = payload["tags"]
        if not isinstance(tags, list) or any(not isinstance(tag, str) or not tag.strip() for tag in tags):
            raise ValueError("tags must be a list of non-empty strings")
        evidence = _allowed_evidence(payload["evidence"], allowed_evidence)
        draft = CandidateVersionDraft(
            author_kind="llm",
            primary_outcome=outcome,
            classification_confidence=float(confidence),
            name_suggestion=_string(payload["name_suggestion"], "name_suggestion", nullable=True),
            description_suggestion=_string(
                payload["description_suggestion"], "description_suggestion", nullable=True
            ),
            category=category,
            tags=tuple(tags),
            pricing_model=_string(payload["pricing_model"], "pricing_model", nullable=True),
            target_audience=_string(payload["target_audience"], "target_audience", nullable=True),
            evidence=evidence,
            model_version=_string(payload["model_version"], "model_version") or "",
        )
    except (TypeError, ValueError) as error:
        return _needs_review(str(error))
    return LLMParseResult(state=LLMResultState.READY, draft=draft)
