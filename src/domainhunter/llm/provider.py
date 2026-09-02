"""Provider-agnostic LLM enrichment boundary used by the Fast/Detail Tier pipeline."""

import json
from dataclasses import dataclass
from typing import Any, Protocol, Self

import httpx

from domainhunter.domain.candidates import (
    CandidateVersionDraft,
    Evidence,
)
from domainhunter.llm.schema import (
    ALLOWED_CATEGORIES,
    TAXONOMY_VERSION,
    LLMParseResult,
    LLMResultState,
    parse_llm_candidate_output,
)


@dataclass(frozen=True, slots=True)
class LLMResult:
    """One verified LLM draft or a structured reason why human review is required."""

    state: LLMResultState
    draft: CandidateVersionDraft | None = None
    reason: str | None = None


class LLMProvider(Protocol):
    """Pluggable, async LLM enrichment boundary."""

    async def extract(
        self,
        *,
        domain: str,
        evidence: tuple[Evidence, ...],
        schema_version: str,
    ) -> LLMResult: ...


class MockLLMProvider:
    """A deterministic, offline provider used for tests and schema validation."""

    def __init__(
        self,
        *,
        draft: CandidateVersionDraft | None = None,
        reason: str | None = None,
    ) -> None:
        self._draft = draft
        self._reason = reason
        self.calls: list[tuple[str, tuple[Evidence, ...], str]] = []

    async def extract(
        self,
        *,
        domain: str,
        evidence: tuple[Evidence, ...],
        schema_version: str,
    ) -> LLMResult:
        self.calls.append((domain, evidence, schema_version))
        if self._draft is None:
            return LLMResult(state=LLMResultState.NEEDS_REVIEW, reason=self._reason or "mocked")
        return LLMResult(state=LLMResultState.READY, draft=self._draft)


class OpenAICompatibleProvider:
    """Provider that talks to an OpenAI-compatible ``/v1/chat/completions`` endpoint."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        model: str,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 20.0,
    ) -> None:
        if not base_url.strip() or not token.strip():
            raise ValueError("OpenAI-compatible base_url and token must not be empty")
        if not model.strip():
            raise ValueError("model must not be empty")
        normalized = base_url.rstrip("/")
        normalized = normalized.removesuffix("/v1")  # tolerate base_url already including /v1
        self._client = httpx.AsyncClient(
            base_url=normalized,
            timeout=timeout,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )
        self._model = model

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def extract(
        self,
        *,
        domain: str,
        evidence: tuple[Evidence, ...],
        schema_version: str,
    ) -> LLMResult:
        """Call the chat completions endpoint and parse the resulting JSON."""
        messages = [
            {
                "role": "system",
                "content": _SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": _build_user_prompt(
                    domain=domain, evidence=evidence, schema_version=schema_version
                ),
            },
        ]
        try:
            response = await self._client.post(
                "/v1/chat/completions",
                json={"model": self._model, "messages": messages, "temperature": 0.0},
            )
        except httpx.HTTPError as error:
            return LLMResult(
                state=LLMResultState.NEEDS_REVIEW,
                reason=f"http error: {error}",
            )
        if response.status_code != 200:
            return LLMResult(
                state=LLMResultState.NEEDS_REVIEW,
                reason=f"http {response.status_code}: {response.text[:200]}",
            )
        try:
            payload: Any = response.json()
        except json.JSONDecodeError as error:
            return LLMResult(
                state=LLMResultState.NEEDS_REVIEW,
                reason=f"invalid JSON: {error}",
            )
        content = _extract_assistant_content(payload)
        if content is None:
            return LLMResult(
                state=LLMResultState.NEEDS_REVIEW,
                reason="response missing assistant message content",
            )
        parsed = parse_llm_candidate_output(content, allowed_evidence=evidence)
        return _to_llm_result(parsed)


def _to_llm_result(parsed: LLMParseResult) -> LLMResult:
    if parsed.state is LLMResultState.READY:
        return LLMResult(state=parsed.state, draft=parsed.draft)
    return LLMResult(state=parsed.state, reason=parsed.reason)


def _extract_assistant_content(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0]
    if not isinstance(first, dict):
        return None
    message = first.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if not isinstance(content, str):
        return None
    return _strip_thinking(content)


def _strip_thinking(content: str) -> str:
    """Extract the JSON payload from a reasoning-model response.

    Reasoning models (MiniMax-M3, DeepSeek-R1, ...) interleave a
    ``thinking`` section with the actual answer. The answer may be a
    ```json ... ``` fenced block, or plain JSON at the tail (no fence).
    Fall back to the raw content when no JSON can be isolated.
    """
    fenced = _extract_fenced_json(content)
    if fenced is not None:
        return fenced
    tail = _extract_tail_json(content)
    if tail is not None:
        return tail
    return content


def _extract_fenced_json(content: str) -> str | None:
    """Return the last ```json ... ``` block in ``content``, or None."""
    marker = "```json"
    start = content.rfind(marker)
    if start < 0:
        return None
    body_start = start + len(marker)
    end = content.find("```", body_start)
    if end < 0:
        return None
    return content[body_start:end].strip()


def _extract_tail_json(content: str) -> str | None:
    """Return the last ``{...}`` object in ``content``, or None.

    Used for responses where the model emitted plain JSON after a
    thinking block without any markdown fence.
    """
    start = content.rfind("{")
    while start >= 0:
        try:
            json.loads(content[start:])
            return content[start:]
        except (ValueError, TypeError):
            start = content.rfind("{", 0, start)
    return None


def _build_user_prompt(
    *, domain: str, evidence: tuple[Evidence, ...], schema_version: str
) -> str:
    evidence_lines = [
        f"- type={e.evidence_type.value} url={e.url or ''} quote={e.quote!r}"
        for e in evidence
    ]
    return (
        f"domain: {domain}\n"
        f"schema_version: {schema_version}\n"
        f"taxonomy_version: {TAXONOMY_VERSION}\n"
        f"allowed_categories: {sorted(ALLOWED_CATEGORIES)}\n"
        f"evidence:\n" + "\n".join(evidence_lines)
    )


_SCHEMA_EXAMPLE = """{
  "is_candidate": true,
  "classification_confidence": 0.8,
  "primary_outcome_suggestion": "publishable_ai_saas",
  "rejection_reasons": [],
  "name_suggestion": "Example AI",
  "description_suggestion": "AI workflow automation for teams.",
  "category": "automation",
  "tags": ["ai", "workflow"],
  "pricing_model": "subscription",
  "target_audience": "SMBs",
  "evidence": [
    {"type": "title", "url": "https://example.com", "quote": "Example AI"}
  ],
  "model_version": "provider-model-1"
}
"""

_SCHEMA_RULES = (
    "\nRules:\n"
    "- primary_outcome_suggestion must be one of: "
    "publishable_ai_saas, valid_but_not_ready, not_target, "
    "duplicate_or_existing, policy_excluded.\n"
    "- publishable_ai_saas: real, running AI SaaS product with working features.\n"
    "- valid_but_not_ready: real site but not yet a usable AI SaaS (coming-soon, "
    "placeholder, sparse content).\n"
    "- not_target: not an AI product at all (movers, clinics, spam pages).\n"
    "- category must be one of: ai_assistant, automation, content_generation, "
    "customer_support, data_analysis, developer_tools, other.\n"
    "- evidence items must use only the evidence types and urls given in the "
    "user message (type/url/quote triplets from the evidence list).\n"
    "- is_candidate: true only when primary_outcome_suggestion is "
    "publishable_ai_saas.\n"
    "- rejection_reasons: list of non-empty strings when is_candidate is false.\n"
    "- model_version: your own model identifier (e.g. the model name you "
    "are running as), never a placeholder.\n"
    "- Reply with the JSON object only. No markdown fences, no commentary."
)

_SYSTEM_PROMPT = (
    "You are an evidence-restricted product classifier. "
    "You may only cite evidence drawn from the user message. "
    "Reply with strict JSON only, no prose, matching exactly this schema:\n"
    + _SCHEMA_EXAMPLE
    + _SCHEMA_RULES
)
