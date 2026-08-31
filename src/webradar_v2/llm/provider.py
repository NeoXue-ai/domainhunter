"""Provider-agnostic LLM enrichment boundary used by the Fast/Detail Tier pipeline."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any, Awaitable, Callable, Optional, Protocol

import httpx

from webradar_v2.domain.candidates import (
    Evidence,
    EvidenceType,
    CandidateVersionDraft,
    CandidateOutcome,
)
from webradar_v2.llm.schema import (
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
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )
        self._model = model

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "OpenAICompatibleProvider":
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
        except httpx.TimeoutException as error:
            return LLMResult(
                state=LLMResultState.NEEDS_REVIEW,
                reason=f"timeout: {error}",
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
    return content


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


_SYSTEM_PROMPT = (
    "You are an evidence-restricted product classifier. "
    "You may only cite evidence drawn from the user message. "
    "Reply with strict JSON that matches the schema shown to you."
)
