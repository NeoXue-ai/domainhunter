"""LLM provider protocol, MockLLMProvider, and OpenAI-compatible HTTP transport."""

import asyncio
import json
from datetime import UTC, datetime

import httpx

from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.events import SourceEvent
from domainhunter.llm.provider import (
    LLMProvider,
    MockLLMProvider,
    OpenAICompatibleProvider,
    _extract_fenced_json,
    _extract_tail_json,
    _strip_thinking,
)
from domainhunter.llm.schema import LLMResultState
from domainhunter.pipeline import DomainHunterPipeline, enrich_candidate_with_llm
from domainhunter.storage.sqlite import SQLiteStore

NOW = datetime(2026, 8, 17, tzinfo=UTC)
EVIDENCE = (
    Evidence(EvidenceType.H1, "Automate your AI workflows", "https://example.com"),
)


def _draft() -> CandidateVersionDraft:
    return CandidateVersionDraft(
        author_kind="llm",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.91,
        name_suggestion="Example AI",
        description_suggestion="AI workflow automation.",
        category="automation",
        tags=("workflow",),
        pricing_model="paid",
        target_audience="ops teams",
        evidence=EVIDENCE,
        model_version="mock-1",
    )


async def _public_resolver(hostname: str) -> tuple[str, ...]:
    return ("1.1.1.1",)


def test_mock_provider_returns_a_known_candidate_draft() -> None:
    draft = _draft()
    provider = MockLLMProvider(draft=draft)

    async def run():
        return await provider.extract(
            domain="example.com",
            evidence=EVIDENCE,
            schema_version="domainhunter-taxonomy-v1",
        )

    result = asyncio.run(run())
    assert result.state is LLMResultState.READY
    assert result.draft == draft


def test_mock_provider_can_be_constructed_with_needs_review_reason() -> None:
    provider = MockLLMProvider(reason="model is unavailable")

    async def run():
        return await provider.extract(
            domain="example.com",
            evidence=EVIDENCE,
            schema_version="domainhunter-taxonomy-v1",
        )

    result = asyncio.run(run())
    assert result.state is LLMResultState.NEEDS_REVIEW
    assert result.reason == "model is unavailable"


def test_openai_compatible_provider_posts_to_chat_completions_and_parses_response() -> None:
    requests: list[httpx.Request] = []

    response_payload = {
        "choices": [
            {
                "message": {
                    "content": json.dumps(
                        {
                            "is_candidate": True,
                            "classification_confidence": 0.91,
                            "primary_outcome_suggestion": "publishable_ai_saas",
                            "rejection_reasons": [],
                            "name_suggestion": "Example AI",
                            "description_suggestion": "AI workflow automation.",
                            "category": "automation",
                            "tags": ["workflow"],
                            "pricing_model": "paid",
                            "target_audience": "ops teams",
                            "evidence": [
                                {
                                    "type": "h1",
                                    "url": "https://example.com",
                                    "quote": "Automate your AI workflows",
                                }
                            ],
                            "model_version": "gpt-test-1",
                        }
                    )
                }
            }
        ]
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=response_payload)

    provider = OpenAICompatibleProvider(
        base_url="https://llm.example.test/v1",
        token="sk-test",
        model="gpt-test-1",
        transport=httpx.MockTransport(handler),
    )

    async def run():
        return await provider.extract(
            domain="example.com",
            evidence=EVIDENCE,
            schema_version="domainhunter-taxonomy-v1",
        )

    result = asyncio.run(run())
    assert result.state is LLMResultState.READY
    assert result.draft is not None
    assert result.draft.name_suggestion == "Example AI"

    assert len(requests) == 1
    sent = json.loads(requests[0].content)
    assert sent["model"] == "gpt-test-1"
    assert sent["messages"][0]["role"] == "system"
    assert sent["messages"][1]["role"] == "user"
    assert requests[0].headers["authorization"] == "Bearer sk-test"


def test_openai_compatible_provider_maps_non_2xx_to_needs_review() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="model overloaded")

    provider = OpenAICompatibleProvider(
        base_url="https://llm.example.test/v1",
        token="sk-test",
        model="gpt-test-1",
        transport=httpx.MockTransport(handler),
    )

    async def run():
        return await provider.extract(
            domain="example.com",
            evidence=EVIDENCE,
            schema_version="domainhunter-taxonomy-v1",
        )

    result = asyncio.run(run())
    assert result.state is LLMResultState.NEEDS_REVIEW
    assert "500" in (result.reason or "")


def test_openai_compatible_provider_maps_timeout_to_needs_review() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow model", request=request)

    provider = OpenAICompatibleProvider(
        base_url="https://llm.example.test/v1",
        token="sk-test",
        model="gpt-test-1",
        transport=httpx.MockTransport(handler),
    )

    async def run():
        return await provider.extract(
            domain="example.com",
            evidence=EVIDENCE,
            schema_version="domainhunter-taxonomy-v1",
        )

    result = asyncio.run(run())
    assert result.state is LLMResultState.NEEDS_REVIEW
    assert "http error" in (result.reason or "")
    assert "slow model" in (result.reason or "")


def test_pipeline_enriches_a_candidate_using_an_injected_provider(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    html = (
        "<title>Example AI Workflow</title>"
        "<meta name=\"description\" content=\"AI workflow automation platform with a free trial\">"
        "<h1>Automate your AI workflows</h1>"
        "<p>" + ("Useful product text. " * 30) + "</p>"
    )
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    draft = _draft()
    provider: LLMProvider = MockLLMProvider(draft=draft)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver, transport=transport, respect_robots=False
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            event = SourceEvent("ct_log", "argon:42", "example.com", NOW)
            pipeline.ingest_event(event)
            probe_run = await pipeline.probe_domain("example.com", observed_at=NOW)
            assert probe_run.candidate_version is not None
            candidate_id = probe_run.candidate_version.candidate_id
            version = probe_run.candidate_version.version

            persisted = await enrich_candidate_with_llm(
                store=store,
                candidate_id=candidate_id,
                candidate_version=version,
                provider=provider,
                observed_at=NOW,
            )

        assert persisted is not None
        assert persisted.primary_outcome is CandidateOutcome.PUBLISHABLE_AI_SAAS
        assert persisted.evidence == EVIDENCE

        versions = store.list_candidate_versions(candidate_id)
        assert len(versions) == 2
        assert versions[-1].draft.model_version == "mock-1"
        assert versions[-1].draft.author_kind == "llm"

    asyncio.run(run())


def test_pipeline_enrichment_returns_none_when_provider_needs_review(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    html = (
        "<title>Example AI Workflow</title>"
        "<meta name=\"description\" content=\"AI workflow automation platform with a free trial\">"
        "<h1>Automate your AI workflows</h1>"
        "<p>" + ("Useful product text. " * 30) + "</p>"
    )
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    provider = MockLLMProvider(reason="model timed out")

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver, transport=transport, respect_robots=False
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(SourceEvent("ct_log", "argon:42", "example.com", NOW))
            probe_run = await pipeline.probe_domain("example.com", observed_at=NOW)
            assert probe_run.candidate_version is not None
            draft = await enrich_candidate_with_llm(
                store=store,
                candidate_id=probe_run.candidate_version.candidate_id,
                candidate_version=probe_run.candidate_version.version,
                provider=provider,
                observed_at=NOW,
            )

        assert draft is None
        versions = store.list_candidate_versions(probe_run.candidate_version.candidate_id)
        # Only the original rule-authored version remains.
        assert len(versions) == 1
        assert versions[0].draft.author_kind == "rule"

    asyncio.run(run())

def test_extract_fenced_json() -> None:
    content = (
        "thinking about it...\n\n"
        "```json\n{\"a\": 1}\n```\n"
        "trailing prose"
    )
    assert _extract_fenced_json(content) == '{"a": 1}'


def test_extract_fenced_json_absent() -> None:
    assert _extract_fenced_json("just prose here") is None


def test_extract_tail_json() -> None:
    content = (
        "thinking...\n\n"
        '{"is_candidate": false, "reason": "coming soon"}'
    )
    assert _extract_tail_json(content) == (
        '{"is_candidate": false, "reason": "coming soon"}'
    )


def test_extract_tail_json_absent() -> None:
    assert _extract_tail_json("no json at all") is None


def test_strip_thinking_prefers_fence() -> None:
    content = (
        "thinking...\n\n"
        "```json\n{\"a\": 1}\n```"
    )
    assert _strip_thinking(content) == '{"a": 1}'


def test_strip_thinking_falls_back_to_tail() -> None:
    content = "thinking...\n\n" + '{"a": 1}'
    assert _strip_thinking(content) == '{"a": 1}'


def test_strip_thinking_returns_raw_when_no_json() -> None:
    content = "no json here"
    assert _strip_thinking(content) == "no json here"


def test_openai_compatible_provider_tolerates_v1_base_url(monkeypatch) -> None:
    """base_url ending in /v1 must not produce a double-/v1 request path."""

    captured: dict[str, object] = {}

    def fake_post(self, url, **kwargs):
        captured["url"] = url
        return httpx.Response(200, json={"choices": [{"message": {"content": "hi"}}]})

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    provider = OpenAICompatibleProvider(
        base_url="https://api.minimax.chat/v1",
        token="test-token",
        model="MiniMax-M3",
    )
    assert provider._client.base_url == "https://api.minimax.chat"
