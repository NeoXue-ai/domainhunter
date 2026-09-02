"""Tests for the S5 batch enrichment funnel (filter → probe → LLM)."""

import asyncio
from datetime import UTC, datetime

import httpx

from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.filter.enrich import run_batch
from domainhunter.filter.pipeline import FilteredCandidate
from domainhunter.filter.rdap_age import AgeVerdict
from domainhunter.filter.static_signals import DomainScore, compute_signals
from domainhunter.llm.provider import MockLLMProvider
from domainhunter.storage.sqlite import SQLiteStore

NOW = datetime(2026, 8, 17, tzinfo=UTC)

EVIDENCE = (
    Evidence(EvidenceType.H1, "AI workflow automation", "https://new.com"),
)


def _llm_draft() -> CandidateVersionDraft:
    return CandidateVersionDraft(
        author_kind="llm",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.9,
        name_suggestion="New AI",
        description_suggestion="AI automation",
        category="automation",
        tags=("ai",),
        pricing_model=None,
        target_audience=None,
        evidence=EVIDENCE,
        model_version="mock-1",
    )


def _fake_candidate(domain: str, tier: str = "tier1", age_days: int = 5) -> FilteredCandidate:
    return FilteredCandidate(
        domain=domain,
        s1=DomainScore(domain=domain, score=0.9, signals=compute_signals(domain)),
        s2=AgeVerdict(
            domain=domain, tier=tier, age_days=age_days, reason=f"registered {age_days}d ago"
        ),
        s3=None,
        final_tier=tier,
        reason="new",
        observed_at=NOW,
    )


class FakeFilterPipeline:
    def __init__(self, kept: set[str]) -> None:
        self._kept = kept

    def run(self, domains: list[str], *, observed_at=None) -> tuple[FilteredCandidate, ...]:
        return tuple(
            _fake_candidate(d) for d in domains if d in self._kept
        )


def _html_page(domain: str) -> str:
    return (
        "<html><head><title>AI workflow automation</title>"
        "<meta name=\"description\" content=\"Automate your AI workflows\"></head>"
        "<body><h1>Automate your AI workflows</h1>"
        "<p>" + ("Useful product text. " * 30) + "</p></body></html>"
    )


def test_run_batch_enriches_survivors(tmp_path) -> None:
    """A survivor gets a rule draft + LLM draft persisted; dropped domains reported."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text=_html_page("new.com"))
    )

    async def resolver(hostname: str) -> tuple[str, ...]:
        return ("1.1.1.1",)

    async def run() -> tuple:
        async with HTTPProbe(
            resolver=resolver, transport=transport, respect_robots=False
        ) as probe:
            return await run_batch(
                store=store,
                domains=["new.com", "old.com"],
                provider=MockLLMProvider(draft=_llm_draft()),
                pipeline=FakeFilterPipeline(kept={"new.com"}),
                probe=probe,
                observed_at=NOW,
            )

    outcomes = asyncio.run(run())
    by_domain = {o.domain: o for o in outcomes}
    assert by_domain["new.com"].stage == "enriched"
    assert by_domain["new.com"].llm_outcome == "publishable_ai_saas"
    assert by_domain["old.com"].stage == "filtered_out"

    import sqlite3

    db = sqlite3.connect(database)
    candidate_id = db.execute(
        "SELECT candidate_id FROM candidates WHERE domain='new.com'"
    ).fetchone()[0]
    versions = store.list_candidate_versions(candidate_id)
    assert [v.draft.author_kind for v in versions] == ["rule", "llm"]


def test_run_batch_reports_probe_failure(tmp_path) -> None:
    """A domain that probes as dead gets probe_failed, not enriched."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)

    async def resolver(hostname: str) -> tuple[str, ...]:
        return ("1.1.1.1",)

    async def run() -> None:
        async with HTTPProbe(
            resolver=resolver, transport=httpx.MockTransport(lambda r: httpx.Response(500))
        ) as probe:
            outcomes = await run_batch(
                store=store,
                domains=["new.com"],
                provider=MockLLMProvider(draft=_llm_draft()),
                pipeline=FakeFilterPipeline(kept={"new.com"}),
                probe=probe,
                observed_at=NOW,
            )
            assert outcomes[0].stage == "probe_failed"

    asyncio.run(run())


def test_run_batch_reports_llm_skip(tmp_path) -> None:
    """When the LLM fails the schema gate, stage is llm_skipped."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text=_html_page("new.com"))
    )

    async def resolver(hostname: str) -> tuple[str, ...]:
        return ("1.1.1.1",)

    async def run() -> None:
        async with HTTPProbe(
            resolver=resolver, transport=transport, respect_robots=False
        ) as probe:
            outcomes = await run_batch(
                store=store,
                domains=["new.com"],
                provider=MockLLMProvider(reason="model refused"),
                pipeline=FakeFilterPipeline(kept={"new.com"}),
                probe=probe,
                observed_at=NOW,
            )
            assert outcomes[0].stage == "llm_skipped"

    asyncio.run(run())