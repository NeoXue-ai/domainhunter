import asyncio
from datetime import UTC, datetime

import httpx

from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.domain.events import SourceEvent
from domainhunter.domain.observations import OutcomeCode
from domainhunter.pipeline import DomainHunterPipeline
from domainhunter.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


async def _public_resolver(hostname: str) -> tuple[str, ...]:
    return ("1.1.1.1",)


def test_ingests_and_records_a_probe_with_a_retry_decision(tmp_path) -> None:
    html = "<title>Example AI</title><p>" + ("Useful product text. " * 60) + "</p>"
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            assert pipeline.ingest_event(event) is True
            result = await pipeline.probe_domain("www.example.com", observed_at=OBSERVED_AT)

        assert result.observation.domain == "example.com"
        assert result.observation.outcome_code is OutcomeCode.SUCCESS
        assert result.observation.attempt_number == 1
        assert result.observation.status_code == 200
        assert result.retry_decision.next_check_at is None
        assert store.list_observations("example.com") == (result.observation,)

    asyncio.run(run())


def test_probes_only_domains_due_under_the_persisted_retry_policy(tmp_path) -> None:
    html = "<title>Example AI</title><p>" + ("Useful product text. " * 60) + "</p>"
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(event)

            first_runs = await pipeline.probe_due_domains(observed_at=OBSERVED_AT)
            second_runs = await pipeline.probe_due_domains(
                observed_at=OBSERVED_AT.replace(hour=1)
            )

        assert len(first_runs) == 1
        assert first_runs[0].observation.domain == "example.com"
        assert second_runs == ()

    asyncio.run(run())


def test_projects_a_successful_product_probe_into_a_cited_review_candidate(tmp_path) -> None:
    html = (
        "<title>Example AI — Workflow automation</title>"
        '<meta name="description" content="AI automation platform with a free trial">'
        "<p>" + ("Automate operations with AI workflows. " * 30) + "</p>"
    )
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(event)
            result = await pipeline.probe_domain("example.com", observed_at=OBSERVED_AT)

        assert result.candidate_version is not None
        queue = store.list_review_queue()
        assert queue[0].latest_version == result.candidate_version
        assert queue[0].latest_version.draft.evidence[0].quote.startswith("Example AI")

    asyncio.run(run())


def test_strict_probe_keeps_observation_but_rejects_cross_root_redirect_candidate(tmp_path) -> None:
    html = (
        "<title>Old Site</title>"
        '<meta name="description" content="Existing product platform">'
        "<p>" + ("Existing product content. " * 40) + "</p>"
    )

    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.host == "fresh-redirect.com":
            return httpx.Response(
                301,
                headers={"location": "https://old-site.com/"},
                request=request,
            )
        return httpx.Response(200, text=html, request=request)

    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "nimbus:1", "fresh-redirect.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=httpx.MockTransport(responder),
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(event)
            result = await pipeline.probe_domain(
                "fresh-redirect.com",
                observed_at=OBSERVED_AT,
                require_same_final_root=True,
            )

        assert result.observation.final_url == "https://old-site.com/"
        assert result.candidate_version is None
        assert store.list_review_queue() == ()

    asyncio.run(run())


def test_strict_probe_rejects_cross_root_canonical_candidate(tmp_path) -> None:
    html = (
        '<link rel="canonical" href="https://old-site.com/">'
        "<title>Old AI Site</title>"
        '<meta name="description" content="AI product platform with a free trial">'
        "<p>" + ("Existing AI product content. " * 40) + "</p>"
    )
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "nimbus:canonical", "fresh-site.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, text=html, request=request)
            ),
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(event)
            result = await pipeline.probe_domain(
                "fresh-site.com",
                observed_at=OBSERVED_AT,
                require_same_final_root=True,
            )

        assert result.observation.final_url == "https://fresh-site.com"
        assert result.observation.canonical_url == "https://old-site.com/"
        assert result.candidate_version is None
        assert store.list_review_queue() == ()

    asyncio.run(run())


def test_strict_probe_allows_same_root_www_redirect_candidate(tmp_path) -> None:
    html = (
        "<title>Fresh AI</title>"
        '<meta name="description" content="AI product platform with a free trial">'
        "<p>" + ("Fresh AI product content. " * 40) + "</p>"
    )

    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.host == "fresh-site.com":
            return httpx.Response(
                301,
                headers={"location": "https://www.fresh-site.com/"},
                request=request,
            )
        return httpx.Response(200, text=html, request=request)

    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "nimbus:2", "fresh-site.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=httpx.MockTransport(responder),
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(event)
            result = await pipeline.probe_domain(
                "fresh-site.com",
                observed_at=OBSERVED_AT,
                require_same_final_root=True,
            )

        assert result.observation.final_url == "https://www.fresh-site.com/"
        assert result.candidate_version is not None
        assert {item.candidate.domain for item in store.list_review_queue()} == {"fresh-site.com"}

    asyncio.run(run())


def test_probe_domain_records_canonical_and_internal_links_on_observation(tmp_path) -> None:
    html = (
        "<link rel=\"canonical\" href=\"/canonical\">"
        "<title>Example AI</title>"
        "<p>" + ("Useful product text. " * 60) + "</p>"
        "<a href=\"/about\">About</a>"
        "<a href=\"https://www.example.com/pricing\">Pricing</a>"
    )
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=transport,
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(event)
            result = await pipeline.probe_domain("www.example.com", observed_at=OBSERVED_AT)

        assert result.observation.canonical_url == "https://www.example.com/canonical"
        assert "https://www.example.com/about" in result.observation.internal_links
        assert "https://www.example.com/pricing" in result.observation.internal_links

    asyncio.run(run())
