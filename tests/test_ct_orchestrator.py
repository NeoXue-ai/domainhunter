"""Integration tests for the CT → candidate orchestrator.

The poller is fed by a hand-rolled fake fetcher (no httpx), so these tests
prove the wiring between ``CTPoller`` → ``SQLiteStore`` →
``DomainHunterPipeline.probe_domain`` without touching the real network.

The probe is a fake that returns canned ``L1Analysis`` values per hostname
so we can deterministically exercise both the publishable and the
no-candidate paths.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Mapping

from domainhunter.crawler.http_probe import ProbeResult
from domainhunter.crawler.l1_analysis import L1Analysis
from domainhunter.domain.observations import OutcomeCode
from domainhunter.filter.dns_check import DnsResult
from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.filter.rdap_age import MemoryCache, Registration
from domainhunter.ingest.ct_events import build_ct_events
from domainhunter.ingest.ct_orchestrator import CTIngestOrchestrator
from domainhunter.ingest.ct_poller import CTCertificate, CTPage, CTPoller
from domainhunter.pipeline import DomainHunterPipeline
from domainhunter.storage.sqlite import SQLiteStore


_OBSERVED = datetime(2026, 8, 20, 6, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class _FakeProbe:
    """In-memory probe: each hostname maps to a canned L1Analysis."""

    responses: Mapping[str, L1Analysis]

    async def probe(self, hostname: str) -> ProbeResult:
        analysis = self.responses.get(hostname)
        if analysis is None:
            return ProbeResult(
                outcome_code=OutcomeCode.DNS_NOT_FOUND,
                final_url=None,
                analysis=None,
                detail="no canned response",
            )
        return ProbeResult(
            outcome_code=analysis.outcome_code,
            final_url=analysis.final_url,
            analysis=analysis,
        )


@dataclass(frozen=True, slots=True)
class _FilteredRoot:
    """Minimal filter result carrying the domain selected for probing."""

    domain: str


class _FakeFilterPipeline:
    """Records the strict-mode inputs and returns only its configured roots."""

    def __init__(self, kept_domains: set[str]) -> None:
        self._kept_domains = kept_domains
        self.calls: list[tuple[tuple[str, ...], datetime]] = []

    def run(
        self, domains: list[str], *, observed_at: datetime | None = None
    ) -> tuple[_FilteredRoot, ...]:
        assert observed_at is not None
        self.calls.append((tuple(domains), observed_at))
        return tuple(
            _FilteredRoot(domain)
            for domain in domains
            if domain in self._kept_domains
        )


def _ai_publishable(domain: str) -> L1Analysis:
    return L1Analysis(
        outcome_code=OutcomeCode.SUCCESS,
        final_url=f"https://{domain}",
        title=f"AI Platform for {domain}",
        meta_description="AI assistant platform with free trial pricing.",
        text_length=1200,
        is_parking_page=False,
        status_code=200,
    )


def _empty_success(domain: str) -> L1Analysis:
    return L1Analysis(
        outcome_code=OutcomeCode.SUCCESS,
        final_url=f"https://{domain}",
        title=None,
        meta_description=None,
        text_length=80,
        is_parking_page=False,
        status_code=200,
    )


def _cert(cert_id: str, hostname: str) -> CTCertificate:
    cert_data = {
        "leaf_cert": {
            "subject": {"CN": hostname},
            "all_domains": [hostname],
            "issuer": "Test CA",
        }
    }
    events = build_ct_events(cert_data, cert_id, _OBSERVED)
    return CTCertificate(
        source_event_id=cert_id,
        certificate=cert_data,
        observed_at=_OBSERVED,
    )


def _make_poller(store: SQLiteStore, pages: tuple[CTPage, ...]) -> CTPoller:
    queue = list(pages)

    async def fetch(cursor: str | None) -> CTPage:
        if not queue:
            return CTPage(entries=(), next_cursor=cursor)
        return queue.pop(0)

    return CTPoller(store=store, fetch_page=fetch)


def _run(coro):
    import asyncio

    return asyncio.run(coro)


def test_poll_then_probe_creates_publishable_candidate(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    page = CTPage(
        entries=(
            _cert("c1", "nova-ai.com"),
            _cert("c2", "brightcanvas.io"),
        ),
        next_cursor="c2",
    )
    poller = _make_poller(store, (page,))
    probe = _FakeProbe(
        {
            "nova-ai.com": _ai_publishable("nova-ai.com"),
            "brightcanvas.io": _ai_publishable("brightcanvas.io"),
        }
    )
    pipeline = DomainHunterPipeline(store=store, probe=probe)  # type: ignore[arg-type]
    orchestrator = CTIngestOrchestrator(
        store=store, poller=poller, pipeline=pipeline, probe_limit=10
    )

    async def go() -> None:
        summary = await orchestrator.run_once(observed_at=_OBSERVED)
        assert summary.certificates_seen == 2
        assert summary.events_added == 2  # 2 certs × 1 hostname each
        assert summary.probes_run == 2
        assert summary.candidates_created == 2
        assert summary.next_cursor == "c2"

    _run(go())
    domains = set(store.list_domains())
    assert domains == {"nova-ai.com", "brightcanvas.io"}
    queue = store.list_review_queue()
    queue_domains = {item.candidate.domain for item in queue}
    assert queue_domains == domains
    for item in queue:
        assert item.latest_version is not None
        assert item.priority is not None


def test_respects_probe_limit(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    page = CTPage(
        entries=tuple(_cert(f"c{i}", f"site-{i}.ai") for i in range(5)),
        next_cursor="c4",
    )
    poller = _make_poller(store, (page,))
    probe = _FakeProbe({})
    pipeline = DomainHunterPipeline(store=store, probe=probe)  # type: ignore[arg-type]
    orchestrator = CTIngestOrchestrator(
        store=store, poller=poller, pipeline=pipeline, probe_limit=3
    )

    async def go() -> None:
        summary = await orchestrator.run_once(observed_at=_OBSERVED)
        assert summary.probes_run == 3
        assert summary.candidates_created == 0  # all probes returned no candidate
        assert summary.events_added == 5

    _run(go())


def test_strict_mode_filters_before_probe_and_marks_rejected_roots_seen(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    page = CTPage(
        entries=(
            _cert("c1", "azure.com"),
            _cert("c2", "new-ai.dev"),
        ),
        next_cursor="c2",
    )
    poller = _make_poller(store, (page,))
    probe = _FakeProbe(
        {
            "azure.com": _ai_publishable("azure.com"),
            "new-ai.dev": _ai_publishable("new-ai.dev"),
        }
    )
    pipeline = DomainHunterPipeline(store=store, probe=probe)  # type: ignore[arg-type]
    strict_filter = _FakeFilterPipeline({"new-ai.dev"})
    orchestrator = CTIngestOrchestrator(
        store=store,
        poller=poller,
        pipeline=pipeline,
        filter_pipeline=strict_filter,
        require_first_seen=True,
        probe_limit=10,
    )

    async def go() -> None:
        summary = await orchestrator.run_once(observed_at=_OBSERVED)
        assert summary.probes_run == 1
        assert summary.candidates_created == 1

    _run(go())
    assert strict_filter.calls == [(("azure.com", "new-ai.dev"), _OBSERVED)]
    assert store.is_seen("azure.com") is True
    assert store.is_seen("new-ai.dev") is True
    assert {item.candidate.domain for item in store.list_review_queue()} == {"new-ai.dev"}


def test_strict_mode_never_probes_a_domain_dropped_by_rdap_age(tmp_path) -> None:
    """A newly observed certificate cannot make an old registration a candidate."""
    store = SQLiteStore(tmp_path / "domainhunter.db")
    poller = _make_poller(
        store,
        (
            CTPage(
                entries=(_cert("microsoft-certificate", "azure.com"),),
                next_cursor="microsoft-certificate",
            ),
        ),
    )
    old_registration = Registration(
        domain="azure.com",
        registration_date=datetime.now(UTC) - timedelta(days=365 * 20),
        registrar="Example Registrar",
        statuses=(),
    )
    strict_filter = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=lambda _domain: old_registration,
        dns_checker=lambda domains: {
            domain: DnsResult(domain=domain, has_a=True, addresses=("1.2.3.4",))
            for domain in domains
        },
        require_dns=True,
        drop_unknown_rdap=True,
    )
    pipeline = DomainHunterPipeline(store=store, probe=_FakeProbe({}))  # type: ignore[arg-type]
    orchestrator = CTIngestOrchestrator(
        store=store,
        poller=poller,
        pipeline=pipeline,
        filter_pipeline=strict_filter,
        require_first_seen=True,
        probe_limit=10,
    )

    summary = _run(orchestrator.run_once(observed_at=_OBSERVED))

    assert summary.probes_run == 0
    assert summary.candidates_created == 0
    assert store.list_review_queue() == ()
    assert store.is_seen("azure.com") is True


def test_idempotent_on_replay(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    page = CTPage(
        entries=(_cert("c1", "nova-ai.com"),),
        next_cursor="c1",
    )
    poller = _make_poller(store, (page, page))
    probe = _FakeProbe({"nova-ai.com": _ai_publishable("nova-ai.com")})
    pipeline = DomainHunterPipeline(store=store, probe=probe)  # type: ignore[arg-type]
    orchestrator = CTIngestOrchestrator(
        store=store, poller=poller, pipeline=pipeline, probe_limit=10
    )

    async def go() -> None:
        first = await orchestrator.run_once(observed_at=_OBSERVED)
        replay = await orchestrator.run_once(observed_at=_OBSERVED)
        assert first.candidates_created == 1
        assert first.probes_run == 1
        assert replay.certificates_seen == 1
        assert replay.events_added == 0
        assert replay.probes_run == 0
        assert replay.candidates_created == 0

    _run(go())


def test_skips_domains_with_no_candidate(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    page = CTPage(
        entries=(
            _cert("c1", "with-content.com"),
            _cert("c2", "no-content.com"),
        ),
        next_cursor="c2",
    )
    poller = _make_poller(store, (page,))
    probe = _FakeProbe(
        {
            "with-content.com": _ai_publishable("with-content.com"),
            "no-content.com": _empty_success("no-content.com"),
        }
    )
    pipeline = DomainHunterPipeline(store=store, probe=probe)  # type: ignore[arg-type]
    orchestrator = CTIngestOrchestrator(
        store=store, poller=poller, pipeline=pipeline, probe_limit=10
    )

    async def go() -> None:
        summary = await orchestrator.run_once(observed_at=_OBSERVED)
        assert summary.probes_run == 2
        assert summary.candidates_created == 1

    _run(go())


def test_empty_page_returns_zero_summary(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    poller = _make_poller(store, (CTPage(entries=(), next_cursor=None),))
    probe = _FakeProbe({})
    pipeline = DomainHunterPipeline(store=store, probe=probe)  # type: ignore[arg-type]
    orchestrator = CTIngestOrchestrator(
        store=store, poller=poller, pipeline=pipeline, probe_limit=10
    )

    async def go() -> None:
        summary = await orchestrator.run_once(observed_at=_OBSERVED)
        assert summary.certificates_seen == 0
        assert summary.events_added == 0
        assert summary.probes_run == 0
        assert summary.candidates_created == 0

    _run(go())
