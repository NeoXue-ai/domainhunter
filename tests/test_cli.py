import json
from datetime import UTC, datetime
from typing import Self

from domainhunter import cli
from domainhunter.api import create_app
from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.reviews import ReviewAction, build_review_decision
from domainhunter.ingest.ct_poller import CTCertificate, CTPage
from domainhunter.storage.sqlite import SQLiteStore


def test_cli_polls_ct_log_and_reports_due_domain(tmp_path, capsys, monkeypatch) -> None:
    """poll-ct-log invokes the orchestrator, persisting events for due domains."""
    captured: dict[str, object] = {}

    class FakeFetcher:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def __call__(self, cursor: str | None) -> CTPage:
            observed = cli._parse_datetime("2026-08-16T00:00:00+00:00")
            return CTPage(
                entries=(
                    CTCertificate(
                        source_event_id="argon:42",
                        certificate={
                            "leaf_cert": {
                                "subject": {"CN": "app.example.com"},
                                "all_domains": ["app.example.com", "example.com"],
                            }
                        },
                        observed_at=observed,
                    ),
                ),
                next_cursor='{"argon": 43}',
            )

    class _FilteredRoot:
        def __init__(self, domain: str) -> None:
            self.domain = domain

    class FakeFilterPipeline:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def run(self, domains: list[str], **_kwargs) -> tuple[_FilteredRoot, ...]:
            captured["domains"] = tuple(domains)
            return ()

    monkeypatch.setattr(cli, "CTLogFetcher", FakeFetcher)
    monkeypatch.setattr(cli, "FilterPipeline", FakeFilterPipeline)
    database = tmp_path / "domainhunter.db"

    assert (
        cli.main(
            [
                "poll-ct-log",
                "--database",
                str(database),
                "--log",
                "argon=https://ct.example.test/logs/argon",
                "--max-probes",
                "5",
            ]
        )
        == 0
    )

    imported = json.loads(capsys.readouterr().out)
    assert imported["certificates_seen"] == 1
    assert imported["events_added"] == 2
    assert captured["domains"] == ("example.com",)
    assert captured["tier1_days"] == 30
    assert captured["tier2_days"] == 90
    assert captured["require_dns"] is True
    assert captured["drop_unknown_rdap"] is True

    assert (
        cli.main(
            ["status", "--database", str(database), "--at", "2026-08-16T00:00:00+00:00"]
        )
        == 0
    )
    status = json.loads(capsys.readouterr().out)
    assert "example.com" in status["domains"]


def test_cli_serves_the_review_api_from_an_explicit_database(tmp_path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeUvicorn:
        @staticmethod
        def run(app, *, host: str, port: int) -> None:
            captured.update(app=app, host=host, port=port)

    monkeypatch.setattr(cli, "uvicorn", FakeUvicorn, raising=False)

    assert (
        cli.main(
            [
                "serve",
                "--database",
                str(tmp_path / "domainhunter.db"),
                "--host",
                "127.0.0.1",
                "--port",
                "8765",
            ]
        )
        == 0
    )

    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8765


def test_cli_filter_runs_funnel_and_reports(tmp_path, capsys, monkeypatch) -> None:
    """filter invokes the S1→S2→S3 funnel and prints report + candidates."""

    class FakePipeline:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        def report(self, domains: list[str]) -> dict[str, object]:
            return {"input": len(domains), "kept": 1, "dropped": len(domains) - 1}

        def run(self, domains: list[str]) -> tuple:
            from domainhunter.filter.pipeline import FilteredCandidate
            from domainhunter.filter.rdap_age import AgeVerdict
            from domainhunter.filter.static_signals import DomainScore, compute_signals

            return (
                FilteredCandidate(
                    domain=domains[0],
                    s1=DomainScore(
                        domain=domains[0],
                        score=0.9,
                        signals=compute_signals(domains[0]),
                    ),
                    s2=AgeVerdict(
                        domain=domains[0], tier="tier1", age_days=3, reason="new"
                    ),
                    s3=None,
                    final_tier="tier1",
                    reason="new",
                    observed_at=datetime.now(UTC),
                ),
            )

    import domainhunter.filter.pipeline as filter_pipeline

    monkeypatch.setattr(filter_pipeline, "FilterPipeline", FakePipeline)
    input_file = tmp_path / "domains.json"
    input_file.write_text(json.dumps({"domains": ["new.com", "old.com"]}))

    assert (
        cli.main(
            ["filter", "--input", str(input_file), "--tier1-days", "30", "--require-dns"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["report"]["input"] == 2
    assert payload["report"]["kept"] == 1
    assert payload["candidates"][0]["final_tier"] == "tier1"


def test_cli_filter_probe_runs_funnel_and_probes(tmp_path, capsys, monkeypatch) -> None:
    """filter-probe runs the funnel then S4-probes survivors into the DB."""


    class FakePipeline:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        async def run_with_probe(self, domains, **kwargs):
            from domainhunter.filter.pipeline import FilteredCandidate
            from domainhunter.filter.rdap_age import AgeVerdict
            from domainhunter.filter.static_signals import DomainScore, compute_signals

            return (
                FilteredCandidate(
                    domain=domains[0],
                    s1=DomainScore(
                        domain=domains[0], score=0.9, signals=compute_signals(domains[0])
                    ),
                    s2=AgeVerdict(
                        domain=domains[0], tier="tier1", age_days=3, reason="new"
                    ),
                    s3=None,
                    final_tier="tier1",
                    reason="new",
                    observed_at=datetime.now(UTC),
                    probe={"outcome": "success", "status_code": 200},
                ),
            )

    import domainhunter.filter.pipeline as filter_pipeline

    monkeypatch.setattr(filter_pipeline, "FilterPipeline", FakePipeline)
    input_file = tmp_path / "domains.json"
    input_file.write_text(json.dumps({"domains": ["new.com"]}))

    assert (
        cli.main(
            [
                "filter-probe",
                "--database",
                str(tmp_path / "domainhunter.db"),
                "--input",
                str(input_file),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["candidates"][0]["final_tier"] == "tier1"
    assert payload["candidates"][0]["probe"]["outcome"] == "success"


def test_cli_filter_enrich_runs_full_funnel(tmp_path, capsys, monkeypatch) -> None:
    """filter-enrich runs the full S1→S5 funnel with the mock provider."""

    class FakeOutcome:
        def __init__(self, domain: str) -> None:
            self.domain = domain
            self.stage = "enriched"
            self.final_tier = "tier1"
            self.s1_score = 0.9
            self.age_days = 3
            self.probe_outcome = "success"
            self.llm_outcome = "publishable_ai_saas"
            self.llm_confidence = 0.9
            self.llm_model = "mock-1"
            self.reason = "LLM classification persisted"

        def as_payload(self) -> dict[str, object]:
            return {
                "domain": self.domain,
                "stage": self.stage,
                "final_tier": self.final_tier,
                "s1_score": self.s1_score,
                "age_days": self.age_days,
                "probe_outcome": self.probe_outcome,
                "llm_outcome": self.llm_outcome,
                "llm_confidence": self.llm_confidence,
                "llm_model": self.llm_model,
                "reason": self.reason,
            }

    async def fake_run_batch(**kwargs) -> tuple:
        return (FakeOutcome(kwargs["domains"][0]),)

    import domainhunter.filter.enrich as enrich_module

    monkeypatch.setattr(enrich_module, "run_batch", fake_run_batch)
    input_file = tmp_path / "domains.json"
    input_file.write_text(json.dumps({"domains": ["new.com"]}))

    assert (
        cli.main(
            [
                "filter-enrich",
                "--database",
                str(tmp_path / "domainhunter.db"),
                "--input",
                str(input_file),
                "--provider",
                "mock",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcomes"][0]["stage"] == "enriched"
    assert payload["outcomes"][0]["llm_outcome"] == "publishable_ai_saas"


def test_cli_filter_enrich_fresh_only_skips_seen_domains(tmp_path, capsys, monkeypatch) -> None:
    """fresh-only marks inputs as seen and only enriches never-seen domains."""

    class FakeOutcome:
        def __init__(self, domain: str) -> None:
            self.domain = domain
            self.stage = "enriched"
            self.final_tier = "tier1"
            self.s1_score = 0.9
            self.age_days = 3
            self.probe_outcome = "success"
            self.llm_outcome = "publishable_ai_saas"
            self.llm_confidence = 0.9
            self.llm_model = "mock-1"
            self.reason = "LLM classification persisted"

        def as_payload(self) -> dict[str, object]:
            return {
                "domain": self.domain,
                "stage": self.stage,
                "final_tier": self.final_tier,
                "s1_score": self.s1_score,
                "age_days": self.age_days,
                "probe_outcome": self.probe_outcome,
                "llm_outcome": self.llm_outcome,
                "llm_confidence": self.llm_confidence,
                "llm_model": self.llm_model,
                "reason": self.reason,
            }

    async def fake_run_batch(**kwargs) -> tuple:
        return tuple(FakeOutcome(d) for d in kwargs["domains"])

    import domainhunter.filter.enrich as enrich_module

    monkeypatch.setattr(enrich_module, "run_batch", fake_run_batch)
    database = tmp_path / "domainhunter.db"
    input_file = tmp_path / "domains.json"
    input_file.write_text(json.dumps({"domains": ["first.com", "seen.com"]}))

    # First run: both are fresh.
    assert (
        cli.main(
            [
                "filter-enrich",
                "--database",
                str(database),
                "--input",
                str(input_file),
                "--provider",
                "mock",
                "--fresh-only",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["fresh_only"]["fresh"] == 2
    assert len(payload["outcomes"]) == 2

    # Second run: both are now seen → no outcomes.
    assert (
        cli.main(
            [
                "filter-enrich",
                "--database",
                str(database),
                "--input",
                str(input_file),
                "--provider",
                "mock",
                "--fresh-only",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["fresh_only"]["fresh"] == 0
    assert payload["outcomes"] == []


def test_cli_outreach_triggers_a_dry_run_against_an_approved_human_version(
    tmp_path, capsys, monkeypatch
) -> None:
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    candidate = store.create_candidate("example.com", created_at=datetime(2026, 8, 16, tzinfo=UTC))
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="human",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example AI",
            description_suggestion="AI workflow automation",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
        created_at=datetime(2026, 8, 16, tzinfo=UTC),
    )
    store.append_review_decision(
        build_review_decision(
            request_id="approve-cli",
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-1",
            decided_at=datetime(2026, 8, 16, tzinfo=UTC),
        )
    )

    class StaticFetcher:
        def fetch(self, url: str) -> str:
            return "<p>founders@example.com</p>"

    app = create_app(database, contact_fetcher=StaticFetcher())
    monkeypatch.setattr(cli, "create_app", lambda path, **_: app)

    assert (
        cli.main(
            [
                "outreach",
                "--database",
                str(database),
                "--candidate-id",
                candidate.candidate_id,
                "--version",
                str(version.version),
                "--actor-id",
                "actor-1",
                "--recipient-source-url",
                "https://example.com/contact",
                "--request-id",
                "cli-dry",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status_code"] == 201
    body = payload["body"]
    assert body["status"] == "dry_run"
    assert body["contact_preview"][0]["redacted_address"] == "f*******@example.com"

    events = SQLiteStore(database).list_outreach_events(candidate.candidate_id)
    assert len(events) == 1
    assert events[0].dry_run is True


def test_cli_enrich_llm_persists_a_model_version(tmp_path, capsys) -> None:
    """The mock provider persists a second llm version on top of the rule draft."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    candidate = store.create_candidate("example.com", created_at=datetime(2026, 8, 17, tzinfo=UTC))
    store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.9,
            name_suggestion="Example AI",
            description_suggestion="An AI workflow helper.",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
    )

    assert (
        cli.main(
            [
                "enrich-llm",
                "--database",
                str(database),
                "--candidate-id",
                candidate.candidate_id,
                "--version",
                "1",
                "--provider",
                "mock",
                "--name",
                "Mock Draft",
                "--description",
                "Mock description.",
                "--category",
                "automation",
                "--tag",
                "mock",
                "--tag",
                "test",
                "--url",
                "https://example.com",
                "--evidence-quote",
                "Automate your AI workflows",
                "--model-version",
                "cli-mock-1",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["persisted"] is True
    assert payload["model_version"] == "cli-mock-1"
    versions = SQLiteStore(database).list_candidate_versions(candidate.candidate_id)
    assert len(versions) == 2
    assert versions[-1].draft.author_kind == "llm"
