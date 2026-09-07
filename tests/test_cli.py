import json
from datetime import UTC, datetime
from typing import Self

import pytest

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
    assert imported["source_errors"] == []
    assert imported["pending_work"] == 1
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


def test_cli_discover_uses_the_builtin_strict_ct_path(tmp_path, capsys, monkeypatch) -> None:
    """``discover`` must not depend on a separately installed CT monitor."""
    captured: dict[str, object] = {}

    class _FakeFetcher:
        def __init__(self, **kwargs) -> None:
            captured["fetcher"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            return None

    class _FakePoller:
        def __init__(self, **kwargs) -> None:
            captured["poller"] = kwargs

    class _FakeProbeContext:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            return None

    class _FakePipeline:
        def __init__(self, **kwargs) -> None:
            captured["pipeline"] = kwargs

    class _FakeFilterPipeline:
        def __init__(self, **kwargs) -> None:
            captured["filter"] = kwargs

    class _FakeOrchestrator:
        def __init__(self, **kwargs) -> None:
            captured["orchestrator"] = kwargs

        async def run_once(self):
            from domainhunter.ingest.ct_orchestrator import CTIngestRunSummary

            return CTIngestRunSummary(
                certificates_seen=4,
                events_added=3,
                probes_run=1,
                candidates_created=1,
                next_cursor='{"nimbus": 12}',
                roots_observed=3,
                strict_rejections=2,
                llm_enriched=1,
            )

    monkeypatch.setattr(cli, "CTLogFetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "CTPoller", _FakePoller)
    monkeypatch.setattr(cli, "HTTPProbe", _FakeProbeContext)
    monkeypatch.setattr(cli, "DomainHunterPipeline", _FakePipeline)
    monkeypatch.setattr(cli, "FilterPipeline", _FakeFilterPipeline)
    monkeypatch.setattr(cli, "CTIngestOrchestrator", _FakeOrchestrator)

    assert (
        cli.main(
            [
                "discover",
                "--database",
                str(tmp_path / "domainhunter.db"),
                "--provider",
                "mock",
                "--max-rounds",
                "1",
                "--round-seconds",
                "0",
                "--log",
                "nimbus=https://ct.example.test/logs/nimbus",
                "--catchup",
                "17",
                "--page-size",
                "11",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "database": str(tmp_path / "domainhunter.db"),
        "failed_rounds": 0,
        "last_error": None,
        "rounds": 1,
        "source_errors": [],
        "successful_rounds": 1,
    }
    assert captured["fetcher"]["catchup_entries"] == 17
    assert captured["fetcher"]["max_entries_per_page"] == 11
    assert captured["fetcher"]["logs"][0].log_id == "nimbus"
    assert captured["filter"] == {
        "tier1_days": 30,
        "tier2_days": 90,
        "require_dns": True,
        "drop_unknown_rdap": True,
    }
    assert captured["orchestrator"]["require_first_seen"] is True
    assert captured["orchestrator"]["provider"].__class__.__name__ == "MockLLMProvider"


def test_cli_discover_reports_a_bounded_source_failure(tmp_path, capsys, monkeypatch) -> None:
    class _FakeFetcher:
        def __init__(self, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            return None

    class _FakePoller:
        def __init__(self, **_kwargs) -> None:
            pass

    class _FakeProbeContext:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            return None

    class _FakePipeline:
        def __init__(self, **_kwargs) -> None:
            pass

    class _FakeFilterPipeline:
        def __init__(self, **_kwargs) -> None:
            pass

    class _FailingOrchestrator:
        def __init__(self, **_kwargs) -> None:
            pass

        async def run_once(self):
            raise RuntimeError("test CT source is offline")

    monkeypatch.setattr(cli, "CTLogFetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "CTPoller", _FakePoller)
    monkeypatch.setattr(cli, "HTTPProbe", _FakeProbeContext)
    monkeypatch.setattr(cli, "DomainHunterPipeline", _FakePipeline)
    monkeypatch.setattr(cli, "FilterPipeline", _FakeFilterPipeline)
    monkeypatch.setattr(cli, "CTIngestOrchestrator", _FailingOrchestrator)

    assert (
        cli.main(
            [
                "discover",
                "--database",
                str(tmp_path / "domainhunter.db"),
                "--provider",
                "mock",
                "--max-rounds",
                "1",
            ]
        )
        == 1
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["rounds"] == 1
    assert payload["successful_rounds"] == 0
    assert payload["failed_rounds"] == 1
    assert payload["last_error"] == "test CT source is offline"


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

    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs
            captured.update(kwargs)

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
            ["filter", "--input", str(input_file), "--tier1-days", "30"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["report"]["input"] == 2
    assert payload["report"]["kept"] == 1
    assert payload["candidates"][0]["final_tier"] == "tier1"
    assert captured["require_dns"] is True
    assert captured["drop_unknown_rdap"] is True


def test_cli_filter_probe_runs_funnel_and_probes(tmp_path, capsys, monkeypatch) -> None:
    """filter-probe runs the funnel then S4-probes survivors into the DB."""

    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs
            captured.update(kwargs)

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
    assert captured["require_dns"] is True
    assert captured["drop_unknown_rdap"] is True


def test_cli_filter_probe_rejects_relaxed_strict_gates(tmp_path) -> None:
    """A command that persists candidates cannot disable DNS or RDAP gates."""
    parser = cli._build_parser()

    with pytest.raises(SystemExit) as error:
        parser.parse_args(
            [
                "filter-probe",
                "--database",
                str(tmp_path / "domainhunter.db"),
                "--input",
                str(tmp_path / "domains.json"),
                "--no-require-dns",
            ]
        )

    assert error.value.code == 2


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

    captured: dict[str, object] = {}

    async def fake_run_batch(**kwargs) -> tuple:
        captured.update(kwargs)
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
    assert captured["require_dns"] is True
    assert captured["drop_unknown_rdap"] is True


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
