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
