import json
from datetime import UTC, datetime
from types import SimpleNamespace

from fastapi.testclient import TestClient

from webradar_v2 import cli
from webradar_v2.api import create_app
from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.reviews import ReviewAction, build_review_decision
from webradar_v2.ingest.github_poller import GitHubPage, GitHubRepository
from webradar_v2.storage.sqlite import SQLiteStore


def test_cli_imports_github_page_and_reports_due_domain(tmp_path, capsys) -> None:
    database = tmp_path / "webradar.db"
    source_page = tmp_path / "github-page.json"
    source_page.write_text(
        json.dumps(
            {
                "next_cursor": "page-2",
                "repositories": [
                    {
                        "repository_id": "123",
                        "homepage": "https://app.example.com",
                        "observed_at": "2026-08-16T00:00:00+00:00",
                    }
                ],
            }
        )
    )

    assert cli.main(["init", "--database", str(database)]) == 0
    capsys.readouterr()
    assert (
        cli.main(
            [
                "ingest-github-page",
                "--database",
                str(database),
                "--input",
                str(source_page),
            ]
        )
        == 0
    )
    imported = json.loads(capsys.readouterr().out)

    assert imported["events_added"] == 1
    assert cli.main(["status", "--database", str(database), "--at", "2026-08-16T00:00:00+00:00"]) == 0
    status = json.loads(capsys.readouterr().out)

    assert status["domains"] == ["example.com"]
    assert status["due_domains"] == ["example.com"]


def test_cli_polls_github_api_with_an_explicit_query_and_token(tmp_path, capsys, monkeypatch) -> None:
    captured: dict[str, str | None] = {}

    class FakeFetcher:
        def __init__(self, *, query: str, token: str | None) -> None:
            captured.update(query=query, token=token)

        async def __call__(self, cursor: str | None) -> GitHubPage:
            return GitHubPage(
                repositories=(
                    GitHubRepository(
                        "123",
                        "https://app.example.com",
                        cli._parse_datetime("2026-08-16T00:00:00+00:00"),
                    ),
                ),
                next_cursor="2",
            )

        async def __aenter__(self) -> "FakeFetcher":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(cli, "GitHubSearchFetcher", FakeFetcher)
    database = tmp_path / "webradar.db"

    assert (
        cli.main(
            [
                "poll-github-api",
                "--database",
                str(database),
                "--query",
                "topic:artificial-intelligence",
                "--token",
                "secret-token",
            ]
        )
        == 0
    )

    assert json.loads(capsys.readouterr().out)["events_added"] == 1
    assert captured == {"query": "topic:artificial-intelligence", "token": "secret-token"}


def test_cli_listens_to_certstream_with_a_bounded_message_count(tmp_path, capsys, monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeListener:
        def __init__(self, *, store, url: str) -> None:
            captured["url"] = url

        async def listen(self, *, max_messages: int | None):
            captured["max_messages"] = max_messages
            return SimpleNamespace(
                messages_seen=3,
                certificate_updates=2,
                invalid_messages=1,
                events_added=4,
            )

    monkeypatch.setattr(cli, "CertStreamListener", FakeListener)

    assert (
        cli.main(
            [
                "listen-certstream",
                "--database",
                str(tmp_path / "webradar.db"),
                "--url",
                "wss://ct.example.test",
                "--max-messages",
                "3",
            ]
        )
        == 0
    )

    assert json.loads(capsys.readouterr().out)["events_added"] == 4
    assert captured == {"url": "wss://ct.example.test", "max_messages": 3}


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
                str(tmp_path / "webradar.db"),
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


def test_cli_outreach_triggers_a_dry_run_against_an_approved_human_version(
    tmp_path, capsys, monkeypatch
) -> None:
    database = tmp_path / "webradar.db"
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
    database = tmp_path / "webradar.db"
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
