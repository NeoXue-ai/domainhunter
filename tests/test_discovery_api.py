"""Tests for the web one-click discovery API surface."""

from fastapi.testclient import TestClient

from domainhunter import api
from domainhunter.api import create_app
from domainhunter.ingest.ct_orchestrator import CTIngestRunSummary


def test_discovery_overview_empty(tmp_path) -> None:
    client = TestClient(create_app(tmp_path / "discovery.db"))
    response = client.get("/v1/discovery/overview")
    assert response.status_code == 200
    payload = response.json()
    assert payload["domains"] == []
    assert payload["counts"]["source_events"] == 0


def test_legacy_dashboard_routes_redirect_to_inbox(tmp_path) -> None:
    client = TestClient(create_app(tmp_path / "legacy-dashboard.db"))

    for path in ("/discovery", "/ops"):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/"


def test_discovery_run_validates_max_probes(tmp_path) -> None:
    client = TestClient(create_app(tmp_path / "discovery-validation.db"))
    response = client.post("/v1/run/discovery", json={"max_probes": 0})
    assert response.status_code == 422


def test_web_discovery_requires_verified_age_and_dns_before_l1(
    monkeypatch, tmp_path
) -> None:
    """The browser action must opt into the strict CT gate before probing."""
    captured: dict[str, object] = {}

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

    class _CapturingOrchestrator:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        async def run_once(self) -> CTIngestRunSummary:
            return CTIngestRunSummary(
                certificates_seen=12,
                events_added=8,
                probes_run=3,
                candidates_created=1,
                next_cursor="42",
                roots_observed=8,
                strict_rejections=5,
            )

    monkeypatch.setattr(api, "CTLogFetcher", _FakeFetcher)
    monkeypatch.setattr(api, "CTPoller", _FakePoller)
    monkeypatch.setattr(api, "HTTPProbe", _FakeProbeContext)
    monkeypatch.setattr(api, "DomainHunterPipeline", _FakePipeline)
    monkeypatch.setattr(api, "CTIngestOrchestrator", _CapturingOrchestrator)

    response = TestClient(create_app(tmp_path / "strict.db")).post(
        "/v1/run/discovery", json={"max_probes": 5}
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "completed",
        "next_cursor": "42",
        "certificates_seen": 12,
        "events_added": 8,
        "roots_observed": 8,
        "strict_rejections": 5,
        "probes_run": 3,
        "candidates_created": 1,
        "source_errors": [],
        "pending_work": 0,
    }
    strict_filter = captured["filter_pipeline"]
    assert strict_filter._drop_unknown_rdap is True
    assert strict_filter._require_dns is True
    assert captured["require_first_seen"] is True


def test_web_discovery_calls_an_empty_strict_run_no_candidates(monkeypatch, tmp_path) -> None:
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

    class _EmptyOrchestrator:
        def __init__(self, **_kwargs) -> None:
            pass

        async def run_once(self) -> CTIngestRunSummary:
            return CTIngestRunSummary(
                certificates_seen=4,
                events_added=4,
                probes_run=0,
                candidates_created=0,
                next_cursor="9",
                roots_observed=4,
                strict_rejections=4,
            )

    monkeypatch.setattr(api, "CTLogFetcher", _FakeFetcher)
    monkeypatch.setattr(api, "CTPoller", _FakePoller)
    monkeypatch.setattr(api, "HTTPProbe", _FakeProbeContext)
    monkeypatch.setattr(api, "DomainHunterPipeline", _FakePipeline)
    monkeypatch.setattr(api, "CTIngestOrchestrator", _EmptyOrchestrator)

    response = TestClient(create_app(tmp_path / "empty.db")).post(
        "/v1/run/discovery", json={"max_probes": 5}
    )

    assert response.status_code == 200
    assert response.json()["status"] == "no_candidates"
    assert response.json()["strict_rejections"] == 4


def test_web_discovery_reports_queued_work_and_partial_source_failure(
    monkeypatch, tmp_path
) -> None:
    """The UI API must distinguish unfinished work from an empty result."""

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

    class _QueuedOrchestrator:
        def __init__(self, **_kwargs) -> None:
            pass

        async def run_once(self) -> CTIngestRunSummary:
            return CTIngestRunSummary(
                certificates_seen=6,
                events_added=6,
                probes_run=2,
                candidates_created=0,
                next_cursor="10",
                roots_observed=6,
                strict_rejections=1,
                source_errors=("Nimbus endpoint timed out",),
                pending_work=4,
            )

    monkeypatch.setattr(api, "CTLogFetcher", _FakeFetcher)
    monkeypatch.setattr(api, "CTPoller", _FakePoller)
    monkeypatch.setattr(api, "HTTPProbe", _FakeProbeContext)
    monkeypatch.setattr(api, "DomainHunterPipeline", _FakePipeline)
    monkeypatch.setattr(api, "CTIngestOrchestrator", _QueuedOrchestrator)

    response = TestClient(create_app(tmp_path / "queued.db")).post(
        "/v1/run/discovery", json={"max_probes": 5}
    )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["pending_work"] == 4
    assert response.json()["source_errors"] == ["Nimbus endpoint timed out"]
