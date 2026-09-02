"""Tests for the web one-click discovery API surface."""

from fastapi.testclient import TestClient

from domainhunter.api import create_app


def test_discovery_overview_empty(tmp_path) -> None:
    client = TestClient(create_app(tmp_path / "discovery.db"))
    response = client.get("/v1/discovery/overview")
    assert response.status_code == 200
    payload = response.json()
    assert payload["domains"] == []
    assert payload["counts"]["source_events"] == 0


def test_review_console_contains_discovery_button(tmp_path) -> None:
    client = TestClient(create_app(tmp_path / "discovery-console.db"))
    response = client.get("/discovery")
    assert response.status_code == 200
    assert "btn-run-discovery" in response.text
    assert "/v1/run/discovery" in response.text


def test_discovery_run_validates_max_probes(tmp_path) -> None:
    client = TestClient(create_app(tmp_path / "discovery-validation.db"))
    response = client.post("/v1/run/discovery", json={"max_probes": 0})
    assert response.status_code == 422
