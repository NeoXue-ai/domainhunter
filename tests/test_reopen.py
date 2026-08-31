from datetime import UTC, datetime

from fastapi.testclient import TestClient

from webradar_v2 import cli
from webradar_v2.api import create_app
from webradar_v2.domain.events import SourceEvent
from webradar_v2.domain.observations import Observation, OutcomeCode
from webradar_v2.domain.reopens import build_reopen_event
from webradar_v2.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)
SOURCE_EVENT_ID = "argon:42"


def _seed_terminal_candidate(
    store: SQLiteStore,
    *,
    outcome: OutcomeCode,
    attempts: int,
    source_event_id: str = SOURCE_EVENT_ID,
) -> tuple[str, str]:
    event = SourceEvent("ct_log", source_event_id, "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    observation = Observation(
        domain="example.com",
        outcome_code=outcome,
        observed_at=OBSERVED_AT,
        attempt_number=attempts,
    )
    store.append_observation(observation)
    return candidate.candidate_id, candidate.domain


def _seed_success_candidate(store: SQLiteStore) -> str:
    event = SourceEvent("ct_log", SOURCE_EVENT_ID, "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    observation = Observation(
        domain="example.com",
        outcome_code=OutcomeCode.SUCCESS,
        observed_at=OBSERVED_AT,
        attempt_number=1,
    )
    store.append_observation(observation)
    return candidate.candidate_id


def _post_reopen(
    client: TestClient,
    candidate_id: str,
    *,
    request_id: str = "req-1",
    trigger_source_event_id: str = SOURCE_EVENT_ID,
    new_outcome: str = "success",
    reason: str = "manual re-probe",
    actor_id: str = "operator-1",
):
    return client.post(
        f"/v1/candidates/{candidate_id}/reopen",
        json={
            "request_id": request_id,
            "trigger_source_event_id": trigger_source_event_id,
            "new_outcome": new_outcome,
            "reason": reason,
        },
        headers={"X-Actor-ID": actor_id},
    )


def test_reopen_terminal_content_insufficient_succeeds(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, _ = _seed_terminal_candidate(
        store, outcome=OutcomeCode.CONTENT_INSUFFICIENT, attempts=4
    )
    client = TestClient(create_app(database))

    response = _post_reopen(client, candidate_id, new_outcome="success")

    assert response.status_code == 201
    body = response.json()
    assert body["created"] is True
    assert body["new_outcome"] == "success"
    assert body["previous_outcome"] == "content_insufficient"

    observations = store.list_observations("example.com")
    assert len(observations) == 2
    assert observations[-1].outcome_code is OutcomeCode.SUCCESS
    assert observations[-1].attempt_number == 1


def test_reopen_non_terminal_returns_409(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id = _seed_success_candidate(store)
    client = TestClient(create_app(database))

    response = _post_reopen(client, candidate_id)

    assert response.status_code == 409


def test_reopen_unknown_candidate_returns_404(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    client = TestClient(create_app(database))

    response = _post_reopen(client, "missing-candidate-id")

    assert response.status_code == 404


def test_reopen_unknown_trigger_source_returns_422(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, _ = _seed_terminal_candidate(
        store, outcome=OutcomeCode.HTTP_4XX, attempts=4
    )
    client = TestClient(create_app(database))

    response = _post_reopen(client, candidate_id, trigger_source_event_id="argon:does-not-exist")

    assert response.status_code == 422


def test_reopen_unknown_outcome_returns_422(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, _ = _seed_terminal_candidate(
        store, outcome=OutcomeCode.HTTP_4XX, attempts=4
    )
    client = TestClient(create_app(database))

    response = _post_reopen(client, candidate_id, new_outcome="not_a_real_outcome")

    assert response.status_code == 422


def test_reopen_idempotent_by_request_id(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, _ = _seed_terminal_candidate(
        store, outcome=OutcomeCode.HTTP_4XX, attempts=4
    )
    client = TestClient(create_app(database))

    first = _post_reopen(client, candidate_id, request_id="stable-id")
    second = _post_reopen(client, candidate_id, request_id="stable-id")

    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["reopen_id"] == second.json()["reopen_id"]
    assert second.json()["created"] is False

    events = store.list_reopen_events(candidate_id)
    assert len(events) == 1


def test_reopen_appends_fresh_observation_resets_retry(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, _ = _seed_terminal_candidate(
        store, outcome=OutcomeCode.REDIRECT_LOOP, attempts=5
    )
    client = TestClient(create_app(database))

    response = _post_reopen(client, candidate_id, new_outcome="success")

    assert response.status_code == 201
    observations = store.list_observations("example.com")
    latest = observations[-1]
    assert latest.outcome_code is OutcomeCode.SUCCESS
    assert latest.attempt_number == 1
    assert store.is_reopenable("example.com") is False


def test_list_reopen_events_returns_in_order(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, _ = _seed_terminal_candidate(
        store, outcome=OutcomeCode.CONTENT_INSUFFICIENT, attempts=4
    )

    first = build_reopen_event(
        request_id="first",
        candidate_id=candidate_id,
        trigger_source_event_id=SOURCE_EVENT_ID,
        previous_outcome=OutcomeCode.CONTENT_INSUFFICIENT,
        new_outcome=OutcomeCode.SUCCESS,
        actor_id="operator-1",
        reason="first reopen",
        reopened_at=OBSERVED_AT,
    )
    second = build_reopen_event(
        request_id="second",
        candidate_id=candidate_id,
        trigger_source_event_id=SOURCE_EVENT_ID,
        previous_outcome=OutcomeCode.SUCCESS,
        new_outcome=OutcomeCode.CONTENT_INSUFFICIENT,
        actor_id="operator-2",
        reason="second reopen",
        reopened_at=OBSERVED_AT,
    )
    assert store.append_reopen_event(first) is True
    assert store.append_reopen_event(second) is True

    events = store.list_reopen_events(candidate_id)
    assert [event.reopen_id for event in events] == [first.reopen_id, second.reopen_id]
    assert [event.reason for event in events] == ["first reopen", "second reopen"]


def test_cli_reopen_subcommand(tmp_path) -> None:
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate_id, _ = _seed_terminal_candidate(
        store, outcome=OutcomeCode.HTTP_4XX, attempts=4
    )

    exit_code = cli.main(
        [
            "reopen",
            "--database",
            str(database),
            "--candidate-id",
            candidate_id,
            "--trigger-source-event-id",
            SOURCE_EVENT_ID,
            "--new-outcome",
            "success",
            "--reason",
            "cli reopen",
            "--actor-id",
            "operator-cli",
        ]
    )
    assert exit_code == 0

    events = store.list_reopen_events(candidate_id)
    assert len(events) == 1
    assert events[0].actor_id == "operator-cli"
    assert events[0].reason == "cli reopen"

    observations = store.list_observations("example.com")
    assert observations[-1].outcome_code is OutcomeCode.SUCCESS
    assert observations[-1].attempt_number == 1
