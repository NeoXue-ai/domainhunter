"""Concurrent review decisions must converge on a single row.

Spec §15 calls out concurrent edit as a required review-queue acceptance test.
Two reviewers may double-click the approve button at almost the same time;
the system must end up with exactly one persisted decision per ``request_id``
regardless of which request wins the race.

These tests pin the contract at both the store level and the HTTP boundary.
The HTTP tests run a real uvicorn server in a background thread so the
single-threaded ``TestClient`` portal does not serialize the requests.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import socket
import threading
import time

import httpx
import uvicorn

from webradar_v2.api import create_app
from webradar_v2.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from webradar_v2.domain.reviews import ReviewAction, build_review_decision
from webradar_v2.domain.review_priority import ReviewPriorityInputs, calculate_review_priority
from webradar_v2.storage.sqlite import SQLiteStore


OBSERVED_AT = datetime(2026, 8, 17, tzinfo=UTC)


def _reviewable_candidate(store: SQLiteStore):
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.8,
            name_suggestion="Example AI",
            description_suggestion="AI workflow automation",
            evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
        ),
        created_at=OBSERVED_AT,
    )
    store.append_review_priority(
        candidate.candidate_id,
        calculate_review_priority(ReviewPriorityInputs(0.8, 0.8, None, 0.8)),
        calculated_at=OBSERVED_AT,
    )
    return candidate, version


class _LiveServer:
    """Run the FastAPI app under uvicorn on a free port inside a thread."""

    def __init__(self, database_path) -> None:
        self.database_path = database_path
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self.host = "127.0.0.1"
        self.port = self._free_port()

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    def __enter__(self) -> "_LiveServer":
        config = uvicorn.Config(
            create_app(self.database_path),
            host=self.host,
            port=self.port,
            log_level="warning",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run, name="review-test-server", daemon=True
        )
        self._thread.start()
        # Wait for the server socket to be listening.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((self.host, self.port), timeout=0.1):
                    return self
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("uvicorn server did not start in time")

    def __exit__(self, *exc_info: object) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


def test_store_appends_idempotent_concurrent_decisions_only_once(tmp_path) -> None:
    """Two threads racing on the same request_id must produce one decision row."""
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, version = _reviewable_candidate(store)
    request_id = "concurrent-decision-1"

    barrier = threading.Barrier(2)
    results: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        decision = build_review_decision(
            request_id=request_id,
            candidate_id=candidate.candidate_id,
            candidate_version=version.version,
            action=ReviewAction.APPROVE,
            actor_id="reviewer-x",
            decided_at=datetime.now(UTC),
        )
        barrier.wait()  # synchronize the inserts
        created = store.append_review_decision(decision)
        with lock:
            results.append(created)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker) for _ in range(2)]
        for future in futures:
            future.result()

    assert sorted(results) == [False, True], f"expected exactly one True, got {results}"
    decisions = store.list_review_decisions(candidate.candidate_id)
    assert len(decisions) == 1
    assert decisions[0].request_id == request_id


def test_http_api_concurrent_decisions_resolve_to_one_created(tmp_path) -> None:
    """Two simultaneous POSTs with the same request_id must yield one created row."""
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, version = _reviewable_candidate(store)
    body = {
        "request_id": "concurrent-decision-http",
        "action": "approve",
        "reason_tags": [],
    }
    headers = {"X-Actor-ID": "reviewer-y", "Content-Type": "application/json"}

    with _LiveServer(database) as server:
        url = (
            f"{server.base_url}/v1/candidates/{candidate.candidate_id}"
            f"/versions/{version.version}/decisions"
        )

        barrier = threading.Barrier(2)
        responses: list[httpx.Response] = []
        response_lock = threading.Lock()

        def worker() -> None:
            with httpx.Client(timeout=10.0) as client:
                barrier.wait()
                response = client.post(url, json=body, headers=headers)
            with response_lock:
                responses.append(response)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert len(responses) == 2
    assert all(response.status_code in {200, 201} for response in responses), [
        response.status_code for response in responses
    ]
    created_flags = sorted(response.json()["created"] for response in responses)
    assert created_flags == [False, True], f"expected one True, one False, got {created_flags}"
    decisions = store.list_review_decisions(candidate.candidate_id)
    assert len(decisions) == 1


def test_http_api_different_request_ids_return_one_conflict(tmp_path) -> None:
    """Per spec §11, the second concurrent POST with a different request_id is a 409.

    The first request wins and is persisted (201). The second sees an active
    decision with a different request_id and the API responds with 409 plus
    the active decision's identifiers.
    """
    database = tmp_path / "webradar.db"
    store = SQLiteStore(database)
    candidate, version = _reviewable_candidate(store)

    with _LiveServer(database) as server:
        url = (
            f"{server.base_url}/v1/candidates/{candidate.candidate_id}"
            f"/versions/{version.version}/decisions"
        )

        barrier = threading.Barrier(2)
        responses: list[httpx.Response] = []
        response_lock = threading.Lock()

        def worker(request_id: str) -> None:
            body = {
                "request_id": request_id,
                "action": "approve",
                "reason_tags": [],
            }
            headers = {
                "X-Actor-ID": f"reviewer-{request_id}",
                "Content-Type": "application/json",
            }
            with httpx.Client(timeout=10.0) as client:
                barrier.wait()
                response = client.post(url, json=body, headers=headers)
            with response_lock:
                responses.append(response)

        threads = [
            threading.Thread(target=worker, args=("concurrent-a",)),
            threading.Thread(target=worker, args=("concurrent-b",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    statuses = sorted(response.status_code for response in responses)
    assert statuses == [201, 409], f"expected one 201 and one 409, got {statuses}"
    conflicts = [response for response in responses if response.status_code == 409]
    assert len(conflicts) == 1
    detail = conflicts[0].json()["detail"]
    assert detail["detail"] == "concurrent decision conflict"
    assert detail["active_request_id"] in {"concurrent-a", "concurrent-b"}
    decisions = store.list_review_decisions(candidate.candidate_id)
    assert len(decisions) == 1
    assert decisions[0].request_id in {"concurrent-a", "concurrent-b"}
