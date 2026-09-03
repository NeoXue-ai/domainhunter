# Discovery Inbox Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the dashboard with a truthful Chinese discovery inbox, an independent candidate evidence page, and one real strict-scan dialog.

**Architecture:** FastAPI and SQLite remain the only runtime. A strict scan writes an immutable verification snapshot beside the candidate version. The browser consumes a pending-only inbox projection and a detail projection; it never derives or invents newness, reachability, or review state.

**Tech Stack:** Python 3.12, FastAPI, SQLite, server-rendered HTML/CSS, vanilla JavaScript, pytest, gstack browse.

---

## File map

| File | Change |
| --- | --- |
| `src/domainhunter/domain/verification.py` | New immutable strict-scan evidence model. |
| `src/domainhunter/storage/sqlite.py` | New verification table plus review-state and first-seen readers. |
| `src/domainhunter/ingest/ct_orchestrator.py` | Persist strict-filter evidence with created candidate versions; count strict rejections. |
| `src/domainhunter/api.py` | Truthful API projections, reduced routes, and the new two-screen interface. |
| `tests/test_candidate_verification.py` | Durable evidence storage tests. |
| `tests/test_discovery_inbox_api.py` | Read-model, decision-state, and scan-result contract tests. |
| `tests/test_discovery_inbox_ui.py` | Server-rendered UI contract tests. |
| `tests/test_review_api.py`, `tests/test_discovery_api.py` | Update old dashboard assumptions. |
| `README.md` | Describe the inbox and one-shot strict scan accurately. |

## Task 1: Add durable strict-scan verification facts

**Files:**
- Create: `src/domainhunter/domain/verification.py`
- Modify: `src/domainhunter/storage/sqlite.py`
- Test: `tests/test_candidate_verification.py`

- [ ] **Step 1: Write the failing round-trip test.**

    from datetime import UTC, datetime
    from domainhunter.domain.candidates import CandidateOutcome, CandidateVersionDraft, Evidence, EvidenceType
    from domainhunter.domain.verification import CandidateVerification
    from domainhunter.storage.sqlite import SQLiteStore

    NOW = datetime(2026, 9, 3, tzinfo=UTC)

    def _versioned_candidate(store: SQLiteStore):
        candidate = store.create_candidate("newsite.ai", created_at=NOW)
        version = store.append_candidate_version(
            candidate.candidate_id,
            CandidateVersionDraft(
                author_kind="rule",
                primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
                classification_confidence=0.8,
                name_suggestion="Newsite",
                description_suggestion="A new AI product",
                evidence=(Evidence(EvidenceType.TITLE, "Newsite", "https://newsite.ai/"),),
            ),
            created_at=NOW,
        )
        return candidate, version

    def test_round_trips_candidate_verification(tmp_path) -> None:
        store = SQLiteStore(tmp_path / "verification.db")
        candidate, version = _versioned_candidate(store)
        item = CandidateVerification(
            candidate_id=candidate.candidate_id, candidate_version=version.version,
            checked_at=NOW, ct_first_seen_at=NOW, rdap_tier="tier1",
            rdap_age_days=3, rdap_registration_at=NOW, dns_has_a=True,
            http_status_code=200, final_url="https://newsite.ai/",
            canonical_url="https://newsite.ai/", final_root_matches=True,
        )
        assert store.append_candidate_verification(item) is True
        assert store.get_candidate_verification(candidate.candidate_id, version.version) == item

    def test_verification_is_idempotent_per_version(tmp_path) -> None:
        store = SQLiteStore(tmp_path / "verification.db")
        candidate, version = _versioned_candidate(store)
        item = CandidateVerification(
            candidate_id=candidate.candidate_id, candidate_version=version.version,
            checked_at=NOW, ct_first_seen_at=NOW, rdap_tier="tier1",
            rdap_age_days=3, rdap_registration_at=NOW, dns_has_a=True,
            http_status_code=200, final_url="https://newsite.ai/",
            canonical_url="https://newsite.ai/", final_root_matches=True,
        )
        assert store.append_candidate_verification(item) is True
        assert store.append_candidate_verification(item) is False

- [ ] **Step 2: Verify failure.**

    Run: `.venv/bin/python -m pytest tests/test_candidate_verification.py -v`

    Expected: `ModuleNotFoundError: No module named 'domainhunter.domain.verification'`.

- [ ] **Step 3: Add the immutable domain model.**

    Implement this frozen, slotted dataclass in `verification.py`:

    @dataclass(frozen=True, slots=True)
    class CandidateVerification:
        candidate_id: str
        candidate_version: int
        checked_at: datetime
        ct_first_seen_at: datetime | None
        rdap_tier: str | None
        rdap_age_days: int | None
        rdap_registration_at: datetime | None
        dns_has_a: bool | None
        http_status_code: int | None
        final_url: str | None
        canonical_url: str | None
        final_root_matches: bool | None

    Reject blank IDs, non-positive versions, naive datetimes, tiers outside `tier1`, `tier2`, and `unknown`, and malformed non-HTTP(S) URLs.

- [ ] **Step 4: Persist without overwriting history.**

    Add the following SQLite table beside `candidate_versions`:

    CREATE TABLE IF NOT EXISTS candidate_verifications (
        candidate_id TEXT NOT NULL,
        candidate_version INTEGER NOT NULL,
        checked_at TEXT NOT NULL,
        ct_first_seen_at TEXT,
        rdap_tier TEXT,
        rdap_age_days INTEGER,
        rdap_registration_at TEXT,
        dns_has_a INTEGER,
        http_status_code INTEGER,
        final_url TEXT,
        canonical_url TEXT,
        final_root_matches INTEGER,
        PRIMARY KEY (candidate_id, candidate_version),
        FOREIGN KEY (candidate_id, candidate_version)
          REFERENCES candidate_versions(candidate_id, version)
    )

    Implement `append_candidate_verification(item) -> bool` with `INSERT OR IGNORE`, and `get_candidate_verification(candidate_id, candidate_version) -> CandidateVerification | None`. Serialize booleans as SQLite integers and deserialize them as `bool | None`. Also add `get_ct_first_seen_at(domain: str) -> datetime | None`, reading `ct_seen_domains.first_seen_at` for the normalized registrable domain.

- [ ] **Step 5: Verify and commit.**

    Run: `.venv/bin/python -m pytest tests/test_candidate_verification.py -v`

    Expected: PASS.

    git add src/domainhunter/domain/verification.py src/domainhunter/storage/sqlite.py tests/test_candidate_verification.py
    git commit -m "feat: persist candidate verification facts"

## Task 2: Capture evidence during strict CT discovery

**Files:**
- Modify: `src/domainhunter/ingest/ct_orchestrator.py`
- Modify: `src/domainhunter/api.py`
- Test: `tests/test_discovery_api.py`

- [ ] **Step 1: Write failing scan-result tests.**

    Extend the existing fake-orchestrator test to construct:

    CTIngestRunSummary(
        certificates_seen=12, events_added=8, probes_run=3,
        candidates_created=1, next_cursor="42",
        roots_observed=8, strict_rejections=5,
    )

    Assert the POST response contains:

    {
        "status": "completed",
        "next_cursor": "42",
        "certificates_seen": 12,
        "events_added": 8,
        "roots_observed": 8,
        "strict_rejections": 5,
        "probes_run": 3,
        "candidates_created": 1,
    }

    Add a second case with `candidates_created=0` and assert `status == "no_candidates"`.

- [ ] **Step 2: Verify failure.**

    Run: `.venv/bin/python -m pytest tests/test_discovery_api.py -k 'strict or no_candidates' -v`

    Expected: FAIL because the dataclass has no new counters and the endpoint has no `status`.

- [ ] **Step 3: Keep FilteredCandidate evidence until candidate creation.**

    In `CTIngestOrchestrator.run_once`, keep the exact strict-filter result:

    filtered_candidates = self._filter_pipeline.run(new_roots, observed_at=stamp)
    filtered_by_domain = {item.domain: item for item in filtered_candidates}
    roots_to_probe = list(filtered_by_domain)
    strict_rejections = len(new_roots) - len(filtered_candidates)

    After `probe_domain` creates a candidate version, write a `CandidateVerification` populated from the matching `FilteredCandidate.s2` and `.s3`, `store.get_ct_first_seen_at(domain)`, and the persisted observation. Set `final_root_matches=True` only when this strict orchestrator passed `require_same_final_root=True`; non-strict callers leave it `None`.

    Extend `CTIngestRunSummary` with defaults so existing non-strict callers remain valid:

    @dataclass(frozen=True, slots=True)
    class CTIngestRunSummary:
        certificates_seen: int
        events_added: int
        probes_run: int
        candidates_created: int
        next_cursor: str | None
        roots_observed: int = 0
        strict_rejections: int = 0

- [ ] **Step 4: Return truthful one-shot scan states.**

    In `POST /v1/run/discovery`, return `status="completed"` only when `candidates_created > 0`; otherwise return `status="no_candidates"`. Preserve HTTP 502 and its error detail for exceptions. Do not introduce a progress percentage or infer scan phases in the browser.

- [ ] **Step 5: Verify and commit.**

    Run: `.venv/bin/python -m pytest tests/test_discovery_api.py -v`

    Expected: PASS.

    git add src/domainhunter/ingest/ct_orchestrator.py src/domainhunter/api.py tests/test_discovery_api.py
    git commit -m "feat: expose strict discovery evidence and outcomes"

## Task 3: Add pending-only inbox and candidate-detail read models

**Files:**
- Modify: `src/domainhunter/storage/sqlite.py`
- Modify: `src/domainhunter/api.py`
- Create: `tests/test_discovery_inbox_api.py`

- [ ] **Step 1: Write failing projection tests.**

    Seed one undecided candidate with a verification snapshot and one candidate with an unrevoked approval. Assert:

    def test_inbox_returns_only_pending_candidates(tmp_path) -> None:
        client, pending_id, _approved_id = seeded_inbox_client(tmp_path)
        items = client.get("/v1/review-queue").json()["items"]
        assert [item["candidate_id"] for item in items] == [pending_id]
        assert items[0]["review_state"] == "pending"
        assert items[0]["newness"]["status"] == "passed"
        assert items[0]["reachability"]["same_root"] is True

    def test_context_marks_missing_verification_unknown(tmp_path) -> None:
        client, candidate_id, version = seeded_candidate_without_verification(tmp_path)
        payload = client.get(
            f"/v1/candidates/{candidate_id}/versions/{version}/review-context"
        ).json()
        assert payload["newness"]["status"] == "unknown"
        assert payload["reachability"]["status"] == "unknown"

    Also test a 404 context response, cited evidence in the context, and chronological unrevoked decision history.

- [ ] **Step 2: Verify failure.**

    Run: `.venv/bin/python -m pytest tests/test_discovery_inbox_api.py -v`

    Expected: FAIL because the queue returns all review states and the context route does not exist.

- [ ] **Step 3: Add review-state readers and a shared presenter.**

    Add:

    def active_review_action(
        self, candidate_id: str, candidate_version: int
    ) -> ReviewAction | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT action FROM review_decisions
                WHERE candidate_id = ? AND candidate_version = ?
                  AND revoked_at IS NULL
                ORDER BY decided_at DESC, decision_id DESC LIMIT 1
                """,
                (candidate_id, candidate_version),
            ).fetchone()
        return ReviewAction(row["action"]) if row is not None else None

    It selects the latest `review_decisions` row with `revoked_at IS NULL`, ordered by `decided_at DESC, decision_id DESC`. In `GET /v1/review-queue`, omit any candidate whose active action is not `None`; every returned record gets `review_state: "pending"`.

    Add one API presenter used by both endpoints:

    def _review_projection(item, verification, review_state):
        return {
            "candidate_id": item.candidate.candidate_id,
            "version": item.latest_version.version,
            "domain": item.candidate.domain,
            "name_suggestion": item.latest_version.draft.name_suggestion,
            "description_suggestion": item.latest_version.draft.description_suggestion,
            "primary_outcome": item.latest_version.draft.primary_outcome.value,
            "classification_confidence": item.latest_version.draft.classification_confidence,
            "priority": {"score": item.priority.priority.score},
            "review_state": review_state,
            "newness": _newness_payload(verification),
            "reachability": _reachability_payload(verification),
        }

- [ ] **Step 4: Add the detail context endpoint.**

    Implement the helpers used by the presenter:

    def _newness_payload(verification: CandidateVerification | None) -> dict[str, object]:
        if verification is None:
            return {"status": "unknown", "ct_first_seen_at": None,
                    "rdap_tier": None, "rdap_age_days": None,
                    "rdap_registration_at": None}
        passed = verification.ct_first_seen_at is not None and verification.rdap_tier in {"tier1", "tier2"}
        return {
            "status": "passed" if passed else "unknown",
            "ct_first_seen_at": verification.ct_first_seen_at.isoformat() if verification.ct_first_seen_at else None,
            "rdap_tier": verification.rdap_tier,
            "rdap_age_days": verification.rdap_age_days,
            "rdap_registration_at": verification.rdap_registration_at.isoformat() if verification.rdap_registration_at else None,
        }

    def _reachability_payload(verification: CandidateVerification | None) -> dict[str, object]:
        if verification is None:
            return {"status": "unknown", "http_status_code": None,
                    "final_url": None, "canonical_url": None, "same_root": None}
        status = "passed" if verification.final_root_matches is True else (
            "failed" if verification.final_root_matches is False else "unknown"
        )
        return {"status": status, "http_status_code": verification.http_status_code,
                "final_url": verification.final_url,
                "canonical_url": verification.canonical_url,
                "same_root": verification.final_root_matches}

    Implement `GET /v1/candidates/{candidate_id}/versions/{candidate_version}/review-context`. It loads the exact version or returns 404; includes cited evidence and decision history; puts raw values under `audit`; and assigns evidence states as follows:

    - `newness.status = "passed"` only when the snapshot has `ct_first_seen_at` and RDAP tier `tier1` or `tier2`;
    - `reachability.status = "passed"` only when `final_root_matches is True`;
    - missing snapshot values are `"unknown"`, never a positive inference;
    - an explicitly stored `final_root_matches is False` becomes `"failed"`.

- [ ] **Step 5: Verify and commit.**

    Run: `.venv/bin/python -m pytest tests/test_discovery_inbox_api.py tests/test_review_api.py -v`

    Expected: PASS.

    git add src/domainhunter/storage/sqlite.py src/domainhunter/api.py tests/test_discovery_inbox_api.py tests/test_review_api.py
    git commit -m "feat: add truthful discovery inbox projections"

## Task 4: Build the candidate inbox and strict-scan dialog

**Files:**
- Modify: `src/domainhunter/api.py`
- Create: `tests/test_discovery_inbox_ui.py`

- [ ] **Step 1: Write failing HTML contracts.**

    def test_home_is_inbox_not_dashboard(tmp_path) -> None:
        response = TestClient(create_app(tmp_path / "ui.db")).get("/")
        assert response.status_code == 200
        assert 'data-page="inbox"' in response.text
        assert 'id="candidate-search"' in response.text
        assert 'id="scan-dialog"' in response.text
        assert "spark" not in response.text
        assert ">OPS<" not in response.text
        assert ">DISCOVERY<" not in response.text

    Add a test requiring `data-candidate-id`, a `查看证据` label, and an `/review/{candidate_id}` link on a rendered card.

- [ ] **Step 2: Verify failure.**

    Run: `.venv/bin/python -m pytest tests/test_discovery_inbox_ui.py -v`

    Expected: FAIL because the old queue table and KPI strip are still rendered.

- [ ] **Step 3: Replace the shared shell and CSS.**

    Replace dashboard styles with these primitives and one 56px top bar:

    :root {
      --canvas: #f7f4ed; --surface: #ffffff; --ink: #17202c;
      --muted: #5f6b7a; --line: #e1ddd5; --action: #1769e0;
      --verified: #007e72; --warning: #a66100; --danger: #b42318;
      --focus: #1d4ed8;
    }

    The only top-bar content is `DomainHunter`, `候选收件箱`, and a `扫描新网站` button. Remove metric strips, sparklines, global reviewer input, language toggles, and all four-page navigation.

- [ ] **Step 4: Implement inbox behavior with no inferred facts.**

    Replace `QUEUE_HTML`/script with `#inbox-summary`, `#candidate-search`, `#priority-candidate`, `#candidate-list`, `#scan-dialog`, and `#ui-status[aria-live="polite"]`.

    Persist only client navigation state:

    const stored = sessionStorage.getItem("domainhunter.inbox");
    const inboxState = stored ? JSON.parse(stored) : { query: "", scrollY: 0 };
    function persistInboxState(query) {
      sessionStorage.setItem(
        "domainhunter.inbox",
        JSON.stringify({ query: query, scrollY: window.scrollY }),
      );
    }

    Fetch `/v1/review-queue`, render the first item under `最高优先级`, and render remaining cards under `其他待审核候选`. Use only `newness` and `reachability` payload fields; show `未验证` for an unknown state. Search is the only first-release filter.

    The dialog posts once to `/v1/run/discovery`, disables duplicate submission, and renders `completed`, `no_candidates`, and failure as three different Chinese messages. It must not render a fake scan percentage.

- [ ] **Step 5: Verify and commit.**

    Run: `.venv/bin/python -m pytest tests/test_discovery_inbox_ui.py tests/test_discovery_api.py -v`

    Expected: PASS.

    git add src/domainhunter/api.py tests/test_discovery_inbox_ui.py
    git commit -m "feat: build the discovery inbox"

## Task 5: Build the independent evidence page and decisions

**Files:**
- Modify: `src/domainhunter/api.py`
- Modify: `tests/test_review_api.py`
- Modify: `tests/test_discovery_inbox_ui.py`

- [ ] **Step 1: Write failing detail-page contracts.**

    def test_detail_page_is_independent_and_evidence_led(tmp_path) -> None:
        database, candidate_id = seeded_reviewable_database(tmp_path)
        response = TestClient(create_app(database)).get(f"/review/{candidate_id}")
        assert response.status_code == 200
        assert 'data-page="candidate-detail"' in response.text
        assert 'id="candidate-detail"' in response.text
        assert 'id="newness-evidence"' in response.text
        assert 'id="reachability-evidence"' in response.text
        assert 'id="audit-details"' in response.text
        assert 'id="review-actions"' in response.text
        assert "nav-next" not in response.text

    Require the script to fetch `review-context`, send `X-Actor-ID`, and return to `/` after a successful decision.

- [ ] **Step 2: Verify failure.**

    Run: `.venv/bin/python -m pytest tests/test_review_api.py -v`

    Expected: FAIL because the old hero, gauge, next/previous navigation, and local approval cache remain.

- [ ] **Step 3: Replace detail markup with semantic sections.**

    <main class="detail-page" data-page="candidate-detail">
      <a class="back-link" href="/">← 返回候选收件箱</a>
      <section id="candidate-detail"></section>
      <section id="newness-evidence"></section>
      <section id="reachability-evidence"></section>
      <section id="product-evidence"></section>
      <details id="audit-details"><summary>查看原始审计数据</summary><pre></pre></details>
      <section id="review-actions" aria-label="审核决定"></section>
      <div id="review-dialog" role="dialog" aria-modal="true" hidden></div>
    </main>

    Render every evidence item as text status (`通过`/ `未验证`/ `不通过`), title, explanation, optional timestamp, and optional source link. External websites use `target="_blank" rel="noopener noreferrer"`.

- [ ] **Step 4: Implement explicit review submission.**

    async function submitDecision(action) {
      const actorId = await requireActorId();
      if (!actorId) return;
      if (["reject", "blocklist"].includes(action) && !window.confirm(confirmCopy[action])) return;
      setActionsDisabled(true);
      const response = await fetch(decisionUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Actor-ID": actorId },
        body: JSON.stringify({ request_id: crypto.randomUUID(), action, reason_tags: [] }),
      });
      if (response.ok) {
        sessionStorage.setItem("domainhunter.flash", decisionCopy[action]);
        location.assign("/");
        return;
      }
      renderDecisionError(await response.json());
      setActionsDisabled(false);
    }

    Handle 401 with an actor-ID dialog, 404 with a return-to-inbox state, 409 with a reload message, and all other failures visibly. Remove keyboard shortcuts, outreach controls, gauge charts, edit no-op, local approval cache, and automatic next-candidate navigation.

- [ ] **Step 5: Verify and commit.**

    Run: `.venv/bin/python -m pytest tests/test_review_api.py tests/test_discovery_inbox_ui.py -v`

    Expected: PASS.

    git add src/domainhunter/api.py tests/test_review_api.py tests/test_discovery_inbox_ui.py
    git commit -m "feat: add independent candidate evidence review"

## Task 6: Retire dashboard routes and verify real data

**Files:**
- Modify: `src/domainhunter/api.py`
- Modify: `tests/test_discovery_api.py`
- Modify: `README.md`

- [ ] **Step 1: Write failing legacy-route tests.**

    def test_legacy_dashboard_routes_redirect_to_inbox(tmp_path) -> None:
        client = TestClient(create_app(tmp_path / "legacy.db"))
        for path in ("/discovery", "/ops"):
            response = client.get(path, follow_redirects=False)
            assert response.status_code in {302, 303, 307}
            assert response.headers["location"] == "/"

- [ ] **Step 2: Verify failure.**

    Run: `.venv/bin/python -m pytest tests/test_discovery_api.py -k legacy -v`

    Expected: FAIL because both routes return old dashboard HTML with status 200.

- [ ] **Step 3: Redirect and remove dead UI.**

    Make `/discovery` and `/ops` return `RedirectResponse(url="/", status_code=303)`. Delete old Discovery/Ops templates and scripts, metric polling, sparkline helpers, queue-table styles, gauge styles, and retired translation keys. Keep the scan API, inbox API, review API, and external route compatibility.

    Rewrite the README review-console section to describe the inbox, independent evidence page, and one-shot strict scan. State that a real scan result can be completed, empty, or failed.

- [ ] **Step 4: Run full automated and browser verification.**

    Run: `.venv/bin/python -m pytest -q`

    Expected: all tests PASS.

    Run:

    .venv/bin/domainhunter serve --database work/live-ct-strict-nimbus-final-root-20260903-05.db --host 127.0.0.1 --port 8034

    With gstack browse, inspect `/` and its top candidate detail at desktop and 390px viewport. Confirm: the top candidate appears without a KPI strip, missing historical verification shows `未验证`, no browser console errors occur, scan failure differs from zero candidates, and the detail page never auto-advances.

- [ ] **Step 5: Commit only after verification.**

    git add src/domainhunter/api.py tests/test_discovery_api.py README.md
    git commit -m "feat: retire dashboard routes for discovery inbox"
    git status --short

## Plan self-review

### Spec coverage

- Inbox, independent detail route, true scan dialog, Chinese copy, mobile behavior, keyboard/accessibility, no dashboard navigation, and real-data-only rendering are covered by Tasks 3–6.
- Persistent CT/RDAP/DNS/HTTP facts needed to support truthful evidence are covered by Tasks 1–3.
- Completed, zero-candidate, and failed scan outcomes are explicitly covered by Task 2 and Task 4.
- Actor ID, destructive confirmations, idempotency, and concurrent-decision feedback are covered by Task 5.

### Placeholder scan

The plan defines every new model, storage API, endpoint semantics, page anchor, test command, and commit boundary. It intentionally does not add a chart library, a frontend build system, a mock-data layer, or a third main screen.

### Type consistency

`CandidateVerification`, `CTIngestRunSummary.roots_observed`, `CTIngestRunSummary.strict_rejections`, `review_state`, `newness`, `reachability`, and `review-context` use the same names in their defining task and their consuming task.
