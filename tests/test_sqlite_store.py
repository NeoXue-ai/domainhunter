import sqlite3
from datetime import UTC, datetime, timedelta

from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.events import SourceEvent
from domainhunter.domain.exposure import ExposureChannel, ExposureCheck, ExposureStatus
from domainhunter.domain.observations import Observation, OutcomeCode
from domainhunter.domain.outreach import OutreachEvent
from domainhunter.domain.publications import PublicationRecord
from domainhunter.domain.review_priority import (
    ReviewPriorityInputs,
    calculate_review_priority,
)
from domainhunter.domain.reviews import ReasonTag, ReviewAction, build_review_decision
from domainhunter.publish.aiknows_client import SyncStatus
from domainhunter.storage.sqlite import SQLiteStore

OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def test_appends_source_event_once_and_maps_it_to_the_registrable_domain(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "app.example.co.uk", OBSERVED_AT)

    assert store.append_source_event(event, hostname="app.example.co.uk") is True
    assert store.append_source_event(event, hostname="app.example.co.uk") is False
    assert store.list_domains() == ("example.co.uk",)
    assert store.event_domains(event.idempotency_key) == ("example.co.uk",)


def test_ct_source_event_atomically_records_its_first_seen_time(tmp_path) -> None:
    """A persisted CT event must never outlive its first-seen audit record."""
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "app.new-site.ai", OBSERVED_AT)

    assert store.append_source_event(event, hostname="app.new-site.ai") is True
    assert store.get_ct_first_seen_at("new-site.ai") == OBSERVED_AT


def test_store_rebuilds_ct_discovery_state_for_pre_queue_events(tmp_path) -> None:
    """Opening an upgraded database recovers CT events written before the work queue."""
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    event = SourceEvent("ct_log", "argon:42", "new-site.ai", OBSERVED_AT)
    assert store.append_source_event(event, hostname="new-site.ai") is True

    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM ct_discovery_work")
        connection.execute("DELETE FROM ct_seen_domains")
        connection.commit()

    upgraded = SQLiteStore(database)
    work = upgraded.claim_ct_discovery_work(
        now=OBSERVED_AT, lease_seconds=60, limit=1
    )

    assert [item.domain for item in work] == ["new-site.ai"]
    assert upgraded.get_ct_first_seen_at("new-site.ai") == OBSERVED_AT


def test_ct_discovery_work_uses_a_lease_and_can_be_retried(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "new-site.ai", OBSERVED_AT)
    store.append_source_event(event, hostname="new-site.ai")

    first = store.claim_ct_discovery_work(
        now=OBSERVED_AT, lease_seconds=60, limit=1
    )

    assert len(first) == 1
    assert first[0].domain == "new-site.ai"
    assert first[0].attempt_number == 1
    assert store.claim_ct_discovery_work(
        now=OBSERVED_AT, lease_seconds=60, limit=1
    ) == ()

    assert store.retry_ct_discovery_work(
        "new-site.ai",
        lease_token=first[0].lease_token,
        scheduled_at=OBSERVED_AT,
        error="dns_not_ready",
    ) is True

    second = store.claim_ct_discovery_work(
        now=OBSERVED_AT, lease_seconds=60, limit=1
    )
    assert len(second) == 1
    assert second[0].attempt_number == 2


def test_appends_source_event_with_issuer_and_persists_it(tmp_path) -> None:
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    event = SourceEvent(
        "ct_log",
        "argon:42",
        "example.com",
        OBSERVED_AT,
        issuer="Let's Encrypt",
    )

    assert store.append_source_event(event, hostname="example.com") is True

    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT issuer FROM source_events WHERE idempotency_key = ?",
            (event.idempotency_key,),
        ).fetchone()
    assert row[0] == "Let's Encrypt"


def test_legacy_database_is_migrated_to_include_issuer_column(tmp_path) -> None:
    database = tmp_path / "domainhunter.db"
    connection = sqlite3.connect(database)
    try:
        connection.executescript(
            """
            CREATE TABLE source_events (
                id INTEGER PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                raw_subject TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                source_timestamp TEXT,
                evidence_summary TEXT,
                parser_version TEXT
            );
            """
        )
        connection.execute(
            "INSERT INTO source_events (idempotency_key, source, source_event_id, raw_subject, observed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("legacy-key", "ct_log", "legacy:1", "legacy.example.com", OBSERVED_AT.isoformat()),
        )
        connection.commit()
    finally:
        connection.close()

    store = SQLiteStore(database)

    with sqlite3.connect(database) as verify:
        columns = {row[1] for row in verify.execute("PRAGMA table_info(source_events)").fetchall()}
    assert "issuer" in columns

    event = SourceEvent(
        "ct_log",
        "argon:42",
        "example.com",
        OBSERVED_AT,
        issuer="Let's Encrypt",
    )
    assert store.append_source_event(event, hostname="example.com") is True

    with sqlite3.connect(database) as verify:
        row = verify.execute(
            "SELECT issuer FROM source_events WHERE idempotency_key = ?",
            (event.idempotency_key,),
        ).fetchone()
    assert row[0] == "Let's Encrypt"


def test_appends_observations_without_overwriting_prior_results(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    first = Observation("example.com", OutcomeCode.CONNECT_TIMEOUT, OBSERVED_AT, 1)
    second = Observation(
        "example.com",
        OutcomeCode.SUCCESS,
        OBSERVED_AT.replace(hour=1),
        2,
        status_code=200,
        final_url="https://example.com",
    )

    store.append_observation(first)
    store.append_observation(second)

    assert store.list_observations("www.example.com") == (first, second)


def test_persists_canonical_url_and_internal_links_with_observations(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    observation = Observation(
        "example.com",
        OutcomeCode.SUCCESS,
        OBSERVED_AT,
        1,
        status_code=200,
        final_url="https://example.com",
        canonical_url="https://example.com/",
        internal_links=(
            "https://example.com/about",
            "https://example.com/pricing",
        ),
    )

    store.append_observation(observation)
    reloaded = store.list_observations("example.com")

    assert reloaded == (observation,)


def test_upgrades_legacy_observations_table_with_canonical_columns(tmp_path) -> None:
    database = tmp_path / "domainhunter.db"
    store = SQLiteStore(database)
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    legacy = Observation(
        "example.com",
        OutcomeCode.SUCCESS,
        OBSERVED_AT,
        1,
    )
    store.append_observation(legacy)

    reopened = SQLiteStore(database)

    assert reopened.list_observations("example.com")[0].canonical_url is None
    assert reopened.list_observations("example.com")[0].internal_links == ()


def test_lists_domains_due_for_the_next_bounded_retry(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    observation = Observation(
        "example.com",
        OutcomeCode.CONTENT_INSUFFICIENT,
        OBSERVED_AT,
        1,
    )
    store.append_observation(observation)

    assert store.due_domains(OBSERVED_AT + timedelta(hours=23)) == ()
    assert store.due_domains(OBSERVED_AT + timedelta(hours=24)) == ("example.com",)


def test_persists_source_cursor_for_restartable_polling(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")

    assert store.get_source_cursor("ct_log") is None
    store.set_source_cursor("ct_log", "argon:43")

    assert store.get_source_cursor("ct_log") == "argon:43"


def test_appends_candidate_versions_without_overwriting_model_evidence(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    candidate = store.create_candidate("app.example.com", created_at=OBSERVED_AT)
    replay = store.create_candidate("www.example.com", created_at=OBSERVED_AT.replace(hour=1))
    draft = CandidateVersionDraft(
        author_kind="llm",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.82,
        name_suggestion="Example AI",
        description_suggestion="An AI writing assistant.",
        evidence=(
            Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),
        ),
        model_version="gpt-5.6",
    )

    assert replay.candidate_id == candidate.candidate_id
    first = store.append_candidate_version(candidate.candidate_id, draft, created_at=OBSERVED_AT)
    second = store.append_candidate_version(
        candidate.candidate_id,
        draft,
        created_at=OBSERVED_AT.replace(hour=1),
    )
    versions = store.list_candidate_versions(candidate.candidate_id)

    assert first.version == 1
    assert second.version == 2
    assert len(versions) == 2
    assert versions[0].draft.model_version == "gpt-5.6"
    assert versions[0].draft.evidence[0].quote == "Example AI"


def test_appends_review_decisions_idempotently_and_keeps_action_history(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    draft = CandidateVersionDraft(
        author_kind="rule",
        primary_outcome=CandidateOutcome.VALID_BUT_NOT_READY,
        classification_confidence=0.42,
        name_suggestion="Example",
        description_suggestion=None,
        evidence=(Evidence(EvidenceType.TITLE, "Example", "https://example.com"),),
    )
    store.append_candidate_version(candidate.candidate_id, draft, created_at=OBSERVED_AT)
    decision = build_review_decision(
        request_id="request-1",
        candidate_id=candidate.candidate_id,
        candidate_version=1,
        action=ReviewAction.DEFER,
        actor_id="reviewer-1",
        decided_at=OBSERVED_AT,
        reason_tags=(ReasonTag.INSUFFICIENT_EVIDENCE,),
    )

    assert store.append_review_decision(decision) is True
    assert store.append_review_decision(decision) is False
    decisions = store.list_review_decisions(candidate.candidate_id)

    assert len(decisions) == 1
    assert decisions[0].action is ReviewAction.DEFER
    assert decisions[0].reason_tags == (ReasonTag.INSUFFICIENT_EVIDENCE,)


def test_persists_exposure_checks_and_review_priority_snapshots(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    check = ExposureCheck(
        channel=ExposureChannel.PRODUCT_HUNT,
        checked_at=OBSERVED_AT,
        status=ExposureStatus.NOT_OBSERVED,
        query="Example AI",
    )
    priority = calculate_review_priority(
        ReviewPriorityInputs(
            product_evidence=0.8,
            early_presence=0.9,
            low_exposure=check.low_exposure_score,
            data_completeness=0.7,
        )
    )

    store.append_exposure_check(candidate.candidate_id, check)
    store.append_review_priority(candidate.candidate_id, priority, calculated_at=OBSERVED_AT)

    assert store.list_exposure_checks(candidate.candidate_id) == (check,)
    snapshot = store.latest_review_priority(candidate.candidate_id)
    assert snapshot is not None
    assert snapshot.priority == priority


def test_appends_publication_attempts_for_reconciliation(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    draft = CandidateVersionDraft(
        author_kind="human",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.9,
        name_suggestion="Example AI",
        description_suggestion="AI workflow automation",
        evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
    )
    version = store.append_candidate_version(candidate.candidate_id, draft, created_at=OBSERVED_AT)
    record = PublicationRecord(
        candidate_id=candidate.candidate_id,
        candidate_version=version.version,
        requested_at=OBSERVED_AT,
        sync_status=SyncStatus.RECONCILIATION_REQUIRED,
        detail="unknown external result",
    )

    assert store.append_publication(record) is True
    assert store.list_publications(candidate.candidate_id) == (record,)


def test_reports_a_funnel_snapshot_with_observation_outcomes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "argon:42", "example.com", OBSERVED_AT)
    store.append_source_event(event, hostname="example.com")
    store.append_observation(
        Observation("example.com", OutcomeCode.CONNECT_TIMEOUT, OBSERVED_AT, 1)
    )

    metrics = store.funnel_metrics()

    assert metrics.source_events == 1
    assert metrics.domains == 1
    assert metrics.observations == 1
    assert metrics.observation_outcomes == {"connect_timeout": 1}


def test_appends_outreach_events_and_lists_them_in_chronological_order(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    candidate = store.create_candidate("example.com", created_at=OBSERVED_AT)
    draft = CandidateVersionDraft(
        author_kind="human",
        primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
        classification_confidence=0.9,
        name_suggestion="Example AI",
        description_suggestion="AI workflow automation",
        evidence=(Evidence(EvidenceType.TITLE, "Example AI", "https://example.com"),),
    )
    version = store.append_candidate_version(
        candidate.candidate_id, draft, created_at=OBSERVED_AT
    )
    first = OutreachEvent(
        candidate_id=candidate.candidate_id,
        candidate_version=version.version,
        actor_id="actor-1",
        triggered_at=OBSERVED_AT,
        dry_run=True,
        recipient_source_url="https://example.com/contact",
        claim_tokens_issued=0,
        contact_count=1,
        contact_preview_json='[{"redacted_address":"f*******@example.com","source_url":"https://example.com/contact"}]',
    )
    second = OutreachEvent(
        candidate_id=candidate.candidate_id,
        candidate_version=version.version,
        actor_id="actor-1",
        triggered_at=OBSERVED_AT.replace(hour=1),
        dry_run=False,
        recipient_source_url="https://example.com/contact",
        claim_tokens_issued=2,
        contact_count=2,
        contact_preview_json="[]",
    )

    store.append_outreach_event(first)
    store.append_outreach_event(second)

    events = store.list_outreach_events(candidate.candidate_id)
    assert events == (first, second)


def test_mark_seen_tracks_first_seen_history(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    at = datetime(2026, 8, 16, tzinfo=UTC)
    later = datetime(2026, 8, 17, tzinfo=UTC)

    new_count = store.mark_seen(("a.com", "b.com"), at=at, source="ct")
    assert new_count == 2
    assert store.is_seen("a.com") is True
    assert store.is_seen("b.com") is True
    assert store.is_seen("c.com") is False
    assert store.seen_domain_count() == 2

    # Re-marking keeps the first-seen timestamp.
    store.mark_seen(("a.com",), at=later, source="ct")
    assert store.seen_domain_count() == 2


def test_filter_new_returns_only_never_seen(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    at = datetime(2026, 8, 16, tzinfo=UTC)
    store.mark_seen(("old.com",), at=at, source="ct")

    fresh = store.filter_new(("old.com", "new.com", "brandnew.io"))
    assert fresh == ("new.com", "brandnew.io")


def test_filter_new_empty_and_unknown(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "domainhunter.db")
    assert store.filter_new(()) == ()
    assert store.filter_new(("x.com", "y.com")) == ("x.com", "y.com")
    assert store.seen_domain_count() == 0
