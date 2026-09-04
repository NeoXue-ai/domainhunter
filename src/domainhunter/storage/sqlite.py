"""Append-only local SQLite persistence for source signals and observations."""

import json
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from domainhunter.domain.alerts import Alert, AlertKind, AlertSeverity
from domainhunter.domain.audit import AIKnowsAuditEntry
from domainhunter.domain.candidates import (
    Candidate,
    CandidateOutcome,
    CandidateVersion,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
    build_candidate,
)
from domainhunter.domain.claims import ClaimTokenRecord, hash_claim_token
from domainhunter.domain.events import SourceEvent
from domainhunter.domain.exposure import ExposureChannel, ExposureCheck, ExposureStatus
from domainhunter.domain.metrics import FunnelAnalytics, FunnelMetrics, percentile
from domainhunter.domain.normalization import normalize_hostname
from domainhunter.domain.observations import Observation, OutcomeCode
from domainhunter.domain.outreach import OutreachEvent
from domainhunter.domain.publications import PublicationRecord
from domainhunter.domain.reopens import ReopenEvent
from domainhunter.domain.retry_policy import decide_next_action
from domainhunter.domain.review_priority import ReviewPriority, ReviewPrioritySnapshot
from domainhunter.domain.review_queue import ReviewQueueItem
from domainhunter.domain.reviews import ReasonTag, ReviewAction, ReviewDecision
from domainhunter.domain.work_queue import BudgetReservation, WorkLease, WorkStage
from domainhunter.domain.verification import CandidateVerification
from domainhunter.publish.aiknows_client import SyncStatus


class ConcurrentDecisionError(Exception):
    """Raised when a different request_id already holds an active decision.

    Decisions are append-only; once an unrevoked decision exists for a
    ``(candidate_id, candidate_version)`` pair, a second decision with a
    different ``request_id`` is a spec §11 concurrent conflict. The original
    active decision identifiers are surfaced so the API layer can return a
    structured 409 response.
    """

    def __init__(self, active_decision_id: str, active_request_id: str) -> None:
        super().__init__(
            f"candidate version already has an active decision "
            f"(decision_id={active_decision_id}, request_id={active_request_id})"
        )
        self.active_decision_id = active_decision_id
        self.active_request_id = active_request_id


@dataclass(frozen=True, slots=True)
class CTDiscoveryWorkLease:
    """One exclusive, retryable unit of CT discovery work."""

    domain: str
    attempt_number: int
    lease_token: str


DEFAULT_DAILY_BUDGET: Mapping[WorkStage, float] = {
    WorkStage.SIGNAL_INGEST: 100000.0,
    WorkStage.L1: 100.0,
    WorkStage.L2: 10.0,
    WorkStage.L3: 5.0,
    WorkStage.LLM: 20.0,
    WorkStage.PUBLICATION: 50.0,
    WorkStage.EXPOSURE: 20.0,
    WorkStage.CLAIM: 100.0,
}


_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_events (
    id INTEGER PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    raw_subject TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source_timestamp TEXT,
    evidence_summary TEXT,
    parser_version TEXT,
    issuer TEXT
);

CREATE TABLE IF NOT EXISTS domains (
    domain TEXT PRIMARY KEY,
    first_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_domains (
    event_id INTEGER NOT NULL REFERENCES source_events(id),
    domain TEXT NOT NULL REFERENCES domains(domain),
    PRIMARY KEY (event_id, domain)
);

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY,
    domain TEXT NOT NULL REFERENCES domains(domain),
    outcome_code TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status_code INTEGER,
    final_url TEXT,
    detail TEXT,
    canonical_url TEXT,
    internal_links_json TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS source_cursors (
    source TEXT PRIMARY KEY,
    cursor TEXT
);

CREATE TABLE IF NOT EXISTS ct_seen_domains (
    domain TEXT PRIMARY KEY,
    first_seen_at TEXT NOT NULL,
    first_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ct_discovery_work (
    domain TEXT PRIMARY KEY REFERENCES domains(domain),
    first_observed_at TEXT NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    lease_token TEXT,
    lease_expires_at TEXT,
    completed_at TEXT,
    completion_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_ct_discovery_work_due
    ON ct_discovery_work (completed_at, next_attempt_at, lease_expires_at, first_observed_at);

CREATE TABLE IF NOT EXISTS candidates (
    candidate_id TEXT PRIMARY KEY,
    domain TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candidate_versions (
    candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    author_kind TEXT NOT NULL,
    primary_outcome TEXT NOT NULL,
    classification_confidence REAL NOT NULL,
    name_suggestion TEXT,
    description_suggestion TEXT,
    category TEXT,
    tags_json TEXT NOT NULL,
    pricing_model TEXT,
    target_audience TEXT,
    model_version TEXT,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY (candidate_id, version)
);

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
);

CREATE TABLE IF NOT EXISTS review_decisions (
    decision_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    candidate_version INTEGER NOT NULL,
    action TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    reason_tags_json TEXT NOT NULL,
    revoked_at TEXT,
    revoked_by TEXT,
    revoke_reason TEXT,
    FOREIGN KEY (candidate_id, candidate_version)
        REFERENCES candidate_versions(candidate_id, version)
);

CREATE TABLE IF NOT EXISTS exposure_checks (
    id INTEGER PRIMARY KEY,
    candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
    channel TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    status TEXT NOT NULL,
    query TEXT NOT NULL,
    evidence_url TEXT,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS review_priorities (
    id INTEGER PRIMARY KEY,
    candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
    calculated_at TEXT NOT NULL,
    score REAL NOT NULL,
    product_evidence_contribution REAL NOT NULL,
    early_presence_contribution REAL NOT NULL,
    low_exposure_contribution REAL NOT NULL,
    data_completeness_contribution REAL NOT NULL,
    formula_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publications (
    id INTEGER PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    candidate_version INTEGER NOT NULL,
    requested_at TEXT NOT NULL,
    sync_status TEXT NOT NULL,
    external_entry_id TEXT,
    external_version TEXT,
    publication_status TEXT,
    field_errors_json TEXT NOT NULL,
    detail TEXT,
    FOREIGN KEY (candidate_id, candidate_version)
        REFERENCES candidate_versions(candidate_id, version)
);

CREATE TABLE IF NOT EXISTS work_queue (
    id INTEGER PRIMARY KEY,
    stage TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    lease_token TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    completed_at TEXT,
    UNIQUE(stage, entity_id)
);

CREATE TABLE IF NOT EXISTS budget_ledger (
    id INTEGER PRIMARY KEY,
    stage TEXT NOT NULL,
    entity_id TEXT,
    occurred_at TEXT NOT NULL,
    units REAL NOT NULL,
    status TEXT NOT NULL,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS claim_tokens (
    id INTEGER PRIMARY KEY,
    candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
    token_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    used_at TEXT
);

CREATE TABLE IF NOT EXISTS outreach_events (
    id INTEGER PRIMARY KEY,
    candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
    candidate_version INTEGER NOT NULL,
    actor_id TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    dry_run INTEGER NOT NULL,
    recipient_source_url TEXT,
    claim_tokens_issued INTEGER NOT NULL,
    contact_count INTEGER NOT NULL,
    contact_preview_json TEXT NOT NULL,
    FOREIGN KEY (candidate_id, candidate_version) REFERENCES candidate_versions(candidate_id, version)
);

CREATE TABLE IF NOT EXISTS ai_knows_audit (
    id INTEGER PRIMARY KEY,
    method TEXT NOT NULL,
    url TEXT NOT NULL,
    status_code INTEGER,
    latency_ms REAL NOT NULL,
    candidate_id TEXT,
    candidate_version INTEGER,
    occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reopen_events (
    reopen_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    trigger_source_event_id TEXT NOT NULL,
    previous_outcome TEXT NOT NULL,
    new_outcome TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    reopened_at TEXT NOT NULL,
    FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id)
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    runbook_id TEXT NOT NULL,
    related_candidate_id TEXT,
    related_stage TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_alerts_fingerprint
    ON alerts (kind, related_candidate_id, related_stage, opened_at);

CREATE TABLE IF NOT EXISTS budget_config (
    stage TEXT PRIMARY KEY,
    daily_limit REAL NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stage_pauses (
    id INTEGER PRIMARY KEY,
    stage TEXT NOT NULL,
    paused INTEGER NOT NULL,
    reason TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    paused_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stage_pauses_stage_at
    ON stage_pauses (stage, paused_at);
"""


class SQLiteStore:
    """A small local store that preserves source and observation history."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript(_SCHEMA)
            self._ensure_source_events_issuer_column(connection)
            self._upgrade_observations_table(connection)
            self._upgrade_review_decisions_table(connection)
            self._upgrade_ct_discovery_work_table(connection)
            self._backfill_ct_discovery_state(connection)

    @staticmethod
    def _ensure_source_events_issuer_column(connection: sqlite3.Connection) -> None:
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(source_events)").fetchall()
        }
        if "issuer" not in columns:
            connection.execute("ALTER TABLE source_events ADD COLUMN issuer TEXT")

    @staticmethod
    def _upgrade_observations_table(connection: sqlite3.Connection) -> None:
        existing = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(observations)").fetchall()
        }
        if "canonical_url" not in existing:
            connection.execute("ALTER TABLE observations ADD COLUMN canonical_url TEXT")
        if "internal_links_json" not in existing:
            connection.execute(
                "ALTER TABLE observations ADD COLUMN internal_links_json TEXT NOT NULL DEFAULT '[]'"
            )

    @staticmethod
    def _upgrade_review_decisions_table(connection: sqlite3.Connection) -> None:
        existing = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(review_decisions)").fetchall()
        }
        if "revoked_at" not in existing:
            connection.execute("ALTER TABLE review_decisions ADD COLUMN revoked_at TEXT")
        if "revoked_by" not in existing:
            connection.execute("ALTER TABLE review_decisions ADD COLUMN revoked_by TEXT")
        if "revoke_reason" not in existing:
            connection.execute("ALTER TABLE review_decisions ADD COLUMN revoke_reason TEXT")

    @staticmethod
    def _upgrade_ct_discovery_work_table(connection: sqlite3.Connection) -> None:
        existing = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(ct_discovery_work)").fetchall()
        }
        if "attempt_number" not in existing:
            connection.execute(
                "ALTER TABLE ct_discovery_work ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 0"
            )
        if "next_attempt_at" not in existing:
            connection.execute("ALTER TABLE ct_discovery_work ADD COLUMN next_attempt_at TEXT")
            connection.execute(
                """
                UPDATE ct_discovery_work
                SET next_attempt_at = first_observed_at
                WHERE next_attempt_at IS NULL
                """
            )
        if "last_error" not in existing:
            connection.execute("ALTER TABLE ct_discovery_work ADD COLUMN last_error TEXT")
        if "lease_token" not in existing:
            connection.execute("ALTER TABLE ct_discovery_work ADD COLUMN lease_token TEXT")
        if "lease_expires_at" not in existing:
            connection.execute("ALTER TABLE ct_discovery_work ADD COLUMN lease_expires_at TEXT")

    @staticmethod
    def _backfill_ct_discovery_state(connection: sqlite3.Connection) -> None:
        """Recover durable CT state for events persisted before queue support.

        The inserts are intentionally idempotent: current installations write both
        records with each event, while an upgraded installation gains work and a
        first-seen timestamp for every historical CT event exactly once.
        """
        connection.execute(
            """
            INSERT OR IGNORE INTO ct_discovery_work (
                domain, first_observed_at, next_attempt_at
            )
            SELECT event_domains.domain, MIN(source_events.observed_at),
                   MIN(source_events.observed_at)
            FROM event_domains
            JOIN source_events ON source_events.id = event_domains.event_id
            WHERE source_events.source = 'ct_log'
            GROUP BY event_domains.domain
            """
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO ct_seen_domains (
                domain, first_seen_at, first_source
            )
            SELECT event_domains.domain, MIN(source_events.observed_at), 'ct_log'
            FROM event_domains
            JOIN source_events ON source_events.id = event_domains.event_id
            WHERE source_events.source = 'ct_log'
            GROUP BY event_domains.domain
            """
        )

    def append_source_event(self, event: SourceEvent, *, hostname: str) -> bool:
        """Append a source event exactly once and link it to a root domain."""
        domain = normalize_hostname(hostname).registrable_domain
        with self._connection() as connection:
            self._ensure_source_events_issuer_column(connection)
            event_cursor = connection.execute(
                """
                INSERT OR IGNORE INTO source_events (
                    idempotency_key, source, source_event_id, raw_subject, observed_at,
                    source_timestamp, evidence_summary, parser_version, issuer
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.idempotency_key,
                    event.source,
                    event.source_event_id,
                    event.raw_subject,
                    event.observed_at.isoformat(),
                    event.source_timestamp.isoformat() if event.source_timestamp else None,
                    event.evidence_summary,
                    event.parser_version,
                    event.issuer,
                ),
            )
            if event_cursor.rowcount == 0:
                return False

            event_id = event_cursor.lastrowid
            connection.execute(
                "INSERT OR IGNORE INTO domains (domain, first_seen_at) VALUES (?, ?)",
                (domain, event.observed_at.isoformat()),
            )
            connection.execute(
                "INSERT INTO event_domains (event_id, domain) VALUES (?, ?)",
                (event_id, domain),
            )
            if event.source == "ct_log":
                connection.execute(
                    """
                    INSERT OR IGNORE INTO ct_discovery_work (
                        domain, first_observed_at, next_attempt_at
                    ) VALUES (?, ?, ?)
                    """,
                    (domain, event.observed_at.isoformat(), event.observed_at.isoformat()),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO ct_seen_domains (
                        domain, first_seen_at, first_source
                    ) VALUES (?, ?, ?)
                    """,
                    (domain, event.observed_at.isoformat(), event.source),
                )
        return True

    def list_domains(self) -> tuple[str, ...]:
        """Return known registrable domains in deterministic lexical order."""
        with self._connection() as connection:
            rows = connection.execute("SELECT domain FROM domains ORDER BY domain").fetchall()
        return tuple(row["domain"] for row in rows)

    def mark_seen(
        self, domains: tuple[str, ...], *, at: datetime, source: str
    ) -> int:
        """Record that CT logs showed these domains, keeping first-seen history.

        Returns the number of domains that were *new* (first time seen).
        """
        new_count = 0
        at_iso = at.isoformat()
        with self._connection() as connection:
            for domain in domains:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO ct_seen_domains (domain, first_seen_at, first_source)
                    VALUES (?, ?, ?)
                    """,
                    (domain, at_iso, source),
                )
                if cursor.rowcount:
                    new_count += 1
        return new_count

    def get_ct_first_seen_at(self, domain: str) -> datetime | None:
        """Return the durable first CT observation time for one root domain."""
        normalized = normalize_hostname(domain).registrable_domain
        with self._connection() as connection:
            row = connection.execute(
                "SELECT first_seen_at FROM ct_seen_domains WHERE domain = ?",
                (normalized,),
            ).fetchone()
        return datetime.fromisoformat(row["first_seen_at"]) if row is not None else None

    def list_pending_ct_discovery_domains(self, *, limit: int) -> tuple[str, ...]:
        """Return a bounded oldest-first batch of CT roots awaiting discovery."""
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT domain FROM ct_discovery_work
                WHERE completed_at IS NULL AND lease_token IS NULL
                ORDER BY next_attempt_at, first_observed_at, domain
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(row["domain"] for row in rows)

    def pending_ct_discovery_work_count(self) -> int:
        """Count unfinished CT roots, including scheduled retries and active leases."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM ct_discovery_work WHERE completed_at IS NULL"
            ).fetchone()
        return int(row["count"])

    def claim_ct_discovery_work(
        self, *, now: datetime, lease_seconds: float, limit: int
    ) -> tuple[CTDiscoveryWorkLease, ...]:
        """Lease due CT roots once so overlapping scans cannot duplicate probes."""
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if lease_seconds <= 0 or limit < 1:
            raise ValueError("lease_seconds and limit must be positive")
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT domain, attempt_number FROM ct_discovery_work
                WHERE completed_at IS NULL
                  AND COALESCE(next_attempt_at, first_observed_at) <= ?
                  AND (lease_token IS NULL OR lease_expires_at <= ?)
                ORDER BY next_attempt_at, first_observed_at, domain
                LIMIT ?
                """,
                (now.isoformat(), now.isoformat(), limit),
            ).fetchall()
            leases: list[CTDiscoveryWorkLease] = []
            for row in rows:
                token = uuid4().hex
                connection.execute(
                    """
                    UPDATE ct_discovery_work
                    SET attempt_number = attempt_number + 1,
                        lease_token = ?, lease_expires_at = ?
                    WHERE domain = ?
                    """,
                    (token, lease_expires_at.isoformat(), row["domain"]),
                )
                leases.append(
                    CTDiscoveryWorkLease(
                        domain=row["domain"],
                        attempt_number=int(row["attempt_number"]) + 1,
                        lease_token=token,
                    )
                )
        return tuple(leases)

    def retry_ct_discovery_work(
        self,
        domain: str,
        *,
        lease_token: str,
        scheduled_at: datetime,
        error: str,
    ) -> bool:
        """Release a failed lease so the same root is retried at a known time."""
        if scheduled_at.tzinfo is None:
            raise ValueError("scheduled_at must be timezone-aware")
        if not lease_token or not error.strip():
            raise ValueError("lease_token and error must not be empty")
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE ct_discovery_work
                SET next_attempt_at = ?, last_error = ?,
                    lease_token = NULL, lease_expires_at = NULL
                WHERE domain = ? AND lease_token = ? AND completed_at IS NULL
                """,
                (scheduled_at.isoformat(), error, domain, lease_token),
            )
        return cursor.rowcount == 1

    def complete_ct_discovery_domain(
        self, domain: str, *, lease_token: str, at: datetime, reason: str
    ) -> bool:
        """Mark one CT root terminal only after its discovery work is complete."""
        if at.tzinfo is None:
            raise ValueError("at must be timezone-aware")
        if not lease_token or not reason.strip():
            raise ValueError("lease_token and reason must not be empty")
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE ct_discovery_work
                SET completed_at = ?, completion_reason = ?,
                    lease_token = NULL, lease_expires_at = NULL
                WHERE domain = ? AND lease_token = ? AND completed_at IS NULL
                """,
                (at.isoformat(), reason, domain, lease_token),
            )
        return cursor.rowcount == 1

    def is_seen(self, domain: str) -> bool:
        """Return True if the domain was ever observed in a CT log."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT 1 FROM ct_seen_domains WHERE domain = ?", (domain,)
            ).fetchone()
        return row is not None

    def filter_new(self, domains: tuple[str, ...]) -> tuple[str, ...]:
        """Return only domains that have never been seen in a CT log before."""
        if not domains:
            return ()
        placeholders = ",".join("?" for _ in domains)
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT domain FROM ct_seen_domains WHERE domain IN ({placeholders})",
                domains,
            ).fetchall()
        known = {row["domain"] for row in rows}
        return tuple(d for d in domains if d not in known)

    def seen_domain_count(self) -> int:
        """Total number of domains ever observed across CT logs."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS n FROM ct_seen_domains"
            ).fetchone()
        return int(row["n"])

    def event_domains(self, idempotency_key: str) -> tuple[str, ...]:
        """Return domains linked to one persisted source event."""
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT event_domains.domain
                FROM event_domains
                JOIN source_events ON source_events.id = event_domains.event_id
                WHERE source_events.idempotency_key = ?
                ORDER BY event_domains.domain
                """,
                (idempotency_key,),
            ).fetchall()
        return tuple(row["domain"] for row in rows)

    def append_observation(self, observation: Observation) -> int:
        """Append an observation for a known root domain without replacing history."""
        domain = normalize_hostname(observation.domain).registrable_domain
        with self._connection() as connection:
            known_domain = connection.execute(
                "SELECT 1 FROM domains WHERE domain = ?", (domain,)
            ).fetchone()
            if known_domain is None:
                raise ValueError(f"cannot observe unknown domain: {domain}")
            cursor = connection.execute(
                """
                INSERT INTO observations (
                    domain, outcome_code, observed_at, attempt_number,
                    status_code, final_url, detail, canonical_url, internal_links_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    domain,
                    observation.outcome_code.value,
                    observation.observed_at.isoformat(),
                    observation.attempt_number,
                    observation.status_code,
                    observation.final_url,
                    observation.detail,
                    observation.canonical_url,
                    json.dumps(tuple(observation.internal_links), ensure_ascii=False),
                ),
            )
        return cursor.lastrowid

    def list_observations(self, hostname: str) -> tuple[Observation, ...]:
        """Reload immutable observations for a hostname's registrable domain."""
        domain = normalize_hostname(hostname).registrable_domain
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT domain, outcome_code, observed_at, attempt_number,
                       status_code, final_url, detail, canonical_url,
                       internal_links_json
                FROM observations
                WHERE domain = ?
                ORDER BY id
                """,
                (domain,),
            ).fetchall()
        return tuple(
            Observation(
                domain=row["domain"],
                outcome_code=OutcomeCode(row["outcome_code"]),
                observed_at=datetime.fromisoformat(row["observed_at"]),
                attempt_number=row["attempt_number"],
                status_code=row["status_code"],
                final_url=row["final_url"],
                detail=row["detail"],
                canonical_url=row["canonical_url"],
                internal_links=tuple(json.loads(row["internal_links_json"])),
            )
            for row in rows
        )

    def due_domains(self, now: datetime) -> tuple[str, ...]:
        """Return domains whose latest append-only observation permits another probe."""
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT domains.domain, observations.outcome_code,
                       observations.observed_at, observations.attempt_number
                FROM domains
                LEFT JOIN observations ON observations.id = (
                    SELECT id
                    FROM observations AS latest
                    WHERE latest.domain = domains.domain
                    ORDER BY latest.id DESC
                    LIMIT 1
                )
                ORDER BY domains.domain
                """
            ).fetchall()

        due: list[str] = []
        for row in rows:
            if row["outcome_code"] is None:
                due.append(row["domain"])
                continue
            decision = decide_next_action(
                OutcomeCode(row["outcome_code"]),
                attempt_number=row["attempt_number"],
                now=datetime.fromisoformat(row["observed_at"]),
            )
            if decision.next_check_at is not None and decision.next_check_at <= now:
                due.append(row["domain"])
        return tuple(due)

    def get_source_cursor(self, source: str) -> str | None:
        """Return the last committed cursor for one independently polled source."""
        if not source.strip():
            raise ValueError("source must not be empty")
        with self._connection() as connection:
            row = connection.execute(
                "SELECT cursor FROM source_cursors WHERE source = ?", (source,)
            ).fetchone()
        return None if row is None else row["cursor"]

    def set_source_cursor(self, source: str, cursor: str | None) -> None:
        """Atomically replace the restart cursor after a fully persisted source page."""
        if not source.strip():
            raise ValueError("source must not be empty")
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO source_cursors (source, cursor) VALUES (?, ?)
                ON CONFLICT(source) DO UPDATE SET cursor = excluded.cursor
                """,
                (source, cursor),
            )

    def create_candidate(self, hostname: str, *, created_at: datetime) -> Candidate:
        """Create or reload the stable candidate identity for a root domain."""
        candidate = build_candidate(hostname, created_at=created_at)
        with self._connection() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO candidates (candidate_id, domain, created_at)
                VALUES (?, ?, ?)
                """,
                (candidate.candidate_id, candidate.domain, candidate.created_at.isoformat()),
            )
            row = connection.execute(
                "SELECT candidate_id, domain, created_at FROM candidates WHERE domain = ?",
                (candidate.domain,),
            ).fetchone()
        return Candidate(
            candidate_id=row["candidate_id"],
            domain=row["domain"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    def get_candidate(self, candidate_id: str) -> Candidate | None:
        """Return one stable candidate identity, if it exists."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT candidate_id, domain, created_at FROM candidates WHERE candidate_id = ?",
                (candidate_id,),
            ).fetchone()
        if row is None:
            return None
        return Candidate(
            candidate_id=row["candidate_id"],
            domain=row["domain"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    def append_candidate_version(
        self,
        candidate_id: str,
        draft: CandidateVersionDraft,
        *,
        created_at: datetime,
    ) -> CandidateVersion:
        """Append a cited candidate interpretation without overwriting prior versions."""
        if created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        evidence_json = json.dumps(
            [
                {
                    "evidence_type": evidence.evidence_type.value,
                    "quote": evidence.quote,
                    "url": evidence.url,
                }
                for evidence in draft.evidence
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        tags_json = json.dumps(draft.tags, ensure_ascii=False)
        with self._connection() as connection:
            exists = connection.execute(
                "SELECT 1 FROM candidates WHERE candidate_id = ?", (candidate_id,)
            ).fetchone()
            if exists is None:
                raise ValueError(f"cannot version unknown candidate: {candidate_id}")
            version = connection.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS next_version "
                "FROM candidate_versions WHERE candidate_id = ?",
                (candidate_id,),
            ).fetchone()["next_version"]
            connection.execute(
                """
                INSERT INTO candidate_versions (
                    candidate_id, version, created_at, author_kind, primary_outcome,
                    classification_confidence, name_suggestion, description_suggestion,
                    category, tags_json, pricing_model, target_audience, model_version,
                    evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate_id,
                    version,
                    created_at.isoformat(),
                    draft.author_kind,
                    draft.primary_outcome.value,
                    draft.classification_confidence,
                    draft.name_suggestion,
                    draft.description_suggestion,
                    draft.category,
                    tags_json,
                    draft.pricing_model,
                    draft.target_audience,
                    draft.model_version,
                    evidence_json,
                ),
            )
        return CandidateVersion(candidate_id=candidate_id, version=version, created_at=created_at, draft=draft)

    def list_candidate_versions(self, candidate_id: str) -> tuple[CandidateVersion, ...]:
        """Reload every immutable rule, model, and human version in append order."""
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM candidate_versions WHERE candidate_id = ? ORDER BY version",
                (candidate_id,),
            ).fetchall()
        return tuple(
            CandidateVersion(
                candidate_id=row["candidate_id"],
                version=row["version"],
                created_at=datetime.fromisoformat(row["created_at"]),
                draft=CandidateVersionDraft(
                    author_kind=row["author_kind"],
                    primary_outcome=CandidateOutcome(row["primary_outcome"]),
                    classification_confidence=row["classification_confidence"],
                    name_suggestion=row["name_suggestion"],
                    description_suggestion=row["description_suggestion"],
                    evidence=tuple(
                        Evidence(
                            evidence_type=EvidenceType(item["evidence_type"]),
                            quote=item["quote"],
                            url=item["url"],
                        )
                        for item in json.loads(row["evidence_json"])
                    ),
                    model_version=row["model_version"],
                    category=row["category"],
                    tags=tuple(json.loads(row["tags_json"])),
                    pricing_model=row["pricing_model"],
                    target_audience=row["target_audience"],
                ),
            )
            for row in rows
        )

    def get_candidate_version(
        self, candidate_id: str, version: int
    ) -> CandidateVersion | None:
        """Return one immutable candidate version without implying it is current."""
        if version < 1:
            raise ValueError("version must be positive")
        return next(
            (
                item
                for item in self.list_candidate_versions(candidate_id)
                if item.version == version
            ),
            None,
        )

    def append_candidate_verification(
        self, verification: CandidateVerification
    ) -> bool:
        """Persist strict-scan facts once for an immutable candidate version."""
        with self._connection() as connection:
            exists = connection.execute(
                """
                SELECT 1 FROM candidate_versions
                WHERE candidate_id = ? AND version = ?
                """,
                (verification.candidate_id, verification.candidate_version),
            ).fetchone()
            if exists is None:
                raise ValueError(
                    "verification references an unknown candidate version"
                )
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO candidate_verifications (
                    candidate_id, candidate_version, checked_at, ct_first_seen_at,
                    rdap_tier, rdap_age_days, rdap_registration_at, dns_has_a,
                    http_status_code, final_url, canonical_url, final_root_matches
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    verification.candidate_id,
                    verification.candidate_version,
                    verification.checked_at.isoformat(),
                    (
                        verification.ct_first_seen_at.isoformat()
                        if verification.ct_first_seen_at
                        else None
                    ),
                    verification.rdap_tier,
                    verification.rdap_age_days,
                    (
                        verification.rdap_registration_at.isoformat()
                        if verification.rdap_registration_at
                        else None
                    ),
                    int(verification.dns_has_a)
                    if verification.dns_has_a is not None
                    else None,
                    verification.http_status_code,
                    verification.final_url,
                    verification.canonical_url,
                    int(verification.final_root_matches)
                    if verification.final_root_matches is not None
                    else None,
                ),
            )
        return cursor.rowcount == 1

    def get_candidate_verification(
        self, candidate_id: str, candidate_version: int
    ) -> CandidateVerification | None:
        """Reload strict-scan facts for one candidate version."""
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM candidate_verifications
                WHERE candidate_id = ? AND candidate_version = ?
                """,
                (candidate_id, candidate_version),
            ).fetchone()
        if row is None:
            return None
        return CandidateVerification(
            candidate_id=row["candidate_id"],
            candidate_version=row["candidate_version"],
            checked_at=datetime.fromisoformat(row["checked_at"]),
            ct_first_seen_at=(
                datetime.fromisoformat(row["ct_first_seen_at"])
                if row["ct_first_seen_at"]
                else None
            ),
            rdap_tier=row["rdap_tier"],
            rdap_age_days=row["rdap_age_days"],
            rdap_registration_at=(
                datetime.fromisoformat(row["rdap_registration_at"])
                if row["rdap_registration_at"]
                else None
            ),
            dns_has_a=bool(row["dns_has_a"])
            if row["dns_has_a"] is not None
            else None,
            http_status_code=row["http_status_code"],
            final_url=row["final_url"],
            canonical_url=row["canonical_url"],
            final_root_matches=bool(row["final_root_matches"])
            if row["final_root_matches"] is not None
            else None,
        )

    def append_review_decision(self, decision: ReviewDecision) -> bool:
        """Append a review action once, only when its candidate version exists.

        Per spec §11, a candidate version may only carry one active decision at
        a time. If an unrevoked decision for the same ``(candidate_id,
        candidate_version)`` already exists with a *different* ``request_id``,
        raise :class:`ConcurrentDecisionError` so the API layer can return 409.
        A replay with the same ``request_id`` is idempotent and still succeeds.

        Uses ``BEGIN IMMEDIATE`` so the active-decision lookup and the insert
        happen inside the same write transaction; without it, two threads
        issuing different request_ids can both pass the SELECT-then-INSERT
        check before either commits.
        """
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            version_exists = connection.execute(
                """
                SELECT 1 FROM candidate_versions
                WHERE candidate_id = ? AND version = ?
                """,
                (decision.candidate_id, decision.candidate_version),
            ).fetchone()
            if version_exists is None:
                raise ValueError("review decision references an unknown candidate version")
            active = connection.execute(
                """
                SELECT decision_id, request_id FROM review_decisions
                WHERE candidate_id = ? AND candidate_version = ?
                  AND revoked_at IS NULL
                ORDER BY rowid DESC LIMIT 1
                """,
                (decision.candidate_id, decision.candidate_version),
            ).fetchone()
            if active is not None and active["request_id"] != decision.request_id:
                raise ConcurrentDecisionError(
                    active_decision_id=active["decision_id"],
                    active_request_id=active["request_id"],
                )
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO review_decisions (
                    decision_id, request_id, candidate_id, candidate_version,
                    action, actor_id, decided_at, reason_tags_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision.decision_id,
                    decision.request_id,
                    decision.candidate_id,
                    decision.candidate_version,
                    decision.action.value,
                    decision.actor_id,
                    decision.decided_at.isoformat(),
                    json.dumps(decision.reason_tags, ensure_ascii=False),
                ),
            )
        return cursor.rowcount == 1

    def list_review_decisions(self, candidate_id: str) -> tuple[ReviewDecision, ...]:
        """Reload the complete append-only human decision history for a candidate."""
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT decision_id, request_id, candidate_id, candidate_version,
                       action, actor_id, decided_at, reason_tags_json
                FROM review_decisions
                WHERE candidate_id = ?
                ORDER BY decided_at, decision_id
                """,
                (candidate_id,),
            ).fetchall()
        return tuple(
            ReviewDecision(
                decision_id=row["decision_id"],
                request_id=row["request_id"],
                candidate_id=row["candidate_id"],
                candidate_version=row["candidate_version"],
                action=ReviewAction(row["action"]),
                actor_id=row["actor_id"],
                decided_at=datetime.fromisoformat(row["decided_at"]),
                reason_tags=tuple(ReasonTag(tag) for tag in json.loads(row["reason_tags_json"])),
            )
            for row in rows
        )

    def active_review_action(
        self, candidate_id: str, candidate_version: int
    ) -> ReviewAction | None:
        """Return the latest unrevoked decision action for one exact version."""
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT action FROM review_decisions
                WHERE candidate_id = ? AND candidate_version = ?
                  AND revoked_at IS NULL
                ORDER BY rowid DESC LIMIT 1
                """,
                (candidate_id, candidate_version),
            ).fetchone()
        return ReviewAction(row["action"]) if row is not None else None

    def is_version_approved(self, candidate_id: str, candidate_version: int) -> bool:
        """Return whether the current unrevoked decision is approval."""
        return (
            self.active_review_action(candidate_id, candidate_version)
            is ReviewAction.APPROVE
        )

    def count_approved_versions(self) -> int:
        """Count the distinct candidate versions whose latest decision is approval."""
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS approved FROM (
                    SELECT candidate_id, candidate_version, action,
                           ROW_NUMBER() OVER (
                               PARTITION BY candidate_id, candidate_version
                               ORDER BY rowid DESC
                           ) AS rn
                    FROM review_decisions
                    WHERE revoked_at IS NULL
                ) latest
                WHERE latest.rn = 1 AND latest.action = ?
                """,
                (ReviewAction.APPROVE.value,),
            ).fetchone()
        return int(row["approved"])

    def revoke_review_decision(
        self,
        decision_id: str,
        *,
        actor_id: str,
        revoked_at: datetime,
        reason: str,
    ) -> bool:
        """Soft-revoke one review decision; idempotent for repeat calls."""
        if revoked_at.tzinfo is None:
            raise ValueError("revoked_at must be timezone-aware")
        if not actor_id or not actor_id.strip():
            raise ValueError("actor_id must not be empty")
        if not decision_id or not decision_id.strip():
            raise ValueError("decision_id must not be empty")
        with self._connection() as connection:
            self._upgrade_review_decisions_table(connection)
            cursor = connection.execute(
                """
                UPDATE review_decisions
                SET revoked_at = ?, revoked_by = ?, revoke_reason = ?
                WHERE decision_id = ? AND revoked_at IS NULL
                """,
                (revoked_at.isoformat(), actor_id, reason, decision_id),
            )
        return cursor.rowcount == 1

    def is_decision_revoked(self, decision_id: str) -> bool:
        """Return whether one decision has been soft-revoked."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT revoked_at FROM review_decisions WHERE decision_id = ?",
                (decision_id,),
            ).fetchone()
        return row is not None and row["revoked_at"] is not None

    def append_exposure_check(self, candidate_id: str, check: ExposureCheck) -> int:
        """Append one explicit public-channel result for a known candidate."""
        with self._connection() as connection:
            if not self._candidate_exists(connection, candidate_id):
                raise ValueError(f"cannot observe unknown candidate: {candidate_id}")
            cursor = connection.execute(
                """
                INSERT INTO exposure_checks (
                    candidate_id, channel, checked_at, status, query, evidence_url, detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate_id,
                    check.channel.value,
                    check.checked_at.isoformat(),
                    check.status.value,
                    check.query,
                    check.evidence_url,
                    check.detail,
                ),
            )
        return cursor.lastrowid

    def list_exposure_checks(self, candidate_id: str) -> tuple[ExposureCheck, ...]:
        """Reload all exposure evidence in append order."""
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT channel, checked_at, status, query, evidence_url, detail
                FROM exposure_checks WHERE candidate_id = ? ORDER BY id
                """,
                (candidate_id,),
            ).fetchall()
        return tuple(
            ExposureCheck(
                channel=ExposureChannel(row["channel"]),
                checked_at=datetime.fromisoformat(row["checked_at"]),
                status=ExposureStatus(row["status"]),
                query=row["query"],
                evidence_url=row["evidence_url"],
                detail=row["detail"],
            )
            for row in rows
        )

    def append_review_priority(
        self,
        candidate_id: str,
        priority: ReviewPriority,
        *,
        calculated_at: datetime,
    ) -> int:
        """Append, never overwrite, the formula breakdown used for human ordering."""
        snapshot = ReviewPrioritySnapshot(calculated_at=calculated_at, priority=priority)
        with self._connection() as connection:
            if not self._candidate_exists(connection, candidate_id):
                raise ValueError(f"cannot score unknown candidate: {candidate_id}")
            cursor = connection.execute(
                """
                INSERT INTO review_priorities (
                    candidate_id, calculated_at, score, product_evidence_contribution,
                    early_presence_contribution, low_exposure_contribution,
                    data_completeness_contribution, formula_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate_id,
                    snapshot.calculated_at.isoformat(),
                    snapshot.priority.score,
                    snapshot.priority.product_evidence_contribution,
                    snapshot.priority.early_presence_contribution,
                    snapshot.priority.low_exposure_contribution,
                    snapshot.priority.data_completeness_contribution,
                    snapshot.priority.formula_version,
                ),
            )
        return cursor.lastrowid

    def latest_review_priority(self, candidate_id: str) -> ReviewPrioritySnapshot | None:
        """Return the latest saved calculation without recomputing inputs implicitly."""
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT calculated_at, score, product_evidence_contribution,
                       early_presence_contribution, low_exposure_contribution,
                       data_completeness_contribution, formula_version
                FROM review_priorities
                WHERE candidate_id = ? ORDER BY id DESC LIMIT 1
                """,
                (candidate_id,),
            ).fetchone()
        if row is None:
            return None
        return ReviewPrioritySnapshot(
            calculated_at=datetime.fromisoformat(row["calculated_at"]),
            priority=ReviewPriority(
                score=row["score"],
                product_evidence_contribution=row["product_evidence_contribution"],
                early_presence_contribution=row["early_presence_contribution"],
                low_exposure_contribution=row["low_exposure_contribution"],
                data_completeness_contribution=row["data_completeness_contribution"],
                formula_version=row["formula_version"],
            ),
        )

    def list_review_queue(self) -> tuple[ReviewQueueItem, ...]:
        """Project reviewable candidates by their latest saved version and score only."""
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT candidates.candidate_id, candidates.domain, candidates.created_at
                FROM candidates
                JOIN candidate_versions ON candidate_versions.candidate_id = candidates.candidate_id
                JOIN review_priorities ON review_priorities.candidate_id = candidates.candidate_id
                WHERE candidate_versions.version = (
                    SELECT MAX(latest_versions.version) FROM candidate_versions AS latest_versions
                    WHERE latest_versions.candidate_id = candidates.candidate_id
                )
                AND review_priorities.id = (
                    SELECT MAX(latest_priorities.id) FROM review_priorities AS latest_priorities
                    WHERE latest_priorities.candidate_id = candidates.candidate_id
                )
                ORDER BY review_priorities.score DESC, candidates.candidate_id ASC
                """
            ).fetchall()
        items: list[ReviewQueueItem] = []
        for row in rows:
            candidate = Candidate(
                candidate_id=row["candidate_id"],
                domain=row["domain"],
                created_at=datetime.fromisoformat(row["created_at"]),
            )
            versions = self.list_candidate_versions(candidate.candidate_id)
            priority = self.latest_review_priority(candidate.candidate_id)
            if not versions or priority is None:
                continue
            latest_observation = self._latest_observation(candidate.domain)
            items.append(
                ReviewQueueItem(
                    candidate=candidate,
                    latest_version=versions[-1],
                    priority=priority,
                    canonical_url=latest_observation.canonical_url if latest_observation else None,
                    internal_links=latest_observation.internal_links if latest_observation else (),
                )
            )
        return tuple(items)

    def _latest_observation(self, domain: str) -> Observation | None:
        observations = self.list_observations(domain)
        return observations[-1] if observations else None

    def append_publication(self, record: PublicationRecord) -> bool:
        """Append an external-sync attempt for a real, immutable candidate version."""
        with self._connection() as connection:
            version_exists = connection.execute(
                """
                SELECT 1 FROM candidate_versions
                WHERE candidate_id = ? AND version = ?
                """,
                (record.candidate_id, record.candidate_version),
            ).fetchone()
            if version_exists is None:
                raise ValueError("publication references an unknown candidate version")
            connection.execute(
                """
                INSERT INTO publications (
                    candidate_id, candidate_version, requested_at, sync_status,
                    external_entry_id, external_version, publication_status,
                    field_errors_json, detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.candidate_id,
                    record.candidate_version,
                    record.requested_at.isoformat(),
                    record.sync_status.value,
                    record.external_entry_id,
                    record.external_version,
                    record.publication_status,
                    json.dumps(record.field_errors, ensure_ascii=False),
                    record.detail,
                ),
            )
        return True

    def list_publications(self, candidate_id: str) -> tuple[PublicationRecord, ...]:
        """Reload every explicit external-sync outcome in insertion order."""
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT candidate_id, candidate_version, requested_at, sync_status,
                       external_entry_id, external_version, publication_status,
                       field_errors_json, detail
                FROM publications WHERE candidate_id = ? ORDER BY id
                """,
                (candidate_id,),
            ).fetchall()
        return tuple(
            PublicationRecord(
                candidate_id=row["candidate_id"],
                candidate_version=row["candidate_version"],
                requested_at=datetime.fromisoformat(row["requested_at"]),
                sync_status=SyncStatus(row["sync_status"]),
                external_entry_id=row["external_entry_id"],
                external_version=row["external_version"],
                publication_status=row["publication_status"],
                field_errors=tuple(json.loads(row["field_errors_json"])),
                detail=row["detail"],
            )
            for row in rows
        )

    def enqueue_work(self, stage: WorkStage, entity_id: str, *, scheduled_at: datetime) -> bool:
        """Add one idempotent work item; duplicate active work is not multiplied."""
        if not entity_id.strip():
            raise ValueError("entity_id must not be empty")
        if scheduled_at.tzinfo is None:
            raise ValueError("scheduled_at must be timezone-aware")
        with self._connection() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO work_queue (stage, entity_id, scheduled_at)
                VALUES (?, ?, ?)
                """,
                (stage.value, entity_id, scheduled_at.isoformat()),
            )
        return cursor.rowcount == 1

    def claim_work(
        self,
        *,
        worker_id: str,
        now: datetime,
        lease_seconds: float,
        limit: int,
    ) -> tuple[WorkLease, ...]:
        """Atomically lease due work; expired leases are safely reclaimable."""
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if lease_seconds <= 0 or limit < 1:
            raise ValueError("lease_seconds and limit must be positive")
        from datetime import timedelta

        expires_at = now + timedelta(seconds=lease_seconds)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT id, stage, entity_id, scheduled_at FROM work_queue
                WHERE completed_at IS NULL AND scheduled_at <= ?
                  AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                ORDER BY scheduled_at, id LIMIT ?
                """,
                (now.isoformat(), now.isoformat(), limit),
            ).fetchall()
            leases: list[WorkLease] = []
            for row in rows:
                token = uuid4().hex
                connection.execute(
                    """
                    UPDATE work_queue
                    SET lease_token = ?, lease_owner = ?, lease_expires_at = ?
                    WHERE id = ?
                    """,
                    (token, worker_id, expires_at.isoformat(), row["id"]),
                )
                leases.append(
                    WorkLease(
                        work_id=row["id"],
                        stage=WorkStage(row["stage"]),
                        entity_id=row["entity_id"],
                        scheduled_at=datetime.fromisoformat(row["scheduled_at"]),
                        lease_token=token,
                        lease_owner=worker_id,
                        lease_expires_at=expires_at,
                    )
                )
        return tuple(leases)

    def release_work(
        self,
        work_id: int,
        *,
        lease_token: str,
        reschedule_at: datetime,
    ) -> bool:
        """Return one leased work item to the queue so it can be retried later."""
        if reschedule_at.tzinfo is None:
            raise ValueError("reschedule_at must be timezone-aware")
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE work_queue
                SET lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                    scheduled_at = ?
                WHERE id = ? AND lease_token = ? AND completed_at IS NULL
                """,
                (reschedule_at.isoformat(), work_id, lease_token),
            )
        return cursor.rowcount == 1

    def complete_work(self, work_id: int, *, lease_token: str) -> bool:
        """Mark one leased work item as completed exactly once."""
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE work_queue
                SET lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                    completed_at = ?
                WHERE id = ? AND lease_token = ? AND completed_at IS NULL
                """,
                (datetime.now(UTC).isoformat(), work_id, lease_token),
            )
        return cursor.rowcount == 1

    def record_budget_deferred(
        self,
        stage: WorkStage,
        *,
        entity_id: str | None,
        units: float,
        occurred_at: datetime,
        reason: str,
    ) -> None:
        """Append an explicit budget-deferred entry for an unimplemented stage."""
        if units <= 0:
            raise ValueError("units must be positive")
        if occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO budget_ledger (stage, entity_id, occurred_at, units, status, reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    stage.value,
                    entity_id,
                    occurred_at.isoformat(),
                    units,
                    "deferred",
                    reason,
                ),
            )

    def reserve_budget(
        self,
        stage: WorkStage,
        *,
        units: float,
        daily_limit: float,
        occurred_at: datetime,
        entity_id: str | None = None,
    ) -> BudgetReservation:
        """Append a daily cost decision without silently discarding deferred work."""
        if units <= 0 or daily_limit < 0:
            raise ValueError("units must be positive and daily_limit must be non-negative")
        if occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        from datetime import UTC, timedelta

        utc_time = occurred_at.astimezone(UTC)
        day_start = utc_time.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            used = connection.execute(
                """
                SELECT COALESCE(SUM(units), 0) AS used FROM budget_ledger
                WHERE stage = ? AND status = 'reserved'
                  AND occurred_at >= ? AND occurred_at < ?
                """,
                (stage.value, day_start.isoformat(), day_end.isoformat()),
            ).fetchone()["used"]
            allowed = used + units <= daily_limit
            connection.execute(
                """
                INSERT INTO budget_ledger (stage, entity_id, occurred_at, units, status, reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    stage.value,
                    entity_id,
                    utc_time.isoformat(),
                    units,
                    "reserved" if allowed else "deferred",
                    None if allowed else "budget_deferred",
                ),
            )
        return BudgetReservation(
            allowed=allowed,
            remaining_units=max(0.0, float(daily_limit - (used + units if allowed else used))),
            reason=None if allowed else "budget_deferred",
        )

    def append_claim_token(self, record: ClaimTokenRecord) -> bool:
        """Persist only the one-way token hash for a known candidate."""
        with self._connection() as connection:
            if not self._candidate_exists(connection, record.candidate_id):
                raise ValueError("claim token references an unknown candidate")
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO claim_tokens (
                    candidate_id, token_hash, created_at, expires_at, revoked_at, used_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.candidate_id,
                    record.token_hash,
                    record.created_at.isoformat(),
                    record.expires_at.isoformat(),
                    record.revoked_at.isoformat() if record.revoked_at else None,
                    record.used_at.isoformat() if record.used_at else None,
                ),
            )
        return cursor.rowcount == 1

    def redeem_claim_token(
        self, raw_token: str, *, redeemed_at: datetime
    ) -> ClaimTokenRecord | None:
        """Atomically consume one unexpired, unrevoked claim token exactly once."""
        if redeemed_at.tzinfo is None:
            raise ValueError("redeemed_at must be timezone-aware")
        token_hash = hash_claim_token(raw_token)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT id, candidate_id, token_hash, created_at, expires_at, revoked_at, used_at
                FROM claim_tokens
                WHERE token_hash = ? AND used_at IS NULL AND revoked_at IS NULL AND expires_at > ?
                """,
                (token_hash, redeemed_at.isoformat()),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                "UPDATE claim_tokens SET used_at = ? WHERE id = ?",
                (redeemed_at.isoformat(), row["id"]),
            )
        return ClaimTokenRecord(
            candidate_id=row["candidate_id"],
            token_hash=row["token_hash"],
            created_at=datetime.fromisoformat(row["created_at"]),
            expires_at=datetime.fromisoformat(row["expires_at"]),
            revoked_at=datetime.fromisoformat(row["revoked_at"]) if row["revoked_at"] else None,
            used_at=redeemed_at,
        )

    def revoke_claim_token(self, raw_token: str, *, revoked_at: datetime) -> bool:
        """Revoke an unconsumed token; this is intentionally idempotent for callers."""
        if revoked_at.tzinfo is None:
            raise ValueError("revoked_at must be timezone-aware")
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE claim_tokens SET revoked_at = ?
                WHERE token_hash = ? AND revoked_at IS NULL AND used_at IS NULL
                """,
                (revoked_at.isoformat(), hash_claim_token(raw_token)),
            )
        return cursor.rowcount == 1

    def append_outreach_event(self, event: OutreachEvent) -> int:
        """Append a manual outreach trigger record (dry-run or real)."""
        with self._connection() as connection:
            version_exists = connection.execute(
                """
                SELECT 1 FROM candidate_versions
                WHERE candidate_id = ? AND version = ?
                """,
                (event.candidate_id, event.candidate_version),
            ).fetchone()
            if version_exists is None:
                raise ValueError("outreach event references an unknown candidate version")
            cursor = connection.execute(
                """
                INSERT INTO outreach_events (
                    candidate_id, candidate_version, actor_id, triggered_at,
                    dry_run, recipient_source_url, claim_tokens_issued,
                    contact_count, contact_preview_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.candidate_id,
                    event.candidate_version,
                    event.actor_id,
                    event.triggered_at.isoformat(),
                    1 if event.dry_run else 0,
                    event.recipient_source_url,
                    event.claim_tokens_issued,
                    event.contact_count,
                    event.contact_preview_json,
                ),
            )
        return cursor.lastrowid

    def list_outreach_events(self, candidate_id: str) -> tuple[OutreachEvent, ...]:
        """Reload every outreach trigger for one candidate in chronological order."""
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT candidate_id, candidate_version, actor_id, triggered_at,
                       dry_run, recipient_source_url, claim_tokens_issued,
                       contact_count, contact_preview_json
                FROM outreach_events
                WHERE candidate_id = ?
                ORDER BY triggered_at, id
                """,
                (candidate_id,),
            ).fetchall()
        return tuple(
            OutreachEvent(
                candidate_id=row["candidate_id"],
                candidate_version=row["candidate_version"],
                actor_id=row["actor_id"],
                triggered_at=datetime.fromisoformat(row["triggered_at"]),
                dry_run=bool(row["dry_run"]),
                recipient_source_url=row["recipient_source_url"],
                claim_tokens_issued=row["claim_tokens_issued"],
                contact_count=row["contact_count"],
                contact_preview_json=row["contact_preview_json"],
            )
            for row in rows
        )

    def funnel_metrics(self) -> FunnelMetrics:
        """Return an explicit local snapshot of throughput, failure reasons, and budget pressure."""
        with self._connection() as connection:
            def count(table: str) -> int:
                return connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"]

            counts = {
                table: count(table)
                for table in (
                    "source_events",
                    "domains",
                    "observations",
                    "candidates",
                    "candidate_versions",
                    "review_decisions",
                    "publications",
                    "outreach_events",
                    "work_queue",
                )
            }
            outcome_rows = connection.execute(
                "SELECT outcome_code, COUNT(*) AS count FROM observations GROUP BY outcome_code"
            ).fetchall()
            budget_rows = connection.execute(
                """
                SELECT status, COALESCE(SUM(units), 0) AS units
                FROM budget_ledger GROUP BY status
                """
            ).fetchall()
        budgets = {row["status"]: float(row["units"]) for row in budget_rows}
        reserved_units = budgets.get("reserved", 0.0)
        approved_version_count = self.count_approved_versions()
        cost_per_effective_candidate = reserved_units / max(1, approved_version_count)
        return FunnelMetrics(
            source_events=counts["source_events"],
            domains=counts["domains"],
            observations=counts["observations"],
            candidates=counts["candidates"],
            candidate_versions=counts["candidate_versions"],
            review_decisions=counts["review_decisions"],
            publications=counts["publications"],
            outreach_events=counts["outreach_events"],
            queued_work=counts["work_queue"],
            budget_reserved_units=reserved_units,
            budget_deferred_units=budgets.get("deferred", 0.0),
            cost_per_effective_candidate=cost_per_effective_candidate,
            observation_outcomes={row["outcome_code"]: row["count"] for row in outcome_rows},
        )

    def compute_funnel_analytics(self, *, now: datetime | None = None) -> FunnelAnalytics:
        """Return conversion rates, latency percentiles, and backlog shape.

        Latency samples are pulled per-row rather than aggregated in SQL so we
        can keep using ISO-8601 strings and avoid ``MIN(...)`` across the
        joined set, which would return a string SQLite may not order the way
        Python expects across the full UTC offset range.
        """
        computed_at = now if now is not None else datetime.now(UTC)
        if computed_at.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        with self._connection() as connection:
            source_event_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM source_events").fetchone()["count"]
            )
            candidate_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM candidates").fetchone()["count"]
            )
            candidate_version_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM candidate_versions").fetchone()["count"]
            )
            publication_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM publications").fetchone()["count"]
            )
            approved_version_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS approved FROM (
                        SELECT candidate_id, candidate_version, action,
                               ROW_NUMBER() OVER (
                                   PARTITION BY candidate_id, candidate_version
                                   ORDER BY rowid DESC
                               ) AS rn
                        FROM review_decisions
                        WHERE revoked_at IS NULL
                    ) latest
                    WHERE latest.rn = 1 AND latest.action = ?
                    """,
                    (ReviewAction.APPROVE.value,),
                ).fetchone()["approved"]
            )

            first_signal_rows = connection.execute(
                """
                SELECT c.candidate_id, c.created_at, MIN(se.observed_at) AS first_signal
                FROM candidates c
                JOIN event_domains ed ON ed.domain = c.domain
                JOIN source_events se ON se.id = ed.event_id
                GROUP BY c.candidate_id, c.created_at
                """
            ).fetchall()
            first_signal_latencies: list[float] = []
            for row in first_signal_rows:
                created_at = datetime.fromisoformat(row["created_at"])
                first_signal = datetime.fromisoformat(row["first_signal"])
                first_signal_latencies.append(
                    (created_at - first_signal).total_seconds()
                )

            decision_latency_rows = connection.execute(
                """
                SELECT rd.decided_at, cv.created_at AS version_created_at
                FROM review_decisions rd
                JOIN candidate_versions cv
                  ON cv.candidate_id = rd.candidate_id
                 AND cv.version = rd.candidate_version
                """
            ).fetchall()
            decision_latencies: list[float] = []
            for row in decision_latency_rows:
                decided_at = datetime.fromisoformat(row["decided_at"])
                version_created_at = datetime.fromisoformat(row["version_created_at"])
                decision_latencies.append(
                    (decided_at - version_created_at).total_seconds()
                )

            backlog_rows = connection.execute(
                """
                SELECT stage, COUNT(*) AS count FROM work_queue
                WHERE completed_at IS NULL
                GROUP BY stage
                """
            ).fetchall()
            backlog_over_1h_rows = connection.execute(
                """
                SELECT stage, COUNT(*) AS count FROM work_queue
                WHERE completed_at IS NULL
                  AND scheduled_at < ?
                GROUP BY stage
                """,
                ((computed_at - timedelta(hours=1)).isoformat(),),
            ).fetchall()

        def clamp01(value: float) -> float:
            return min(max(value, 0.0), 1.0)

        conversion_source_to_candidate = clamp01(
            candidate_count / max(1, source_event_count)
        )
        conversion_candidate_to_approved = clamp01(
            approved_version_count / max(1, candidate_version_count)
        )
        conversion_source_to_published = clamp01(
            publication_count / max(1, source_event_count)
        )

        return FunnelAnalytics(
            conversion_source_to_candidate=conversion_source_to_candidate,
            conversion_candidate_to_approved=conversion_candidate_to_approved,
            conversion_source_to_published=conversion_source_to_published,
            latency_first_signal_to_candidate_p50_seconds=percentile(first_signal_latencies, 50.0),
            latency_first_signal_to_candidate_p95_seconds=percentile(first_signal_latencies, 95.0),
            latency_candidate_to_decision_p50_seconds=percentile(decision_latencies, 50.0),
            latency_candidate_to_decision_p95_seconds=percentile(decision_latencies, 95.0),
            backlog_by_stage={
                row["stage"]: int(row["count"]) for row in backlog_rows
            },
            backlog_over_1h_by_stage={
                row["stage"]: int(row["count"]) for row in backlog_over_1h_rows
            },
            computed_at=computed_at,
        )

    @staticmethod
    def _candidate_exists(connection: sqlite3.Connection, candidate_id: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM candidates WHERE candidate_id = ?", (candidate_id,)
            ).fetchone()
            is not None
        )

    def append_audit(self, entry: AIKnowsAuditEntry) -> bool:
        """Append one AIKnows audit row; never overwritten by design."""
        with self._connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO ai_knows_audit (
                    method, url, status_code, latency_ms,
                    candidate_id, candidate_version, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.method,
                    entry.url,
                    entry.status_code,
                    entry.latency_ms,
                    entry.candidate_id,
                    entry.candidate_version,
                    entry.occurred_at.isoformat(),
                ),
            )
        return cursor.lastrowid is not None

    def list_audit(
        self, *, candidate_id: str | None = None, limit: int = 200
    ) -> tuple[AIKnowsAuditEntry, ...]:
        """Return audit entries for one candidate, newest first."""
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._connection() as connection:
            if candidate_id is None:
                rows = connection.execute(
                    """
                    SELECT method, url, status_code, latency_ms,
                           candidate_id, candidate_version, occurred_at
                    FROM ai_knows_audit ORDER BY id DESC LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT method, url, status_code, latency_ms,
                           candidate_id, candidate_version, occurred_at
                    FROM ai_knows_audit
                    WHERE candidate_id = ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (candidate_id, limit),
                ).fetchall()
        return tuple(
            AIKnowsAuditEntry(
                method=row["method"],
                url=row["url"],
                status_code=row["status_code"],
                latency_ms=row["latency_ms"],
                candidate_id=row["candidate_id"],
                candidate_version=row["candidate_version"],
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
            )
            for row in rows
        )

# ------------------------------------------------------------------
    # Reopen events (spec §18)
    # ------------------------------------------------------------------

    def append_reopen_event(self, event: ReopenEvent) -> bool:
        """Append one reopen trigger exactly once; idempotent by reopen_id."""
        if event.reopened_at.tzinfo is None:
            raise ValueError("reopened_at must be timezone-aware")
        with self._connection() as connection:
            if not self._candidate_exists(connection, event.candidate_id):
                raise ValueError(f"reopen references an unknown candidate: {event.candidate_id}")
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO reopen_events (
                    reopen_id, candidate_id, trigger_source_event_id,
                    previous_outcome, new_outcome, actor_id, reason, reopened_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.reopen_id,
                    event.candidate_id,
                    event.trigger_source_event_id,
                    event.previous_outcome.value,
                    event.new_outcome.value,
                    event.actor_id,
                    event.reason,
                    event.reopened_at.isoformat(),
                ),
            )
        return cursor.rowcount == 1

    def list_reopen_events(self, candidate_id: str) -> tuple[ReopenEvent, ...]:
        """Reload every append-only reopen trigger for a candidate in insertion order."""
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT reopen_id, candidate_id, trigger_source_event_id,
                       previous_outcome, new_outcome, actor_id, reason, reopened_at
                FROM reopen_events
                WHERE candidate_id = ?
                ORDER BY reopened_at, reopen_id
                """,
                (candidate_id,),
            ).fetchall()
        return tuple(
            ReopenEvent(
                reopen_id=row["reopen_id"],
                candidate_id=row["candidate_id"],
                trigger_source_event_id=row["trigger_source_event_id"],
                previous_outcome=OutcomeCode(row["previous_outcome"]),
                new_outcome=OutcomeCode(row["new_outcome"]),
                actor_id=row["actor_id"],
                reason=row["reason"],
                reopened_at=datetime.fromisoformat(row["reopened_at"]),
            )
            for row in rows
        )

    def source_event_links_to_domain(self, source_event_id: str, domain: str) -> bool:
        """Return whether the given source event is linked to the candidate's registrable domain."""
        if not source_event_id.strip():
            raise ValueError("source_event_id must not be empty")
        from domainhunter.domain.normalization import normalize_hostname

        registrable = normalize_hostname(domain).registrable_domain
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM event_domains
                JOIN source_events ON source_events.id = event_domains.event_id
                WHERE source_events.source_event_id = ? AND event_domains.domain = ?
                LIMIT 1
                """,
                (source_event_id, registrable),
            ).fetchone()
        return row is not None

    def is_reopenable(self, hostname: str) -> bool:
        """Return whether the latest observation for a hostname is in a terminal state."""
        latest = self._latest_observation(normalize_hostname(hostname).registrable_domain)
        if latest is None:
            return False
        decision = decide_next_action(
            latest.outcome_code,
            attempt_number=latest.attempt_number,
            now=latest.observed_at,
        )
        return decision.terminal_status is not None

    # ------------------------------------------------------------------
    # Alert persistence (spec §14)
    # ------------------------------------------------------------------

    def append_alert(
        self,
        alert: Alert,
        *,
        dedup_window: timedelta | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Idempotently persist one operator-actionable anomaly.

        Returns ``True`` when a new row was inserted; ``False`` when the same
        fingerprint was already on record inside ``dedup_window`` (6h default).
        """

        if alert.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        window = dedup_window or timedelta(hours=6)
        anchor = now or alert.opened_at
        window_start = anchor - window
        with self._connection() as connection:
            duplicate = connection.execute(
                """
                SELECT 1 FROM alerts
                WHERE kind = ? AND related_candidate_id IS ? AND related_stage IS ?
                  AND opened_at >= ?
                LIMIT 1
                """,
                (
                    alert.kind.value,
                    alert.related_candidate_id,
                    alert.related_stage,
                    window_start.isoformat(),
                ),
            ).fetchone()
            if duplicate is not None:
                return False
            connection.execute(
                """
                INSERT INTO alerts (
                    alert_id, kind, severity, title, summary, opened_at,
                    runbook_id, related_candidate_id, related_stage, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alert.alert_id,
                    alert.kind.value,
                    alert.severity.value,
                    alert.title,
                    alert.summary,
                    alert.opened_at.isoformat(),
                    alert.runbook_id,
                    alert.related_candidate_id,
                    alert.related_stage,
                    json.dumps(dict(alert.payload), ensure_ascii=False, sort_keys=True),
                ),
            )
        return True

    def list_alerts(self, since: datetime | None = None) -> tuple[Alert, ...]:
        """Return persisted alerts in append order, optionally filtered by ``since``."""

        with self._connection() as connection:
            if since is None:
                rows = connection.execute(
                    """
                    SELECT alert_id, kind, severity, title, summary, opened_at,
                           runbook_id, related_candidate_id, related_stage,
                           payload_json
                    FROM alerts ORDER BY opened_at, alert_id
                    """
                ).fetchall()
            else:
                if since.tzinfo is None:
                    raise ValueError("since must be timezone-aware")
                rows = connection.execute(
                    """
                    SELECT alert_id, kind, severity, title, summary, opened_at,
                           runbook_id, related_candidate_id, related_stage,
                           payload_json
                    FROM alerts WHERE opened_at >= ? ORDER BY opened_at, alert_id
                    """,
                    (since.isoformat(),),
                ).fetchall()
        return tuple(
            Alert(
                alert_id=row["alert_id"],
                kind=AlertKind(row["kind"]),
                severity=AlertSeverity(row["severity"]),
                title=row["title"],
                summary=row["summary"],
                opened_at=datetime.fromisoformat(row["opened_at"]),
                runbook_id=row["runbook_id"],
                related_candidate_id=row["related_candidate_id"],
                related_stage=row["related_stage"],
                payload=dict(json.loads(row["payload_json"])),
            )
            for row in rows
        )

    # ------------------------------------------------------------------
    # Operations runtime config (spec §14 budget + pause controls)
    # ------------------------------------------------------------------

    def get_budget_config(self) -> dict[WorkStage, float]:
        """Return persisted daily_limit per stage; missing rows fall back to defaults.

        No rows are auto-inserted; if a stage has no row, the hardcoded
        :data:`DEFAULT_DAILY_BUDGET` value is returned. ``set_budget_config``
        is the only writer that materializes a row.
        """

        with self._connection() as connection:
            rows = connection.execute(
                "SELECT stage, daily_limit FROM budget_config"
            ).fetchall()
        persisted: dict[WorkStage, float] = {
            WorkStage(row["stage"]): float(row["daily_limit"]) for row in rows
        }
        result: dict[WorkStage, float] = {stage: DEFAULT_DAILY_BUDGET[stage] for stage in WorkStage}
        result.update(persisted)
        return result

    def set_budget_config(
        self,
        stage: WorkStage,
        *,
        daily_limit: float,
        updated_by: str,
        occurred_at: datetime,
    ) -> bool:
        """Upsert one stage's daily_limit; idempotent on identical (stage, updated_at)."""

        if daily_limit <= 0:
            raise ValueError("daily_limit must be positive")
        if not updated_by or not updated_by.strip():
            raise ValueError("updated_by must not be empty")
        if occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        timestamp = occurred_at.isoformat()
        with self._connection() as connection:
            existing = connection.execute(
                "SELECT daily_limit, updated_at FROM budget_config WHERE stage = ?",
                (stage.value,),
            ).fetchone()
            if existing is not None and (
                float(existing["daily_limit"]) == float(daily_limit)
                and existing["updated_at"] == timestamp
            ):
                return False
            connection.execute(
                """
                INSERT INTO budget_config (stage, daily_limit, updated_at, updated_by)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(stage) DO UPDATE SET
                    daily_limit = excluded.daily_limit,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
                """,
                (stage.value, float(daily_limit), timestamp, updated_by),
            )
        return True

    def get_budget_config_row(self, stage: WorkStage) -> tuple[str, float, datetime, str] | None:
        """Return the persisted row for a single stage, if any."""

        with self._connection() as connection:
            row = connection.execute(
                "SELECT stage, daily_limit, updated_at, updated_by FROM budget_config WHERE stage = ?",
                (stage.value,),
            ).fetchone()
        if row is None:
            return None
        return (
            row["stage"],
            float(row["daily_limit"]),
            datetime.fromisoformat(row["updated_at"]),
            row["updated_by"],
        )

    def set_stage_pause(
        self,
        stage: WorkStage,
        *,
        paused: bool,
        reason: str,
        actor_id: str,
        paused_at: datetime,
    ) -> bool:
        """Append one pause/unpause audit row; idempotent on identical repeated writes.

        Idempotent on ``(stage, paused, reason)`` within a 1-minute window of
        ``paused_at``: if a matching row already exists in that window, the
        call returns ``False`` without inserting. Otherwise a new row is
        appended and ``True`` is returned. The latest row per stage wins for
        ``is_stage_paused`` / ``list_stage_pauses``.
        """

        if not reason or not reason.strip():
            raise ValueError("reason must not be empty")
        if not actor_id or not actor_id.strip():
            raise ValueError("actor_id must not be empty")
        if paused_at.tzinfo is None:
            raise ValueError("paused_at must be timezone-aware")
        timestamp = paused_at.isoformat()
        window_start = (paused_at - timedelta(minutes=1)).isoformat()
        with self._connection() as connection:
            duplicate = connection.execute(
                """
                SELECT 1 FROM stage_pauses
                WHERE stage = ? AND paused = ? AND reason = ?
                  AND paused_at >= ? AND paused_at <= ?
                LIMIT 1
                """,
                (
                    stage.value,
                    1 if paused else 0,
                    reason,
                    window_start,
                    timestamp,
                ),
            ).fetchone()
            if duplicate is not None:
                return False
            connection.execute(
                """
                INSERT INTO stage_pauses (stage, paused, reason, actor_id, paused_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (stage.value, 1 if paused else 0, reason, actor_id, timestamp),
            )
        return True

    def is_stage_paused(
        self, stage: WorkStage, *, now: datetime | None = None
    ) -> bool:
        """Return whether the most recent pause row for ``stage`` is paused."""

        del now  # accepted for future TTL semantics; the table is an audit log
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT paused FROM stage_pauses
                WHERE stage = ?
                ORDER BY paused_at DESC, id DESC LIMIT 1
                """,
                (stage.value,),
            ).fetchone()
        if row is None:
            return False
        return bool(row["paused"])

    def list_stage_pauses(
        self,
    ) -> tuple[tuple[WorkStage, bool, str, str, datetime], ...]:
        """Return the latest pause row per stage in deterministic stage order."""

        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT stage, paused, reason, actor_id, paused_at FROM (
                    SELECT stage, paused, reason, actor_id, paused_at,
                           ROW_NUMBER() OVER (
                               PARTITION BY stage ORDER BY paused_at DESC, id DESC
                           ) AS rn
                    FROM stage_pauses
                ) latest WHERE rn = 1 ORDER BY stage
                """
            ).fetchall()
        return tuple(
            (
                WorkStage(row["stage"]),
                bool(row["paused"]),
                row["reason"],
                row["actor_id"],
                datetime.fromisoformat(row["paused_at"]),
            )
            for row in rows
        )

    def latest_stage_pause(
        self, stage: WorkStage
    ) -> tuple[WorkStage, bool, str, str, datetime] | None:
        """Return the latest pause row for one stage, if any."""

        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT stage, paused, reason, actor_id, paused_at
                FROM stage_pauses
                WHERE stage = ?
                ORDER BY paused_at DESC, id DESC LIMIT 1
                """,
                (stage.value,),
            ).fetchone()
        if row is None:
            return None
        return (
            WorkStage(row["stage"]),
            bool(row["paused"]),
            row["reason"],
            row["actor_id"],
            datetime.fromisoformat(row["paused_at"]),
        )
