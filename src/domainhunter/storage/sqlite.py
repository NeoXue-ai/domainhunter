"""Append-only local SQLite persistence for source signals and observations."""

import json
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from domainhunter.domain.candidates import (
    Candidate,
    CandidateOutcome,
    CandidateVersion,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
    build_candidate,
)
from domainhunter.domain.events import SourceEvent
from domainhunter.domain.metrics import FunnelMetrics
from domainhunter.domain.normalization import normalize_hostname
from domainhunter.domain.observations import Observation, OutcomeCode
from domainhunter.domain.retry_policy import decide_next_action
from domainhunter.domain.review_priority import ReviewPriority, ReviewPrioritySnapshot
from domainhunter.domain.review_queue import ReviewQueueItem
from domainhunter.domain.reviews import ReasonTag, ReviewAction, ReviewDecision
from domainhunter.domain.verification import CandidateVerification


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

    def funnel_metrics(self) -> FunnelMetrics:
        """Return a compact count snapshot of the discovery funnel."""
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
                )
            }
            outcome_rows = connection.execute(
                "SELECT outcome_code, COUNT(*) AS count FROM observations GROUP BY outcome_code"
            ).fetchall()
        return FunnelMetrics(
            source_events=counts["source_events"],
            domains=counts["domains"],
            observations=counts["observations"],
            candidates=counts["candidates"],
            candidate_versions=counts["candidate_versions"],
            review_decisions=counts["review_decisions"],
            observation_outcomes={row["outcome_code"]: row["count"] for row in outcome_rows},
        )

    @staticmethod
    def _candidate_exists(connection: sqlite3.Connection, candidate_id: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM candidates WHERE candidate_id = ?", (candidate_id,)
            ).fetchone()
            is not None
        )
