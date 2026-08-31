"""Spec §14 AlertEngine: 8 actionable checks against the local SQLite store."""

from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import logging
import statistics
from typing import Any, Mapping

from webradar_v2.domain.alerts import Alert, AlertKind, AlertSeverity
from webradar_v2.domain.candidates import CandidateOutcome
from webradar_v2.domain.observations import OutcomeCode
from webradar_v2.domain.work_queue import WorkStage
from webradar_v2.publish.aiknows_client import SyncStatus
from webradar_v2.scheduler.runbooks import get_runbook
from webradar_v2.storage.sqlite import SQLiteStore


_LOGGER = logging.getLogger("webradar_v2.scheduler.alerts")


# Errors that count as "transport/protocol" failures (spec §14 错误率突增).
_ERROR_OUTCOME_CODES: frozenset[OutcomeCode] = frozenset(
    {
        OutcomeCode.DNS_TIMEOUT,
        OutcomeCode.TLS_ERROR,
        OutcomeCode.CONNECT_TIMEOUT,
        OutcomeCode.HTTP_5XX,
        OutcomeCode.RENDER_TIMEOUT,
    }
)
# SSRF/WAF/robots interceptions are tracked separately per spec §14.
_INTERCEPTION_OUTCOME_CODES: frozenset[OutcomeCode] = frozenset(
    {OutcomeCode.BLOCKED_SSRF, OutcomeCode.ROBOTS_DISALLOWED}
)


class AlertEngine:
    """Run the 8 spec §14 checks against one local SQLite store."""

    def __init__(
        self,
        store: SQLiteStore,
        *,
        window: timedelta = timedelta(hours=1),
        baseline_window: timedelta = timedelta(hours=24),
        daily_budget_per_stage: Mapping[WorkStage, float] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._window = window
        self._baseline_window = baseline_window
        self._daily_budget_per_stage = dict(daily_budget_per_stage or {})
        self._clock = clock or (lambda: datetime.now(UTC))

    async def evaluate(self) -> tuple[Alert, ...]:
        """Run every check; persist and return only newly emitted alerts."""

        now = self._clock()
        candidates: list[Alert | None] = [
            self._check_zero_input(now=now),
            self._check_baseline_drift(now=now),
            self._check_queue_backlog(now=now),
            self._check_budget_exhausted(now=now),
            self._check_error_rate_spike(now=now),
            self._check_ssrf_interception_spike(now=now),
            self._check_schema_failure(now=now),
            self._check_external_sync_failure(now=now),
        ]
        emitted: list[Alert] = []
        for candidate in candidates:
            if candidate is None:
                continue
            inserted = self._store.append_alert(candidate, now=now)
            if inserted:
                emitted.append(candidate)
                _LOGGER.info(
                    "scheduler.alert.emitted",
                    extra={
                        "alert_id": candidate.alert_id,
                        "kind": candidate.kind.value,
                        "severity": candidate.severity.value,
                        "title": candidate.title,
                    },
                )
        return tuple(emitted)

    # ------------------------------------------------------------------
    # 8 individual checks
    # ------------------------------------------------------------------

    def _check_zero_input(self, *, now: datetime) -> Alert | None:
        recent = self._count_source_events(window=self._window, now=now)
        baseline_rate = self._source_events_per_hour(window=self._baseline_window, now=now)
        if recent > 0 or baseline_rate <= 0:
            return None
        alert = self._build_alert(
            kind=AlertKind.ZERO_INPUT,
            severity=AlertSeverity.CRITICAL,
            title="Source ingest has been silent for the last hour",
            summary=(
                f"0 source_events in the last {self._window}; "
                f"24h baseline rate is {baseline_rate:.2f}/h."
            ),
            now=now,
            payload={
                "recent_count": recent,
                "baseline_per_hour": baseline_rate,
                "window_minutes": int(self._window.total_seconds() // 60),
            },
        )
        return alert

    def _check_baseline_drift(self, *, now: datetime) -> Alert | None:
        recent_rate = self._source_events_per_hour(window=self._window, now=now)
        hourly = self._hourly_source_events(window=self._baseline_window, now=now)
        if not hourly:
            return None
        baseline_median = statistics.median(hourly)
        if baseline_median <= 0:
            return None
        ratio = recent_rate / baseline_median
        if 0.5 <= ratio <= 2.0:
            return None
        direction = "below" if ratio < 0.5 else "above"
        return self._build_alert(
            kind=AlertKind.BASELINE_DRIFT,
            severity=AlertSeverity.WARN,
            title=f"Ingest rate is {direction} the 24h baseline",
            summary=(
                f"1h rate {recent_rate:.2f}/h vs 24h median {baseline_median:.2f}/h "
                f"(ratio={ratio:.2f})."
            ),
            now=now,
            payload={
                "recent_per_hour": recent_rate,
                "baseline_median_per_hour": baseline_median,
                "ratio": ratio,
            },
        )

    def _check_queue_backlog(self, *, now: datetime) -> Alert | None:
        with self._store._connection() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT COUNT(*) AS stuck FROM work_queue
                WHERE completed_at IS NULL AND scheduled_at < ?
                """,
                ((now - self._window).isoformat(),),
            ).fetchone()
        stuck = int(row["stuck"])
        if stuck <= 0:
            return None
        return self._build_alert(
            kind=AlertKind.QUEUE_BACKLOG,
            severity=AlertSeverity.WARN,
            title=f"{stuck} work item(s) scheduled more than {int(self._window.total_seconds() // 60)}m ago",
            summary=(
                f"{stuck} queued work item(s) are still uncompleted after the "
                f"{int(self._window.total_seconds() // 60)}m grace window."
            ),
            now=now,
            payload={"stuck_count": stuck, "window_minutes": int(self._window.total_seconds() // 60)},
        )

    def _check_budget_exhausted(self, *, now: datetime) -> Alert | None:
        if not self._daily_budget_per_stage:
            return None
        metrics = self._store.funnel_metrics()
        with self._store._connection() as connection:  # noqa: SLF001
            rows = connection.execute(
                """
                SELECT stage, COALESCE(SUM(units), 0) AS reserved
                FROM budget_ledger
                WHERE status = 'reserved'
                  AND occurred_at >= ? AND occurred_at < ?
                GROUP BY stage
                """,
                (
                    (now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)).isoformat(),
                    (now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
                    + timedelta(days=1),
                ),
            ).fetchall()
        reserved_by_stage = {row["stage"]: float(row["reserved"]) for row in rows}
        exhausted: list[dict[str, object]] = []
        for stage, cap in self._daily_budget_per_stage.items():
            if cap <= 0:
                continue
            used = reserved_by_stage.get(stage.value, 0.0)
            if used / cap > 0.8:
                exhausted.append(
                    {
                        "stage": stage.value,
                        "reserved": used,
                        "daily_limit": cap,
                        "utilization": used / cap,
                    }
                )
        if not exhausted:
            return None
        # Use the most-saturated one in the payload; keep all stages in payload.
        worst = max(exhausted, key=lambda entry: float(entry["utilization"]))
        return self._build_alert(
            kind=AlertKind.BUDGET_EXHAUSTED,
            severity=AlertSeverity.CRITICAL,
            title="Daily budget exhausted for at least one stage",
            summary=(
                f"{len(exhausted)} stage(s) above 80% of daily cap; "
                f"worst: {worst['stage']} {worst['utilization']*100:.1f}%."
            ),
            now=now,
            payload={
                "exhausted_stages": exhausted,
                "budget_reserved_units_total": metrics.budget_reserved_units,
            },
            related_stage=worst["stage"],
        )

    def _check_error_rate_spike(self, *, now: datetime) -> Alert | None:
        recent = self._outcome_window(now=now)
        baseline = self._outcome_baseline_window(now=now)
        recent_rate = _ratio(recent["errors"], recent["total"])
        baseline_rate = _ratio(baseline["errors"], baseline["total"])
        if baseline_rate <= 0 or recent_rate <= 0:
            return None
        if recent_rate / baseline_rate <= 2.0:
            return None
        return self._build_alert(
            kind=AlertKind.ERROR_RATE_SPIKE,
            severity=AlertSeverity.CRITICAL,
            title="Observation error rate has more than doubled",
            summary=(
                f"1h error rate {recent_rate*100:.1f}% "
                f"({recent['errors']}/{recent['total']}) "
                f"vs 24h baseline {baseline_rate*100:.1f}% "
                f"({baseline['errors']}/{baseline['total']})."
            ),
            now=now,
            payload={
                "recent_rate": recent_rate,
                "baseline_rate": baseline_rate,
                "ratio": recent_rate / baseline_rate,
                "recent_errors": recent["errors"],
                "recent_total": recent["total"],
            },
        )

    def _check_ssrf_interception_spike(self, *, now: datetime) -> Alert | None:
        recent = self._interception_window(now=now)
        baseline = self._interception_baseline_window(now=now)
        recent_rate = _ratio(recent["interceptions"], recent["total"])
        baseline_rate = _ratio(baseline["interceptions"], baseline["total"])
        if baseline_rate <= 0 or recent_rate <= 0:
            return None
        if recent_rate / baseline_rate <= 2.0:
            return None
        return self._build_alert(
            kind=AlertKind.SSRF_INTERCEPTION_SPIKE,
            severity=AlertSeverity.WARN,
            title="SSRF / robots interception rate has more than doubled",
            summary=(
                f"1h interception rate {recent_rate*100:.1f}% "
                f"({recent['interceptions']}/{recent['total']}) "
                f"vs 24h baseline {baseline_rate*100:.1f}%."
            ),
            now=now,
            payload={
                "recent_rate": recent_rate,
                "baseline_rate": baseline_rate,
                "ratio": recent_rate / baseline_rate,
                "recent_interceptions": recent["interceptions"],
                "recent_total": recent["total"],
                "outcome_codes": [code.value for code in _INTERCEPTION_OUTCOME_CODES],
            },
        )

    def _check_schema_failure(self, *, now: datetime) -> Alert | None:
        with self._store._connection() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT COUNT(*) AS count FROM candidate_versions
                WHERE author_kind = 'llm'
                  AND primary_outcome = ?
                  AND created_at >= ?
                """,
                (
                    CandidateOutcome.VALID_BUT_NOT_READY.value,
                    (now - self._window).isoformat(),
                ),
            ).fetchone()
        count = int(row["count"])
        if count <= 0:
            return None
        return self._build_alert(
            kind=AlertKind.SCHEMA_FAILURE,
            severity=AlertSeverity.WARN,
            title="LLM schema/taxonomy failures escalated to manual review",
            summary=(
                f"{count} llm-authored candidate version(s) in the last "
                f"{int(self._window.total_seconds() // 60)}m marked "
                f"valid_but_not_ready (likely schema/taxonomy failure)."
            ),
            now=now,
            payload={
                "llm_needs_review_count": count,
                "window_minutes": int(self._window.total_seconds() // 60),
                "outcome": CandidateOutcome.VALID_BUT_NOT_READY.value,
            },
        )

    def _check_external_sync_failure(self, *, now: datetime) -> Alert | None:
        with self._store._connection() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT COUNT(*) AS count FROM ai_knows_audit
                WHERE occurred_at >= ?
                """,
                ((now - self._window).isoformat(),),
            ).fetchone()
            rows = connection.execute(
                """
                SELECT candidate_id, COUNT(*) AS count FROM ai_knows_audit
                WHERE candidate_id IS NOT NULL
                  AND occurred_at >= ?
                GROUP BY candidate_id
                ORDER BY count DESC
                """,
                ((now - self._window).isoformat(),),
            ).fetchall()
        # Without status_code we cannot distinguish reconciliation from success;
        # the alert fires when we see any candidate-tagged traffic in the window
        # so an operator can run the runbook query and triage manually.
        candidates_in_window = [(row["candidate_id"], int(row["count"])) for row in rows]
        if not candidates_in_window:
            return None
        top_candidate, top_count = candidates_in_window[0]
        return self._build_alert(
            kind=AlertKind.EXTERNAL_SYNC_FAILURE,
            severity=AlertSeverity.WARN,
            title="AIKnows external sync activity in the last window — review for reconciliation",
            summary=(
                f"{len(candidates_in_window)} candidate(s) had AIKnows audit traffic; "
                f"top: {top_candidate} with {top_count} entries."
            ),
            now=now,
            payload={
                "candidates_with_audit": len(candidates_in_window),
                "top_candidate_id": top_candidate,
                "top_candidate_audit_count": top_count,
                "window_minutes": int(self._window.total_seconds() // 60),
                "sync_statuses_to_check": [SyncStatus.RECONCILIATION_REQUIRED.value],
            },
            related_candidate_id=top_candidate,
        )

    # ------------------------------------------------------------------
    # Helpers — counts and ratios
    # ------------------------------------------------------------------

    def _count_source_events(self, *, window: timedelta, now: datetime) -> int:
        with self._store._connection() as connection:  # noqa: SLF001
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM source_events WHERE observed_at >= ?",
                ((now - window).isoformat(),),
            ).fetchone()
        return int(row["count"])

    def _source_events_per_hour(self, *, window: timedelta, now: datetime) -> float:
        count = self._count_source_events(window=window, now=now)
        hours = window.total_seconds() / 3600.0
        if hours <= 0:
            return 0.0
        return count / hours

    def _hourly_source_events(self, *, window: timedelta, now: datetime) -> list[int]:
        """Return a coarse per-hour bucket series for the baseline window.

        The implementation buckets timestamps into 1-hour slices anchored to
        ``now`` and counts source events per slice. Using fixed-clock
        boundaries avoids calendar artefacts (DST, hour-of-day alignment).
        """
        hours = max(int(window.total_seconds() // 3600), 1)
        with self._store._connection() as connection:  # noqa: SLF001
            rows = connection.execute(
                """
                SELECT observed_at FROM source_events WHERE observed_at >= ?
                """,
                ((now - window).isoformat(),),
            ).fetchall()
        counts = [0] * hours
        window_start = now - window
        for row in rows:
            ts = datetime.fromisoformat(row["observed_at"])
            delta = ts - window_start
            bucket = int(delta.total_seconds() // 3600)
            if 0 <= bucket < hours:
                counts[bucket] += 1
        return counts

    def _outcome_window(self, *, now: datetime) -> dict[str, int]:
        return self._outcome_counts(window=self._window, now=now, codes=_ERROR_OUTCOME_CODES)

    def _outcome_baseline_window(self, *, now: datetime) -> dict[str, int]:
        return self._outcome_counts(window=self._baseline_window, now=now, codes=_ERROR_OUTCOME_CODES)

    def _interception_window(self, *, now: datetime) -> dict[str, int]:
        return self._outcome_counts(window=self._window, now=now, codes=_INTERCEPTION_OUTCOME_CODES)

    def _interception_baseline_window(self, *, now: datetime) -> dict[str, int]:
        return self._outcome_counts(
            window=self._baseline_window, now=now, codes=_INTERCEPTION_OUTCOME_CODES
        )

    def _outcome_counts(
        self, *, window: timedelta, now: datetime, codes: frozenset[OutcomeCode]
    ) -> dict[str, int]:
        placeholders = ",".join("?" for _ in codes)
        with self._store._connection() as connection:  # noqa: SLF001
            row = connection.execute(
                f"""
                SELECT
                    SUM(CASE WHEN outcome_code IN ({placeholders}) THEN 1 ELSE 0 END) AS matched,
                    COUNT(*) AS total
                FROM observations WHERE observed_at >= ?
                """,
                [code.value for code in codes] + [(now - window).isoformat()],
            ).fetchone()
        matched = int(row["matched"] or 0)
        total = int(row["total"])
        return {"errors": matched, "interceptions": matched, "total": total}

    def _build_alert(
        self,
        *,
        kind: AlertKind,
        severity: AlertSeverity,
        title: str,
        summary: str,
        now: datetime,
        payload: Mapping[str, Any],
        related_candidate_id: str | None = None,
        related_stage: str | None = None,
    ) -> Alert:
        runbook = get_runbook(kind)
        # Deterministic alert_id — dedup depends on this exact format.
        material = json.dumps(
            {
                "kind": kind.value,
                "candidate": related_candidate_id,
                "stage": related_stage,
                "bucket": int(now.timestamp() // 60),  # minute bucket keeps IDs stable across retries
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        alert_id = sha256(material.encode("utf-8")).hexdigest()
        # Payload must be a frozen mapping; copy + freeze via MappingProxyType would
        # mutate callers, so we accept a regular dict and trust the Alert dataclass
        # to wrap it in slots + freeze it from the outside (immutable in practice
        # because Alert is a frozen dataclass).
        return Alert(
            alert_id=alert_id,
            kind=kind,
            severity=severity,
            title=title,
            summary=summary,
            opened_at=now,
            runbook_id=runbook.runbook_id,
            related_candidate_id=related_candidate_id,
            related_stage=related_stage,
            payload=dict(payload),
        )


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


__all__ = ["AlertEngine", "asdict"]  # re-exported for tests