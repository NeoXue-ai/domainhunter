"""End-to-end orchestrator that turns CT poller output into real candidates.

Sits between the cursor-based ``CTPoller`` and the bounded L1 probe in
``WebRadarPipeline``. Each invocation:

1. Runs one poller tick — page → idempotent source_event rows.
2. Diffs ``store.list_domains()`` to find the registrable roots that just
   landed (the poller already enforces uniqueness on the same host).
3. For every new root (up to ``probe_limit``), calls
   ``pipeline.probe_domain(root)`` which writes the observation, builds the
   rule-author candidate draft, persists the candidate version, and stores
   the review-priority snapshot.

The orchestrator never reads back into the pipeline's mutable state; it just
threads the live store through both layers.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from webradar_v2.ingest.ct_poller import CTPoller
from webradar_v2.pipeline import WebRadarPipeline
from webradar_v2.storage.sqlite import SQLiteStore


@dataclass(frozen=True, slots=True)
class CTIngestRunSummary:
    """Observable counts for one end-to-end CT → candidate run."""

    certificates_seen: int
    events_added: int
    probes_run: int
    candidates_created: int
    next_cursor: str | None


class CTIngestOrchestrator:
    """Wire the CT poller to the probe pipeline so each new root gets a candidate."""

    def __init__(
        self,
        *,
        store: SQLiteStore,
        poller: CTPoller,
        pipeline: WebRadarPipeline,
        probe_limit: int = 50,
    ) -> None:
        if probe_limit < 1:
            raise ValueError("probe_limit must be positive")
        self._store = store
        self._poller = poller
        self._pipeline = pipeline
        self._probe_limit = probe_limit

    async def run_once(
        self, *, observed_at: datetime | None = None
    ) -> CTIngestRunSummary:
        """Poll one CT page, then probe each newly-seen registrable root."""
        stamp = observed_at or datetime.now(UTC)
        domains_before = set(self._store.list_domains())
        poll_result = await self._poller.poll()
        domains_after = set(self._store.list_domains())
        new_roots = sorted(domains_after - domains_before)
        candidates_created = 0
        probes_run = 0
        for root in new_roots[: self._probe_limit]:
            run = await self._pipeline.probe_domain(root, observed_at=stamp)
            probes_run += 1
            if run.candidate_version is not None:
                candidates_created += 1
        return CTIngestRunSummary(
            certificates_seen=poll_result.certificates_seen,
            events_added=poll_result.events_added,
            probes_run=probes_run,
            candidates_created=candidates_created,
            next_cursor=poll_result.next_cursor,
        )