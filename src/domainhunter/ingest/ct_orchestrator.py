"""End-to-end orchestrator that turns CT poller output into real candidates.

Sits between the cursor-based ``CTPoller`` and the bounded L1 probe in
``DomainHunterPipeline``. Each invocation:

1. Runs one poller tick — page → idempotent source_event rows.
2. Diffs ``store.list_domains()`` to find the registrable roots that just
   landed (the poller already enforces uniqueness on the same host).
3. Optionally narrows roots to their durable first CT sighting, then passes
   them through the synchronous filter funnel.
4. For every remaining root (up to ``probe_limit``), calls
   ``pipeline.probe_domain(root)`` which writes the observation, builds the
   rule-author candidate draft, persists the candidate version, and stores
   the review-priority snapshot.

The orchestrator never reads back into the pipeline's mutable state; it just
threads the live store through both layers.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from domainhunter.filter.pipeline import FilterPipeline, FilteredCandidate
from domainhunter.ingest.ct_poller import CTPoller
from domainhunter.pipeline import DomainHunterPipeline
from domainhunter.storage.sqlite import SQLiteStore
from domainhunter.domain.verification import CandidateVerification


@dataclass(frozen=True, slots=True)
class CTIngestRunSummary:
    """Observable counts for one end-to-end CT → candidate run."""

    certificates_seen: int
    events_added: int
    probes_run: int
    candidates_created: int
    next_cursor: str | None
    roots_observed: int = 0
    strict_rejections: int = 0


class CTIngestOrchestrator:
    """Wire the CT poller to the probe pipeline so each new root gets a candidate."""

    def __init__(
        self,
        *,
        store: SQLiteStore,
        poller: CTPoller,
        pipeline: DomainHunterPipeline,
        filter_pipeline: FilterPipeline | None = None,
        require_first_seen: bool = False,
        probe_limit: int = 50,
    ) -> None:
        if probe_limit < 1:
            raise ValueError("probe_limit must be positive")
        self._store = store
        self._poller = poller
        self._pipeline = pipeline
        self._filter_pipeline = filter_pipeline
        self._require_first_seen = require_first_seen
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
        roots_to_probe = new_roots
        filtered_by_domain: dict[str, FilteredCandidate] = {}
        strict_rejections = 0
        if self._require_first_seen:
            roots_to_probe = list(self._store.filter_new(tuple(roots_to_probe)))
        if self._filter_pipeline is not None:
            filtered = self._filter_pipeline.run(roots_to_probe, observed_at=stamp)
            strict_rejections = len(roots_to_probe) - len(filtered)
            filtered_by_domain = {
                candidate.domain: candidate
                for candidate in filtered
                if isinstance(candidate, FilteredCandidate)
            }
            roots_to_probe = [candidate.domain for candidate in filtered]
        if self._require_first_seen:
            self._store.mark_seen(tuple(new_roots), at=stamp, source="ct")
        candidates_created = 0
        probes_run = 0
        for root in roots_to_probe[: self._probe_limit]:
            run = await self._pipeline.probe_domain(
                root,
                observed_at=stamp,
                require_same_final_root=(
                    self._require_first_seen and self._filter_pipeline is not None
                ),
            )
            probes_run += 1
            if run.candidate_version is not None:
                candidates_created += 1
                filtered_candidate = filtered_by_domain.get(root)
                if filtered_candidate is not None:
                    self._store.append_candidate_verification(
                        CandidateVerification(
                            candidate_id=run.candidate_version.candidate_id,
                            candidate_version=run.candidate_version.version,
                            checked_at=stamp,
                            ct_first_seen_at=self._store.get_ct_first_seen_at(root),
                            rdap_tier=filtered_candidate.s2.tier,
                            rdap_age_days=filtered_candidate.s2.age_days,
                            rdap_registration_at=(
                                filtered_candidate.s2.registration.registration_date
                                if filtered_candidate.s2.registration
                                else None
                            ),
                            dns_has_a=(
                                filtered_candidate.s3.has_a
                                if filtered_candidate.s3 is not None
                                else None
                            ),
                            http_status_code=run.observation.status_code,
                            final_url=run.observation.final_url,
                            canonical_url=run.observation.canonical_url,
                            final_root_matches=(
                                True
                                if self._require_first_seen
                                and self._filter_pipeline is not None
                                else None
                            ),
                        )
                    )
        return CTIngestRunSummary(
            certificates_seen=poll_result.certificates_seen,
            events_added=poll_result.events_added,
            probes_run=probes_run,
            candidates_created=candidates_created,
            next_cursor=poll_result.next_cursor,
            roots_observed=len(new_roots),
            strict_rejections=strict_rejections,
        )
