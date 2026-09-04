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
5. Optionally adds an LLM-authored version using the same strict evidence.

The orchestrator never reads back into the pipeline's mutable state; it just
threads the live store through both layers.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from domainhunter.filter.pipeline import FilterDecision, FilterPipeline, FilteredCandidate
from domainhunter.ingest.ct_poller import CTPoller
from domainhunter.llm.provider import LLMProvider
from domainhunter.pipeline import DomainHunterPipeline, enrich_candidate_with_llm
from domainhunter.storage.sqlite import SQLiteStore
from domainhunter.domain.verification import CandidateVerification

_LOGGER = logging.getLogger("domainhunter.ct_ingest")


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
    llm_enriched: int = 0
    source_errors: tuple[str, ...] = ()
    pending_work: int = 0


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
        provider: LLMProvider | None = None,
        work_lease_seconds: float = 300.0,
    ) -> None:
        if probe_limit < 1:
            raise ValueError("probe_limit must be positive")
        if work_lease_seconds <= 0:
            raise ValueError("work_lease_seconds must be positive")
        self._store = store
        self._poller = poller
        self._pipeline = pipeline
        self._filter_pipeline = filter_pipeline
        self._require_first_seen = require_first_seen
        self._probe_limit = probe_limit
        self._provider = provider
        self._work_lease_seconds = work_lease_seconds

    async def run_once(
        self, *, observed_at: datetime | None = None
    ) -> CTIngestRunSummary:
        """Poll one CT page, then probe each newly-seen registrable root."""
        stamp = observed_at or datetime.now(UTC)
        domains_before = set(self._store.list_domains())
        poll_result = await self._poller.poll()
        # Live CT entries are stamped while the HTTP poll is in progress.  Use
        # the post-poll clock for an implicit live run so those just-persisted
        # tasks are eligible immediately, rather than waiting a full round.
        if observed_at is None:
            stamp = datetime.now(UTC)
        domains_after = set(self._store.list_domains())
        new_roots = sorted(domains_after - domains_before)
        work_items = self._store.claim_ct_discovery_work(
            now=stamp,
            lease_seconds=self._work_lease_seconds,
            limit=self._probe_limit,
        )
        work_by_domain = {item.domain: item for item in work_items}
        roots_to_probe = list(work_by_domain)
        filtered_by_domain: dict[str, FilteredCandidate] = {}
        strict_rejections = 0
        if self._filter_pipeline is not None:
            decisions: tuple[FilterDecision, ...] | None = None
            if isinstance(self._filter_pipeline, FilterPipeline):
                decisions = self._filter_pipeline.evaluate(roots_to_probe, observed_at=stamp)
                filtered = tuple(
                    decision.candidate
                    for decision in decisions
                    if decision.candidate is not None
                )
                strict_rejections = sum(
                    1
                    for decision in decisions
                    if decision.candidate is None and not decision.retryable
                )
            else:
                filtered = self._filter_pipeline.run(roots_to_probe, observed_at=stamp)
                strict_rejections = len(roots_to_probe) - len(filtered)
            filtered_by_domain = {
                candidate.domain: candidate
                for candidate in filtered
                if isinstance(candidate, FilteredCandidate)
            }
            roots_to_probe = [candidate.domain for candidate in filtered]
            if decisions is not None:
                for decision in decisions:
                    if decision.candidate is None and not decision.retryable:
                        self._store.complete_ct_discovery_domain(
                            decision.domain,
                            lease_token=work_by_domain[decision.domain].lease_token,
                            at=stamp,
                            reason=decision.reason,
                        )
                    elif decision.candidate is None:
                        self._store.retry_ct_discovery_work(
                            decision.domain,
                            lease_token=work_by_domain[decision.domain].lease_token,
                            scheduled_at=stamp + timedelta(minutes=5),
                            error=decision.reason,
                        )
            elif set(roots_to_probe) != set(work_by_domain):
                for root in set(work_by_domain) - set(roots_to_probe):
                    self._store.retry_ct_discovery_work(
                        root,
                        lease_token=work_by_domain[root].lease_token,
                        scheduled_at=stamp + timedelta(minutes=5),
                        error="filter_rejected_without_retry_metadata",
                    )
        candidates_created = 0
        probes_run = 0
        llm_enriched = 0
        for root in roots_to_probe:
            work = work_by_domain[root]
            try:
                run = await self._pipeline.probe_domain(
                    root,
                    observed_at=stamp,
                    require_same_final_root=(
                        self._require_first_seen and self._filter_pipeline is not None
                    ),
                )
            except Exception as error:
                self._store.retry_ct_discovery_work(
                    root,
                    lease_token=work.lease_token,
                    scheduled_at=stamp,
                    error=str(error) or type(error).__name__,
                )
                raise
            probes_run += 1
            if run.candidate_version is not None:
                candidates_created += 1
                filtered_candidate = filtered_by_domain.get(root)
                if filtered_candidate is not None:
                    self._append_verification(
                        candidate_id=run.candidate_version.candidate_id,
                        candidate_version=run.candidate_version.version,
                        root=root,
                        filtered_candidate=filtered_candidate,
                        observed_at=stamp,
                        status_code=run.observation.status_code,
                        final_url=run.observation.final_url,
                        canonical_url=run.observation.canonical_url,
                    )
                if self._provider is not None:
                    try:
                        draft = await enrich_candidate_with_llm(
                            store=self._store,
                            candidate_id=run.candidate_version.candidate_id,
                            candidate_version=run.candidate_version.version,
                            provider=self._provider,
                            observed_at=stamp,
                        )
                    except Exception as error:  # noqa: BLE001 - preserve a successful strict discovery
                        _LOGGER.exception(
                            "ct_ingest.llm_enrichment.failed "
                            "[domain=%s, candidate_id=%s, error=%s]",
                            root,
                            run.candidate_version.candidate_id,
                            str(error) or type(error).__name__,
                        )
                        draft = None
                    if draft is not None:
                        llm_enriched += 1
                        if filtered_candidate is not None:
                            latest_version = self._store.list_candidate_versions(
                                run.candidate_version.candidate_id
                            )[-1]
                            self._append_verification(
                                candidate_id=latest_version.candidate_id,
                                candidate_version=latest_version.version,
                                root=root,
                                filtered_candidate=filtered_candidate,
                                observed_at=stamp,
                                status_code=run.observation.status_code,
                                final_url=run.observation.final_url,
                                canonical_url=run.observation.canonical_url,
                            )
                self._store.complete_ct_discovery_domain(
                    root,
                    lease_token=work.lease_token,
                    at=stamp,
                    reason="candidate_created",
                )
            elif run.probe_result.analysis is not None:
                self._store.complete_ct_discovery_domain(
                    root,
                    lease_token=work.lease_token,
                    at=stamp,
                    reason="not_a_candidate",
                )
            else:
                self._store.retry_ct_discovery_work(
                    root,
                    lease_token=work.lease_token,
                    scheduled_at=(
                        run.retry_decision.next_check_at
                        or stamp + timedelta(minutes=5)
                    ),
                    error=run.observation.outcome_code.value,
                )
        return CTIngestRunSummary(
            certificates_seen=poll_result.certificates_seen,
            events_added=poll_result.events_added,
            probes_run=probes_run,
            candidates_created=candidates_created,
            next_cursor=poll_result.next_cursor,
            roots_observed=len(new_roots),
            strict_rejections=strict_rejections,
            llm_enriched=llm_enriched,
            source_errors=poll_result.source_errors,
            pending_work=self._store.pending_ct_discovery_work_count(),
        )

    def _append_verification(
        self,
        *,
        candidate_id: str,
        candidate_version: int,
        root: str,
        filtered_candidate: FilteredCandidate,
        observed_at: datetime,
        status_code: int | None,
        final_url: str | None,
        canonical_url: str | None,
    ) -> None:
        """Attach the same strict source facts to every derived version."""
        self._store.append_candidate_verification(
            CandidateVerification(
                candidate_id=candidate_id,
                candidate_version=candidate_version,
                checked_at=observed_at,
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
                http_status_code=status_code,
                final_url=final_url,
                canonical_url=canonical_url,
                final_root_matches=(
                    True
                    if self._require_first_seen and self._filter_pipeline is not None
                    else None
                ),
            )
        )
