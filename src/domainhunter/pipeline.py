"""Small orchestration boundary joining ingestion, L1 probing, and persistence."""

from dataclasses import dataclass
from datetime import datetime

from domainhunter.crawler.http_probe import HTTPProbe, ProbeResult
from domainhunter.domain.candidates import CandidateVersion, CandidateVersionDraft, Evidence
from domainhunter.domain.events import SourceEvent
from domainhunter.domain.normalization import normalize_hostname
from domainhunter.domain.observations import Observation
from domainhunter.domain.retry_policy import RetryDecision, decide_next_action
from domainhunter.domain.review_priority import ReviewPriorityInputs, calculate_review_priority
from domainhunter.domain.rule_candidates import build_rule_candidate_draft
from domainhunter.llm.provider import LLMProvider
from domainhunter.llm.schema import LLMResultState, TAXONOMY_VERSION
from domainhunter.storage.sqlite import SQLiteStore


@dataclass(frozen=True, slots=True)
class ProbeRun:
    """The durable observation and the next deterministic scheduler decision."""

    observation: Observation
    retry_decision: RetryDecision
    probe_result: ProbeResult
    candidate_version: CandidateVersion | None = None


class DomainHunterPipeline:
    """Coordinate one source event and one bounded L1 probe."""

    def __init__(self, *, store: SQLiteStore, probe: HTTPProbe) -> None:
        self._store = store
        self._probe = probe

    def ingest_event(self, event: SourceEvent) -> bool:
        """Persist a source event using its raw hostname as the domain signal."""
        return self._store.append_source_event(event, hostname=event.raw_subject)

    async def probe_domain(self, hostname: str, *, observed_at: datetime) -> ProbeRun:
        """Probe a known domain, append its result, and calculate its next action."""
        normalized = normalize_hostname(hostname)
        prior_observations = self._store.list_observations(normalized.hostname)
        probe_result = await self._probe.probe(normalized.hostname)
        analysis = probe_result.analysis
        status_code = analysis.status_code if analysis else None
        canonical_url = analysis.canonical_url if analysis else None
        internal_links = analysis.internal_links if analysis else ()
        observation = Observation(
            domain=normalized.registrable_domain,
            outcome_code=probe_result.outcome_code,
            observed_at=observed_at,
            attempt_number=len(prior_observations) + 1,
            status_code=status_code,
            final_url=probe_result.final_url,
            detail=probe_result.detail,
            canonical_url=canonical_url,
            internal_links=internal_links,
        )
        self._store.append_observation(observation)
        candidate_version: CandidateVersion | None = None
        if probe_result.analysis is not None:
            draft = build_rule_candidate_draft(probe_result.analysis)
            if draft is not None:
                candidate = self._store.create_candidate(
                    normalized.registrable_domain, created_at=observed_at
                )
                candidate_version = self._store.append_candidate_version(
                    candidate.candidate_id, draft, created_at=observed_at
                )
                evidence_count = len(draft.evidence)
                priority = calculate_review_priority(
                    ReviewPriorityInputs(
                        product_evidence=(
                            0.9
                            if draft.primary_outcome.value == "publishable_ai_saas"
                            else 0.5
                        ),
                        early_presence=1.0,
                        low_exposure=None,
                        data_completeness=min(1.0, evidence_count / 3),
                    )
                )
                self._store.append_review_priority(
                    candidate.candidate_id, priority, calculated_at=observed_at
                )
        retry_decision = decide_next_action(
            observation.outcome_code,
            attempt_number=observation.attempt_number,
            now=observed_at,
        )
        return ProbeRun(
            observation=observation,
            retry_decision=retry_decision,
            probe_result=probe_result,
            candidate_version=candidate_version,
        )

    async def probe_due_domains(
        self, *, observed_at: datetime, limit: int | None = None
    ) -> tuple[ProbeRun, ...]:
        """Run bounded probes only for domains due at this scheduler tick."""
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive")
        due = self._store.due_domains(observed_at)
        if limit is not None:
            due = due[:limit]
        runs: list[ProbeRun] = []
        for domain in due:
            runs.append(await self.probe_domain(domain, observed_at=observed_at))
        return tuple(runs)


async def enrich_candidate_with_llm(
    *,
    store: SQLiteStore,
    candidate_id: str,
    candidate_version: int,
    provider: LLMProvider,
    observed_at: datetime,
) -> CandidateVersionDraft | None:
    """Persist an LLM-authored candidate version on top of an existing rule one.

    Returns the persisted draft when the provider produced a verified
    candidate, or ``None`` when the model output failed the schema/evidence
    gate and must be retried or escalated to manual review.
    """
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    version = store.get_candidate_version(candidate_id, candidate_version)
    if version is None:
        raise ValueError("candidate version does not exist")
    allowed_evidence: tuple[Evidence, ...] = tuple(version.draft.evidence)
    candidate = store.get_candidate(candidate_id)
    domain_label = candidate.domain if candidate is not None else candidate_id

    result = await provider.extract(
        domain=domain_label,
        evidence=allowed_evidence,
        schema_version=TAXONOMY_VERSION,
    )
    if result.state is not LLMResultState.READY or result.draft is None:
        return None
    store.append_candidate_version(
        candidate_id, result.draft, created_at=observed_at
    )
    return result.draft
