"""S5 automation: filter → probe → LLM enrich → review queue.

This module wires the whole newborn-domain funnel end to end:

    S1 static signals → S2 RDAP age → S3 DNS → S4 live probe → S5 LLM
    classification → persisted candidate versions (rule + llm) → review queue.

``run_batch`` is the single entry point used by the CLI; it returns a
JSON-safe summary for every input domain so operators can see which
stage each domain reached and what the LLM decided.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from domainhunter.crawler.http_probe import HTTPProbe
from domainhunter.domain.events import SourceEvent
from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.llm.provider import LLMProvider, OpenAICompatibleProvider
from domainhunter.pipeline import DomainHunterPipeline, enrich_candidate_with_llm
from domainhunter.storage.sqlite import SQLiteStore


@dataclass(frozen=True, slots=True)
class BatchOutcome:
    """One domain's journey through the funnel, JSON-safe for the console."""

    domain: str
    stage: str  # filtered_out | filtered_unknown | probe_failed | enriched | llm_skipped
    final_tier: str | None
    s1_score: float | None
    age_days: int | None
    probe_outcome: str | None
    llm_outcome: str | None
    llm_confidence: float | None
    llm_model: str | None
    reason: str

    def as_payload(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "stage": self.stage,
            "final_tier": self.final_tier,
            "s1_score": self.s1_score,
            "age_days": self.age_days,
            "probe_outcome": self.probe_outcome,
            "llm_outcome": self.llm_outcome,
            "llm_confidence": self.llm_confidence,
            "llm_model": self.llm_model,
            "reason": self.reason,
        }


async def run_batch(
    *,
    store: SQLiteStore,
    domains: list[str],
    provider: LLMProvider,
    tier1_days: int = 30,
    tier2_days: int = 90,
    require_dns: bool = False,
    drop_unknown_rdap: bool = False,
    observed_at: datetime | None = None,
    pipeline: FilterPipeline | None = None,
    probe: HTTPProbe | None = None,
) -> tuple[BatchOutcome, ...]:
    """Run the full S1→S5 funnel over ``domains`` and persist everything.

    ``pipeline`` and ``probe`` are injectable for hermetic tests; they
    default to the real filter funnel and HTTP probe.
    """
    observed_at = observed_at or datetime.now(UTC)
    pipeline = pipeline or FilterPipeline(
        tier1_days=tier1_days,
        tier2_days=tier2_days,
        require_dns=require_dns,
        drop_unknown_rdap=drop_unknown_rdap,
    )
    candidates = pipeline.run(domains, observed_at=observed_at)
    candidate_by_domain = {c.domain: c for c in candidates}

    if probe is None:
        async with HTTPProbe() as owned_probe:
            return await _probe_and_enrich(
                store=store,
                domains=domains,
                provider=provider,
                candidates=candidates,
                candidate_by_domain=candidate_by_domain,
                probe=owned_probe,
                observed_at=observed_at,
            )
    return await _probe_and_enrich(
        store=store,
        domains=domains,
        provider=provider,
        candidates=candidates,
        candidate_by_domain=candidate_by_domain,
        probe=probe,
        observed_at=observed_at,
    )


async def _probe_and_enrich(
    *,
    store: SQLiteStore,
    domains: list[str],
    provider: LLMProvider,
    candidates: tuple,
    candidate_by_domain: dict,
    probe: HTTPProbe,
    observed_at: datetime,
) -> tuple[BatchOutcome, ...]:
    outcomes: list[BatchOutcome] = []
    domainhunter_pipeline = DomainHunterPipeline(store=store, probe=probe)
    for domain in domains:
        candidate = candidate_by_domain.get(domain)
        if candidate is None:
            outcomes.append(
                BatchOutcome(
                    domain=domain,
                    stage="filtered_out",
                    final_tier=None,
                    s1_score=None,
                    age_days=None,
                    probe_outcome=None,
                    llm_outcome=None,
                    llm_confidence=None,
                    llm_model=None,
                    reason="dropped by the age gate (or DNS)",
                )
            )
            continue
        # Persist as a filter source event (append-only invariant).
        store.append_source_event(
            SourceEvent(
                source="filter",
                source_event_id=f"filter:{domain}",
                raw_subject=domain,
                observed_at=observed_at,
            ),
            hostname=domain,
        )
        try:
            probe_run = await domainhunter_pipeline.probe_domain(domain, observed_at=observed_at)
        except Exception as exc:  # noqa: BLE001 - one bad domain must not kill the batch
            outcomes.append(
                BatchOutcome(
                    domain=domain,
                    stage="probe_failed",
                    final_tier=candidate.final_tier,
                    s1_score=candidate.s1.score,
                    age_days=candidate.s2.age_days,
                    probe_outcome="error",
                    llm_outcome=None,
                    llm_confidence=None,
                    llm_model=None,
                    reason=f"probe error: {exc}",
                )
            )
            continue
        probe_outcome = probe_run.observation.outcome_code.value
        if probe_run.candidate_version is None:
            outcomes.append(
                BatchOutcome(
                    domain=domain,
                    stage="probe_failed",
                    final_tier=candidate.final_tier,
                    s1_score=candidate.s1.score,
                    age_days=candidate.s2.age_days,
                    probe_outcome=probe_outcome,
                    llm_outcome=None,
                    llm_confidence=None,
                    llm_model=None,
                    reason="probe produced no rule draft",
                )
            )
            continue
        # S5: LLM classification on top of the rule draft.
        draft = await enrich_candidate_with_llm(
            store=store,
            candidate_id=probe_run.candidate_version.candidate_id,
            candidate_version=probe_run.candidate_version.version,
            provider=provider,
            observed_at=observed_at,
        )
        if draft is None:
            outcomes.append(
                BatchOutcome(
                    domain=domain,
                    stage="llm_skipped",
                    final_tier=candidate.final_tier,
                    s1_score=candidate.s1.score,
                    age_days=candidate.s2.age_days,
                    probe_outcome=probe_outcome,
                    llm_outcome=None,
                    llm_confidence=None,
                    llm_model=None,
                    reason="LLM output failed the schema gate",
                )
            )
            continue
        outcomes.append(
            BatchOutcome(
                domain=domain,
                stage="enriched",
                final_tier=candidate.final_tier,
                s1_score=candidate.s1.score,
                age_days=candidate.s2.age_days,
                probe_outcome=probe_outcome,
                llm_outcome=draft.primary_outcome.value,
                llm_confidence=draft.classification_confidence,
                llm_model=draft.model_version,
                reason="LLM classification persisted",
            )
        )
    return tuple(outcomes)


def build_openai_provider(*, base_url: str, token: str, model: str) -> OpenAICompatibleProvider:
    """Construct the standard OpenAI-compatible provider for the batch run."""
    return OpenAICompatibleProvider(base_url=base_url, token=token, model=model)