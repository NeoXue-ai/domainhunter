"""Filter-layer funnel: S1 static → S2 RDAP age → S3 DNS presence → S4 probe.

The funnel turns a batch of registrable domains (from the CT ingestion
layer) into a shortlist of "newborn domain" candidates, each carrying
an evidence chain so the review console can show *why* it was kept.

The pipeline is intentionally sequential and cheap-first: S1 scores
(no network), S2 RDAP (one HTTP query per domain, cached), S3 DNS
(one resolve per domain). Only domains that survive the age gate get
the DNS probe, and only candidates get the optional S4 live probe.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from domainhunter.filter.dns_check import DnsResult, check_dns
from domainhunter.filter.rdap_age import (
    AgeVerdict,
    MemoryCache,
    RegistrationCache,
    classify_age,
    fetch_registration,
    rdap_base,
)
from domainhunter.filter.static_signals import DomainScore, score_domain

ProbeCallable = Callable[..., Awaitable[object]]


@dataclass(frozen=True, slots=True)
class FilteredCandidate:
    """One domain that survived the S2 age gate, with full evidence."""

    domain: str
    s1: DomainScore
    s2: AgeVerdict
    s3: DnsResult | None
    final_tier: str
    reason: str
    observed_at: datetime
    probe: object | None = None

    def as_payload(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "final_tier": self.final_tier,
            "reason": self.reason,
            "observed_at": self.observed_at.isoformat(),
            "s1": self.s1.as_payload(),
            "s2": self.s2.as_payload(),
            "s3": self.s3.as_payload() if self.s3 else None,
            "probe": (
                _probe_payload(self.probe) if self.probe is not None else None
            ),
        }


@dataclass(frozen=True, slots=True)
class FilterDecision:
    """One S1→S3 decision, including whether a rejection should be retried."""

    domain: str
    candidate: FilteredCandidate | None
    retryable: bool
    reason: str


def _probe_payload(probe: object) -> dict[str, object]:
    """Best-effort serialization of a probe result for the review console."""
    if hasattr(probe, "as_payload"):
        return probe.as_payload()  # type: ignore[no-any-return]
    if isinstance(probe, dict):
        return probe
    return {"probe": str(probe)}


def _fetch_registrations_parallel(
    domains: list[str], rdap_fetcher, concurrency: int
) -> dict[str, object | None]:
    """Fetch RDAP registrations with a bounded thread pool.

    Each domain is independent, so results are merged back in the calling
    thread — the cache is only touched serially, staying race-free.
    """
    from concurrent.futures import ThreadPoolExecutor

    results: dict[str, object | None] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {domain: pool.submit(rdap_fetcher, domain) for domain in domains}
        for domain, future in futures.items():
            results[domain] = future.result()
    return results


def _check_dns_parallel(
    domains: list[str], dns_checker, concurrency: int
) -> dict[str, DnsResult]:
    """Run the DNS checker one domain at a time across a bounded thread pool."""
    from concurrent.futures import ThreadPoolExecutor

    merged: dict[str, DnsResult] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {domain: pool.submit(dns_checker, [domain]) for domain in domains}
        for domain, future in futures.items():
            merged.update(future.result())
    return merged


class FilterPipeline:
    """Run the S1→S2→S3 funnel over a batch of registrable domains."""

    def __init__(
        self,
        *,
        cache: RegistrationCache | None = None,
        tier1_days: int = 30,
        tier2_days: int = 90,
        require_dns: bool = False,
        drop_unknown_rdap: bool = False,
        rdap_fetcher=None,
        dns_checker=None,
        probe: ProbeCallable | None = None,
        rdap_concurrency: int = 1,
        dns_concurrency: int = 1,
    ) -> None:
        if rdap_concurrency < 1 or dns_concurrency < 1:
            raise ValueError("RDAP/DNS concurrency must be positive")
        self._cache = cache or MemoryCache()
        self._tier1_days = tier1_days
        self._tier2_days = tier2_days
        self._require_dns = require_dns
        self._drop_unknown_rdap = drop_unknown_rdap
        self._rdap_fetcher = rdap_fetcher or fetch_registration
        self._dns_checker = dns_checker or check_dns
        self._probe = probe
        self._rdap_concurrency = rdap_concurrency
        self._dns_concurrency = dns_concurrency

    def run(
        self, domains: list[str], *, observed_at: datetime | None = None
    ) -> tuple[FilteredCandidate, ...]:
        """Run the funnel. Returns candidates that pass the age gate."""
        return tuple(
            decision.candidate
            for decision in self.evaluate(domains, observed_at=observed_at)
            if decision.candidate is not None
        )

    def evaluate(
        self, domains: list[str], *, observed_at: datetime | None = None
    ) -> tuple[FilterDecision, ...]:
        """Classify every input and identify which strict rejections may recover."""
        observed_at = observed_at or datetime.now(UTC)
        s1_results: dict[str, DomainScore] = {
            domain: score_domain(domain) for domain in domains
        }

        # S2: RDAP age — only domains we haven't already classified this run.
        age_verdicts: dict[str, AgeVerdict] = {}
        pending_rdap: list[str] = []
        for domain in domains:
            cached = self._cache.get(domain)
            if cached is not None:
                age_verdicts[domain] = classify_age(
                    cached,
                    now=observed_at,
                    tier1_days=self._tier1_days,
                    tier2_days=self._tier2_days,
                )
            elif self._cache.miss(domain):
                age_verdicts[domain] = AgeVerdict(
                    domain=domain, tier="unknown", age_days=None, reason="rdap_unavailable"
                )
            elif rdap_base(domain.rsplit(".", 1)[-1].lower()) is None:
                # No RDAP endpoint exists for this TLD — that is permanent,
                # not transient. Retrying forever would starve the queue.
                age_verdicts[domain] = AgeVerdict(
                    domain=domain, tier="unknown", age_days=None, reason="rdap_unsupported"
                )
            else:
                pending_rdap.append(domain)

        if pending_rdap:
            if self._rdap_concurrency > 1:
                registrations = _fetch_registrations_parallel(
                    pending_rdap,
                    self._rdap_fetcher,
                    self._rdap_concurrency,
                )
            else:
                registrations = {
                    domain: self._rdap_fetcher(domain) for domain in pending_rdap
                }
            for domain in pending_rdap:
                reg = registrations[domain]
                if reg is not None:
                    self._cache.put(reg)
                else:
                    self._cache.record_miss(domain)
                age_verdicts[domain] = classify_age(
                    reg,
                    now=observed_at,
                    tier1_days=self._tier1_days,
                    tier2_days=self._tier2_days,
                )

        # S3: DNS — only for domains that pass the age gate. Unknown RDAP
        # degrades to tier2 unless drop_unknown_rdap is set (then unknown
        # domains never reach the DNS layer at all).
        allowed_tiers = ("tier1", "tier2", "unknown")
        if self._drop_unknown_rdap:
            allowed_tiers = ("tier1", "tier2")
        kept = [
            d for d in domains if age_verdicts[d].tier in allowed_tiers
        ]
        dns_results: dict[str, DnsResult] = {}
        if kept:
            if self._dns_concurrency > 1:
                dns_results = _check_dns_parallel(
                    kept, self._dns_checker, self._dns_concurrency
                )
            else:
                dns_results = self._dns_checker(kept)

        decisions: list[FilterDecision] = []
        for domain in domains:
            verdict = age_verdicts[domain]
            if verdict.tier == "drop":
                decisions.append(
                    FilterDecision(
                        domain=domain,
                        candidate=None,
                        retryable=False,
                        reason="rdap_too_old",
                    )
                )
                continue
            if verdict.tier == "unknown" and self._drop_unknown_rdap:
                decisions.append(
                    FilterDecision(
                        domain=domain,
                        candidate=None,
                        retryable=verdict.reason != "rdap_unsupported",
                        reason=verdict.reason,
                    )
                )
                continue
            dns = dns_results.get(domain)
            final_tier = verdict.tier
            if final_tier == "unknown":
                final_tier = "tier2"  # unknown RDAP degrades to tier2
            reason = f"{verdict.reason}"

            if self._require_dns and (dns is None or not dns.has_a):
                decisions.append(
                    FilterDecision(
                        domain=domain,
                        candidate=None,
                        retryable=True,
                        reason="dns_not_ready",
                    )
                )
                continue
            if dns is not None and not dns.has_a:
                reason += " | no DNS"
            candidate = FilteredCandidate(
                domain=domain,
                s1=s1_results[domain],
                s2=verdict,
                s3=dns,
                final_tier=final_tier,
                reason=reason,
                observed_at=observed_at,
            )
            decisions.append(
                FilterDecision(
                    domain=domain,
                    candidate=candidate,
                    retryable=False,
                    reason=reason,
                )
            )

        return tuple(decisions)

    async def run_with_probe(
        self,
        domains: list[str],
        *,
        observed_at: datetime | None = None,
        probe: ProbeCallable | None = None,
    ) -> tuple[FilteredCandidate, ...]:
        """Run the funnel, then probe each survivor with the S4 callback.

        ``probe`` defaults to the constructor's probe. Probing is
        sequential to keep network behavior bounded and polite.
        """
        probe_fn = probe or self._probe
        if probe_fn is None:
            raise ValueError("run_with_probe requires a probe callback")
        observed_at = observed_at or datetime.now(UTC)
        candidates = self.run(domains, observed_at=observed_at)
        probed: list[FilteredCandidate] = []
        for candidate in candidates:
            result = await probe_fn(candidate.domain, observed_at=observed_at)
            probed.append(
                FilteredCandidate(
                    domain=candidate.domain,
                    s1=candidate.s1,
                    s2=candidate.s2,
                    s3=candidate.s3,
                    final_tier=candidate.final_tier,
                    reason=candidate.reason,
                    observed_at=candidate.observed_at,
                    probe=result,
                )
            )
        return tuple(probed)

    def report(self, domains: list[str]) -> dict[str, object]:
        """Run the funnel and summarize layer-by-layer reduction."""
        candidates = self.run(domains)
        kept = len(candidates)
        return {
            "input": len(domains),
            "kept": kept,
            "dropped": len(domains) - kept,
            "tier1": sum(1 for c in candidates if c.final_tier == "tier1"),
            "tier2": sum(1 for c in candidates if c.final_tier == "tier2"),
        }
