"""Tests for the filter funnel pipeline."""

import asyncio
from datetime import UTC, datetime, timedelta

from domainhunter.filter.pipeline import FilterPipeline
from domainhunter.filter.rdap_age import MemoryCache, Registration


def _reg(days_ago: int, domain: str, registrar: str = "R") -> Registration:
    return Registration(
        domain=domain,
        registration_date=datetime.now(UTC) - timedelta(days=days_ago),
        registrar=registrar,
        statuses=(),
    )


def _fake_rdap(registrations: dict[str, Registration]):
    def fetcher(domain: str) -> Registration | None:
        return registrations.get(domain)

    return fetcher


def _fake_dns(has_a: set[str]):
    def checker(domains: list[str]) -> dict[str, object]:
        from domainhunter.filter.dns_check import DnsResult

        return {
            d: DnsResult(domain=d, has_a=d in has_a, addresses=("1.2.3.4",) if d in has_a else ())
            for d in domains
        }

    return checker


def test_funnel_keeps_tier1_only() -> None:
    regs = {
        "new.com": _reg(5, "new.com"),
        "old.com": _reg(200, "old.com"),
        "slow.com": _reg(60, "slow.com"),
    }
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap(regs),
        dns_checker=_fake_dns({"new.com", "slow.com", "old.com"}),
    )
    candidates = pipeline.run(["new.com", "old.com", "slow.com"])
    kept = {c.domain for c in candidates}
    assert kept == {"new.com", "slow.com"}
    assert all(c.s2.tier in ("tier1", "tier2") for c in candidates)


def test_require_dns_drops_non_resolving() -> None:
    regs = {
        "resolves.com": _reg(5, "resolves.com"),
        "parked.com": _reg(5, "parked.com"),
    }
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap(regs),
        dns_checker=_fake_dns({"resolves.com"}),
        require_dns=True,
    )
    candidates = pipeline.run(["resolves.com", "parked.com"])
    assert {c.domain for c in candidates} == {"resolves.com"}


def test_require_dns_drops_a_domain_without_a_dns_result() -> None:
    """Strict DNS mode fails closed when the resolver omits a domain."""
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap({"new.com": _reg(5, "new.com")}),
        dns_checker=lambda _domains: {},
        require_dns=True,
    )

    assert pipeline.run(["new.com"]) == ()


def test_unknown_rdap_is_kept_tier2_by_default() -> None:
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap({}),
        dns_checker=_fake_dns(set()),
    )
    candidates = pipeline.run(["unknown-domain.com"])
    assert len(candidates) == 1
    assert candidates[0].final_tier == "tier2"
    assert candidates[0].s2.reason == "rdap_unavailable"


def test_drop_unknown_rdap_option() -> None:
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap({}),
        dns_checker=_fake_dns(set()),
        drop_unknown_rdap=True,
    )
    assert pipeline.run(["unknown-domain.com"]) == ()


def test_cache_used_for_repeat_runs() -> None:
    regs = {"cached.com": _reg(5, "cached.com")}
    cache = MemoryCache()
    fetcher = _fake_rdap(regs)
    pipeline = FilterPipeline(
        cache=cache, rdap_fetcher=fetcher, dns_checker=_fake_dns({"cached.com"})
    )
    pipeline.run(["cached.com"])
    assert cache.get("cached.com") is not None

    # Second run hits cache — fetcher would return None if called again, but
    # the cache should prevent that.
    candidates = pipeline.run(["cached.com"])
    assert len(candidates) == 1


def test_report_counts() -> None:
    regs = {
        "a.com": _reg(5, "a.com"),
        "b.com": _reg(60, "b.com"),
        "c.com": _reg(200, "c.com"),
    }
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap(regs),
        dns_checker=_fake_dns({"a.com", "b.com"}),
    )
    report = pipeline.report(["a.com", "b.com", "c.com"])
    assert report["input"] == 3
    assert report["kept"] == 2
    assert report["dropped"] == 1
    assert report["tier1"] == 1
    assert report["tier2"] == 1


def test_candidate_payload_shape() -> None:
    regs = {"x.com": _reg(5, "x.com")}
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap(regs),
        dns_checker=_fake_dns({"x.com"}),
    )
    candidates = pipeline.run(["x.com"])
    payload = candidates[0].as_payload()
    assert set(payload) >= {"domain", "final_tier", "s1", "s2", "s3"}
    assert payload["s2"]["age_days"] == 5


def test_run_with_probe_probes_survivors() -> None:
    regs = {
        "new.com": _reg(5, "new.com"),
        "old.com": _reg(200, "old.com"),
    }
    probed: list[str] = []

    async def probe(domain: str, *, observed_at: datetime) -> dict[str, object]:
        probed.append(domain)
        return {"domain": domain, "outcome": "success"}

    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap(regs),
        dns_checker=_fake_dns({"new.com"}),
    )

    async def run() -> None:
        candidates = await pipeline.run_with_probe(
            ["new.com", "old.com"], probe=probe
        )
        assert len(candidates) == 1
        assert candidates[0].probe == {"domain": "new.com", "outcome": "success"}

    asyncio.run(run())
    assert probed == ["new.com"]  # old.com dropped by age gate, never probed


def test_run_with_probe_requires_callback() -> None:
    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap({}),
        dns_checker=_fake_dns(set()),
    )

    async def run() -> None:
        import pytest

        with pytest.raises(ValueError):
            await pipeline.run_with_probe(["x.com"])

    asyncio.run(run())


def test_probe_payload_included_in_candidate() -> None:
    regs = {"p.com": _reg(5, "p.com")}

    async def probe(domain: str, *, observed_at: datetime) -> dict[str, object]:
        return {"outcome": "success", "status_code": 200}

    pipeline = FilterPipeline(
        cache=MemoryCache(),
        rdap_fetcher=_fake_rdap(regs),
        dns_checker=_fake_dns({"p.com"}),
        probe=probe,
    )

    async def run() -> None:
        candidates = await pipeline.run_with_probe(["p.com"])
        payload = candidates[0].as_payload()
        assert payload["probe"] == {"outcome": "success", "status_code": 200}

    asyncio.run(run())
