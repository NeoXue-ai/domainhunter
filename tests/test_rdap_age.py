"""Tests for S2 RDAP registration-age layer."""

from datetime import UTC, datetime, timedelta

from domainhunter.filter.rdap_age import (
    MemoryCache,
    Registration,
    classify_age,
    rdap_base,
)


def _reg(days_ago: int, domain: str = "example.com") -> Registration:
    return Registration(
        domain=domain,
        registration_date=datetime.now(UTC) - timedelta(days=days_ago),
        registrar="Some Registrar",
        statuses=("clientTransferProhibited",),
    )


def test_tier1_within_30_days() -> None:
    verdict = classify_age(_reg(5), domain="example.com")
    assert verdict.tier == "tier1"
    assert verdict.age_days == 5
    assert verdict.registration is not None


def test_tier1_boundary_30_days() -> None:
    verdict = classify_age(_reg(30), domain="example.com")
    assert verdict.tier == "tier1"


def test_tier2_slow_starter() -> None:
    verdict = classify_age(_reg(60), domain="example.com")
    assert verdict.tier == "tier2"
    assert "slow starter" in verdict.reason


def test_tier2_boundary_90_days() -> None:
    verdict = classify_age(_reg(90), domain="example.com")
    assert verdict.tier == "tier2"


def test_drop_older_than_90_days() -> None:
    verdict = classify_age(_reg(200), domain="example.com")
    assert verdict.tier == "drop"
    assert verdict.age_days == 200


def test_unknown_when_no_registration() -> None:
    verdict = classify_age(None, domain="example.com")
    assert verdict.tier == "unknown"
    assert verdict.age_days is None


def test_future_registration_is_unknown() -> None:
    now = datetime.now(UTC)
    reg = Registration(
        domain="x.com",
        registration_date=now + timedelta(days=1),
        registrar="R",
        statuses=(),
    )
    verdict = classify_age(reg, now=now)
    assert verdict.tier == "unknown"


def test_custom_thresholds() -> None:
    verdict = classify_age(_reg(45), tier1_days=90, tier2_days=180, domain="x.com")
    assert verdict.tier == "tier1"


def test_age_days_property() -> None:
    reg = _reg(10)
    assert 9 <= reg.age_days <= 11


def test_rdap_base_verisign() -> None:
    assert rdap_base("com") == "https://rdap.verisign.com/com/v1/domain/"


def test_rdap_base_other() -> None:
    assert rdap_base("ai") == "https://rdap.nic.ai/domain/"


def test_rdap_base_unknown_tld() -> None:
    assert rdap_base("zzzz") is None


def test_memory_cache() -> None:
    cache = MemoryCache()
    reg = _reg(5)
    assert cache.get("example.com") is None
    assert cache.miss("example.com") is False

    cache.record_miss("example.com")
    assert cache.miss("example.com") is True

    cache.put(reg)
    assert cache.get("example.com") is reg
    assert cache.miss("example.com") is False