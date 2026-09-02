"""Tests for S1 static domain signals."""

from domainhunter.filter.static_signals import (
    batch_registration_flag,
    compute_signals,
    score_domain,
)


def test_random_string_scores_low() -> None:
    score = score_domain("zqxjk4t8xyz.com")
    assert score.score < 0.4


def test_brand_word_scores_high() -> None:
    score = score_domain("cloudagent.ai")
    assert score.score > 0.6


def test_tech_tld_boost() -> None:
    plain = score_domain("somebrand.io")
    assert plain.signals.tech_tld is True
    assert plain.score >= 0.5


def test_spammy_tld_penalty() -> None:
    score = score_domain("bestdeal.xyz")
    assert score.signals.spammy_tld is True
    assert score.score < 0.5


def test_digit_heavy_low() -> None:
    score = score_domain("b3st3st0nline123.com")
    assert score.signals.digit_count >= 2
    assert score.score < 0.4


def test_consonant_run_detected() -> None:
    signals = compute_signals("strnglybrnd.com")
    assert signals.consonant_run is True


def test_vowel_ratio_captured() -> None:
    signals = compute_signals("aeiouyy.com")
    assert signals.vowel_ratio > 0.4


def test_word_match_detected() -> None:
    signals = compute_signals("chatkit.ai")
    assert signals.word_match is True


def test_score_clamped_to_unit_range() -> None:
    for domain in ["zzzzzzzzzzz.xyz", "aixy.io", "perfectcloudstudio.ai"]:
        score = score_domain(domain)
        assert 0.0 <= score.score <= 1.0


def test_batch_registration_flag() -> None:
    domains = [f"rand{k}.xyz" for k in range(12)]
    registrar = {d: "SpamRegistrar Inc" for d in domains}
    day = {d: "2026-08-31" for d in domains}
    flagged = batch_registration_flag(
        domains, registrar_by_domain=registrar, registration_day_by_domain=day
    )
    assert all(flagged[d] for d in domains)


def test_batch_below_threshold_not_flagged() -> None:
    domains = [f"rand{k}.xyz" for k in range(5)]
    registrar = {d: "SpamRegistrar Inc" for d in domains}
    day = {d: "2026-08-31" for d in domains}
    flagged = batch_registration_flag(
        domains, registrar_by_domain=registrar, registration_day_by_domain=day
    )
    assert not any(flagged.values())


def test_missing_metadata_not_flagged() -> None:
    flagged = batch_registration_flag(
        ["somedomain.com"],
        registrar_by_domain={},
        registration_day_by_domain={},
    )
    assert flagged["somedomain.com"] is False