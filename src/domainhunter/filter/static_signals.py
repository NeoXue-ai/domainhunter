"""S1 static signals: domain-name shape scoring for the filter layer.

Cheap, zero-network scoring of a registrable domain's name itself.
The goal is not to decide anything by itself, but to separate the
"random-string batch-registration" noise (phishing farms, SEO spam)
from brand-like names before the RDAP and DNS layers run.

Scoring is additive, clamped to ``[0, 1]``, and each signal is exposed
so the review console can show *why* a domain scored the way it did.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# TLDs that correlate with small/indie tech products.
TECH_TLDS = frozenset(
    {"ai", "io", "dev", "app", "tech", "co", "me", "software", "digital", "cloud"}
)

# TLDs that correlate with bulk registration / SEO spam.
SPAMMY_TLDS = frozenset({"xyz", "top", "club", "icu", "buzz", "click", "link", "site"})

_VOWELS = frozenset("aeiouy")
_DIGIT_RE = re.compile(r"\d")
_HYPHEN_RE = re.compile(r"-")
_CONSONANT_RUN_RE = re.compile(r"[bcdfghjklmnpqrstvwxz]{4,}")
_WORDLIST: frozenset[str] = frozenset(
    {
        "app", "ai", "gpt", "chat", "agent", "automation", "cloud", "data", "digital",
        "online", "tech", "smart", "intelligence", "learning", "bot", "assistant",
        "studio", "labs", "lab", "soft", "software", "system", "solutions", "service",
        "media", "network", "web", "mobile", "social", "shop", "store", "market",
        "design", "creative", "analytics", "insight", "platform", "engine", "core",
        "nexus", "pixel", "nova", "quantum", "vertex", "vector", "lattice", "orbit",
        "bloom", "forge", "spark", "craft", "works", "haus", "hub", "kit", "stack",
    }
)


@dataclass(frozen=True, slots=True)
class DomainSignals:
    """Shape signals for one registrable domain label (the apex label)."""

    label: str
    tld: str
    length: int
    digit_count: int
    hyphen_count: int
    consonant_run: bool
    vowel_ratio: float
    word_match: bool
    tech_tld: bool
    spammy_tld: bool


@dataclass(frozen=True, slots=True)
class DomainScore:
    """S1 score plus the per-signal breakdown for the review console."""

    domain: str
    score: float
    signals: DomainSignals

    def as_payload(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "score": self.score,
            "signals": {
                "label": self.signals.label,
                "tld": self.signals.tld,
                "length": self.signals.length,
                "digits": self.signals.digit_count,
                "hyphens": self.signals.hyphen_count,
                "consonant_run": self.signals.consonant_run,
                "vowel_ratio": self.signals.vowel_ratio,
                "word_match": self.signals.word_match,
                "tech_tld": self.signals.tech_tld,
                "spammy_tld": self.signals.spammy_tld,
            },
        }


def _label_of(domain: str) -> tuple[str, str]:
    """Return (apex label, tld) for a registrable domain."""
    parts = domain.rstrip(".").lower().split(".")
    label = parts[0]
    tld = parts[-1]
    return label, tld


def compute_signals(domain: str) -> DomainSignals:
    """Compute the shape signals for a registrable domain."""
    label, tld = _label_of(domain)
    cleaned = label.replace("-", "")
    length = len(cleaned)
    digits = sum(1 for ch in cleaned if ch.isdigit())
    hyphens = label.count("-")
    consonant_run = bool(_CONSONANT_RUN_RE.search(cleaned))
    if length == 0:
        vowel_ratio = 0.0
    else:
        vowels = sum(1 for ch in cleaned if ch in _VOWELS)
        vowel_ratio = vowels / length
    word_match = any(word in cleaned for word in _WORDLIST)
    return DomainSignals(
        label=label,
        tld=tld,
        length=length,
        digit_count=digits,
        hyphen_count=hyphens,
        consonant_run=consonant_run,
        vowel_ratio=vowel_ratio,
        word_match=word_match,
        tech_tld=tld in TECH_TLDS,
        spammy_tld=tld in SPAMMY_TLDS,
    )


def score_domain(domain: str) -> DomainScore:
    """Score one registrable domain, returning the breakdown payload too."""
    signals = compute_signals(domain)
    score = 0.5  # neutral baseline

    # Length: brand-like domains are 6-12 chars; very short or very long
    # labels lean toward bulk-registered junk.
    if signals.length >= 8 and signals.length <= 14:
        score += 0.15
    elif signals.length >= 6:
        score += 0.05
    else:
        score -= 0.2

    # Brand words are the strongest positive signal.
    if signals.word_match:
        score += 0.3

    if signals.tech_tld:
        score += 0.1
    if signals.spammy_tld:
        score -= 0.2

    # Random-string fingerprints.
    if signals.digit_count >= 2:
        score -= 0.2
    elif signals.digit_count == 1:
        score -= 0.05
    if signals.hyphen_count >= 2:
        score -= 0.15
    elif signals.hyphen_count == 1:
        score -= 0.05
    if signals.consonant_run:
        score -= 0.2
    if signals.vowel_ratio < 0.25:
        score -= 0.15
    elif signals.vowel_ratio > 0.45:
        score += 0.05

    return DomainScore(
        domain=domain, score=max(0.0, min(1.0, score)), signals=signals
    )


def batch_registration_flag(
    domains: list[str],
    *,
    registrar_by_domain: dict[str, str],
    registration_day_by_domain: dict[str, str],
    min_batch_size: int = 10,
) -> dict[str, bool]:
    """Flag domains that look like part of a bulk registration run.

    A domain is flagged when, for its (registrar, registration day),
    at least ``min_batch_size`` other domains in the same batch share
    the same registrar and the same registration day. This catches the
    "one registrar, one day, hundreds of similar names" pattern typical
    of phishing farms and SEO spam.

    Args:
        domains: The candidate registrable domains.
        registrar_by_domain: RDAP registrar name keyed by domain.
        registration_day_by_domain: ``YYYY-MM-DD`` registration date keyed by domain.
        min_batch_size: Batch size threshold to consider it a bulk run.
    """
    counts: dict[tuple[str, str], int] = {}
    for domain in domains:
        registrar = registrar_by_domain.get(domain)
        day = registration_day_by_domain.get(domain)
        if registrar and day:
            counts[(registrar, day)] = counts.get((registrar, day), 0) + 1

    flagged: dict[str, bool] = {}
    for domain in domains:
        registrar = registrar_by_domain.get(domain)
        day = registration_day_by_domain.get(domain)
        if registrar and day:
            flagged[domain] = counts.get((registrar, day), 0) >= min_batch_size
        else:
            flagged[domain] = False
    return flagged