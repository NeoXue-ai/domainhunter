"""S2 RDAP registration-date layer for the filter pipeline.

Queries RDAP (the modern WHOIS replacement) for a registrable domain's
registration date, registrar, and status. Pure HTTP, no API key. This
is the core "is it actually a new domain?" decision: a domain whose
registration is within the freshness window is a real newborn; older
registrations are dropped (or downgraded) regardless of how fresh the
certificate was.

Rate limits are real (Verisign et al.), so callers should route
queries through a small cache + retry queue. A ``Cache`` protocol is
provided so the pipeline can persist answers across runs.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

# Verisign RDAP serves these TLDs.
_VERISIGN_TLDS = frozenset(
    {"com", "net", "org", "tv", "cc", "name", "biz", "info"}
)

_TLD_RDAP: dict[str, str] = {
    "ai": "https://rdap.nic.ai/domain/",
    "uk": "https://rdap.nominet.uk/domain/",
    "de": "https://rdap.denic.de/domain/",
    "fr": "https://rdap.nic.fr/domain/",
    "dk": "https://rdap.dk-hostmaster.dk/domain/",
    "za": "https://rdap.registry.net.za/domain/",
    "ar": "https://rdap.nic.ar/domain/",
    "ca": "https://rdap.ca-central-1.identitydigital.services/domain/",
    "br": "https://rdap.registro.br/domain/",
    "cl": "https://rdap.nic.cl/domain/",
    "eu": "https://rdap.eu/domain/",
    "ch": "https://rdap.nic.ch/domain/",
    "tr": "https://rdap.trabis.gov.tr/domain/",
    "ru": "https://rdap.tcinet.ru/domain/",
    "pl": "https://rdap.dns.pl/domain/",
    "mm": "https://rdap.nic.mm/domain/",
    "ge": "https://rdap.registry.ge/domain/",
    "nl": "https://rdap.sidn.nl/domain/",
    "au": "https://rdap.auda.org.au/domain/",
    "online": "https://rdap.centralnic.com/domain/",
    "xyz": "https://rdap.centralnic.com/domain/",
    "site": "https://rdap.centralnic.com/domain/",
    "shop": "https://rdap.centralnic.com/domain/",
    "cloud": "https://rdap.centralnic.com/domain/",
    "dev": "https://rdap.nic.dev/domain/",
    "app": "https://rdap.nic.app/domain/",
    "io": "https://rdap.identitydigital.services/domain/",
    "co": "https://rdap.identitydigital.services/domain/",
    "us": "https://rdap.identitydigital.services/domain/",
    "me": "https://rdap.identitydigital.services/domain/",
    "pro": "https://rdap.identitydigital.services/domain/",
}


@dataclass(frozen=True, slots=True)
class Registration:
    """RDAP registration facts for one domain."""

    domain: str
    registration_date: datetime
    registrar: str
    statuses: tuple[str, ...]

    @property
    def age_days(self) -> int:
        return (datetime.now(UTC) - self.registration_date).days


class RegistrationCache(Protocol):
    def get(self, domain: str) -> Registration | None: ...
    def put(self, registration: Registration) -> None: ...
    def miss(self, domain: str) -> bool: ...


class MemoryCache:
    """Thread/process-local cache with explicit miss tracking."""

    def __init__(self) -> None:
        self._data: dict[str, Registration] = {}
        self._misses: set[str] = set()

    def get(self, domain: str) -> Registration | None:
        return self._data.get(domain)

    def put(self, registration: Registration) -> None:
        self._data[registration.domain] = registration
        self._misses.discard(registration.domain)

    def miss(self, domain: str) -> bool:
        return domain in self._misses

    def record_miss(self, domain: str) -> None:
        self._misses.add(domain)


def rdap_base(tld: str) -> str | None:
    if tld in _VERISIGN_TLDS:
        return f"https://rdap.verisign.com/{tld}/v1/domain/"
    return _TLD_RDAP.get(tld)


def _find_registrar(entities: list[dict]) -> str:
    for entity in entities:
        roles = entity.get("roles", [])
        if "registrar" in roles:
            vcards = entity.get("vcardArray", [])
            if len(vcards) >= 2:
                for item in vcards[1]:
                    if item and item[0] == "fn" and len(item) >= 3:
                        return str(item[3])
    return "unknown"


def fetch_registration(
    domain: str, *, timeout: float = 10.0, user_agent: str = "domainhunter-filter/1.0"
) -> Registration | None:
    """Query RDAP for one registrable domain. Returns None on failure."""
    tld = domain.rsplit(".", 1)[-1].lower()
    base = rdap_base(tld)
    if base is None:
        return None
    try:
        req = urllib.request.Request(
            base + domain, headers={"User-Agent": user_agent}
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            data = json.load(response)
    except Exception:  # noqa: BLE001 - network/parse errors are heterogeneous
        return None

    registration_ts: datetime | None = None
    for event in data.get("events", []):
        if event.get("eventAction") == "registration":
            try:
                parsed = datetime.fromisoformat(event["eventDate"])
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                registration_ts = parsed
            except (KeyError, ValueError):
                pass
            break
    if registration_ts is None:
        return None

    registrar = _find_registrar(data.get("entities", []))
    statuses = tuple(data.get("status", []))
    return Registration(
        domain=domain,
        registration_date=registration_ts,
        registrar=registrar,
        statuses=statuses,
    )


@dataclass(frozen=True, slots=True)
class AgeVerdict:
    """S2 decision for one domain."""

    domain: str
    tier: str  # "tier1" | "tier2" | "drop" | "unknown"
    age_days: int | None
    reason: str
    registration: Registration | None = None

    def as_payload(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "tier": self.tier,
            "age_days": self.age_days,
            "reason": self.reason,
            "registrar": (
                self.registration.registrar if self.registration else None
            ),
            "registration_date": (
                self.registration.registration_date.isoformat()
                if self.registration
                else None
            ),
        }


def classify_age(
    registration: Registration | None,
    *,
    now: datetime | None = None,
    tier1_days: int = 30,
    tier2_days: int = 90,
    domain: str = "",
) -> AgeVerdict:
    """Classify a registration into the freshness tiers.

    ``tier1``: registered within ``tier1_days`` (true newborns).
    ``tier2``: within ``tier2_days`` (slow starters) or unknown RDAP.
    ``drop``:  older than ``tier2_days``.
    """
    if registration is None:
        return AgeVerdict(
            domain=domain,
            tier="unknown",
            age_days=None,
            reason="rdap_unavailable",
        )
    now = now or datetime.now(UTC)
    age = (now - registration.registration_date).days
    if age < 0:
        return AgeVerdict(
            domain=domain, tier="unknown", age_days=age, reason="future_registration"
        )
    if age <= tier1_days:
        return AgeVerdict(
            domain=domain,
            tier="tier1",
            age_days=age,
            reason=f"registered {age}d ago",
            registration=registration,
        )
    if age <= tier2_days:
        return AgeVerdict(
            domain=domain,
            tier="tier2",
            age_days=age,
            reason=f"registered {age}d ago (slow starter)",
            registration=registration,
        )
    return AgeVerdict(
        domain=domain,
        tier="drop",
        age_days=age,
        reason=f"registered {age}d ago (too old)",
        registration=registration,
    )