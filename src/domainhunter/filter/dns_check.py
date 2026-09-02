"""S3 DNS presence layer: is the domain actually resolving?

Cheap one-shot DNS check per domain. The signal is binary-ish:
a registered domain that does not resolve at all is likely parked,
squatted, or a placeholder — not a live newborn site.

Uses only the standard library (``socket.getaddrinfo``) so the filter
layer stays dependency-free. DNS failures (NXDOMAIN, timeouts) are
treated as "no A record" rather than errors, because for our purpose
a non-resolving domain is exactly the signal we want to surface.
"""

from __future__ import annotations

import socket
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DnsResult:
    """DNS presence result for one domain."""

    domain: str
    has_a: bool
    addresses: tuple[str, ...]
    error: str | None = None

    def as_payload(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "has_a": self.has_a,
            "addresses": list(self.addresses),
            "error": self.error,
        }


def check_dns(
    domains: Iterable[str],
    *,
    timeout: float = 5.0,
    family: int = socket.AF_UNSPEC,
) -> dict[str, DnsResult]:
    """Resolve A/AAAA records for each domain. One shot per domain."""
    results: dict[str, DnsResult] = {}
    for domain in domains:
        domain = domain.rstrip(".").lower()
        try:
            infos = socket.getaddrinfo(
                domain, None, family=family, type=socket.SOCK_STREAM
            )
            addresses = tuple(
                sorted({info[4][0] for info in infos})
            )
            results[domain] = DnsResult(
                domain=domain, has_a=True, addresses=addresses
            )
        except socket.gaierror as exc:
            results[domain] = DnsResult(
                domain=domain, has_a=False, addresses=(), error=str(exc)
            )
        except OSError as exc:
            results[domain] = DnsResult(
                domain=domain, has_a=False, addresses=(), error=str(exc)
            )
    return results