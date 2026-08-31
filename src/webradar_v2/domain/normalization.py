"""Hostname normalization with Public Suffix List support."""

from dataclasses import dataclass
import ipaddress

from publicsuffix2 import PublicSuffixList


_PUBLIC_SUFFIX_LIST = PublicSuffixList()


class InvalidHostname(ValueError):
    """Raised when a value cannot represent a public hostname."""


@dataclass(frozen=True, slots=True)
class NormalizedHostname:
    """A normalized hostname and its Public Suffix List registrable domain."""

    raw: str
    hostname: str
    registrable_domain: str


def normalize_hostname(raw: str) -> NormalizedHostname:
    """Normalize a public hostname without performing DNS resolution."""
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise InvalidHostname("hostname must be a non-empty, whitespace-free string")

    if raw.startswith("*.") or "://" in raw:
        raise InvalidHostname("wildcards and URLs are not hostnames")

    hostname = raw.rstrip(".").lower()
    if not hostname or "." not in hostname:
        raise InvalidHostname("hostname must contain a domain suffix")

    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        raise InvalidHostname("IP addresses are not public hostnames")

    try:
        hostname = hostname.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise InvalidHostname("hostname is not valid IDNA") from error

    if any(not label or len(label) > 63 for label in hostname.split(".")):
        raise InvalidHostname("hostname contains an invalid label")

    registrable_domain = _PUBLIC_SUFFIX_LIST.get_sld(hostname)
    if not registrable_domain:
        raise InvalidHostname("hostname has no registrable domain")

    return NormalizedHostname(
        raw=raw,
        hostname=hostname,
        registrable_domain=registrable_domain,
    )
