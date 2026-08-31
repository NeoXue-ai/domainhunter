"""Outbound target validation shared by HTTP and browser clients."""

import socket
from ipaddress import IPv4Address, IPv6Address, ip_address


class BlockedNetworkTarget(ValueError):
    """Raised when a resolved address is not safe for public web crawling."""


def validate_public_addresses(addresses: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Accept only globally routable IPv4/IPv6 addresses.

    DNS resolution and redirect handling must call this function for every
    resolved target, not only for the initial URL.
    """
    validated: list[str] = []
    for raw_address in addresses:
        try:
            address = ip_address(raw_address)
        except ValueError as error:
            raise BlockedNetworkTarget(f"invalid network target: {raw_address}") from error

        if not isinstance(address, (IPv4Address, IPv6Address)) or not address.is_global:
            raise BlockedNetworkTarget(f"non-public network target: {raw_address}")
        validated.append(str(address))
    return tuple(validated)


def resolve_public_addresses(hostname: str) -> tuple[str, ...]:
    """Resolve ``hostname`` via the OS resolver and keep only globally routable answers.

    Every address returned by :func:`socket.getaddrinfo` is forwarded through
    :func:`validate_public_addresses`. If the lookup fails or any resolved
    address is non-public, ``BlockedNetworkTarget`` is raised so callers can
    record ``blocked_ssrf`` without ever opening a socket.
    """
    try:
        results = socket.getaddrinfo(hostname, None)
    except socket.gaierror as error:
        raise BlockedNetworkTarget(f"DNS resolution failed for {hostname}: {error}") from error

    raw_addresses = tuple(dict.fromkeys(entry[4][0] for entry in results))
    public = validate_public_addresses(raw_addresses)
    if not public:
        raise BlockedNetworkTarget(f"no public addresses for {hostname}")
    return public
