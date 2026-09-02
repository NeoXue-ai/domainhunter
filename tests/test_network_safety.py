import socket

import pytest

from domainhunter.network_safety import (
    BlockedNetworkTarget,
    resolve_public_addresses,
    validate_public_addresses,
)


def test_accepts_global_ipv4_and_ipv6_addresses() -> None:
    addresses = validate_public_addresses(["1.1.1.1", "2606:4700:4700::1111"])

    assert addresses == ("1.1.1.1", "2606:4700:4700::1111")


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "192.168.1.1",
        "::1",
        "fe80::1",
        "fc00::1",
    ],
)
def test_rejects_private_and_link_local_addresses(address: str) -> None:
    with pytest.raises(BlockedNetworkTarget):
        validate_public_addresses([address])


def test_rejects_mixed_public_and_private_resolution() -> None:
    with pytest.raises(BlockedNetworkTarget):
        validate_public_addresses(["1.1.1.1", "127.0.0.1"])


def test_rejects_invalid_address_values() -> None:
    with pytest.raises(BlockedNetworkTarget):
        validate_public_addresses(["not-an-ip"])


def test_resolve_public_addresses_returns_only_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All-public answers are returned, deduplicated, in resolver order."""

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        return [
            (socket.AF_INET, None, None, None, ("1.1.1.1", 0)),
            (socket.AF_INET, None, None, None, ("9.9.9.9", 0)),
            (socket.AF_INET, None, None, None, ("1.1.1.1", 0)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    assert resolve_public_addresses("example.com") == ("1.1.1.1", "9.9.9.9")


def test_resolve_public_addresses_rejects_metadata_when_dns_returns_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolver that returns 169.254.169.254 must raise BlockedNetworkTarget."""

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        return [
            (socket.AF_INET, None, None, None, ("169.254.169.254", 0)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(BlockedNetworkTarget):
        resolve_public_addresses("example.com")


def test_resolve_public_addresses_rejects_invalid_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolver that fails raises BlockedNetworkTarget."""

    def fake_getaddrinfo(hostname: str, *args: object, **kwargs: object) -> list[tuple]:
        raise socket.gaierror(-2, "name or service not known")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(BlockedNetworkTarget):
        resolve_public_addresses("example.invalid")
