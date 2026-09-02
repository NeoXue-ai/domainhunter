"""Tests for S3 DNS presence layer."""

import socket

from domainhunter.filter.dns_check import check_dns


def test_resolving_domain_has_a(monkeypatch) -> None:
    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    results = check_dns(["example.com"])
    assert results["example.com"].has_a is True
    assert results["example.com"].addresses == ("93.184.216.34",)


def test_nxdomain_has_no_a(monkeypatch) -> None:
    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        raise socket.gaierror(-2, "Name or service not known")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    results = check_dns(["nonexistent-domain-zzz.com"])
    assert results["nonexistent-domain-zzz.com"].has_a is False
    assert results["nonexistent-domain-zzz.com"].addresses == ()


def test_oserror_treated_as_no_a(monkeypatch) -> None:
    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        raise OSError("temporary failure")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    results = check_dns(["timeout-domain.com"])
    assert results["timeout-domain.com"].has_a is False
    assert "temporary failure" in (results["timeout-domain.com"].error or "")


def test_multiple_domains(monkeypatch) -> None:
    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        if host == "good.com":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.2.3.4", 0))]
        raise socket.gaierror(-2, "nxdomain")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    results = check_dns(["good.com", "bad.com"])
    assert results["good.com"].has_a is True
    assert results["bad.com"].has_a is False


def test_normalizes_domain(monkeypatch) -> None:
    def fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        assert host == "example.com"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.2.3.4", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    results = check_dns(["EXAMPLE.COM."])
    assert results["example.com"].has_a is True