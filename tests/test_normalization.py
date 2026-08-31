import pytest

from webradar_v2.domain.normalization import InvalidHostname, normalize_hostname


@pytest.mark.parametrize(
    ("raw", "hostname", "registrable_domain"),
    [
        ("WWW.Example.COM.", "www.example.com", "example.com"),
        ("app.example.co.uk", "app.example.co.uk", "example.co.uk"),
        ("bücher.example.com", "xn--bcher-kva.example.com", "example.com"),
    ],
)
def test_normalizes_hostname_and_registrable_domain(
    raw: str, hostname: str, registrable_domain: str
) -> None:
    result = normalize_hostname(raw)
    assert result.hostname == hostname
    assert result.registrable_domain == registrable_domain


@pytest.mark.parametrize("raw", ["localhost", "127.0.0.1", "*.example.com", "bad host"])
def test_rejects_non_public_hostnames(raw: str) -> None:
    with pytest.raises(InvalidHostname):
        normalize_hostname(raw)
