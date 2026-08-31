"""Self-tests for the in-process FakeHTTPServer fixture."""

import urllib.request

import pytest

from tests.support.fake_http import FakeHTTPServer


@pytest.fixture
def server() -> FakeHTTPServer:
    server = FakeHTTPServer()
    server.start()
    try:
        yield server
    finally:
        server.stop()


def test_serves_html_route(server: FakeHTTPServer) -> None:
    server.add_html("/", "<html><title>Hi</title></html>")

    response = urllib.request.urlopen(server.base_url + "/")

    assert response.status == 200
    assert response.read().decode("utf-8") == "<html><title>Hi</title></html>"


def test_serves_text_route(server: FakeHTTPServer) -> None:
    server.add_text("/robots.txt", "User-agent: *\nDisallow: /private\n")

    response = urllib.request.urlopen(server.base_url + "/robots.txt")

    assert response.status == 200
    assert b"Disallow: /private" in response.read()


def test_serves_redirect_with_location_header(server: FakeHTTPServer) -> None:
    server.add_redirect("/redirect", "/about")
    server.add_html("/about", "<title>About</title>")

    # urllib follows redirects by default; assert manually via a raw request.
    request = urllib.request.Request(server.base_url + "/redirect", method="GET")
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
    response = opener.open(request)

    assert response.status == 200
    assert response.url.endswith("/about")
    assert b"<title>About</title>" in response.read()


def test_serves_5xx_status(server: FakeHTTPServer) -> None:
    server.add_html("/oops", "<p>boom</p>", status=503)

    try:
        urllib.request.urlopen(server.base_url + "/oops")
    except urllib.error.HTTPError as error:
        assert error.code == 503
        assert error.read() == b"<p>boom</p>"
    else:
        pytest.fail("expected urllib to raise HTTPError on 5xx")


def test_serves_huge_route_with_exact_size(server: FakeHTTPServer) -> None:
    server.add_huge("/huge", body_size=10_485_760)  # 10 MiB

    response = urllib.request.urlopen(server.base_url + "/huge")

    body = response.read()
    assert len(body) == 10_485_760
    assert body[:4] == b"AAAA"


def test_unknown_path_returns_404(server: FakeHTTPServer) -> None:
    try:
        urllib.request.urlopen(server.base_url + "/missing")
    except urllib.error.HTTPError as error:
        assert error.code == 404
    else:
        pytest.fail("expected urllib to raise HTTPError on missing route")


def test_hits_record_request_paths(server: FakeHTTPServer) -> None:
    server.add_html("/", "<p>root</p>")
    server.add_html("/about", "<p>about</p>")

    urllib.request.urlopen(server.base_url + "/")
    urllib.request.urlopen(server.base_url + "/about")

    assert server.hits == ("/", "/about")


def test_resolver_returns_loopback(server: FakeHTTPServer) -> None:
    import asyncio

    resolver = server.public_resolver()

    async def run() -> str:
        return (await resolver("anything.example.com"))[0]

    assert asyncio.run(run()) == "127.0.0.1"


def test_metadata_redirect_target_resolves_to_private_ip(server: FakeHTTPServer) -> None:
    server.add_metadata_redirect("/metadata-redirect")

    request = urllib.request.Request(server.base_url + "/metadata-redirect", method="GET")
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
    try:
        opener.open(request)
    except Exception:
        # Either OSError (loopback) or HTTPError (no route at the IP target) is fine —
        # what matters is that the fake server hands out the redirect to 169.254.x.
        pass

    # The route was hit; the Location header is preserved by BaseHTTPRequestHandler.
    assert any(hit == "/metadata-redirect" for hit in server.hits)
    location = server._state.routes["/metadata-redirect"].headers[0][1]
    assert location.startswith("http://169.254.169.254/")


def test_double_start_raises() -> None:
    server = FakeHTTPServer()
    server.start()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            server.start()
    finally:
        server.stop()