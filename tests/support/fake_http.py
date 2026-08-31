"""In-process HTTP fixture server for spec §15 acceptance tests.

The server binds to ``127.0.0.1`` on a random port and serves a small,
declarative route table. Tests compose routes that exercise the same code
paths the real public crawler sees — HTTPS-style request bodies, redirects,
``robots.txt``, 5xx responses, oversized payloads, and metadata redirects —
without ever leaving the test process.

Companion resolver
------------------

Because :mod:`webradar_v2.crawler.http_probe` calls a resolver before
issuing any request, the server also exposes
:meth:`FakeHTTPServer.public_resolver` — an awaitable that maps every
hostname to ``127.0.0.1:<bound_port>`` so callers can drive the real
``HTTPProbe`` against this fake origin. Pinned transport then routes the
TCP connect to the loopback IP, and the test stays in-process.
"""

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from typing import Any
import urllib.parse


@dataclass(frozen=True, slots=True)
class _Route:
    """Declarative description of one HTTP route the fake server will serve."""

    path: str
    status: int = 200
    headers: tuple[tuple[str, str], ...] = ()
    body: bytes = b""
    content_type: str | None = None

    def render(self) -> tuple[int, list[tuple[str, str]], bytes]:
        headers = list(self.headers)
        if self.content_type is not None:
            headers.append(("Content-Type", self.content_type))
        headers.append(("Content-Length", str(len(self.body))))
        return self.status, headers, self.body


def html_route(path: str, html: str, *, status: int = 200) -> _Route:
    """Helper for serving a fixed HTML body."""
    return _Route(path=path, status=status, body=html.encode("utf-8"), content_type="text/html; charset=utf-8")


def text_route(path: str, text: str, *, status: int = 200) -> _Route:
    return _Route(path=path, status=status, body=text.encode("utf-8"), content_type="text/plain; charset=utf-8")


def redirect_route(path: str, location: str, *, status: int = 302) -> _Route:
    headers = (("Location", location),)
    return _Route(path=path, status=status, headers=headers, body=b"")


def huge_route(path: str, *, body_size: int) -> _Route:
    """Serve a deterministic ``body_size`` byte payload.

    The byte pattern is ``b'A' * 4096`` repeated to keep the response cheap
    to generate and easy to assert against.
    """
    chunk = b"A" * 4096
    repeats, remainder = divmod(body_size, len(chunk))
    body = chunk * repeats + chunk[:remainder]
    return _Route(
        path=path,
        status=200,
        body=body,
        content_type="application/octet-stream",
    )


@dataclass(slots=True)
class _State:
    routes: dict[str, _Route] = field(default_factory=dict)
    hits: list[str] = field(default_factory=list)


class FakeHTTPServer:
    """A small, single-process HTTP fixture server bound to 127.0.0.1."""

    def __init__(self) -> None:
        self._state = _State()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._host: str = "127.0.0.1"
        self._port: int = 0

    # --- route registration ----------------------------------------------------

    def add_route(self, path: str, *, status: int = 200, headers: Iterable[tuple[str, str]] = (), body: bytes = b"", content_type: str | None = None) -> _Route:
        route = _Route(path=path, status=status, headers=tuple(headers), body=body, content_type=content_type)
        self._state.routes[path] = route
        return route

    def add_redirect(self, path: str, location: str, *, status: int = 302) -> _Route:
        route = redirect_route(path, location, status=status)
        self._state.routes[path] = route
        return route

    def add_html(self, path: str, html: str, *, status: int = 200) -> _Route:
        route = html_route(path, html, status=status)
        self._state.routes[path] = route
        return route

    def add_text(self, path: str, text: str, *, status: int = 200) -> _Route:
        route = text_route(path, text, status=status)
        self._state.routes[path] = route
        return route

    def add_huge(self, path: str, *, body_size: int) -> _Route:
        route = huge_route(path, body_size=body_size)
        self._state.routes[path] = route
        return route

    def add_metadata_redirect(self, path: str, *, host: str = "169.254.169.254") -> _Route:
        return self.add_redirect(path, f"http://{host}/latest/meta-data/")

    # --- lifecycle -------------------------------------------------------------

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def base_url(self) -> str:
        return f"http://{self._host}:{self._port}"

    def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("FakeHTTPServer is already running; call stop() first")

        state = self._state
        outer_self = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
                state.hits.append(self.path)
                parsed = urllib.parse.urlsplit(self.path)
                route = state.routes.get(parsed.path)
                if route is None:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                status, headers, body = route.render()
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                # Silence the default stderr access log to keep pytest output clean.
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, name="fake-http", daemon=True)
        thread.start()

        self._server = server
        self._thread = thread
        self._port = server.server_address[1]
        outer_self._host = "127.0.0.1"

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None

    # --- observation -----------------------------------------------------------

    @property
    def hits(self) -> tuple[str, ...]:
        return tuple(self._state.hits)

    def reset_hits(self) -> None:
        self._state.hits.clear()

    # --- mock transport bridge -------------------------------------------------

    def as_httpx_handler(self) -> Callable[..., Any]:
        """Return an ``httpx``-compatible request handler driven by the route table.

        The real :class:`HTTPProbe` refuses to connect to ``127.0.0.1`` for
        SSRF reasons, so the live ``FakeHTTPServer`` cannot be exercised
        through the probe. For end-to-end probe tests, mount the same route
        table behind :class:`httpx.MockTransport` so the probe exercises
        the full code path (analysis, redirects, robots, oversized bodies)
        without performing a real socket connect.
        """
        import httpx as _httpx  # local import to keep module import cheap

        state = self._state

        def handler(request: _httpx.Request) -> _httpx.Response:
            path = request.url.path
            state.hits.append(path)
            route = state.routes.get(path)
            if route is None:
                return _httpx.Response(404, content=b"")
            status, headers, body = route.render()
            response_headers = {name: value for name, value in headers}
            # httpx computes its own Content-Length; do not pass it twice.
            response_headers.pop("Content-Length", None)
            return _httpx.Response(status, headers=response_headers, content=body)

        return handler

    # --- companion resolver ----------------------------------------------------

    def public_resolver(self) -> Callable[[str], Awaitable[tuple[str, ...]]]:
        """Return an awaitable resolver that maps every hostname to this loopback IP."""
        ip = "127.0.0.1"

        async def resolve(hostname: str) -> tuple[str, ...]:
            return (ip,)

        return resolve


# --- pytest fixture-style helpers --------------------------------------------------


def make_route_table(*routes: _Route) -> dict[str, _Route]:
    """Build a route table from declarative route objects (useful in tests)."""
    return {route.path: route for route in routes}