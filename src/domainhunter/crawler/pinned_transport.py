"""HTTP transport that pins every connect to a pre-validated IP.

The default :mod:`httpx` transport resolves DNS at connect time, which means a
valid public answer can be swapped for a private one between the call to
``validate_public_addresses`` and the actual TCP connect (a classic DNS rebinding
attack). :class:`PinnedTransport` short-circuits that window by intercepting
``socket.getaddrinfo`` while a request is in flight, returning the IP the
caller already validated.

The mapping is supplied by the caller (typically
:func:`domainhunter.network_safety.resolve_public_addresses`), so the transport
itself stays small and testable.
"""

import socket
from collections.abc import Iterator
from contextlib import contextmanager


class PinnedTransport:
    """Monkey-patch helper that pins ``socket.getaddrinfo`` for one request.

    Usage::

        transport = PinnedTransport({"example.com": "203.0.113.10"})
        with transport.patch():
            async with httpx.AsyncClient(transport=...) as client:
                ...

    The patch is scoped: it disables itself when the ``with`` block exits,
    even if an exception is raised inside the request.
    """

    def __init__(self, address_map: dict[str, str]) -> None:
        if not address_map:
            raise ValueError("address_map must contain at least one pinned hostname")
        self._address_map = dict(address_map)
        self._original = socket.getaddrinfo

    def pinned_address(self, hostname: str) -> str | None:
        return self._address_map.get(hostname)

    @contextmanager
    def patch(self) -> Iterator[None]:
        address_map = self._address_map
        original = self._original

        def pinned_getaddrinfo(
            host: str, *args: object, **kwargs: object
        ) -> list[tuple]:
            pinned = address_map.get(host)
            if pinned is not None:
                # Mirror the canonical shape of getaddrinfo for both families.
                try:
                    family_hint = kwargs.get("family")
                except Exception:
                    family_hint = None
                socktype = kwargs.get("type") or 0
                if isinstance(family_hint, int) and family_hint:
                    family = family_hint
                else:
                    family = socket.AF_INET6 if ":" in pinned else socket.AF_INET
                return [(family, socktype, 0, "", (pinned, 0))]
            return original(host, *args, **kwargs)

        socket.getaddrinfo = pinned_getaddrinfo  # type: ignore[assignment]
        try:
            yield
        finally:
            socket.getaddrinfo = original  # type: ignore[assignment]