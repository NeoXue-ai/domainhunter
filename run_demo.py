"""Tiny ASGI entry for `uvicorn scripts.run_demo:app` to launch the demo DB."""

from __future__ import annotations

from pathlib import Path

from domainhunter.api import create_app

_DB = Path(__file__).resolve().parent.parent / "demo.db"


class _DemoContactFetcher:
    """Return a stub contact page so outreach can extract a fake redacted email.

    Real outreach is intentionally not wired in the demo — the page below is a
    fixture the console can render to show what a redacted preview looks like.
    """

    def fetch(self, url: str) -> str:
        return (
            "<html><body><main>"
            "<h1>Contact</h1>"
            "<p>For partnership inquiries, email partnerships@example.test.</p>"
            "<p>Press: press@example.test</p>"
            "</main></body></html>"
        )


app = create_app(_DB, contact_fetcher=_DemoContactFetcher())