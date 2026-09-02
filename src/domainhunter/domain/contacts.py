"""Minimal extraction of public contact addresses with redacted page evidence."""

from dataclasses import dataclass
import re
from urllib.parse import urlparse


_EMAIL_PATTERN = re.compile(r"(?<![\w.+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}(?![\w.-])", re.I)


@dataclass(frozen=True, slots=True)
class PublicContact:
    """A public address held only for an approved, manual contact workflow."""

    address: str
    redacted_address: str
    source_url: str


@dataclass(frozen=True, slots=True)
class ContactExtraction:
    contacts: tuple[PublicContact, ...]
    redacted_html: str


def _redact(address: str) -> str:
    local, domain = address.split("@", maxsplit=1)
    return f"{local[0]}{'*' * (len(local) - 1)}@{domain}"


def extract_public_contacts(html: str, *, source_url: str) -> ContactExtraction:
    """Extract unique public email addresses while returning safe-to-store redacted HTML."""
    parsed = urlparse(source_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("source_url must be an HTTP(S) URL")
    found: list[str] = []
    for match in _EMAIL_PATTERN.finditer(html):
        address = match.group(0).lower()
        if address not in found:
            found.append(address)
    contacts = tuple(
        PublicContact(address=address, redacted_address=_redact(address), source_url=source_url)
        for address in found
    )
    redacted = _EMAIL_PATTERN.sub(lambda match: _redact(match.group(0).lower()), html)
    return ContactExtraction(contacts=contacts, redacted_html=redacted)
