"""Pure L1 HTML analysis with no network side effects."""

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse

from domainhunter.domain.observations import OutcomeCode


_PARKING_MARKERS = (
    "domain for sale",
    "buy this domain",
    "premium domain",
    "parking page",
    "under construction",
)
_MAX_INTERNAL_LINKS = 20


@dataclass(frozen=True, slots=True)
class L1Analysis:
    outcome_code: OutcomeCode
    final_url: str
    title: str | None
    meta_description: str | None
    text_length: int
    is_parking_page: bool
    status_code: int
    canonical_url: str | None = None
    internal_links: tuple[str, ...] = ()


class _MetadataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self.meta_description: str | None = None
        self.canonical_href: str | None = None
        self.link_hrefs: list[str] = []
        self._ignored_depth = 0
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"}:
            self._ignored_depth += 1
        if lowered == "title":
            self._in_title = True
        if lowered == "meta":
            values = {name.lower(): value for name, value in attrs if value is not None}
            if values.get("name", "").lower() == "description":
                self.meta_description = values.get("content")
        if lowered == "link":
            values = {name.lower(): value for name, value in attrs if value is not None}
            rel = values.get("rel", "").lower().split()
            if "canonical" in rel and self.canonical_href is None:
                self.canonical_href = values.get("href")
        if lowered == "a":
            values = {name.lower(): value for name, value in attrs if value is not None}
            if values.get("href"):
                self.link_hrefs.append(values["href"])

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"} and self._ignored_depth:
            self._ignored_depth -= 1
        if lowered == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._ignored_depth:
            return
        if self._in_title:
            self.title_parts.append(data)
        self.text_parts.append(data)


def _compact(parts: list[str]) -> str:
    return " ".join(" ".join(parts).split())


def _absolute_http_url(base_url: str, href: str) -> str | None:
    parsed = urlparse(urljoin(base_url, href))
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    return urlunparse(parsed._replace(fragment=""))


def _internal_link_summary(base_url: str, hrefs: list[str]) -> tuple[str, ...]:
    base = urlparse(base_url)
    links: list[str] = []
    for href in hrefs:
        if href.strip().startswith("#"):
            continue
        absolute = _absolute_http_url(base_url, href)
        if absolute is None:
            continue
        parsed = urlparse(absolute)
        if parsed.hostname != base.hostname or absolute in links:
            continue
        links.append(absolute)
        if len(links) == _MAX_INTERNAL_LINKS:
            break
    return tuple(links)


def analyze_http_document(*, status_code: int, final_url: str, html: str) -> L1Analysis:
    """Classify an HTTP response and extract the metadata needed by later stages."""
    if status_code >= 500:
        return L1Analysis(OutcomeCode.HTTP_5XX, final_url, None, None, 0, False, status_code)
    if status_code == 429:
        return L1Analysis(OutcomeCode.HTTP_429, final_url, None, None, 0, False, status_code)
    if status_code >= 400:
        return L1Analysis(OutcomeCode.HTTP_4XX, final_url, None, None, 0, False, status_code)

    parser = _MetadataParser()
    parser.feed(html)
    parser.close()

    title = _compact(parser.title_parts) or None
    text = _compact(parser.text_parts)
    combined = " ".join(value for value in (title, parser.meta_description, text) if value).lower()
    is_parking_page = any(marker in combined for marker in _PARKING_MARKERS)
    outcome_code = (
        OutcomeCode.CONTENT_INSUFFICIENT
        if is_parking_page or len(text) < 500
        else OutcomeCode.SUCCESS
    )
    return L1Analysis(
        outcome_code=outcome_code,
        final_url=final_url,
        title=title,
        meta_description=parser.meta_description,
        text_length=len(text),
        is_parking_page=is_parking_page,
        status_code=status_code,
        canonical_url=(
            _absolute_http_url(final_url, parser.canonical_href)
            if parser.canonical_href
            else None
        ),
        internal_links=_internal_link_summary(final_url, parser.link_hrefs),
    )
