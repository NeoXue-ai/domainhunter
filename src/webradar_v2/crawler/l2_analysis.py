"""Pure extraction of review facts from a rendered browser DOM snapshot."""

from dataclasses import dataclass
from html.parser import HTMLParser


_IGNORED_TAGS = {"script", "style", "noscript"}
_HEADING_TAGS = {"h1", "h2", "h3"}
_CTA_TAGS = {"a", "button"}
_PRICING_TERMS = ("pricing", "plans", "$", "per month", "per year")
_REGISTRATION_TERMS = ("start free trial", "sign up", "get started", "register")


@dataclass(frozen=True, slots=True)
class L2Facts:
    headings: tuple[str, ...]
    ctas: tuple[str, ...]
    pricing_evidence: str | None
    registration_evidence: str | None
    dom_excerpt: str


class _RenderedDOMParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._ignored_depth = 0
        self._heading_tag: str | None = None
        self._heading_parts: list[str] = []
        self.headings: list[str] = []
        self._cta_tag: str | None = None
        self._cta_parts: list[str] = []
        self.ctas: list[str] = []
        self.visible_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered in _IGNORED_TAGS:
            self._ignored_depth += 1
            return
        if self._ignored_depth:
            return
        if lowered in _HEADING_TAGS:
            self._heading_tag = lowered
            self._heading_parts = []
        if lowered in _CTA_TAGS and self._cta_tag is None:
            self._cta_tag = lowered
            self._cta_parts = []

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in _IGNORED_TAGS and self._ignored_depth:
            self._ignored_depth -= 1
            return
        if self._ignored_depth:
            return
        if lowered == self._heading_tag:
            value = _compact(self._heading_parts)
            if value:
                self.headings.append(value)
            self._heading_tag = None
        if lowered == self._cta_tag:
            value = _compact(self._cta_parts)
            if value:
                self.ctas.append(value)
            self._cta_tag = None

    def handle_data(self, data: str) -> None:
        if self._ignored_depth:
            return
        self.visible_parts.append(data)
        if self._heading_tag:
            self._heading_parts.append(data)
        if self._cta_tag:
            self._cta_parts.append(data)


def _compact(parts: list[str]) -> str:
    return " ".join(" ".join(parts).split())


def _evidence_excerpt(text: str, terms: tuple[str, ...]) -> str | None:
    lowered = text.lower()
    offsets = [lowered.find(term) for term in terms if lowered.find(term) >= 0]
    if not offsets:
        return None
    start = max(0, min(offsets) - 80)
    return text[start : start + 240]


def extract_rendered_facts(html: str) -> L2Facts:
    """Extract compact, citeable product facts from a browser-rendered HTML snapshot."""
    parser = _RenderedDOMParser()
    parser.feed(html)
    parser.close()
    visible_text = _compact(parser.visible_parts)
    return L2Facts(
        headings=tuple(dict.fromkeys(parser.headings)),
        ctas=tuple(dict.fromkeys(parser.ctas)),
        pricing_evidence=_evidence_excerpt(visible_text, _PRICING_TERMS),
        registration_evidence=_evidence_excerpt(visible_text, _REGISTRATION_TERMS),
        dom_excerpt=visible_text[:8_000],
    )
