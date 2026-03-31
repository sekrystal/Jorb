from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
from html.parser import HTMLParser
import re
from typing import Any


SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "Overview": ("overview", "about", "about the role", "role overview", "summary", "the role"),
    "Responsibilities": (
        "responsibilities",
        "what you'll do",
        "what you will do",
        "in this role",
        "what you’ll do",
        "you will",
        "you'll",
        "day to day",
    ),
    "Requirements": (
        "requirements",
        "qualifications",
        "what we're looking for",
        "what we’re looking for",
        "what you bring",
        "what you’ll bring",
        "about you",
        "experience",
        "must have",
        "preferred qualifications",
    ),
    "Benefits": ("benefits", "perks", "what we offer", "compensation", "why join", "our benefits"),
    "Other": (),
}

NOISE_PATTERNS = (
    "skip to content",
    "privacy policy",
    "terms of service",
    "cookie policy",
    "powered by greenhouse",
    "share this job",
    "apply for this job",
    "back to jobs",
    "sign in",
    "sign up",
)


@dataclass
class _SectionBuffer:
    heading: str
    paragraphs: list[str] = field(default_factory=list)
    bullets: list[str] = field(default_factory=list)


class _JobHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.blocks: list[tuple[str, str]] = []
        self._parts: list[str] = []
        self._current_kind = "paragraph"
        self._suppress_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript", "svg"}:
            self._flush()
            self._suppress_depth += 1
            return
        if self._suppress_depth:
            return
        if lowered in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._flush()
            self._current_kind = "heading"
        elif lowered == "li":
            self._flush()
            self._current_kind = "bullet"
        elif lowered in {"p", "div", "section", "article", "ul", "ol", "br"}:
            self._flush()

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript", "svg"} and self._suppress_depth:
            self._suppress_depth -= 1
            return
        if self._suppress_depth:
            return
        if lowered in {"h1", "h2", "h3", "h4", "h5", "h6", "p", "li"}:
            self._flush()
            self._current_kind = "paragraph"
        elif lowered in {"div", "section", "article"}:
            self._flush()

    def handle_data(self, data: str) -> None:
        if self._suppress_depth:
            return
        cleaned = unescape(data or "")
        if cleaned:
            self._parts.append(cleaned)

    def _flush(self) -> None:
        if not self._parts:
            return
        text = _normalize_fragment(" ".join(self._parts))
        self._parts.clear()
        if text:
            self.blocks.append((self._current_kind, text))


def _normalize_fragment(value: str) -> str:
    text = unescape(str(value or ""))
    text = text.replace("\xa0", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _looks_like_heading(text: str) -> bool:
    lowered = text.strip().rstrip(":").lower()
    if len(lowered) > 60:
        return False
    for aliases in SECTION_ALIASES.values():
        if lowered in aliases:
            return True
    return lowered in {"overview", "responsibilities", "requirements", "benefits", "other"}


def _canonical_heading(text: str) -> str:
    lowered = text.strip().rstrip(":").lower()
    for heading, aliases in SECTION_ALIASES.items():
        if lowered == heading.lower() or lowered in aliases:
            return heading
    return "Other"


def _is_noise(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return True
    return any(pattern in lowered for pattern in NOISE_PATTERNS)


def _blocks_from_html(raw_html: str) -> list[tuple[str, str]]:
    parser = _JobHTMLParser()
    parser.feed(raw_html or "")
    parser.close()
    return parser.blocks


def _blocks_from_text(raw_text: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    for line in re.split(r"\n{2,}|\r\n\r\n", raw_text or ""):
        cleaned = _normalize_fragment(line)
        if not cleaned:
            continue
        bullet_match = re.match(r"^[\-\*\u2022]\s+(.*)$", cleaned)
        if bullet_match:
            blocks.append(("bullet", _normalize_fragment(bullet_match.group(1))))
            continue
        if _looks_like_heading(cleaned):
            blocks.append(("heading", cleaned.rstrip(":")))
            continue
        blocks.append(("paragraph", cleaned))
    return blocks


def _dedupe_blocks(blocks: list[tuple[str, str]]) -> list[tuple[str, str]]:
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for kind, text in blocks:
        normalized = _normalize_fragment(text).lower()
        if not normalized or _is_noise(normalized):
            continue
        key = (kind, normalized)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((kind, _normalize_fragment(text)))
    return deduped


def _assign_sections(blocks: list[tuple[str, str]]) -> list[_SectionBuffer]:
    sections: list[_SectionBuffer] = []
    current = _SectionBuffer("Overview")

    def _flush_current() -> None:
        nonlocal current
        if current.paragraphs or current.bullets:
            sections.append(current)
        current = _SectionBuffer(current.heading)

    for kind, text in blocks:
        if kind == "heading":
            _flush_current()
            current = _SectionBuffer(_canonical_heading(text))
            continue
        if kind == "bullet":
            current.bullets.append(text)
        else:
            current.paragraphs.append(text)
    _flush_current()

    merged: list[_SectionBuffer] = []
    for section in sections:
        if merged and merged[-1].heading == section.heading:
            merged[-1].paragraphs.extend(section.paragraphs)
            merged[-1].bullets.extend(section.bullets)
            continue
        merged.append(section)
    return merged


def _dedupe_section_lines(section: _SectionBuffer) -> _SectionBuffer:
    seen: set[str] = set()
    paragraphs: list[str] = []
    bullets: list[str] = []
    for value in section.paragraphs:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        paragraphs.append(value)
    for value in section.bullets:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        bullets.append(value)
    return _SectionBuffer(section.heading, paragraphs=paragraphs, bullets=bullets)


def _render_canonical_text(sections: list[_SectionBuffer]) -> str:
    chunks: list[str] = []
    for section in sections:
        if not section.paragraphs and not section.bullets:
            continue
        body: list[str] = []
        body.extend(section.paragraphs)
        body.extend(f"- {item}" for item in section.bullets)
        chunks.append(f"{section.heading}\n" + "\n".join(body))
    return "\n\n".join(chunks).strip()


def clean_job_content(
    *,
    source_type: str,
    raw_text: str | None = None,
    raw_html: str | None = None,
    page_text: str | None = None,
) -> dict[str, Any]:
    html_candidate = str(raw_html or "").strip()
    text_candidate = str(raw_text or "").strip()
    page_candidate = str(page_text or "").strip()

    blocks = _blocks_from_html(html_candidate) if html_candidate else []
    if not blocks and text_candidate:
        blocks = _blocks_from_text(text_candidate)
    if not blocks and page_candidate:
        blocks = _blocks_from_text(page_candidate)

    deduped_blocks = _dedupe_blocks(blocks)
    sections = [_dedupe_section_lines(section) for section in _assign_sections(deduped_blocks)]
    sections_payload = [
        {"heading": section.heading, "paragraphs": list(section.paragraphs), "bullets": list(section.bullets)}
        for section in sections
        if section.paragraphs or section.bullets
    ]
    canonical_text = _render_canonical_text(sections)
    plain_text = " ".join(
        item
        for section in sections
        for item in [*section.paragraphs, *section.bullets]
    ).strip()
    summary = next(
        (
            paragraph
            for section in sections
            for paragraph in [*section.paragraphs, *section.bullets]
            if paragraph
        ),
        "",
    )
    return {
        "source_type": source_type,
        "source_format": "html" if html_candidate else "text",
        "sections": sections_payload,
        "canonical_text": canonical_text or _normalize_fragment(text_candidate or page_candidate),
        "plain_text": plain_text or _normalize_fragment(text_candidate or page_candidate),
        "summary": summary or _normalize_fragment(text_candidate or page_candidate),
    }
