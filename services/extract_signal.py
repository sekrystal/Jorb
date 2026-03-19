from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Optional

from core.schemas import SignalRecord
from services.ai_judges import interpret_signal_with_ai


ROLE_PATTERNS = [
    "founding ops",
    "chief of staff",
    "deployment strategist",
    "implementation lead",
    "technical pm",
    "business rhythm architect",
    "operations lead",
]
LOCATION_PATTERNS = ["sf", "san francisco", "nyc", "new york", "remote", "bay area"]


def extract_signal_fields(raw_text: str, source_url: str, author_handle: Optional[str] = None, query_text: str = "") -> SignalRecord:
    ai_result = interpret_signal_with_ai(raw_text, source_url, author_handle=author_handle, query_text=query_text)
    lowered = raw_text.lower()
    role_guess = next((role for role in ROLE_PATTERNS if role in lowered), None)
    location_guess = next((location for location in LOCATION_PATTERNS if location in lowered), None)
    company_guess = None

    at_match = re.search(r"\bat\s+([A-Z][A-Za-z0-9&\-\s]+)", raw_text)
    if at_match:
        company_guess = at_match.group(1).strip(" .,!?:;")
        company_guess = company_guess.split(" in ")[0].strip()
    else:
        company_hint = re.search(r"\bfor\s+([A-Z][A-Za-z0-9&\-\s]+)", raw_text)
        if company_hint and "startup" not in company_hint.group(1).lower():
            company_guess = company_hint.group(1).strip(" .,!?:;").split(" in ")[0].strip()

    confidence = 0.25
    if "hiring" in lowered or "looking for" in lowered:
        confidence += 0.2
    if role_guess:
        confidence += 0.2
    if company_guess:
        confidence += 0.2
    if location_guess:
        confidence += 0.1

    if ai_result:
        company_guess = ai_result.get("company_guess") or company_guess
        role_guess = ai_result.get("role_guess") or role_guess
        location_guess = ai_result.get("location_guess") or location_guess
        confidence = max(confidence, float(ai_result.get("hiring_confidence", confidence) or confidence))

    return SignalRecord(
        source_type="x",
        source_url=source_url,
        author_handle=author_handle,
        raw_text=raw_text,
        company_guess=company_guess,
        role_guess=role_guess,
        location_guess=location_guess,
        hiring_confidence=min(confidence, 0.95),
        signal_status=(ai_result or {}).get("signal_status") or ("resolved" if company_guess else "weak"),
        metadata_json={"query_text": query_text, "ai_signal_reason": (ai_result or {}).get("reason")},
    )


def extract_many(raw_signals: Iterable[dict]) -> list[SignalRecord]:
    items = []
    for item in raw_signals:
        record = extract_signal_fields(
            raw_text=item["text"],
            source_url=item["url"],
            author_handle=item.get("author_handle"),
            query_text=item.get("query_text", ""),
        )
        if item.get("published_at"):
            record.published_at = item["published_at"]
        items.append(record)
    return items
