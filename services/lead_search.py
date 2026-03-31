from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


SEARCH_FIELDS = (
    "title",
    "company",
    "location",
    "description",
    "tags",
    "explanation",
    "source",
)


def normalize_search_query(query: str) -> dict[str, Any]:
    raw = str(query or "")
    normalized = " ".join(raw.strip().lower().split())
    tokens = [token for token in "".join(ch if ch.isalnum() else " " for ch in normalized).split() if token]
    return {
        "raw": raw,
        "text": normalized,
        "tokens": tokens,
    }


def _searchable_text(value: Any) -> str:
    text = " ".join(str(value or "").strip().lower().split())
    if text.startswith("todo"):
        return ""
    return text


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _description_from_payload(lead: dict[str, Any]) -> str:
    evidence = lead.get("evidence_json") or {}
    description = str(evidence.get("description_text") or lead.get("description") or "").strip()
    if description:
        return description
    snippets = [str(snippet).strip() for snippet in (evidence.get("snippets") or []) if str(snippet).strip()]
    return " ".join(snippets)


def _explanation_from_payload(lead: dict[str, Any]) -> str:
    score_payload = lead.get("score_breakdown_json") or {}
    explanation = score_payload.get("explanation") or {}
    return str(
        explanation.get("headline")
        or explanation.get("summary")
        or lead.get("explanation")
        or lead.get("why_this_job")
        or ""
    ).strip()


def _source_from_payload(lead: dict[str, Any]) -> str:
    evidence = lead.get("evidence_json") or {}
    return str(
        lead.get("source_type")
        or evidence.get("source_type")
        or lead.get("source_platform")
        or evidence.get("source_platform")
        or lead.get("source_lineage")
        or evidence.get("source_lineage")
        or lead.get("source")
        or ""
    ).strip()


def _work_mode_from_payload(lead: dict[str, Any]) -> str:
    evidence = lead.get("evidence_json") or {}
    location = str(evidence.get("location") or "").strip().lower()
    location_scope = str(evidence.get("location_scope") or "").strip().lower()
    if "remote" in location or location_scope.startswith("remote"):
        return "remote"
    if "hybrid" in location or location_scope.startswith("hybrid"):
        return "hybrid"
    if location:
        return "onsite"
    return ""


def _tags_from_payload(lead: dict[str, Any]) -> str:
    explicit_tags = lead.get("tags")
    if isinstance(explicit_tags, list) and explicit_tags:
        return " ".join(str(tag).strip() for tag in explicit_tags if str(tag).strip())
    return " ".join(
        str(value).strip()
        for value in (
            lead.get("freshness_label"),
            lead.get("qualification_fit_label"),
            lead.get("confidence_label"),
            _work_mode_from_payload(lead),
        )
        if str(value or "").strip()
    )


def build_search_document(lead: dict[str, Any]) -> dict[str, Any]:
    fields = {
        "title": _searchable_text(lead.get("primary_title") or lead.get("title")),
        "company": _searchable_text(lead.get("company_name") or lead.get("company")),
        "location": _searchable_text((lead.get("evidence_json") or {}).get("location") or lead.get("location")),
        "description": _searchable_text(_description_from_payload(lead)),
        "tags": _searchable_text(_tags_from_payload(lead)),
        "explanation": _searchable_text(_explanation_from_payload(lead)),
        "source": _searchable_text(_source_from_payload(lead)),
    }
    recommendation_sort = lead.get("_recommendation_sort")
    if recommendation_sort is None:
        score_payload = lead.get("score_breakdown_json") or {}
        recommendation_sort = float(score_payload.get("final_score", score_payload.get("composite", 0.0)) or 0.0)
    posted_at_sort = lead.get("_posted_at_sort")
    if posted_at_sort is None:
        posted_at_sort = _parse_timestamp(lead.get("posted_at") or lead.get("surfaced_at")) or datetime.min.replace(tzinfo=timezone.utc)
    haystack = " ".join(value for value in fields.values() if value)
    return {
        "fields": fields,
        "haystack": haystack,
        "recommendation_sort": float(recommendation_sort or 0.0),
        "posted_at_sort": posted_at_sort,
        "title": str(lead.get("primary_title") or lead.get("title") or ""),
        "company": str(lead.get("company_name") or lead.get("company") or ""),
    }


def match_search_document(document: dict[str, Any], normalized_query: dict[str, Any]) -> dict[str, Any] | None:
    phrase = normalized_query["text"]
    tokens = list(normalized_query["tokens"])
    if not phrase and not tokens:
        return None
    fields = document["fields"]
    haystack = str(document["haystack"] or "")
    if not haystack:
        return None
    if phrase and phrase not in haystack and not all(token in haystack for token in tokens):
        return None

    score = 0.0
    matched_fields: list[str] = []
    matched_tokens: list[str] = []

    def _mark(field: str, bonus: float) -> None:
        nonlocal score
        if field not in matched_fields:
            matched_fields.append(field)
        score += bonus

    title = fields["title"]
    company = fields["company"]
    if phrase:
        if title == phrase:
            _mark("title_exact", 140.0)
        elif phrase in title:
            _mark("title", 110.0)
        if company == phrase:
            _mark("company_exact", 125.0)
        elif phrase in company:
            _mark("company", 95.0)
        for field, value in fields.items():
            if field in {"title", "company"} or not value:
                continue
            if phrase in value:
                _mark(field, 55.0)
    token_hits = 0
    for token in tokens:
        token_matched = False
        for field, value in fields.items():
            if value and token in value:
                token_matched = True
                if field not in matched_fields:
                    matched_fields.append(field)
        if token_matched:
            token_hits += 1
            matched_tokens.append(token)
    if tokens:
        coverage = token_hits / float(len(tokens))
        score += coverage * 40.0
        if coverage == 1.0:
            score += 25.0
    if not matched_fields:
        return None
    return {
        "score": round(score, 3),
        "matched_fields": matched_fields,
        "matched_tokens": matched_tokens,
    }


def search_sort_key(document: dict[str, Any], match: dict[str, Any]) -> tuple[float, float, float, str, str]:
    recency = document["posted_at_sort"]
    return (
        float(match["score"]),
        float(document["recommendation_sort"]),
        recency.timestamp() if isinstance(recency, datetime) else 0.0,
        document["title"],
        document["company"],
    )
