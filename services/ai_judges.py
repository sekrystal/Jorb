from __future__ import annotations

import json
import time
from typing import Any, Optional

import requests

from core.config import get_settings
from core.logging import get_logger


logger = get_logger(__name__)
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_WARNING_DEDUPE_SECONDS = 300
_OPENAI_WARNING_CACHE: dict[str, float] = {}


def ai_available() -> bool:
    settings = get_settings()
    return bool(settings.openai_enabled and settings.openai_api_key)


def _extract_response_text(payload: dict[str, Any]) -> Optional[str]:
    if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
        return payload["output_text"]
    for item in payload.get("output", []):
        for content in item.get("content", []):
            if isinstance(content.get("json"), dict):
                return json.dumps(content["json"])
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                return text
    return None


def build_openai_request_payload(
    schema_name: str,
    schema: dict[str, Any],
    system_prompt: str,
    user_prompt: str,
    model: str,
) -> dict[str, Any]:
    return {
        "model": model,
        "instructions": system_prompt,
        "input": user_prompt,
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "schema": schema,
                "strict": True,
            }
        },
    }


def _extract_error_summary(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        body = (response.text or "").strip()
        return body[:300] if body else f"http_{response.status_code}"

    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message") or "unknown_error"
        error_type = error.get("type")
        param = error.get("param")
        code = error.get("code")
        bits = [str(part) for part in [message, f"type={error_type}" if error_type else None, f"param={param}" if param else None, f"code={code}" if code else None] if part]
        return " | ".join(bits)[:300]
    return json.dumps(payload)[:300]


def _log_openai_warning_once(key: str, message: str) -> None:
    now = time.time()
    last_logged_at = _OPENAI_WARNING_CACHE.get(key)
    if last_logged_at and now - last_logged_at < OPENAI_WARNING_DEDUPE_SECONDS:
        return
    _OPENAI_WARNING_CACHE[key] = now
    logger.warning(message)


def call_openai_json(schema_name: str, schema: dict[str, Any], system_prompt: str, user_prompt: str) -> Optional[dict[str, Any]]:
    settings = get_settings()
    if not ai_available():
        return None

    payload = build_openai_request_payload(
        schema_name=schema_name,
        schema=schema,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=settings.openai_model,
    )
    headers = {
        "Authorization": f"Bearer {settings.openai_api_key}",
        "Content-Type": "application/json",
    }

    last_error_summary: str | None = None
    attempts = max(settings.openai_max_retries, 1)
    for _attempt in range(attempts):
        try:
            response = requests.post(
                OPENAI_RESPONSES_URL,
                headers=headers,
                json=payload,
                timeout=settings.openai_timeout_seconds,
            )
            status_code = getattr(response, "status_code", 200)
            if status_code >= 400:
                last_error_summary = _extract_error_summary(response)
                response.raise_for_status()
            data = response.json()
            text = _extract_response_text(data)
            if not text:
                _log_openai_warning_once(
                    f"{schema_name}:empty_text",
                    f"OpenAI returned no structured text for {schema_name}; falling back to deterministic logic.",
                )
                return None
            return json.loads(text)
        except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
            if last_error_summary is None:
                last_error_summary = str(exc)

    _log_openai_warning_once(
        f"{schema_name}:{last_error_summary}",
        f"OpenAI {schema_name} failed, falling back to deterministic logic: {last_error_summary}",
    )
    return None


def interpret_signal_with_ai(raw_text: str, source_url: str, author_handle: str | None = None, query_text: str = "") -> Optional[dict[str, Any]]:
    schema = {
        "type": "object",
        "properties": {
            "company_guess": {"type": ["string", "null"]},
            "role_guess": {"type": ["string", "null"]},
            "location_guess": {"type": ["string", "null"]},
            "hiring_confidence": {"type": "number"},
            "signal_status": {"type": "string", "enum": ["new", "weak", "resolved", "needs_recheck"]},
            "reason": {"type": "string"},
        },
        "required": ["company_guess", "role_guess", "location_guess", "hiring_confidence", "signal_status", "reason"],
        "additionalProperties": False,
    }
    return call_openai_json(
        "signal_interpretation",
        schema,
        "You extract startup hiring signals into conservative structured JSON. Be skeptical and do not invent company names.",
        f"Signal text: {raw_text}\nSource URL: {source_url}\nAuthor: {author_handle or 'unknown'}\nQuery: {query_text or 'none'}",
    )


def judge_fit_with_ai(profile_text: str, title: str, company_name: str, location: str | None, description_text: str) -> Optional[dict[str, Any]]:
    schema = {
        "type": "object",
        "properties": {
            "classification": {
                "type": "string",
                "enum": ["strong_fit", "adjacent", "stretch", "underqualified", "overqualified", "unclear"],
            },
            "reasons": {"type": "array", "items": {"type": "string"}},
            "matched_profile_fields": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["classification", "reasons", "matched_profile_fields"],
        "additionalProperties": False,
    }
    return call_openai_json(
        "fit_judgment",
        schema,
        "You compare a candidate profile against a job and return a conservative fit assessment.",
        (
            f"Candidate profile:\n{profile_text}\n\n"
            f"Company: {company_name}\nTitle: {title}\nLocation: {location or 'unknown'}\n"
            f"Description:\n{description_text[:4000]}"
        ),
    )


def judge_critic_with_ai(title: str, company_name: str, description_text: str, listing_status: str | None, freshness_days: int | None, page_text: str, url: str | None) -> Optional[dict[str, Any]]:
    schema = {
        "type": "object",
        "properties": {
            "quality_assessment": {"type": "string", "enum": ["live", "uncertain", "stale", "suppress"]},
            "reasons": {"type": "array", "items": {"type": "string"}},
            "flags": {
                "type": "object",
                "properties": {
                    "stale_like": {"type": "boolean"},
                    "broken_like": {"type": "boolean"},
                    "duplicate_like": {"type": "boolean"},
                    "low_info": {"type": "boolean"},
                },
                "required": ["stale_like", "broken_like", "duplicate_like", "low_info"],
                "additionalProperties": False,
            },
        },
        "required": ["quality_assessment", "reasons", "flags"],
        "additionalProperties": False,
    }
    return call_openai_json(
        "critic_judgment",
        schema,
        "You review job listings for stale, broken, low-quality, or uncertain signals. Be conservative and do not mark a listing live unless the evidence supports it.",
        (
            f"Company: {company_name}\nTitle: {title}\nListing status: {listing_status or 'unknown'}\n"
            f"Freshness days: {freshness_days if freshness_days is not None else 'unknown'}\n"
            f"URL: {url or 'missing'}\nDescription:\n{description_text[:2500]}\n\nPage text:\n{page_text[:2000]}"
        ),
    )


def write_explanation_with_ai(context: dict[str, Any]) -> Optional[str]:
    schema = {
        "type": "object",
        "properties": {"explanation": {"type": "string"}},
        "required": ["explanation"],
        "additionalProperties": False,
    }
    result = call_openai_json(
        "lead_explanation",
        schema,
        "You write concise, concrete opportunity explanations grounded in candidate fit, source evidence, freshness, and uncertainty.",
        json.dumps(context),
    )
    if not result:
        return None
    return result.get("explanation")
