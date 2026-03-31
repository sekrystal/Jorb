from __future__ import annotations

from typing import Any

from core.schemas import CandidateProfilePayload
from services.profile import (
    KNOWN_DOMAINS,
    KNOWN_LOCATIONS,
    KNOWN_STAGES,
    KNOWN_TITLES,
    _extract_summary,
    _match_known_terms,
    extract_text_from_resume_upload,
)


def preview_resume_upload(filename: str, file_bytes: bytes) -> dict[str, Any]:
    raw_text, warnings = extract_text_from_resume_upload(filename, file_bytes)
    return preview_resume_text(filename, raw_text, warnings=warnings)


def preview_resume_text(filename: str, raw_text: str, warnings: list[str] | None = None) -> dict[str, Any]:
    cleaned_text = raw_text.strip()
    if not cleaned_text:
        raise ValueError("Resume text was empty after cleanup.")

    parsed = _extract_summary(cleaned_text)
    matched_titles = _match_known_terms(cleaned_text, KNOWN_TITLES)
    matched_domains = _match_known_terms(cleaned_text, KNOWN_DOMAINS)
    matched_locations = _match_known_terms(cleaned_text, KNOWN_LOCATIONS)
    matched_stages = _match_known_terms(cleaned_text, KNOWN_STAGES)
    matched_skills = parsed["confirmed_skills_json"]
    matched_competencies = parsed["competencies_json"]
    matched_preferences = parsed["explicit_preferences_json"]

    missing_fields: list[str] = []
    if not matched_titles:
        missing_fields.append("preferred titles")
    if not matched_domains:
        missing_fields.append("preferred domains")
    if not matched_locations:
        missing_fields.append("preferred locations")
    if not matched_stages:
        missing_fields.append("stage preferences")

    merged_warnings = list(warnings or [])
    if missing_fields:
        merged_warnings.append("Extraction was partial. Review the inferred profile fields before saving.")

    extracted_summary = {
        "summary": parsed["summary"],
        "resume_filename": filename,
        "extraction_status": "partial" if missing_fields else "complete",
        "missing_fields": missing_fields,
        "years_experience": parsed.get("years_experience"),
    }

    payload = CandidateProfilePayload(
        profile_schema_version="v1",
        name=filename.rsplit(".", 1)[0] or "Candidate",
        raw_resume_text=cleaned_text,
        extracted_summary_json=extracted_summary,
        preferred_titles_json=parsed["preferred_titles_json"],
        adjacent_titles_json=parsed["adjacent_titles_json"],
        excluded_titles_json=parsed["excluded_titles_json"],
        preferred_domains_json=parsed["preferred_domains_json"],
        preferred_locations_json=parsed["preferred_locations_json"],
        target_roles_json=parsed["target_roles_json"],
        work_mode_preference=parsed["work_mode_preference"],
        confirmed_skills_json=parsed["confirmed_skills_json"],
        competencies_json=parsed["competencies_json"],
        explicit_preferences_json=parsed["explicit_preferences_json"],
        seniority_guess=parsed["seniority_guess"],
        years_experience=parsed["years_experience"],
        stage_preferences_json=parsed["stage_preferences_json"],
        core_titles_json=parsed["core_titles_json"],
        excluded_keywords_json=parsed["excluded_keywords_json"],
        min_seniority_band=parsed["min_seniority_band"],
        max_seniority_band=parsed["max_seniority_band"],
        stretch_role_families_json=parsed["stretch_role_families_json"],
    )

    return {
        "filename": filename,
        "raw_text": cleaned_text,
        "text_preview": cleaned_text[:600],
        "warnings": merged_warnings,
        "status": "partial" if missing_fields else "complete",
        "missing_fields": missing_fields,
        "matched_terms": {
            "preferred_titles": matched_titles,
            "preferred_domains": matched_domains,
            "preferred_locations": matched_locations,
            "stage_preferences": matched_stages,
            "confirmed_skills": matched_skills,
            "competencies": matched_competencies,
            "explicit_preferences": matched_preferences,
            "years_experience": parsed.get("years_experience"),
        },
        "candidate_profile": payload.model_dump(mode="json"),
    }
