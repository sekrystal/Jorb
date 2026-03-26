from __future__ import annotations

from typing import Any


def build_profile_review_rows(profile: dict[str, Any]) -> list[dict[str, str]]:
    extracted_summary = profile.get("extracted_summary_json") or {}
    structured_profile = extracted_summary.get("structured_profile") or profile.get("structured_profile_json") or {}
    targeting = structured_profile.get("targeting") or {}
    seniority = targeting.get("seniority") or {}
    scoring = structured_profile.get("scoring") or {}

    rows = [
        {"field": "Preferred titles", "value": _csv(targeting.get("preferred_titles") or profile.get("preferred_titles_json"))},
        {"field": "Core titles", "value": _csv(targeting.get("core_titles") or profile.get("core_titles_json"))},
        {"field": "Adjacent titles", "value": _csv(targeting.get("adjacent_titles") or profile.get("adjacent_titles_json"))},
        {"field": "Preferred domains", "value": _csv(targeting.get("preferred_domains") or profile.get("preferred_domains_json"))},
        {"field": "Preferred locations", "value": _csv(targeting.get("preferred_locations") or profile.get("preferred_locations_json"))},
        {"field": "Target roles", "value": _csv(targeting.get("target_roles") or profile.get("target_roles_json"))},
        {"field": "Work mode", "value": str(targeting.get("work_mode_preference") or profile.get("work_mode_preference") or "")},
        {"field": "Preferred stages", "value": _csv(targeting.get("stage_preferences") or profile.get("stage_preferences_json"))},
        {"field": "Excluded keywords", "value": _csv(targeting.get("excluded_keywords") or profile.get("excluded_keywords_json"))},
        {"field": "Stretch role families", "value": _csv(targeting.get("stretch_role_families") or profile.get("stretch_role_families_json"))},
        {"field": "Seniority", "value": _seniority_value(seniority, profile)},
        {"field": "Minimum fit threshold", "value": str(scoring.get("minimum_fit_threshold") or profile.get("minimum_fit_threshold") or "")},
    ]
    return [row for row in rows if row["value"]]


def _csv(values: Any) -> str:
    if not values:
        return ""
    if isinstance(values, str):
        return values
    return ", ".join(str(value) for value in values if str(value).strip())


def _seniority_value(seniority: dict[str, Any], profile: dict[str, Any]) -> str:
    guess = seniority.get("guess") or profile.get("seniority_guess") or ""
    minimum = seniority.get("minimum_band") or profile.get("min_seniority_band") or ""
    maximum = seniority.get("maximum_band") or profile.get("max_seniority_band") or ""
    if not any([guess, minimum, maximum]):
        return ""
    return f"{guess or 'unknown'} ({minimum or 'n/a'} to {maximum or 'n/a'})"
