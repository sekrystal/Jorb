from __future__ import annotations

import re
from typing import Optional

from core.models import CandidateProfile
from services.profile import build_search_intent


ROLE_FAMILY_KEYWORDS = {
    "operations": ["ops", "operations", "chief of staff", "bizops", "program", "strategic"],
    "go_to_market": ["deployment", "implementation", "customer", "solutions", "growth"],
    "product": ["product", "pm", "technical product", "program"],
    "engineering": ["engineer", "infrastructure", "platform", "ai infra"],
}
SENIORITY_ORDER = {"entry": 0, "junior": 1, "mid": 2, "senior": 3, "staff": 4, "executive": 5}
TITLE_HARD_FILTERS = ["intern", "new grad", "ceo", "founder", "principal scientist", "rocket propulsion engineer"]
QUALIFICATION_SPECIALIZATIONS = ["rocket propulsion", "specialized hardware", "principal scientist", "bar admission"]
WORK_MODE_KEYWORDS = {
    "remote": ["remote", "work from home", "distributed"],
    "hybrid": ["hybrid"],
    "onsite": ["onsite", "on-site", "on site", "in office", "in-office"],
}


def _first_case_insensitive_match(value: Optional[str], choices: list[str]) -> Optional[str]:
    if not value:
        return None
    lowered = value.lower()
    for choice in choices:
        if choice.lower() in lowered:
            return choice
    return None


def build_role_match_explanation(title_fit_label: str, matched_profile_fields: list[str], search_intent, title: str) -> str:
    if title_fit_label == "target role match":
        target_role = next((role for role in search_intent.target_roles if role.lower() in title.lower()), title)
        return f"Role match: title matches explicit target role '{target_role}'."
    if title_fit_label == "core match":
        return "Role match: title aligns with a core role from the profile."
    if title_fit_label == "adjacent match":
        return "Role match: title is adjacent to the candidate's preferred scope."
    if title_fit_label == "unexpected but plausible":
        return "Role match: scope keywords in the description make the role plausibly relevant."
    if "excluded title" in matched_profile_fields:
        return "Role match: title conflicts with an excluded role from the profile."
    return "Role match: title is only a weak match for the candidate's target scope."


def build_location_fit_explanation(
    location: Optional[str],
    preferred_locations: list[str],
    work_mode_preference: str,
    lead_work_mode: str,
    search_intent,
    location_fit: float,
) -> str:
    matched_location = _first_case_insensitive_match(location, preferred_locations)
    parts: list[str] = []

    if matched_location:
        parts.append(f"location '{location}' matches preferred geography '{matched_location}'")
    elif location:
        parts.append(f"location '{location}' does not match preferred geographies")
    else:
        parts.append("location is unspecified")

    if work_mode_preference != "unspecified":
        if lead_work_mode == work_mode_preference:
            parts.append(f"work mode matches the {work_mode_preference} preference")
        elif lead_work_mode == "unspecified":
            parts.append(f"work mode could not be confirmed against the {work_mode_preference} preference")
        elif search_intent.explicit_work_mode:
            parts.append(f"work mode conflicts with the {work_mode_preference} preference")
        else:
            parts.append(f"work mode differs from the inferred {work_mode_preference} preference")

    direction = "positive" if location_fit > 0 else "neutral" if location_fit == 0 else "negative"
    return f"Location fit: {'; '.join(parts)} ({direction} signal)."


def infer_role_family(title: str, description_text: str = "") -> str:
    lowered = f"{title} {description_text}".lower()
    for family, keywords in ROLE_FAMILY_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return family
    return "generalist"


def infer_seniority_band(title: str, description_text: str = "") -> str:
    lowered = f"{title} {description_text}".lower()
    years = [int(item) for item in re.findall(r"(\d+)\+?\s+years", lowered)]
    if "chief of staff" in lowered:
        return "senior"
    if "founding" in lowered or "architect" in lowered:
        return "mid"
    if re.search(r"\bintern\b|\bnew grad\b|\bcampus\b|\bentry level\b", lowered):
        return "entry"
    if years and max(years) <= 2:
        return "junior"
    if years and max(years) <= 5:
        return "mid"
    if years and max(years) <= 8:
        return "senior"
    if years and max(years) > 8:
        return "staff"
    if any(keyword in lowered for keyword in ["director", "head of", "lead"]):
        return "senior"
    if any(keyword in lowered for keyword in ["vp", "chief", "founder", "ceo"]):
        return "executive"
    return "mid"


def infer_work_mode(location: Optional[str], description_text: str = "") -> str:
    lowered = f"{location or ''} {description_text}".lower()
    for work_mode, keywords in WORK_MODE_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return work_mode
    return "unspecified"


def classify_title_fit(profile: CandidateProfile, title: str, description_text: str = "") -> tuple[str, float, list[str]]:
    title_lower = title.lower()
    description_lower = description_text.lower()
    matched_fields: list[str] = []
    search_intent = build_search_intent(profile)

    if any(excluded.lower() in title_lower for excluded in profile.excluded_titles_json or []):
        return "excluded", -10.0, ["excluded title"]

    core_titles = [item.lower() for item in (profile.core_titles_json or profile.preferred_titles_json or [])]
    adjacent_titles = [item.lower() for item in (profile.adjacent_titles_json or [])]
    explicit_target_roles = [item.lower() for item in search_intent.explicit_target_roles]

    if explicit_target_roles and any(target_role in title_lower for target_role in explicit_target_roles):
        matched_fields.append("target role")
        return "target role match", 2.8, matched_fields

    if any(core in title_lower for core in core_titles):
        matched_fields.append("core title")
        return "core match", 2.4, matched_fields
    if any(adjacent in title_lower for adjacent in adjacent_titles):
        matched_fields.append("adjacent title")
        return "adjacent match", 1.5, matched_fields

    scope_keywords = set()
    for item in core_titles + adjacent_titles:
        scope_keywords.update(item.split())
    scope_keywords.update(["systems", "operating", "operations", "cadence", "planning", "deployment", "customer"])
    overlap = [word for word in scope_keywords if len(word) > 3 and word in description_lower]
    if len(overlap) >= 2:
        matched_fields.append("scope match")
        return "unexpected but plausible", 1.1, matched_fields

    return "weak title match", 0.2, matched_fields


def classify_qualification_fit(profile: CandidateProfile, title: str, description_text: str = "") -> tuple[str, float, list[str]]:
    title_lower = title.lower()
    description_lower = description_text.lower()
    reasons: list[str] = []
    seniority_band = infer_seniority_band(title, description_text)
    role_family = infer_role_family(title, description_text)
    min_band = SENIORITY_ORDER.get(profile.min_seniority_band, 2)
    max_band = SENIORITY_ORDER.get(profile.max_seniority_band, 3)
    lead_band = SENIORITY_ORDER.get(seniority_band, 2)

    if any(pattern in f"{title_lower} {description_lower}" for pattern in QUALIFICATION_SPECIALIZATIONS):
        return "underqualified", -3.0, ["specialized requirement mismatch"]
    if any(pattern in title_lower for pattern in TITLE_HARD_FILTERS):
        if any(pattern in title_lower for pattern in ["intern", "new grad"]):
            return "overqualified", -2.5, ["role is clearly junior"]
        return "underqualified", -3.0, ["title implies unrealistic qualification gap"]
    if lead_band < min_band:
        return "overqualified", -1.8, ["seniority below candidate floor"]
    if lead_band > max_band:
        if role_family in (profile.stretch_role_families_json or []):
            return "stretch", 0.4, ["above normal seniority but in stretch family"]
        return "underqualified", -2.0, ["seniority above candidate band"]
    if "phd required" in description_lower or "board certification" in description_lower:
        return "underqualified", -2.4, ["credential requirement mismatch"]

    strong_title_signals = [
        "chief of staff",
        "operations",
        "operator",
        "bizops",
        "business operations",
        "strategic operations",
    ]
    if any(signal in title_lower for signal in strong_title_signals):
        return "strong fit", 1.0, reasons

    return "stretch", 0.4, ["qualification fit is plausible but not strongly evidenced by title"]


def score_lead(
    profile: CandidateProfile,
    lead_type: str,
    title: str,
    company_name: str,
    company_domain: Optional[str],
    location: Optional[str],
    description_text: str,
    freshness_label: str,
    listing_status: Optional[str],
    source_type: str,
    evidence_count: int,
    feedback_learning: Optional[dict] = None,
) -> dict:
    feedback_learning = feedback_learning or {}
    search_intent = build_search_intent(profile)
    role_family = infer_role_family(title, description_text)
    title_fit_label, title_fit_score, matched_title_fields = classify_title_fit(profile, title, description_text)
    qualification_fit_label, qualification_score, qualification_reasons = classify_qualification_fit(profile, title, description_text)
    matched_profile_fields = list(dict.fromkeys([*matched_title_fields, *qualification_reasons]))
    preferred_locations = search_intent.preferred_locations
    work_mode_preference = search_intent.work_mode_preference
    lead_work_mode = infer_work_mode(location, description_text)

    freshness_score = {"fresh": 1.6, "recent": 1.0, "stale": -1.2, "unknown": -0.5}[freshness_label]
    source_quality = {"greenhouse": 1.2, "ashby": 1.2, "yc_jobs": 0.9, "x": 0.5, "x_signal": 0.5}.get(source_type, 0.6)
    evidence_quality = min(0.4 * max(evidence_count, 1), 1.2)
    novelty = 0.5 if lead_type in {"signal", "combined"} else 0.2
    location_fit = 0.0
    if location and any(item.lower() in location.lower() for item in preferred_locations):
        location_fit += 1.0
        matched_profile_fields.append("preferred geography")
    if work_mode_preference != "unspecified":
        if lead_work_mode == work_mode_preference:
            location_fit += 0.6 if search_intent.explicit_work_mode else 0.3
            matched_profile_fields.append("work mode preference")
        elif search_intent.explicit_work_mode and lead_work_mode != "unspecified":
            location_fit -= 0.6
    domain_fit = 0.9 if company_domain and any(item.lower() in company_domain.lower() for item in (profile.preferred_domains_json or [])) else 0.0
    stage_fit = 0.5 if any(stage in description_text.lower() for stage in (profile.stage_preferences_json or [])) else 0.0
    role_family_fit = 0.8 if role_family in {"operations", "go_to_market"} else 0.3
    negative_signals = 0.0

    if listing_status in {"expired", "suspected_expired"}:
        negative_signals -= 3.0
    if lead_type in {"listing", "combined"} and freshness_label == "unknown":
        negative_signals -= 1.2
    if lead_type == "signal":
        negative_signals -= 0.2
    if company_name.lower() in [item.lower() for item in (profile.excluded_companies_json or [])]:
        negative_signals -= 4.0
    if any(keyword.lower() in f"{title.lower()} {description_text.lower()}" for keyword in (profile.excluded_keywords_json or [])):
        negative_signals -= 3.5

    title_weights = feedback_learning.get("title_weights", {})
    role_family_weights = feedback_learning.get("role_family_weights", {})
    domain_weights = feedback_learning.get("domain_weights", {})
    source_penalties = feedback_learning.get("source_penalties", {})
    title_feedback = title_weights.get(title.lower(), 0.0)
    role_family_feedback = role_family_weights.get(role_family, 0.0)
    domain_feedback = domain_weights.get((company_domain or "").lower(), 0.0)
    source_feedback = -source_penalties.get(source_type, 0.0)

    positive_feedback_total = sum(max(value, 0.0) for value in [title_feedback, role_family_feedback, domain_feedback])
    positive_feedback_cap = 1.5
    if positive_feedback_total > positive_feedback_cap:
        scale = positive_feedback_cap / positive_feedback_total
        title_feedback = title_feedback * scale if title_feedback > 0 else title_feedback
        role_family_feedback = role_family_feedback * scale if role_family_feedback > 0 else role_family_feedback
        domain_feedback = domain_feedback * scale if domain_feedback > 0 else domain_feedback

    composite = round(
        freshness_score
        + title_fit_score
        + role_family_fit
        + domain_fit
        + location_fit
        + stage_fit
        + source_quality
        + evidence_quality
        + novelty
        + qualification_score
        + title_feedback
        + role_family_feedback
        + domain_feedback
        + source_feedback
        + negative_signals,
        2,
    )

    rank_label = "strong" if composite >= 7.5 else "medium" if composite >= max(profile.minimum_fit_threshold, 3.0) else "weak"
    confidence_components = [
        source_quality,
        evidence_quality,
        0.8 if lead_type == "combined" else 0.4,
        0.7 if listing_status == "active" else 0.0,
        -0.4 if freshness_label == "unknown" else 0.0,
    ]
    confidence_total = sum(confidence_components)
    confidence_label = "high" if confidence_total >= 2.6 else "medium" if confidence_total >= 1.4 else "low"
    role_match_explanation = build_role_match_explanation(title_fit_label, matched_profile_fields, search_intent, title)
    location_fit_explanation = build_location_fit_explanation(
        location=location,
        preferred_locations=preferred_locations,
        work_mode_preference=work_mode_preference,
        lead_work_mode=lead_work_mode,
        search_intent=search_intent,
        location_fit=location_fit,
    )

    return {
        "final_score": composite,
        "composite": composite,
        "freshness": round(freshness_score, 2),
        "title_fit": round(title_fit_score, 2),
        "role_family_fit": round(role_family_fit, 2),
        "domain_fit": round(domain_fit, 2),
        "location_fit": round(location_fit, 2),
        "stage_company_fit": round(stage_fit, 2),
        "source_quality": round(source_quality, 2),
        "evidence_quality": round(evidence_quality, 2),
        "novelty": round(novelty, 2),
        "negative_signals": round(negative_signals, 2),
        "feedback_title_boost": round(title_feedback, 2),
        "feedback_role_family_boost": round(role_family_feedback, 2),
        "feedback_domain_boost": round(domain_feedback, 2),
        "feedback_source_penalty": round(source_feedback, 2),
        "rank_label": rank_label,
        "confidence_label": confidence_label,
        "freshness_label": freshness_label,
        "title_fit_label": title_fit_label,
        "qualification_fit_label": qualification_fit_label,
        "matched_profile_fields": list(dict.fromkeys(matched_profile_fields)),
        "role_match_explanation": role_match_explanation,
        "location_fit_explanation": location_fit_explanation,
        "role_family": role_family,
        "search_intent": search_intent.model_dump(),
        "target_roles": search_intent.target_roles,
        "preferred_locations": preferred_locations,
        "work_mode_preference": work_mode_preference,
        "work_mode_match": lead_work_mode if work_mode_preference != "unspecified" else "not_applied",
        "applied_profile_constraints": search_intent.applied_constraints,
        "defaulted_profile_constraints": search_intent.defaulted_constraints,
    }
