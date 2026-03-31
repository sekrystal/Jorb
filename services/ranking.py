from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Optional

from core.models import CandidateProfile
from services.profile import KNOWN_COMPETENCIES, KNOWN_DOMAINS, KNOWN_SKILLS, build_search_intent


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
SOURCE_QUALITY_SCORES = {"greenhouse": 1.2, "ashby": 1.2, "yc_jobs": 0.9, "x": 0.5, "x_signal": 0.5}
SOURCE_DECISION_BONUS = {"greenhouse": 0.55, "ashby": 0.55, "yc_jobs": 0.25, "search_web": 0.1, "x": -0.2, "x_signal": -0.2}
SENIORITY_YEARS_FLOOR = {"entry": 0, "junior": 1, "mid": 3, "senior": 5, "staff": 8, "executive": 10}
SKILL_SYNONYMS = {
    "sql": ("sql", "analytics sql", "reporting"),
    "analytics": ("analytics", "reporting", "dashboards", "metrics"),
    "stakeholder management": ("stakeholder management", "stakeholder alignment", "cross functional", "cross-functional"),
    "cross-functional leadership": ("cross-functional leadership", "cross functional leadership", "leadership"),
    "customer discovery": ("customer discovery", "user research", "customer interviews"),
    "recruiting": ("recruiting", "talent", "hiring"),
    "program management": ("program management", "program manager", "programs"),
    "implementation": ("implementation", "deployment", "onboarding"),
    "process design": ("process design", "process improvement", "operating cadence"),
    "systems thinking": ("systems thinking", "systems design", "operating system"),
    "operator judgment": ("operator judgment", "business judgment", "operating judgment"),
    "zero-to-one execution": ("zero-to-one execution", "0 to 1", "zero to one"),
    "execution": ("execution", "operational execution"),
}


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(cleaned)
    return ordered


def _skill_patterns(value: str) -> tuple[str, ...]:
    normalized = str(value or "").strip().lower()
    return SKILL_SYNONYMS.get(normalized, (normalized,))


def _extract_years_required(title: str, description_text: str = "") -> int | None:
    lowered = f"{title} {description_text}".lower()
    years = [int(item) for item in re.findall(r"(\d+)\+?\s+years", lowered)]
    if years:
        return max(years)
    return None


def _section_texts(listing_metadata: dict[str, Any] | None, heading: str) -> list[str]:
    sections = list((listing_metadata or {}).get("description_sections") or [])
    items: list[str] = []
    for section in sections:
        if str(section.get("heading") or "").strip().lower() != heading.lower():
            continue
        items.extend(str(item).strip() for item in section.get("paragraphs") or [] if str(item).strip())
        items.extend(str(item).strip() for item in section.get("bullets") or [] if str(item).strip())
    return items


def _match_signal_terms(text: str, terms: list[str]) -> list[str]:
    lowered = text.lower()
    matches: list[str] = []
    for term in terms:
        patterns = _skill_patterns(term)
        if any(pattern and pattern in lowered for pattern in patterns):
            matches.append(term)
    return _dedupe_preserve_order(matches)


def _job_signal_snapshot(
    *,
    title: str,
    company_domain: Optional[str],
    description_text: str,
    listing_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    requirements_text = "\n".join(_section_texts(listing_metadata, "Requirements"))
    responsibilities_text = "\n".join(_section_texts(listing_metadata, "Responsibilities"))
    overview_text = "\n".join(_section_texts(listing_metadata, "Overview"))
    searchable = " ".join([title, description_text, requirements_text, responsibilities_text, overview_text])
    required_skill_terms = _match_signal_terms(requirements_text or searchable, KNOWN_SKILLS)
    responsibility_skill_terms = _match_signal_terms(responsibilities_text or searchable, KNOWN_SKILLS)
    competency_terms = _match_signal_terms(searchable, KNOWN_COMPETENCIES)
    domain_terms = _match_signal_terms(" ".join([company_domain or "", searchable]), KNOWN_DOMAINS)
    return {
        "years_required": _extract_years_required(title, description_text),
        "seniority_band": infer_seniority_band(title, description_text),
        "required_skill_terms": required_skill_terms,
        "responsibility_skill_terms": responsibility_skill_terms,
        "competency_terms": competency_terms,
        "domain_terms": domain_terms,
    }


def _resume_alignment(
    *,
    confirmed_skills: list[str],
    competencies: list[str],
    preferred_domains: list[str],
    search_intent,
    title: str,
    company_domain: Optional[str],
    description_text: str,
    listing_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    job_signals = _job_signal_snapshot(
        title=title,
        company_domain=company_domain,
        description_text=description_text,
        listing_metadata=listing_metadata,
    )
    searchable = f"{title} {description_text}"
    required_skill_terms = job_signals["required_skill_terms"]
    responsibility_skill_terms = job_signals["responsibility_skill_terms"]
    competency_terms = job_signals["competency_terms"]
    domain_terms = job_signals["domain_terms"]

    matched_required_skills = [skill for skill in confirmed_skills if skill in _match_signal_terms(" ".join(required_skill_terms), [skill])]
    if not matched_required_skills:
        matched_required_skills = _match_signal_terms(" ".join(required_skill_terms), confirmed_skills)
    matched_supporting_skills = _match_signal_terms(searchable, confirmed_skills)
    matched_competencies = _match_signal_terms(searchable, competencies)
    matched_domains = [domain for domain in preferred_domains if domain.lower() in {item.lower() for item in domain_terms}]

    required_coverage = (
        len(matched_required_skills) / max(len(required_skill_terms), 1)
        if required_skill_terms
        else 0.0
    )
    supporting_coverage = (
        len(set(matched_supporting_skills + matched_competencies))
        / max(len(_dedupe_preserve_order(confirmed_skills + competencies)), 1)
        if (confirmed_skills or competencies)
        else 0.0
    )
    domain_bonus = 0.25 * len(matched_domains)

    years_experience = getattr(search_intent, "years_experience", None)
    years_required = job_signals["years_required"]
    seniority_signal_score = 0.0
    seniority_note = "seniority not explicit"
    if years_experience is not None and years_required is not None:
        delta = years_experience - years_required
        if delta >= 0:
            seniority_signal_score = 0.45 if delta <= 6 else 0.25
            seniority_note = f"resume meets the {years_required}+ year requirement"
        else:
            seniority_signal_score = -0.75 if delta <= -2 else -0.35
            seniority_note = f"resume trails the {years_required}+ year requirement"
    elif search_intent.seniority_guess:
        candidate_band = SENIORITY_ORDER.get(search_intent.seniority_guess or "mid", 2)
        lead_band = SENIORITY_ORDER.get(job_signals["seniority_band"], 2)
        delta = candidate_band - lead_band
        if delta >= 0:
            seniority_signal_score = 0.25
            seniority_note = "seniority band is compatible"
        else:
            seniority_signal_score = -0.4
            seniority_note = "role seniority looks above the profile"

    score = round((required_coverage * 1.8) + (supporting_coverage * 0.9) + domain_bonus + seniority_signal_score, 2)
    top_signals = _dedupe_preserve_order(
        [f"required skill: {skill}" for skill in matched_required_skills]
        + [f"skill: {skill}" for skill in matched_supporting_skills if skill not in matched_required_skills]
        + [f"competency: {competency}" for competency in matched_competencies]
        + [f"domain: {domain}" for domain in matched_domains]
        + ([seniority_note] if seniority_signal_score > 0 else [])
    )[:4]
    missing_signals = _dedupe_preserve_order(
        [f"missing required skill: {skill}" for skill in required_skill_terms if skill not in matched_required_skills]
        + ([seniority_note] if seniority_signal_score < 0 else [])
    )[:4]
    return {
        "score": score,
        "matched_required_skills": matched_required_skills,
        "matched_supporting_skills": matched_supporting_skills,
        "matched_competencies": matched_competencies,
        "matched_domains": matched_domains,
        "top_signals": top_signals,
        "missing_signals": missing_signals,
        "job_signals": job_signals,
    }


def _match_tier(
    *,
    final_score: float,
    qualification_fit_label: str,
    resume_alignment_score: float,
    minimum_fit_threshold: float,
) -> str:
    if qualification_fit_label in {"underqualified", "overqualified"}:
        return "low"
    if final_score >= 7.2 and resume_alignment_score >= 1.2:
        return "high"
    if final_score >= max(minimum_fit_threshold, 3.0) and (
        resume_alignment_score >= 0.35 or (final_score >= 6.5 and resume_alignment_score >= 0.0)
    ):
        return "medium"
    return "low"


def _parse_feedback_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _feedback_event_weight(event: dict[str, Any], *, now: datetime) -> float:
    event_at = _parse_feedback_timestamp(event.get("at"))
    if event_at is None:
        return 0.2
    age_days = max((now - event_at).total_seconds() / 86400.0, 0.0)
    return max(0.15, 1.0 - min(age_days / 45.0, 0.85))


def _feedback_influence(
    *,
    feedback_learning: dict[str, Any],
    title: str,
    company_name: str,
    company_domain: Optional[str],
    role_family: str,
    source_type: str,
    allow_positive: bool,
) -> dict[str, float]:
    now = datetime.now(timezone.utc)
    title_lower = title.lower()
    company_lower = company_name.lower()
    domain_lower = (company_domain or "").lower()
    events = list(feedback_learning.get("feedback_events", []) or [])
    title_delta = 0.0
    role_delta = 0.0
    domain_delta = 0.0
    company_delta = 0.0
    source_delta = 0.0

    positive_actions = {"save": 0.9, "applied": 1.35, "like": 0.55, "more_like_this": 0.85}
    negative_actions = {"dislike": -1.0, "wrong_function": -0.65, "too_senior": -0.6, "too_junior": -0.6, "wrong_geography": -0.45}

    for event in events:
        action = str(event.get("action") or "").strip().lower()
        if action in positive_actions:
            action_scale = positive_actions[action]
        elif action in negative_actions:
            action_scale = negative_actions[action]
        else:
            continue
        if action_scale > 0 and not allow_positive:
            continue
        weight = _feedback_event_weight(event, now=now)
        event_title = str(event.get("title") or "").strip().lower()
        event_company = str(event.get("company_name") or "").strip().lower()
        event_domain = str(event.get("company_domain") or "").strip().lower()
        event_role_family = str(event.get("role_family") or "").strip().lower()
        event_source = str(event.get("source_type") or "").strip().lower()

        if event_title and event_title == title_lower:
            title_delta += action_scale * weight
        if event_role_family and event_role_family == role_family:
            role_delta += action_scale * weight * 0.75
        if event_domain and domain_lower and event_domain == domain_lower:
            domain_delta += action_scale * weight * 0.65
        if event_company and event_company == company_lower:
            company_delta += action_scale * weight * 0.9
        if event_source and event_source == source_type:
            source_delta += action_scale * weight * 0.25

    return {
        "title": round(title_delta, 2),
        "role_family": round(role_delta, 2),
        "domain": round(domain_delta, 2),
        "company": round(company_delta, 2),
        "source": round(source_delta, 2),
    }


def _structured_targeting_values(profile: CandidateProfile, field_name: str) -> list[str]:
    extracted_summary = getattr(profile, "extracted_summary_json", {}) or {}
    structured_profile = extracted_summary.get("structured_profile") if isinstance(extracted_summary.get("structured_profile"), dict) else {}
    targeting = structured_profile.get("targeting") if isinstance(structured_profile.get("targeting"), dict) else {}
    values = targeting.get(field_name) or []
    return [str(value).strip() for value in values if str(value).strip()]


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
    listing_metadata: dict[str, Any] | None = None,
    feedback_learning: Optional[dict] = None,
) -> dict:
    feedback_learning = feedback_learning or {}
    search_intent = build_search_intent(profile)
    role_family = infer_role_family(title, description_text)
    title_fit_label, title_fit_score, matched_title_fields = classify_title_fit(profile, title, description_text)
    qualification_fit_label, qualification_score, qualification_reasons = classify_qualification_fit(profile, title, description_text)
    matched_profile_fields = list(dict.fromkeys([*matched_title_fields, *qualification_reasons]))
    confirmed_skills = _structured_targeting_values(profile, "confirmed_skills")
    competencies = _structured_targeting_values(profile, "competencies")
    preferred_domains = [str(item).strip() for item in (profile.preferred_domains_json or []) if str(item).strip()]
    preferred_locations = search_intent.preferred_locations
    work_mode_preference = search_intent.work_mode_preference
    lead_work_mode = infer_work_mode(location, description_text)
    searchable_text = f"{title} {description_text}".lower()

    resume_alignment = _resume_alignment(
        confirmed_skills=confirmed_skills,
        competencies=competencies,
        preferred_domains=preferred_domains,
        search_intent=search_intent,
        title=title,
        company_domain=company_domain,
        description_text=description_text,
        listing_metadata=listing_metadata,
    )
    skill_matches = resume_alignment["matched_supporting_skills"]
    competency_matches = resume_alignment["matched_competencies"]
    skill_fit = resume_alignment["score"]
    if skill_matches:
        matched_profile_fields.extend([f"confirmed skill: {skill}" for skill in skill_matches])
    if competency_matches:
        matched_profile_fields.extend([f"competency: {competency}" for competency in competency_matches])
    if resume_alignment["matched_domains"]:
        matched_profile_fields.extend([f"preferred domain: {domain}" for domain in resume_alignment["matched_domains"]])

    freshness_score = {"fresh": 1.6, "recent": 1.0, "stale": -1.2, "unknown": -0.5}[freshness_label]
    source_quality = SOURCE_QUALITY_SCORES.get(source_type, 0.6)
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
    company_signal_fit = 0.0
    if company_domain and any(item.lower() in company_domain.lower() for item in (profile.preferred_domains_json or [])):
        company_signal_fit += 0.45
    company_signal_fit += SOURCE_DECISION_BONUS.get(source_type, 0.0)
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

    base_composite = round(
        freshness_score
        + title_fit_score
        + role_family_fit
        + domain_fit
        + location_fit
        + stage_fit
        + company_signal_fit
        + source_quality
        + evidence_quality
        + novelty
        + skill_fit
        + qualification_score
        + negative_signals,
        2,
    )
    allow_positive_feedback = _match_tier(
        final_score=base_composite,
        qualification_fit_label=qualification_fit_label,
        resume_alignment_score=resume_alignment["score"],
        minimum_fit_threshold=profile.minimum_fit_threshold,
    ) != "low"

    title_weights = feedback_learning.get("title_weights", {})
    role_family_weights = feedback_learning.get("role_family_weights", {})
    domain_weights = feedback_learning.get("domain_weights", {})
    source_penalties = feedback_learning.get("source_penalties", {})
    company_penalties = feedback_learning.get("company_penalties", {})
    title_feedback = title_weights.get(title.lower(), 0.0) if allow_positive_feedback else min(title_weights.get(title.lower(), 0.0), 0.0)
    role_family_feedback = (
        role_family_weights.get(role_family, 0.0) if allow_positive_feedback else min(role_family_weights.get(role_family, 0.0), 0.0)
    )
    domain_feedback = (
        domain_weights.get((company_domain or "").lower(), 0.0)
        if allow_positive_feedback
        else min(domain_weights.get((company_domain or "").lower(), 0.0), 0.0)
    )
    source_feedback = -source_penalties.get(source_type, 0.0)
    company_feedback = 0.0
    company_penalty = -max(company_penalties.get(company_name.lower(), 0.0), 0.0)

    event_feedback = _feedback_influence(
        feedback_learning=feedback_learning,
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        role_family=role_family,
        source_type=source_type,
        allow_positive=allow_positive_feedback,
    )
    title_feedback += event_feedback["title"]
    role_family_feedback += event_feedback["role_family"]
    domain_feedback += event_feedback["domain"]
    company_feedback += event_feedback["company"]
    source_feedback += event_feedback["source"]

    positive_feedback_total = sum(max(value, 0.0) for value in [title_feedback, role_family_feedback, domain_feedback])
    positive_feedback_cap = 1.5 if allow_positive_feedback else 0.0
    if positive_feedback_total > positive_feedback_cap:
        scale = positive_feedback_cap / positive_feedback_total
        title_feedback = title_feedback * scale if title_feedback > 0 else title_feedback
        role_family_feedback = role_family_feedback * scale if role_family_feedback > 0 else role_family_feedback
        domain_feedback = domain_feedback * scale if domain_feedback > 0 else domain_feedback
    if company_feedback > 0:
        company_feedback = min(company_feedback, 0.7 if allow_positive_feedback else 0.0)

    composite = round(
        base_composite
        + title_feedback
        + role_family_feedback
        + domain_feedback
        + company_feedback
        + company_penalty
        + source_feedback
        ,
        2,
    )

    match_tier = _match_tier(
        final_score=composite,
        qualification_fit_label=qualification_fit_label,
        resume_alignment_score=resume_alignment["score"],
        minimum_fit_threshold=profile.minimum_fit_threshold,
    )
    rank_label = "strong" if match_tier == "high" else "medium" if match_tier == "medium" else "weak"
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
        "company_signal_fit": round(company_signal_fit, 2),
        "source_quality": round(source_quality, 2),
        "evidence_quality": round(evidence_quality, 2),
        "novelty": round(novelty, 2),
        "skill_fit": round(skill_fit, 2),
        "negative_signals": round(negative_signals, 2),
        "feedback_title_boost": round(title_feedback, 2),
        "feedback_role_family_boost": round(role_family_feedback, 2),
        "feedback_domain_boost": round(domain_feedback, 2),
        "feedback_company_boost": round(company_feedback, 2),
        "feedback_company_penalty": round(company_penalty, 2),
        "feedback_source_penalty": round(source_feedback, 2),
        "rank_label": rank_label,
        "match_tier": match_tier,
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
        "skill_match_terms": skill_matches,
        "competency_match_terms": competency_matches,
        "top_matching_signals": resume_alignment["top_signals"],
        "missing_signals": resume_alignment["missing_signals"],
        "required_skill_terms": resume_alignment["job_signals"]["required_skill_terms"],
        "years_required": resume_alignment["job_signals"]["years_required"],
        "years_experience": getattr(search_intent, "years_experience", None),
        "applied_profile_constraints": search_intent.applied_constraints,
        "defaulted_profile_constraints": search_intent.defaulted_constraints,
    }
