from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field
from pydantic import model_validator


FeedbackAction = Literal[
    "like",
    "dislike",
    "seen",
    "restore",
    "save",
    "applied",
    "mute_company",
    "mute_title_pattern",
    "too_senior",
    "too_junior",
    "wrong_function",
    "wrong_geography",
    "irrelevant_company",
    "more_like_this",
]


RECOMMENDATION_COMPONENT_SEMANTICS: dict[str, str] = {
    "freshness": "Rewards recent, still-live opportunities and penalizes stale ones.",
    "title_fit": "Measures how closely the role title aligns with the candidate's target scope.",
    "role_family_fit": "Rewards role families that match the candidate's operating lanes.",
    "domain_fit": "Rewards company domains that align with stated candidate preferences.",
    "location_fit": "Rewards locations that match the candidate's preferred working geography.",
    "stage_company_fit": "Rewards company stage or context signals that match the candidate's stated preferences.",
    "source_quality": "Rewards sources with stronger verification and cleaner job normalization.",
    "evidence_quality": "Rewards leads backed by multiple concrete evidence points.",
    "novelty": "Rewards weak-signal or combined discovery paths that may expose less obvious opportunities.",
    "negative_signals": "Captures penalties from stale status, muted companies, and exclusion rules.",
    "feedback_title_boost": "Applies learned boosts from positive feedback on similar titles.",
    "feedback_role_family_boost": "Applies learned boosts from positive feedback on similar role families.",
    "feedback_domain_boost": "Applies learned boosts from positive feedback on similar company domains.",
    "feedback_source_penalty": "Applies learned penalties to lower-value source types.",
}

RECOMMENDATION_COMPONENT_LABELS: dict[str, str] = {
    "freshness": "Freshness",
    "title_fit": "Title alignment",
    "role_family_fit": "Role family alignment",
    "domain_fit": "Domain alignment",
    "location_fit": "Location alignment",
    "stage_company_fit": "Stage alignment",
    "source_quality": "Source quality",
    "evidence_quality": "Evidence quality",
    "novelty": "Novelty",
    "negative_signals": "Negative signals",
    "feedback_title_boost": "Feedback title boost",
    "feedback_role_family_boost": "Feedback role-family boost",
    "feedback_domain_boost": "Feedback domain boost",
    "feedback_source_penalty": "Feedback source penalty",
}

RECOMMENDATION_COMPONENT_ORDER = [
    "freshness",
    "title_fit",
    "role_family_fit",
    "domain_fit",
    "location_fit",
    "stage_company_fit",
    "source_quality",
    "evidence_quality",
    "novelty",
    "negative_signals",
    "feedback_title_boost",
    "feedback_role_family_boost",
    "feedback_domain_boost",
    "feedback_source_penalty",
]


class RecommendationScoreComponent(BaseModel):
    key: str
    label: str
    score: float
    semantics: str
    trace_inputs: list[str] = Field(default_factory=list)


class RecommendationScoreExplanation(BaseModel):
    headline: str
    summary: str
    supporting_points: list[str] = Field(default_factory=list)


class RecommendationScoreSchema(BaseModel):
    schema_version: str = "v1"
    final_score: float = 0.0
    recommendation_band: str = "weak"
    confidence_label: str = "low"
    title_fit_label: str = "unclear"
    qualification_fit_label: str = "unclear"
    role_family: str = "generalist"
    action_label: str = "Wait"
    action_explanation: str = ""
    component_metrics: list[RecommendationScoreComponent] = Field(default_factory=list)
    trace_inputs: dict[str, Any] = Field(default_factory=dict)
    explanation: RecommendationScoreExplanation


def _format_trace_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item not in {None, ""}) or "none"
    if value in {None, ""}:
        return "none"
    return str(value)


def _component_trace_inputs(component_key: str, raw_score: dict[str, Any], evidence: dict[str, Any], labels: dict[str, Any]) -> list[str]:
    trace_by_component = {
        "freshness": [
            f"freshness_label={_format_trace_value(labels.get('freshness_label'))}",
            f"listing_status={_format_trace_value(evidence.get('listing_status'))}",
            f"freshness_days={_format_trace_value(evidence.get('freshness_days'))}",
        ],
        "title_fit": [
            f"title_fit_label={_format_trace_value(labels.get('title_fit_label'))}",
            f"matched_profile_fields={_format_trace_value(evidence.get('matched_profile_fields'))}",
        ],
        "role_family_fit": [
            f"role_family={_format_trace_value(raw_score.get('role_family'))}",
            f"matched_profile_fields={_format_trace_value(evidence.get('matched_profile_fields'))}",
        ],
        "domain_fit": [
            f"company_domain={_format_trace_value(evidence.get('company_domain'))}",
        ],
        "location_fit": [
            f"location={_format_trace_value(evidence.get('location'))}",
            f"location_scope={_format_trace_value(evidence.get('location_scope'))}",
            f"work_mode_preference={_format_trace_value(evidence.get('work_mode_preference'))}",
            f"work_mode_match={_format_trace_value(evidence.get('work_mode_match'))}",
        ],
        "stage_company_fit": [
            f"feedback_notes={_format_trace_value(evidence.get('feedback_notes'))}",
        ],
        "source_quality": [
            f"source_platform={_format_trace_value(evidence.get('source_platform') or evidence.get('source_type'))}",
            f"source_lineage={_format_trace_value(evidence.get('source_lineage'))}",
        ],
        "evidence_quality": [
            f"matched_profile_fields={_format_trace_value(evidence.get('matched_profile_fields'))}",
            f"resolution_story={_format_trace_value(evidence.get('resolution_story'))}",
        ],
        "novelty": [
            f"lead_type={_format_trace_value(evidence.get('lead_type'))}",
            f"discovery_source={_format_trace_value(evidence.get('discovery_source'))}",
        ],
        "negative_signals": [
            f"qualification_fit_label={_format_trace_value(labels.get('qualification_fit_label'))}",
            f"listing_status={_format_trace_value(evidence.get('listing_status'))}",
        ],
        "feedback_title_boost": [
            f"feedback_notes={_format_trace_value(evidence.get('feedback_notes'))}",
        ],
        "feedback_role_family_boost": [
            f"role_family={_format_trace_value(raw_score.get('role_family'))}",
            f"feedback_notes={_format_trace_value(evidence.get('feedback_notes'))}",
        ],
        "feedback_domain_boost": [
            f"company_domain={_format_trace_value(evidence.get('company_domain'))}",
            f"feedback_notes={_format_trace_value(evidence.get('feedback_notes'))}",
        ],
        "feedback_source_penalty": [
            f"source_platform={_format_trace_value(evidence.get('source_platform') or evidence.get('source_type'))}",
            f"feedback_notes={_format_trace_value(evidence.get('feedback_notes'))}",
        ],
    }
    return [item for item in trace_by_component.get(component_key, []) if not item.endswith("=none")]


def _build_recommendation_explanation(
    raw_score: dict[str, Any],
    component_metrics: list[dict[str, Any]],
    explanation_text: str | None,
    labels: dict[str, Any],
) -> RecommendationScoreExplanation:
    final_score = float(raw_score.get("final_score", raw_score.get("composite", 0.0)) or 0.0)
    recommendation_band = str(raw_score.get("recommendation_band", raw_score.get("rank_label", "weak")) or "weak")
    confidence_label = str(labels.get("confidence_label", raw_score.get("confidence_label", "low")) or "low")
    top_positive = next((component for component in component_metrics if component["score"] > 0), None)
    top_negative = next((component for component in reversed(component_metrics) if component["score"] < 0), None)
    headline = f"{recommendation_band.title()} recommendation at {final_score:.2f} with {confidence_label} confidence."
    summary_parts = [explanation_text or "Recommendation assembled from deterministic component metrics."]
    if top_positive:
        summary_parts.append(f"Top positive driver: {top_positive['label']} ({top_positive['score']:+.2f}).")
    if top_negative:
        summary_parts.append(f"Top negative driver: {top_negative['label']} ({top_negative['score']:+.2f}).")
    supporting_points = [
        f"Title fit: {labels.get('title_fit_label', raw_score.get('title_fit_label', 'unclear'))}",
        f"Qualification fit: {labels.get('qualification_fit_label', raw_score.get('qualification_fit_label', 'unclear'))}",
    ]
    return RecommendationScoreExplanation(
        headline=headline,
        summary=" ".join(part.strip() for part in summary_parts if part and str(part).strip()),
        supporting_points=supporting_points,
    )


def _component_metric_lookup(component_metrics: list[dict[str, Any]], key: str) -> float:
    for component in component_metrics:
        if component.get("key") == key:
            return float(component.get("score") or 0.0)
    return 0.0


def _recommendation_action_guidance(
    raw_score: dict[str, Any],
    component_metrics: list[dict[str, Any]],
    labels: dict[str, Any],
    evidence: dict[str, Any],
) -> tuple[str, str]:
    final_score = float(raw_score.get("final_score", raw_score.get("composite", 0.0)) or 0.0)
    recommendation_band = str(raw_score.get("recommendation_band", raw_score.get("rank_label", "weak")) or "weak")
    qualification_fit = str(labels.get("qualification_fit_label", raw_score.get("qualification_fit_label", "unclear")) or "unclear")
    freshness_label = str(labels.get("freshness_label", raw_score.get("freshness_label", "unknown")) or "unknown")
    lead_type = str(evidence.get("lead_type") or "listing")
    source_quality = _component_metric_lookup(component_metrics, "source_quality")
    novelty = _component_metric_lookup(component_metrics, "novelty")

    positive_components = [component for component in component_metrics if float(component.get("score") or 0.0) > 0]
    positive_components.sort(key=lambda component: float(component.get("score") or 0.0), reverse=True)
    top_positive = ", ".join(
        f"{component.get('label') or component.get('key')} {float(component.get('score') or 0.0):+.2f}"
        for component in positive_components[:2]
    )
    negative_component = next(
        (
            component
            for component in sorted(component_metrics, key=lambda item: float(item.get("score") or 0.0))
            if float(component.get("score") or 0.0) < 0
        ),
        None,
    )
    negative_text = ""
    if negative_component:
        negative_text = (
            f" Biggest drag: {(negative_component.get('label') or negative_component.get('key'))} "
            f"{float(negative_component.get('score') or 0.0):+.2f}."
        )

    if qualification_fit in {"underqualified", "overqualified"} or recommendation_band == "weak" or final_score < 3.0:
        action_label = "Skip"
        action_explanation = (
            f"Skip because the final score is {final_score:.2f}, band is {recommendation_band}, "
            f"and qualification fit is {qualification_fit}."
        )
    elif freshness_label == "stale":
        action_label = "Wait"
        action_explanation = (
            f"Wait because the final score is {final_score:.2f}, but freshness is {freshness_label}; "
            f"recheck before spending application effort."
        )
    elif lead_type == "signal" or (novelty > 0 and source_quality < 1.0):
        action_label = "Seek referral"
        action_explanation = (
            f"Seek referral because the final score is {final_score:.2f} and discovery leans on weaker source signals "
            f"(novelty {novelty:+.2f}, source quality {source_quality:+.2f})."
        )
    else:
        action_label = "Act now"
        action_explanation = (
            f"Act now because the final score is {final_score:.2f} with {recommendation_band} support and "
            f"{freshness_label} timing."
        )

    if top_positive:
        action_explanation = f"{action_explanation} Strongest signals: {top_positive}."
    return action_label, f"{action_explanation}{negative_text}"


def normalize_recommendation_score_schema(
    raw_score: dict[str, Any] | None,
    *,
    explanation_text: str | None = None,
    evidence: dict[str, Any] | None = None,
    labels: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_score = dict(raw_score or {})
    evidence = dict(evidence or {})
    labels = dict(labels or {})
    evidence.setdefault("lead_type", evidence.get("lead_type"))

    component_metrics: list[dict[str, Any]] = []
    for key in RECOMMENDATION_COMPONENT_ORDER:
        if key not in raw_score:
            continue
        component_metrics.append(
            RecommendationScoreComponent(
                key=key,
                label=RECOMMENDATION_COMPONENT_LABELS.get(key, key.replace("_", " ").title()),
                score=float(raw_score.get(key) or 0.0),
                semantics=RECOMMENDATION_COMPONENT_SEMANTICS.get(key, "Contributes to the deterministic recommendation score."),
                trace_inputs=_component_trace_inputs(key, raw_score, evidence, labels),
            ).model_dump()
        )

    normalized = dict(raw_score)
    normalized["schema_version"] = str(raw_score.get("schema_version") or "v1")
    normalized["final_score"] = float(raw_score.get("final_score", raw_score.get("composite", 0.0)) or 0.0)
    normalized["recommendation_band"] = str(raw_score.get("recommendation_band", raw_score.get("rank_label", "weak")) or "weak")
    normalized["confidence_label"] = str(labels.get("confidence_label", raw_score.get("confidence_label", "low")) or "low")
    normalized["title_fit_label"] = str(labels.get("title_fit_label", raw_score.get("title_fit_label", "unclear")) or "unclear")
    normalized["qualification_fit_label"] = str(
        labels.get("qualification_fit_label", raw_score.get("qualification_fit_label", "unclear")) or "unclear"
    )
    normalized["role_family"] = str(raw_score.get("role_family") or "generalist")
    action_label, action_explanation = _recommendation_action_guidance(raw_score, component_metrics, labels, evidence)
    normalized["action_label"] = action_label
    normalized["action_explanation"] = action_explanation
    normalized["component_metrics"] = component_metrics
    normalized["trace_inputs"] = {
        "matched_profile_fields": list(evidence.get("matched_profile_fields") or []),
        "feedback_notes": list(evidence.get("feedback_notes") or []),
        "source_platform": evidence.get("source_platform") or evidence.get("source_type"),
        "source_lineage": evidence.get("source_lineage"),
        "listing_status": evidence.get("listing_status"),
        "freshness_label": labels.get("freshness_label", raw_score.get("freshness_label")),
        "target_roles": list(evidence.get("target_roles") or []),
        "work_mode_preference": evidence.get("work_mode_preference"),
        "profile_constraints_applied": list(evidence.get("profile_constraints_applied") or []),
        "profile_constraints_defaulted": list(evidence.get("profile_constraints_defaulted") or []),
    }
    normalized["explanation"] = _build_recommendation_explanation(raw_score, component_metrics, explanation_text, labels).model_dump()
    RecommendationScoreSchema(**normalized)
    return normalized


class SignalRecord(BaseModel):
    source_type: str
    source_url: str
    author_handle: Optional[str] = None
    raw_text: str
    published_at: Optional[datetime] = None
    company_guess: Optional[str] = None
    role_guess: Optional[str] = None
    location_guess: Optional[str] = None
    hiring_confidence: float = 0.0
    signal_status: str = "new"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class CanonicalJobRecord(BaseModel):
    schema_version: str = "v1"
    url: str
    company: str
    title: str
    location: str
    source_type: str
    identity_key: str = ""
    company_key: str = ""
    role_key: str = ""
    location_key: str = ""


class ListingRecord(BaseModel):
    company_name: str
    company_domain: Optional[str] = None
    careers_url: Optional[str] = None
    company_id: Optional[int] = None
    title: str
    location: Optional[str] = None
    url: str
    source_type: str
    posted_at: Optional[datetime] = None
    first_published_at: Optional[datetime] = None
    discovered_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    description_text: Optional[str] = None
    listing_status: str = "unknown"
    expiration_confidence: float = 0.0
    freshness_hours: Optional[float] = None
    freshness_days: Optional[int] = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    canonical_job: Optional[CanonicalJobRecord] = None

    @model_validator(mode="after")
    def normalize_canonical_job_schema(self) -> "ListingRecord":
        def _normalize_company(value: str) -> str:
            cleaned = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
            suffixes = {"inc", "llc", "ltd", "corp", "corporation", "company", "co"}
            parts = [part for part in cleaned.split() if part not in suffixes]
            return "-".join(parts) or "unknown-company"

        def _normalize_role(value: str) -> str:
            lowered = value.lower()
            replacements = {
                "sr": "senior",
                "mgr": "manager",
                "pm": "product manager",
                "&": " and ",
            }
            for source, target in replacements.items():
                lowered = lowered.replace(source, target)
            cleaned = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
            stopwords = {"the", "a", "an"}
            parts = [part for part in cleaned.split() if part not in stopwords]
            return "-".join(parts) or "unknown-role"

        def _normalize_location(value: str) -> str:
            lowered = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
            if not lowered:
                return "unspecified"
            if "remote" in lowered:
                return "remote"
            replacements = {
                "san francisco": "san-francisco",
                "new york city": "new-york",
                "new york": "new-york",
                "nyc": "new-york",
                "bay area": "bay-area",
                "united states": "us",
            }
            for source, target in replacements.items():
                lowered = lowered.replace(source, target)
            return "-".join(lowered.split()) or "unspecified"

        self.company_name = str(self.company_name or "").strip() or "Unknown Company"
        self.title = str(self.title or "").strip() or "Untitled Role"
        self.url = str(self.url or "").strip()
        self.source_type = str(self.source_type or "").strip() or "unknown"
        self.location = str(self.location or "").strip() or "Unspecified"
        self.metadata_json = dict(self.metadata_json or {})
        if not self.url:
            raise ValueError("ListingRecord.url is required")

        company_key = _normalize_company(self.company_name)
        role_key = _normalize_role(self.title)
        location_key = _normalize_location(self.location)
        self.canonical_job = CanonicalJobRecord(
            url=self.url,
            company=self.company_name,
            title=self.title,
            location=self.location,
            source_type=self.source_type,
            identity_key=f"{company_key}::{role_key}::{location_key}",
            company_key=company_key,
            role_key=role_key,
            location_key=location_key,
        )
        self.metadata_json["canonical_job"] = self.canonical_job.model_dump()
        return self


class CandidateProfilePayload(BaseModel):
    profile_schema_version: str = "v1"
    name: str = "Demo Candidate"
    raw_resume_text: str = ""
    extracted_summary_json: dict[str, Any] = Field(default_factory=dict)
    preferred_titles_json: list[str] = Field(default_factory=list)
    adjacent_titles_json: list[str] = Field(default_factory=list)
    excluded_titles_json: list[str] = Field(default_factory=list)
    preferred_domains_json: list[str] = Field(default_factory=list)
    excluded_companies_json: list[str] = Field(default_factory=list)
    preferred_locations_json: list[str] = Field(default_factory=list)
    target_roles_json: list[str] = Field(default_factory=list)
    work_mode_preference: str = "unspecified"
    confirmed_skills_json: list[str] = Field(default_factory=list)
    competencies_json: list[str] = Field(default_factory=list)
    explicit_preferences_json: list[str] = Field(default_factory=list)
    seniority_guess: Optional[str] = None
    years_experience: Optional[int] = None
    stage_preferences_json: list[str] = Field(default_factory=list)
    core_titles_json: list[str] = Field(default_factory=list)
    excluded_keywords_json: list[str] = Field(default_factory=list)
    min_seniority_band: str = "mid"
    max_seniority_band: str = "senior"
    stretch_role_families_json: list[str] = Field(default_factory=list)
    minimum_fit_threshold: float = 2.8
    structured_profile_json: Optional["StructuredCandidateProfile"] = None

    @model_validator(mode="after")
    def sync_structured_profile(self) -> "CandidateProfilePayload":
        if self.structured_profile_json is None:
            self.structured_profile_json = StructuredCandidateProfile(
                version=self.profile_schema_version,
                targeting=ProfileTargetingPreferences(
                    preferred_titles=self.preferred_titles_json,
                    core_titles=self.core_titles_json,
                    adjacent_titles=self.adjacent_titles_json,
                    excluded_titles=self.excluded_titles_json,
                    preferred_domains=self.preferred_domains_json,
                    preferred_locations=self.preferred_locations_json,
                    target_roles=self.target_roles_json,
                    work_mode_preference=self.work_mode_preference,
                    excluded_companies=self.excluded_companies_json,
                    confirmed_skills=self.confirmed_skills_json,
                    competencies=self.competencies_json,
                    explicit_preferences=self.explicit_preferences_json,
                    stage_preferences=self.stage_preferences_json,
                    stretch_role_families=self.stretch_role_families_json,
                    excluded_keywords=self.excluded_keywords_json,
                    seniority=ProfileSeniorityPreferences(
                        guess=self.seniority_guess,
                        years_experience=self.years_experience,
                        minimum_band=self.min_seniority_band,
                        maximum_band=self.max_seniority_band,
                    ),
                ),
                scoring=ProfileScoringPreferences(minimum_fit_threshold=self.minimum_fit_threshold),
            )
        else:
            self.profile_schema_version = self.structured_profile_json.version
            targeting = self.structured_profile_json.targeting
            provided_fields = getattr(self, "model_fields_set", set())
            if "preferred_locations_json" in provided_fields:
                targeting.preferred_locations = list(self.preferred_locations_json)
            if "target_roles_json" in provided_fields:
                targeting.target_roles = list(self.target_roles_json)
            if "work_mode_preference" in provided_fields:
                targeting.work_mode_preference = self.work_mode_preference
            self.preferred_titles_json = list(targeting.preferred_titles)
            self.core_titles_json = list(targeting.core_titles)
            self.adjacent_titles_json = list(targeting.adjacent_titles)
            self.excluded_titles_json = list(targeting.excluded_titles)
            self.preferred_domains_json = list(targeting.preferred_domains)
            self.preferred_locations_json = list(targeting.preferred_locations)
            self.target_roles_json = list(targeting.target_roles)
            self.work_mode_preference = targeting.work_mode_preference
            self.excluded_companies_json = list(targeting.excluded_companies)
            self.confirmed_skills_json = list(targeting.confirmed_skills)
            self.competencies_json = list(targeting.competencies)
            self.explicit_preferences_json = list(targeting.explicit_preferences)
            self.stage_preferences_json = list(targeting.stage_preferences)
            self.stretch_role_families_json = list(targeting.stretch_role_families)
            self.excluded_keywords_json = list(targeting.excluded_keywords)
            self.seniority_guess = targeting.seniority.guess
            self.years_experience = targeting.seniority.years_experience
            self.min_seniority_band = targeting.seniority.minimum_band
            self.max_seniority_band = targeting.seniority.maximum_band
            self.minimum_fit_threshold = self.structured_profile_json.scoring.minimum_fit_threshold
        return self


class ProfileSeniorityPreferences(BaseModel):
    guess: Optional[str] = None
    years_experience: Optional[int] = None
    minimum_band: str = "mid"
    maximum_band: str = "senior"


class ProfileTargetingPreferences(BaseModel):
    preferred_titles: list[str] = Field(default_factory=list)
    core_titles: list[str] = Field(default_factory=list)
    adjacent_titles: list[str] = Field(default_factory=list)
    excluded_titles: list[str] = Field(default_factory=list)
    preferred_domains: list[str] = Field(default_factory=list)
    preferred_locations: list[str] = Field(default_factory=list)
    target_roles: list[str] = Field(default_factory=list)
    work_mode_preference: str = "unspecified"
    excluded_companies: list[str] = Field(default_factory=list)
    confirmed_skills: list[str] = Field(default_factory=list)
    competencies: list[str] = Field(default_factory=list)
    explicit_preferences: list[str] = Field(default_factory=list)
    stage_preferences: list[str] = Field(default_factory=list)
    stretch_role_families: list[str] = Field(default_factory=list)
    excluded_keywords: list[str] = Field(default_factory=list)
    seniority: ProfileSeniorityPreferences = Field(default_factory=ProfileSeniorityPreferences)


class ProfileScoringPreferences(BaseModel):
    minimum_fit_threshold: float = 2.8


class SearchIntent(BaseModel):
    target_roles: list[str] = Field(default_factory=list)
    preferred_locations: list[str] = Field(default_factory=list)
    work_mode_preference: str = "unspecified"
    seniority_guess: Optional[str] = None
    years_experience: Optional[int] = None
    min_seniority_band: str = "mid"
    max_seniority_band: str = "senior"
    applied_constraints: list[str] = Field(default_factory=list)
    defaulted_constraints: list[str] = Field(default_factory=list)
    explicit_target_roles: list[str] = Field(default_factory=list)
    explicit_preferred_locations: list[str] = Field(default_factory=list)
    explicit_work_mode: bool = False


class StructuredCandidateProfile(BaseModel):
    version: str = "v1"
    targeting: ProfileTargetingPreferences = Field(default_factory=ProfileTargetingPreferences)
    scoring: ProfileScoringPreferences = Field(default_factory=ProfileScoringPreferences)
    search_intent: Optional[SearchIntent] = None

    @model_validator(mode="after")
    def ensure_search_intent(self) -> "StructuredCandidateProfile":
        if self.search_intent is None:
            self.search_intent = SearchIntent(
                target_roles=list(self.targeting.target_roles),
                preferred_locations=list(self.targeting.preferred_locations),
                work_mode_preference=self.targeting.work_mode_preference,
                seniority_guess=self.targeting.seniority.guess,
                years_experience=self.targeting.seniority.years_experience,
                min_seniority_band=self.targeting.seniority.minimum_band,
                max_seniority_band=self.targeting.seniority.maximum_band,
            )
        return self


class ResumeUploadRequest(BaseModel):
    filename: str
    raw_text: str


class ResumeUploadResponse(BaseModel):
    resume_document_id: int
    candidate_profile: CandidateProfilePayload
    warnings: list[str] = Field(default_factory=list)


class FeedbackRequest(BaseModel):
    lead_id: int
    action: FeedbackAction
    subtype: Optional[str] = None
    reason: Optional[str] = None
    pattern: Optional[str] = None


class ApplicationStatusUpdate(BaseModel):
    lead_id: int
    current_status: str
    status_reason_code: Optional[str] = None
    outcome_code: Optional[str] = None
    outcome_reason_code: Optional[str] = None
    notes: Optional[str] = None
    date_applied: Optional[datetime] = None

    @model_validator(mode="after")
    def normalize_tracker_fields(self) -> "ApplicationStatusUpdate":
        self.current_status = self.current_status.strip()
        self.status_reason_code = (self.status_reason_code or "").strip() or None
        self.outcome_code = (self.outcome_code or "").strip() or None
        self.outcome_reason_code = (self.outcome_reason_code or "").strip() or None
        self.notes = (self.notes or "").strip() or None
        return self


class LeadResponse(BaseModel):
    id: int
    lead_type: str
    company_name: str
    primary_title: str
    url: Optional[str] = None
    source_type: str
    listing_status: Optional[str] = None
    first_published_at: Optional[datetime] = None
    discovered_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    freshness_hours: Optional[float] = None
    freshness_days: Optional[int] = None
    posted_at: Optional[datetime] = None
    surfaced_at: datetime
    rank_label: str
    confidence_label: str
    freshness_label: str
    title_fit_label: str
    qualification_fit_label: str
    source_platform: str
    source_provenance: Optional[str] = None
    source_lineage: Optional[str] = None
    discovery_source: Optional[str] = None
    saved: bool = False
    seen: bool = False
    applied: bool = False
    current_status: Optional[str] = None
    status_reason_code: Optional[str] = None
    outcome_code: Optional[str] = None
    outcome_reason_code: Optional[str] = None
    date_saved: Optional[datetime] = None
    date_applied: Optional[datetime] = None
    application_notes: Optional[str] = None
    application_updated_at: Optional[datetime] = None
    next_action: Optional[str] = None
    follow_up_due: bool = False
    explanation: Optional[str] = None
    last_agent_action: Optional[str] = None
    hidden: bool
    score_breakdown_json: dict[str, Any]
    evidence_json: dict[str, Any]

    @model_validator(mode="after")
    def normalize_recommendation_score(self) -> "LeadResponse":
        self.score_breakdown_json = normalize_recommendation_score_schema(
            self.score_breakdown_json,
            explanation_text=self.explanation,
            evidence={**(self.evidence_json or {}), "lead_type": self.lead_type},
            labels={
                "freshness_label": self.freshness_label,
                "confidence_label": self.confidence_label,
                "title_fit_label": self.title_fit_label,
                "qualification_fit_label": self.qualification_fit_label,
            },
        )
        return self


class LeadsResponse(BaseModel):
    items: list[LeadResponse]
    search_meta: Optional[dict[str, Any]] = None


class SyncResult(BaseModel):
    signals_ingested: int
    listings_ingested: int
    leads_created: int
    leads_updated: int
    rechecks_queued: int
    live_mode_used: bool
    discovery_metrics: dict[str, dict[str, int]] = Field(default_factory=dict)
    surfaced_count: int = 0
    discovery_summary: Optional[str] = None
    discovery_status: dict[str, Any] = Field(default_factory=dict)


class StatsResponse(BaseModel):
    total_leads: int
    visible_leads: int
    active_listings: int
    fresh_listings: int
    combined_leads: int
    signal_only_leads: int
    saved_leads: int
    applied_leads: int
    pending_rechecks: int


class LearningSummary(BaseModel):
    boosted_titles: list[str] = Field(default_factory=list)
    boosted_domains: list[str] = Field(default_factory=list)
    penalized_sources: list[str] = Field(default_factory=list)
    generated_queries: list[str] = Field(default_factory=list)


class SourceQueryResponse(BaseModel):
    id: int
    query_text: str
    source_type: str
    status: str
    performance_stats_json: dict[str, Any]


class ApplicationsResponse(BaseModel):
    items: list[LeadResponse]


class AgentActivityResponse(BaseModel):
    id: int
    timestamp: datetime
    agent_name: str
    action: str
    target_type: Optional[str] = None
    target_count: Optional[int] = None
    target_entity: Optional[str] = None
    result_summary: str


class AgentActivitiesResponse(BaseModel):
    items: list[AgentActivityResponse]


class AgentRunRequest(BaseModel):
    agent: Literal["scout", "resolver", "fit", "ranker", "critic", "tracker", "learning", "full_pipeline", "reset_demo"]


class AgentRunResponse(BaseModel):
    status: str = "ok"
    agent: str
    summary: str


class AutonomyHealthResponse(BaseModel):
    last_successful_run_at: Optional[datetime] = None
    last_failed_run_at: Optional[datetime] = None
    latest_success_summary: Optional[str] = None
    latest_failure_summary: Optional[str] = None
    open_investigations: int = 0
    suppressed_leads: int = 0
    due_follow_ups: int = 0
    scheduler_enabled: bool = False
    runtime_state: str = "paused"
    worker_state: str = "idle"
    runtime_phase: str = "paused"
    run_once_requested: bool = False
    last_cycle_started_at: Optional[datetime] = None
    last_successful_cycle_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    sleep_until: Optional[datetime] = None
    next_cycle_at: Optional[datetime] = None
    current_interval_seconds: int = 0
    status_message: Optional[str] = None
    last_control_action: Optional[str] = None
    last_control_at: Optional[datetime] = None
    operator_hints: list[str] = Field(default_factory=list)


class RuntimeControlResponse(BaseModel):
    run_state: Literal["running", "paused"] = "paused"
    worker_state: Literal["idle", "paused", "sleeping", "running_cycle", "stopping", "error"] = "idle"
    runtime_phase: str = "paused"
    run_once_requested: bool = False
    last_cycle_started_at: Optional[datetime] = None
    last_successful_cycle_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    sleep_until: Optional[datetime] = None
    next_cycle_at: Optional[datetime] = None
    current_interval_seconds: int = 0
    status_message: Optional[str] = None
    last_control_action: Optional[str] = None
    last_control_at: Optional[datetime] = None
    last_cycle_summary: Optional[str] = None
    latest_failure_summary: Optional[str] = None
    operator_hints: list[str] = Field(default_factory=list)


class RuntimeControlRequest(BaseModel):
    action: Optional[Literal["play", "pause", "run_once"]] = None
    run_state: Optional[Literal["running", "paused"]] = None

    @model_validator(mode="after")
    def validate_payload(self):
        if self.action is None and self.run_state is None:
            raise ValueError("Provide either action or run_state.")
        return self


class AutonomyDigestResponse(BaseModel):
    run_at: Optional[datetime] = None
    summary: Optional[str] = None
    new_leads: list[str] = Field(default_factory=list)
    suppressed_leads: list[str] = Field(default_factory=list)
    investigations_changed: int = 0
    follow_ups_created: list[str] = Field(default_factory=list)
    watchlist_changes: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)


class ConnectorHealthResponse(BaseModel):
    connector_name: str
    status: str
    blocked_reason: Optional[str] = None
    config_key: Optional[str] = None
    consecutive_failures: int
    recent_successes: int = 0
    recent_failures: int = 0
    trust_score: float = 0.0
    circuit_state: str
    disabled_until: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None
    last_error: Optional[str] = None
    last_failure_classification: Optional[str] = None
    last_mode: Optional[str] = None
    last_item_count: int = 0
    quarantine_count: int = 0
    approved_for_unattended: bool = False
    last_freshness_lag_seconds: Optional[int] = None


class ConnectorResetRequest(BaseModel):
    confirm: bool = False


class ConnectorResetResponse(BaseModel):
    connector_name: str
    status: str
    summary: str


class CompanyDiscoveryRowResponse(BaseModel):
    company_name: str
    company_domain: Optional[str] = None
    normalized_company_key: str
    discovery_source: str
    discovery_query: Optional[str] = None
    first_discovered_at: datetime
    last_discovered_at: datetime
    last_expanded_at: Optional[datetime] = None
    board_type: str
    board_locator: str
    surface_provenance: Optional[str] = None
    source_lineage: Optional[str] = None
    expansion_status: str
    expansion_attempts: int
    last_expansion_result_count: int
    visible_yield_count: int
    suppressed_yield_count: int
    location_filtered_count: int = 0
    utility_score: float
    blocked_reason: Optional[str] = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DiscoverySourceMatrixRow(BaseModel):
    source_key: str
    label: str
    classification: Literal["working", "partially_working", "not_working"]
    runtime_state: str
    toggle_key: str
    toggle_enabled: bool = False
    runtime_enabled: bool = False
    strict_live_enabled: bool = False
    live_ready: bool = False
    trusted_for_output: bool = False
    reason: str
    blocked_reason: Optional[str] = None
    connector_status: Optional[str] = None
    last_mode: Optional[str] = None
    last_error: Optional[str] = None
    ran: bool = False
    failed: bool = False
    zero_yield: bool = False
    run_count: int = 0
    failure_count: int = 0
    zero_yield_count: int = 0
    yielded_results_count: int = 0
    surfaced_jobs_count: int = 0
    fallback_count: int = 0
    fallback_order: list[str] = Field(default_factory=list)
    last_status: Optional[str] = None
    summary: Optional[str] = None


class SearchRunResponse(BaseModel):
    id: int
    source_key: str
    worker_name: str
    provider: str
    status: str
    live: bool = False
    zero_yield: bool = False
    query_count: int = 0
    result_count: int = 0
    queries: list[str] = Field(default_factory=list)
    failure_classification: Optional[str] = None
    error: Optional[str] = None
    diagnostics_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class DiscoveryStatusResponse(BaseModel):
    total_known_companies: int = 0
    discovered_last_24h: int = 0
    expanded_last_24h: int = 0
    source_matrix: list[DiscoverySourceMatrixRow] = Field(default_factory=list)
    recent_items: list[CompanyDiscoveryRowResponse] = Field(default_factory=list)
    latest_planner_run: Optional[dict[str, Any]] = None
    recent_plans: list[dict[str, Any]] = Field(default_factory=list)
    recent_expansions: list[dict[str, Any]] = Field(default_factory=list)
    recent_visible_yield: list[CompanyDiscoveryRowResponse] = Field(default_factory=list)
    blocked_or_cooled_down: list[CompanyDiscoveryRowResponse] = Field(default_factory=list)
    recent_greenhouse_tokens: list[dict[str, Any]] = Field(default_factory=list)
    recent_ashby_identifiers: list[dict[str, Any]] = Field(default_factory=list)
    recent_geography_rejections: list[dict[str, Any]] = Field(default_factory=list)
    recent_agentic_leads: list[dict[str, Any]] = Field(default_factory=list)
    agentic_slice_status: dict[str, Any] = Field(default_factory=dict)
    next_recommended_queries: list[str] = Field(default_factory=list)
    latest_openai_usage: dict[str, bool] = Field(default_factory=dict)
    cycle_metrics: dict[str, Any] = Field(default_factory=dict)
    recent_successful_expansions: list[dict[str, Any]] = Field(default_factory=list)
    recent_search_runs: list[SearchRunResponse] = Field(default_factory=list)


class DailyDigestResponse(BaseModel):
    digest_date: str
    summary: str
    new_leads: list[str] = Field(default_factory=list)
    suppressed_leads: list[str] = Field(default_factory=list)
    investigations_changed: int = 0
    follow_ups_created: list[str] = Field(default_factory=list)
    watchlist_changes: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)


class AutonomyStatusResponse(BaseModel):
    health: AutonomyHealthResponse
    digest: AutonomyDigestResponse
    daily_digest: Optional[DailyDigestResponse] = None
    connector_health: list[ConnectorHealthResponse] = Field(default_factory=list)


class InvestigationResponse(BaseModel):
    id: int
    signal_id: int
    company_guess: Optional[str] = None
    role_guess: Optional[str] = None
    confidence: float
    status: str
    attempts: int
    next_check_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None
    source_url: Optional[str] = None
    raw_text: Optional[str] = None


class InvestigationsResponse(BaseModel):
    items: list[InvestigationResponse]


class QueryLearningRow(BaseModel):
    query_text: str
    source_type: str
    status: str
    decision_reason: Optional[str] = None
    leads_generated: int
    likes: int
    saves: int
    applies: int
    dislikes: int
    last_run_at: Optional[datetime] = None


class WatchlistItemResponse(BaseModel):
    item_type: str
    value: str
    source_reason: str
    confidence: str
    status: str
    decision_reason: Optional[str] = None


class FollowUpTaskResponse(BaseModel):
    application_id: int
    company_name: str
    title: str
    task_type: str
    due_at: datetime
    status: str
    notes: Optional[str] = None


class LearningViewResponse(BaseModel):
    top_queries: list[QueryLearningRow]
    generated_queries: list[str] = Field(default_factory=list)
    suppressed_queries: list[str] = Field(default_factory=list)
    inferred_title_families: list[str] = Field(default_factory=list)
    inferred_domains: list[str] = Field(default_factory=list)
    watchlist_items: list[WatchlistItemResponse] = Field(default_factory=list)
    follow_up_tasks: list[FollowUpTaskResponse] = Field(default_factory=list)
