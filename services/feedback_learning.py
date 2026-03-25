from __future__ import annotations

from typing import Optional


REJECTION_STATUS_REASON_LABELS = {
    "": "Select stage",
    "recruiter_screen_decline": "Recruiter screen decline",
    "hiring_manager_decline": "Hiring manager decline",
    "panel_decline": "Panel decline",
    "final_round_decline": "Final round decline",
}

REJECTION_OUTCOME_REASON_LABELS = {
    "": "Select reason",
    "insufficient_b2b_saas_depth": "Needs deeper B2B SaaS depth",
    "insufficient_pricing_depth": "Needs deeper pricing or monetization depth",
    "insufficient_zero_to_one_depth": "Needs stronger zero-to-one examples",
    "insufficient_people_management_depth": "Needs stronger people management scope",
    "insufficient_technical_depth": "Needs stronger technical fluency",
    "insufficient_execution_examples": "Needs sharper execution examples",
    "communication_gap": "Needs clearer communication or narrative",
    "scope_mismatch": "Role scope mismatch",
}

REJECTION_BUCKET_LABELS = {
    "interview_performance": "Interview performance",
    "domain_depth": "Domain depth",
    "pricing_depth": "Pricing depth",
    "stage_depth": "Stage depth",
    "leadership_depth": "Leadership depth",
    "technical_depth": "Technical depth",
    "evidence_sharpness": "Evidence sharpness",
    "communication": "Communication",
    "scope_calibration": "Scope calibration",
}

_BUCKETS_BY_STATUS_REASON = {
    "recruiter_screen_decline": ["interview_performance", "scope_calibration"],
    "hiring_manager_decline": ["interview_performance"],
    "panel_decline": ["interview_performance"],
    "final_round_decline": ["interview_performance"],
}

_BUCKETS_BY_OUTCOME_REASON = {
    "insufficient_b2b_saas_depth": ["domain_depth"],
    "insufficient_pricing_depth": ["pricing_depth", "domain_depth"],
    "insufficient_zero_to_one_depth": ["stage_depth"],
    "insufficient_people_management_depth": ["leadership_depth"],
    "insufficient_technical_depth": ["technical_depth"],
    "insufficient_execution_examples": ["evidence_sharpness"],
    "communication_gap": ["communication"],
    "scope_mismatch": ["scope_calibration"],
}

_KEYWORD_BUCKETS = (
    (("b2b", "saas", "enterprise"), "domain_depth"),
    (("pricing", "monetization"), "pricing_depth"),
    (("zero-to-one", "0-1", "founding", "seed"), "stage_depth"),
    (("manager", "leadership", "team size", "hiring"), "leadership_depth"),
    (("technical", "systems", "sql", "analytics"), "technical_depth"),
    (("metrics", "quantified", "not specific enough", "lacked clear metrics"), "evidence_sharpness"),
    (("communication", "storytelling", "narrative", "executive presence"), "communication"),
    (("scope", "level", "seniority", "too senior", "too junior"), "scope_calibration"),
)

_RECOMMENDATIONS_BY_BUCKET = {
    "interview_performance": "Convert the rejection theme into two stage-matched interview stories with a crisp problem, decision, and measurable outcome.",
    "domain_depth": "Add one resume bullet and one interview example that show direct B2B SaaS ownership with concrete pipeline, pricing, or retention metrics.",
    "pricing_depth": "Prepare a pricing-specific case story that shows packaging, monetization tradeoffs, or revenue impact with explicit numbers.",
    "stage_depth": "Separate zero-to-one examples from scale examples so target companies can see stage-relevant pattern matching immediately.",
    "leadership_depth": "Make team scope explicit by naming headcount, cross-functional ownership, and hiring or coaching responsibilities.",
    "technical_depth": "Tighten one example around systems fluency by naming the tooling, data used, and how you debugged or improved the workflow.",
    "evidence_sharpness": "Replace general responsibility language with quantified before-and-after outcomes from the same type of problem.",
    "communication": "Shorten your core stories into outcome-first answers with one clear metric and one tradeoff rather than broad background.",
    "scope_calibration": "Retune target titles and your opening narrative so the role scope clearly matches the level you want to pursue.",
}


def normalize_reason_code(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def categorize_rejection_feedback(
    status_reason_code: Optional[str] = None,
    outcome_reason_code: Optional[str] = None,
    notes: Optional[str] = None,
) -> dict[str, object]:
    normalized_status = normalize_reason_code(status_reason_code)
    normalized_outcome = normalize_reason_code(outcome_reason_code)
    lowered_notes = (notes or "").strip().lower()

    reason_buckets: list[str] = []
    for bucket in _BUCKETS_BY_STATUS_REASON.get(normalized_status, []):
        if bucket not in reason_buckets:
            reason_buckets.append(bucket)
    for bucket in _BUCKETS_BY_OUTCOME_REASON.get(normalized_outcome, []):
        if bucket not in reason_buckets:
            reason_buckets.append(bucket)
    for keywords, bucket in _KEYWORD_BUCKETS:
        if bucket in reason_buckets:
            continue
        if any(keyword in lowered_notes for keyword in keywords):
            reason_buckets.append(bucket)

    return {
        "status_reason_code": normalized_status or None,
        "outcome_reason_code": normalized_outcome or None,
        "reason_buckets": reason_buckets,
    }


def bucket_label(bucket: str) -> str:
    return REJECTION_BUCKET_LABELS.get(bucket, bucket.replace("_", " ").title())


def reason_label(code: Optional[str], labels: dict[str, str]) -> str:
    normalized = normalize_reason_code(code)
    return labels.get(normalized, normalized.replace("_", " ").title() if normalized else labels[""])


def generate_improvement_recommendations(
    status_reason_code: Optional[str] = None,
    outcome_reason_code: Optional[str] = None,
    notes: Optional[str] = None,
) -> list[str]:
    feedback = categorize_rejection_feedback(
        status_reason_code=status_reason_code,
        outcome_reason_code=outcome_reason_code,
        notes=notes,
    )
    recommendations: list[str] = []
    for bucket in feedback["reason_buckets"]:
        recommendation = _RECOMMENDATIONS_BY_BUCKET.get(bucket)
        if recommendation and recommendation not in recommendations:
            recommendations.append(recommendation)
    return recommendations
