from __future__ import annotations

from typing import Optional

from services.ai_judges import write_explanation_with_ai


def build_explanation(
    lead_type: str,
    matched_profile_fields: list[str],
    feedback_notes: list[str],
    freshness_label: str,
    confidence_label: str,
    candidate_context: Optional[str] = None,
    fit_assessment: Optional[dict] = None,
    critic_assessment: Optional[dict] = None,
    uncertainty: Optional[str] = None,
) -> str:
    ai_explanation = write_explanation_with_ai(
        {
            "lead_type": lead_type,
            "matched_profile_fields": matched_profile_fields,
            "feedback_notes": feedback_notes,
            "freshness_label": freshness_label,
            "confidence_label": confidence_label,
            "candidate_context": candidate_context,
            "fit_assessment": fit_assessment,
            "critic_assessment": critic_assessment,
            "uncertainty": uncertainty,
        }
    )
    if ai_explanation:
        return ai_explanation

    matched = ", ".join(matched_profile_fields[:3]) if matched_profile_fields else "limited direct profile matches"
    feedback = ", ".join(feedback_notes[:2]) if feedback_notes else "no strong feedback adjustments yet"
    uncertainty_text = f" Uncertainty: {uncertainty}." if uncertainty else ""
    return (
        f"Surfaced as a {lead_type} lead. Matched profile fields: {matched}. "
        f"Feedback influence: {feedback}. Freshness is {freshness_label} and confidence is {confidence_label}."
        f"{uncertainty_text}"
    )
