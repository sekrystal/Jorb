from __future__ import annotations

from core.schemas import CandidateProfilePayload
from services.ranking import classify_qualification_fit, classify_title_fit, score_lead


class DummyProfile:
    def __init__(self) -> None:
        payload = CandidateProfilePayload(
            core_titles_json=["chief of staff", "founding operations lead"],
            adjacent_titles_json=["business operations", "implementation lead"],
            excluded_titles_json=["intern"],
            excluded_keywords_json=["rocket propulsion"],
            min_seniority_band="mid",
            max_seniority_band="staff",
            stretch_role_families_json=["go_to_market"],
        )
        for key, value in payload.model_dump().items():
            setattr(self, key, value)


def test_weird_but_relevant_title_gets_adjacent_or_scope_match() -> None:
    profile = DummyProfile()
    label, score, _ = classify_title_fit(
        profile,
        title="Business Rhythm Architect",
        description_text="Own planning cadences, internal systems, and cross-functional operating rhythm.",
    )
    assert label in {"adjacent match", "unexpected but plausible"}
    assert score > 0.5


def test_specialized_role_is_underqualified() -> None:
    profile = DummyProfile()
    label, score, reasons = classify_qualification_fit(
        profile,
        title="Rocket Propulsion Engineer",
        description_text="Design propulsion systems and specialized hardware.",
    )
    assert label == "underqualified"
    assert score < 0
    assert reasons


def test_go_to_market_title_defaults_to_stretch_without_strong_title_signal() -> None:
    profile = DummyProfile()
    label, score, reasons = classify_qualification_fit(
        profile,
        title="Deployment Strategist",
        description_text="Help customers deploy product across a fast-growing startup.",
    )
    assert label == "stretch"
    assert score == 0.4
    assert reasons


def test_feedback_boosts_are_capped_and_do_not_exceed_total_positive_limit() -> None:
    profile = DummyProfile()
    breakdown = score_lead(
        profile=profile,
        lead_type="combined",
        title="Deployment Strategist",
        company_name="Mercor",
        company_domain="mercor.ai",
        location="Remote",
        description_text="Deployment work for an early-stage startup customer team.",
        freshness_label="fresh",
        listing_status="active",
        source_type="ashby",
        evidence_count=2,
        feedback_learning={
            "title_weights": {"deployment strategist": 1.2},
            "role_family_weights": {"go_to_market": 0.8},
            "domain_weights": {"mercor.ai": 0.5},
        },
    )
    total_positive_feedback = (
        breakdown["feedback_title_boost"]
        + breakdown["feedback_role_family_boost"]
        + breakdown["feedback_domain_boost"]
    )
    assert round(total_positive_feedback, 2) == 1.5
    assert isinstance(breakdown["final_score"], float)


def test_strong_rank_threshold_now_requires_more_than_73() -> None:
    profile = DummyProfile()
    breakdown = score_lead(
        profile=profile,
        lead_type="listing",
        title="Strategic Programs Lead",
        company_name="Ramp",
        company_domain=None,
        location="Remote",
        description_text="Own operating cadence and planning for customers.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={},
    )
    assert breakdown["composite"] == 7.0
    assert breakdown["final_score"] == 7.0
    assert breakdown["rank_label"] == "medium"


def test_explicit_target_role_changes_title_fit_and_score() -> None:
    baseline_profile = DummyProfile()
    targeted_profile = DummyProfile()
    targeted_profile.extracted_summary_json = {
        "structured_profile": {
            "targeting": {
                "preferred_locations": ["remote"],
                "target_roles": ["deployment strategist"],
                "work_mode_preference": "remote",
            }
        }
    }

    baseline = score_lead(
        profile=baseline_profile,
        lead_type="listing",
        title="Deployment Strategist",
        company_name="Mercor",
        company_domain="mercor.ai",
        location="Remote - US",
        description_text="Deployment work for an early-stage startup customer team.",
        freshness_label="fresh",
        listing_status="active",
        source_type="ashby",
        evidence_count=1,
        feedback_learning={},
    )
    targeted = score_lead(
        profile=targeted_profile,
        lead_type="listing",
        title="Deployment Strategist",
        company_name="Mercor",
        company_domain="mercor.ai",
        location="Remote - US",
        description_text="Deployment work for an early-stage startup customer team.",
        freshness_label="fresh",
        listing_status="active",
        source_type="ashby",
        evidence_count=1,
        feedback_learning={},
    )

    assert targeted["title_fit_label"] == "target role match"
    assert targeted["final_score"] > baseline["final_score"]
    assert "target role" in targeted["matched_profile_fields"]
    assert targeted["search_intent"]["target_roles"] == ["deployment strategist"]
    assert "Role match:" in targeted["role_match_explanation"]
    assert "explicit target role" in targeted["role_match_explanation"]
    assert "Location fit:" in targeted["location_fit_explanation"]


def test_explicit_work_mode_preference_penalizes_mismatch() -> None:
    profile = DummyProfile()
    profile.preferred_locations_json = ["san francisco"]
    profile.extracted_summary_json = {
        "structured_profile": {
            "targeting": {
                "preferred_locations": ["san francisco"],
                "target_roles": ["chief of staff"],
                "work_mode_preference": "onsite",
            }
        }
    }

    onsite = score_lead(
        profile=profile,
        lead_type="listing",
        title="Chief of Staff",
        company_name="Acme",
        company_domain=None,
        location="San Francisco, CA",
        description_text="Lead operating cadence in office with the founding team.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={},
    )
    remote = score_lead(
        profile=profile,
        lead_type="listing",
        title="Chief of Staff",
        company_name="Acme",
        company_domain=None,
        location="Remote - US",
        description_text="Lead operating cadence in a distributed team.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={},
    )

    assert onsite["location_fit"] > remote["location_fit"]
    assert onsite["work_mode_match"] == "onsite"
    assert remote["work_mode_match"] == "remote"
    assert onsite["search_intent"]["work_mode_preference"] == "onsite"
    assert "matches preferred geography" in onsite["location_fit_explanation"]
    assert "conflicts with the onsite preference" in remote["location_fit_explanation"]
