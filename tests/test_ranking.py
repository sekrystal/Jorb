from __future__ import annotations

from datetime import datetime, timedelta, timezone

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
    assert breakdown["composite"] == 7.55
    assert breakdown["final_score"] == 7.55
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


def test_confirmed_skills_and_competencies_increase_match_score_when_job_text_matches() -> None:
    profile = DummyProfile()
    profile.extracted_summary_json = {
        "structured_profile": {
            "targeting": {
                "target_roles": ["chief of staff"],
                "preferred_locations": ["remote"],
                "work_mode_preference": "remote",
                "confirmed_skills": ["sql", "stakeholder management"],
                "competencies": ["process design"],
            }
        }
    }

    matched = score_lead(
        profile=profile,
        lead_type="listing",
        title="Chief of Staff",
        company_name="Acme",
        company_domain=None,
        location="Remote - US",
        description_text="Own stakeholder management, SQL-heavy reporting, and process design for the executive team.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={},
    )
    unmatched = score_lead(
        profile=profile,
        lead_type="listing",
        title="Chief of Staff",
        company_name="Acme",
        company_domain=None,
        location="Remote - US",
        description_text="Own planning cadence and executive communication.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={},
    )

    assert matched["skill_fit"] > unmatched["skill_fit"]
    assert matched["final_score"] > unmatched["final_score"]
    assert "sql" in matched["skill_match_terms"]
    assert "process design" in matched["competency_match_terms"]
    assert any(field.startswith("confirmed skill:") for field in matched["matched_profile_fields"])
    assert matched["match_tier"] in {"medium", "high"}


def test_resume_alignment_produces_high_tier_with_required_skills_and_years_match() -> None:
    profile = DummyProfile()
    profile.years_experience = 8
    profile.preferred_domains_json = ["ai", "infra"]
    profile.extracted_summary_json = {
        "structured_profile": {
            "targeting": {
                "target_roles": ["chief of staff"],
                "preferred_locations": ["remote"],
                "work_mode_preference": "remote",
                "confirmed_skills": ["sql", "stakeholder management", "program management"],
                "competencies": ["process design", "operator judgment"],
                "seniority": {"guess": "senior", "years_experience": 8, "minimum_band": "mid", "maximum_band": "staff"},
            }
        }
    }

    breakdown = score_lead(
        profile=profile,
        lead_type="listing",
        title="Chief of Staff",
        company_name="Acme",
        company_domain="acme.ai",
        location="Remote - US",
        description_text="Requirements\n- 6+ years leading cross-functional programs\n- SQL and stakeholder management\n\nResponsibilities\n- Own process design for executive operations.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=2,
        listing_metadata={
            "description_sections": [
                {"heading": "Requirements", "paragraphs": [], "bullets": ["6+ years leading cross-functional programs", "SQL and stakeholder management"]},
                {"heading": "Responsibilities", "paragraphs": [], "bullets": ["Own process design for executive operations."]},
            ]
        },
        feedback_learning={},
    )

    assert breakdown["match_tier"] == "high"
    assert "required skill: sql" in breakdown["top_matching_signals"]
    assert breakdown["years_required"] == 6
    assert breakdown["years_experience"] == 8


def test_resume_alignment_surfaces_missing_required_signals_for_low_tier_jobs() -> None:
    profile = DummyProfile()
    profile.years_experience = 2
    profile.extracted_summary_json = {
        "structured_profile": {
            "targeting": {
                "target_roles": ["chief of staff"],
                "preferred_locations": ["remote"],
                "work_mode_preference": "remote",
                "confirmed_skills": ["sql"],
                "competencies": ["process design"],
                "seniority": {"guess": "mid", "years_experience": 2, "minimum_band": "junior", "maximum_band": "mid"},
            }
        }
    }

    breakdown = score_lead(
        profile=profile,
        lead_type="listing",
        title="Head of Business Operations",
        company_name="Acme",
        company_domain="acme.ai",
        location="Remote - US",
        description_text="Requirements\n- 8+ years experience\n- recruiting and stakeholder management\n\nResponsibilities\n- lead executive recruiting systems.",
        freshness_label="fresh",
        listing_status="active",
        source_type="search_web",
        evidence_count=1,
        listing_metadata={
            "description_sections": [
                {"heading": "Requirements", "paragraphs": [], "bullets": ["8+ years experience", "recruiting and stakeholder management"]},
            ]
        },
        feedback_learning={},
    )

    assert breakdown["match_tier"] == "low"
    assert any(item.startswith("missing required skill:") for item in breakdown["missing_signals"])
    assert any("trails the 8+ year requirement" in item for item in breakdown["missing_signals"])


def test_recent_save_feedback_boosts_similar_roles_more_than_old_feedback() -> None:
    profile = DummyProfile()
    now = datetime.now(timezone.utc)
    stale = (now - timedelta(days=90)).isoformat()
    recent = (now - timedelta(days=1)).isoformat()

    stale_breakdown = score_lead(
        profile=profile,
        lead_type="listing",
        title="Chief of Staff",
        company_name="Acme",
        company_domain="acme.ai",
        location="Remote - US",
        description_text="Lead operations planning and stakeholder management.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={
            "feedback_events": [
                {
                    "at": stale,
                    "action": "save",
                    "title": "Chief of Staff",
                    "company_name": "Acme",
                    "company_domain": "acme.ai",
                    "role_family": "operations",
                    "source_type": "greenhouse",
                }
            ]
        },
    )
    recent_breakdown = score_lead(
        profile=profile,
        lead_type="listing",
        title="Chief of Staff",
        company_name="Acme",
        company_domain="acme.ai",
        location="Remote - US",
        description_text="Lead operations planning and stakeholder management.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={
            "feedback_events": [
                {
                    "at": recent,
                    "action": "save",
                    "title": "Chief of Staff",
                    "company_name": "Acme",
                    "company_domain": "acme.ai",
                    "role_family": "operations",
                    "source_type": "greenhouse",
                }
            ]
        },
    )

    recent_total = (
        recent_breakdown["feedback_title_boost"]
        + recent_breakdown["feedback_role_family_boost"]
        + recent_breakdown["feedback_domain_boost"]
        + recent_breakdown["feedback_company_boost"]
    )
    stale_total = (
        stale_breakdown["feedback_title_boost"]
        + stale_breakdown["feedback_role_family_boost"]
        + stale_breakdown["feedback_domain_boost"]
        + stale_breakdown["feedback_company_boost"]
    )
    assert recent_total > stale_total
    assert recent_breakdown["final_score"] > stale_breakdown["final_score"]


def test_repeated_dismissals_penalize_similar_roles() -> None:
    profile = DummyProfile()
    now = datetime.now(timezone.utc).isoformat()
    neutral = score_lead(
        profile=profile,
        lead_type="listing",
        title="Deployment Strategist",
        company_name="Acme",
        company_domain="acme.ai",
        location="Remote - US",
        description_text="Customer implementation and deployment role.",
        freshness_label="fresh",
        listing_status="active",
        source_type="ashby",
        evidence_count=1,
        feedback_learning={},
    )
    penalized = score_lead(
        profile=profile,
        lead_type="listing",
        title="Deployment Strategist",
        company_name="Acme",
        company_domain="acme.ai",
        location="Remote - US",
        description_text="Customer implementation and deployment role.",
        freshness_label="fresh",
        listing_status="active",
        source_type="ashby",
        evidence_count=1,
        feedback_learning={
            "feedback_events": [
                {
                    "at": now,
                    "action": "dislike",
                    "title": "Implementation Lead",
                    "company_name": "Beta",
                    "company_domain": "beta.ai",
                    "role_family": "go_to_market",
                    "source_type": "ashby",
                },
                {
                    "at": now,
                    "action": "dislike",
                    "title": "Solutions Deployment Manager",
                    "company_name": "Gamma",
                    "company_domain": "gamma.ai",
                    "role_family": "go_to_market",
                    "source_type": "greenhouse",
                },
            ]
        },
    )

    assert penalized["feedback_role_family_boost"] < 0
    assert penalized["final_score"] < neutral["final_score"]


def test_positive_feedback_does_not_rescue_low_fit_role() -> None:
    profile = DummyProfile()
    boosted = score_lead(
        profile=profile,
        lead_type="listing",
        title="Rocket Propulsion Engineer",
        company_name="LaunchCo",
        company_domain="launch.ai",
        location="Remote - US",
        description_text="Requires specialized hardware and rocket propulsion.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={
            "feedback_events": [
                {
                    "at": datetime.now(timezone.utc).isoformat(),
                    "action": "applied",
                    "title": "Rocket Propulsion Engineer",
                    "company_name": "LaunchCo",
                    "company_domain": "launch.ai",
                    "role_family": "engineering",
                    "source_type": "greenhouse",
                }
            ],
            "title_weights": {"rocket propulsion engineer": 2.0},
            "role_family_weights": {"engineering": 1.5},
            "domain_weights": {"launch.ai": 1.0},
        },
    )

    assert boosted["qualification_fit_label"] == "underqualified"
    assert boosted["match_tier"] == "low"
