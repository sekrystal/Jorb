from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.schemas import LeadResponse
from core.models import AgentActivity, Base, Lead, Listing
from core.schemas import SyncResult
from services.discovery_agents import planner_agent
from services.pipeline import ingest_user_job_link, recommendation_component_value, recommendation_score_value, run_scout_agent
from services.profile import ingest_resume, update_candidate_profile
from services.sync import sync_all


def test_run_scout_agent_adds_demo_batch_and_logs_activity() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Senior operator with 7+ years in AI and developer tools. Chief of staff and deployment lead.",
    )
    sync_all(session, include_rechecks=True)
    baseline_leads = session.query(Lead).count()
    baseline_listings = session.query(Listing).count()

    result = run_scout_agent(session)
    session.commit()

    assert result.agent == "scout"
    assert "Scout added" in result.summary
    assert session.query(Listing).count() > baseline_listings
    assert session.query(Lead).count() >= baseline_leads
    assert session.query(AgentActivity).filter(AgentActivity.agent_name == "Scout").count() >= 1

    listing = session.query(Listing).order_by(Listing.id.desc()).first()
    assert listing is not None
    intelligence = (listing.metadata_json or {}).get("opportunity_intelligence") or {}
    assert intelligence["freshness_label"] == "fresh"
    assert intelligence["evergreen_likelihood"] == "low"

    lead = session.query(Lead).filter(Lead.listing_id == listing.id).order_by(Lead.id.desc()).first()
    assert lead is not None
    assert "Freshness logic:" in (lead.explanation or "")
    assert (lead.evidence_json or {}).get("opportunity_intelligence", {}).get("freshness_label") == "fresh"


def test_lead_response_normalizes_recommendation_score_schema_with_traceable_components() -> None:
    lead = LeadResponse(
        id=1,
        lead_type="listing",
        company_name="Ramp",
        primary_title="Strategic Programs Lead",
        url="https://example.com/job",
        source_type="greenhouse",
        listing_status="active",
        first_published_at=None,
        discovered_at=None,
        last_seen_at=None,
        updated_at=None,
        freshness_hours=6.0,
        freshness_days=0,
        posted_at=None,
        surfaced_at="2026-03-25T12:00:00Z",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        source_platform="greenhouse",
        source_provenance=None,
        source_lineage="greenhouse",
        discovery_source=None,
        saved=False,
        applied=False,
        current_status=None,
        date_saved=None,
        date_applied=None,
        application_notes=None,
        application_updated_at=None,
        next_action=None,
        follow_up_due=False,
        explanation="Strong operator match with fresh, verified evidence.",
        last_agent_action=None,
        hidden=False,
        score_breakdown_json={
            "composite": 8.4,
            "freshness": 1.6,
            "title_fit": 2.4,
            "role_family_fit": 0.8,
            "source_quality": 1.2,
            "evidence_quality": 0.8,
            "negative_signals": -0.2,
            "rank_label": "strong",
            "confidence_label": "high",
            "role_family": "operations",
        },
        evidence_json={
            "matched_profile_fields": ["core title", "scope match"],
            "feedback_notes": ["liked similar strategic operations roles"],
            "source_platform": "greenhouse",
            "source_lineage": "greenhouse",
            "listing_status": "active",
            "freshness_days": 0,
            "location": "New York, NY",
        },
    )

    score_payload = lead.score_breakdown_json

    assert score_payload["schema_version"] == "v1"
    assert score_payload["final_score"] == 8.4
    assert score_payload["recommendation_band"] == "strong"
    assert score_payload["action_label"] == "Act now"
    assert "final score is 8.40" in score_payload["action_explanation"]
    assert "Title alignment +2.40" in score_payload["action_explanation"]
    assert score_payload["explanation"]["headline"] == "Strong recommendation at 8.40 with high confidence."
    assert score_payload["trace_inputs"]["matched_profile_fields"] == ["core title", "scope match"]
    assert any(component["key"] == "freshness" for component in score_payload["component_metrics"])
    title_fit_component = next(component for component in score_payload["component_metrics"] if component["key"] == "title_fit")
    assert "title_fit_label=core match" in title_fit_component["trace_inputs"]


def test_lead_response_normalizes_signal_only_roles_to_seek_referral_guidance() -> None:
    lead = LeadResponse(
        id=2,
        lead_type="signal",
        company_name="Stealth AI",
        primary_title="Business Operations Lead",
        url=None,
        source_type="x",
        listing_status="unknown",
        first_published_at=None,
        discovered_at=None,
        last_seen_at=None,
        updated_at=None,
        freshness_hours=4.0,
        freshness_days=0,
        posted_at=None,
        surfaced_at="2026-03-25T12:00:00Z",
        rank_label="good",
        confidence_label="medium",
        freshness_label="fresh",
        title_fit_label="adjacent match",
        qualification_fit_label="strong fit",
        source_platform="x",
        source_provenance=None,
        source_lineage="x",
        discovery_source="search",
        saved=False,
        applied=False,
        current_status=None,
        date_saved=None,
        date_applied=None,
        application_notes=None,
        application_updated_at=None,
        next_action=None,
        follow_up_due=False,
        explanation="Signal-only lead with plausible hiring evidence.",
        last_agent_action=None,
        hidden=False,
        score_breakdown_json={
            "composite": 5.6,
            "novelty": 0.7,
            "source_quality": 0.4,
            "title_fit": 1.9,
            "evidence_quality": 0.8,
            "negative_signals": -0.1,
            "rank_label": "good",
            "confidence_label": "medium",
            "role_family": "operations",
        },
        evidence_json={
            "matched_profile_fields": ["adjacent title"],
            "feedback_notes": [],
            "source_platform": "x",
            "source_lineage": "x",
            "listing_status": "unknown",
        },
    )

    score_payload = lead.score_breakdown_json

    assert score_payload["action_label"] == "Seek referral"
    assert "novelty +0.70" in score_payload["action_explanation"]
    assert "source quality +0.40" in score_payload["action_explanation"]


def test_recommendation_score_helpers_support_legacy_and_structured_payloads() -> None:
    assert recommendation_score_value({"composite": 6.2}) == 6.2
    assert recommendation_score_value({"final_score": 7.1, "composite": 6.2}) == 7.1
    assert recommendation_component_value({"title_fit": 2.4}, "title_fit") == 2.4
    assert (
        recommendation_component_value(
            {"component_metrics": [{"key": "title_fit", "score": 1.9}]},
            "title_fit",
    )
        == 1.9
    )


def test_planner_agent_applies_profile_driven_role_geography_and_work_mode_constraints() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    profile = ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Chief of staff operator based in San Francisco.",
    ).candidate_profile
    profile.target_roles_json = ["founding operations lead"]
    profile.work_mode_preference = "onsite"
    profile.preferred_locations_json = ["san francisco"]
    saved_profile = update_candidate_profile(session, profile)

    plan = planner_agent(session, saved_profile)

    assert "target_roles" in plan["profile_constraints_applied"]
    assert "work_mode" in plan["profile_constraints_applied"]
    assert "geography" in plan["profile_constraints_applied"]
    assert plan["target_roles"] == ["founding operations lead"]
    assert plan["work_mode_preference"] == "onsite"
    assert any("san francisco" in query.lower() for query in plan["queries"])
    assert any("onsite" in query.lower() for query in plan["queries"])
    assert not any("remote us" in query.lower() for query in plan["queries"])


def test_run_scout_agent_records_high_evergreen_temporal_intelligence_for_old_active_listing(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Operator with chief of staff and business operations experience.",
    )
    listing = Listing(
        company_name="Evergreen Co",
        title="General Application",
        location="Remote",
        url="https://example.com/jobs/general-application",
        source_type="greenhouse",
        posted_at=None,
        description_text="We are always hiring and review future opportunities on a rolling basis.",
        listing_status="active",
        freshness_hours=24.0 * 60,
        freshness_days=60,
        metadata_json={"page_text": "Always hiring strategic operators."},
    )
    session.add(listing)
    session.commit()

    def fake_sync_all(*_args, **_kwargs) -> SyncResult:
        listing.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        session.add(listing)
        session.flush()
        return SyncResult(
            signals_ingested=0,
            listings_ingested=0,
            leads_created=0,
            leads_updated=0,
            rechecks_queued=0,
            live_mode_used=False,
            discovery_metrics={},
            surfaced_count=0,
            discovery_summary="No jobs found from any connector.",
            discovery_status={},
        )

    monkeypatch.setattr("services.pipeline.sync_all", fake_sync_all)

    result = run_scout_agent(session, source_mode="live", enabled_connectors=set())
    session.commit()

    session.refresh(listing)
    intelligence = (listing.metadata_json or {}).get("opportunity_intelligence") or {}
    assert intelligence["freshness_label"] == "stale"
    assert intelligence["evergreen_likelihood"] == "high"
    assert "always hiring" in intelligence["evergreen_signals"]
    assert "evergreen_high=1" in result.summary


def test_ingest_user_job_link_routes_manual_submission_through_listing_pipeline() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Operator with chief of staff and strategic programs experience in AI companies.",
    )

    result = ingest_user_job_link(
        session,
        job_url="https://boards.greenhouse.io/ramp/jobs/9999",
        company_name="Ramp",
        title="Strategic Programs Lead",
        location="New York, NY",
        description_text="Lead strategic programs, executive reporting, and operating cadences.",
        posted_at=datetime.now(timezone.utc),
    )
    session.commit()

    listing = session.get(Listing, result["listing_id"])
    lead = session.get(Lead, result["lead_id"])

    assert listing is not None
    assert lead is not None
    assert listing.source_type == "greenhouse"
    assert (listing.metadata_json or {})["surface_provenance"] == "user_submitted"
    assert (listing.metadata_json or {})["source_lineage"] == "greenhouse+user_submitted"
    assert (listing.metadata_json or {})["submission_origin"] == "user_link"
    assert lead.lead_type == "listing"
    assert (lead.evidence_json or {})["source_provenance"] == "user_submitted"
    assert (lead.evidence_json or {})["source_lineage"] == "greenhouse+user_submitted"
    assert (lead.evidence_json or {})["submission_origin"] == "user_link"
    assert lead.last_agent_action == "Scout: ingested user-submitted link"
    assert lead.score_breakdown_json["source_quality"] == 1.2
    assert "Freshness logic:" in (lead.explanation or "")
