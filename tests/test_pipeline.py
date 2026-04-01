from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from connectors.search_web import ATSExtractionResult, SearchDiscoveryResult
from core.config import Settings
from core.schemas import LeadResponse
from core.models import AgentActivity, AgentRun, Base, CompanyDiscovery, Lead, Listing, SearchRun
from core.schemas import SyncResult
from services.company_discovery import build_discovery_status
from services.lead_search import build_search_document
from services.normalize import normalize_ashby_job, normalize_greenhouse_job, normalize_yc_job
from services.discovery_agents import planner_agent
from services.pipeline import ingest_user_job_link, recommendation_component_value, recommendation_score_value, run_scout_agent
from services.profile import ingest_resume, update_candidate_profile
from services.sync import _build_discovery_summary, sync_all
from services.explain import build_explanation


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


def test_source_normalizers_map_into_shared_canonical_job_schema() -> None:
    greenhouse_record = normalize_greenhouse_job(
        {
            "company_name": "Acme",
            "title": "Founding Operator",
            "absolute_url": "https://job-boards.greenhouse.io/acme/jobs/123",
            "location": {"name": "Remote US"},
            "content": "Build execution systems.",
            "id": "123",
        }
    )
    ashby_record = normalize_ashby_job(
        {
            "companyName": "Beta",
            "title": "Chief of Staff",
            "jobUrl": "https://jobs.ashbyhq.com/beta/456",
            "location": {},
            "descriptionPlain": "Drive company priorities.",
            "id": "456",
        }
    )
    yc_record = normalize_yc_job(
        {
            "company_name": "Gamma",
            "title": "Business Operations Lead",
            "url": "https://www.workatastartup.com/jobs/789",
            "location": "",
            "description_text": "Run operating cadence.",
            "source_job_id": "789",
        }
    )

    for record, expected_source, expected_location in [
        (greenhouse_record, "greenhouse", "Remote US"),
        (ashby_record, "ashby", "Unspecified"),
        (yc_record, "yc_jobs", "Unspecified"),
    ]:
        assert record.canonical_job is not None
        assert record.canonical_job.schema_version == "v1"
        assert record.canonical_job.url == record.url
        assert record.canonical_job.company == record.company_name
        assert record.canonical_job.title == record.title
        assert record.canonical_job.location == expected_location
        assert record.canonical_job.source_type == expected_source
        assert record.location == expected_location
        assert (record.metadata_json or {})["canonical_job"] == record.canonical_job.model_dump()


def test_source_normalizers_build_clean_structured_descriptions_across_sources() -> None:
    greenhouse_record = normalize_greenhouse_job(
        {
            "company_name": "Acme",
            "title": "Founding Operator",
            "absolute_url": "https://job-boards.greenhouse.io/acme/jobs/123",
            "location": {"name": "Remote US"},
            "content": """
                <div>Overview</div>
                <p>Lead operating cadence and recruiting systems.</p>
                <div>Responsibilities</div>
                <ul>
                  <li>Build recruiting systems</li>
                  <li>Build recruiting systems</li>
                </ul>
                <script>analytics()</script>
                <div>Apply for this job</div>
            """,
            "id": "123",
        }
    )
    ashby_record = normalize_ashby_job(
        {
            "companyName": "Beta",
            "title": "Chief of Staff",
            "jobUrl": "https://jobs.ashbyhq.com/beta/456",
            "location": {},
            "descriptionHtml": """
                <h2>Requirements</h2>
                <ul><li>5+ years leading cross-functional programs</li></ul>
                <h2>Benefits</h2>
                <p>Medical, dental, and vision.</p>
                <p>Medical, dental, and vision.</p>
            """,
            "id": "456",
        }
    )
    yc_record = normalize_yc_job(
        {
            "company_name": "Gamma",
            "title": "Business Operations Lead",
            "url": "https://www.workatastartup.com/jobs/789",
            "location": "",
            "description_text": "Overview\nOwn planning cadence.\n\nResponsibilities\n- Run hiring systems\n- Run hiring systems",
            "description_html": "<h2>Overview</h2><p>Own planning cadence.</p><h2>Responsibilities</h2><ul><li>Run hiring systems</li><li>Run hiring systems</li></ul>",
            "source_job_id": "789",
        }
    )

    for record in [greenhouse_record, ashby_record, yc_record]:
        assert "<" not in (record.description_text or "")
        assert "Apply for this job" not in (record.description_text or "")
        assert "Responsibilities" in (record.description_text or "") or "Requirements" in (record.description_text or "")
        assert (record.metadata_json or {})["description_sections"]
        assert (record.metadata_json or {})["page_text"]

    assert greenhouse_record.description_text.count("Build recruiting systems") == 1
    assert ashby_record.description_text.count("Medical, dental, and vision.") == 1
    assert yc_record.description_text.count("Run hiring systems") == 1


def test_search_document_uses_cleaned_canonical_description_text() -> None:
    record = normalize_greenhouse_job(
        {
            "company_name": "Acme",
            "title": "Founding Operator",
            "absolute_url": "https://job-boards.greenhouse.io/acme/jobs/123",
            "location": {"name": "Remote US"},
            "content": "<h2>Responsibilities</h2><ul><li>Build recruiting systems</li></ul>",
            "id": "123",
        }
    )

    document = build_search_document(
        {
            "title": record.title,
            "company": record.company_name,
            "location": record.location,
            "description": record.description_text,
            "source": record.source_type,
            "tags": [],
        }
    )

    assert "<li>" not in document["fields"]["description"]
    assert "build recruiting systems" in document["fields"]["description"]


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


def test_build_explanation_includes_role_and_location_fragments() -> None:
    explanation = build_explanation(
        lead_type="listing",
        matched_profile_fields=["core title", "preferred geography"],
        feedback_notes=[],
        freshness_label="fresh",
        confidence_label="high",
        role_match_explanation="Role match: title aligns with a core role from the profile.",
        location_fit_explanation="Location fit: location 'San Francisco, CA' matches preferred geography 'san francisco' (positive signal).",
    )

    assert "Role match:" in explanation
    assert "Location fit:" in explanation


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
    assert plan["search_intent"]["target_roles"] == ["founding operations lead"]
    assert plan["search_intent"]["preferred_locations"] == ["san francisco"]
    assert plan["search_intent"]["work_mode_preference"] == "onsite"
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


def test_build_discovery_summary_surfaces_zero_yield_reason_and_unavailable_sources() -> None:
    summary = _build_discovery_summary(
        discovery_metrics={
            "greenhouse": {"raw": 0, "verified": 0},
            "ashby": {"raw": 0, "verified": 0},
            "search_web": {"raw": 0, "verified": 0},
            "x_search": {"raw": 0, "verified": 0},
        },
        surfaced_count=0,
        source_matrix=[
            {"source_key": "search_web", "label": "Search Web", "classification": "partially_working"},
            {"source_key": "greenhouse", "label": "Greenhouse", "classification": "not_working"},
        ],
        cycle_metrics={
            "search_zero_yield": {
                "reason": "provider self-links only",
                "zero_yield_attempt_count": 2,
            }
        },
    )

    assert "Search discovery returned no accepted results after 2 attempt(s): provider self-links only." in summary
    assert "Unavailable sources this cycle: Greenhouse." in summary


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
    assert (listing.metadata_json or {})["canonical_url"] == "https://job-boards.greenhouse.io/ramp/jobs/9999"
    assert (listing.metadata_json or {})["verification"] == {
        "canonical_url": "https://job-boards.greenhouse.io/ramp/jobs/9999",
        "freshness_label": "fresh",
        "listing_status": "active",
        "dead_link_detected": False,
    }
    assert (listing.metadata_json or {})["canonical_job"] == {
        "schema_version": "v1",
        "url": "https://job-boards.greenhouse.io/ramp/jobs/9999",
        "company": "Ramp",
        "title": "Strategic Programs Lead",
        "location": "New York, NY",
        "source_type": "greenhouse",
        "identity_key": "ramp::strategic-programs-lead::new-york-ny",
        "company_key": "ramp",
        "role_key": "strategic-programs-lead",
        "location_key": "new-york-ny",
    }


def test_ingest_user_job_link_marks_yc_jobs_submission_with_source_lineage() -> None:
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
        job_url="https://www.workatastartup.com/jobs/12345",
        company_name="Acme",
        title="Founding Operations Lead",
        location="San Francisco, CA",
        description_text="Lead recruiting systems, founder operations, and executive cadence.",
        posted_at=datetime.now(timezone.utc),
    )
    session.commit()

    listing = session.get(Listing, result["listing_id"])
    lead = session.get(Lead, result["lead_id"])

    assert listing is not None
    assert lead is not None
    assert listing.source_type == "yc_jobs"
    assert (listing.metadata_json or {})["source_lineage"] == "yc_jobs+user_submitted"
    assert (lead.evidence_json or {})["source_lineage"] == "yc_jobs+user_submitted"
    assert lead.score_breakdown_json["source_quality"] == 0.9


def test_ingest_user_job_link_canonicalizes_and_dedupes_manual_greenhouse_variants() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Operator with chief of staff and strategic programs experience in AI companies.",
    )

    first = ingest_user_job_link(
        session,
        job_url="https://boards.greenhouse.io/ramp/jobs/9999?gh_jid=9999",
        company_name="Ramp",
        title="Strategic Programs Lead",
        location="New York, NY",
        description_text="Lead strategic programs, executive reporting, and operating cadences.",
        posted_at=datetime.now(timezone.utc),
    )
    second = ingest_user_job_link(
        session,
        job_url="https://job-boards.greenhouse.io/ramp/jobs/9999/",
        company_name="Ramp",
        title="Strategic Programs Lead",
        location="New York, NY",
        description_text="Lead strategic programs, executive reporting, and operating cadences.",
        posted_at=datetime.now(timezone.utc),
    )
    session.commit()

    assert first["listing_id"] == second["listing_id"]
    assert first["lead_id"] == second["lead_id"]
    assert session.query(Listing).count() == 1
    assert session.query(Lead).count() == 1


def test_ingest_user_job_link_rejects_dead_links_during_verification() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Operator with chief of staff and strategic programs experience in AI companies.",
    )

    with pytest.raises(ValueError, match="listing verification"):
        ingest_user_job_link(
            session,
            job_url="https://job-boards.greenhouse.io/ramp/jobs/9999",
            company_name="Ramp",
            title="Strategic Programs Lead",
            location="New York, NY",
            description_text="This position has been filled and is no longer accepting applications.",
            posted_at=datetime.now(timezone.utc),
        )

    assert session.query(Listing).count() == 0
    assert session.query(Lead).count() == 0


def test_ingest_user_job_link_flags_stale_records_in_verification_metadata() -> None:
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
        job_url="https://www.workatastartup.com/jobs/12345?ref=homepage",
        company_name="Acme",
        title="Founding Operations Lead",
        location="San Francisco, CA",
        description_text="Lead recruiting systems, founder operations, and executive cadence.",
        posted_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
    )
    session.commit()

    listing = session.get(Listing, result["listing_id"])

    assert listing is not None
    assert listing.url == "https://www.workatastartup.com/jobs/12345"
    assert listing.freshness_days is not None and listing.freshness_days >= 14
    assert (listing.metadata_json or {})["verification"]["freshness_label"] == "stale"
    assert listing.listing_status == "suspected_expired"


def test_sync_all_surfaces_yc_jobs_listing_from_search_discovery(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Operator with chief of staff and founding operations experience.",
    )

    settings = Settings(
        demo_mode=True,
        search_discovery_enabled=True,
        discovery_max_search_queries_per_cycle=4,
        discovery_max_expansions_per_cycle=4,
        discovery_max_new_companies_per_cycle=4,
        openai_enabled=False,
    )

    monkeypatch.setattr("services.sync.get_settings", lambda: settings)
    monkeypatch.setattr(
        "services.sync.SearchDiscoveryConnector.fetch",
        lambda self, queries, require_live=False: (
            [
                SearchDiscoveryResult(
                    query_text=queries[0],
                    title="Founding Operations Lead at Acme | Work at a Startup",
                    url="https://www.workatastartup.com/jobs/12345",
                    source_surface="search_web",
                    query_family="role_market",
                )
            ],
            True,
        ),
    )
    monkeypatch.setattr("services.sync.GreenhouseConnector.fetch", lambda self, *_args, **_kwargs: ([], False))
    monkeypatch.setattr("services.sync.AshbyConnector.fetch", lambda self, *_args, **_kwargs: ([], False))
    monkeypatch.setattr("services.sync.XSearchConnector.fetch", lambda self, *_args, **_kwargs: ([], False))
    monkeypatch.setattr(
        "services.sync.fetch_page_snapshot",
        lambda _url: (
            "https://www.workatastartup.com/jobs/12345",
            """
            <html>
              <head>
                <title>Founding Operations Lead at Acme | Work at a Startup</title>
                <script type="application/ld+json">
                  {
                    "@context": "https://schema.org",
                    "@type": "JobPosting",
                    "title": "Founding Operations Lead",
                    "datePosted": "2026-03-20T00:00:00Z",
                    "description": "<p>Lead operating cadence and recruiting systems.</p>",
                    "identifier": {"@type": "PropertyValue", "value": "12345"},
                    "hiringOrganization": {"@type": "Organization", "name": "Acme"},
                    "jobLocation": {
                      "@type": "Place",
                      "address": {
                        "@type": "PostalAddress",
                        "addressLocality": "San Francisco",
                        "addressRegion": "CA",
                        "addressCountry": "US"
                      }
                    },
                    "url": "https://www.workatastartup.com/jobs/12345"
                  }
                </script>
              </head>
            </html>
            """,
        ),
    )

    result = sync_all(session, include_rechecks=False, enabled_connectors={"search_web"})
    session.commit()

    listing = session.query(Listing).filter(Listing.source_type == "yc_jobs").one()
    lead = session.query(Lead).filter(Lead.listing_id == listing.id).one()

    assert result.discovery_metrics["yc_jobs"]["verified"] == 1
    assert listing.company_name == "Acme"
    assert listing.title == "Founding Operations Lead"
    assert listing.listing_status == "active"
    assert (listing.metadata_json or {})["source_lineage"] == "yc_jobs+search_web"
    assert (lead.evidence_json or {})["source_lineage"] == "yc_jobs+search_web"


def test_sync_all_treats_ats_seed_results_as_search_acquisition_success(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Operator with chief of staff and founding operations experience.",
    )

    settings = Settings(
        demo_mode=True,
        search_discovery_enabled=True,
        discovery_max_search_queries_per_cycle=4,
        discovery_max_expansions_per_cycle=4,
        discovery_max_new_companies_per_cycle=4,
        openai_enabled=False,
    )

    monkeypatch.setattr("services.sync.get_settings", lambda: settings)
    monkeypatch.setattr(
        "services.sync.planner_agent",
        lambda *_args, **_kwargs: {
            "queries": ['"chief of staff" startup careers'],
            "structured_query_plans": {
                "ats": [
                    {
                        "query_text": 'site:job-boards.greenhouse.io "chief of staff"',
                        "executable": True,
                    }
                ],
                "search": [
                    {
                        "query_text": '"chief of staff" startup careers',
                        "executable": True,
                    }
                ],
                "weak_signal": [],
            },
            "query_themes": [],
            "company_archetypes": [],
            "priority_notes": [],
        },
    )

    def fake_search_fetch(self, queries, require_live=False):
        if queries and queries[0].startswith("site:job-boards.greenhouse.io"):
            return (
                [
                    SearchDiscoveryResult(
                        query_text=queries[0],
                        title="Chief of Staff - Acme",
                        url="https://job-boards.greenhouse.io/acme/jobs/1",
                        source_surface="search_web",
                        query_family="ats_direct",
                    )
                ],
                True,
            )
        return ([], True)

    monkeypatch.setattr("services.sync.SearchDiscoveryConnector.fetch", fake_search_fetch)
    monkeypatch.setattr("services.sync.GreenhouseConnector.fetch", lambda self, *_args, **_kwargs: ([], False))
    monkeypatch.setattr("services.sync.AshbyConnector.fetch", lambda self, *_args, **_kwargs: ([], False))
    monkeypatch.setattr("services.sync.XSearchConnector.fetch", lambda self, *_args, **_kwargs: ([], False))

    result = sync_all(session, include_rechecks=False, enabled_connectors={"search_web"})
    session.commit()

    cycle_metrics = result.discovery_status["cycle_metrics"]
    assert "search_zero_yield" not in cycle_metrics
    assert cycle_metrics["search_fetch_diagnostics"]["status"] == "results"
    assert cycle_metrics["search_fetch_diagnostics"]["result_count"] == 1
    assert cycle_metrics["search_fetch_diagnostics"]["worker_diagnostics"]["ats_resolver"]["status"] == "results"
    assert cycle_metrics["search_fetch_diagnostics"]["worker_diagnostics"]["search"]["status"] == "empty"
    assert result.discovery_status["source_matrix"]
    source_truth = {item["source_key"]: item for item in result.discovery_status["source_matrix"]}
    assert source_truth["search_web"]["ran"] is True
    assert source_truth["search_web"]["zero_yield"] is False
    assert source_truth["search_web"]["yielded_results_count"] == 1


def test_sync_all_uses_scrape_fallback_when_structured_jobs_verify_to_zero(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    ingest_resume(
        session,
        filename="resume.txt",
        raw_text="Operator with chief of staff and founding operations experience.",
    )

    settings = Settings(
        demo_mode=True,
        search_discovery_enabled=True,
        discovery_max_search_queries_per_cycle=4,
        discovery_max_expansions_per_cycle=4,
        discovery_max_new_companies_per_cycle=4,
        openai_enabled=False,
    )

    monkeypatch.setattr("services.sync.get_settings", lambda: settings)
    monkeypatch.setattr(
        "services.sync.planner_agent",
        lambda *_args, **_kwargs: {
            "queries": ['"chief of staff" startup careers'],
            "structured_query_plans": {
                "ats": [
                    {
                        "query_text": 'site:job-boards.greenhouse.io "chief of staff"',
                        "executable": True,
                    }
                ],
                "search": [],
                "weak_signal": [],
            },
            "query_themes": [],
            "company_archetypes": [],
            "priority_notes": [],
        },
    )
    monkeypatch.setattr(
        "services.sync.SearchDiscoveryConnector.fetch",
        lambda self, queries, require_live=False: (
            [
                SearchDiscoveryResult(
                    query_text=queries[0],
                    title="Chief of Staff - Acme",
                    url="https://job-boards.greenhouse.io/acme/jobs/1",
                    source_surface="search_web",
                    query_family="ats_direct",
                )
            ],
            True,
        ),
    )

    def fake_extractor(results, settings=None):
        if results and results[0].source_surface == "structured_zero_yield_fallback":
            extraction = ATSExtractionResult(
                source_url="https://acme.ai/careers",
                final_url="https://acme.ai/careers",
                page_title="Acme Careers",
                company_name="Acme",
                careers_url="https://acme.ai/careers",
                ats_type="greenhouse",
                greenhouse_tokens=["acme-next"],
                discovered_urls=["https://acme.ai/careers"],
                confidence=0.91,
            )
            return (
                [extraction],
                [
                    SearchDiscoveryResult(
                        query_text=results[0].query_text,
                        title="Chief of Staff - Acme",
                        url="https://job-boards.greenhouse.io/acme-next/jobs",
                        source_surface="structured_zero_yield_fallback",
                        query_family="ats_direct",
                    )
                ],
            )
        extraction = ATSExtractionResult(
            source_url="https://job-boards.greenhouse.io/acme/jobs/1",
            final_url="https://job-boards.greenhouse.io/acme/jobs/1",
            page_title="Chief of Staff - Acme",
            company_name="Acme",
            careers_url="https://acme.ai/careers",
            ats_type="greenhouse",
            greenhouse_tokens=["acme"],
            discovered_urls=["https://acme.ai/careers"],
            confidence=0.87,
        )
        return ([extraction], [])

    monkeypatch.setattr("services.sync.extractor_agent", fake_extractor)

    def fake_greenhouse_fetch(self, require_live, tokens, discovery_queries=None):
        if tokens == ["acme-next"]:
            return (
                [
                    {
                        "company_name": "Acme",
                        "company_domain": "acme.ai",
                        "title": "Chief of Staff",
                        "absolute_url": "https://job-boards.greenhouse.io/acme-next/jobs/2",
                        "url": "https://job-boards.greenhouse.io/acme-next/jobs/2",
                        "location": {"name": "Remote US"},
                        "content": "Build operating cadence.",
                        "source_board_token": "acme-next",
                        "id": "2",
                        "first_published": "2026-03-28T00:00:00Z",
                        "updated_at": "2026-03-28T12:00:00Z",
                        "discovery_source": "search_web",
                        "source_queries": ['site:job-boards.greenhouse.io "chief of staff"'],
                    }
                ],
                True,
            )
        return (
            [
                {
                    "company_name": "Acme",
                    "company_domain": "acme.ai",
                    "title": "Chief of Staff",
                    "absolute_url": "https://job-boards.greenhouse.io/acme/jobs/1",
                    "url": "https://job-boards.greenhouse.io/acme/jobs/1",
                    "location": {"name": "Remote US"},
                    "content": "Build operating cadence.",
                    "source_board_token": "acme",
                    "id": "1",
                    "first_published": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T12:00:00Z",
                    "discovery_source": "search_web",
                    "source_queries": ['site:job-boards.greenhouse.io "chief of staff"'],
                }
            ],
            True,
        )

    monkeypatch.setattr("services.sync.GreenhouseConnector.fetch", fake_greenhouse_fetch)
    monkeypatch.setattr("services.sync.AshbyConnector.fetch", lambda self, *_args, **_kwargs: ([], False))
    monkeypatch.setattr("services.sync.XSearchConnector.fetch", lambda self, *_args, **_kwargs: ([], False))
    monkeypatch.setattr(
        "services.sync.verify_listing",
        lambda record: None if record.url.endswith("/acme/jobs/1") else record,
    )

    result = sync_all(session, include_rechecks=False, enabled_connectors={"search_web", "greenhouse"})
    session.commit()

    listing = session.query(Listing).filter(Listing.source_type == "greenhouse").one()
    company = session.query(CompanyDiscovery).filter(CompanyDiscovery.discovery_key == "greenhouse:acme").one()
    source_truth = {item["source_key"]: item for item in result.discovery_status["source_matrix"]}

    assert listing.url == "https://job-boards.greenhouse.io/acme-next/jobs/2"
    assert result.discovery_metrics["greenhouse"]["verified"] == 1
    assert company.last_expansion_result_count == 2
    assert source_truth["greenhouse"]["yielded_results_count"] == 1
    assert source_truth["greenhouse"]["fallback_count"] >= 1
    assert source_truth["search_web_scrape_fallback"]["ran"] is True


def test_build_discovery_status_surfaces_verified_ranked_agentic_jobs() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    listing = Listing(
        company_name="Acme",
        title="Founding Operations Lead",
        location="San Francisco, CA",
        url="https://www.workatastartup.com/jobs/12345",
        source_type="yc_jobs",
        listing_status="active",
        freshness_days=1,
        metadata_json={
            "verification": {
                "canonical_url": "https://www.workatastartup.com/jobs/12345",
                "freshness_label": "fresh",
                "listing_status": "active",
                "dead_link_detected": False,
            }
        },
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="Acme",
            primary_title="Founding Operations Lead",
            listing_id=listing.id,
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            qualification_fit_label="strong fit",
            explanation="Strong operator match with fresh, verified evidence.",
            hidden=False,
            score_breakdown_json={
                "final_score": 8.7,
                "action_label": "Act now",
                "action_explanation": "Apply soon.",
                "explanation": {"headline": "Strong recommendation at 8.70 with high confidence."},
            },
            evidence_json={
                "source_platform": "yc_jobs",
                "source_lineage": "yc_jobs+search_web",
                "source_provenance": "discovered_new",
                "discovery_source": "search_web",
            },
        )
    )
    session.add(
        AgentRun(
            agent_name="Discovery",
            action="recorded discovery cycle metrics",
            summary="Discovery cycle metrics recorded.",
            affected_count=1,
            metadata_json={"cycle_metrics": {"search_zero_yield": {"reason": "search provider returned no accepted results", "zero_yield_attempt_count": 2}}},
        )
    )
    session.commit()

    status = build_discovery_status(session)

    assert status.agentic_slice_status["status"] == "verified_jobs_available"
    assert status.agentic_slice_status["verified_jobs"] == 1
    assert status.recent_agentic_leads[0]["company_name"] == "Acme"
    assert status.recent_agentic_leads[0]["verified"] is True
    assert status.recent_agentic_leads[0]["verification_status"] == "active"
    assert status.recent_agentic_leads[0]["recommendation_score"] == 8.7
    assert status.recent_agentic_leads[0]["source_provenance"] == "discovered_new"
    assert status.recent_agentic_leads[0]["explanation"] == "Strong recommendation at 8.70 with high confidence."
    assert status.recent_agentic_leads[0]["match_summary"] == "Strong recommendation at 8.70 with high confidence."


def test_build_discovery_status_reports_zero_yield_when_no_verified_agentic_jobs_exist() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    session.add(
        AgentRun(
            agent_name="Discovery",
            action="recorded discovery cycle metrics",
            summary="Discovery cycle metrics recorded.",
            affected_count=0,
            metadata_json={"cycle_metrics": {"search_zero_yield": {"reason": "provider self-links only", "zero_yield_attempt_count": 3}}},
        )
    )
    session.commit()

    status = build_discovery_status(session)

    assert status.recent_agentic_leads == []
    assert status.agentic_slice_status["status"] == "zero_yield"
    assert status.agentic_slice_status["zero_yield"] is True
    assert "provider self-links only" in status.agentic_slice_status["summary"]


def test_build_discovery_status_reports_live_discovery_unavailable_when_only_demo_or_disabled_sources_exist(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    monkeypatch.setattr(
        "services.company_discovery.get_settings",
        lambda: Settings(
            demo_mode=True,
            greenhouse_enabled=False,
            ashby_orgs=[],
            search_discovery_enabled=False,
        ),
    )

    status = build_discovery_status(session)

    assert status.recent_agentic_leads == []
    assert status.agentic_slice_status["status"] == "live_discovery_unavailable"
    assert status.agentic_slice_status["live_runnable"] is False
    assert "Live job discovery is not runnable in this environment." in status.agentic_slice_status["summary"]
    assert "Search Web" in status.agentic_slice_status["summary"]
    assert "Greenhouse" in status.agentic_slice_status["summary"]


def test_build_discovery_status_reports_latest_live_discovery_failure() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    session.add(
        SearchRun(
            source_key="search_web",
            worker_name="search",
            provider="duckduckgo_html",
            status="failed",
            live=True,
            query_count=1,
            result_count=0,
            queries_json=['"chief of staff" startup careers'],
            failure_classification="search_timeout",
            error="search request timed out",
            diagnostics_json={"status": "failed", "failure_classification": "search_timeout", "error": "search request timed out"},
        )
    )
    session.add(
        AgentRun(
            agent_name="Discovery",
            action="recorded discovery cycle metrics",
            summary="Discovery cycle metrics recorded.",
            affected_count=0,
            metadata_json={
                "cycle_metrics": {
                    "search_fetch_diagnostics": {
                        "status": "failed",
                        "failure_classification": "search_timeout",
                        "error": "search request timed out",
                        "worker_diagnostics": {
                            "search": {"status": "failed", "failure_classification": "search_timeout", "error": "search request timed out"},
                            "ats_resolver": {"status": "empty"},
                        },
                    }
                }
            },
        )
    )
    session.commit()

    status = build_discovery_status(session)

    assert status.agentic_slice_status["status"] == "live_discovery_failed"
    assert status.agentic_slice_status["failure_classification"] == "search_timeout"
    assert status.agentic_slice_status["failed_workers"] == ["search"]
    assert "search request timed out" in status.agentic_slice_status["summary"]
    assert '"chief of staff" startup careers' in status.agentic_slice_status["summary"]


def test_build_discovery_status_falls_back_to_latest_search_run_truth_when_cycle_metrics_are_missing() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    session.add(
        SearchRun(
            source_key="search_web",
            worker_name="search",
            provider="duckduckgo_html",
            status="failed",
            live=True,
            query_count=1,
            result_count=0,
            queries_json=['"chief of staff" startup careers'],
            failure_classification="search_timeout",
            error="search request timed out",
            diagnostics_json={"status": "failed", "failure_classification": "search_timeout", "error": "search request timed out"},
        )
    )
    session.commit()

    status = build_discovery_status(session)

    assert status.agentic_slice_status["status"] == "live_discovery_failed"
    assert status.agentic_slice_status["failure_classification"] == "search_timeout"
    assert "search request timed out" in status.agentic_slice_status["summary"]
    assert '"chief of staff" startup careers' in status.agentic_slice_status["summary"]
