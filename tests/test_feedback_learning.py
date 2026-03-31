from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime

from core.models import Application, Base, Lead, Listing, SourceQuery, SourceQueryStat
from core.schemas import FeedbackRequest
from services.feedback import submit_feedback
from services.feedback_learning import categorize_rejection_feedback, generate_improvement_recommendations
from services.profile import get_candidate_profile


def test_feedback_generates_learning_and_queries() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    profile = get_candidate_profile(session)
    lead = Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Deployment Strategist",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "go_to_market"},
        evidence_json={"company_domain": "demo.ai", "source_type": "ashby", "source_queries": ["deployment strategist hiring"], "snippets": ["customer deployments and systems"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="more_like_this"))
    learning = profile.extracted_summary_json["learning"]
    assert learning["title_weights"]["deployment strategist"] > 0
    assert learning["generated_queries"]


def test_save_and_applied_create_application_state() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    lead = Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Chief of Staff",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "operations", "composite": 7.0},
        evidence_json={"company_domain": "demo.ai", "source_type": "ashby", "source_queries": [], "snippets": ["ops"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="save"))
    application = session.query(Application).filter(Application.lead_id == lead.id).one()
    assert application.current_status == "saved"
    assert application.date_saved is not None

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="applied"))
    assert application.current_status == "applied"
    assert application.date_applied is not None


def test_wrong_geography_and_irrelevant_company_update_learning_biases() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    profile = get_candidate_profile(session)
    lead = Lead(
        lead_type="listing",
        company_name="FarAwayCo",
        primary_title="Operations Lead",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "operations", "composite": 7.0},
        evidence_json={"company_domain": "faraway.co", "source_type": "greenhouse", "location_scope": "uk", "source_queries": [], "snippets": ["ops"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="wrong_geography"))
    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="irrelevant_company"))
    learning = profile.extracted_summary_json["learning"]
    assert learning["location_penalties"]["uk"] > 0
    assert learning["company_penalties"]["farawayco"] > 0


def test_dislike_persists_user_dismissal_and_restore_clears_it() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    listing = Listing(
        company_name="DismissCo",
        title="Chief of Staff",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/dismissco/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Operator partner role.",
        listing_status="active",
        freshness_hours=1.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add(listing)
    session.flush()

    lead = Lead(
        lead_type="listing",
        company_name="DismissCo",
        primary_title="Chief of Staff",
        listing_id=listing.id,
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "operations", "composite": 7.0},
        evidence_json={"company_domain": "dismiss.co", "source_type": "greenhouse", "source_queries": [], "snippets": ["ops"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="dislike"))

    assert lead.hidden is True
    assert lead.evidence_json["user_dismissed_at"] is not None
    assert lead.evidence_json["user_hidden_reason"] == "Dismissed from jobs list"

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="restore"))

    assert "user_dismissed_at" not in (lead.evidence_json or {})
    assert "user_hidden_reason" not in (lead.evidence_json or {})
    assert lead.hidden is False


def test_feedback_updates_query_stats_once_per_event() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    lead = Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Deployment Strategist",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "go_to_market"},
        evidence_json={"company_domain": "demo.ai", "source_type": "x", "source_queries": ["deployment strategist hiring"], "snippets": ["ops"]},
    )
    session.add(lead)
    session.add(
        SourceQuery(
            query_text="deployment strategist hiring",
            source_type="x",
            performance_stats_json={"likes": 0},
        )
    )
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="more_like_this"))

    query = session.query(SourceQuery).filter(SourceQuery.query_text == "deployment strategist hiring").one()
    stat = session.query(SourceQueryStat).filter(SourceQueryStat.query_text == "deployment strategist hiring").one()
    assert query.performance_stats_json["likes"] == 1
    assert stat.likes == 1


def test_feedback_persists_recent_events_for_ranking() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    profile = get_candidate_profile(session)
    lead = Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Chief of Staff",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "operations", "composite": 7.0},
        evidence_json={"company_domain": "demo.ai", "source_type": "greenhouse", "source_queries": [], "snippets": ["ops"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="save"))
    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="dislike"))

    events = profile.extracted_summary_json["learning"]["feedback_events"]
    assert len(events) == 2
    assert events[-2]["action"] == "save"
    assert events[-1]["action"] == "dislike"
    assert events[-1]["role_family"] == "operations"


def test_categorize_rejection_feedback_maps_codes_into_reason_buckets() -> None:
    feedback = categorize_rejection_feedback(
        status_reason_code="panel_decline",
        outcome_reason_code="insufficient_b2b_saas_depth",
        notes="Panel wanted deeper pricing ownership examples.",
    )

    assert feedback["status_reason_code"] == "panel_decline"
    assert feedback["outcome_reason_code"] == "insufficient_b2b_saas_depth"
    assert feedback["reason_buckets"] == ["interview_performance", "domain_depth", "pricing_depth"]


def test_generate_improvement_recommendations_uses_note_fallbacks_for_targeted_guidance() -> None:
    recommendations = generate_improvement_recommendations(
        status_reason_code=None,
        outcome_reason_code=None,
        notes="Feedback was that the examples were not specific enough and lacked clear metrics.",
    )

    assert recommendations == [
        "Replace general responsibility language with quantified before-and-after outcomes from the same type of problem."
    ]
