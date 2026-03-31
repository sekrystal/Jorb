from __future__ import annotations

from datetime import datetime, timedelta
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from connectors.search_web import ATSExtractionResult, SearchDiscoveryResult
from core.config import Settings
from core.models import AgentRun, Base, CandidateProfile, Lead, Listing
from core.schemas import FeedbackRequest
from services.feedback import submit_feedback
from services.pipeline import run_critic_agent
from services import sync as sync_service
from services.sync import list_leads, list_leads_payload


def _seed_profile(session) -> None:
    session.add(
        CandidateProfile(
            name="Tester",
            raw_resume_text="ops profile",
            core_titles_json=["operations lead", "deployment strategist"],
            preferred_locations_json=["Remote", "San Francisco", "New York"],
            minimum_fit_threshold=2.8,
        )
    )
    session.commit()


def test_signal_only_leads_are_excluded_by_default() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    session.add(
        Lead(
            lead_type="signal",
            company_name="Signal Co",
            primary_title="Chief of Staff",
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
                confidence_label="low",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Signal lead",
                score_breakdown_json={"composite": 4.5},
                evidence_json={"url": "https://x.com/demo/status/1", "source_type": "x", "source_platform": "x_demo", "freshness_days": 0},
                hidden=False,
            )
        )
    session.add(
        Listing(
            company_name="Listing Co",
            title="Operations Lead",
            location="Remote",
            url="https://jobs.example.com/1",
            source_type="ashby",
            posted_at=datetime.utcnow(),
            first_published_at=datetime.utcnow(),
            last_seen_at=datetime.utcnow(),
            description_text="Own operating cadence and planning.",
            listing_status="active",
            freshness_hours=4.0,
            freshness_days=1,
            metadata_json={},
        )
    )
    session.flush()
    listing = session.query(Listing).filter(Listing.company_name == "Listing Co").one()
    session.add(
        Lead(
            lead_type="listing",
            company_name="Listing Co",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Listing lead",
            score_breakdown_json={"composite": 6.2},
            evidence_json={"url": "https://jobs.example.com/1", "source_type": "ashby", "source_platform": "ashby", "freshness_days": 1, "listing_status": "active"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)
    assert len(items) == 1
    assert items[0].lead_type == "listing"
    assert items[0].source_platform == "ashby"

    items_with_signals = list_leads(session, include_signal_only=True)
    assert {item.lead_type for item in items_with_signals} == {"listing", "signal"}


def test_lead_response_exposes_source_provenance_and_lineage() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="Agentic Co",
        title="Operations Lead",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/agentic/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Own operating cadence and planning.",
        listing_status="active",
        freshness_hours=3.0,
        freshness_days=0,
        metadata_json={
            "discovery_source": "search_web",
            "surface_provenance": "discovered_new",
            "source_lineage": "greenhouse+search_web",
            "source_board_token": "agentic",
        },
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="Agentic Co",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Agent discovered listing",
            score_breakdown_json={"composite": 7.5},
            evidence_json={"url": listing.url, "source_type": "greenhouse", "source_platform": "greenhouse+search_web"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)

    assert len(items) == 1
    assert items[0].source_provenance == "discovered_new"
    assert items[0].source_lineage == "greenhouse+search_web"
    assert items[0].discovery_source == "search_web"


def test_list_leads_payload_applies_backend_search_contract_and_ranks_stronger_results_first() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)
    profile = session.query(CandidateProfile).one()
    profile.core_titles_json = [*(profile.core_titles_json or []), "chief of staff", "operations program lead"]
    session.commit()

    listing_exact = Listing(
        company_name="ExactCo",
        title="Chief of Staff",
        location="Remote, US",
        url="https://jobs.example.com/exact",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Own operating cadence for the leadership team.",
        listing_status="active",
        freshness_hours=2.0,
        freshness_days=0,
        metadata_json={},
    )
    listing_related = Listing(
        company_name="RelatedCo",
        title="Operations Program Lead",
        location="Remote, US",
        url="https://jobs.example.com/related",
        source_type="ashby",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="This role partners closely with the chief of staff and CEO.",
        listing_status="active",
        freshness_hours=2.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add_all([listing_exact, listing_related])
    session.flush()
    session.add_all(
        [
            Lead(
                lead_type="listing",
                company_name="ExactCo",
                primary_title="Chief of Staff",
                listing_id=listing_exact.id,
                surfaced_at=datetime.utcnow(),
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Exact title match.",
                score_breakdown_json={"final_score": 7.5, "explanation": {"summary": "Exact title fit."}},
                evidence_json={"url": listing_exact.url, "source_type": "greenhouse", "source_platform": "greenhouse", "location": "Remote, US"},
                hidden=False,
            ),
            Lead(
                lead_type="listing",
                company_name="RelatedCo",
                primary_title="Operations Program Lead",
                listing_id=listing_related.id,
                surfaced_at=datetime.utcnow(),
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="adjacent match",
                qualification_fit_label="strong fit",
                explanation="Description mentions the chief of staff partnership.",
                score_breakdown_json={"final_score": 9.1, "explanation": {"summary": "Description-only match."}},
                evidence_json={"url": listing_related.url, "source_type": "ashby", "source_platform": "ashby", "location": "Remote, US"},
                hidden=False,
            ),
        ]
    )
    session.commit()

    payload = list_leads_payload(session, include_hidden=True, include_unqualified=True, q="chief of staff")

    assert [item.company_name for item in payload["items"]] == ["ExactCo", "RelatedCo"]
    assert payload["search_meta"] == {
        "query": "chief of staff",
        "normalized_query": "chief of staff",
        "tokens": ["chief", "of", "staff"],
        "searched_fields": ["title", "company", "location", "description", "tags", "explanation", "source"],
        "backend_applied": True,
        "fallback_mode": "backend",
        "partial_results": False,
        "status": "results",
        "candidate_count": 2,
        "result_count": 2,
        "ranking": "match_score_then_recommendation_then_recency_then_title_then_company",
    }
    assert payload["items"][0].evidence_json["search_match"]["matched_fields"][0] == "title_exact"


def test_list_leads_payload_search_contract_reports_empty_results_with_query_meta() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="Acme",
        title="Operations Lead",
        location="Remote, US",
        url="https://jobs.example.com/acme",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Own recruiting systems and planning.",
        listing_status="active",
        freshness_hours=2.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="Acme",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Relevant operations role.",
            score_breakdown_json={"final_score": 8.0, "explanation": {"summary": "Operations fit."}},
            evidence_json={"url": listing.url, "source_type": "greenhouse", "source_platform": "greenhouse", "location": "Remote, US"},
            hidden=False,
        )
    )
    session.commit()

    payload = list_leads_payload(session, include_hidden=True, include_unqualified=True, q="nuclear engineer")

    assert payload["items"] == []
    assert payload["search_meta"]["status"] == "empty"
    assert payload["search_meta"]["query"] == "nuclear engineer"
    assert payload["search_meta"]["result_count"] == 0


def test_discovery_status_cycle_metrics_support_bridge_samples() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    session.add(
        AgentRun(
            agent_name="Discovery",
            action="recorded discovery cycle metrics",
            summary="Discovery bridge metrics recorded.",
            affected_count=1,
            metadata_json={
                "cycle_metrics": {
                    "accepted_results_count": 5,
                    "candidate_count": 3,
                    "selected_expansion_count": 2,
                    "empty_expansion_count": 1,
                    "listings_yielded_count": 4,
                    "candidate_conversion_success_count": 3,
                    "candidate_conversion_drop_count": 2,
                    "accepted_urls_sample": ["https://job-boards.greenhouse.io/acme"],
                    "dropped_urls_sample": ["https://duckduckgo.com/"],
                }
            },
        )
    )
    session.commit()

    from services.company_discovery import build_discovery_status

    status = build_discovery_status(session)
    assert status.cycle_metrics["accepted_results_count"] == 5
    assert status.cycle_metrics["candidate_count"] == 3
    assert status.cycle_metrics["selected_expansion_count"] == 2
    assert status.cycle_metrics["empty_expansion_count"] == 1
    assert status.cycle_metrics["listings_yielded_count"] == 4
    assert status.cycle_metrics["candidate_conversion_success_count"] == 3
    assert status.cycle_metrics["accepted_urls_sample"] == ["https://job-boards.greenhouse.io/acme"]


def test_default_leads_query_suppresses_stale_listing_even_if_lead_snapshot_looks_fresh() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="ArchiveCo",
        title="Chief of Staff",
        location="San Francisco, CA",
        url="https://boards.greenhouse.io/archive/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow() - timedelta(days=45),
        first_published_at=datetime.utcnow() - timedelta(days=45),
        last_seen_at=datetime.utcnow() - timedelta(days=45),
        description_text="This position has been filled.",
        listing_status="expired",
        freshness_hours=45 * 24,
        freshness_days=45,
        metadata_json={"page_text": "job no longer available"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="ArchiveCo",
            primary_title="Chief of Staff",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Should not surface",
            score_breakdown_json={"composite": 7.0},
            evidence_json={"url": listing.url, "source_type": "greenhouse", "freshness_days": 1, "listing_status": "active"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)
    assert items == []


def test_combined_lead_with_stale_backing_listing_is_hidden_by_default() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="Mercor",
        title="Deployment Strategist",
        location="Remote, US",
        url="https://jobs.ashbyhq.com/Mercor/1",
        source_type="ashby",
        posted_at=datetime.utcnow() - timedelta(days=31),
        first_published_at=datetime.utcnow() - timedelta(days=31),
        last_seen_at=datetime.utcnow() - timedelta(days=31),
        description_text="Posting closed archived position.",
        listing_status="suspected_expired",
        freshness_hours=31 * 24,
        freshness_days=31,
        metadata_json={"page_text": "posting closed archived"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="combined",
            company_name="Mercor",
            primary_title="Deployment Strategist",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Combined lead",
            score_breakdown_json={"composite": 7.0},
            evidence_json={"url": listing.url, "source_type": "ashby", "freshness_days": 1, "listing_status": "active"},
            hidden=False,
        )
    )
    session.commit()

    assert list_leads(session) == []


def test_critic_is_final_gate_and_marks_non_live_rows_hidden() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="BrokenCo",
        title="Ops Lead",
        location="Remote",
        url="https://jobs.example.com/broken",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Role text",
        listing_status="active",
        freshness_hours=2.0,
        freshness_days=1,
        metadata_json={"http_status": 404},
    )
    session.add(listing)
    session.flush()
    lead = Lead(
        lead_type="listing",
        company_name="BrokenCo",
        primary_title="Ops Lead",
        listing_id=listing.id,
        surfaced_at=datetime.utcnow(),
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Broken should hide",
        score_breakdown_json={"composite": 6.0},
        evidence_json={"url": listing.url, "source_type": "greenhouse"},
        hidden=False,
    )
    session.add(lead)
    session.commit()

    run_critic_agent(session)
    session.commit()
    session.refresh(lead)

    assert lead.hidden is True
    assert lead.evidence_json["critic_status"] == "suppressed"
    assert "HTTP 404" in "; ".join(lead.evidence_json["critic_reasons"])


def test_ranker_cannot_force_suppressed_row_into_default_results() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="OldCo",
        title="Operations Lead",
        location="Remote",
        url="https://jobs.example.com/old",
        source_type="greenhouse",
        posted_at=datetime.utcnow() - timedelta(days=50),
        first_published_at=datetime.utcnow() - timedelta(days=50),
        last_seen_at=datetime.utcnow() - timedelta(days=50),
        description_text="Page not found.",
        listing_status="expired",
        freshness_hours=50 * 24,
        freshness_days=50,
        metadata_json={"page_text": "page not found"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="OldCo",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Ranked strongly but should hide",
            score_breakdown_json={"composite": 9.0},
            evidence_json={"url": listing.url, "source_type": "greenhouse"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)
    assert items == []


def test_list_leads_emits_timing_logs_on_success(caplog) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="Timing Co",
        title="Operations Lead",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/timing/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Own planning and cadence.",
        listing_status="active",
        freshness_hours=2.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="Timing Co",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Visible listing",
            score_breakdown_json={"composite": 6.1},
            evidence_json={"url": listing.url, "source_type": "greenhouse", "source_platform": "greenhouse"},
            hidden=False,
        )
    )
    session.commit()

    with caplog.at_level(logging.INFO):
        items = list_leads(session)

    assert len(items) == 1
    messages = [record.getMessage() for record in caplog.records]
    assert any("[LEADS_REQUEST_START]" in message for message in messages)
    assert any("[LEADS_STAGE_TIMING]" in message for message in messages)
    assert any("[LEADS_TIMING]" in message for message in messages)


def test_list_leads_location_cache_and_deduped_location_logs(caplog) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    first_listing = Listing(
        company_name="Stripe",
        title="Account Executive",
        location="Dublin, Ireland",
        url="https://boards.greenhouse.io/stripe/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Revenue role.",
        listing_status="active",
        freshness_hours=4.0,
        freshness_days=0,
        metadata_json={},
    )
    second_listing = Listing(
        company_name="Stripe",
        title="Account Executive",
        location="Dublin, Ireland",
        url="https://boards.greenhouse.io/stripe/jobs/2",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Revenue role duplicate.",
        listing_status="active",
        freshness_hours=4.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add_all([first_listing, second_listing])
    session.flush()
    session.add_all(
        [
            Lead(
                lead_type="listing",
                company_name="Stripe",
                primary_title="Account Executive",
                listing_id=first_listing.id,
                surfaced_at=datetime.utcnow(),
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Blocked by geography",
                score_breakdown_json={"composite": 6.0},
                evidence_json={"url": first_listing.url, "source_type": "greenhouse", "source_platform": "greenhouse"},
                hidden=False,
            ),
            Lead(
                lead_type="listing",
                company_name="Stripe",
                primary_title="Account Executive",
                listing_id=second_listing.id,
                surfaced_at=datetime.utcnow(),
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Blocked by geography",
                score_breakdown_json={"composite": 6.0},
                evidence_json={"url": second_listing.url, "source_type": "greenhouse", "source_platform": "greenhouse"},
                hidden=False,
            ),
        ]
    )
    session.commit()

    with caplog.at_level(logging.INFO):
        items = list_leads(session)

    assert items == []
    messages = [record.getMessage() for record in caplog.records]
    location_gate_messages = [message for message in messages if "[LOCATION_GATE]" in message]
    assert len(location_gate_messages) == 1

    cache_message = next(message for message in messages if "[LOCATION_GATE_CACHE]" in message)
    assert "'total_calls': 2" in cache_message
    assert "'unique_keys': 1" in cache_message
    assert "'cache_hits': 1" in cache_message
    assert "'cache_misses': 1" in cache_message

    deduped_message = next(message for message in messages if "[LOCATION_GATE_DEDUPED]" in message)
    assert "'emitted_count': 1" in deduped_message
    assert "'suppressed_duplicate_count': 0" in deduped_message


def test_list_leads_does_not_invoke_ai_critic_when_readtime_ai_disabled(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="NoAI Co",
        title="Operations Lead",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/noai/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Own planning and cadence.",
        listing_status="active",
        freshness_hours=1.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="NoAI Co",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Visible listing",
            score_breakdown_json={"composite": 6.5},
            evidence_json={"url": listing.url, "source_type": "greenhouse"},
            hidden=False,
        )
    )
    session.commit()

    monkeypatch.setattr(sync_service, "get_settings", lambda: Settings(enable_ai_readtime_critic=False))

    def fail_if_called(*args, **kwargs):
        raise AssertionError("judge_critic_with_ai should not be called during read path when disabled")

    monkeypatch.setattr(sync_service, "judge_critic_with_ai", fail_if_called)

    items = list_leads(session)

    assert len(items) == 1


def test_list_leads_reuses_persisted_ai_critic_without_fresh_call(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="PersistedAI Co",
        title="Chief of Staff",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/persisted/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Founder partner role.",
        listing_status="active",
        freshness_hours=1.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="PersistedAI Co",
            primary_title="Chief of Staff",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Visible listing",
            score_breakdown_json={"composite": 7.0},
            evidence_json={
                "url": listing.url,
                "source_type": "greenhouse",
                "ai_critic_assessment": {
                    "quality_assessment": "uncertain",
                    "reasons": ["persisted warning"],
                },
            },
            hidden=False,
        )
    )
    session.commit()

    monkeypatch.setattr(sync_service, "get_settings", lambda: Settings(enable_ai_readtime_critic=False))

    def fail_if_called(*args, **kwargs):
        raise AssertionError("judge_critic_with_ai should not be called when persisted assessment exists")

    monkeypatch.setattr(sync_service, "judge_critic_with_ai", fail_if_called)

    items = list_leads(session, include_hidden=True)

    assert len(items) == 1
    assert items[0].hidden is True
    assert items[0].evidence_json["critic_status"] == "uncertain"


def test_dismissed_lead_is_hidden_by_default_and_restorable() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="RestoreCo",
        title="Chief of Staff",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/restoreco/jobs/1",
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
        company_name="RestoreCo",
        primary_title="Chief of Staff",
        listing_id=listing.id,
        surfaced_at=datetime.utcnow(),
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Visible listing",
        score_breakdown_json={"composite": 7.0},
        evidence_json={"url": listing.url, "source_type": "greenhouse"},
        hidden=False,
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="dislike"))

    visible_items = list_leads(session)
    assert lead.id not in [item.id for item in visible_items]

    hidden_items = list_leads(session, include_hidden=True)
    hidden_item = next(item for item in hidden_items if item.id == lead.id)
    assert hidden_item.hidden is True
    assert hidden_item.evidence_json["suppression_category"] == "user_dismissed"

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="restore"))

    restored_items = list_leads(session)
    restored_item = next(item for item in restored_items if item.id == lead.id)
    assert restored_item.hidden is False


def test_duplicate_listings_collapse_into_one_visible_lead_across_sources() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    greenhouse = Listing(
        company_name="Acme, Inc.",
        title="Founding Operations Lead",
        location="San Francisco, CA",
        url="https://job-boards.greenhouse.io/acme/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Operator role.",
        listing_status="active",
        freshness_hours=4.0,
        freshness_days=0,
        metadata_json={"canonical_job": {"identity_key": "acme::founding-operations-lead::san-francisco"}},
    )
    ashby = Listing(
        company_name="Acme",
        title="Founding Operations Lead",
        location="San Francisco",
        url="https://jobs.ashbyhq.com/acme/2",
        source_type="ashby",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Same operator role.",
        listing_status="active",
        freshness_hours=5.0,
        freshness_days=0,
        metadata_json={"canonical_job": {"identity_key": "acme::founding-operations-lead::san-francisco"}},
    )
    session.add_all([greenhouse, ashby])
    session.flush()
    lead_one = Lead(
        lead_type="listing",
        company_name="Acme, Inc.",
        primary_title="Founding Operations Lead",
        listing_id=greenhouse.id,
        surfaced_at=datetime.utcnow(),
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Higher quality ATS lead",
        score_breakdown_json={"composite": 8.1, "match_tier": "high"},
        evidence_json={"url": greenhouse.url, "source_type": "greenhouse"},
        hidden=False,
    )
    lead_two = Lead(
        lead_type="listing",
        company_name="Acme",
        primary_title="Founding Operations Lead",
        listing_id=ashby.id,
        surfaced_at=datetime.utcnow(),
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Duplicate ashby lead",
        score_breakdown_json={"composite": 8.0, "match_tier": "high"},
        evidence_json={"url": ashby.url, "source_type": "ashby"},
        hidden=False,
    )
    session.add_all([lead_one, lead_two])
    session.commit()

    items = list_leads(session)

    assert len(items) == 1
    assert items[0].source_type == "greenhouse"
    assert items[0].evidence_json["duplicate_merge"]["duplicate_count"] == 2
    assert set(items[0].evidence_json["duplicate_merge"]["merged_sources"]) == {"greenhouse", "ashby"}


def test_low_match_tier_lead_is_hidden_from_default_views() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="LowFitCo",
        title="Head of Recruiting",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/lowfit/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Executive recruiting leadership role.",
        listing_status="active",
        freshness_hours=1.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add(listing)
    session.flush()
    lead = Lead(
        lead_type="listing",
        company_name="LowFitCo",
        primary_title="Head of Recruiting",
        listing_id=listing.id,
        surfaced_at=datetime.utcnow(),
        rank_label="weak",
        confidence_label="medium",
        freshness_label="fresh",
        title_fit_label="weak title match",
        qualification_fit_label="stretch",
        explanation="Low fit listing",
        score_breakdown_json={"composite": 4.5, "match_tier": "low"},
        evidence_json={"url": listing.url, "source_type": "greenhouse"},
        hidden=False,
    )
    session.add(lead)
    session.commit()

    assert list_leads(session) == []

    hidden_items = list_leads(session, include_hidden=True)
    assert len(hidden_items) == 1
    assert hidden_items[0].hidden is True
    assert hidden_items[0].evidence_json["suppression_category"] == "low_fit"


def test_feedback_changes_visible_ranking_order_for_similar_jobs(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)
    def rerank_current_leads(local_session) -> None:
        from services.profile import get_candidate_profile
        from services.ranking import score_lead

        profile = get_candidate_profile(local_session)
        learning = (profile.extracted_summary_json or {}).get("learning", {})
        for lead in local_session.query(Lead).all():
            listing = local_session.get(Listing, lead.listing_id)
            breakdown = score_lead(
                profile=profile,
                lead_type=lead.lead_type,
                title=lead.primary_title,
                company_name=lead.company_name,
                company_domain=(lead.evidence_json or {}).get("company_domain"),
                location=(listing.location if listing else (lead.evidence_json or {}).get("location")),
                description_text=(listing.description_text if listing else ""),
                freshness_label=lead.freshness_label,
                listing_status=lead.listing_status if hasattr(lead, "listing_status") else (listing.listing_status if listing else "active"),
                source_type=(lead.evidence_json or {}).get("source_type", "greenhouse"),
                evidence_count=1,
                feedback_learning=learning,
            )
            lead.score_breakdown_json = breakdown
            lead.rank_label = breakdown["rank_label"]
        local_session.flush()

    monkeypatch.setattr("services.feedback.run_ranker_agent", rerank_current_leads)
    monkeypatch.setattr("services.feedback.run_critic_agent", lambda _session: None)

    first_listing = Listing(
        company_name="Alpha",
        title="Chief of Staff",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/alpha/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Chief of Staff role with SQL and stakeholder management.",
        listing_status="active",
        freshness_hours=1.0,
        freshness_days=0,
        metadata_json={},
    )
    second_listing = Listing(
        company_name="Beta",
        title="Chief of Staff",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/beta/jobs/2",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Chief of Staff role with SQL, stakeholder management, and process design for the executive team.",
        listing_status="active",
        freshness_hours=1.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add_all([first_listing, second_listing])
    session.flush()
    first_lead = Lead(
        lead_type="listing",
        company_name="Alpha",
        primary_title="Chief of Staff",
        listing_id=first_listing.id,
        surfaced_at=datetime.utcnow(),
        rank_label="medium",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Baseline lead",
        score_breakdown_json={"final_score": 7.2, "composite": 7.2, "match_tier": "medium", "role_family": "operations"},
        evidence_json={"url": first_listing.url, "source_type": "greenhouse", "company_domain": "alpha.ai"},
        hidden=False,
    )
    second_lead = Lead(
        lead_type="listing",
        company_name="Beta",
        primary_title="Chief of Staff",
        listing_id=second_listing.id,
        surfaced_at=datetime.utcnow(),
        rank_label="medium",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Slightly lower before feedback",
        score_breakdown_json={"final_score": 7.0, "composite": 7.0, "match_tier": "medium", "role_family": "operations"},
        evidence_json={"url": second_listing.url, "source_type": "greenhouse", "company_domain": "beta.ai"},
        hidden=False,
    )
    session.add_all([first_lead, second_lead])
    session.commit()

    before = [item for item in list_leads(session) if item.company_name in {"Alpha", "Beta"}]
    assert [item.company_name for item in before][:2] == ["Alpha", "Beta"]

    submit_feedback(session, FeedbackRequest(lead_id=second_lead.id, action="save"))

    after = [item for item in list_leads(session) if item.company_name in {"Alpha", "Beta"}]
    assert [item.company_name for item in after][:2] == ["Beta", "Alpha"]


def test_sync_all_caps_ai_fit_calls_per_cycle(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    now = datetime.utcnow()
    monkeypatch.setattr(sync_service, "get_candidate_profile", lambda _session: session.query(CandidateProfile).first())
    monkeypatch.setattr(sync_service, "ensure_source_queries", lambda _session: [])
    monkeypatch.setattr(sync_service, "generate_follow_up_tasks", lambda _session: 0)

    call_counter = {"count": 0}

    def fake_judge_fit_with_ai(**kwargs):
        call_counter["count"] += 1
        return {"classification": "strong_fit", "reasons": [], "matched_profile_fields": []}

    monkeypatch.setattr(sync_service, "judge_fit_with_ai", fake_judge_fit_with_ai)

    greenhouse_jobs = [
        {
            "id": f"gh-{index}",
            "title": "Operations Lead",
            "absolute_url": f"https://job-boards.greenhouse.io/capco-{index}/jobs/{index}",
            "updated_at": now.isoformat(),
            "first_published": now.isoformat(),
            "location": {"name": "Remote, US"},
            "content": "Own planning and cadence.",
            "companyName": f"CapCo {index}",
            "source_board_token": f"capco-{index}",
        }
        for index in range(3)
    ]

    def fake_run_connector_fetch(_session, connector_name, fetch_fn, date_fields=None):
        if connector_name == "greenhouse":
            return greenhouse_jobs, True, None
        return [], False, None

    monkeypatch.setattr(sync_service, "run_connector_fetch", fake_run_connector_fetch)
    monkeypatch.setattr(sync_service, "get_settings", lambda: Settings(greenhouse_enabled=True, ai_fit_max_calls_per_cycle=1))

    result = sync_service.sync_all(session, enabled_connectors={"greenhouse"})

    assert call_counter["count"] == 1
    assert result.discovery_status["cycle_metrics"]["ai_fit_calls_used"] == 1


def test_sync_all_persists_query_family_metrics(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    monkeypatch.setattr(sync_service, "get_candidate_profile", lambda _session: session.query(CandidateProfile).first())
    monkeypatch.setattr(sync_service, "ensure_source_queries", lambda _session: [])
    monkeypatch.setattr(sync_service, "generate_follow_up_tasks", lambda _session: 0)
    monkeypatch.setattr(
        sync_service,
        "planner_agent",
        lambda *_args, **_kwargs: {
            "queries": ['site:job-boards.greenhouse.io "operations lead"'],
            "query_themes": ["ats_direct"],
            "company_archetypes": [],
            "priority_notes": [],
        },
    )
    monkeypatch.setattr(sync_service, "extractor_agent", lambda *_args, **_kwargs: ([], []))
    monkeypatch.setattr(sync_service, "learning_agent", lambda *_args, **_kwargs: {"next_queries": [], "focus_companies": [], "notes": []})

    def fake_triage_agent(*, session, profile, candidate, configured_boards, settings):
        return 3.2, ["deterministic test"], "pursue"

    monkeypatch.setattr(sync_service, "triage_agent", fake_triage_agent)

    def fake_run_connector_fetch(_session, connector_name, fetch_fn, date_fields=None):
        if connector_name == "search_web":
            return [
                SearchDiscoveryResult(
                    query_text='site:job-boards.greenhouse.io "operations lead"',
                    title="Operations Lead - Acme",
                    url="https://job-boards.greenhouse.io/acme/jobs/1",
                    query_family="ats_direct",
                )
            ], True, None
        return [], False, None

    monkeypatch.setattr(sync_service, "run_connector_fetch", fake_run_connector_fetch)
    monkeypatch.setattr(sync_service, "get_settings", lambda: Settings(search_discovery_enabled=True))

    result = sync_service.sync_all(session, enabled_connectors={"search_web"})

    query_family_metrics = result.discovery_status["cycle_metrics"]["query_family_metrics"]
    assert query_family_metrics["ats_direct"]["queries_attempted"] == 1
    assert query_family_metrics["ats_direct"]["accepted_results"] == 1
    assert query_family_metrics["ats_direct"]["candidate_conversions"] == 1
    assert query_family_metrics["ats_direct"]["selected_for_expansion"] == 1
    assert query_family_metrics["ats_direct"]["zero_visible_yield_expansions"] == 1
    assert query_family_metrics["ats_direct"]["visible_yield_count"] == 0
    assert result.discovery_status["cycle_metrics"]["accepted_results_count"] == 1
    assert result.discovery_status["cycle_metrics"]["candidate_count"] == 1
    assert result.discovery_status["cycle_metrics"]["selected_expansion_count"] == 1
    assert result.discovery_status["cycle_metrics"]["empty_expansion_count"] == 1
    assert result.discovery_status["cycle_metrics"]["listings_yielded_count"] == 0
    source_truth = {item["source_key"]: item for item in result.discovery_status["source_matrix"]}
    assert source_truth["search_web"]["ran"] is True
    assert source_truth["search_web"]["zero_yield"] is False
    assert source_truth["search_web"]["yielded_results_count"] == 1
    assert source_truth["search_web"]["fallback_order"] == ["provider_query", "provider_failover_rewrite", "scrape_parse_extraction"]
    assert source_truth["search_web"]["last_status"] == "success"
    assert source_truth["greenhouse"]["ran"] is True
    assert source_truth["greenhouse"]["zero_yield"] is True
    assert source_truth["greenhouse"]["surfaced_jobs_count"] == 0
    assert source_truth["greenhouse"]["yielded_results_count"] == 0
    assert source_truth["greenhouse"]["last_status"] == "zero_visible_yield"

    metrics_run = (
        session.query(AgentRun)
        .filter(AgentRun.agent_name == "Discovery", AgentRun.action == "recorded discovery cycle metrics")
        .order_by(AgentRun.id.desc())
        .first()
    )
    assert metrics_run is not None
    assert metrics_run.metadata_json["cycle_metrics"]["query_family_metrics"]["ats_direct"]["selected_for_expansion"] == 1
    assert metrics_run.metadata_json["cycle_metrics"]["query_family_metrics"]["ats_direct"]["zero_visible_yield_expansions"] == 1
    observer = metrics_run.metadata_json["cycle_metrics"]["source_runtime_observer"]
    assert observer["search_web"]["run_count"] == 1
    assert observer["search_web"]["yielded_results_count"] == 1
    assert observer["greenhouse"]["zero_yield_count"] == 1
    assert observer["greenhouse"]["last_status"] == "zero_visible_yield"
    company = session.query(sync_service.CompanyDiscovery).filter(sync_service.CompanyDiscovery.discovery_key == "greenhouse:acme").one()
    assert company.metadata_json["expansion_diagnostics"]["status"] == "empty"
    assert company.metadata_json["expansion_diagnostics"]["failure_boundary"] == "connector_yield"
    assert company.metadata_json["expansion_diagnostics"]["fallback_order"] == ["structured_connector_poll", "scrape_parse_fallback"]
    assert company.metadata_json["expansion_diagnostics"]["scrape_parse_attempted"] is False
    assert company.metadata_json["expansion_diagnostics"]["scrape_parse_status"] == "not_applicable"
    assert company.metadata_json["discovery_lineage"]["planner"]["query_family"] == "ats_direct"
    assert company.metadata_json["discovery_lineage"]["surface"]["source_lineage"] == "greenhouse+duckduckgo_html"
    assert company.metadata_json["discovery_lineage"]["expansion"]["status"] == "empty"
    assert company.metadata_json["discovery_lineage"]["expansion"]["visible_yield_state"] == "zero_yield"


def test_sync_all_persists_ashby_invalid_surface_diagnostics(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    monkeypatch.setattr(sync_service, "get_candidate_profile", lambda _session: session.query(CandidateProfile).first())
    monkeypatch.setattr(sync_service, "ensure_source_queries", lambda _session: [])
    monkeypatch.setattr(sync_service, "generate_follow_up_tasks", lambda _session: 0)
    monkeypatch.setattr(
        sync_service,
        "planner_agent",
        lambda *_args, **_kwargs: {
            "queries": ['site:jobs.ashbyhq.com "deployment strategist"'],
            "query_themes": ["ats_direct"],
            "company_archetypes": [],
            "priority_notes": [],
        },
    )
    monkeypatch.setattr(sync_service, "extractor_agent", lambda *_args, **_kwargs: ([], []))
    monkeypatch.setattr(sync_service, "learning_agent", lambda *_args, **_kwargs: {"next_queries": [], "focus_companies": [], "notes": []})
    monkeypatch.setattr(sync_service, "triage_agent", lambda **_kwargs: (3.2, ["deterministic test"], "pursue"))

    def fake_run_connector_fetch(_session, connector_name, fetch_fn, date_fields=None):
        if connector_name == "search_web":
            return [
                SearchDiscoveryResult(
                    query_text='site:jobs.ashbyhq.com "deployment strategist"',
                    title="Deployment Strategist - Acme",
                    url="https://jobs.ashbyhq.com/acme/123",
                    query_family="ats_direct",
                )
            ], True, None
        if connector_name == "ashby":
            connector = fetch_fn.func.__self__
            connector.last_org_statuses = {"acme": "invalid_identifier"}
            connector.last_per_org_counts = {"acme": 0}
            connector.last_empty_orgs = ["acme"]
            return [], True, None
        return [], False, None

    monkeypatch.setattr(sync_service, "run_connector_fetch", fake_run_connector_fetch)
    monkeypatch.setattr(sync_service, "get_settings", lambda: Settings(search_discovery_enabled=True, ashby_enabled=True))

    result = sync_service.sync_all(session, enabled_connectors={"search_web", "ashby"})

    company = session.query(sync_service.CompanyDiscovery).filter(sync_service.CompanyDiscovery.discovery_key == "ashby:acme").one()
    assert result.discovery_status["cycle_metrics"]["accepted_results_count"] == 1
    assert result.discovery_status["cycle_metrics"]["candidate_count"] == 1
    assert result.discovery_status["cycle_metrics"]["selected_expansion_count"] == 1
    assert result.discovery_status["cycle_metrics"]["empty_expansion_count"] == 1
    assert company.metadata_json["expansion_diagnostics"]["surface_status"] == "invalid_identifier"
    assert company.metadata_json["expansion_diagnostics"]["failure_boundary"] == "invalid_discovered_surface"
    assert company.metadata_json["expansion_diagnostics"]["fallback_order"] == ["structured_connector_poll", "scrape_parse_fallback"]


def test_sync_all_records_scrape_parse_fallback_for_careers_page_zero_yield(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    monkeypatch.setattr(sync_service, "get_candidate_profile", lambda _session: session.query(CandidateProfile).first())
    monkeypatch.setattr(sync_service, "ensure_source_queries", lambda _session: [])
    monkeypatch.setattr(sync_service, "generate_follow_up_tasks", lambda _session: 0)
    monkeypatch.setattr(
        sync_service,
        "planner_agent",
        lambda *_args, **_kwargs: {
            "queries": ['"Acme" "operations lead" careers'],
            "query_themes": ["company_targeted"],
            "company_archetypes": [],
            "priority_notes": [],
        },
    )
    monkeypatch.setattr(sync_service, "learning_agent", lambda *_args, **_kwargs: {"next_queries": [], "focus_companies": [], "notes": []})
    monkeypatch.setattr(sync_service, "triage_agent", lambda **_kwargs: (3.4, ["deterministic test"], "pursue"))
    monkeypatch.setattr(
        sync_service,
        "extractor_agent",
        lambda results, **_kwargs: (
            [
                ATSExtractionResult(
                    source_url=results[0].url,
                    final_url=results[0].url,
                    page_title="Acme Careers",
                    company_name="Acme",
                    careers_url=results[0].url,
                    ats_type="careers_page",
                    greenhouse_tokens=[],
                    ashby_identifiers=[],
                    discovered_urls=[],
                    geography_hints=[],
                    confidence=0.41,
                    via_openai=False,
                )
            ],
            [],
        ),
    )

    def fake_run_connector_fetch(_session, connector_name, fetch_fn, date_fields=None):
        if connector_name == "search_web":
            return [
                SearchDiscoveryResult(
                    query_text='"Acme" "operations lead" careers',
                    title="Acme Careers",
                    url="https://acme.ai/careers",
                    query_family="company_targeted",
                )
            ], True, None
        return [], False, None

    monkeypatch.setattr(sync_service, "run_connector_fetch", fake_run_connector_fetch)
    monkeypatch.setattr(sync_service, "get_settings", lambda: Settings(search_discovery_enabled=True))

    result = sync_service.sync_all(session, enabled_connectors={"search_web"})

    company = session.query(sync_service.CompanyDiscovery).filter(sync_service.CompanyDiscovery.discovery_key == "careers_page:acme.ai").one()
    assert result.discovery_status["cycle_metrics"]["scrape_parse_extraction_count"] == 1
    assert company.metadata_json["expansion_diagnostics"]["fallback_order"] == ["structured_connector_poll", "scrape_parse_fallback"]
    assert company.metadata_json["expansion_diagnostics"]["scrape_parse_attempted"] is True
    assert company.metadata_json["expansion_diagnostics"]["scrape_parse_status"] == "no_ats_identifiers_extracted"
    assert company.metadata_json["expansion_diagnostics"]["surface_status"] == "no_usable_jobs_found"
    assert company.metadata_json["expansion_diagnostics"]["failure_boundary"] == "scrape_parse_yield"


def test_sync_all_persists_productive_query_family_lineage(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    now = datetime.utcnow()

    monkeypatch.setattr(sync_service, "get_candidate_profile", lambda _session: session.query(CandidateProfile).first())
    monkeypatch.setattr(sync_service, "ensure_source_queries", lambda _session: [])
    monkeypatch.setattr(sync_service, "generate_follow_up_tasks", lambda _session: 0)
    monkeypatch.setattr(
        sync_service,
        "planner_agent",
        lambda *_args, **_kwargs: {
            "queries": ['"Acme" "operations lead" careers'],
            "query_themes": ["company_targeted"],
            "company_archetypes": [],
            "priority_notes": [],
        },
    )
    monkeypatch.setattr(sync_service, "extractor_agent", lambda *_args, **_kwargs: ([], []))
    monkeypatch.setattr(sync_service, "learning_agent", lambda *_args, **_kwargs: {"next_queries": [], "focus_companies": [], "notes": []})
    monkeypatch.setattr(sync_service, "triage_agent", lambda **_kwargs: (3.5, ["deterministic test"], "pursue"))
    monkeypatch.setattr(
        sync_service,
        "judge_fit_with_ai",
        lambda **_kwargs: {"classification": "strong_fit", "reasons": [], "matched_profile_fields": []},
    )

    def fake_run_connector_fetch(_session, connector_name, fetch_fn, date_fields=None):
        if connector_name == "search_web":
            return [
                SearchDiscoveryResult(
                    query_text='"Acme" "operations lead" careers',
                    title="Operations Lead - Acme",
                    url="https://job-boards.greenhouse.io/acme/jobs/1",
                    query_family="company_targeted",
                )
            ], True, None
        if connector_name == "greenhouse":
            connector = fetch_fn.func.__self__
            connector.last_board_counts = {"acme": 1}
            return [
                {
                    "id": "gh-acme-1",
                    "title": "Operations Lead",
                    "absolute_url": "https://job-boards.greenhouse.io/acme/jobs/1",
                    "updated_at": now.isoformat(),
                    "first_published": now.isoformat(),
                    "location": {"name": "Remote"},
                    "content": "Own operating cadence and planning.",
                    "company_name": "Acme",
                    "source_board_token": "acme",
                    "discovery_source": "search_web",
                }
            ], True, None
        return [], False, None

    monkeypatch.setattr(sync_service, "run_connector_fetch", fake_run_connector_fetch)
    monkeypatch.setattr(
        sync_service,
        "get_settings",
        lambda: Settings(search_discovery_enabled=True, greenhouse_enabled=True, ai_fit_max_calls_per_cycle=1, enable_ai_readtime_critic=False),
    )

    result = sync_service.sync_all(session, enabled_connectors={"search_web", "greenhouse"})

    query_family_metrics = result.discovery_status["cycle_metrics"]["query_family_metrics"]
    assert query_family_metrics["company_targeted"]["selected_for_expansion"] == 1
    assert query_family_metrics["company_targeted"]["listings_yielded"] == 1
    assert query_family_metrics["company_targeted"]["visible_yield_count"] == 1
    assert query_family_metrics["company_targeted"]["expansions_with_visible_yield"] == 1
    source_truth = {item["source_key"]: item for item in result.discovery_status["source_matrix"]}
    assert source_truth["search_web"]["ran"] is True
    assert source_truth["search_web"]["surfaced_jobs_count"] == 1
    assert source_truth["search_web"]["yielded_results_count"] == 1
    assert source_truth["greenhouse"]["ran"] is True
    assert source_truth["greenhouse"]["zero_yield"] is False
    assert source_truth["greenhouse"]["surfaced_jobs_count"] == 1
    assert source_truth["greenhouse"]["yielded_results_count"] == 1
    assert source_truth["greenhouse"]["last_status"] == "productive"

    company = session.query(sync_service.CompanyDiscovery).filter(sync_service.CompanyDiscovery.discovery_key == "greenhouse:acme").one()
    assert company.last_expansion_result_count == 1
    assert company.visible_yield_count == 1
    assert company.metadata_json["discovery_lineage"]["planner"]["query_family"] == "company_targeted"
    assert company.metadata_json["discovery_lineage"]["surface"]["source_lineage"] == "greenhouse+duckduckgo_html"
    assert company.metadata_json["discovery_lineage"]["expansion"]["result_count"] == 1
    assert company.metadata_json["discovery_lineage"]["expansion"]["visible_yield_count"] == 1
    assert company.metadata_json["discovery_lineage"]["expansion"]["visible_yield_state"] == "productive"


def test_default_leads_query_returns_timestamp_precision_and_recently_seen_rows_only() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    now = datetime.utcnow()
    listing = Listing(
        company_name="CurrentCo",
        title="Deployment Strategist",
        location="Remote",
        url="https://jobs.example.com/current",
        source_type="greenhouse",
        posted_at=now - timedelta(hours=6, minutes=15),
        first_published_at=now - timedelta(hours=6, minutes=15),
        discovered_at=now - timedelta(hours=5, minutes=30),
        last_seen_at=now - timedelta(minutes=10),
        description_text="Run customer deployments for an AI startup.",
        listing_status="active",
        freshness_hours=6.25,
        freshness_days=0,
        metadata_json={},
    )
    stale_listing = Listing(
        company_name="MissingCo",
        title="Operations Lead",
        location="Remote",
        url="https://jobs.example.com/missing",
        source_type="greenhouse",
        posted_at=now - timedelta(hours=5),
        first_published_at=now - timedelta(hours=5),
        discovered_at=now - timedelta(hours=5),
        last_seen_at=now - timedelta(hours=72),
        description_text="This row should be suppressed because it has not been seen recently.",
        listing_status="active",
        freshness_hours=5.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add_all([listing, stale_listing])
    session.flush()
    session.add_all(
        [
            Lead(
                lead_type="listing",
                company_name="CurrentCo",
                primary_title="Deployment Strategist",
                listing_id=listing.id,
                surfaced_at=now,
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Visible",
                score_breakdown_json={"composite": 8.0},
                evidence_json={"url": listing.url, "source_type": "greenhouse"},
                hidden=False,
            ),
            Lead(
                lead_type="listing",
                company_name="MissingCo",
                primary_title="Operations Lead",
                listing_id=stale_listing.id,
                surfaced_at=now,
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Should not surface",
                score_breakdown_json={"composite": 8.0},
                evidence_json={"url": stale_listing.url, "source_type": "greenhouse"},
                hidden=False,
            ),
        ]
    )
    session.commit()

    items = list_leads(session)
    assert len(items) == 1
    item = items[0]
    assert item.company_name == "CurrentCo"
    assert item.freshness_hours is not None
    assert item.freshness_hours > 6
    assert item.posted_at is not None and item.posted_at.microsecond != 0
    assert item.posted_at.tzinfo is not None
    assert item.first_published_at is not None and item.first_published_at.microsecond != 0
    assert item.first_published_at.tzinfo is not None
    assert item.last_seen_at is not None
    assert item.last_seen_at.tzinfo is not None
    assert item.evidence_json["first_published_at"].endswith("Z")
    assert item.evidence_json["last_seen_at"].endswith("Z")


def test_default_leads_query_suppresses_out_of_region_listing() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="LondonCo",
        title="Operations Lead",
        location="London, UK",
        url="https://job-boards.greenhouse.io/londonco/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Run strategic operations for an AI startup.",
        listing_status="active",
        freshness_hours=3.0,
        freshness_days=0,
        metadata_json={"location_scope": "uk", "location_reason": "matched region hint uk"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="LondonCo",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Should be filtered by location policy",
            score_breakdown_json={"composite": 8.0},
            evidence_json={"url": listing.url, "source_type": "greenhouse"},
            hidden=False,
        )
    )
    session.commit()

    assert list_leads(session) == []
