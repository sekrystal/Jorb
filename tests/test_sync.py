from __future__ import annotations

from datetime import datetime, timedelta
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config import Settings
from core.models import AgentRun, Base, CandidateProfile, Lead, Listing
from services.pipeline import run_critic_agent
from services import sync as sync_service
from services.sync import list_leads


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
    assert "'suppressed_duplicate_count': 1" in deduped_message


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
    assert item.posted_at is not None and item.posted_at.second != 0
    assert item.posted_at.tzinfo is not None
    assert item.first_published_at is not None and item.first_published_at.second != 0
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
