from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, CandidateProfile, Lead, Listing
from services.pipeline import run_critic_agent
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
            description_text="Own operating cadence and planning.",
            listing_status="active",
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

    items_with_signals = list_leads(session, include_signal_only=True)
    assert {item.lead_type for item in items_with_signals} == {"listing", "signal"}


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
        posted_at=datetime.utcnow(),
        description_text="This position has been filled.",
        listing_status="expired",
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
        posted_at=datetime.utcnow(),
        description_text="Posting closed archived position.",
        listing_status="suspected_expired",
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
        description_text="Role text",
        listing_status="active",
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
        posted_at=datetime.utcnow(),
        description_text="Page not found.",
        listing_status="expired",
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
