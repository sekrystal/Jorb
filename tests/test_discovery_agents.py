from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from connectors.search_web import SearchDiscoveryResult
from core.models import AgentRun, Base, CandidateProfile, CompanyDiscovery, Lead
from services.discovery_agents import planner_agent, triage_agent
from services.company_discovery import candidate_from_search_result


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def _profile(session):
    profile = CandidateProfile(
        name="Tester",
        raw_resume_text="Senior operator focused on chief of staff, bizops, and deployment roles in AI startups.",
        preferred_titles_json=["chief of staff", "deployment strategist"],
        core_titles_json=["chief of staff", "business operations lead"],
        adjacent_titles_json=["deployment strategist", "implementation lead"],
        preferred_domains_json=["ai", "infra"],
        preferred_locations_json=["remote", "san francisco", "new york"],
        min_seniority_band="mid",
        max_seniority_band="staff",
        stretch_role_families_json=["operations", "go_to_market"],
        extracted_summary_json={"summary": "Operator for AI startups.", "learning": {"title_weights": {"deployment strategist": 0.8}}},
    )
    session.add(profile)
    session.commit()
    return profile


def test_planner_agent_generates_queries_without_openai() -> None:
    session = _session()
    profile = _profile(session)
    session.add(Lead(lead_type="listing", company_name="SavedCo", primary_title="Chief of Staff"))
    session.commit()

    plan = planner_agent(session, profile)
    assert plan["queries"]
    assert isinstance(plan["queries"], list)
    assert "queries" in plan
    assert any("startup careers" in query for query in plan["queries"])


def test_triage_agent_handles_ashby_candidate_without_ai() -> None:
    session = _session()
    profile = _profile(session)
    result = SearchDiscoveryResult(
        query_text='"deployment strategist" startup careers ashby',
        title="Deployment Strategist - Example",
        url="https://jobs.ashbyhq.com/example/123",
    )
    candidate = candidate_from_search_result(result)
    assert candidate is not None

    score, reasons, decision = triage_agent(session, profile, candidate, configured_boards=set())
    assert score > 0
    assert reasons
    assert decision in {"pursue", "investigate", "defer", "drop"}


def test_triage_agent_respects_company_penalty_learning() -> None:
    session = _session()
    profile = _profile(session)
    profile.extracted_summary_json = {
        "summary": "Operator for AI startups.",
        "learning": {"company_penalties": {"example": 2.0}},
    }
    session.add(
        CompanyDiscovery(
            discovery_key="ashby:example",
            company_name="Example",
            company_domain=None,
            normalized_company_key="example",
            discovery_source="duckduckgo_html",
            discovery_query="example careers",
            board_type="ashby",
            board_locator="example",
        )
    )
    session.commit()

    result = SearchDiscoveryResult(
        query_text='"business operations" startup careers ashby',
        title="Business Operations Lead - Example",
        url="https://jobs.ashbyhq.com/example/123",
    )
    candidate = candidate_from_search_result(result)
    assert candidate is not None
    score, reasons, _ = triage_agent(session, profile, candidate, configured_boards=set())
    assert any("company penalty" in reason for reason in reasons)
    assert score < 5
