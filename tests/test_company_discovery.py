from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from connectors.search_web import SearchDiscoveryResult
from core.config import Settings
from core.models import AgentRun, Base, CandidateProfile
from services.company_discovery import (
    build_discovery_source_matrix,
    build_discovery_status,
    candidate_from_search_result,
    classify_surface_provenance,
    inspect_search_result_candidate,
    record_expansion_attempt,
    select_candidates_for_expansion,
    source_lineage_for_surface,
    triage_candidate,
    upsert_discovered_company,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def _profile(session):
    profile = CandidateProfile(
        name="Tester",
        core_titles_json=["chief of staff", "business operations lead"],
        adjacent_titles_json=["deployment strategist"],
        preferred_domains_json=["ai", "infra"],
        preferred_locations_json=["remote", "san francisco", "new york"],
        min_seniority_band="mid",
        max_seniority_band="staff",
        stretch_role_families_json=["operations", "go_to_market"],
    )
    session.add(profile)
    session.commit()
    return profile


def test_company_discovery_dedupes_by_board_locator() -> None:
    session = _session()
    profile = _profile(session)
    result = SearchDiscoveryResult(
        query_text='site:job-boards.greenhouse.io "chief of staff"',
        title="Chief of Staff - Example",
        url="https://job-boards.greenhouse.io/example/jobs/1",
    )
    candidate = candidate_from_search_result(result)
    assert candidate is not None
    score, reasons, _ = triage_candidate(session, candidate, profile, configured_boards=set())
    row1, created1 = upsert_discovered_company(session, candidate, score, reasons)
    row2, created2 = upsert_discovered_company(session, candidate, score, reasons)

    assert created1 is True
    assert created2 is False
    assert row1.id == row2.id


def test_candidate_conversion_handles_hosted_board_roots_and_redirects() -> None:
    greenhouse_root = SearchDiscoveryResult(
        query_text="acme greenhouse",
        title="Acme Greenhouse",
        url="https://job-boards.greenhouse.io/acme",
    )
    ashby_root = SearchDiscoveryResult(
        query_text="acme ashby",
        title="Acme Ashby",
        url="https://jobs.ashbyhq.com/acme",
    )
    ddg_redirect = SearchDiscoveryResult(
        query_text="acme greenhouse",
        title="redirect",
        url="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fboards.greenhouse.io%2Facme%2Fjobs%2F1",
    )

    greenhouse_candidate = candidate_from_search_result(greenhouse_root)
    ashby_candidate = candidate_from_search_result(ashby_root)
    redirect_candidate = candidate_from_search_result(ddg_redirect)
    inspection = inspect_search_result_candidate(ddg_redirect)

    assert greenhouse_candidate is not None
    assert greenhouse_candidate.board_type == "greenhouse"
    assert greenhouse_candidate.board_locator == "acme"
    assert ashby_candidate is not None
    assert ashby_candidate.board_type == "ashby"
    assert ashby_candidate.board_locator == "acme"
    assert redirect_candidate is not None
    assert redirect_candidate.result_url == "https://boards.greenhouse.io/acme/jobs/1"
    assert inspection["normalized_url"] == "https://boards.greenhouse.io/acme/jobs/1"


def test_surface_provenance_classifies_preseeded_and_discovered() -> None:
    settings = Settings(greenhouse_board_tokens="ramp")
    assert classify_surface_provenance("greenhouse", "ramp", is_new=False, settings=settings) == "preseeded"
    assert classify_surface_provenance("ashby", "acme", is_new=True) == "discovered_new"
    assert source_lineage_for_surface("greenhouse", "preseeded", "search_web") == "greenhouse"
    assert source_lineage_for_surface("greenhouse", "discovered_new", "search_web") == "greenhouse+search_web"


def test_company_discovery_budget_prefers_useful_and_limits_new_expansions() -> None:
    session = _session()
    profile = _profile(session)
    rows = []
    for locator, utility in [("alpha", 2.0), ("beta", 1.0), ("gamma", 0.2)]:
        result = SearchDiscoveryResult(
            query_text='"business operations" startup careers greenhouse',
            title=f"Business Operations Lead - {locator.title()}",
            url=f"https://job-boards.greenhouse.io/{locator}/jobs/1",
        )
        candidate = candidate_from_search_result(result)
        assert candidate is not None
        score, reasons, _ = triage_candidate(session, candidate, profile, configured_boards=set())
        row, _ = upsert_discovered_company(session, candidate, score, reasons)
        row.utility_score = utility
        rows.append((candidate, row, score, reasons))
    session.commit()

    selected = select_candidates_for_expansion(rows)
    assert len(selected) <= 4
    assert selected[0][1].utility_score >= selected[-1][1].utility_score


def test_record_expansion_attempt_updates_yield_and_status() -> None:
    session = _session()
    profile = _profile(session)
    result = SearchDiscoveryResult(
        query_text='"deployment strategist" startup careers ashby',
        title="Deployment Strategist - Example",
        url="https://jobs.ashbyhq.com/example/123",
    )
    candidate = candidate_from_search_result(result)
    assert candidate is not None
    score, reasons, _ = triage_candidate(session, candidate, profile, configured_boards=set())
    row, _ = upsert_discovered_company(session, candidate, score, reasons)
    record_expansion_attempt(row, result_count=5, visible_yield=2, suppressed_yield=1, location_filtered=1)
    session.commit()

    assert row.expansion_attempts == 1
    assert row.last_expansion_result_count == 5
    assert row.visible_yield_count == 2
    assert row.location_filtered_count == 1
    assert row.expansion_status == "expanded"
    assert row.metadata_json["discovery_lineage"]["planner"]["query_text"] == candidate.discovery_query
    assert row.metadata_json["discovery_lineage"]["expansion"]["status"] == "expanded"
    assert row.metadata_json["discovery_lineage"]["expansion"]["visible_yield_count"] == 2
    assert row.metadata_json["discovery_lineage"]["expansion"]["visible_yield_state"] == "productive"


def test_discovery_status_returns_recent_items() -> None:
    session = _session()
    profile = _profile(session)
    result = SearchDiscoveryResult(
        query_text='"chief of staff" startup careers greenhouse',
        title="Chief of Staff - Example",
        url="https://job-boards.greenhouse.io/example/jobs/1",
    )
    candidate = candidate_from_search_result(result)
    assert candidate is not None
    score, reasons, _ = triage_candidate(session, candidate, profile, configured_boards=set())
    row, _ = upsert_discovered_company(session, candidate, score, reasons)
    row.last_discovered_at = datetime.utcnow()
    row.visible_yield_count = 2

    result_two = SearchDiscoveryResult(
        query_text='"business operations" startup careers greenhouse',
        title="Business Operations Lead - Blocked",
        url="https://job-boards.greenhouse.io/blocked/jobs/2",
    )
    candidate_two = candidate_from_search_result(result_two)
    assert candidate_two is not None
    score_two, reasons_two, _ = triage_candidate(session, candidate_two, profile, configured_boards=set())
    blocked_row, _ = upsert_discovered_company(session, candidate_two, score_two, reasons_two)
    blocked_row.expansion_status = "empty"
    blocked_row.blocked_reason = "cooldown"

    result_three = SearchDiscoveryResult(
        query_text='"deployment strategist" startup careers ashby',
        title="Deployment Strategist - Ashby",
        url="https://jobs.ashbyhq.com/acme/3",
    )
    candidate_three = candidate_from_search_result(result_three)
    assert candidate_three is not None
    score_three, reasons_three, _ = triage_candidate(session, candidate_three, profile, configured_boards=set())
    ashby_row, _ = upsert_discovered_company(session, candidate_three, score_three, reasons_three)

    session.add(
        AgentRun(
            agent_name="Planner",
            action="planned discovery cycle",
            summary="Planner prepared discovery queries.",
            affected_count=3,
            metadata_json={"queries": ["chief of staff startup careers greenhouse"], "used_openai": True},
        )
    )
    session.add(
        AgentRun(
            agent_name="Triage",
            action="prioritized discovery candidates",
            summary="Triage selected candidates.",
            affected_count=2,
            metadata_json={"used_openai": False},
        )
    )
    session.add(
        AgentRun(
            agent_name="Learning",
            action="updated discovery priors",
            summary="Learning proposed next queries.",
            affected_count=2,
            metadata_json={"next_queries": ["deployment strategist careers ashby"], "used_openai": True},
        )
    )
    session.add(
        AgentRun(
            agent_name="Discovery",
            action="recorded discovery cycle metrics",
            summary="Discovery cycle metrics recorded.",
            affected_count=1,
            metadata_json={
                "cycle_metrics": {
                    "discovered_companies_new_count": 2,
                    "agent_discovered_visible_leads_count": 1,
                    "accepted_urls_sample": ["https://job-boards.greenhouse.io/example"],
                    "dropped_urls_sample": ["https://linkedin.com/jobs/1"],
                    "query_family_metrics": {
                        "company_targeted": {
                            "queries_attempted": 1,
                            "selected_for_expansion": 1,
                        }
                    },
                }
            },
        )
    )
    row.metadata_json = {**(row.metadata_json or {}), "surface_provenance": "preseeded", "source_lineage": "greenhouse"}
    blocked_row.metadata_json = {**(blocked_row.metadata_json or {}), "surface_provenance": "discovered_new", "source_lineage": "greenhouse+search_web"}
    ashby_row.metadata_json = {
        **(ashby_row.metadata_json or {}),
        "surface_provenance": "discovered_new",
        "source_lineage": "ashby+search_web",
        "ashby_identifiers": [ashby_row.board_locator],
    }
    row.last_expansion_result_count = 4
    session.commit()

    status = build_discovery_status(session)
    assert status.total_known_companies == 3
    assert status.source_matrix
    assert {item.company_name for item in status.recent_items} == {row.company_name, blocked_row.company_name, ashby_row.company_name}
    assert status.latest_planner_run is not None
    assert status.latest_planner_run["agent_name"] == "Planner"
    assert {item.company_name for item in status.recent_visible_yield} == {row.company_name}
    assert {item.company_name for item in status.blocked_or_cooled_down} == {blocked_row.company_name}
    assert status.next_recommended_queries == ["deployment strategist careers ashby"]
    assert status.recent_greenhouse_tokens
    assert any(item["identifier"] == ashby_row.board_locator for item in status.recent_ashby_identifiers)
    assert status.latest_openai_usage == {"planner": True, "triage": False, "learning": True}
    assert status.cycle_metrics["discovered_companies_new_count"] == 2
    assert status.cycle_metrics["accepted_urls_sample"] == ["https://job-boards.greenhouse.io/example"]
    assert status.cycle_metrics["query_family_metrics"]["company_targeted"]["queries_attempted"] == 1
    assert status.cycle_metrics["query_family_metrics"]["company_targeted"]["selected_for_expansion"] == 1
    assert status.recent_successful_expansions
    recent_example = next(item for item in status.recent_items if item.company_name == row.company_name)
    assert recent_example.metadata_json["discovery_lineage"]["surface"]["source_lineage"] == "greenhouse"
    assert recent_example.metadata_json["discovery_lineage"]["planner"]["query_family"] == "unknown"


def test_discovery_source_matrix_classifies_live_truth_explicitly() -> None:
    session = _session()
    settings = Settings(
        demo_mode=False,
        greenhouse_enabled=True,
        greenhouse_board_tokens="cursor",
        ashby_org_keys="mercor",
        search_discovery_enabled=True,
        x_bearer_token=None,
    )

    matrix = build_discovery_source_matrix(
        session,
        settings=settings,
        enabled_connectors={"greenhouse", "ashby", "search_web"},
        strict_live_connectors={"greenhouse", "ashby", "search_web"},
    )
    by_key = {item.source_key: item for item in matrix}

    assert by_key["greenhouse"].classification == "working"
    assert by_key["greenhouse"].trusted_for_output is True
    assert by_key["ashby"].classification == "working"
    assert by_key["search_web"].classification == "partially_working"
    assert by_key["search_web"].trusted_for_output is False
    assert by_key["search_web_scrape_fallback"].classification == "partially_working"
    assert by_key["x_search"].classification == "not_working"
    assert by_key["user_submitted"].classification == "working"


def test_discovery_source_matrix_marks_disabled_sources_explicitly() -> None:
    session = _session()
    settings = Settings(
        demo_mode=False,
        greenhouse_enabled=False,
        greenhouse_board_tokens="",
        ashby_org_keys="",
        search_discovery_enabled=False,
        x_bearer_token=None,
    )

    matrix = build_discovery_source_matrix(
        session,
        settings=settings,
        enabled_connectors=set(),
        strict_live_connectors=set(),
    )
    by_key = {item.source_key: item for item in matrix}

    assert by_key["greenhouse"].classification == "not_working"
    assert by_key["ashby"].classification == "not_working"
    assert by_key["search_web"].classification == "not_working"
    assert by_key["search_web_scrape_fallback"].classification == "not_working"
    assert by_key["x_search"].classification == "not_working"
    assert by_key["user_submitted"].classification == "working"


def test_discovery_status_uses_latest_relevant_runs_beyond_recent_window() -> None:
    session = _session()
    profile = _profile(session)
    result = SearchDiscoveryResult(
        query_text='"chief of staff" startup careers greenhouse',
        title="Chief of Staff - Example",
        url="https://job-boards.greenhouse.io/example/jobs/1",
    )
    candidate = candidate_from_search_result(result)
    assert candidate is not None
    score, reasons, _ = triage_candidate(session, candidate, profile, configured_boards=set())
    row, _ = upsert_discovered_company(session, candidate, score, reasons)
    record_expansion_attempt(row, result_count=0, visible_yield=0, suppressed_yield=0, location_filtered=0)

    metrics_run = AgentRun(
        agent_name="Discovery",
        action="recorded discovery cycle metrics",
        summary="Zero-yield discovery cycle recorded.",
        affected_count=0,
        metadata_json={
            "cycle_metrics": {
                "discovered_companies_new_count": 0,
                "agent_discovered_visible_leads_count": 0,
                "accepted_results_count": 0,
            }
        },
    )
    metrics_run.created_at = datetime.utcnow() - timedelta(minutes=9)
    session.add(metrics_run)

    planner_run = AgentRun(
        agent_name="Planner",
        action="planned discovery cycle",
        summary="Latest planner run.",
        affected_count=1,
        metadata_json={"queries": ["ops careers"], "used_openai": True},
    )
    planner_run.created_at = datetime.utcnow() - timedelta(minutes=8)
    session.add(planner_run)

    filler_agents = ["Learning", "Triage", "Planner", "Learning", "Triage", "Planner", "Learning", "Triage"]
    for index, agent_name in enumerate(filler_agents):
        filler = AgentRun(
            agent_name=agent_name,
            action=f"filler run {index}",
            summary=f"{agent_name} filler run {index}",
            affected_count=index,
            metadata_json={"used_openai": index % 2 == 0},
        )
        filler.created_at = datetime.utcnow() - timedelta(minutes=7 - index)
        session.add(filler)

    latest_learning = AgentRun(
        agent_name="Learning",
        action="updated discovery priors",
        summary="Latest learning run.",
        affected_count=1,
        metadata_json={"next_queries": ["remote ops careers"], "used_openai": False},
    )
    latest_learning.created_at = datetime.utcnow() + timedelta(minutes=1)
    session.add(latest_learning)

    latest_triage = AgentRun(
        agent_name="Triage",
        action="prioritized discovery candidates",
        summary="Latest triage run.",
        affected_count=1,
        metadata_json={"used_openai": True},
    )
    latest_triage.created_at = datetime.utcnow() + timedelta(minutes=2)
    session.add(latest_triage)
    session.commit()

    status = build_discovery_status(session)

    assert status.latest_planner_run is not None
    assert status.latest_planner_run["summary"] == "Latest planner run."
    assert status.latest_openai_usage == {"planner": True, "triage": True, "learning": False}
    assert status.cycle_metrics["agent_discovered_visible_leads_count"] == 0
    assert status.cycle_metrics["accepted_results_count"] == 0


def test_candidate_from_search_result_preserves_query_family() -> None:
    candidate = candidate_from_search_result(
        SearchDiscoveryResult(
            query_text='site:job-boards.greenhouse.io "chief of staff"',
            title="Chief of Staff - Example",
            url="https://job-boards.greenhouse.io/example/jobs/1",
            query_family="ats_direct",
        )
    )

    assert candidate is not None
    assert candidate.query_family == "ats_direct"


def test_discovery_status_includes_latest_empty_expansion_for_older_company() -> None:
    session = _session()
    profile = _profile(session)

    old_result = SearchDiscoveryResult(
        query_text='"chief of staff" startup careers greenhouse',
        title="Chief of Staff - Older",
        url="https://job-boards.greenhouse.io/older/jobs/1",
    )
    old_candidate = candidate_from_search_result(old_result)
    assert old_candidate is not None
    old_score, old_reasons, _ = triage_candidate(session, old_candidate, profile, configured_boards=set())
    old_row, _ = upsert_discovered_company(session, old_candidate, old_score, old_reasons)
    old_row.last_discovered_at = datetime.utcnow() - timedelta(days=40)
    record_expansion_attempt(old_row, result_count=0, blocked_reason="cooldown")

    for index in range(30):
        result = SearchDiscoveryResult(
            query_text=f'"business operations" startup careers greenhouse {index}',
            title=f"Business Operations Lead - Fresh {index}",
            url=f"https://job-boards.greenhouse.io/fresh-{index}/jobs/{index}",
        )
        candidate = candidate_from_search_result(result)
        assert candidate is not None
        score, reasons, _ = triage_candidate(session, candidate, profile, configured_boards=set())
        row, _ = upsert_discovered_company(session, candidate, score, reasons)
        row.last_discovered_at = datetime.utcnow() - timedelta(minutes=index)

    session.commit()

    status = build_discovery_status(session)

    assert old_row.company_name not in {item.company_name for item in status.recent_items}
    assert old_row.company_name in {item.company_name for item in status.blocked_or_cooled_down}
