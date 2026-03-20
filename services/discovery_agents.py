from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from connectors.search_web import (
    ATSExtractionResult,
    SearchDiscoveryResult,
    build_search_queries,
    derive_search_results_from_extraction,
    extract_ats_identifiers_from_html,
    fetch_page_snapshot,
)
from core.config import Settings, get_settings
from core.models import AgentRun, Application, CandidateProfile, CompanyDiscovery, Lead
from core.logging import get_logger
from services.ai_judges import (
    critique_discovery_cycle_with_ai,
    interpret_discovery_page_with_ai,
    judge_discovery_candidate_with_ai,
    plan_search_with_ai,
)
from services.company_discovery import CompanyDiscoveryCandidate, build_query_inputs, triage_candidate


logger = get_logger(__name__)

ROLE_SYNONYMS = {
    "operations": ["bizops", "business operations", "strategic operations", "program operations"],
    "go_to_market": ["deployment", "implementation", "solutions", "customer success"],
}


def _recent_successes(session: Session) -> dict[str, list[str]]:
    saved_or_applied = session.execute(
        select(Lead.company_name, Lead.primary_title)
        .join(Application, Application.lead_id == Lead.id)
        .where(Application.current_status.in_(["saved", "applied"]))
        .order_by(Application.updated_at.desc())
        .limit(10)
    ).all()
    successful_companies = []
    successful_titles = []
    for company_name, title in saved_or_applied:
        if company_name not in successful_companies:
            successful_companies.append(company_name)
        if title not in successful_titles:
            successful_titles.append(title)
    return {
        "successful_companies": successful_companies[:6],
        "successful_titles": successful_titles[:6],
    }


def planner_agent(session: Session, profile: CandidateProfile, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    query_inputs = build_query_inputs(session, profile)
    learning = (profile.extracted_summary_json or {}).get("learning", {})
    successes = _recent_successes(session)
    recent_discoveries = session.scalars(
        select(CompanyDiscovery)
        .order_by(CompanyDiscovery.updated_at.desc())
        .limit(8)
    ).all()
    recent_failures = [
        {
            "company_name": row.company_name,
            "board_type": row.board_type,
            "status": row.expansion_status,
            "last_result_count": row.last_expansion_result_count,
            "blocked_reason": row.blocked_reason,
        }
        for row in recent_discoveries
        if row.expansion_status in {"empty", "blocked"}
    ][:5]
    deterministic_queries = build_search_queries(
        core_titles=profile.core_titles_json or profile.preferred_titles_json or [],
        adjacent_titles=profile.adjacent_titles_json or [],
        preferred_domains=profile.preferred_domains_json or [],
        watchlist_items=[row.company_name for row in recent_discoveries[:4]],
        role_families=query_inputs["role_families"],
        boosted_titles=query_inputs["boosted_titles"],
        recent_titles=query_inputs["recent_titles"] + successes["successful_titles"],
    )
    for family in query_inputs["role_families"][:3]:
        for synonym in ROLE_SYNONYMS.get(family, []):
            deterministic_queries.append(f'"{synonym}" startup careers')
            deterministic_queries.append(f'"{synonym}" startup jobs')
            deterministic_queries.append(f'"{synonym}" remote us careers')
            deterministic_queries.append(f'"{synonym}" startup greenhouse')
            deterministic_queries.append(f'"{synonym}" startup ashby')
    deterministic_queries = list(dict.fromkeys(deterministic_queries))

    ai_plan = plan_search_with_ai(
        profile_text=profile.raw_resume_text or (profile.extracted_summary_json or {}).get("summary", ""),
        learning_summary={
            "boosted_titles": query_inputs["boosted_titles"],
            "role_families": query_inputs["role_families"],
            "recent_titles": query_inputs["recent_titles"],
            "successful_companies": successes["successful_companies"],
        },
        recent_outcomes={"recent_failures": recent_failures},
    )
    ai_queries = (ai_plan or {}).get("candidate_queries", [])
    logger.info(
        "[OPENAI_PLANNER] %s",
        {
            "used_openai": bool(ai_plan),
            "query_theme_count": len((ai_plan or {}).get("query_themes", [])),
            "candidate_query_count": len(ai_queries),
        },
    )
    queries = list(dict.fromkeys((deterministic_queries + ai_queries)))[: settings.discovery_max_search_queries_per_cycle]
    plan = {
        "generated_at": datetime.utcnow().isoformat(),
        "query_themes": (ai_plan or {}).get("query_themes", []),
        "role_clusters": (ai_plan or {}).get("role_clusters", query_inputs["role_families"]),
        "company_archetypes": (ai_plan or {}).get("company_archetypes", profile.preferred_domains_json or []),
        "priority_notes": (ai_plan or {}).get("priority_notes", []),
        "queries": queries,
        "successful_companies": successes["successful_companies"],
        "successful_titles": successes["successful_titles"],
        "recent_failures": recent_failures,
        "company_penalties": learning.get("company_penalties", {}),
        "location_penalties": learning.get("location_penalties", {}),
        "used_openai": bool(ai_plan),
    }
    return plan


def extractor_agent(
    results: list[SearchDiscoveryResult],
    settings: Settings | None = None,
) -> tuple[list[ATSExtractionResult], list[SearchDiscoveryResult]]:
    settings = settings or get_settings()
    extractions: list[ATSExtractionResult] = []
    derived_results: list[SearchDiscoveryResult] = []
    crawled = 0
    for result in results:
        if crawled >= settings.discovery_max_pages_to_crawl_per_cycle:
            break
        lowered = result.url.lower()
        if "greenhouse.io" in lowered or "ashbyhq.com" in lowered:
            continue
        if not any(token in lowered for token in ["/careers", "/jobs", "/join-us", "/work-with-us"]):
            continue
        try:
            final_url, html = fetch_page_snapshot(result.url)
        except Exception as exc:
            logger.info("[CAREERS_PAGE_CLASSIFICATION] %s", {"url": result.url, "status": "fetch_failed", "error": str(exc)})
            continue
        ai_interpretation = interpret_discovery_page_with_ai(
            {
                "source_url": result.url,
                "final_url": final_url,
                "title": result.title,
                "html_excerpt": html[:12000],
            }
        )
        extraction = extract_ats_identifiers_from_html(
            source_url=result.url,
            html=html,
            final_url=final_url,
            ai_interpretation=ai_interpretation,
        )
        crawled += 1
        extractions.append(extraction)
        derived_results.extend(derive_search_results_from_extraction(result.query_text, extraction))
        logger.info(
            "[CAREERS_PAGE_CLASSIFICATION] %s",
            {
                "url": result.url,
                "final_url": final_url,
                "ats_type": extraction.ats_type,
                "greenhouse_tokens": extraction.greenhouse_tokens[:4],
                "ashby_identifiers": extraction.ashby_identifiers[:4],
                "company_name": extraction.company_name,
                "used_openai": extraction.via_openai,
            },
        )
    return extractions, derived_results


def classify_search_surface(result: SearchDiscoveryResult) -> str:
    lowered = f"{result.title} {result.url}".lower()
    if "job-boards.greenhouse.io" in lowered or "boards.greenhouse.io" in lowered:
        return "greenhouse"
    if "jobs.ashbyhq.com" in lowered:
        return "ashby"
    if any(token in lowered for token in ["/careers", "/jobs", "join-us", "work-with-us"]):
        return "careers_page"
    return "unknown"


def triage_agent(
    session: Session,
    profile: CandidateProfile,
    candidate: CompanyDiscoveryCandidate,
    configured_boards: set[str],
    settings: Settings | None = None,
) -> tuple[float, list[str], str]:
    settings = settings or get_settings()
    score, reasons, _ = triage_candidate(session, candidate, profile, configured_boards, settings=settings)
    learning = (profile.extracted_summary_json or {}).get("learning", {})
    company_penalties = learning.get("company_penalties", {})
    if company_penalties.get(candidate.company_name.lower(), 0.0) > 0:
        penalty = float(company_penalties[candidate.company_name.lower()])
        score = round(score - penalty, 2)
        reasons.append(f"company penalty {penalty}")
    ai_judgment = judge_discovery_candidate_with_ai(
        {
            "company_name": candidate.company_name,
            "company_domain": candidate.company_domain,
            "board_type": candidate.board_type,
            "board_locator": candidate.board_locator,
            "result_title": candidate.result_title,
            "result_url": candidate.result_url,
            "query": candidate.discovery_query,
            "preferred_domains": profile.preferred_domains_json or [],
            "preferred_titles": profile.core_titles_json or profile.preferred_titles_json or [],
        }
    )
    decision = "pursue"
    if ai_judgment:
        score = round(score + float(ai_judgment.get("priority_adjustment", 0.0)), 2)
        decision = ai_judgment.get("decision", decision)
        reasons = reasons + [f"ai:{reason}" for reason in ai_judgment.get("reasons", [])]
        source_kind = ai_judgment.get("source_kind")
        if source_kind in {"greenhouse", "ashby", "careers_page"}:
            candidate.board_type = source_kind
    logger.info(
        "[OPENAI_TRIAGE] %s",
        {
            "company": candidate.company_name,
            "board_type": candidate.board_type,
            "used_openai": bool(ai_judgment),
            "decision": decision,
            "score": score,
        },
    )
    if classify_search_surface(SearchDiscoveryResult(candidate.discovery_query, candidate.result_title, candidate.result_url)) == "careers_page":
        reasons.append("surface classified as careers page")
        if decision == "pursue":
            decision = "investigate"
    return score, reasons[:8], decision


def learning_agent(session: Session, profile: CandidateProfile, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    recent_rows = session.scalars(
        select(CompanyDiscovery).order_by(CompanyDiscovery.updated_at.desc()).limit(12)
    ).all()
    positive = [row.company_name for row in recent_rows if row.visible_yield_count > 0][:5]
    negative = [row.company_name for row in recent_rows if row.location_filtered_count > 0 or row.expansion_status == "empty"][:5]
    ai_critique = critique_discovery_cycle_with_ai(
        {
            "positive_companies": positive,
            "negative_companies": negative,
            "recent_queries": [row.discovery_query for row in recent_rows if row.discovery_query][:8],
            "profile_summary": (profile.extracted_summary_json or {}).get("summary", ""),
        }
    )
    logger.info(
        "[OPENAI_LEARNING] %s",
        {
            "used_openai": bool(ai_critique),
            "next_query_count": len((ai_critique or {}).get("next_queries", [])),
            "focus_company_count": len((ai_critique or {}).get("focus_companies", [])),
        },
    )
    return {
        "positive_companies": positive,
        "negative_companies": negative,
        "next_queries": (ai_critique or {}).get("next_queries", []),
        "notes": (ai_critique or {}).get("notes", []),
        "focus_companies": (ai_critique or {}).get("focus_companies", []),
        "used_openai": bool(ai_critique),
    }


def recent_discovery_agent_runs(session: Session, limit: int = 8) -> list[dict]:
    rows = session.scalars(
        select(AgentRun)
        .where(AgentRun.agent_name.in_(["Planner", "Discovery", "Triage", "Learning"]))
        .order_by(AgentRun.created_at.desc())
        .limit(limit)
    ).all()
    return [
        {
            "agent_name": row.agent_name,
            "action": row.action,
            "summary": row.summary,
            "created_at": row.created_at.isoformat(),
            "metadata_json": row.metadata_json or {},
        }
        for row in rows
    ]


def summarize_expansion_actions(rows: list[CompanyDiscovery]) -> list[dict]:
    return [
        {
            "company_name": row.company_name,
            "board_type": row.board_type,
            "board_locator": row.board_locator,
            "surface_provenance": (row.metadata_json or {}).get("surface_provenance"),
            "source_lineage": (row.metadata_json or {}).get("source_lineage"),
            "expansion_status": row.expansion_status,
            "last_expansion_result_count": row.last_expansion_result_count,
            "visible_yield_count": row.visible_yield_count,
            "location_filtered_count": row.location_filtered_count,
            "utility_score": row.utility_score,
        }
        for row in rows[:10]
    ]
