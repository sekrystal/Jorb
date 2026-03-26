from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from connectors.search_web import (
    ATSExtractionResult,
    SearchDiscoveryConnector,
    SearchDiscoveryResult,
    build_search_queries,
    classify_query_family,
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
from services.profile import build_search_intent


logger = get_logger(__name__)

ROLE_SYNONYMS = {
    "operations": ["bizops", "business operations", "strategic operations", "program operations"],
    "go_to_market": ["deployment", "implementation", "solutions", "customer success"],
}


@dataclass
class AcquisitionWorkerExecution:
    worker_name: str
    query_texts: list[str]
    results: list[SearchDiscoveryResult]
    live: bool
    diagnostics: dict[str, object]
    extractions: list[ATSExtractionResult] | None = None
    derived_results: list[SearchDiscoveryResult] | None = None

    def summary(self) -> dict[str, object]:
        payload = {
            "worker_name": self.worker_name,
            "query_count": len(self.query_texts),
            "queries": self.query_texts,
            "result_count": len(self.results),
            "candidate_urls": [result.url for result in self.results[:10]],
            "live": self.live,
            "diagnostics": self.diagnostics,
        }
        if self.extractions is not None:
            payload["extraction_count"] = len(self.extractions)
        if self.derived_results is not None:
            payload["derived_result_count"] = len(self.derived_results)
            payload["derived_candidate_urls"] = [result.url for result in (self.derived_results or [])[:10]]
        return payload


def _dedupe_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value.strip() for value in values if value and value.strip()))


def _bounded_worker_queries(values: list[str], limit: int) -> list[str]:
    if limit <= 0:
        return []
    return _dedupe_strings(values)[:limit]


def _top_geographies(preferred_locations: list[str]) -> list[str]:
    return [
        location
        for location in preferred_locations
        if "remote" not in location.lower() and "hybrid" not in location.lower() and "onsite" not in location.lower()
    ][:2]


def _planner_location_targets(search_intent) -> list[dict[str, str]]:
    preferred_locations = list(search_intent.preferred_locations or [])
    work_mode_preference = str(search_intent.work_mode_preference or "unspecified")
    targets: list[dict[str, str]] = []

    def add(location: str, work_mode: str) -> None:
        normalized_location = (location or "").strip()
        normalized_work_mode = (work_mode or "unspecified").strip() or "unspecified"
        if not normalized_location:
            return
        target = {"location": normalized_location, "work_mode": normalized_work_mode}
        if target not in targets:
            targets.append(target)

    if work_mode_preference == "remote" or any("remote" in location.lower() for location in preferred_locations):
        add("remote us", "remote")
    for geography in _top_geographies(preferred_locations):
        add(geography, work_mode_preference)
    if not targets:
        add("remote us", "remote" if work_mode_preference == "remote" else "unspecified")
    return targets[:3]


def _structured_discovery_plans(search_intent) -> dict[str, list[dict[str, object]]]:
    target_roles = _dedupe_strings(list(search_intent.target_roles or []))[:3]
    location_targets = _planner_location_targets(search_intent)
    plans: dict[str, list[dict[str, object]]] = {"ats": [], "search": [], "weak_signal": []}
    seen: set[tuple[str, str, str, str]] = set()

    def add(plan_type: str, *, role: str, location: str, work_mode: str, query_text: str, execution_target: str, executable: bool) -> None:
        key = (plan_type, role, location, query_text)
        if key in seen:
            return
        seen.add(key)
        plans[plan_type].append(
            {
                "planner_type": plan_type,
                "role": role,
                "location": location,
                "work_mode": work_mode,
                "query_text": query_text,
                "query_family": classify_query_family(query_text),
                "query_classification": None,
                "execution_target": execution_target,
                "executable": executable,
            }
        )

    for role in target_roles:
        for target in location_targets:
            location = target["location"]
            work_mode = target["work_mode"]
            if work_mode == "remote":
                search_query = f'"{role}" remote us startup careers'
                weak_signal_query = f'"{role}" remote us startup hiring'
                greenhouse_query = f'site:job-boards.greenhouse.io "{role}" "remote"'
                ashby_query = f'site:jobs.ashbyhq.com "{role}" "remote"'
            elif work_mode in {"hybrid", "onsite"}:
                search_query = f'"{role}" {work_mode} "{location}" startup careers'
                weak_signal_query = f'"{role}" {work_mode} "{location}" startup hiring'
                greenhouse_query = f'site:job-boards.greenhouse.io "{role}" "{location}"'
                ashby_query = f'site:jobs.ashbyhq.com "{role}" "{location}"'
            else:
                search_query = f'"{role}" "{location}" startup careers'
                weak_signal_query = f'"{role}" "{location}" startup hiring'
                greenhouse_query = f'site:job-boards.greenhouse.io "{role}" "{location}"'
                ashby_query = f'site:jobs.ashbyhq.com "{role}" "{location}"'

            add("ats", role=role, location=location, work_mode=work_mode, query_text=greenhouse_query, execution_target="search_web", executable=True)
            add("ats", role=role, location=location, work_mode=work_mode, query_text=ashby_query, execution_target="search_web", executable=True)
            add("search", role=role, location=location, work_mode=work_mode, query_text=search_query, execution_target="search_web", executable=True)
            add("weak_signal", role=role, location=location, work_mode=work_mode, query_text=weak_signal_query, execution_target="x_search", executable=False)

    for plan_type, entries in plans.items():
        for entry in entries:
            entry["query_classification"] = plan_type
    return plans


def _apply_profile_search_constraints(base_queries: list[str], search_intent) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    def add(query: str) -> None:
        normalized = query.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        queries.append(normalized)

    target_roles = list(search_intent.target_roles or [])
    preferred_locations = list(search_intent.preferred_locations or [])
    work_mode_preference = str(search_intent.work_mode_preference or "unspecified")
    top_geographies = _top_geographies(preferred_locations)

    for role in target_roles[:3]:
        add(f'"{role}" startup careers')
        add(f'"{role}" startup jobs')
        if work_mode_preference == "remote":
            add(f'"{role}" remote us careers')
        elif work_mode_preference in {"hybrid", "onsite"}:
            for geography in top_geographies or preferred_locations[:1]:
                add(f'"{role}" {work_mode_preference} "{geography}" startup careers')
                add(f'"{role}" "{geography}" startup jobs')
        else:
            for geography in top_geographies:
                add(f'"{role}" "{geography}" startup careers')

    for query in base_queries:
        lowered = query.lower()
        if work_mode_preference in {"hybrid", "onsite"} and "remote us" in lowered:
            continue
        add(query)

    return queries


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
    search_intent = build_search_intent(profile)
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
        core_titles=search_intent.target_roles or profile.core_titles_json or profile.preferred_titles_json or [],
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
    deterministic_queries = _apply_profile_search_constraints(list(dict.fromkeys(deterministic_queries)), search_intent)

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
    structured_query_plans = _structured_discovery_plans(search_intent)
    plan = {
        "generated_at": datetime.utcnow().isoformat(),
        "query_themes": (ai_plan or {}).get("query_themes", []),
        "role_clusters": (ai_plan or {}).get("role_clusters", query_inputs["role_families"]),
        "company_archetypes": (ai_plan or {}).get("company_archetypes", profile.preferred_domains_json or []),
        "priority_notes": (ai_plan or {}).get("priority_notes", []),
        "queries": queries,
        "structured_query_plans": structured_query_plans,
        "query_plan_summary": {
            plan_type: {
                "count": len(entries),
                "executable_count": sum(1 for entry in entries if entry.get("executable")),
                "execution_targets": _dedupe_strings([str(entry.get("execution_target") or "") for entry in entries]),
            }
            for plan_type, entries in structured_query_plans.items()
        },
        "profile_constraints_applied": search_intent.applied_constraints,
        "profile_constraints_defaulted": search_intent.defaulted_constraints,
        "search_intent": search_intent.model_dump(),
        "target_roles": search_intent.target_roles,
        "preferred_locations": search_intent.preferred_locations,
        "work_mode_preference": search_intent.work_mode_preference,
        "successful_companies": successes["successful_companies"],
        "successful_titles": successes["successful_titles"],
        "recent_failures": recent_failures,
        "company_penalties": learning.get("company_penalties", {}),
        "location_penalties": learning.get("location_penalties", {}),
        "used_openai": bool(ai_plan),
    }
    return plan


def build_acquisition_plan(planner_plan: dict, settings: Settings | None = None) -> dict[str, list[str]]:
    settings = settings or get_settings()
    max_queries = max(int(settings.discovery_max_search_queries_per_cycle or 0), 0)
    structured_plans = planner_plan.get("structured_query_plans") or {}
    ats_queries = [
        str(entry.get("query_text") or "").strip()
        for entry in structured_plans.get("ats", [])
        if entry.get("executable")
    ]
    bounded_ats_limit = 0
    if max_queries > 0:
        bounded_ats_limit = min(len(_dedupe_strings(ats_queries)), max(1, min(4, max_queries // 2 or 1)))
    selected_ats_queries = _bounded_worker_queries(ats_queries, bounded_ats_limit)
    selected_search_queries = _bounded_worker_queries(
        [query for query in list(planner_plan.get("queries") or []) if query not in set(selected_ats_queries)],
        max(max_queries - len(selected_ats_queries), 0),
    )
    return {
        "ats_queries": selected_ats_queries,
        "search_queries": selected_search_queries,
    }


def _execute_search_worker(
    worker_name: str,
    query_texts: list[str],
    fetcher: Callable[[list[str]], tuple[list[SearchDiscoveryResult], bool]],
    diagnostics_provider: Callable[[], dict[str, object]] | None = None,
) -> AcquisitionWorkerExecution:
    if not query_texts:
        return AcquisitionWorkerExecution(
            worker_name=worker_name,
            query_texts=[],
            results=[],
            live=False,
            diagnostics={"status": "not_run", "reason": "no_queries"},
        )
    results, live = fetcher(query_texts)
    diagnostics = dict(diagnostics_provider() if diagnostics_provider else {})
    diagnostics.setdefault("status", "results" if results else "empty")
    diagnostics.setdefault("query_count", len(query_texts))
    diagnostics.setdefault("result_count", len(results))
    return AcquisitionWorkerExecution(
        worker_name=worker_name,
        query_texts=query_texts,
        results=results,
        live=live,
        diagnostics=diagnostics,
    )


def ats_resolver_worker(
    planner_plan: dict,
    *,
    connector: SearchDiscoveryConnector | None = None,
    settings: Settings | None = None,
    require_live: bool = False,
    fetcher: Callable[[list[str]], tuple[list[SearchDiscoveryResult], bool]] | None = None,
) -> AcquisitionWorkerExecution:
    settings = settings or get_settings()
    connector = connector or SearchDiscoveryConnector()
    acquisition_plan = build_acquisition_plan(planner_plan, settings=settings)
    return _execute_search_worker(
        "ats_resolver",
        acquisition_plan["ats_queries"],
        fetcher or (lambda query_texts: connector.fetch(query_texts, require_live)),
        lambda: dict(getattr(connector, "last_fetch_diagnostics", {}) or {}),
    )


def search_acquisition_worker(
    planner_plan: dict,
    *,
    connector: SearchDiscoveryConnector | None = None,
    settings: Settings | None = None,
    require_live: bool = False,
    fetcher: Callable[[list[str]], tuple[list[SearchDiscoveryResult], bool]] | None = None,
) -> AcquisitionWorkerExecution:
    settings = settings or get_settings()
    connector = connector or SearchDiscoveryConnector()
    acquisition_plan = build_acquisition_plan(planner_plan, settings=settings)
    return _execute_search_worker(
        "search",
        acquisition_plan["search_queries"],
        fetcher or (lambda query_texts: connector.fetch(query_texts, require_live)),
        lambda: dict(getattr(connector, "last_fetch_diagnostics", {}) or {}),
    )


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


def parser_acquisition_worker(
    results: list[SearchDiscoveryResult],
    settings: Settings | None = None,
    extractor: Callable[[list[SearchDiscoveryResult], Settings | None], tuple[list[ATSExtractionResult], list[SearchDiscoveryResult]]] | None = None,
) -> AcquisitionWorkerExecution:
    settings = settings or get_settings()
    extractor_fn = extractor or extractor_agent
    extractions, derived_results = extractor_fn(results, settings=settings)
    diagnostics = {
        "status": "results" if extractions or derived_results else "empty",
        "input_result_count": len(results),
        "pages_crawled": len(extractions),
        "derived_result_count": len(derived_results),
        "greenhouse_tokens": sorted({token for extraction in extractions for token in extraction.greenhouse_tokens})[:12],
        "ashby_identifiers": sorted({org for extraction in extractions for org in extraction.ashby_identifiers})[:12],
    }
    return AcquisitionWorkerExecution(
        worker_name="parser",
        query_texts=[],
        results=results,
        live=False,
        diagnostics=diagnostics,
        extractions=extractions,
        derived_results=derived_results,
    )


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
