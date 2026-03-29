from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import re
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from connectors.search_web import SearchDiscoveryResult
from core.config import Settings, get_settings
from core.models import AgentRun, Application, CompanyDiscovery, ConnectorHealth, Lead, Listing
from core.schemas import CompanyDiscoveryRowResponse, DiscoverySourceMatrixRow, DiscoveryStatusResponse
from services.search_runs import list_recent_search_runs
from core.time import utcnow
from services.connector_admin import connector_blocked_reason
from services.ops import get_runtime_connector_set


def classify_surface_provenance(
    board_type: str,
    board_locator: str,
    *,
    is_new: bool,
    settings: Settings | None = None,
) -> str:
    settings = settings or get_settings()
    normalized = (board_locator or "").lower()
    if board_type == "greenhouse" and normalized in {token.lower() for token in settings.greenhouse_tokens}:
        return "preseeded"
    if board_type == "ashby" and normalized in {org.lower() for org in settings.ashby_orgs}:
        return "preseeded"
    return "discovered_new" if is_new else "discovered_existing"


def source_lineage_for_surface(board_type: str, provenance: str, discovery_source: Optional[str]) -> str:
    if provenance == "preseeded" or not discovery_source:
        return board_type
    return f"{board_type}+{discovery_source}"


def normalize_company_key(company_name: str, company_domain: Optional[str] = None) -> str:
    if company_domain:
        return re.sub(r"[^a-z0-9]+", "-", company_domain.lower()).strip("-")
    return re.sub(r"[^a-z0-9]+", "-", (company_name or "unknown-company").lower()).strip("-")


@dataclass
class CompanyDiscoveryCandidate:
    company_name: str
    company_domain: Optional[str]
    normalized_company_key: str
    discovery_source: str
    discovery_query: str
    board_type: str
    board_locator: str
    result_url: str
    result_title: str
    query_family: str = "unknown"
    triage_score: float = 0.0
    triage_reasons: list[str] = field(default_factory=list)
    is_new: bool = False

    @property
    def discovery_key(self) -> str:
        return f"{self.board_type}:{self.board_locator.lower()}"


def _discovery_metadata_with_lineage(
    row: CompanyDiscovery,
    *,
    query_family: Optional[str] = None,
    discovery_query: Optional[str] = None,
    surface_provenance: Optional[str] = None,
    source_lineage: Optional[str] = None,
    selected: Optional[bool] = None,
    selected_score: Optional[float] = None,
    selected_reasons: Optional[list[str]] = None,
    result_count: Optional[int] = None,
    visible_yield_count: Optional[int] = None,
    suppressed_yield_count: Optional[int] = None,
    location_filtered_count: Optional[int] = None,
    expansion_status: Optional[str] = None,
    blocked_reason: Optional[str] = None,
    failure_boundary: Optional[str] = None,
    surface_status: Optional[str] = None,
) -> dict:
    def _as_count(value: Optional[int]) -> int:
        return int(value or 0)

    metadata = dict(row.metadata_json or {})
    lineage = dict(metadata.get("discovery_lineage") or {})

    planner_lineage = dict(lineage.get("planner") or {})
    planner_lineage["query_family"] = query_family or metadata.get("query_family") or planner_lineage.get("query_family") or "unknown"
    planner_lineage["query_text"] = discovery_query or row.discovery_query or planner_lineage.get("query_text")
    lineage["planner"] = planner_lineage

    surface_lineage = dict(lineage.get("surface") or {})
    surface_lineage["board_type"] = row.board_type
    surface_lineage["board_locator"] = row.board_locator
    surface_lineage["discovery_source"] = row.discovery_source
    surface_lineage["surface_provenance"] = (
        surface_provenance or metadata.get("surface_provenance") or surface_lineage.get("surface_provenance")
    )
    surface_lineage["source_lineage"] = source_lineage or metadata.get("source_lineage") or surface_lineage.get("source_lineage")
    lineage["surface"] = surface_lineage

    expansion_lineage = dict(lineage.get("expansion") or {})
    if selected is not None:
        expansion_lineage["selected"] = selected
    if selected_score is not None:
        expansion_lineage["selected_score"] = selected_score
    if selected_reasons is not None:
        expansion_lineage["selected_reasons"] = selected_reasons
    expansion_lineage["result_count"] = (
        _as_count(row.last_expansion_result_count) if result_count is None else _as_count(result_count)
    )
    expansion_lineage["visible_yield_count"] = (
        _as_count(row.visible_yield_count) if visible_yield_count is None else _as_count(visible_yield_count)
    )
    expansion_lineage["suppressed_yield_count"] = (
        _as_count(row.suppressed_yield_count) if suppressed_yield_count is None else _as_count(suppressed_yield_count)
    )
    expansion_lineage["location_filtered_count"] = (
        _as_count(row.location_filtered_count) if location_filtered_count is None else _as_count(location_filtered_count)
    )
    expansion_lineage["status"] = expansion_status or row.expansion_status
    expansion_lineage["blocked_reason"] = blocked_reason if blocked_reason is not None else row.blocked_reason
    if failure_boundary is not None:
        expansion_lineage["failure_boundary"] = failure_boundary
    if surface_status is not None:
        expansion_lineage["surface_status"] = surface_status
    expansion_lineage["visible_yield_state"] = (
        "productive" if expansion_lineage["visible_yield_count"] > 0 else "zero_yield"
    )
    lineage["expansion"] = expansion_lineage

    metadata["discovery_lineage"] = lineage
    return metadata


def persist_discovery_lineage(
    row: CompanyDiscovery,
    *,
    query_family: Optional[str] = None,
    discovery_query: Optional[str] = None,
    surface_provenance: Optional[str] = None,
    source_lineage: Optional[str] = None,
    selected: Optional[bool] = None,
    selected_score: Optional[float] = None,
    selected_reasons: Optional[list[str]] = None,
    result_count: Optional[int] = None,
    visible_yield_count: Optional[int] = None,
    suppressed_yield_count: Optional[int] = None,
    location_filtered_count: Optional[int] = None,
    expansion_status: Optional[str] = None,
    blocked_reason: Optional[str] = None,
    failure_boundary: Optional[str] = None,
    surface_status: Optional[str] = None,
) -> None:
    row.metadata_json = _discovery_metadata_with_lineage(
        row,
        query_family=query_family,
        discovery_query=discovery_query,
        surface_provenance=surface_provenance,
        source_lineage=source_lineage,
        selected=selected,
        selected_score=selected_score,
        selected_reasons=selected_reasons,
        result_count=result_count,
        visible_yield_count=visible_yield_count,
        suppressed_yield_count=suppressed_yield_count,
        location_filtered_count=location_filtered_count,
        expansion_status=expansion_status,
        blocked_reason=blocked_reason,
        failure_boundary=failure_boundary,
        surface_status=surface_status,
    )


def _company_name_from_locator(locator: str) -> str:
    return locator.replace("-", " ").replace("_", " ").title()


def _company_name_from_result_title(title: str, fallback: str) -> str:
    normalized = (title or "").strip()
    if not normalized:
        return _company_name_from_locator(fallback)
    cleaned = normalized.replace(" | Work at a Startup", "").replace(" | YC Jobs", "").strip()
    match = re.search(r"\bat\s+(?P<company>[^|]+)$", cleaned, re.IGNORECASE)
    if match:
        return match.group("company").strip()
    return _company_name_from_locator(fallback)


def _normalize_result_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        uddg = parse_qs(parsed.query).get("uddg", [])
        if uddg:
            return unquote(uddg[0])
    return url


def inspect_search_result_candidate(result: SearchDiscoveryResult) -> dict[str, Optional[str]]:
    normalized_url = _normalize_result_url(result.url)
    parsed = urlparse(normalized_url)
    host = parsed.netloc.lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    board_type = None
    board_locator = None
    reason = None

    if not parsed.scheme or not host:
        reason = "missing_host"
    elif parsed.scheme not in {"http", "https"}:
        reason = "non_http_url"
    elif ("job-boards.greenhouse.io" in host or "boards.greenhouse.io" in host) and len(path_parts) >= 1:
        board_type = "greenhouse"
        board_locator = path_parts[0]
    elif "jobs.ashbyhq.com" in host and path_parts:
        board_type = "ashby"
        board_locator = path_parts[0]
    elif host in {"workatastartup.com", "www.workatastartup.com"} and len(path_parts) >= 2 and path_parts[0] == "jobs":
        board_type = "yc_jobs"
        board_locator = path_parts[-1]
    elif host.startswith("careers.") or any(token in parsed.path.lower() for token in ["/careers", "/jobs", "/join-us", "/work-with-us", "/open-roles", "/join", "/company/careers"]):
        board_type = "careers_page"
        board_locator = host
    else:
        reason = "unsupported_surface"

    if board_type and not board_locator:
        reason = "missing_board_locator"

    return {
        "normalized_url": normalized_url,
        "host": host,
        "path": parsed.path,
        "board_type": board_type,
        "board_locator": board_locator,
        "reason": reason,
    }


def candidate_from_search_result(result: SearchDiscoveryResult) -> CompanyDiscoveryCandidate | None:
    inspection = inspect_search_result_candidate(result)
    board_type = inspection["board_type"]
    board_locator = inspection["board_locator"]
    if not board_type or not board_locator:
        return None
    if board_type == "careers_page":
        company_name = _company_name_from_locator(board_locator.split(".")[0])
        company_domain = inspection["host"]
    elif board_type == "yc_jobs":
        company_name = _company_name_from_result_title(result.title, board_locator)
        company_domain = inspection["host"]
    else:
        company_name = _company_name_from_locator(board_locator)
        company_domain = None
    return CompanyDiscoveryCandidate(
        company_name=company_name,
        company_domain=company_domain,
        normalized_company_key=normalize_company_key(company_name, company_domain),
        discovery_source=result.source_surface,
        discovery_query=result.query_text,
        query_family=result.query_family,
        board_type=board_type,
        board_locator=board_locator,
        result_url=inspection["normalized_url"] or result.url,
        result_title=result.title,
    )


def build_query_inputs(session: Session, profile) -> dict[str, list[str]]:
    learning = (profile.extracted_summary_json or {}).get("learning", {})
    boosted_titles = [title for title, _ in sorted((learning.get("title_weights") or {}).items(), key=lambda item: item[1], reverse=True)[:3]]
    role_families = [family for family, _ in sorted((learning.get("role_family_weights") or {}).items(), key=lambda item: item[1], reverse=True)[:3]]
    recent_titles = [
        row[0]
        for row in session.execute(
            select(Lead.primary_title)
            .join(Application, Application.lead_id == Lead.id)
            .where(Application.current_status.in_(["saved", "applied"]))
            .order_by(Application.updated_at.desc())
            .limit(4)
        ).all()
    ]
    return {
        "boosted_titles": boosted_titles,
        "role_families": role_families,
        "recent_titles": recent_titles,
    }


def triage_candidate(
    session: Session,
    candidate: CompanyDiscoveryCandidate,
    profile,
    configured_boards: set[str],
    settings: Settings | None = None,
) -> tuple[float, list[str], CompanyDiscovery | None]:
    settings = settings or get_settings()
    existing = session.scalar(select(CompanyDiscovery).where(CompanyDiscovery.discovery_key == candidate.discovery_key))
    score = 0.0
    reasons: list[str] = []
    query_lower = candidate.discovery_query.lower()
    title_lower = candidate.result_title.lower()

    if candidate.discovery_key not in configured_boards:
        score += 1.8
        reasons.append("new board outside configured seed set")
    else:
        score += 0.6
        reasons.append("known configured board")

    core_titles = [item.lower() for item in (profile.core_titles_json or profile.preferred_titles_json or [])]
    adjacent_titles = [item.lower() for item in (profile.adjacent_titles_json or [])]
    if any(title in query_lower or title in title_lower for title in core_titles):
        score += 1.5
        reasons.append("matched core title")
    elif any(title in query_lower or title in title_lower for title in adjacent_titles):
        score += 0.9
        reasons.append("matched adjacent title")

    if any(domain.lower() in query_lower or domain.lower() in title_lower for domain in (profile.preferred_domains_json or [])):
        score += 0.7
        reasons.append("matched preferred domain theme")

    if any(term in query_lower for term in ["careers", "greenhouse", "ashby", "startup", "work at a startup", "yc jobs"]):
        score += 0.4
        reasons.append("query indicates job-surface intent")

    if existing:
        candidate.is_new = False
        score += min(existing.utility_score, 3.0)
        if existing.last_expansion_result_count == 0 and existing.last_expanded_at:
            cooldown_cutoff = utcnow() - timedelta(minutes=settings.discovery_company_cooldown_minutes)
            if existing.last_expanded_at >= cooldown_cutoff:
                score -= 2.5
                reasons.append("recent empty expansion cooldown")
        if existing.blocked_reason:
            score -= 1.5
            reasons.append(f"existing blocked reason: {existing.blocked_reason}")
    else:
        candidate.is_new = True
        score += 1.0
        reasons.append("newly discovered company")

    return round(score, 2), reasons, existing


def upsert_discovered_company(
    session: Session,
    candidate: CompanyDiscoveryCandidate,
    triage_score: float,
    triage_reasons: list[str],
) -> tuple[CompanyDiscovery, bool]:
    row = session.scalar(select(CompanyDiscovery).where(CompanyDiscovery.discovery_key == candidate.discovery_key))
    now = utcnow()
    metadata = {
        "result_url": candidate.result_url,
        "result_title": candidate.result_title,
        "query_family": candidate.query_family,
        "triage_score": triage_score,
        "triage_reasons": triage_reasons,
    }
    if row:
        row.company_name = candidate.company_name
        row.company_domain = candidate.company_domain or row.company_domain
        row.normalized_company_key = candidate.normalized_company_key
        row.discovery_source = candidate.discovery_source
        row.discovery_query = candidate.discovery_query
        row.last_discovered_at = now
        row.metadata_json = {**(row.metadata_json or {}), **metadata}
        persist_discovery_lineage(
            row,
            query_family=candidate.query_family,
            discovery_query=candidate.discovery_query,
        )
        session.flush()
        return row, False

    row = CompanyDiscovery(
        discovery_key=candidate.discovery_key,
        company_name=candidate.company_name,
        company_domain=candidate.company_domain,
        normalized_company_key=candidate.normalized_company_key,
        discovery_source=candidate.discovery_source,
        discovery_query=candidate.discovery_query,
        first_discovered_at=now,
        last_discovered_at=now,
        board_type=candidate.board_type,
        board_locator=candidate.board_locator,
        expansion_status="discovered",
        metadata_json=metadata,
    )
    session.add(row)
    persist_discovery_lineage(
        row,
        query_family=candidate.query_family,
        discovery_query=candidate.discovery_query,
    )
    session.flush()
    return row, True


def select_candidates_for_expansion(
    rows: list[tuple[CompanyDiscoveryCandidate, CompanyDiscovery, float, list[str]]],
    settings: Settings | None = None,
) -> list[tuple[CompanyDiscoveryCandidate, CompanyDiscovery, float, list[str]]]:
    settings = settings or get_settings()
    ranked = sorted(
        rows,
        key=lambda item: (
            item[1].visible_yield_count > 0,
            item[2],
            item[1].utility_score,
            item[0].is_new,
        ),
        reverse=True,
    )
    selected: list[tuple[CompanyDiscoveryCandidate, CompanyDiscovery, float, list[str]]] = []
    new_count = 0
    for item in ranked:
        candidate, row, _, _ = item
        if len(selected) >= settings.discovery_max_expansions_per_cycle:
            break
        if candidate.is_new and new_count >= settings.discovery_max_new_companies_per_cycle:
            continue
        cooldown_cutoff = utcnow() - timedelta(minutes=settings.discovery_company_cooldown_minutes)
        if row.last_expanded_at and row.last_expansion_result_count == 0 and row.last_expanded_at >= cooldown_cutoff:
            continue
        selected.append(item)
        if candidate.is_new:
            new_count += 1
    return selected


def record_expansion_attempt(
    row: CompanyDiscovery,
    result_count: int,
    visible_yield: int = 0,
    suppressed_yield: int = 0,
    location_filtered: int = 0,
    blocked_reason: Optional[str] = None,
    count_attempt: bool = True,
) -> None:
    row.last_expanded_at = utcnow()
    if count_attempt:
        row.expansion_attempts += 1
    row.last_expansion_result_count = result_count
    row.visible_yield_count += visible_yield
    row.suppressed_yield_count += suppressed_yield
    row.location_filtered_count += location_filtered
    row.blocked_reason = blocked_reason
    if blocked_reason == "investigate":
        row.expansion_status = "investigate"
        row.utility_score = round(max(row.utility_score, 0.5), 2)
        persist_discovery_lineage(row)
        return
    if result_count == 0:
        row.expansion_status = "empty"
        row.utility_score = round(row.utility_score - 0.8, 2)
    else:
        row.expansion_status = "expanded"
        row.utility_score = round(
            row.utility_score
            + (visible_yield * 1.4)
            - (suppressed_yield * 0.15)
            - (location_filtered * 0.35),
            2,
        )
    persist_discovery_lineage(row)


def _company_discovery_row_response(row: CompanyDiscovery) -> CompanyDiscoveryRowResponse:
    metadata_json = _discovery_metadata_with_lineage(row)
    return CompanyDiscoveryRowResponse(
        company_name=row.company_name,
        company_domain=row.company_domain,
        normalized_company_key=row.normalized_company_key,
        discovery_source=row.discovery_source,
        discovery_query=row.discovery_query,
        first_discovered_at=row.first_discovered_at,
        last_discovered_at=row.last_discovered_at,
        last_expanded_at=row.last_expanded_at,
        board_type=row.board_type,
        board_locator=row.board_locator,
        surface_provenance=(row.metadata_json or {}).get("surface_provenance"),
        source_lineage=(row.metadata_json or {}).get("source_lineage"),
        expansion_status=row.expansion_status,
        expansion_attempts=row.expansion_attempts,
        last_expansion_result_count=row.last_expansion_result_count,
        visible_yield_count=row.visible_yield_count,
        suppressed_yield_count=row.suppressed_yield_count,
        location_filtered_count=row.location_filtered_count,
        utility_score=row.utility_score,
        blocked_reason=row.blocked_reason,
        metadata_json=metadata_json,
    )


def _serialize_agent_run(row: AgentRun | None) -> dict[str, object] | None:
    if row is None:
        return None
    return {
        "agent_name": row.agent_name,
        "action": row.action,
        "summary": row.summary,
        "created_at": row.created_at.isoformat(),
        "metadata_json": row.metadata_json or {},
    }


def _recommendation_headline(lead: Lead) -> str:
    score_payload = dict(lead.score_breakdown_json or {})
    explanation = dict(score_payload.get("explanation") or {})
    return (
        explanation.get("headline")
        or explanation.get("summary")
        or lead.explanation
        or score_payload.get("action_explanation")
        or "Recommendation explanation unavailable."
    )


def _recommendation_score(lead: Lead) -> float:
    score_payload = dict(lead.score_breakdown_json or {})
    return float(score_payload.get("final_score", score_payload.get("composite", 0.0)) or 0.0)


def _agentic_lead_payload(lead: Lead, listing: Listing | None) -> dict[str, object]:
    score_payload = dict(lead.score_breakdown_json or {})
    evidence = dict(lead.evidence_json or {})
    verification = dict((listing.metadata_json or {}).get("verification") or {}) if listing is not None else {}
    match_summary = _recommendation_headline(lead)
    return {
        "lead_id": lead.id,
        "company_name": lead.company_name,
        "title": lead.primary_title,
        "url": listing.url if listing is not None else None,
        "source_platform": evidence.get("source_platform"),
        "source_provenance": evidence.get("source_provenance"),
        "source_lineage": evidence.get("source_lineage"),
        "discovery_source": evidence.get("discovery_source"),
        "rank_label": lead.rank_label,
        "confidence_label": lead.confidence_label,
        "freshness_label": lead.freshness_label,
        "recommendation_score": _recommendation_score(lead),
        "action_label": score_payload.get("action_label"),
        "action_explanation": score_payload.get("action_explanation"),
        "explanation": match_summary,
        "match_summary": match_summary,
        "verification_status": verification.get("listing_status") or (listing.listing_status if listing is not None else "unknown"),
        "dead_link_detected": bool(verification.get("dead_link_detected")),
        "verified": not bool(verification.get("dead_link_detected")) and (verification.get("listing_status") or (listing.listing_status if listing is not None else None)) == "active",
        "updated_at": lead.updated_at.isoformat() if lead.updated_at else None,
    }


def _agentic_slice_status(agentic_leads: list[dict[str, object]], cycle_metrics: dict[str, object]) -> dict[str, object]:
    if agentic_leads:
        return {
            "status": "verified_jobs_available",
            "summary": f"{len(agentic_leads)} verified search-discovered job(s) are ranked and ready in the UI.",
            "verified_jobs": len(agentic_leads),
            "zero_yield": False,
        }

    zero_yield = dict(cycle_metrics.get("search_zero_yield") or {})
    if zero_yield:
        reason = str(zero_yield.get("reason") or "search provider returned no accepted results")
        attempts = int(zero_yield.get("zero_yield_attempt_count", 0) or 0)
        return {
            "status": "zero_yield",
            "summary": f"Zero verified jobs this cycle. Search discovery returned no accepted results after {attempts} attempt(s): {reason}.",
            "verified_jobs": 0,
            "zero_yield": True,
            "reason": reason,
            "zero_yield_attempt_count": attempts,
        }

    return {
        "status": "no_verified_jobs",
        "summary": "No verified search-discovered jobs are currently available in the UI.",
        "verified_jobs": 0,
        "zero_yield": False,
    }


def _latest_agent_run(
    session: Session,
    *,
    agent_name: str,
    action: str | None = None,
) -> dict[str, object] | None:
    query = select(AgentRun).where(AgentRun.agent_name == agent_name)
    if action is not None:
        query = query.where(AgentRun.action == action)
    row = session.scalar(query.order_by(AgentRun.created_at.desc(), AgentRun.id.desc()).limit(1))
    return _serialize_agent_run(row)


def build_discovery_source_matrix(
    session: Session,
    *,
    settings: Settings | None = None,
    enabled_connectors: set[str] | None = None,
    strict_live_connectors: set[str] | None = None,
) -> list[DiscoverySourceMatrixRow]:
    settings = settings or get_settings()
    _, runtime_enabled_connectors, strict_runtime_connectors = get_runtime_connector_set(settings)
    enabled_connectors = runtime_enabled_connectors if enabled_connectors is None else set(enabled_connectors)
    strict_live_connectors = strict_runtime_connectors if strict_live_connectors is None else set(strict_live_connectors)
    health_rows = {
        row.connector_name: row
        for row in session.scalars(select(ConnectorHealth)).all()
    }

    def _connector_payload(
        *,
        source_key: str,
        label: str,
        classification: str,
        runtime_state: str,
        toggle_key: str,
        toggle_enabled: bool,
        runtime_enabled: bool,
        strict_live_enabled: bool,
        live_ready: bool,
        trusted_for_output: bool,
        reason: str,
    ) -> DiscoverySourceMatrixRow:
        health_row = health_rows.get(source_key)
        blocked_reason = None
        if runtime_state != "demo_enabled":
            blocked_reason = (
                connector_blocked_reason(source_key, health_row, settings=settings)
                if source_key in health_rows or source_key in {"greenhouse", "ashby", "search_web", "x_search"}
                else None
            )
        return DiscoverySourceMatrixRow(
            source_key=source_key,
            label=label,
            classification=classification,
            runtime_state=runtime_state,
            toggle_key=toggle_key,
            toggle_enabled=toggle_enabled,
            runtime_enabled=runtime_enabled,
            strict_live_enabled=strict_live_enabled,
            live_ready=live_ready,
            trusted_for_output=trusted_for_output,
            reason=reason,
            blocked_reason=blocked_reason,
            connector_status=health_row.status if health_row else None,
            last_mode=health_row.last_mode if health_row else None,
            last_error=health_row.last_error if health_row else None,
        )

    rows: list[DiscoverySourceMatrixRow] = []

    greenhouse_live_ready = settings.greenhouse_enabled and bool(settings.greenhouse_tokens)
    greenhouse_runtime_enabled = "greenhouse" in enabled_connectors
    if settings.demo_mode and greenhouse_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="greenhouse",
                label="Greenhouse",
                classification="working",
                runtime_state="demo_enabled",
                toggle_key="GREENHOUSE_ENABLED + GREENHOUSE_BOARD_TOKENS",
                toggle_enabled=settings.greenhouse_enabled,
                runtime_enabled=greenhouse_runtime_enabled,
                strict_live_enabled="greenhouse" in strict_live_connectors,
                live_ready=greenhouse_live_ready,
                trusted_for_output=True,
                reason="Structured ATS polling works in demo mode here and becomes live-trustworthy when Greenhouse is enabled with board tokens.",
            )
        )
    elif greenhouse_live_ready and greenhouse_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="greenhouse",
                label="Greenhouse",
                classification="working",
                runtime_state="live_enabled",
                toggle_key="GREENHOUSE_ENABLED + GREENHOUSE_BOARD_TOKENS",
                toggle_enabled=True,
                runtime_enabled=True,
                strict_live_enabled="greenhouse" in strict_live_connectors,
                live_ready=True,
                trusted_for_output=True,
                reason="Configured Greenhouse boards are polled directly and remain the trusted ATS source of truth.",
            )
        )
    elif settings.greenhouse_enabled and not settings.greenhouse_tokens:
        rows.append(
            _connector_payload(
                source_key="greenhouse",
                label="Greenhouse",
                classification="not_working",
                runtime_state="misconfigured",
                toggle_key="GREENHOUSE_ENABLED + GREENHOUSE_BOARD_TOKENS",
                toggle_enabled=True,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="Greenhouse is enabled but no board tokens are configured, so no live polling can run.",
            )
        )
    else:
        rows.append(
            _connector_payload(
                source_key="greenhouse",
                label="Greenhouse",
                classification="not_working",
                runtime_state="disabled",
                toggle_key="GREENHOUSE_ENABLED + GREENHOUSE_BOARD_TOKENS",
                toggle_enabled=False,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="Greenhouse discovery is disabled until the kill switch is on and board tokens are configured.",
            )
        )

    ashby_runtime_enabled = "ashby" in enabled_connectors
    ashby_live_ready = bool(settings.ashby_orgs)
    if settings.demo_mode and ashby_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="ashby",
                label="Ashby",
                classification="working",
                runtime_state="demo_enabled",
                toggle_key="ASHBY_ORG_KEYS",
                toggle_enabled=bool(settings.ashby_orgs),
                runtime_enabled=True,
                strict_live_enabled="ashby" in strict_live_connectors,
                live_ready=ashby_live_ready,
                trusted_for_output=True,
                reason="Ashby expansion works in demo mode and can run live when org keys are configured.",
            )
        )
    elif ashby_live_ready and ashby_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="ashby",
                label="Ashby",
                classification="working",
                runtime_state="live_enabled",
                toggle_key="ASHBY_ORG_KEYS",
                toggle_enabled=True,
                runtime_enabled=True,
                strict_live_enabled="ashby" in strict_live_connectors,
                live_ready=True,
                trusted_for_output=True,
                reason="Configured Ashby org keys are polled directly and their normalized jobs can surface as trusted listings.",
            )
        )
    elif settings.search_discovery_enabled and ashby_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="ashby",
                label="Ashby",
                classification="partially_working",
                runtime_state="discovery_bridge_only",
                toggle_key="ASHBY_ORG_KEYS",
                toggle_enabled=False,
                runtime_enabled=True,
                strict_live_enabled="ashby" in strict_live_connectors,
                live_ready=False,
                trusted_for_output=True,
                reason="Ashby is only expanded from search-discovered identifiers right now, so coverage is opportunistic rather than guaranteed.",
            )
        )
    else:
        rows.append(
            _connector_payload(
                source_key="ashby",
                label="Ashby",
                classification="not_working",
                runtime_state="disabled",
                toggle_key="ASHBY_ORG_KEYS",
                toggle_enabled=False,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="Ashby direct polling is inactive until org keys are configured or search discovery is allowed to bridge identifiers.",
            )
        )

    search_runtime_enabled = "search_web" in enabled_connectors
    if settings.search_discovery_enabled and search_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="search_web",
                label="Search Web",
                classification="partially_working",
                runtime_state="live_enabled",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=True,
                runtime_enabled=True,
                strict_live_enabled="search_web" in strict_live_connectors,
                live_ready=True,
                trusted_for_output=False,
                reason=f"Web search runs through {settings.search_discovery_provider} as bounded recall expansion only and does not directly establish trusted listing truth.",
            )
        )
        rows.append(
            DiscoverySourceMatrixRow(
                source_key="yc_jobs",
                label="YC Jobs",
                classification="partially_working",
                runtime_state="bounded_follow_on",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=True,
                runtime_enabled=True,
                strict_live_enabled=False,
                live_ready=True,
                trusted_for_output=True,
                reason="Search-discovered YC job pages can be fetched and normalized one listing at a time, but there is no universal board polling or broad posting-page support yet.",
            )
        )
        rows.append(
            DiscoverySourceMatrixRow(
                source_key="search_web_scrape_fallback",
                label="Search Scrape Fallback",
                classification="partially_working",
                runtime_state="bounded_follow_on",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=True,
                runtime_enabled=True,
                strict_live_enabled=False,
                live_ready=True,
                trusted_for_output=False,
                reason="Careers-page scraping is bounded and only extracts ATS identifiers or careers surfaces; it does not directly trust scraped jobs.",
            )
        )
        rows.append(
            DiscoverySourceMatrixRow(
                source_key="broader_web_sources",
                label="Broader Web Sources",
                classification="not_working",
                runtime_state="staged",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=True,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="Additional non-ATS sources are explicitly staged; only YC direct job pages are normalized in this slice, and generic posting pages are not treated as supported.",
            )
        )
    else:
        rows.append(
            _connector_payload(
                source_key="search_web",
                label="Search Web",
                classification="not_working",
                runtime_state="disabled",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=False,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="Search-driven discovery is disabled, so no web recall expansion or ATS identifier discovery will run.",
            )
        )
        rows.append(
            DiscoverySourceMatrixRow(
                source_key="search_web_scrape_fallback",
                label="Search Scrape Fallback",
                classification="not_working",
                runtime_state="disabled",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=False,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="Careers-page scraping is only active behind search discovery and is fully off when search discovery is disabled.",
            )
        )
        rows.append(
            DiscoverySourceMatrixRow(
                source_key="yc_jobs",
                label="YC Jobs",
                classification="not_working",
                runtime_state="disabled",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=False,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="YC Jobs support is only available through bounded search discovery and direct page normalization when search discovery is enabled.",
            )
        )
        rows.append(
            DiscoverySourceMatrixRow(
                source_key="broader_web_sources",
                label="Broader Web Sources",
                classification="not_working",
                runtime_state="staged",
                toggle_key="SEARCH_DISCOVERY_ENABLED",
                toggle_enabled=False,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="Additional broader job sources remain staged until a bounded, truthful normalization path exists for them.",
            )
        )

    x_runtime_enabled = "x_search" in enabled_connectors
    x_live_ready = bool(settings.x_bearer_token)
    if settings.demo_mode and x_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="x_search",
                label="X Search",
                classification="partially_working",
                runtime_state="demo_enabled",
                toggle_key="X_BEARER_TOKEN",
                toggle_enabled=x_live_ready,
                runtime_enabled=True,
                strict_live_enabled="x_search" in strict_live_connectors,
                live_ready=x_live_ready,
                trusted_for_output=False,
                reason="X search only provides weak hiring signals here; demo mode uses seeded signals and does not prove live coverage.",
            )
        )
    elif x_live_ready and x_runtime_enabled:
        rows.append(
            _connector_payload(
                source_key="x_search",
                label="X Search",
                classification="partially_working",
                runtime_state="live_enabled",
                toggle_key="X_BEARER_TOKEN",
                toggle_enabled=True,
                runtime_enabled=True,
                strict_live_enabled="x_search" in strict_live_connectors,
                live_ready=True,
                trusted_for_output=False,
                reason="X search can fetch live hiring signals, but those signals are weak evidence and are not a trusted listing source on their own.",
            )
        )
    else:
        rows.append(
            _connector_payload(
                source_key="x_search",
                label="X Search",
                classification="not_working",
                runtime_state="disabled",
                toggle_key="X_BEARER_TOKEN",
                toggle_enabled=False,
                runtime_enabled=False,
                strict_live_enabled=False,
                live_ready=False,
                trusted_for_output=False,
                reason="X search is off until a bearer token is configured; without it, the product should not imply live X coverage.",
            )
        )

    rows.append(
        DiscoverySourceMatrixRow(
            source_key="user_submitted",
            label="User-Supplied Links",
            classification="working",
            runtime_state="manual_ingest",
            toggle_key="manual",
            toggle_enabled=True,
            runtime_enabled=True,
            strict_live_enabled=False,
            live_ready=True,
            trusted_for_output=True,
            reason="User-supplied links ingest directly into the same listing and lead validation flow, so this path is explicit and trustworthy.",
        )
    )
    return rows


def _search_source_truth(cycle_metrics: dict[str, object]) -> dict[str, int]:
    diagnostics = dict(cycle_metrics.get("search_fetch_diagnostics") or {})
    accepted_results_count = int(cycle_metrics.get("accepted_results_count", 0) or 0)
    dropped_result_count = int(cycle_metrics.get("dropped_result_count", 0) or 0)
    search_ran = (
        (bool(diagnostics) and diagnostics.get("status") != "not_run")
        or accepted_results_count > 0
        or dropped_result_count > 0
    )
    search_failed = diagnostics.get("status") == "failed"
    search_zero_yield = diagnostics.get("status") == "empty" or bool(cycle_metrics.get("search_zero_yield"))
    return {
        "run_count": 1 if search_ran else 0,
        "failure_count": 1 if search_failed else 0,
        "zero_yield_count": 1 if search_zero_yield else 0,
        "surfaced_jobs_count": int(cycle_metrics.get("agent_discovered_visible_leads_count", 0) or 0),
    }


def _row_failed(row: CompanyDiscovery) -> bool:
    diagnostics = dict((row.metadata_json or {}).get("expansion_diagnostics") or {})
    failure_boundary = diagnostics.get("failure_boundary")
    return bool(failure_boundary and failure_boundary not in {"connector_yield", "empty_discovered_surface", "scrape_parse_yield"})


def _rows_source_truth(rows: list[CompanyDiscovery]) -> dict[str, int]:
    attempted_rows = [
        row
        for row in rows
        if row.last_expanded_at is not None
        or int(row.last_expansion_result_count or 0) > 0
        or int(row.visible_yield_count or 0) > 0
        or int(row.expansion_attempts or 0) > 0
    ]
    return {
        "run_count": len(attempted_rows),
        "failure_count": sum(1 for row in attempted_rows if _row_failed(row)),
        "zero_yield_count": sum(1 for row in attempted_rows if row.last_expansion_result_count == 0),
        "surfaced_jobs_count": sum(int(row.visible_yield_count or 0) for row in attempted_rows),
    }


def _format_source_truth_summary(
    *,
    run_count: int,
    failure_count: int,
    zero_yield_count: int,
    yielded_results_count: int = 0,
    surfaced_jobs_count: int,
    fallback_count: int = 0,
) -> str:
    if run_count == 0 and failure_count == 0 and zero_yield_count == 0 and yielded_results_count == 0 and surfaced_jobs_count == 0:
        return "No observed discovery runs or surfaced jobs yet."

    parts = [f"ran {run_count} time{'s' if run_count != 1 else ''}"] if run_count > 0 else []
    if failure_count > 0:
        parts.append(f"{failure_count} failure{'s' if failure_count != 1 else ''}")
    if zero_yield_count > 0:
        parts.append(f"{zero_yield_count} zero-yield run{'s' if zero_yield_count != 1 else ''}")
    if yielded_results_count > 0:
        parts.append(f"{yielded_results_count} yielded result{'s' if yielded_results_count != 1 else ''}")
    if surfaced_jobs_count > 0:
        parts.append(f"{surfaced_jobs_count} surfaced job{'s' if surfaced_jobs_count != 1 else ''}")
    if fallback_count > 0:
        parts.append(f"{fallback_count} fallback{'s' if fallback_count != 1 else ''}")
    return "; ".join(parts) if parts else "No observed discovery runs or surfaced jobs yet."


def _observer_source_truth(cycle_metrics: dict[str, object], source_key: str) -> dict[str, object] | None:
    observer = dict(cycle_metrics.get("source_runtime_observer") or {})
    entry = observer.get(source_key)
    if not isinstance(entry, dict):
        return None
    return {
        "run_count": int(entry.get("run_count", 0) or 0),
        "failure_count": int(entry.get("failure_count", 0) or 0),
        "zero_yield_count": int(entry.get("zero_yield_count", 0) or 0),
        "yielded_results_count": int(entry.get("yielded_results_count", 0) or 0),
        "surfaced_jobs_count": int(entry.get("surfaced_jobs_count", 0) or 0),
        "fallback_count": int(entry.get("fallback_count", 0) or 0),
        "fallback_order": list(entry.get("fallback_order") or []),
        "last_status": entry.get("last_status"),
    }


def annotate_source_matrix_with_truth(
    source_matrix: list[DiscoverySourceMatrixRow],
    *,
    cycle_metrics: dict[str, object] | None = None,
    discovery_rows: list[CompanyDiscovery] | None = None,
) -> list[DiscoverySourceMatrixRow]:
    cycle_metrics = dict(cycle_metrics or {})
    discovery_rows = list(discovery_rows or [])
    rows_by_board_type = {
        "greenhouse": [row for row in discovery_rows if row.board_type == "greenhouse"],
        "ashby": [row for row in discovery_rows if row.board_type == "ashby"],
        "yc_jobs": [row for row in discovery_rows if row.board_type == "yc_jobs"],
        "search_web_scrape_fallback": [row for row in discovery_rows if row.board_type == "careers_page"],
    }

    annotated_rows: list[DiscoverySourceMatrixRow] = []
    for item in source_matrix:
        observer_truth = _observer_source_truth(cycle_metrics, item.source_key)
        if observer_truth is not None:
            truth = observer_truth
        elif item.source_key == "search_web":
            truth = _search_source_truth(cycle_metrics)
        else:
            truth = _rows_source_truth(rows_by_board_type.get(item.source_key, []))

        failed = truth["failure_count"] > 0 or (item.connector_status or "") in {"failed", "circuit_open"}
        annotated_rows.append(
            item.model_copy(
                update={
                    "ran": truth["run_count"] > 0,
                    "failed": failed,
                    "zero_yield": truth["zero_yield_count"] > 0,
                    "run_count": truth["run_count"],
                    "failure_count": truth["failure_count"],
                    "zero_yield_count": truth["zero_yield_count"],
                    "yielded_results_count": int(truth.get("yielded_results_count", 0) or 0),
                    "surfaced_jobs_count": truth["surfaced_jobs_count"],
                    "fallback_count": int(truth.get("fallback_count", 0) or 0),
                    "fallback_order": list(truth.get("fallback_order") or []),
                    "last_status": truth.get("last_status"),
                    "summary": _format_source_truth_summary(
                        run_count=truth["run_count"],
                        failure_count=truth["failure_count"],
                        zero_yield_count=truth["zero_yield_count"],
                        yielded_results_count=int(truth.get("yielded_results_count", 0) or 0),
                        surfaced_jobs_count=truth["surfaced_jobs_count"],
                        fallback_count=int(truth.get("fallback_count", 0) or 0),
                    ),
                }
            )
        )
    return annotated_rows


def build_discovery_status(session: Session) -> DiscoveryStatusResponse:
    from services.discovery_agents import recent_discovery_agent_runs, summarize_expansion_actions

    since = utcnow() - timedelta(hours=24)
    recent_runs = recent_discovery_agent_runs(session)
    recent_search_runs = list_recent_search_runs(session)
    rows = session.scalars(
        select(CompanyDiscovery)
        .order_by(CompanyDiscovery.last_discovered_at.desc(), CompanyDiscovery.utility_score.desc())
        .limit(25)
    ).all()
    expansion_rows = session.scalars(
        select(CompanyDiscovery)
        .where(CompanyDiscovery.last_expanded_at.is_not(None))
        .order_by(CompanyDiscovery.last_expanded_at.desc(), CompanyDiscovery.id.desc())
        .limit(25)
    ).all()
    successful_expansion_rows = session.scalars(
        select(CompanyDiscovery)
        .where(CompanyDiscovery.last_expansion_result_count > 0)
        .order_by(
            func.coalesce(CompanyDiscovery.last_expanded_at, CompanyDiscovery.last_discovered_at).desc(),
            CompanyDiscovery.id.desc(),
        )
        .limit(10)
    ).all()
    visible_yield_rows = session.scalars(
        select(CompanyDiscovery)
        .where(CompanyDiscovery.visible_yield_count > 0)
        .order_by(
            func.coalesce(CompanyDiscovery.last_expanded_at, CompanyDiscovery.last_discovered_at).desc(),
            CompanyDiscovery.id.desc(),
        )
        .limit(10)
    ).all()
    blocked_or_cooled_down_rows = session.scalars(
        select(CompanyDiscovery)
        .where(
            (CompanyDiscovery.blocked_reason.is_not(None))
            | (CompanyDiscovery.expansion_status.in_(["empty", "investigate"]))
        )
        .order_by(
            func.coalesce(CompanyDiscovery.last_expanded_at, CompanyDiscovery.last_discovered_at).desc(),
            CompanyDiscovery.id.desc(),
        )
        .limit(10)
    ).all()
    planner_run = _latest_agent_run(session, agent_name="Planner", action="planned discovery cycle")
    triage_run = _latest_agent_run(session, agent_name="Triage", action="prioritized discovery candidates")
    learning_run = _latest_agent_run(session, agent_name="Learning", action="updated discovery priors")
    latest_metrics_run = _latest_agent_run(
        session,
        agent_name="Discovery",
        action="recorded discovery cycle metrics",
    )
    cycle_metrics = dict((latest_metrics_run or {}).get("metadata_json", {}).get("cycle_metrics", {}))
    source_matrix = annotate_source_matrix_with_truth(
        build_discovery_source_matrix(session),
        cycle_metrics=cycle_metrics,
        discovery_rows=rows,
    )
    geography_rejections = session.scalars(
        select(Lead)
        .where(Lead.updated_at >= since)
        .order_by(Lead.updated_at.desc())
        .limit(40)
    ).all()
    candidate_agentic_leads = session.scalars(
        select(Lead)
        .where(Lead.hidden.is_(False))
        .order_by(Lead.updated_at.desc())
        .limit(30)
    ).all()
    listing_ids = sorted({lead.listing_id for lead in candidate_agentic_leads if lead.listing_id})
    listings_by_id = {
        listing.id: listing
        for listing in session.scalars(select(Listing).where(Listing.id.in_(listing_ids))).all()
    } if listing_ids else {}
    agentic_leads = [
        _agentic_lead_payload(lead, listings_by_id.get(lead.listing_id))
        for lead in candidate_agentic_leads
        if (lead.evidence_json or {}).get("discovery_source") == "search_web"
        and lead.listing_id
        and listings_by_id.get(lead.listing_id) is not None
        and not bool(dict((listings_by_id.get(lead.listing_id).metadata_json or {}).get("verification") or {}).get("dead_link_detected"))
        and (dict((listings_by_id.get(lead.listing_id).metadata_json or {}).get("verification") or {}).get("listing_status") or listings_by_id.get(lead.listing_id).listing_status) == "active"
    ]
    agentic_leads.sort(
        key=lambda item: (
            float(item.get("recommendation_score") or 0.0),
            str(item.get("updated_at") or ""),
            str(item.get("company_name") or ""),
        ),
        reverse=True,
    )
    agentic_leads = agentic_leads[:12]
    return DiscoveryStatusResponse(
        total_known_companies=session.scalar(select(func.count(CompanyDiscovery.id))) or 0,
        discovered_last_24h=session.scalar(select(func.count(CompanyDiscovery.id)).where(CompanyDiscovery.last_discovered_at >= since)) or 0,
        expanded_last_24h=session.scalar(select(func.count(CompanyDiscovery.id)).where(CompanyDiscovery.last_expanded_at >= since)) or 0,
        source_matrix=source_matrix,
        latest_planner_run=planner_run,
        recent_plans=recent_runs,
        recent_expansions=summarize_expansion_actions(expansion_rows),
        recent_successful_expansions=summarize_expansion_actions(successful_expansion_rows),
        recent_visible_yield=[_company_discovery_row_response(row) for row in visible_yield_rows],
        blocked_or_cooled_down=[_company_discovery_row_response(row) for row in blocked_or_cooled_down_rows],
        recent_greenhouse_tokens=[
            {
                "company_name": row.company_name,
                "token": token,
                "board_locator": row.board_locator,
                "surface_provenance": (row.metadata_json or {}).get("surface_provenance"),
                "last_discovered_at": row.last_discovered_at.isoformat(),
                "expansion_status": row.expansion_status,
            }
            for row in rows
            for token in ((row.metadata_json or {}).get("greenhouse_tokens") or ([row.board_locator] if row.board_type == "greenhouse" else []))
        ][:12],
        recent_ashby_identifiers=[
            {
                "company_name": row.company_name,
                "identifier": identifier,
                "board_locator": row.board_locator,
                "surface_provenance": (row.metadata_json or {}).get("surface_provenance"),
                "last_discovered_at": row.last_discovered_at.isoformat(),
                "expansion_status": row.expansion_status,
            }
            for row in rows
            for identifier in ((row.metadata_json or {}).get("ashby_identifiers") or ([row.board_locator] if row.board_type == "ashby" else []))
        ][:12],
        recent_geography_rejections=[
            {
                "company_name": lead.company_name,
                "title": lead.primary_title,
                "location_scope": (lead.evidence_json or {}).get("location_scope"),
                "location_reason": (lead.evidence_json or {}).get("location_reason"),
                "suppression_category": (lead.evidence_json or {}).get("suppression_category"),
                "source_provenance": (lead.evidence_json or {}).get("source_provenance"),
                "source_lineage": (lead.evidence_json or {}).get("source_lineage"),
            }
            for lead in geography_rejections
            if (lead.evidence_json or {}).get("suppression_category") == "location"
        ][:12],
        recent_agentic_leads=agentic_leads,
        agentic_slice_status=_agentic_slice_status(agentic_leads, cycle_metrics),
        next_recommended_queries=[
            note
            for run in recent_runs
            if run["agent_name"] == "Learning"
            for note in (run["metadata_json"].get("next_queries") or [])
        ][:8],
        latest_openai_usage={
            "planner": bool((planner_run or {}).get("metadata_json", {}).get("used_openai")),
            "triage": bool((triage_run or {}).get("metadata_json", {}).get("used_openai")),
            "learning": bool((learning_run or {}).get("metadata_json", {}).get("used_openai")),
        },
        cycle_metrics=cycle_metrics,
        recent_items=[_company_discovery_row_response(row) for row in rows],
        recent_search_runs=recent_search_runs,
    )


def summarize_source_mix(rows: list[CompanyDiscovery]) -> dict[str, int]:
    return dict(Counter(row.board_type for row in rows))
