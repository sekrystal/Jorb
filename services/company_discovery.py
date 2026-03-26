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
from core.models import AgentRun, Application, CompanyDiscovery, ConnectorHealth, Lead
from core.schemas import CompanyDiscoveryRowResponse, DiscoverySourceMatrixRow, DiscoveryStatusResponse
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
    company_name = _company_name_from_locator(board_locator.split(".")[0] if board_type == "careers_page" else board_locator)
    company_domain = inspection["host"] if board_type == "careers_page" else None
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

    if any(term in query_lower for term in ["careers", "greenhouse", "ashby", "startup"]):
        score += 0.4
        reasons.append("query indicates job-surface intent")

    if existing:
        candidate.is_new = False
        score += min(existing.utility_score, 3.0)
        if existing.last_expansion_result_count == 0 and existing.last_expanded_at:
            cooldown_cutoff = datetime.utcnow() - timedelta(minutes=settings.discovery_company_cooldown_minutes)
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
    now = datetime.utcnow()
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
        cooldown_cutoff = datetime.utcnow() - timedelta(minutes=settings.discovery_company_cooldown_minutes)
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
    row.last_expanded_at = datetime.utcnow()
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


def build_discovery_status(session: Session) -> DiscoveryStatusResponse:
    from services.discovery_agents import recent_discovery_agent_runs, summarize_expansion_actions

    since = datetime.utcnow() - timedelta(hours=24)
    source_matrix = build_discovery_source_matrix(session)
    recent_runs = recent_discovery_agent_runs(session)
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
    geography_rejections = session.scalars(
        select(Lead)
        .where(Lead.updated_at >= since)
        .order_by(Lead.updated_at.desc())
        .limit(40)
    ).all()
    agentic_leads = session.scalars(
        select(Lead)
        .where(Lead.hidden.is_(False))
        .order_by(Lead.updated_at.desc())
        .limit(30)
    ).all()
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
        recent_agentic_leads=[
            {
                "company_name": lead.company_name,
                "title": lead.primary_title,
                "source_platform": (lead.evidence_json or {}).get("source_platform"),
                "source_provenance": (lead.evidence_json or {}).get("source_provenance"),
                "source_lineage": (lead.evidence_json or {}).get("source_lineage"),
                "discovery_source": (lead.evidence_json or {}).get("discovery_source"),
                "rank_label": lead.rank_label,
                "confidence_label": lead.confidence_label,
                "updated_at": lead.updated_at.isoformat() if lead.updated_at else None,
            }
            for lead in agentic_leads
            if (lead.evidence_json or {}).get("discovery_source") == "search_web"
        ][:12],
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
        cycle_metrics=dict((latest_metrics_run or {}).get("metadata_json", {}).get("cycle_metrics", {})),
        recent_items=[_company_discovery_row_response(row) for row in rows],
    )


def summarize_source_mix(rows: list[CompanyDiscovery]) -> dict[str, int]:
    return dict(Counter(row.board_type for row in rows))
