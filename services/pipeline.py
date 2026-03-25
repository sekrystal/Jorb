from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections import Counter

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.config import get_settings
from core.logging import get_logger
from core.models import AgentRun, FollowUpTask, Investigation, Lead, Listing, RunDigest, Signal
from core.schemas import AgentRunResponse, ListingRecord, SignalRecord
from services.activity import append_lead_agent_trace, log_agent_activity, log_agent_run
from services.digests import record_run_digest
from services.freshness import classify_freshness_label, validate_listing
from services.governance import evaluate_learning_governance
from services.learning import add_watchlist_item, generate_follow_up_tasks
from services.profile import get_candidate_profile
from services.resolve_company import get_or_create_company
from services.sync import (
    _duplicate_winner_context,
    _upsert_lead,
    _upsert_listing,
    _upsert_signal,
    apply_critic_decision_to_lead,
    sync_all,
)


logger = get_logger(__name__)


DEMO_SCOUT_BATCHES: list[dict[str, list[dict]]] = [
    {
        "listings": [
            {
                "company_name": "Ramp",
                "company_domain": "ramp.com/fintech",
                "careers_url": "https://ramp.com/careers",
                "title": "Strategic Programs Lead",
                "location": "New York, NY",
                "url": "https://boards.greenhouse.io/ramp/jobs/4001",
                "source_type": "greenhouse",
                "posted_at": (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(),
                "description_text": "Own cross-functional operating cadences, executive reporting, and launch coordination for a fast-scaling fintech team.",
                "metadata_json": {
                    "page_text": "Fresh job posting for strategic programs at Ramp.",
                },
            }
        ],
        "signals": [],
    },
    {
        "listings": [],
        "signals": [
            {
                "source_type": "x",
                "source_url": "https://x.com/builderops/status/2001",
                "author_handle": "@builderops",
                "raw_text": "Hiring a business operations lead for an applied AI startup in SF. No listing yet, but this is a real operator role.",
                "published_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                "company_guess": "Applied AI Startup",
                "role_guess": "business operations lead",
                "location_guess": "san francisco",
                "hiring_confidence": 0.62,
            }
        ],
    },
    {
        "listings": [
            {
                "company_name": "Applied AI Startup",
                "company_domain": "appliedai.dev",
                "careers_url": "https://appliedai.dev/careers",
                "title": "Business Operations Lead",
                "location": "San Francisco, CA",
                "url": "https://boards.greenhouse.io/appliedai/jobs/4002",
                "source_type": "greenhouse",
                "posted_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                "description_text": "Own business operations, planning cadence, recruiting coordination, and early customer deployment support for an applied AI team.",
                "metadata_json": {"page_text": "Fresh job posting for business operations lead."},
            }
        ],
        "signals": [],
    },
]


def recommendation_score_value(score_breakdown: dict | None) -> float:
    score_breakdown = score_breakdown or {}
    value = score_breakdown.get("final_score", score_breakdown.get("composite", 0.0))
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def recommendation_component_value(score_breakdown: dict | None, component_key: str) -> float:
    score_breakdown = score_breakdown or {}
    if component_key in score_breakdown:
        try:
            return float(score_breakdown.get(component_key) or 0.0)
        except (TypeError, ValueError):
            return 0.0
    for component in score_breakdown.get("component_metrics", []) or []:
        if component.get("key") == component_key:
            try:
                return float(component.get("score") or 0.0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _mark_leads(session: Session, leads: list[Lead], agent_name: str, action: str, message_fn) -> None:
    for lead in leads:
        change_state = None
        if agent_name == "Scout":
            change_state = "updated"
        elif agent_name == "Ranker":
            change_state = "reranked"
        elif agent_name == "Critic" and lead.hidden:
            change_state = "suppressed"
        append_lead_agent_trace(lead, agent_name, action, message_fn(lead), change_state=change_state)


def _coerce_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _insert_demo_batch(session: Session) -> tuple[int, int, list[Listing], list[Signal]]:
    existing_listing_urls = {
        row[0]
        for row in session.execute(select(Listing.url).where(Listing.url.in_([item["url"] for batch in DEMO_SCOUT_BATCHES for item in batch["listings"]])))
    }
    existing_signal_urls = {
        row[0]
        for row in session.execute(select(Signal.source_url).where(Signal.source_url.in_([item["source_url"] for batch in DEMO_SCOUT_BATCHES for item in batch["signals"]])))
    }

    next_batch = None
    for batch in DEMO_SCOUT_BATCHES:
        batch_listing_urls = {item["url"] for item in batch["listings"]}
        batch_signal_urls = {item["source_url"] for item in batch["signals"]}
        if not batch_listing_urls.issubset(existing_listing_urls) or not batch_signal_urls.issubset(existing_signal_urls):
            next_batch = batch
            break

    if not next_batch:
        return 0, 0, [], []

    listing_count = 0
    inserted_listings: list[Listing] = []
    for item in next_batch["listings"]:
        record = ListingRecord(
            company_name=item["company_name"],
            company_domain=item.get("company_domain"),
            careers_url=item.get("careers_url"),
            title=item["title"],
            location=item.get("location"),
            url=item["url"],
            source_type=item["source_type"],
            posted_at=_coerce_datetime(item.get("posted_at")),
            description_text=item.get("description_text"),
            metadata_json=item.get("metadata_json", {}),
        )
        record = validate_listing(record)
        company = get_or_create_company(
            session,
            name=record.company_name,
            domain=record.company_domain,
            careers_url=record.careers_url,
            ats_provider=record.source_type,
        )
        listing, _ = _upsert_listing(session, record, company.id)
        inserted_listings.append(listing)
        listing_count += 1

    signal_count = 0
    inserted_signals: list[Signal] = []
    for item in next_batch["signals"]:
        signal = SignalRecord(
            source_type=item["source_type"],
            source_url=item["source_url"],
            author_handle=item.get("author_handle"),
            raw_text=item["raw_text"],
            published_at=_coerce_datetime(item.get("published_at")),
            company_guess=item.get("company_guess"),
            role_guess=item.get("role_guess"),
            location_guess=item.get("location_guess"),
            hiring_confidence=item.get("hiring_confidence", 0.0),
            signal_status="new",
        )
        inserted_signals.append(_upsert_signal(session, signal))
        signal_count += 1

    return listing_count, signal_count, inserted_listings, inserted_signals


def run_scout_agent(
    session: Session,
    source_mode: str = "demo",
    enabled_connectors: set[str] | None = None,
    strict_live_connectors: set[str] | None = None,
) -> AgentRunResponse:
    cycle_started_at = datetime.utcnow()
    listing_count = 0
    signal_count = 0
    inserted_listings: list[Listing] = []
    inserted_signals: list[Signal] = []

    if source_mode == "demo":
        listing_count, signal_count, inserted_listings, inserted_signals = _insert_demo_batch(session)
    profile = get_candidate_profile(session)
    inserted_leads: list[Lead] = []
    if source_mode == "demo":
        for listing in inserted_listings:
            lead, _ = _upsert_lead(
                session=session,
                lead_type="listing",
                company_name=listing.company_name,
                company_id=listing.company_id,
                title=listing.title,
                listing=listing,
                signal=None,
                profile=profile,
                listing_url=listing.url,
                source_type=listing.source_type,
                company_domain=(listing.metadata_json or {}).get("company_domain"),
                location=listing.location,
                description_text=listing.description_text or "",
                listing_status=listing.listing_status,
                freshness_label=classify_freshness_label(listing.freshness_days),
                evidence_json={"snippets": [(listing.description_text or "")[:240]], "source_queries": []},
            )
            inserted_leads.append(lead)
        for signal in inserted_signals:
            company = get_or_create_company(session, name=signal.company_guess or "Unresolved signal", ats_provider="x")
            lead, _ = _upsert_lead(
                session=session,
                lead_type="signal",
                company_name=company.name,
                company_id=company.id,
                title=(signal.role_guess or "Hiring signal").title(),
                listing=None,
                signal=signal,
                profile=profile,
                listing_url=signal.source_url,
                source_type="x",
                company_domain=company.domain,
                location=signal.location_guess,
                description_text=signal.raw_text,
                listing_status=None,
                freshness_label="fresh",
                evidence_json={"snippets": [signal.raw_text[:220]], "source_queries": []},
            )
            inserted_leads.append(lead)
    result = sync_all(
        session,
        include_rechecks=True,
        enabled_connectors=enabled_connectors,
        strict_live_connectors=strict_live_connectors,
    )
    touched = result.leads_created + result.leads_updated
    recent_leads = session.scalars(select(Lead).order_by(Lead.updated_at.desc()).limit(max(touched, 5))).all()
    _mark_leads(
        session,
        recent_leads,
        "Scout",
        "ingested new sourcing batch",
        lambda lead: f"Scout refreshed source data for {lead.company_name} / {lead.primary_title}",
    )
    if source_mode == "live":
        inserted_leads = session.scalars(select(Lead).where(Lead.created_at >= cycle_started_at).order_by(Lead.created_at.desc())).all()
        listing_count = result.listings_ingested
        signal_count = result.signals_ingested
    new_names = [f"{lead.company_name} / {lead.primary_title}" for lead in inserted_leads[:4]]
    mode_label = "live source data" if source_mode == "live" else "source data"
    discovery_suffix = f" {result.discovery_summary}" if result.discovery_summary else ""
    discovery_memory_suffix = ""
    if result.discovery_status:
        discovery_memory_suffix = (
            f" New companies discovered: {result.discovery_status.get('new_companies_discovered', 0)}. "
            f"Companies expanded: {result.discovery_status.get('companies_selected_for_expansion', 0)}."
        )
    summary = (
        f"Scout added {listing_count} listings and {signal_count} signals from {mode_label}. "
        f"New rows: {', '.join(new_names) if new_names else 'none'}.{discovery_suffix}{discovery_memory_suffix}"
    )
    log_agent_activity(
        session,
        agent_name="Scout",
        action="ingested source data",
        target_type="records",
        target_count=listing_count + signal_count,
        result_summary=summary,
    )
    log_agent_run(
        session,
        "Scout",
        "ingested source data",
        summary,
        listing_count + signal_count,
        metadata_json={
            "new_rows": new_names,
            "source_mode": source_mode,
            "listing_count": listing_count,
            "signal_count": signal_count,
            "discovery_metrics": result.discovery_metrics,
            "discovery_summary": result.discovery_summary,
            "discovery_status": result.discovery_status,
        },
    )
    return AgentRunResponse(agent="scout", summary=summary)


def run_resolver_agent(
    session: Session,
    resync: bool = True,
    enabled_connectors: set[str] | None = None,
    strict_live_connectors: set[str] | None = None,
) -> AgentRunResponse:
    before_combined = session.scalar(select(func.count(Lead.id)).where(Lead.lead_type == "combined")) or 0
    result = (
        sync_all(
            session,
            include_rechecks=True,
            enabled_connectors=enabled_connectors,
            strict_live_connectors=strict_live_connectors,
        )
        if resync
        else None
    )
    after_combined = session.scalar(select(func.count(Lead.id)).where(Lead.lead_type == "combined")) or 0
    leads = session.scalars(select(Lead).where(Lead.lead_type.in_(["combined", "signal"]))).all()
    _mark_leads(
        session,
        leads,
        "Resolver",
        "linked signals and companies",
        lambda lead: "Linked signal to active listing" if lead.lead_type == "combined" else "Kept weak signal unresolved but visible",
    )
    resolved_delta = max(after_combined - before_combined, 0)
    unresolved = session.scalar(select(func.count(Signal.id)).where(Signal.signal_status == "needs_recheck")) or 0
    combined_names = [f"{lead.company_name} / {lead.primary_title}" for lead in leads if lead.lead_type == "combined"][:3]
    summary = f"Resolver linked {resolved_delta} additional signals. Combined leads: {', '.join(combined_names) if combined_names else 'none'}. {unresolved} signals still need recheck."
    log_agent_activity(
        session,
        agent_name="Resolver",
        action="resolved signal relationships",
        target_type="signals",
        target_count=result.signals_ingested if result else len(leads),
        result_summary=summary,
    )
    log_agent_run(
        session,
        "Resolver",
        "resolved signal relationships",
        summary,
        result.signals_ingested if result else len(leads),
        metadata_json={
            "combined_leads": combined_names,
            "resolved_delta": resolved_delta,
            "open_investigations": unresolved,
        },
    )
    return AgentRunResponse(agent="resolver", summary=summary)


def run_fit_agent(session: Session) -> AgentRunResponse:
    leads = session.scalars(select(Lead)).all()
    _mark_leads(
        session,
        leads,
        "Fit",
        "classified fit and qualification",
        lambda lead: f"Fit classified this lead as {lead.qualification_fit_label}",
    )
    hidden_count = sum(1 for lead in leads if lead.qualification_fit_label in {"underqualified", "overqualified"})
    mismatched = [f"{lead.company_name} / {lead.primary_title}" for lead in leads if lead.qualification_fit_label in {"underqualified", "overqualified"}][:3]
    summary = f"Fit reviewed {len(leads)} leads and flagged {hidden_count} as clearly mismatched. Examples: {', '.join(mismatched) if mismatched else 'none'}."
    log_agent_activity(
        session,
        agent_name="Fit",
        action="classified fit",
        target_type="leads",
        target_count=len(leads),
        result_summary=summary,
    )
    log_agent_run(
        session,
        "Fit",
        "classified fit",
        summary,
        len(leads),
        metadata_json={"mismatched_examples": mismatched, "hidden_count": hidden_count},
    )
    return AgentRunResponse(agent="fit", summary=summary)


def run_ranker_agent(
    session: Session,
    resync: bool = True,
    enabled_connectors: set[str] | None = None,
    strict_live_connectors: set[str] | None = None,
) -> AgentRunResponse:
    if resync:
        sync_all(
            session,
            include_rechecks=False,
            enabled_connectors=enabled_connectors,
            strict_live_connectors=strict_live_connectors,
        )
    leads = session.scalars(select(Lead)).all()
    _mark_leads(
        session,
        leads,
        "Ranker",
        "reprioritized leads",
        lambda lead: f"Ranker set {lead.rank_label} priority with {lead.confidence_label} confidence",
    )
    visible = sum(1 for lead in leads if not lead.hidden)
    top_visible = session.scalars(select(Lead).where(Lead.hidden.is_(False)).order_by(Lead.updated_at.desc()).limit(10)).all()
    visible_all = session.scalars(select(Lead).where(Lead.hidden.is_(False))).all()
    title_fit_buckets = Counter(lead.title_fit_label for lead in visible_all)
    qualification_buckets = Counter(lead.qualification_fit_label for lead in visible_all)
    rank_buckets = Counter(lead.rank_label for lead in visible_all)
    source_buckets = Counter(
        (lead.evidence_json or {}).get("source_platform", (lead.evidence_json or {}).get("source_type", "unknown"))
        for lead in visible_all
    )
    visible_scores = [
        recommendation_score_value(lead.score_breakdown_json)
        for lead in top_visible
    ]
    if visible_scores:
        max_score = max(visible_scores)
        min_score = min(visible_scores)
        avg_score = round(sum(visible_scores) / len(visible_scores), 2)
        logger.info("[RANK_DISTRIBUTION] max=%s min=%s avg=%s", max_score, min_score, avg_score)
        if len(set(visible_scores)) == 1 or (max_score - min_score) <= 0.15:
            logger.error("[RANK_FAILURE] Ranking has no meaningful differentiation between jobs.")
    for lead in top_visible[:10]:
        score_breakdown = lead.score_breakdown_json or {}
        logger.info(
            "[JOB_DEBUG] title=%s company=%s score=%s fit=%s source=%s",
            lead.primary_title,
            lead.company_name,
            recommendation_score_value(score_breakdown),
            recommendation_component_value(score_breakdown, "title_fit"),
            (lead.evidence_json or {}).get("source_platform", (lead.evidence_json or {}).get("source_type")),
        )
    for lead in top_visible[:5]:
        logger.info(
            "[TOP_SCORE_BREAKDOWN] %s",
            {
                "title": lead.primary_title,
                "company": lead.company_name,
                "source": (lead.evidence_json or {}).get("source_platform", (lead.evidence_json or {}).get("source_type")),
                "rank_label": lead.rank_label,
                "confidence_label": lead.confidence_label,
                "qualification_fit_label": lead.qualification_fit_label,
                "score_breakdown_json": lead.score_breakdown_json or {},
            },
        )
    logger.info("[VISIBLE_TITLE_FIT_BUCKETS] %s", dict(title_fit_buckets))
    logger.info("[VISIBLE_QUALIFICATION_BUCKETS] %s", dict(qualification_buckets))
    logger.info("[VISIBLE_RANK_BUCKETS] %s", dict(rank_buckets))
    logger.info("[VISIBLE_SOURCE_BUCKETS] %s", dict(source_buckets))
    logger.info(
        "[TOP_JOBS_DEBUG] %s",
        [
            {
                "title": lead.primary_title,
                "company": lead.company_name,
                "score": recommendation_score_value(lead.score_breakdown_json),
                "fit": recommendation_component_value(lead.score_breakdown_json, "title_fit"),
                "source": (lead.evidence_json or {}).get("source_platform", (lead.evidence_json or {}).get("source_type")),
            }
            for lead in top_visible[:5]
        ],
    )
    top_visible = top_visible[:3]
    top_names = [f"{lead.company_name} / {lead.primary_title}" for lead in top_visible]
    summary = f"Ranker reprioritized the lead set. Top visible rows: {', '.join(top_names) if top_names else 'none'}."
    log_agent_activity(
        session,
        agent_name="Ranker",
        action="reprioritized leads",
        target_type="leads",
        target_count=len(leads),
        result_summary=summary,
    )
    log_agent_run(
        session,
        "Ranker",
        "reprioritized leads",
        summary,
        len(leads),
        metadata_json={"top_visible": top_names, "visible_count": visible},
    )
    return AgentRunResponse(agent="ranker", summary=summary)


def run_critic_agent(session: Session) -> AgentRunResponse:
    leads = session.scalars(select(Lead)).all()
    profile = get_candidate_profile(session)
    duplicate_losers = _duplicate_winner_context(session, leads)
    suppressed = 0
    kept = 0
    uncertain = 0
    for lead in leads:
        decision = apply_critic_decision_to_lead(session, lead, profile, duplicate_losers=duplicate_losers)
        if decision["visible"]:
            kept += 1
            reason = "kept visible after liveness and fit review"
        else:
            suppressed += 1
            if decision["status"] in {"uncertain", "investigation"}:
                uncertain += 1
            reason = f"{decision['status']} — {'; '.join(decision['reasons'])}"
        append_lead_agent_trace(lead, "Critic", reason, f"Critic {reason} for {lead.company_name} / {lead.primary_title}")
    suppressed_names = [f"{lead.company_name} / {lead.primary_title}" for lead in leads if lead.hidden][:4]
    summary = (
        f"Critic kept {kept} leads visible, suppressed {suppressed} leads, and marked {uncertain} as uncertain or investigative. "
        f"Suppressed: {', '.join(suppressed_names) if suppressed_names else 'none'}."
    )
    log_agent_activity(
        session,
        agent_name="Critic",
        action="suppressed weak or stale rows",
        target_type="leads",
        target_count=suppressed,
        result_summary=summary,
    )
    log_agent_run(
        session,
        "Critic",
        "suppressed weak or stale rows",
        summary,
        suppressed,
        metadata_json={"suppressed": suppressed_names, "visible_count": kept, "uncertain_count": uncertain},
    )
    return AgentRunResponse(agent="critic", summary=summary)


def run_tracker_agent(session: Session) -> AgentRunResponse:
    created = generate_follow_up_tasks(session)
    summary = f"Tracker reviewed applications and created {created} follow-up tasks."
    log_agent_activity(
        session,
        agent_name="Tracker",
        action="generated follow-up tasks",
        target_type="applications",
        target_count=created,
        result_summary=summary,
    )
    log_agent_run(
        session,
        "Tracker",
        "generated follow-up tasks",
        summary,
        created,
        metadata_json={"follow_up_count": created},
    )
    return AgentRunResponse(agent="tracker", summary=summary)


def run_query_evolution_agent(session: Session) -> AgentRunResponse:
    settings = get_settings()
    leads = session.scalars(select(Lead).where(Lead.hidden.is_(False))).all()
    added = 0
    added_values: list[str] = []
    for lead in leads[:5]:
        if added >= settings.learning_max_watchlist_additions_per_cycle:
            break
        evidence = lead.evidence_json or {}
        if evidence.get("company_domain"):
            changed = add_watchlist_item(
                session,
                item_type="company_domain",
                value=evidence["company_domain"],
                source_reason=f"Visible lead from {lead.company_name}",
                confidence="medium",
                status="proposed",
            )
            if changed:
                added += 1
                added_values.append(evidence["company_domain"])
    summary = f"Learning proposed {added} watchlist expansions from visible leads."
    log_agent_activity(
        session,
        agent_name="Learning",
        action="expanded watchlist",
        target_type="watchlist",
        target_count=added,
        result_summary=summary,
    )
    log_agent_run(
        session,
        "Learning",
        "expanded watchlist",
        summary,
        added,
        metadata_json={"watchlist_added": added_values},
    )
    return AgentRunResponse(agent="learning", summary=summary)


def run_full_pipeline(
    session: Session,
    source_mode: str = "demo",
    enabled_connectors: set[str] | None = None,
    strict_live_connectors: set[str] | None = None,
) -> AgentRunResponse:
    cycle_started_at = datetime.utcnow()
    hidden_before = {
        f"{lead.company_name} / {lead.primary_title}"
        for lead in session.scalars(select(Lead).where(Lead.hidden.is_(True))).all()
    }
    scout = run_scout_agent(
        session,
        source_mode=source_mode,
        enabled_connectors=enabled_connectors,
        strict_live_connectors=strict_live_connectors,
    )
    resolver = run_resolver_agent(
        session,
        resync=False if source_mode == "live" else True,
        enabled_connectors=enabled_connectors,
        strict_live_connectors=strict_live_connectors,
    )
    fit = run_fit_agent(session)
    ranker = run_ranker_agent(
        session,
        resync=False if source_mode == "live" else True,
        enabled_connectors=enabled_connectors,
        strict_live_connectors=strict_live_connectors,
    )
    critic = run_critic_agent(session)
    tracker = run_tracker_agent(session)
    learning = run_query_evolution_agent(session)
    governance = evaluate_learning_governance(session)
    final_jobs = session.scalars(
        select(Lead).where(Lead.hidden.is_(False)).order_by(Lead.updated_at.desc(), Lead.surfaced_at.desc()).limit(5)
    ).all()
    logger.info("[SURFACED_JOBS] count=%s", session.scalar(select(func.count(Lead.id)).where(Lead.hidden.is_(False))) or 0)
    logger.info("[TOP_JOBS] %s", [lead.primary_title for lead in final_jobs])
    summary = " | ".join(
        [
            scout.summary,
            resolver.summary,
            fit.summary,
            ranker.summary,
            critic.summary,
            tracker.summary,
            learning.summary,
            (
                "Governance promoted "
                f"{governance['promoted_queries'] + governance['promoted_watchlist']} items, "
                f"suppressed or rolled back "
                f"{governance['suppressed_queries'] + governance['suppressed_watchlist'] + governance['rolled_back_queries']} items, "
                f"and expired {governance['expired_queries'] + governance['expired_watchlist']} items."
            ),
        ]
    )
    new_rows = [
        f"{lead.company_name} / {lead.primary_title}"
        for lead in session.scalars(select(Lead).where(Lead.created_at >= cycle_started_at)).all()
        if (lead.evidence_json or {}).get("change_state") == "new"
    ][:8]
    hidden_after = {
        f"{lead.company_name} / {lead.primary_title}"
        for lead in session.scalars(select(Lead).where(Lead.hidden.is_(True))).all()
    }
    suppressed = list(hidden_after - hidden_before)[:8]
    follow_up_changes = [
        f"{task.application_id}:{task.task_type}"
        for task in session.scalars(select(FollowUpTask).where(FollowUpTask.created_at >= cycle_started_at)).all()
    ][:8]
    final_visible_all = session.scalars(select(Lead).where(Lead.hidden.is_(False))).all()
    final_source_buckets = Counter(
        (lead.evidence_json or {}).get("source_platform", (lead.evidence_json or {}).get("source_type", "unknown"))
        for lead in final_visible_all
    )
    logger.info("[FINAL_VISIBLE_SOURCE_MIX] %s", dict(final_source_buckets))
    latest_learning_run = session.scalar(
        select(AgentRun)
        .where(AgentRun.agent_name == "Learning", AgentRun.action == "expanded watchlist")
        .order_by(AgentRun.created_at.desc())
        .limit(1)
    )
    learning_added = ((latest_learning_run.metadata_json or {}).get("watchlist_added", []) if latest_learning_run else [])[:8]
    watchlist_changes = list(dict.fromkeys([*learning_added, *governance.get("changed_items", [])]))[:8]
    investigations_changed = session.scalar(
        select(func.count(Investigation.id)).where(Investigation.updated_at >= cycle_started_at)
    ) or 0
    log_agent_activity(
        session,
        agent_name="Pipeline",
        action="ran full pipeline",
        target_type="pipeline",
        target_count=7,
        result_summary=summary,
    )
    pipeline_run = log_agent_run(
        session,
        "Pipeline",
        "ran full pipeline",
        summary,
        7,
        metadata_json={
            "new_leads": new_rows,
            "suppressed_leads": suppressed,
            "investigations_changed": investigations_changed,
            "cycle_started_at": cycle_started_at.isoformat(),
        },
    )
    digest_summary = scout.summary
    if "No jobs found from any connector." in scout.summary:
        digest_summary = "No jobs found from any connector."
    elif "Jobs were discovered but all were filtered out before surfacing." in scout.summary:
        digest_summary = "Jobs were discovered but all were filtered out before surfacing."
    elif final_jobs:
        digest_summary = "Jobs found and surfaced normally."
    record_run_digest(
        session,
        agent_run=pipeline_run,
        summary=digest_summary if not any([new_rows, suppressed, investigations_changed, follow_up_changes, watchlist_changes]) else summary,
        new_leads=new_rows,
        suppressed_leads=suppressed,
        investigations_changed=investigations_changed,
        follow_ups_created=follow_up_changes,
        watchlist_changes=watchlist_changes,
    )
    return AgentRunResponse(agent="full_pipeline", summary=summary)
