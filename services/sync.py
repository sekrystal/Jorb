from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import partial
from time import perf_counter
from collections import defaultdict
from collections import Counter
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from connectors.ashby import AshbyConnector
from connectors.greenhouse import GreenhouseConnector
from connectors.search_web import (
    SearchDiscoveryConnector,
    build_search_queries,
    classify_query_family,
)
from connectors.x_search import XSearchConnector
from core.config import get_settings
from core.logging import get_logger
from core.models import Application, CompanyDiscovery, FollowUpTask, Investigation, Lead, Listing, RecheckQueue, Signal, SourceQuery, WatchlistItem
from core.schemas import ListingRecord, LeadResponse, SignalRecord, StatsResponse, SyncResult
from services.activity import append_lead_agent_trace, log_agent_activity, log_agent_run
from services.ai_judges import judge_critic_with_ai, judge_fit_with_ai
from services.company_discovery import (
    build_discovery_source_matrix,
    build_query_inputs,
    candidate_from_search_result,
    classify_surface_provenance,
    inspect_search_result_candidate,
    normalize_company_key,
    persist_discovery_lineage,
    record_expansion_attempt,
    select_candidates_for_expansion,
    source_lineage_for_surface,
    summarize_source_mix,
    triage_candidate,
    upsert_discovered_company,
)
from services.connectors_health import run_connector_fetch
from services.discovery_agents import extractor_agent, learning_agent, planner_agent, triage_agent
from services.explain import build_explanation
from services.extract_signal import extract_many
from services.freshness import classify_freshness_label, has_expired_pattern, validate_listing
from services.investigations import mark_investigation_attempt, upsert_investigation
from services.learning import generate_follow_up_tasks, increment_query_stat
from services.location_policy import classify_location_scope, is_location_allowed_for_profile
from services.normalize import normalize_ashby_job, normalize_greenhouse_job
from services.profile import get_candidate_profile
from services.query_learning import ensure_source_queries
from services.ranking import infer_role_family, score_lead
from services.resolve_company import get_or_create_company, queue_recheck, resolve_company_name


logger = get_logger(__name__)


def _bump_query_family_metric(
    metrics: dict[str, dict[str, int]],
    query_family: str | None,
    field: str,
    amount: int = 1,
) -> None:
    family = query_family or "unknown"
    metrics.setdefault(family, {})
    metrics[family][field] = int(metrics[family].get(field, 0)) + amount


def _source_learning(profile) -> dict:
    return (profile.extracted_summary_json or {}).get("learning", {})


def _ensure_utc_datetime(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _isoformat_utc(value):
    normalized = _ensure_utc_datetime(value)
    if not normalized:
        return None
    return normalized.isoformat().replace("+00:00", "Z")


def _verify_listing_record(record: ListingRecord) -> bool:
    if not record:
        return False
    if not record.url:
        return False
    external_id = (record.metadata_json or {}).get("internal_job_id")
    if not external_id:
        return False
    lowered_url = record.url.lower()
    if "greenhouse.io" not in lowered_url and "ashbyhq.com" not in lowered_url:
        return False
    return True


def _verify_signal_record(record: SignalRecord) -> bool:
    if not record:
        return False
    if not record.source_url:
        return False
    if not record.raw_text:
        return False
    return True


def _upsert_signal(session: Session, record: SignalRecord) -> Signal:
    existing = session.scalar(select(Signal).where(Signal.source_url == record.source_url))
    payload = record.model_dump(exclude={"metadata_json"})
    payload["signal_status"] = payload.get("signal_status") or "new"
    if existing:
        for key, value in payload.items():
            setattr(existing, key, value)
        return existing
    signal = Signal(**payload)
    session.add(signal)
    session.flush()
    return signal


def _upsert_listing(session: Session, record: ListingRecord, company_id: Optional[int]) -> tuple[Listing, bool]:
    existing = session.scalar(select(Listing).where(Listing.url == record.url))
    payload = record.model_dump()
    payload["company_id"] = company_id
    metadata = dict(payload.get("metadata_json") or {})
    if payload.get("company_domain"):
        metadata["company_domain"] = payload["company_domain"]
    if payload.get("careers_url"):
        metadata["careers_url"] = payload["careers_url"]
    payload["metadata_json"] = metadata
    payload.pop("company_domain", None)
    payload.pop("careers_url", None)
    if existing:
        material_changed = False
        for key, value in payload.items():
            if value is not None and getattr(existing, key) != value:
                setattr(existing, key, value)
                material_changed = True
        if existing.last_seen_at != payload.get("last_seen_at"):
            existing.last_seen_at = payload.get("last_seen_at") or datetime.now(timezone.utc)
            material_changed = True
        if material_changed:
            existing.updated_at = datetime.utcnow()
        return existing, False
    listing = Listing(**payload)
    session.add(listing)
    session.flush()
    return listing, True


def _query_stats_increment(session: Session, query_texts: list[str], delta: int = 1) -> None:
    for query_text in query_texts:
        item = session.scalar(
            select(SourceQuery).where(SourceQuery.query_text == query_text, SourceQuery.source_type == "x")
        )
        if not item:
            continue
        stats = dict(item.performance_stats_json or {})
        stats["leads_generated"] = stats.get("leads_generated", 0) + delta
        item.performance_stats_json = stats
        increment_query_stat(session, source_type="x", query_text=query_text, field_name="leads_generated", delta=delta)


def _matching_listing_for_signal(listings: list[Listing], signal: Signal) -> Optional[Listing]:
    for listing in listings:
        if listing.company_name.lower() != (signal.company_guess or "").lower():
            continue
        if listing.listing_status != "active":
            continue
        signal_role = (signal.role_guess or "").lower()
        if signal_role and signal_role in listing.title.lower():
            return listing
        if infer_role_family(listing.title, listing.description_text or "") == infer_role_family(signal.role_guess or "", signal.raw_text):
            return listing
    return None


def _authoritative_listing_context(session: Session, lead: Lead, listing_cache: Optional[dict[int, Listing]] = None) -> dict:
    evidence = dict(lead.evidence_json or {})
    listing = (listing_cache or {}).get(lead.listing_id) if lead.listing_id and listing_cache else None
    if listing is None and lead.listing_id:
        listing = session.get(Listing, lead.listing_id)
    page_text = ""
    http_status = None
    metadata = {}
    if listing:
        metadata = dict(listing.metadata_json or {})
        page_text = metadata.get("page_text", "")
        http_status = metadata.get("http_status")
    else:
        page_text = evidence.get("page_text", "")
        http_status = evidence.get("http_status")
    return {
        "listing": listing,
        "url": (listing.url if listing else evidence.get("url")),
        "posted_at": _ensure_utc_datetime(listing.posted_at) if listing else _ensure_utc_datetime(evidence.get("posted_at")),
        "first_published_at": _ensure_utc_datetime(listing.first_published_at) if listing else _ensure_utc_datetime(evidence.get("first_published_at")),
        "discovered_at": _ensure_utc_datetime(listing.discovered_at) if listing else _ensure_utc_datetime(evidence.get("discovered_at")),
        "last_seen_at": _ensure_utc_datetime(listing.last_seen_at) if listing else _ensure_utc_datetime(evidence.get("last_seen_at")),
        "updated_at": _ensure_utc_datetime(listing.updated_at) if listing else _ensure_utc_datetime(evidence.get("updated_at")),
        "freshness_hours": listing.freshness_hours if listing else evidence.get("freshness_hours"),
        "freshness_days": listing.freshness_days if listing else evidence.get("freshness_days"),
        "listing_status": listing.listing_status if listing else evidence.get("listing_status"),
        "expiration_confidence": listing.expiration_confidence if listing else evidence.get("expiration_confidence", 0.0),
        "description_text": listing.description_text if listing else "",
        "location": listing.location if listing else evidence.get("location"),
        "location_scope": metadata.get("location_scope") if listing else evidence.get("location_scope"),
        "metadata_json": metadata if listing else dict(evidence.get("listing_metadata_json") or {}),
        "page_text": page_text or "",
        "http_status": http_status,
    }


def _duplicate_winner_context(session: Session, leads: list[Lead], listing_cache: Optional[dict[int, Listing]] = None) -> dict[int, str]:
    freshness_order = {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}
    lead_type_order = {"combined": 0, "listing": 1, "signal": 2}
    duplicate_groups: dict[tuple, list[Lead]] = defaultdict(list)

    for lead in leads:
        context = _authoritative_listing_context(session, lead, listing_cache=listing_cache)
        dedupe_key = (
            lead.listing_id or None,
            (context["url"] or "").lower(),
            lead.company_name.lower(),
            lead.primary_title.lower(),
        )
        duplicate_groups[dedupe_key].append(lead)

    losers: dict[int, str] = {}
    for grouped in duplicate_groups.values():
        if len(grouped) <= 1:
            continue
        ordered = sorted(
            grouped,
            key=lambda lead: (
                lead_type_order.get(lead.lead_type, 9),
                freshness_order.get(lead.freshness_label, 9),
                -int(lead.updated_at.timestamp()) if lead.updated_at else 0,
                lead.id,
            ),
        )
        winner = ordered[0]
        for loser in ordered[1:]:
            losers[loser.id] = f"Duplicate of {winner.company_name} / {winner.primary_title}"
    return losers


def evaluate_critic_decision(
    session: Session,
    lead: Lead,
    profile,
    freshness_window_days: Optional[int] = 14,
    duplicate_losers: Optional[dict[int, str]] = None,
    listing_cache: Optional[dict[int, Listing]] = None,
    location_policy_evaluator=None,
    location_log_state: Optional[dict] = None,
    settings=None,
    ai_critic_runtime_state: Optional[dict] = None,
) -> dict:
    settings = settings or get_settings()
    duplicate_losers = duplicate_losers or {}
    context = _authoritative_listing_context(session, lead, listing_cache=listing_cache)
    url = context["url"]
    listing_status = context["listing_status"]
    freshness_days = context["freshness_days"]
    freshness_hours = context["freshness_hours"]
    expiration_confidence = context["expiration_confidence"] or 0.0
    page_text = context["page_text"]
    description_text = context["description_text"]
    http_status = context["http_status"]
    last_seen_at = context["last_seen_at"]
    location = context["location"]
    location_policy = (
        location_policy_evaluator(lead, location)
        if location_policy_evaluator
        else is_location_allowed_for_profile(profile, location, settings=settings)
    )
    reasons: list[str] = []
    status = "visible"
    suppression_category = "none"
    persisted_ai_critic = dict((lead.evidence_json or {}).get("ai_critic_assessment") or {}) or None
    ai_critic = persisted_ai_critic
    used_live_ai_call = False

    if lead.id in duplicate_losers:
        reasons.append(duplicate_losers[lead.id])
        status = "suppressed"
        suppression_category = "duplicate"

    if lead.lead_type in {"listing", "combined"}:
        if not context["listing"]:
            reasons.append("Missing backing listing record")
            status = "suppressed"
            suppression_category = "non_live"
        if not url or not str(url).startswith("http"):
            reasons.append("Missing or invalid job URL")
            status = "suppressed"
            suppression_category = "broken"
        if http_status in {404, 410}:
            reasons.append(f"Job page returned HTTP {http_status}")
            status = "suppressed"
            suppression_category = "broken"
        if has_expired_pattern(description_text, page_text):
            reasons.append("Expired text pattern detected in job content")
            status = "suppressed"
            suppression_category = "expired"
        if last_seen_at is None:
            reasons.append("Listing has no last-seen timestamp from connector fetches")
            status = "uncertain"
            suppression_category = "uncertain"
        else:
            age_since_seen_hours = max((datetime.now(timezone.utc) - last_seen_at).total_seconds() / 3600, 0.0)
            if age_since_seen_hours > 48:
                reasons.append(f"Listing has not been seen in {round(age_since_seen_hours, 1)} hours")
                status = "suppressed"
                suppression_category = "non_live"
        if listing_status in {"expired", "suspected_expired"}:
            reasons.append(f"Listing status is {listing_status}")
            status = "suppressed"
            suppression_category = "expired"
        elif listing_status != "active" and status == "visible":
            if freshness_hours is not None and freshness_hours <= 72 and expiration_confidence < 0.2:
                reasons.append("Listing is recent but liveness is still uncertain")
                status = "uncertain"
                suppression_category = "uncertain"
            else:
                reasons.append(f"Listing status is {listing_status or 'unknown'}")
                status = "uncertain"
                suppression_category = "uncertain"
        if freshness_hours is None and status == "visible":
            reasons.append("No reliable posted date found")
            status = "uncertain"
            suppression_category = "uncertain"
        elif freshness_hours is not None and freshness_window_days is not None and freshness_hours > freshness_window_days * 24:
            reasons.append(f"Freshness exceeded the default {freshness_window_days}-day window")
            status = "suppressed"
            suppression_category = "stale"
        if not location_policy["allowed"] and status == "visible":
            location_payload = {
                "company": lead.company_name,
                "title": lead.primary_title,
                "location": location,
                "scope": location_policy["scope"],
                "status": location_policy["status"],
                "reason": location_policy["reason"],
            }
            if location_log_state is not None:
                event_key = (
                    lead.company_name,
                    lead.primary_title,
                    location,
                    location_policy["scope"],
                    location_policy["status"],
                    location_policy["reason"],
                )
                if event_key not in location_log_state["seen"]:
                    location_log_state["seen"].add(event_key)
                    location_log_state["emitted_count"] += 1
                    logger.info("[LOCATION_GATE] %s", location_payload)
                else:
                    location_log_state["suppressed_duplicate_count"] += 1
            else:
                logger.info("[LOCATION_GATE] %s", location_payload)
            reasons.append(location_policy["reason"])
            if location_policy["status"] == "blocked":
                status = "suppressed"
                suppression_category = "location"
            else:
                status = "uncertain"
                suppression_category = "location"
        if lead.confidence_label == "low" and status == "visible":
            reasons.append("Confidence is too low for default surfaced listings")
            status = "uncertain"
            suppression_category = "uncertain"
        if (
            settings.enable_ai_readtime_critic
            and ai_critic is None
            and ai_critic_runtime_state is not None
            and ai_critic_runtime_state.get("remaining", 0) > 0
        ):
            ai_critic_runtime_state["remaining"] -= 1
            ai_critic = judge_critic_with_ai(
                title=lead.primary_title,
                company_name=lead.company_name,
                description_text=description_text,
                listing_status=listing_status,
                freshness_days=freshness_days,
                page_text=page_text,
                url=url,
            )
            used_live_ai_call = True
        logger.info(
            "[READTIME_AI_CRITIC] %s",
            {
                "enabled": settings.enable_ai_readtime_critic,
                "used_persisted_assessment": persisted_ai_critic is not None,
                "lead_id": lead.id,
                "company": lead.company_name,
                "title": lead.primary_title,
                "used_live_call": used_live_ai_call,
            },
        )
        if ai_critic and status == "visible" and ai_critic.get("quality_assessment") in {"uncertain", "stale", "suppress"}:
            reasons.append(f"AI critic flagged: {'; '.join(ai_critic.get('reasons', []))}")
            status = "uncertain"
            suppression_category = "uncertain"

    if lead.qualification_fit_label in {"underqualified", "overqualified"} and status == "visible":
        reasons.append(f"Qualification fit is {lead.qualification_fit_label}")
        status = "hidden"
        suppression_category = "qualification"

    if (lead.score_breakdown_json or {}).get("composite", 0.0) < profile.minimum_fit_threshold and status == "visible":
        reasons.append("Composite fit is below the candidate threshold")
        status = "hidden"
        suppression_category = "low_fit"

    if lead.company_name.lower() in [item.lower() for item in (profile.excluded_companies_json or [])] and status == "visible":
        reasons.append("Company is muted in the candidate profile")
        status = "hidden"
        suppression_category = "user_suppressed"

    if lead.lead_type == "signal":
        signal = session.get(Signal, lead.signal_id) if lead.signal_id else None
        if signal and signal.signal_status in {"needs_recheck", "resolved_no_listing"} and status == "visible":
            status = "investigation"
            reasons = reasons or ["Weak signal is under investigation without a confirmed active listing"]
            suppression_category = "investigation"
        elif status == "visible":
            status = "uncertain"
            reasons = reasons or ["Signal-only lead requires explicit opt-in"]
            suppression_category = "uncertain"

    if status == "visible":
        reasons = ["Passed freshness, liveness, duplicate, and qualification gates"]

    return {
        "status": status,
        "visible": status == "visible",
        "reasons": reasons,
        "suppression_category": suppression_category,
        "listing": context["listing"],
        "authoritative_url": url,
        "listing_status": listing_status,
        "freshness_hours": freshness_hours,
        "freshness_days": freshness_days,
        "posted_at": context["posted_at"],
        "first_published_at": context["first_published_at"],
        "discovered_at": context["discovered_at"],
        "last_seen_at": context["last_seen_at"],
        "updated_at": context["updated_at"],
        "liveness_evidence": {
            "listing_status": listing_status,
            "freshness_hours": freshness_hours,
            "freshness_days": freshness_days,
            "expiration_confidence": round(expiration_confidence, 2),
            "http_status": http_status,
            "expired_pattern_detected": has_expired_pattern(description_text, page_text),
            "location": location,
            "location_scope": location_policy["scope"],
            "location_allowed": location_policy["allowed"],
            "location_reason": location_policy["reason"],
            "first_published_at": context["first_published_at"],
            "discovered_at": context["discovered_at"],
            "last_seen_at": context["last_seen_at"],
            "updated_at": context["updated_at"],
        },
        "ai_critic_assessment": ai_critic,
        "location_policy": location_policy,
    }


def apply_critic_decision_to_lead(
    session: Session,
    lead: Lead,
    profile,
    freshness_window_days: Optional[int] = 14,
    duplicate_losers: Optional[dict[int, str]] = None,
) -> dict:
    decision = evaluate_critic_decision(
        session=session,
        lead=lead,
        profile=profile,
        freshness_window_days=freshness_window_days,
        duplicate_losers=duplicate_losers,
    )
    evidence = dict(lead.evidence_json or {})
    evidence["critic_status"] = decision["status"]
    evidence["critic_reasons"] = decision["reasons"]
    evidence["suppression_reason"] = "; ".join(decision["reasons"]) if decision["status"] != "visible" else None
    evidence["suppression_category"] = decision["suppression_category"]
    evidence["liveness_evidence"] = decision["liveness_evidence"]
    evidence["ai_critic_assessment"] = decision.get("ai_critic_assessment")
    evidence["listing_status"] = decision["listing_status"]
    evidence["freshness_hours"] = decision["freshness_hours"]
    evidence["freshness_days"] = decision["freshness_days"]
    evidence["url"] = decision["authoritative_url"]
    evidence["posted_at"] = _isoformat_utc(decision["posted_at"])
    evidence["first_published_at"] = _isoformat_utc(decision["first_published_at"])
    evidence["discovered_at"] = _isoformat_utc(decision["discovered_at"])
    evidence["last_seen_at"] = _isoformat_utc(decision["last_seen_at"])
    evidence["updated_at"] = _isoformat_utc(decision["updated_at"])
    evidence["location_scope"] = decision["location_policy"]["scope"]
    evidence["location_allowed"] = decision["location_policy"]["allowed"]
    evidence["location_reason"] = decision["location_policy"]["reason"]
    liveness = dict(evidence.get("liveness_evidence") or {})
    for key in ("first_published_at", "discovered_at", "last_seen_at", "updated_at"):
        liveness[key] = _isoformat_utc(liveness.get(key))
    evidence["liveness_evidence"] = liveness
    lead.evidence_json = evidence
    lead.hidden = not decision["visible"]
    return decision


def _upsert_lead(
    session: Session,
    lead_type: str,
    company_name: str,
    company_id: Optional[int],
    title: str,
    listing: Optional[Listing],
    signal: Optional[Signal],
    profile,
    listing_url: Optional[str],
    source_type: str,
    company_domain: Optional[str],
    location: Optional[str],
    description_text: str,
    listing_status: Optional[str],
    freshness_label: str,
    evidence_json: dict,
    ai_fit_runtime_state: Optional[dict[str, int]] = None,
) -> tuple[Lead, bool]:
    existing = session.scalar(
        select(Lead).where(
            Lead.lead_type == lead_type,
            Lead.company_name == company_name,
            Lead.primary_title == title,
            Lead.listing_id == (listing.id if listing else None),
            Lead.signal_id == (signal.id if signal else None),
        )
    )
    feedback_learning = _source_learning(profile)
    breakdown = score_lead(
        profile=profile,
        lead_type=lead_type,
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        location=location,
        description_text=description_text,
        freshness_label=freshness_label,
        listing_status=listing_status,
        source_type=source_type,
        evidence_count=len(evidence_json.get("snippets", [])),
        feedback_learning=feedback_learning,
    )
    candidate_context = profile.raw_resume_text or (profile.extracted_summary_json or {}).get("summary", "")
    existing_ai_fit = None
    if existing and existing.evidence_json:
        existing_ai_fit = (existing.evidence_json or {}).get("ai_fit_assessment")
    ai_fit = existing_ai_fit
    if ai_fit is None:
        remaining_ai_fit_calls = None
        if ai_fit_runtime_state is not None:
            remaining_ai_fit_calls = ai_fit_runtime_state.get("remaining", 0)
        if remaining_ai_fit_calls is None or remaining_ai_fit_calls > 0:
            ai_fit = judge_fit_with_ai(
                profile_text=candidate_context,
                title=title,
                company_name=company_name,
                location=location,
                description_text=description_text,
            )
            if ai_fit_runtime_state is not None and ai_fit_runtime_state.get("remaining", 0) > 0:
                ai_fit_runtime_state["remaining"] -= 1
        else:
            logger.info(
                "[AI_FIT_BUDGET] %s",
                {
                    "title": title,
                    "company": company_name,
                    "source_type": source_type,
                    "remaining": 0,
                    "used_persisted_assessment": bool(existing_ai_fit),
                },
            )
    displayed_fit_label = breakdown["qualification_fit_label"]
    if ai_fit:
        displayed_fit_label = {
            "strong_fit": "strong fit",
            "adjacent": "adjacent",
            "stretch": "stretch",
            "underqualified": "underqualified",
            "overqualified": "overqualified",
            "unclear": "unclear",
        }.get(ai_fit.get("classification"), displayed_fit_label)
        if displayed_fit_label != breakdown["qualification_fit_label"]:
            logger.info(
                "[AI_FIT_LABEL_CHANGE] %s",
                {
                    "title": title,
                    "company": company_name,
                    "deterministic_label": breakdown["qualification_fit_label"],
                    "ai_label": displayed_fit_label,
                    "source_type": source_type,
                },
            )
        ai_matched_fields = ai_fit.get("matched_profile_fields", [])
        if ai_matched_fields:
            breakdown["matched_profile_fields"] = list(
                dict.fromkeys(breakdown.get("matched_profile_fields", []) + ai_matched_fields)
            )

    feedback_notes = (profile.extracted_summary_json or {}).get("learning", {}).get("feedback_notes", [])[-3:]
    uncertainty = None
    if lead_type == "signal":
        uncertainty = "Signal exists without a confirmed active listing yet"
    elif listing_status != "active":
        uncertainty = f"Listing status is {listing_status}"

    explanation = build_explanation(
        lead_type=lead_type,
        matched_profile_fields=breakdown.get("matched_profile_fields", []),
        feedback_notes=feedback_notes,
        freshness_label=breakdown["freshness_label"],
        confidence_label=breakdown["confidence_label"],
        candidate_context=candidate_context[:1000] if candidate_context else None,
        fit_assessment=ai_fit,
        uncertainty=uncertainty,
    )

    score_breakdown = {key: value for key, value in breakdown.items() if key not in {"matched_profile_fields"}}
    evidence_json = dict(evidence_json)
    discovery_source = evidence_json.get("discovery_source") or ((listing.metadata_json or {}).get("discovery_source") if listing else None)
    source_platform = "x_demo" if source_type == "x" else source_type
    if discovery_source:
        source_platform = f"{source_type}+{discovery_source}"
    location_classification = classify_location_scope(location)
    evidence_json.update(
        {
            "matched_profile_fields": breakdown.get("matched_profile_fields", []),
            "feedback_notes": feedback_notes,
            "freshness_status": freshness_label,
            "freshness_hours": listing.freshness_hours if listing else 0.0,
            "freshness_days": listing.freshness_days if listing else 0,
            "confidence_status": breakdown["confidence_label"],
            "listing_status": listing_status,
            "source_type": source_type,
            "source_platform": source_platform,
            "discovery_source": discovery_source,
            "source_provenance": (listing.metadata_json or {}).get("surface_provenance") if listing else evidence_json.get("source_provenance"),
            "source_lineage": (listing.metadata_json or {}).get("source_lineage") if listing else evidence_json.get("source_lineage", source_platform),
            "company_domain": company_domain,
            "location": location,
            "location_scope": (listing.metadata_json or {}).get("location_scope") if listing else location_classification["scope"],
            "location_reason": (listing.metadata_json or {}).get("location_reason") if listing else location_classification["reason"],
            "target_roles": breakdown.get("target_roles", []),
            "work_mode_preference": breakdown.get("work_mode_preference"),
            "work_mode_match": breakdown.get("work_mode_match"),
            "profile_constraints_applied": breakdown.get("applied_profile_constraints", []),
            "profile_constraints_defaulted": breakdown.get("defaulted_profile_constraints", []),
            "listing_metadata_json": dict(listing.metadata_json or {}) if listing else {},
            "url": listing_url,
            "first_published_at": listing.first_published_at.isoformat() if listing and listing.first_published_at else None,
            "discovered_at": listing.discovered_at.isoformat() if listing and listing.discovered_at else None,
            "last_seen_at": listing.last_seen_at.isoformat() if listing and listing.last_seen_at else None,
            "ai_fit_assessment": ai_fit,
        }
    )
    if lead_type == "combined" and signal and listing:
        evidence_json["resolution_story"] = [
            f"Weak hiring signal found from {signal.source_type}",
            f"Company guess resolved to {company_name}",
            f"Fresh active listing found via {listing.source_type}",
            "Signal and listing merged into one surfaced lead",
        ]
    elif lead_type == "signal":
        evidence_json["resolution_story"] = [
            "Weak hiring signal found",
            "No active listing confirmed yet",
        ]

    payload = {
        "lead_type": lead_type,
        "company_name": company_name,
        "company_id": company_id,
        "primary_title": title,
        "listing_id": listing.id if listing else None,
        "signal_id": signal.id if signal else None,
        "rank_label": breakdown["rank_label"],
        "confidence_label": breakdown["confidence_label"],
        "freshness_label": breakdown["freshness_label"],
        "title_fit_label": breakdown["title_fit_label"],
        "qualification_fit_label": displayed_fit_label,
        "explanation": explanation,
        "score_breakdown_json": score_breakdown,
        "evidence_json": evidence_json,
        "last_agent_action": "Resolver: surfaced lead",
        "hidden": False,
    }

    if existing:
        material_changed = False
        for key, value in payload.items():
            if getattr(existing, key) != value:
                setattr(existing, key, value)
                material_changed = True
        apply_critic_decision_to_lead(session, existing, profile)
        if material_changed:
            existing.updated_at = datetime.utcnow()
            append_lead_agent_trace(existing, "Resolver", "surfaced lead", f"Resolver refreshed {company_name} / {title}", change_state="updated")
        return existing, False

    lead = Lead(**payload)
    session.add(lead)
    session.flush()
    apply_critic_decision_to_lead(session, lead, profile)
    append_lead_agent_trace(lead, "Resolver", "surfaced lead", f"Resolver surfaced {company_name} / {title}", change_state="new")
    return lead, True


def sync_all(
    session: Session,
    include_rechecks: bool = True,
    enabled_connectors: set[str] | None = None,
    strict_live_connectors: set[str] | None = None,
) -> SyncResult:
    settings = get_settings()
    profile = get_candidate_profile(session)
    queries = ensure_source_queries(session)
    enabled_connectors = enabled_connectors or {"greenhouse", "ashby", "x_search"}
    strict_live_connectors = strict_live_connectors or set()

    greenhouse_connector = GreenhouseConnector()
    ashby_connector = AshbyConnector()
    search_connector = SearchDiscoveryConnector()
    x_connector = XSearchConnector()

    greenhouse_jobs: list[dict] = []
    ashby_jobs: list[dict] = []
    search_results = []
    x_raw_signals: list[dict] = []
    greenhouse_live = False
    ashby_live = False
    search_live = False
    x_live = False
    discovered_greenhouse_queries: dict[str, list[str]] = {}
    discovered_ashby_queries: dict[str, list[str]] = {}
    discovery_metrics: dict[str, dict[str, int]] = {}
    discovery_status: dict[str, object] = {}
    cycle_metrics: Counter[str] = Counter()
    query_family_metrics: dict[str, dict[str, int]] = {}
    ai_fit_runtime_state = {"remaining": settings.ai_fit_max_calls_per_cycle}

    watchlist_values = [
        item.value
        for item in session.scalars(
            select(WatchlistItem).where(WatchlistItem.status.in_(["active", "proposed"])).order_by(WatchlistItem.updated_at.desc())
        ).all()
    ]
    planner_plan = planner_agent(session, profile, settings=settings)
    query_inputs = build_query_inputs(session, profile)
    search_queries = planner_plan["queries"][: settings.discovery_max_search_queries_per_cycle]
    for query_text in search_queries:
        _bump_query_family_metric(query_family_metrics, classify_query_family(query_text), "queries_attempted")
    logger.info(
        "[PLANNER_PLAN] %s",
        {
            "count": len(search_queries),
            "examples": search_queries[:6],
            "query_themes": planner_plan.get("query_themes", []),
            "role_clusters": planner_plan.get("role_clusters", []),
            "company_archetypes": planner_plan.get("company_archetypes", []),
            "priority_notes": planner_plan.get("priority_notes", []),
        },
    )
    logger.info("[QUERY_DIVERSIFICATION] %s", {"queries": search_queries[:10]})
    logger.info(
        "[AI_FIT_BUDGET] %s",
        {
            "max_calls_per_cycle": settings.ai_fit_max_calls_per_cycle,
            "remaining": ai_fit_runtime_state["remaining"],
        },
    )
    log_agent_activity(
        session,
        agent_name="Planner",
        action="planned discovery cycle",
        target_type="queries",
        target_count=len(search_queries),
        result_summary=f"Planner prepared {len(search_queries)} discovery queries across {len(planner_plan.get('query_themes', [])) or len(search_queries)} themes.",
    )
    log_agent_run(
        session,
        "Planner",
        "planned discovery cycle",
        f"Planner prepared {len(search_queries)} discovery queries and prioritized {len(planner_plan.get('company_archetypes', []))} company archetypes.",
        len(search_queries),
        metadata_json=planner_plan,
    )

    configured_board_keys = {
        *{f"greenhouse:{token}" for token in settings.greenhouse_tokens},
        *{f"ashby:{org}" for org in settings.ashby_orgs},
    }
    selected_discoveries: list[tuple] = []
    selected_discovery_rows_by_key: dict[str, CompanyDiscovery] = {}
    discovery_rows_touched: list[CompanyDiscovery] = []
    new_discovery_count = 0

    if settings.search_discovery_enabled and search_queries:
        search_results, search_live, _ = run_connector_fetch(
            session,
            "search_web",
            partial(search_connector.fetch, search_queries, "search_web" in strict_live_connectors),
            date_fields=[],
        )
        extractions, derived_results = extractor_agent(search_results, settings=settings)
        extraction_by_url: dict[str, object] = {}
        for extraction in extractions:
            extraction_by_url[extraction.source_url] = extraction
            extraction_by_url[extraction.final_url] = extraction
            for derived in derived_results:
                if any(token and token in derived.url for token in [*extraction.greenhouse_tokens, *extraction.ashby_identifiers]):
                    extraction_by_url.setdefault(derived.url, extraction)
        if derived_results:
            deduped_results: list[SearchDiscoveryResult] = []
            seen_result_urls: set[str] = set()
            for result in [*search_results, *derived_results]:
                if result.url in seen_result_urls:
                    continue
                seen_result_urls.add(result.url)
                deduped_results.append(result)
            search_results = deduped_results
        logger.info(
            "[EXTRACTOR_DISCOVERY] %s",
            {
                "pages_crawled": len(extractions),
                "derived_ats_results": len(derived_results),
                "greenhouse_tokens": sorted({token for item in extractions for token in item.greenhouse_tokens})[:12],
                "ashby_identifiers": sorted({org for item in extractions for org in item.ashby_identifiers})[:12],
            },
        )
        if extractions:
            new_greenhouse_tokens = {
                token
                for item in extractions
                for token in item.greenhouse_tokens
                if token.lower() not in {configured.lower() for configured in settings.greenhouse_tokens}
            }
            new_ashby_identifiers = {
                org
                for item in extractions
                for org in item.ashby_identifiers
                if org.lower() not in {configured.lower() for configured in settings.ashby_orgs}
            }
            cycle_metrics["discovered_greenhouse_tokens_new_count"] += len(new_greenhouse_tokens)
            cycle_metrics["discovered_ashby_identifiers_new_count"] += len(new_ashby_identifiers)
            logger.info(
                "[GREENHOUSE_TOKEN_DISCOVERY] %s",
                {
                    "count": sum(1 for item in extractions if item.greenhouse_tokens),
                    "tokens": sorted({token for item in extractions for token in item.greenhouse_tokens})[:12],
                    "new_tokens": sorted(new_greenhouse_tokens)[:12],
                },
            )
            logger.info(
                "[ASHBY_IDENTIFIER_DISCOVERY] %s",
                {
                    "count": sum(1 for item in extractions if item.ashby_identifiers),
                    "identifiers": sorted({org for item in extractions for org in item.ashby_identifiers})[:12],
                    "new_identifiers": sorted(new_ashby_identifiers)[:12],
                },
            )
        logger.info(
            "[DISCOVERY_RESULTS] %s",
            {
                "raw_results": len(search_results),
                "surface_mix": dict(Counter(result.source_surface for result in search_results)),
            },
        )
        candidate_rows_by_key: dict[str, tuple] = {}
        dropped_results: list[dict[str, str]] = []
        convertible_candidate_count = 0
        logger.info(
            "[DISCOVERY_ACCEPTED_RESULTS] %s",
            {
                "accepted_results_input_count": len(search_results),
                "accepted_urls": [result.url for result in search_results[:10]],
            },
        )
        for result in search_results:
            inspection = inspect_search_result_candidate(result)
            candidate = candidate_from_search_result(result)
            if not candidate:
                _bump_query_family_metric(query_family_metrics, result.query_family, "dropped_results")
                dropped_results.append(
                    {
                        "url": inspection["normalized_url"] or result.url,
                        "reason": inspection["reason"] or "candidate_none",
                    }
                )
                logger.info(
                    "[DISCOVERY_CANDIDATE_DROP] %s",
                    {
                        "url": inspection["normalized_url"] or result.url,
                        "host": inspection["host"],
                        "path": inspection["path"],
                        "reason": inspection["reason"] or "candidate_none",
                        "board_type": inspection["board_type"],
                        "board_locator": inspection["board_locator"],
                    },
                )
                continue
            convertible_candidate_count += 1
            _bump_query_family_metric(query_family_metrics, candidate.query_family, "accepted_results")
            logger.info(
                "[DISCOVERY_CANDIDATE_CONVERSION] %s",
                {
                    "url": candidate.result_url,
                    "host": inspection["host"],
                    "path": inspection["path"],
                    "board_type": candidate.board_type,
                    "board_locator": candidate.board_locator,
                    "query_family": candidate.query_family,
                    "reason": "converted",
                },
            )
            extraction = extraction_by_url.get(result.url)
            if extraction:
                if extraction.company_name:
                    candidate.company_name = extraction.company_name
                    candidate.normalized_company_key = normalize_company_key(extraction.company_name, candidate.company_domain)
                if extraction.ats_type in {"greenhouse", "ashby", "careers_page"}:
                    candidate.board_type = extraction.ats_type
            triage_score, triage_reasons, triage_decision = triage_agent(
                session=session,
                profile=profile,
                candidate=candidate,
                configured_boards=configured_board_keys,
                settings=settings,
            )
            row, is_new = upsert_discovered_company(session, candidate, triage_score, triage_reasons)
            surface_provenance = classify_surface_provenance(
                candidate.board_type,
                candidate.board_locator,
                is_new=is_new,
                settings=settings,
            )
            source_lineage = source_lineage_for_surface(candidate.board_type, surface_provenance, candidate.discovery_source)
            row.utility_score = max(row.utility_score, triage_score)
            extra_metadata = {}
            if extraction:
                extra_metadata = {
                    "careers_url": extraction.careers_url,
                    "ats_type_hypothesis": extraction.ats_type,
                    "greenhouse_tokens": extraction.greenhouse_tokens,
                    "ashby_identifiers": extraction.ashby_identifiers,
                    "geography_hints": extraction.geography_hints,
                    "discovered_urls": extraction.discovered_urls[:10],
                    "extraction_confidence": extraction.confidence,
                    "extraction_used_openai": extraction.via_openai,
                }
            row.metadata_json = {
                **(row.metadata_json or {}),
                "query_family": candidate.query_family,
                "triage_decision": triage_decision,
                "surface_provenance": surface_provenance,
                "source_lineage": source_lineage,
                "triage_used_openai": any(reason.startswith("ai:") for reason in triage_reasons),
                **extra_metadata,
            }
            persist_discovery_lineage(
                row,
                query_family=candidate.query_family,
                discovery_query=candidate.discovery_query,
                surface_provenance=surface_provenance,
                source_lineage=source_lineage,
            )
            discovery_rows_touched.append(row)
            _bump_query_family_metric(query_family_metrics, candidate.query_family, "candidate_conversions")
            candidate.is_new = is_new
            if is_new:
                new_discovery_count += 1
                cycle_metrics["discovered_companies_new_count"] += 1
                logger.info(
                    "[NEW_COMPANY_DISCOVERED] %s",
                    {
                        "company": candidate.company_name,
                        "board_type": candidate.board_type,
                        "board_locator": candidate.board_locator,
                        "query_family": candidate.query_family,
                        "surface_provenance": surface_provenance,
                        "source_lineage": source_lineage,
                        "query": candidate.discovery_query,
                        "score": triage_score,
                        "decision": triage_decision,
                    },
                )
            else:
                logger.info(
                    "[DISCOVERY_DEDUPE] %s",
                    {
                        "company": candidate.company_name,
                        "board_type": candidate.board_type,
                        "board_locator": candidate.board_locator,
                        "query_family": candidate.query_family,
                        "surface_provenance": surface_provenance,
                        "source_lineage": source_lineage,
                        "query": candidate.discovery_query,
                        "score": triage_score,
                        "decision": triage_decision,
                        "existing_status": row.expansion_status,
                        "existing_utility": row.utility_score,
                    },
                )
            logger.info(
                "[DISCOVERY_PROVENANCE] %s",
                {
                    "company": candidate.company_name,
                    "board_type": candidate.board_type,
                    "board_locator": candidate.board_locator,
                    "surface_provenance": surface_provenance,
                    "source_lineage": source_lineage,
                    "is_new_company_row": is_new,
                },
            )
            existing_candidate = candidate_rows_by_key.get(candidate.discovery_key)
            if triage_decision == "drop":
                continue
            if existing_candidate is None or triage_score > existing_candidate[2]:
                if existing_candidate is not None:
                    logger.info(
                        "[DISCOVERY_DEDUPE] %s",
                        {
                            "discovery_key": candidate.discovery_key,
                            "kept_company": candidate.company_name,
                            "replaced_lower_score": existing_candidate[2],
                            "kept_score": triage_score,
                        },
                    )
                candidate_rows_by_key[candidate.discovery_key] = (candidate, row, triage_score, triage_reasons)
        candidate_rows = list(candidate_rows_by_key.values())
        cycle_metrics["accepted_results_input_count"] = len(search_results)
        cycle_metrics["accepted_results_count"] = len(search_results)
        cycle_metrics["convertible_candidate_count"] = convertible_candidate_count
        cycle_metrics["candidate_conversion_success_count"] = convertible_candidate_count
        cycle_metrics["candidate_count"] = len(candidate_rows)
        cycle_metrics["dropped_result_count"] = max(len(search_results) - convertible_candidate_count, 0)
        cycle_metrics["candidate_conversion_drop_count"] = max(len(search_results) - convertible_candidate_count, 0)
        cycle_metrics["accepted_urls_sample"] = [result.url for result in search_results[:10]]
        cycle_metrics["dropped_urls_sample"] = [item["url"] for item in dropped_results[:10]]
        selected_discoveries = select_candidates_for_expansion(candidate_rows, settings=settings)
        cycle_metrics["selected_expansion_count"] = len(selected_discoveries)
        cycle_metrics["empty_expansion_count"] += 0
        cycle_metrics["listings_yielded_count"] += 0
        for candidate, _, _, _ in selected_discoveries:
            _bump_query_family_metric(query_family_metrics, candidate.query_family, "selected_for_expansion")
            _bump_query_family_metric(query_family_metrics, candidate.query_family, f"{candidate.board_type}_selected")
        selected_discovery_rows_by_key = {row.discovery_key: row for _, row, _, _ in selected_discoveries}
        for candidate, row, score, reasons in selected_discoveries:
            provenance = (row.metadata_json or {}).get("surface_provenance")
            logger.info(
                "[EXPANSION_SELECTED] %s",
                {
                    "company": candidate.company_name,
                    "board_type": candidate.board_type,
                    "board_locator": candidate.board_locator,
                    "query_family": candidate.query_family,
                    "surface_provenance": provenance,
                    "source_lineage": (row.metadata_json or {}).get("source_lineage"),
                    "score": score,
                    "reasons": reasons,
                    "is_new": candidate.is_new,
                    "status": row.expansion_status,
                },
            )
            if provenance in {"discovered_existing", "discovered_new"}:
                cycle_metrics[f"agent_discovered_{candidate.board_type}_expansion_attempts"] += 1
            if candidate.board_type == "ashby":
                logger.info(
                    "[ASHBY_EXPANSION_INPUT] %s",
                    {
                        "company": candidate.company_name,
                        "board_locator": candidate.board_locator,
                        "surface_provenance": provenance,
                        "source_lineage": (row.metadata_json or {}).get("source_lineage"),
                    },
                )
        discovered_greenhouse_queries = {
            candidate.board_locator: [candidate.discovery_query]
            for candidate, _, _, _ in selected_discoveries
            if candidate.board_type == "greenhouse"
        }
        discovered_ashby_queries = {
            candidate.board_locator: [candidate.discovery_query]
            for candidate, _, _, _ in selected_discoveries
            if candidate.board_type == "ashby"
        }
        logger.info("[ASHBY_DISCOVERY] %s", {"orgs": list(discovered_ashby_queries), "count": len(discovered_ashby_queries)})
        logger.info(
            "[DISCOVERED_COMPANIES] %s",
            {
                "search_results": len(search_results),
                "candidate_companies": len(candidate_rows),
                "new_companies": new_discovery_count,
                "known_companies": max(len(candidate_rows) - new_discovery_count, 0),
            },
        )
        logger.info(
            "[COMPANY_TRIAGE] %s",
            [
                {
                    "company": candidate.company_name,
                    "board_type": candidate.board_type,
                    "board_locator": candidate.board_locator,
                    "score": score,
                    "reasons": reasons,
                    "is_new": candidate.is_new,
                    "decision": (row.metadata_json or {}).get("triage_decision", "pursue"),
                }
                for candidate, _, score, reasons in sorted(candidate_rows, key=lambda item: item[2], reverse=True)[:8]
            ],
        )
        logger.info(
            "[DISCOVERY_BUDGET] %s",
            {
                "max_search_queries_per_cycle": settings.discovery_max_search_queries_per_cycle,
                "max_new_companies_per_cycle": settings.discovery_max_new_companies_per_cycle,
                "max_expansions_per_cycle": settings.discovery_max_expansions_per_cycle,
                "selected_for_expansion": [
                    f"{candidate.board_type}:{candidate.board_locator}" for candidate, _, _, _ in selected_discoveries
                ],
            },
        )
        log_agent_activity(
            session,
            agent_name="Discovery",
            action="searched new companies and sources",
            target_type="search_results",
            target_count=len(search_results),
            result_summary=f"Discovery found {len(search_results)} search results and {len(candidate_rows)} candidate companies.",
        )
        log_agent_run(
            session,
            "Discovery",
            "searched new companies and sources",
            f"Discovery found {len(search_results)} search results and {len(candidate_rows)} triaged company candidates.",
            len(search_results),
            metadata_json={
                "queries": search_queries,
                "search_result_count": len(search_results),
                "candidate_count": len(candidate_rows),
                "new_company_count": new_discovery_count,
            },
        )
        log_agent_activity(
            session,
            agent_name="Triage",
            action="prioritized discovery candidates",
            target_type="companies",
            target_count=len(selected_discoveries),
            result_summary=f"Triage selected {len(selected_discoveries)} companies/boards for expansion.",
        )
        log_agent_run(
            session,
            "Triage",
            "prioritized discovery candidates",
            f"Triage selected {len(selected_discoveries)} expansion targets out of {len(candidate_rows)} candidates.",
            len(selected_discoveries),
            metadata_json={
                "selected": [f"{candidate.board_type}:{candidate.board_locator}" for candidate, _, _, _ in selected_discoveries],
                "candidate_count": len(candidate_rows),
                "used_openai": any((row.metadata_json or {}).get("triage_used_openai") for _, row, _, _ in candidate_rows),
            },
        )
    search_verified_count = sum(
        1
        for result in search_results
        if result.url and ("greenhouse.io" in result.url.lower() or "ashbyhq.com" in result.url.lower())
    )
    discovery_metrics["search_web"] = {
        "raw": len(search_results),
        "normalized": len(search_results),
        "verified": search_verified_count,
    }

    if "greenhouse" in enabled_connectors:
        greenhouse_tokens = list(dict.fromkeys(settings.greenhouse_tokens + list(discovered_greenhouse_queries)))
        greenhouse_jobs, greenhouse_live, _ = run_connector_fetch(
            session,
            "greenhouse",
            partial(
                greenhouse_connector.fetch,
                "greenhouse" in strict_live_connectors,
                greenhouse_tokens,
                discovered_greenhouse_queries,
            ),
            date_fields=["first_published", "updated_at"],
        )
    if "ashby" in enabled_connectors:
        ashby_orgs = list(dict.fromkeys(settings.ashby_orgs + list(discovered_ashby_queries)))
        logger.info(
            "[ASHBY_DISCOVERY_BRIDGE] %s",
            {
                "configured_orgs": settings.ashby_orgs,
                "discovered_orgs": list(discovered_ashby_queries),
                "requested_orgs": ashby_orgs,
            },
        )
        ashby_jobs, ashby_live, _ = run_connector_fetch(
            session,
            "ashby",
            partial(
                ashby_connector.fetch,
                "ashby" in strict_live_connectors,
                ashby_orgs,
                discovered_ashby_queries,
            ),
            date_fields=["publishedDate"],
        )
    greenhouse_job_counts = dict(getattr(greenhouse_connector, "last_board_counts", {}) or {})
    ashby_job_counts = Counter(job.get("source_org_key") for job in ashby_jobs if job.get("source_org_key"))
    ashby_org_statuses = dict(getattr(ashby_connector, "last_org_statuses", {}) or {})
    configured_greenhouse_tokens = {token.lower() for token in settings.greenhouse_tokens}
    configured_ashby_orgs = {org.lower() for org in settings.ashby_orgs}
    for job in greenhouse_jobs:
        token = (job.get("source_board_token") or "").lower()
        discovery_key = f"greenhouse:{token}" if token else None
        selected_row = selected_discovery_rows_by_key.get(discovery_key) if discovery_key else None
        provenance = "preseeded" if token in configured_greenhouse_tokens else "discovered_existing"
        if selected_row is not None:
            provenance = (selected_row.metadata_json or {}).get("surface_provenance", provenance)
        job["surface_provenance"] = provenance
        job["source_lineage"] = source_lineage_for_surface("greenhouse", provenance, job.get("discovery_source"))
    for job in ashby_jobs:
        org = (job.get("source_org_key") or "").lower()
        discovery_key = f"ashby:{org}" if org else None
        selected_row = selected_discovery_rows_by_key.get(discovery_key) if discovery_key else None
        provenance = "preseeded" if org in configured_ashby_orgs else "discovered_existing"
        if selected_row is not None:
            provenance = (selected_row.metadata_json or {}).get("surface_provenance", provenance)
        job["surface_provenance"] = provenance
        job["source_lineage"] = source_lineage_for_surface("ashby", provenance, job.get("discovery_source"))
    for candidate, row, score, reasons in selected_discoveries:
        result_count = (
            greenhouse_job_counts.get(candidate.board_locator, 0)
            if candidate.board_type == "greenhouse"
            else int(ashby_job_counts.get(candidate.board_locator, 0))
        )
        blocked_reason = "investigate" if candidate.board_type == "careers_page" else None
        record_expansion_attempt(row, result_count=result_count, blocked_reason=blocked_reason)
        expansion_diagnostics = {
            "stage": "expansion_outcome",
            "selected": True,
            "selected_score": score,
            "selected_reasons": reasons,
            "result_count": result_count,
            "status": row.expansion_status,
            "blocked_reason": blocked_reason,
            "empty_surface": result_count == 0,
            "yielded_listings": result_count,
            "failure_boundary": "connector_yield" if result_count == 0 else None,
        }
        if candidate.board_type == "ashby":
            ashby_status = ashby_org_statuses.get(candidate.board_locator.lower())
            expansion_diagnostics["surface_status"] = ashby_status or "unknown"
            if ashby_status == "invalid_identifier":
                expansion_diagnostics["failure_boundary"] = "invalid_discovered_surface"
            elif ashby_status == "valid_identifier_empty_jobs":
                expansion_diagnostics["failure_boundary"] = "empty_discovered_surface"
        elif candidate.board_type == "greenhouse":
            expansion_diagnostics["surface_status"] = "jobs_returned" if result_count > 0 else "empty_jobs"
        row.metadata_json = {
            **(row.metadata_json or {}),
            "selected_score": score,
            "selected_reasons": reasons,
            "selected_this_cycle_at": datetime.utcnow().isoformat(),
            "expansion_diagnostics": expansion_diagnostics,
        }
        persist_discovery_lineage(
            row,
            selected=True,
            selected_score=score,
            selected_reasons=reasons,
            result_count=result_count,
            expansion_status=row.expansion_status,
            blocked_reason=blocked_reason,
            failure_boundary=expansion_diagnostics.get("failure_boundary"),
            surface_status=expansion_diagnostics.get("surface_status"),
        )
        provenance = (row.metadata_json or {}).get("surface_provenance")
        logger.info(
            "[COMPANY_EXPANSION] %s",
            {
                "company": candidate.company_name,
                "board_type": candidate.board_type,
                "board_locator": candidate.board_locator,
                "surface_provenance": provenance,
                "result_count": result_count,
                "score": score,
            },
        )
        logger.info(
            "[EXPANSION_RESULT] %s",
            {
                "company": candidate.company_name,
                "board_type": candidate.board_type,
                "board_locator": candidate.board_locator,
                "query_family": candidate.query_family,
                "surface_provenance": provenance,
                "source_lineage": (row.metadata_json or {}).get("source_lineage"),
                "result_count": result_count,
                "blocked_reason": blocked_reason,
                "status": row.expansion_status,
            },
        )
        cycle_metrics[f"{candidate.board_type}_expansion_attempts"] += 1
        _bump_query_family_metric(query_family_metrics, candidate.query_family, "expansion_attempts")
        if result_count > 0:
            cycle_metrics[f"{candidate.board_type}_expansion_successes"] += 1
            cycle_metrics[f"{candidate.board_type}_listings_yielded"] += result_count
            cycle_metrics["listings_yielded_count"] += result_count
            _bump_query_family_metric(query_family_metrics, candidate.query_family, "expansions_with_listings")
            _bump_query_family_metric(query_family_metrics, candidate.query_family, "listings_yielded", result_count)
        else:
            cycle_metrics[f"{candidate.board_type}_empty_expansions"] += 1
            cycle_metrics["empty_expansion_count"] += 1
            _bump_query_family_metric(query_family_metrics, candidate.query_family, "empty_expansions")
        if provenance in {"discovered_existing", "discovered_new"} and result_count > 0:
            cycle_metrics[f"agent_discovered_{candidate.board_type}_expansion_successes"] += 1
        logger.info(
            "[EXPANSION_ACTION] %s",
            {
                "action": f"expand_{candidate.board_type}",
                "board_locator": candidate.board_locator,
                "company": candidate.company_name,
                "result_count": result_count,
            },
        )
    if "x_search" in enabled_connectors:
        x_raw_signals, x_live, _ = run_connector_fetch(
            session,
            "x_search",
            partial(x_connector.fetch, queries, "x_search" in strict_live_connectors),
            date_fields=["published_at"],
        )

    greenhouse_normalized = [normalize_greenhouse_job(job) for job in greenhouse_jobs]
    greenhouse_verified = [validate_listing(record) for record in greenhouse_normalized if _verify_listing_record(record)]
    cycle_metrics["agent_discovered_listings_count"] += sum(
        1
        for record in greenhouse_normalized
        if (record.metadata_json or {}).get("surface_provenance") in {"discovered_existing", "discovered_new"}
    )
    cycle_metrics["agent_discovered_verified_listings_count"] += sum(
        1
        for record in greenhouse_verified
        if (record.metadata_json or {}).get("surface_provenance") in {"discovered_existing", "discovered_new"}
    )
    discovery_metrics["greenhouse"] = {
        "raw": len(greenhouse_jobs),
        "normalized": len(greenhouse_normalized),
        "verified": len(greenhouse_verified),
    }
    logger.info(
        "[VERIFICATION] connector=greenhouse before=%s after=%s",
        len(greenhouse_normalized),
        len(greenhouse_verified),
    )

    ashby_normalized = [normalize_ashby_job(job, job.get("companyName")) for job in ashby_jobs]
    ashby_verified = [validate_listing(record) for record in ashby_normalized if _verify_listing_record(record)]
    cycle_metrics["agent_discovered_listings_count"] += sum(
        1
        for record in ashby_normalized
        if (record.metadata_json or {}).get("surface_provenance") in {"discovered_existing", "discovered_new"}
    )
    cycle_metrics["agent_discovered_verified_listings_count"] += sum(
        1
        for record in ashby_verified
        if (record.metadata_json or {}).get("surface_provenance") in {"discovered_existing", "discovered_new"}
    )
    discovery_metrics["ashby"] = {
        "raw": len(ashby_jobs),
        "normalized": len(ashby_normalized),
        "verified": len(ashby_verified),
    }
    logger.info(
        "[VERIFICATION] connector=ashby before=%s after=%s",
        len(ashby_normalized),
        len(ashby_verified),
    )

    signals_ingested = 0
    listings_ingested = 0
    leads_created = 0
    leads_updated = 0
    rechecks_queued = 0
    investigations_opened = 0

    extracted_signals = extract_many(x_raw_signals)
    verified_signals = [raw for raw in extracted_signals if _verify_signal_record(raw)]
    discovery_metrics["x_search"] = {
        "raw": len(x_raw_signals),
        "normalized": len(extracted_signals),
        "verified": len(verified_signals),
    }
    logger.info(
        "[VERIFICATION] connector=x_search before=%s after=%s",
        len(extracted_signals),
        len(verified_signals),
    )
    logger.info("[DISCOVERY_METRICS] %s", discovery_metrics)

    signal_objects: list[Signal] = []
    for raw in verified_signals:
        if raw.published_at and isinstance(raw.published_at, str):
            raw.published_at = datetime.fromisoformat(raw.published_at.replace("Z", "+00:00"))
        signal = _upsert_signal(session, raw)
        signals_ingested += 1
        signal_objects.append(signal)

    listing_records = list(greenhouse_verified)
    listing_records.extend(ashby_verified)

    listing_objects: list[Listing] = []
    for record in listing_records:
        resolved_company = resolve_company_name(session, record.company_name, record.description_text or "") or record.company_name
        company = get_or_create_company(
            session,
            name=resolved_company,
            domain=record.company_domain,
            careers_url=record.careers_url,
            ats_provider=record.source_type,
        )
        record.company_name = company.name
        listing, _ = _upsert_listing(session, record, company.id)
        listing_objects.append(listing)
        listings_ingested += 1

    discovery_yield_counts: dict[str, Counter[str]] = defaultdict(Counter)

    used_listing_ids: set[int] = set()
    for signal in signal_objects:
        resolved_company = resolve_company_name(session, signal.company_guess, signal.raw_text)
        if resolved_company:
            signal.company_guess = resolved_company
        matching_listing = _matching_listing_for_signal(listing_objects, signal) if resolved_company else None

        if matching_listing:
            used_listing_ids.add(matching_listing.id)
            lead, created = _upsert_lead(
                session=session,
                lead_type="combined",
                company_name=matching_listing.company_name,
                company_id=matching_listing.company_id,
                title=matching_listing.title,
                listing=matching_listing,
                signal=signal,
                profile=profile,
                listing_url=matching_listing.url,
                source_type=matching_listing.source_type,
                company_domain=(matching_listing.metadata_json or {}).get("company_domain"),
                location=matching_listing.location,
                description_text=f"{matching_listing.description_text or ''}\n{signal.raw_text}",
                listing_status=matching_listing.listing_status,
                freshness_label=classify_freshness_label(matching_listing.freshness_days, matching_listing.freshness_hours),
                evidence_json={
                    "snippets": [signal.raw_text[:220], (matching_listing.description_text or "")[:220]],
                    "source_queries": [
                        item.get("query_text")
                        for item in x_raw_signals
                        if item["url"] == signal.source_url and item.get("query_text")
                    ],
                },
                ai_fit_runtime_state=ai_fit_runtime_state,
            )
            leads_created += 1 if created else 0
            leads_updated += 0 if created else 1
            if (matching_listing.metadata_json or {}).get("discovery_source") == "search_web":
                discovery_key = None
                if matching_listing.source_type == "greenhouse" and (matching_listing.metadata_json or {}).get("source_board_token"):
                    discovery_key = f"greenhouse:{matching_listing.metadata_json['source_board_token']}"
                elif matching_listing.source_type == "ashby" and (matching_listing.metadata_json or {}).get("source_org_key"):
                    discovery_key = f"ashby:{matching_listing.metadata_json['source_org_key']}"
                if discovery_key:
                    discovery_yield_counts[discovery_key]["visible" if not lead.hidden else "suppressed"] += 1
                    if (lead.evidence_json or {}).get("suppression_category") == "location":
                        discovery_yield_counts[discovery_key]["location"] += 1
                        cycle_metrics["geography_rejected_discovered_surfaces_count"] += 1
                        logger.info(
                            "[DISCOVERY_LOCATION_REJECT] %s",
                            {
                                "company": matching_listing.company_name,
                                "title": matching_listing.title,
                                "board_type": matching_listing.source_type,
                                "discovery_key": discovery_key,
                                "surface_provenance": (matching_listing.metadata_json or {}).get("surface_provenance"),
                                "source_lineage": (matching_listing.metadata_json or {}).get("source_lineage"),
                                "location": matching_listing.location,
                                "location_scope": (lead.evidence_json or {}).get("location_scope"),
                                "reason": (lead.evidence_json or {}).get("location_reason"),
                            },
                        )
            query_text = next((item.get("query_text") for item in x_raw_signals if item["url"] == signal.source_url), None)
            if query_text:
                _query_stats_increment(session, [query_text])
            signal.signal_status = "matched_to_listing"
            upsert_investigation(
                session,
                signal=signal,
                status="resolved",
                confidence=signal.hiring_confidence,
                note=f"Resolved to active listing at {matching_listing.company_name}.",
            )
            continue

        if resolved_company:
            company = get_or_create_company(session, name=resolved_company, ats_provider="x")
            query_text = next((item.get("query_text") for item in x_raw_signals if item["url"] == signal.source_url), None)
            lead, created = _upsert_lead(
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
                freshness_label=classify_freshness_label(0),
                evidence_json={
                    "snippets": [signal.raw_text[:220]],
                    "source_queries": [query_text] if query_text else [],
                },
                ai_fit_runtime_state=ai_fit_runtime_state,
            )
            leads_created += 1 if created else 0
            leads_updated += 0 if created else 1
            if query_text:
                _query_stats_increment(session, [query_text])
            signal.signal_status = "resolved_no_listing"
            existing_investigation = session.scalar(select(Investigation).where(Investigation.signal_id == signal.id))
            if existing_investigation or investigations_opened < settings.max_investigations_opened_per_cycle:
                upsert_investigation(
                    session,
                    signal=signal,
                    status="open",
                    confidence=signal.hiring_confidence,
                    note="Promising weak signal without a confirmed active listing yet.",
                    next_check_at=datetime.utcnow() + timedelta(hours=6),
                )
                if not existing_investigation:
                    investigations_opened += 1
        else:
            signal.signal_status = "needs_recheck"
            queue_recheck(session, "signal", signal.id, "Unresolved weak signal without confident company resolution")
            rechecks_queued += 1
            existing_investigation = session.scalar(select(Investigation).where(Investigation.signal_id == signal.id))
            if existing_investigation or investigations_opened < settings.max_investigations_opened_per_cycle:
                upsert_investigation(
                    session,
                    signal=signal,
                    status="open",
                    confidence=signal.hiring_confidence,
                    note="Could not confidently resolve the company yet. Recheck queued.",
                    next_check_at=datetime.utcnow() + timedelta(hours=6),
                )
                if not existing_investigation:
                    investigations_opened += 1

    for listing in listing_objects:
        if listing.id in used_listing_ids:
            continue
        company = get_or_create_company(session, name=listing.company_name, ats_provider=listing.source_type)
        query_texts = listing.metadata_json.get("source_queries", []) if listing.metadata_json else []
        lead, created = _upsert_lead(
            session=session,
            lead_type="listing",
            company_name=listing.company_name,
            company_id=company.id,
            title=listing.title,
            listing=listing,
            signal=None,
            profile=profile,
            listing_url=listing.url,
            source_type=listing.source_type,
            company_domain=company.domain,
            location=listing.location,
            description_text=listing.description_text or "",
            listing_status=listing.listing_status,
            freshness_label=classify_freshness_label(listing.freshness_days, listing.freshness_hours),
            evidence_json={
                "snippets": [(listing.description_text or "")[:240]],
                "source_queries": query_texts,
                "discovery_source": (listing.metadata_json or {}).get("discovery_source"),
            },
            ai_fit_runtime_state=ai_fit_runtime_state,
        )
        leads_created += 1 if created else 0
        leads_updated += 0 if created else 1
        if (listing.metadata_json or {}).get("discovery_source") == "search_web":
            discovery_key = None
            if listing.source_type == "greenhouse" and (listing.metadata_json or {}).get("source_board_token"):
                discovery_key = f"greenhouse:{listing.metadata_json['source_board_token']}"
            elif listing.source_type == "ashby" and (listing.metadata_json or {}).get("source_org_key"):
                discovery_key = f"ashby:{listing.metadata_json['source_org_key']}"
            if discovery_key:
                discovery_yield_counts[discovery_key]["visible" if not lead.hidden else "suppressed"] += 1
                if (lead.evidence_json or {}).get("suppression_category") == "location":
                    discovery_yield_counts[discovery_key]["location"] += 1
                    cycle_metrics["geography_rejected_discovered_surfaces_count"] += 1
                    logger.info(
                        "[DISCOVERY_LOCATION_REJECT] %s",
                        {
                            "company": listing.company_name,
                            "title": listing.title,
                            "board_type": listing.source_type,
                            "discovery_key": discovery_key,
                            "surface_provenance": (listing.metadata_json or {}).get("surface_provenance"),
                            "source_lineage": (listing.metadata_json or {}).get("source_lineage"),
                            "location": listing.location,
                            "location_scope": (lead.evidence_json or {}).get("location_scope"),
                            "reason": (lead.evidence_json or {}).get("location_reason"),
                        },
                    )
        if query_texts:
            _query_stats_increment(session, query_texts)

    if include_rechecks:
        due_items = session.scalars(
            select(RecheckQueue).where(
                RecheckQueue.status.in_(["queued", "retrying"]),
                RecheckQueue.next_check_at <= datetime.utcnow(),
            )
        ).all()
        for item in due_items:
            item.status = "retrying" if item.retry_count < 2 else "exhausted"
            item.retry_count += 1
            if item.entity_type == "signal":
                mark_investigation_attempt(
                    session,
                    signal_id=item.entity_id,
                    note=f"Automatic resolver recheck attempt {item.retry_count} ran.",
                )

    generate_follow_up_tasks(session)
    for discovery_key, row in selected_discovery_rows_by_key.items():
        counts = discovery_yield_counts.get(discovery_key, Counter())
        query_family = (row.metadata_json or {}).get("query_family")
        if (row.metadata_json or {}).get("surface_provenance") in {"discovered_existing", "discovered_new"}:
            cycle_metrics["agent_discovered_visible_leads_count"] += counts.get("visible", 0)
        record_expansion_attempt(
            row,
            result_count=row.last_expansion_result_count,
            visible_yield=counts.get("visible", 0),
            suppressed_yield=counts.get("suppressed", 0),
            location_filtered=counts.get("location", 0),
            count_attempt=False,
        )
        _bump_query_family_metric(query_family_metrics, query_family, "visible_yield_count", counts.get("visible", 0))
        _bump_query_family_metric(query_family_metrics, query_family, "suppressed_yield_count", counts.get("suppressed", 0))
        _bump_query_family_metric(query_family_metrics, query_family, "location_filtered_count", counts.get("location", 0))
        if counts.get("visible", 0) > 0:
            _bump_query_family_metric(query_family_metrics, query_family, "expansions_with_visible_yield")
        else:
            _bump_query_family_metric(query_family_metrics, query_family, "zero_visible_yield_expansions")
        logger.info(
            "[NEW_COMPANY_VISIBLE_YIELD] %s",
            {
                "company": row.company_name,
                "board_type": row.board_type,
                "board_locator": row.board_locator,
                "surface_provenance": (row.metadata_json or {}).get("surface_provenance"),
                "source_lineage": (row.metadata_json or {}).get("source_lineage"),
                "visible": counts.get("visible", 0),
                "suppressed": counts.get("suppressed", 0),
                "location_filtered": counts.get("location", 0),
                "utility_score": row.utility_score,
            },
        )
    learning_update = learning_agent(session, profile, settings=settings)
    cycle_metrics["ai_fit_calls_remaining"] = ai_fit_runtime_state["remaining"]
    cycle_metrics["ai_fit_calls_used"] = max(settings.ai_fit_max_calls_per_cycle - ai_fit_runtime_state["remaining"], 0)
    cycle_metrics["query_family_metrics"] = query_family_metrics
    logger.info(
        "[LEARNING_UPDATE] %s",
        {
            "positive_companies": learning_update.get("positive_companies", []),
            "negative_companies": learning_update.get("negative_companies", []),
            "next_queries": learning_update.get("next_queries", [])[:6],
        },
    )
    logger.info(
        "[NEXT_CYCLE_RECOMMENDATIONS] %s",
        {
            "focus_companies": learning_update.get("focus_companies", []),
            "notes": learning_update.get("notes", []),
        },
    )
    logger.info(
        "[PLANNER_NEXT_STEPS] %s",
        {
            "next_queries": learning_update.get("next_queries", [])[:8],
            "focus_companies": learning_update.get("focus_companies", [])[:8],
            "notes": learning_update.get("notes", [])[:6],
        },
    )
    log_agent_activity(
        session,
        agent_name="Learning",
        action="updated discovery priors",
        target_type="queries",
        target_count=len(learning_update.get("next_queries", [])),
        result_summary=f"Learning proposed {len(learning_update.get('next_queries', []))} next-cycle discovery queries.",
    )
    log_agent_run(
        session,
        "Learning",
        "updated discovery priors",
        f"Learning proposed {len(learning_update.get('next_queries', []))} next-cycle queries and highlighted {len(learning_update.get('focus_companies', []))} focus companies.",
        len(learning_update.get("next_queries", [])),
        metadata_json=learning_update,
    )
    logger.info("[DISCOVERY_CYCLE_METRICS] %s", dict(cycle_metrics))
    log_agent_run(
        session,
        "Discovery",
        "recorded discovery cycle metrics",
        "Discovery cycle metrics recorded for agent-discovered ATS provenance and yield.",
        int(cycle_metrics.get("agent_discovered_visible_leads_count", 0)),
        metadata_json={"cycle_metrics": dict(cycle_metrics)},
    )
    session.flush()
    surfaced_count = session.scalar(select(func.count(Lead.id)).where(Lead.hidden.is_(False))) or 0
    source_matrix = [row.model_dump() for row in build_discovery_source_matrix(
        session,
        settings=settings,
        enabled_connectors=enabled_connectors,
        strict_live_connectors=strict_live_connectors,
    )]
    unavailable_automatic_sources = [
        row["label"]
        for row in source_matrix
        if row["source_key"] in {"greenhouse", "ashby", "search_web", "search_web_scrape_fallback", "x_search"}
        and row["classification"] == "not_working"
    ]
    runnable_automatic_sources = [
        row["label"]
        for row in source_matrix
        if row["source_key"] in {"greenhouse", "ashby", "search_web", "search_web_scrape_fallback", "x_search"}
        and row["classification"] in {"working", "partially_working"}
    ]
    discovery_summary = None
    if (
        discovery_metrics.get("greenhouse", {}).get("verified", 0) == 0
        and discovery_metrics.get("ashby", {}).get("verified", 0) == 0
        and discovery_metrics.get("search_web", {}).get("raw", 0) > 0
        and surfaced_count == 0
    ):
        discovery_summary = "Jobs were discovered but all were filtered out before surfacing."
        logger.error("[DISCOVERY_FAILURE] No high-signal jobs. Only weak signals found and filtered out.")
    elif all(item.get("raw", 0) == 0 for item in discovery_metrics.values()):
        if not runnable_automatic_sources:
            discovery_summary = (
                "No jobs found from any connector. "
                f"Automatic discovery is not runnable: {', '.join(unavailable_automatic_sources)}."
            )
        elif unavailable_automatic_sources:
            discovery_summary = (
                "No jobs found from any connector. "
                f"Unavailable sources this cycle: {', '.join(unavailable_automatic_sources)}."
            )
        else:
            discovery_summary = "No jobs found from any connector."
        logger.error("[DISCOVERY_FAILURE] No jobs found from any source.")
    elif surfaced_count > 0:
        discovery_summary = "Jobs found and surfaced normally."
    discovery_status = {
        "new_companies_discovered": new_discovery_count,
        "companies_selected_for_expansion": len(selected_discoveries),
        "selected_source_mix": summarize_source_mix(list(selected_discovery_rows_by_key.values())),
        "selected_companies": [row.company_name for row in selected_discovery_rows_by_key.values()],
        "next_recommended_queries": learning_update.get("next_queries", []),
        "focus_companies": learning_update.get("focus_companies", []),
        "cycle_metrics": dict(cycle_metrics),
        "source_matrix": source_matrix,
        "unavailable_sources": unavailable_automatic_sources,
    }
    return SyncResult(
        signals_ingested=signals_ingested,
        listings_ingested=listings_ingested,
        leads_created=leads_created,
        leads_updated=leads_updated,
        rechecks_queued=rechecks_queued,
        live_mode_used=any([greenhouse_live, ashby_live, search_live, x_live]),
        discovery_metrics=discovery_metrics,
        surfaced_count=surfaced_count,
        discovery_summary=discovery_summary,
        discovery_status=discovery_status,
    )


def list_leads(
    session: Session,
    freshness_window_days: Optional[int] = 14,
    include_hidden: bool = False,
    include_unqualified: bool = False,
    lead_type: Optional[str] = None,
    only_saved: bool = False,
    only_applied: bool = False,
    status: Optional[str] = None,
    include_signal_only: bool = False,
) -> list[LeadResponse]:
    started_at = perf_counter()
    settings = get_settings()
    rank_order = {"strong": 0, "medium": 1, "weak": 2}
    freshness_order = {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}
    lead_type_order = {"combined": 0, "listing": 1, "signal": 2}

    def recency_value(item: LeadResponse) -> float:
        reference = item.posted_at or item.surfaced_at
        return -reference.timestamp() if reference else 0.0

    logger.info(
        "[LEADS_REQUEST_START] %s",
        {
            "freshness_window_days": freshness_window_days,
            "include_hidden": include_hidden,
            "include_unqualified": include_unqualified,
            "include_signal_only": include_signal_only,
            "only_saved": only_saved,
            "only_applied": only_applied,
        },
    )

    def _log_stage_timing(stage: str, stage_started_at: float, rows_seen: int) -> None:
        logger.info(
            "[LEADS_STAGE_TIMING] %s",
            {
                "stage": stage,
                "elapsed_ms": round((perf_counter() - stage_started_at) * 1000, 2),
                "rows_seen": rows_seen,
            },
        )

    db_fetch_ms = 0.0
    critic_filter_ms = 0.0
    serialization_ms = 0.0
    total_considered = 0
    items: list[LeadResponse] = []
    omitted_by_status: Counter[str] = Counter()
    omitted_by_category: Counter[str] = Counter()
    location_cache: dict[tuple, dict] = {}
    location_gate_stats = {"total_calls": 0, "cache_hits": 0, "cache_misses": 0}
    location_log_state = {"seen": set(), "emitted_count": 0, "suppressed_duplicate_count": 0}
    critic_stage_started_at: Optional[float] = None
    serialization_stage_started_at: Optional[float] = None

    try:
        db_started_at = perf_counter()
        records = session.scalars(select(Lead).order_by(Lead.surfaced_at.desc(), Lead.rank_label.asc())).all()
        lead_ids = [lead.id for lead in records]
        listing_ids = [lead.listing_id for lead in records if lead.listing_id]
        applications = (
            session.scalars(select(Application).where(Application.lead_id.in_(lead_ids))).all()
            if lead_ids
            else []
        )
        application_ids = [application.id for application in applications]
        follow_up_tasks = (
            session.scalars(
                select(FollowUpTask)
                .where(FollowUpTask.application_id.in_(application_ids), FollowUpTask.status == "open")
                .order_by(FollowUpTask.application_id.asc(), FollowUpTask.due_at.asc())
            ).all()
            if application_ids
            else []
        )
        listing_cache = {
            listing.id: listing
            for listing in (
                session.scalars(select(Listing).where(Listing.id.in_(listing_ids))).all()
                if listing_ids
                else []
            )
        }
        profile = get_candidate_profile(session)
        application_by_lead_id = {application.lead_id: application for application in applications}
        follow_up_by_application_id: dict[int, FollowUpTask] = {}
        for task in follow_up_tasks:
            follow_up_by_application_id.setdefault(task.application_id, task)
        db_fetch_ms = round((perf_counter() - db_started_at) * 1000, 2)
        _log_stage_timing("db_fetch", db_started_at, len(records))

        prefilter_started_at = perf_counter()
        candidate_records: list[Lead] = []
        for lead in records:
            application = application_by_lead_id.get(lead.id)
            saved = application is not None and application.date_saved is not None
            applied = application is not None and application.date_applied is not None
            current_status = application.current_status if application else None

            if only_saved and not saved:
                continue
            if only_applied and not applied:
                continue
            if status and current_status != status:
                continue
            if lead_type and lead.lead_type != lead_type:
                omitted_by_status["lead_type_filtered"] += 1
                continue
            if lead.lead_type == "signal" and lead_type != "signal" and not include_signal_only:
                omitted_by_status["signal_only_filtered"] += 1
                continue
            candidate_records.append(lead)
        _log_stage_timing("cheap_prefilter", prefilter_started_at, len(candidate_records))

        duplicate_started_at = perf_counter()
        duplicate_losers = _duplicate_winner_context(session, candidate_records, listing_cache=listing_cache)
        _log_stage_timing("duplicate_detection", duplicate_started_at, len(candidate_records))

        def _evaluate_location_policy(lead: Lead, location: Optional[str]) -> dict:
            cache_key = (
                lead.company_name.strip().lower(),
                lead.primary_title.strip().lower(),
                (location or "").strip().lower(),
            )
            location_gate_stats["total_calls"] += 1
            if cache_key in location_cache:
                location_gate_stats["cache_hits"] += 1
                return location_cache[cache_key]
            location_gate_stats["cache_misses"] += 1
            location_cache[cache_key] = is_location_allowed_for_profile(profile, location, settings=settings)
            return location_cache[cache_key]

        ai_critic_runtime_state = {"remaining": 5 if settings.enable_ai_readtime_critic else 0}
        critic_stage_started_at = perf_counter()
        for lead in candidate_records:
            total_considered += 1
            critic_started = perf_counter()
            evidence = lead.evidence_json or {}
            decision = evaluate_critic_decision(
                session=session,
                lead=lead,
                profile=profile,
                freshness_window_days=freshness_window_days,
                duplicate_losers=duplicate_losers,
                listing_cache=listing_cache,
                location_policy_evaluator=_evaluate_location_policy,
                location_log_state=location_log_state,
                settings=settings,
                ai_critic_runtime_state=ai_critic_runtime_state,
            )
            authoritative_listing = decision.get("listing")
            freshness_days = decision["freshness_days"]
            freshness_hours = decision["freshness_hours"]
            listing_status = decision["listing_status"]
            source_type = evidence.get("source_type", lead.lead_type)
            application = application_by_lead_id.get(lead.id)
            saved = application is not None and application.date_saved is not None
            applied = application is not None and application.date_applied is not None
            current_status = application.current_status if application else None

            if not include_hidden and not decision["visible"]:
                if lead.lead_type == "signal" and include_signal_only and decision["status"] in {"uncertain", "investigation"}:
                    pass
                elif include_unqualified and decision["suppression_category"] == "qualification":
                    pass
                else:
                    omitted_by_status[decision["status"]] += 1
                    omitted_by_category[decision["suppression_category"]] += 1
                    critic_filter_ms += (perf_counter() - critic_started) * 1000
                    continue
            if freshness_window_days is not None and freshness_hours is not None and freshness_hours > freshness_window_days * 24:
                omitted_by_status["freshness_window_filtered"] += 1
                omitted_by_category["stale"] += 1
                critic_filter_ms += (perf_counter() - critic_started) * 1000
                continue
            if application:
                follow_up_task = follow_up_by_application_id.get(application.id)
                next_action = follow_up_task.notes or "Follow up on this application." if follow_up_task else None
                follow_up_due = bool(follow_up_task and follow_up_task.due_at <= datetime.utcnow())
            else:
                next_action, follow_up_due = None, False
            critic_filter_ms += (perf_counter() - critic_started) * 1000

            if serialization_stage_started_at is None:
                serialization_stage_started_at = perf_counter()
            serialization_started = perf_counter()
            response_evidence = dict(evidence)
            listing_metadata = dict((authoritative_listing.metadata_json or {})) if authoritative_listing else {}
            response_evidence["discovery_source"] = response_evidence.get("discovery_source") or listing_metadata.get("discovery_source")
            response_evidence["source_provenance"] = response_evidence.get("source_provenance") or listing_metadata.get("surface_provenance")
            response_evidence["source_lineage"] = response_evidence.get("source_lineage") or listing_metadata.get("source_lineage") or response_evidence.get("source_platform")
            response_evidence["critic_status"] = decision["status"]
            response_evidence["critic_reasons"] = decision["reasons"]
            response_evidence["suppression_reason"] = "; ".join(decision["reasons"]) if decision["status"] != "visible" else None
            response_evidence["suppression_category"] = decision["suppression_category"]
            response_evidence["liveness_evidence"] = decision["liveness_evidence"]
            response_evidence["listing_status"] = listing_status
            response_evidence["freshness_hours"] = freshness_hours
            response_evidence["freshness_days"] = freshness_days
            response_evidence["url"] = decision["authoritative_url"]
            response_evidence["posted_at"] = _isoformat_utc(decision["posted_at"])
            response_evidence["first_published_at"] = _isoformat_utc(decision["first_published_at"])
            response_evidence["discovered_at"] = _isoformat_utc(decision["discovered_at"])
            response_evidence["last_seen_at"] = _isoformat_utc(decision["last_seen_at"])
            response_evidence["updated_at"] = _isoformat_utc(decision["updated_at"])

            items.append(
                LeadResponse(
                    id=lead.id,
                    lead_type=lead.lead_type,
                    company_name=lead.company_name,
                    primary_title=lead.primary_title,
                    url=decision["authoritative_url"],
                    source_type=source_type,
                    listing_status=listing_status,
                    first_published_at=_ensure_utc_datetime(decision["first_published_at"]),
                    discovered_at=_ensure_utc_datetime(decision["discovered_at"]),
                    last_seen_at=_ensure_utc_datetime(decision["last_seen_at"]),
                    updated_at=_ensure_utc_datetime(decision["updated_at"] or lead.updated_at),
                    freshness_hours=freshness_hours,
                    freshness_days=freshness_days,
                    posted_at=_ensure_utc_datetime(decision["posted_at"]),
                    surfaced_at=_ensure_utc_datetime(lead.surfaced_at),
                    rank_label=lead.rank_label,
                    confidence_label=lead.confidence_label,
                    freshness_label=lead.freshness_label,
                    title_fit_label=lead.title_fit_label,
                    qualification_fit_label=lead.qualification_fit_label,
                    source_platform=evidence.get("source_platform", source_type),
                    source_provenance=response_evidence.get("source_provenance"),
                    source_lineage=response_evidence.get("source_lineage", response_evidence.get("source_platform", source_type)),
                    discovery_source=response_evidence.get("discovery_source"),
                    saved=saved,
                    applied=applied,
                    current_status=current_status,
                    date_saved=_ensure_utc_datetime(application.date_saved) if application else None,
                    date_applied=_ensure_utc_datetime(application.date_applied) if application else None,
                    application_notes=application.notes if application else None,
                    application_updated_at=_ensure_utc_datetime(application.updated_at) if application else None,
                    next_action=next_action,
                    follow_up_due=follow_up_due,
                    explanation=lead.explanation,
                    last_agent_action=lead.last_agent_action,
                    hidden=not decision["visible"],
                    score_breakdown_json=lead.score_breakdown_json or {},
                    evidence_json=response_evidence,
                )
            )
            serialization_ms += (perf_counter() - serialization_started) * 1000
        if critic_stage_started_at is not None:
            _log_stage_timing("critic_filter", critic_stage_started_at, total_considered)
        if serialization_stage_started_at is not None:
            _log_stage_timing("serialization", serialization_stage_started_at, len(items))
        logger.info(
            "[READTIME_CRITIC_DROPS] %s",
            {
                "total_considered": total_considered,
                "total_returned": len(items),
                "omitted_by_status": dict(omitted_by_status),
                "omitted_by_category": dict(omitted_by_category),
            },
        )
        return sorted(
            items,
            key=lambda item: (
                rank_order.get(item.rank_label, 3),
                lead_type_order.get(item.lead_type, 3),
                freshness_order.get(item.freshness_label, 4),
                recency_value(item),
                item.company_name.lower(),
            ),
        )
    finally:
        if critic_stage_started_at is not None and total_considered == 0:
            _log_stage_timing("critic_filter", critic_stage_started_at, total_considered)
        if serialization_stage_started_at is not None and not items:
            _log_stage_timing("serialization", serialization_stage_started_at, len(items))
        logger.info(
            "[LOCATION_GATE_CACHE] %s",
            {
                "total_calls": location_gate_stats["total_calls"],
                "unique_keys": len(location_cache),
                "cache_hits": location_gate_stats["cache_hits"],
                "cache_misses": location_gate_stats["cache_misses"],
            },
        )
        logger.info(
            "[LOCATION_GATE_DEDUPED] %s",
            {
                "emitted_count": location_log_state["emitted_count"],
                "suppressed_duplicate_count": location_log_state["suppressed_duplicate_count"],
            },
        )
        total_ms = round((perf_counter() - started_at) * 1000, 2)
        logger.info(
            "[LEADS_TIMING] %s",
            {
                "db_fetch_ms": db_fetch_ms,
                "critic_filter_ms": round(critic_filter_ms, 2),
                "serialization_ms": round(serialization_ms, 2),
                "total_ms": total_ms,
                "records_considered": total_considered,
                "records_returned": len(items),
                "location_gate_calls": location_gate_stats["total_calls"],
                "unique_location_gate_keys": len(location_cache),
            },
        )

    return []


def get_stats(session: Session) -> StatsResponse:
    return StatsResponse(
        total_leads=session.scalar(select(func.count(Lead.id))) or 0,
        visible_leads=session.scalar(select(func.count(Lead.id)).where(Lead.hidden.is_(False))) or 0,
        active_listings=session.scalar(select(func.count(Listing.id)).where(Listing.listing_status == "active")) or 0,
        fresh_listings=session.scalar(select(func.count(Listing.id)).where(Listing.freshness_days <= 7, Listing.listing_status == "active")) or 0,
        combined_leads=session.scalar(select(func.count(Lead.id)).where(Lead.lead_type == "combined")) or 0,
        signal_only_leads=session.scalar(select(func.count(Lead.id)).where(Lead.lead_type == "signal")) or 0,
        saved_leads=session.scalar(select(func.count(Application.id)).where(Application.date_saved.is_not(None))) or 0,
        applied_leads=session.scalar(select(func.count(Application.id)).where(Application.date_applied.is_not(None))) or 0,
        pending_rechecks=session.scalar(select(func.count(RecheckQueue.id)).where(RecheckQueue.status.in_(["queued", "retrying"]))) or 0,
    )
