from __future__ import annotations

from datetime import datetime, timedelta
from functools import partial
from collections import defaultdict
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from connectors.ashby import AshbyConnector
from connectors.greenhouse import GreenhouseConnector
from connectors.x_search import XSearchConnector
from core.config import get_settings
from core.models import Application, Investigation, Lead, Listing, RecheckQueue, Signal, SourceQuery
from core.schemas import ListingRecord, LeadResponse, SignalRecord, StatsResponse, SyncResult
from services.activity import append_lead_agent_trace
from services.ai_judges import judge_critic_with_ai, judge_fit_with_ai
from services.connectors_health import run_connector_fetch
from services.explain import build_explanation
from services.extract_signal import extract_many
from services.freshness import classify_freshness_label, has_expired_pattern, validate_listing
from services.investigations import mark_investigation_attempt, upsert_investigation
from services.learning import generate_follow_up_tasks, increment_query_stat, next_action_for_application
from services.normalize import normalize_ashby_job, normalize_greenhouse_job
from services.profile import get_candidate_profile
from services.query_learning import ensure_source_queries
from services.ranking import infer_role_family, score_lead
from services.resolve_company import get_or_create_company, queue_recheck, resolve_company_name


def _source_learning(profile) -> dict:
    return (profile.extracted_summary_json or {}).get("learning", {})


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


def _authoritative_listing_context(session: Session, lead: Lead) -> dict:
    evidence = dict(lead.evidence_json or {})
    listing = session.get(Listing, lead.listing_id) if lead.listing_id else None
    page_text = ""
    http_status = None
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
        "posted_at": listing.posted_at if listing else None,
        "freshness_days": listing.freshness_days if listing else evidence.get("freshness_days"),
        "listing_status": listing.listing_status if listing else evidence.get("listing_status"),
        "expiration_confidence": listing.expiration_confidence if listing else evidence.get("expiration_confidence", 0.0),
        "description_text": listing.description_text if listing else "",
        "page_text": page_text or "",
        "http_status": http_status,
    }


def _duplicate_winner_context(session: Session, leads: list[Lead]) -> dict[int, str]:
    freshness_order = {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}
    lead_type_order = {"combined": 0, "listing": 1, "signal": 2}
    duplicate_groups: dict[tuple, list[Lead]] = defaultdict(list)

    for lead in leads:
        context = _authoritative_listing_context(session, lead)
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
) -> dict:
    duplicate_losers = duplicate_losers or {}
    context = _authoritative_listing_context(session, lead)
    url = context["url"]
    listing_status = context["listing_status"]
    freshness_days = context["freshness_days"]
    expiration_confidence = context["expiration_confidence"] or 0.0
    page_text = context["page_text"]
    description_text = context["description_text"]
    http_status = context["http_status"]
    reasons: list[str] = []
    status = "visible"
    suppression_category = "none"
    ai_critic = None

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
        if listing_status in {"expired", "suspected_expired"}:
            reasons.append(f"Listing status is {listing_status}")
            status = "suppressed"
            suppression_category = "expired"
        elif listing_status != "active":
            if freshness_days is not None and freshness_days <= 3 and expiration_confidence < 0.2:
                reasons.append("Listing is recent but liveness is still uncertain")
                status = "uncertain"
                suppression_category = "uncertain"
            else:
                reasons.append(f"Listing status is {listing_status or 'unknown'}")
                status = "uncertain"
                suppression_category = "uncertain"
        if freshness_days is None:
            reasons.append("No reliable posted date found")
            status = "uncertain"
            suppression_category = "uncertain"
        elif freshness_window_days is not None and freshness_days > freshness_window_days:
            reasons.append(f"Freshness exceeded the default {freshness_window_days}-day window")
            status = "suppressed"
            suppression_category = "stale"
        if lead.confidence_label == "low":
            reasons.append("Confidence is too low for default surfaced listings")
            status = "uncertain"
            suppression_category = "uncertain"
        ai_critic = judge_critic_with_ai(
            title=lead.primary_title,
            company_name=lead.company_name,
            description_text=description_text,
            listing_status=listing_status,
            freshness_days=freshness_days,
            page_text=page_text,
            url=url,
        )
        if ai_critic and status == "visible" and ai_critic.get("quality_assessment") in {"uncertain", "stale", "suppress"}:
            reasons.append(f"AI critic flagged: {'; '.join(ai_critic.get('reasons', []))}")
            status = "uncertain"
            suppression_category = "uncertain"

    if lead.qualification_fit_label in {"underqualified", "overqualified"}:
        reasons.append(f"Qualification fit is {lead.qualification_fit_label}")
        status = "hidden"
        suppression_category = "qualification"

    if (lead.score_breakdown_json or {}).get("composite", 0.0) < profile.minimum_fit_threshold:
        reasons.append("Composite fit is below the candidate threshold")
        status = "hidden"
        suppression_category = "low_fit"

    if lead.company_name.lower() in [item.lower() for item in (profile.excluded_companies_json or [])]:
        reasons.append("Company is muted in the candidate profile")
        status = "hidden"
        suppression_category = "user_suppressed"

    if lead.lead_type == "signal":
        signal = session.get(Signal, lead.signal_id) if lead.signal_id else None
        if signal and signal.signal_status in {"needs_recheck", "resolved_no_listing"}:
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
        "authoritative_url": url,
        "listing_status": listing_status,
        "freshness_days": freshness_days,
        "posted_at": context["posted_at"],
        "liveness_evidence": {
            "listing_status": listing_status,
            "freshness_days": freshness_days,
            "expiration_confidence": round(expiration_confidence, 2),
            "http_status": http_status,
            "expired_pattern_detected": has_expired_pattern(description_text, page_text),
        },
        "ai_critic_assessment": ai_critic,
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
    evidence["freshness_days"] = decision["freshness_days"]
    evidence["url"] = decision["authoritative_url"]
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
    ai_fit = judge_fit_with_ai(
        profile_text=candidate_context,
        title=title,
        company_name=company_name,
        location=location,
        description_text=description_text,
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
    evidence_json.update(
        {
            "matched_profile_fields": breakdown.get("matched_profile_fields", []),
            "feedback_notes": feedback_notes,
            "freshness_status": freshness_label,
            "freshness_days": listing.freshness_days if listing else 0,
            "confidence_status": breakdown["confidence_label"],
            "listing_status": listing_status,
            "source_type": source_type,
            "source_platform": "x_demo" if source_type == "x" else source_type,
            "company_domain": company_domain,
            "url": listing_url,
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
    x_connector = XSearchConnector()

    greenhouse_jobs: list[dict] = []
    ashby_jobs: list[dict] = []
    x_raw_signals: list[dict] = []
    greenhouse_live = False
    ashby_live = False
    x_live = False

    if "greenhouse" in enabled_connectors:
        greenhouse_jobs, greenhouse_live, _ = run_connector_fetch(
            session,
            "greenhouse",
            partial(greenhouse_connector.fetch, "greenhouse" in strict_live_connectors),
            date_fields=["first_published", "updated_at"],
        )
    if "ashby" in enabled_connectors:
        ashby_jobs, ashby_live, _ = run_connector_fetch(
            session,
            "ashby",
            partial(ashby_connector.fetch, "ashby" in strict_live_connectors),
            date_fields=["publishedDate"],
        )
    if "x_search" in enabled_connectors:
        x_raw_signals, x_live, _ = run_connector_fetch(
            session,
            "x_search",
            partial(x_connector.fetch, queries, "x_search" in strict_live_connectors),
            date_fields=["published_at"],
        )

    signals_ingested = 0
    listings_ingested = 0
    leads_created = 0
    leads_updated = 0
    rechecks_queued = 0
    investigations_opened = 0

    signal_objects: list[Signal] = []
    for raw in extract_many(x_raw_signals):
        if raw.published_at and isinstance(raw.published_at, str):
            raw.published_at = datetime.fromisoformat(raw.published_at.replace("Z", "+00:00"))
        signal = _upsert_signal(session, raw)
        signals_ingested += 1
        signal_objects.append(signal)

    listing_records = [validate_listing(normalize_greenhouse_job(job)) for job in greenhouse_jobs]
    listing_records.extend(validate_listing(normalize_ashby_job(job, job.get("companyName"))) for job in ashby_jobs)

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
                freshness_label=classify_freshness_label(matching_listing.freshness_days),
                evidence_json={
                    "snippets": [signal.raw_text[:220], (matching_listing.description_text or "")[:220]],
                    "source_queries": [
                        item.get("query_text")
                        for item in x_raw_signals
                        if item["url"] == signal.source_url and item.get("query_text")
                    ],
                },
            )
            leads_created += 1 if created else 0
            leads_updated += 0 if created else 1
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
            freshness_label=classify_freshness_label(listing.freshness_days),
            evidence_json={
                "snippets": [(listing.description_text or "")[:240]],
                "source_queries": query_texts,
            },
        )
        leads_created += 1 if created else 0
        leads_updated += 0 if created else 1
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
    session.flush()
    return SyncResult(
        signals_ingested=signals_ingested,
        listings_ingested=listings_ingested,
        leads_created=leads_created,
        leads_updated=leads_updated,
        rechecks_queued=rechecks_queued,
        live_mode_used=any([greenhouse_live, ashby_live, x_live]),
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
    records = session.scalars(select(Lead).order_by(Lead.surfaced_at.desc(), Lead.rank_label.asc())).all()
    profile = get_candidate_profile(session)
    duplicate_losers = _duplicate_winner_context(session, records)
    items: list[LeadResponse] = []
    for lead in records:
        evidence = lead.evidence_json or {}
        decision = evaluate_critic_decision(
            session=session,
            lead=lead,
            profile=profile,
            freshness_window_days=freshness_window_days,
            duplicate_losers=duplicate_losers,
        )
        authoritative = _authoritative_listing_context(session, lead)
        freshness_days = decision["freshness_days"]
        listing_status = decision["listing_status"]
        source_type = evidence.get("source_type", lead.lead_type)
        application = session.scalar(select(Application).where(Application.lead_id == lead.id))
        saved = application is not None and application.date_saved is not None
        applied = application is not None and application.date_applied is not None
        current_status = application.current_status if application else None
        next_action, follow_up_due = next_action_for_application(session, application.id) if application else (None, False)

        if only_saved and not saved:
            continue
        if only_applied and not applied:
            continue
        if status and current_status != status:
            continue

        if lead_type and lead.lead_type != lead_type:
            continue
        if lead.lead_type == "signal" and lead_type != "signal" and not include_signal_only:
            continue
        if not include_hidden and not decision["visible"]:
            if lead.lead_type == "signal" and include_signal_only and decision["status"] in {"uncertain", "investigation"}:
                pass
            elif include_unqualified and decision["suppression_category"] == "qualification":
                pass
            else:
                continue
        if freshness_window_days is not None and freshness_days is not None and freshness_days > freshness_window_days:
            continue

        response_evidence = dict(evidence)
        response_evidence["critic_status"] = decision["status"]
        response_evidence["critic_reasons"] = decision["reasons"]
        response_evidence["suppression_reason"] = "; ".join(decision["reasons"]) if decision["status"] != "visible" else None
        response_evidence["suppression_category"] = decision["suppression_category"]
        response_evidence["liveness_evidence"] = decision["liveness_evidence"]
        response_evidence["listing_status"] = listing_status
        response_evidence["freshness_days"] = freshness_days
        response_evidence["url"] = decision["authoritative_url"]

        items.append(
            LeadResponse(
                id=lead.id,
                lead_type=lead.lead_type,
                company_name=lead.company_name,
                primary_title=lead.primary_title,
                url=decision["authoritative_url"],
                source_type=source_type,
                listing_status=listing_status,
                freshness_days=freshness_days,
                posted_at=decision["posted_at"],
                surfaced_at=lead.surfaced_at,
                rank_label=lead.rank_label,
                confidence_label=lead.confidence_label,
                freshness_label=lead.freshness_label,
                title_fit_label=lead.title_fit_label,
                qualification_fit_label=lead.qualification_fit_label,
                source_platform=evidence.get("source_platform", source_type),
                saved=saved,
                applied=applied,
                current_status=current_status,
                date_saved=application.date_saved if application else None,
                date_applied=application.date_applied if application else None,
                application_notes=application.notes if application else None,
                application_updated_at=application.updated_at if application else None,
                next_action=next_action,
                follow_up_due=follow_up_due,
                explanation=lead.explanation,
                last_agent_action=lead.last_agent_action,
                hidden=not decision["visible"],
                score_breakdown_json=lead.score_breakdown_json or {},
                evidence_json=response_evidence,
            )
        )
    rank_order = {"strong": 0, "medium": 1, "weak": 2}
    freshness_order = {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}
    lead_type_order = {"combined": 0, "listing": 1, "signal": 2}

    def recency_value(item: LeadResponse) -> float:
        reference = item.posted_at or item.surfaced_at
        return -reference.timestamp() if reference else 0.0

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
