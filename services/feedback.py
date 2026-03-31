from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import get_settings
from core.models import CandidateProfile, Feedback, Lead, SourceQuery
from core.schemas import FeedbackRequest
from core.time import utcnow
from services.activity import append_lead_agent_trace, log_agent_activity
from services.applications import mark_applied, save_for_later
from services.learning import add_watchlist_item, increment_query_stat, mark_query_status
from services.pipeline import run_critic_agent, run_ranker_agent
from services.profile import get_candidate_profile
from services.query_learning import generate_queries_from_preferences, upsert_generated_queries


def _dismiss_lead(lead: Lead, *, reason: str) -> None:
    evidence = dict(lead.evidence_json or {})
    evidence["user_dismissed_at"] = utcnow().isoformat()
    evidence["user_hidden_reason"] = reason
    lead.evidence_json = evidence
    lead.hidden = True


def _restore_lead(lead: Lead) -> None:
    evidence = dict(lead.evidence_json or {})
    evidence.pop("user_dismissed_at", None)
    evidence.pop("user_hidden_reason", None)
    lead.evidence_json = evidence
    lead.hidden = False


def _mark_seen(lead: Lead) -> None:
    evidence = dict(lead.evidence_json or {})
    evidence.setdefault("user_seen_at", utcnow().isoformat())
    lead.evidence_json = evidence


def _get_learning(profile: CandidateProfile) -> dict:
    summary = profile.extracted_summary_json or {}
    learning = summary.get("learning", {})
    learning.setdefault("title_weights", {})
    learning.setdefault("role_family_weights", {})
    learning.setdefault("domain_weights", {})
    learning.setdefault("source_penalties", {})
    learning.setdefault("company_penalties", {})
    learning.setdefault("location_penalties", {})
    learning.setdefault("generated_queries", [])
    learning.setdefault("feedback_notes", [])
    learning.setdefault("feedback_events", [])
    return learning


def _append_feedback_event(
    learning: dict,
    *,
    action: str,
    lead: Lead,
    company_domain: str,
    role_family: str,
    source_type: str,
) -> None:
    events = list(learning.get("feedback_events", []))
    events.append(
        {
            "at": utcnow().isoformat(),
            "action": action,
            "title": lead.primary_title,
            "company_name": lead.company_name,
            "company_domain": company_domain or "",
            "role_family": role_family,
            "source_type": source_type,
        }
    )
    learning["feedback_events"] = events[-40:]


def _persist_learning(profile: CandidateProfile, learning: dict) -> None:
    summary = dict(profile.extracted_summary_json or {})
    summary["learning"] = learning
    profile.extracted_summary_json = summary


def _update_source_query_stats(session: Session, query_texts: list[str], stat_key: str) -> None:
    for query_text in query_texts:
        increment_query_stat(session, source_type="x", query_text=query_text, field_name=stat_key)
        query = session.scalar(
            select(SourceQuery).where(SourceQuery.query_text == query_text, SourceQuery.source_type == "x")
        )
        if not query:
            continue
        stats = dict(query.performance_stats_json or {})
        stats[stat_key] = stats.get(stat_key, 0) + 1
        query.performance_stats_json = stats


def submit_feedback(session: Session, request: FeedbackRequest) -> Feedback:
    settings = get_settings()
    lead = session.get(Lead, request.lead_id)
    if not lead:
        raise ValueError(f"Lead {request.lead_id} not found")

    profile = get_candidate_profile(session)
    learning = _get_learning(profile)
    evidence = lead.evidence_json or {}
    company_domain = evidence.get("company_domain", "")
    source_type = evidence.get("source_type", "unknown")
    query_texts = evidence.get("source_queries", [])

    title_weights = dict(learning.get("title_weights", {}))
    role_family_weights = dict(learning.get("role_family_weights", {}))
    domain_weights = dict(learning.get("domain_weights", {}))
    source_penalties = dict(learning.get("source_penalties", {}))
    company_penalties = dict(learning.get("company_penalties", {}))
    location_penalties = dict(learning.get("location_penalties", {}))
    feedback_notes = list(learning.get("feedback_notes", []))
    role_family = (lead.score_breakdown_json or {}).get("role_family", "generalist")
    location_scope = evidence.get("location_scope", "unknown")

    if request.action in {"like", "save", "applied", "more_like_this"}:
        delta = 1.2 if request.action == "applied" else 0.8 if request.action == "more_like_this" else 0.6
        title_weights[lead.primary_title.lower()] = round(title_weights.get(lead.primary_title.lower(), 0.0) + delta, 2)
        role_family_weights[role_family] = round(role_family_weights.get(role_family, 0.0) + (0.8 if request.action == "applied" else 0.5), 2)
        if lead.company_name:
            company_penalties[lead.company_name.lower()] = round(company_penalties.get(lead.company_name.lower(), 0.0) - (0.7 if request.action == "applied" else 0.45), 2)
        if company_domain:
            domain_weights[company_domain.lower()] = round(domain_weights.get(company_domain.lower(), 0.0) + 0.5, 2)
            add_watchlist_item(
                session,
                item_type="domain",
                value=company_domain.lower(),
                source_reason=f"Positive feedback on {lead.company_name} / {lead.primary_title}",
                confidence="high" if request.action == "applied" else "medium",
                status="active",
            )
        add_watchlist_item(
            session,
            item_type="title_family",
            value=role_family,
            source_reason=f"{request.action} feedback on {lead.primary_title}",
            confidence="high" if request.action in {"applied", "more_like_this"} else "medium",
            status="active",
        )
        feedback_notes.append(f"{request.action} boosted roles like {lead.primary_title}")
        stat_key = "applies" if request.action == "applied" else "likes"
        if request.action == "save":
            stat_key = "saves"
        _update_source_query_stats(session, query_texts, stat_key)
        if request.action == "save":
            save_for_later(session, lead)
        elif request.action == "applied":
            mark_applied(session, lead)
        _append_feedback_event(
            learning,
            action=request.action,
            lead=lead,
            company_domain=company_domain,
            role_family=role_family,
            source_type=source_type,
        )
    elif request.action in {"dislike", "wrong_function", "too_senior", "too_junior", "wrong_geography"}:
        title_weights[lead.primary_title.lower()] = round(title_weights.get(lead.primary_title.lower(), 0.0) - 0.6, 2)
        role_family_weights[role_family] = round(role_family_weights.get(role_family, 0.0) - 0.4, 2)
        source_penalties[source_type] = round(source_penalties.get(source_type, 0.0) + 0.4, 2)
        company_penalties[lead.company_name.lower()] = round(company_penalties.get(lead.company_name.lower(), 0.0) + (0.9 if request.action == "dislike" else 0.4), 2)
        if company_domain:
            domain_weights[company_domain.lower()] = round(domain_weights.get(company_domain.lower(), 0.0) - 0.35, 2)
        if request.action == "wrong_geography":
            location_penalties[location_scope] = round(location_penalties.get(location_scope, 0.0) + 0.8, 2)
        feedback_notes.append(f"{request.action} reduced similar sourcing from {source_type}")
        _update_source_query_stats(session, query_texts, "dislikes")
        for query_text in query_texts:
            mark_query_status(session, query_text=query_text, source_type="x", status="suppressed")
        if request.action == "dislike":
            _dismiss_lead(lead, reason="Dismissed from jobs list")
        _append_feedback_event(
            learning,
            action=request.action,
            lead=lead,
            company_domain=company_domain,
            role_family=role_family,
            source_type=source_type,
        )
    elif request.action == "seen":
        _mark_seen(lead)
    elif request.action == "restore":
        _restore_lead(lead)
        feedback_notes.append(f"restored {lead.company_name} / {lead.primary_title} to active views")
    elif request.action == "irrelevant_company":
        company_penalties[lead.company_name.lower()] = round(company_penalties.get(lead.company_name.lower(), 0.0) + 1.2, 2)
        source_penalties[source_type] = round(source_penalties.get(source_type, 0.0) + 0.2, 2)
        feedback_notes.append(f"irrelevant_company reduced future focus on {lead.company_name}")
        add_watchlist_item(
            session,
            item_type="company",
            value=lead.company_name,
            source_reason="Marked irrelevant from user feedback",
            confidence="high",
            status="suppressed",
        )
    elif request.action == "mute_company":
        companies = set(profile.excluded_companies_json or [])
        companies.add(lead.company_name)
        profile.excluded_companies_json = sorted(companies)
        feedback_notes.append(f"muted company {lead.company_name}")
        add_watchlist_item(
            session,
            item_type="company",
            value=lead.company_name,
            source_reason="Muted from user feedback",
            confidence="high",
            status="suppressed",
        )
    elif request.action == "mute_title_pattern":
        pattern = request.pattern or lead.primary_title
        excluded = set(profile.excluded_titles_json or [])
        excluded.add(pattern.lower())
        profile.excluded_titles_json = sorted(excluded)
        feedback_notes.append(f"muted title pattern {pattern}")
        add_watchlist_item(
            session,
            item_type="title_pattern",
            value=pattern.lower(),
            source_reason="Muted from user feedback",
            confidence="high",
            status="suppressed",
        )

    new_queries = []
    if request.action in {"like", "applied", "more_like_this", "save"}:
        new_queries = generate_queries_from_preferences(
            titles=[lead.primary_title],
            domains=[company_domain] if company_domain else [],
            role_families=[(lead.score_breakdown_json or {}).get("role_family", "")],
            evidence_snippets=(lead.evidence_json or {}).get("snippets", []),
        )
        new_queries = new_queries[: settings.feedback_max_generated_queries_per_event]
        created = upsert_generated_queries(session, new_queries)
        learning["generated_queries"] = list(
            dict.fromkeys((learning.get("generated_queries", []) + created))
        )[-settings.learning_max_generated_queries_total :]
        for query_text in created:
            add_watchlist_item(
                session,
                item_type="query",
                value=query_text,
                source_reason=f"Generated from positive feedback on {lead.primary_title}",
                confidence="medium",
                status="proposed",
            )

    learning["title_weights"] = title_weights
    learning["role_family_weights"] = role_family_weights
    learning["domain_weights"] = domain_weights
    learning["source_penalties"] = source_penalties
    learning["company_penalties"] = company_penalties
    learning["location_penalties"] = location_penalties
    learning["feedback_notes"] = feedback_notes[-8:]
    _persist_learning(profile, learning)

    feedback = Feedback(
        lead_id=request.lead_id,
        action=request.action,
        subtype=request.subtype,
        reason=request.reason or request.pattern,
    )
    session.add(feedback)
    append_lead_agent_trace(
        lead,
        "Tracker" if request.action in {"save", "applied"} else "Ranker",
        request.action,
        f"Feedback recorded: {request.action}",
        change_state="updated",
    )
    log_agent_activity(
        session,
        agent_name="Tracker" if request.action in {"save", "applied"} else "Ranker",
        action=f"feedback:{request.action}",
        target_type="lead",
        target_count=1,
        target_entity=f"{lead.company_name} / {lead.primary_title}",
        result_summary=f"Captured {request.action} feedback for {lead.company_name} / {lead.primary_title}.",
    )
    run_ranker_agent(session)
    run_critic_agent(session)
    session.flush()
    return feedback
