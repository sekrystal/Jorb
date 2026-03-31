from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any, Callable

import pandas as pd
import streamlit as st

from services.lead_search import build_search_document, match_search_document, normalize_search_query
from ui.components.job_card import render_job_card
from ui.components.topbar import render_jobs_topbar


def is_restorable_dismissed_lead(lead: dict[str, Any]) -> bool:
    evidence = lead.get("evidence_json") or {}
    return bool(evidence.get("user_dismissed_at")) or str(evidence.get("suppression_category") or "").strip().lower() == "user_dismissed"


def filter_restorable_dismissed_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [lead for lead in leads if is_restorable_dismissed_lead(lead)]


def build_jobs_action_feedback(action: str) -> dict[str, str]:
    if action == "seen":
        return {"tone": "info", "message": "Marked as seen."}
    if action == "dislike":
        return {
            "tone": "success",
            "message": "Job dismissed. It is now hidden from Jobs, Saved, and Applied. Open Dismissed to restore it.",
        }
    if action == "restore":
        return {
            "tone": "success",
            "message": "Job restored. It is visible in active job views again.",
        }
    if action == "save":
        return {"tone": "success", "message": "Job saved."}
    if action == "applied":
        return {"tone": "success", "message": "Job marked as applied."}
    return {"tone": "info", "message": "Action recorded."}


def build_jobs_empty_state_markup(view_model: dict[str, Any]) -> str:
    palette = {
        "info": {"background": "#F8FAFC", "border": "#CBD5E1", "accent": "#334155"},
        "success": {"background": "#F0FDF4", "border": "#BBF7D0", "accent": "#166534"},
        "warning": {"background": "#FFFBEB", "border": "#FDE68A", "accent": "#92400E"},
        "error": {"background": "#FEF2F2", "border": "#FECACA", "accent": "#B91C1C"},
    }[str(view_model.get("tone") or "info")]
    title = escape(str(view_model.get("title") or "Nothing to show."))
    detail = escape(str(view_model.get("detail") or ""))
    eyebrow = escape(str(view_model.get("eyebrow") or "State"))
    badge = escape(str(view_model.get("badge") or eyebrow))
    return f"""
        <div style="
            background:{palette['background']};
            border:1px solid {palette['border']};
            border-radius:1rem;
            padding:1rem 1.05rem;
            margin:0.35rem 0 0.75rem 0;
        ">
          <div style="display:flex;justify-content:space-between;gap:0.75rem;align-items:flex-start;flex-wrap:wrap;">
            <div style="font-size:0.74rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:{palette['accent']};margin-bottom:0.3rem;">{eyebrow}</div>
            <div style="display:inline-flex;align-items:center;border-radius:999px;padding:0.2rem 0.55rem;background:#FFFFFFCC;border:1px solid {palette['border']};font-size:0.74rem;font-weight:700;color:{palette['accent']};">{badge}</div>
          </div>
          <div style="font-size:1rem;font-weight:700;line-height:1.35;color:#111827;overflow-wrap:anywhere;">{title}</div>
          <div style="margin-top:0.35rem;font-size:0.92rem;line-height:1.5;color:#475569;overflow-wrap:anywhere;">{detail}</div>
        </div>
    """


def build_jobs_detail_empty_state_markup() -> str:
    return """
        <div style="
            background:#F8FAFC;
            border:1px solid #CBD5E1;
            border-radius:1rem;
            padding:1rem 1.05rem;
            min-height:12rem;
        ">
          <div style="display:flex;justify-content:space-between;gap:0.75rem;align-items:flex-start;flex-wrap:wrap;">
            <div style="font-size:0.74rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#475569;margin-bottom:0.3rem;">Details</div>
            <div style="display:inline-flex;align-items:center;border-radius:999px;padding:0.2rem 0.55rem;background:#FFFFFFCC;border:1px solid #CBD5E1;font-size:0.74rem;font-weight:700;color:#475569;">Nothing selected</div>
          </div>
          <div style="font-size:1rem;font-weight:700;line-height:1.35;color:#111827;">Select a job to inspect its full rationale.</div>
          <div style="margin-top:0.35rem;font-size:0.92rem;line-height:1.5;color:#475569;">Use the list on the left to compare matches, then open one focused detail view without losing your place.</div>
        </div>
    """


def build_jobs_intro_state_markup(*, title: str, intro_message: str) -> str:
    title_copy = escape(str(title or "Workspace"))
    intro_copy = escape(str(intro_message or ""))
    return f"""
        <div style="
            background:#F8FAFC;
            border:1px solid #CBD5E1;
            border-radius:1rem;
            padding:0.95rem 1.05rem;
            margin:0.15rem 0 0.9rem 0;
        ">
          <div style="display:flex;justify-content:space-between;gap:0.75rem;align-items:flex-start;flex-wrap:wrap;">
            <div style="font-size:0.74rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#475569;">Workspace</div>
            <div style="display:inline-flex;align-items:center;border-radius:999px;padding:0.2rem 0.55rem;background:#FFFFFFCC;border:1px solid #CBD5E1;font-size:0.74rem;font-weight:700;color:#475569;">{title_copy}</div>
          </div>
          <div style="margin-top:0.2rem;font-size:0.96rem;font-weight:700;line-height:1.35;color:#111827;">{title_copy} view</div>
          <div style="margin-top:0.32rem;font-size:0.92rem;line-height:1.5;color:#475569;overflow-wrap:anywhere;">{intro_copy}</div>
        </div>
    """


def _search_run_finished_with_zero_results(search_run: dict[str, Any]) -> bool:
    status = str(search_run.get("status") or "").strip().lower()
    if bool(search_run.get("zero_yield")):
        return True
    return status in {"empty", "zero_yield"}


def build_jobs_search_loading_message(query: str) -> str:
    cleaned = str(query or "").strip()
    return f"Searching jobs for '{cleaned}'..." if cleaned else "Loading jobs..."


def _format_search_fields(search_meta: dict[str, Any]) -> str:
    fields = [str(field).strip() for field in (search_meta.get("searched_fields") or []) if str(field).strip()]
    if not fields:
        return "job fields"
    return ", ".join(fields)


def build_search_state_view_model(
    search_run: dict[str, Any] | None,
    *,
    search_meta: dict[str, Any] | None = None,
    visible_job_count: int | None = None,
) -> dict[str, str]:
    if search_meta and str(search_meta.get("query") or "").strip():
        query = str(search_meta.get("query") or "").strip()
        status = str(search_meta.get("status") or "").strip().lower()
        result_count = int(search_meta.get("result_count") or 0)
        visible_count = visible_job_count if visible_job_count is not None else result_count
        searched_fields = _format_search_fields(search_meta)

        if status == "error" or search_meta.get("error"):
            return {
                "tone": "error",
                "eyebrow": "Search",
                "badge": "Error",
                "title": "Search failed.",
                "detail": f"Could not load results for '{query}'. Backend search failed before results were returned.",
            }

        if status in {"queued", "running", "loading"}:
            return {
                "tone": "info",
                "eyebrow": "Search",
                "badge": "Loading",
                "title": "Search is running.",
                "detail": f"Searching '{query}' across {searched_fields}. Results will appear when loading finishes.",
            }

        if result_count == 0 or status == "empty":
            return {
                "tone": "warning",
                "eyebrow": "Search",
                "badge": "Zero results",
                "title": "No jobs matched this search.",
                "detail": f"No jobs matched '{query}' across {searched_fields}.",
            }

        detail = f"Found {result_count} job{'s' if result_count != 1 else ''} for '{query}' across {searched_fields}."
        if visible_job_count is not None and visible_count < result_count:
            detail += f" {visible_count} remain after local location or remote filters."
        return {
            "tone": "success",
            "eyebrow": "Search",
            "badge": "Loaded",
            "title": "Search results loaded.",
            "detail": detail,
        }

    if not search_run:
        return {
            "tone": "info",
            "eyebrow": "Search",
            "badge": "Idle",
            "title": "Search has not run yet.",
            "detail": "Refresh jobs to load this view.",
        }

    status = str(search_run.get("status") or "").strip().lower()
    query_count = int(search_run.get("query_count") or 0)
    result_count = int(search_run.get("result_count") or 0)
    created_at = _parse_timestamp(search_run.get("created_at"))
    created_label = (
        created_at.astimezone(timezone.utc).strftime("%b %d, %Y %I:%M %p UTC")
        if created_at is not None
        else "unknown time"
    )

    if search_run.get("error") or status in {"failed", "error"}:
        reason = search_run.get("failure_classification") or search_run.get("error") or "unknown error"
        return {
            "tone": "error",
            "eyebrow": "Search",
            "badge": "Error",
            "title": "Search failed.",
            "detail": f"The latest run ended with {reason} at {created_label}.",
        }

    if status in {"queued", "running", "started"}:
        return {
            "tone": "info",
            "eyebrow": "Search",
            "badge": "Loading",
            "title": "Search is running.",
            "detail": "Jobs will update here when the current run finishes.",
        }

    if _search_run_finished_with_zero_results(search_run):
        return {
            "tone": "warning",
            "eyebrow": "Search",
            "badge": "Zero results",
            "title": "Search finished with no matching jobs.",
            "detail": f"The latest run checked {query_count} quer{'y' if query_count == 1 else 'ies'} at {created_label}.",
        }

    return {
        "tone": "success",
        "eyebrow": "Search",
        "badge": "Loaded",
        "title": "Search finished successfully.",
        "detail": (
            f"The latest run found {result_count} job{'s' if result_count != 1 else ''} "
            f"across {query_count} quer{'y' if query_count == 1 else 'ies'} at {created_label}."
        ),
    }


def build_manual_search_feedback(sync_result: dict[str, Any]) -> dict[str, str]:
    surfaced_count = int(sync_result.get("surfaced_count") or 0)
    if surfaced_count > 0:
        tone = "success"
    else:
        tone = "warning"
    message = f"Refresh finished. Surfaced {surfaced_count} job{'s' if surfaced_count != 1 else ''}."
    return {"tone": tone, "message": message}


def render_search_status_region(
    search_run: dict[str, Any] | None,
    *,
    visible_job_count: int,
    search_meta: dict[str, Any] | None = None,
) -> None:
    search_state = build_search_state_view_model(search_run, search_meta=search_meta, visible_job_count=visible_job_count)
    palette = {
        "info": {"background": "#F8FAFC", "border": "#CBD5E1", "accent": "#334155"},
        "success": {"background": "#F0FDF4", "border": "#BBF7D0", "accent": "#166534"},
        "warning": {"background": "#FFFBEB", "border": "#FDE68A", "accent": "#92400E"},
        "error": {"background": "#FEF2F2", "border": "#FECACA", "accent": "#B91C1C"},
    }[search_state["tone"]]
    count_label = f"{visible_job_count} job{'s' if visible_job_count != 1 else ''} in view"
    title = escape(search_state["title"])
    detail = escape(search_state["detail"])
    eyebrow = escape(str(search_state.get("eyebrow") or "Search"))
    badge = escape(str(search_state.get("badge") or "Status"))
    count = escape(count_label)
    st.caption("Search status")
    st.markdown(
        f"""
        <div style="
            margin: 0 0 1rem 0;
            padding: 0.85rem 1rem;
            border: 1px solid {palette['border']};
            border-radius: 0.85rem;
            background: {palette['background']};
        ">
          <div style="display:flex; flex-wrap:wrap; justify-content:space-between; gap:0.85rem; align-items:flex-start;">
            <div style="min-width:16rem; flex:1 1 24rem;">
              <div style="font-size:0.74rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:{palette['accent']};margin-bottom:0.25rem;">{eyebrow}</div>
              <div style="font-size:0.98rem; font-weight:600; color:#111827; overflow-wrap:anywhere;">{title}</div>
              <div style="margin-top:0.2rem; font-size:0.9rem; line-height:1.45; color:#4B5563; overflow-wrap:anywhere;">{detail}</div>
            </div>
            <div style="display:flex;flex-wrap:wrap;gap:0.45rem;justify-content:flex-end;">
              <div style="
                  white-space:nowrap;
                  font-size:0.78rem;
                  font-weight:700;
                  color:{palette['accent']};
                  background:#FFFFFFCC;
                  border:1px solid {palette['border']};
                  border-radius:999px;
                  padding:0.25rem 0.6rem;
              ">{badge}</div>
              <div style="
                  white-space:nowrap;
                  font-size:0.78rem;
                  font-weight:600;
                  color:{palette['accent']};
                  background:#FFFFFFCC;
                  border:1px solid {palette['border']};
                  border-radius:999px;
                  padding:0.25rem 0.6rem;
              ">{count}</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_jobs_empty_state_view_model(
    search_run: dict[str, Any] | None,
    *,
    total_job_count: int,
    filters: dict[str, Any],
    search_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    has_filters = bool(filters["search"].strip() or filters["location"].strip() or filters["remote_only"])
    backend_search_active = bool(search_meta and str(search_meta.get("query") or "").strip())
    if total_job_count > 0 and has_filters:
        if backend_search_active:
            query = str(search_meta.get("query") or "").strip()
            result_count = int(search_meta.get("result_count") or total_job_count)
            return {
                "tone": "info",
                "eyebrow": "Filters",
                "title": "Search found jobs, but local filters hide them.",
                "detail": (
                    f"Backend search found {result_count} job{'s' if result_count != 1 else ''} for '{query}', "
                    "but the current location or remote filters hide them."
                ),
                "show_clear_filters": True,
            }
        return {
            "tone": "info",
            "eyebrow": "Filters",
            "title": "No jobs match the current filters.",
            "detail": "Clear filters or adjust the current search terms to see the latest jobs in this list.",
            "show_clear_filters": True,
        }

    search_state = build_search_state_view_model(search_run, search_meta=search_meta, visible_job_count=0)
    result_count = int((search_run or {}).get("result_count") or 0)
    if backend_search_active:
        result_count = int(search_meta.get("result_count") or 0)
    if search_state["tone"] == "success" and result_count > 0:
        return {
            "tone": "info",
            "eyebrow": "Visibility",
            "title": "Search finished, but no jobs are visible yet.",
            "detail": search_state["detail"],
            "show_clear_filters": False,
        }

    return {
        "tone": search_state["tone"],
        "eyebrow": "Search",
        "title": search_state["title"],
        "detail": search_state["detail"],
        "show_clear_filters": False,
    }


def render_jobs_empty_state(
    search_run: dict[str, Any] | None,
    *,
    total_job_count: int,
    filters: dict[str, Any],
    page_key: str,
    search_meta: dict[str, Any] | None = None,
) -> None:
    empty_state = build_jobs_empty_state_view_model(search_run, total_job_count=total_job_count, filters=filters, search_meta=search_meta)
    st.markdown(build_jobs_empty_state_markup(empty_state), unsafe_allow_html=True)
    if not empty_state["show_clear_filters"]:
        return
    if st.button("Clear filters", key=f"jobs-clear-filters-{page_key}"):
        st.session_state[f"jobs-search-{page_key}"] = ""
        st.session_state[f"jobs-location-{page_key}"] = ""
        st.session_state[f"jobs-remote-{page_key}"] = False
        st.rerun()


def _match_label(score_payload: dict[str, Any], lead: dict[str, Any]) -> str:
    tier = (score_payload.get("match_tier") or "").lower()
    if tier == "high":
        return "High fit"
    if tier == "medium":
        return "Medium fit"
    band = (score_payload.get("recommendation_band") or lead.get("rank_label") or "").lower()
    return "High fit" if band == "strong" else "Medium fit" if band == "medium" else "Low fit"


def _match_score_display(score_payload: dict[str, Any]) -> str:
    final_score = score_payload.get("final_score", score_payload.get("composite"))
    if final_score is None:
        return "n/a"
    try:
        return f"{float(final_score):.1f}"
    except (TypeError, ValueError):
        return "n/a"


def _description_from_evidence(evidence: dict[str, Any]) -> tuple[str, bool]:
    description_text = (evidence.get("description_text") or "").strip()
    if description_text:
        compact = " ".join(description_text.split())
        return compact[:240] + ("..." if len(compact) > 240 else ""), False
    snippets = [snippet.strip() for snippet in (evidence.get("snippets") or []) if snippet and snippet.strip()]
    if snippets:
        compact = " ".join(snippets)
        return compact[:240] + ("..." if len(compact) > 240 else ""), False
    return "Description unavailable from the source listing.", True


def _full_description_from_evidence(evidence: dict[str, Any]) -> tuple[str, bool]:
    description_text = (evidence.get("description_text") or "").strip()
    if description_text:
        return description_text, False
    snippets = [snippet.strip() for snippet in (evidence.get("snippets") or []) if snippet and snippet.strip()]
    if snippets:
        return "\n\n".join(snippets), False
    return "Full description unavailable from the source listing.", True


def _work_mode_from_evidence(evidence: dict[str, Any]) -> tuple[str, bool]:
    listing_metadata = evidence.get("listing_metadata_json") or {}
    explicit_work_mode = str(
        evidence.get("work_mode")
        or listing_metadata.get("work_mode")
        or listing_metadata.get("work_mode_preference")
        or ""
    ).strip().lower()
    if explicit_work_mode in {"remote", "hybrid", "onsite"}:
        return explicit_work_mode, False
    location = (evidence.get("location") or "").strip().lower()
    location_scope = (evidence.get("location_scope") or "").strip().lower()
    if "remote" in location or location_scope.startswith("remote"):
        return "remote", False
    if "hybrid" in location or location_scope.startswith("hybrid"):
        return "hybrid", False
    if location:
        return "onsite", False
    return "not specified", True


def _source_fields(lead: dict[str, Any], evidence: dict[str, Any]) -> tuple[str, str]:
    source = (
        lead.get("source_type")
        or evidence.get("source_type")
        or lead.get("source_platform")
        or evidence.get("source_platform")
        or "Unknown source"
    )
    provenance = (
        lead.get("source_lineage")
        or evidence.get("source_lineage")
        or lead.get("source_platform")
        or evidence.get("source_platform")
        or source
    )
    return str(source), str(provenance)


def build_job_view_model(lead: dict[str, Any]) -> dict[str, Any]:
    evidence = lead.get("evidence_json") or {}
    score_payload = lead.get("score_breakdown_json") or {}
    description, description_missing = _description_from_evidence(evidence)
    full_description, full_description_missing = _full_description_from_evidence(evidence)
    work_mode, work_mode_missing = _work_mode_from_evidence(evidence)
    source, source_provenance = _source_fields(lead, evidence)
    state = (
        "dismissed" if evidence.get("user_dismissed_at")
        else "applied" if lead.get("applied")
        else "saved" if lead.get("saved")
        else "seen" if lead.get("seen")
        else "new"
    )
    top_matching_signals = [str(item).strip() for item in (score_payload.get("top_matching_signals") or evidence.get("top_matching_signals") or []) if str(item).strip()]
    missing_signals = [str(item).strip() for item in (score_payload.get("missing_signals") or evidence.get("missing_signals") or []) if str(item).strip()]
    explanation_summary = (
        (score_payload.get("explanation") or {}).get("summary")
        or lead.get("explanation")
        or "Backend did not return a recommendation explanation."
    )
    decision_explanation = (
        f"Top signals: {', '.join(top_matching_signals[:3])}."
        if top_matching_signals
        else explanation_summary
    )
    if missing_signals:
        decision_explanation = f"{decision_explanation} Missing: {', '.join(missing_signals[:2])}."
    tags = [
        item
        for item in [
            lead.get("freshness_label"),
            lead.get("qualification_fit_label"),
            lead.get("confidence_label"),
            work_mode,
        ]
        if item
    ][:4]
    gaps: list[str] = []
    if work_mode_missing:
        gaps.append("work_mode")
    if description_missing:
        gaps.append("description")
    if full_description_missing:
        gaps.append("full_description")
    if not evidence.get("location"):
        gaps.append("location")
    view_model = {
        "id": str(lead["id"]),
        "lead_id": lead["id"],
        "title": lead.get("primary_title") or "Untitled role",
        "company": lead.get("company_name") or "Unknown company",
        "location": evidence.get("location") or "Location not specified",
        "work_mode": work_mode,
        "description": description,
        "full_description": full_description,
        "match_score_display": _match_score_display(score_payload),
        "match_label": _match_label(score_payload, lead),
        "match_tier": score_payload.get("match_tier") or ("high" if lead.get("rank_label") == "strong" else "medium" if lead.get("rank_label") == "medium" else "low"),
        "explanation": (
            decision_explanation
        ),
        "tags": tags,
        "posted_date": lead.get("posted_at") or lead.get("surfaced_at") or "Unknown date",
        "salary": evidence.get("salary") or None,
        "source": source,
        "source_provenance": source_provenance,
        "state": state,
        "why_this_job": decision_explanation,
        "what_you_are_missing": ", ".join(missing_signals[:3]) if missing_signals else None,
        "suggested_next_steps": score_payload.get("action_explanation") or "Open the source and decide whether to save, dismiss, or apply.",
        "url": lead.get("url"),
        "backend_gaps": gaps,
        "top_matching_signals": top_matching_signals,
        "missing_signals": missing_signals,
        "raw_lead": lead,
    }
    search_document = build_search_document(lead)
    view_model["_search_fields"] = search_document["fields"]
    view_model["_search_haystack"] = search_document["haystack"]
    view_model["_posted_at_sort"] = search_document["posted_at_sort"]
    view_model["_recommendation_sort"] = search_document["recommendation_sort"]
    view_model["_search_document"] = search_document
    return view_model


def build_job_detail_panel_markup(job: dict[str, Any]) -> str:
    status_label = (
        "Dismissed" if job.get("state") == "dismissed"
        else "Applied" if job.get("state") == "applied"
        else "Saved" if job.get("state") == "saved"
        else "Seen" if job.get("state") == "seen"
        else "New"
    )
    chips = [
        f'<span class="jorb-job-detail-chip">{escape(str(job.get("company") or "Unknown company"))}</span>',
        f'<span class="jorb-job-detail-chip">{escape(str(job.get("location") or "Unknown location"))}</span>',
        f'<span class="jorb-job-detail-chip">{escape(str(job.get("work_mode") or "Unknown mode"))}</span>',
        f'<span class="jorb-job-detail-chip">{escape(str(job.get("source") or "Unknown source"))}</span>',
        f'<span class="jorb-job-detail-chip status">{escape(status_label)}</span>',
    ]
    if job.get("tags"):
        chips.extend(
            f'<span class="jorb-job-detail-chip muted">{escape(str(tag))}</span>'
            for tag in job.get("tags", [])[:4]
            if str(tag).strip()
        )
    missing_fields = [str(item).replace("_", " ") for item in (job.get("backend_gaps") or []) if str(item).strip()]
    gaps_markup = ""
    if missing_fields:
        gaps_markup = (
            '<div class="jorb-job-detail-callout">'
            '<div class="jorb-job-detail-callout-title">Source gaps</div>'
            f'<div class="jorb-job-detail-callout-copy">Some upstream fields are still incomplete for this job: {escape(", ".join(missing_fields))}.</div>'
            "</div>"
        )
    sections = [
        ("Recommendation summary", job.get("explanation") or "No recommendation summary recorded."),
        ("Why this job", job.get("why_this_job") or "No detailed rationale recorded."),
        ("What you are missing", job.get("what_you_are_missing") or "No major gaps flagged."),
        ("Suggested next steps", job.get("suggested_next_steps") or "Open the source and decide whether to save, dismiss, or apply."),
        ("Full description", job.get("full_description") or "No full description available."),
    ]
    sections_markup = "".join(
        (
            f'<div class="jorb-job-detail-section-title">{escape(title)}</div>'
            f'<div class="jorb-job-detail-section-copy">{escape(str(copy))}</div>'
        )
        for title, copy in sections
    )
    return f"""
        <div class="jorb-job-detail-shell">
          <div class="jorb-job-detail-topline">
            <div class="jorb-job-detail-eyebrow">Selected job</div>
            <div class="jorb-job-detail-badge">{escape(job.get("match_label") or "Match")}</div>
          </div>
          <div class="jorb-job-detail-title">{escape(job["title"])}</div>
          <div class="jorb-job-detail-subtitle">{escape(str(job.get("company") or ""))}</div>
          <div class="jorb-job-detail-chip-row">{''.join(chips)}</div>
          <div class="jorb-job-detail-score"><strong>{escape(job["match_score_display"])}</strong> recommendation score</div>
          {gaps_markup}
          {sections_markup}
        </div>
    """


def jobs_backend_gap_frame(jobs: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for job in jobs:
        for gap in job.get("backend_gaps", []):
            rows.append(
                {
                    "job_id": job["lead_id"],
                    "title": job["title"],
                    "company": job["company"],
                    "missing_field": gap,
                }
            )
    return pd.DataFrame(rows)


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def normalize_job_search_query(query: str) -> dict[str, Any]:
    return normalize_search_query(query)


def _searchable_text(value: Any) -> str:
    return str(build_search_document({"title": value})["fields"]["title"])


def _job_search_fields(job: dict[str, Any]) -> dict[str, str]:
    cached = job.get("_search_document")
    if isinstance(cached, dict):
        return dict(cached.get("fields") or {})
    raw_lead = dict(job.get("raw_lead") or {})
    raw_lead.setdefault("title", job.get("title"))
    raw_lead.setdefault("company", job.get("company"))
    raw_lead.setdefault("location", job.get("location"))
    raw_lead.setdefault("source", job.get("source"))
    raw_lead.setdefault("description", job.get("description"))
    raw_lead.setdefault("explanation", job.get("explanation"))
    raw_lead.setdefault("tags", job.get("tags"))
    return build_search_document(raw_lead)["fields"]


def _job_search_match(job: dict[str, Any], normalized_query: dict[str, Any]) -> dict[str, Any] | None:
    document = job.get("_search_document")
    if not isinstance(document, dict):
        raw_lead = dict(job.get("raw_lead") or {})
        raw_lead.setdefault("title", job.get("title"))
        raw_lead.setdefault("company", job.get("company"))
        raw_lead.setdefault("location", job.get("location"))
        raw_lead.setdefault("source", job.get("source"))
        raw_lead.setdefault("description", job.get("description"))
        raw_lead.setdefault("explanation", job.get("explanation"))
        raw_lead.setdefault("tags", job.get("tags"))
        document = build_search_document(raw_lead)
    return match_search_document(document, normalized_query)


def _filter_jobs(jobs: list[dict[str, Any]], filters: dict[str, Any], *, search_meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    filtered = list(jobs)
    search_matches: dict[str, dict[str, Any]] = {}
    normalized_query = normalize_job_search_query(filters["search"])
    backend_search_active = bool(
        search_meta
        and str(search_meta.get("query") or "").strip()
        and bool(search_meta.get("backend_applied"))
    )
    if (normalized_query["tokens"] or normalized_query["text"]) and not backend_search_active:
        matched_jobs: list[dict[str, Any]] = []
        for job in filtered:
            match = _job_search_match(job, normalized_query)
            if match is None:
                continue
            search_matches[str(job.get("id"))] = match
            matched_jobs.append(job)
        filtered = matched_jobs
    if filters["location"].strip():
        location_term = filters["location"].strip().lower()
        filtered = [job for job in filtered if location_term in (job.get("location") or "").lower()]
    if filters["remote_only"]:
        filtered = [job for job in filtered if job.get("work_mode") == "remote"]
    if (normalized_query["tokens"] or normalized_query["text"]) and not backend_search_active:
        def _search_sort_key(job: dict[str, Any]) -> tuple[float, float, float, str, str]:
            match = search_matches.get(str(job.get("id"))) or {"score": 0.0}
            recency = job.get("_posted_at_sort") or datetime.min.replace(tzinfo=timezone.utc)
            recommendation = float(job.get("_recommendation_sort") or 0.0)
            secondary = recency.timestamp() if filters["sort_by"] == "Newest" else recommendation
            return (
                float(match["score"]),
                secondary,
                recommendation,
                str(job.get("title") or ""),
                str(job.get("company") or ""),
            )

        filtered = sorted(filtered, key=_search_sort_key, reverse=True)
    elif backend_search_active:
        filtered = list(filtered)
    elif filters["sort_by"] == "Newest":
        filtered = sorted(
            filtered,
            key=lambda job: job.get("_posted_at_sort") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
    else:
        filtered = sorted(
            filtered,
            key=lambda job: float(job.get("_recommendation_sort") or 0.0),
            reverse=True,
        )
    return filtered


def render_job_detail_panel(
    job: dict[str, Any],
    *,
    page_key: str,
    on_close: Callable[[], None],
    on_save: Callable[[], None],
    on_apply: Callable[[], None],
    on_dismiss: Callable[[], None],
    dismiss_label: str = "Dismiss",
) -> None:
    st.markdown(
        """
        <style>
        .jorb-job-detail-shell {
            background: #ffffff;
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 1rem;
            padding: 1.05rem 1.05rem 1rem 1.05rem;
            overflow: hidden;
        }
        .jorb-job-detail-topline {
            display:flex;
            justify-content:space-between;
            gap:0.75rem;
            align-items:flex-start;
            flex-wrap:wrap;
            margin-bottom:0.32rem;
        }
        .jorb-job-detail-eyebrow {
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #475569;
        }
        .jorb-job-detail-badge {
            display:inline-flex;
            align-items:center;
            border-radius:999px;
            padding:0.22rem 0.58rem;
            background:#EEF4FF;
            border:1px solid #DBE7FF;
            color:#1E3A8A;
            font-size:0.74rem;
            font-weight:700;
        }
        .jorb-job-detail-title {
            font-size: 1.35rem;
            line-height: 1.25;
            font-weight: 700;
            color: #111827;
            margin-bottom: 0.18rem;
            overflow-wrap: anywhere;
        }
        .jorb-job-detail-subtitle {
            color:#334155;
            font-size:1rem;
            font-weight:600;
            line-height:1.4;
            margin-bottom:0.65rem;
            overflow-wrap: anywhere;
        }
        .jorb-job-detail-chip-row {
            display:flex;
            flex-wrap:wrap;
            gap:0.45rem;
            margin-bottom:0.85rem;
        }
        .jorb-job-detail-chip {
            display:inline-flex;
            align-items:center;
            border-radius:999px;
            padding:0.22rem 0.58rem;
            background:#F8FAFC;
            border:1px solid rgba(15, 23, 42, 0.08);
            color:#475569;
            font-size:0.78rem;
            font-weight:600;
            overflow-wrap:anywhere;
        }
        .jorb-job-detail-chip.status {
            background:#EFF6FF;
            color:#1D4ED8;
        }
        .jorb-job-detail-chip.muted {
            background:#F3F4F6;
            color:#4B5563;
            font-weight:500;
        }
        .jorb-job-detail-score {
            background: #f8fafc;
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 0.9rem;
            padding: 0.85rem 0.95rem;
            margin-bottom: 1rem;
            line-height: 1.5;
            overflow-wrap: anywhere;
        }
        .jorb-job-detail-callout {
            background:#FFFBEB;
            border:1px solid #FDE68A;
            border-radius:0.85rem;
            padding:0.8rem 0.9rem;
            margin-bottom:1rem;
        }
        .jorb-job-detail-callout-title {
            font-size:0.76rem;
            font-weight:700;
            letter-spacing:0.08em;
            text-transform:uppercase;
            color:#92400E;
            margin-bottom:0.24rem;
        }
        .jorb-job-detail-callout-copy {
            font-size:0.88rem;
            line-height:1.5;
            color:#78350F;
            overflow-wrap:anywhere;
        }
        .jorb-job-detail-section-title {
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 0.35rem;
        }
        .jorb-job-detail-section-copy {
            color: #374151;
            line-height: 1.6;
            overflow-wrap: anywhere;
            white-space: pre-wrap;
            margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(build_job_detail_panel_markup(job), unsafe_allow_html=True)
    action_cols = st.columns(4)
    if action_cols[0].button("Close", key=f"close-detail-{page_key}-{job['id']}", use_container_width=True):
        on_close()
    if action_cols[1].button("Save", key=f"detail-save-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "saved"):
        on_save()
    if action_cols[2].button("Apply", key=f"detail-apply-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "applied"):
        on_apply()
    if action_cols[3].button(dismiss_label, key=f"detail-dismiss-{page_key}-{job['id']}", use_container_width=True):
        on_dismiss()
    if job.get("url"):
        st.link_button("Open source", job["url"], use_container_width=True)


def render_jobs_screen(
    *,
    leads: list[dict[str, Any]],
    search_run: dict[str, Any] | None = None,
    search_meta: dict[str, Any] | None = None,
    page_key: str,
    title: str,
    empty_message: str,
    last_updated: datetime | None,
    run_manual_search_fn: Callable[[], dict[str, Any]] | None = None,
    send_feedback_fn: Callable[[int, str], None],
    dismiss_action: str = "dislike",
    dismiss_label: str = "Dismiss",
    intro_message: str | None = None,
    refresh_label: str = "Refresh jobs",
) -> None:
    jobs = [build_job_view_model(lead) for lead in leads]
    jobs_by_id = {job["id"]: job for job in jobs}
    filters = render_jobs_topbar(page_key=page_key, last_updated=last_updated, title=title, refresh_label=refresh_label)
    if filters["refresh"]:
        if title == "Jobs" and run_manual_search_fn is not None:
            feedback_key = f"jobs-manual-search-feedback-{page_key}"
            try:
                with st.spinner("Refreshing jobs..."):
                    result = run_manual_search_fn()
            except Exception as exc:
                st.error(f"Refresh failed: {exc}")
            else:
                st.session_state[feedback_key] = build_manual_search_feedback(result)
                st.session_state[f"jobs-last-updated-{page_key}"] = datetime.now(timezone.utc)
                st.rerun()
        else:
            st.session_state[f"jobs-last-updated-{page_key}"] = datetime.now(timezone.utc)
            st.rerun()

    filtered_jobs = _filter_jobs(jobs, filters, search_meta=search_meta)
    action_feedback_key = f"jobs-action-feedback-{page_key}"
    action_feedback = st.session_state.pop(action_feedback_key, None)
    if isinstance(action_feedback, dict):
        tone = str(action_feedback.get("tone") or "info")
        message = str(action_feedback.get("message") or "").strip()
        if message:
            getattr(st, tone if tone in {"success", "warning", "info", "error"} else "info")(message)
    if title == "Jobs":
        feedback_key = f"jobs-manual-search-feedback-{page_key}"
        feedback = st.session_state.pop(feedback_key, None)
        if isinstance(feedback, dict):
            tone = str(feedback.get("tone") or "info")
            message = str(feedback.get("message") or "").strip()
            if message:
                getattr(st, tone if tone in {"success", "warning", "info", "error"} else "info")(message)
    if title == "Jobs" or (search_meta and str(search_meta.get("query") or "").strip()):
        render_search_status_region(search_run, visible_job_count=len(filtered_jobs), search_meta=search_meta)
    elif intro_message:
        st.markdown(build_jobs_intro_state_markup(title=title, intro_message=intro_message), unsafe_allow_html=True)

    def _submit_feedback_action(lead_id: int, action: str) -> None:
        send_feedback_fn(lead_id, action)
        if action != "seen":
            st.session_state[action_feedback_key] = build_jobs_action_feedback(action)
        st.session_state[f"jobs-last-updated-{page_key}"] = datetime.now(timezone.utc)
        st.rerun()

    def _open_job(job: dict[str, Any]) -> None:
        st.session_state[f"selected-job-{page_key}"] = job["id"]
        if job.get("state") == "new":
            send_feedback_fn(job["lead_id"], "seen")
        st.session_state[f"jobs-last-updated-{page_key}"] = datetime.now(timezone.utc)
        st.rerun()

    selected_job_id = st.session_state.get(f"selected-job-{page_key}")
    selected_job = jobs_by_id.get(selected_job_id)
    if selected_job is not None and not any(job["id"] == selected_job_id for job in filtered_jobs):
        selected_job = None
    if selected_job is None and filtered_jobs:
        selected_job = filtered_jobs[0]
        st.session_state[f"selected-job-{page_key}"] = selected_job["id"]
    list_col, detail_col = st.columns([1.65, 1], gap="large")

    with list_col:
        if not filtered_jobs:
            if title == "Jobs":
                render_jobs_empty_state(
                    search_run,
                    total_job_count=len(jobs),
                    filters=filters,
                    page_key=page_key,
                    search_meta=search_meta,
                )
            else:
                st.markdown(
                    build_jobs_empty_state_markup(
                        {
                            "tone": "info",
                            "eyebrow": "Workspace",
                            "badge": title,
                            "title": empty_message,
                            "detail": intro_message or f"No jobs are currently visible in {title}.",
                        }
                    ),
                    unsafe_allow_html=True,
                )
            return
        for job in filtered_jobs:
            render_job_card(
                job,
                page_key=page_key,
                selected=selected_job is not None and selected_job["id"] == job["id"],
                on_open=lambda current_job=job: _open_job(current_job),
                on_save=lambda lead_id=job["lead_id"]: _submit_feedback_action(lead_id, "save"),
                on_apply=lambda lead_id=job["lead_id"]: _submit_feedback_action(lead_id, "applied"),
                on_dismiss=lambda lead_id=job["lead_id"], action=dismiss_action: _submit_feedback_action(lead_id, action),
                dismiss_label=dismiss_label,
            )

    with detail_col:
        if selected_job is None:
            st.markdown(build_jobs_detail_empty_state_markup(), unsafe_allow_html=True)
        else:
            render_job_detail_panel(
                selected_job,
                page_key=page_key,
                on_close=lambda: (st.session_state.pop(f"selected-job-{page_key}", None), st.rerun()),
                on_save=lambda lead_id=selected_job["lead_id"]: _submit_feedback_action(lead_id, "save"),
                on_apply=lambda lead_id=selected_job["lead_id"]: _submit_feedback_action(lead_id, "applied"),
                on_dismiss=lambda lead_id=selected_job["lead_id"], action=dismiss_action: _submit_feedback_action(lead_id, action),
                dismiss_label=dismiss_label,
            )
