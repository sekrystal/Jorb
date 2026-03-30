from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any, Callable

import pandas as pd
import streamlit as st

from ui.components.job_card import render_job_card
from ui.components.topbar import render_jobs_topbar


def _search_run_finished_with_zero_results(search_run: dict[str, Any]) -> bool:
    status = str(search_run.get("status") or "").strip().lower()
    if bool(search_run.get("zero_yield")):
        return True
    return status in {"empty", "zero_yield"}


def build_search_state_view_model(search_run: dict[str, Any] | None) -> dict[str, str]:
    if not search_run:
        return {
            "tone": "info",
            "title": "Search has not run yet.",
            "detail": "Run a manual search to load jobs into this view.",
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
            "title": "Search failed.",
            "detail": f"The latest run ended with {reason} at {created_label}.",
        }

    if status in {"queued", "running", "started"}:
        return {
            "tone": "info",
            "title": "Search is running.",
            "detail": "Jobs will update here when the current run finishes.",
        }

    if _search_run_finished_with_zero_results(search_run):
        return {
            "tone": "warning",
            "title": "Search finished with no matching jobs.",
            "detail": f"The latest run checked {query_count} quer{'y' if query_count == 1 else 'ies'} at {created_label}.",
        }

    return {
        "tone": "success",
        "title": "Search finished successfully.",
        "detail": (
            f"The latest run found {result_count} job{'s' if result_count != 1 else ''} "
            f"across {query_count} quer{'y' if query_count == 1 else 'ies'} at {created_label}."
        ),
    }


def build_manual_search_feedback(sync_result: dict[str, Any]) -> dict[str, str]:
    surfaced_count = int(sync_result.get("surfaced_count") or 0)
    summary = str(sync_result.get("discovery_summary") or "").strip()
    if surfaced_count > 0:
        tone = "success"
    elif summary:
        tone = "warning"
    else:
        tone = "info"
    message = f"Manual search finished. Surfaced {surfaced_count} job{'s' if surfaced_count != 1 else ''}."
    if summary:
        message = f"{message} {summary}"
    return {"tone": tone, "message": message}


def render_search_status_region(search_run: dict[str, Any] | None, *, visible_job_count: int) -> None:
    search_state = build_search_state_view_model(search_run)
    palette = {
        "info": {"background": "#F8FAFC", "border": "#CBD5E1", "accent": "#334155"},
        "success": {"background": "#F0FDF4", "border": "#BBF7D0", "accent": "#166534"},
        "warning": {"background": "#FFFBEB", "border": "#FDE68A", "accent": "#92400E"},
        "error": {"background": "#FEF2F2", "border": "#FECACA", "accent": "#B91C1C"},
    }[search_state["tone"]]
    count_label = f"{visible_job_count} job{'s' if visible_job_count != 1 else ''} in view"
    title = escape(search_state["title"])
    detail = escape(search_state["detail"])
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
          <div style="display:flex; justify-content:space-between; gap:1rem; align-items:flex-start;">
            <div>
              <div style="font-size:0.98rem; font-weight:600; color:#111827;">{title}</div>
              <div style="margin-top:0.2rem; font-size:0.9rem; color:#4B5563;">{detail}</div>
            </div>
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
        """,
        unsafe_allow_html=True,
    )


def build_jobs_empty_state_view_model(
    search_run: dict[str, Any] | None,
    *,
    total_job_count: int,
    filters: dict[str, Any],
) -> dict[str, Any]:
    has_filters = bool(filters["search"].strip() or filters["location"].strip() or filters["remote_only"])
    if total_job_count > 0 and has_filters:
        return {
            "title": "No jobs match the current filters.",
            "detail": "Clear filters or adjust the current search terms to see the latest jobs in this list.",
            "show_clear_filters": True,
        }

    search_state = build_search_state_view_model(search_run)
    result_count = int((search_run or {}).get("result_count") or 0)
    if search_state["tone"] == "success" and result_count > 0:
        return {
            "title": "Search finished, but no jobs are visible yet.",
            "detail": search_state["detail"],
            "show_clear_filters": False,
        }

    return {
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
) -> None:
    empty_state = build_jobs_empty_state_view_model(search_run, total_job_count=total_job_count, filters=filters)
    st.info(f"{empty_state['title']} {empty_state['detail']}".strip())
    if not empty_state["show_clear_filters"]:
        return
    if st.button("Clear filters", key=f"jobs-clear-filters-{page_key}"):
        st.session_state[f"jobs-search-{page_key}"] = ""
        st.session_state[f"jobs-location-{page_key}"] = ""
        st.session_state[f"jobs-remote-{page_key}"] = False
        st.rerun()


def _match_label(score_payload: dict[str, Any], lead: dict[str, Any]) -> str:
    band = (score_payload.get("recommendation_band") or lead.get("rank_label") or "").lower()
    if band == "strong":
        return "Strong Match"
    if band == "medium":
        return "Medium Match"
    return "Stretch"


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
    return "TODO: backend did not return a short description.", True


def _full_description_from_evidence(evidence: dict[str, Any]) -> tuple[str, bool]:
    description_text = (evidence.get("description_text") or "").strip()
    if description_text:
        return description_text, False
    snippets = [snippet.strip() for snippet in (evidence.get("snippets") or []) if snippet and snippet.strip()]
    if snippets:
        return "\n\n".join(snippets), False
    return "TODO: backend did not return a full description.", True


def _work_mode_from_evidence(evidence: dict[str, Any]) -> tuple[str, bool]:
    location = (evidence.get("location") or "").strip().lower()
    location_scope = (evidence.get("location_scope") or "").strip().lower()
    if "remote" in location or location_scope.startswith("remote"):
        return "remote", False
    return "TODO work mode", True


def _source_fields(lead: dict[str, Any], evidence: dict[str, Any]) -> tuple[str, str]:
    source = (
        lead.get("source_type")
        or evidence.get("source_type")
        or lead.get("source_platform")
        or evidence.get("source_platform")
        or "unknown"
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
    state = "applied" if lead.get("applied") else "saved" if lead.get("saved") else "new"
    tags = [
        item
        for item in [
            lead.get("freshness_label"),
            lead.get("qualification_fit_label"),
            lead.get("confidence_label"),
            source_provenance,
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
    return {
        "id": str(lead["id"]),
        "lead_id": lead["id"],
        "title": lead.get("primary_title") or "TODO title",
        "company": lead.get("company_name") or "TODO company",
        "location": evidence.get("location") or "TODO location",
        "work_mode": work_mode,
        "description": description,
        "full_description": full_description,
        "match_score_display": _match_score_display(score_payload),
        "match_label": _match_label(score_payload, lead),
        "explanation": (
            (score_payload.get("explanation") or {}).get("headline")
            or (score_payload.get("explanation") or {}).get("summary")
            or lead.get("explanation")
            or "TODO: backend did not return a recommendation explanation."
        ),
        "tags": tags,
        "posted_date": lead.get("posted_at") or lead.get("surfaced_at") or "Unknown date",
        "salary": evidence.get("salary") or None,
        "source": source,
        "source_provenance": source_provenance,
        "state": state,
        "why_this_job": (score_payload.get("explanation") or {}).get("summary") or lead.get("explanation"),
        "what_you_are_missing": (
            "Qualification fit is stretch." if lead.get("qualification_fit_label") == "stretch" else
            "Qualification fit is unclear." if lead.get("qualification_fit_label") == "unclear" else
            None
        ),
        "suggested_next_steps": score_payload.get("action_explanation") or "TODO: backend did not return suggested next steps.",
        "url": lead.get("url"),
        "backend_gaps": gaps,
        "raw_lead": lead,
    }


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


def _filter_jobs(jobs: list[dict[str, Any]], filters: dict[str, Any]) -> list[dict[str, Any]]:
    filtered = jobs
    if filters["search"].strip():
        term = filters["search"].strip().lower()
        filtered = [
            job for job in filtered
            if term in (job.get("title") or "").lower() or term in (job.get("company") or "").lower()
        ]
    if filters["location"].strip():
        location_term = filters["location"].strip().lower()
        filtered = [job for job in filtered if location_term in (job.get("location") or "").lower()]
    if filters["remote_only"]:
        filtered = [job for job in filtered if job.get("work_mode") == "remote"]
    if filters["sort_by"] == "Newest":
        filtered = sorted(
            filtered,
            key=lambda job: _parse_timestamp(job["raw_lead"].get("posted_at") or job["raw_lead"].get("surfaced_at")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
    else:
        filtered = sorted(
            filtered,
            key=lambda job: float((job["raw_lead"].get("score_breakdown_json") or {}).get("final_score", 0.0) or 0.0),
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
) -> None:
    st.markdown("### Job detail")
    st.markdown(f"**{job['title']}**")
    st.caption(f"{job['company']} • {job['location']} • {job['work_mode']}")
    st.caption(f"Source: {job['source']} • Provenance: {job['source_provenance']}")
    stat_cols = st.columns(2)
    stat_cols[0].metric("Match", job["match_score_display"])
    stat_cols[1].metric("Label", job["match_label"])
    action_cols = st.columns(4)
    if action_cols[0].button("Close", key=f"close-detail-{page_key}-{job['id']}", use_container_width=True):
        on_close()
    if action_cols[1].button("Save", key=f"detail-save-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "saved"):
        on_save()
    if action_cols[2].button("Apply", key=f"detail-apply-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "applied"):
        on_apply()
    if action_cols[3].button("Dismiss", key=f"detail-dismiss-{page_key}-{job['id']}", use_container_width=True):
        on_dismiss()
    if job.get("url"):
        st.link_button("Open source", job["url"], use_container_width=True)
    st.markdown("#### Recommendation")
    st.write(job["explanation"])
    st.markdown("#### Why this job")
    st.write(job.get("why_this_job") or "TODO: backend did not return a detailed rationale.")
    st.markdown("#### What you are missing")
    st.write(job.get("what_you_are_missing") or "No explicit gap recorded.")
    st.markdown("#### Suggested next steps")
    st.write(job.get("suggested_next_steps") or "TODO: backend did not return next steps.")
    st.markdown("#### Full description")
    st.write(job["full_description"])
    if job.get("backend_gaps"):
        st.warning("Backend/UI contract gaps for this card: " + ", ".join(job["backend_gaps"]))


def render_jobs_screen(
    *,
    leads: list[dict[str, Any]],
    search_run: dict[str, Any] | None = None,
    page_key: str,
    title: str,
    empty_message: str,
    last_updated: datetime | None,
    run_manual_search_fn: Callable[[], dict[str, Any]] | None = None,
    send_feedback_fn: Callable[[int, str], None],
) -> None:
    jobs = [build_job_view_model(lead) for lead in leads]
    filters = render_jobs_topbar(page_key=page_key, last_updated=last_updated)
    if filters["refresh"]:
        st.session_state[f"jobs-last-updated-{page_key}"] = datetime.now(timezone.utc)
        st.rerun()

    filtered_jobs = _filter_jobs(jobs, filters)
    gap_frame = jobs_backend_gap_frame(filtered_jobs)
    st.markdown(f"### {title}")
    if title == "Jobs":
        feedback_key = f"jobs-manual-search-feedback-{page_key}"
        feedback = st.session_state.pop(feedback_key, None)
        if isinstance(feedback, dict):
            tone = str(feedback.get("tone") or "info")
            message = str(feedback.get("message") or "").strip()
            if message:
                getattr(st, tone if tone in {"success", "warning", "info", "error"} else "info")(message)
        render_search_status_region(search_run, visible_job_count=len(filtered_jobs))
        if run_manual_search_fn is not None and st.button("Run manual search", key=f"jobs-manual-search-{page_key}"):
            try:
                with st.spinner("Running manual search..."):
                    result = run_manual_search_fn()
            except Exception as exc:
                st.error(f"Manual search failed: {exc}")
            else:
                st.session_state[feedback_key] = build_manual_search_feedback(result)
                st.session_state[f"jobs-last-updated-{page_key}"] = datetime.now(timezone.utc)
                st.rerun()
    if not gap_frame.empty:
        with st.expander("Backend/UI field gaps", expanded=False):
            st.dataframe(gap_frame, use_container_width=True, hide_index=True)

    selected_job_id = st.session_state.get(f"selected-job-{page_key}")
    selected_job = next((job for job in filtered_jobs if job["id"] == selected_job_id), None)
    list_col, detail_col = st.columns([1.65, 1], gap="large")

    with list_col:
        if not filtered_jobs:
            if title == "Jobs":
                render_jobs_empty_state(
                    search_run,
                    total_job_count=len(jobs),
                    filters=filters,
                    page_key=page_key,
                )
            else:
                st.info(empty_message)
            return
        for job in filtered_jobs:
            render_job_card(
                job,
                page_key=page_key,
                on_open=lambda job_id=job["id"]: st.session_state.__setitem__(f"selected-job-{page_key}", job_id),
                on_save=lambda lead_id=job["lead_id"]: (send_feedback_fn(lead_id, "save"), st.rerun()),
                on_apply=lambda lead_id=job["lead_id"]: (send_feedback_fn(lead_id, "applied"), st.rerun()),
                on_dismiss=lambda lead_id=job["lead_id"]: (send_feedback_fn(lead_id, "dislike"), st.rerun()),
            )

    with detail_col:
        if selected_job is None:
            st.info("Select a job card and open details to see the right-hand panel.")
        else:
            render_job_detail_panel(
                selected_job,
                page_key=page_key,
                on_close=lambda: (st.session_state.pop(f"selected-job-{page_key}", None), st.rerun()),
                on_save=lambda lead_id=selected_job["lead_id"]: (send_feedback_fn(lead_id, "save"), st.rerun()),
                on_apply=lambda lead_id=selected_job["lead_id"]: (send_feedback_fn(lead_id, "applied"), st.rerun()),
                on_dismiss=lambda lead_id=selected_job["lead_id"]: (send_feedback_fn(lead_id, "dislike"), st.rerun()),
            )
