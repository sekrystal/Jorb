from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.document_ingest import preview_resume_text, preview_resume_upload
from services.profile_ingest import build_profile_review_rows


API_BASE_URL = os.getenv("OPPORTUNITY_SCOUT_API_URL", "http://127.0.0.1:8000")
APPLICATION_STATUSES = [
    "saved",
    "applied",
    "recruiter screen",
    "hiring manager",
    "interview loop",
    "final round",
    "rejected",
    "offer",
    "archived",
]

SORT_OPTIONS = [
    "Newest surfaced",
    "Newest posted",
    "Company A-Z",
    "Title A-Z",
    "Freshest first",
    "Best fit first",
    "Highest confidence first",
    "Status",
]

FRESHNESS_ORDER = {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}
FIT_ORDER = {"strong fit": 0, "adjacent": 1, "stretch": 2, "unclear": 3, "overqualified": 4, "underqualified": 5}
CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
STATUS_ORDER = {status: index for index, status in enumerate(APPLICATION_STATUSES)}


class TableFilters(dict):
    search: str
    lead_type: str
    freshness: str
    fit: str
    status: str
    surfaced_since: Optional[date]
    surfaced_until: Optional[date]
    posted_since: Optional[date]
    posted_until: Optional[date]
    sort_mode: str


def fetch_json(path: str, method: str = "GET", payload: Optional[dict] = None) -> Any:
    try:
        timeout = 10 if path.startswith("/leads") else 30
        response = requests.request(method, f"{API_BASE_URL}{path}", json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        if path.startswith("/leads"):
            st.error(f"Leads request failed while loading `{path}`: {exc}. The rest of the page is still available.")
            return {"items": []}
        raise


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def get_profile_form_source(profile: dict[str, Any], latest_resume_ingest: Optional[dict[str, Any]]) -> dict[str, Any]:
    if latest_resume_ingest and latest_resume_ingest.get("candidate_profile"):
        return latest_resume_ingest["candidate_profile"]
    return profile


def build_profile_update_payload(
    profile: dict[str, Any],
    form_source: dict[str, Any],
    form_values: dict[str, Any],
) -> dict[str, Any]:
    return {
        "profile_schema_version": form_source.get("profile_schema_version", profile.get("profile_schema_version", "v1")),
        "name": form_values["name"],
        "raw_resume_text": form_source.get("raw_resume_text", profile.get("raw_resume_text", "")),
        "extracted_summary_json": form_source.get("extracted_summary_json", profile.get("extracted_summary_json", {})),
        "preferred_titles_json": parse_csv(form_values["preferred_titles"]),
        "adjacent_titles_json": parse_csv(form_values["adjacent_titles"]),
        "excluded_titles_json": parse_csv(form_values["excluded_titles"]),
        "preferred_domains_json": parse_csv(form_values["preferred_domains"]),
        "excluded_companies_json": parse_csv(form_values["excluded_companies"]),
        "preferred_locations_json": parse_csv(form_values["preferred_locations"]),
        "confirmed_skills_json": parse_csv(form_values["confirmed_skills"]),
        "competencies_json": parse_csv(form_values["competencies"]),
        "explicit_preferences_json": parse_csv(form_values["explicit_preferences"]),
        "seniority_guess": form_source.get("seniority_guess", profile.get("seniority_guess")),
        "stage_preferences_json": parse_csv(form_values["stage_preferences"]),
        "core_titles_json": parse_csv(form_values["core_titles"]),
        "excluded_keywords_json": parse_csv(form_values["excluded_keywords"]),
        "min_seniority_band": form_values["min_seniority_band"],
        "max_seniority_band": form_values["max_seniority_band"],
        "stretch_role_families_json": parse_csv(form_values["stretch_role_families"]),
        "minimum_fit_threshold": form_values["minimum_fit_threshold"],
    }


def discovery_query_family_frame(cycle_metrics: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for query_family, metrics in sorted((cycle_metrics.get("query_family_metrics") or {}).items()):
        row = {"query_family": query_family}
        row.update(metrics or {})
        rows.append(row)
    return pd.DataFrame(rows)


def format_timestamp(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except ValueError:
        return str(value)


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def runtime_surface_payload(runtime: dict[str, Any], health: dict[str, Any], digest: dict[str, Any]) -> dict[str, Any]:
    latest_success_summary = health.get("latest_success_summary") or runtime.get("last_cycle_summary") or digest.get("summary")
    latest_failure_summary = health.get("latest_failure_summary") or runtime.get("latest_failure_summary")
    operator_hints = health.get("operator_hints") or runtime.get("operator_hints") or []
    return {
        "runtime_phase": health.get("runtime_phase") or runtime.get("runtime_phase") or runtime.get("worker_state") or "idle",
        "latest_success_summary": latest_success_summary,
        "latest_failure_summary": latest_failure_summary,
        "operator_hints": operator_hints,
    }


def build_query(
    freshness_days: int,
    include_hidden: bool,
    include_unqualified: bool,
    lead_type: str = "all",
    only_saved: bool = False,
    only_applied: bool = False,
    include_signal_only: bool = False,
) -> str:
    params = [
        f"freshness_window_days={freshness_days}",
        f"include_hidden={'true' if include_hidden else 'false'}",
        f"include_unqualified={'true' if include_unqualified else 'false'}",
        f"include_signal_only={'true' if include_signal_only else 'false'}",
    ]
    if lead_type != "all":
        params.append(f"lead_type={lead_type}")
    if only_saved:
        params.append("only_saved=true")
    if only_applied:
        params.append("only_applied=true")
    return "/leads?" + "&".join(params)


def get_runtime_control() -> dict[str, Any]:
    return fetch_json("/runtime-control")


def set_runtime_control(action: str) -> dict[str, Any]:
    return fetch_json("/runtime-control", method="POST", payload={"action": action})


def send_feedback(lead_id: int, action: str, pattern: Optional[str] = None) -> None:
    fetch_json("/feedback", method="POST", payload={"lead_id": lead_id, "action": action, "pattern": pattern})


def update_application_status(lead_id: int, current_status: str, notes: str, date_applied_value: Optional[date]) -> None:
    payload: dict[str, Any] = {"lead_id": lead_id, "current_status": current_status, "notes": notes or None}
    if date_applied_value:
        payload["date_applied"] = datetime.combine(date_applied_value, datetime.min.time()).isoformat()
    fetch_json("/applications/status", method="POST", payload=payload)


def lead_frame(leads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for lead in leads:
        rows.append(
            {
                "lead_id": lead["id"],
                "open_url": lead.get("url") or "",
                "surfaced_at_raw": parse_timestamp(lead.get("surfaced_at")),
                "posted_at_raw": parse_timestamp(lead.get("posted_at")),
                "updated_at_raw": parse_timestamp(lead.get("application_updated_at")),
                "surfaced_at": format_timestamp(lead.get("surfaced_at")),
                "posted_at": format_timestamp(lead.get("posted_at")),
                "company": lead["company_name"],
                "title": lead["primary_title"],
                "lead_type": lead["lead_type"],
                "freshness": lead["freshness_label"],
                "fit": lead["qualification_fit_label"],
                "confidence": lead["confidence_label"],
                "current_status": lead.get("current_status") or "",
                "source": lead.get("source_platform") or lead.get("source_type") or "",
                "provenance": lead.get("source_lineage") or lead.get("source_platform") or lead.get("source_type") or "",
                "change": (lead.get("evidence_json") or {}).get("change_state") or "",
                "last_agent_action": lead.get("last_agent_action") or "",
                "saved": "yes" if lead.get("saved") else "",
                "applied": "yes" if lead.get("applied") else "",
                "date_saved": format_timestamp(lead.get("date_saved")),
                "date_applied": format_timestamp(lead.get("date_applied")),
                "notes": lead.get("application_notes") or "",
                "updated_at": format_timestamp(lead.get("application_updated_at")),
                "next_action": lead.get("next_action") or "",
                "follow_up_due": "yes" if lead.get("follow_up_due") else "",
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["surfaced_at_raw"] = pd.to_datetime(frame["surfaced_at_raw"], errors="coerce", utc=True)
    frame["posted_at_raw"] = pd.to_datetime(frame["posted_at_raw"], errors="coerce", utc=True)
    frame["updated_at_raw"] = pd.to_datetime(frame["updated_at_raw"], errors="coerce", utc=True)
    return frame


def filter_and_sort_table(table: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    if table.empty:
        return table

    filtered = table.copy()
    if filters["search"].strip():
        term = filters["search"].strip()
        filtered = filtered.loc[
            filtered["company"].str.contains(term, case=False, na=False)
            | filtered["title"].str.contains(term, case=False, na=False)
        ]
    if filters["lead_type"] != "all":
        filtered = filtered.loc[filtered["lead_type"] == filters["lead_type"]]
    if filters["freshness"] != "all":
        filtered = filtered.loc[filtered["freshness"] == filters["freshness"]]
    if filters["fit"] != "all":
        filtered = filtered.loc[filtered["fit"] == filters["fit"]]
    if filters["status"] != "all":
        filtered = filtered.loc[filtered["current_status"] == filters["status"]]
    if filters["surfaced_since"]:
        filtered = filtered.loc[filtered["surfaced_at_raw"].notna() & filtered["surfaced_at_raw"].dt.date.ge(filters["surfaced_since"])]
    if filters["surfaced_until"]:
        filtered = filtered.loc[filtered["surfaced_at_raw"].notna() & filtered["surfaced_at_raw"].dt.date.le(filters["surfaced_until"])]
    if filters["posted_since"]:
        filtered = filtered.loc[filtered["posted_at_raw"].notna() & filtered["posted_at_raw"].dt.date.ge(filters["posted_since"])]
    if filters["posted_until"]:
        filtered = filtered.loc[filtered["posted_at_raw"].notna() & filtered["posted_at_raw"].dt.date.le(filters["posted_until"])]

    sort_mode = filters["sort_mode"]
    if sort_mode == "Newest surfaced":
        filtered = filtered.sort_values(by=["surfaced_at_raw", "company"], ascending=[False, True], na_position="last")
    elif sort_mode == "Newest posted":
        filtered = filtered.sort_values(by=["posted_at_raw", "surfaced_at_raw"], ascending=[False, False], na_position="last")
    elif sort_mode == "Company A-Z":
        filtered = filtered.sort_values(by=["company", "title"], ascending=[True, True], na_position="last")
    elif sort_mode == "Title A-Z":
        filtered = filtered.sort_values(by=["title", "company"], ascending=[True, True], na_position="last")
    elif sort_mode == "Freshest first":
        filtered = filtered.assign(_freshness_sort=filtered["freshness"].map(FRESHNESS_ORDER).fillna(99))
        filtered = filtered.sort_values(by=["_freshness_sort", "surfaced_at_raw"], ascending=[True, False], na_position="last").drop(columns="_freshness_sort")
    elif sort_mode == "Best fit first":
        filtered = filtered.assign(_fit_sort=filtered["fit"].map(FIT_ORDER).fillna(99))
        filtered = filtered.sort_values(by=["_fit_sort", "surfaced_at_raw"], ascending=[True, False], na_position="last").drop(columns="_fit_sort")
    elif sort_mode == "Highest confidence first":
        filtered = filtered.assign(_confidence_sort=filtered["confidence"].map(CONFIDENCE_ORDER).fillna(99))
        filtered = filtered.sort_values(by=["_confidence_sort", "surfaced_at_raw"], ascending=[True, False], na_position="last").drop(columns="_confidence_sort")
    elif sort_mode == "Status":
        filtered = filtered.assign(_status_sort=filtered["current_status"].map(STATUS_ORDER).fillna(99))
        filtered = filtered.sort_values(by=["_status_sort", "company"], ascending=[True, True], na_position="last").drop(columns="_status_sort")
    return filtered


def apply_table_controls(table: pd.DataFrame, key: str) -> pd.DataFrame:
    st.caption("Search, filter, and sort the workbench. Every control below is wired to the live table state.")
    row1 = st.columns(6)
    row2 = st.columns(4)
    filters = {
        "search": row1[0].text_input("Search title or company", key=f"search-{key}"),
        "lead_type": row1[1].selectbox("Lead type", ["all", "combined", "listing", "signal"], key=f"type-{key}"),
        "freshness": row1[2].selectbox("Freshness", ["all", "fresh", "recent", "stale", "unknown"], key=f"fresh-{key}"),
        "fit": row1[3].selectbox("Fit", ["all", "strong fit", "stretch", "unclear", "overqualified", "underqualified"], key=f"fit-{key}"),
        "status": row1[4].selectbox("Status", ["all", *APPLICATION_STATUSES], key=f"status-{key}"),
        "sort_mode": row1[5].selectbox("Sort", SORT_OPTIONS, key=f"sort-{key}"),
        "surfaced_since": row2[0].date_input("Surfaced since", value=None, key=f"surfaced-since-{key}"),
        "surfaced_until": row2[1].date_input("Surfaced until", value=None, key=f"surfaced-until-{key}"),
        "posted_since": row2[2].date_input("Posted since", value=None, key=f"posted-since-{key}"),
        "posted_until": row2[3].date_input("Posted until", value=None, key=f"posted-until-{key}"),
    }
    return filter_and_sort_table(table, filters)


def render_table(leads: list[dict[str, Any]], key: str, applied_view: bool = False) -> Optional[dict[str, Any]]:
    table = lead_frame(leads)
    if table.empty:
        st.info("No rows in this view.")
        return None

    filtered = apply_table_controls(table, key)
    if filtered.empty:
        st.info("No rows match the current filters.")
        return None

    columns = [
        "open_url",
        "surfaced_at",
        "posted_at",
        "company",
        "title",
        "provenance",
        "lead_type",
        "freshness",
        "fit",
        "confidence",
        "current_status",
        "source",
        "change",
        "last_agent_action",
    ]
    if key == "saved":
        columns.insert(9, "date_saved")
    if applied_view:
        columns.extend(["date_applied", "notes", "next_action", "follow_up_due", "updated_at"])

    event = st.dataframe(
        filtered[columns],
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "open_url": st.column_config.LinkColumn("Open", display_text="open", validate="^https?://"),
            "surfaced_at": "Surfaced",
            "posted_at": "Posted",
            "lead_type": "Type",
            "provenance": "Provenance",
            "freshness": "Freshness",
            "fit": "Fit",
            "confidence": "Confidence",
            "current_status": "Status",
            "last_agent_action": "Last agent action",
            "change": "Changed",
            "date_saved": "Saved on",
            "date_applied": "Applied on",
            "updated_at": "Updated",
            "next_action": "Next action",
            "follow_up_due": "Due",
        },
    )
    selected_rows = event.selection.rows if event is not None else []
    selected_index = selected_rows[0] if selected_rows else 0
    lead_id = int(filtered.iloc[selected_index]["lead_id"])
    return next(item for item in leads if item["id"] == lead_id)


def render_detail(lead: dict[str, Any], key: str) -> None:
    evidence = lead.get("evidence_json", {})
    agent_actions = evidence.get("agent_actions", [])
    critic_status = evidence.get("critic_status", "unknown")
    critic_reasons = evidence.get("critic_reasons", [])
    liveness = evidence.get("liveness_evidence", {})
    ai_fit = evidence.get("ai_fit_assessment") or {}
    ai_critic = evidence.get("ai_critic_assessment") or {}

    st.divider()
    st.subheader(f"{lead['company_name']} — {lead['primary_title']}")
    st.write(lead.get("explanation") or "No explanation recorded.")

    summary = st.columns(6)
    summary[0].write(f"Type: `{lead['lead_type']}`")
    summary[1].write(f"Freshness: `{lead['freshness_label']}`")
    summary[2].write(f"Fit: `{lead['qualification_fit_label']}`")
    summary[3].write(f"Confidence: `{lead['confidence_label']}`")
    summary[4].write(f"Critic: `{critic_status}`")
    summary[5].write(f"Last agent action: `{lead.get('last_agent_action') or 'none'}`")
    change_state = evidence.get("change_state")
    if change_state:
        st.caption(f"Change marker: {change_state}")
    st.caption(
        f"Source: {lead.get('source_platform') or lead.get('source_type')} | Provenance: {lead.get('source_lineage') or lead.get('source_platform') or lead.get('source_type')} | URL: {lead.get('url') or 'none'} | Application status: {lead.get('current_status') or 'unsaved'}"
    )

    action_row = st.columns(6)
    if lead.get("url"):
        action_row[0].link_button("Open source", lead["url"], use_container_width=True)
    else:
        action_row[0].button("No source", disabled=True, use_container_width=True, key=f"no-source-{key}-{lead['id']}")
    if action_row[1].button("Relevant", use_container_width=True, key=f"like-{key}-{lead['id']}"):
        send_feedback(lead["id"], "like")
        st.rerun()
    if action_row[2].button("Not relevant", use_container_width=True, key=f"dislike-{key}-{lead['id']}"):
        send_feedback(lead["id"], "dislike")
        st.rerun()
    if action_row[3].button("Save", use_container_width=True, key=f"save-{key}-{lead['id']}"):
        send_feedback(lead["id"], "save")
        st.rerun()
    if action_row[4].button("Apply", use_container_width=True, key=f"apply-{key}-{lead['id']}"):
        send_feedback(lead["id"], "applied")
        st.rerun()
    mute_pattern = action_row[5].text_input("Mute title pattern", value=lead["primary_title"], key=f"mute-pattern-{key}-{lead['id']}")
    mute_row = st.columns(2)
    if mute_row[0].button("Mute company", use_container_width=True, key=f"mute-company-{key}-{lead['id']}"):
        send_feedback(lead["id"], "mute_company")
        st.rerun()
    if mute_row[1].button("Mute title pattern", use_container_width=True, key=f"mute-title-{key}-{lead['id']}"):
        send_feedback(lead["id"], "mute_title_pattern", pattern=mute_pattern)
        st.rerun()

    with st.expander("Why this surfaced", expanded=True):
        st.write(f"Matched profile: {', '.join(evidence.get('matched_profile_fields', [])) or 'scope-based fit'}")
        st.write(f"Source type: {lead['lead_type']}")
        st.write(f"Freshness context: {lead['freshness_label']}")
        st.write(f"Fit context: {lead['qualification_fit_label']}")
        st.write(f"Critic decision: {critic_status}")
        st.write(f"Feedback influence: {', '.join(evidence.get('feedback_notes', [])) or 'no material feedback yet'}")
        if ai_fit:
            st.write(f"AI fit assessment: {ai_fit.get('classification', 'unknown')}")
            if ai_fit.get("reasons"):
                st.caption("AI fit reasons: " + "; ".join(ai_fit["reasons"]))
        if critic_reasons:
            st.write(f"Critic reasons: {'; '.join(critic_reasons)}")
        if ai_critic:
            st.caption("AI critic: " + "; ".join(ai_critic.get("reasons", [])))
        if liveness:
            st.write(
                "Liveness evidence: "
                f"status={liveness.get('listing_status')}, "
                f"freshness_hours={liveness.get('freshness_hours')}, "
                f"freshness_days={liveness.get('freshness_days')}, "
                f"expiration_confidence={liveness.get('expiration_confidence')}, "
                f"http_status={liveness.get('http_status') or 'n/a'}"
            )
            st.caption(
                "Timestamps: "
                f"posted={format_timestamp(lead.get('posted_at')) or 'n/a'} | "
                f"first_published={format_timestamp(evidence.get('first_published_at')) or 'n/a'} | "
                f"discovered={format_timestamp(evidence.get('discovered_at')) or 'n/a'} | "
                f"last_seen={format_timestamp(evidence.get('last_seen_at')) or 'n/a'} | "
                f"updated={format_timestamp(evidence.get('updated_at')) or 'n/a'}"
            )
        if evidence.get("resolution_story"):
            st.write("Resolution story:")
            for item in evidence["resolution_story"]:
                st.caption(item)
        for snippet in evidence.get("snippets", [])[:3]:
            st.caption(snippet)

    with st.expander("Agent trace", expanded=False):
        if not agent_actions:
            st.caption("No agent actions recorded yet.")
        else:
            trace_df = pd.DataFrame(agent_actions)
            st.dataframe(trace_df, use_container_width=True, hide_index=True)

    with st.expander("Application tracker", expanded=bool(lead.get("applied"))):
        cols = st.columns([1.2, 1.2, 2.0])
        current_status = lead.get("current_status") or "saved"
        next_status = cols[0].selectbox(
            "Status",
            APPLICATION_STATUSES,
            index=APPLICATION_STATUSES.index(current_status) if current_status in APPLICATION_STATUSES else 0,
            key=f"status-{key}-{lead['id']}",
        )
        applied_default = parse_timestamp(lead.get("date_applied"))
        applied_date = cols[1].date_input(
            "Date applied",
            value=applied_default.date() if applied_default else date.today(),
            key=f"applied-date-{key}-{lead['id']}",
        )
        notes = cols[2].text_input(
            "Notes",
            value=lead.get("application_notes") or "",
            key=f"notes-{key}-{lead['id']}",
        )
        if st.button("Update tracker", use_container_width=True, key=f"tracker-{key}-{lead['id']}"):
            update_application_status(
                lead["id"],
                current_status=next_status,
                notes=notes,
                date_applied_value=applied_date if next_status != "saved" else None,
            )
            st.rerun()
        if lead.get("next_action"):
            st.info(f"Next action: {lead['next_action']}")


def render_profile_tab(profile: dict[str, Any], learning: dict[str, Any]) -> None:
    st.subheader("Resume and profile")
    upload = st.file_uploader("Upload resume PDF, TXT, or MD", type=["pdf", "txt", "md"])
    pasted_resume = st.text_area("Paste resume text", height=120)
    if st.button("Parse resume", use_container_width=True):
        try:
            if upload is not None:
                preview = preview_resume_upload(upload.name, upload.getvalue())
                response = fetch_json("/resume", method="POST", payload={"filename": upload.name, "raw_text": preview["raw_text"]})
            elif pasted_resume.strip():
                preview = preview_resume_text("pasted_resume.txt", pasted_resume.strip())
                response = fetch_json("/resume", method="POST", payload={"filename": "pasted_resume.txt", "raw_text": preview["raw_text"]})
            else:
                st.warning("Upload a resume or paste text first.")
                return
            st.session_state["latest_resume_ingest"] = {
                "filename": preview["filename"],
                "status": preview["status"],
                "warnings": [*preview["warnings"], *response.get("warnings", [])],
                "missing_fields": preview["missing_fields"],
                "matched_terms": preview["matched_terms"],
                "text_preview": preview["text_preview"],
                "candidate_profile": response.get("candidate_profile", preview["candidate_profile"]),
            }
            st.rerun()
        except Exception as exc:
            st.error(f"Resume parsing failed: {exc}")

    latest_resume_ingest = st.session_state.get("latest_resume_ingest")
    review_profile = get_profile_form_source(profile, latest_resume_ingest)
    review_rows = build_profile_review_rows(review_profile)

    if latest_resume_ingest:
        st.markdown("#### Latest extraction")
        st.caption(f"Source: {latest_resume_ingest['filename']} | Status: {latest_resume_ingest['status']}")
        for warning in latest_resume_ingest["warnings"]:
            st.info(warning)
        if latest_resume_ingest["missing_fields"]:
            st.caption("Needs review: " + ", ".join(latest_resume_ingest["missing_fields"]))
        matched_terms = latest_resume_ingest.get("matched_terms") or {}
        for field_name, terms in matched_terms.items():
            if terms:
                st.caption(f"{field_name.replace('_', ' ').title()}: {', '.join(terms)}")
        if latest_resume_ingest.get("text_preview"):
            st.code(latest_resume_ingest["text_preview"], language="text")

    if review_rows:
        st.markdown("#### Extracted profile fields")
        st.dataframe(pd.DataFrame(review_rows), use_container_width=True, hide_index=True)

    with st.form("profile-form"):
        name = st.text_input("Profile name", value=review_profile.get("name", "Demo Candidate"))
        preferred_titles = st.text_input("Preferred titles", value=", ".join(review_profile.get("preferred_titles_json", [])))
        core_titles = st.text_input("Core titles", value=", ".join(review_profile.get("core_titles_json", [])))
        adjacent_titles = st.text_input("Adjacent titles", value=", ".join(review_profile.get("adjacent_titles_json", [])))
        excluded_titles = st.text_input("Excluded titles", value=", ".join(review_profile.get("excluded_titles_json", [])))
        preferred_domains = st.text_input("Preferred domains", value=", ".join(review_profile.get("preferred_domains_json", [])))
        preferred_locations = st.text_input("Preferred locations", value=", ".join(review_profile.get("preferred_locations_json", [])))
        excluded_companies = st.text_input("Excluded companies", value=", ".join(review_profile.get("excluded_companies_json", [])))
        confirmed_skills = st.text_input("Confirmed skills", value=", ".join(review_profile.get("confirmed_skills_json", [])))
        competencies = st.text_input("Competencies", value=", ".join(review_profile.get("competencies_json", [])))
        explicit_preferences = st.text_input("Explicit preferences", value=", ".join(review_profile.get("explicit_preferences_json", [])))
        stage_preferences = st.text_input("Preferred stages", value=", ".join(review_profile.get("stage_preferences_json", [])))
        stretch_role_families = st.text_input("Stretch role families", value=", ".join(review_profile.get("stretch_role_families_json", [])))
        excluded_keywords = st.text_input("Excluded keywords", value=", ".join(review_profile.get("excluded_keywords_json", [])))
        minimum_fit_threshold = st.number_input(
            "Minimum fit threshold",
            min_value=0.0,
            max_value=5.0,
            step=0.1,
            value=float(review_profile.get("minimum_fit_threshold", 2.8)),
        )
        bands = ["entry", "junior", "mid", "senior", "staff", "executive"]
        min_seniority_value = review_profile.get("min_seniority_band", "mid")
        max_seniority_value = review_profile.get("max_seniority_band", "senior")
        min_seniority = st.selectbox("Min seniority", bands, index=bands.index(min_seniority_value if min_seniority_value in bands else "mid"))
        max_seniority = st.selectbox("Max seniority", bands, index=bands.index(max_seniority_value if max_seniority_value in bands else "senior"))
        if st.form_submit_button("Save profile", use_container_width=True):
            payload = build_profile_update_payload(
                profile,
                review_profile,
                {
                    "name": name,
                    "preferred_titles": preferred_titles,
                    "adjacent_titles": adjacent_titles,
                    "excluded_titles": excluded_titles,
                    "preferred_domains": preferred_domains,
                    "excluded_companies": excluded_companies,
                    "preferred_locations": preferred_locations,
                    "confirmed_skills": confirmed_skills,
                    "competencies": competencies,
                    "explicit_preferences": explicit_preferences,
                    "stage_preferences": stage_preferences,
                    "core_titles": core_titles,
                    "excluded_keywords": excluded_keywords,
                    "min_seniority_band": min_seniority,
                    "max_seniority_band": max_seniority,
                    "stretch_role_families": stretch_role_families,
                    "minimum_fit_threshold": minimum_fit_threshold,
                },
            )
            fetch_json(
                "/candidate-profile",
                method="POST",
                payload=payload,
            )
            st.session_state.pop("latest_resume_ingest", None)
            st.rerun()

    st.caption(f"Profile schema: {profile.get('profile_schema_version', 'v1')}")
    st.caption(profile.get("extracted_summary_json", {}).get("summary", "No profile summary yet."))
    learning_df = pd.DataFrame(
        {
            "boosted_titles": [", ".join(learning.get("boosted_titles", [])) or ""],
            "boosted_domains": [", ".join(learning.get("boosted_domains", [])) or ""],
            "generated_queries": [", ".join(learning.get("generated_queries", [])) or ""],
        }
    )
    st.dataframe(learning_df, use_container_width=True, hide_index=True)


def render_agent_activity_tab() -> None:
    st.subheader("Agent Activity")
    st.caption("Control the autonomous worker and inspect what changed.")

    autonomy = fetch_json("/autonomy-status")
    runtime = get_runtime_control()
    health = autonomy.get("health", {})
    digest = autonomy.get("digest", {})
    runtime_surface = runtime_surface_payload(runtime, health, digest)
    summary = st.columns(6)
    summary[0].metric("Run state", runtime.get("run_state", "paused"))
    summary[1].metric("Worker state", runtime.get("worker_state", health.get("worker_state", "idle")))
    summary[2].metric("Run once queued", "yes" if runtime.get("run_once_requested") else "no")
    summary[3].metric("Last cycle start", format_timestamp(runtime.get("last_cycle_started_at")) or "never")
    summary[4].metric("Last cycle success", format_timestamp(runtime.get("last_successful_cycle_at")) or "never")
    summary[5].metric("Heartbeat", format_timestamp(runtime.get("last_heartbeat_at")) or "never")
    ops = st.columns(2)
    ops[0].metric("Open investigations", health.get("open_investigations", 0))
    ops[1].metric("Due follow-ups", health.get("due_follow_ups", 0))
    interval_cols = st.columns(4)
    interval_cols[0].metric("Runtime phase", runtime_surface["runtime_phase"])
    interval_cols[1].metric("Next cycle", format_timestamp(runtime.get("next_cycle_at")) or "pending")
    interval_cols[2].metric("Current interval", f"{runtime.get('current_interval_seconds', 0)}s")
    interval_cols[3].metric("Last control", runtime.get("last_control_action") or "none")
    if health.get("last_failed_run_at"):
        st.caption(f"Last failed run: {format_timestamp(health.get('last_failed_run_at'))}")
    if runtime.get("last_control_at"):
        st.caption(f"Last control change: {format_timestamp(runtime.get('last_control_at'))}")
    for hint in runtime_surface["operator_hints"]:
        st.caption(hint)
    if runtime.get("status_message"):
        st.caption(runtime["status_message"])
    if runtime_surface["latest_success_summary"]:
        st.info(f"Latest success: {runtime_surface['latest_success_summary']}")
    if runtime_surface["latest_failure_summary"]:
        st.warning(f"Latest failure: {runtime_surface['latest_failure_summary']}")

    if digest.get("summary"):
        with st.expander("Latest run summary", expanded=True):
            st.write(digest["summary"])
            if digest.get("new_leads"):
                st.caption(f"New leads: {', '.join(digest['new_leads'])}")
            if digest.get("suppressed_leads"):
                st.caption(f"Suppressed leads: {', '.join(digest['suppressed_leads'])}")
            if digest.get("investigations_changed"):
                st.caption(f"Investigations changed: {digest['investigations_changed']}")
            if digest.get("follow_ups_created"):
                st.caption(f"Follow-ups created: {', '.join(digest['follow_ups_created'])}")
            if digest.get("watchlist_changes"):
                st.caption(f"Watchlist changes: {', '.join(digest['watchlist_changes'])}")

    controls = st.columns(3)
    if controls[0].button("Play", use_container_width=True):
        set_runtime_control("play")
        st.rerun()
    if controls[1].button("Pause", use_container_width=True):
        set_runtime_control("pause")
        st.rerun()
    if controls[2].button("Run once", use_container_width=True):
        set_runtime_control("run_once")
        st.rerun()

    activity = fetch_json("/agent-activity")["items"]
    if not activity:
        st.info("No agent activity recorded yet.")
        return
    activity_df = pd.DataFrame(activity)
    activity_df["timestamp"] = activity_df["timestamp"].map(format_timestamp)
    st.dataframe(
        activity_df[["timestamp", "agent_name", "action", "target_count", "target_entity", "result_summary"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "agent_name": "Agent",
            "target_count": "Count",
            "target_entity": "Entity",
            "result_summary": "What changed",
        },
    )


def render_investigations_tab() -> None:
    st.subheader("Investigations")
    st.caption("Promising weak signals that still need more resolver work.")
    investigations = fetch_json("/investigations")["items"]
    if not investigations:
        st.info("No open investigations.")
        return
    df = pd.DataFrame(investigations)
    if not df.empty:
        df["next_step"] = df["status"].map(
            {
                "open": "Resolver will retry on the next cycle",
                "rechecking": "Automatic recheck is in progress",
                "resolved": "Resolved into a surfaced company or lead",
            }
        ).fillna("Needs operator review")
        status_order = {"open": 0, "rechecking": 1, "resolved": 2}
        df["_status_sort"] = df["status"].map(status_order).fillna(9)
        df = df.sort_values(by=["_status_sort", "confidence", "attempts"], ascending=[True, False, True]).drop(columns="_status_sort")
    if "next_check_at" in df:
        df["next_check_at"] = df["next_check_at"].map(format_timestamp)
    st.dataframe(
        df[["company_guess", "role_guess", "confidence", "status", "attempts", "next_check_at", "next_step", "resolution_notes", "source_url"]],
        use_container_width=True,
        hide_index=True,
        column_config={"source_url": st.column_config.LinkColumn("Source", display_text="open")},
    )


def render_learning_tab() -> None:
    st.subheader("Learning")
    st.caption("What the system has learned from feedback, source performance, investigations, and applications.")
    payload = fetch_json("/learning")

    top_queries = pd.DataFrame(payload.get("top_queries", []))
    if not top_queries.empty:
        if "last_run_at" in top_queries:
            top_queries["last_run_at"] = top_queries["last_run_at"].map(format_timestamp)
        st.markdown("#### Query Performance")
        st.dataframe(top_queries, use_container_width=True, hide_index=True)

    watchlist = pd.DataFrame(payload.get("watchlist_items", []))
    if not watchlist.empty:
        st.markdown("#### Watchlist Expansions")
        st.dataframe(watchlist, use_container_width=True, hide_index=True)
        proposed = watchlist.loc[watchlist["status"] == "proposed"]
        active = watchlist.loc[watchlist["status"] == "active"]
        suppressed = watchlist.loc[watchlist["status"].isin(["suppressed", "rolled_back", "expired"])]
        summary = st.columns(3)
        summary[0].metric("Proposed", len(proposed))
        summary[1].metric("Active", len(active))
        summary[2].metric("Suppressed / inactive", len(suppressed))

    followups = pd.DataFrame(payload.get("follow_up_tasks", []))
    if not followups.empty:
        followups["due_at"] = followups["due_at"].map(format_timestamp)
        st.markdown("#### Follow-up Tasks")
        st.dataframe(followups, use_container_width=True, hide_index=True)

    bullets = []
    if payload.get("generated_queries"):
        bullets.append(f"Generated queries: {', '.join(payload['generated_queries'])}")
    if payload.get("suppressed_queries"):
        bullets.append(f"Suppressed queries: {', '.join(payload['suppressed_queries'])}")
    if payload.get("inferred_title_families"):
        bullets.append(f"Inferred title families: {', '.join(payload['inferred_title_families'])}")
    if payload.get("inferred_domains"):
        bullets.append(f"Inferred domains: {', '.join(payload['inferred_domains'])}")
    for line in bullets:
        st.caption(line)


def render_discovery_tab() -> None:
    st.subheader("Discovery")
    st.caption("Recent planner output, ATS surface discoveries, expansions, and visible leads from agent-discovered sources.")
    payload = fetch_json("/discovery-status")

    top = st.columns(4)
    top[0].metric("Known companies", payload.get("total_known_companies", 0))
    top[1].metric("Discovered 24h", payload.get("discovered_last_24h", 0))
    top[2].metric("Expanded 24h", payload.get("expanded_last_24h", 0))
    top[3].metric("Agentic visible leads", len(payload.get("recent_agentic_leads", [])))

    cycle_metrics = payload.get("cycle_metrics") or {}
    if cycle_metrics:
        st.caption(
            "Cycle metrics: "
            f"new companies={cycle_metrics.get('discovered_companies_new_count', 0)}, "
            f"new greenhouse tokens={cycle_metrics.get('discovered_greenhouse_tokens_new_count', 0)}, "
            f"new ashby identifiers={cycle_metrics.get('discovered_ashby_identifiers_new_count', 0)}, "
            f"agent-discovered visible leads={cycle_metrics.get('agent_discovered_visible_leads_count', 0)}"
        )
        query_family_df = discovery_query_family_frame(cycle_metrics)
        if not query_family_df.empty:
            st.markdown("#### Query Family Diagnostics")
            st.dataframe(query_family_df, use_container_width=True, hide_index=True)
    openai_usage = payload.get("latest_openai_usage") or {}
    if openai_usage:
        st.caption(
            "OpenAI usage: "
            f"planner={'yes' if openai_usage.get('planner') else 'fallback'}, "
            f"triage={'yes' if openai_usage.get('triage') else 'fallback'}, "
            f"learning={'yes' if openai_usage.get('learning') else 'fallback'}"
        )

    planner = payload.get("latest_planner_run") or {}
    if planner:
        with st.expander("Latest planner run", expanded=True):
            st.write(planner.get("summary") or "No planner summary.")
            metadata = planner.get("metadata_json") or {}
            if metadata.get("queries"):
                st.caption("Queries: " + ", ".join(metadata["queries"][:8]))
            if metadata.get("company_archetypes"):
                st.caption("Company archetypes: " + ", ".join(metadata["company_archetypes"][:6]))
            if metadata.get("priority_notes"):
                st.caption("Priority notes: " + "; ".join(metadata["priority_notes"][:4]))

    if payload.get("recent_greenhouse_tokens"):
        st.markdown("#### Greenhouse Tokens")
        st.dataframe(pd.DataFrame(payload["recent_greenhouse_tokens"]), use_container_width=True, hide_index=True)
    if payload.get("recent_ashby_identifiers"):
        st.markdown("#### Ashby Identifiers")
        st.dataframe(pd.DataFrame(payload["recent_ashby_identifiers"]), use_container_width=True, hide_index=True)
    if payload.get("recent_expansions"):
        st.markdown("#### Recent Expansions")
        st.dataframe(pd.DataFrame(payload["recent_expansions"]), use_container_width=True, hide_index=True)
    if payload.get("recent_successful_expansions"):
        st.markdown("#### Successful Expansions")
        st.dataframe(pd.DataFrame(payload["recent_successful_expansions"]), use_container_width=True, hide_index=True)
    if payload.get("recent_visible_yield"):
        st.markdown("#### Visible Yield")
        visible_df = pd.DataFrame(payload["recent_visible_yield"])
        st.dataframe(
            visible_df[["company_name", "board_type", "board_locator", "visible_yield_count", "location_filtered_count", "utility_score"]],
            use_container_width=True,
            hide_index=True,
        )
    if payload.get("recent_geography_rejections"):
        st.markdown("#### Geography Rejections")
        st.dataframe(pd.DataFrame(payload["recent_geography_rejections"]), use_container_width=True, hide_index=True)
    if payload.get("recent_agentic_leads"):
        st.markdown("#### Agent-Discovered Visible Leads")
        st.dataframe(pd.DataFrame(payload["recent_agentic_leads"]), use_container_width=True, hide_index=True)
    if payload.get("blocked_or_cooled_down"):
        st.markdown("#### Blocked Or Cooled Down")
        blocked_df = pd.DataFrame(payload["blocked_or_cooled_down"])
        st.dataframe(
            blocked_df[["company_name", "board_type", "expansion_status", "blocked_reason", "last_expansion_result_count", "utility_score"]],
            use_container_width=True,
            hide_index=True,
        )
    if payload.get("next_recommended_queries"):
        st.markdown("#### Next Queries")
        for query in payload["next_recommended_queries"]:
            st.caption(query)


def render_autonomy_ops_tab() -> None:
    st.subheader("Autonomy Ops")
    st.caption("Operational health, governance, failures, and digest summaries for unattended runs.")
    payload = fetch_json("/autonomy-status")
    health = payload.get("health", {})
    digest = payload.get("digest", {})
    daily = payload.get("daily_digest") or {}
    connector_rows = pd.DataFrame(payload.get("connector_health", []))

    top = st.columns(5)
    top[0].metric("Run state", health.get("runtime_state", "paused"))
    top[1].metric("Open investigations", health.get("open_investigations", 0))
    top[2].metric("Suppressed leads", health.get("suppressed_leads", 0))
    top[3].metric("Due follow-ups", health.get("due_follow_ups", 0))
    top[4].metric("Last success", format_timestamp(health.get("last_successful_cycle_at") or health.get("last_successful_run_at")) or "never")
    if health.get("last_failed_run_at"):
        st.warning(f"Last failed run: {format_timestamp(health.get('last_failed_run_at'))}")

    if not connector_rows.empty:
        st.markdown("#### Connector Health")
        connector_rows["last_success_at"] = connector_rows["last_success_at"].map(format_timestamp)
        connector_rows["last_failure_at"] = connector_rows["last_failure_at"].map(format_timestamp)
        st.dataframe(
            connector_rows[
                [
                    "connector_name",
                    "status",
                    "circuit_state",
                    "approved_for_unattended",
                    "trust_score",
                    "consecutive_failures",
                    "recent_successes",
                    "recent_failures",
                    "last_failure_classification",
                    "last_mode",
                    "last_item_count",
                    "quarantine_count",
                    "last_freshness_lag_seconds",
                    "last_success_at",
                    "last_failure_at",
                    "last_error",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
        incident_rows = connector_rows.loc[
            connector_rows["status"].isin(["failed", "circuit_open", "recovering"])
            | connector_rows["last_failure_at"].astype(str).ne("")
        ]
        st.markdown("#### Connector Incidents")
        if incident_rows.empty:
            st.caption("No recent connector incidents.")
        else:
            st.dataframe(
                incident_rows[
                    [
                        "connector_name",
                        "status",
                        "circuit_state",
                        "last_failure_classification",
                        "last_failure_at",
                        "last_error",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

    if digest.get("summary"):
        st.markdown("#### Latest Run Digest")
        st.write(digest["summary"])
        if digest.get("new_leads"):
            st.caption(f"New leads: {', '.join(digest['new_leads'])}")
        if digest.get("suppressed_leads"):
            st.caption(f"Suppressed leads: {', '.join(digest['suppressed_leads'])}")
        if digest.get("follow_ups_created"):
            st.caption(f"Follow-ups: {', '.join(digest['follow_ups_created'])}")
        if digest.get("watchlist_changes"):
            st.caption(f"Watchlist/query changes: {', '.join(digest['watchlist_changes'])}")
        if digest.get("failures"):
            st.caption(f"Failures: {', '.join(digest['failures'])}")

    if daily.get("summary"):
        st.markdown("#### Daily Digest")
        st.write(daily["summary"])

    learning = fetch_json("/learning")
    top_queries = pd.DataFrame(learning.get("top_queries", []))
    if not top_queries.empty:
        st.markdown("#### Expansion Governance")
        proposed_queries = top_queries.loc[top_queries["status"].isin(["proposed", "generated"])]
        active_queries = top_queries.loc[top_queries["status"] == "active"]
        inactive_queries = top_queries.loc[top_queries["status"].isin(["suppressed", "expired", "rolled_back"])]
        cols = st.columns(3)
        cols[0].metric("Proposed queries", len(proposed_queries))
        cols[1].metric("Active queries", len(active_queries))
        cols[2].metric("Suppressed / inactive", len(inactive_queries))
        st.dataframe(top_queries, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Opportunity Scout", layout="wide")
    st.title("Opportunity Scout")
    st.caption("A functional workbench for refreshing, evaluating, and tracking startup leads.")

    try:
        profile = fetch_json("/candidate-profile")
        learning = fetch_json("/profile-learning")
    except requests.RequestException:
        st.error("Backend unavailable. Start FastAPI first, then refresh.")
        st.stop()

    toolbar = st.columns(4)
    freshness_choice = toolbar[0].selectbox("Default freshness window", ["7 days", "14 days", "all"], index=1)
    lead_visibility = toolbar[1].toggle("Show signal-only leads", value=False)
    include_hidden = toolbar[2].toggle("Show hidden leads", value=False)
    include_unqualified = toolbar[3].toggle("Show under or overqualified", value=False)
    freshness_map = {"7 days": 7, "14 days": 14, "all": 0}

    tabs = st.tabs(["Leads", "Saved", "Applied", "Profile", "Discovery", "Agent Activity", "Investigations", "Learning", "Autonomy Ops"])

    base_query = build_query(
        freshness_days=freshness_map[freshness_choice],
        include_hidden=include_hidden,
        include_unqualified=include_unqualified,
        include_signal_only=lead_visibility,
    )

    with tabs[0]:
        leads = fetch_json(base_query)["items"]
        selected = render_table(leads, key="leads")
        if selected:
            render_detail(selected, "leads")

    with tabs[1]:
        saved = fetch_json(
            build_query(
                freshness_days=freshness_map[freshness_choice],
                include_hidden=include_hidden,
                include_unqualified=include_unqualified,
                only_saved=True,
                include_signal_only=lead_visibility,
            )
        )["items"]
        selected = render_table(saved, key="saved")
        if selected:
            render_detail(selected, "saved")

    with tabs[2]:
        applied = fetch_json(
            build_query(
                freshness_days=freshness_map[freshness_choice],
                include_hidden=include_hidden,
                include_unqualified=include_unqualified,
                only_applied=True,
                include_signal_only=lead_visibility,
            )
        )["items"]
        selected = render_table(applied, key="applied", applied_view=True)
        if selected:
            render_detail(selected, "applied")

    with tabs[3]:
        render_profile_tab(profile, learning)

    with tabs[4]:
        render_discovery_tab()

    with tabs[5]:
        render_agent_activity_tab()

    with tabs[6]:
        render_investigations_tab()

    with tabs[7]:
        render_learning_tab()

    with tabs[8]:
        render_autonomy_ops_tab()


if __name__ == "__main__":
    main()
