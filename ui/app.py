from __future__ import annotations

import json
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
from core.db import SessionLocal
from services.feedback_learning import (
    REJECTION_OUTCOME_REASON_LABELS,
    REJECTION_STATUS_REASON_LABELS,
    bucket_label,
    categorize_rejection_feedback,
    generate_improvement_recommendations,
    reason_label,
)
from services.network_import import match_referral_paths, parse_network_csv
from services.profile import attach_network_import, build_profile_data_inventory, extract_network_import
from services.profile_ingest import build_profile_review_rows
from services.pipeline import ingest_user_job_link
from ui.components.sidebar import render_sidebar
from ui.screens.jobs import render_jobs_screen


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
    "Highest recommendation first",
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


def fetch_optional_json(path: str) -> Optional[dict[str, Any]]:
    try:
        return fetch_json(path)
    except requests.exceptions.RequestException:
        return None


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def get_profile_form_source(profile: dict[str, Any], latest_resume_ingest: Optional[dict[str, Any]]) -> dict[str, Any]:
    if latest_resume_ingest and latest_resume_ingest.get("candidate_profile"):
        return latest_resume_ingest["candidate_profile"]
    return profile


def dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(cleaned)
    return ordered


def build_target_role_options(profile_source: dict[str, Any]) -> list[str]:
    return dedupe_preserving_order(
        [
            *(profile_source.get("core_titles_json") or []),
            *(profile_source.get("preferred_titles_json") or []),
            *(profile_source.get("adjacent_titles_json") or []),
            "chief of staff",
            "founding operations lead",
        ]
    )


def apply_target_role_selection(payload: dict[str, Any], target_role: str) -> dict[str, Any]:
    selected_role = target_role.strip()
    if not selected_role:
        return payload

    updated = dict(payload)
    preferred_titles = dedupe_preserving_order([selected_role, *(updated.get("preferred_titles_json") or []), *(updated.get("core_titles_json") or [])])
    core_titles = dedupe_preserving_order([selected_role, *(updated.get("core_titles_json") or []), *(updated.get("preferred_titles_json") or [])])
    extracted_summary = dict(updated.get("extracted_summary_json") or {})

    updated["preferred_titles_json"] = preferred_titles
    updated["core_titles_json"] = core_titles[:3]
    extracted_summary["selected_target_role"] = selected_role
    updated["extracted_summary_json"] = extracted_summary
    return updated


def build_onboarding_state(
    profile: dict[str, Any],
    latest_resume_ingest: Optional[dict[str, Any]],
    draft_profile: Optional[dict[str, Any]],
) -> dict[str, Any]:
    resume_complete = bool((latest_resume_ingest and latest_resume_ingest.get("candidate_profile")) or profile.get("raw_resume_text"))
    review_complete = draft_profile is not None or (resume_complete and latest_resume_ingest is None)
    target_role_complete = bool(
        latest_resume_ingest is None
        and draft_profile is None
        and dedupe_preserving_order(
            [
                *((profile.get("core_titles_json") or [])),
                *((profile.get("preferred_titles_json") or [])),
            ]
        )
    )
    if not resume_complete:
        current_step = "resume"
    elif latest_resume_ingest is not None and draft_profile is None:
        current_step = "review"
    elif draft_profile is not None:
        current_step = "target_role"
    else:
        current_step = "discovery"
    return {
        "resume_complete": resume_complete,
        "review_complete": review_complete,
        "target_role_complete": target_role_complete,
        "current_step": current_step,
    }


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


def build_profile_persistence_payload(
    profile: dict[str, Any],
    *,
    extracted_summary_json: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "profile_schema_version": profile.get("profile_schema_version", "v1"),
        "name": profile.get("name", "Demo Candidate"),
        "raw_resume_text": profile.get("raw_resume_text", ""),
        "extracted_summary_json": extracted_summary_json if extracted_summary_json is not None else profile.get("extracted_summary_json", {}),
        "preferred_titles_json": profile.get("preferred_titles_json", []),
        "adjacent_titles_json": profile.get("adjacent_titles_json", []),
        "excluded_titles_json": profile.get("excluded_titles_json", []),
        "preferred_domains_json": profile.get("preferred_domains_json", []),
        "excluded_companies_json": profile.get("excluded_companies_json", []),
        "preferred_locations_json": profile.get("preferred_locations_json", []),
        "confirmed_skills_json": profile.get("confirmed_skills_json", []),
        "competencies_json": profile.get("competencies_json", []),
        "explicit_preferences_json": profile.get("explicit_preferences_json", []),
        "seniority_guess": profile.get("seniority_guess"),
        "stage_preferences_json": profile.get("stage_preferences_json", []),
        "core_titles_json": profile.get("core_titles_json", []),
        "excluded_keywords_json": profile.get("excluded_keywords_json", []),
        "min_seniority_band": profile.get("min_seniority_band", "mid"),
        "max_seniority_band": profile.get("max_seniority_band", "senior"),
        "stretch_role_families_json": profile.get("stretch_role_families_json", []),
        "minimum_fit_threshold": profile.get("minimum_fit_threshold", 2.8),
    }


def referral_matches_for_lead(lead: dict[str, Any], profile: dict[str, Any], limit: int = 3) -> list[dict[str, Any]]:
    network_payload = extract_network_import(profile.get("extracted_summary_json"))
    return match_referral_paths(lead.get("company_name") or "", network_payload, limit=limit)


def referral_strategy_summary(lead: dict[str, Any], profile: dict[str, Any]) -> str:
    matches = referral_matches_for_lead(lead, profile)
    if not matches:
        return "No saved referral paths for this company."
    return "Possible referral paths: " + "; ".join(match["path_summary"] for match in matches)


def profile_inventory_frame(profile: dict[str, Any]) -> pd.DataFrame:
    inventory_rows = build_profile_data_inventory(profile)
    frame_rows = []
    for row in inventory_rows:
        frame_rows.append(
            {
                "Category": row["category"],
                "Stored": "yes" if row["stored"] else "no",
                "Items": row["item_count"],
                "Processing path": "Local only" if row["processing_path"] == "local_only" else "Cloud assisted",
                "Provenance": row["provenance"],
                "Usage": row["usage"],
                "Examples": ", ".join(row["example_values"]) or "",
            }
        )
    return pd.DataFrame(frame_rows)


def profile_inventory_export(profile: dict[str, Any]) -> dict[str, Any]:
    inventory_rows = build_profile_data_inventory(profile)
    return {
        "inventory_version": "v1",
        "profile_name": profile.get("name", "Demo Candidate"),
        "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "summary": {
            "stored_categories": sum(1 for row in inventory_rows if row["stored"]),
            "local_only_categories": sum(1 for row in inventory_rows if row["processing_path"] == "local_only"),
            "cloud_assisted_categories": sum(1 for row in inventory_rows if row["processing_path"] == "cloud_assisted"),
        },
        "categories": inventory_rows,
    }


def render_onboarding_progress(state: dict[str, Any]) -> None:
    labels = {
        "resume": "Upload resume",
        "review": "Review profile",
        "target_role": "Pick target role",
        "discovery": "Enter discovery",
    }
    order = ["resume", "review", "target_role", "discovery"]
    current_step = state["current_step"]
    status_by_step = {
        "resume": state["resume_complete"],
        "review": state["review_complete"],
        "target_role": state["target_role_complete"],
        "discovery": current_step == "discovery",
    }
    current_index = order.index(current_step)
    steps = []
    for index, step in enumerate(order):
        if status_by_step[step]:
            marker = "Complete"
        elif index == current_index:
            marker = "Current"
        else:
            marker = "Next"
        steps.append(f"{index + 1}. {labels[step]} [{marker}]")
    st.caption(" -> ".join(steps))


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


def update_application_status(
    lead_id: int,
    current_status: str,
    notes: str,
    date_applied_value: Optional[date],
    status_reason_code: Optional[str] = None,
    outcome_reason_code: Optional[str] = None,
) -> None:
    payload: dict[str, Any] = {"lead_id": lead_id, "current_status": current_status, "notes": notes or None}
    if date_applied_value:
        payload["date_applied"] = datetime.combine(date_applied_value, datetime.min.time()).isoformat()
    if status_reason_code:
        payload["status_reason_code"] = status_reason_code
    if outcome_reason_code:
        payload["outcome_reason_code"] = outcome_reason_code
    fetch_json("/applications/status", method="POST", payload=payload)


def submit_user_job_link(
    job_url: str,
    company_name: str,
    title: str,
    location: Optional[str] = None,
    description_text: Optional[str] = None,
    posted_on: Optional[date] = None,
) -> dict[str, Any]:
    session = SessionLocal()
    try:
        result = ingest_user_job_link(
            session,
            job_url=job_url,
            company_name=company_name,
            title=title,
            location=location,
            description_text=description_text,
            posted_at=datetime.combine(posted_on, datetime.min.time()) if posted_on else None,
        )
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def lead_frame(leads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for lead in leads:
        score_payload = lead.get("score_breakdown_json") or {}
        recommendation_score = score_payload.get("final_score", score_payload.get("composite"))
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
                "recommendation_score": float(recommendation_score) if recommendation_score is not None else None,
                "recommendation_action": score_payload.get("action_label") or "",
                "match_summary": recommendation_table_explanation(lead),
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


def recommendation_score_rows(lead: dict[str, Any]) -> pd.DataFrame:
    score_payload = lead.get("score_breakdown_json") or {}
    component_metrics = score_payload.get("component_metrics") or []
    rows: list[dict[str, Any]] = []
    for component in component_metrics:
        rows.append(
            {
                "component": component.get("label") or component.get("key") or "",
                "score": component.get("score"),
                "semantics": component.get("semantics") or "",
                "trace_inputs": ", ".join(component.get("trace_inputs") or []),
            }
        )
    return pd.DataFrame(rows)


def recommendation_score_summary(lead: dict[str, Any]) -> str:
    score_payload = lead.get("score_breakdown_json") or {}
    final_score = score_payload.get("final_score", score_payload.get("composite"))
    if final_score is None:
        return "Recommendation score unavailable."
    band = score_payload.get("recommendation_band") or lead.get("rank_label") or "unknown"
    confidence = score_payload.get("confidence_label") or lead.get("confidence_label") or "unknown"
    return f"Recommendation score: {float(final_score):.2f} | Band: {band} | Confidence: {confidence}"


def recommendation_action_summary(lead: dict[str, Any]) -> str:
    score_payload = lead.get("score_breakdown_json") or {}
    action_label = score_payload.get("action_label") or "No action"
    action_explanation = score_payload.get("action_explanation") or "No action guidance recorded."
    return f"{action_label}: {action_explanation}"


def recommendation_table_explanation(lead: dict[str, Any]) -> str:
    score_payload = lead.get("score_breakdown_json") or {}
    score_explanation = score_payload.get("explanation") or {}
    return (
        score_explanation.get("headline")
        or score_explanation.get("summary")
        or lead.get("explanation")
        or recommendation_action_summary(lead)
    )


def rejection_feedback_summary(lead: dict[str, Any]) -> str:
    feedback = categorize_rejection_feedback(
        status_reason_code=lead.get("status_reason_code"),
        outcome_reason_code=lead.get("outcome_reason_code"),
        notes=lead.get("application_notes"),
    )
    buckets = feedback["reason_buckets"]
    if not buckets:
        return "No structured rejection feedback recorded."
    return "Detected rejection themes: " + ", ".join(bucket_label(bucket) for bucket in buckets)


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
    if sort_mode == "Highest recommendation first":
        filtered = filtered.sort_values(
            by=["recommendation_score", "surfaced_at_raw", "company"],
            ascending=[False, False, True],
            na_position="last",
        )
    elif sort_mode == "Newest surfaced":
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
    st.caption("Search, filter, and sort the ranked opportunities table.")
    row1 = st.columns(6)
    row2 = st.columns(4)
    default_sort_index = SORT_OPTIONS.index("Highest recommendation first") if key == "leads" else SORT_OPTIONS.index("Newest surfaced")
    filters = {
        "search": row1[0].text_input("Search title or company", key=f"search-{key}"),
        "lead_type": row1[1].selectbox("Lead type", ["all", "combined", "listing", "signal"], key=f"type-{key}"),
        "freshness": row1[2].selectbox("Freshness", ["all", "fresh", "recent", "stale", "unknown"], key=f"fresh-{key}"),
        "fit": row1[3].selectbox("Fit", ["all", "strong fit", "stretch", "unclear", "overqualified", "underqualified"], key=f"fit-{key}"),
        "status": row1[4].selectbox("Status", ["all", *APPLICATION_STATUSES], key=f"status-{key}"),
        "sort_mode": row1[5].selectbox("Sort", SORT_OPTIONS, index=default_sort_index, key=f"sort-{key}"),
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
        "recommendation_score",
        "recommendation_action",
        "match_summary",
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
            "recommendation_score": st.column_config.NumberColumn("Score", format="%.2f"),
            "surfaced_at": "Surfaced",
            "posted_at": "Posted",
            "recommendation_action": "Recommendation",
            "match_summary": "Why it surfaced",
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


def render_detail(lead: dict[str, Any], key: str, profile: dict[str, Any]) -> None:
    evidence = lead.get("evidence_json", {})
    score_payload = lead.get("score_breakdown_json") or {}
    score_explanation = score_payload.get("explanation") or {}
    agent_actions = evidence.get("agent_actions", [])
    critic_status = evidence.get("critic_status", "unknown")
    critic_reasons = evidence.get("critic_reasons", [])
    liveness = evidence.get("liveness_evidence", {})
    ai_fit = evidence.get("ai_fit_assessment") or {}
    ai_critic = evidence.get("ai_critic_assessment") or {}

    st.divider()
    st.subheader(f"{lead['company_name']} — {lead['primary_title']}")
    st.write(recommendation_table_explanation(lead))
    top_summary = st.columns(3)
    score_value = score_payload.get("final_score", score_payload.get("composite"))
    top_summary[0].metric("Recommendation", score_payload.get("action_label") or "No action")
    top_summary[1].metric("Score", f"{float(score_value):.2f}" if score_value is not None else "n/a")
    top_summary[2].metric("Confidence", score_payload.get("confidence_label") or lead.get("confidence_label") or "unknown")
    st.caption(recommendation_score_summary(lead))
    st.info(recommendation_action_summary(lead))
    st.caption(referral_strategy_summary(lead, profile))

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
        if score_explanation:
            st.write(f"Score explanation: {score_explanation.get('headline') or score_explanation.get('summary')}")
            supporting_points = score_explanation.get("supporting_points") or []
            if supporting_points:
                st.write("Score context: " + " | ".join(supporting_points))
        if score_payload.get("action_label") or score_payload.get("action_explanation"):
            st.write("Recommended action: " + (score_payload.get("action_label") or "none"))
            st.caption(score_payload.get("action_explanation") or "No action explanation recorded.")
        referral_matches = referral_matches_for_lead(lead, profile)
        if referral_matches:
            st.write("Referral paths:")
            st.dataframe(
                pd.DataFrame(referral_matches)[["contact_name", "company", "title", "relationship", "adjacency_label", "profile_url", "notes"]],
                use_container_width=True,
                hide_index=True,
                column_config={"profile_url": st.column_config.LinkColumn("Profile", display_text="open", validate="^https?://")},
            )
            st.caption("Local referral suggestions only. No outreach copy or automation is generated.")
        score_rows = recommendation_score_rows(lead)
        if not score_rows.empty:
            st.dataframe(score_rows, use_container_width=True, hide_index=True)
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
        saved_status_reason = lead.get("status_reason_code") or ""
        saved_outcome_reason = lead.get("outcome_reason_code") or ""
        status_reason_code = saved_status_reason
        outcome_reason_code = saved_outcome_reason
        if next_status == "rejected":
            rejection_cols = st.columns(2)
            status_reason_options = list(REJECTION_STATUS_REASON_LABELS.keys())
            outcome_reason_options = list(REJECTION_OUTCOME_REASON_LABELS.keys())
            status_reason_index = status_reason_options.index(saved_status_reason) if saved_status_reason in status_reason_options else 0
            outcome_reason_index = outcome_reason_options.index(saved_outcome_reason) if saved_outcome_reason in outcome_reason_options else 0
            status_reason_code = rejection_cols[0].selectbox(
                "Rejection stage",
                status_reason_options,
                index=status_reason_index,
                format_func=lambda value: reason_label(value, REJECTION_STATUS_REASON_LABELS),
                key=f"rejection-stage-{key}-{lead['id']}",
            )
            outcome_reason_code = rejection_cols[1].selectbox(
                "Rejection reason",
                outcome_reason_options,
                index=outcome_reason_index,
                format_func=lambda value: reason_label(value, REJECTION_OUTCOME_REASON_LABELS),
                key=f"rejection-reason-{key}-{lead['id']}",
            )
            notes = cols[2].text_area(
                "Feedback notes",
                value=lead.get("application_notes") or "",
                key=f"notes-{key}-{lead['id']}",
                height=100,
            )
            feedback = categorize_rejection_feedback(
                status_reason_code=status_reason_code,
                outcome_reason_code=outcome_reason_code,
                notes=notes,
            )
            recommendations = generate_improvement_recommendations(
                status_reason_code=status_reason_code,
                outcome_reason_code=outcome_reason_code,
                notes=notes,
            )
            if feedback["reason_buckets"]:
                st.caption("Detected rejection themes: " + ", ".join(bucket_label(bucket) for bucket in feedback["reason_buckets"]))
            if recommendations:
                st.warning("Improvement recommendations:\n" + "\n".join(f"- {item}" for item in recommendations))
        else:
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
                status_reason_code=status_reason_code or None,
                outcome_reason_code=outcome_reason_code or None,
            )
            st.rerun()
        if lead.get("next_action"):
            st.info(f"Next action: {lead['next_action']}")
        if current_status == "rejected":
            st.caption(rejection_feedback_summary(lead))
            recommendations = generate_improvement_recommendations(
                status_reason_code=lead.get("status_reason_code"),
                outcome_reason_code=lead.get("outcome_reason_code"),
                notes=lead.get("application_notes"),
            )
            if recommendations:
                st.info("Current improvement recommendations:\n" + "\n".join(f"- {item}" for item in recommendations))


def render_user_job_link_form() -> None:
    st.markdown("#### Add job link")
    st.caption("Submit a job URL with minimal context. It will be normalized into the same listing-to-lead evaluation flow and marked as user-submitted provenance.")
    with st.form("user-job-link-form"):
        top = st.columns(3)
        job_url = top[0].text_input("Job URL")
        company_name = top[1].text_input("Company")
        title = top[2].text_input("Title")
        lower = st.columns(2)
        location = lower[0].text_input("Location")
        posted_on = lower[1].date_input("Posted date", value=None)
        description_text = st.text_area("Description or notes", height=100)
        submitted = st.form_submit_button("Ingest job link", use_container_width=True)
    if submitted:
        if not job_url.strip() or not company_name.strip() or not title.strip():
            st.warning("Enter a job URL, company name, and title.")
            return
        try:
            result = submit_user_job_link(
                job_url=job_url,
                company_name=company_name,
                title=title,
                location=location,
                description_text=description_text,
                posted_on=posted_on,
            )
        except Exception as exc:
            st.error(f"Job link ingestion failed: {exc}")
            return
        st.session_state["user_job_link_ingest_result"] = result
        st.rerun()
    if st.session_state.get("user_job_link_ingest_result"):
        result = st.session_state["user_job_link_ingest_result"]
        st.success(f"{result['summary']} Provenance: {result['source_lineage']}.")


def render_profile_tab(profile: dict[str, Any], learning: dict[str, Any]) -> None:
    st.subheader("Onboarding")
    st.caption("Move from resume upload to profile review, pick a target role, then continue into discovery.")
    latest_resume_ingest = st.session_state.get("latest_resume_ingest")
    draft_profile = st.session_state.get("onboarding_profile_draft")
    onboarding_state = build_onboarding_state(profile, latest_resume_ingest, draft_profile)
    render_onboarding_progress(onboarding_state)

    st.markdown("#### Step 1: Upload resume")
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
            st.session_state.pop("onboarding_profile_draft", None)
            st.rerun()
        except Exception as exc:
            st.error(f"Resume parsing failed: {exc}")

    review_profile = draft_profile or get_profile_form_source(profile, latest_resume_ingest)
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

    if onboarding_state["resume_complete"]:
        st.markdown("#### Step 2: Review profile")
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
            if st.form_submit_button("Continue to target role", use_container_width=True):
                st.session_state["onboarding_profile_draft"] = build_profile_update_payload(
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
                st.rerun()

    if draft_profile:
        st.markdown("#### Step 3: Pick target role")
        target_role_options = build_target_role_options(draft_profile)
        default_target_role = (draft_profile.get("core_titles_json") or draft_profile.get("preferred_titles_json") or [""])[0]
        default_index = target_role_options.index(default_target_role) if default_target_role in target_role_options else 0
        with st.form("target-role-form"):
            target_role = st.selectbox("Target role", target_role_options, index=default_index)
            custom_target_role = st.text_input("Or enter a custom target role", value="")
            st.caption(
                "This selection becomes the first preferred and core title used for profile-aware search and fit scoring."
            )
            if st.form_submit_button("Save profile and enter discovery", use_container_width=True):
                selected_target_role = custom_target_role.strip() or target_role
                payload = apply_target_role_selection(draft_profile, selected_target_role)
                fetch_json("/candidate-profile", method="POST", payload=payload)
                st.session_state["last_onboarding_target_role"] = selected_target_role
                st.session_state.pop("latest_resume_ingest", None)
                st.session_state.pop("onboarding_profile_draft", None)
                st.rerun()

    if onboarding_state["current_step"] == "discovery":
        st.markdown("#### Step 4: Enter discovery")
        selected_target_role = (profile.get("extracted_summary_json") or {}).get("selected_target_role") or st.session_state.get("last_onboarding_target_role")
        if selected_target_role:
            st.success(f"Target role saved: {selected_target_role}")
        st.caption(
            "Continue in the Leads tab for ranked matches or the Discovery tab to inspect recall expansion from this saved profile."
        )

    st.markdown("#### Local network import")
    st.caption("Import LinkedIn export or network CSV locally to suggest referral paths. JORB does not generate outreach.")
    network_upload = st.file_uploader("Upload network CSV", type=["csv"], key="network-import-upload")
    pasted_network_csv = st.text_area("Or paste network CSV", height=120, key="network-import-text")
    if st.button("Import network data", use_container_width=True):
        try:
            if network_upload is not None:
                imported_network = parse_network_csv(network_upload.name, network_upload.getvalue().decode("utf-8", errors="ignore"))
            elif pasted_network_csv.strip():
                imported_network = parse_network_csv("pasted_network.csv", pasted_network_csv.strip())
            else:
                st.warning("Upload a CSV or paste CSV text first.")
                return
            extracted_summary = attach_network_import(profile.get("extracted_summary_json"), imported_network)
            fetch_json("/candidate-profile", method="POST", payload=build_profile_persistence_payload(profile, extracted_summary_json=extracted_summary))
            st.session_state["latest_network_import"] = imported_network
            st.rerun()
        except Exception as exc:
            st.error(f"Network import failed: {exc}")

    network_payload = extract_network_import(profile.get("extracted_summary_json"))
    if network_payload.get("contacts"):
        import_summary = network_payload.get("import_summary") or {}
        cols = st.columns(2)
        cols[0].metric("Imported contacts", import_summary.get("contact_count", 0))
        cols[1].metric("Indexed companies", import_summary.get("indexed_company_count", 0))
        st.caption(network_payload.get("guidance") or "Local referral suggestions only.")
        contacts_df = pd.DataFrame(network_payload["contacts"])
        if not contacts_df.empty:
            visible_columns = [column for column in ["name", "company", "title", "relationship", "location", "profile_url", "notes"] if column in contacts_df.columns]
            st.dataframe(
                contacts_df[visible_columns],
                use_container_width=True,
                hide_index=True,
                column_config={"profile_url": st.column_config.LinkColumn("Profile", display_text="open", validate="^https?://")},
            )

    st.markdown("#### Privacy and local data inventory")
    st.caption(
        "Inspect what JORB stores locally for your profile, where it came from, and whether a category stays local or can flow into cloud-assisted discovery paths."
    )
    inventory_frame = profile_inventory_frame(profile)
    inventory_export = profile_inventory_export(profile)
    inventory_summary = st.columns(3)
    inventory_summary[0].metric("Stored categories", inventory_export["summary"]["stored_categories"])
    inventory_summary[1].metric("Local only", inventory_export["summary"]["local_only_categories"])
    inventory_summary[2].metric("Cloud assisted", inventory_export["summary"]["cloud_assisted_categories"])
    if not inventory_frame.empty:
        st.dataframe(inventory_frame, use_container_width=True, hide_index=True)
    st.download_button(
        "Export inventory JSON",
        data=json.dumps(inventory_export, indent=2),
        file_name="jorb-profile-data-inventory.json",
        mime="application/json",
        use_container_width=True,
    )

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
    st.title("Jorb")
    st.caption("Jobs-first opportunity intelligence, using the real backend system.")

    try:
        profile = fetch_json("/candidate-profile")
        learning = fetch_json("/profile-learning")
    except requests.RequestException:
        st.error("Backend unavailable. Start FastAPI first, then refresh.")
        st.stop()

    stats = fetch_optional_json("/stats")
    runtime = fetch_optional_json("/runtime-control")
    health = fetch_optional_json("/autonomy-status")
    primary_page, operator_page = render_sidebar(stats=stats, runtime=runtime, health=health)

    with st.sidebar.expander("Advanced filters", expanded=False):
        freshness_choice = st.selectbox("Default freshness window", ["7 days", "14 days", "all"], index=1)
        lead_visibility = st.toggle("Show signal-only leads", value=False)
        include_hidden = st.toggle("Show hidden leads", value=False)
        include_unqualified = st.toggle("Show under or overqualified", value=False)
    freshness_map = {"7 days": 7, "14 days": 14, "all": 0}

    base_query = build_query(
        freshness_days=freshness_map[freshness_choice],
        include_hidden=include_hidden,
        include_unqualified=include_unqualified,
        include_signal_only=lead_visibility,
    )

    if operator_page:
        primary_page = operator_page

    if primary_page == "Jobs":
        render_user_job_link_form()
        leads = fetch_json(base_query)["items"]
        render_jobs_screen(
            leads=leads,
            page_key="jobs",
            title="Jobs",
            empty_message="No matching jobs found. Try adjusting filters or wait for the next discovery cycle.",
            last_updated=datetime.now(),
            send_feedback_fn=send_feedback,
        )
    elif primary_page == "Saved":
        saved = fetch_json(
            build_query(
                freshness_days=freshness_map[freshness_choice],
                include_hidden=include_hidden,
                include_unqualified=include_unqualified,
                only_saved=True,
                include_signal_only=lead_visibility,
            )
        )["items"]
        render_jobs_screen(
            leads=saved,
            page_key="saved",
            title="Saved",
            empty_message="No saved jobs yet.",
            last_updated=datetime.now(),
            send_feedback_fn=send_feedback,
        )
    elif primary_page == "Applied":
        applied = fetch_json(
            build_query(
                freshness_days=freshness_map[freshness_choice],
                include_hidden=include_hidden,
                include_unqualified=include_unqualified,
                only_applied=True,
                include_signal_only=lead_visibility,
            )
        )["items"]
        render_jobs_screen(
            leads=applied,
            page_key="applied",
            title="Applied",
            empty_message="No applied jobs yet.",
            last_updated=datetime.now(),
            send_feedback_fn=send_feedback,
        )
    elif primary_page == "Profile":
        render_profile_tab(profile, learning)
    elif primary_page == "Discovery":
        render_discovery_tab()
    elif primary_page == "Agent Activity":
        render_agent_activity_tab()
    elif primary_page == "Investigations":
        render_investigations_tab()
    elif primary_page == "Learning":
        render_learning_tab()
    elif primary_page == "Autonomy Ops":
        render_autonomy_ops_tab()


if __name__ == "__main__":
    main()
