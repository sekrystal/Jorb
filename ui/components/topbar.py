from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import streamlit as st


def _relative_time_label(value: datetime | None) -> str:
    if value is None:
        return "Not refreshed yet"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - value.astimezone(timezone.utc)
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} hr ago"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''} ago"


def count_active_job_filters(*, search: str, location: str, remote_only: bool) -> int:
    return int(bool(search.strip())) + int(bool(location.strip())) + int(bool(remote_only))


def build_jobs_page_header_copy(*, title: str) -> dict[str, str]:
    normalized = str(title or "").strip().lower()
    if normalized == "jobs":
        return {
            "eyebrow": "Workspace",
            "title": "Jobs",
            "description": "Set what you want, run discovery, and review ranked jobs with one clear detail view at a time.",
        }
    if normalized == "saved":
        return {
            "eyebrow": "Workspace",
            "title": "Saved",
            "description": "Keep promising jobs in a focused shortlist before you decide whether to apply.",
        }
    if normalized == "applied":
        return {
            "eyebrow": "Workspace",
            "title": "Applied",
            "description": "Track jobs you have already acted on without mixing them into the active queue.",
        }
    if normalized == "dismissed":
        return {
            "eyebrow": "Workspace",
            "title": "Dismissed",
            "description": "Review jobs hidden from active views and restore only the ones you want back.",
        }
    return {
        "eyebrow": "Workspace",
        "title": title,
        "description": "Review the current jobs list with the same ranking and filtering controls used across the workbench.",
    }


def build_jobs_filters_panel_copy(*, active_filter_count: int) -> dict[str, str]:
    if active_filter_count == 0:
        return {
            "eyebrow": "Filters",
            "description": "Narrow the jobs list without leaving the current results.",
            "count_label": "No active filters",
        }
    return {
        "eyebrow": "Filters",
        "description": "Narrow the jobs list without leaving the current results.",
        "count_label": f"{active_filter_count} active filter{'s' if active_filter_count != 1 else ''}",
    }


def render_jobs_topbar(
    *,
    page_key: str,
    last_updated: datetime | None,
    title: str,
    default_search: str = "",
    default_location: str = "",
    default_remote_only: bool = False,
    default_sort: str = "Best Match",
    refresh_label: str = "Refresh jobs",
) -> dict[str, Any]:
    search_key = f"jobs-search-{page_key}"
    location_key = f"jobs-location-{page_key}"
    remote_key = f"jobs-remote-{page_key}"
    sort_key = f"jobs-sort-{page_key}"

    search_value = str(st.session_state.get(search_key, default_search))
    location_value = str(st.session_state.get(location_key, default_location))
    remote_value = bool(st.session_state.get(remote_key, default_remote_only))
    active_filter_count = count_active_job_filters(
        search=search_value,
        location=location_value,
        remote_only=remote_value,
    )
    panel_copy = build_jobs_filters_panel_copy(active_filter_count=active_filter_count)
    header_copy = build_jobs_page_header_copy(title=title)

    st.markdown(
        """
        <style>
        .jorb-topbar-header {
            background: #ffffff;
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 1rem;
            padding: 1.05rem 1.1rem;
            margin-bottom: 0.85rem;
        }
        .jorb-topbar-eyebrow {
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 0.2rem;
        }
        .jorb-topbar-title {
            font-size: 1.25rem;
            line-height: 1.25;
            font-weight: 700;
            color: #111827;
        }
        .jorb-topbar-description {
            margin-top: 0.35rem;
            color: #475569;
            font-size: 0.92rem;
            line-height: 1.45;
            max-width: 48rem;
        }
        .jorb-topbar-meta {
            text-align: right;
            color: #6b7280;
            font-size: 0.85rem;
            padding-top: 0.15rem;
            white-space: nowrap;
        }
        .jorb-filters-shell {
            background: #ffffff;
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 1rem;
            padding: 1rem 1.05rem;
            margin-bottom: 1rem;
        }
        .jorb-filters-eyebrow {
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #475569;
            margin-bottom: 0.2rem;
        }
        .jorb-filters-description {
            font-size: 0.9rem;
            color: #475569;
            margin: 0;
        }
        .jorb-filters-count {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            white-space: nowrap;
            padding: 0.25rem 0.65rem;
            border-radius: 999px;
            border: 1px solid rgba(15, 23, 42, 0.1);
            background: #f8fafc;
            color: #334155;
            font-size: 0.8rem;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="jorb-topbar-header">', unsafe_allow_html=True)
    header = st.columns([3.4, 1])
    header[0].markdown(
        (
            f'<div class="jorb-topbar-eyebrow">{header_copy["eyebrow"]}</div>'
            f'<div class="jorb-topbar-title">{header_copy["title"]}</div>'
            f'<div class="jorb-topbar-description">{header_copy["description"]}</div>'
        ),
        unsafe_allow_html=True,
    )
    header[1].markdown(
        f'<div class="jorb-topbar-meta">Last updated: {_relative_time_label(last_updated)}</div>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="jorb-filters-shell">', unsafe_allow_html=True)
    intro_row = st.columns([4, 1.2, 1.2])
    intro_row[0].markdown(
        (
            f'<div class="jorb-filters-eyebrow">{panel_copy["eyebrow"]}</div>'
            f'<p class="jorb-filters-description">{panel_copy["description"]}</p>'
        ),
        unsafe_allow_html=True,
    )
    intro_row[1].markdown(f'<div class="jorb-filters-count">{panel_copy["count_label"]}</div>', unsafe_allow_html=True)
    clear_filters = intro_row[2].button("Clear filters", key=f"jobs-clear-panel-{page_key}", use_container_width=True)
    if clear_filters:
        st.session_state[search_key] = ""
        st.session_state[location_key] = ""
        st.session_state[remote_key] = False
        st.session_state[sort_key] = default_sort
        st.rerun()

    row = st.columns([2.4, 1.35, 0.9, 1.1, 1.0])
    search = row[0].text_input("Search", value=default_search, placeholder="Search roles or keywords...", key=search_key)
    location = row[1].text_input("Location", value=default_location, placeholder="Location", key=location_key)
    remote_only = row[2].toggle("Remote only", value=default_remote_only, key=remote_key)
    sort_by = row[3].selectbox("Sort", ["Best Match", "Newest"], index=0 if default_sort == "Best Match" else 1, key=sort_key)
    refresh = row[4].button(refresh_label, key=f"jobs-refresh-{page_key}", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    return {
        "search": search,
        "location": location,
        "remote_only": remote_only,
        "sort_by": sort_by,
        "refresh": refresh,
    }
