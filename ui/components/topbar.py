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


def render_jobs_topbar(
    *,
    page_key: str,
    last_updated: datetime | None,
    default_search: str = "",
    default_location: str = "",
    default_remote_only: bool = False,
    default_sort: str = "Best Match",
) -> dict[str, Any]:
    st.markdown(
        """
        <style>
        .jorb-topbar-title { font-size: 1.75rem; font-weight: 600; color: #111827; }
        .jorb-topbar-subtitle { color: #6b7280; margin-bottom: 0.75rem; }
        .jorb-topbar-meta { text-align: right; color: #6b7280; font-size: 0.85rem; padding-top: 0.35rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    header = st.columns([3, 1])
    header[0].markdown('<div class="jorb-topbar-title">Jobs</div>', unsafe_allow_html=True)
    header[0].markdown(
        '<div class="jorb-topbar-subtitle">Review the highest-signal roles first, then open details on the right.</div>',
        unsafe_allow_html=True,
    )
    header[1].markdown(
        f'<div class="jorb-topbar-meta">Last updated: {_relative_time_label(last_updated)}</div>',
        unsafe_allow_html=True,
    )

    row = st.columns([2.4, 1.4, 1, 1.1, 0.9])
    search = row[0].text_input("Search", value=default_search, placeholder="Search roles or keywords...", key=f"jobs-search-{page_key}")
    location = row[1].text_input("Location", value=default_location, placeholder="Location", key=f"jobs-location-{page_key}")
    remote_only = row[2].toggle("Remote only", value=default_remote_only, key=f"jobs-remote-{page_key}")
    sort_by = row[3].selectbox("Sort", ["Best Match", "Newest"], index=0 if default_sort == "Best Match" else 1, key=f"jobs-sort-{page_key}")
    refresh = row[4].button("Refresh Jobs", key=f"jobs-refresh-{page_key}", use_container_width=True)
    return {
        "search": search,
        "location": location,
        "remote_only": remote_only,
        "sort_by": sort_by,
        "refresh": refresh,
    }
