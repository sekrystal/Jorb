from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import streamlit as st


PRIMARY_PAGES = ["Jobs", "Saved", "Applied", "Profile"]
OPERATOR_PAGES = ["Discovery", "Agent Activity", "Investigations", "Learning", "Autonomy Ops"]


def _format_relative_timestamp(value: str | None) -> str:
    if not value:
        return "No runs yet"
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)
    except ValueError:
        return value
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "Just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} hr ago"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''} ago"


def _health_label(health: dict[str, Any] | None) -> tuple[str, str]:
    if not health:
        return "Unavailable", "#9ca3af"
    failed_connectors = [
        connector
        for connector in health.get("connectors", [])
        if connector.get("status") in {"failed", "circuit_open"}
    ]
    if failed_connectors:
        return "Needs attention", "#ef4444"
    runtime_phase = (health.get("runtime_phase") or "").lower()
    if runtime_phase in {"running", "sleeping", "queued", "running_bounded_cycle"}:
        return "Healthy", "#10b981"
    if runtime_phase in {"paused", "idle"}:
        return "Paused", "#f59e0b"
    return "Healthy", "#10b981"


def _render_sidebar_chrome(*, title: str, caption: str) -> None:
    st.sidebar.markdown(
        """
        <style>
        section[data-testid="stSidebar"] .jorb-sidebar-title {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 0.25rem;
        }
        section[data-testid="stSidebar"] .jorb-system-card {
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 0.8rem;
            padding: 0.85rem 0.9rem;
            background: #ffffff;
            margin-top: 1rem;
        }
        section[data-testid="stSidebar"] .jorb-system-label {
            font-size: 0.72rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: #6b7280;
            margin-bottom: 0.5rem;
        }
        section[data-testid="stSidebar"] .jorb-system-row {
            display: flex;
            justify-content: space-between;
            gap: 0.75rem;
            font-size: 0.85rem;
            color: #111827;
            margin-bottom: 0.25rem;
        }
        section[data-testid="stSidebar"] .jorb-health-dot {
            width: 0.55rem;
            height: 0.55rem;
            border-radius: 999px;
            display: inline-block;
            margin-right: 0.35rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.sidebar.markdown(f'<div class="jorb-sidebar-title">{title}</div>', unsafe_allow_html=True)
    st.sidebar.caption(caption)


def _render_system_status(*, stats: dict[str, Any] | None, runtime: dict[str, Any] | None, health: dict[str, Any] | None) -> None:
    last_run = _format_relative_timestamp((runtime or {}).get("last_successful_cycle_at"))
    jobs_found = (stats or {}).get("total_leads", "n/a")
    health_text, health_color = _health_label(health)
    st.sidebar.markdown(
        f"""
        <div class="jorb-system-card">
          <div class="jorb-system-label">System status</div>
          <div class="jorb-system-row"><span>Last run</span><span>{last_run}</span></div>
          <div class="jorb-system-row"><span>Jobs found</span><span>{jobs_found}</span></div>
          <div class="jorb-system-row">
            <span>Status</span>
            <span><span class="jorb-health-dot" style="background:{health_color};"></span>{health_text}</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar(
    *,
    stats: dict[str, Any] | None,
    runtime: dict[str, Any] | None,
    health: dict[str, Any] | None,
) -> tuple[str, bool]:
    _render_sidebar_chrome(title="Jorb", caption="Jobs-first workbench")
    primary_page = st.sidebar.radio("Navigate", PRIMARY_PAGES, index=0, label_visibility="collapsed")
    _render_system_status(stats=stats, runtime=runtime, health=health)
    with st.sidebar.expander("Admin / debug", expanded=False):
        st.caption("Open validation tools without changing the jobs-first shell.")
        open_operator_console = st.button("Open validation tools", use_container_width=True)
    return primary_page, open_operator_console


def render_operator_sidebar(
    *,
    stats: dict[str, Any] | None,
    runtime: dict[str, Any] | None,
    health: dict[str, Any] | None,
) -> tuple[str, bool]:
    _render_sidebar_chrome(title="Jorb Operator", caption="Diagnostics and runtime control")
    operator_page = st.sidebar.radio("Operator navigate", OPERATOR_PAGES, index=0, label_visibility="collapsed")
    _render_system_status(stats=stats, runtime=runtime, health=health)
    back_to_jobs = st.sidebar.button("Back to jobs shell", use_container_width=True)
    return operator_page, back_to_jobs
