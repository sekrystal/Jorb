from __future__ import annotations

from typing import Any, Callable

import streamlit as st


def render_job_card_styles() -> None:
    st.markdown(
        """
        <style>
        .jorb-job-card {
            background: #ffffff;
            border: 1px solid rgba(15, 23, 42, 0.10);
            border-radius: 16px;
            padding: 1rem 1.05rem;
            margin-bottom: 0.9rem;
            box-shadow: 0 6px 16px rgba(15, 23, 42, 0.04);
        }
        .jorb-job-card.saved { border-color: rgba(59, 130, 246, 0.35); }
        .jorb-job-card.applied { border-color: rgba(16, 185, 129, 0.35); }
        .jorb-job-title { font-size: 1.1rem; font-weight: 600; color: #111827; margin-bottom: 0.2rem; }
        .jorb-job-meta { color: #4b5563; font-size: 0.9rem; display: flex; flex-wrap: wrap; gap: 0.45rem; margin-bottom: 0.65rem; }
        .jorb-job-desc { color: #4b5563; font-size: 0.92rem; margin-bottom: 0.7rem; }
        .jorb-job-explainer {
            background: #eef4ff;
            border: 1px solid #dbe7ff;
            color: #1e3a8a;
            border-radius: 10px;
            padding: 0.7rem 0.8rem;
            font-size: 0.88rem;
            margin-bottom: 0.7rem;
        }
        .jorb-job-tags { display: flex; flex-wrap: wrap; gap: 0.45rem; margin-bottom: 0.7rem; }
        .jorb-job-tag {
            background: #f3f4f6;
            color: #374151;
            border-radius: 999px;
            padding: 0.22rem 0.55rem;
            font-size: 0.76rem;
            display: inline-block;
        }
        .jorb-score-wrap { text-align: right; }
        .jorb-score {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 3.25rem;
            height: 3.25rem;
            border-radius: 12px;
            font-weight: 600;
            font-size: 1rem;
            border: 1px solid transparent;
        }
        .jorb-score.strong { background: #dcfce7; color: #166534; border-color: #bbf7d0; }
        .jorb-score.medium { background: #fef3c7; color: #92400e; border-color: #fde68a; }
        .jorb-score.stretch { background: #ffedd5; color: #9a3412; border-color: #fdba74; }
        .jorb-score-label { color: #6b7280; font-size: 0.74rem; margin-top: 0.25rem; }
        .jorb-job-fallback { color: #9ca3af; font-style: italic; }
        .jorb-job-state { color: #2563eb; font-size: 0.76rem; margin-bottom: 0.4rem; font-weight: 600; }
        .jorb-job-state.applied { color: #059669; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _score_class(match_label: str) -> str:
    lowered = (match_label or "").lower()
    if "strong" in lowered:
        return "strong"
    if "medium" in lowered:
        return "medium"
    return "stretch"


def _state_label(job: dict[str, Any]) -> str:
    state = job.get("state") or "new"
    if state == "saved":
        return '<div class="jorb-job-state">Saved</div>'
    if state == "applied":
        return '<div class="jorb-job-state applied">Applied</div>'
    return ""


def render_job_card(
    job: dict[str, Any],
    *,
    page_key: str,
    on_open: Callable[[], None],
    on_save: Callable[[], None],
    on_apply: Callable[[], None],
    on_dismiss: Callable[[], None],
) -> None:
    render_job_card_styles()
    card_class = f"jorb-job-card {job.get('state', 'new')}"
    description = job.get("description") or '<span class="jorb-job-fallback">TODO: backend did not return a short description.</span>'
    work_mode = job.get("work_mode") or "TODO work mode"
    tags = "".join(f'<span class="jorb-job-tag">{tag}</span>' for tag in job.get("tags", [])[:4])
    state_label = _state_label(job)
    metadata = " • ".join(item for item in [job.get("posted_date"), job.get("salary"), job.get("source")] if item)
    st.markdown(
        f"""
        <div class="{card_class}">
          {state_label}
          <div style="display:flex; gap:1rem; justify-content:space-between; align-items:flex-start;">
            <div style="flex:1;">
              <div class="jorb-job-title">{job.get("title") or "TODO title"}</div>
              <div class="jorb-job-meta">
                <span><strong>{job.get("company") or "TODO company"}</strong></span>
                <span>{job.get("location") or "TODO location"}</span>
                <span>{work_mode}</span>
              </div>
            </div>
            <div class="jorb-score-wrap">
              <div class="jorb-score {_score_class(job.get('match_label') or '')}">{job.get("match_score_display") or "n/a"}</div>
              <div class="jorb-score-label">{job.get("match_label") or "TODO match label"}</div>
            </div>
          </div>
          <div class="jorb-job-desc">{description}</div>
          <div class="jorb-job-explainer">{job.get("explanation") or "TODO: backend did not return a recommendation explanation."}</div>
          <div class="jorb-job-tags">{tags}</div>
          <div class="jorb-job-meta">{metadata}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    action_cols = st.columns([1.15, 1, 1, 1.1])
    if action_cols[0].button("Open details", key=f"open-{page_key}-{job['id']}", use_container_width=True):
        on_open()
    if action_cols[1].button("Save", key=f"save-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "saved"):
        on_save()
    if action_cols[2].button("Apply", key=f"apply-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "applied"):
        on_apply()
    if action_cols[3].button("Dismiss", key=f"dismiss-{page_key}-{job['id']}", use_container_width=True):
        on_dismiss()
