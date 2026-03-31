from __future__ import annotations

from html import escape
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
            padding: 1rem 1.05rem 1rem 1.05rem;
            margin-bottom: 0.9rem;
            box-shadow: 0 6px 16px rgba(15, 23, 42, 0.04);
            overflow: hidden;
        }
        .jorb-job-card.saved { border-color: rgba(59, 130, 246, 0.35); }
        .jorb-job-card.applied { border-color: rgba(16, 185, 129, 0.35); }
        .jorb-job-card.seen { border-color: rgba(148, 163, 184, 0.35); }
        .jorb-job-card.dismissed { border-color: rgba(148, 163, 184, 0.28); }
        .jorb-job-card.selected {
            border-color: rgba(15, 23, 42, 0.25);
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
        }
        .jorb-job-topline {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            justify-content: space-between;
            gap: 0.5rem;
            margin-bottom: 0.65rem;
        }
        .jorb-job-eyebrow {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #64748b;
        }
        .jorb-job-source-chip {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.18rem 0.55rem;
            background: #f8fafc;
            border: 1px solid rgba(15, 23, 42, 0.08);
            color: #475569;
            font-size: 0.74rem;
            font-weight: 600;
            max-width: 100%;
            overflow-wrap: anywhere;
        }
        .jorb-job-header {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 1rem;
            margin-bottom: 0.75rem;
        }
        .jorb-job-title-block {
            flex: 1;
            min-width: 0;
        }
        .jorb-job-title {
            font-size: 1.1rem;
            line-height: 1.35;
            font-weight: 600;
            color: #111827;
            margin-bottom: 0.25rem;
            overflow-wrap: anywhere;
        }
        .jorb-job-company-row {
            color: #4b5563;
            font-size: 0.9rem;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.35rem;
            margin-bottom: 0.2rem;
            min-width: 0;
        }
        .jorb-job-company {
            color: #111827;
            font-weight: 600;
            overflow-wrap: anywhere;
        }
        .jorb-job-submeta {
            color: #4b5563;
            font-size: 0.82rem;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.35rem;
            min-width: 0;
        }
        .jorb-meta-sep {
            color: #9ca3af;
        }
        .jorb-work-mode-pill {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.15rem 0.5rem;
            font-size: 0.74rem;
            font-weight: 600;
            text-transform: capitalize;
            background: #f3f4f6;
            color: #374151;
        }
        .jorb-work-mode-pill.remote { background: #dbeafe; color: #1d4ed8; }
        .jorb-work-mode-pill.hybrid { background: #ede9fe; color: #6d28d9; }
        .jorb-work-mode-pill.onsite { background: #f3f4f6; color: #374151; }
        .jorb-job-desc {
            color: #4b5563;
            font-size: 0.92rem;
            line-height: 1.5;
            margin-bottom: 0.85rem;
            display: -webkit-box;
            -webkit-box-orient: vertical;
            -webkit-line-clamp: 4;
            overflow: hidden;
            overflow-wrap: anywhere;
        }
        .jorb-job-section-label {
            color: #6b7280;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            margin-bottom: 0.3rem;
        }
        .jorb-job-explainer {
            background: #eef4ff;
            border: 1px solid #dbe7ff;
            color: #1e3a8a;
            border-radius: 10px;
            padding: 0.7rem 0.8rem;
            font-size: 0.88rem;
            line-height: 1.45;
            margin-bottom: 0.85rem;
            overflow-wrap: anywhere;
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
        .jorb-score-wrap { text-align: right; flex-shrink: 0; width: 4.7rem; }
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
        .jorb-job-state {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.2rem 0.55rem;
            font-size: 0.72rem;
            margin-bottom: 0.55rem;
            font-weight: 700;
        }
        .jorb-job-state.saved { background: #eff6ff; color: #2563eb; }
        .jorb-job-state.applied { background: #ecfdf5; color: #059669; }
        .jorb-job-state.seen { background: #f8fafc; color: #475569; }
        .jorb-job-state.new { background: #eef2ff; color: #4338ca; }
        .jorb-job-state.dismissed { background: #f1f5f9; color: #475569; }
        .jorb-job-metadata {
            color: #6b7280;
            font-size: 0.8rem;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.35rem;
            margin-bottom: 0.1rem;
            overflow-wrap: anywhere;
        }
        .jorb-job-actions-note {
            color: #64748b;
            font-size: 0.76rem;
            margin-top: 0.2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _score_class(match_label: str) -> str:
    lowered = (match_label or "").lower()
    if "high" in lowered or "strong" in lowered:
        return "strong"
    if "medium" in lowered:
        return "medium"
    return "stretch"


def _state_label(job: dict[str, Any]) -> str:
    state = job.get("state") or "new"
    if state == "saved":
        return '<div class="jorb-job-state saved">Saved</div>'
    if state == "applied":
        return '<div class="jorb-job-state applied">Applied</div>'
    if state == "seen":
        return '<div class="jorb-job-state seen">Seen</div>'
    if state == "dismissed":
        return '<div class="jorb-job-state dismissed">Dismissed</div>'
    return '<div class="jorb-job-state new">New</div>'


def _work_mode_class(work_mode: str) -> str:
    normalized = (work_mode or "").strip().lower()
    if normalized in {"remote", "hybrid", "onsite"}:
        return normalized
    return "unknown"


def _meta_span(value: str, *, extra_class: str = "") -> str:
    classes = " ".join(part for part in ["jorb-job-meta-item", extra_class] if part)
    return f'<span class="{classes}">{escape(value)}</span>'


def build_job_card_markup(job: dict[str, Any], *, selected: bool) -> str:
    selected_class = " selected" if selected else ""
    card_class = f"jorb-job-card {escape(str(job.get('state', 'new')))}{selected_class}"
    description = job.get("description") or "TODO: backend did not return a short description."
    explanation = job.get("explanation") or "TODO: backend did not return a recommendation explanation."
    work_mode = job.get("work_mode") or "TODO work mode"
    tags = "".join(
        f'<span class="jorb-job-tag">{escape(str(tag))}</span>'
        for tag in job.get("tags", [])[:4]
        if str(tag).strip()
    )
    state_label = _state_label(job)
    metadata_items = [
        item
        for item in [
            str(job.get("posted_date") or "").strip(),
            str(job.get("salary") or "").strip(),
            str(job.get("source") or "").strip(),
        ]
        if item
    ]
    metadata = ""
    if metadata_items:
        parts: list[str] = []
        for index, item in enumerate(metadata_items):
            if index > 0:
                parts.append('<span class="jorb-meta-sep">&bull;</span>')
            parts.append(_meta_span(item))
        metadata = f'<div class="jorb-job-metadata">{"".join(parts)}</div>'
    source_chip = escape(str(job.get("source") or "unknown source"))
    return f"""
        <div class="{card_class}">
          {state_label}
          <div class="jorb-job-topline">
            <div class="jorb-job-eyebrow">Opportunity</div>
            <div class="jorb-job-source-chip">{source_chip}</div>
          </div>
          <div class="jorb-job-header">
            <div class="jorb-job-title-block">
              <div class="jorb-job-title">{escape(str(job.get("title") or "TODO title"))}</div>
              <div class="jorb-job-company-row">
                <span class="jorb-job-company">{escape(str(job.get("company") or "TODO company"))}</span>
                <span class="jorb-meta-sep">&bull;</span>
                {_meta_span(str(job.get("location") or "TODO location"))}
              </div>
              <div class="jorb-job-submeta">
                <span class="jorb-work-mode-pill {_work_mode_class(work_mode)}">{escape(work_mode)}</span>
              </div>
            </div>
            <div class="jorb-score-wrap">
              <div class="jorb-score {_score_class(str(job.get('match_label') or ''))}">{escape(str(job.get("match_score_display") or "n/a"))}</div>
              <div class="jorb-score-label">{escape(str(job.get("match_label") or "TODO match label"))}</div>
            </div>
          </div>
          <div class="jorb-job-section-label">Summary</div>
          <div class="jorb-job-desc">{escape(str(description))}</div>
          <div class="jorb-job-section-label">Why this matches</div>
          <div class="jorb-job-explainer">{escape(str(explanation))}</div>
          <div class="jorb-job-tags">{tags}</div>
          {metadata}
          <div class="jorb-job-actions-note">Open details to review the full rationale and description before you act.</div>
        </div>
    """


def render_job_card(
    job: dict[str, Any],
    *,
    page_key: str,
    selected: bool,
    on_open: Callable[[], None],
    on_save: Callable[[], None],
    on_apply: Callable[[], None],
    on_dismiss: Callable[[], None],
    dismiss_label: str = "Dismiss",
) -> None:
    render_job_card_styles()
    st.markdown(
        build_job_card_markup(job, selected=selected),
        unsafe_allow_html=True,
    )
    action_cols = st.columns([1.1, 1, 1, 1])
    if action_cols[0].button("Details", key=f"open-{page_key}-{job['id']}", use_container_width=True):
        on_open()
    if action_cols[1].button("Save", key=f"save-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "saved"):
        on_save()
    if action_cols[2].button("Apply", key=f"apply-{page_key}-{job['id']}", use_container_width=True, disabled=job.get("state") == "applied"):
        on_apply()
    if action_cols[3].button(dismiss_label, key=f"dismiss-{page_key}-{job['id']}", use_container_width=True):
        on_dismiss()
