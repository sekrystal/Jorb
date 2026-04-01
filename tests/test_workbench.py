from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from services.profile_ingest import build_profile_review_rows
from ui import app as ui_app
from ui.components import sidebar as sidebar_component
from ui.components.topbar import build_jobs_filters_panel_copy, build_jobs_page_header_copy, count_active_job_filters
from ui.app import filter_and_sort_table
from ui.components.job_card import build_job_card_markup
from ui.screens.jobs import (
    build_jobs_action_feedback,
    build_job_detail_panel_markup,
    build_jobs_detail_empty_state_markup,
    build_jobs_empty_state_markup,
    build_jobs_intro_state_markup,
    build_jobs_search_loading_message,
    build_job_view_model,
    build_jobs_empty_state_view_model,
    build_manual_search_feedback,
    build_search_state_view_model,
    filter_restorable_dismissed_leads,
    jobs_backend_gap_frame,
    normalize_job_search_query,
    render_search_status_region,
    _filter_jobs,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_filter_and_sort_table_filters_by_search_and_status() -> None:
    table = pd.DataFrame(
        [
            {
                "company": "Mercor",
                "title": "Deployment Strategist",
                "lead_type": "combined",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "applied",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T10:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-17T10:00:00Z"),
            },
            {
                "company": "Linear",
                "title": "Strategic Operations Lead",
                "lead_type": "listing",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T09:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-16T10:00:00Z"),
            },
        ]
    )

    filtered = filter_and_sort_table(
        table,
        {
            "search": "merc",
            "lead_type": "all",
            "freshness": "all",
            "fit": "all",
            "status": "applied",
            "surfaced_since": None,
            "surfaced_until": None,
            "posted_since": None,
            "posted_until": None,
            "sort_mode": "Company A-Z",
        },
    )

    assert filtered["company"].tolist() == ["Mercor"]


def test_filter_and_sort_table_supports_richer_tracker_statuses() -> None:
    table = pd.DataFrame(
        [
            {
                "company": "Mercor",
                "title": "Deployment Strategist",
                "lead_type": "combined",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "recruiter screen",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T10:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-17T10:00:00Z"),
            },
            {
                "company": "Linear",
                "title": "Strategic Operations Lead",
                "lead_type": "listing",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "saved",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T09:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-16T10:00:00Z"),
            },
        ]
    )

    filtered = filter_and_sort_table(
        table,
        {
            "search": "",
            "lead_type": "all",
            "freshness": "all",
            "fit": "all",
            "status": "recruiter screen",
            "surfaced_since": None,
            "surfaced_until": None,
            "posted_since": None,
            "posted_until": None,
            "sort_mode": "Company A-Z",
        },
    )

    assert filtered["company"].tolist() == ["Mercor"]


def test_filter_and_sort_table_sorts_by_freshness_and_date() -> None:
    table = pd.DataFrame(
        [
            {
                "company": "RecentCo",
                "title": "Ops Lead",
                "lead_type": "listing",
                "freshness": "recent",
                "fit": "stretch",
                "confidence": "medium",
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T09:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-12T10:00:00Z"),
            },
            {
                "company": "FreshCo",
                "title": "Chief of Staff",
                "lead_type": "listing",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T08:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-17T10:00:00Z"),
            },
        ]
    )

    filtered = filter_and_sort_table(
        table,
        {
            "search": "",
            "lead_type": "all",
            "freshness": "all",
            "fit": "all",
            "status": "all",
            "surfaced_since": None,
            "surfaced_until": None,
            "posted_since": None,
            "posted_until": None,
            "sort_mode": "Freshest first",
        },
    )

    assert filtered["company"].tolist() == ["FreshCo", "RecentCo"]


def test_filter_and_sort_table_sorts_by_highest_recommendation_first() -> None:
    table = pd.DataFrame(
        [
            {
                "company": "LowerRanked",
                "title": "Ops Lead",
                "lead_type": "listing",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "recommendation_score": 6.1,
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T09:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-17T10:00:00Z"),
            },
            {
                "company": "HigherRanked",
                "title": "Chief of Staff",
                "lead_type": "listing",
                "freshness": "recent",
                "fit": "stretch",
                "confidence": "medium",
                "recommendation_score": 8.4,
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T08:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-16T10:00:00Z"),
            },
        ]
    )

    filtered = filter_and_sort_table(
        table,
        {
            "search": "",
            "lead_type": "all",
            "freshness": "all",
            "fit": "all",
            "status": "all",
            "surfaced_since": None,
            "surfaced_until": None,
            "posted_since": None,
            "posted_until": None,
            "sort_mode": "Highest recommendation first",
        },
    )

    assert filtered["company"].tolist() == ["HigherRanked", "LowerRanked"]


def test_count_active_job_filters_counts_only_visible_jobs_filters() -> None:
    assert count_active_job_filters(search="ops", location="Remote", remote_only=True) == 3
    assert count_active_job_filters(search=" ", location="", remote_only=False) == 0


def test_build_jobs_filters_panel_copy_reports_active_filter_count() -> None:
    empty_copy = build_jobs_filters_panel_copy(active_filter_count=0)
    active_copy = build_jobs_filters_panel_copy(active_filter_count=2)

    assert empty_copy == {
        "eyebrow": "Filters",
        "description": "Narrow the jobs list without leaving the current results.",
        "count_label": "No active filters",
    }
    assert active_copy["eyebrow"] == "Filters"
    assert active_copy["count_label"] == "2 active filters"


def test_build_jobs_page_header_copy_describes_workspace_views() -> None:
    jobs_copy = build_jobs_page_header_copy(title="Jobs")
    dismissed_copy = build_jobs_page_header_copy(title="Dismissed")

    assert jobs_copy["eyebrow"] == "Workspace"
    assert "set what you want" in jobs_copy["description"].lower()
    assert dismissed_copy["title"] == "Dismissed"
    assert "restore" in dismissed_copy["description"].lower()


def test_build_query_includes_backend_search_param_when_query_present() -> None:
    query = ui_app.build_query(
        freshness_days=14,
        include_hidden=False,
        include_unqualified=False,
        include_signal_only=False,
        q="chief of staff",
    )

    assert query.startswith("/leads?")
    assert "q=chief+of+staff" in query


def test_fetch_json_returns_search_meta_for_failed_backend_search(monkeypatch) -> None:
    captured: dict[str, str] = {}

    class FakeStreamlit:
        session_state: dict[str, object] = {}

        def error(self, value: str) -> None:
            captured["error"] = value

    def fail(_path: str, _revision: int) -> object:
        raise requests.exceptions.RequestException("backend timeout")

    monkeypatch.setattr(ui_app, "st", FakeStreamlit())
    monkeypatch.setattr(ui_app, "_fetch_json_cached", fail)

    payload = ui_app.fetch_json("/leads?freshness_window_days=14&q=chief+of+staff")

    assert "backend timeout" in captured["error"]
    assert payload["items"] == []
    assert payload["search_meta"]["query"] == "chief of staff"
    assert payload["search_meta"]["status"] == "error"


def test_build_profile_review_rows_include_skills_competencies_and_preferences() -> None:
    rows = build_profile_review_rows(
        {
            "confirmed_skills_json": ["sql", "stakeholder management"],
            "competencies_json": ["process design"],
            "explicit_preferences_json": ["hands-on teams"],
        }
    )

    fields = {row["field"]: row["value"] for row in rows}
    assert fields["Confirmed skills"] == "sql, stakeholder management"
    assert fields["Competencies"] == "process design"
    assert fields["Explicit preferences"] == "hands-on teams"


def test_recent_search_runs_frame_exposes_query_and_failure_observability() -> None:
    frame = ui_app.recent_search_runs_frame(
        [
            {
                "created_at": "2026-03-29T12:30:00Z",
                "source_key": "search_web_ats",
                "worker_name": "ats_resolver",
                "provider": "duckduckgo_html",
                "status": "empty",
                "live": True,
                "zero_yield": True,
                "queries": ['site:job-boards.greenhouse.io "chief of staff"', '"chief of staff" careers'],
                "query_count": 2,
                "result_count": 0,
                "failure_classification": "search_provider_failure",
                "error": "provider self-links only",
            }
        ]
    )

    row = frame.iloc[0].to_dict()
    assert row["source"] == "search_web_ats"
    assert row["worker"] == "ats_resolver"
    assert row["live"] == "yes"
    assert row["zero_yield"] == "yes"
    assert row["query_count"] == 2
    assert row["result_count"] == 0
    assert row["failure_classification"] == "search_provider_failure"
    assert row["queries"] == 'site:job-boards.greenhouse.io "chief of staff" | "chief of staff" careers'


def test_lead_frame_includes_recommendation_action_label() -> None:
    frame = ui_app.lead_frame(
        [
            {
                "id": 1,
                "url": "https://example.com/job",
                "surfaced_at": "2026-03-18T10:00:00Z",
                "posted_at": "2026-03-17T10:00:00Z",
                "application_updated_at": None,
                "company_name": "Mercor",
                "primary_title": "Deployment Strategist",
                "lead_type": "combined",
                "freshness_label": "fresh",
                "qualification_fit_label": "strong fit",
                "confidence_label": "high",
                "current_status": "",
                "source_platform": "greenhouse",
                "source_type": "greenhouse",
                "source_lineage": "greenhouse",
                "evidence_json": {},
                "last_agent_action": "",
                "saved": False,
                "applied": False,
                "date_saved": None,
                "date_applied": None,
                "application_notes": "",
                "next_action": None,
                "follow_up_due": False,
                "score_breakdown_json": {
                    "final_score": 8.4,
                    "action_label": "Act now",
                    "explanation": {"headline": "Strong recommendation at 8.40 with high confidence."},
                },
            }
        ]
    )

    assert frame["recommendation_action"].tolist() == ["Act now"]
    assert frame["recommendation_score"].tolist() == [8.4]
    assert frame["match_summary"].tolist() == ["Strong recommendation at 8.40 with high confidence."]


def test_recommendation_action_summary_uses_action_label_and_explanation() -> None:
    summary = ui_app.recommendation_action_summary(
        {
            "score_breakdown_json": {
                "action_label": "Seek referral",
                "action_explanation": "Seek referral because the source signal is still weak.",
            }
        }
    )

    assert summary == "Seek referral: Seek referral because the source signal is still weak."


def test_acceptance_docs_require_live_runtime_smoke_for_product_work() -> None:
    readme = Path("README.md").read_text()
    operations = Path("OPERATIONS.md").read_text()

    assert "Acceptance-Critical Validation" in readme
    assert "./scripts/runtime_self_check.sh" in readme
    assert "Live runtime smoke proof is separate from local test success." in readme
    assert "API, worker, and internal Streamlit harness validation" in readme
    assert "PRIMARY_UI_URL=http://127.0.0.1:5173 ./scripts/runtime_self_check.sh" in readme
    assert "must not be marked complete without it passing against a real running stack" in readme
    assert "Acceptance-Critical Runtime Validation" in operations
    assert "./scripts/runtime_self_check.sh" in operations
    assert "Do not treat pytest or preflight success as product proof." in operations
    assert "PRIMARY_UI_URL=http://127.0.0.1:5173" in operations
    assert "Passing pytest and preflight without live runtime smoke evidence is not enough" in operations


def test_discovery_source_matrix_frame_exposes_truth_columns() -> None:
    frame = ui_app.discovery_source_matrix_frame(
        [
            {
                "source_key": "greenhouse",
                "label": "Greenhouse",
                "classification": "working",
                "runtime_state": "live_enabled",
                "ran": True,
                "failed": False,
                "zero_yield": False,
                "run_count": 2,
                "failure_count": 0,
                "zero_yield_count": 0,
                "surfaced_jobs_count": 3,
                "toggle_key": "GREENHOUSE_ENABLED + GREENHOUSE_BOARD_TOKENS",
                "runtime_enabled": True,
                "strict_live_enabled": True,
                "trusted_for_output": True,
                "blocked_reason": None,
                "reason": "Configured boards are polled directly.",
                "summary": "ran 2 times; 3 surfaced jobs",
            },
            {
                "source_key": "search_web",
                "label": "Search Web",
                "classification": "partially_working",
                "runtime_state": "live_enabled",
                "ran": True,
                "failed": False,
                "zero_yield": True,
                "run_count": 1,
                "failure_count": 0,
                "zero_yield_count": 1,
                "surfaced_jobs_count": 0,
                "toggle_key": "SEARCH_DISCOVERY_ENABLED",
                "runtime_enabled": True,
                "strict_live_enabled": True,
                "trusted_for_output": False,
                "blocked_reason": "cooldown",
                "reason": "Search is recall expansion only.",
                "summary": "ran 1 time; 1 zero-yield run",
            },
        ]
    )

    assert frame["source"].tolist() == ["Greenhouse", "Search Web"]
    assert frame["classification"].tolist() == ["working", "partially working"]
    assert frame["ran"].tolist() == ["yes", "yes"]
    assert frame["zero_yield"].tolist() == ["no", "yes"]
    assert frame["run_count"].tolist() == [2, 1]
    assert frame["surfaced_jobs"].tolist() == [3, 0]
    assert frame["runtime_enabled"].tolist() == ["yes", "yes"]
    assert frame["trusted_for_output"].tolist() == ["yes", "no"]
    assert frame["blocked_reason"].tolist() == ["", "cooldown"]


def test_agentic_leads_frame_exposes_verified_ranked_slice_columns() -> None:
    frame = ui_app.agentic_leads_frame(
        [
            {
                "company_name": "Acme",
                "title": "Founding Operations Lead",
                "recommendation_score": 8.7,
                "rank_label": "strong",
                "confidence_label": "high",
                "freshness_label": "fresh",
                "verified": True,
                "verification_status": "active",
                "action_label": "Act now",
                "explanation": "Strong recommendation at 8.70 with high confidence.",
                "match_summary": "Strong recommendation at 8.70 with high confidence.",
                "source_platform": "yc_jobs",
                "source_provenance": "discovered_new",
                "source_lineage": "yc_jobs+search_web",
                "updated_at": "2026-03-25T12:00:00Z",
                "url": "https://www.workatastartup.com/jobs/12345",
            }
        ]
    )

    assert frame["company"].tolist() == ["Acme"]
    assert frame["verification_status"].tolist() == ["active"]
    assert frame["verified"].tolist() == ["yes"]
    assert frame["recommendation_score"].tolist() == [8.7]
    assert frame["explanation"].tolist() == ["Strong recommendation at 8.70 with high confidence."]
    assert frame["source"].tolist() == ["yc_jobs"]
    assert frame["provenance"].tolist() == ["discovered_new"]


def test_recommendation_table_explanation_prefers_structured_score_headline() -> None:
    summary = ui_app.recommendation_table_explanation(
        {
            "explanation": "Fallback explanation",
            "score_breakdown_json": {
                "action_label": "Act now",
                "action_explanation": "Move fast.",
                "explanation": {"headline": "Strong recommendation at 8.40 with high confidence."},
            },
        }
    )

    assert summary == "Strong recommendation at 8.40 with high confidence."


def test_referral_strategy_summary_uses_saved_network_matches() -> None:
    summary = ui_app.referral_strategy_summary(
        {"company_name": "Linear"},
        {
            "extracted_summary_json": {
                "network_import": {
                    "contacts": [
                        {
                            "name": "Alex Rivera",
                            "company": "Linear",
                            "company_keys": ["linear"],
                            "title": "Product Operations",
                            "relationship": "former teammate",
                            "profile_url": "https://linkedin.com/in/alex",
                            "notes": "Worked together on launch ops",
                            "location": "",
                        }
                    ]
                }
            }
        },
    )

    assert summary == "Possible referral paths: Alex Rivera at Linear (former teammate)"


def test_update_application_status_includes_structured_rejection_feedback(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_json(path: str, method: str = "GET", payload: dict | None = None):
        captured["path"] = path
        captured["method"] = method
        captured["payload"] = payload or {}
        return {"ok": True}

    monkeypatch.setattr(ui_app, "fetch_json", fake_fetch_json)

    ui_app.update_application_status(
        lead_id=17,
        current_status="rejected",
        notes="Panel wanted more direct pricing examples.",
        date_applied_value=date(2026, 3, 20),
        status_reason_code="panel_decline",
        outcome_reason_code="insufficient_pricing_depth",
    )

    assert captured["path"] == "/applications/status"
    assert captured["method"] == "POST"
    assert captured["payload"] == {
        "lead_id": 17,
        "current_status": "rejected",
        "notes": "Panel wanted more direct pricing examples.",
        "date_applied": "2026-03-20T00:00:00",
        "status_reason_code": "panel_decline",
        "outcome_reason_code": "insufficient_pricing_depth",
    }


def test_js_saved_and_applied_pages_use_shared_tracker_workspace() -> None:
    source = Path("frontend/src/views/LeadPages.tsx").read_text()

    assert 'surface="saved"' in source
    assert 'surface="applied"' in source
    assert 'description="Continue from the main jobs flow with saved roles backed by persisted tracker records."' in source
    assert 'description="Work the applied tracker as a first-class product surface with real status and follow-up data."' in source
    assert "Open Saved queue" in source
    assert "Open Applied tracker" in source
    assert "Tracker timeline" in source
    assert "Follow-up due" in source


def test_streamlit_primary_navigation_keeps_product_pages_separate_from_operator_pages() -> None:
    assert sidebar_component.PRIMARY_PAGES == ["Jobs", "Saved", "Applied", "Dismissed", "Preferences"]
    assert sidebar_component.OPERATOR_PAGES == ["Discovery", "Agent Activity", "Investigations", "Learning", "Autonomy Ops"]
    assert not set(sidebar_component.PRIMARY_PAGES) & set(sidebar_component.OPERATOR_PAGES)


def test_streamlit_jobs_shell_demotes_job_link_and_moves_operator_access_out_of_primary_nav() -> None:
    app_source = (REPO_ROOT / "ui/app.py").read_text()
    sidebar_source = (REPO_ROOT / "ui/components/sidebar.py").read_text()
    jobs_source = (REPO_ROOT / "ui/screens/jobs.py").read_text()

    assert 'with st.expander("Add a job link", expanded=False):' in app_source
    assert app_source.index('render_jobs_screen(') < app_source.index('with st.expander("Add a job link", expanded=False):')
    assert "show_operator_console" in app_source
    assert "render_operator_sidebar" in app_source
    assert 'with st.sidebar.expander("Advanced filters", expanded=False):' not in app_source
    assert 'with st.sidebar.expander("Workspace tools", expanded=False):' in sidebar_source
    assert "Open workspace tools without leaving the jobs-first shell." in sidebar_source
    assert 'st.button("Open workspace tools", use_container_width=True)' in sidebar_source
    assert "Back to jobs shell" in sidebar_source
    assert 'with st.sidebar.expander("Operator surfaces"' not in sidebar_source
    assert 'with st.expander("Backend/UI field gaps", expanded=False):' not in jobs_source
    assert 'st.button("Run manual search"' not in jobs_source


def test_streamlit_primary_shell_copy_avoids_internal_system_language() -> None:
    app_source = (REPO_ROOT / "ui/app.py").read_text()
    sidebar_source = (REPO_ROOT / "ui/components/sidebar.py").read_text()

    assert '#### Search setup' in app_source
    assert 'Search setup saved. Jorb will use these preferences for discovery and ranking.' in app_source
    assert 'Set what you want, review matched jobs, and act from one clear workspace.' in app_source
    assert 'Inspect what JORB stores locally for your profile' not in app_source
    assert 'Local network import' not in app_source
    assert 'No matching jobs found. Try adjusting filters or check back after the next refresh.' in app_source
    assert "Open workspace tools" in sidebar_source
    assert "Save profile and enter discovery" not in app_source
    assert "Step 4: Enter discovery" not in app_source
    assert "the next discovery cycle" not in app_source
    assert "Admin / debug" not in sidebar_source
    assert "Open internal harness" not in sidebar_source


def test_rejection_feedback_summary_surfaces_structured_buckets() -> None:
    summary = ui_app.rejection_feedback_summary(
        {
            "status_reason_code": "panel_decline",
            "outcome_reason_code": "insufficient_b2b_saas_depth",
            "application_notes": "Strong operator profile, but the panel wanted deeper pricing experience.",
        }
    )

    assert summary == "Detected rejection themes: Interview performance, Domain depth, Pricing depth"


def test_fetch_json_returns_empty_leads_payload_on_request_failure(monkeypatch) -> None:
    captured: list[str] = []
    captured_timeout: list[int] = []

    def fake_request(*args, **kwargs):
        captured_timeout.append(kwargs.get("timeout"))
        raise requests.exceptions.ReadTimeout("timeout")

    def fake_error(message: str) -> None:
        captured.append(message)

    monkeypatch.setattr(ui_app.requests, "request", fake_request)
    monkeypatch.setattr(ui_app.st, "error", fake_error)

    payload = ui_app.fetch_json("/leads?freshness_window_days=14")

    assert payload == {"items": [], "search_meta": None}
    assert captured
    assert captured_timeout == [10]


def test_cached_get_fetch_reuses_response_until_revision_changes(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_request_json(path: str, method: str = "GET", payload: dict | None = None):
        calls.append((path, method))
        return {"path": path, "call_count": len(calls)}

    ui_app._fetch_json_cached.clear()
    monkeypatch.setattr(ui_app, "_request_json", fake_request_json)

    first = ui_app._fetch_json_cached("/candidate-profile", 0)
    second = ui_app._fetch_json_cached("/candidate-profile", 0)
    third = ui_app._fetch_json_cached("/candidate-profile", 1)

    assert first == {"path": "/candidate-profile", "call_count": 1}
    assert second == first
    assert third == {"path": "/candidate-profile", "call_count": 2}
    assert calls == [
        ("/candidate-profile", "GET"),
        ("/candidate-profile", "GET"),
    ]


def test_profile_inventory_frame_labels_local_and_cloud_assisted_categories() -> None:
    frame = ui_app.profile_inventory_frame(
        {
            "name": "Privacy Test",
            "raw_resume_text": "operator resume",
            "preferred_titles_json": ["chief of staff"],
            "core_titles_json": ["chief of staff"],
            "preferred_domains_json": ["ai"],
            "extracted_summary_json": {
                "summary": "Saved profile",
                "resume_filename": "resume.txt",
                "structured_profile": {"targeting": {"preferred_titles": ["chief of staff"]}},
                "network_import": {
                    "contacts": [{"name": "Alex Rivera", "company": "Linear"}],
                },
            },
        }
    )

    assert frame["Category"].tolist() == [
        "Resume text",
        "Profile preferences",
        "Structured profile schema",
        "Network contacts",
        "Learning state",
    ]
    assert "Local only" in frame["Processing path"].tolist()
    assert "Cloud assisted" in frame["Processing path"].tolist()


def test_profile_inventory_export_summarizes_basic_export_behavior() -> None:
    export = ui_app.profile_inventory_export(
        {
            "name": "Privacy Test",
            "raw_resume_text": "operator resume",
            "preferred_titles_json": ["chief of staff"],
            "core_titles_json": ["chief of staff"],
            "preferred_domains_json": ["ai"],
            "extracted_summary_json": {
                "resume_filename": "resume.txt",
                "structured_profile": {"targeting": {"preferred_titles": ["chief of staff"]}},
                "learning": {"generated_queries": ["chief of staff ai"]},
            },
        }
    )

    assert export["inventory_version"] == "v1"
    assert export["profile_name"] == "Privacy Test"
    assert export["summary"]["stored_categories"] == 4
    assert export["summary"]["local_only_categories"] == 2
    assert export["summary"]["cloud_assisted_categories"] == 3
    assert export["categories"][0]["category_key"] == "resume_text"


def test_discovery_query_family_frame_flattens_metrics_for_ui() -> None:
    frame = ui_app.discovery_query_family_frame(
        {
            "query_family_metrics": {
                "ats_direct": {"queries_attempted": 2, "accepted_results": 1, "zero_visible_yield_expansions": 1},
                "careers_broad": {"queries_attempted": 3, "selected_for_expansion": 1, "visible_yield_count": 2},
            }
        }
    )

    assert frame["query_family"].tolist() == ["ats_direct", "careers_broad"]
    assert frame.loc[frame["query_family"] == "ats_direct", "accepted_results"].iloc[0] == 1
    assert frame.loc[frame["query_family"] == "careers_broad", "visible_yield_count"].iloc[0] == 2


def test_build_job_view_model_uses_real_fields_and_explicit_placeholders() -> None:
    job = build_job_view_model(
        {
            "id": 17,
            "company_name": "Mercor",
            "primary_title": "Chief of Staff",
            "saved": False,
            "applied": False,
            "rank_label": "strong",
            "freshness_label": "fresh",
            "qualification_fit_label": "strong fit",
            "confidence_label": "high",
            "source_type": "greenhouse",
            "source_lineage": "greenhouse+search_web",
            "score_breakdown_json": {
                "final_score": 8.4,
                "recommendation_band": "strong",
                "action_explanation": "Apply soon.",
                "explanation": {"headline": "Strong recommendation", "summary": "High overlap with operating scope."},
            },
            "evidence_json": {
                "location": "Remote - US",
                "location_scope": "remote_us",
                "description_text": "Run operating cadence and execution for the leadership team.",
            },
            "posted_at": "2026-03-24T10:00:00Z",
            "surfaced_at": "2026-03-24T12:00:00Z",
            "url": "https://example.com/job",
        }
    )

    assert job["company"] == "Mercor"
    assert job["work_mode"] == "remote"
    assert job["match_score_display"] == "8.4"
    assert job["source"] == "greenhouse"
    assert job["source_provenance"] == "greenhouse+search_web"
    assert "description" not in job["backend_gaps"]


def test_build_job_view_model_marks_missing_fields_explicitly() -> None:
    job = build_job_view_model(
        {
            "id": 18,
            "company_name": "UnknownCo",
            "primary_title": "Ops Lead",
            "saved": False,
            "applied": False,
            "rank_label": "weak",
            "freshness_label": "unknown",
            "qualification_fit_label": "unclear",
            "confidence_label": "low",
            "source_lineage": "signal",
            "score_breakdown_json": {},
            "evidence_json": {},
            "surfaced_at": "2026-03-24T12:00:00Z",
        }
    )

    assert "work_mode" in job["backend_gaps"]
    assert "description" in job["backend_gaps"]
    assert job["location"] == "Location not specified"


def test_build_job_view_model_exposes_explicit_source_and_provenance_fields() -> None:
    job = build_job_view_model(
        {
            "id": 19,
            "company_name": "Acme",
            "primary_title": "Founding Operations Lead",
            "saved": False,
            "applied": False,
            "rank_label": "strong",
            "freshness_label": "fresh",
            "qualification_fit_label": "strong fit",
            "confidence_label": "high",
            "source_type": "yc_jobs",
            "source_lineage": "yc_jobs+search_web",
            "score_breakdown_json": {"final_score": 8.1, "recommendation_band": "strong", "explanation": {"headline": "Strong recommendation"}},
            "evidence_json": {
                "location": "San Francisco, CA",
                "description_text": "Lead founder operations and recruiting systems.",
            },
            "surfaced_at": "2026-03-24T12:00:00Z",
            "url": "https://www.workatastartup.com/jobs/12345",
        }
    )

    assert job["source"] == "yc_jobs"
    assert job["source_provenance"] == "yc_jobs+search_web"
    assert job["tags"] == ["fresh", "strong fit", "high", "onsite"]


def test_build_job_view_model_precomputes_search_and_sort_fields() -> None:
    job = build_job_view_model(
        {
            "id": 21,
            "company_name": "Acme",
            "primary_title": "Founding Operations Lead",
            "saved": False,
            "applied": False,
            "freshness_label": "fresh",
            "qualification_fit_label": "strong fit",
            "confidence_label": "high",
            "score_breakdown_json": {"final_score": 8.9, "recommendation_band": "strong", "explanation": {"headline": "Strong recommendation"}},
            "evidence_json": {
                "location": "Remote - US",
                "location_scope": "remote_us",
                "description_text": "Build recruiting systems and run operating cadence.",
            },
            "posted_at": "2026-03-24T10:00:00Z",
        }
    )

    assert "founding operations lead" in job["_search_haystack"]
    assert job["_search_fields"]["location"] == "remote - us"
    assert job["_recommendation_sort"] == 8.9
    assert isinstance(job["_posted_at_sort"], datetime)


def test_build_job_view_model_keeps_canonical_multiline_description_readable() -> None:
    job = build_job_view_model(
        {
            "id": 22,
            "company_name": "Acme",
            "primary_title": "Founding Operations Lead",
            "saved": False,
            "applied": False,
            "freshness_label": "fresh",
            "qualification_fit_label": "strong fit",
            "confidence_label": "high",
            "score_breakdown_json": {"final_score": 8.9, "recommendation_band": "strong", "explanation": {"headline": "Strong recommendation"}},
            "evidence_json": {
                "location": "Remote - US",
                "location_scope": "remote_us",
                "description_text": "Overview\nLead operating cadence.\n\nResponsibilities\n- Build recruiting systems",
            },
            "posted_at": "2026-03-24T10:00:00Z",
        }
    )

    assert "Overview\nLead operating cadence." in job["full_description"]
    assert "<p>" not in job["full_description"]


def test_filter_jobs_uses_precomputed_search_fields_from_view_models(monkeypatch) -> None:
    jobs = [
        {
            "id": "1",
            "title": "Chief of Staff",
            "company": "Acme",
            "location": "Remote",
            "source": "greenhouse",
            "description": "Run leadership operations",
            "explanation": "Great fit",
            "tags": ["fresh"],
            "work_mode": "remote",
            "_search_fields": {
                "title": "chief of staff",
                "company": "acme",
                "location": "remote",
                "source": "greenhouse",
                "description": "run leadership operations",
                "explanation": "great fit",
                "tags": "fresh",
            },
            "_search_haystack": "chief of staff acme remote greenhouse run leadership operations great fit fresh",
            "_posted_at_sort": datetime(2026, 3, 24, tzinfo=timezone.utc),
            "_recommendation_sort": 8.4,
            "raw_lead": {"posted_at": "2026-03-24T00:00:00Z", "score_breakdown_json": {"final_score": 8.4}},
        }
    ]

    def fail_if_called(value):
        raise AssertionError(f"_searchable_text should not run for precomputed jobs: {value}")

    monkeypatch.setattr("ui.screens.jobs._searchable_text", fail_if_called)

    filtered = _filter_jobs(
        jobs,
        {
            "search": "chief remote",
            "location": "",
            "remote_only": False,
            "sort_by": "Best match",
        },
    )

    assert [job["id"] for job in filtered] == ["1"]


def test_build_job_card_markup_matches_figma_hierarchy_without_diagnostics() -> None:
    markup = build_job_card_markup(
        {
            "id": "19",
            "title": "Founding Operations Lead",
            "company": "Acme",
            "location": "San Francisco, CA",
            "work_mode": "onsite",
            "description": "Lead founder operations and recruiting systems across the business.",
            "match_score_display": "8.1",
            "match_label": "High fit",
            "explanation": "Strong overlap with operating cadence and systems ownership.",
            "tags": ["fresh", "strong fit", "high", "onsite"],
            "posted_date": "2026-03-24T12:00:00Z",
            "salary": "$180k-$210k",
            "source": "yc_jobs",
            "state": "saved",
            "source_provenance": "yc_jobs+search_web",
        },
        selected=True,
    )

    assert 'class="jorb-job-title"' in markup
    assert "Founding Operations Lead" in markup
    assert "Acme" in markup
    assert "San Francisco, CA" in markup
    assert "Opportunity" in markup
    assert "Summary" in markup
    assert "yc_jobs" in markup
    assert "Why this matches" in markup
    assert "Strong overlap with operating cadence and systems ownership." in markup
    assert "Saved" in markup
    assert "yc_jobs+search_web" not in markup
    for prohibited in ["discovery", "autonomy", "connectors", "source matrix"]:
        assert prohibited not in markup.lower()


def test_build_job_card_markup_hides_new_state_badge_and_escapes_content() -> None:
    markup = build_job_card_markup(
        {
            "id": "20",
            "title": "Chief <Ops>",
            "company": "North & South",
            "location": "Remote - US",
            "work_mode": "remote",
            "description": "Own <systems> and planning.",
            "match_score_display": "7.4",
            "match_label": "Medium fit",
            "explanation": "Fits & scales quickly.",
            "tags": ["fresh"],
            "posted_date": "2026-03-24T12:00:00Z",
            "source": "greenhouse",
            "state": "new",
        },
        selected=False,
    )

    assert "Chief &lt;Ops&gt;" in markup
    assert "North &amp; South" in markup
    assert "Own &lt;systems&gt; and planning." in markup
    assert "Fits &amp; scales quickly." in markup
    assert "New" in markup


def test_build_job_detail_panel_markup_preserves_hierarchy_and_multiline_copy() -> None:
    markup = build_job_detail_panel_markup(
        {
            "id": "19",
            "title": "Founding Operations Lead",
            "company": "Acme",
            "location": "San Francisco, CA",
            "work_mode": "onsite",
            "source": "greenhouse",
            "state": "saved",
            "tags": ["fresh", "strong fit"],
            "match_score_display": "8.1",
            "match_label": "High fit",
            "explanation": "Strong overlap with operating cadence.",
            "why_this_job": "You have led recruiting systems.",
            "what_you_are_missing": "No major gaps recorded.",
            "suggested_next_steps": "Reach out to the hiring manager.",
            "full_description": "Responsibilities\n- Build systems\n\nRequirements\n- 5+ years",
            "backend_gaps": ["salary"],
        }
    )

    assert "Selected job" in markup
    assert "Recommendation summary" in markup
    assert "Full description" in markup
    assert "jorb-job-detail-section-copy" in markup
    assert "Source gaps" in markup
    assert "salary" in markup


def test_build_job_view_model_surfaces_seen_state_and_decision_signals() -> None:
    job = build_job_view_model(
        {
            "id": 42,
            "primary_title": "Chief of Staff",
            "company_name": "Acme",
            "posted_at": "2026-03-24T12:00:00Z",
            "rank_label": "medium",
            "seen": True,
            "saved": False,
            "applied": False,
            "score_breakdown_json": {
                "final_score": 6.8,
                "match_tier": "medium",
                "top_matching_signals": ["required skill: sql", "domain: ai"],
                "missing_signals": ["missing required skill: recruiting"],
                "action_explanation": "Review before applying.",
            },
            "evidence_json": {
                "location": "Remote - US",
                "source_type": "greenhouse",
                "description_text": "Lead executive operations.",
            },
        }
    )

    assert job["state"] == "seen"
    assert job["match_label"] == "Medium fit"
    assert "Top signals: required skill: sql, domain: ai." in job["explanation"]
    assert "Missing: missing required skill: recruiting." in job["explanation"]


def test_jobs_backend_gap_frame_flattens_missing_fields() -> None:
    frame = jobs_backend_gap_frame(
        [
            {"lead_id": 1, "title": "Chief of Staff", "company": "Mercor", "backend_gaps": ["work_mode", "salary"]},
            {"lead_id": 2, "title": "Ops Lead", "company": "Linear", "backend_gaps": []},
        ]
    )

    assert frame["missing_field"].tolist() == ["work_mode", "salary"]


def test_build_search_state_view_model_reports_running_state() -> None:
    view_model = build_search_state_view_model(
        {
            "status": "running",
            "query_count": 2,
            "result_count": 0,
        }
    )

    assert view_model["tone"] == "info"
    assert view_model["badge"] == "Loading"
    assert view_model["title"] == "Search is running."
    assert "current run finishes" in view_model["detail"]


def test_normalize_job_search_query_collapses_case_spacing_and_punctuation() -> None:
    normalized = normalize_job_search_query("  Chief-of   Staff, AI  ")

    assert normalized["text"] == "chief-of staff, ai"
    assert normalized["tokens"] == ["chief", "of", "staff", "ai"]


def test_build_jobs_search_loading_message_mentions_query() -> None:
    assert build_jobs_search_loading_message("chief of staff") == "Searching jobs for 'chief of staff'..."


def test_build_search_state_view_model_reports_manual_trigger_when_no_run_exists() -> None:
    view_model = build_search_state_view_model(None)

    assert view_model["tone"] == "info"
    assert view_model["title"] == "Search has not run yet."
    assert view_model["detail"] == "Refresh jobs to load this view."


def test_build_search_state_view_model_reports_failure_state() -> None:
    view_model = build_search_state_view_model(
        {
            "status": "failed",
            "failure_classification": "timeout",
            "created_at": "2026-03-29T12:30:00Z",
        }
    )

    assert view_model["tone"] == "error"
    assert view_model["title"] == "Search failed."
    assert "timeout" in view_model["detail"]


def test_build_search_state_view_model_reports_backend_search_error_state() -> None:
    view_model = build_search_state_view_model(
        None,
        search_meta={
            "query": "chief of staff",
            "status": "error",
            "error": "timeout",
            "backend_applied": False,
        },
    )

    assert view_model["tone"] == "error"
    assert view_model["badge"] == "Error"
    assert view_model["title"] == "Search failed."
    assert "chief of staff" in view_model["detail"]


def test_build_search_state_view_model_reports_zero_results_state() -> None:
    view_model = build_search_state_view_model(
        {
            "status": "empty",
            "query_count": 1,
            "result_count": 0,
            "created_at": "2026-03-29T12:30:00Z",
        }
    )

    assert view_model["tone"] == "warning"
    assert view_model["badge"] == "Zero results"
    assert view_model["title"] == "Search finished with no matching jobs."
    assert "checked 1 query" in view_model["detail"]


def test_build_search_state_view_model_reports_success_state() -> None:
    view_model = build_search_state_view_model(
        {
            "status": "results",
            "query_count": 2,
            "result_count": 5,
            "created_at": "2026-03-29T12:30:00Z",
        }
    )

    assert view_model["tone"] == "success"
    assert view_model["badge"] == "Loaded"
    assert view_model["title"] == "Search finished successfully."
    assert "found 5 jobs across 2 queries" in view_model["detail"]


def test_build_search_state_view_model_requires_zero_yield_truth_for_zero_result_copy() -> None:
    view_model = build_search_state_view_model(
        {
            "status": "results",
            "query_count": 2,
            "result_count": 0,
            "zero_yield": False,
            "created_at": "2026-03-29T12:30:00Z",
        }
    )

    assert view_model["tone"] == "success"
    assert view_model["title"] == "Search finished successfully."
    assert "found 0 jobs across 2 queries" in view_model["detail"]


def test_render_search_status_region_renders_inline_jobs_status(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeStreamlit:
        def caption(self, value: str) -> None:
            captured["caption"] = value

        def markdown(self, value: str, unsafe_allow_html: bool = False) -> None:
            captured["markdown"] = value
            captured["unsafe_allow_html"] = unsafe_allow_html

    monkeypatch.setattr("ui.screens.jobs.st", FakeStreamlit())

    render_search_status_region(
        {
            "status": "results",
            "query_count": 2,
            "result_count": 5,
            "created_at": "2026-03-29T12:30:00Z",
        },
        visible_job_count=3,
    )

    assert captured["caption"] == "Search status"
    assert captured["unsafe_allow_html"] is True
    assert "Search finished successfully." in str(captured["markdown"])
    assert "Loaded" in str(captured["markdown"])
    assert "The latest run found 5 jobs across 2 queries" in str(captured["markdown"])
    assert "3 jobs in view" in str(captured["markdown"])


def test_render_search_status_region_prefers_backend_search_meta_over_latest_search_run(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeStreamlit:
        def caption(self, value: str) -> None:
            captured["caption"] = value

        def markdown(self, value: str, unsafe_allow_html: bool = False) -> None:
            captured["markdown"] = value
            captured["unsafe_allow_html"] = unsafe_allow_html

    monkeypatch.setattr("ui.screens.jobs.st", FakeStreamlit())

    render_search_status_region(
        {
            "status": "results",
            "query_count": 5,
            "result_count": 99,
            "created_at": "2026-03-29T12:30:00Z",
        },
        visible_job_count=2,
        search_meta={
            "query": "chief of staff",
            "status": "results",
            "result_count": 2,
            "searched_fields": ["title", "company", "location", "description", "tags", "explanation", "source"],
            "backend_applied": True,
        },
    )

    assert captured["caption"] == "Search status"
    assert "Search results loaded." in str(captured["markdown"])
    assert "Found 2 jobs for &#x27;chief of staff&#x27;" in str(captured["markdown"])
    assert "99 jobs" not in str(captured["markdown"])


def test_build_manual_search_feedback_reports_surfaced_jobs() -> None:
    feedback = build_manual_search_feedback(
        {
            "surfaced_count": 2,
            "discovery_summary": "Jobs found and surfaced normally.",
        }
    )

    assert feedback == {
        "tone": "success",
        "message": "Refresh finished. Surfaced 2 jobs. Jobs found and surfaced normally.",
    }


def test_build_manual_search_feedback_reports_zero_yield_summary() -> None:
    feedback = build_manual_search_feedback(
        {
            "surfaced_count": 0,
            "discovery_summary": "No jobs found from any connector.",
        }
    )

    assert feedback == {
        "tone": "warning",
        "message": "Refresh finished. Surfaced 0 jobs. No jobs found from any connector.",
    }


def test_build_search_state_view_model_reports_blocked_automatic_discovery() -> None:
    view_model = build_search_state_view_model(
        None,
        discovery_status={
            "agentic_slice_status": {
                "status": "no_verified_jobs",
                "summary": "No verified search-discovered jobs are currently available in the UI.",
            },
            "source_matrix": [
                {"source_key": "greenhouse", "label": "Greenhouse", "classification": "not_working", "failed": False},
                {"source_key": "ashby", "label": "Ashby", "classification": "not_working", "failed": False},
                {"source_key": "search_web", "label": "Search Web", "classification": "not_working", "failed": False},
                {"source_key": "search_web_scrape_fallback", "label": "Scrape Fallback", "classification": "not_working", "failed": False},
                {"source_key": "x_search", "label": "X Search", "classification": "not_working", "failed": False},
            ],
        },
    )

    assert view_model["tone"] == "error"
    assert view_model["title"] == "Automatic discovery is not runnable."
    assert "Automatic discovery is currently blocked" in view_model["detail"]


def test_build_jobs_empty_state_view_model_uses_discovery_zero_yield_status() -> None:
    view_model = build_jobs_empty_state_view_model(
        None,
        total_job_count=0,
        filters={"search": "", "location": "", "remote_only": False},
        discovery_status={
            "agentic_slice_status": {
                "status": "zero_yield",
                "summary": "Zero verified jobs this cycle. Search discovery returned no accepted results after 3 attempt(s): provider self-links only.",
            },
            "source_matrix": [
                {"source_key": "search_web", "label": "Search Web", "classification": "partially_working", "failed": False},
            ],
        },
    )

    assert view_model["tone"] == "warning"
    assert view_model["title"] == "Discovery ran but found no verified jobs."
    assert "provider self-links only" in view_model["detail"]


def test_build_jobs_empty_state_view_model_reports_filter_hidden_results() -> None:
    view_model = build_jobs_empty_state_view_model(
        {
            "status": "results",
            "query_count": 2,
            "result_count": 5,
            "created_at": "2026-03-29T12:30:00Z",
        },
        total_job_count=5,
        filters={"search": "staff", "location": "", "remote_only": False},
    )

    assert view_model["title"] == "No jobs match the current filters."
    assert view_model["show_clear_filters"] is True
    assert view_model["tone"] == "info"


def test_build_jobs_empty_state_view_model_uses_backend_search_meta_for_zero_results() -> None:
    view_model = build_jobs_empty_state_view_model(
        None,
        total_job_count=0,
        filters={"search": "chief of staff", "location": "", "remote_only": False},
        search_meta={
            "query": "chief of staff",
            "status": "empty",
            "result_count": 0,
            "searched_fields": ["title", "company", "location", "description", "tags", "explanation", "source"],
            "backend_applied": True,
        },
    )

    assert view_model["tone"] == "warning"
    assert view_model["title"] == "No jobs matched this search."
    assert "chief of staff" in view_model["detail"]


def test_build_jobs_empty_state_markup_renders_structured_card_copy() -> None:
    markup = build_jobs_empty_state_markup(
        {
            "tone": "warning",
            "eyebrow": "Search",
            "badge": "Zero results",
            "title": "Search finished with no matching jobs.",
            "detail": "The latest run checked 2 queries.",
        }
    )

    assert "Search finished with no matching jobs." in markup
    assert "The latest run checked 2 queries." in markup
    assert "Zero results" in markup
    assert "text-transform:uppercase" in markup


def test_build_jobs_detail_empty_state_markup_guides_selection() -> None:
    markup = build_jobs_detail_empty_state_markup()

    assert "Select a job to inspect its full rationale." in markup
    assert "Use the list on the left" in markup
    assert "Nothing selected" in markup


def test_build_jobs_intro_state_markup_keeps_non_jobs_views_structured() -> None:
    markup = build_jobs_intro_state_markup(
        title="Dismissed",
        intro_message="Dismissed jobs stay hidden from active views until you restore them here.",
    )

    assert "Dismissed view" in markup
    assert "Dismissed jobs stay hidden from active views" in markup
    assert "Workspace" in markup


def test_filter_restorable_dismissed_leads_only_keeps_user_dismissed_rows() -> None:
    leads = [
        {
            "id": 1,
            "hidden": True,
            "evidence_json": {"user_dismissed_at": "2026-03-31T12:00:00Z", "suppression_category": "user_dismissed"},
        },
        {
            "id": 2,
            "hidden": True,
            "evidence_json": {"suppression_category": "stale"},
        },
        {
            "id": 3,
            "hidden": False,
            "evidence_json": {},
        },
    ]

    filtered = filter_restorable_dismissed_leads(leads)

    assert [lead["id"] for lead in filtered] == [1]


def test_build_jobs_action_feedback_makes_dismiss_and_restore_clear() -> None:
    dismiss_feedback = build_jobs_action_feedback("dislike")
    restore_feedback = build_jobs_action_feedback("restore")

    assert dismiss_feedback["tone"] == "success"
    assert "hidden from Jobs, Saved, and Applied" in dismiss_feedback["message"]
    assert restore_feedback == {
        "tone": "success",
        "message": "Job restored. It is visible in active job views again.",
    }


def test_filter_jobs_matches_description_tags_and_source_not_just_title_company() -> None:
    jobs = [
        {
            "id": "1",
            "title": "Operations Lead",
            "company": "Acme",
            "location": "Remote",
            "work_mode": "remote",
            "description": "Own recruiting systems and hiring operations for an agentic tooling team.",
            "explanation": "Agent systems fit.",
            "source": "search_web",
            "tags": ["fresh", "agentic"],
            "raw_lead": {"score_breakdown_json": {"final_score": 7.2}, "posted_at": "2026-03-29T10:00:00Z", "surfaced_at": "2026-03-29T11:00:00Z"},
        },
        {
            "id": "2",
            "title": "Founding Recruiter",
            "company": "Beta",
            "location": "New York",
            "work_mode": "onsite",
            "description": "General recruiting work.",
            "explanation": "General fit.",
            "source": "greenhouse",
            "tags": ["fresh"],
            "raw_lead": {"score_breakdown_json": {"final_score": 8.1}, "posted_at": "2026-03-30T10:00:00Z", "surfaced_at": "2026-03-30T11:00:00Z"},
        },
    ]

    filtered = _filter_jobs(
        jobs,
        {"search": "agentic tooling", "location": "", "remote_only": False, "sort_by": "Best Match"},
    )

    assert [job["id"] for job in filtered] == ["1"]


def test_filter_jobs_preserves_backend_order_when_backend_search_is_active() -> None:
    jobs = [
        {
            "id": "1",
            "title": "Chief of Staff",
            "company": "Acme",
            "location": "Remote",
            "work_mode": "remote",
            "description": "Exact title match.",
            "explanation": "Strong fit.",
            "source": "greenhouse",
            "tags": ["fresh"],
            "_search_document": {
                "fields": {},
                "haystack": "",
                "recommendation_sort": 4.0,
                "posted_at_sort": datetime(2026, 3, 24, tzinfo=timezone.utc),
                "title": "Chief of Staff",
                "company": "Acme",
            },
        },
        {
            "id": "2",
            "title": "Operations Program Lead",
            "company": "Beta",
            "location": "Remote",
            "work_mode": "remote",
            "description": "Description-only match.",
            "explanation": "Also relevant.",
            "source": "ashby",
            "tags": ["fresh"],
            "_search_document": {
                "fields": {},
                "haystack": "",
                "recommendation_sort": 9.0,
                "posted_at_sort": datetime(2026, 3, 30, tzinfo=timezone.utc),
                "title": "Operations Program Lead",
                "company": "Beta",
            },
        },
    ]

    filtered = _filter_jobs(
        jobs,
        {"search": "chief of staff", "location": "", "remote_only": False, "sort_by": "Newest"},
        search_meta={"query": "chief of staff", "status": "results", "result_count": 2, "backend_applied": True},
    )

    assert [job["id"] for job in filtered] == ["1", "2"]


def test_filter_jobs_ranks_exact_title_match_above_weaker_description_match() -> None:
    jobs = [
        {
            "id": "1",
            "title": "Chief of Staff",
            "company": "Acme",
            "location": "Remote",
            "work_mode": "remote",
            "description": "Staff role for company operations.",
            "explanation": "Strong fit.",
            "source": "search_web",
            "tags": ["fresh"],
            "raw_lead": {"score_breakdown_json": {"final_score": 6.5}, "posted_at": "2026-03-28T10:00:00Z", "surfaced_at": "2026-03-28T11:00:00Z"},
        },
        {
            "id": "2",
            "title": "Operations Program Lead",
            "company": "Beta",
            "location": "Remote",
            "work_mode": "remote",
            "description": "This role partners closely with the chief of staff.",
            "explanation": "Medium fit.",
            "source": "greenhouse",
            "tags": ["fresh"],
            "raw_lead": {"score_breakdown_json": {"final_score": 9.2}, "posted_at": "2026-03-30T10:00:00Z", "surfaced_at": "2026-03-30T11:00:00Z"},
        },
    ]

    filtered = _filter_jobs(
        jobs,
        {"search": "chief of staff", "location": "", "remote_only": False, "sort_by": "Best Match"},
    )

    assert [job["id"] for job in filtered] == ["1", "2"]


def test_filter_jobs_does_not_match_placeholder_todo_rows() -> None:
    jobs = [
        {
            "id": "1",
            "title": "Untitled role",
            "company": "Unknown company",
            "location": "Location not specified",
            "work_mode": "not specified",
            "description": "Description unavailable from the source listing.",
            "explanation": "Recommendation details unavailable.",
            "source": "unknown",
            "tags": [],
            "raw_lead": {"score_breakdown_json": {"final_score": 0.0}, "posted_at": None, "surfaced_at": None},
        }
    ]

    filtered = _filter_jobs(
        jobs,
        {"search": "todo", "location": "", "remote_only": False, "sort_by": "Best Match"},
    )

    assert filtered == []


def test_build_core_preferences_payload_syncs_titles_locations_and_work_mode() -> None:
    payload = ui_app.build_core_preferences_payload(
        {
            "name": "Demo Candidate",
            "preferred_titles_json": ["operator"],
            "core_titles_json": ["operator"],
            "target_roles_json": ["operator"],
            "preferred_locations_json": ["remote"],
            "preferred_domains_json": ["ai"],
            "work_mode_preference": "unspecified",
            "extracted_summary_json": {},
        },
        desired_titles="chief of staff, founding operations lead",
        preferred_locations="remote, san francisco",
        work_mode_preference="remote",
        preferred_domains="ai, developer tools",
    )

    assert payload["preferred_titles_json"] == ["chief of staff", "founding operations lead"]
    assert payload["core_titles_json"] == ["chief of staff", "founding operations lead"]
    assert payload["target_roles_json"] == ["chief of staff", "founding operations lead"]
    assert payload["preferred_locations_json"] == ["remote", "san francisco"]
    assert payload["preferred_domains_json"] == ["ai", "developer tools"]
    assert payload["work_mode_preference"] == "remote"
    assert payload["extracted_summary_json"]["selected_target_role"] == "chief of staff"


def test_build_jobs_empty_state_view_model_reports_running_search() -> None:
    view_model = build_jobs_empty_state_view_model(
        {
            "status": "running",
            "query_count": 2,
            "result_count": 0,
            "created_at": "2026-03-29T12:30:00Z",
        },
        total_job_count=0,
        filters={"search": "", "location": "", "remote_only": False},
    )

    assert view_model["title"] == "Search is running."
    assert "current run finishes" in view_model["detail"]


def test_build_jobs_empty_state_view_model_uses_search_run_truth_for_zero_results() -> None:
    view_model = build_jobs_empty_state_view_model(
        {
            "status": "results",
            "query_count": 2,
            "result_count": 0,
            "zero_yield": False,
            "created_at": "2026-03-29T12:30:00Z",
        },
        total_job_count=0,
        filters={"search": "", "location": "", "remote_only": False},
    )

    assert view_model["title"] == "Search finished successfully."
    assert "found 0 jobs across 2 queries" in view_model["detail"]
    assert view_model["show_clear_filters"] is False


def test_runtime_surface_payload_prefers_health_truth_and_merges_summaries() -> None:
    payload = ui_app.runtime_surface_payload(
        runtime={
            "runtime_phase": "paused",
            "last_cycle_summary": "Runtime success summary",
            "latest_failure_summary": "Runtime failure summary",
            "operator_hints": ["runtime hint"],
        },
        health={
            "runtime_phase": "queued",
            "latest_success_summary": "Health success summary",
            "latest_failure_summary": "Health failure summary",
            "operator_hints": ["health hint"],
        },
        digest={"summary": "Digest summary"},
    )

    assert payload["runtime_phase"] == "queued"
    assert payload["latest_success_summary"] == "Health success summary"
    assert payload["latest_failure_summary"] == "Health failure summary"
    assert payload["operator_hints"] == ["health hint"]


def test_build_profile_review_rows_flattens_structured_profile_for_ui() -> None:
    rows = build_profile_review_rows(
        {
            "preferred_titles_json": ["chief of staff"],
            "preferred_domains_json": ["ai"],
            "preferred_locations_json": ["remote"],
            "target_roles_json": ["chief of staff"],
            "work_mode_preference": "remote",
            "stage_preferences_json": ["series a"],
            "seniority_guess": "senior",
            "min_seniority_band": "mid",
            "max_seniority_band": "staff",
            "minimum_fit_threshold": 3.1,
            "structured_profile_json": {
                "targeting": {
                    "preferred_titles": ["chief of staff"],
                    "preferred_domains": ["ai"],
                    "preferred_locations": ["remote"],
                    "target_roles": ["chief of staff"],
                    "work_mode_preference": "remote",
                    "stage_preferences": ["series a"],
                    "seniority": {"guess": "senior", "minimum_band": "mid", "maximum_band": "staff"},
                },
                "scoring": {"minimum_fit_threshold": 3.1},
            },
        }
    )

    by_field = {row["field"]: row["value"] for row in rows}
    assert by_field["Preferred titles"] == "chief of staff"
    assert by_field["Preferred domains"] == "ai"
    assert by_field["Target roles"] == "chief of staff"
    assert by_field["Work mode"] == "remote"
    assert by_field["Seniority"] == "senior (mid to staff)"


def test_get_profile_form_source_prefers_latest_resume_ingest_candidate_profile() -> None:
    profile = {"name": "Saved Profile", "preferred_titles_json": ["operator"]}
    latest_resume_ingest = {
        "candidate_profile": {
            "name": "resume",
            "preferred_titles_json": ["chief of staff"],
            "raw_resume_text": "resume text",
        }
    }

    form_source = ui_app.get_profile_form_source(profile, latest_resume_ingest)

    assert form_source["name"] == "resume"
    assert form_source["preferred_titles_json"] == ["chief of staff"]


def test_build_onboarding_state_requires_resume_before_review() -> None:
    state = ui_app.build_onboarding_state(
        profile={"raw_resume_text": "", "preferred_titles_json": []},
        latest_resume_ingest=None,
        draft_profile=None,
    )

    assert state["resume_complete"] is False
    assert state["review_complete"] is False
    assert state["target_role_complete"] is False
    assert state["current_step"] == "resume"


def test_resume_analysis_contract_explains_behavior_and_limits() -> None:
    contract = ui_app.build_resume_analysis_contract_view_model()

    assert "structured targeting fields" in contract["summary"]
    assert any("skills and competencies" in item for item in contract["does"])
    assert any("does not rewrite your resume" in item.lower() for item in contract["does_not"])


def test_resume_analysis_feedback_reports_complete_and_partial_states() -> None:
    complete = ui_app.build_resume_analysis_feedback(
        {"status": "complete", "warnings": [], "missing_fields": []},
        {"warnings": []},
    )
    partial = ui_app.build_resume_analysis_feedback(
        {"status": "partial", "warnings": ["partial"], "missing_fields": ["preferred domains"]},
        {"warnings": []},
    )

    assert complete["tone"] == "success"
    assert "saved to your profile" in complete["message"]
    assert partial["tone"] == "warning"
    assert "Needs review: preferred domains." in partial["message"]


def test_resume_failure_feedback_handles_unsupported_and_empty_sources() -> None:
    unsupported = ui_app.build_resume_failure_feedback(ValueError("Unsupported file type. Upload a PDF, TXT, or MD resume."))
    empty = ui_app.build_resume_failure_feedback(ValueError("PDF text extraction returned no readable text."))

    assert unsupported["tone"] == "warning"
    assert "supports PDF, TXT, and MD files only" in unsupported["message"]
    assert empty["tone"] == "warning"
    assert "could not find readable text" in empty["message"]


def test_build_onboarding_state_allows_discovery_when_setup_is_deferred() -> None:
    state = ui_app.build_onboarding_state(
        profile={"raw_resume_text": "", "preferred_titles_json": []},
        latest_resume_ingest=None,
        draft_profile=None,
        onboarding_deferred=True,
    )

    assert state["resume_complete"] is False
    assert state["current_step"] == "discovery"
    assert state["onboarding_deferred"] is True


def test_build_onboarding_state_moves_from_review_to_target_role() -> None:
    latest_resume_ingest = {
        "candidate_profile": {
            "raw_resume_text": "parsed resume text",
            "preferred_titles_json": ["chief of staff"],
            "core_titles_json": ["chief of staff"],
        }
    }

    review_state = ui_app.build_onboarding_state(
        profile={"raw_resume_text": "", "preferred_titles_json": []},
        latest_resume_ingest=latest_resume_ingest,
        draft_profile=None,
    )
    target_role_state = ui_app.build_onboarding_state(
        profile={"raw_resume_text": "", "preferred_titles_json": []},
        latest_resume_ingest=latest_resume_ingest,
        draft_profile={"raw_resume_text": "parsed resume text", "preferred_titles_json": ["chief of staff"]},
    )

    assert review_state["current_step"] == "review"
    assert target_role_state["current_step"] == "target_role"
    assert target_role_state["review_complete"] is True


def test_apply_target_role_selection_prioritizes_selected_role_in_structured_inputs() -> None:
    payload = ui_app.apply_target_role_selection(
        {
            "preferred_titles_json": ["operator", "chief of staff"],
            "core_titles_json": ["operator"],
            "target_roles_json": ["operator"],
            "extracted_summary_json": {"summary": "Operator profile"},
        },
        "founding operations lead",
    )

    assert payload["preferred_titles_json"][0] == "founding operations lead"
    assert payload["core_titles_json"][0] == "founding operations lead"
    assert payload["target_roles_json"][0] == "founding operations lead"
    assert payload["extracted_summary_json"]["selected_target_role"] == "founding operations lead"


def test_recommendation_score_helpers_surface_structured_summary_and_components() -> None:
    lead = {
        "rank_label": "strong",
        "confidence_label": "high",
        "score_breakdown_json": {
            "final_score": 8.4,
            "recommendation_band": "strong",
            "confidence_label": "high",
            "component_metrics": [
                {
                    "key": "freshness",
                    "label": "Freshness",
                    "score": 1.6,
                    "semantics": "Rewards recent, still-live opportunities and penalizes stale ones.",
                    "trace_inputs": ["freshness_label=fresh", "listing_status=active"],
                },
                {
                    "key": "title_fit",
                    "label": "Title alignment",
                    "score": 2.4,
                    "semantics": "Measures how closely the role title aligns with the candidate's target scope.",
                    "trace_inputs": ["title_fit_label=core match"],
                },
            ],
            "explanation": {
                "headline": "Strong recommendation at 8.40 with high confidence.",
                "summary": "Strong operator match with fresh, verified evidence.",
                "supporting_points": ["Title fit: core match", "Qualification fit: strong fit"],
            },
        },
    }

    summary = ui_app.recommendation_score_summary(lead)
    rows = ui_app.recommendation_score_rows(lead)

    assert summary == "Recommendation score: 8.40 | Band: strong | Confidence: high"
    assert rows["component"].tolist() == ["Freshness", "Title alignment"]
    assert rows.loc[rows["component"] == "Freshness", "trace_inputs"].iloc[0] == "freshness_label=fresh, listing_status=active"


def test_build_profile_update_payload_preserves_extracted_resume_draft_fields() -> None:
    saved_profile = {
        "profile_schema_version": "v1",
        "name": "Saved Profile",
        "raw_resume_text": "old raw text",
        "extracted_summary_json": {"summary": "old summary"},
        "seniority_guess": "mid",
        "target_roles_json": ["operator"],
        "work_mode_preference": "hybrid",
    }
    review_profile = {
        "profile_schema_version": "v1",
        "name": "resume",
        "raw_resume_text": "new parsed raw text",
        "extracted_summary_json": {
            "summary": "new summary",
            "resume_filename": "resume.pdf",
            "extraction_status": "partial",
            "missing_fields": ["preferred domains"],
        },
        "seniority_guess": "senior",
        "target_roles_json": ["chief of staff"],
        "work_mode_preference": "remote",
    }

    payload = ui_app.build_profile_update_payload(
        saved_profile,
        review_profile,
        {
            "name": "Edited Candidate",
            "preferred_titles": "chief of staff, operator",
            "adjacent_titles": "program manager",
            "excluded_titles": "intern",
            "preferred_domains": "ai",
            "excluded_companies": "BigCo",
            "preferred_locations": "remote",
            "confirmed_skills": "sql, stakeholder management",
            "competencies": "process design, operator judgment",
            "explicit_preferences": "hands-on teams, customer-facing work",
            "stage_preferences": "series a",
            "core_titles": "chief of staff",
            "excluded_keywords": "clearance required",
            "min_seniority_band": "mid",
            "max_seniority_band": "staff",
            "stretch_role_families": "operations",
            "minimum_fit_threshold": 3.3,
        },
    )

    assert payload["name"] == "Edited Candidate"
    assert payload["raw_resume_text"] == "new parsed raw text"
    assert payload["extracted_summary_json"]["resume_filename"] == "resume.pdf"
    assert payload["extracted_summary_json"]["extraction_status"] == "partial"
    assert payload["preferred_titles_json"] == ["chief of staff", "operator"]
    assert payload["confirmed_skills_json"] == ["sql", "stakeholder management"]
    assert payload["competencies_json"] == ["process design", "operator judgment"]
    assert payload["explicit_preferences_json"] == ["hands-on teams", "customer-facing work"]
    assert payload["seniority_guess"] == "senior"
    assert payload["target_roles_json"] == ["chief of staff"]
    assert payload["work_mode_preference"] == "remote"


def test_build_profile_update_payload_still_writes_structured_profile_without_resume_flow() -> None:
    saved_profile = {
        "profile_schema_version": "v1",
        "name": "Saved Profile",
        "raw_resume_text": "",
        "extracted_summary_json": {"summary": "existing summary"},
        "seniority_guess": "mid",
        "target_roles_json": [],
        "work_mode_preference": "unspecified",
    }

    payload = ui_app.build_profile_update_payload(
        saved_profile,
        saved_profile,
        {
            "name": "Edited Candidate",
            "preferred_titles": "chief of staff, operator",
            "adjacent_titles": "program manager",
            "excluded_titles": "intern",
            "preferred_domains": "ai",
            "excluded_companies": "BigCo",
            "preferred_locations": "remote",
            "confirmed_skills": "sql, stakeholder management",
            "competencies": "process design, operator judgment",
            "explicit_preferences": "hands-on teams, customer-facing work",
            "stage_preferences": "series a",
            "core_titles": "chief of staff",
            "excluded_keywords": "clearance required",
            "min_seniority_band": "mid",
            "max_seniority_band": "staff",
            "stretch_role_families": "operations",
            "minimum_fit_threshold": 3.3,
        },
    )

    assert payload["raw_resume_text"] == ""
    assert payload["preferred_titles_json"] == ["chief of staff", "operator"]
    assert payload["core_titles_json"] == ["chief of staff"]
    assert payload["preferred_domains_json"] == ["ai"]
    assert payload["preferred_locations_json"] == ["remote"]
    assert payload["minimum_fit_threshold"] == 3.3


def test_lead_frame_prefers_source_lineage_for_provenance() -> None:
    frame = ui_app.lead_frame(
        [
            {
                "id": 11,
                "company_name": "Ramp",
                "primary_title": "Strategic Programs Lead",
                "lead_type": "listing",
                "url": "https://boards.greenhouse.io/ramp/jobs/9999",
                "source_platform": "greenhouse",
                "source_type": "greenhouse",
                "source_lineage": "greenhouse+user_submitted",
                "freshness_label": "fresh",
                "qualification_fit_label": "strong fit",
                "confidence_label": "high",
                "surfaced_at": "2026-03-25T10:00:00Z",
                "posted_at": "2026-03-25T09:00:00Z",
                "evidence_json": {},
            }
        ]
    )

    assert frame["provenance"].tolist() == ["greenhouse+user_submitted"]


def test_submit_user_job_link_uses_local_session_and_normalizes_optional_date(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeSession:
        def commit(self) -> None:
            captured["committed"] = True

        def rollback(self) -> None:
            captured["rolled_back"] = True

        def close(self) -> None:
            captured["closed"] = True

    def fake_session_local() -> FakeSession:
        return FakeSession()

    def fake_ingest(session, **kwargs):
        captured["session"] = session
        captured["kwargs"] = kwargs
        return {"summary": "ok", "source_lineage": "user_submitted"}

    monkeypatch.setattr(ui_app, "SessionLocal", fake_session_local)
    monkeypatch.setattr(ui_app, "ingest_user_job_link", fake_ingest)

    result = ui_app.submit_user_job_link(
        job_url="https://example.com/jobs/1",
        company_name="Example",
        title="Operator",
        location="Remote",
        description_text="Own planning and execution.",
        posted_on=date(2026, 3, 24),
    )

    assert result == {"summary": "ok", "source_lineage": "user_submitted"}
    assert captured["kwargs"]["job_url"] == "https://example.com/jobs/1"
    assert captured["kwargs"]["company_name"] == "Example"
    assert captured["kwargs"]["title"] == "Operator"
    assert captured["kwargs"]["location"] == "Remote"
    assert captured["kwargs"]["description_text"] == "Own planning and execution."
    assert captured["kwargs"]["posted_at"] == datetime(2026, 3, 24, 0, 0)
    assert captured["committed"] is True
    assert captured["closed"] is True
