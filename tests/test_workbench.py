from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import requests

from services.profile_ingest import build_profile_review_rows
from ui import app as ui_app
from ui.app import filter_and_sort_table


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
                "score_breakdown_json": {"action_label": "Act now"},
            }
        ]
    )

    assert frame["recommendation_action"].tolist() == ["Act now"]


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

    assert payload == {"items": []}
    assert captured
    assert captured_timeout == [10]


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
            "extracted_summary_json": {"summary": "Operator profile"},
        },
        "founding operations lead",
    )

    assert payload["preferred_titles_json"][0] == "founding operations lead"
    assert payload["core_titles_json"][0] == "founding operations lead"
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
