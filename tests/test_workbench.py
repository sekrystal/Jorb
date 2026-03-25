from __future__ import annotations

from datetime import datetime, timezone

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
                "ats_direct": {"queries_attempted": 2, "accepted_results": 1},
                "careers_broad": {"queries_attempted": 3, "selected_for_expansion": 1},
            }
        }
    )

    assert frame["query_family"].tolist() == ["ats_direct", "careers_broad"]
    assert frame.loc[frame["query_family"] == "ats_direct", "accepted_results"].iloc[0] == 1


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
