from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import requests

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
