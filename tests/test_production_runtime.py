from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config import Settings
from core.models import AgentRun, AlertEvent, Base, ConnectorHealth, RunDigest, SourceQuery, WatchlistItem
from services.alerts import evaluate_alerts
from services.connectors_health import record_connector_failure, record_connector_success
from services.ops import can_add_watchlist_items_today, can_create_generated_queries_today, get_runtime_connector_set


def build_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def test_runtime_connector_set_respects_greenhouse_kill_switch() -> None:
    settings = Settings(
        demo_mode=False,
        autonomy_enabled=True,
        greenhouse_enabled=False,
        search_discovery_enabled=False,
        database_url="sqlite:///:memory:",
    )
    source_mode, enabled, strict = get_runtime_connector_set(settings)
    assert source_mode == "live"
    assert enabled == set()
    assert strict == set()


def test_runtime_connector_set_enables_search_and_ashby_for_search_discovery() -> None:
    settings = Settings(
        demo_mode=False,
        autonomy_enabled=True,
        greenhouse_enabled=True,
        search_discovery_enabled=True,
        database_url="sqlite:///:memory:",
    )
    source_mode, enabled, strict = get_runtime_connector_set(settings)
    assert source_mode == "live"
    assert enabled == {"greenhouse", "ashby", "search_web"}
    assert strict == {"greenhouse", "ashby", "search_web"}


def test_runtime_connector_set_enables_ashby_when_orgs_are_configured() -> None:
    settings = Settings(
        demo_mode=False,
        autonomy_enabled=True,
        greenhouse_enabled=False,
        ashby_org_keys="mercor,vercel",
        search_discovery_enabled=False,
        database_url="sqlite:///:memory:",
    )
    source_mode, enabled, strict = get_runtime_connector_set(settings)
    assert source_mode == "live"
    assert enabled == {"ashby"}
    assert strict == {"ashby"}


def test_daily_caps_limit_generated_queries_and_watchlist_items() -> None:
    session = build_session()
    settings = Settings(
        database_url="sqlite:///:memory:",
        max_generated_queries_per_day=2,
        max_watchlist_additions_per_day=2,
    )
    for idx in range(2):
        session.add(SourceQuery(query_text=f"query-{idx}", source_type="x", status="generated"))
        session.add(WatchlistItem(item_type="query", value=f"watch-{idx}", source_reason="seed", confidence="low"))
    session.flush()
    assert can_create_generated_queries_today(session, settings, requested=3) == 0
    assert can_add_watchlist_items_today(session, settings, requested=3) == 0


def test_alerts_record_greenhouse_incident_and_rate_limit() -> None:
    session = build_session()
    settings = Settings(
        database_url="sqlite:///:memory:",
        alerts_enabled=False,
        autonomy_enabled=True,
        greenhouse_enabled=True,
        alert_window_seconds=3600,
        alert_max_per_window=10,
        alert_no_successful_fetch_seconds=1,
    )
    record_connector_failure(session, "greenhouse", "boom", classification="transient_network")
    session.flush()
    first = evaluate_alerts(session, settings=settings)
    assert any(event.alert_key == "greenhouse_no_recent_success" for event in first)
    second = evaluate_alerts(session, settings=settings)
    assert any(event.status == "rate_limited" for event in second)


def test_alerts_capture_recent_worker_failure() -> None:
    session = build_session()
    settings = Settings(database_url="sqlite:///:memory:", alerts_enabled=False, alert_window_seconds=3600)
    session.add(
        AgentRun(
            agent_name="Worker",
            action="run cycle",
            status="failed",
            summary="Worker loop failed: boom",
            affected_count=0,
        )
    )
    session.flush()
    alerts = evaluate_alerts(session, settings=settings)
    assert any(event.alert_key == "worker_loop_failure" for event in alerts)


def test_connector_success_can_be_approved_for_unattended() -> None:
    session = build_session()
    row = record_connector_success(
        session,
        "greenhouse",
        items=[{"first_published": datetime.utcnow().isoformat()}],
        mode="live",
        date_fields=["first_published"],
    )
    row = record_connector_success(
        session,
        "greenhouse",
        items=[{"first_published": datetime.utcnow().isoformat()}],
        mode="live",
        date_fields=["first_published"],
    )
    assert row.status == "healthy"
    assert row.approved_for_unattended is True


def test_frontend_shell_uses_product_first_routes_and_streamlit_secondary_path() -> None:
    router_source = Path("frontend/src/router.tsx").read_text()
    shell_source = Path("frontend/src/shell/AppShell.tsx").read_text()

    assert 'Navigate to="/jobs"' in router_source
    assert '{ path: "jobs", element: <JobsPage /> }' in router_source
    assert '{ path: "validation-harness", element: <ValidationHarnessPage /> }' in router_source
    assert "Agent Activity" not in shell_source
    assert "Autonomy Ops" not in shell_source
    assert "Validation Harness" in shell_source


def test_frontend_shell_dev_wiring_targets_existing_fastapi_backend() -> None:
    package_json = json.loads(Path("frontend/package.json").read_text())
    vite_config = Path("frontend/vite.config.ts").read_text()
    api_client = Path("frontend/src/lib/api.ts").read_text()
    readme = Path("README.md").read_text()

    assert package_json["scripts"]["dev"] == "vite --host 127.0.0.1 --port 5173"
    assert 'target: "http://127.0.0.1:8000"' in vite_config
    assert 'path.replace(/^\\/api/, "")' in vite_config
    assert "/opportunities" in api_client
    assert '"/candidate-profile"' in api_client
    assert '"/applications/status"' in api_client
    assert "Streamlit remains the temporary validation harness" in readme
