from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config import Settings
from core.models import AgentRun, AlertEvent, Base, ConnectorHealth, RunDigest, SearchRun, SourceQuery, WatchlistItem
from core.time import utcnow
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


def test_runtime_schema_creates_search_runs_table() -> None:
    session = build_session()
    session.add(SearchRun(worker_name="search"))
    session.flush()
    assert session.query(SearchRun).count() == 1


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
        items=[{"first_published": utcnow().isoformat()}],
        mode="live",
        date_fields=["first_published"],
    )
    row = record_connector_success(
        session,
        "greenhouse",
        items=[{"first_published": utcnow().isoformat()}],
        mode="live",
        date_fields=["first_published"],
    )
    assert row.status == "healthy"
    assert row.approved_for_unattended is True


def test_frontend_shell_uses_product_first_routes_and_streamlit_secondary_path() -> None:
    router_source = Path("frontend/src/router.tsx").read_text()
    shell_source = Path("frontend/src/shell/AppShell.tsx").read_text()
    validation_source = Path("frontend/src/views/ValidationHarnessPage.tsx").read_text()

    assert 'Navigate to="/jobs"' in router_source
    assert '{ path: "jobs", element: <JobsPage /> }' in router_source
    assert '{ path: "validation-harness", element: <ValidationHarnessPage /> }' in router_source
    assert "Agent Activity" not in shell_source
    assert "Autonomy Ops" not in shell_source
    assert "Primary product shell" in shell_source
    assert "Streamlit is reserved for internal validation and operator workflows." in shell_source
    assert "Internal Harness" in shell_source
    assert "Streamlit is now the internal validation and operator harness" in validation_source
    assert "operator-only surfaces" in validation_source


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
    assert "primary product path: treat `http://127.0.0.1:5173/` as the default product shell during local development and demos" in readme
    assert "Streamlit is the internal validation and operator harness at `http://127.0.0.1:8500`" in readme


def test_runtime_self_check_requires_live_stack_evidence() -> None:
    script = Path("scripts/runtime_self_check.sh").read_text()

    assert 'UI_URL="${UI_URL:-http://127.0.0.1:8500}"' in script
    assert 'PRIMARY_UI_URL="${PRIMARY_UI_URL:-}"' in script
    assert 'require_process "api" \'uvicorn api.main:app\'' in script
    assert 'require_process "worker" \'scripts/run_worker.py\'' in script
    assert 'require_process "ui" \'streamlit run ui/app.py\'' in script
    assert 'curl -fsS' in script
    assert 'curl /opportunities' in script
    assert 'curl primary product shell' in script
    assert 'curl primary product path' in script
    assert 'PRIMARY_UI_URL=http://127.0.0.1:5173' in script
    assert "Live runtime smoke passed: API, worker, and primary UI path were directly reachable." in script
    assert "Acceptance-critical runtime proof recorded for /health, /autonomy-status, /runtime-control, /discovery-status, /opportunities, run_once worker execution, and the default Streamlit workbench." in script
    assert "Primary product shell proof not recorded; set PRIMARY_UI_URL=http://127.0.0.1:5173 when acceptance-critical work changes the JS shell or primary user path." in script
    assert "Local tests and preflight checks are still not live product proof on their own." in script


def test_preflight_check_declares_it_is_not_live_runtime_proof() -> None:
    script = Path("scripts/preflight_check.sh").read_text()

    assert "This script proves local imports, DB init, and test coverage only." in script
    assert "It is not live runtime proof." in script
    assert "./scripts/runtime_self_check.sh" in script
