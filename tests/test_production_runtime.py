from __future__ import annotations

import json
from asyncio import run
from datetime import timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api.main import app
from api.routes.search_runs import latest_search_run
from core.db import get_db
from core.config import Settings
from core.models import AgentRun, AlertEvent, Base, ConnectorHealth, RunDigest, SearchRun, SourceQuery, WatchlistItem
from core.time import utcnow
from services.alerts import evaluate_alerts
from services.connectors_health import record_connector_failure, record_connector_success
from services.ops import can_add_watchlist_items_today, can_create_generated_queries_today, get_runtime_connector_set


def build_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def request_json(path: str, method: str = "GET") -> tuple[int, dict[str, str], object]:
    messages: list[dict[str, object]] = []
    body_sent = False

    async def receive() -> dict[str, object]:
        nonlocal body_sent
        if body_sent:
            return {"type": "http.disconnect"}
        body_sent = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [(b"host", b"testserver")],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "root_path": "",
    }
    run(app(scope, receive, send))

    start = next(message for message in messages if message["type"] == "http.response.start")
    body_chunks = [message.get("body", b"") for message in messages if message["type"] == "http.response.body"]
    payload = b"".join(chunk if isinstance(chunk, bytes) else b"" for chunk in body_chunks).decode("utf-8")
    headers = {
        key.decode("latin1"): value.decode("latin1")
        for key, value in start.get("headers", [])
        if isinstance(key, bytes) and isinstance(value, bytes)
    }
    return int(start["status"]), headers, json.loads(payload) if payload else None


def get_json(path: str) -> tuple[int, dict[str, str], object]:
    return request_json(path, method="GET")


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


def test_search_runs_latest_endpoint_is_registered() -> None:
    assert any(getattr(route, "path", None) == "/search-runs/latest" for route in app.routes)
    assert any(getattr(route, "path", None) == "/search-runs/manual" for route in app.routes)
    schema = app.openapi()
    assert "/search-runs/latest" in schema["paths"]
    assert "get" in schema["paths"]["/search-runs/latest"]
    assert "/search-runs/manual" in schema["paths"]
    assert "post" in schema["paths"]["/search-runs/manual"]


def test_search_runs_latest_endpoint_returns_null_when_no_runs_exist() -> None:
    session = build_session()
    try:
        assert latest_search_run(session) is None
    finally:
        session.close()


def test_search_runs_latest_endpoint_returns_most_recent_run() -> None:
    session = build_session()
    session.add(
        SearchRun(
            worker_name="search",
            provider="duckduckgo_html",
            status="empty",
            query_count=1,
            result_count=0,
            queries_json=["older query"],
        )
    )
    session.flush()
    latest = SearchRun(
        worker_name="search",
        provider="duckduckgo_html",
        status="results",
        live=True,
        query_count=2,
        result_count=5,
        queries_json=["latest query one", "latest query two"],
        diagnostics_json={"status": "results"},
    )
    session.add(latest)
    session.flush()

    try:
        response = latest_search_run(session)
    finally:
        session.close()

    assert response is not None
    payload = response.model_dump(mode="json") if hasattr(response, "model_dump") else json.loads(response.json())
    assert payload == {
        "id": latest.id,
        "source_key": "search_web",
        "worker_name": "search",
        "provider": "duckduckgo_html",
        "status": "results",
        "live": True,
        "zero_yield": False,
        "query_count": 2,
        "result_count": 5,
        "queries": ["latest query one", "latest query two"],
        "failure_classification": None,
        "error": None,
        "diagnostics_json": {"status": "results"},
        "created_at": latest.created_at.isoformat().replace("+00:00", "Z"),
    }


def test_search_runs_latest_http_endpoint_returns_latest_run_payload() -> None:
    session = build_session()
    latest = SearchRun(
        worker_name="search",
        provider="duckduckgo_html",
        status="results",
        live=True,
        zero_yield=False,
        query_count=2,
        result_count=5,
        queries_json=["latest query one", "latest query two"],
        diagnostics_json={"status": "results"},
    )
    session.add(latest)
    session.flush()

    def override_get_db():
        try:
            yield session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    try:
        status_code, _headers, payload = get_json("/search-runs/latest")
    finally:
        app.dependency_overrides.pop(get_db, None)
        session.close()

    assert status_code == 200
    assert payload == {
        "id": latest.id,
        "source_key": "search_web",
        "worker_name": "search",
        "provider": "duckduckgo_html",
        "status": "results",
        "live": True,
        "zero_yield": False,
        "query_count": 2,
        "result_count": 5,
        "queries": ["latest query one", "latest query two"],
        "failure_classification": None,
        "error": None,
        "diagnostics_json": {"status": "results"},
        "created_at": latest.created_at.isoformat().replace("+00:00", "Z"),
    }


def test_manual_search_http_endpoint_runs_sync_and_returns_payload(monkeypatch) -> None:
    session = build_session()

    def fake_sync_all(db, include_rechecks: bool):
        assert db is session
        assert include_rechecks is True
        return {
            "signals_ingested": 1,
            "listings_ingested": 2,
            "leads_created": 3,
            "leads_updated": 4,
            "rechecks_queued": 5,
            "live_mode_used": False,
            "discovery_metrics": {"search_web": {"raw": 2}},
            "surfaced_count": 2,
            "discovery_summary": "Manual search test summary.",
            "discovery_status": {"selected_companies": ["Mercor"]},
        }

    def override_get_db():
        try:
            yield session
        finally:
            pass

    monkeypatch.setattr("api.routes.search_runs.sync_all", fake_sync_all)
    app.dependency_overrides[get_db] = override_get_db
    try:
        status_code, _headers, payload = request_json("/search-runs/manual", method="POST")
    finally:
        app.dependency_overrides.pop(get_db, None)
        session.close()

    assert status_code == 200
    assert payload == {
        "signals_ingested": 1,
        "listings_ingested": 2,
        "leads_created": 3,
        "leads_updated": 4,
        "rechecks_queued": 5,
        "live_mode_used": False,
        "discovery_metrics": {"search_web": {"raw": 2}},
        "surfaced_count": 2,
        "discovery_summary": "Manual search test summary.",
        "discovery_status": {"selected_companies": ["Mercor"]},
    }

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
