from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config import Settings
from connectors.greenhouse import GreenhouseFetchError
from core.models import (
    AgentActivity,
    AgentRun,
    Application,
    Base,
    FollowUpTask,
    Investigation,
    Lead,
    Listing,
    RunDigest,
    RuntimeControl,
    SourceQueryStat,
    WatchlistItem,
)
from services.activity import log_agent_activity
from services.autonomy import build_autonomy_health, build_latest_run_digest, list_connector_health
from services.connector_admin import reset_connector_health
from services.connectors_health import record_connector_success, run_connector_fetch
from services.sync import sync_all
from services.governance import evaluate_learning_governance
from services.pipeline import run_query_evolution_agent
from services.profile import get_candidate_profile


def build_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def test_log_agent_activity_dedupes_repeated_noop_entries() -> None:
    session = build_session()
    log_agent_activity(session, "Tracker", "generated follow-up tasks", "Tracker reviewed applications and created 0 follow-up tasks.")
    log_agent_activity(session, "Tracker", "generated follow-up tasks", "Tracker reviewed applications and created 0 follow-up tasks.")
    session.flush()
    assert session.query(AgentRun).count() == 0
    assert session.query(AgentActivity).count() == 1


def test_learning_agent_caps_watchlist_growth_per_cycle() -> None:
    session = build_session()
    get_candidate_profile(session)
    for idx in range(6):
        session.add(
            Lead(
                lead_type="listing",
                company_name=f"DemoCo {idx}",
                primary_title="Ops Lead",
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="scope match",
                qualification_fit_label="strong fit",
                hidden=False,
                evidence_json={"company_domain": f"demo{idx}.ai"},
                score_breakdown_json={},
            )
        )
    session.flush()
    result = run_query_evolution_agent(session)
    assert "Learning proposed 4 watchlist expansions" in result.summary


def test_autonomy_health_and_digest_include_recent_run_state() -> None:
    session = build_session()
    session.add(Investigation(signal_id=1, status="open", confidence=0.5))
    runtime = RuntimeControl(
        run_state="paused",
        worker_state="paused",
        run_once_requested=True,
        status_message="Single cycle requested.",
        last_cycle_summary="Latest successful worker cycle.",
        last_successful_cycle_at=datetime.utcnow() - timedelta(minutes=5),
    )
    session.add(runtime)
    session.add(
        Application(
            lead_id=1,
            company_name="Mercor",
            title="Deployment Strategist",
            current_status="applied",
            date_applied=datetime.utcnow() - timedelta(days=8),
        )
    )
    session.add(
        FollowUpTask(
            application_id=1,
            task_type="follow_up",
            due_at=datetime.utcnow() - timedelta(days=1),
            status="open",
            notes="Follow up",
        )
    )
    run = AgentRun(
        agent_name="Pipeline",
        action="ran full pipeline",
        status="ok",
        summary="Pipeline run summary",
        affected_count=7,
    )
    session.add(run)
    session.add(
        AgentRun(
            agent_name="Pipeline",
            action="ran full pipeline",
            status="failed",
            summary="Connector fetch failed for greenhouse.",
            affected_count=0,
        )
    )
    session.flush()
    session.add(
        RunDigest(
            agent_run_id=run.id,
            run_type="pipeline",
            summary="Pipeline run summary",
            new_leads_json=["Ramp / Strategic Programs Lead"],
            suppressed_leads_json=["ArchiveCo / Chief of Staff"],
            investigations_changed=1,
        )
    )
    session.flush()

    settings = Settings(database_url="sqlite:///:memory:", autonomy_enabled=True, demo_mode=True)
    health = build_autonomy_health(session, settings=settings)
    digest = build_latest_run_digest(session)

    assert health.open_investigations == 1
    assert health.due_follow_ups == 1
    assert health.runtime_phase == "queued"
    assert health.latest_success_summary == "Latest successful worker cycle."
    assert health.latest_failure_summary == "Connector fetch failed for greenhouse."
    assert "A single bounded cycle is queued" in health.operator_hints[0]
    assert digest.new_leads == ["Ramp / Strategic Programs Lead"]
    assert digest.suppressed_leads == ["ArchiveCo / Chief of Staff"]


def test_governance_promotes_and_suppresses_learned_items() -> None:
    session = build_session()
    session.add(
        SourceQueryStat(
            source_type="x",
            query_text="good query",
            status="generated",
            likes=1,
            saves=1,
            applies=0,
        )
    )
    session.add(
        SourceQueryStat(
            source_type="x",
            query_text="bad query",
            status="generated",
            dislikes=2,
        )
    )
    session.add(
        WatchlistItem(
            item_type="domain",
            value="great.ai",
            source_reason="Strong applied signal",
            confidence="high",
            status="proposed",
        )
    )
    counts = evaluate_learning_governance(session)
    good = session.query(SourceQueryStat).filter_by(query_text="good query").one()
    bad = session.query(SourceQueryStat).filter_by(query_text="bad query").one()
    watch = session.query(WatchlistItem).filter_by(value="great.ai").one()
    assert counts["promoted_queries"] == 1
    assert counts["suppressed_queries"] == 1
    assert good.status == "active"
    assert bad.status == "suppressed"
    assert watch.status == "active"


def test_connector_failures_open_circuit_breaker() -> None:
    session = build_session()

    def failing_fetch():
        raise RuntimeError("boom")

    for _ in range(3):
        items, live, row = run_connector_fetch(session, "x_search", failing_fetch, date_fields=["published_at"], retries=0)
        assert items == []
        assert live is False
    assert row.circuit_state == "open"
    assert row.status == "circuit_open"


def test_connector_recovers_from_failure_to_healthy_live_state() -> None:
    session = build_session()

    def failing_fetch():
        raise GreenhouseFetchError("transient_network", "boom")

    _, _, row = run_connector_fetch(session, "greenhouse", failing_fetch, date_fields=["first_published"], retries=0)
    assert row.status == "failed"
    assert row.last_failure_classification == "transient_network"

    row = record_connector_success(
        session,
        "greenhouse",
        items=[{"first_published": datetime.utcnow().isoformat()}],
        mode="live",
        date_fields=["first_published"],
    )
    assert row.status == "recovering"
    assert row.approved_for_unattended is False

    row = record_connector_success(
        session,
        "greenhouse",
        items=[{"first_published": datetime.utcnow().isoformat()}],
        mode="live",
        date_fields=["first_published"],
    )
    assert row.status == "healthy"
    assert row.trust_score > 0.75


def test_connector_success_with_quarantine_is_degraded() -> None:
    session = build_session()
    row = record_connector_success(
        session,
        "greenhouse",
        items=[{"first_published": datetime.utcnow().isoformat()}],
        mode="live",
        date_fields=["first_published"],
        failure_classification="quarantined_rows",
        quarantine_count=2,
    )
    assert row.status == "degraded"
    assert row.quarantine_count == 2
    assert row.approved_for_unattended is False


def test_sync_all_prevents_duplicate_listings_across_repeated_greenhouse_polls(monkeypatch) -> None:
    session = build_session()
    get_candidate_profile(session)

    def greenhouse_fetch(self, require_live: bool = False, board_tokens_override=None, discovery_queries=None):
        return (
            [
                {
                    "id": 9001,
                    "title": "Strategic Programs Lead",
                    "absolute_url": "https://job-boards.greenhouse.io/example/jobs/9001",
                    "location": {"name": "San Francisco, CA"},
                    "first_published": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                    "content": "Own operations systems and planning cadence.",
                    "company_name": "ExampleCo",
                    "company_domain": "example.co",
                }
            ],
            True,
        )

    monkeypatch.setattr("connectors.greenhouse.GreenhouseConnector.fetch", greenhouse_fetch)
    monkeypatch.setattr("connectors.ashby.AshbyConnector.fetch", lambda self, require_live=False, orgs_override=None, discovery_queries=None: ([], False))
    monkeypatch.setattr("connectors.x_search.XSearchConnector.fetch", lambda self, queries, require_live=False: ([], False))

    sync_all(session, enabled_connectors={"greenhouse"})
    sync_all(session, enabled_connectors={"greenhouse"})

    assert session.query(Listing).count() == 1
    assert session.query(Lead).count() == 1


def test_connector_health_reports_missing_tokens_for_greenhouse(monkeypatch) -> None:
    monkeypatch.setenv("GREENHOUSE_ENABLED", "true")
    monkeypatch.setenv("GREENHOUSE_BOARD_TOKENS", "")
    from core.config import get_settings

    get_settings.cache_clear()
    session = build_session()
    rows = list_connector_health(session)
    greenhouse = next(item for item in rows if item.connector_name == "greenhouse")
    assert greenhouse.blocked_reason == "missing_tokens"
    assert greenhouse.config_key == "GREENHOUSE_BOARD_TOKENS"


def test_reset_connector_health_clears_open_circuit() -> None:
    session = build_session()

    def failing_fetch():
        raise RuntimeError("boom")

    for _ in range(3):
        run_connector_fetch(session, "greenhouse", failing_fetch, date_fields=["first_published"], retries=0)

    reset = reset_connector_health(session, "greenhouse")
    assert reset.status == "unknown"
    assert reset.circuit_state == "closed"
    assert reset.disabled_until is None
    assert reset.consecutive_failures == 0


def test_search_discovery_expands_greenhouse_tokens_without_bypassing_pipeline(monkeypatch) -> None:
    session = build_session()
    profile = get_candidate_profile(session)
    profile.core_titles_json = ["operations lead"]
    profile.preferred_domains_json = ["developer tools"]
    session.flush()

    monkeypatch.setenv("SEARCH_DISCOVERY_ENABLED", "true")
    monkeypatch.setenv("GREENHOUSE_ENABLED", "true")
    monkeypatch.setenv("GREENHOUSE_BOARD_TOKENS", "")
    from core.config import get_settings

    get_settings.cache_clear()

    def search_fetch(self, queries, require_live=False):
        from connectors.search_web import SearchDiscoveryResult

        return (
            [
                SearchDiscoveryResult(
                    query_text=queries[0],
                    title="Ops Lead",
                    url="https://job-boards.greenhouse.io/example/jobs/9001",
                )
            ],
            True,
        )

    def greenhouse_fetch(self, require_live=False, board_tokens_override=None, discovery_queries=None):
        assert board_tokens_override == ["example"]
        assert discovery_queries is not None
        assert "example" in discovery_queries
        return (
            [
                {
                    "id": 9001,
                    "title": "Strategic Programs Lead",
                    "absolute_url": "https://job-boards.greenhouse.io/example/jobs/9001",
                    "location": {"name": "San Francisco, CA"},
                    "first_published": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                    "content": "Own operations systems and planning cadence.",
                    "company_name": "ExampleCo",
                    "company_domain": "example.co",
                    "source_board_token": "example",
                    "discovery_source": "search_web",
                    "source_queries": discovery_queries["example"],
                }
            ],
            True,
        )

    monkeypatch.setattr("connectors.search_web.SearchDiscoveryConnector.fetch", search_fetch)
    monkeypatch.setattr("connectors.greenhouse.GreenhouseConnector.fetch", greenhouse_fetch)
    monkeypatch.setattr("connectors.ashby.AshbyConnector.fetch", lambda self, require_live=False, orgs_override=None, discovery_queries=None: ([], False))
    monkeypatch.setattr("connectors.x_search.XSearchConnector.fetch", lambda self, queries, require_live=False: ([], False))

    sync_all(session, enabled_connectors={"greenhouse"})

    lead = session.query(Lead).one()
    assert lead.evidence_json["discovery_source"] == "search_web"
    assert lead.evidence_json["source_platform"] == "greenhouse+search_web"
