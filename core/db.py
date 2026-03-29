from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from core.config import get_settings
from core.models import Base


settings = get_settings()
engine = create_engine(
    settings.database_url,
    connect_args={"check_same_thread": False} if settings.database_url.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


SQLITE_COMPATIBILITY_COLUMNS: dict[str, dict[str, str]] = {
    "applications": {
        "status_reason_code": "TEXT",
        "outcome_code": "VARCHAR(50)",
        "outcome_reason_code": "TEXT",
    }
}


def _apply_sqlite_compatibility_migrations() -> None:
    if not str(engine.url).startswith("sqlite"):
        return

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    with engine.begin() as connection:
        for table_name, column_defs in SQLITE_COMPATIBILITY_COLUMNS.items():
            if table_name not in existing_tables:
                continue
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, column_type in column_defs.items():
                if column_name in existing_columns:
                    continue
                connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))


def init_db() -> None:
    _apply_sqlite_compatibility_migrations()
    inspector = inspect(engine)
    expected_columns = {
        "signals": {"signal_status"},
        "listings": {"listing_status", "freshness_hours", "freshness_days", "first_published_at", "last_seen_at"},
        "leads": {"lead_type", "rank_label", "qualification_fit_label", "last_agent_action"},
        "candidate_profiles": {"core_titles_json", "minimum_fit_threshold"},
        "source_queries": {"performance_stats_json"},
        "source_query_stats": {"query_text", "leads_generated", "last_run_at", "decision_reason"},
        "applications": {
            "current_status",
            "date_applied",
            "status_reason_code",
            "outcome_code",
            "outcome_reason_code",
        },
        "agent_activities": {"agent_name", "action", "result_summary"},
        "investigations": {"signal_id", "status", "next_check_at"},
        "watchlist_items": {"item_type", "value", "status", "decision_reason"},
        "follow_up_tasks": {"application_id", "task_type", "due_at"},
        "agent_runs": {"agent_name", "action", "status"},
        "search_runs": {"source_key", "worker_name", "provider", "status", "query_count", "result_count"},
        "runtime_control": {
            "run_state",
            "worker_state",
            "run_once_requested",
            "last_successful_cycle_at",
            "last_heartbeat_at",
            "sleep_until",
            "current_interval_seconds",
            "status_message",
            "last_control_action",
            "last_control_at",
        },
        "connector_health": {
            "connector_name",
            "status",
            "circuit_state",
            "recent_successes",
            "recent_failures",
            "trust_score",
            "last_failure_classification",
            "quarantine_count",
            "approved_for_unattended",
        },
        "company_discovery": {
            "discovery_key",
            "company_name",
            "normalized_company_key",
            "discovery_source",
            "board_type",
            "board_locator",
            "expansion_status",
            "visible_yield_count",
            "location_filtered_count",
            "utility_score",
        },
        "run_digests": {"agent_run_id", "summary", "is_noop"},
        "daily_digests": {"digest_date", "summary"},
        "alert_events": {"alert_key", "category", "severity", "status"},
    }

    needs_rebuild = False
    existing_tables = set(inspector.get_table_names())
    for table_name, required_columns in expected_columns.items():
        if table_name not in existing_tables:
            continue
        existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
        if not required_columns.issubset(existing_columns):
            needs_rebuild = True
            break

    if needs_rebuild:
        Base.metadata.drop_all(bind=engine)
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS opportunities"))
    Base.metadata.create_all(bind=engine)


def reset_sqlite_db() -> None:
    if not str(engine.url).startswith("sqlite"):
        Base.metadata.drop_all(bind=engine)
        return

    # Wipe the current schema first so a reset is deterministic even if another
    # local process still has the SQLite file open.
    with engine.begin() as connection:
        connection.execute(text("PRAGMA foreign_keys = OFF"))
        connection.execute(text("DROP TABLE IF EXISTS opportunities"))
    Base.metadata.drop_all(bind=engine)
    engine.dispose()

    database_path = engine.url.database
    if not database_path:
        return

    db_file = Path(database_path)
    sidecars = [db_file.with_name(f"{db_file.name}-wal"), db_file.with_name(f"{db_file.name}-shm")]
    for path in [db_file, *sidecars]:
        if path.exists():
            path.unlink()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
