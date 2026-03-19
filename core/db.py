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


def init_db() -> None:
    inspector = inspect(engine)
    expected_columns = {
        "signals": {"signal_status"},
        "listings": {"listing_status", "freshness_days"},
        "leads": {"lead_type", "rank_label", "qualification_fit_label", "last_agent_action"},
        "candidate_profiles": {"core_titles_json", "minimum_fit_threshold"},
        "source_queries": {"performance_stats_json"},
        "source_query_stats": {"query_text", "leads_generated", "last_run_at", "decision_reason"},
        "applications": {"current_status", "date_applied"},
        "agent_activities": {"agent_name", "action", "result_summary"},
        "investigations": {"signal_id", "status", "next_check_at"},
        "watchlist_items": {"item_type", "value", "status", "decision_reason"},
        "follow_up_tasks": {"application_id", "task_type", "due_at"},
        "agent_runs": {"agent_name", "action", "status"},
        "runtime_control": {"run_state", "run_once_requested", "last_successful_cycle_at"},
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
