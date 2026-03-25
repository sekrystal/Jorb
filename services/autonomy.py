from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.config import Settings, get_settings
from core.models import AgentRun, ConnectorHealth, DailyDigest, FollowUpTask, Investigation, Lead, RunDigest, RuntimeControl
from core.schemas import AutonomyDigestResponse, AutonomyHealthResponse, ConnectorHealthResponse, DailyDigestResponse
from services.connector_admin import CONNECTOR_CONFIG_KEYS, connector_blocked_reason
from services.runtime_control import effective_worker_interval_seconds, runtime_operator_hints, runtime_phase


def build_autonomy_health(session: Session, settings: Settings | None = None) -> AutonomyHealthResponse:
    settings = settings or get_settings()
    latest_success_run = session.execute(
        select(AgentRun.created_at, AgentRun.summary)
        .where(AgentRun.status == "ok")
        .order_by(AgentRun.created_at.desc())
        .limit(1)
    ).first()
    open_investigations = session.scalar(
        select(func.count(Investigation.id)).where(Investigation.status.in_(["open", "rechecking"]))
    ) or 0
    suppressed_leads = session.scalar(select(func.count(Lead.id)).where(Lead.hidden.is_(True))) or 0
    due_follow_ups = session.scalar(
        select(func.count(FollowUpTask.id)).where(FollowUpTask.status == "open", FollowUpTask.due_at <= datetime.utcnow())
    ) or 0
    latest_failure_run = session.execute(
        select(AgentRun.created_at, AgentRun.summary).where(AgentRun.status == "failed").order_by(AgentRun.created_at.desc()).limit(1)
    ).first()
    runtime = session.scalar(select(RuntimeControl).order_by(RuntimeControl.id.asc()))
    runtime_state = runtime.run_state if runtime else ("running" if settings.autonomy_enabled else "paused")
    worker_state = runtime.worker_state if runtime else "idle"
    run_once_requested = runtime.run_once_requested if runtime else False
    next_cycle_at = runtime.sleep_until if runtime else None
    last_successful_cycle_at = runtime.last_successful_cycle_at if runtime else None
    phase = runtime_phase(runtime_state, worker_state, run_once_requested, autonomy_enabled=settings.autonomy_enabled)
    latest_success_summary = (runtime.last_cycle_summary if runtime and runtime.last_cycle_summary else None) or (
        latest_success_run.summary if latest_success_run else None
    )
    latest_failure_summary = latest_failure_run.summary if latest_failure_run else None
    return AutonomyHealthResponse(
        last_successful_run_at=latest_success_run.created_at if latest_success_run else None,
        last_failed_run_at=latest_failure_run.created_at if latest_failure_run else None,
        latest_success_summary=latest_success_summary,
        latest_failure_summary=latest_failure_summary,
        open_investigations=open_investigations,
        suppressed_leads=suppressed_leads,
        due_follow_ups=due_follow_ups,
        scheduler_enabled=settings.enable_scheduler,
        runtime_state=runtime_state,
        worker_state=worker_state,
        runtime_phase=phase,
        run_once_requested=run_once_requested,
        last_cycle_started_at=runtime.last_cycle_started_at if runtime else None,
        last_successful_cycle_at=last_successful_cycle_at,
        last_heartbeat_at=runtime.last_heartbeat_at if runtime else None,
        sleep_until=runtime.sleep_until if runtime else None,
        next_cycle_at=next_cycle_at,
        current_interval_seconds=effective_worker_interval_seconds(settings),
        status_message=runtime.status_message if runtime else None,
        last_control_action=runtime.last_control_action if runtime else None,
        last_control_at=runtime.last_control_at if runtime else None,
        operator_hints=runtime_operator_hints(
            phase=phase,
            run_once_requested=run_once_requested,
            next_cycle_at=next_cycle_at,
            last_successful_cycle_at=last_successful_cycle_at,
            latest_failure_summary=latest_failure_summary,
        ),
    )


def build_latest_run_digest(session: Session) -> AutonomyDigestResponse:
    latest_pipeline = session.scalar(
        select(RunDigest)
        .order_by(RunDigest.created_at.desc())
        .limit(1)
    )
    if not latest_pipeline:
        return AutonomyDigestResponse()
    return AutonomyDigestResponse(
        run_at=latest_pipeline.created_at,
        summary=latest_pipeline.summary,
        new_leads=list(dict.fromkeys(latest_pipeline.new_leads_json or [])),
        suppressed_leads=list(dict.fromkeys(latest_pipeline.suppressed_leads_json or [])),
        investigations_changed=latest_pipeline.investigations_changed or 0,
        follow_ups_created=list(dict.fromkeys(latest_pipeline.follow_ups_created_json or [])),
        watchlist_changes=list(dict.fromkeys(latest_pipeline.watchlist_changes_json or [])),
        failures=list(dict.fromkeys(latest_pipeline.failures_json or [])),
    )


def build_daily_digest(session: Session) -> DailyDigestResponse | None:
    row = session.scalar(select(DailyDigest).order_by(DailyDigest.digest_date.desc()).limit(1))
    if not row:
        return None
    return DailyDigestResponse(
        digest_date=row.digest_date,
        summary=row.summary,
        new_leads=row.new_leads_json or [],
        suppressed_leads=row.suppressed_leads_json or [],
        investigations_changed=row.investigations_changed or 0,
        follow_ups_created=row.follow_ups_created_json or [],
        watchlist_changes=row.watchlist_changes_json or [],
        failures=row.failures_json or [],
    )


def list_connector_health(session: Session) -> list[ConnectorHealthResponse]:
    settings = get_settings()
    rows = {
        row.connector_name: row
        for row in session.scalars(select(ConnectorHealth).order_by(ConnectorHealth.connector_name.asc())).all()
    }
    connector_names = sorted(set(rows) | set(CONNECTOR_CONFIG_KEYS))
    responses: list[ConnectorHealthResponse] = []
    for name in connector_names:
        row = rows.get(name)
        if row is None:
            blocked_reason = connector_blocked_reason(name, None, settings)
            responses.append(
                ConnectorHealthResponse(
                    connector_name=name,
                    status="not_configured" if blocked_reason in {"disabled", "missing_tokens"} else "unknown",
                    blocked_reason=blocked_reason,
                    config_key=CONNECTOR_CONFIG_KEYS.get(name),
                    consecutive_failures=0,
                    recent_successes=0,
                    recent_failures=0,
                    trust_score=0.0,
                    circuit_state="closed",
                    approved_for_unattended=False,
                )
            )
            continue
        responses.append(
            ConnectorHealthResponse(
                connector_name=row.connector_name,
                status=row.status,
                blocked_reason=connector_blocked_reason(row.connector_name, row, settings),
                config_key=CONNECTOR_CONFIG_KEYS.get(row.connector_name),
                consecutive_failures=row.consecutive_failures,
                recent_successes=row.recent_successes,
                recent_failures=row.recent_failures,
                trust_score=row.trust_score,
                circuit_state=row.circuit_state,
                disabled_until=row.disabled_until,
                last_success_at=row.last_success_at,
                last_failure_at=row.last_failure_at,
                last_error=row.last_error,
                last_failure_classification=row.last_failure_classification,
                last_mode=row.last_mode,
                last_item_count=row.last_item_count,
                quarantine_count=row.quarantine_count,
                approved_for_unattended=row.approved_for_unattended,
                last_freshness_lag_seconds=row.last_freshness_lag_seconds,
            )
        )
    return responses
