from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import Settings, get_settings
from core.models import RuntimeControl
from core.schemas import RuntimeControlResponse


def get_runtime_control(session: Session, settings: Settings | None = None) -> RuntimeControl:
    control = session.scalar(select(RuntimeControl).order_by(RuntimeControl.id.asc()))
    if control:
        return control

    default_state = "running" if settings and settings.autonomy_enabled and not settings.demo_mode else "paused"
    control = RuntimeControl(run_state=default_state)
    session.add(control)
    session.flush()
    return control


def effective_worker_interval_seconds(settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    if settings.demo_mode:
        return max(1, min(settings.worker_interval_seconds, settings.interactive_worker_interval_seconds))
    return max(1, settings.worker_interval_seconds)


def runtime_phase(run_state: str, worker_state: str, run_once_requested: bool, autonomy_enabled: bool = True) -> str:
    if not autonomy_enabled:
        return "disabled"
    if worker_state == "error":
        return "error"
    if worker_state == "running_cycle":
        return "running"
    if run_state == "paused":
        return "queued" if run_once_requested else "paused"
    if worker_state == "sleeping":
        return "sleeping"
    if run_once_requested:
        return "queued"
    return "idle"


def runtime_operator_hints(
    *,
    phase: str,
    run_once_requested: bool,
    next_cycle_at: datetime | None,
    last_successful_cycle_at: datetime | None,
    latest_failure_summary: str | None = None,
) -> list[str]:
    hints: list[str] = []
    if phase == "disabled":
        hints.append("Global autonomy is disabled, so the worker will not start until that kill switch is lifted.")
    elif phase == "paused":
        hints.append("Worker is paused. Press Play to resume unattended cycles or Run once for one bounded cycle.")
    elif phase == "queued":
        hints.append("A single bounded cycle is queued and will start when the worker loop checks runtime control.")
    elif phase == "running":
        hints.append("Worker is actively running the pipeline now.")
    elif phase == "sleeping":
        hints.append(
            "Worker is healthy and sleeping until the next scheduled cycle."
            if next_cycle_at
            else "Worker is sleeping between cycles."
        )
    elif phase == "idle":
        hints.append("Worker is ready for the next bounded cycle.")
    elif phase == "error":
        hints.append("Worker reported an error state. Check the latest failure summary and worker logs before resuming.")

    if latest_failure_summary:
        hints.append("Latest failure is still visible below so operators can compare recovery attempts against the last known break.")
    if run_once_requested and phase != "queued":
        hints.append("Run once remains requested and will be consumed after the next successful cycle.")
    if last_successful_cycle_at is None:
        hints.append("No successful worker cycle has been recorded yet in this environment.")
    return hints


def runtime_control_payload(control: RuntimeControl, settings: Settings | None = None) -> RuntimeControlResponse:
    settings = settings or get_settings()
    next_cycle_at = control.sleep_until
    phase = runtime_phase(
        control.run_state,
        control.worker_state,
        control.run_once_requested,
        autonomy_enabled=settings.autonomy_enabled,
    )
    return RuntimeControlResponse(
        run_state=control.run_state,
        worker_state=control.worker_state,
        runtime_phase=phase,
        run_once_requested=control.run_once_requested,
        last_cycle_started_at=control.last_cycle_started_at,
        last_successful_cycle_at=control.last_successful_cycle_at,
        last_heartbeat_at=control.last_heartbeat_at,
        sleep_until=control.sleep_until,
        next_cycle_at=next_cycle_at,
        current_interval_seconds=control.current_interval_seconds or effective_worker_interval_seconds(settings),
        status_message=control.status_message,
        last_control_action=control.last_control_action,
        last_control_at=control.last_control_at,
        last_cycle_summary=control.last_cycle_summary,
        latest_failure_summary=None,
        operator_hints=runtime_operator_hints(
            phase=phase,
            run_once_requested=control.run_once_requested,
            next_cycle_at=next_cycle_at,
            last_successful_cycle_at=control.last_successful_cycle_at,
        ),
    )


def set_runtime_action(session: Session, action: str, settings: Settings | None = None) -> RuntimeControl:
    control = get_runtime_control(session, settings=settings)
    if action == "play":
        control.run_state = "running"
        control.run_once_requested = False
        control.worker_state = "idle"
        control.sleep_until = None
        control.status_message = "Worker will start the next cycle immediately."
    elif action == "pause":
        control.run_state = "paused"
        control.run_once_requested = False
        control.worker_state = "paused"
        control.sleep_until = None
        control.status_message = "Pause requested. The worker will stop after the current bounded step."
    elif action == "run_once":
        control.run_once_requested = True
        control.worker_state = "idle"
        control.sleep_until = None
        control.status_message = "Single cycle requested."
    control.last_heartbeat_at = datetime.utcnow()
    control.last_control_action = action
    control.last_control_at = control.last_heartbeat_at
    session.flush()
    return control


def determine_cycle_mode(session: Session, settings: Settings) -> tuple[str, RuntimeControl]:
    control = get_runtime_control(session, settings=settings)
    if not settings.autonomy_enabled:
        return "disabled", control
    if control.run_state == "paused" and not control.run_once_requested:
        return "paused", control
    if control.run_once_requested:
        return "run_once", control
    return "running", control


def mark_cycle_started(control: RuntimeControl) -> None:
    control.last_cycle_started_at = datetime.utcnow()
    control.last_heartbeat_at = control.last_cycle_started_at
    control.worker_state = "running_cycle"
    control.sleep_until = None
    control.current_interval_seconds = 0
    control.status_message = "Worker is running a pipeline cycle."


def mark_cycle_success(control: RuntimeControl, summary: str, consume_run_once: bool = True) -> None:
    control.last_successful_cycle_at = datetime.utcnow()
    control.last_heartbeat_at = control.last_successful_cycle_at
    control.worker_state = "idle"
    control.sleep_until = None
    control.current_interval_seconds = 0
    control.status_message = "Worker finished the last cycle successfully."
    control.last_cycle_summary = summary
    if consume_run_once:
        control.run_once_requested = False


def mark_worker_state(
    control: RuntimeControl,
    state: str,
    message: str,
    sleep_seconds: int | None = None,
) -> None:
    control.worker_state = state
    control.last_heartbeat_at = datetime.utcnow()
    control.status_message = message
    control.current_interval_seconds = max(sleep_seconds or 0, 0)
    control.sleep_until = (
        control.last_heartbeat_at + timedelta(seconds=max(sleep_seconds, 0))
        if sleep_seconds is not None
        else None
    )


def mark_cycle_error(control: RuntimeControl, message: str) -> None:
    control.worker_state = "error"
    control.last_heartbeat_at = datetime.utcnow()
    control.sleep_until = None
    control.current_interval_seconds = 0
    control.status_message = message
