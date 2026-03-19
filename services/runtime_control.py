from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import Settings
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


def runtime_control_payload(control: RuntimeControl) -> RuntimeControlResponse:
    return RuntimeControlResponse(
        run_state=control.run_state,
        run_once_requested=control.run_once_requested,
        last_cycle_started_at=control.last_cycle_started_at,
        last_successful_cycle_at=control.last_successful_cycle_at,
        last_cycle_summary=control.last_cycle_summary,
    )


def set_runtime_action(session: Session, action: str, settings: Settings | None = None) -> RuntimeControl:
    control = get_runtime_control(session, settings=settings)
    if action == "play":
        control.run_state = "running"
        control.run_once_requested = False
    elif action == "pause":
        control.run_state = "paused"
        control.run_once_requested = False
    elif action == "run_once":
        control.run_once_requested = True
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


def mark_cycle_success(control: RuntimeControl, summary: str, consume_run_once: bool = True) -> None:
    control.last_successful_cycle_at = datetime.utcnow()
    control.last_cycle_summary = summary
    if consume_run_once:
        control.run_once_requested = False

