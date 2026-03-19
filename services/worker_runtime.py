from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from core.config import Settings
from services.activity import log_agent_activity
from services.ops import get_runtime_connector_set
from services.pipeline import run_full_pipeline
from services.runtime_control import determine_cycle_mode, mark_cycle_started, mark_cycle_success


def run_worker_cycle(session: Session, settings: Settings) -> dict[str, Any]:
    cycle_mode, control = determine_cycle_mode(session, settings)
    if cycle_mode == "disabled":
        log_agent_activity(
            session,
            "Worker",
            "autonomy disabled",
            "Worker is idle because the global autonomy kill switch is active.",
            target_type="worker",
            target_count=0,
        )
        return {"state": "disabled", "ran": False, "summary": "Autonomy disabled."}

    if cycle_mode == "paused":
        log_agent_activity(
            session,
            "Worker",
            "paused",
            "Worker is paused. Resume it from the runtime controls or queue a single run.",
            target_type="worker",
            target_count=0,
        )
        return {"state": "paused", "ran": False, "summary": "Worker paused."}

    source_mode, enabled_connectors, strict_live_connectors = get_runtime_connector_set(settings)
    if not enabled_connectors:
        log_agent_activity(
            session,
            "Worker",
            "connector disabled",
            "Worker is idle because no live connectors are enabled.",
            target_type="worker",
            target_count=0,
        )
        return {"state": "no_connectors", "ran": False, "summary": "No connectors enabled."}

    mark_cycle_started(control)
    response = run_full_pipeline(
        session,
        source_mode=source_mode,
        enabled_connectors=enabled_connectors,
        strict_live_connectors=strict_live_connectors,
    )
    mark_cycle_success(control, response.summary, consume_run_once=True)
    log_agent_activity(
        session,
        "Worker",
        "completed cycle",
        f"Worker cycle completed at {datetime.utcnow().isoformat()}. {response.summary}",
        target_type="worker",
        target_count=1,
    )
    return {"state": cycle_mode, "ran": True, "summary": response.summary}

