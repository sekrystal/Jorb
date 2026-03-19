from __future__ import annotations

import signal
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import get_settings
from core.db import SessionLocal, init_db
from core.logging import configure_logging, get_logger
from services.activity import log_agent_failure
from services.alerts import evaluate_alerts
from services.worker_runtime import run_worker_cycle


logger = get_logger(__name__)
STOP_REQUESTED = False


def _handle_signal(signum, _frame) -> None:  # pragma: no cover
    global STOP_REQUESTED
    STOP_REQUESTED = True
    logger.info("Worker received signal %s and will exit after the current cycle.", signum)


def run_worker_loop() -> None:
    configure_logging()
    init_db()
    settings = get_settings()
    logger.info("Starting Opportunity Scout worker in %s mode.", "demo" if settings.demo_mode else "live")

    while not STOP_REQUESTED:
        settings = get_settings()
        try:
            with SessionLocal() as session:
                outcome = run_worker_cycle(session, settings)
                evaluate_alerts(session, settings=settings)
                session.commit()
        except Exception as exc:  # pragma: no cover
            logger.exception("Worker cycle failed: %s", exc)
            with SessionLocal() as session:
                session.rollback()
                log_agent_failure(session, "Worker", "run cycle", f"Worker loop failed: {exc}")
                evaluate_alerts(session, settings=settings)
                session.commit()

        if STOP_REQUESTED:
            break
        sleep_seconds = settings.worker_interval_seconds
        if outcome.get("state") in {"paused", "disabled", "no_connectors"}:
            sleep_seconds = min(settings.worker_interval_seconds, 5)
        time.sleep(sleep_seconds)


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    run_worker_loop()


if __name__ == "__main__":
    main()
