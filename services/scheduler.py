from __future__ import annotations

import threading
from typing import Optional

from core.config import Settings, get_settings
from core.logging import get_logger
from core.db import SessionLocal
from services.activity import log_agent_failure
from services.worker_runtime import run_worker_cycle


logger = get_logger(__name__)


class LocalScheduler:
    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._cycle_count = 0

    def start(self) -> None:
        if not self.settings.enable_scheduler or not self.settings.autonomy_enabled or self._thread:
            return
        self._thread = threading.Thread(target=self._run_loop, name="opportunity-scout-scheduler", daemon=True)
        self._thread.start()
        logger.info("Local scheduler started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def _run_loop(self) -> None:
        initial_delay = self.settings.scheduler_initial_delay_seconds
        if initial_delay is None:
            initial_delay = self.settings.sync_interval_seconds
        self._stop_event.wait(initial_delay)
        while not self._stop_event.is_set():
            try:
                with SessionLocal() as session:
                    run_worker_cycle(session=session, settings=self.settings)
                    session.commit()
            except Exception as exc:  # pragma: no cover
                logger.exception("Scheduler sync failed: %s", exc)
                with SessionLocal() as session:
                    session.rollback()
                    log_agent_failure(session, "Scheduler", "run full pipeline", f"Scheduler sync failed: {exc}")
                    session.commit()
            self._cycle_count += 1
            if self.settings.scheduler_max_cycles and self._cycle_count >= self.settings.scheduler_max_cycles:
                logger.info("Scheduler reached max cycles (%s) and is stopping", self.settings.scheduler_max_cycles)
                break
            self._stop_event.wait(self.settings.sync_interval_seconds)
        self._thread = None

    @property
    def cycle_count(self) -> int:
        return self._cycle_count

    def wait_until_finished(self, timeout: float | None = None) -> None:
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
