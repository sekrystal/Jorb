from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


APP_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    app_name: str = "Opportunity Scout"
    environment: str = "development"
    host: str = "127.0.0.1"
    port: int = 8000
    database_url: str = Field(
        default=f"sqlite:///{(APP_ROOT / 'opportunity_scout.db').as_posix()}"
    )
    demo_mode: bool = True
    autonomy_enabled: bool = True
    greenhouse_enabled: bool = True
    sync_interval_seconds: int = 900
    worker_interval_seconds: int = 900
    enable_scheduler: bool = False
    scheduler_initial_delay_seconds: Optional[int] = None
    scheduler_max_cycles: int = 0
    activity_dedupe_window_seconds: int = 90
    learning_max_watchlist_additions_per_cycle: int = 4
    feedback_max_generated_queries_per_event: int = 4
    learning_max_generated_queries_total: int = 16
    max_generated_queries_per_day: int = 24
    max_watchlist_additions_per_day: int = 24
    max_investigations_opened_per_cycle: int = 20
    alerts_enabled: bool = False
    slack_webhook_url: Optional[str] = None
    alert_max_per_window: int = 12
    alert_window_seconds: int = 3600
    alert_greenhouse_degraded_seconds: int = 3600
    alert_no_successful_fetch_seconds: int = 7200
    alert_duplicate_lead_threshold: int = 1
    alert_visible_stale_threshold: int = 1
    alert_empty_digest_seconds: int = 7200
    openai_enabled: bool = False
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-5.4-mini"
    openai_timeout_seconds: int = 20
    openai_max_retries: int = 2
    x_bearer_token: Optional[str] = None
    greenhouse_board_tokens: str = ""
    ashby_org_keys: str = ""

    model_config = SettingsConfigDict(
        env_file=APP_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("database_url", mode="before")
    @classmethod
    def resolve_database_url(cls, value: str) -> str:
        if isinstance(value, str) and value.startswith("sqlite:///./"):
            relative_path = value.removeprefix("sqlite:///./")
            return f"sqlite:///{(APP_ROOT / relative_path).as_posix()}"
        return value

    @property
    def greenhouse_tokens(self) -> list[str]:
        return [item.strip() for item in self.greenhouse_board_tokens.split(",") if item.strip()]

    @property
    def ashby_orgs(self) -> list[str]:
        return [item.strip() for item in self.ashby_org_keys.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
