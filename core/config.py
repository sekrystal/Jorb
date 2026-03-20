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
    autonomy_enabled: bool = False
    greenhouse_enabled: bool = False
    sync_interval_seconds: int = 900
    worker_interval_seconds: int = 900
    interactive_worker_interval_seconds: int = 15
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
    openai_model: str = "gpt-5-mini"
    openai_timeout_seconds: int = 20
    openai_max_retries: int = 2
    enable_ai_readtime_critic: bool = False
    ai_fit_max_calls_per_cycle: int = 40
    x_bearer_token: Optional[str] = None
    greenhouse_board_tokens: str = ""
    ashby_org_keys: str = ""
    search_discovery_enabled: bool = False
    search_discovery_provider: str = "duckduckgo_html"
    search_discovery_query_limit: int = 8
    search_discovery_result_limit: int = 5
    discovery_max_search_queries_per_cycle: int = 8
    discovery_max_new_companies_per_cycle: int = 6
    discovery_max_expansions_per_cycle: int = 4
    discovery_company_cooldown_minutes: int = 180
    discovery_max_pages_to_crawl_per_cycle: int = 4
    discovery_explore_ratio: float = 0.4
    allowed_location_scopes: str = "us,remote_us"
    allow_remote_global: bool = False
    allow_ambiguous_locations: bool = False

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

    @property
    def allowed_location_scope_list(self) -> list[str]:
        return [item.strip().lower() for item in self.allowed_location_scopes.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
