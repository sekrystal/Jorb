from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field
from pydantic import model_validator


FeedbackAction = Literal[
    "like",
    "dislike",
    "save",
    "applied",
    "mute_company",
    "mute_title_pattern",
    "too_senior",
    "too_junior",
    "wrong_function",
    "wrong_geography",
    "irrelevant_company",
    "more_like_this",
]


class SignalRecord(BaseModel):
    source_type: str
    source_url: str
    author_handle: Optional[str] = None
    raw_text: str
    published_at: Optional[datetime] = None
    company_guess: Optional[str] = None
    role_guess: Optional[str] = None
    location_guess: Optional[str] = None
    hiring_confidence: float = 0.0
    signal_status: str = "new"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class ListingRecord(BaseModel):
    company_name: str
    company_domain: Optional[str] = None
    careers_url: Optional[str] = None
    company_id: Optional[int] = None
    title: str
    location: Optional[str] = None
    url: str
    source_type: str
    posted_at: Optional[datetime] = None
    first_published_at: Optional[datetime] = None
    discovered_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    description_text: Optional[str] = None
    listing_status: str = "unknown"
    expiration_confidence: float = 0.0
    freshness_hours: Optional[float] = None
    freshness_days: Optional[int] = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class CandidateProfilePayload(BaseModel):
    name: str = "Demo Candidate"
    raw_resume_text: str = ""
    extracted_summary_json: dict[str, Any] = Field(default_factory=dict)
    preferred_titles_json: list[str] = Field(default_factory=list)
    adjacent_titles_json: list[str] = Field(default_factory=list)
    excluded_titles_json: list[str] = Field(default_factory=list)
    preferred_domains_json: list[str] = Field(default_factory=list)
    excluded_companies_json: list[str] = Field(default_factory=list)
    preferred_locations_json: list[str] = Field(default_factory=list)
    seniority_guess: Optional[str] = None
    stage_preferences_json: list[str] = Field(default_factory=list)
    core_titles_json: list[str] = Field(default_factory=list)
    excluded_keywords_json: list[str] = Field(default_factory=list)
    min_seniority_band: str = "mid"
    max_seniority_band: str = "senior"
    stretch_role_families_json: list[str] = Field(default_factory=list)
    minimum_fit_threshold: float = 2.8


class ResumeUploadRequest(BaseModel):
    filename: str
    raw_text: str


class ResumeUploadResponse(BaseModel):
    resume_document_id: int
    candidate_profile: CandidateProfilePayload
    warnings: list[str] = Field(default_factory=list)


class FeedbackRequest(BaseModel):
    lead_id: int
    action: FeedbackAction
    subtype: Optional[str] = None
    reason: Optional[str] = None
    pattern: Optional[str] = None


class ApplicationStatusUpdate(BaseModel):
    lead_id: int
    current_status: str
    notes: Optional[str] = None
    date_applied: Optional[datetime] = None


class LeadResponse(BaseModel):
    id: int
    lead_type: str
    company_name: str
    primary_title: str
    url: Optional[str] = None
    source_type: str
    listing_status: Optional[str] = None
    first_published_at: Optional[datetime] = None
    discovered_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    freshness_hours: Optional[float] = None
    freshness_days: Optional[int] = None
    posted_at: Optional[datetime] = None
    surfaced_at: datetime
    rank_label: str
    confidence_label: str
    freshness_label: str
    title_fit_label: str
    qualification_fit_label: str
    source_platform: str
    source_provenance: Optional[str] = None
    source_lineage: Optional[str] = None
    discovery_source: Optional[str] = None
    saved: bool = False
    applied: bool = False
    current_status: Optional[str] = None
    date_saved: Optional[datetime] = None
    date_applied: Optional[datetime] = None
    application_notes: Optional[str] = None
    application_updated_at: Optional[datetime] = None
    next_action: Optional[str] = None
    follow_up_due: bool = False
    explanation: Optional[str] = None
    last_agent_action: Optional[str] = None
    hidden: bool
    score_breakdown_json: dict[str, Any]
    evidence_json: dict[str, Any]


class LeadsResponse(BaseModel):
    items: list[LeadResponse]


class SyncResult(BaseModel):
    signals_ingested: int
    listings_ingested: int
    leads_created: int
    leads_updated: int
    rechecks_queued: int
    live_mode_used: bool
    discovery_metrics: dict[str, dict[str, int]] = Field(default_factory=dict)
    surfaced_count: int = 0
    discovery_summary: Optional[str] = None
    discovery_status: dict[str, Any] = Field(default_factory=dict)


class StatsResponse(BaseModel):
    total_leads: int
    visible_leads: int
    active_listings: int
    fresh_listings: int
    combined_leads: int
    signal_only_leads: int
    saved_leads: int
    applied_leads: int
    pending_rechecks: int


class LearningSummary(BaseModel):
    boosted_titles: list[str] = Field(default_factory=list)
    boosted_domains: list[str] = Field(default_factory=list)
    penalized_sources: list[str] = Field(default_factory=list)
    generated_queries: list[str] = Field(default_factory=list)


class SourceQueryResponse(BaseModel):
    id: int
    query_text: str
    source_type: str
    status: str
    performance_stats_json: dict[str, Any]


class ApplicationsResponse(BaseModel):
    items: list[LeadResponse]


class AgentActivityResponse(BaseModel):
    id: int
    timestamp: datetime
    agent_name: str
    action: str
    target_type: Optional[str] = None
    target_count: Optional[int] = None
    target_entity: Optional[str] = None
    result_summary: str


class AgentActivitiesResponse(BaseModel):
    items: list[AgentActivityResponse]


class AgentRunRequest(BaseModel):
    agent: Literal["scout", "resolver", "fit", "ranker", "critic", "tracker", "learning", "full_pipeline", "reset_demo"]


class AgentRunResponse(BaseModel):
    status: str = "ok"
    agent: str
    summary: str


class AutonomyHealthResponse(BaseModel):
    last_successful_run_at: Optional[datetime] = None
    last_failed_run_at: Optional[datetime] = None
    latest_success_summary: Optional[str] = None
    latest_failure_summary: Optional[str] = None
    open_investigations: int = 0
    suppressed_leads: int = 0
    due_follow_ups: int = 0
    scheduler_enabled: bool = False
    runtime_state: str = "paused"
    worker_state: str = "idle"
    runtime_phase: str = "paused"
    run_once_requested: bool = False
    last_cycle_started_at: Optional[datetime] = None
    last_successful_cycle_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    sleep_until: Optional[datetime] = None
    next_cycle_at: Optional[datetime] = None
    current_interval_seconds: int = 0
    status_message: Optional[str] = None
    last_control_action: Optional[str] = None
    last_control_at: Optional[datetime] = None
    operator_hints: list[str] = Field(default_factory=list)


class RuntimeControlResponse(BaseModel):
    run_state: Literal["running", "paused"] = "paused"
    worker_state: Literal["idle", "paused", "sleeping", "running_cycle", "stopping", "error"] = "idle"
    runtime_phase: str = "paused"
    run_once_requested: bool = False
    last_cycle_started_at: Optional[datetime] = None
    last_successful_cycle_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    sleep_until: Optional[datetime] = None
    next_cycle_at: Optional[datetime] = None
    current_interval_seconds: int = 0
    status_message: Optional[str] = None
    last_control_action: Optional[str] = None
    last_control_at: Optional[datetime] = None
    last_cycle_summary: Optional[str] = None
    latest_failure_summary: Optional[str] = None
    operator_hints: list[str] = Field(default_factory=list)


class RuntimeControlRequest(BaseModel):
    action: Optional[Literal["play", "pause", "run_once"]] = None
    run_state: Optional[Literal["running", "paused"]] = None

    @model_validator(mode="after")
    def validate_payload(self):
        if self.action is None and self.run_state is None:
            raise ValueError("Provide either action or run_state.")
        return self


class AutonomyDigestResponse(BaseModel):
    run_at: Optional[datetime] = None
    summary: Optional[str] = None
    new_leads: list[str] = Field(default_factory=list)
    suppressed_leads: list[str] = Field(default_factory=list)
    investigations_changed: int = 0
    follow_ups_created: list[str] = Field(default_factory=list)
    watchlist_changes: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)


class ConnectorHealthResponse(BaseModel):
    connector_name: str
    status: str
    blocked_reason: Optional[str] = None
    config_key: Optional[str] = None
    consecutive_failures: int
    recent_successes: int = 0
    recent_failures: int = 0
    trust_score: float = 0.0
    circuit_state: str
    disabled_until: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None
    last_error: Optional[str] = None
    last_failure_classification: Optional[str] = None
    last_mode: Optional[str] = None
    last_item_count: int = 0
    quarantine_count: int = 0
    approved_for_unattended: bool = False
    last_freshness_lag_seconds: Optional[int] = None


class ConnectorResetRequest(BaseModel):
    confirm: bool = False


class ConnectorResetResponse(BaseModel):
    connector_name: str
    status: str
    summary: str


class CompanyDiscoveryRowResponse(BaseModel):
    company_name: str
    company_domain: Optional[str] = None
    normalized_company_key: str
    discovery_source: str
    discovery_query: Optional[str] = None
    first_discovered_at: datetime
    last_discovered_at: datetime
    last_expanded_at: Optional[datetime] = None
    board_type: str
    board_locator: str
    surface_provenance: Optional[str] = None
    source_lineage: Optional[str] = None
    expansion_status: str
    expansion_attempts: int
    last_expansion_result_count: int
    visible_yield_count: int
    suppressed_yield_count: int
    location_filtered_count: int = 0
    utility_score: float
    blocked_reason: Optional[str] = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DiscoveryStatusResponse(BaseModel):
    total_known_companies: int = 0
    discovered_last_24h: int = 0
    expanded_last_24h: int = 0
    recent_items: list[CompanyDiscoveryRowResponse] = Field(default_factory=list)
    latest_planner_run: Optional[dict[str, Any]] = None
    recent_plans: list[dict[str, Any]] = Field(default_factory=list)
    recent_expansions: list[dict[str, Any]] = Field(default_factory=list)
    recent_visible_yield: list[CompanyDiscoveryRowResponse] = Field(default_factory=list)
    blocked_or_cooled_down: list[CompanyDiscoveryRowResponse] = Field(default_factory=list)
    recent_greenhouse_tokens: list[dict[str, Any]] = Field(default_factory=list)
    recent_ashby_identifiers: list[dict[str, Any]] = Field(default_factory=list)
    recent_geography_rejections: list[dict[str, Any]] = Field(default_factory=list)
    recent_agentic_leads: list[dict[str, Any]] = Field(default_factory=list)
    next_recommended_queries: list[str] = Field(default_factory=list)
    latest_openai_usage: dict[str, bool] = Field(default_factory=dict)
    cycle_metrics: dict[str, Any] = Field(default_factory=dict)
    recent_successful_expansions: list[dict[str, Any]] = Field(default_factory=list)


class DailyDigestResponse(BaseModel):
    digest_date: str
    summary: str
    new_leads: list[str] = Field(default_factory=list)
    suppressed_leads: list[str] = Field(default_factory=list)
    investigations_changed: int = 0
    follow_ups_created: list[str] = Field(default_factory=list)
    watchlist_changes: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)


class AutonomyStatusResponse(BaseModel):
    health: AutonomyHealthResponse
    digest: AutonomyDigestResponse
    daily_digest: Optional[DailyDigestResponse] = None
    connector_health: list[ConnectorHealthResponse] = Field(default_factory=list)


class InvestigationResponse(BaseModel):
    id: int
    signal_id: int
    company_guess: Optional[str] = None
    role_guess: Optional[str] = None
    confidence: float
    status: str
    attempts: int
    next_check_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None
    source_url: Optional[str] = None
    raw_text: Optional[str] = None


class InvestigationsResponse(BaseModel):
    items: list[InvestigationResponse]


class QueryLearningRow(BaseModel):
    query_text: str
    source_type: str
    status: str
    decision_reason: Optional[str] = None
    leads_generated: int
    likes: int
    saves: int
    applies: int
    dislikes: int
    last_run_at: Optional[datetime] = None


class WatchlistItemResponse(BaseModel):
    item_type: str
    value: str
    source_reason: str
    confidence: str
    status: str
    decision_reason: Optional[str] = None


class FollowUpTaskResponse(BaseModel):
    application_id: int
    company_name: str
    title: str
    task_type: str
    due_at: datetime
    status: str
    notes: Optional[str] = None


class LearningViewResponse(BaseModel):
    top_queries: list[QueryLearningRow]
    generated_queries: list[str] = Field(default_factory=list)
    suppressed_queries: list[str] = Field(default_factory=list)
    inferred_title_families: list[str] = Field(default_factory=list)
    inferred_domains: list[str] = Field(default_factory=list)
    watchlist_items: list[WatchlistItemResponse] = Field(default_factory=list)
    follow_up_tasks: list[FollowUpTaskResponse] = Field(default_factory=list)
