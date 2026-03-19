from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    return datetime.utcnow()


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    domain: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    careers_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    ats_provider: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class CandidateProfile(Base):
    __tablename__ = "candidate_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), default="Demo Candidate")
    raw_resume_text: Mapped[str] = mapped_column(Text, default="")
    extracted_summary_json: Mapped[dict] = mapped_column(JSON, default=dict)
    preferred_titles_json: Mapped[list] = mapped_column(JSON, default=list)
    adjacent_titles_json: Mapped[list] = mapped_column(JSON, default=list)
    excluded_titles_json: Mapped[list] = mapped_column(JSON, default=list)
    preferred_domains_json: Mapped[list] = mapped_column(JSON, default=list)
    excluded_companies_json: Mapped[list] = mapped_column(JSON, default=list)
    preferred_locations_json: Mapped[list] = mapped_column(JSON, default=list)
    seniority_guess: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    stage_preferences_json: Mapped[list] = mapped_column(JSON, default=list)
    core_titles_json: Mapped[list] = mapped_column(JSON, default=list)
    excluded_keywords_json: Mapped[list] = mapped_column(JSON, default=list)
    min_seniority_band: Mapped[str] = mapped_column(String(50), default="mid")
    max_seniority_band: Mapped[str] = mapped_column(String(50), default="senior")
    stretch_role_families_json: Mapped[list] = mapped_column(JSON, default=list)
    minimum_fit_threshold: Mapped[float] = mapped_column(Float, default=2.8)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class ResumeDocument(Base):
    __tablename__ = "resume_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String(255))
    raw_text: Mapped[str] = mapped_column(Text)
    parsed_json: Mapped[dict] = mapped_column(JSON, default=dict)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(50), index=True)
    source_url: Mapped[str] = mapped_column(String(500), unique=True, index=True)
    author_handle: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    raw_text: Mapped[str] = mapped_column(Text)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    company_guess: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    role_guess: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    location_guess: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    hiring_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    signal_status: Mapped[str] = mapped_column(String(50), default="new", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_name: Mapped[str] = mapped_column(String(255), index=True)
    company_id: Mapped[Optional[int]] = mapped_column(ForeignKey("companies.id"), nullable=True, index=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    location: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    url: Mapped[str] = mapped_column(String(500), unique=True, index=True)
    source_type: Mapped[str] = mapped_column(String(50), index=True)
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    description_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    listing_status: Mapped[str] = mapped_column(String(50), default="unknown", index=True)
    expiration_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    freshness_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_type: Mapped[str] = mapped_column(String(50), index=True)
    company_name: Mapped[str] = mapped_column(String(255), index=True)
    company_id: Mapped[Optional[int]] = mapped_column(ForeignKey("companies.id"), nullable=True, index=True)
    primary_title: Mapped[str] = mapped_column(String(255), index=True)
    listing_id: Mapped[Optional[int]] = mapped_column(ForeignKey("listings.id"), nullable=True, index=True)
    signal_id: Mapped[Optional[int]] = mapped_column(ForeignKey("signals.id"), nullable=True, index=True)
    surfaced_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    rank_label: Mapped[str] = mapped_column(String(50), default="weak", index=True)
    confidence_label: Mapped[str] = mapped_column(String(50), default="low", index=True)
    freshness_label: Mapped[str] = mapped_column(String(50), default="unknown", index=True)
    title_fit_label: Mapped[str] = mapped_column(String(50), default="weak title match")
    qualification_fit_label: Mapped[str] = mapped_column(String(50), default="unclear", index=True)
    explanation: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    score_breakdown_json: Mapped[dict] = mapped_column(JSON, default=dict)
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    last_agent_action: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    hidden: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Application(Base):
    __tablename__ = "applications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id"), unique=True, index=True)
    company_name: Mapped[str] = mapped_column(String(255), index=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    date_saved: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    date_applied: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    current_status: Mapped[str] = mapped_column(String(50), default="saved", index=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Feedback(Base):
    __tablename__ = "feedback"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id"), index=True)
    action: Mapped[str] = mapped_column(String(50), index=True)
    subtype: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class SourceQuery(Base):
    __tablename__ = "source_queries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query_text: Mapped[str] = mapped_column(String(255), index=True)
    source_type: Mapped[str] = mapped_column(String(50), index=True)
    status: Mapped[str] = mapped_column(String(50), default="active", index=True)
    performance_stats_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class SourceQueryStat(Base):
    __tablename__ = "source_query_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(50), index=True)
    query_text: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(50), default="active", index=True)
    leads_generated: Mapped[int] = mapped_column(Integer, default=0)
    likes: Mapped[int] = mapped_column(Integer, default=0)
    saves: Mapped[int] = mapped_column(Integer, default=0)
    applies: Mapped[int] = mapped_column(Integer, default=0)
    dislikes: Mapped[int] = mapped_column(Integer, default=0)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    decision_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_evaluated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_promoted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_suppressed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class RecheckQueue(Base):
    __tablename__ = "recheck_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(50), index=True)
    entity_id: Mapped[int] = mapped_column(Integer, index=True)
    next_check_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(50), default="queued", index=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Investigation(Base):
    __tablename__ = "investigations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id"), unique=True, index=True)
    company_guess: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    role_guess: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String(50), default="open", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_check_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_type: Mapped[str] = mapped_column(String(50), index=True)
    value: Mapped[str] = mapped_column(String(255), index=True)
    source_reason: Mapped[str] = mapped_column(Text)
    confidence: Mapped[str] = mapped_column(String(50), default="medium")
    status: Mapped[str] = mapped_column(String(50), default="proposed", index=True)
    decision_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_evaluated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_promoted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_suppressed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class FollowUpTask(Base):
    __tablename__ = "follow_up_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(ForeignKey("applications.id"), index=True)
    task_type: Mapped[str] = mapped_column(String(50), index=True)
    due_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    status: Mapped[str] = mapped_column(String(50), default="open", index=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class AgentActivity(Base):
    __tablename__ = "agent_activities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_name: Mapped[str] = mapped_column(String(100), index=True)
    action: Mapped[str] = mapped_column(String(100), index=True)
    target_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    target_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    target_entity: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    result_summary: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class AgentRun(Base):
    __tablename__ = "agent_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_name: Mapped[str] = mapped_column(String(100), index=True)
    action: Mapped[str] = mapped_column(String(100), index=True)
    status: Mapped[str] = mapped_column(String(50), default="ok", index=True)
    summary: Mapped[str] = mapped_column(Text)
    affected_count: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class RuntimeControl(Base):
    __tablename__ = "runtime_control"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_state: Mapped[str] = mapped_column(String(50), default="paused", index=True)
    run_once_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    last_cycle_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_successful_cycle_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_cycle_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class ConnectorHealth(Base):
    __tablename__ = "connector_health"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    connector_name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(50), default="unknown", index=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    recent_successes: Mapped[int] = mapped_column(Integer, default=0)
    recent_failures: Mapped[int] = mapped_column(Integer, default=0)
    trust_score: Mapped[float] = mapped_column(Float, default=0.0)
    circuit_state: Mapped[str] = mapped_column(String(50), default="closed", index=True)
    disabled_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_failure_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_failure_classification: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    last_mode: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    last_item_count: Mapped[int] = mapped_column(Integer, default=0)
    quarantine_count: Mapped[int] = mapped_column(Integer, default=0)
    approved_for_unattended: Mapped[bool] = mapped_column(Boolean, default=False)
    last_freshness_lag_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class RunDigest(Base):
    __tablename__ = "run_digests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_run_id: Mapped[int] = mapped_column(ForeignKey("agent_runs.id"), unique=True, index=True)
    run_type: Mapped[str] = mapped_column(String(50), default="pipeline", index=True)
    summary: Mapped[str] = mapped_column(Text)
    new_leads_json: Mapped[list] = mapped_column(JSON, default=list)
    suppressed_leads_json: Mapped[list] = mapped_column(JSON, default=list)
    investigations_changed: Mapped[int] = mapped_column(Integer, default=0)
    follow_ups_created_json: Mapped[list] = mapped_column(JSON, default=list)
    watchlist_changes_json: Mapped[list] = mapped_column(JSON, default=list)
    failures_json: Mapped[list] = mapped_column(JSON, default=list)
    is_noop: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class DailyDigest(Base):
    __tablename__ = "daily_digests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    digest_date: Mapped[str] = mapped_column(String(20), unique=True, index=True)
    summary: Mapped[str] = mapped_column(Text)
    new_leads_json: Mapped[list] = mapped_column(JSON, default=list)
    suppressed_leads_json: Mapped[list] = mapped_column(JSON, default=list)
    investigations_changed: Mapped[int] = mapped_column(Integer, default=0)
    follow_ups_created_json: Mapped[list] = mapped_column(JSON, default=list)
    watchlist_changes_json: Mapped[list] = mapped_column(JSON, default=list)
    failures_json: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class AlertEvent(Base):
    __tablename__ = "alert_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_key: Mapped[str] = mapped_column(String(255), index=True)
    category: Mapped[str] = mapped_column(String(100), index=True)
    severity: Mapped[str] = mapped_column(String(50), default="warning", index=True)
    status: Mapped[str] = mapped_column(String(50), default="logged", index=True)
    summary: Mapped[str] = mapped_column(Text)
    details_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
