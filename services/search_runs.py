from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import get_settings
from core.models import SearchRun
from core.schemas import SearchRunResponse

if TYPE_CHECKING:
    from services.discovery_agents import AcquisitionWorkerExecution


def record_search_run(
    session: Session,
    execution: AcquisitionWorkerExecution,
    *,
    source_key: str = "search_web",
    provider: str | None = None,
) -> SearchRun:
    diagnostics = dict(execution.diagnostics or {})
    status = str(diagnostics.get("status") or ("results" if execution.results else "empty"))
    row = SearchRun(
        source_key=source_key,
        worker_name=execution.worker_name,
        provider=provider or get_settings().search_discovery_provider,
        status=status,
        live=bool(execution.live),
        zero_yield=(len(execution.query_texts) > 0 and len(execution.results) == 0 and status in {"empty", "zero_yield"}),
        query_count=len(execution.query_texts),
        result_count=len(execution.results),
        queries_json=list(execution.query_texts),
        failure_classification=(
            str(diagnostics.get("failure_classification"))
            if diagnostics.get("failure_classification") is not None
            else None
        ),
        error=str(diagnostics.get("error")) if diagnostics.get("error") is not None else None,
        diagnostics_json=diagnostics,
    )
    session.add(row)
    session.flush()
    return row


def list_recent_search_runs(session: Session, *, limit: int = 12) -> list[SearchRunResponse]:
    rows = session.scalars(select(SearchRun).order_by(SearchRun.created_at.desc(), SearchRun.id.desc()).limit(limit)).all()
    return [
        SearchRunResponse(
            id=row.id,
            source_key=row.source_key,
            worker_name=row.worker_name,
            provider=row.provider,
            status=row.status,
            live=row.live,
            zero_yield=row.zero_yield,
            query_count=row.query_count,
            result_count=row.result_count,
            queries=list(row.queries_json or []),
            failure_classification=row.failure_classification,
            error=row.error,
            diagnostics_json=dict(row.diagnostics_json or {}),
            created_at=row.created_at,
        )
        for row in rows
    ]


def get_latest_search_run(session: Session) -> SearchRunResponse | None:
    row = session.scalar(select(SearchRun).order_by(SearchRun.created_at.desc(), SearchRun.id.desc()).limit(1))
    if row is None:
        return None
    return SearchRunResponse(
        id=row.id,
        source_key=row.source_key,
        worker_name=row.worker_name,
        provider=row.provider,
        status=row.status,
        live=row.live,
        zero_yield=row.zero_yield,
        query_count=row.query_count,
        result_count=row.result_count,
        queries=list(row.queries_json or []),
        failure_classification=row.failure_classification,
        error=row.error,
        diagnostics_json=dict(row.diagnostics_json or {}),
        created_at=row.created_at,
    )
