from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.db import get_db
from core.schemas import SearchRunResponse, SyncResult
from services.search_runs import get_latest_search_run
from services.sync import sync_all


router = APIRouter()


@router.get("/search-runs/latest", response_model=Optional[SearchRunResponse])
def latest_search_run(db: Session = Depends(get_db)) -> Optional[SearchRunResponse]:
    return get_latest_search_run(db)


@router.post("/search-runs/manual", response_model=SyncResult)
def run_manual_search(db: Session = Depends(get_db)) -> SyncResult:
    result = sync_all(db, include_rechecks=True)
    db.commit()
    return result
