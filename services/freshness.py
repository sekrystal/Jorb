from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from core.schemas import ListingRecord


EXPIRED_PATTERNS = [
    "job no longer available",
    "position has been filled",
    "position filled",
    "page not found",
    "no longer accepting applications",
    "archived",
    "posting closed",
]


def has_expired_pattern(*parts: str | None) -> bool:
    haystack = " ".join(part or "" for part in parts).lower()
    return any(pattern in haystack for pattern in EXPIRED_PATTERNS)


def compute_freshness_days(posted_at: Optional[datetime]) -> Optional[int]:
    if not posted_at:
        return None
    current = datetime.now(timezone.utc)
    posted = posted_at if posted_at.tzinfo else posted_at.replace(tzinfo=timezone.utc)
    return max(int((current - posted).total_seconds() // 86400), 0)


def classify_freshness_label(freshness_days: Optional[int]) -> str:
    if freshness_days is None:
        return "unknown"
    if freshness_days <= 3:
        return "fresh"
    if freshness_days <= 14:
        return "recent"
    return "stale"


def validate_listing(record: ListingRecord) -> ListingRecord:
    text = f"{record.description_text or ''} {(record.metadata_json or {}).get('page_text', '')}".lower()
    freshness_days = compute_freshness_days(record.posted_at)
    listing_status = "active" if freshness_days is not None else "unknown"
    expiration_confidence = 0.05

    if has_expired_pattern(text):
        listing_status = "expired"
        expiration_confidence = 0.98

    if listing_status != "expired" and freshness_days is not None and freshness_days > 30:
        listing_status = "suspected_expired"
        expiration_confidence = 0.7

    if record.metadata_json.get("http_status") in {404, 410}:
        listing_status = "expired"
        expiration_confidence = 0.99

    record.freshness_days = freshness_days
    record.listing_status = listing_status
    record.expiration_confidence = max(record.expiration_confidence, expiration_confidence)
    return record
