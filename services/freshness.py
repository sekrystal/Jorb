from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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

TRACKING_QUERY_PREFIXES = ("utm_", "gh_", "ashby_", "lever_", "trk", "ref")


def has_expired_pattern(*parts: str | None) -> bool:
    haystack = " ".join(part or "" for part in parts).lower()
    return any(pattern in haystack for pattern in EXPIRED_PATTERNS)


def compute_freshness_hours(posted_at: Optional[datetime]) -> Optional[float]:
    if not posted_at:
        return None
    current = datetime.now(timezone.utc)
    posted = posted_at if posted_at.tzinfo else posted_at.replace(tzinfo=timezone.utc)
    return round(max((current - posted).total_seconds() / 3600, 0.0), 2)


def compute_freshness_days(posted_at: Optional[datetime]) -> Optional[int]:
    freshness_hours = compute_freshness_hours(posted_at)
    if freshness_hours is None:
        return None
    return int(freshness_hours // 24)


def classify_freshness_label(freshness_days: Optional[int], freshness_hours: Optional[float] = None) -> str:
    if freshness_days is None and freshness_hours is None:
        return "unknown"
    if freshness_hours is None:
        freshness_hours = freshness_days * 24
    if freshness_hours <= 72:
        return "fresh"
    if freshness_hours <= 14 * 24:
        return "recent"
    return "stale"


def resolve_canonical_listing_url(url: str, source_type: str | None = None) -> str | None:
    raw_url = str(url or "").strip()
    if not raw_url:
        return None
    parsed = urlsplit(raw_url)
    scheme = (parsed.scheme or "").lower()
    host = (parsed.netloc or "").lower()
    if scheme not in {"http", "https"} or not host:
        return None

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"

    normalized_source = (source_type or "").lower()
    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)

    if normalized_source == "greenhouse" or "greenhouse.io" in host:
        if host == "boards.greenhouse.io":
            host = "job-boards.greenhouse.io"
        query = ""
    elif normalized_source in {"ashby", "yc_jobs"} or "ashbyhq.com" in host or "workatastartup.com" in host:
        query = ""
    else:
        query = urlencode(
            [(key, value) for key, value in query_pairs if not any(key.lower().startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES)]
        )

    return urlunsplit(("https", host, path, query, ""))


def listing_dedupe_key(record: ListingRecord) -> tuple[str, str, str, str]:
    metadata = dict(record.metadata_json or {})
    canonical_url = metadata.get("canonical_url") or record.url
    internal_job_id = str(metadata.get("internal_job_id") or "").strip().lower()
    return (
        str(record.source_type or "").strip().lower(),
        internal_job_id,
        str(canonical_url or "").strip().lower(),
        f"{str(record.company_name or '').strip().lower()}::{str(record.title or '').strip().lower()}",
    )


def validate_listing(record: ListingRecord) -> ListingRecord:
    text = f"{record.description_text or ''} {(record.metadata_json or {}).get('page_text', '')}".lower()
    freshness_hours = compute_freshness_hours(record.posted_at)
    freshness_days = compute_freshness_days(record.posted_at)
    listing_status = "active" if freshness_hours is not None else "unknown"
    expiration_confidence = 0.05

    if has_expired_pattern(text):
        listing_status = "expired"
        expiration_confidence = 0.98

    if listing_status != "expired" and freshness_hours is not None and freshness_hours > 30 * 24:
        listing_status = "suspected_expired"
        expiration_confidence = 0.7

    if record.metadata_json.get("http_status") in {404, 410}:
        listing_status = "expired"
        expiration_confidence = 0.99

    record.freshness_hours = freshness_hours
    record.freshness_days = freshness_days
    record.listing_status = listing_status
    record.expiration_confidence = max(record.expiration_confidence, expiration_confidence)
    return record


def verify_listing(record: ListingRecord) -> ListingRecord | None:
    canonical_url = resolve_canonical_listing_url(record.url, record.source_type)
    if canonical_url is None:
        return None

    metadata = dict(record.metadata_json or {})
    metadata["canonical_url"] = canonical_url
    record.url = canonical_url
    if record.canonical_job is not None:
        record.canonical_job.url = canonical_url
    metadata["canonical_job"] = record.canonical_job.model_dump() if record.canonical_job is not None else metadata.get("canonical_job")
    record.metadata_json = metadata
    record = validate_listing(record)

    verification = dict(record.metadata_json or {})
    verification["verification"] = {
        "canonical_url": canonical_url,
        "freshness_label": classify_freshness_label(record.freshness_days, record.freshness_hours),
        "listing_status": record.listing_status,
        "dead_link_detected": record.listing_status == "expired",
    }
    record.metadata_json = verification

    if record.listing_status == "expired":
        return None
    return record


def dedupe_listing_records(records: list[ListingRecord]) -> list[ListingRecord]:
    deduped: list[ListingRecord] = []
    seen: set[tuple[str, str, str, str]] = set()
    for record in records:
        key = listing_dedupe_key(record)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped
