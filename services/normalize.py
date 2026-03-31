from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from core.schemas import ListingRecord
from services.job_content import clean_job_content
from services.location_policy import classify_location_scope


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _build_listing_record(
    *,
    company_name: str | None,
    company_domain: str | None,
    careers_url: str | None,
    title: str | None,
    location: str | None,
    url: str | None,
    source_type: str,
    posted_at: datetime | None,
    first_published_at: datetime | None,
    description_text: str | None,
    metadata_json: dict,
) -> ListingRecord:
    return ListingRecord(
        company_name=company_name or "Unknown Company",
        company_domain=company_domain,
        careers_url=careers_url,
        title=(title or "").strip(),
        location=location,
        url=url or "",
        source_type=source_type,
        posted_at=posted_at,
        first_published_at=first_published_at,
        last_seen_at=datetime.now(timezone.utc),
        description_text=description_text,
        metadata_json=metadata_json,
    )


def normalize_greenhouse_job(job: dict) -> ListingRecord:
    content = job.get("content", "") or ""
    location = (job.get("location") or {}).get("name")
    location_classification = classify_location_scope(location)
    first_published_at = _parse_datetime(job.get("first_published"))
    created_at = _parse_datetime(job.get("created_at"))
    updated_at = _parse_datetime(job.get("updated_at"))
    cleaned_content = clean_job_content(
        source_type="greenhouse",
        raw_text=content,
        raw_html=content,
        page_text=job.get("page_text", ""),
    )
    return _build_listing_record(
        company_name=job.get("company_name"),
        company_domain=job.get("company_domain"),
        careers_url=job.get("absolute_url"),
        title=job.get("title"),
        location=location,
        url=job.get("absolute_url") or job.get("url"),
        source_type="greenhouse",
        posted_at=first_published_at or created_at or updated_at,
        first_published_at=first_published_at,
        description_text=cleaned_content["canonical_text"],
        metadata_json={
            "provider": "greenhouse",
            "page_text": cleaned_content["plain_text"],
            "raw_page_text": job.get("page_text", ""),
            "description_sections": cleaned_content["sections"],
            "description_summary": cleaned_content["summary"],
            "description_source_format": cleaned_content["source_format"],
            "source_queries": job.get("source_queries", []),
            "discovery_source": job.get("discovery_source"),
            "surface_provenance": job.get("surface_provenance"),
            "source_lineage": job.get("source_lineage"),
            "company_domain": job.get("company_domain"),
            "source_board_token": job.get("source_board_token"),
            "internal_job_id": job.get("internal_job_id") or job.get("id"),
            "live_quality": job.get("live_quality", "unknown"),
            "source_updated_at": job.get("updated_at"),
            "source_created_at": job.get("created_at"),
            "location_scope": location_classification["scope"],
            "location_reason": location_classification["reason"],
        },
    )


def normalize_ashby_job(job: dict, org_name: Optional[str] = None) -> ListingRecord:
    description = job.get("descriptionPlain") or job.get("descriptionHtml") or ""
    location = None
    if job.get("location"):
        location = job["location"].get("location") or job["location"].get("name")
    location_classification = classify_location_scope(location)
    published_at = _parse_datetime(job.get("publishedDate"))
    updated_at = _parse_datetime(job.get("updatedAt"))
    cleaned_content = clean_job_content(
        source_type="ashby",
        raw_text=job.get("descriptionPlain") or description,
        raw_html=job.get("descriptionHtml"),
        page_text=job.get("page_text", ""),
    )

    return _build_listing_record(
        company_name=job.get("companyName") or org_name,
        company_domain=job.get("companyDomain"),
        careers_url=job.get("jobUrl") or job.get("applyUrl"),
        title=job.get("title"),
        location=location,
        url=job.get("jobUrl") or job.get("applyUrl"),
        source_type="ashby",
        posted_at=published_at or updated_at,
        first_published_at=published_at,
        description_text=cleaned_content["canonical_text"],
        metadata_json={
            "provider": "ashby",
            "page_text": cleaned_content["plain_text"],
            "raw_page_text": job.get("page_text", ""),
            "description_sections": cleaned_content["sections"],
            "description_summary": cleaned_content["summary"],
            "description_source_format": cleaned_content["source_format"],
            "source_queries": job.get("source_queries", []),
            "discovery_source": job.get("discovery_source"),
            "surface_provenance": job.get("surface_provenance"),
            "source_lineage": job.get("source_lineage"),
            "company_domain": job.get("companyDomain"),
            "source_org_key": job.get("source_org_key"),
            "internal_job_id": job.get("id"),
            "source_updated_at": job.get("updatedAt"),
            "location_scope": location_classification["scope"],
            "location_reason": location_classification["reason"],
        },
    )


def normalize_yc_job(job: dict) -> ListingRecord:
    location = job.get("location")
    location_classification = classify_location_scope(location)
    posted_at = _parse_datetime(job.get("posted_at"))
    cleaned_content = clean_job_content(
        source_type="yc_jobs",
        raw_text=job.get("description_text"),
        raw_html=job.get("description_html"),
        page_text=job.get("page_text", ""),
    )

    return _build_listing_record(
        company_name=job.get("company_name"),
        company_domain=job.get("company_domain"),
        careers_url=job.get("source_url") or job.get("url"),
        title=job.get("title"),
        location=location,
        url=job.get("url"),
        source_type="yc_jobs",
        posted_at=posted_at,
        first_published_at=posted_at,
        description_text=cleaned_content["canonical_text"],
        metadata_json={
            "provider": "yc_jobs",
            "page_text": cleaned_content["plain_text"],
            "raw_page_text": job.get("page_text", ""),
            "description_sections": cleaned_content["sections"],
            "description_summary": cleaned_content["summary"],
            "description_source_format": cleaned_content["source_format"],
            "source_queries": job.get("source_queries", []),
            "discovery_source": job.get("discovery_source"),
            "surface_provenance": job.get("surface_provenance"),
            "source_lineage": job.get("source_lineage"),
            "company_domain": job.get("company_domain"),
            "source_job_id": job.get("source_job_id"),
            "internal_job_id": job.get("source_job_id"),
            "apply_url": job.get("apply_url"),
            "source_url": job.get("source_url"),
            "location_scope": location_classification["scope"],
            "location_reason": location_classification["reason"],
        },
    )
