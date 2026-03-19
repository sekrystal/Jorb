from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core.schemas import ListingRecord
from services.freshness import validate_listing


def test_expired_listing_is_detected() -> None:
    listing = ListingRecord(
        company_name="DemoCo",
        title="Chief of Staff",
        url="https://example.com/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.now(timezone.utc) - timedelta(days=40),
        description_text="This position has been filled and is no longer accepting applications.",
    )
    validated = validate_listing(listing)
    assert validated.listing_status == "expired"
    assert validated.expiration_confidence > 0.9


def test_validate_listing_overrides_overly_optimistic_status() -> None:
    listing = ListingRecord(
        company_name="DemoCo",
        title="Chief of Staff",
        url="https://example.com/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.now(timezone.utc) - timedelta(days=2),
        description_text="This position has been filled and is no longer accepting applications.",
        listing_status="active",
    )
    validated = validate_listing(listing)
    assert validated.listing_status == "expired"
