from __future__ import annotations

from datetime import datetime, timedelta, timezone

import requests

from core.config import get_settings
from core.logging import get_logger


logger = get_logger(__name__)

MOCK_ASHBY_JOBS = [
    {
        "id": "ashby-3001",
        "title": "Chief of Staff",
        "jobUrl": "https://jobs.ashbyhq.com/Granola/ashby-3001",
        "publishedDate": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
        "descriptionPlain": "Partner with the founders on strategic planning, recruiting coordination, and internal operating rhythm at an early-stage AI startup.",
        "location": {"location": "New York, NY"},
        "companyName": "Granola",
        "companyDomain": "granola.ai",
    },
    {
        "id": "ashby-3002",
        "title": "Deployment Strategist",
        "jobUrl": "https://jobs.ashbyhq.com/Mercor/ashby-3002",
        "publishedDate": (datetime.now(timezone.utc) - timedelta(hours=20)).isoformat(),
        "descriptionPlain": "Work with customers to deploy AI workflows and close the loop with product and engineering. Early-stage team.",
        "location": {"location": "Remote, US"},
        "companyName": "Mercor",
        "companyDomain": "mercor.com/ai",
    },
    {
        "id": "ashby-3003",
        "title": "Implementation Strategy Lead",
        "jobUrl": "https://jobs.ashbyhq.com/Vercel/ashby-3003",
        "publishedDate": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        "descriptionPlain": "Help enterprise users roll out developer tooling and partner with customer teams on complex deployments.",
        "location": {"location": "Remote, US"},
        "companyName": "Vercel",
        "companyDomain": "vercel.com/developer-tools",
    },
]


class AshbyConnector:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.last_error: str | None = None

    def fetch(
        self,
        require_live: bool = False,
        orgs_override: list[str] | None = None,
        discovery_queries: dict[str, list[str]] | None = None,
    ) -> tuple[list[dict], bool]:
        orgs = orgs_override or self.settings.ashby_orgs
        discovery_queries = discovery_queries or {}
        if orgs:
            try:
                self.last_error = None
                jobs = self._fetch_live(orgs)
                for job in jobs:
                    org = job.get("source_org_key")
                    if org and org in discovery_queries:
                        job["source_queries"] = list(dict.fromkeys(discovery_queries[org]))
                        job["discovery_source"] = "search_web"
                return jobs, True
            except Exception as exc:
                self.last_error = str(exc)
                logger.warning("Falling back to mock Ashby data: %s", exc)
                if require_live or not self.settings.demo_mode:
                    raise
        elif require_live or not self.settings.demo_mode:
            raise RuntimeError("No Ashby org keys configured for live mode.")
        self.last_error = self.last_error or "Live Ashby orgs unavailable; using demo listings."
        return MOCK_ASHBY_JOBS, False

    def _fetch_live(self, orgs: list[str]) -> list[dict]:
        jobs: list[dict] = []
        per_org_counts: dict[str, int] = {}
        empty_orgs: list[str] = []
        org_statuses: dict[str, str] = {}
        for org in orgs:
            normalized_org = _normalize_ashby_org_key(org)
            logger.info("[ASHBY_FETCH_REQUEST] %s", {"requested_org": org, "normalized_org": normalized_org})
            response = requests.post(
                "https://jobs.ashbyhq.com/api/non-user-graphql",
                json={
                    "operationName": "ApiJobBoardWithTeams",
                    "variables": {"organizationHostedJobsPageName": normalized_org},
                    "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobs { id title jobUrl publishedDate descriptionPlain location { location } } } }",
                },
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            job_board = payload.get("data", {}).get("jobBoard")
            if job_board is None:
                org_statuses[normalized_org] = "invalid_identifier"
                per_org_counts[normalized_org] = 0
                empty_orgs.append(normalized_org)
                continue
            org_jobs = job_board.get("jobs", []) or []
            per_org_counts[normalized_org] = len(org_jobs)
            if not org_jobs:
                org_statuses[normalized_org] = "valid_identifier_empty_jobs"
                empty_orgs.append(normalized_org)
            else:
                org_statuses[normalized_org] = "jobs_returned"
            for job in org_jobs:
                job["companyName"] = normalized_org.replace("-", " ").title()
                job["source_org_key"] = normalized_org
                jobs.append(job)
        logger.info(
            "[ASHBY_FETCH_RESULTS] %s",
            {"per_org_counts": per_org_counts, "empty_orgs": empty_orgs[:10], "org_statuses": org_statuses},
        )
        return jobs


def _normalize_ashby_org_key(value: str) -> str:
    parsed = requests.utils.urlparse(value)
    if parsed.netloc.endswith("ashbyhq.com"):
        path_parts = [part for part in parsed.path.split("/") if part]
        if path_parts:
            return path_parts[0]
    return value.strip().strip("/").lower()
