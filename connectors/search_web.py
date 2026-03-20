from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
from typing import Iterable, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse
import re

import requests

from core.config import get_settings
from core.logging import get_logger


logger = get_logger(__name__)
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
RESULT_LINK_RE = re.compile(
    r'<a[^>]+class="result__a"[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")
HREF_RE = re.compile(r'href=["\'](?P<href>[^"\']+)["\']', re.IGNORECASE)
TITLE_RE = re.compile(r"<title>(?P<title>.*?)</title>", re.IGNORECASE | re.DOTALL)
GREENHOUSE_TOKEN_RE = re.compile(
    r"(?:job-boards|boards)\.greenhouse\.io/(?P<token>[A-Za-z0-9_-]+)(?:/jobs(?:/|$))?",
    re.IGNORECASE,
)
GREENHOUSE_API_TOKEN_RE = re.compile(
    r"boards-api\.greenhouse\.io/v1/boards/(?P<token>[A-Za-z0-9_-]+)/jobs",
    re.IGNORECASE,
)
ASHBY_IDENTIFIER_RE = re.compile(
    r"jobs\.ashbyhq\.com/(?P<org>[A-Za-z0-9._-]+)(?:/|$)",
    re.IGNORECASE,
)
ASHBY_HOSTED_NAME_RE = re.compile(
    r'organizationHostedJobsPageName["\']?\s*:\s*["\'](?P<org>[A-Za-z0-9._-]+)["\']',
    re.IGNORECASE,
)
COMPANY_NAME_RE = re.compile(
    r"<meta[^>]+property=[\"']og:site_name[\"'][^>]+content=[\"'](?P<name>[^\"']+)[\"']",
    re.IGNORECASE,
)
DDG_ZERO_YIELD_MARKERS = [
    "detected unusual traffic",
    "automated requests",
    "verify you are human",
    "captcha",
    "anomaly detected",
    "unusual activity",
]
BLOCKED_AGGREGATOR_HOSTS = ["linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com", "wellfound.com"]
PROVIDER_OWNED_HOSTS = {"duckduckgo.com", "www.duckduckgo.com"}


@dataclass
class SearchDiscoveryResult:
    query_text: str
    title: str
    url: str
    source_surface: str = "duckduckgo_html"


@dataclass
class ATSExtractionResult:
    source_url: str
    final_url: str
    page_title: str
    company_name: Optional[str] = None
    careers_url: Optional[str] = None
    ats_type: str = "unknown"
    greenhouse_tokens: list[str] = field(default_factory=list)
    ashby_identifiers: list[str] = field(default_factory=list)
    discovered_urls: list[str] = field(default_factory=list)
    geography_hints: list[str] = field(default_factory=list)
    confidence: float = 0.0
    via_openai: bool = False


class SearchDiscoveryConnector:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.last_error: str | None = None

    def fetch(self, queries: list[str], require_live: bool = False) -> tuple[list[SearchDiscoveryResult], bool]:
        if self.settings.search_discovery_enabled and queries:
            try:
                self.last_error = None
                return self._fetch_live(queries), True
            except Exception as exc:
                self.last_error = str(exc)
                logger.warning("Search discovery failed: %s", exc)
                if require_live or not self.settings.demo_mode:
                    raise
        elif require_live or not self.settings.demo_mode:
            raise RuntimeError("Search discovery is disabled or has no queries configured.")
        self.last_error = self.last_error or "Search discovery disabled; no web search performed."
        return [], False

    def _fetch_live(self, queries: list[str]) -> list[SearchDiscoveryResult]:
        results: list[SearchDiscoveryResult] = []
        seen_urls: set[str] = set()
        zero_yield_queries: list[dict[str, object]] = []
        for query_text in queries[: self.settings.search_discovery_query_limit]:
            response = requests.get(
                "https://duckduckgo.com/html/",
                params={"q": query_text},
                timeout=(5, 20),
                headers={"User-Agent": BROWSER_USER_AGENT},
                allow_redirects=True,
            )
            response.raise_for_status()
            html = response.text
            block_markers = [marker for marker in DDG_ZERO_YIELD_MARKERS if marker in html.lower()]
            strict_matches = list(RESULT_LINK_RE.finditer(html))
            fallback_candidates = _extract_fallback_anchor_candidates(html)
            logger.info(
                "[SEARCH_PROVIDER_RESPONSE] %s",
                {
                    "query": query_text,
                    "status_code": response.status_code,
                    "final_url": response.url,
                    "response_bytes": len(response.content or b""),
                    "block_markers": block_markers,
                },
            )
            query_results, diagnostics = _parse_search_results_from_html(
                query_text,
                html,
                seen_urls,
                result_limit=self.settings.search_discovery_result_limit,
            )
            logger.info(
                "[SEARCH_PROVIDER_PARSE] %s",
                {
                    "query": query_text,
                    "strict_match_count": len(strict_matches),
                    "fallback_anchor_candidate_count": len(fallback_candidates),
                    "accepted_result_count": len(query_results),
                },
            )
            if diagnostics.get("candidate_urls"):
                logger.info(
                    "[SEARCH_PROVIDER_CANDIDATE_URLS] %s",
                    {
                        "query": query_text,
                        "candidate_urls": diagnostics["candidate_urls"][:10],
                    },
                )
            if diagnostics.get("accepted_urls"):
                logger.info(
                    "[SEARCH_PROVIDER_ACCEPTED_URLS] %s",
                    {
                        "query": query_text,
                        "accepted_urls": diagnostics["accepted_urls"][:10],
                        "accepted_reasons": diagnostics["accepted_reasons"][:10],
                    },
                )
            if diagnostics.get("rejected_urls"):
                logger.info(
                    "[SEARCH_PROVIDER_REJECTED_URLS] %s",
                    {
                        "query": query_text,
                        "rejected_urls": diagnostics["rejected_urls"][:10],
                        "rejected_reasons": diagnostics["rejected_reasons"][:10],
                    },
                )
            if not query_results:
                zero_yield = {
                    "query": query_text,
                    "status_code": response.status_code,
                    "final_url": response.url,
                    "response_bytes": len(response.content or b""),
                    "strict_match_count": len(strict_matches),
                    "fallback_anchor_candidate_count": len(fallback_candidates),
                    "block_markers": block_markers,
                    "reason": diagnostics["reason"],
                }
                zero_yield_queries.append(zero_yield)
                logger.warning("[SEARCH_PROVIDER_ZERO_RESULTS] %s", zero_yield)
            for item in query_results:
                results.append(item)
                seen_urls.add(item.url)
        if not results:
            reason = zero_yield_queries[0]["reason"] if zero_yield_queries else "search provider returned no accepted results"
            self.last_error = f"Search discovery zero-yield: {reason}"
        return results


def _clean_html(value: str) -> str:
    return unescape(TAG_RE.sub("", value or "").strip())


def _extract_result_url(href: str) -> str | None:
    if not href:
        return None
    parsed = urlparse(href)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        uddg = parse_qs(parsed.query).get("uddg", [])
        if uddg:
            return unquote(uddg[0])
    if parsed.path.startswith("/l/"):
        uddg = parse_qs(parsed.query).get("uddg", [])
        if uddg:
            return unquote(uddg[0])
    if parsed.scheme and parsed.netloc:
        return href
    return None


def _is_supported_job_surface(url: str) -> bool:
    return _surface_acceptance_reason(url).startswith("accepted_")


def _surface_acceptance_reason(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if not parsed.scheme or not host:
        return "missing_host"
    if parsed.scheme not in {"http", "https"}:
        return "non_http_url"
    if host in PROVIDER_OWNED_HOSTS:
        return "provider_self_link"
    if any(blocked in host for blocked in BLOCKED_AGGREGATOR_HOSTS):
        return "aggregator_blocked"
    path_parts = [part for part in path.split("/") if part]
    if "job-boards.greenhouse.io" in host or "boards.greenhouse.io" in host:
        if len(path_parts) >= 1:
            if any(part == "jobs" for part in path_parts):
                return "accepted_greenhouse_job"
            return "accepted_greenhouse_root"
        return "unsupported_surface"
    if "jobs.ashbyhq.com" in host:
        if len(path_parts) >= 1:
            if len(path_parts) >= 2:
                return "accepted_ashby_job"
            return "accepted_ashby_root"
        return "unsupported_surface"
    if host.startswith("careers."):
        return "accepted_careers_page"
    if any(token in path for token in ["/careers", "/jobs", "/join-us", "/work-with-us", "/open-roles", "/join", "/company/careers"]):
        return "accepted_careers_page"
    return "unsupported_surface"


def _extract_fallback_anchor_candidates(html: str) -> list[str]:
    candidates: list[str] = []
    for match in HREF_RE.finditer(html):
        href = _extract_result_url(match.group("href"))
        if not href:
            continue
        candidates.append(href)
    return candidates


def _fallback_title_from_url(url: str) -> str:
    parsed = urlparse(url)
    slug = parsed.path.strip("/").split("/")[-1] if parsed.path.strip("/") else parsed.netloc
    slug = slug.replace("-", " ").replace("_", " ").strip() or parsed.netloc
    return slug.title()


def _parse_search_results_from_html(
    query_text: str,
    html: str,
    seen_urls: set[str],
    *,
    result_limit: int,
) -> tuple[list[SearchDiscoveryResult], dict[str, str]]:
    accepted: list[SearchDiscoveryResult] = []
    candidate_urls: list[str] = []
    accepted_urls: list[str] = []
    accepted_reasons: list[str] = []
    rejected_urls: list[str] = []
    rejected_reasons: list[str] = []
    for match in RESULT_LINK_RE.finditer(html):
        href = _extract_result_url(match.group("href"))
        title = _clean_html(match.group("title"))
        if href:
            candidate_urls.append(href)
        reason = _surface_acceptance_reason(href) if href else "missing_host"
        if not href or href in seen_urls or not reason.startswith("accepted_"):
            if href and href not in seen_urls:
                rejected_urls.append(href)
                rejected_reasons.append(reason)
            continue
        accepted.append(SearchDiscoveryResult(query_text=query_text, title=title or _fallback_title_from_url(href), url=href))
        accepted_urls.append(href)
        accepted_reasons.append(reason)
        if len(accepted) >= result_limit:
            return accepted, {
                "reason": "strict matches accepted",
                "candidate_urls": candidate_urls,
                "accepted_urls": accepted_urls,
                "accepted_reasons": accepted_reasons,
                "rejected_urls": rejected_urls,
                "rejected_reasons": rejected_reasons,
            }

    for href in _extract_fallback_anchor_candidates(html):
        candidate_urls.append(href)
        reason = _surface_acceptance_reason(href)
        if href in seen_urls or not reason.startswith("accepted_"):
            if href not in seen_urls:
                rejected_urls.append(href)
                rejected_reasons.append(reason)
            continue
        accepted.append(SearchDiscoveryResult(query_text=query_text, title=_fallback_title_from_url(href), url=href))
        accepted_urls.append(href)
        accepted_reasons.append(reason)
        if len(accepted) >= result_limit:
            return accepted, {
                "reason": "fallback anchors accepted",
                "candidate_urls": candidate_urls,
                "accepted_urls": accepted_urls,
                "accepted_reasons": accepted_reasons,
                "rejected_urls": rejected_urls,
                "rejected_reasons": rejected_reasons,
            }
    if accepted:
        return accepted, {
            "reason": "fallback anchors accepted",
            "candidate_urls": candidate_urls,
            "accepted_urls": accepted_urls,
            "accepted_reasons": accepted_reasons,
            "rejected_urls": rejected_urls,
            "rejected_reasons": rejected_reasons,
        }

    if RESULT_LINK_RE.search(html):
        return accepted, {
            "reason": "strict matches found but none were accepted",
            "candidate_urls": candidate_urls,
            "accepted_urls": accepted_urls,
            "accepted_reasons": accepted_reasons,
            "rejected_urls": rejected_urls,
            "rejected_reasons": rejected_reasons,
        }
    if _extract_fallback_anchor_candidates(html):
        return accepted, {
            "reason": "fallback anchors found but none were accepted",
            "candidate_urls": candidate_urls,
            "accepted_urls": accepted_urls,
            "accepted_reasons": accepted_reasons,
            "rejected_urls": rejected_urls,
            "rejected_reasons": rejected_reasons,
        }
    return accepted, {
        "reason": "no parseable anchors detected",
        "candidate_urls": candidate_urls,
        "accepted_urls": accepted_urls,
        "accepted_reasons": accepted_reasons,
        "rejected_urls": rejected_urls,
        "rejected_reasons": rejected_reasons,
    }


def fetch_page_snapshot(url: str, timeout: tuple[int, int] = (5, 15)) -> tuple[str, str]:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": BROWSER_USER_AGENT},
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.url, response.text[:250000]


def extract_ats_identifiers_from_html(
    source_url: str,
    html: str,
    final_url: Optional[str] = None,
    *,
    ai_interpretation: Optional[dict] = None,
) -> ATSExtractionResult:
    normalized_url = final_url or source_url
    lowered = html.lower()
    page_title_match = TITLE_RE.search(html)
    page_title = _clean_html(page_title_match.group("title")) if page_title_match else ""
    company_meta = COMPANY_NAME_RE.search(html)
    company_name = _clean_html(company_meta.group("name")) if company_meta else None

    greenhouse_tokens = {
        match.group("token")
        for pattern in (GREENHOUSE_TOKEN_RE, GREENHOUSE_API_TOKEN_RE)
        for match in pattern.finditer(html)
    }
    ashby_identifiers = {
        match.group("org")
        for pattern in (ASHBY_IDENTIFIER_RE, ASHBY_HOSTED_NAME_RE)
        for match in pattern.finditer(html)
    }

    discovered_urls: list[str] = []
    for href_match in HREF_RE.finditer(html):
        href = href_match.group("href")
        absolute = urljoin(normalized_url, href)
        if not absolute.startswith("http"):
            continue
        if _is_supported_job_surface(absolute):
            discovered_urls.append(absolute)
        for match in GREENHOUSE_TOKEN_RE.finditer(absolute):
            greenhouse_tokens.add(match.group("token"))
        for match in ASHBY_IDENTIFIER_RE.finditer(absolute):
            ashby_identifiers.add(match.group("org"))

    geography_hints = [
        token
        for token in ["remote us", "united states", "usa", "ireland", "london", "uk", "bangalore", "india", "singapore", "australia"]
        if token in lowered
    ]

    ats_type = "unknown"
    confidence = 0.0
    if greenhouse_tokens:
        ats_type = "greenhouse"
        confidence = 0.92
    elif ashby_identifiers:
        ats_type = "ashby"
        confidence = 0.92
    elif any(token in lowered for token in ["/careers", "careers", "join us", "work with us"]):
        ats_type = "careers_page"
        confidence = 0.45

    if ai_interpretation:
        ai_tokens = ai_interpretation.get("greenhouse_tokens") or []
        ai_ashby = ai_interpretation.get("ashby_identifiers") or []
        greenhouse_tokens.update(ai_tokens)
        ashby_identifiers.update(ai_ashby)
        if ai_interpretation.get("company_name") and not company_name:
            company_name = ai_interpretation["company_name"]
        if ai_interpretation.get("ats_type") in {"greenhouse", "ashby", "careers_page", "direct_listing"}:
            ats_type = ai_interpretation["ats_type"]
        confidence = max(confidence, float(ai_interpretation.get("confidence", 0.0) or 0.0))

    return ATSExtractionResult(
        source_url=source_url,
        final_url=normalized_url,
        page_title=page_title,
        company_name=company_name,
        careers_url=normalized_url if ats_type == "careers_page" else None,
        ats_type=ats_type,
        greenhouse_tokens=sorted(greenhouse_tokens),
        ashby_identifiers=sorted(ashby_identifiers),
        discovered_urls=list(dict.fromkeys(discovered_urls))[:20],
        geography_hints=geography_hints,
        confidence=round(confidence, 2),
        via_openai=bool(ai_interpretation),
    )


def derive_search_results_from_extraction(
    query_text: str,
    extraction: ATSExtractionResult,
    source_surface: str = "search_web_crawl",
) -> list[SearchDiscoveryResult]:
    results: list[SearchDiscoveryResult] = []
    title = extraction.page_title or extraction.company_name or extraction.final_url
    for token in extraction.greenhouse_tokens:
        results.append(
            SearchDiscoveryResult(
                query_text=query_text,
                title=f"{title} [greenhouse:{token}]",
                url=f"https://job-boards.greenhouse.io/{token}/jobs",
                source_surface=source_surface,
            )
        )
    for org in extraction.ashby_identifiers:
        results.append(
            SearchDiscoveryResult(
                query_text=query_text,
                title=f"{title} [ashby:{org}]",
                url=f"https://jobs.ashbyhq.com/{org}",
                source_surface=source_surface,
            )
        )
    return results


def build_search_queries(
    core_titles: Iterable[str],
    adjacent_titles: Iterable[str],
    preferred_domains: Iterable[str],
    watchlist_items: Iterable[str],
    role_families: Iterable[str] = (),
    boosted_titles: Iterable[str] = (),
    recent_titles: Iterable[str] = (),
) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    def add(query: str) -> None:
        query = query.strip()
        if not query or query in seen:
            return
        seen.add(query)
        queries.append(query)

    primary_titles = list(dict.fromkeys([*list(core_titles)[:3], *list(boosted_titles)[:2], *list(recent_titles)[:2]]))
    secondary_titles = list(dict.fromkeys([*list(adjacent_titles)[:3], *list(recent_titles)[:2]]))
    domain_themes = list(preferred_domains)[:3]
    companies = list(watchlist_items)[:4]

    for company in companies:
        for title in primary_titles[:2]:
            add(f'"{company}" "{title}" careers')
            add(f'"{company}" "{title}" startup careers')
            add(f'"{company}" "{title}" jobs')
            add(f'"{company}" "{title}" greenhouse')
            add(f'"{company}" "{title}" ashby')

    for domain in domain_themes:
        for title in primary_titles[:2]:
            add(f'"{domain}" startup careers "{title}"')
            add(f'"{domain}" startup jobs "{title}"')
            add(f'AI startup careers "{title}"')
            add(f'"{domain}" startup greenhouse "{title}"')
            add(f'"{domain}" startup ashby "{title}"')

    for title in primary_titles[:4]:
        add(f'"{title}" startup careers')
        add(f'"{title}" startup jobs')
        add(f'"{title}" remote us careers')
        add(f'"{title}" company careers')
        add(f'"{title}" startup greenhouse')
        add(f'"{title}" startup ashby')

    for title in secondary_titles[:4]:
        add(f'"{title}" startup careers')
        add(f'"{title}" remote us careers')
        add(f'"{title}" company careers')

    for family in list(role_families)[:3]:
        family_query = family.replace("_", " ")
        add(f'"{family_query}" startup careers')
        add(f'"{family_query}" startup jobs')
        add(f'"{family_query}" remote us careers')
        add(f'"{family_query}" startup greenhouse')
        add(f'"{family_query}" startup ashby')

    # Keep some ATS-direct probes, but make them a minority of the query mix.
    for title in primary_titles[:2]:
        add(f'site:job-boards.greenhouse.io "{title}"')
        add(f'site:jobs.ashbyhq.com "{title}"')
    return queries


def extract_discovered_greenhouse_tokens(results: list[SearchDiscoveryResult]) -> dict[str, list[str]]:
    discovered: dict[str, list[str]] = {}
    for result in results:
        parsed = urlparse(result.url)
        host = parsed.netloc.lower()
        path_parts = [part for part in parsed.path.split("/") if part]
        token = None
        if "job-boards.greenhouse.io" in host and len(path_parts) >= 2:
            token = path_parts[0]
        elif "boards.greenhouse.io" in host and len(path_parts) >= 2:
            token = path_parts[0]
        if token:
            discovered.setdefault(token, []).append(result.query_text)
    return discovered


def extract_discovered_ashby_orgs(results: list[SearchDiscoveryResult]) -> dict[str, list[str]]:
    discovered: dict[str, list[str]] = {}
    for result in results:
        parsed = urlparse(result.url)
        host = parsed.netloc.lower()
        path_parts = [part for part in parsed.path.split("/") if part]
        if "jobs.ashbyhq.com" not in host or not path_parts:
            continue
        org = path_parts[0]
        discovered.setdefault(org, []).append(result.query_text)
    return discovered
