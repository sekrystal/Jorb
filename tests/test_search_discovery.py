from __future__ import annotations

from types import SimpleNamespace

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from connectors.search_web import (
    DirectJobExtractionResult,
    SearchDiscoveryConnector,
    SearchDiscoveryResult,
    _extract_result_url,
    _is_supported_job_surface,
    _parse_search_results_from_html,
    _rewrite_query_for_provider_failover,
    _surface_acceptance_reason,
    build_search_queries,
    classify_temporal_intelligence,
    classify_query_family,
    derive_search_results_from_extraction,
    extract_direct_listing_from_html,
    extract_ats_identifiers_from_html,
)
from core.config import Settings
from core.models import Base, SearchRun
from services.company_discovery import build_discovery_status
from services.discovery_agents import ats_resolver_worker, parser_acquisition_worker, search_acquisition_worker
from services.search_runs import record_search_run


def build_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def test_extract_ats_identifiers_from_careers_page_html() -> None:
    html = """
    <html>
      <head>
        <title>Acme Careers</title>
        <meta property="og:site_name" content="Acme AI" />
      </head>
      <body>
        <a href="https://job-boards.greenhouse.io/acme/jobs/123">View jobs</a>
        <a href="https://jobs.ashbyhq.com/acme/456">More jobs</a>
        <p>Remote US preferred</p>
      </body>
    </html>
    """

    extraction = extract_ats_identifiers_from_html(
        source_url="https://acme.ai/careers",
        html=html,
        final_url="https://acme.ai/careers",
    )

    assert extraction.company_name == "Acme AI"
    assert extraction.greenhouse_tokens == ["acme"]
    assert extraction.ashby_identifiers == ["acme"]
    assert "remote us" in extraction.geography_hints


def test_derive_search_results_from_extraction_creates_connector_ready_urls() -> None:
    html = '<a href="https://job-boards.greenhouse.io/acme/jobs/123">GH</a>'
    extraction = extract_ats_identifiers_from_html(
        source_url="https://acme.ai/careers",
        html=html,
        final_url="https://acme.ai/careers",
    )

    derived = derive_search_results_from_extraction("acme careers", extraction)

    assert derived
    assert isinstance(derived[0], SearchDiscoveryResult)
    assert derived[0].url == "https://job-boards.greenhouse.io/acme/jobs"


def test_parse_search_results_falls_back_when_result_markup_is_absent() -> None:
    html = """
    <html>
      <body>
        <a href="https://careers.acme.ai/open-roles">Open roles</a>
        <a href="https://job-boards.greenhouse.io/acme/jobs/123">Greenhouse</a>
        <a href="https://www.linkedin.com/jobs/view/1">Aggregator</a>
      </body>
    </html>
    """

    results, diagnostics = _parse_search_results_from_html("acme careers", html, set(), result_limit=5)

    assert diagnostics["reason"] == "fallback anchors accepted"
    assert [item.url for item in results] == [
        "https://careers.acme.ai/open-roles",
        "https://job-boards.greenhouse.io/acme/jobs/123",
    ]


def test_extract_result_url_decodes_duckduckgo_uddg_redirect() -> None:
    href = "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fjobs.ashbyhq.com%2Facme%2F123"
    assert _extract_result_url(href) == "https://jobs.ashbyhq.com/acme/123"


def test_supported_job_surface_accepts_careers_variants_and_blocks_aggregators() -> None:
    assert _is_supported_job_surface("https://careers.acme.ai/open-roles")
    assert _is_supported_job_surface("https://acme.ai/company/careers")
    assert _is_supported_job_surface("https://acme.ai/join")
    assert _is_supported_job_surface("https://job-boards.greenhouse.io/acme")
    assert _is_supported_job_surface("https://boards.greenhouse.io/acme")
    assert _is_supported_job_surface("https://job-boards.greenhouse.io/acme/jobs/123")
    assert _is_supported_job_surface("https://jobs.ashbyhq.com/acme")
    assert _is_supported_job_surface("https://www.workatastartup.com/jobs/12345")
    assert not _is_supported_job_surface("https://www.linkedin.com/jobs/view/1")
    assert not _is_supported_job_surface("https://www.indeed.com/viewjob?jk=123")


def test_extract_direct_listing_from_yc_jobs_html() -> None:
    html = """
    <html>
      <head>
        <title>Founding Operations Lead at Acme | Work at a Startup</title>
        <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "JobPosting",
            "title": "Founding Operations Lead",
            "datePosted": "2026-03-20T00:00:00Z",
            "description": "<p>Lead operating cadence and recruiting systems.</p>",
            "identifier": {"@type": "PropertyValue", "value": "12345"},
            "hiringOrganization": {"@type": "Organization", "name": "Acme"},
            "jobLocation": {
              "@type": "Place",
              "address": {
                "@type": "PostalAddress",
                "addressLocality": "San Francisco",
                "addressRegion": "CA",
                "addressCountry": "US"
              }
            },
            "url": "https://www.workatastartup.com/jobs/12345"
          }
        </script>
      </head>
    </html>
    """

    extraction = extract_direct_listing_from_html(
        "https://www.workatastartup.com/jobs/12345",
        html,
        final_url="https://www.workatastartup.com/jobs/12345",
    )

    assert isinstance(extraction, DirectJobExtractionResult)
    assert extraction is not None
    assert extraction.source_type == "yc_jobs"
    assert extraction.job_id == "12345"
    assert extraction.company_name == "Acme"
    assert extraction.title == "Founding Operations Lead"
    assert extraction.location == "San Francisco, CA, US"


def test_surface_acceptance_reason_rejects_duckduckgo_self_links() -> None:
    assert _surface_acceptance_reason("https://duckduckgo.com/") == "provider_self_link"
    assert _surface_acceptance_reason("https://duckduckgo.com/help") == "provider_self_link"


def test_parse_search_results_classifies_provider_self_links_only_as_provider_failure() -> None:
    html = """
    <html>
      <body>
        <a class="result__a" href="https://duckduckgo.com/help">Help</a>
        <a class="result__a" href="https://duckduckgo.com/?q=acme">Search</a>
      </body>
    </html>
    """

    results, diagnostics = _parse_search_results_from_html('site:job-boards.greenhouse.io "chief of staff"', html, set(), result_limit=5)

    assert results == []
    assert diagnostics["reason"] == "provider self-links only"
    assert diagnostics["rejected_reasons"] == ["provider_self_link", "provider_self_link"]


def test_rewrite_query_for_provider_failover_converts_provider_specific_probe_to_careers_query() -> None:
    assert _rewrite_query_for_provider_failover('site:job-boards.greenhouse.io "chief of staff"') == '"chief of staff" careers'
    assert _rewrite_query_for_provider_failover('"Acme" "chief of staff" greenhouse') == '"Acme" "chief of staff" careers'


def test_fetch_retries_once_with_rewritten_query_after_provider_self_link_only_results(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self, query: str, html: str) -> None:
            self.status_code = 200
            self.url = f"https://duckduckgo.com/html/?q={query}"
            self.text = html
            self.content = html.encode("utf-8")

        def raise_for_status(self) -> None:
            return None

    seen_queries: list[str] = []

    def fake_get(url: str, *, params: dict[str, str], **_kwargs) -> FakeResponse:
        query = params["q"]
        seen_queries.append(query)
        if len(seen_queries) == 1:
            return FakeResponse(
                query,
                """
                <html>
                  <body>
                    <a class="result__a" href="https://duckduckgo.com/help">Help</a>
                  </body>
                </html>
                """,
            )
        return FakeResponse(
            query,
            """
            <html>
              <body>
                <a class="result__a" href="https://careers.acme.ai/open-roles">Open roles</a>
              </body>
            </html>
            """,
        )

    monkeypatch.setattr(
        "connectors.search_web.get_settings",
        lambda: SimpleNamespace(
            search_discovery_enabled=True,
            search_discovery_query_limit=8,
            search_discovery_result_limit=5,
            demo_mode=True,
        ),
    )
    monkeypatch.setattr("connectors.search_web.requests.get", fake_get)

    connector = SearchDiscoveryConnector()

    results, live = connector.fetch(['site:job-boards.greenhouse.io "chief of staff"'])

    assert live is True
    assert seen_queries == ['site:job-boards.greenhouse.io "chief of staff"', '"chief of staff" careers']
    assert [item.url for item in results] == ["https://careers.acme.ai/open-roles"]
    assert connector.last_failure_classification is None
    assert connector.last_fetch_diagnostics["status"] == "results"
    assert connector.last_fetch_diagnostics["zero_yield_attempt_count"] == 1
    assert connector.last_fetch_diagnostics["fallback_order"] == [
        "provider_query",
        "provider_failover_rewrite",
        "scrape_parse_extraction",
    ]


def test_fetch_classifies_zero_yield_provider_failure_when_no_results_survive(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self, query: str) -> None:
            self.status_code = 200
            self.url = f"https://duckduckgo.com/html/?q={query}"
            self.text = """
            <html>
              <body>
                <a class="result__a" href="https://duckduckgo.com/help">Help</a>
              </body>
            </html>
            """
            self.content = self.text.encode("utf-8")

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        "connectors.search_web.get_settings",
        lambda: SimpleNamespace(
            search_discovery_enabled=True,
            search_discovery_query_limit=8,
            search_discovery_result_limit=5,
            demo_mode=True,
        ),
    )
    monkeypatch.setattr("connectors.search_web.requests.get", lambda _url, *, params, **_kwargs: FakeResponse(params["q"]))

    connector = SearchDiscoveryConnector()

    results, live = connector.fetch(['site:job-boards.greenhouse.io "chief of staff"'])

    assert live is True
    assert results == []
    assert connector.last_failure_classification == "search_provider_failure"
    assert connector.last_error == "Search discovery zero-yield: provider self-links only"
    assert connector.last_fetch_diagnostics["status"] == "empty"
    assert connector.last_fetch_diagnostics["failure_classification"] == "search_provider_failure"
    assert connector.last_fetch_diagnostics["zero_yield_queries"][0]["fallback_stage"] == "provider_query"


def test_fetch_classifies_timeout_without_raising_in_demo_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "connectors.search_web.get_settings",
        lambda: SimpleNamespace(
            search_discovery_enabled=True,
            search_discovery_query_limit=8,
            search_discovery_result_limit=5,
            demo_mode=True,
        ),
    )

    def fake_get(*_args, **_kwargs):
        raise requests.exceptions.Timeout("simulated timeout")

    monkeypatch.setattr("connectors.search_web.requests.get", fake_get)

    connector = SearchDiscoveryConnector()

    results, live = connector.fetch(['"chief of staff" careers'])

    assert live is True
    assert results == []
    assert connector.last_failure_classification == "search_timeout"
    assert connector.last_error == "Search discovery zero-yield: search request timed out"
    assert connector.last_fetch_diagnostics["status"] == "empty"
    assert connector.last_fetch_diagnostics["failure_classification"] == "search_timeout"


def test_build_search_queries_prefers_careers_mix_over_ats_direct_only() -> None:
    queries = build_search_queries(
        core_titles=["chief of staff"],
        adjacent_titles=["business operations lead"],
        preferred_domains=["ai"],
        watchlist_items=["Acme"],
        role_families=["operations"],
        boosted_titles=["founding operations"],
        recent_titles=["deployment strategist"],
    )

    assert any('"chief of staff" startup careers' in query for query in queries)
    assert any('"chief of staff" startup jobs' in query for query in queries)
    assert any('"Acme" "chief of staff" careers' in query for query in queries)
    ats_direct_count = sum(1 for query in queries if query.startswith("site:job-boards.greenhouse.io") or query.startswith("site:jobs.ashbyhq.com"))
    assert ats_direct_count < len(queries)


def test_acquisition_workers_split_ats_and_search_execution_with_candidate_urls() -> None:
    planner_plan = {
        "queries": ['"chief of staff" startup careers', '"business operations lead" startup careers'],
        "structured_query_plans": {
            "ats": [
                {"query_text": 'site:job-boards.greenhouse.io "chief of staff"', "executable": True},
                {"query_text": 'site:jobs.ashbyhq.com "chief of staff"', "executable": True},
            ],
            "search": [],
            "weak_signal": [],
        },
    }

    def fake_fetch(query_texts: list[str]) -> tuple[list[SearchDiscoveryResult], bool]:
        if query_texts and query_texts[0].startswith("site:"):
            return (
                [
                    SearchDiscoveryResult(
                        query_text=query_texts[0],
                        title="Chief of Staff - Example",
                        url="https://job-boards.greenhouse.io/example/jobs/123",
                    )
                ],
                True,
            )
        return (
            [
                SearchDiscoveryResult(
                    query_text=query_texts[0],
                    title="Example Careers",
                    url="https://careers.example.com/jobs",
                )
            ],
            True,
        )

    ats_execution = ats_resolver_worker(
        planner_plan,
        settings=Settings(discovery_max_search_queries_per_cycle=4),
        fetcher=fake_fetch,
    )
    search_execution = search_acquisition_worker(
        planner_plan,
        settings=Settings(discovery_max_search_queries_per_cycle=4),
        fetcher=fake_fetch,
    )

    assert ats_execution.worker_name == "ats_resolver"
    assert ats_execution.results[0].url == "https://job-boards.greenhouse.io/example/jobs/123"
    assert ats_execution.summary()["candidate_urls"] == ["https://job-boards.greenhouse.io/example/jobs/123"]
    assert search_execution.worker_name == "search"
    assert search_execution.results[0].url == "https://careers.example.com/jobs"
    assert search_execution.summary()["candidate_urls"] == ["https://careers.example.com/jobs"]


def test_parser_acquisition_worker_extracts_job_links_from_careers_page(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.discovery_agents.fetch_page_snapshot",
        lambda _url: (
            "https://example.com/careers",
            """
            <html>
              <head><title>Example Careers</title></head>
              <body>
                <a href="https://job-boards.greenhouse.io/example/jobs/123">Open roles</a>
              </body>
            </html>
            """,
        ),
    )
    monkeypatch.setattr("services.discovery_agents.interpret_discovery_page_with_ai", lambda _context: None)

    execution = parser_acquisition_worker(
        [
            SearchDiscoveryResult(
                query_text='"chief of staff" startup careers',
                title="Example Careers",
                url="https://example.com/careers",
            )
        ],
        settings=Settings(discovery_max_pages_to_crawl_per_cycle=2),
    )

    assert execution.worker_name == "parser"
    assert execution.diagnostics["pages_crawled"] == 1
    assert execution.derived_results is not None
    assert execution.derived_results[0].url == "https://job-boards.greenhouse.io/example/jobs"


def test_record_search_run_persists_observable_runtime_object() -> None:
    session = build_session()
    planner_plan = {
        "queries": ['"chief of staff" startup careers'],
        "structured_query_plans": {
            "ats": [],
            "search": [],
            "weak_signal": [],
        },
    }

    execution = search_acquisition_worker(
        planner_plan,
        settings=Settings(discovery_max_search_queries_per_cycle=4),
        fetcher=lambda query_texts: (
            [
                SearchDiscoveryResult(
                    query_text=query_texts[0],
                    title="Example Careers",
                    url="https://careers.example.com/jobs",
                )
            ],
            True,
        ),
    )

    row = record_search_run(session, execution, provider="duckduckgo_html")
    status = build_discovery_status(session)

    persisted = session.get(SearchRun, row.id)
    assert persisted is not None
    assert persisted.worker_name == "search"
    assert persisted.status == "results"
    assert persisted.query_count == 1
    assert persisted.result_count == 1
    assert persisted.queries_json == ['"chief of staff" startup careers']
    assert status.recent_search_runs[0].id == row.id
    assert status.recent_search_runs[0].worker_name == "search"
    assert status.recent_search_runs[0].queries == ['"chief of staff" startup careers']


def test_classify_query_family_captures_existing_query_mix() -> None:
    assert classify_query_family('site:job-boards.greenhouse.io "chief of staff"') == "ats_direct"
    assert classify_query_family('"Acme" "chief of staff" careers') == "company_targeted"
    assert classify_query_family('"chief of staff" company careers') == "careers_broad"
    assert classify_query_family('"ai" startup jobs "chief of staff"') == "role_market"


def test_classify_temporal_intelligence_marks_fresh_active_listing() -> None:
    metrics = classify_temporal_intelligence(
        title="Chief of Staff",
        text="Recently posted strategic operator role.",
        freshness_hours=18,
        freshness_days=0,
        listing_status="active",
    )

    assert metrics["freshness_label"] == "fresh"
    assert metrics["is_fresh"] is True
    assert metrics["is_stale"] is False
    assert metrics["evergreen_likelihood"] == "low"


def test_classify_temporal_intelligence_marks_stale_listing_from_age_and_status() -> None:
    metrics = classify_temporal_intelligence(
        title="Operations Lead",
        text="Older role that may no longer be active.",
        freshness_days=31,
        listing_status="suspected_expired",
    )

    assert metrics["freshness_label"] == "stale"
    assert metrics["is_stale"] is True
    assert "listing status is suspected_expired" in metrics["staleness_reasons"]


def test_classify_temporal_intelligence_marks_evergreen_listing_from_copy_and_age() -> None:
    metrics = classify_temporal_intelligence(
        title="General Application",
        text="We are always hiring exceptional operators for future opportunities.",
        freshness_days=60,
        listing_status="active",
    )

    assert metrics["freshness_label"] == "stale"
    assert metrics["evergreen_likelihood"] == "high"
    assert "always hiring" in metrics["evergreen_signals"]
