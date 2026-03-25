from __future__ import annotations

from types import SimpleNamespace

from connectors.search_web import (
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
    extract_ats_identifiers_from_html,
)


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
    assert not _is_supported_job_surface("https://www.linkedin.com/jobs/view/1")
    assert not _is_supported_job_surface("https://www.indeed.com/viewjob?jk=123")


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
