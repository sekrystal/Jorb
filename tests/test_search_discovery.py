from __future__ import annotations

from connectors.search_web import (
    SearchDiscoveryResult,
    _extract_result_url,
    _is_supported_job_surface,
    _parse_search_results_from_html,
    _surface_acceptance_reason,
    build_search_queries,
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
