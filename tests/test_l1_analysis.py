from webradar_v2.crawler.l1_analysis import analyze_http_document
from webradar_v2.domain.observations import OutcomeCode


def test_extracts_product_metadata_from_successful_html() -> None:
    html = """
    <html>
      <head>
        <title>Example AI</title>
        <meta name="description" content="An AI writing assistant">
      </head>
      <body><h1>Write better content</h1><p>""" + ("Useful product text. " * 40) + "</p></body></html>"

    result = analyze_http_document(status_code=200, final_url="https://example.com", html=html)

    assert result.outcome_code is OutcomeCode.SUCCESS
    assert result.status_code == 200
    assert result.title == "Example AI"
    assert result.meta_description == "An AI writing assistant"
    assert result.text_length >= 500
    assert result.is_parking_page is False


def test_marks_short_successful_pages_as_content_insufficient() -> None:
    result = analyze_http_document(
        status_code=200,
        final_url="https://example.com",
        html="<title>Coming Soon</title><p>Launching shortly.</p>",
    )

    assert result.outcome_code is OutcomeCode.CONTENT_INSUFFICIENT


def test_marks_parking_pages_as_content_insufficient() -> None:
    html = "<title>Domain for sale</title><p>Domain for sale. Buy this premium domain.</p>"

    result = analyze_http_document(status_code=200, final_url="https://example.com", html=html)

    assert result.outcome_code is OutcomeCode.CONTENT_INSUFFICIENT
    assert result.is_parking_page is True


def test_classifies_http_errors_without_parsing_html() -> None:
    result = analyze_http_document(status_code=503, final_url="https://example.com", html="")

    assert result.outcome_code is OutcomeCode.HTTP_5XX
    assert result.status_code == 503
    assert result.title is None


def test_extracts_canonical_and_bounded_same_host_link_summary() -> None:
    html = """
    <link rel="canonical" href="/canonical">
    <a href="/pricing">Pricing</a>
    <a href="features">Features</a>
    <a href="https://external.example/pricing">External</a>
    <a href="#team">Fragment</a>
    <a href="mailto:team@example.com">Email</a>
    <a href="/pricing">Duplicate</a>
    """

    result = analyze_http_document(
        status_code=200,
        final_url="https://example.com/app/",
        html=html,
    )

    assert result.canonical_url == "https://example.com/canonical"
    assert result.internal_links == (
        "https://example.com/pricing",
        "https://example.com/app/features",
    )
