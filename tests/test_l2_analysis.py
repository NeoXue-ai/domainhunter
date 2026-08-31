from webradar_v2.crawler.l2_analysis import extract_rendered_facts


def test_extracts_headings_ctas_and_pricing_registration_evidence() -> None:
    html = """
    <h1>Automate your AI workflows</h1>
    <h2>Built for operations teams</h2>
    <button>Start free trial</button>
    <a href="/pricing">View pricing</a>
    <section>Plans start at $29 per month and include unlimited projects.</section>
    <script>ignore this embedded content</script>
    """

    facts = extract_rendered_facts(html)

    assert facts.headings == (
        "Automate your AI workflows",
        "Built for operations teams",
    )
    assert facts.ctas == ("Start free trial", "View pricing")
    assert "Plans start at $29" in facts.pricing_evidence
    assert "Start free trial" in facts.registration_evidence
    assert "ignore this" not in facts.dom_excerpt
