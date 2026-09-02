from domainhunter.domain.contacts import extract_public_contacts


def test_extracts_unique_public_contact_addresses_and_redacts_page_text() -> None:
    result = extract_public_contacts(
        "<p>Contact founders@example.com or <a href='mailto:sales@example.com'>sales</a>.</p>",
        source_url="https://example.com/contact",
    )

    assert [contact.address for contact in result.contacts] == [
        "founders@example.com",
        "sales@example.com",
    ]
    assert result.contacts[0].redacted_address == "f*******@example.com"
    assert "founders@example.com" not in result.redacted_html
    assert "sales@example.com" not in result.redacted_html
