from datetime import UTC, datetime

import pytest

from webradar_v2.ingest.ct_events import build_ct_events, extract_certificate_hostnames


OBSERVED_AT = datetime(2026, 8, 16, tzinfo=UTC)


def test_extracts_unique_cn_and_san_hostnames() -> None:
    cert_data = {
        "leaf_cert": {
            "subject": {"CN": "Example.COM"},
            "all_domains": ["*.example.com", "app.example.com", "Example.COM"],
        }
    }

    assert extract_certificate_hostnames(cert_data) == (
        "app.example.com",
        "example.com",
    )


def test_ignores_invalid_certificate_names() -> None:
    cert_data = {
        "leaf_cert": {
            "subject": {"CN": "localhost"},
            "all_domains": ["127.0.0.1", "bad host", "valid.example.com"],
        }
    }

    assert extract_certificate_hostnames(cert_data) == ("valid.example.com",)


def test_builds_one_idempotent_event_per_hostname() -> None:
    cert_data = {
        "leaf_cert": {
            "subject": {"CN": "example.com"},
            "all_domains": ["app.example.com"],
        }
    }

    events = build_ct_events(cert_data, "argon:42", OBSERVED_AT)

    assert [event.raw_subject for event in events] == ["app.example.com", "example.com"]
    assert events[0].source == "ct_log"
    assert events[0].source_event_id == "argon:42:app.example.com"
    assert events[0].observed_at == OBSERVED_AT


def test_empty_certificate_produces_no_events() -> None:
    assert build_ct_events({}, "argon:42", OBSERVED_AT) == ()


def test_requires_a_stable_source_event_id() -> None:
    cert_data = {"leaf_cert": {"subject": {"CN": "example.com"}}}

    with pytest.raises(ValueError, match="source_event_id must not be empty"):
        build_ct_events(cert_data, "", OBSERVED_AT)


def test_extracts_issuer_from_leaf_cert_issuer_string() -> None:
    cert_data = {
        "leaf_cert": {
            "subject": {"CN": "example.com"},
            "all_domains": ["example.com"],
            "issuer": "Let's Encrypt",
        }
    }

    events = build_ct_events(cert_data, "argon:42", OBSERVED_AT)

    assert all(event.issuer == "Let's Encrypt" for event in events)


def test_extracts_issuer_from_leaf_cert_issuer_mapping() -> None:
    cert_data = {
        "leaf_cert": {
            "subject": {"CN": "example.com"},
            "all_domains": ["example.com"],
            "issuer": {"O": "Let's Encrypt", "CN": "R3"},
        }
    }

    events = build_ct_events(cert_data, "argon:42", OBSERVED_AT)

    assert all(event.issuer == "Let's Encrypt" for event in events)


def test_issuer_defaults_to_none_when_missing() -> None:
    cert_data = {
        "leaf_cert": {
            "subject": {"CN": "example.com"},
            "all_domains": ["example.com"],
        }
    }

    events = build_ct_events(cert_data, "argon:42", OBSERVED_AT)

    assert all(event.issuer is None for event in events)
