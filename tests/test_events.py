from datetime import UTC, datetime

from webradar_v2.domain.events import SourceEvent


def test_same_source_and_event_id_have_same_idempotency_key() -> None:
    observed_at = datetime(2026, 8, 16, tzinfo=UTC)
    first = SourceEvent("ct_log", "argon:42", "www.example.com", observed_at)
    replay = SourceEvent("ct_log", "argon:42", "www.example.com", observed_at)

    assert first.idempotency_key == replay.idempotency_key


def test_different_sources_do_not_collide() -> None:
    observed_at = datetime(2026, 8, 16, tzinfo=UTC)
    ct = SourceEvent("ct_log", "42", "example.com", observed_at)
    github = SourceEvent("github", "42", "example.com", observed_at)

    assert ct.idempotency_key != github.idempotency_key


def test_source_event_carries_optional_issuer() -> None:
    observed_at = datetime(2026, 8, 16, tzinfo=UTC)
    event = SourceEvent(
        "ct_log",
        "argon:42",
        "example.com",
        observed_at,
        issuer="Let's Encrypt",
    )

    assert event.issuer == "Let's Encrypt"


def test_source_event_issuer_defaults_to_none() -> None:
    observed_at = datetime(2026, 8, 16, tzinfo=UTC)
    event = SourceEvent("ct_log", "argon:42", "example.com", observed_at)

    assert event.issuer is None


def test_idempotency_key_unchanged_when_issuer_differs() -> None:
    observed_at = datetime(2026, 8, 16, tzinfo=UTC)
    without_issuer = SourceEvent("ct_log", "argon:42", "example.com", observed_at)
    with_issuer = SourceEvent(
        "ct_log",
        "argon:42",
        "example.com",
        observed_at,
        issuer="Let's Encrypt",
    )

    assert without_issuer.idempotency_key == with_issuer.idempotency_key
