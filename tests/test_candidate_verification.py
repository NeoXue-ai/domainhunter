from datetime import UTC, datetime

from domainhunter.domain.candidates import (
    CandidateOutcome,
    CandidateVersionDraft,
    Evidence,
    EvidenceType,
)
from domainhunter.domain.verification import CandidateVerification
from domainhunter.storage.sqlite import SQLiteStore


NOW = datetime(2026, 9, 3, tzinfo=UTC)


def _versioned_candidate(store: SQLiteStore):
    candidate = store.create_candidate("newsite.ai", created_at=NOW)
    version = store.append_candidate_version(
        candidate.candidate_id,
        CandidateVersionDraft(
            author_kind="rule",
            primary_outcome=CandidateOutcome.PUBLISHABLE_AI_SAAS,
            classification_confidence=0.8,
            name_suggestion="Newsite",
            description_suggestion="A new AI product",
            evidence=(
                Evidence(EvidenceType.TITLE, "Newsite", "https://newsite.ai/"),
            ),
        ),
        created_at=NOW,
    )
    return candidate, version


def _verification(candidate_id: str, version: int) -> CandidateVerification:
    return CandidateVerification(
        candidate_id=candidate_id,
        candidate_version=version,
        checked_at=NOW,
        ct_first_seen_at=NOW,
        rdap_tier="tier1",
        rdap_age_days=3,
        rdap_registration_at=NOW,
        dns_has_a=True,
        http_status_code=200,
        final_url="https://newsite.ai/",
        canonical_url="https://newsite.ai/",
        final_root_matches=True,
    )


def test_round_trips_candidate_verification(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "verification.db")
    candidate, version = _versioned_candidate(store)
    item = _verification(candidate.candidate_id, version.version)

    assert store.append_candidate_verification(item) is True
    assert store.get_candidate_verification(candidate.candidate_id, version.version) == item


def test_verification_is_idempotent_per_candidate_version(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "verification.db")
    candidate, version = _versioned_candidate(store)
    item = _verification(candidate.candidate_id, version.version)

    assert store.append_candidate_verification(item) is True
    assert store.append_candidate_verification(item) is False


def test_returns_ct_first_seen_time_for_a_known_domain(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "verification.db")
    store.mark_seen(("newsite.ai",), at=NOW, source="ct")

    assert store.get_ct_first_seen_at("newsite.ai") == NOW
