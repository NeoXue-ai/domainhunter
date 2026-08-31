from datetime import UTC, datetime, timedelta

from webradar_v2.domain.claims import build_claim_token, hash_claim_token
from webradar_v2.storage.sqlite import SQLiteStore


NOW = datetime(2026, 8, 16, tzinfo=UTC)


def test_stores_only_a_claim_token_hash_and_redeems_it_once(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate = store.create_candidate("example.com", created_at=NOW)
    raw_token, record = build_claim_token(
        candidate_id=candidate.candidate_id,
        created_at=NOW,
        expires_at=NOW + timedelta(days=7),
    )

    store.append_claim_token(record)

    assert record.token_hash == hash_claim_token(raw_token)
    redeemed = store.redeem_claim_token(raw_token, redeemed_at=NOW)
    assert redeemed is not None
    assert redeemed.token_hash == record.token_hash
    assert redeemed.used_at == NOW
    assert store.redeem_claim_token(raw_token, redeemed_at=NOW) is None


def test_revoked_claim_tokens_cannot_be_redeemed(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "webradar.db")
    candidate = store.create_candidate("example.com", created_at=NOW)
    raw_token, record = build_claim_token(
        candidate_id=candidate.candidate_id,
        created_at=NOW,
        expires_at=NOW + timedelta(days=7),
    )
    store.append_claim_token(record)

    assert store.revoke_claim_token(raw_token, revoked_at=NOW) is True
    assert store.redeem_claim_token(raw_token, redeemed_at=NOW) is None
