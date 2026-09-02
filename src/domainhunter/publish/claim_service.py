"""Human-approval gate for one-time ownership-claim tokens."""

from datetime import UTC, datetime, timedelta

from domainhunter.domain.claims import ClaimTokenRecord, build_claim_token
from domainhunter.storage.sqlite import SQLiteStore


class ClaimNotApproved(PermissionError):
    """Raised before a claim token can be created for an unapproved version."""


class ClaimService:
    """Issue a raw token once; SQLite retains only its hash."""

    def __init__(self, *, store: SQLiteStore) -> None:
        self._store = store

    def issue_approved_claim(
        self,
        candidate_id: str,
        candidate_version: int,
        *,
        created_at: datetime,
        expires_at: datetime,
    ) -> str:
        version = self._store.get_candidate_version(candidate_id, candidate_version)
        if version is None:
            raise ValueError("candidate version does not exist")
        if version.draft.author_kind != "human" or not self._store.is_version_approved(
            candidate_id, candidate_version
        ):
            raise ClaimNotApproved("only an approved human version may receive a claim token")
        raw_token, record = build_claim_token(
            candidate_id=candidate_id,
            created_at=created_at,
            expires_at=expires_at,
        )
        self._store.append_claim_token(record)
        return raw_token

    def issue_token(
        self,
        candidate_id: str,
        candidate_version: int,
        *,
        actor_id: str,
        ttl_days: int = 7,
        now: datetime | None = None,
    ) -> ClaimTokenRecord:
        """Mint a one-time Claim token, persist the hash, and return its record.

        The caller only sees the persisted :class:`ClaimTokenRecord` (hash only);
        the raw token bytes are deliberately not exposed here so the outreach
        trigger can keep its preview/audit surface minimal.
        """
        if not actor_id or not actor_id.strip():
            raise ValueError("actor_id must not be empty")
        if ttl_days <= 0:
            raise ValueError("ttl_days must be positive")
        if now is None:
            now = datetime.now(UTC)
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        expires_at = now + timedelta(days=ttl_days)
        version = self._store.get_candidate_version(candidate_id, candidate_version)
        if version is None:
            raise ValueError("candidate version does not exist")
        if version.draft.author_kind != "human" or not self._store.is_version_approved(
            candidate_id, candidate_version
        ):
            raise ClaimNotApproved(
                "only an approved human version may receive a claim token"
            )
        _raw_token, record = build_claim_token(
            candidate_id=candidate_id,
            created_at=now,
            expires_at=expires_at,
        )
        self._store.append_claim_token(record)
        return record
