"""One-time claim tokens: only a hash is suitable for persistent storage."""

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from secrets import token_urlsafe


def hash_claim_token(token: str) -> str:
    if not token or not token.strip():
        raise ValueError("claim token must not be empty")
    return sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ClaimTokenRecord:
    candidate_id: str
    token_hash: str
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None
    used_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.candidate_id or not self.token_hash:
            raise ValueError("candidate_id and token_hash must not be empty")
        if self.created_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("claim timestamps must be timezone-aware")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")


def build_claim_token(
    *, candidate_id: str, created_at: datetime, expires_at: datetime
) -> tuple[str, ClaimTokenRecord]:
    """Create a raw one-time token for delivery and a separate hash-only record."""
    raw_token = token_urlsafe(32)
    return raw_token, ClaimTokenRecord(
        candidate_id=candidate_id,
        token_hash=hash_claim_token(raw_token),
        created_at=created_at,
        expires_at=expires_at,
    )
