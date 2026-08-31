"""Certificate Transparency event parsing without network or persistence."""

from datetime import datetime
from typing import Any, Mapping

from webradar_v2.domain.events import SourceEvent
from webradar_v2.domain.normalization import InvalidHostname, normalize_hostname


def _candidate_names(cert_data: Mapping[str, Any]) -> set[str]:
    leaf_cert = cert_data.get("leaf_cert")
    if not isinstance(leaf_cert, Mapping):
        return set()

    names: set[str] = set()
    subject = leaf_cert.get("subject")
    if isinstance(subject, Mapping) and isinstance(subject.get("CN"), str):
        names.add(subject["CN"])

    all_domains = leaf_cert.get("all_domains", [])
    if isinstance(all_domains, list):
        names.update(name for name in all_domains if isinstance(name, str))
    return names


def extract_certificate_hostnames(cert_data: Mapping[str, Any]) -> tuple[str, ...]:
    """Return sorted, unique, public hostnames found in CN and SAN fields."""
    hostnames: set[str] = set()
    for raw_name in _candidate_names(cert_data):
        name = raw_name[2:] if raw_name.startswith("*.") else raw_name
        try:
            hostnames.add(normalize_hostname(name).hostname)
        except InvalidHostname:
            continue
    return tuple(sorted(hostnames))


def extract_certificate_issuer(cert_data: Mapping[str, Any]) -> str | None:
    """Return the certificate issuer string when present, else None."""
    leaf_cert = cert_data.get("leaf_cert")
    if isinstance(leaf_cert, Mapping):
        issuer = _coerce_issuer(leaf_cert.get("issuer"))
        if issuer is not None:
            return issuer
    return _coerce_issuer(cert_data.get("issuer"))


def _coerce_issuer(value: object) -> str | None:
    """Return a non-empty issuer string from common certstream shapes."""
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, Mapping):
        for key in ("O", "CN", "DN"):
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
    return None


def build_ct_events(
    cert_data: Mapping[str, Any],
    source_event_id: str,
    observed_at: datetime,
    *,
    parser_version: str = "ct-v1",
) -> tuple[SourceEvent, ...]:
    """Convert one CT certificate update into one event per hostname."""
    if not source_event_id.strip():
        raise ValueError("source_event_id must not be empty")
    issuer = extract_certificate_issuer(cert_data)
    return tuple(
        SourceEvent(
            source="ct_log",
            source_event_id=f"{source_event_id}:{hostname}",
            raw_subject=hostname,
            observed_at=observed_at,
            evidence_summary="certificate_update",
            parser_version=parser_version,
            issuer=issuer,
        )
        for hostname in extract_certificate_hostnames(cert_data)
    )
