# CT fixture suite

These JSON files are replayable CertStream/ct_log payloads used to exercise
`build_ct_events`, `extract_certificate_hostnames`, and the listener/poller
idempotency contract before any real network call.

Each fixture shares the same envelope:

```json
{
  "scenario": "<short name>",
  "description": "<human-readable purpose>",
  "expected_hostnames": ["..."],
  "expected_events_count": <int>,
  "expected_idempotent_on_replay": <bool>,
  "entries": [
    {
      "source_event_id": "argon:...",
      "observed_at": "2026-08-10T00:00:00Z",
      "certificate": { "leaf_cert": { "subject": {"CN": "..."}, "all_domains": [...], "issuer": "..." } }
    }
  ]
}
```

## Scenarios

| File | Purpose | Expected hostnames | Replay behaviour |
| --- | --- | --- | --- |
| `renewal.json` | Same domain appears at two timestamps | `example.com` | First append wins; replay adds 0 |
| `wildcard.json` | `CN=*.example.com` + SAN subdomains | `app.example.com`, `blog.example.com`, `example.com` | Each per-hostname event appends once |
| `duplicate.json` | Same `source_event_id` × 3 | `example.com` | Only 1 row inserted; 2 replays skipped |
| `idn.json` | Unicode + Punycode IDN | `xn--bcher-kva.example.com` | Both forms normalize to the same Punycode |
| `malformed.json` | Empty CN, IP-only SAN, label with spaces, empty source_event_id, plus one valid entry | `valid.example.com` | Only the valid row inserts; 4 entries skipped |

## Usage

Load a fixture in a test:

```python
import json
from pathlib import Path

fixture = json.loads((Path("tests/fixtures/ct/wildcard.json")).read_text())
for entry in fixture["entries"]:
    events = build_ct_events(entry["certificate"], entry["source_event_id"], entry["observed_at"])
    ...
```

For end-to-end poller tests, wrap the fixture in a `CTPage`:

```python
page = CTPage(
    entries=tuple(
        CTCertificate(
            source_event_id=entry["source_event_id"],
            certificate=entry["certificate"],
            observed_at=datetime.fromisoformat(entry["observed_at"].replace("Z", "+00:00")),
        )
        for entry in fixture["entries"]
    ),
    next_cursor=None,
)
```

## Why these scenarios

* **renewal** — proves cert reissuance doesn't multiply candidate identities.
* **wildcard** — proves the `*.` prefix is stripped before PSL normalization.
* **duplicate** — proves the idempotency_key constraint survives replay.
* **idn** — proves Unicode inputs encode to Punycode and collapse to one event.
* **malformed** — proves the listener skips junk without crashing or losing good data.