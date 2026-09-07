# DomainHunter

Find newborn domains the moment they first appear in Certificate
Transparency logs.

DomainHunter watches configured public CT logs, keeps a first-seen
baseline, and runs a five-stage funnel — static name signals, RDAP
registration age, DNS presence, live HTTP probing, and optional LLM
classification — so a single reviewer gets a short queue of *actually
new* domains that look like real products, each with a full evidence
chain.

Small and dependency-light: pure HTTP, no WebSocket, no third-party
aggregator, no API keys. Any OpenAI-compatible LLM works.

## How it works

```
CT logs (configured RFC 6962 logs, default Cloudflare Nimbus 2026)
  ▼
first-seen baseline     ct_seen_domains — first sighting is the signal
  ▼
S1  static signals      domain-name shape: brand-like vs random spam
  ▼
S2  RDAP age            ≤30d → tier 1 · ≤90d → tier 2 · older → drop
  ▼
S3  DNS presence        does the domain resolve at all?
  ▼
S4  live probe          bounded HTTP probe, SSRF-guarded, evidence quotes
  ▼
S5  LLM classify        optional, fixed schema, evidence-restricted
  ▼
review queue            human review inbox
```

Every stage's output is persisted, and the review console shows *why*
a candidate is in the queue. Failed probes are retried automatically
(`ct_discovery_work`) — no separate scheduler needed.

## Quickstart

```bash
python3 -m venv .venv
bash dev_install.sh                 # editable install + macOS .pth shim
.venv/bin/python -m pytest -q       # test suite
```

## Commands

There are four:

```bash
domainhunter init --database ./domainhunter.db        # create/open the DB

domainhunter discover --database ./domainhunter.db    # background discovery
domainhunter serve  --database ./domainhunter.db      # review inbox UI
domainhunter status --database ./domainhunter.db      # known + due domains
```

### Background discovery

```bash
domainhunter discover \
  --database ./domainhunter.db \
  --round-seconds 120 \
  --provider openai-compatible --base-url "https://api.deepseek.com" \
  --token-env DEEPSEEK_TOKEN --model "deepseek-chat"
```

One round = poll new CT entries → strict first-seen/RDAP/DNS gates →
bounded HTTP probe → candidates queued for review. Repeat `--log` to
poll more sources. Runs forever until Ctrl-C; with `--max-rounds` a
source failure exits non-zero instead of looking like "no candidates".

Drop the `--provider*` flags to run rules-only (S1-S4); attach any
OpenAI-compatible endpoint for S5 classification. Providers known to
work: DeepSeek (`deepseek-chat`), MiniMax (`MiniMax-M3`), OpenAI
(`gpt-4o-mini`), Ollama (`http://localhost:11434`), vLLM. Reasoning
models are handled (thinking preambles stripped, JSON extracted from
fenced or tail blocks, `base_url` normalized with or without `/v1`).

### Review inbox

```bash
domainhunter serve --database ./domainhunter.db
# open http://127.0.0.1:8000/
```

The home page is the candidate inbox: undecided candidates sorted by
review priority. Click into a candidate to see its persisted evidence,
then approve, defer, reject, or blocklist. Actions require an
`X-Actor-ID` and are recorded in SQLite. "扫描新网站" triggers one
strict CT pass via `POST /v1/run/discovery`.

## HTTP API

Loopback-only unless `--host 0.0.0.0`.

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/review-queue` | inbox projection with priorities |
| `GET` | `/v1/candidates/{id}/review-context` | latest version + evidence |
| `GET` | `/v1/candidates/{id}/versions/{v}/review-context` | immutable version |
| `POST` | `/v1/candidates/{id}/versions/{v}/decisions` | idempotent on `request_id` |
| `POST` | `/v1/candidates/{id}/versions/{v}/decisions/{d}/revoke` | undo a decision |
| `POST` | `/v1/run/discovery` | one strict CT pass |
| `GET` | `/v1/discovery/overview` | domains, cursor, funnel counts |
| `GET` | `/healthz` | `{"ok": true}` |

## Project layout

```
src/domainhunter/
  api.py        — FastAPI routes
  cli.py        — `domainhunter` entry point (init/status/discover/serve)
  pipeline.py   — S4 probe + retry policy + candidate creation
  crawler/      — HTTP probe, L1 content analysis, SSRF-safe transport
  filter/       — S1 static signals, S2 RDAP age, S3 DNS, funnel
  ingest/       — RFC 6962 CT log adapter + orchestrator
  llm/          — fixed-schema provider adapter (S5)
  scheduler/    — discovery daemon loop
  domain/       — PSL normalization, candidates, reviews, retry policy
  storage/      — append-only SQLite store
```

## Non-goals

- No production authentication; bind loopback by default.
- No automatic email or publication. Review decisions stay local.
- No vendor lock-in: the classifier speaks the OpenAI-compatible
  chat-completions dialect only.

## License

[MIT](LICENSE).
