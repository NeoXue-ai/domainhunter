# ct-candidate-radar

A small, opinionated tool that watches Certificate Transparency logs and GitHub
for newly-registered domains, decides which ones look like real AI tools, and
hands a tidy review queue to a human.

It is built for one specific loopback workflow: a single reviewer working
through the queue, approving, rejecting, or blocking candidates — with the
option of an outreach draft when something is approved. There is no auth,
no scheduler, and no public binding; everything is `127.0.0.1` by default.

## Why it exists

Certificate Transparency logs are public, append-only, and roughly real-time.
Every new TLS certificate appears within seconds of issuance. If you filter
for `*.example.com`-style hostnames with low exposure (no web archive hits,
no DNS records older than a week), you get a stream of "this domain just
appeared and nothing about it is indexed yet" — which is exactly the
fingerprint of a freshly-deployed product.

This tool turns that stream into a queue of cited candidates.

## What it does

- Polls CertStream (`wss://certstream.calidog.io/` or any compatible feed)
  and GitHub repository search for AI-tagged repos whose Homepage field
  is a fresh domain.
- For each new hostname: L1 HTTP probe → L2 Playwright render → L3
  same-host crawl. The pipeline is bounded and never phones home to
  a vendor — every page is fetched through a strict SSRF guard.
- Persists a cited candidate version (rule / LLM / human) and a
  review-priority snapshot. The priority formula is auditable: each of
  the four contributions is stored separately.
- Surfaces the queue in a local review console (see below), where one
  keyboard-driven reviewer can approve / reject / defer / blocklist.
- Approved versions can be exported as an outreach draft (dry-run
  produces a redacted contact page; real run requires explicit consent).
- Logs funnel analytics: signal→candidate conversion, latency P50/P95,
  per-stage budget consumption. Surfaces alerts when budgets run low.

## Review console

The console is split into four focused pages — each can breathe and
keep a focused aesthetic, instead of cramming everything onto one
screen:

| Route | Page | What's on it |
|---|---|---|
| `GET /` | **Queue** | KPI strip + candidate list + a slim "Run discovery" entry |
| `GET /review/{candidate_id}` | **Review** | 48px mono domain hero, evidence, 4-arc circular gauge, decision buttons |
| `GET /discovery` | **Discovery** | One-click CertStream run, max-probes input, recent domains |
| `GET /ops` | **Ops** | Analytics grid, alerts list, runbook links |

Keyboard shortcuts on `/review/<id>`: `A` approve · `R` reject ·
`D` defer · `B` blocklist · `E` edit · `O` outreach (dry) · `J`/`K`
and `←`/`→` paginate.

The design rationale for splitting the console is in
[`docs/design/review-console-redesign.md`](docs/design/review-console-redesign.md).

## Local quickstart

```bash
python3 -m venv .venv
bash scripts/dev_install.sh    # pip install -e . + a .pth shim for Python 3.14
.venv/bin/python -m pytest -q  # ~320 tests
.venv/bin/webradar serve --database ./webradar.db
# open http://127.0.0.1:8000/
```

> **Why the `.pth` shim?** Python 3.14 marks `pip install -e .`'s
> `__editable__…pth` as `UF_HIDDEN` on macOS, and `site.py` silently
> skips hidden files. The companion `webradar-v2.pth` is not hidden,
> so the package is importable without any `PYTHONPATH` override.

## CLI surface

```bash
webradar init --database ./webradar.db
webradar status --database ./webradar.db
webradar ingest-ct-page --database ./webradar.db --input ./ct-page.json
webradar ingest-github-page --database ./webradar.db --input ./github-page.json
webradar probe-due --database ./webradar.db --limit 10
webradar serve --database ./webradar.db --host 127.0.0.1 --port 8000
webradar listen-certstream --database ./webradar.db --max-messages 100
webradar poll-certstream-latest --database ./webradar.db --max-probes 20
webradar poll-github-api --database ./webradar.db --query 'topic:artificial-intelligence'
```

`probe-due` performs real HTTP requests against domains already
persisted by an ingestion command. It will not invent targets.

## CertStream feed durability

The default `wss://certstream.calidog.io/` is a public service that
frequently returns 502 and exposes only a 25-cert snapshot. For a
durable feed, run a self-hosted
[certstream-server-rust](https://github.com/reloading01/certstream-server-rust)
on loopback:

```bash
brew install reloading01/tap/certstream-server-rust
mkdir -p /tmp/cs-state
CERTSTREAM_HOST=127.0.0.1 CERTSTREAM_PORT=8080 \
  CERTSTREAM_CT_LOG_STATE_FILE=/tmp/cs-state/state.json \
  certstream-server-rust &

webradar listen-certstream --database ./webradar.db \
  --url ws://127.0.0.1:8080/ --max-messages 100
```

If the WebSocket stalls, fall back to the lightweight JSON snapshot:

```bash
webradar poll-certstream-latest --database ./webradar.db --max-probes 20
```

## HTTP API

The local API is intentionally small. All endpoints are loopback-only
unless you pass `--host 0.0.0.0`.

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/review-queue` | Queue projection with cited evidence + priority |
| `POST` | `/v1/candidates/{id}/versions/{v}/decisions` | Idempotent on `request_id`; requires `X-Actor-ID` header |
| `POST` | `/v1/candidates/{id}/versions/{v}/outreach` | Dry-run by default; emits redacted contact page |
| `GET` | `/v1/discovery/overview` | Source-event counts + recent candidates |
| `POST` | `/v1/run/discovery` | One CertStream `latest.json` pass end-to-end |
| `GET` | `/v1/metrics` | Local funnel snapshot (read-only) |
| `GET` | `/v1/analytics` | Signal→candidate conversion, latency percentiles |
| `GET` | `/v1/alerts` | Budget-exhausted and runbook hints |
| `GET` | `/healthz` | `{"ok": true}` |

## Project layout

```
src/webradar_v2/
  api.py        — FastAPI routes + the four-page review console
  cli.py        — `webradar` command-line entry point
  crawler/      — bounded L3 same-host crawler
  domain/       — PSL-aware hostname normalization, evidence, candidate model
  ingest/       — CertStream + GitHub adapters
  llm/          — fixed-schema LLM output validator (taxonomy-versioned)
  pipeline.py   — L1 → L2 → L3 orchestrator with retry budget
  publish/      — AIKnows draft-sync client + audit log
  scheduler/    — work-item leases, daily budget ledger, alerts
  storage/      — append-only SQLite adapter
tests/          — 320+ deterministic unit tests + CT/GitHub fixtures
docs/design/    — review console visual + IA spec
```

## Non-goals

- No production authentication, no public binding configuration.
- No automatic email. Outreach always requires an explicit human
  approval per recipient.
- No vendor-specific LLM provider is wired in; the validator is
  fixed-schema and rejects anything that doesn't already match
  collected evidence.
- No automatic publication. Approved human versions become an
  *outbound draft request* only — never a public post.

## License

[MIT](LICENSE).
