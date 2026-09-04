# DomainHunter

Find newborn domains the moment they first appear in Certificate
Transparency logs.

DomainHunter watches configured public CT logs, keeps a first-seen
baseline of every domain it has ever observed, and runs a five-stage
filter funnel — static name signals, RDAP registration age, DNS
presence, live HTTP probing, and LLM classification — so a single
reviewer gets a short queue of *actually new* domains that look like
real products, each with a full evidence chain.

Small, opinionated, dependency-light: pure HTTP, no WebSocket, no
third-party aggregator, no API keys. Any OpenAI-compatible LLM works.

## How it works

```
CT logs (one or more configured RFC 6962 logs)
  │  get-sth / get-entries / tiles
  ▼
first-seen baseline          ct_seen_domains — a domain appears for
  │                          the first time in configured sources → early signal
  ▼
S1  static signals           domain-name shape score (brand-like vs
  │                          random-string spam), tech-TLD boost,
  │                          bulk-registration fingerprint
  ▼
S2  RDAP registration age    ≤30d → tier 1 · ≤90d → tier 2 · older → drop
  │                          (free RDAP, no key; unknown → drop in strict paths)
  ▼
S3  DNS presence             does the domain resolve at all?
  ▼
S4  live probe               L1 HTTP probe with strict SSRF guard,
  │                          canonical URL + evidence quotes
  ▼
S5  LLM classification       is it a real, running AI SaaS? fixed
  │                          schema, evidence-restricted, any provider
  ▼
review queue                 keyboard-driven human review console
```

Every stage's output is kept: `S1` score, `S2` age verdict, `S3` DNS,
`S4` probe outcome, `S5` LLM draft. The review console shows *why* a
candidate is in the queue.

## Quickstart

```bash
python3 -m venv .venv
bash dev_install.sh                 # editable install + macOS .pth shim
.venv/bin/python -m pytest -q       # full deterministic test suite
```

### One-shot funnel

```bash
# S1→S3 only (no network probing):
domainhunter filter --input domains.json

# S1→S4: filter + live HTTP probe, candidates persisted to SQLite:
domainhunter filter-probe --database ./domainhunter.db --input domains.json

# S1→S5: full funnel with LLM classification (MiniMax, DeepSeek, ...):
domainhunter filter-enrich \
  --database ./domainhunter.db \
  --input domains.json \
  --provider openai-compatible \
  --base-url "https://api.minimax.chat" \
  --token "$MINIMAX_TOKEN" \
  --model "MiniMax-M3"

# Only process domains never seen before (first-seen baseline):
domainhunter filter-enrich --database ./domainhunter.db \
  --input domains.json --fresh-only --provider mock
```

### Long-running discovery daemon

```bash
domainhunter discover \
  --database ./domainhunter.db \
  --log cloudflare-nimbus2026=https://ct.cloudflare.com/logs/nimbus2026 \
  --catchup 300 --page-size 300 --round-seconds 120 \
  --provider openai-compatible --base-url "..." --token "$TOKEN" --model "..."
```

Every round uses the built-in RFC 6962 adapter, a persisted source cursor,
the strict first-seen/RDAP/DNS/final-root gates, and optional S5 LLM
enrichment. It has no separate `ct_moniteur` installation step. Repeat
`--log` to poll more than one source; the default is Cloudflare Nimbus 2026.
Runs forever until Ctrl-C. With `--max-rounds`, a source failure is returned
as a non-zero exit and the final JSON includes `failed_rounds` and
`last_error` rather than pretending that it found zero candidates.

### 候选收件箱

```bash
domainhunter serve --database ./domainhunter.db --host 127.0.0.1 --port 8000
# open http://127.0.0.1:8000/
```

首页是中文候选收件箱，而不是监控大盘：它只显示尚未人工决定、并已按审核优先级排序的候选。点击候选会进入独立详情页，查看持久化的新网站、可访问性和产品证据后，再批准、暂缓、拒绝或拉黑。审核动作需要审核人 ID，并会记录到 SQLite。

点击“扫描新网站”会发起一次真实的严格 CT 扫描。界面只显示服务端返回的结果，且明确区分三种状态：

- `completed`：本次扫描产生了可审核候选；
- `queued`：本次有待核验根域仍在队列中，下一次扫描会继续处理它们；
- `no_candidates`：扫描完成，但没有候选通过严格规则；
- 部分来源失败：健康来源仍会继续处理，界面会显示失败来源，而不会把它伪装成零候选；
- 失败：请求或上游扫描失败，界面会显示可复制的错误摘要，不会伪装成零候选。

旧的 `/discovery` 和 `/ops` 链接会重定向到收件箱。

## First-seen: the core idea

CT logs are append-only: a domain's first appearance in the configured
sources for this database is a useful early-presence signal. Newly registered
domains typically get their first certificate within hours to days of
registration, but a CT sighting alone is not proof of registration age.
Renewals and older domains are rejected by the durable `ct_seen_domains`
baseline when present, then by RDAP registration-age checks.

RDAP registration age then provides the decisive newness check: a
registrable domain that is ≤30 days old and was never seen by this collector
before is a strong newborn candidate, rather than an old domain that bought a
fresh certificate.

## LLM providers

Any OpenAI-compatible `/v1/chat/completions` endpoint works — the
classifier is provider-agnostic and the output schema is fixed and
evidence-restricted.

| Provider | base_url | model |
|---|---|---|
| MiniMax | `https://api.minimax.chat` | `MiniMax-M3` |
| DeepSeek | `https://api.deepseek.com` | `deepseek-chat` |
| OpenAI | `https://api.openai.com` | `gpt-4o-mini` |
| Ollama (local) | `http://localhost:11434` | `llama3` |
| vLLM / llama.cpp | your server | any |

Reasoning models (MiniMax-M3, DeepSeek-R1) are handled: thinking
preambles are stripped, JSON is extracted from fenced or tail blocks,
and `base_url` is normalized whether or not it ends in `/v1`.

## CLI surface

```bash
domainhunter init --database ./domainhunter.db
domainhunter status --database ./domainhunter.db
domainhunter filter --input domains.json
domainhunter filter-probe --database ./domainhunter.db --input domains.json
domainhunter filter-enrich --database ./domainhunter.db --input domains.json --provider openai-compatible ...
domainhunter discover --database ./domainhunter.db
domainhunter serve --database ./domainhunter.db --host 127.0.0.1 --port 8000
domainhunter poll-ct-log --database ./domainhunter.db --max-probes 20
domainhunter probe-due --database ./domainhunter.db --limit 10
```

## HTTP API

The local API is intentionally small. All endpoints are loopback-only
unless you pass `--host 0.0.0.0`.

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/review-queue` | 仅待审核候选的收件箱投影，含持久化证据状态与优先级 |
| `GET` | `/v1/candidates/{id}/review-context` | 独立详情页使用的最新候选版本与证据上下文 |
| `GET` | `/v1/candidates/{id}/versions/{v}/review-context` | 指定不可变版本的证据上下文 |
| `POST` | `/v1/candidates/{id}/versions/{v}/decisions` | Idempotent on `request_id`; requires `X-Actor-ID` header |
| `POST` | `/v1/candidates/{id}/versions/{v}/outreach` | Dry-run by default; emits redacted contact page |
| `GET` | `/v1/discovery/overview` | Source-event counts + recent candidates |
| `POST` | `/v1/run/discovery` | Strict CT pass; reports `completed` / `queued` / `no_candidates`, pending work, and partial-source errors |
| `GET` | `/v1/metrics` | Local funnel snapshot (read-only) |
| `GET` | `/v1/analytics` | Signal→candidate conversion, latency percentiles |
| `GET` | `/v1/alerts` | Budget-exhausted and runbook hints |
| `GET` | `/healthz` | `{"ok": true}` |

## Project layout

```
src/domainhunter/
  api.py        — FastAPI routes + 候选收件箱和独立证据详情页
  cli.py        — `domainhunter` command-line entry point
  crawler/      — bounded L3 same-host crawler
  domain/       — PSL-aware hostname normalization, evidence, candidate model
  filter/       — S1 static signals, S2 RDAP age, S3 DNS, funnel pipeline,
                  S5 batch enrichment (enrich.py)
  ingest/       — RFC 6962 CT log adapter
  llm/          — fixed-schema LLM output validator (taxonomy-versioned),
                  provider-agnostic OpenAI-compatible adapter
  pipeline.py   — L1 → L2 → L3 orchestrator with retry budget
  publish/      — AIKnows draft-sync client + audit log
  scheduler/    — discovery daemon, lag monitor, work leases, alerts
  storage/      — append-only SQLite adapter + first-seen baseline
tests/          — deterministic unit tests + CT fixtures
docs/           — design specs, filter-layer plan, CT poller backlog
```

## Non-goals

- No production authentication, no public binding configuration.
- No automatic email. Outreach always requires an explicit human
  approval per recipient.
- No vendor-specific LLM provider is wired in; the classifier is a
  fixed-schema, evidence-restricted adapter that works with any
  OpenAI-compatible endpoint.
- No automatic publication. Approved human versions become an
  *outbound draft request* only — never a public post.

## License

[MIT](LICENSE).
