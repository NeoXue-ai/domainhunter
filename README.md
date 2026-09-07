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
git clone https://github.com/NeoXue-ai/domainhunter.git
cd domainhunter
python3 -m venv .venv
.venv/bin/pip install -e .          # Windows: .venv\Scripts\pip install -e .
.venv/bin/python -m pytest -q       # test suite
```

> macOS + Python 3.14 only: `site.py` skips hidden `.pth` files, which breaks
> editable installs. Run `bash dev_install.sh` instead of plain pip — it does
> the same install plus the fix. Linux and Windows never need this.

Foreground running is the same everywhere: start the command, it runs until
`Ctrl-C`. The next section is only for keeping it alive when you are away.

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

Loopback-only unless `--host 0.0.0.0`. The API has **no authentication** —
if you bind a remote server, keep port 8000 behind a firewall allowlist or
reach it through an SSH tunnel (`ssh -L 8000:127.0.0.1:8000 server`).

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

## Run it 24/7

A background service is only needed so the daemon survives logouts, crashes,
and reboots. Cursor persistence is durable: after any restart it resumes from
where it stopped and never re-scans.

### Linux — systemd

```ini
# /etc/systemd/system/domainhunter.service
[Unit]
After=network-online.target

[Service]
ExecStart=/opt/domainhunter/.venv/bin/domainhunter discover --database /var/lib/domainhunter/dh.db
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now domainhunter    # start + auto-start on boot
sudo systemctl stop domainhunter            # the off switch
journalctl -u domainhunter -f               # logs
```

### macOS — launchd

Save as `~/Library/LaunchAgents/ai.neoxue.domainhunter.plist` (absolute paths
required):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>ai.neoxue.domainhunter</string>
  <key>ProgramArguments</key><array>
    <string>/Users/YOU/domainhunter/.venv/bin/domainhunter</string>
    <string>discover</string>
    <string>--database</string>
    <string>/Users/YOU/domainhunter/dh.db</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/YOU/domainhunter/discover.log</string>
  <key>StandardErrorPath</key><string>/Users/YOU/domainhunter/discover.err.log</string>
</dict></plist>
```

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/ai.neoxue.domainhunter.plist  # on
launchctl bootout gui/$(id -u)/ai.neoxue.domainhunter                                 # off
```

Laptop caveat: closing the lid sleeps the Mac and pauses discovery; disable
auto-sleep if you need true 24h coverage.

### Windows — Task Scheduler (built-in) or NSSM

```bat
schtasks /Create /TN DomainHunter /SC ONSTART /RU SYSTEM ^
  /TR "C:\domainhunter\.venv\Scripts\domainhunter.exe discover --database C:\domainhunter\db\dh.db"
```

For restart-on-failure, open `taskschd.msc` and set "restart on failure" in
the task's settings — or install [NSSM](https://nssm.cc) for a real Windows
service with `nssm install DomainHunter <exe> <args>` and `nssm start/stop`
as the switch.

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
