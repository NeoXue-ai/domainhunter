# DomainHunter Signal Desk — Full Frontend Design

**Status:** superseded on 2026-09-03 by `2026-09-03-discovery-inbox-product-design.md`
**Date:** 2026-09-03
**Scope:** complete redesign of the local review console: Queue, Review, Discovery, and Ops
**Implementation surface:** `src/domainhunter/api.py` HTML, CSS, and client-side scripts; existing HTTP endpoints and scoring rules remain unchanged.

## 1. Product outcome

DomainHunter exists to surface genuinely new websites. Its interface must make the best verified candidate impossible to overlook while preserving the ability to audit every supporting signal.

The current console presents all queued results as visually equivalent, dense rows. In the verified Nimbus run this leaves one publishable candidate (`limitlessrouter.com`, score 0.71) almost indistinguishable from 36 lower-confidence leads. The redesign makes **decision readiness**, not raw record volume, the primary organising principle.

### Design principles

1. **Lead with the decision.** Show whether a candidate is ready, why, and what the reviewer should do next before lower-level metadata.
2. **Make evidence legible.** Translate source facts into a chronological, human-readable chain while retaining drill-down audit data.
3. **Separate product review from operations.** A reviewer should not have to parse runner controls to approve a candidate; an operator should not have to scan candidate cards to assess system health.
4. **Use density intentionally.** Queue and Review prioritise spacious comprehension. Ops preserves higher density for technical work.
5. **No cosmetic rewrite of rules.** No frontend presentation can upgrade a candidate, hide a strict rejection, or change score/newness semantics.

## 2. Shared application shell

### Navigation and hierarchy

- A sticky top bar contains the DomainHunter wordmark, environment label, navigation, current scan status, language toggle, and reviewer ID.
- Primary navigation: **Queue**, **Review**, **Discovery**, **Ops**. Queue is the default route. Review links to the current selected candidate, and is disabled only when no candidate is available.
- The shell uses one spacious content column, capped at a readable desktop width. Page-specific tools appear in a contextual header instead of the global navigation.
- On widths below 900px, navigation collapses to a compact menu while the current page title remains visible.

### Visual language

- Base: warm off-white canvas (`#F7F4ED`) with white surfaces, ink text (`#17202C`), muted slate metadata, and restrained hairlines.
- Primary action/interactive color: accessible blue (`#1769E0`). Positive/ready status: deep teal (`#007E72`). Warning/needs-review: amber. Rejected/error: accessible red.
- Typography: system sans for prose and labels; monospace only for domains, IDs, scores, timestamps, URLs, and commands. No small all-monospace body copy.
- Type scale: page titles 32–44px; candidate domains 28–38px; body 15–16px; metadata no smaller than 12px.
- Cards use 10–14px corners, a light border, and subtle shadow only when elevation communicates interaction. Pills are reserved for brief status labels.
- Every semantic color has a text label and icon/shape companion. Focus rings remain visible and meet contrast requirements.

### Persistent interaction conventions

- A `loading` skeleton occupies the same shape as the content it replaces, preventing jumps.
- Success, failure, and validation messages appear in an aria-live status region and remain long enough to read.
- Technical JSON and long raw records are hidden behind an explicit **View audit data** disclosure; they are never the default reading surface.
- Language choice remains client-side and preserves the existing Chinese/English content coverage.

## 3. Queue — prioritised discovery inbox

### Purpose

Answer one question in seconds: **what is the most credible new website worth reviewing now?**

### Layout

1. **Page header:** `Discovery queue`, a plain-language summary (for example, `1 ready to publish · 36 need review`), search, and filters.
2. **Readiness strip:** four calm count chips: Ready, Needs review, Rejected/held, and Total signals. This replaces the current six equally prominent KPI cards.
3. **Ready to publish:** shown only when qualifying candidates exist. The first candidate is a full-width feature card—not merely the first table row.
4. **Needs review:** lower-confidence candidates presented as compact cards/list rows with visible reason, confidence, freshness state, and review action.
5. **No-ready state:** when no candidate is ready, the top region says so plainly and directs the reviewer to the strongest lead or a new discovery run.

### Candidate cards

The feature card contains:

- domain/product name and canonical URL;
- verdict label such as `Publishable AI SaaS` or `Needs more proof`;
- confidence score with an explanatory label, not a decorative chart;
- three short proof points, such as `CT observed`, `RDAP confirms recent registration`, and `HTTP product page reachable`;
- a concise product description when available;
- one primary **Review candidate** action and one secondary **Open site** external link.

Lower-priority cards keep the same semantic fields but use a single-line evidence summary. Sorting defaults to review readiness, then confidence, then recency. The UI does not claim that a score is a chronological age; it always displays explicit evidence for newness.

### Filters and states

- Filters: readiness, outcome/type, minimum confidence, source, and free-text domain/product search.
- Active filters are readable removable chips and update a live result count.
- Empty search/filter state: explain that no current candidates match and provide a reset action.
- Network/API failure: retain the previous list where possible and offer retry; do not replace data with a blank page.

### Data contract

Uses the current `GET /v1/review-queue` response plus `GET /v1/metrics`. Client-side grouping is presentation-only. Existing candidate IDs, versions, detail routing, and scores are preserved.

## 4. Review — evidence-led decision workspace

### Purpose

Let a human confidently approve, reject, defer, or blocklist a single candidate without interpreting raw JSON.

### Layout

1. **Candidate masthead:** domain, product name, readiness verdict, confidence, candidate version, canonical URL, external open link, and previous/next navigation.
2. **Decision rail:** persistent on desktop and immediately after the masthead on mobile. Buttons are Approve, Reject, Defer, Blocklist, and Edit where currently supported. The destructive action carries confirmation copy.
3. **Evidence timeline:** a left-to-right or vertical sequence: CT observation → root-domain normalization → RDAP/newness result → DNS/HTTP reachability → product classification → review decision. Each step shows status, timestamp/source, and a one-sentence explanation.
4. **Why this verdict:** a concise score/evidence breakdown answering what passed, what remains uncertain, and which strict gate would prevent publication.
5. **Audit panel:** collapsible raw signals, decision history, version data, and API payloads for forensic review.

### Decision behaviour

- Reviewer ID is required before a mutating action, with clear inline validation.
- Existing decision endpoints remain authoritative. A successful action updates the current candidate state and safely advances to the next appropriate item without losing a clear completion message.
- Revoke/unpublish/outreach actions retain their existing confirmation and API behavior but are visually grouped as secondary or advanced actions.
- If another user/process changes a candidate, refresh the decision panel and report the conflict rather than silently overwriting state.

### Data contract

Uses the existing candidate decision, revoke, unpublish, and outreach endpoints. No decision payloads, policy tags, or audit fields change.

## 5. Discovery — understandable scans, not a control wall

### Purpose

Start a fresh real-data discovery run and understand its progress, source health, and outcome.

### Layout

1. **Run panel:** source selector, scan limits, and current strictness summary. Defaults are visible and explained in plain language.
2. **Primary action:** one prominent **Start discovery run** button, disabled while a run is active.
3. **Live progress:** a timeline/card sequence for submitted → collecting signals → normalizing roots → strict newness checks → HTTP validation → candidates queued.
4. **Run result:** total signals, distinct domains, probes, strict rejections, and candidates ready for review. A direct **Open queue** action appears when candidates are created.
5. **Source detail:** degraded/unavailable source status is visibly separated from the candidate result. It never masquerades as a successful zero-result run.

### States and errors

- Before the first run: explain what the scan will and will not prove.
- During a run: show elapsed time, active step, and cancel/leave-page guidance if cancellation is unsupported.
- On failure: show the affected source, a concise cause, retry action, and a disclosure for raw diagnostics.
- On zero candidates: distinguish `no fresh signals`, `signals failed strict gates`, and `run unavailable` when data supports the distinction.

### Data contract

Uses the existing `GET /v1/discovery/overview` and `POST /v1/run/discovery` endpoints. It does not introduce mock data or synthetic completion states.

## 6. Ops — technical health at a deliberate density

### Purpose

Give the operator a concise view of pipeline health without competing with review work.

### Layout

1. **Health summary:** last run status/time, source availability, queue size, database state, and attention-required count.
2. **Recent runs:** a compact, sortable chronology with outcome, duration, source, processed counts, candidate count, and error badge.
3. **Source health:** each source gets availability, freshness/last-seen time, failure trend, and retry/degradation explanation.
4. **Alerts and runbooks:** expandable panels. Critical alerts start expanded; reference runbooks remain one click away.
5. **Operational controls:** pauses/budget controls use labelled forms, explicit status, and confirmation feedback. They never appear in the Queue or Review decision rail.

### Data contract

Uses existing metrics, analytics, alerts, runbooks, budget, and pause endpoints. No operational data is removed; the new grouping only changes its presentation.

## 7. Responsive and accessibility rules

| Viewport | Required behavior |
| --- | --- |
| Desktop ≥ 1200px | Feature candidate and evidence use broad, readable panels; secondary information may form a side rail. |
| Tablet 768–1199px | Side rails stack beneath primary content; action labels remain visible. |
| Mobile < 768px | One column; sticky decision rail becomes a fixed bottom action bar with an overflow menu for advanced actions. |

- Full keyboard navigation works for navigation, filters, disclosures, candidate movement, and every decision action.
- Minimum contrast meets WCAG AA for normal text; status labels do not rely on color alone.
- Target size is at least 40px on touch layouts.
- Live updates use polite aria-live messages and do not steal focus.
- Reduced-motion preference removes nonessential transitions.

## 8. Non-goals and constraints

- This work does **not** change CT sampling, domain normalization, strict cross-root rules, scoring, review policy, database schema, or API semantics.
- This work does **not** introduce mock candidates. Every interface state reflects actual API data or a truthful loading/error/empty state.
- The server remains local-only and keeps the existing route structure unless an internal shared template extraction preserves routes exactly.
- The redesign replaces the narrower, dark-only visual direction in `docs/design/review-console-redesign.md` for these four user-facing screens. Its useful interaction requirements should be retained where they do not conflict with this spec.

## 9. Acceptance criteria

1. With one ready candidate and 36 lower-confidence leads, the ready candidate is the first meaningful object on the Queue and its readiness evidence is readable without opening a detail page.
2. With no ready candidates, the Queue accurately says so and gives a clear next action; it never implies a candidate was found.
3. A reviewer can understand a candidate's verdict, chronological newness evidence, and available decision actions on the Review page without opening audit JSON.
4. Discovery visibly differentiates a completed zero-candidate run, a strict-gate rejection outcome, and a source/API failure.
5. Ops surfaces unhealthy sources and recent failed runs without obscuring the current operational state.
6. Existing API routes, decision behavior, Chinese/English labels, and live-data workflow continue to work.
7. The console is legible at 200% browser zoom and usable by keyboard.

## 10. Validation plan

- Run the existing automated suite after the implementation.
- Serve the actual verified-candidate SQLite database and inspect Queue, Review, Discovery, and Ops in a browser at desktop and narrow widths.
- Exercise client-side filters, candidate navigation, a non-destructive review flow where the test fixture allows it, and every loading/error/empty state that can be triggered safely.
- Confirm browser console remains free of JavaScript errors and that no API response is replaced by mock content.
