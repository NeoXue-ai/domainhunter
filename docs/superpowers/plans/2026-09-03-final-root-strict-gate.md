# Final-root strict gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent a newly registered CT hostname from creating a candidate when its HTTP probe ends on a different registrable domain.

**Architecture:** Preserve every L1 observation for audit, but make candidate creation fail closed in strict discovery when the final HTTP URL does not have the same PSL registrable root as the CT-filtered domain. This keeps RDAP and DNS proof bound to the website whose content becomes candidate evidence. Raw low-level pipeline callers retain their current behavior unless they explicitly opt into the new gate.

**Tech Stack:** Python 3.14, httpx MockTransport, SQLite, pytest, Public Suffix List normalization.

---

### Task 1: Express the cross-root redirect regression at the candidate boundary

**Files:**
- Modify: `tests/test_pipeline.py`
- Modify later: `src/domainhunter/pipeline.py`

- [x] **Step 1: Add a failing cross-root redirect test**

Add this test and import no production implementation code beyond the file's existing imports:

```python
def test_strict_probe_keeps_observation_but_rejects_cross_root_redirect_candidate(tmp_path) -> None:
    html = (
        "<title>Old Site</title>"
        '<meta name="description" content="Existing product platform">'
        "<p>" + ("Existing product content. " * 40) + "</p>"
    )

    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.host == "fresh-redirect.com":
            return httpx.Response(
                301,
                headers={"location": "https://old-site.com/"},
                request=request,
            )
        return httpx.Response(200, text=html, request=request)

    store = SQLiteStore(tmp_path / "domainhunter.db")
    event = SourceEvent("ct_log", "nimbus:1", "fresh-redirect.com", OBSERVED_AT)

    async def run() -> None:
        async with HTTPProbe(
            resolver=_public_resolver,
            transport=httpx.MockTransport(responder),
            respect_robots=False,
        ) as probe:
            pipeline = DomainHunterPipeline(store=store, probe=probe)
            pipeline.ingest_event(event)
            result = await pipeline.probe_domain(
                "fresh-redirect.com",
                observed_at=OBSERVED_AT,
                require_same_final_root=True,
            )

        assert result.observation.final_url == "https://old-site.com/"
        assert result.candidate_version is None
        assert store.list_review_queue() == ()

    asyncio.run(run())
```

- [x] **Step 2: Verify the test is red**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_pipeline.py::test_strict_probe_keeps_observation_but_rejects_cross_root_redirect_candidate
```

Expected: failure because `probe_domain` does not yet accept `require_same_final_root`.

- [x] **Step 3: Add a same-root control test**

Add a sibling test with a 301 from `fresh-site.com` to `https://www.fresh-site.com/`. With `require_same_final_root=True`, assert `result.candidate_version is not None`. This proves that HTTPS/www redirects remain eligible while cross-root redirects do not.

### Task 2: Fail closed before candidate creation when strict probes cross roots

**Files:**
- Modify: `src/domainhunter/pipeline.py:1-98`
- Test: `tests/test_pipeline.py`

- [x] **Step 1: Add the opt-in parameter and root comparison helper**

Add `require_same_final_root: bool = False` to `DomainHunterPipeline.probe_domain`. Import `urlparse` and add this helper:

```python
def _final_url_matches_domain(domain: str, final_url: str | None) -> bool:
    if final_url is None:
        return False
    hostname = urlparse(final_url).hostname
    if hostname is None:
        return False
    try:
        return normalize_hostname(hostname).registrable_domain == domain
    except InvalidHostname:
        return False
```

Use it only at the candidate creation boundary:

```python
can_create_candidate = probe_result.analysis is not None
if require_same_final_root:
    can_create_candidate = can_create_candidate and _final_url_matches_domain(
        normalized.registrable_domain, probe_result.final_url
    )
if can_create_candidate:
    # Existing draft, candidate-version, and priority persistence unchanged.
```

Do not suppress the observation or alter raw caller behavior.

- [x] **Step 2: Verify both tests are green**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_pipeline.py
```

Expected: cross-root redirect has an observation but no review candidate; same-root www redirect still creates one.

### Task 3: Enable final-root gating in strict CT orchestration

**Files:**
- Modify: `src/domainhunter/ingest/ct_orchestrator.py:62-92`
- Modify: `tests/test_ct_orchestrator.py`

- [x] **Step 1: Add a failing strict orchestration integration test**

Use the existing CT poller and fake filter helpers. Feed `fresh-redirect.com` through a strict filter that keeps it, and provide an `L1Analysis` whose `final_url` is `https://old-site.com/`. Assert `summary.probes_run == 1`, `summary.candidates_created == 0`, and an empty review queue.

- [x] **Step 2: Verify it is red**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_ct_orchestrator.py::test_strict_mode_rejects_cross_root_redirect_candidate
```

Expected: it fails because strict orchestration currently calls `probe_domain` without the final-root gate.

- [x] **Step 3: Pass strictness explicitly to the pipeline**

Change the orchestrator probe call to:

```python
run = await self._pipeline.probe_domain(
    root,
    observed_at=stamp,
    require_same_final_root=(
        self._require_first_seen and self._filter_pipeline is not None
    ),
)
```

- [x] **Step 4: Verify the orchestrator suite**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_ct_orchestrator.py
```

Expected: old-domain and cross-root-redirect regressions both pass; raw default tests remain unchanged.

### Task 4: Verify the web and CLI strict paths, then seek a genuine real-data candidate

**Files:**
- Test only: `tests/test_discovery_api.py`, `tests/test_cli.py`
- Runtime database: `work/live-ct-strict-<log>-<window>.db`

### Runtime correction discovered during Task 4: canonical ownership

The first current-source live run found `elrincondesuenos.com`, whose HTTP final URL
remained on its own root but whose declared canonical URL was `rincondesuenos.mx`.
That allows an alias to reuse another website's identity without an HTTP redirect.

- [x] **Step 1: Add and run a failing external-canonical regression**

`tests/test_pipeline.py` now proves that a strict probe retains the observation but
does not create a candidate when its canonical URL resolves to another root.

- [x] **Step 2: Gate canonical URLs in the strict candidate boundary**

The existing final-root option now also requires a present canonical URL to share the
source registrable domain. Same-root and no-canonical pages retain existing behavior;
the complete suite passes with 370 tests.

### Runtime correction discovered during Task 4

The first live CLI run exposed a stale non-editable package in `.venv`: pytest loaded
`src/`, while `python -m domainhunter.cli` loaded an older copy in `site-packages`.
The live result is therefore invalid until the installer itself makes the worktree
source importable.

- [x] **Step 1: Add and run a failing installer regression test**

`tests/test_dev_install.py` now proves a root-level `bash dev_install.sh` must install
and run the package that sits beside the script. It failed because the script treated
the repository's parent directory as its root.

- [x] **Step 2: Fix the installer and reinstall this worktree**

`dev_install.sh` now resolves its own directory with `BASH_SOURCE`, rebuilds the shim
on every install, and dispatches the package CLI directly. The regression test passes
and the live virtual environment imports `src/domainhunter`.

- [x] **Step 1: Run all affected automated suites**

```bash
.venv/bin/python -m pytest -q \
  tests/test_pipeline.py tests/test_ct_orchestrator.py \
  tests/test_discovery_api.py tests/test_cli.py tests/test_filter_pipeline.py
```

- [x] **Step 2: Run the complete suite**

```bash
.venv/bin/python -m pytest -q
```

- [x] **Step 3: Sample fresh public CT log databases**

Every run used a fresh database. The decisive current-source run used ten 500-entry
pages from the 5,000-entry Nimbus window in
`work/live-ct-strict-nimbus-final-root-20260903-05.db`; TrustAsia, Wyvern, and Google
Argon were also sampled without lowering a gate.

Example command:

```bash
.venv/bin/python -m domainhunter.cli poll-ct-log \
  --database work/live-ct-strict-nimbus-final-root-300.db \
  --log cloudflare-nimbus2026=https://ct.cloudflare.com/logs/nimbus2026 \
  --catchup 300 --page-size 300 --max-probes 50
```

- [x] **Step 4: Independently verify the genuine candidate**

For every queued candidate, verify all of the following before declaring success:

1. Candidate domain equals the registrable root of its `observations.final_url`.
2. RDAP registration date of that same final root is at most 90 days old.
3. The same final root has a public A or AAAA record.
4. HTTP evidence is a non-redirect content response from that same root.
5. If the product target is AI/SaaS, the candidate outcome is `publishable_ai_saas` or it has completed S5 review. A `valid_but_not_ready` fallback is a review lead, not success.

If no candidate satisfies all five, keep sampling fresh logs/databases; never lower the RDAP, DNS, or final-root gate to force a positive result.

`limitlessrouter.com` satisfied all five: its candidate and observation roots are
identical, its official Verisign RDAP registration is `2026-09-01T00:53:45Z`, public
A and AAAA records resolve, the live page is a same-root HTTPS 200 response, and the
rule outcome is `publishable_ai_saas` with 0.78 confidence. Browser verification
found a paid, OpenAI-compatible public AI inference platform with a model catalog.
