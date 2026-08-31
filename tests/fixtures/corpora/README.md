# Redacted web evidence corpus

A small, hand-curated set of HTML pages and their expected classifier
outcomes. This corpus exists so we can replay classifier changes
(rule edits, prompt tweaks, model swaps) against the same evidence
without ever touching the live web.

## What's in here

```
tests/fixtures/corpora/web_pages/
├── ai_saas_strong.jsonl    # one JSON object per line: { url, html, expected_extracted_evidence, expected_taxonomy_tag, expected_outcome, expected_confidence }
├── expected_outcomes.jsonl # per-url expected (primary_outcome, confidence, category)
└── README.md
```

`ai_saas_strong.jsonl` is the **input** to a classifier — the HTML pages a
candidate crawler would have fetched. `expected_outcomes.jsonl` is the
expected classification output used by :func:`webradar_v2.llm.eval.evaluate_predictions`
to compute Precision@K.

## What the corpus covers

10 records, each labelled with one of the five
:class:`CandidateOutcome` values:

| Outcome | Count | Why |
| --- | --- | --- |
| `publishable_ai_saas` | 2 | Real product marketing pages with pricing + signup |
| `valid_but_not_ready` | 1 | Open research notebook with no commercial offering yet |
| `not_target` | 2 | Service agency + parking page — never AI SaaS |
| `duplicate_or_existing` | 1 | Re-observation of an already-known candidate |
| `policy_excluded` | 4 | Prompt directory, blog, open-source demo, adult content |

The mix is intentionally skewed toward negative classes to exercise
Precision@K on a realistic, imbalanced distribution.

## Redaction guarantees

* Every URL uses ``example.com / example.io / example.dev / example.app``
  subdomains — never a real brand.
* Every page title uses a fake product name (`Acme AI Studio`, `Vector
  Desk`, `PromptForge`, etc.).
* No real person, real company, or real customer name appears anywhere.
* The HTML contains no tracking scripts, no analytics calls, and no
  third-party requests.

If you want to add a record:

1. Pick a fake ``example.tld`` subdomain and a fake product name.
2. Write enough HTML that ``analyze_http_document`` returns
   ``OutcomeCode.SUCCESS`` (≥ 500 chars of text, no parking markers).
3. Add a matching row to ``expected_outcomes.jsonl``.
4. Re-run ``tests/test_web_corpus_replay.py`` to make sure Precision@K
   stays above the spec §15 threshold.

## Usage

```python
import json
from pathlib import Path

records = []
for line in Path("tests/fixtures/corpora/web_pages/ai_saas_strong.jsonl").read_text().splitlines():
    records.append(json.loads(line))

expected = []
for line in Path("tests/fixtures/corpora/web_pages/expected_outcomes.jsonl").read_text().splitlines():
    expected.append(json.loads(line))
```

The replay test loads both files and feeds them into
:func:`webradar_v2.llm.eval.evaluate_predictions`. To benchmark a new
classifier, run it over the JSONL in an offline subprocess, capture the
predictions, and pass them to the same evaluator.