# GitHub fixture suite

Replayable GitHub API payloads for `GitHubPoller` and `build_github_homepage_event`.

Envelope (single page):

```json
{
  "scenario": "<name>",
  "description": "<purpose>",
  "expected_repositories": <int>,
  "expected_invalid_homepages": <int>,
  "expected_events_added": <int>,
  "expected_domains": ["..."],
  "repositories": [
    {
      "repository_id": "repo-...",
      "homepage": "https://...",
      "observed_at": "2026-08-10T00:00:00Z"
    }
  ]
}
```

Pagination variant uses `pages: [{ "next_cursor": ..., "repositories": [...] }]`.

## Scenarios

| File | Purpose | Expect |
| --- | --- | --- |
| `valid_homepage.json` | 4 repos with HTTPS homepages | 4 events added, 0 invalid |
| `duplicate.json` | Same `repository_id` × 4 with one alternative URL | 2 events added on first poll, 0 on replay |
| `invalid_homepage.json` | IP / ftp / empty / schemeless / credentialed URLs | 1 of 6 events added |
| `pagination.json` | Two-page fixture exercising next_cursor | All 3 repos append; final cursor is `null` |

## Usage

```python
import json
from datetime import datetime
from pathlib import Path

from webradar_v2.ingest.github_poller import GitHubPage, GitHubPoller, GitHubRepository
from webradar_v2.storage.sqlite import SQLiteStore

fixture = json.loads(Path("tests/fixtures/github/valid_homepage.json").read_text())
page = GitHubPage(
    repositories=tuple(
        GitHubRepository(
            repository_id=r["repository_id"],
            homepage=r["homepage"],
            observed_at=datetime.fromisoformat(r["observed_at"].replace("Z", "+00:00")),
        )
        for r in fixture["repositories"]
    ),
    next_cursor=None,
)

async def fetch(cursor):
    return page

store = SQLiteStore(tmp_path / "db.sqlite")
result = await GitHubPoller(store=store, fetch_page=fetch).poll()
assert result.events_added == fixture["expected_events_added"]
```

For pagination, loop until `next_cursor is None`.