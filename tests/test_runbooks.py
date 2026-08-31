"""Contract tests for the curated spec §14 runbook registry."""

from webradar_v2.domain.alerts import AlertKind, Runbook
from webradar_v2.scheduler.runbooks import (
    get_runbook,
    get_runbook_by_id,
    list_runbooks,
)


def test_runbook_registry_covers_all_kinds() -> None:
    runbooks = list_runbooks()
    seen = {runbook.kind for runbook in runbooks}

    assert seen == set(AlertKind)
    assert len(runbooks) == len(AlertKind)


def test_every_runbook_has_three_sections_with_required_keywords() -> None:
    runbooks = list_runbooks()

    for runbook in runbooks:
        assert isinstance(runbook, Runbook)
        assert runbook.steps, runbook.runbook_id
        joined = "\n".join(runbook.steps)
        for keyword in ("先看什么", "能否安全重试", "何时人工介入"):
            assert keyword in joined, f"{runbook.runbook_id} missing section: {keyword}"


def test_get_runbook_returns_matching_registry_entry() -> None:
    for kind in AlertKind:
        runbook = get_runbook(kind)
        assert runbook.kind is kind
        assert runbook.runbook_id.startswith("rb.")


def test_get_runbook_by_id_resolves_known_and_unknown() -> None:
    found = get_runbook_by_id("rb.zero_input")
    assert found is not None
    assert found.kind is AlertKind.ZERO_INPUT

    missing = get_runbook_by_id("rb.does_not_exist")
    assert missing is None


def test_every_runbook_cites_a_concrete_data_source() -> None:
    # The runbook text must reference at least one of: metrics endpoint,
    # audit table, observations table, candidate_versions, ai_knows_audit,
    # source_events, budget_ledger, work_queue.
    concrete_hints = (
        "/v1/metrics",
        "source_events",
        "observations",
        "candidate_versions",
        "ai_knows_audit",
        "budget_ledger",
        "work_queue",
    )
    for runbook in list_runbooks():
        joined = "\n".join(runbook.steps)
        assert any(hint in joined for hint in concrete_hints), (
            f"{runbook.runbook_id} does not cite any concrete data source"
        )