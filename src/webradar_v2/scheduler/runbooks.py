"""Curated runbook registry — one entry per AlertKind with three operator sections."""

from webradar_v2.domain.alerts import AlertKind, Runbook


_RUNBOOKS: dict[AlertKind, Runbook] = {
    AlertKind.ZERO_INPUT: Runbook(
        runbook_id="rb.zero_input",
        kind=AlertKind.ZERO_INPUT,
        title="Source ingest has stopped delivering new events",
        steps=(
            "先看什么："
            "GET /v1/metrics 关注 source_events 增量；"
            "SELECT COUNT(*), source FROM source_events "
            "WHERE observed_at >= datetime('now','-1 hour') GROUP BY source；"
            "确认 CT log 与 GitHub Search 两条链路是否同时断流，"
            "还是单源掉线。",
            "能否安全重试："
            "可在 CLI 中手动 webradar listen-certstream --max-messages 1 "
            "或 webradar poll-github-api --query topic:artificial-intelligence "
            "验证可达性；"
            "短期补抓不会污染 funnel_metrics，"
            "idempotency_key 保证重放安全。",
            "何时人工介入："
            "持续 1 小时仍无事件需排查上游 CT log / GitHub API 状态页；"
            "若单源持续静默，按需要下线对应 poller "
            "并在 ai_knows_audit 中记录手工决策。",
        ),
    ),
    AlertKind.BASELINE_DRIFT: Runbook(
        runbook_id="rb.baseline_drift",
        kind=AlertKind.BASELINE_DRIFT,
        title="Ingest rate has drifted outside the 24h baseline band",
        steps=(
            "先看什么："
            "GET /v1/metrics 比对 source_events 1h 速率与 24h 中位数；"
            "SELECT strftime('%H', observed_at) AS hour, COUNT(*) FROM source_events "
            "WHERE observed_at >= datetime('now','-24 hour') GROUP BY hour "
            "ORDER BY hour 画出 24h 速率柱状图；"
            "重点排查低活跃来源（GitHub Search、certstream）是否异常。",
            "能否安全重试："
            "无需自动重试——漂移本质是源行为变化；"
            "若判定上游结构变化（如新增 TLD），"
            "webradar ingest-ct-page / ingest-github-page 重放近 1h 增量即可。",
            "何时人工介入："
            "比率 < 0.5 或 > 2 持续 3 个 tick 需人工核对；"
            "若上游因 GDPR 或上游条款减少推送，"
            "需更新 parser_version 并把对应源降级到 half-rate。",
        ),
    ),
    AlertKind.QUEUE_BACKLOG: Runbook(
        runbook_id="rb.queue_backlog",
        kind=AlertKind.QUEUE_BACKLOG,
        title="Work queue has items scheduled more than 1h in the past",
        steps=(
            "先看什么："
            "SELECT stage, COUNT(*) FROM work_queue "
            "WHERE completed_at IS NULL AND scheduled_at < datetime('now','-1 hour') "
            "GROUP BY stage；"
            "对照 funnel_metrics.queued_work 数值，"
            "定位是 L1 积压、L2 渲染卡住或 LLM 队列塞车。",
            "能否安全重试："
            "已释放的 lease 自然会被下一个 daemon tick 重新 claim，"
            "无需手工干预；"
            "对卡住的 stage 可短暂提高 tick_seconds 与 lease_seconds，"
            "让现有 worker 跑完再恢复。",
            "何时人工介入："
            "持续 1h 积压 + budget_reserved_units 已饱和 → "
            "暂停昂贵 stage 并人工审批漏斗是否阻塞；"
            "若某 stage 长期未实现（如 L2/L3/EXPOSURE），"
            "确认 budget_deferred 日志而非真的丢任务。",
        ),
    ),
    AlertKind.BUDGET_EXHAUSTED: Runbook(
        runbook_id="rb.budget_exhausted",
        kind=AlertKind.BUDGET_EXHAUSTED,
        title="Per-stage daily reserved units exceeded 80% of the configured cap",
        steps=(
            "先看什么："
            "SELECT stage, SUM(units) FROM budget_ledger "
            "WHERE status='reserved' AND occurred_at >= datetime('now','-1 day') "
            "GROUP BY stage；"
            "对比 WorkStage.L1/L2/LLM 当日已配置上限（CLI --budget-per-stage-*）。",
            "能否安全重试："
            "未触顶的 stage 可继续 drain work_queue，"
            "daemon 的 _budget_gate_open 会自动避开已饱和 stage；"
            "对短期超支无需回滚，ledger 已 audit 完整。",
            "何时人工介入："
            "reserved/deferred 比值长期 > 0.8 表示预算配置偏低；"
            "通过 CLI 临时调高 --budget-per-stage-llm 或 --budget-per-stage-l1；"
            "若 LLM 单次成本失控，审查 model_version 并锁回稳定模型。",
        ),
    ),
    AlertKind.ERROR_RATE_SPIKE: Runbook(
        runbook_id="rb.error_rate_spike",
        kind=AlertKind.ERROR_RATE_SPIKE,
        title="DNS/HTTP/render error rate has doubled versus the 24h baseline",
        steps=(
            "先看什么："
            "SELECT outcome_code, COUNT(*) FROM observations "
            "WHERE observed_at >= datetime('now','-1 hour') GROUP BY outcome_code "
            "ORDER BY COUNT(*) DESC；"
            "再 SELECT outcome_code, COUNT(*) FROM observations "
            "WHERE observed_at >= datetime('now','-24 hour') GROUP BY outcome_code "
            "做分母基线。",
            "能否安全重试："
            "retry_policy.decide_next_action 已按 outcome 调度退避；"
            "无需重复重试；"
            "若 DNS_TIMEOUT / TLS_ERROR 占比 > 50% 可临时下调 tick_seconds "
            "减少并发抖动。",
            "何时人工介入："
            "持续 1h 翻倍 + 同一 ASN 占比高 → 暂停对应域再探测；"
            "若 RENDER_TIMEOUT 飙升，检查 L2 渲染 worker 健康度；"
            "若 HTTP_5XX 来自同一上游，确认是否 WAF 拦截而非真错误，"
            "切到 SSRF_INTERCEPTION_SPIKE 的 runbook。",
        ),
    ),
    AlertKind.SSRF_INTERCEPTION_SPIKE: Runbook(
        runbook_id="rb.ssrf_interception_spike",
        kind=AlertKind.SSRF_INTERCEPTION_SPIKE,
        title="SSRF / robots / WAF block rate has doubled versus baseline",
        steps=(
            "先看什么："
            "SELECT domain, COUNT(*) FROM observations "
            "WHERE outcome_code IN ('blocked_ssrf','robots_disallowed') "
            "AND observed_at >= datetime('now','-1 hour') GROUP BY domain "
            "ORDER BY COUNT(*) DESC LIMIT 20；"
            "确认是单域还是全网拦截。",
            "能否安全重试："
            "blocked_ssrf / robots_disallowed 不会消耗 budget；"
            "do NOT 自动重试——可能是上游策略变化或 IP 被加入黑名单；"
            "短时间 1h 内不再 claim 同一域即可。",
            "何时人工介入："
            "持续翻倍 1h 需立即暂停相关 poller；"
            "若 robots_disallowed 大面积出现，确认 robots.txt parser_version "
            "是否升级；"
            "若 blocked_ssrf 集中到某 ASN，先关停 L1 probe 走白名单重审。",
        ),
    ),
    AlertKind.SCHEMA_FAILURE: Runbook(
        runbook_id="rb.schema_failure",
        kind=AlertKind.SCHEMA_FAILURE,
        title="LLM output failed the fixed schema or taxonomy validator",
        steps=(
            "先看什么："
            "SELECT candidate_id, version, model_version, primary_outcome "
            "FROM candidate_versions WHERE author_kind='llm' "
            "AND primary_outcome='valid_but_not_ready' "
            "ORDER BY version DESC LIMIT 20；"
            "配合 webradar enrich-llm --provider mock 重放失败样本。",
            "能否安全重试："
            "可对单个 candidate_version 调用 webradar enrich-llm "
            "或 enrich_candidate_with_llm() 重跑；"
            "parser_version 变更后必须重放 web_corpus_replay 才能回滚。",
            "何时人工介入："
            "1h 内失败率持续上升 → 暂停 LLM 阶段并人工核对 schema；"
            "若 model_version 升级引入漂移，回滚到上一稳定版本 "
            "并锁定 parser_version + TAXONOMY_VERSION；"
            "对 valid_but_not_ready 的人工复核通过后写一条新的 human 版本。",
        ),
    ),
    AlertKind.EXTERNAL_SYNC_FAILURE: Runbook(
        runbook_id="rb.external_sync_failure",
        kind=AlertKind.EXTERNAL_SYNC_FAILURE,
        title="AIKnows draft sync returned reconciliation_required",
        steps=(
            "先看什么："
            "SELECT candidate_id, candidate_version, status_code, latency_ms, url "
            "FROM ai_knows_audit WHERE candidate_id IS NOT NULL "
            "ORDER BY id DESC LIMIT 20；"
            "对照 GET /v1/audit/aiknows?candidate_id=… 验证细节。",
            "能否安全重试："
            "幂等性键 Idempotency-Key=candidate_id:version 保证重发安全；"
            "可在 CLI 中 webradar outreach --request-id … 重发；"
            "只在 sync_reconciliation_required 上重试，"
            "validation_error 必须先修字段。",
            "何时人工介入："
            "sync_reconciliation_required 持续 > 1h → "
            "切换到手工模式并联系 AIKnows；"
            "若 external_version 冲突，对照 publications.external_version "
            "决定回滚或合并；"
            "切勿在 unknown 状态自动重写外部条目。",
        ),
    ),
}


def get_runbook(kind: AlertKind) -> Runbook:
    """Return the curated runbook for a given anomaly class.

    Raises ``KeyError`` if a future ``AlertKind`` is added without a runbook —
    that's the safety signal callers depend on, never a silent fallback.
    """

    return _RUNBOOKS[kind]


def list_runbooks() -> tuple[Runbook, ...]:
    """Return all curated runbooks in canonical kind order."""

    return tuple(_RUNBOOKS[kind] for kind in AlertKind)


def get_runbook_by_id(runbook_id: str) -> Runbook | None:
    """Return the runbook matching ``runbook_id`` if one is registered."""

    for runbook in _RUNBOOKS.values():
        if runbook.runbook_id == runbook_id:
            return runbook
    return None