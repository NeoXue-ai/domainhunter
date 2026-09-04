# 筛选层设计与验证记录

> 初稿记录于 2026-09-01，严格策略更新于 2026-09-04。采集层之后的过滤漏斗：把已配置 CT 日志中首次观察到的注册级域名过滤成可审计的"真新网站"候选队列。
> 状态：**S1-S5 已实现**；所有会写入审核队列的命令都强制 RDAP、DNS 与最终根域一致性门槛。

## 目标

把"新签发的证书域名"过滤成"真新网站"，最终进人类 review 队列。

## 漏斗（成本递增、量递减）

```
采集层输出：first-seen 注册级域名（估每天几百~几千）
  │
  ▼  S1 静态信号（零成本，纯文本分析）
  域名特征打分 + 批量注册指纹
  │
  ▼  S2 RDAP 注册时间（HTTP，需缓存/限速）
  ≤30 天 Tier 1 / 31-90 天 Tier 2 / >90 天丢弃 / 未知则重试
  交叉验证：注册日期 vs 证书 first-seen 日期
  │
  ▼  S3 DNS 探测（便宜，一次查询）
  A 记录存在（严格路径必需；未就绪则重试）
  │
  ▼  S4 L1 HTTP 探测（已有 pipeline）
  存活 + 标题/描述提取
  │
  ▼  S5 AI 判型（最贵，最后，可选）
  规则预筛 → LLM 判型
  │
  ▼  输出：人类 review 队列（估每天 5~15 个）
```

## 各层定义

### S1 静态信号（零成本）
- **随机字符检测**：元音/辅音比、连续辅音数、字典词匹配。示例：`wasyal.com`（垃圾农场特征）vs `moralespsychservices.com`（真品牌）。
- **科技 TLD 加权**：`.ai`/`.io`/`.dev`/`.app` 加分；`.xyz`/`.top` 中性偏疑（不绝对）。
- **批量注册指纹**：同注册商 + 同注册日 + 命名相似 → 垃圾农场信号。

### S2 RDAP 注册时间（核心刀）
- **双桶**：`≤30 天` Tier 1（高优先级）；`31-90 天` Tier 2（标记 "slow starter"，人工可翻）；`>90 天` 丢弃。
- **阈值可配置**（`--tier1-days` / `--tier2-days`）。
- **交叉验证**：`注册日期 ≤ 证书 first-seen 日期` 才合理；若 first-seen 早于注册日期 → 域名刚易主（老域名新注册），自动降级。
- **失败重试**：RDAP 暂不可用（如部分 TLD 或网络故障）不会降级为候选，也不会永久丢弃；域名保留在持久化工作队列中等待重试。
- 技术：RDAP（非传统 WHOIS），免费无 key，Verisign + 各 TLD RDAP 已验证可用。

### S3 DNS 探测（便宜）
- A 记录存在才进入严格候选路径。
- 无解析或解析暂未就绪 = 保留重试，不创建候选。

### S4 L1 HTTP 探测（已有）
- 复用 `DomainHunterPipeline.probe_domain`。

### S5 AI 判型（可选）
- 规则预筛（`build_rule_candidate_draft`）+ 可选 LLM 判型（MiniMax/OpenAI-compatible）。
- LLM 失败不会撤销已通过 S1-S4 严格证据的候选；只会跳过附加的模型版本。

## 架构决策（已定）

1. **硬门槛 + 可审计排序**：候选必须通过 RDAP 年龄、DNS 和最终根域一致性；每层仍保留证据链供 review 查看。老域名重新建站不属于本项目的"新网站"候选范围。
2. **RDAP 需要缓存层**：有速率限制，每天几百个查询需要队列 + 缓存 + 重试。这是筛选层唯一需要"工程化"的部分。
3. **入队命令没有宽松开关**：`filter-probe`、`filter-enrich` 与 `discover` 不允许关闭严格 RDAP/DNS 门槛。

## 验证计划

1. 实现 S1+S2+S3（全免费、纯 HTTP、无 key）。**✅ 已完成**
2. 对采集层现有真实数据（/tmp/ct_xlog_experiment 的 23k 注册级域名）回放一遍，看每刀砍掉多少。**✅ 已完成**
3. 输出"新生域名候选 + 证据链"队列，人工检查质量。**✅ CLI `filter` 命令已可用**
4. 严格路径在真实 CT 抽样与回归测试中验证；阈值调整属于后续产品策略，不改变准入不变量。

## 实现记录（2026-09-01）

- `src/domainhunter/filter/static_signals.py` — S1 域名特征打分（长度/单词/TLD/随机串指纹），`score_domain()` + `batch_registration_flag()`
- `src/domainhunter/filter/rdap_age.py` — S2 RDAP 查询 + 双桶分级（`classify_age`）+ MemoryCache
- `src/domainhunter/filter/dns_check.py` — S3 DNS A 记录探测（标准库 socket，零依赖）
- `src/domainhunter/filter/pipeline.py` — 漏斗编排 `FilterPipeline`，输出带证据链的 `FilteredCandidate`；`run_with_probe()` 支持 S4 探测
- `src/domainhunter/filter/enrich.py` — S5 自动化：`run_batch()` 把 S1→S5 全链路串起来（filter → source event 入库 → probe → LLM 判型 → rule+llm 双版本落库），单域名容错（坏域名不中断 batch）
- CLI：`domainhunter filter --input domains.json [--tier1-days 30]`（默认严格 RDAP/DNS）
- CLI：`domainhunter filter-probe --database db --input domains.json` — 漏斗 + S4 真实探测（候选先以 `filter` source event 入库，再 L1 探测 + 规则判型）
- CLI：`domainhunter filter-enrich --database db --input domains.json --provider openai-compatible --base-url ... --token ... --model ...`（或 `--provider mock`）— S5 全链路
- 测试：+47 个新测试（static_signals 12 / rdap_age 13 / dns_check 5 / pipeline 10 / enrich 3 / CLI 4）

### S5 LLM 兼容性修复（真实 MiniMax-M3 验证）

- **base_url 双 /v1 bug**：`base_url` 带 `/v1` 时与内部路径拼成 `/v1/v1/...` 404。provider 自动剥离尾部 `/v1`。
- **推理模型 thinking 污染**：MiniMax-M3/DeepSeek-R1 的 content 含 thinking 过程，JSON 答案在尾部（有/无 ```json 围栏）。`_strip_thinking()` 三层提取：fenced → tail-json → raw。
- **prompt schema 对齐**：system prompt 内置完整 JSON schema 示例 + 规则（outcome 枚举与 `CandidateOutcome` 对齐、model_version 如实填写），模型输出精确匹配 12 键。
- **httpx 全面异常捕获**：`httpx.HTTPError` 而非仅 TimeoutException（RemoteProtocolError 等也降级为 NEEDS_REVIEW）。
- **推理模型偶发不稳**：temperature=0 下 MiniMax-M3 仍可能偶发输出不合 schema（如空 model_version）→ `llm_skipped` 降级 + 重试可成功，机制正确。

### 真实数据回放结论（23k 注册级域名，4 分钟跨日志窗口）

- S1 分布拉开后 0.6+ 占 52%（原 66%），高分 0.8+ 12%
- RDAP 抽样 280：解析 63，<90 天 10 个（16% 年轻率）
- **S1 优先级排序有效**：300 个高 S1 → 21 个 <90 天；80 个低 S1 → 4 个（3 个 punycode + 1 个随机串 = 全是"我们不要的"）
- 真新品牌样本：`mywhitecoatai.com`（医疗 AI）、`getzerowait.com`、`appellateos.com`
- S1 满分 ≠ 真产品（`eclaizance.com` 等），需 S4/S5 后续判定 → 验证了分层必要性
- 端到端 `filter` 命令：12 输入 → 9 保留（7 tier1 + 2 tier2），老域名正确丢弃
- 端到端 `filter-probe` 命令（S4 真实探测）：9 候选 → 8 入库候选，**`mywhitecoatai.com` 规则判型为 `publishable_ai_saas`**（医疗 AI 产品）；`wasyal.com` S4 暴露为过期域名拍卖页（重定向 expireddomains.com），S1 只给 0.60 分 → 验证了 S4 的必要性

## 后续策略调优（不影响严格准入）

- S1 打分的具体权重（随机字符 vs 品牌词的判定标准）
- S5 AI 判型标签在真实审核反馈后的细分
