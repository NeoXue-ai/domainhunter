# CT 采集层优化待办（Deferred）

> 记录于 2026-09-01。当前决策：采集层做到"全日志覆盖 + first-seen 基线 + 注册级标准化 + lag 监控"即 90 分，以下优化项验证过价值但边际收益低或依赖环境，暂缓，留待以后。

## 已完成/进行中

- [x] **A1a tuscolo 库 bug 修复**（`ct-moniteur` `httpx_ratelimit.py` 除零，429 重试时 `elapsed=0` / `rate_limit=0`）。已在本机 venv patch 验证两个日志可读。
- [ ] **A1b tuscolo patch 固化**：以 vendored/monkeypatch 形式固化进项目（uv 重装会覆盖 site-packages 改动）。
- [x] **A3 lag 监控**：每日志落后量监控，防高吞吐日志永久落后。`src/domainhunter/scheduler/lag_monitor.py` + 测试。
- [x] **first-seen 基线（增量版）**：`ct_seen_domains` 表（domain/first_seen_at/first_source）+ `mark_seen`/`filter_new`/`is_seen` + CLI `filter-enrich --fresh-only`。重复输入自动跳过，不浪费 RDAP/探测/LLM 成本。验证：12 域名首跑全 fresh，二跑全 seen（0 处理）。

## 待办（以后搞）

### B1. Google argon/xenon 日志覆盖（环境依赖）
- 问题：`ct.googleapis.com/logs/us1/argon2026h2`、`argon2027h1`、`eu1/xenon2026h2`、`xenon2027h1` 在开发机（中国网络）完全不可达（连接超时）。日志本身可用（log list 标记 usable），**不是代码问题**。
- 行动：部署到美国 VPS 后自然可达，无需代码改动。验证点：VPS 上确认 4 个日志进入 active logs。
- 价值：+4 日志覆盖，预期发现量 +5~10%。

### B2. 日志清单定期自动刷新
- 问题：当前 `CTMoniteur` 的 `refresh_interval=6.0`（小时）已自动刷新 log list，但需要确认新日志/退役日志被正确处理（`_periodic_refresh` 是否重建 clients）。
- 行动：验证 `_periodic_refresh` 逻辑，必要时补充测试。
- 价值：长期不维护也能跟随 Google log list 变化。

### B3. 深回扫 90 天（可选，可能被 RDAP 替代）
- 问题：新注册域名可能注册后数周才首次签发证书，7 天浅回扫会漏掉"延迟建站"的域名。
- 成本：90 天 × 25 日志 ≈ 数十亿条，成本极高。
- **替代方案（推荐）**：first-seen + RDAP 注册时间验证，效果相同、成本低一个数量级。深回扫仅当 RDAP 不可行时再考虑。

### B4. 证书指纹去重（precert + final）
- 同一张证书会以 precert + final 两种形式出现在日志（甚至多个日志）。域名级已去重（dict key），指纹去重主要用于统计准确性。
- 价值：低（对域名级发现无影响）。

### B5. 多日志交叉确认
- 新域名同时出现在 2+ 日志 = 确认真实性；单日志独有 = 可能是日志提交延迟的假象。
- 价值：中，作为 first-seen 的可靠性增强。

### B6. 证书元数据信号排序（www+apex、issuer、SAN 数）
- 给 first-seen 域名排队：not_before 新鲜度、SAN 数量（1-3 = 个人/小项目）、issuer（LE/ZeroSSL = 自助建站）、www+apex 同时出现（真建站信号）。
- 价值：中，只影响处理顺序，不影响发现量。筛选层做完后可作为第二优先级优化。

### B7. 密度感知调度
- 统计每日志"新域名密度"（first-seen 域名 / 总条目），优先盯高密度日志，低密度日志降低 poll 频率。
- 价值：中，长期省资源、提效率。

### B8. 自建 Static CT API 轻量镜像（暂缓）
- 背景：tiled logs 的瓦片本质是静态文件（checkpoint + /tile/data/x...），按 index 可读任意历史。Google `ct.googleapis.com` 在国内被阻断，但 ParcelYard 的 GCS 静态端点可达。
- 设想：增量同步近期瓦片到本地静态目录 + 简单索引（域名 → 证书记录），换来国内可达 + 按域名历史查询 + first-seen 基线。
- 否决原因：全量镜像不现实（单日志数亿条 × ~1.5KB ≈ 几十 TB）。轻量方案（深回扫 7-90 天 + 增量）成本低但暂不急。
- 价值：中；前置条件：first-seen 基线做成后，若国内网络仍是大问题再考虑。

## 已放弃/不做的

- CertStream WebSocket / certstream-server-rust：公共 WebSocket 已死（~9 个月无数据），纯 HTTP 轮询替代。
- crt.sh 实时流：限流、滞后不可靠。
- GitHub 搜索源：用户明确砍掉（专一 CT）。