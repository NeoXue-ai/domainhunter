# DomainHunter Review Console — Redesign Spec

**Status:** v2 — structural redesign (supersedes v1)
**Scope:** visual + interaction redesign of `_REVIEW_CONSOLE_HTML` in `src/domainhunter/api.py`
**Audience:** human reviewers using the local loopback console at `http://127.0.0.1:8000/`

---

## 0 · 修订记录

### v2（当前） — 结构性重设计

**v1 是 reskin，不是重设计。** v1 只换了 token 与配色，但保留了原结构的全部骨架：6 个窄 KPI 卡片排成一行、宽双列 grid 中间塞 review 主体、aside 堆 4 张独立卡片。这套骨架本质上是「内容列表页」而非「运营仪表盘」。

v2 推翻这条骨架，按仪表盘应有的视觉权重重排：

| 区域 | v1（reskin） | v2（重设计） |
|------|------------|------------|
| 当前候选 | `fs-h1` 22px，塞在 1.4fr/1fr 双列左半 | **hero 区，48px mono，独自占满首屏视觉中心** |
| KPI 数字 | 32px，6 张平均分摊 | **56–64px**，2 列 3 行大数字，配合 7 段 sparkline |
| 优先级 score | 36px 数字 + 单条横 bar | **120px 环形 SVG 仪表**，分数在中心，外环是 4 个分项色带 |
| Aside | 4 张独立卡堆叠（Discovery / Analytics / Alerts / Runbook） | **1 张 tabbed console 卡**，3 个 tab 切换 |
| 视觉层次 | 满屏 1px hairline | **背景深浅阶梯**：`--bg-0` 页底 / `--bg-1` 主区 / `--bg-2` 凹槽 / `--bg-3` 浮层；hairline 只用于最强分隔 |
| 操作区 | 4 个等宽按钮横排 | **按钮组**用主色 + 描边混排，快捷键徽章大到一眼能扫到 |

### v1（已弃用）

完整 token 体系（§2）、交互矩阵（§6）、可访问性（§7）仍然有效；v2 保留全部 token 和交互，仅重做信息架构（§3）和区域规范（§4）。

---

## 1 · 方向与定位

### 1.1 一句话定位

> 一个**面向运营审核员的暗色高端仪表盘**：让人感觉「这套系统很值钱」，而不是「这套系统能跑」。

### 1.2 设计原则（按优先级排序）

| # | 原则 | 含义 |
|---|------|------|
| P0 | **可读 > 装饰** | 数字、域名、版本号必须一眼看清；任何样式不得削弱信息密度 |
| P0 | **决策优先** | 4 个决策按钮（批准 / 拒绝 / 推迟 / 拉黑）永远是最显眼的可点击元素 |
| P1 | **氛围克制** | 不堆 emoji、不堆渐变；阴影与高光是「暗示」而非「炫耀」 |
| P1 | **状态可读** | 漏斗、告警、运行时延必须一眼看出是「健康」还是「出问题」 |
| P2 | **微动效有意义** | 动效只用于：a) 反馈操作成功 b) 引导视线到新数据 c) 消除 layout shift |
| P2 | **键盘至上** | A/R/D/B/E/O/J/K/←/→ 全程可达，且不与表单冲突 |

### 1.3 风格坐标

| 维度 | 取值 |
|------|------|
| 主题 | 深色（`prefers-color-scheme: dark` 强制；暂不提供浅色切换） |
| 温度 | 中性偏暖（一抹焦糖金做强调，避免冷蓝） |
| 密度 | 中（信息密度向 SRE 工具靠拢，但留白向 SaaS 靠拢） |
| 字体 | Sans（系统字体）+ Mono（数字与代码）；不引入 web font |
| 圆角 | 小（6–10px），不要「药丸状」卡片 |
| 阴影 | 极少；只在悬浮/浮层用 |
| 边框 | **主导分隔手段改为背景深浅**；hairline 仅保留在最关键分隔处 |

### 1.4 参考基调

不是「炫酷」，是「贵」。

- **Linear**：克制配色 + 等宽数字 + 极细分割线 + 极小阴影
- **Datadog**：状态语义用色（绿/黄/红/紫/蓝），但卡面密度降低
- **Vercel**：单色对比 + 单一品牌色（这里用焦糖金 #D4A574 而非紫）

---

## 2 · 设计 Token（Design Tokens）

所有视觉值都先 token 化，再在 CSS 中引用。`api.py` 是单一字符串，无法拆文件，所以 token 直接挂在 `:root` 上。

### 2.1 颜色 — 中性

| Token | Hex | 用途 |
|-------|-----|------|
| `--bg-0` | `#0B0E12` | 页面底色（最暗），KPI 卡间缝隙 |
| `--bg-1` | `#11151B` | 卡片 / hero / KPI / evidence 背景 |
| `--bg-2` | `#161B23` | 凹槽、outreach 背景、input 背景、kicker 条 |
| `--bg-3` | `#1D2330` | 浮层、toast、button hover、priority 背景环 |
| `--line-1` | `#1F2530` | 一级分隔（极少使用） |
| `--line-2` | `#2A3140` | 二级分隔（evidence 左 border、input border） |
| `--line-3` | `#3A4356` | 三级分隔（按钮描边） |
| `--ink-1` | `#E8ECF1` | 主文本 |
| `--ink-2` | `#9AA4B2` | 次要文本 |
| `--ink-3` | `#5C6675` | 辅助文本、placeholder |
| `--ink-4` | `#3F4754` | 极弱文本（disabled 标签） |

### 2.2 颜色 — 语义

| Token | Hex | 含义 |
|-------|-----|------|
| `--accent` | `#D4A574` | 主强调（域名高亮、CTA、品牌色、关键数字） |
| `--accent-soft` | `#E5C399` | 强调 hover / 弱化 |
| `--accent-ink` | `#1A1410` | 强调色上的文字（深色，确保 4.5:1 对比度） |
| `--good` | `#6FCF97` | 成功 / 已批准 / 批准按钮 |
| `--warn` | `#E8B547` | 警告 / 待审 / 推迟按钮 |
| `--bad` | `#E07B7B` | 失败 / 拒绝 / 严重告警 |
| `--info` | `#7BB8FF` | 信息 / cert evidence / 规则触发 |
| `--outreach` | `#7FD4C0` | 触达 / mint（去糖绿）/ early evidence |
| `--muted` | `#6F7A89` | 中性 / complete evidence / blocklist |

### 2.3 字体

```css
--font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
             "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
--font-mono: ui-monospace, "SF Mono", "JetBrains Mono", "Cascadia Code",
             Consolas, monospace;
```

**字号刻度（v2）** — 数字更激进，让 KPI / hero 真的撑得起呼吸：

| Token | px | 行高 | 用例 |
|-------|----|----|------|
| `--fs-hero` | 48 | 1.05 | HERO 域名 |
| `--fs-kpi` | 56 | 1.0 | KPI 主数字 |
| `--fs-gauge` | 32 | 1.0 | 环形仪表中心分数 |
| `--fs-h2` | 16 | 1.35 | 区块标题、按钮 |
| `--fs-body` | 14 | 1.55 | 正文 |
| `--fs-meta` | 12 | 1.45 | 辅助说明、label |
| `--fs-micro` | 11 | 1.4 | 时间戳、键盘提示、kicker |

字重只用 400 / 500 / 600。**永不** 用 700+（粗体在小字号下糊）。

### 2.4 间距 / 圆角 / 边框

```
--space-1: 4px    --space-5: 20px
--space-2: 8px    --space-6: 28px
--space-3: 12px   --space-7: 40px
--space-4: 16px   --space-8: 56px

--r-sm: 4px    --r-md: 8px    --r-lg: 10px    --r-pill: 999px

--line: 1px    永远不出现 2px 边框（用阴影或双层结构代替粗细）
```

### 2.5 动效

```
--ease-out: cubic-bezier(0.2, 0.7, 0.2, 1)
--ease-in-out: cubic-bezier(0.4, 0, 0.2, 1)
--dur-fast: 120ms     /* hover、focus */
--dur-base: 200ms     /* 状态切换 */
--dur-slow: 320ms     /* 新数据 fade-in */
```

**原则**：`transform` 与 `opacity` 优先；永远不动 `width/height/top/left`。
**例外**：环形 SVG 的 `stroke-dashoffset` 动效是允许的，因为不是 layout 触发的。

---

## 3 · 信息架构（v2 重设计）

### 3.1 页面分 5 个区

```
┌─────────────────────────────────────────────────────────────────────────┐
│ TOPBAR   品牌 · 状态 · 语言 · Reviewer ID                                  │ 48px
├──────────────────────────────────┬──────────────────────────────────────┤
│                                  │   KPI STRIP                           │
│   HERO                           │   ┌──────┐ ┌──────┐ ┌──────┐         │
│   domain 48px mono · accent      │   │ 1,284 │ │ 312  │ │ 247  │         │
│   candidate v3 · chip            │   └──────┘ └──────┘ └──────┘         │
│   publishable_ai_saas            │   signals   domains  probes           │  tall
│   canonical_url                  │   ▁▂▃▅▆▇    ▁▂▃▅     ▁▂▃▅▆         │  right
│   description                    │   ┌──────┐ ┌──────┐ ┌──────┐         │  column
│                                  │   │  48  │ │   9  │ │   6  │         │
│                                  │   └──────┘ └──────┘ └──────┘         │
│                                  │   candidates approved outreach        │
├──────────────────────────────────┴──────────────────────────────────────┤
│   ◉ PRODUCT EVIDENCE     │    ◉ PRIORITY (环形 SVG 仪表, 120px)         │
│   ────────────────       │    ─────────────                              │
│   [evidence rows]        │    score 0.812                                │
│                          │    product 0.28 · early 0.21 · exposure 0.18  │
│                          │    complete 0.15 · formula v3 · conf 0.87     │
│                          │    primary outcome: publishable_ai_saas      │
├─────────────────────────────────────────────────────────────────────────┤
│   ACTIONS  [✓ Approve (A)]  [✕ Reject (R)]  [⏸ Defer (D)]  [⊘ Blocklist (B)]│
│                                                          [Edit (E)]      │
└─────────────────────────────────────────────────────────────────────────┘

   ASIDE (在 main 之下) — 1 张 tabbed console 卡
   ┌─ [DISCOVERY] [STATS] [ALERTS] ──────────────────────────────────────┐
   │  (active tab content)                                              │
   └────────────────────────────────────────────────────────────────────┘
```

- **桌面**（≥ 1280px）：顶部 hero 1.5fr / kpi 1fr 并排；下方 evidence 1.4fr / 仪表 1fr 并排；aside console 折到 main 下方
- **平板**（768–1280px）：顶部不变；下方 evidence + 仪表堆叠；aside 仍折在底部
- **窄屏**（< 768px）：全部堆叠，KPI 2 列

### 3.2 视觉重量梯度（v2）

按降序：

1. **域名**（HERO 区）— 48px mono / `--accent`，占据页面第一焦距
2. **KPI 主数字**（右侧 tall column）— 56px mono / `--ink-1`，配合 sparkline 暗示趋势
3. **环形优先级仪表**（evidence 旁）— 120px 直径 SVG，分项颜色环围绕中央大数字
4. **决策按钮**（底部 actions）— 主色实色按钮 + 16px mono 快捷键徽章
5. **证据 / aside / 告警** — 默认 body / meta 字号

---

## 4 · 各区域详细规范（v2 重设计）

### 4.1 TOPBAR（48px，sticky）

```
┌─────────────────────────────────────────────────────────────────────────┐
│ ◉ DomainHunter · loopback        ● live         [中] [EN]   reviewer ▢  │
└─────────────────────────────────────────────────────────────────────────┘
```

- **高度**：48px（v1 是 56px）— 越薄越好，让 hero 占满第一焦距
- **左**：品牌 14px mono + 环境 chip（`loopback` / `本地`），环境 chip 走 `--bg-2` 凹槽色，无边框
- **中**：8px 状态灯 + 状态文本，文本用 `--ink-2` / 12px
- **右**：语言切换（中 / EN 二选一，激活态用 `--accent` 实色），reviewer 输入框 width 144px
- **下分隔**：从 1px hairline 改为 `backdrop-filter: blur(8px)` 的 frosted glass，背景色 `--bg-0` 85% alpha

### 4.2 HERO 区（左上，主视觉锚）

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   krea.ai                                                           │
│   ────                                                              │
│   candidate v3 · human  ·  ✓ approved this session (chip)           │
│                                                                     │
│   primary_outcome:  publishable_ai_saas                             │
│                                                                     │
│   canonical_url   https://krea.ai                                   │
│                                                                     │
│   "Krea is an AI design tool that turns sketches into…"            │
│                                                                     │
│   ←  prev                                              next  →      │
│   position: 3 of 12                                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

- **域名**：48px / `var(--font-mono)` / `font-weight:500` / `letter-spacing:-0.02em`
- **域名色**：默认 `--ink-1`；已批准时 `--good`；评审优先级 ≥ 0.7 时 `--accent`
- **域名下分隔**：1px `--accent` 的 56px 宽短下划线（不是 hairline border，是 accent brand mark）
- **meta 行**：12px mono `--ink-2`，形如 `candidate v3 · human · approved this session`
- **chip 集**：v3 chip / outcome chip / approved chip 横向排布，间距 8px
- **canonical**：13px mono `--ink-3`，用 `--line-2` 作左边框、内左边距 12px 的「console 引文」样式
- **description**：14px `--ink-2`，最多 3 行（`-webkit-line-clamp: 3`），溢出省略
- **prev / next**：底部一行，文字按钮 + mono 快捷键徽章（← / →）
- **空状态时**：整个 hero 区变成大空状态（居中 + 24px 标题 + 运行发现按钮）

### 4.3 KPI STRIP（右上，tall column）

**形态**：不再是横向 6 卡；改为 **2 列 3 行** tall column，每行一个 KPI，**占据与 hero 相同高度**。

```
┌───────────────────────────────────────────────┐
│  SIGNALS                          +12 /1h     │
│  1,284                                  ▲     │
│  ▁▂▃▅▆▇ (sparkline, 7 段)                  │
├───────────────────────────────────────────────┤
│  DOMAINS                            +9 /1h   │
│  312                                    ▲    │
│  ▁▂▃▅▆▇                                   │
├───────────────────────────────────────────────┤
│  PROBES                              89% ok  │
│  247                                          │
│  ▁▂▃▅▆▇                                       │
├───────────────────────────────────────────────┤
│  CANDIDATES                                   │
│  48                                           │
│  ▁▂▃▅▆▇                                       │
├───────────────────────────────────────────────┤
│  APPROVED                                     │
│  9                                            │
│  ▁▂▃▅▆▇                                       │
├───────────────────────────────────────────────┤
│  OUTREACH                                     │
│  6                                            │
│  ▁▂▃▅▆▇                                       │
└───────────────────────────────────────────────┘
```

- **每个 KPI 卡**：无边框、纯 `--bg-1` 背景色块，**卡之间用 `--bg-0` 页底色 1px 缝隙**（这是 hairline border 的替代品）
- **主数字**：56px / `var(--font-mono)` / `font-weight:500` / `--ink-1` / `font-variant-numeric: tabular-nums`
- **副文本（label）**：11px / 大写 / `letter-spacing: 0.08em` / `--ink-2` / mono
- **右上 delta**：12px / `--ink-3` 或语义色（+`--good` / -`--bad`）
- **sparkline**：内联 SVG `<polyline>`，颜色 `--accent-soft` 60% alpha，高度 24px，宽度撑满卡
- **数字变化动效**：fade-out 120ms → 新数字 → fade-in 200ms（与 v1 一致）
- **hover**：背景从 `--bg-1` → `--bg-2`，过渡 120ms（不是 border 变化）

### 4.4 PRODUCT EVIDENCE（main 左下）

**形态**：占 main 的 1.4fr 列，从左侧入卡，证据列表垂直排布。

- **标题**：11px mono `--ink-2` 「PRODUCT EVIDENCE」，下方 1px `--accent` 短下划线 32px
- **空状态**：「暂无引用证据。」，14px `--ink-3`
- **每条证据**：背景 `--bg-2`，左边 3px 边框（按类型染色）
  - `cert_transparency` → `--info`
  - `meta_tag` → `--accent`
  - `heading` → `--outreach`
  - `body` / `cta` → `--ink-3`
- **kicker**：11px mono 大写（`CERT · ISSUED 2026-08-30 BY LET'S ENCRYPT`）
- **quote**：14px / `--ink-1` / 行高 1.55
- **source**：11px mono `--ink-3`，溢出截断
- **最大高度**：max-height: 360px + 内部滚动条，避免 evidence 多时把仪表挤变形

### 4.5 PRIORITY GAUGE（main 右下，环形 SVG）

**形态**：120px 直径的 SVG 圆环，4 段颜色环拼成完整环，中央放分数。

```
        ┌────────────────┐
        │      0.812      │   ← 32px mono / --accent / tabular-nums
        │      score      │   ← 11px mono / --ink-3 / 大写
        └────────────────┘

  ┌────────────────────────────────────────┐
  │   product   0.28   ████████░░░░░░░░░   │   ← bar 4px
  │   early     0.21   ██████░░░░░░░░░░░   │
  │   exposure  0.18   █████░░░░░░░░░░░░░  │
  │   complete  0.15   ████░░░░░░░░░░░░░░  │
  └────────────────────────────────────────┘

  formula v3 · confidence 0.87
  primary outcome: publishable_ai_saas
```

- **环形**：4 个 25% 弧段拼成完整 360°，每段颜色 = 该分项的语义色（product=accent / early=outreach / exposure=info / complete=muted）
- **背景环**：在彩色环下方 6px 偏移画一条 `--bg-3` 完整环，凸显彩色段
- **数字动效**：score 变化时数字 fade-in（不是整环旋转，节省 CPU）；弧段长度用 `stroke-dashoffset` 过渡 320ms
- **分项 bars**：4 行 grid，每行 `label | bar | num` 三栏对齐；bar 高 4px，bg `--bg-3`，fg 用对应分项色
- **meta 行**：12px `--ink-2`，`formula v3 · confidence 0.87` + `primary outcome: publishable_ai_saas`

### 4.6 ACTIONS（main 底部）

```
[ ✓ APPROVE  A ]  [ ✕ REJECT  R ]  [ ⏸ DEFER  D ]  [ ⊘ BLOCKLIST  B ]                            [ ✎ EDIT  E ]
```

- **横向 5 按钮**：前 4 个等宽（flex:1），Edit 固定宽度 120px，靠右
- **实色按钮**：approve = `--good`、reject = `--bad`、defer = `--warn` 描边、blocklist = `--muted` 描边、edit = `--info` 描边
- **快捷键徽章**：按钮内右角 16px mono 徽章，深色色阶背景 + 主按钮色文字
- **按钮高度**：48px（v1 是 40px），按 SaaS dashboard 标准
- **pending 态**：所有按钮 disabled + 顶部 2px `--accent` 进度条 + 320ms 横扫动画

### 4.7 OUTREACH PANEL（仅 approved 后展开，actions 之下）

- 整块用 `--bg-2` 凹槽背景，与 actions 拉开层级
- URL 输入框占满一行（mono / 13px），Dry-run / Real run 两个按钮并排
- 历史记录：12px mono `--ink-2`，每行 `chip dry|real` + `时间 · 联系人 · token`

### 4.8 ASIDE CONSOLE（main 之下，1 张 tabbed 卡）

**形态**：4 张 aside 卡合并为 1 张「CONSOLE」卡，3 个 tab 切换。

```
┌─ [DISCOVERY]  [STATS]  [ALERTS] ─────────────────────────────────────┐
│                                                                      │
│   (active tab content)                                               │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

- **顶部 tab nav**：左 3 个 tab，激活 tab 用 `--accent` 短下划线（不是边框色变化）+ `--ink-1` 文字；非激活 `--ink-3`
- **tab 切换**：纯前端 `display: none/block`，无路由
- **DISCOVERY tab**：保留 v1 discovery 内容（max-probes 输入 + Run 按钮 + summary + 域名列表）
- **STATS tab**：原 ANALYTICS 2×2 网格（signal→candidate / cost / P50 / P95）
- **ALERTS tab**：告警列表 + 末尾一行 runbook 提示（合并 v1 的 alerts + runbook）
- **高度**：tabs 切时不重新计算高度，统一 max-height: 480px + 内滚动

### 4.9 EMPTY STATE

队列空时，整张 hero 区变成一个大空状态（占满 main + KPI strip 区域）：

- 居中，垂直 / 水平 padding 80px
- 大字 `当前没有待审候选`（24px mono `--ink-1`）
- 副提示 `全部处理完毕，可运行发现或等待新信号。`（14px `--ink-2`）
- 「▶ 运行发现」按钮（主色实色）
- KPI strip 仍展示数字（不为空）

---

## 5 · 状态矩阵

### 5.1 REVIEW 卡状态

| 状态 | 视觉差异 |
|------|---------|
| **Loading** | 骨架屏（不是 spinner）：3 行灰色条 + 占位文本 |
| **Empty**（队列空） | hero 区变成 §4.9 大空状态，KPI strip 仍展示数字 |
| **Loaded** | 完整视图（hero + evidence + 仪表 + actions） |
| **Approved (this session)** | 域名 `--good` 色 + 追加绿色 chip；OUTREACH 区块展开；A 按钮被自身替换为禁用「已批准 ✓」 |
| **Pending**（请求中） | 所有按钮 `disabled` + 顶部进度条（`--accent` 2px 横线，320ms 进度动画） |
| **Error** | 顶部 toast 红色 + 当前卡片保留 + 「重试」按钮 |

### 5.2 KPI 数字变化动效

- 数字相同时不刷新（diff）
- 数字增大：fade-out 120ms → 数字 +1 → fade-in 200ms
- 数字减小：同
- 「+N /1h」副文本用 `+` 用 `--good`，`-` 用 `--bad`，静态显示

### 5.3 Discovery 按钮运行态

- Idle：`▶ 运行`
- Running：`◌ 正在运行…`（按钮变 `--bg-3`，无旋转图标，只有文字暗示）
- Done：`✓ 完成` 320ms 后回 idle
- Error：`! 失败` 红色描边 + toast

---

## 6 · 交互细节

### 6.1 键盘矩阵（**不可破坏**）

| 键 | 行为 | 冲突解决 |
|----|------|---------|
| `A` | approve | 仅当焦点不在 input/textarea/select 时触发 |
| `R` | reject | 同上 |
| `D` | defer | 同上 |
| `B` | blocklist | 同上 |
| `E` | edit（no-op 占位） | 同上 |
| `O` | outreach（dry） | 仅当当前卡已批准 |
| `J` / `←` | 上一张 | 同上 |
| `K` / `→` | 下一张 | 同上 |
| `Esc` | blur 当前 input | 不冒泡到全局 |
| `Tab` | 浏览器默认 | — |
| `Cmd/Ctrl + Enter` | 在 actor input 中 → 跳到第一条 | 提升可用性 |

**冲突解决的关键**：监听器最前面一行 `if (target is INPUT/TEXTAREA/SELECT) return`；当前事件本身是 Esc 的话也允许 blur。

### 6.2 焦点环

- 全部可交互元素必须可见焦点环
- 颜色 `--accent`，`outline: 2px solid var(--accent); outline-offset: 2px;`
- 不使用 `outline: none` 一刀切

### 6.3 Hover

- 按钮 hover：背景色 `--bg-2 → --bg-3`，过渡 120ms
- 链接 hover：颜色 `--accent → --accent-soft`
- 卡片 hover：**不变化**（不是导航元素，hover 应被禁用）

### 6.4 触摸目标

- 决策按钮在窄屏（< 720px）改为垂直堆叠，padding 14px 18px（保证 44px+ 高度）

---

## 7 · 可访问性

| 项 | 要求 |
|----|------|
| 色彩对比 | 所有正文 ≥ 4.5:1；强调色文字 ≥ 4.5:1 |
| 语义 HTML | `<button>` / `<a>` / `<main>` / `<aside>` / `<section>` 正确使用 |
| aria-label | 决策按钮带 `aria-keyshortcuts="A"`；图标按钮带 `aria-label` |
| 减少动画 | 监听 `prefers-reduced-motion: reduce`，禁用所有非必要 transition |
| 焦点顺序 | DOM 顺序与视觉顺序一致 |

---

## 8 · 性能约束

| 项 | 约束 |
|----|------|
| HTML 体积 | 单页 < 50KB（gzip 前），目前约 32KB |
| 字体 | 系统字体栈，零下载 |
| 图片 | 无（一切用 SVG 内联或 CSS） |
| JS 体积 | 内联，无依赖 |
| 重渲染 | KPI 数字 3 秒轮询，其余按事件触发 |
| 首屏可交互 | < 200ms（本地服务，无网络字体阻塞） |

---

## 9 · 与现有功能的兼容矩阵

| 现有功能 | 新设计如何呈现 |
|---------|---------------|
| 中/英 i18n 切换 | 保留 `I18N` 字典；DOM `data-i18n` 标注不变 |
| Reviewer localStorage 持久化 | 不变 |
| Approved/Outreach 历史 localStorage | 不变 |
| 快捷键 A/R/D/B/E/O/J/K/←/→ | 不变（§6.1） |
| `/healthz` 轮询 | 新增（状态指示灯） |
| 现有的 `fmtLatency` | 不变 |
| Esc blur 处理 | 不变 |
| 4 张 aside 卡 → 1 张 tabbed console | 内容合并，DOM 结构变化，**功能不变** |

**没有任何 API 端点变化**。前端是纯展示升级。

---

## 10 · 验收清单（开发后自检）

- [ ] 顶栏 sticky 48px，frosted glass 下分隔
- [ ] HERO 域名 48px mono，下方 56px accent 短下划线
- [ ] KPI 6 个数字 56px，每行 1 个 + sparkline 7 段
- [ ] PRIORITY 是 SVG 环形仪表（不是横 bar），中心分数 32px
- [ ] Aside 是 1 张 tabbed console，3 tab 切换
- [ ] 切换语言，所有文本跟随
- [ ] 快捷键 A/R/D/B/E/O/J/K/←/→ 全部生效
- [ ] 在 actor input 输入字符时，按 a/r 不会触发决策（焦点保护）
- [ ] 按 ESC 退出 input，焦点回到 body，可继续用快捷键
- [ ] 一键发现：运行 → 状态从 idle → running → done 切换正确
- [ ] 决策请求 pending 时，按钮全部 disabled
- [ ] 决策成功 → 队列前进 + 数字刷新 + toast 出现
- [ ] 队列空 → 显示 empty state（不是空白）
- [ ] 1200px / 800px / 400px 三个断点均无横向滚动
- [ ] dark mode 强制生效（系统浅色时也是深色）
- [ ] 所有 token 都在 `:root` 中声明，CSS 中无硬编码色
- [ ] HTML 字符串仍可被 FastAPI 直接返回（不引入新文件）

---

## 11 · 不做的事（明确范围）

- ❌ 切换浅色主题（dark only）
- ❌ 引入 web font / Tailwind / 任何 CDN
- ❌ 多页面路由（保持单 HTML）
- ❌ WebSocket 实时推送（仍走 3s 轮询）
- ❌ 拖拽排序、虚拟滚动（数据量小，没必要）
- ❌ 重写后端 API

---

## 12 · 实现步骤（v2）

1. 在 `:root` 重写 token（§2）：新增 `--fs-hero 48px`、`--fs-kpi 56px`、`--fs-gauge 32px`；删 `--fs-display 32px`、`--fs-h1 22px`
2. 重写 base layout：topbar(48px) / [hero 1.5fr + kpi strip 1fr] / [evidence 1.4fr + gauge 1fr] / actions / tabbed aside
3. 实现 HERO 区（域名 + meta + canonical + description + nav）
4. 实现 KPI STRIP（tall column，每行带 sparkline）
5. 实现环形 SVG 仪表（4 段弧 + 中心数字 + 分项 bars）
6. 实现 tabbed aside console（3 个 tab + 共享 max-height）
7. 接入 `/healthz` 探活，更新 TOPBAR 状态灯
8. 行为不变性回归：快捷键、i18n、API 调用、localStorage 全部保留
9. 浏览器打开 `http://127.0.0.1:8000/`，按 §10 验收清单逐项核
10. 在三个断点（1280 / 900 / 420）截图存档

---

**审阅请关注**：

- 方向 §1.1 是否对（「贵」感而非「炫」感）
- v2 改造 §0 的「v1 → v2」对照表，是否真的消除了 reskin 痕迹
- 信息架构 §3 的 hero + kpi 顶部并排是否成立（vs v1 的 kpi 横排 + review 主体）
- 环形仪表 §4.5 是否足够「仪表盘」感
- 还有什么**应该砍掉**或**应该加上**的区