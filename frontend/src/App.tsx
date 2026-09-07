import React, { useState, useEffect, useMemo, useCallback } from "react";
import {
  Search,
  ExternalLink,
  ArrowRight,
  RotateCw,
  CheckCircle2,
  AlertCircle,
  HelpCircle,
  XCircle,
  ShieldAlert,
  ChevronDown,
  ArrowLeft,
  X,
  Copy,
  Check,
  ShieldCheck,
  Sun,
  Moon,
  Languages,
  Terminal,
  Activity,
  Cpu,
  CornerDownRight,
  Sliders,
  CheckCheck
} from "lucide-react";

// ==========================================
// 1. 国际化字典
// ==========================================

export type Language = "zh" | "en";
export type Theme = "light" | "dark";

const DICT = {
  zh: {
    brandSubtitle: "网络侦测控制台",
    scanBtn: "触发探针轮询",
    runtimeLog: "运行日志",
    runtimeLogEmpty: "discover 未在运行或暂无日志",
    topPrioritySection: "CRITICAL TARGET // 优先侦查标的",
    otherCandidatesSection: "ACTIVE QUEUE // 待裁定流",
    priorityPrefix: "SCORE",
    priorityValue: "优先级",
    searchPlaceholder: "过滤域名、指标或产品特征... (按 / 聚焦)",
    inboxCount: "{n} 个待决标的正在队列",
    inboxSubtitle: "已配置 CT 日志 · 严格根域准入 · 可执行连通性核验",
    pipelineGate: "STRICT_GATE_ENABLED",
    newnessFact: "CT 首次见网",
    reachabilityFact: "HTTP/TLS 状态",
    viewEvidence: "打开诊断卷宗",
    backToInbox: "返回中控清册",
    visitSite: "外网握手",
    pendingStatus: "UNRESOLVED",
    whyInboxHeading: "INGESTION TELEMETRY // 准入遥测链",
    newnessEvidenceHeading: "证书链捕获事实",
    reachabilityEvidenceHeading: "端点探针遥测",
    observedAt: "捕获时间: ",
    productAssessmentHeading: "INFERENCE // 模型意图判定",
    classificationConclusion: "分类标签: ",
    confidence: "置信权重: ",
    viewAuditData: "查看原始探针字节报文 (RAW LOG)",
    reviewPanelTitle: "DECISION DESK // 审核指令台",
    currentReviewer: "OPERATOR:",
    changeReviewer: "切换",
    firstReviewPrompt: "操作员标识未配置",
    deferBtn: "暂缓 [D]",
    rejectBtn: "剔除 [R]",
    blocklistBtn: "拉黑 [B]",
    approveBtn: "通过核验 [A]",
    submitting: "COMMITTING...",
    approving: "STAMPING...",
    emptyTitle: "队列全部清空 (PIPELINE IDLE)",
    emptyDesc: "当前无挂起待审目标。可随时开始一次严格的 CT 实时扫描。",
    scanModalTitle: "DISPATCH PROBES // 启动严格网络探针",
    scanModalDesc: "从已配置的公共证书透明度日志拉取新记录，并对根域执行严格的新网站性、解析与可访问性核验。",
    scanRuleHeading: "硬件级过滤网关已常驻",
    scanRuleDesc: "强制剔除通配符泛停放、CDN 默认模板页及跨域 Redirect 链。",
    cancel: "取消",
    startScan: "开始探测",
    scanningTitle: "正在与边缘分布式探针同步数据...",
    scanningDesc: "正在执行 TLS 1.3 真实握手与 HTTP 存活性测试",
    scanSuccess: "PROBE COMPLETED // 探针成功生成凭据",
    scanQueued: "PROBE QUEUED // 严格核验仍在队列中",
    scanNoCandidates: "PROBE COMPLETE // 本轮没有通过严格准入的候选",
    scanFailed: "PROBE TERMINATED // 探测被物理中断",
    statSampled: "采样信号",
    statDomains: "排重根域",
    statStrictRejections: "严格过滤",
    statCreated: "准入建档",
    statPendingWork: "待续处理",
    queuedWork: "本轮仍有 {n} 个根域等待严格核验；再次扫描会从队列继续。",
    noCandidatesDesc: "本轮信号已完成严格筛选，没有任何域名被当作候选推入审核队列。",
    partialSourceFailure: "部分 CT 来源未响应；其余健康来源已继续处理。",
    continueScan: "继续扫描",
    viewNewCandidates: "检阅新资产",
    retry: "重新轮询",
    actorModalTitle: "配置操作员标识 (OPERATOR ID)",
    actorModalDesc: "该签名将作为不可篡改的操作员指纹写入本地日志审计链。",
    actorPlaceholder: "例如：operator_07",
    confirm: "确认签名",
    rejectConfirmTitle: "确认从活跃队列剔除标的？",
    blocklistConfirmTitle: "确认执行全局黑名单阻断？",
    rejectConfirmDesc: "该资产将标记为 REJECTED，后续不再作为有效候选推入中控台。",
    blocklistConfirmDesc: "该操作将在所有底层拉取流中全局静默 Drop 该域名及其全部子域。",
    executeBtn: "确认下发",
    statusPassed: "PASS",
    statusFailed: "FAIL",
    statusUnknown: "UNCHECKED"
  },
  en: {
    brandSubtitle: "Discovery Console",
    scanBtn: "Dispatch CT Probe",
    runtimeLog: "Runtime log",
    runtimeLogEmpty: "discover is not running or no log yet",
    topPrioritySection: "CRITICAL TARGET // Priority Triage",
    otherCandidatesSection: "ACTIVE QUEUE // Ingestion Stream",
    priorityPrefix: "SCORE",
    priorityValue: "Priority",
    searchPlaceholder: "Filter by apex, metrics, or intent... (Press / to focus)",
    inboxCount: "{n} targets pending review",
    inboxSubtitle: "Configured CT Logs · Strict Ingress Rules · Reachability Checks Available",
    pipelineGate: "STRICT_GATE_ENABLED",
    newnessFact: "CT Genesis",
    reachabilityFact: "HTTP/TLS State",
    viewEvidence: "Open Telemetry",
    backToInbox: "Back to Queue",
    visitSite: "Connect Apex",
    pendingStatus: "UNRESOLVED",
    whyInboxHeading: "INGESTION TELEMETRY // Trace History",
    newnessEvidenceHeading: "Certificate Lineage",
    reachabilityEvidenceHeading: "Endpoint Viability",
    observedAt: "Timestamp: ",
    productAssessmentHeading: "INFERENCE // Payload Diagnostics",
    classificationConclusion: "Verdict: ",
    confidence: "Weight: ",
    viewAuditData: "Inspect Raw Probe Payload (RAW LOG)",
    reviewPanelTitle: "DECISION DESK // Action Terminal",
    currentReviewer: "OPERATOR:",
    changeReviewer: "Switch",
    firstReviewPrompt: "Operator Unset",
    deferBtn: "Defer [D]",
    rejectBtn: "Reject [R]",
    blocklistBtn: "Blocklist [B]",
    approveBtn: "Authorize [A]",
    submitting: "COMMITTING...",
    approving: "STAMPING...",
    emptyTitle: "PIPELINE IDLE // Queue Clear",
    emptyDesc: "No unresolved candidates in stream. Start a strict live CT scan at any time.",
    scanModalTitle: "DISPATCH PROBES // Edge Discovery",
    scanModalDesc: "Polls new records from configured public Certificate Transparency logs and strictly verifies age, DNS, and apex reachability.",
    scanRuleHeading: "Hardware-level Gate Enabled",
    scanRuleDesc: "Instantly drops wildcard parking, CDN template pages, and redirect loops.",
    cancel: "Cancel",
    startScan: "Start Probe",
    scanningTitle: "Synchronizing socket events with edge probe nodes...",
    scanningDesc: "Executing TLS 1.3 handshakes without synthetic latency",
    scanSuccess: "PROBE COMPLETED // Signals Filed",
    scanQueued: "PROBE QUEUED // Strict checks remain in queue",
    scanNoCandidates: "PROBE COMPLETE // No candidate passed strict admission",
    scanFailed: "PROBE TERMINATED // Socket Fault",
    statSampled: "Sampled",
    statDomains: "Unique Apex",
    statStrictRejections: "Strict Filtered",
    statCreated: "Ingested",
    statPendingWork: "Pending Work",
    queuedWork: "{n} apex domains still await strict verification. The next scan resumes this queue.",
    noCandidatesDesc: "This run completed strict screening; no domain was admitted to the review queue.",
    partialSourceFailure: "Some CT sources did not respond; healthy sources continued processing.",
    continueScan: "Continue Scan",
    viewNewCandidates: "Inspect Targets",
    retry: "Retry Probe",
    actorModalTitle: "Set Operator Handle (OPERATOR ID)",
    actorModalDesc: "Fingerprint will be stamped irrevocably into the local audit trail.",
    actorPlaceholder: "e.g. operator_07",
    confirm: "Authorize Handle",
    rejectConfirmTitle: "Purge candidate from active stream?",
    blocklistConfirmTitle: "Apply global drop rule for apex?",
    rejectConfirmDesc: "Target marked as REJECTED and retired from triage stream.",
    blocklistConfirmDesc: "Silently drops all ingestion events matching this apex domain across workers.",
    executeBtn: "Execute Action",
    statusPassed: "PASS",
    statusFailed: "FAIL",
    statusUnknown: "UNCHECKED"
  }
};

// ==========================================
// 2. 类型定义与真实模拟数据
// ==========================================

export type ReviewState = "pending" | "approved" | "deferred" | "rejected" | "blocklisted";
export type EvidenceStatus = "passed" | "unknown" | "failed";

export interface EvidenceItem {
  category: "newness" | "reachability" | "product" | "classification";
  status: EvidenceStatus;
  title: string;
  explanation: string;
  observedAt?: string;
  sourceUrl?: string;
  quote?: string;
}

export interface CandidateSummary {
  id: string;
  version: number;
  domain: string;
  displayName?: string;
  canonicalUrl?: string;
  reviewState: ReviewState;
  verdict: {
    label: string;
    confidence?: number;
  };
  priority: number;
  newness: {
    status: EvidenceStatus;
    summary: string;
  };
  reachability: {
    status: EvidenceStatus;
    summary: string;
  };
  productSummary?: string;
  updatedAt: string;
}

export interface CandidateDetail extends CandidateSummary {
  evidence: EvidenceItem[];
  audit?: {
    sourceEvents: unknown[];
    observations: unknown[];
    decisionHistory: unknown[];
  };
}


export type ReviewAction = "approve" | "defer" | "reject" | "blocklist";
const INITIAL_DATA: CandidateDetail[] = [
  {
    id: "NODE-8812-LIMITLESS",
    version: 1,
    domain: "limitlessrouter.com",
    displayName: "Limitless Router",
    canonicalUrl: "https://limitlessrouter.com",
    reviewState: "pending",
    verdict: { label: "AI INFRASTRUCTURE", confidence: 0.88 },
    priority: 0.71,
    newness: {
      status: "passed",
      summary: "CT FIRST-SEEN 2h ago · RDAP 2026-09-02"
    },
    reachability: {
      status: "passed",
      summary: "HTTP 200 OK · RTT 82ms · APEX VERIFIED"
    },
    productSummary: "On-premise edge gateway for intelligent LLM routing, distributed load shedding, and latency optimization across 20+ regions.",
    updatedAt: "2026-09-03T10:14:20Z",
    evidence: [
      {
        category: "newness",
        status: "passed",
        title: "CT Log Genesis Ingestion",
        explanation: "Cert recorded in Google Argon2026 log. No precursor serials found in CT search trees.",
        observedAt: "2026-09-03T08:12:00Z"
      },
      {
        category: "newness",
        status: "passed",
        title: "RDAP Fresh Registration Age",
        explanation: "Registrar confirmed as Porkbun LLC. Timestamp 2026-09-02T19:04:11Z.",
        observedAt: "2026-09-03T08:15:22Z"
      },
      {
        category: "reachability",
        status: "passed",
        title: "Direct Apex Consistency & TLS 1.3",
        explanation: "GET / responded with 200 OK. TLS ALPN h2 negotiated. No third-party redirect or parking banner signatures.",
        sourceUrl: "https://limitlessrouter.com"
      },
      {
        category: "product",
        status: "passed",
        title: "Developer Intent & Swagger Endpoints",
        explanation: "Parsed public API pricing table and OpenAPI JSON schemas. High-probability B2B developer tool.",
        quote: "Limitless Router: Intelligently proxy and throttle LLM traffic across 20+ edge regions."
      },
      {
        category: "classification",
        status: "unknown",
        title: "Entity Ownership Verification",
        explanation: "WHOIS privacy shielded. Footer lacks formal corporate registry identifiers.",
        observedAt: "2026-09-03T08:20:00Z"
      }
    ],
    audit: {
      sourceEvents: [{ type: "ct_stream", id: "evt_991823", log: "argon2026" }],
      observations: [{ probe: "http_get", rtt_ms: 82, tls_version: "TLSv1.3", cipher: "TLS_AES_128_GCM_SHA256" }],
      decisionHistory: []
    }
  },
  {
    id: "NODE-4209-SYNTHFLOW",
    version: 1,
    domain: "synthflow-lab.io",
    displayName: "SynthFlow Studio",
    canonicalUrl: "https://synthflow-lab.io",
    reviewState: "pending",
    verdict: { label: "AI WORKFLOW SANDBOX", confidence: 0.65 },
    priority: 0.58,
    newness: {
      status: "passed",
      summary: "CT FIRST-SEEN 5h ago · NO HISTORICAL DNS"
    },
    reachability: {
      status: "passed",
      summary: "HTTP 200 OK · RTT 124ms · APEX MATCH"
    },
    productSummary: "Visual DAG canvas for real-time model output comparison, cost tracking, and collaborative prompt debugging.",
    updatedAt: "2026-09-03T06:40:11Z",
    evidence: [
      {
        category: "newness",
        status: "passed",
        title: "Let's Encrypt Log Emission",
        explanation: "Fresh root certificate logged. DNS A record resolved with zero historical DNS mutations.",
        observedAt: "2026-09-03T06:00:00Z"
      },
      {
        category: "reachability",
        status: "passed",
        title: "WASM Bundle Execution",
        explanation: "Single-page application loaded and mounted WebAssembly evaluation sandbox cleanly.",
        sourceUrl: "https://synthflow-lab.io"
      }
    ]
  }
];

let globalStore = [...INITIAL_DATA];

const api = {
  async fetchCandidates(): Promise<CandidateSummary[]> {
    await new Promise((r) => setTimeout(r, 220));
    return globalStore.filter((c) => c.reviewState === "pending");
  },
  async getCandidate(id: string): Promise<CandidateDetail> {
    await new Promise((r) => setTimeout(r, 160));
    const item = globalStore.find((c) => c.id === id);
    if (!item) throw new Error("NODE_ID_NOT_FOUND");
    return { ...item };
  },
  async submitReview(id: string, action: ReviewAction, actorId: string, version: number): Promise<void> {
    await new Promise((r) => setTimeout(r, 260));
    const target = globalStore.find((c) => c.id === id);
    if (!target) throw new Error("TARGET_DROPPED");
    if (target.version !== version) {
      throw new Error("MUTATION_CONFLICT: TARGET UPDATED CONCURRENTLY");
    }
    const stateMap: Record<ReviewAction, ReviewState> = {
      approve: "approved",
      defer: "deferred",
      reject: "rejected",
      blocklist: "blocklisted"
    };
    target.reviewState = stateMap[action];
    target.version += 1;
    target.updatedAt = new Date().toISOString();
  }
};

type BackendCandidate = Record<string, any>;

const statusSummary = (item: BackendCandidate, key: "newness" | "reachability") => {
  const fact = item[key] || {};
  if (fact.status !== "passed") return fact.status === "failed" ? "不通过" : "未验证";
  if (key === "newness") return `CT 首见${fact.ct_first_seen_at ? ` ${fact.ct_first_seen_at}` : ""}${typeof fact.rdap_age_days === "number" ? ` · RDAP ${fact.rdap_age_days} 天` : ""}`;
  return `HTTP ${fact.http_status_code ?? "已验证"}${fact.same_root === true ? " · 根域名一致" : ""}`;
};

const toCandidate = (item: BackendCandidate): CandidateDetail => ({
  id: item.candidate_id,
  version: item.version,
  domain: item.domain,
  displayName: item.name_suggestion || undefined,
  canonicalUrl: item.canonical_url || undefined,
  reviewState: item.review_state === "pending" ? "pending" : ({ approve: "approved", defer: "deferred", reject: "rejected", blocklist: "blocklisted" } as Record<string, ReviewState>)[item.review_state] || "pending",
  verdict: { label: item.primary_outcome || "待判断", confidence: item.classification_confidence },
  priority: item.priority?.score ?? 0,
  newness: { status: item.newness?.status || "unknown", summary: statusSummary(item, "newness") },
  reachability: { status: item.reachability?.status || "unknown", summary: statusSummary(item, "reachability") },
  productSummary: item.description_suggestion || undefined,
  updatedAt: item.newness?.checked_at || "",
  evidence: [
    { category: "newness", status: item.newness?.status || "unknown", title: "CT 与 RDAP 新网站证据", explanation: statusSummary(item, "newness"), observedAt: item.newness?.checked_at },
    { category: "reachability", status: item.reachability?.status || "unknown", title: "可访问性与根域名一致性", explanation: statusSummary(item, "reachability"), sourceUrl: item.reachability?.final_url || item.canonical_url },
    ...(item.evidence || []).map((e: BackendCandidate) => ({ category: "product" as const, status: "passed" as const, title: e.type || "产品证据", explanation: e.quote, quote: e.quote, sourceUrl: e.url })),
  ],
  audit: item.audit ? { sourceEvents: [], observations: [], decisionHistory: item.audit.decisions || [] } : undefined,
});

const liveApi = {
  async fetchCandidates(): Promise<CandidateSummary[]> {
    const response = await fetch("/v1/review-queue", { cache: "no-store" });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
    return (body.items || []).map(toCandidate);
  },
  async getCandidate(id: string): Promise<CandidateDetail> {
    const response = await fetch(`/v1/candidates/${encodeURIComponent(id)}/review-context`, { cache: "no-store" });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
    return toCandidate(body);
  },
  async fetchLog(tail = 120): Promise<string[]> {
    const response = await fetch(`/v1/discovery/log?tail=${tail}`, { cache: "no-store" });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
    return body.lines || [];
  },
  async submitReview(id: string, action: ReviewAction, actorId: string, version: number): Promise<void> {
    const response = await fetch(`/v1/candidates/${encodeURIComponent(id)}/versions/${version}/decisions`, { method: "POST", headers: { "Content-Type": "application/json", "X-Actor-ID": actorId }, body: JSON.stringify({ request_id: crypto.randomUUID(), action, reason_tags: [] }) });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
  },
};

function useCounter(target: number, duration: number = 600): number {
  const [current, setCurrent] = useState(0);

  useEffect(() => {
    let startTimestamp: number | null = null;
    const step = (timestamp: number) => {
      if (!startTimestamp) startTimestamp = timestamp;
      const progress = Math.min((timestamp - startTimestamp) / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setCurrent(Math.floor(eased * target));
      if (progress < 1) {
        window.requestAnimationFrame(step);
      } else {
        setCurrent(target);
      }
    };
    window.requestAnimationFrame(step);
  }, [target, duration]);

  return current;
}

const ConsoleBadge: React.FC<{ status: EvidenceStatus; text?: string; lang: Language }> = ({
  status,
  text,
  lang
}) => {
  const d = DICT[lang];
  if (status === "passed") {
    return (
      <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-sm font-mono text-[10px] font-semibold bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/30">
        <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
        {text || d.statusPassed}
      </span>
    );
  }
  if (status === "failed") {
    return (
      <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-sm font-mono text-[10px] font-semibold bg-rose-500/10 text-rose-600 dark:text-rose-400 border border-rose-500/30">
        <XCircle className="w-2.5 h-2.5" />
        {text || d.statusFailed}
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-sm font-mono text-[10px] font-semibold bg-slate-500/10 text-slate-600 dark:text-slate-400 border border-slate-500/30">
      <HelpCircle className="w-2.5 h-2.5" />
      {text || d.statusUnknown}
    </span>
  );
};


const LogPanel: React.FC<{ lang: Language }> = ({ lang }) => {
  const [lines, setLines] = useState<string[]>([]);
  const [exists, setExists] = useState(true);
  const d = DICT[lang];

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const fetched = await liveApi.fetchLog();
        if (!alive) return;
        setExists(true);
        setLines(fetched);
      } catch {
        if (alive) setExists(false);
      }
    };
    load();
    const timer = window.setInterval(load, 5000);
    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, []);

  return (
    <section className="border border-slate-200 dark:border-slate-800 rounded-lg overflow-hidden">
      <div className="flex items-center gap-2 px-4 py-2 border-b border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-[#141A24] text-[10px] font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
        <Terminal className="w-3 h-3 text-blue-500" />
        <span>{d.runtimeLog}</span>
        <span className="ml-auto text-slate-400 dark:text-slate-600">auto-refresh 5s</span>
      </div>
      <pre className="px-4 py-3 max-h-64 overflow-auto text-[10px] leading-relaxed font-mono text-slate-600 dark:text-slate-400 bg-white dark:bg-black/40">
        {lines.length === 0 ? d.runtimeLogEmpty : lines.join("\n")}
      </pre>
    </section>
  );
};

const InboxView: React.FC<{
  onSelectCandidate: (id: string) => void;
  searchQuery: string;
  onSearchChange: (q: string) => void;
  candidates: CandidateSummary[];
  isLoading: boolean;
  error: string | null;
  onRetry: () => void;
  lang: Language;
}> = ({
  onSelectCandidate,
  searchQuery,
  onSearchChange,
  candidates,
  isLoading,
  error,
  onRetry,
  lang
}) => {
  const d = DICT[lang];

  const filtered = useMemo(() => {
    if (!searchQuery.trim()) return candidates;
    const q = searchQuery.toLowerCase().trim();
    return candidates.filter(
      (c) =>
        c.domain.toLowerCase().includes(q) ||
        (c.displayName && c.displayName.toLowerCase().includes(q)) ||
        (c.productSummary && c.productSummary.toLowerCase().includes(q))
    );
  }, [candidates, searchQuery]);

  const topCandidate = filtered.length > 0 ? filtered[0] : null;
  const otherCandidates = filtered.length > 1 ? filtered.slice(1) : [];

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 py-8 space-y-8 font-mono">
      {/* 顶部遥测与搜索栏 */}
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 border-b border-slate-200/80 dark:border-slate-800/80 pb-5">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-xl sm:text-2xl font-bold tracking-tight text-slate-900 dark:text-slate-100 uppercase">
              {d.inboxCount.replace("{n}", candidates.length.toString())}
            </h1>
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20">
              <Activity className="w-3 h-3 animate-pulse" />
              {d.pipelineGate}
            </span>
          </div>
          <p className="text-xs text-slate-500 dark:text-slate-400 mt-1 font-sans">{d.inboxSubtitle}</p>
        </div>

        <div className="relative w-full sm:w-72">
          <Search className="w-3.5 h-3.5 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 dark:text-slate-500" />
          <input
            type="search"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder={d.searchPlaceholder}
            className="w-full pl-8 pr-3 py-1.5 text-xs bg-white dark:bg-[#10141C] border border-slate-200 dark:border-slate-800 rounded-md placeholder-slate-400 dark:placeholder-slate-600 text-slate-900 dark:text-slate-100 focus:outline-hidden focus:border-blue-500 transition-colors"
          />
        </div>
      </div>

      {error && (
        <div className="p-3 bg-rose-500/10 border border-rose-500/20 rounded-md flex items-center justify-between text-xs text-rose-600 dark:text-rose-400">
          <div className="flex items-center gap-2">
            <AlertCircle className="w-3.5 h-3.5 shrink-0" />
            <span>{error}</span>
          </div>
          <button onClick={onRetry} className="font-bold underline ml-2">
            {d.retry}
          </button>
        </div>
      )}

      {isLoading ? (
        <div className="space-y-3">
          <div className="h-44 bg-slate-200/50 dark:bg-slate-800/40 rounded-lg animate-shimmer border border-slate-200 dark:border-slate-800" />
          <div className="h-20 bg-slate-200/50 dark:bg-slate-800/40 rounded-lg animate-shimmer border border-slate-200 dark:border-slate-800" />
        </div>
      ) : candidates.length === 0 ? (
        <div className="border border-dashed border-slate-300 dark:border-slate-800 rounded-lg p-12 text-center space-y-4">
          <Cpu className="w-8 h-8 text-slate-400 dark:text-slate-600 mx-auto" />
          <div className="text-sm font-bold tracking-wider text-slate-800 dark:text-slate-300 uppercase">{d.emptyTitle}</div>
          <p className="text-xs text-slate-500 font-sans max-w-sm mx-auto">{d.emptyDesc}</p>
                  </div>
      ) : (
        <div className="space-y-8">
          {/* 1. CRITICAL TARGET: 顶部优先侦查标的 (已去四角加号，纯净亚光包边) */}
          {topCandidate && (
            <section className="space-y-2 animate-cascade">
              <div className="flex items-center justify-between text-[11px] font-bold text-slate-400 dark:text-slate-500 tracking-wider">
                <span>{d.topPrioritySection}</span>
                <span className="text-blue-600 dark:text-blue-400 font-mono">{topCandidate.id}</span>
              </div>

              <div
                tabIndex={0}
                role="button"
                onClick={() => onSelectCandidate(topCandidate.id)}
                onKeyDown={(e) => e.key === "Enter" && onSelectCandidate(topCandidate.id)}
                className="group relative bg-white dark:bg-[#10141C] border border-slate-200/90 dark:border-slate-800/90 hover:border-blue-500/80 dark:hover:border-blue-500/50 rounded-lg p-6 shadow-xs hover:shadow-md transition-all cursor-pointer focus:outline-hidden"
              >
                <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-3">
                  <div className="space-y-2">
                    <div className="flex items-baseline gap-3 flex-wrap">
                      <span className="text-2xl font-bold tracking-tight text-slate-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                        {topCandidate.domain}
                      </span>
                      {topCandidate.displayName && (
                        <span className="text-xs text-slate-500 font-normal">
                          [{topCandidate.displayName}]
                        </span>
                      )}
                      <span className="px-2 py-0.5 text-[10px] font-semibold bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 rounded-sm">
                        {topCandidate.verdict.label}
                      </span>
                    </div>

                    {topCandidate.productSummary && (
                      <p className="text-xs text-slate-600 dark:text-slate-400 font-sans leading-relaxed max-w-2xl">
                        {topCandidate.productSummary}
                      </p>
                    )}
                  </div>

                  <div className="flex flex-col items-end shrink-0 gap-2">
                    <div className="text-right">
                      <div className="text-[10px] text-slate-400 uppercase tracking-widest">{d.priorityPrefix}</div>
                      <div className="text-xl font-black text-slate-900 dark:text-white font-mono">{topCandidate.priority.toFixed(2)}</div>
                    </div>
                    <div className="inline-flex items-center gap-1 text-xs font-bold text-blue-600 dark:text-blue-400 group-hover:translate-x-1 transition-transform">
                      <span>{d.viewEvidence}</span>
                      <ArrowRight className="w-3.5 h-3.5" />
                    </div>
                  </div>
                </div>

                <div className="mt-5 pt-3 border-t border-slate-100 dark:border-slate-800/80 flex flex-wrap gap-x-6 gap-y-2 text-xs text-slate-500 dark:text-slate-400">
                  <div className="flex items-center gap-2">
                    <span className="text-slate-400">{d.newnessFact}:</span>
                    <span className="text-slate-700 dark:text-slate-300">{topCandidate.newness.summary}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-slate-400">{d.reachabilityFact}:</span>
                    <span className="text-slate-700 dark:text-slate-300">{topCandidate.reachability.summary}</span>
                  </div>
                </div>
              </div>
            </section>
          )}

          {/* 2. ACTIVE QUEUE: 控制台紧凑数据行 */}
          {otherCandidates.length > 0 && (
            <section className="space-y-2">
              <div className="flex items-center justify-between text-[11px] font-bold text-slate-400 dark:text-slate-500 tracking-wider">
                <span>{d.otherCandidatesSection}</span>
                <span>CHANNELS: {otherCandidates.length}</span>
              </div>

              <div className="border border-slate-200/90 dark:border-slate-800/90 divide-y divide-slate-100 dark:divide-slate-800/80 rounded-lg bg-white dark:bg-[#10141C] overflow-hidden">
                {otherCandidates.map((cand, idx) => (
                  <div
                    key={cand.id}
                    tabIndex={0}
                    role="button"
                    onClick={() => onSelectCandidate(cand.id)}
                    onKeyDown={(e) => e.key === "Enter" && onSelectCandidate(cand.id)}
                    style={{ animationDelay: `${(idx + 1) * 35}ms` }}
                    className="animate-cascade group px-4 py-3 flex flex-col sm:flex-row sm:items-center justify-between gap-3 hover:bg-slate-50 dark:hover:bg-[#141A24] transition-colors cursor-pointer focus:outline-hidden"
                  >
                    <div className="flex items-baseline gap-3 truncate">
                      <span className="text-xs text-slate-400 dark:text-slate-600 font-bold">
                        {String(idx + 2).padStart(2, "0")}
                      </span>
                      <span className="text-sm font-bold text-slate-900 dark:text-slate-100 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                        {cand.domain}
                      </span>
                      {cand.displayName && (
                        <span className="text-xs text-slate-500 truncate hidden md:inline">
                          [{cand.displayName}]
                        </span>
                      )}
                      <span className="text-xs text-slate-400 dark:text-slate-500 truncate font-sans">
                        · {cand.productSummary || cand.newness.summary}
                      </span>
                    </div>

                    <div className="flex items-center justify-between sm:justify-end gap-5 shrink-0 text-xs">
                      <span className="text-slate-400 dark:text-slate-500 text-[11px]">
                        SCORE:{cand.priority.toFixed(2)}
                      </span>
                      <span className="text-blue-600 dark:text-blue-400 font-bold flex items-center gap-1 group-hover:translate-x-0.5 transition-transform">
                        <span>OPEN</span>
                        <ArrowRight className="w-3 h-3" />
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}
        </div>
      )}

      <LogPanel lang={lang} />
    </div>
  );
};

// ==========================================
// 7. 详情页视图
// ==========================================

const CandidateDetailView: React.FC<{
  candidateId: string;
  onBackToInbox: () => void;
  actorId: string;
  onSaveActorId: (id: string) => void;
  lang: Language;
}> = ({ candidateId, onBackToInbox, actorId, onSaveActorId, lang }) => {
  const [data, setData] = useState<CandidateDetail | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [stampType, setStampType] = useState<"approved" | "rejected" | "deferred" | "blocklisted" | null>(null);
  const [isFadingOut, setIsFadingOut] = useState(false);

  const [auditOpen, setAuditOpen] = useState(false);
  const [activeSubmitting, setActiveSubmitting] = useState<ReviewAction | null>(null);
  const [confirmModal, setConfirmModal] = useState<{ action: "reject" | "blocklist" } | null>(null);
  const [actorPromptOpen, setActorPromptOpen] = useState(false);
  const [pendingAction, setPendingAction] = useState<ReviewAction | null>(null);
  const [tempActorInput, setTempActorInput] = useState(actorId);

  const d = DICT[lang];

  const loadData = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const item = await liveApi.getCandidate(candidateId);
      setData(item);
    } catch (err: unknown) {
      setError((err as Error)?.message || "NODE_READ_FAILURE");
    } finally {
      setIsLoading(false);
    }
  }, [candidateId]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const executeAction = async (action: ReviewAction, currentActor: string) => {
    if (!data) return;
    setActiveSubmitting(action);

    try {
      await liveApi.submitReview(data.id, action, currentActor, data.version);

      const stampMap: Record<ReviewAction, "approved" | "rejected" | "deferred" | "blocklisted"> = {
        approve: "approved",
        reject: "rejected",
        defer: "deferred",
        blocklist: "blocklisted"
      };
      setStampType(stampMap[action]);

      setTimeout(() => {
        setIsFadingOut(true);
      }, 350);

      setTimeout(() => {
        onBackToInbox();
      }, 650);
    } catch (err: unknown) {
      alert((err as Error)?.message || "OPERATION_FAILED");
      loadData();
      setActiveSubmitting(null);
    }
  };

  const handleActionClick = (action: ReviewAction) => {
    if (!actorId) {
      setPendingAction(action);
      setActorPromptOpen(true);
      return;
    }
    if (action === "reject" || action === "blocklist") {
      setConfirmModal({ action });
      return;
    }
    executeAction(action, actorId);
  };

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      if (confirmModal || actorPromptOpen || activeSubmitting) return;

      if (e.key === "a" || e.key === "A") {
        handleActionClick("approve");
      } else if (e.key === "d" || e.key === "D") {
        handleActionClick("defer");
      } else if (e.key === "r" || e.key === "R") {
        handleActionClick("reject");
      } else if (e.key === "b" || e.key === "B") {
        handleActionClick("blocklist");
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  });

  const handleActorSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!tempActorInput.trim()) return;
    onSaveActorId(tempActorInput.trim());
    setActorPromptOpen(false);
    if (pendingAction) {
      if (pendingAction === "reject" || pendingAction === "blocklist") {
        setConfirmModal({ action: pendingAction });
      } else {
        executeAction(pendingAction, tempActorInput.trim());
      }
      setPendingAction(null);
    }
  };

  if (isLoading) {
    return (
      <div className="max-w-5xl mx-auto px-4 py-8 space-y-4 font-mono">
        <div className="h-5 w-24 bg-slate-200 dark:bg-slate-800 rounded-md animate-shimmer" />
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">
          <div className="lg:col-span-8 space-y-4">
            <div className="h-28 bg-white dark:bg-[#10141C] border border-slate-200 dark:border-slate-800 rounded-md animate-shimmer" />
            <div className="h-44 bg-white dark:bg-[#10141C] border border-slate-200 dark:border-slate-800 rounded-md animate-shimmer" />
          </div>
          <div className="lg:col-span-4 h-60 bg-white dark:bg-[#10141C] border border-slate-200 dark:border-slate-800 rounded-md animate-shimmer" />
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="max-w-md mx-auto my-20 bg-white dark:bg-[#10141C] border border-slate-200 dark:border-slate-800 rounded-md p-8 text-center space-y-4 font-mono">
        <AlertCircle className="w-8 h-8 text-slate-400 mx-auto" />
        <div className="text-sm font-bold text-slate-900 dark:text-white uppercase">{error}</div>
        <button
          onClick={onBackToInbox}
          className="px-4 py-1.5 text-xs font-bold uppercase bg-blue-600 text-white rounded-md hover:bg-blue-500"
        >
          {d.backToInbox}
        </button>
      </div>
    );
  }

  const allEvidences = data.evidence;

  return (
    <div
      className={`max-w-5xl mx-auto px-4 sm:px-6 py-6 pb-28 lg:pb-12 space-y-5 transition-all duration-300 ease-out font-mono ${
        isFadingOut ? "opacity-0 -translate-y-3 pointer-events-none" : "opacity-100 translate-y-0"
      }`}
    >
      <div>
        <button
          onClick={onBackToInbox}
          className="inline-flex items-center gap-1.5 text-xs font-bold text-slate-500 hover:text-slate-900 dark:hover:text-slate-200 transition"
        >
          <ArrowLeft className="w-3.5 h-3.5" />
          <span>{d.backToInbox}</span>
        </button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-5 items-start">
        {/* 左侧案卷遥测区 */}
        <div className="lg:col-span-8 space-y-5">
          {/* 1. 资产标头 */}
          <section className="relative bg-white dark:bg-[#10141C] border border-slate-200/90 dark:border-slate-800/90 rounded-lg p-6 shadow-xs">
            {stampType && (
              <div
                className={`absolute right-6 top-6 pointer-events-none select-none z-20 font-mono font-black tracking-widest text-xs sm:text-sm uppercase px-3 py-1 rounded-sm border transition-all duration-150 animate-stamp ${
                  stampType === "approved"
                    ? "text-emerald-500 border-emerald-500 bg-emerald-500/10 shadow-[0_0_20px_rgba(16,185,129,0.2)]"
                    : stampType === "rejected" || stampType === "blocklisted"
                    ? "text-rose-500 border-rose-500 bg-rose-500/10 shadow-[0_0_20px_rgba(244,63,94,0.2)]"
                    : "text-amber-500 border-amber-500 bg-amber-500/10"
                }`}
              >
                [{stampType.toUpperCase()}]
              </div>
            )}

            <div className="space-y-2">
              <div className="flex items-center justify-between text-[10px] text-slate-400 dark:text-slate-500">
                <span>IDENTIFIER: {data.id}</span>
                <span>STATUS: {d.pendingStatus}</span>
              </div>
              <div className="flex items-baseline gap-3 flex-wrap">
                <h1 className="text-2xl sm:text-3xl font-black tracking-tight text-slate-900 dark:text-white">
                  {data.domain}
                </h1>
                <a
                  href={data.canonicalUrl || `https://${data.domain}`}
                  target="_blank"
                  rel="noreferrer noopener"
                  className="inline-flex items-center gap-1 text-xs font-bold text-blue-600 dark:text-blue-400 hover:underline"
                >
                  <span>{d.visitSite}</span>
                  <ExternalLink className="w-3 h-3" />
                </a>
              </div>
              {data.displayName && (
                <div className="text-xs text-slate-500 font-bold">
                  CANONICAL_REF: {data.displayName}
                </div>
              )}
            </div>
          </section>

          {/* 2. 遥测链面板 */}
          <section className="bg-white dark:bg-[#10141C] border border-slate-200/90 dark:border-slate-800/90 rounded-lg p-6 space-y-4">
            <div className="flex items-center justify-between border-b border-slate-100 dark:border-slate-800/80 pb-2.5">
              <span className="text-xs font-bold tracking-wider uppercase text-slate-700 dark:text-slate-300">
                {d.whyInboxHeading}
              </span>
              <span className="text-[10px] text-slate-400">TELEMETRY_PIPELINE</span>
            </div>

            <div className="space-y-3">
              {allEvidences.map((ev, i) => (
                <div key={i} className="p-3 bg-slate-50 dark:bg-black/30 border border-slate-200/80 dark:border-slate-800/70 rounded-md space-y-1.5 text-xs">
                  <div className="flex items-center justify-between gap-4">
                    <span className="font-bold text-slate-900 dark:text-slate-100 flex items-center gap-1.5">
                      <CornerDownRight className="w-3 h-3 text-slate-400" />
                      {ev.title}
                    </span>
                    <ConsoleBadge status={ev.status} lang={lang} />
                  </div>

                  <p className="text-slate-600 dark:text-slate-400 font-sans leading-relaxed text-[11px] pl-4">
                    {ev.explanation}
                  </p>

                  {ev.quote && (
                    <div className="ml-4 p-2 bg-slate-100 dark:bg-slate-900 border-l-2 border-blue-500 text-[11px] text-slate-800 dark:text-slate-300 font-mono">
                      PAYLOAD_DIFF: “{ev.quote}”
                    </div>
                  )}

                  {ev.observedAt && (
                    <div className="pl-4 text-[10px] text-slate-400 dark:text-slate-500">
                      {d.observedAt}{new Date(ev.observedAt).toLocaleString()}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </section>

          {/* 3. 模型意图判定 */}
          <section className="bg-white dark:bg-[#10141C] border border-slate-200/90 dark:border-slate-800/90 rounded-lg p-5 space-y-2">
            <div className="text-xs font-bold uppercase tracking-wider text-slate-700 dark:text-slate-300">
              {d.productAssessmentHeading}
            </div>
            <p className="text-xs text-slate-600 dark:text-slate-300 font-sans leading-relaxed">
              {data.productSummary || "NO_SUMMARY_AVAILABLE"}
            </p>
            <div className="pt-2 text-[11px] text-slate-400 dark:text-slate-500 flex items-center gap-4 border-t border-slate-100 dark:border-slate-800/80">
              <span>{d.classificationConclusion}{data.verdict.label}</span>
              {data.verdict.confidence && (
                <span>{d.confidence}{(data.verdict.confidence * 100).toFixed(0)}%</span>
              )}
            </div>
          </section>

          {/* 4. 底层探针报文 */}
          <section className="bg-white dark:bg-[#10141C] border border-slate-200/90 dark:border-slate-800/90 rounded-lg overflow-hidden">
            <button
              type="button"
              onClick={() => setAuditOpen(!auditOpen)}
              className="w-full flex items-center justify-between px-5 py-3 text-xs font-bold text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800/40 transition"
            >
              <span>{d.viewAuditData}</span>
              <ChevronDown className={`w-3.5 h-3.5 transition-transform duration-200 ${auditOpen ? "rotate-180" : ""}`} />
            </button>
            <div className={`grid transition-all duration-200 border-t border-slate-200 dark:border-slate-800 ${auditOpen ? "grid-rows-[1fr]" : "grid-rows-[0fr]"}`}>
              <div className="overflow-hidden">
                <div className="p-4 bg-black text-emerald-400 text-xs overflow-x-auto">
                  <pre className="text-[11px] leading-relaxed">{JSON.stringify(data.audit || { id: data.id }, null, 2)}</pre>
                </div>
              </div>
            </div>
          </section>
        </div>

        {/* 右侧吸顶机架操作台中控条 */}
        <aside className="hidden lg:block lg:col-span-4 sticky top-20">
          <div className="bg-white dark:bg-[#10141C] border border-slate-200/90 dark:border-slate-800/90 rounded-lg p-5 shadow-xs space-y-4">
            <div className="flex items-center justify-between border-b border-slate-100 dark:border-slate-800 pb-2.5">
              <span className="font-bold text-xs tracking-wider uppercase text-slate-700 dark:text-slate-300 flex items-center gap-1.5">
                <Sliders className="w-3.5 h-3.5 text-blue-500" />
                {d.reviewPanelTitle}
              </span>
              <span className="text-[10px] text-emerald-500 font-bold">ONLINE</span>
            </div>

            {/* 操作员徽章 */}
            <div className="p-2.5 bg-slate-50 dark:bg-black/40 border border-slate-200 dark:border-slate-800 rounded-md space-y-1 text-xs">
              <div className="flex items-center justify-between text-slate-400">
                <span>{d.currentReviewer}</span>
                <button
                  onClick={() => setActorPromptOpen(true)}
                  className="text-blue-600 dark:text-blue-400 hover:underline font-bold"
                >
                  {d.changeReviewer}
                </button>
              </div>
              <div className="font-bold text-slate-900 dark:text-white truncate">
                {actorId ? actorId : <span className="font-normal text-amber-500">{d.firstReviewPrompt}</span>}
              </div>
            </div>

            {/* 控制台操作指令 */}
            <div className="space-y-2 pt-1">
              <button
                type="button"
                disabled={activeSubmitting !== null}
                onClick={() => handleActionClick("approve")}
                className="w-full py-2.5 text-xs font-bold uppercase tracking-wider text-white bg-blue-600 hover:bg-blue-500 rounded-md shadow-xs transition active:scale-[0.99] disabled:opacity-50 flex items-center justify-center gap-2"
              >
                <CheckCheck className="w-4 h-4" />
                <span>{activeSubmitting === "approve" ? d.approving : d.approveBtn}</span>
              </button>

              <button
                type="button"
                disabled={activeSubmitting !== null}
                onClick={() => handleActionClick("defer")}
                className="w-full py-2 text-xs font-bold uppercase tracking-wider border border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800 rounded-md transition disabled:opacity-50"
              >
                {activeSubmitting === "defer" ? d.submitting : d.deferBtn}
              </button>

              <div className="grid grid-cols-2 gap-2 pt-1">
                <button
                  type="button"
                  disabled={activeSubmitting !== null}
                  onClick={() => handleActionClick("reject")}
                  className="w-full py-2 text-xs font-bold uppercase tracking-wider text-rose-600 dark:text-rose-400 bg-rose-500/10 hover:bg-rose-500/20 border border-rose-500/20 rounded-md transition disabled:opacity-50"
                >
                  {activeSubmitting === "reject" ? d.submitting : d.rejectBtn}
                </button>
                <button
                  type="button"
                  disabled={activeSubmitting !== null}
                  onClick={() => handleActionClick("blocklist")}
                  className="w-full py-2 text-xs font-bold uppercase tracking-wider text-rose-700 dark:text-rose-300 bg-rose-950/40 border border-rose-900 hover:bg-rose-900/50 rounded-md transition disabled:opacity-50"
                >
                  {activeSubmitting === "blocklist" ? d.submitting : d.blocklistBtn}
                </button>
              </div>
            </div>
          </div>
        </aside>
      </div>

      {/* 移动端吸底控制栏 */}
      <div className="lg:hidden fixed bottom-0 left-0 right-0 bg-white/95 dark:bg-[#10141C]/95 backdrop-blur-md border-t border-slate-200 dark:border-slate-800 py-3 px-4 z-40">
        <div className="max-w-4xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <div className="text-xs text-slate-500">
            {actorId ? (
              <span>
                {d.currentReviewer} <strong>{actorId}</strong>{" "}
                <button onClick={() => setActorPromptOpen(true)} className="text-blue-600 underline ml-1 font-bold">
                  {d.changeReviewer}
                </button>
              </span>
            ) : (
              <span>{d.firstReviewPrompt}</span>
            )}
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              disabled={activeSubmitting !== null}
              onClick={() => handleActionClick("defer")}
              className="px-3.5 py-1.5 text-xs font-bold uppercase border border-slate-200 dark:border-slate-700 rounded-md disabled:opacity-50"
            >
              {d.deferBtn}
            </button>
            <button
              type="button"
              disabled={activeSubmitting !== null}
              onClick={() => handleActionClick("reject")}
              className="px-3.5 py-1.5 text-xs font-bold uppercase text-rose-500 bg-rose-500/10 border border-rose-500/20 rounded-md disabled:opacity-50"
            >
              {d.rejectBtn}
            </button>
            <button
              type="button"
              disabled={activeSubmitting !== null}
              onClick={() => handleActionClick("blocklist")}
              className="px-3.5 py-1.5 text-xs font-bold uppercase text-rose-300 border border-rose-900 rounded-md disabled:opacity-50"
            >
              {d.blocklistBtn}
            </button>
            <button
              type="button"
              disabled={activeSubmitting !== null}
              onClick={() => handleActionClick("approve")}
              className="px-4 py-1.5 text-xs font-bold uppercase text-white bg-blue-600 rounded-md disabled:opacity-50"
            >
              {d.approveBtn}
            </button>
          </div>
        </div>
      </div>

      {/* 操作员签名弹窗 */}
      {actorPromptOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
          <form
            onSubmit={handleActorSubmit}
            className="w-full max-w-sm bg-white dark:bg-[#10141C] text-slate-800 dark:text-slate-200 border border-slate-200 dark:border-slate-800 rounded-lg p-6 space-y-4 shadow-2xl text-xs"
          >
            <h3 className="font-bold text-sm text-slate-900 dark:text-white uppercase">{d.actorModalTitle}</h3>
            <p className="text-slate-500 font-sans leading-relaxed">{d.actorModalDesc}</p>
            <input
              type="text"
              required
              autoFocus
              value={tempActorInput}
              onChange={(e) => setTempActorInput(e.target.value)}
              placeholder={d.actorPlaceholder}
              className="w-full px-3 py-1.5 border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-black text-slate-900 dark:text-white rounded-md focus:outline-hidden focus:border-blue-500"
            />
            <div className="flex justify-end gap-2 pt-2 font-mono">
              <button
                type="button"
                onClick={() => {
                  setActorPromptOpen(false);
                  setPendingAction(null);
                }}
                className="px-3 py-1.5 text-xs text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-md"
              >
                {d.cancel}
              </button>
              <button
                type="submit"
                className="px-4 py-1.5 text-xs font-bold uppercase bg-blue-600 text-white rounded-md hover:bg-blue-500"
              >
                {d.confirm}
              </button>
            </div>
          </form>
        </div>
      )}

      {/* 二次确认弹窗 */}
      {confirmModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4 text-xs">
          <div className="w-full max-w-md bg-white dark:bg-[#10141C] text-slate-800 dark:text-slate-200 border border-slate-200 dark:border-slate-800 rounded-lg p-6 space-y-4 shadow-2xl">
            <div className="flex items-center gap-2 text-rose-600 dark:text-rose-400">
              <ShieldAlert className="w-4 h-4 shrink-0" />
              <h3 className="font-bold text-sm uppercase">
                {confirmModal.action === "reject" ? d.rejectConfirmTitle : d.blocklistConfirmTitle}
              </h3>
            </div>
            <p className="text-slate-600 dark:text-slate-400 font-sans leading-relaxed">
              {confirmModal.action === "reject" ? d.rejectConfirmDesc : d.blocklistConfirmDesc}
            </p>
            <div className="flex justify-end gap-2 pt-2">
              <button
                type="button"
                onClick={() => setConfirmModal(null)}
                className="px-3 py-1.5 text-xs text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-md"
              >
                {d.cancel}
              </button>
              <button
                type="button"
                onClick={() => {
                  const act = confirmModal.action;
                  setConfirmModal(null);
                  executeAction(act, actorId);
                }}
                className="px-4 py-1.5 text-xs font-bold uppercase bg-rose-600 text-white rounded-md hover:bg-rose-500"
              >
                {d.executeBtn}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// ==========================================
// 8. 根组件
// ==========================================

export default function App() {
  const [candidates, setCandidates] = useState<CandidateSummary[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [listError, setListError] = useState<string | null>(null);

  const [searchQuery, setSearchQuery] = useState("");
  const routeFromLocation = () => {
    const match = window.location.pathname.match(/^\/review\/([^/]+)$/);
    return match ? `#/candidate/${decodeURIComponent(match[1])}` : "#/";
  };
  const [currentRoute, setCurrentRoute] = useState<string>(routeFromLocation);
  const [actorId, setActorId] = useState<string>(() => localStorage.getItem("dh_actor_id") || "");

  const [lang, setLang] = useState<Language>(() => {
    return (localStorage.getItem("dh_lang") as Language) || "zh";
  });
  const [theme, setTheme] = useState<Theme>(() => {
    return (localStorage.getItem("dh_theme") as Theme) || "dark";
  });

  const [themeRotating, setThemeRotating] = useState(false);
  const [langRotating, setLangRotating] = useState(false);

  useEffect(() => {
    const root = document.documentElement;
    const body = document.body;
    if (theme === "dark") {
      root.classList.add("dark");
      body.classList.add("dark");
    } else {
      root.classList.remove("dark");
      body.classList.remove("dark");
    }
    localStorage.setItem("dh_theme", theme);
  }, [theme]);

  useEffect(() => {
    localStorage.setItem("dh_lang", lang);
  }, [lang]);

  useEffect(() => {
    const handleNavigation = () => {
      setCurrentRoute(routeFromLocation());
    };
    window.addEventListener("popstate", handleNavigation);
    return () => window.removeEventListener("popstate", handleNavigation);
  }, []);

  const loadCandidates = useCallback(async () => {
    setIsLoading(true);
    setListError(null);
    try {
      const data = await liveApi.fetchCandidates();
      setCandidates(data);
    } catch (err: unknown) {
      setListError((err as Error)?.message || "STREAM_POLL_FAILED");
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadCandidates();
  }, [loadCandidates]);

  const navigateTo = (hash: string) => {
    const detail = hash.match(/^#\/candidate\/(.+)$/);
    const path = detail ? `/review/${encodeURIComponent(detail[1])}` : "/";
    window.history.pushState({}, "", path);
    setCurrentRoute(hash);
  };

  const handleSaveActorId = (id: string) => {
    setActorId(id);
    localStorage.setItem("dh_actor_id", id);
  };

  const toggleTheme = () => {
    setThemeRotating(true);
    setTheme((prev) => (prev === "light" ? "dark" : "light"));
    setTimeout(() => setThemeRotating(false), 250);
  };

  const toggleLang = () => {
    setLangRotating(true);
    setLang((prev) => (prev === "zh" ? "en" : "zh"));
    setTimeout(() => setLangRotating(false), 250);
  };

  const d = DICT[lang];
  const detailMatch = currentRoute.match(/^#\/candidate\/([^/]+)$/);
  const activeCandidateId = detailMatch ? detailMatch[1] : null;

  return (
    <div className="min-h-screen bg-ambient-console text-slate-900 dark:text-slate-100 font-mono antialiased selection:bg-blue-500/20 selection:text-blue-500 transition-colors duration-200">
      {/* 极简工程顶栏 */}
      <header className="h-13 bg-white/80 dark:bg-[#090B10]/80 backdrop-blur-md border-b border-slate-200/80 dark:border-slate-800/80 sticky top-0 z-30 font-mono">
        <div className="max-w-5xl mx-auto h-full px-4 sm:px-6 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <span className="font-black tracking-tight text-sm text-slate-900 dark:text-white uppercase">
              DomainHunter
            </span>
            <span className="text-slate-300 dark:text-slate-700">/</span>
            <span className="text-xs text-slate-500 font-bold uppercase">
              {d.brandSubtitle}
            </span>
          </div>

          <div className="flex items-center gap-2 sm:gap-3">
            <button
              type="button"
              onClick={toggleLang}
              className="p-1.5 text-xs text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800/60 rounded-md transition inline-flex items-center gap-1 active:scale-95"
              title="Switch Language"
            >
              <Languages
                className={`w-3.5 h-3.5 text-slate-400 transition-transform duration-250 ${
                  langRotating ? "rotate-180" : "rotate-0"
                }`}
              />
              <span className="font-mono text-[10px] font-bold uppercase">{lang}</span>
            </button>

            <button
              type="button"
              onClick={toggleTheme}
              className="p-1.5 text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800/60 rounded-md transition active:scale-95"
              title="Toggle Theme"
            >
              <div className={`transition-transform duration-250 ${themeRotating ? "rotate-180" : "rotate-0"}`}>
                {theme === "light" ? (
                  <Moon className="w-3.5 h-3.5 text-slate-600" />
                ) : (
                  <Sun className="w-3.5 h-3.5 text-amber-400" />
                )}
              </div>
            </button>

            <div className="h-3 w-px bg-slate-200 dark:bg-slate-800 mx-0.5" />

                      </div>
        </div>
      </header>

      {/* 主视区 */}
      <main>
        {activeCandidateId ? (
          <CandidateDetailView
            candidateId={activeCandidateId}
            onBackToInbox={() => navigateTo("#/")}
            actorId={actorId}
            onSaveActorId={handleSaveActorId}
            lang={lang}
          />
        ) : (
          <InboxView
            candidates={candidates}
            isLoading={isLoading}
            error={listError}
            onRetry={loadCandidates}
            searchQuery={searchQuery}
            onSearchChange={setSearchQuery}
            onSelectCandidate={(id) => navigateTo(`#/candidate/${id}`)}
            lang={lang}
          />
        )}
      </main>

    </div>
  );
}
