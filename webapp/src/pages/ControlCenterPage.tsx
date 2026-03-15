import { useEffect, useMemo, useState } from "react";
import { Link, useOutletContext } from "react-router-dom";

import { fetchOverview, fetchQueen, fetchSystem, fetchWorkers } from "../api/dashboardClient";
import type { components } from "../api/types";
import type { AppShellOutletContext } from "../ui/AppShell";
import { formatLocalDateTime, formatLocalTime } from "../utils/dateTime";

type OverviewPayload = components["schemas"]["DashboardOverviewV2"];
type WorkersPayload = components["schemas"]["DashboardWorkersV2"];
type QueenPayload = components["schemas"]["DashboardQueenV2"];
type SystemPayload = components["schemas"]["DashboardSystemV2"];

type WorkerTemplateConfig = {
  model?: string | null;
  max_thinking_steps?: number;
  default_timeout_seconds?: number;
  available_tools?: string[];
  can_spawn_children?: boolean;
};

type WorkerRow = {
  id?: string;
  template_name?: string;
  template_id?: string;
  status?: string;
  task?: string;
  updated_at?: string;
  summary?: string;
  error?: string;
  result_preview?: string;
  output?: Record<string, unknown> | null;
  tools_used?: string[];
  lineage_id?: string | null;
  parent_worker_id?: string | null;
  spawn_depth?: number;
  template_config?: WorkerTemplateConfig | null;
};

type MetricPoint = {
  at: number;
  activeWorkers: number;
  queueDepth: number;
  queenQueue: number;
};

type SnapshotBundle = {
  overview: OverviewPayload;
  workers: WorkersPayload;
  queen: QueenPayload;
  system: SystemPayload;
};

type LogRow = {
  event?: string;
  level?: string;
  timestamp?: string;
  service?: string;
};

type QueenStep = {
  id: string;
  title: string;
  detail: string;
  level: string;
  timestamp?: string;
};

const GRAPH_TOP_PAD = 28;
const GRAPH_BOTTOM_PAD = 16;

function asNumber(value: unknown): number {
  const n = Number(value);
  if (Number.isFinite(n)) {
    return n;
  }
  return 0;
}

function shortWorkerId(value?: string | null): string {
  if (!value) {
    return "n/a";
  }
  return value.includes("-") ? value.split("-")[0] : value.slice(0, 8);
}

function shortText(value: string, limit = 140): string {
  const trimmed = value.trim();
  if (trimmed.length <= limit) {
    return trimmed;
  }
  return `${trimmed.slice(0, limit - 1)}…`;
}

function hierarchyLabel(worker: WorkerRow): { text: string; isChild: boolean; depth: number } {
  const depth = Math.max(0, Number(worker.spawn_depth ?? 0));
  if (worker.parent_worker_id) {
    return {
      text: `child of ${shortWorkerId(worker.parent_worker_id)}`,
      isChild: true,
      depth,
    };
  }
  return {
    text: "root",
    isChild: false,
    depth,
  };
}

function statusPill(status?: string): string {
  const v = String(status ?? "").toLowerCase();
  if (v === "running" || v === "started" || v === "completed" || v === "ok") {
    return "bg-emerald-500/15 text-emerald-300 ring-1 ring-inset ring-emerald-400/30";
  }
  if (v === "warning" || v === "thinking" || v === "stopped" || v === "idle") {
    return "bg-amber-500/15 text-amber-300 ring-1 ring-inset ring-amber-300/30";
  }
  return "bg-rose-500/15 text-rose-300 ring-1 ring-inset ring-rose-300/30";
}

function tone(status?: string): string {
  const v = String(status ?? "").toLowerCase();
  if (v === "completed" || v === "running" || v === "started") {
    return "text-emerald-300";
  }
  if (v === "warning" || v === "stopped") {
    return "text-amber-300";
  }
  return "text-rose-300";
}

function workerRowTone(status?: string): string {
  const v = String(status ?? "").toLowerCase();
  if (v === "completed") {
    return "bg-emerald-500/[0.06]";
  }
  if (v === "running" || v === "started") {
    return "bg-cyan-500/[0.06]";
  }
  if (v === "warning" || v === "stopped") {
    return "bg-amber-500/[0.06]";
  }
  return "bg-rose-500/[0.08]";
}

function buildLine(points: number[], width: number, height: number, max: number): string {
  if (points.length === 0) {
    return "";
  }
  const plotHeight = Math.max(1, height - GRAPH_TOP_PAD - GRAPH_BOTTOM_PAD);
  if (points.length === 1) {
    const y = GRAPH_TOP_PAD + plotHeight - (points[0] / max) * plotHeight;
    return `M 0 ${y.toFixed(2)} L ${width} ${y.toFixed(2)}`;
  }
  const step = width / (points.length - 1);
  return points
    .map((value, index) => {
      const x = index * step;
      const y = GRAPH_TOP_PAD + plotHeight - (value / max) * plotHeight;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

function formatEventTitle(event?: string): string {
  const raw = String(event ?? "").trim();
  if (!raw) {
    return "No event message";
  }
  const normalized = raw.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function isQueenRelevantLog(entry: LogRow): boolean {
  const service = String(entry.service ?? "").toLowerCase();
  const event = String(entry.event ?? "").toLowerCase();
  if (service.includes("queen")) {
    return true;
  }
  return [
    "worker",
    "followup",
    "approval",
    "route",
    "reply",
    "spawn",
    "control",
    "intent",
    "tool",
    "thinking",
  ].some((needle) => event.includes(needle));
}

function buildQueenSteps(logs: LogRow[]): QueenStep[] {
  return logs
    .filter(isQueenRelevantLog)
    .map((entry, index) => ({
      id: `${entry.timestamp ?? "ts"}-${index}`,
      title: formatEventTitle(entry.event),
      detail: `${String(entry.service ?? "runtime")} • ${String(entry.level ?? "info")}`,
      level: String(entry.level ?? "info").toLowerCase(),
      timestamp: entry.timestamp,
    }));
}

function deriveQueenWaitingOn(args: {
  state: string;
  controlPending: number;
  channelQueueDepth: number;
  channelLabel: string;
  followupQueues: number;
  internalQueues: number;
}): string {
  const {
    state,
    controlPending,
    channelQueueDepth,
    channelLabel,
    followupQueues,
    internalQueues,
  } = args;
  if (controlPending > 0) {
    return `${controlPending} control request(s) waiting to be handled`;
  }
  if (channelQueueDepth > 0) {
    return `${channelQueueDepth} ${channelLabel} item(s) still queued`;
  }
  if (followupQueues > 0) {
    return `${followupQueues} follow-up queue(s) still draining`;
  }
  if (internalQueues > 0) {
    return `${internalQueues} internal queue(s) still draining`;
  }
  if (state === "thinking") {
    return "Routing or composing work right now";
  }
  return "Nothing blocking right now";
}

function RealtimeGraph({ points }: { points: MetricPoint[] }) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const width = 760;
  const height = 220;
  const workers = points.map((point) => point.activeWorkers);
  const queueDepth = points.map((point) => point.queueDepth);
  const queenQueue = points.map((point) => point.queenQueue);
  const rawMaxValue = Math.max(1, ...workers, ...queueDepth, ...queenQueue);
  const yStep = Math.max(1, Math.ceil(rawMaxValue / 3));
  const yAxisMax = yStep * 3;
  const yTicks = [yStep, yStep * 2, yAxisMax];
  const plotHeight = Math.max(1, height - GRAPH_TOP_PAD - GRAPH_BOTTOM_PAD);
  const workerLine = buildLine(workers, width, height, yAxisMax);
  const queueLine = buildLine(queueDepth, width, height, yAxisMax);
  const queenLine = buildLine(queenQueue, width, height, yAxisMax);
  const firstPoint = points[0];
  const lastPoint = points[points.length - 1];
  const startLabel = firstPoint ? formatLocalTime(firstPoint.at) : "--:--";
  const endLabel = lastPoint ? formatLocalTime(lastPoint.at) : "--:--";
  const tzLabel = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const activeIndex = hoverIndex !== null && hoverIndex >= 0 && hoverIndex < points.length ? hoverIndex : null;
  const activePoint = activeIndex !== null ? points[activeIndex] : null;
  const step = points.length > 1 ? width / (points.length - 1) : width;
  const markerX = activeIndex !== null ? activeIndex * step : 0;

  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4 shadow-xl shadow-slate-950/60 xl:flex xl:h-full xl:flex-col">
      <div className="mb-3 flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Live Load</h3>
          <p className="mt-1 text-xs text-slate-500">Workers, total system queue, and queen-only queue.</p>
        </div>
        <span className="text-xs text-slate-400">Last {points.length} samples</span>
      </div>
      <div className="relative mt-2">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="h-66 w-full rounded-lg bg-slate-950/80 xl:h-74"
          onMouseMove={(event) => {
            const rect = event.currentTarget.getBoundingClientRect();
            if (rect.width <= 0 || points.length === 0) {
              setHoverIndex(null);
              return;
            }
            const ratio = (event.clientX - rect.left) / rect.width;
            const clamped = Math.min(1, Math.max(0, ratio));
            const nextIndex = Math.round(clamped * (points.length - 1));
            setHoverIndex(nextIndex);
          }}
          onMouseLeave={() => setHoverIndex(null)}
        >
          {yTicks.map((tick) => {
            const y = GRAPH_TOP_PAD + plotHeight - (tick / yAxisMax) * plotHeight;
            return <line key={tick} x1={0} x2={width} y1={y} y2={y} stroke="#1e293b" strokeWidth={1} />;
          })}
          {yTicks.map((tick) => {
            const y = GRAPH_TOP_PAD + plotHeight - (tick / yAxisMax) * plotHeight;
            return (
              <text key={`label-${tick}`} x={width - 8} y={y - 4} fill="#64748b" fontSize="10" textAnchor="end">
                {tick}
              </text>
            );
          })}
          <text x={width - 8} y={height - 4} fill="#64748b" fontSize="10" textAnchor="end">
            0
          </text>
          <path d={workerLine} fill="none" stroke="#06b6d4" strokeWidth={3} strokeLinecap="round" />
          <path d={queueLine} fill="none" stroke="#f59e0b" strokeWidth={2.5} strokeLinecap="round" />
          <path d={queenLine} fill="none" stroke="#22c55e" strokeWidth={2.5} strokeLinecap="round" />
          {activePoint ? (
            <line x1={markerX} x2={markerX} y1={0} y2={height} stroke="#64748b" strokeWidth={1} strokeDasharray="5 4" />
          ) : null}
        </svg>
        {activePoint ? (
          <div className="pointer-events-none absolute right-4 top-4 rounded-lg border border-slate-700 bg-slate-950/95 px-3 py-2 text-xs text-slate-200 shadow-xl">
            <p className="mb-1 text-[11px] text-slate-400">{formatLocalDateTime(activePoint.at)} ({tzLabel})</p>
            <p className="text-cyan-300">Workers: {activePoint.activeWorkers}</p>
            <p className="text-amber-300">System queue: {activePoint.queueDepth}</p>
            <p className="text-emerald-300">Queen queue: {activePoint.queenQueue}</p>
          </div>
        ) : null}
      </div>
      <div className="mt-2 flex items-center justify-between text-[11px] text-slate-500">
        <span>{startLabel}</span>
        <span>{endLabel}</span>
      </div>
      <div className="mt-3 flex flex-wrap gap-4 text-xs text-slate-300 xl:mt-auto">
        <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-cyan-400" />Workers</span>
        <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-amber-400" />System queue</span>
        <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-emerald-400" />Queen queue</span>
      </div>
    </section>
  );
}

export function ControlCenterPage() {
  const { filters } = useOutletContext<AppShellOutletContext>();
  const [bundle, setBundle] = useState<SnapshotBundle | null>(null);
  const [history, setHistory] = useState<MetricPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expandedWorkerId, setExpandedWorkerId] = useState<string>("");

  useEffect(() => {
    let active = true;

    const refresh = async () => {
      try {
        const [overview, workers, queen, system] = await Promise.all([
          fetchOverview({
            windowMinutes: filters.windowMinutes,
            service: filters.service,
            environment: filters.environment,
            token: filters.token || undefined,
          }),
          fetchWorkers({
            windowMinutes: filters.windowMinutes,
            service: filters.service,
            environment: filters.environment,
            token: filters.token || undefined,
          }),
          fetchQueen({
            windowMinutes: filters.windowMinutes,
            service: filters.service,
            environment: filters.environment,
            token: filters.token || undefined,
          }),
          fetchSystem({
            windowMinutes: filters.windowMinutes,
            service: filters.service,
            environment: filters.environment,
            token: filters.token || undefined,
          }),
        ]);

        if (!active) {
          return;
        }

        const queueDepth = asNumber((overview.kpis as Record<string, { value?: unknown }>)?.queue_depth?.value);
        const activeWorkers = asNumber((workers.workers as Record<string, unknown>)?.running);
        const queenNode = queen.queen as Record<string, unknown>;
        const queenQueue = asNumber(queenNode?.followup_queues) + asNumber(queenNode?.internal_queues);

        setBundle({ overview, workers, queen, system });
        setHistory((prev) => [...prev, { at: Date.now(), activeWorkers, queueDepth, queenQueue }].slice(-32));
        setError("");
      } catch (err: unknown) {
        if (!active) {
          return;
        }
        setError(err instanceof Error ? err.message : "Unknown request error");
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    };

    setLoading(true);
    void refresh();
    const timer = window.setInterval(() => {
      void refresh();
    }, 4000);

    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [filters.environment, filters.service, filters.token, filters.windowMinutes]);

  const workers = (((bundle?.workers.workers as { recent?: WorkerRow[] })?.recent ?? []).slice(0, 12));
  const health = (bundle?.overview.health as { status?: string; summary?: string; reasons?: string[] }) ?? {};
  const queenNode = (bundle?.queen.queen as Record<string, unknown>) ?? {};
  const queenHealth = (bundle?.queen.health as { summary?: string; reasons?: string[] }) ?? {};
  const queenQueues = (bundle?.queen.queues as Record<string, unknown>) ?? {};
  const queenControl = (bundle?.queen.control as Record<string, unknown>) ?? {};
  const logs = ((bundle?.system.logs as LogRow[]) ?? []);

  const queenSteps = useMemo(() => buildQueenSteps(logs).slice(0, 10), [logs]);

  const queenState = String(queenNode.state ?? "idle");
  const followupQueues = asNumber(queenNode.followup_queues);
  const internalQueues = asNumber(queenNode.internal_queues);
  const followupTasks = asNumber(queenNode.followup_tasks);
  const internalTasks = asNumber(queenNode.internal_tasks);
  const controlPending = asNumber(queenControl.pending_requests);
  const channelQueueDepth = asNumber(queenQueues.channel_queue_depth);
  const activeChannelLabel = String(queenQueues.active_channel_label ?? queenQueues.active_channel ?? "Channel");
  const currentQueenStep = queenSteps[0];
  const recentQueenSteps = queenSteps.slice(1);
  const waitingOn = deriveQueenWaitingOn({
    state: queenState,
    controlPending,
    channelQueueDepth,
    channelLabel: activeChannelLabel,
    followupQueues,
    internalQueues,
  });
  const queenHeadline =
    currentQueenStep?.title ??
    (queenState === "idle" ? "Queen is idle right now" : queenHealth.summary ?? "Queen is actively orchestrating work");
  const queenSubline =
    currentQueenStep?.detail ??
    ((queenHealth.reasons ?? []).join(" • ") || "No recent orchestration steps in the visible log window.");

  if (loading) {
    return <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-8 text-slate-300">Loading live operations view...</section>;
  }

  if (error) {
    return (
      <section className="rounded-2xl border border-rose-500/40 bg-rose-950/30 p-8 text-rose-200">
        Failed to load dashboard data: {error}
      </section>
    );
  }

  return (
    <div className="grid gap-5">
      <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-xl shadow-slate-950/60">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">System pulse</p>
            <h2 className="mt-2 text-2xl font-semibold text-slate-100">{health.summary ?? "Runtime status"}</h2>
            <p className="mt-2 max-w-3xl text-sm text-slate-400">
              {(health.reasons ?? []).join(" | ") || "No active degradation reasons."}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <span className={`rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-widest ${statusPill(health.status)}`}>
              {String(health.status ?? "unknown")}
            </span>
            <span className="rounded-full border border-slate-700 bg-slate-950/70 px-3 py-1 text-xs text-slate-300">
              Updated {formatLocalDateTime(bundle?.overview.generated_at)}
            </span>
          </div>
        </div>
      </section>

      <div className="grid gap-5 xl:grid-cols-[380px_minmax(0,1fr)]">
        <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4 shadow-xl shadow-slate-950/60 xl:flex xl:h-full xl:flex-col">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Queen</h3>
              <p className="mt-1 text-xs text-slate-500">What she is doing now, and the last orchestration steps.</p>
            </div>
            <span className={`rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-wide ${statusPill(queenState)}`}>
              {queenState}
            </span>
          </div>

          <div className="mt-4 rounded-2xl border border-slate-800 bg-slate-950/70 p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Now</p>
            <p className="mt-2 text-lg font-semibold text-slate-100">{queenHeadline}</p>
            <p className="mt-2 text-sm text-slate-400">{queenSubline}</p>
          </div>

          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
            <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4">
              <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Queue mix</p>
              <div className="mt-3 flex flex-wrap gap-2">
                <span className="rounded-full border border-cyan-400/20 bg-cyan-500/10 px-2.5 py-1 text-xs text-cyan-200">
                  Follow-up {followupQueues}
                </span>
                <span className="rounded-full border border-emerald-400/20 bg-emerald-500/10 px-2.5 py-1 text-xs text-emerald-200">
                  Internal {internalQueues}
                </span>
                <span className="rounded-full border border-amber-300/20 bg-amber-500/10 px-2.5 py-1 text-xs text-amber-200">
                  Control {controlPending}
                </span>
                <span className="rounded-full border border-slate-700 bg-slate-900 px-2.5 py-1 text-xs text-slate-300">
                  {activeChannelLabel} {channelQueueDepth}
                </span>
              </div>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4">
              <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Waiting on</p>
              <p className="mt-3 text-sm text-slate-200">{waitingOn}</p>
              <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-400">
                <span>{followupTasks} follow-up task(s)</span>
                <span>{internalTasks} internal task(s)</span>
              </div>
            </div>
          </div>

          <div className="mt-4 xl:flex-1">
            <div className="mb-3 flex items-center justify-between">
              <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Recent steps</p>
              <span className="text-xs text-slate-500">Last {queenSteps.length} visible events</span>
            </div>
            <div className="max-h-72 space-y-2 overflow-y-auto pr-1">
              {recentQueenSteps.length === 0 ? (
                <div className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-3 text-sm text-slate-400">
                  No recent queen steps in the current log window.
                </div>
              ) : (
                recentQueenSteps.map((step) => (
                  <article key={step.id} className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-3">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-sm font-medium text-slate-100">{step.title}</p>
                        <p className="mt-1 text-xs text-slate-400">{step.detail}</p>
                      </div>
                      <span className={`rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wide ${statusPill(step.level)}`}>
                        {step.level}
                      </span>
                    </div>
                    {step.timestamp ? (
                      <p className="mt-2 text-[11px] text-slate-500">{formatLocalDateTime(step.timestamp)}</p>
                    ) : null}
                  </article>
                ))
              )}
            </div>
          </div>
        </section>

        <RealtimeGraph points={history} />
      </div>

      <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4 shadow-xl shadow-slate-950/60">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Workers</h3>
            <p className="mt-1 text-xs text-slate-500">Click a worker row to inspect result, output, tools, and template config.</p>
          </div>
          <Link
            to="/workers"
            className="rounded-full border border-cyan-400/40 bg-cyan-400/10 px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-cyan-200 transition hover:border-cyan-300/60 hover:bg-cyan-400/15"
          >
            Open workers page
          </Link>
        </div>

        {workers.length === 0 ? (
          <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4 text-slate-400">
            No recent workers in the current filter window.
          </div>
        ) : (
          <div className="max-h-[42rem] overflow-auto">
            <table className="w-full min-w-[1080px] border-separate border-spacing-y-2 text-left text-sm">
              <thead className="sticky top-0 z-10 bg-slate-900/95 text-xs uppercase tracking-wide text-slate-500">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">Hierarchy</th>
                  <th className="px-3 py-2">Status</th>
                  <th className="px-3 py-2">Template</th>
                  <th className="px-3 py-2">Task</th>
                  <th className="px-3 py-2">Result</th>
                  <th className="px-3 py-2">Updated</th>
                </tr>
              </thead>
              <tbody>
                {workers.flatMap((worker, index) => {
                  const workerKey = worker.id ?? worker.updated_at ?? `worker-${index}`;
                  const workerId = worker.id ?? "";
                  const isExpanded = expandedWorkerId === workerId;
                  const hierarchy = hierarchyLabel(worker);
                  const preview = worker.result_preview?.trim() || worker.summary?.trim() || worker.error?.trim() || "No result yet";
                  const templateConfig = worker.template_config ?? null;
                  const allowedTools = templateConfig?.available_tools ?? [];
                  const usedTools = worker.tools_used ?? [];

                  return [
                    <tr
                      key={`${workerKey}-row`}
                      className={`cursor-pointer rounded-xl align-top transition hover:bg-slate-900 ${workerRowTone(worker.status)}`}
                      onClick={() => {
                        if (!workerId) {
                          return;
                        }
                        setExpandedWorkerId((current) => (current === workerId ? "" : workerId));
                      }}
                    >
                      <td className="rounded-l-xl px-3 py-3 font-mono text-xs text-cyan-300">{shortWorkerId(worker.id)}</td>
                      <td className="px-3 py-3 text-xs text-slate-300">
                        <div className="inline-flex items-center gap-1" style={{ paddingLeft: `${Math.min(28, hierarchy.depth * 8)}px` }}>
                          {hierarchy.isChild ? <span className="text-cyan-400">↳</span> : <span className="text-slate-500">◇</span>}
                          <span>{hierarchy.text}</span>
                        </div>
                      </td>
                      <td className="px-3 py-3">
                        <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold uppercase tracking-wide ${statusPill(worker.status)}`}>
                          {String(worker.status ?? "unknown")}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-slate-200">{worker.template_name ?? worker.template_id ?? "n/a"}</td>
                      <td className="max-w-[360px] px-3 py-3 text-slate-300" title={worker.task ?? ""}>
                        <div className="line-clamp-2">{String(worker.task ?? "") || "n/a"}</div>
                      </td>
                      <td title={preview} className="max-w-[260px] px-3 py-3">
                        <div className={`text-sm ${tone(worker.status)}`}>{shortText(preview, 96)}</div>
                      </td>
                      <td className="rounded-r-xl px-3 py-3 text-slate-400">{formatLocalDateTime(worker.updated_at)}</td>
                    </tr>,
                    isExpanded ? (
                      <tr key={`${workerKey}-details`}>
                        <td colSpan={7} className="px-3 pb-3">
                          <div className="space-y-4 rounded-2xl border border-slate-800 bg-slate-950/95 p-4">
                            <div className="flex flex-wrap gap-3 text-xs text-slate-400">
                              <span>Updated: {formatLocalDateTime(worker.updated_at)}</span>
                              <span>Lineage: {shortWorkerId(worker.lineage_id)}</span>
                              <span>Parent: {worker.parent_worker_id ? shortWorkerId(worker.parent_worker_id) : "root"}</span>
                              <span>Depth: {worker.spawn_depth ?? 0}</span>
                            </div>

                            {worker.summary ? (
                              <div className="space-y-1">
                                <div className="text-xs uppercase tracking-[0.2em] text-cyan-300">Summary</div>
                                <div className="rounded-lg border border-cyan-950/80 bg-cyan-950/20 p-3 text-sm text-slate-100">
                                  {worker.summary}
                                </div>
                              </div>
                            ) : null}

                            {worker.error ? (
                              <div className="space-y-1">
                                <div className="text-xs uppercase tracking-[0.2em] text-rose-300">Error</div>
                                <div className="rounded-lg border border-rose-950/80 bg-rose-950/20 p-3 text-sm text-rose-100">
                                  {worker.error}
                                </div>
                              </div>
                            ) : null}

                            {worker.output ? (
                              <div className="space-y-1">
                                <div className="text-xs uppercase tracking-[0.2em] text-emerald-300">Output</div>
                                <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-all rounded-lg border border-slate-800 bg-slate-900 p-3 text-xs text-slate-200">
                                  {JSON.stringify(worker.output, null, 2)}
                                </pre>
                              </div>
                            ) : null}

                            <div className="grid gap-4 xl:grid-cols-2">
                              <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/80 p-3">
                                <div className="text-xs uppercase tracking-[0.2em] text-slate-400">Worker run</div>
                                <div className="flex flex-wrap gap-2 text-xs text-slate-300">
                                  <span className="rounded-full border border-slate-700 bg-slate-950 px-2.5 py-1">
                                    Used tools {usedTools.length}
                                  </span>
                                  {usedTools.length > 0 ? (
                                    <span className="text-slate-400">{usedTools.join(", ")}</span>
                                  ) : (
                                    <span className="text-slate-500">No tools reported</span>
                                  )}
                                </div>
                              </div>

                              <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/80 p-3">
                                <div className="text-xs uppercase tracking-[0.2em] text-slate-400">Template config</div>
                                {templateConfig ? (
                                  <div className="space-y-2 text-xs text-slate-300">
                                    <div className="flex flex-wrap gap-2">
                                      <span className="rounded-full border border-slate-700 bg-slate-950 px-2.5 py-1">
                                        Thinking steps {templateConfig.max_thinking_steps ?? "n/a"}
                                      </span>
                                      <span className="rounded-full border border-slate-700 bg-slate-950 px-2.5 py-1">
                                        Timeout {templateConfig.default_timeout_seconds ?? "n/a"}s
                                      </span>
                                      <span className="rounded-full border border-slate-700 bg-slate-950 px-2.5 py-1">
                                        {templateConfig.can_spawn_children ? "Can spawn children" : "No child spawning"}
                                      </span>
                                    </div>
                                    <div className="text-slate-400">Model: {templateConfig.model || "default"}</div>
                                    <div className="text-slate-400">
                                      Allowed tools: {allowedTools.length > 0 ? allowedTools.join(", ") : "not declared"}
                                    </div>
                                  </div>
                                ) : (
                                  <div className="text-xs text-slate-500">No template config found for this worker.</div>
                                )}
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    ) : null,
                  ];
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
