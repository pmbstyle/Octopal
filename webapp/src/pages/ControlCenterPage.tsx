import { useEffect, useMemo, useState } from "react";

import { fetchOverview, fetchQueen, fetchWorkers } from "../api/dashboardClient";
import type { components } from "../api/types";
import type { DashboardFilters } from "../ui/GlobalFiltersBar";

type OverviewPayload = components["schemas"]["DashboardOverviewV2"];
type WorkersPayload = components["schemas"]["DashboardWorkersV2"];
type QueenPayload = components["schemas"]["DashboardQueenV2"];

type WorkerRow = {
  id?: string;
  template_name?: string;
  status?: string;
  task?: string;
  updated_at?: string;
  parent_worker_id?: string | null;
  spawn_depth?: number;
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
};

type WorkerTooltip = {
  title: string;
  lines: string[];
  x: number;
  y: number;
  wide?: boolean;
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

function shortWorkerId(value?: string): string {
  if (!value) {
    return "n/a";
  }
  return value.includes("-") ? value.split("-")[0] : value.slice(0, 8);
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
  if (v === "warning" || v === "thinking" || v === "stopped") {
    return "bg-amber-500/15 text-amber-300 ring-1 ring-inset ring-amber-300/30";
  }
  return "bg-rose-500/15 text-rose-300 ring-1 ring-inset ring-rose-300/30";
}

function statusMeta(status?: string): { icon: string; color: string; title: string } {
  const v = String(status ?? "").toLowerCase();
  if (v === "completed") {
    return { icon: "✓", color: "text-emerald-300", title: "completed" };
  }
  if (v === "running" || v === "started" || v === "ok" || v === "thinking") {
    return { icon: "●", color: "text-cyan-300", title: v || "active" };
  }
  if (v === "warning" || v === "stopped") {
    return { icon: "!", color: "text-amber-300", title: v || "warning" };
  }
  return { icon: "×", color: "text-rose-300", title: v || "failed" };
}

function prettyTime(value?: string): string {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
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
  const startLabel = firstPoint
    ? new Date(firstPoint.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    : "--:--";
  const endLabel = lastPoint
    ? new Date(lastPoint.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    : "--:--";
  const tzLabel = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const activeIndex = hoverIndex !== null && hoverIndex >= 0 && hoverIndex < points.length ? hoverIndex : null;
  const activePoint = activeIndex !== null ? points[activeIndex] : null;
  const step = points.length > 1 ? width / (points.length - 1) : width;
  const markerX = activeIndex !== null ? activeIndex * step : 0;

  return (
    <section className="relative rounded-2xl border border-slate-800 bg-slate-900/70 p-4 shadow-xl shadow-slate-950/60">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Realtime Signal</h3>
        <span className="text-xs text-slate-400">Last {points.length} samples</span>
      </div>
      <p className="mb-2 text-xs text-slate-500">
        Y-axis = shared metric count scale (integer counts, 0 to {yAxisMax}). X-axis = local browser time.
      </p>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="h-52 w-full rounded-lg bg-slate-950/80"
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
          return (
          <line
            key={tick}
            x1={0}
            x2={width}
            y1={y}
            y2={y}
            stroke="#1e293b"
            strokeWidth={1}
          />
        )})}
        {yTicks.map((tick) => {
          const y = GRAPH_TOP_PAD + plotHeight - (tick / yAxisMax) * plotHeight;
          return (
          <text
            key={`label-${tick}`}
            x={width - 8}
            y={y - 4}
            fill="#64748b"
            fontSize="10"
            textAnchor="end"
          >
            {tick}
          </text>
        )})}
        <text x={width - 8} y={height - 4} fill="#64748b" fontSize="10" textAnchor="end">
          0
        </text>
        <path d={workerLine} fill="none" stroke="#06b6d4" strokeWidth={3} strokeLinecap="round" />
        <path d={queueLine} fill="none" stroke="#f59e0b" strokeWidth={2.5} strokeLinecap="round" />
        <path d={queenLine} fill="none" stroke="#22c55e" strokeWidth={2.5} strokeLinecap="round" />
        {activePoint ? (
          <>
            <line x1={markerX} x2={markerX} y1={0} y2={height} stroke="#64748b" strokeWidth={1} strokeDasharray="5 4" />
          </>
        ) : null}
      </svg>
      {activePoint ? (
        <div className="pointer-events-none absolute right-6 top-16 rounded-lg border border-slate-700 bg-slate-950/95 px-3 py-2 text-xs text-slate-200 shadow-xl">
          <p className="mb-1 text-[11px] text-slate-400">
            {new Date(activePoint.at).toLocaleString([], {
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
              month: "short",
              day: "2-digit",
            })}{" "}
            ({tzLabel})
          </p>
          <p className="text-cyan-300">Workers: {activePoint.activeWorkers}</p>
          <p className="text-amber-300">System queue: {activePoint.queueDepth}</p>
          <p className="text-emerald-300">Queen queue: {activePoint.queenQueue}</p>
        </div>
      ) : null}
      <div className="mt-2 flex items-center justify-between text-[11px] text-slate-500">
        <span>{startLabel}</span>
        <span>
          Local time axis ({tzLabel}): {startLabel} {"->"} {endLabel}
        </span>
        <span>{endLabel}</span>
      </div>
      <div className="mt-3 flex flex-wrap gap-4 text-xs text-slate-300">
        <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-cyan-400" />Workers</span>
        <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-amber-400" />System queue</span>
        <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-emerald-400" />Queen queue</span>
      </div>
    </section>
  );
}

export function ControlCenterPage({ filters }: { filters: DashboardFilters }) {
  const [bundle, setBundle] = useState<SnapshotBundle | null>(null);
  const [history, setHistory] = useState<MetricPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [workerTooltip, setWorkerTooltip] = useState<WorkerTooltip | null>(null);

  useEffect(() => {
    let active = true;

    const refresh = async () => {
      try {
        const [overview, workers, queen] = await Promise.all([
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
        ]);

        if (!active) {
          return;
        }

        const queueDepth = asNumber((overview.kpis as Record<string, { value?: unknown }>)?.queue_depth?.value);
        const activeWorkers = asNumber((workers.workers as Record<string, unknown>)?.running);
        const queenNode = queen.queen as Record<string, unknown>;
        const queenQueue = asNumber(queenNode?.followup_queues) + asNumber(queenNode?.internal_queues);

        setBundle({ overview, workers, queen });
        setHistory((prev) =>
          [...prev, { at: Date.now(), activeWorkers, queueDepth, queenQueue }].slice(-32),
        );
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

  const workers = ((bundle?.workers.workers as { recent?: WorkerRow[] })?.recent ?? []).slice(0, 12);
  const health = (bundle?.overview.health as { status?: string; summary?: string; reasons?: string[] }) ?? {};
  const kpis = (bundle?.overview.kpis as Record<string, { value?: unknown }>) ?? {};
  const incidents = bundle?.overview.incidents_summary ?? { open: 0, critical: 0, warning: 0 };
  const queen = (bundle?.queen.queen as Record<string, unknown>) ?? {};

  const metricCards = useMemo(
    () => [
      { label: "Active workers", value: asNumber((bundle?.workers.workers as Record<string, unknown>)?.running) },
      { label: "Queue depth", value: asNumber(kpis.queue_depth?.value) },
      { label: "Queen queue", value: asNumber(queen.followup_queues) + asNumber(queen.internal_queues) },
      { label: "Error rate", value: `${asNumber(kpis.error_rate_5m?.value).toFixed(2)}%` },
      { label: "Latency p95", value: `${asNumber(kpis.latency_ms_p95?.value)} ms` },
      { label: "Open incidents", value: incidents.open },
    ],
    [bundle?.workers.workers, incidents.open, kpis.error_rate_5m?.value, kpis.latency_ms_p95?.value, kpis.queue_depth?.value, queen.followup_queues, queen.internal_queues],
  );

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

  const showWorkerTooltip = (
    element: HTMLElement,
    payload: { title: string; lines: string[]; wide?: boolean },
  ) => {
    const rect = element.getBoundingClientRect();
    const tooltipWidth = payload.wide ? 520 : 320;
    const viewportWidth = window.innerWidth;
    const left = Math.max(12, Math.min(rect.left, viewportWidth - tooltipWidth - 12));
    const top = Math.min(window.innerHeight - 120, rect.bottom + 8);
    setWorkerTooltip({
      title: payload.title,
      lines: payload.lines,
      x: left,
      y: top,
      wide: payload.wide,
    });
  };

  const hideWorkerTooltip = () => setWorkerTooltip(null);

  return (
    <div className="grid gap-5">
      <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-xl shadow-slate-950/60">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">System pulse</p>
            <h2 className="mt-2 text-2xl font-semibold text-slate-100">
              {health.summary ?? "Runtime status"}
            </h2>
            <p className="mt-2 max-w-3xl text-sm text-slate-400">
              {(health.reasons ?? []).join(" | ") || "No active degradation reasons."}
            </p>
          </div>
          <div className={`rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-widest ${statusPill(health.status)}`}>
            {String(health.status ?? "unknown")}
          </div>
        </div>
      </section>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
        {metricCards.map((item) => (
          <article key={item.label} className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
            <p className="text-xs uppercase tracking-wide text-slate-400">{item.label}</p>
            <p className="mt-2 text-2xl font-semibold text-slate-100">{String(item.value)}</p>
          </article>
        ))}
      </section>

      <div className="grid gap-5 xl:grid-cols-3">
        <div className="xl:col-span-2">
          <RealtimeGraph points={history} />
        </div>

        <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4">
          <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Incidents</h3>
          <div className="mt-4 space-y-3 text-sm">
            <div className="flex items-center justify-between rounded-lg bg-slate-950/70 px-3 py-2">
              <span className="text-slate-400">Open</span>
              <span className="font-semibold text-slate-100">{incidents.open}</span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-slate-950/70 px-3 py-2">
              <span className="text-slate-400">Critical</span>
              <span className="font-semibold text-rose-300">{incidents.critical}</span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-slate-950/70 px-3 py-2">
              <span className="text-slate-400">Warning</span>
              <span className="font-semibold text-amber-300">{incidents.warning}</span>
            </div>
            <div className="rounded-lg bg-slate-950/70 px-3 py-2">
              <p className="text-xs uppercase tracking-wider text-slate-500">Last update</p>
              <p className="mt-1 text-sm text-slate-300">{prettyTime(bundle?.overview.generated_at)} (local)</p>
            </div>
          </div>
        </section>
      </div>

      <section className="relative rounded-2xl border border-slate-800 bg-slate-900/70 p-4">
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Workers</h3>
          <p className="text-xs text-slate-500">Top 12 by recency, timestamps in local browser time</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[760px] border-separate border-spacing-y-2 text-left text-sm">
            <thead className="text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">Hierarchy</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Task</th>
                <th className="px-3 py-2">Updated</th>
              </tr>
            </thead>
            <tbody>
              {workers.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-3 py-4 text-slate-400">
                    No active workers in current filter window.
                  </td>
                </tr>
              ) : (
                workers.map((worker) => {
                  const hierarchy = hierarchyLabel(worker);
                  const status = statusMeta(worker.status);
                  return (
                  <tr key={`${worker.id}-${worker.updated_at}`} className="rounded-lg bg-slate-950/70">
                    <td className="rounded-l-lg px-3 py-3 font-mono text-xs text-cyan-300">
                      <span className="cursor-help underline decoration-dotted underline-offset-4">
                        <button
                          type="button"
                          className="font-mono text-xs text-cyan-300"
                          onMouseEnter={(event) =>
                            showWorkerTooltip(event.currentTarget, {
                              title: "Worker details",
                              lines: [
                                `ID: ${worker.id ?? "n/a"}`,
                                `Template: ${worker.template_name ?? "n/a"}`,
                                `Status: ${String(worker.status ?? "unknown")}`,
                                `Updated: ${prettyTime(worker.updated_at)} (local)`,
                              ],
                            })
                          }
                          onMouseLeave={hideWorkerTooltip}
                          onClick={(event) =>
                            showWorkerTooltip(event.currentTarget, {
                              title: "Worker details",
                              lines: [
                                `ID: ${worker.id ?? "n/a"}`,
                                `Template: ${worker.template_name ?? "n/a"}`,
                                `Status: ${String(worker.status ?? "unknown")}`,
                                `Updated: ${prettyTime(worker.updated_at)} (local)`,
                              ],
                            })
                          }
                        >
                          {shortWorkerId(worker.id)}
                        </button>
                      </span>
                    </td>
                    <td className="px-3 py-3 text-xs text-slate-300">
                      <div
                        className="inline-flex items-center gap-1"
                        style={{ paddingLeft: `${Math.min(28, hierarchy.depth * 8)}px` }}
                      >
                        {hierarchy.isChild ? <span className="text-cyan-400">↳</span> : <span className="text-slate-500">◇</span>}
                        <span>{hierarchy.text}</span>
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <span
                        className={`inline-flex h-7 w-7 items-center justify-center rounded-full text-sm font-bold ${statusPill(worker.status)} ${status.color}`}
                        title={status.title}
                      >
                        {status.icon}
                      </span>
                    </td>
                    <td className="max-w-[520px] truncate px-3 py-3 text-slate-300">
                      <button
                        type="button"
                        className="max-w-[520px] cursor-help truncate text-left underline decoration-dotted underline-offset-4"
                        onMouseEnter={(event) =>
                          showWorkerTooltip(event.currentTarget, {
                            title: "Task prompt",
                            lines: [
                              worker.task ?? "n/a",
                              "",
                              `ID: ${worker.id ?? "n/a"}`,
                              `Template: ${worker.template_name ?? "n/a"}`,
                            ],
                            wide: true,
                          })
                        }
                        onMouseLeave={hideWorkerTooltip}
                        onClick={(event) =>
                          showWorkerTooltip(event.currentTarget, {
                            title: "Task prompt",
                            lines: [
                              worker.task ?? "n/a",
                              "",
                              `ID: ${worker.id ?? "n/a"}`,
                              `Template: ${worker.template_name ?? "n/a"}`,
                            ],
                            wide: true,
                          })
                        }
                      >
                        {worker.task ?? "n/a"}
                      </button>
                    </td>
                    <td className="rounded-r-lg px-3 py-3 text-slate-400">{prettyTime(worker.updated_at)}</td>
                  </tr>
                )})
              )}
            </tbody>
          </table>
        </div>
        {workerTooltip ? (
          <div
            className={`pointer-events-none fixed z-[100] rounded-lg border border-slate-700 bg-slate-950/95 p-3 text-xs text-slate-200 shadow-xl ${
              workerTooltip.wide ? "w-[32rem] max-w-[84vw]" : "w-80 max-w-[84vw]"
            }`}
            style={{ left: workerTooltip.x, top: workerTooltip.y }}
          >
            <p className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">{workerTooltip.title}</p>
            {workerTooltip.lines.map((line, index) =>
              line ? (
                <p key={`${line}-${index}`} className={index === 0 && workerTooltip.wide ? "whitespace-pre-wrap break-words" : ""}>
                  {line}
                </p>
              ) : (
                <div key={`spacer-${index}`} className="h-2" />
              ),
            )}
          </div>
        ) : null}
      </section>
    </div>
  );
}
