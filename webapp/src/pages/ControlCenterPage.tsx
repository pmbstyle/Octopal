import { useEffect, useMemo, useState } from "react";
import { Link, useOutletContext } from "react-router-dom";

import { fetchOverview, fetchOcto, fetchSystem, fetchWorkers } from "../api/dashboardClient";
import type { components } from "../api/types";
import type { AppShellOutletContext } from "../ui/AppShell";
import { formatLocalDateTime, formatLocalTime } from "../utils/dateTime";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import {
  CartesianGrid,
  Line,
  LineChart,
  XAxis,
  YAxis,
} from "recharts";
import {
  Clock3,
  type LucideIcon,
} from "lucide-react";

type OverviewPayload = components["schemas"]["DashboardOverviewV2"];
type WorkersPayload = components["schemas"]["DashboardWorkersV2"];
type OctoPayload = components["schemas"]["DashboardOctoV2"];
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
  octoQueue: number;
};

type SnapshotBundle = {
  overview: OverviewPayload;
  workers: WorkersPayload;
  octo: OctoPayload;
  system: SystemPayload;
};

type LogRow = {
  event?: string;
  level?: string;
  timestamp?: string;
  service?: string;
};

type OctoStep = {
  id: string;
  title: string;
  detail: string;
  level: string;
  timestamp?: string;
};

type ScheduledTaskMeta = {
  label: string;
  icon: LucideIcon;
};

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

function prettifyScheduledKind(kind: string): string {
  return kind
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function getScheduledTaskMeta(kind: string): ScheduledTaskMeta {
  return {
    label: prettifyScheduledKind(kind),
    icon: Clock3,
  };
}

function parseScheduledTask(task?: string | null): { meta: ScheduledTaskMeta | null; body: string } {
  const raw = String(task ?? "").trim();
  if (!raw) {
    return { meta: null, body: "" };
  }

  const namedMatch = raw.match(/^\[\s*scheduled\s*:\s*([^\]]+)\]\s*([\s\S]*)$/i);
  if (namedMatch) {
    const [, kind, remainder] = namedMatch;
    return {
      meta: getScheduledTaskMeta(kind),
      body: remainder.trim(),
    };
  }

  const genericMatch = raw.match(/^\[\s*scheduled\s*\]\s*([\s\S]*)$/i);
  if (genericMatch) {
    const [, remainder] = genericMatch;
    return {
      meta: getScheduledTaskMeta("scheduled"),
      body: remainder.trim(),
    };
  }

  return { meta: null, body: raw };
}

function statusPill(status?: string): string {
  const v = String(status ?? "").toLowerCase();
  if (v === "waiting_for_children" || v === "awaiting_instruction") {
    return "bg-slate-200/8 text-slate-200 ring-1 ring-inset ring-white/12";
  }
  if (v === "running" || v === "started" || v === "completed" || v === "ok") {
    return "bg-emerald-500/15 text-emerald-300 ring-1 ring-inset ring-emerald-400/30";
  }
  if (v === "info" || v === "idle") {
    return "bg-cyan-500/12 text-cyan-200 ring-1 ring-inset ring-cyan-400/30";
  }
  if (v === "warning" || v === "thinking" || v === "stopped" || v === "idle") {
    return "bg-amber-500/15 text-amber-300 ring-1 ring-inset ring-amber-300/30";
  }
  return "bg-rose-500/15 text-rose-300 ring-1 ring-inset ring-rose-300/30";
}

function tone(status?: string): string {
  const v = String(status ?? "").toLowerCase();
  if (v === "waiting_for_children" || v === "awaiting_instruction") {
    return "text-slate-200";
  }
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
  if (v === "waiting_for_children" || v === "awaiting_instruction") {
    return "bg-slate-200/[0.05]";
  }
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

function formatEventTitle(event?: string): string {
  const raw = String(event ?? "").trim();
  if (!raw) {
    return "No event message";
  }
  const normalized = raw.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function isOctoRelevantLog(entry: LogRow): boolean {
  const service = String(entry.service ?? "").toLowerCase();
  const event = String(entry.event ?? "").toLowerCase();
  if (service.includes("octo")) {
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

function buildOctoSteps(logs: LogRow[]): OctoStep[] {
  return logs
    .filter(isOctoRelevantLog)
    .map((entry, index) => ({
      id: `${entry.timestamp ?? "ts"}-${index}`,
      title: formatEventTitle(entry.event),
      detail: `${String(entry.service ?? "runtime")} • ${String(entry.level ?? "info")}`,
      level: String(entry.level ?? "info").toLowerCase(),
      timestamp: entry.timestamp,
    }));
}

function countToolUsage(tools: string[]): Array<{ name: string; count: number }> {
  const counts = new Map<string, number>();
  for (const tool of tools) {
    const name = String(tool ?? "").trim();
    if (!name) {
      continue;
    }
    counts.set(name, (counts.get(name) ?? 0) + 1);
  }
  return Array.from(counts.entries()).map(([name, count]) => ({ name, count }));
}

function deriveOctoWaitingOn(args: {
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
  const chartData = points.map((point) => ({
    label: formatLocalTime(point.at),
    workers: point.activeWorkers,
    systemQueue: point.queueDepth,
    octoQueue: point.octoQueue,
    timestamp: formatLocalDateTime(point.at),
  }));

  const chartConfig = {
    workers: { label: "Workers", color: "#22d3ee" },
    systemQueue: { label: "System queue", color: "#f59e0b" },
    octoQueue: { label: "Octo queue", color: "#22c55e" },
  } satisfies ChartConfig;

  return (
    <Card className="border-white/6 bg-[var(--surface-panel)] py-0 shadow-[0_24px_80px_rgba(0,0,0,0.26)] xl:flex xl:flex-1 xl:flex-col">
      <CardHeader className="flex flex-row items-start justify-between gap-3 border-b border-white/6 px-5 py-5 md:px-6">
        <div>
          <CardTitle className="text-sm uppercase tracking-[0.16em] text-[var(--text-strong)]">Live Load</CardTitle>
          <CardDescription>Workers, total system queue, and octo-only queue.</CardDescription>
        </div>
        <Badge variant="outline" className="rounded-full border-white/10 bg-white/[0.04] text-[var(--text-muted)]">
          Last {points.length} samples
        </Badge>
      </CardHeader>
      <CardContent className="px-5 pb-5 pt-4 md:px-6 md:pb-6">
        <ChartContainer config={chartConfig} className="h-72 w-full rounded-[22px] border border-white/6 bg-black/20 p-3">
          <LineChart data={chartData} margin={{ top: 10, right: 12, left: 0, bottom: 0 }}>
            <CartesianGrid vertical={false} stroke="rgba(255,255,255,0.08)" />
            <XAxis
              dataKey="label"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={20}
            />
            <YAxis tickLine={false} axisLine={false} tickMargin={8} width={36} />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  labelFormatter={(_, payload) => {
                    const item = payload?.[0]?.payload as { timestamp?: string } | undefined;
                    return item?.timestamp ?? "";
                  }}
                />
              }
            />
            <ChartLegend content={<ChartLegendContent />} />
            <Line type="monotone" dataKey="workers" stroke="var(--color-workers)" strokeWidth={3} dot={false} />
            <Line type="monotone" dataKey="systemQueue" stroke="var(--color-systemQueue)" strokeWidth={2.5} dot={false} />
            <Line type="monotone" dataKey="octoQueue" stroke="var(--color-octoQueue)" strokeWidth={2.5} dot={false} />
          </LineChart>
        </ChartContainer>
      </CardContent>
    </Card>
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
        const [overview, workers, octo, system] = await Promise.all([
          fetchOverview({
            windowMinutes: filters.windowMinutes,
            service: filters.service,
            environment: filters.environment,
            token: filters.token || undefined,
          }),
          fetchWorkers({
            last: 16,
            windowMinutes: filters.windowMinutes,
            service: filters.service,
            environment: filters.environment,
            token: filters.token || undefined,
          }),
          fetchOcto({
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
        const octoNode = octo.octo as Record<string, unknown>;
        const octoQueue = asNumber(octoNode?.followup_queues) + asNumber(octoNode?.internal_queues);

        setBundle({ overview, workers, octo, system });
        setHistory((prev) => [...prev, { at: Date.now(), activeWorkers, queueDepth, octoQueue }].slice(-32));
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

  const workers = (((bundle?.workers.workers as { recent?: WorkerRow[] })?.recent ?? []).slice(0, 16));
  const octoNode = (bundle?.octo.octo as Record<string, unknown>) ?? {};
  const octoHealth = (bundle?.octo.health as { summary?: string; reasons?: string[] }) ?? {};
  const octoQueues = (bundle?.octo.queues as Record<string, unknown>) ?? {};
  const octoControl = (bundle?.octo.control as Record<string, unknown>) ?? {};
  const logs = ((bundle?.system.logs as LogRow[]) ?? []);

  const octoSteps = useMemo(() => buildOctoSteps(logs).slice(0, 10), [logs]);

  const octoState = String(octoNode.state ?? "idle");
  const followupQueues = asNumber(octoNode.followup_queues);
  const internalQueues = asNumber(octoNode.internal_queues);
  const followupTasks = asNumber(octoNode.followup_tasks);
  const internalTasks = asNumber(octoNode.internal_tasks);
  const controlPending = asNumber(octoControl.pending_requests);
  const channelQueueDepth = asNumber(octoQueues.channel_queue_depth);
  const activeChannelLabel = String(octoQueues.active_channel_label ?? octoQueues.active_channel ?? "Channel");
  const currentOctoStep = octoSteps[0];
  const recentOctoSteps = octoSteps.slice(1);
  const waitingOn = deriveOctoWaitingOn({
    state: octoState,
    controlPending,
    channelQueueDepth,
    channelLabel: activeChannelLabel,
    followupQueues,
    internalQueues,
  });
  const octoHeadline =
    currentOctoStep?.title ??
    (octoState === "idle" ? "Octo is idle right now" : octoHealth.summary ?? "Octo is actively orchestrating work");
  const octoSubline =
    currentOctoStep?.detail ??
    ((octoHealth.reasons ?? []).join(" • ") || "No recent orchestration steps in the visible log window.");

  if (loading) {
    return <Card className="border-white/6 bg-[var(--surface-panel)] py-0"><CardContent className="p-8 text-[var(--text-muted)]">Loading live operations view...</CardContent></Card>;
  }

  if (error) {
    return (
      <section className="rounded-[30px] border border-rose-400/30 bg-rose-950/20 p-8 text-rose-200">
        Failed to load dashboard data: {error}
      </section>
    );
  }

  return (
    <div className="grid gap-6">
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_360px] xl:items-stretch">
        <div className="flex flex-col">
          <RealtimeGraph points={history} />
          <section className="grid gap-4 mt-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
            <div className="rounded-xl border border-white/6 bg-[var(--surface-panel)] p-5 shadow-[0_24px_80px_rgba(0,0,0,0.2)]">
              <p className="text-xs uppercase tracking-[0.16em] text-[var(--text-dim)]">Focus</p>
              <p className="mt-2 text-sm text-[var(--text-strong)]">{waitingOn}</p>
              <div className="mt-3 flex flex-wrap gap-2">
                <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
                  {followupTasks} follow-up task(s)
                </Badge>
                <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
                  {internalTasks} internal task(s)
                </Badge>
              </div>
            </div>

            <div className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-xl border border-white/6 bg-[var(--surface-panel)] p-5 shadow-[0_24px_80px_rgba(0,0,0,0.2)]">
                <p className="text-xs uppercase tracking-[0.16em] text-[var(--text-dim)]">Queue mix</p>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Badge variant="outline" className="rounded-full border-cyan-400/20 bg-cyan-500/10 text-cyan-200">
                    Follow-up {followupQueues}
                  </Badge>
                  <Badge variant="outline" className="rounded-full border-emerald-400/20 bg-emerald-500/10 text-emerald-200">
                    Internal {internalQueues}
                  </Badge>
                  <Badge variant="outline" className="rounded-full border-amber-300/20 bg-amber-500/10 text-amber-200">
                    Control {controlPending}
                  </Badge>
                  <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
                    {activeChannelLabel} {channelQueueDepth}
                  </Badge>
                </div>
              </div>

              <div className="rounded-xl border border-white/6 bg-[var(--surface-panel)] p-5 shadow-[0_24px_80px_rgba(0,0,0,0.2)]">
                <p className="text-xs uppercase tracking-[0.16em] text-[var(--text-dim)]">State detail</p>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Badge variant="outline" className="rounded-full border-cyan-400/20 bg-cyan-500/10 text-cyan-200">
                    State {octoState}
                  </Badge>
                  <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
                    {octoSteps.length} recent events
                  </Badge>
                </div>
              </div>
            </div>
          </section>
        </div>

        <section className="h-full rounded-xl border border-white/6 bg-[var(--surface-panel)] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.26)] xl:flex xl:min-h-0 xl:flex-col">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">Octo</h3>
              <p className="mt-1 text-xs text-[var(--text-dim)]">What she is doing now, and the last orchestration steps.</p>
            </div>
            <span className={`rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-wide ${statusPill(octoState)}`}>
              {octoState}
            </span>
          </div>

          <div className="mt-4 rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] px-4 py-4">
            <p className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Now</p>
            <p className="mt-3 text-sm font-medium text-white">{octoHeadline}</p>
            <p className="mt-2 text-xs leading-5 text-[var(--text-muted)]">{octoSubline}</p>
          </div>

          <div className="mt-4 min-h-0 xl:flex xl:flex-1 xl:flex-col">
            <div className="mb-3 flex items-center justify-between">
              <p className="text-xs uppercase tracking-[0.16em] text-[var(--text-dim)]">Recent steps</p>
              <span className="text-xs text-[var(--text-dim)]">Last {octoSteps.length} visible events</span>
            </div>
            <div className="max-h-[26rem] space-y-2 overflow-y-auto pr-2 xl:min-h-0 xl:flex-1">
              {recentOctoSteps.length === 0 ? (
                <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] px-3 py-3 text-sm text-[var(--text-muted)]">
                  No recent octo steps in the current log window.
                </div>
              ) : (
                recentOctoSteps.map((step) => (
                  <article key={step.id} className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] px-3 py-3">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-sm font-medium text-white">{step.title}</p>
                        <p className="mt-1 text-xs text-[var(--text-muted)]">{step.detail}</p>
                      </div>
                      <span className={`rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wide ${statusPill(step.level)}`}>
                        {step.level}
                      </span>
                    </div>
                    {step.timestamp ? (
                      <p className="mt-2 text-[11px] text-[var(--text-dim)]">{formatLocalDateTime(step.timestamp)}</p>
                    ) : null}
                  </article>
                ))
              )}
            </div>
          </div>
        </section>
      </div>

      <Card className="border-white/6 bg-[var(--surface-panel)] py-0 shadow-[0_24px_80px_rgba(0,0,0,0.26)]">
        <CardHeader className="flex flex-row items-center justify-between gap-3 border-b border-white/6 px-5 py-5 md:px-6">
          <div>
            <CardTitle className="text-sm uppercase tracking-[0.16em] text-[var(--text-strong)]">Workers</CardTitle>
            <CardDescription>Click a worker row to inspect result, output, tools, and template config.</CardDescription>
          </div>
          <Button asChild variant="secondary" className="rounded-full bg-white/[0.08] text-[var(--text-strong)] hover:bg-white/[0.12]">
            <Link to="/workers">Open workers page</Link>
          </Button>
        </CardHeader>
        <CardContent className="px-5 pb-5 pt-4 md:px-6 md:pb-6">

          {workers.length === 0 ? (
            <div className="rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] p-4 text-[var(--text-muted)]">
              No recent workers in the current filter window.
            </div>
          ) : (
            <div className="max-h-[42rem] overflow-auto rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)]">
              <div>
                <Table className="min-w-[1120px] table-fixed">
                  <colgroup>
                    <col className="w-[112px]" />
                    <col className="w-[132px]" />
                    <col className="w-[148px]" />
                    <col className="w-[210px]" />
                    <col className="w-[36%]" />
                    <col className="w-[28%]" />
                    <col className="w-[180px]" />
                  </colgroup>
                  <TableHeader>
                    <TableRow className="border-white/6 hover:bg-transparent">
                      <TableHead>ID</TableHead>
                      <TableHead>Hierarchy</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Template</TableHead>
                      <TableHead>Task</TableHead>
                      <TableHead>Result</TableHead>
                      <TableHead>Updated</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {workers.flatMap((worker, index) => {
                      const workerKey = worker.id ?? worker.updated_at ?? `worker-${index}`;
                      const workerId = worker.id ?? "";
                      const isExpanded = expandedWorkerId === workerId;
                      const hierarchy = hierarchyLabel(worker);
                      const scheduledTask = parseScheduledTask(worker.task);
                      const preview = worker.result_preview?.trim() || worker.summary?.trim() || worker.error?.trim() || "No result yet";
                      const templateConfig = worker.template_config ?? null;
                      const allowedTools = templateConfig?.available_tools ?? [];
                      const usedTools = worker.tools_used ?? [];
                      const usedToolCounts = countToolUsage(usedTools);

                      return [
                        <tr
                          key={`${workerKey}-row`}
                          className={`cursor-pointer align-top transition hover:bg-white/[0.03] ${workerRowTone(worker.status)}`}
                          onClick={() => {
                            if (!workerId) {
                              return;
                            }
                            setExpandedWorkerId((current) => (current === workerId ? "" : workerId));
                          }}
                        >
                          <TableCell className="font-mono text-xs text-cyan-300">{shortWorkerId(worker.id)}</TableCell>
                          <TableCell className="text-xs text-[var(--text-strong)]">
                            <div className="inline-flex items-center gap-1" style={{ paddingLeft: `${Math.min(28, hierarchy.depth * 8)}px` }}>
                              {hierarchy.isChild ? <span className="text-cyan-400">↳</span> : <span className="text-[var(--text-dim)]">◇</span>}
                              <span>{hierarchy.text}</span>
                            </div>
                          </TableCell>
                          <TableCell>
                            <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold uppercase tracking-wide ${statusPill(worker.status)}`}>
                              {String(worker.status ?? "unknown")}
                            </span>
                          </TableCell>
                          <TableCell className="text-[var(--text-strong)]" title={worker.template_name ?? worker.template_id ?? ""}>
                            <div className="truncate">{worker.template_name ?? worker.template_id ?? "n/a"}</div>
                          </TableCell>
                          <TableCell className="whitespace-normal text-[var(--text-muted)]" title={worker.task ?? ""}>
                            <div className="space-y-1">
                              {scheduledTask.meta ? (
                                <div>
                                  <span className="inline-flex items-center gap-1 rounded-full border border-cyan-400/30 bg-cyan-500/12 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-cyan-200">
                                    <scheduledTask.meta.icon className="h-3.5 w-3.5" />
                                    Scheduled
                                    <span className="text-white/70">{scheduledTask.meta.label}</span>
                                  </span>
                                </div>
                              ) : null}
                              <div className={`break-words ${scheduledTask.meta ? "line-clamp-3 text-[var(--text-strong)]" : "line-clamp-2"}`}>
                                {scheduledTask.body || "n/a"}
                              </div>
                            </div>
                          </TableCell>
                          <TableCell title={preview} className="whitespace-normal">
                            <div className={`line-clamp-2 break-words text-sm ${tone(worker.status)}`}>{preview}</div>
                          </TableCell>
                          <TableCell className="text-[var(--text-dim)]">{formatLocalDateTime(worker.updated_at)}</TableCell>
                        </tr>,
                        isExpanded ? (
                          <TableRow key={`${workerKey}-details`} className="border-white/6 hover:bg-transparent">
                            <TableCell colSpan={7} className="p-4">
                              <div className="space-y-4 rounded-[22px] border border-white/6 bg-black/20 p-4">
                                <div className="flex flex-wrap gap-2 text-xs">
                                  <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">Updated {formatLocalDateTime(worker.updated_at)}</Badge>
                                  <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">Lineage {shortWorkerId(worker.lineage_id)}</Badge>
                                  <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">Parent {worker.parent_worker_id ? shortWorkerId(worker.parent_worker_id) : "root"}</Badge>
                                  <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">Depth {worker.spawn_depth ?? 0}</Badge>
                                </div>

                                {worker.summary ? (
                                  <div className="space-y-1">
                                    <div className="text-xs uppercase tracking-[0.2em] text-cyan-300">Summary</div>
                                    <div className="rounded-lg border border-white/6 bg-cyan-500/10 p-3 text-sm text-white whitespace-pre-wrap break-words">
                                      {worker.summary}
                                    </div>
                                  </div>
                                ) : null}

                                {worker.error ? (
                                  <div className="space-y-1">
                                    <div className="text-xs uppercase tracking-[0.2em] text-rose-300">Error</div>
                                    <div className="rounded-lg border border-rose-400/15 bg-rose-500/10 p-3 text-sm text-rose-100 whitespace-pre-wrap break-words">
                                      {worker.error}
                                    </div>
                                  </div>
                                ) : null}

                                {worker.output ? (
                                  <div className="space-y-1">
                                    <div className="text-xs uppercase tracking-[0.2em] text-emerald-300">Output</div>
                                    <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-all rounded-lg border border-white/6 bg-[var(--surface-panel-strong)] p-3 text-xs text-[var(--text-strong)]">
                                      {JSON.stringify(worker.output, null, 2)}
                                    </pre>
                                  </div>
                                ) : null}

                                <div className="grid gap-4 xl:grid-cols-2">
                                  <div className="space-y-2 rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] p-3">
                                    <div className="text-xs uppercase tracking-[0.2em] text-[var(--text-dim)]">Worker run</div>
                                    <div className="space-y-2 text-xs text-[var(--text-strong)]">
                                      <span className="rounded-full border border-white/8 bg-white/[0.04] px-2.5 py-1">
                                        Used tools {usedTools.length}
                                      </span>
                                      {usedToolCounts.length > 0 ? (
                                        <div className="flex flex-wrap gap-2">
                                          {usedToolCounts.map(({ name, count }) => (
                                            <span
                                              key={`used-tool-${name}`}
                                              className="rounded-full border border-cyan-400/20 bg-cyan-500/10 px-2.5 py-1 text-[11px] text-cyan-100"
                                            >
                                              {name}
                                              {count > 1 ? ` x${count}` : ""}
                                            </span>
                                          ))}
                                        </div>
                                      ) : (
                                        <span className="text-[var(--text-dim)]">No tools reported</span>
                                      )}
                                    </div>
                                  </div>

                                  <div className="space-y-2 rounded-[22px] border border-white/6 bg-[var(--surface-panel-strong)] p-3">
                                    <div className="text-xs uppercase tracking-[0.2em] text-[var(--text-dim)]">Template config</div>
                                    {templateConfig ? (
                                      <div className="space-y-2 text-xs text-[var(--text-strong)]">
                                        <div className="flex flex-wrap gap-2">
                                          <span className="rounded-full border border-white/8 bg-white/[0.04] px-2.5 py-1">
                                            Thinking steps {templateConfig.max_thinking_steps ?? "n/a"}
                                          </span>
                                          <span className="rounded-full border border-white/8 bg-white/[0.04] px-2.5 py-1">
                                            Timeout {templateConfig.default_timeout_seconds ?? "n/a"}s
                                          </span>
                                          <span className="rounded-full border border-white/8 bg-white/[0.04] px-2.5 py-1">
                                            {templateConfig.can_spawn_children ? "Can spawn children" : "No child spawning"}
                                          </span>
                                        </div>
                                        <div className="text-[var(--text-muted)]">Model: {templateConfig.model || "default"}</div>
                                        <div className="space-y-2">
                                          <div className="text-[var(--text-muted)]">Allowed tools</div>
                                          {allowedTools.length > 0 ? (
                                            <div className="flex flex-wrap gap-2">
                                              {allowedTools.map((toolName) => (
                                                <span
                                                  key={`allowed-tool-${toolName}`}
                                                  className="rounded-full border border-white/8 bg-white/[0.04] px-2.5 py-1 text-[11px] text-[var(--text-muted)]"
                                                >
                                                  {toolName}
                                                </span>
                                              ))}
                                            </div>
                                          ) : (
                                            <div className="text-[var(--text-dim)]">Not declared</div>
                                          )}
                                        </div>
                                      </div>
                                    ) : (
                                      <div className="text-xs text-[var(--text-dim)]">No template config found for this worker.</div>
                                    )}
                                  </div>
                                </div>
                              </div>
                            </TableCell>
                          </TableRow>
                        ) : null,
                      ];
                    })}
                  </TableBody>
                </Table>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
