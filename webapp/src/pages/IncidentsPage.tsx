import { useEffect, useState } from "react";
import { Link, useOutletContext } from "react-router-dom";

import { fetchIncidents } from "../api/dashboardClient";
import type { components } from "../api/types";
import type { AppShellOutletContext } from "../ui/AppShell";
import { formatLocalDateTime } from "../utils/dateTime";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Activity, AlertTriangle, ArrowRight, Clock3, RadioTower, Wrench } from "lucide-react";

type IncidentsPayload = components["schemas"]["DashboardIncidentsV2"];
type IncidentItem = {
  id?: string;
  service?: string;
  severity?: string;
  impact?: number;
  title?: string;
  summary?: string;
  count?: number;
  latest_at?: string;
  source?: string;
};

function severityTone(value?: string): string {
  const v = String(value ?? "").toLowerCase();
  if (v === "critical") {
    return "border-rose-300/30 bg-rose-500/10 text-rose-300";
  }
  if (v === "warning") {
    return "border-amber-300/30 bg-amber-500/10 text-amber-300";
  }
  return "border-emerald-400/30 bg-emerald-500/10 text-emerald-300";
}

function severityCopy(value?: string): string {
  const v = String(value ?? "").toLowerCase();
  if (v === "critical") {
    return "Needs action";
  }
  if (v === "warning") {
    return "Watch";
  }
  return "Informational";
}

function sourceLabel(source?: string): string {
  switch (source) {
    case "service_health":
      return "Service health";
    case "worker_status":
      return "Worker status";
    case "logs":
      return "Log pattern";
    case "control_queue":
      return "Control queue";
    case "queues":
      return "Runtime queue";
    default:
      return "Signal";
  }
}

function actionForIncident(item: IncidentItem): { label: string; to: string; icon: typeof Activity } {
  switch (item.source) {
    case "worker_status":
      return { label: "Open workers", to: "/workers", icon: Wrench };
    case "logs":
      return { label: "Open system logs", to: "/system", icon: AlertTriangle };
    case "control_queue":
    case "queues":
      return { label: "Open control view", to: "/", icon: Activity };
    case "service_health":
    default:
      return { label: "Open system", to: "/system", icon: RadioTower };
  }
}

function whyItMatters(item: IncidentItem): string {
  const service = item.service ?? "service";
  const summary = String(item.summary ?? "").toLowerCase();

  if (item.source === "service_health" && summary.includes("metrics stale")) {
    return `${service} has not refreshed runtime metrics recently. Verify the channel only if it should be active right now.`;
  }
  if (item.source === "worker_status") {
    return "One or more workers failed in the current window; this can mean user-visible work stopped before producing a result.";
  }
  if (item.source === "logs") {
    return "Repeated warning or error logs were grouped so the same symptom is visible without flooding the page.";
  }
  if (item.source === "control_queue") {
    return "Control requests are waiting to be handled, so operator commands may lag behind the live runtime.";
  }
  if (item.source === "queues") {
    return "Queued work is building up faster than the runtime is draining it.";
  }
  return "This signal is active in the selected window and may need a quick operator check.";
}

function nextStepForIncident(item: IncidentItem): string {
  const summary = String(item.summary ?? "").toLowerCase();

  if (item.source === "service_health" && summary.includes("metrics stale")) {
    return "Confirm whether this channel should be producing fresh runtime metrics.";
  }
  if (item.source === "worker_status") {
    return "Inspect the failed worker records and decide whether to rerun or adjust the template.";
  }
  if (item.source === "logs") {
    return "Open the grouped log source and check whether the last event is still repeating.";
  }
  if (item.source === "control_queue") {
    return "Check pending control requests before sending more operator commands.";
  }
  if (item.source === "queues") {
    return "Check the live queue before starting more runtime work.";
  }
  return "Open the source view and verify whether the signal is still active.";
}

function formatWindow(minutes: number): string {
  if (minutes === 15) {
    return "15m";
  }
  if (minutes === 60) {
    return "1h";
  }
  if (minutes === 240) {
    return "4h";
  }
  if (minutes === 1440) {
    return "24h";
  }
  return `${minutes}m`;
}

function formatScope(value: string): string {
  return value === "all" ? "all services" : value.replaceAll("_", " ");
}

function IncidentSignalCard({ item }: { item: IncidentItem }) {
  const action = actionForIncident(item);
  const ActionIcon = action.icon;

  return (
    <article className="rounded-xl border border-white/6 bg-[var(--surface-panel)] p-5 shadow-[0_24px_80px_rgba(0,0,0,0.2)]">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline" className={`rounded-full ${severityTone(item.severity)}`}>
            {severityCopy(item.severity)}
          </Badge>
          <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
            {sourceLabel(item.source)}
          </Badge>
        </div>
        <span className="text-xs text-[var(--text-dim)]">Impact {item.impact ?? 0}</span>
      </div>

      <div className="mt-4">
        <h3 className="text-xl font-semibold text-white">{item.title ?? "Incident signal"}</h3>
        <p className="mt-2 text-sm leading-6 text-[var(--text-muted)]">{item.summary || "No summary available."}</p>
      </div>

      <div className="mt-5 grid gap-3 lg:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
        <div className="rounded-lg border border-white/6 bg-[var(--surface-panel-strong)] p-4">
          <p className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">Why it matters</p>
          <p className="mt-2 text-sm leading-6 text-[var(--text-strong)]">{whyItMatters(item)}</p>
        </div>
        <div className="rounded-lg border border-white/6 bg-[var(--surface-panel-strong)] p-4">
          <p className="text-[11px] uppercase tracking-[0.16em] text-[var(--text-dim)]">Evidence</p>
          <div className="mt-3 flex flex-wrap gap-2">
            <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
              {item.service ?? "unknown"}
            </Badge>
            <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
              {item.count ?? 0} event(s)
            </Badge>
          </div>
          <div className="mt-3 flex items-center gap-2 text-xs text-[var(--text-dim)]">
            <Clock3 className="size-3.5" />
            <span>{formatLocalDateTime(item.latest_at)}</span>
          </div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-white/6 pt-4">
        <p className="text-sm text-[var(--text-muted)]">{nextStepForIncident(item)}</p>
        <Button asChild variant="secondary" className="rounded-full bg-white/[0.08] text-[var(--text-strong)] hover:bg-white/[0.12]">
          <Link to={action.to}>
            <ActionIcon className="size-4" />
            {action.label}
            <ArrowRight className="size-4" />
          </Link>
        </Button>
      </div>
    </article>
  );
}

function EmptyState({ title, message, tone = "neutral" }: { title: string; message: string; tone?: "neutral" | "error" }) {
  const className =
    tone === "error"
      ? "rounded-[30px] border border-rose-400/30 bg-rose-950/20 p-8 text-rose-100"
      : "rounded-[30px] border border-white/6 bg-[var(--surface-panel)] p-8 text-[var(--text-strong)]";

  return (
    <section className={className}>
      <h2 className="text-2xl font-semibold text-white">{title}</h2>
      <p className="mt-2 text-sm text-[var(--text-muted)]">{message}</p>
    </section>
  );
}

export function IncidentsPage() {
  const { filters } = useOutletContext<AppShellOutletContext>();
  const [data, setData] = useState<IncidentsPayload | null>(null);
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");

    void fetchIncidents({
      windowMinutes: filters.windowMinutes,
      service: filters.service,
      environment: filters.environment,
      token: filters.token || undefined,
    })
      .then((payload) => {
        if (active) {
          setData(payload);
        }
      })
      .catch((err: unknown) => {
        if (!active) {
          return;
        }
        setError(err instanceof Error ? err.message : "Unknown request error");
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [filters.environment, filters.service, filters.token, filters.windowMinutes]);

  if (loading) {
    return <EmptyState title="Incidents" message="Loading incident stream..." />;
  }

  if (error) {
    return <EmptyState title="Incidents" message={`Failed to load incidents: ${error}`} tone="error" />;
  }

  const incidentsNode = (data?.incidents ?? {}) as {
    summary?: { open?: number; critical?: number; warning?: number };
    items?: IncidentItem[];
  };

  const summary = incidentsNode.summary ?? {};
  const items = incidentsNode.items ?? [];
  const openCount = summary.open ?? items.length;
  const criticalCount = summary.critical ?? items.filter((item) => item.severity === "critical").length;
  const warningCount = summary.warning ?? items.filter((item) => item.severity === "warning").length;

  return (
    <section className="grid gap-6">
      <Card className="rounded-xl border-white/6 bg-[var(--surface-panel)] py-0 shadow-[0_24px_80px_rgba(0,0,0,0.22)]">
        <CardContent className="flex flex-col gap-5 px-5 py-5 md:px-6 xl:flex-row xl:items-center xl:justify-between">
          <div>
            <p className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-dim)]">Signal triage</p>
            <h2 className="mt-2 text-2xl font-semibold text-white">Active pressure worth checking</h2>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-[var(--text-muted)]">
              Warnings and critical signals in the selected window, grouped by source and severity.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
              {openCount} active
            </Badge>
            <Badge variant="outline" className={`rounded-full ${criticalCount > 0 ? severityTone("critical") : "border-white/8 bg-white/[0.04] text-[var(--text-muted)]"}`}>
              {criticalCount} critical
            </Badge>
            <Badge variant="outline" className={`rounded-full ${warningCount > 0 ? severityTone("warning") : "border-white/8 bg-white/[0.04] text-[var(--text-muted)]"}`}>
              {warningCount} warning
            </Badge>
            <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
              {formatWindow(filters.windowMinutes)}
            </Badge>
            <Badge variant="outline" className="rounded-full border-white/8 bg-white/[0.04] text-[var(--text-muted)]">
              {formatScope(filters.service)}
            </Badge>
          </div>
        </CardContent>
      </Card>

      {items.length === 0 ? (
        <Card className="rounded-xl border-white/6 bg-[var(--surface-panel)] py-0">
          <CardContent className="p-6">
            <div className="rounded-lg border border-emerald-400/20 bg-emerald-500/5 p-5">
              <p className="text-lg font-semibold text-emerald-200">No active warning or critical signals</p>
              <p className="mt-2 text-sm leading-6 text-[var(--text-muted)]">
                Nothing in the selected window needs triage. Use System for raw logs when you need a deeper audit trail.
              </p>
            </div>
          </CardContent>
        </Card>
      ) : (
        <section className="grid gap-4">
          {items.map((item) => <IncidentSignalCard key={item.id ?? item.title} item={item} />)}
        </section>
      )}
    </section>
  );
}
