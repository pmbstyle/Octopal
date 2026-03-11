import { useEffect, useState } from "react";
import { useOutletContext } from "react-router-dom";

import { fetchWorkers } from "../api/dashboardClient";
import type { components } from "../api/types";
import type { AppShellOutletContext } from "../ui/AppShell";
import { formatLocalDateTime } from "../utils/dateTime";

type WorkersPayload = components["schemas"]["DashboardWorkersV2"];
type WorkerItem = {
  id?: string;
  template_name?: string;
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
};

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

function pillTone(status?: string): string {
  const v = String(status ?? "").toLowerCase();
  if (v === "completed" || v === "running" || v === "started") {
    return "border-emerald-400/30 bg-emerald-500/10 text-emerald-300";
  }
  if (v === "warning" || v === "stopped") {
    return "border-amber-300/30 bg-amber-500/10 text-amber-300";
  }
  return "border-rose-300/30 bg-rose-500/10 text-rose-300";
}

function short(value?: string): string {
  if (!value) {
    return "n/a";
  }
  return value.includes("-") ? value.split("-")[0] : value.slice(0, 8);
}

export function WorkersPage() {
  const { filters } = useOutletContext<AppShellOutletContext>();
  const [data, setData] = useState<WorkersPayload | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>("");
  const [expandedWorkerId, setExpandedWorkerId] = useState<string>("");

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");

    void fetchWorkers({
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
    return (
      <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-8 text-slate-300">
        <h2 className="text-2xl font-semibold text-slate-100">Workers</h2>
        <p className="mt-2">Loading workers...</p>
      </section>
    );
  }

  if (error) {
    return (
      <section className="rounded-2xl border border-rose-500/40 bg-rose-950/30 p-8 text-rose-200">
        <h2 className="text-2xl font-semibold text-rose-100">Workers</h2>
        <p className="mt-2">Failed to load workers: {error}</p>
      </section>
    );
  }

  const workersNode = (data?.workers ?? {}) as {
    running?: number;
    root_running?: number;
    subworkers_running?: number;
    completed?: number;
    failed?: number;
    recent?: WorkerItem[];
    topology?: WorkerItem[];
  };

  const recent = workersNode.recent ?? [];
  const topology = workersNode.topology ?? [];

  return (
    <section className="grid gap-5">
      <section className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5 shadow-xl shadow-slate-950/60">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">Worker pool</p>
            <h2 className="mt-2 text-2xl font-semibold text-slate-100">Workers</h2>
            <p className="mt-2 text-sm text-slate-400">
              Detailed recent results and active topology for the current filter window.
            </p>
          </div>
          <div className="grid grid-cols-2 gap-2 text-sm sm:grid-cols-4">
            <div className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-center">
              <div className="text-xs uppercase tracking-wide text-slate-500">Running</div>
              <div className="mt-1 text-xl font-semibold text-slate-100">{workersNode.running ?? 0}</div>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-center">
              <div className="text-xs uppercase tracking-wide text-slate-500">Root</div>
              <div className="mt-1 text-xl font-semibold text-slate-100">{workersNode.root_running ?? 0}</div>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-center">
              <div className="text-xs uppercase tracking-wide text-slate-500">Subworkers</div>
              <div className="mt-1 text-xl font-semibold text-slate-100">{workersNode.subworkers_running ?? 0}</div>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-2 text-center">
              <div className="text-xs uppercase tracking-wide text-slate-500">Failed</div>
              <div className="mt-1 text-xl font-semibold text-rose-300">{workersNode.failed ?? 0}</div>
            </div>
          </div>
        </div>
      </section>

      <article className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4 shadow-xl shadow-slate-950/60">
          <div className="mb-4 flex items-center justify-between">
            <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Recent Workers</h3>
            <p className="text-xs text-slate-500">Click a row to inspect the worker reply</p>
          </div>
          {recent.length === 0 ? (
            <p className="rounded-xl border border-slate-800 bg-slate-950/70 p-4 text-slate-400">No recent workers.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[980px] border-separate border-spacing-y-2 text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-slate-500">
                  <tr>
                    <th className="px-3 py-2">ID</th>
                    <th className="px-3 py-2">Status</th>
                    <th className="px-3 py-2">Template</th>
                    <th className="px-3 py-2">Task</th>
                    <th className="px-3 py-2">Result</th>
                  </tr>
                </thead>
                <tbody>
                  {recent.slice(0, 12).flatMap((worker, index) => {
                    const workerKey = worker.id ?? worker.updated_at ?? `worker-${index}`;
                    const workerId = worker.id ?? "";
                    const isExpanded = expandedWorkerId === worker.id;
                    const preview = worker.result_preview?.trim() || "No result yet";
                    return [
                      <tr
                        key={`${workerKey}-row`}
                        className="cursor-pointer rounded-xl bg-slate-950/70 align-top transition hover:bg-slate-900"
                        onClick={() => {
                          if (!workerId) {
                            return;
                          }
                          setExpandedWorkerId((current) => (current === workerId ? "" : workerId));
                        }}
                      >
                        <td className="rounded-l-xl px-3 py-3 font-mono text-xs text-cyan-300">{short(worker.id)}</td>
                        <td className="px-3 py-3">
                          <span
                            className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold uppercase tracking-wide ${pillTone(worker.status)}`}
                          >
                            {String(worker.status ?? "unknown")}
                          </span>
                        </td>
                        <td className="px-3 py-3 text-slate-200">{worker.template_name ?? "n/a"}</td>
                        <td className="max-w-[340px] px-3 py-3 text-slate-300" title={worker.task ?? ""}>
                          <div className="line-clamp-2">{String(worker.task ?? "") || "n/a"}</div>
                        </td>
                        <td title={preview} className="max-w-xs rounded-r-xl px-3 py-3">
                          <div className={`text-sm ${tone(worker.status)}`}>
                            {preview.length > 88 ? `${preview.slice(0, 88)}...` : preview}
                          </div>
                        </td>
                      </tr>,
                      isExpanded ? (
                        <tr key={`${workerKey}-details`}>
                          <td colSpan={5} className="px-3 pb-3">
                            <div className="space-y-3 rounded-2xl border border-slate-800 bg-slate-950/90 p-4">
                              <div className="flex flex-wrap gap-3 text-xs text-slate-400">
                                <span>Updated: {formatLocalDateTime(worker.updated_at)}</span>
                                <span>Lineage: {short(worker.lineage_id ?? undefined)}</span>
                                <span>
                                  Parent:{" "}
                                  {worker.parent_worker_id ? short(worker.parent_worker_id) : "root"}
                                </span>
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
                                  <pre className="overflow-x-auto whitespace-pre-wrap break-all rounded-lg border border-slate-800 bg-slate-900 p-3 text-xs text-slate-200">
                                    {JSON.stringify(worker.output, null, 2)}
                                  </pre>
                                </div>
                              ) : null}

                              {worker.tools_used && worker.tools_used.length > 0 ? (
                                <div className="space-y-1">
                                  <div className="text-xs uppercase tracking-[0.2em] text-slate-400">Tools</div>
                                  <div className="text-sm text-slate-300">{worker.tools_used.join(", ")}</div>
                                </div>
                              ) : null}
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
      </article>

      <article className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4 shadow-xl shadow-slate-950/60">
        <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Topology Snapshot</h3>
        {topology.length === 0 ? (
          <p className="mt-4 rounded-xl border border-slate-800 bg-slate-950/70 p-4 text-slate-400">
            No active topology nodes.
          </p>
        ) : (
          <ul className="mt-4 space-y-2">
            {topology.slice(0, 20).map((node) => (
              <li
                key={node.id ?? node.updated_at}
                className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-3 text-sm text-slate-300"
              >
                <span
                  className="block"
                  style={{ marginLeft: `${Math.min(64, (node.spawn_depth ?? 0) * 12)}px` }}
                >
                  <strong className="font-mono text-cyan-300">{short(node.id)}</strong>{" "}
                  <span className={tone(node.status)}>[{String(node.status ?? "unknown")}]</span>{" "}
                  {node.parent_worker_id ? `child of ${short(node.parent_worker_id)}` : "root"}
                </span>
              </li>
            ))}
          </ul>
        )}
      </article>
    </section>
  );
}
