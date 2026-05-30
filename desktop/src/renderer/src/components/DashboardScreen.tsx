import { AlertTriangle, Clock, Download, ExternalLink, Eye, FileJson, GitBranch, ListChecks, Pencil, Play, Plus, RotateCw, Square, Trash2, Wrench, X } from "lucide-react";
import { motion } from "framer-motion";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";

import octoIdleSprite from "../../../../assets/octo-idle-sprite.png";
import octoThinkingSprite from "../../../../assets/octo-thinking-sprite.png";
import octoImage from "../../../../assets/octo.png";
import type { CopyFn } from "../lib/appTypes";
import { Button } from "./Button";

type DashboardView = "control" | "workers" | "system";

type LoadPoint = {
  at: number;
  activeWorkers: number;
  queueDepth: number;
  octoQueue: number;
};

type LoadMetricKey = keyof Omit<LoadPoint, "at">;

type WorkerTemplateForm = {
  id: string;
  name: string;
  description: string;
  system_prompt: string;
  available_tools: string;
  required_permissions: string;
  model: string;
  max_thinking_steps: string;
  default_timeout_seconds: string;
  can_spawn_children: boolean;
  allowed_child_templates: string;
};

const emptyTemplateForm: WorkerTemplateForm = {
  id: "",
  name: "",
  description: "",
  system_prompt: "",
  available_tools: "",
  required_permissions: "",
  model: "",
  max_thinking_steps: "10",
  default_timeout_seconds: "300",
  can_spawn_children: false,
  allowed_child_templates: "",
};

function toTemplateForm(template?: DesktopWorkerTemplate | null): WorkerTemplateForm {
  if (!template) {
    return emptyTemplateForm;
  }
  return {
    id: template.id,
    name: template.name,
    description: template.description,
    system_prompt: template.system_prompt,
    available_tools: template.available_tools.join(", "),
    required_permissions: template.required_permissions.join(", "),
    model: template.model ?? "",
    max_thinking_steps: String(template.max_thinking_steps ?? 10),
    default_timeout_seconds: String(template.default_timeout_seconds ?? 300),
    can_spawn_children: Boolean(template.can_spawn_children),
    allowed_child_templates: template.allowed_child_templates.join(", "),
  };
}

function parseList(value: string): string[] {
  return value
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function templatePayload(form: WorkerTemplateForm): DesktopWorkerTemplate {
  return {
    id: form.id.trim(),
    name: form.name.trim(),
    description: form.description.trim(),
    system_prompt: form.system_prompt.trim(),
    available_tools: parseList(form.available_tools),
    required_permissions: parseList(form.required_permissions),
    model: form.model.trim() || null,
    max_thinking_steps: Number(form.max_thinking_steps || 10),
    default_timeout_seconds: Number(form.default_timeout_seconds || 300),
    can_spawn_children: form.can_spawn_children,
    allowed_child_templates: parseList(form.allowed_child_templates),
  };
}

function shortId(value?: string | null): string {
  if (!value) {
    return "-";
  }
  return value.includes("-") ? value.split("-")[0] : value.slice(0, 8);
}

function statusClass(status?: string): string {
  const value = String(status ?? "").toLowerCase();
  if (["running", "started", "thinking"].includes(value)) {
    return "dashboard-status dashboard-status-live";
  }
  if (["completed", "ok", "connected"].includes(value)) {
    return "dashboard-status dashboard-status-good";
  }
  if (["warning", "stopped", "awaiting_instruction", "waiting_for_children"].includes(value)) {
    return "dashboard-status dashboard-status-warn";
  }
  if (["error", "failed", "critical"].includes(value)) {
    return "dashboard-status dashboard-status-bad";
  }
  return "dashboard-status";
}

function statusTextClass(status?: string): string {
  const value = String(status ?? "").toLowerCase();
  if (["running", "started", "thinking"].includes(value)) {
    return "worker-detail-status worker-detail-status-live";
  }
  if (["completed", "ok", "connected"].includes(value)) {
    return "worker-detail-status worker-detail-status-good";
  }
  if (["warning", "stopped", "awaiting_instruction", "waiting_for_children"].includes(value)) {
    return "worker-detail-status worker-detail-status-warn";
  }
  if (["error", "failed", "critical"].includes(value)) {
    return "worker-detail-status worker-detail-status-bad";
  }
  return "worker-detail-status";
}

function isIdleOctoState(status?: string): boolean {
  return String(status ?? "").toLowerCase() === "idle";
}

function isThinkingOctoState(status?: string): boolean {
  return String(status ?? "").toLowerCase() === "thinking";
}

function animatedOctoForState(status?: string): { className: string; sprite: string } | null {
  if (isIdleOctoState(status)) {
    return { className: "dashboard-octo-idle", sprite: octoIdleSprite };
  }
  if (isThinkingOctoState(status)) {
    return { className: "dashboard-octo-thinking", sprite: octoThinkingSprite };
  }
  return null;
}

function formatTime(value?: string | number): string {
  if (!value) {
    return "-";
  }
  const date = typeof value === "number" ? new Date(value) : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" }).format(date);
}

function formatDateTime(value?: string | number): string {
  if (!value) {
    return "-";
  }
  const date = typeof value === "number" ? new Date(value) : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatDuration(start?: string, end?: string): string {
  if (!start || !end) {
    return "-";
  }
  const startDate = new Date(start);
  const endDate = new Date(end);
  const ms = endDate.getTime() - startDate.getTime();
  if (!Number.isFinite(ms) || ms < 0) {
    return "-";
  }
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  if (minutes < 60) {
    return rest > 0 ? `${minutes}m ${rest}s` : `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function formatEventName(value?: string): string {
  const labels: Record<string, string> = {
    worker_spawned: "Worker spawned",
    worker_started: "Process started",
    worker_recovery_attempt: "Recovery attempt",
    worker_waiting_for_children: "Waiting for children",
    worker_resumed_after_children: "Children finished",
    worker_resumed_for_child_instruction: "Child needs instruction",
    worker_awaiting_instruction: "Awaiting instruction",
    worker_instruction_answered: "Instruction answered",
    worker_instruction_timeout: "Instruction timed out",
    worker_result_repaired: "Result repaired",
    worker_result: "Result returned",
    worker_failed: "Worker failed",
    worker_stopped: "Worker stopped",
    intent_approval_requested: "Approval requested",
    intent_approval_granted: "Approval granted",
    intent_approval_denied: "Approval denied",
    intent_executed_reported: "Intent executed",
  };
  if (!value) {
    return "Worker event";
  }
  return labels[value] ?? value.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function countItems(values?: string[]): Array<{ name: string; count: number }> {
  const counts = new Map<string, number>();
  for (const value of values ?? []) {
    const key = String(value || "").trim();
    if (!key) {
      continue;
    }
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return [...counts.entries()].map(([name, count]) => ({ name, count }));
}

function jsonPreview(value: unknown): string {
  if (!value) {
    return "";
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function stripHtmlMarkup(value: string): string {
  return value
    .replace(/<\s*br\s*\/?\s*>/gi, "\n")
    .replace(/<\s*\/\s*(p|div|h[1-6]|center|li)\s*>/gi, "\n")
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'");
}

function limitDisplayText(value: string, maxLength: number): string {
  const normalized = stripHtmlMarkup(value).replace(/\s+/g, " ").trim();
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength - 3).trim()}...` : normalized;
}

function isTransientDashboardError(value: string): boolean {
  return /fetch failed|ECONNREFUSED|ECONNRESET|ENOTFOUND|ETIMEDOUT/i.test(value);
}

function loadGraphMax(points: LoadPoint[]): number {
  return Math.max(
    1,
    ...points.flatMap((point) => [point.activeWorkers, point.queueDepth, point.octoQueue].map((value) => Math.max(0, value))),
  );
}

function linePoints(points: LoadPoint[], key: LoadMetricKey, max: number): string {
  const values = points.length > 0 ? points.map((point) => Math.max(0, point[key])) : [0];
  const width = 1000;
  const height = 300;
  const plotTop = 28;
  const plotBottom = height - 28;
  const plotHeight = plotBottom - plotTop;
  const valueToY = (value: number) => {
    const y = plotBottom - (Number(value) / Math.max(1, max)) * plotHeight;
    return Math.max(plotTop, Math.min(plotBottom, y));
  };
  if (values.length === 1) {
    const y = valueToY(values[0]);
    return `0,${y} ${width},${y}`;
  }
  const step = width / (values.length - 1);
  return values
    .map((value, index) => {
      const x = index * step;
      return `${x},${valueToY(value)}`;
    })
    .join(" ");
}

function Field({
  label,
  children,
  tall,
}: {
  label: string;
  children: React.ReactNode;
  tall?: boolean;
}) {
  return (
    <label className={tall ? "template-field template-field-tall" : "template-field"}>
      <span>{label}</span>
      {children}
    </label>
  );
}

export function DashboardScreen({
  copy,
  installDir,
  runtimeView,
  updateAvailable,
  updateBlocked,
  updateBusy,
  desktopUpdateAvailable,
  desktopUpdateReady,
  desktopUpdateBusy,
  onStart,
  onStop,
  onRestart,
  onUpdateOctopal,
  onUpdateDesktopApp,
}: {
  copy: CopyFn;
  installDir: string;
  runtimeView: { state: string; title: string; detail: string };
  updateAvailable: boolean;
  updateBlocked: boolean;
  updateBusy: boolean;
  desktopUpdateAvailable: boolean;
  desktopUpdateReady: boolean;
  desktopUpdateBusy: boolean;
  onStart: () => void;
  onStop: () => void;
  onRestart: () => void;
  onUpdateOctopal: () => void;
  onUpdateDesktopApp: () => void;
}) {
  const [view, setView] = useState<DashboardView>("control");
  const [snapshot, setSnapshot] = useState<DesktopDashboardSnapshot | null>(null);
  const [history, setHistory] = useState<LoadPoint[]>([]);
  const [dashboardError, setDashboardError] = useState("");
  const [templates, setTemplates] = useState<DesktopWorkerTemplate[]>([]);
  const [templateError, setTemplateError] = useState("");
  const [templateNotice, setTemplateNotice] = useState("");
  const [editingTemplateId, setEditingTemplateId] = useState<string | null>(null);
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(null);
  const [templateForm, setTemplateForm] = useState<WorkerTemplateForm>(emptyTemplateForm);
  const [templateSaving, setTemplateSaving] = useState(false);
  const [startedAt] = useState(() => Date.now());

  const refreshSnapshot = useCallback(async () => {
    if (!window.octopalDesktop || !installDir) {
      return;
    }
    const next = await window.octopalDesktop.getDashboardSnapshot(installDir);
    setSnapshot(next);
    if (!next.ok) {
      if (isTransientDashboardError(next.detail) && Date.now() - startedAt < 45_000) {
        setDashboardError("");
        return;
      }
      setDashboardError(next.detail);
      return;
    }
    setDashboardError("");
    if (next.load) {
      setHistory((current) =>
        [
          ...current,
          {
            at: Date.now(),
            activeWorkers: next.load?.activeWorkers ?? 0,
            queueDepth: next.load?.queueDepth ?? 0,
            octoQueue: next.load?.octoQueue ?? 0,
          },
        ].slice(-32),
      );
    }
  }, [installDir, startedAt]);

  const refreshTemplates = useCallback(async () => {
    if (!window.octopalDesktop || !installDir) {
      return;
    }
    try {
      const next = await window.octopalDesktop.getWorkerTemplates(installDir);
      setTemplates([...next].sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id)));
      setTemplateError("");
    } catch (error) {
      setTemplateError(error instanceof Error ? error.message : copy("failedToLoadDashboard"));
    }
  }, [copy, installDir]);

  useEffect(() => {
    void refreshSnapshot();
    const timer = window.setInterval(() => {
      void refreshSnapshot();
    }, 4000);
    return () => window.clearInterval(timer);
  }, [refreshSnapshot]);

  useEffect(() => {
    void refreshTemplates();
  }, [refreshTemplates]);

  const graphPoints = useMemo(() => {
    if (history.length > 0) {
      return history;
    }
    const load = snapshot?.load ?? { activeWorkers: 0, queueDepth: 0, octoQueue: 0 };
    return [
      {
        at: Date.now(),
        activeWorkers: Math.max(0, load.activeWorkers),
        queueDepth: Math.max(0, load.queueDepth),
        octoQueue: Math.max(0, load.octoQueue),
      },
    ];
  }, [history, snapshot?.load]);
  const graphMax = loadGraphMax(graphPoints);
  const currentLoad = graphPoints.at(-1) ?? { at: Date.now(), activeWorkers: 0, queueDepth: 0, octoQueue: 0 };
  const loadMetrics: Array<{ key: LoadMetricKey; label: string; value: number; className: string }> = [
    {
      key: "activeWorkers",
      label: copy("activeWorkers"),
      value: currentLoad.activeWorkers,
      className: "dashboard-load-swatch-active",
    },
    {
      key: "queueDepth",
      label: copy("workerQueue"),
      value: currentLoad.queueDepth,
      className: "dashboard-load-swatch-queue",
    },
    {
      key: "octoQueue",
      label: copy("octoQueue"),
      value: currentLoad.octoQueue,
      className: "dashboard-load-swatch-octo",
    },
  ];

  const recentWorkers = snapshot?.workers?.recent ?? [];
  const selectedWorker = selectedWorkerId
    ? recentWorkers.find((worker) => worker.id === selectedWorkerId) ?? null
    : null;
  const attention = snapshot?.attention;
  const octoState = snapshot?.octo?.state || runtimeView.state || "idle";
  const displayOctoState = attention ? "error" : octoState;
  const octoHeadlineRaw = snapshot?.octo?.headline || runtimeView.title;
  const octoDetailRaw = snapshot?.octo?.detail || runtimeView.detail || copy("octopalStarted");
  const latestActionRaw = snapshot?.octo?.latestAction || copy("octoLatestFallback");
  const octoHeadline = limitDisplayText(octoHeadlineRaw, 110);
  const octoDetail = limitDisplayText(octoDetailRaw, 260);
  const latestAction = limitDisplayText(latestActionRaw, 130);
  const octoNeedsAttention = ["error", "failed", "critical"].includes(String(displayOctoState).toLowerCase());
  const attentionTitle = attention?.title || octoHeadlineRaw || copy("runtimeStatusError");
  const attentionTitleText = limitDisplayText(attentionTitle, 160);
  const attentionDetail = limitDisplayText(attention?.detail || dashboardError || octoDetailRaw, 520);
  const attentionMeta = [attention?.service, attention?.level, attention?.timestamp ? formatDateTime(attention.timestamp) : ""]
    .filter(Boolean)
    .join(" · ");
  const systemTitle = attention ? attentionTitleText : runtimeView.title;
  const systemDetail = attention ? attentionDetail : runtimeView.detail || copy("systemBody");
  const services = snapshot?.system?.services ?? [];
  const connectedMcpServers = (snapshot?.system?.mcpServers ?? []).filter(
    (server) => String(server.status).toLowerCase() === "connected",
  );
  const logs = snapshot?.system?.logs ?? [];
  const editingTemplate = editingTemplateId
    ? templates.find((template) => template.id === editingTemplateId) ?? null
    : null;
  const isCreatingTemplate = editingTemplateId === "";
  const selectedWorkerTemplate = selectedWorker?.template_id
    ? templates.find((template) => template.id === selectedWorker.template_id) ?? null
    : null;
  const animatedOcto = animatedOctoForState(displayOctoState);

  function startCreateTemplate(): void {
    setEditingTemplateId("");
    setTemplateForm(emptyTemplateForm);
    setTemplateNotice("");
    setTemplateError("");
  }

  function startEditTemplate(template: DesktopWorkerTemplate): void {
    setEditingTemplateId(template.id);
    setTemplateForm(toTemplateForm(template));
    setTemplateNotice("");
    setTemplateError("");
  }

  async function saveTemplate(): Promise<void> {
    if (!window.octopalDesktop || !installDir) {
      return;
    }
    setTemplateSaving(true);
    setTemplateNotice("");
    setTemplateError("");
    try {
      const saved = await window.octopalDesktop.saveWorkerTemplate(
        installDir,
        templatePayload(templateForm),
        isCreatingTemplate ? "create" : "update",
      );
      setTemplates((current) => {
        const next = isCreatingTemplate
          ? [...current.filter((item) => item.id !== saved.id), saved]
          : current.map((item) => (item.id === saved.id ? saved : item));
        return next.sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
      });
      setEditingTemplateId(null);
      setTemplateNotice(copy(isCreatingTemplate ? "templateCreated" : "templateSaved"));
    } catch (error) {
      setTemplateError(error instanceof Error ? error.message : copy("templateSaveFailed"));
    } finally {
      setTemplateSaving(false);
    }
  }

  async function deleteTemplate(): Promise<void> {
    if (!window.octopalDesktop || !installDir || !editingTemplate) {
      return;
    }
    setTemplateSaving(true);
    setTemplateNotice("");
    setTemplateError("");
    try {
      await window.octopalDesktop.deleteWorkerTemplate(installDir, editingTemplate.id);
      setTemplates((current) => current.filter((item) => item.id !== editingTemplate.id));
      setEditingTemplateId(null);
      setTemplateNotice(copy("templateDeleted"));
    } catch (error) {
      setTemplateError(error instanceof Error ? error.message : copy("templateDeleteFailed"));
    } finally {
      setTemplateSaving(false);
    }
  }

  function openSelectedWorkerTemplate(): void {
    if (!selectedWorkerTemplate) {
      return;
    }
    setSelectedWorkerId(null);
    setView("workers");
    startEditTemplate(selectedWorkerTemplate);
  }

  function openLogs(): void {
    if (!window.octopalDesktop || !installDir) {
      return;
    }
    void window.octopalDesktop.openOctopalLogs(installDir);
  }

  function renderDashboardHeader({
    title,
    titleRaw = title,
    detail,
    detailRaw = detail,
    latest,
    latestRaw = latest,
    actions,
  }: {
    title: string;
    titleRaw?: string;
    detail: string;
    detailRaw?: string;
    latest?: string;
    latestRaw?: string;
    actions?: ReactNode;
  }) {
    return (
      <div className="dashboard-assistant-head">
        <div className="dashboard-octo-stack">
          {animatedOcto ? (
            <span
              className={`octo dashboard-octo ${animatedOcto.className}`}
              role="img"
              aria-label="Octopal mascot"
              style={{ backgroundImage: `url(${animatedOcto.sprite})` }}
            />
          ) : (
            <img className="octo dashboard-octo" src={octoImage} alt="Octopal mascot" />
          )}
          <span className={statusClass(displayOctoState)}>{displayOctoState}</span>
        </div>
        <div className="dashboard-bubble">
          <h1 title={titleRaw}>{title}</h1>
          <p className="dashboard-octo-detail" title={detailRaw}>{detail}</p>
          {latest ? (
            <p className="dashboard-latest" title={latestRaw}>
              <strong>{copy("latestAction")}:</strong> {latest}
            </p>
          ) : null}
        </div>
        {actions ? <div className="dashboard-actions">{actions}</div> : null}
      </div>
    );
  }

  function renderControl() {
    return (
      <section className="dashboard-control">
        {renderDashboardHeader({
          title: octoHeadline,
          titleRaw: octoHeadlineRaw,
          detail: octoDetail,
          detailRaw: octoDetailRaw,
          latest: latestAction,
          latestRaw: latestActionRaw,
          actions: updateAvailable || desktopUpdateAvailable ? (
            <Button type="button" variant="ghost" onClick={() => setView("system")}>
              {copy("updateReady")}
            </Button>
          ) : null,
        })}

        {attention || octoNeedsAttention || dashboardError ? (
          <div className="dashboard-attention-panel" role="alert">
            <AlertTriangle />
            <div>
              <span className="dashboard-attention-kicker">{copy("runtimeStatusError")}</span>
              <h2 title={attentionTitle}>{attentionTitleText}</h2>
              <p title={attentionDetail}>{attentionDetail}</p>
              {attentionMeta ? <small>{attentionMeta}</small> : null}
            </div>
            <Button type="button" variant="secondary" onClick={openLogs}>
              <FileJson data-icon="inline-start" />
              {copy("openLogs")}
            </Button>
          </div>
        ) : null}

        <div className="dashboard-panel">
          <div className="dashboard-panel-head">
            <div>
              <h2>{copy("liveLoad")}</h2>
              <p>{copy("liveLoadBody")}</p>
            </div>
            <div className="dashboard-chart-pills">
              <span className="dashboard-pill">{copy("lastSamples")}</span>
            </div>
          </div>
          <div className="dashboard-load-summary">
            {loadMetrics.map((metric) => (
              <div className="dashboard-load-chip" key={metric.key}>
                <span className={`dashboard-load-swatch ${metric.className}`} />
                <span>{metric.label}</span>
                <strong>{metric.value}</strong>
              </div>
            ))}
          </div>
          <div
            className="dashboard-chart"
            aria-label={`${copy("liveLoad")}: ${loadMetrics.map((metric) => `${metric.label} ${metric.value}`).join(", ")}`}
          >
            <span className="dashboard-chart-y dashboard-chart-y-max">{graphMax}</span>
            <span className="dashboard-chart-y dashboard-chart-y-zero">0</span>
            {history.length <= 1 ? <span className="dashboard-chart-samples">{copy("collectingSamples")}</span> : null}
            <svg viewBox="0 0 1000 300" preserveAspectRatio="none">
              <polyline points={linePoints(graphPoints, "activeWorkers", graphMax)} fill="none" stroke="var(--accent)" strokeWidth="7" />
              <polyline points={linePoints(graphPoints, "queueDepth", graphMax)} fill="none" stroke="#f4b84f" strokeWidth="5" />
              <polyline points={linePoints(graphPoints, "octoQueue", graphMax)} fill="none" stroke="var(--success)" strokeWidth="4" />
            </svg>
          </div>
          {dashboardError ? <p className="dashboard-inline-error">{dashboardError}</p> : null}
        </div>

        <div className="dashboard-panel">
          <div className="dashboard-panel-head">
            <div>
              <h2>{copy("workerRuns")}</h2>
              <p>{copy("workerRunsBody")}</p>
            </div>
            <Button type="button" variant="ghost" onClick={() => setView("workers")}>
              {copy("openWorkerStudio")}
            </Button>
          </div>
          <div className="dashboard-worker-table">
            <div className="dashboard-worker-row dashboard-worker-row-head">
              <span>ID</span>
              <span>{copy("status")}</span>
              <span>{copy("template")}</span>
              <span>{copy("task")}</span>
              <span>{copy("updated")}</span>
              <span>Details</span>
            </div>
            {recentWorkers.length === 0 ? (
              <div className="dashboard-empty-row">{copy("noRecentWorkers")}</div>
            ) : (
              recentWorkers.slice(0, 8).map((worker, index) => (
                <button
                  type="button"
                  className="dashboard-worker-row dashboard-worker-row-button"
                  key={worker.id ?? `${worker.updated_at}-${index}`}
                  onClick={() => setSelectedWorkerId(worker.id ?? null)}
                >
                  <strong>{shortId(worker.id)}</strong>
                  <span className={statusClass(worker.status)}>{worker.status ?? "unknown"}</span>
                  <span>{worker.template_name ?? worker.template_id ?? "-"}</span>
                  <span className="dashboard-worker-task" title={worker.task ?? worker.result_preview ?? worker.summary ?? worker.error ?? ""}>
                    {worker.task ?? worker.result_preview ?? worker.summary ?? worker.error ?? "-"}
                  </span>
                  <span>{formatTime(worker.updated_at)}</span>
                  <span className="worker-row-open">
                    <Eye />
                    Open
                  </span>
                </button>
              ))
            )}
          </div>
        </div>
      </section>
    );
  }

  function renderWorkers() {
    return (
      <section className="dashboard-workers-view">
        {renderDashboardHeader({
          title: copy("workerTemplates"),
          detail: copy("workerTemplatesBody"),
          latest: latestAction,
          latestRaw: latestActionRaw,
        })}
        {templateError ? <p className="dashboard-inline-error">{templateError}</p> : null}
        {templateNotice ? <p className="dashboard-inline-notice">{templateNotice}</p> : null}
        <div className="worker-studio-grid">
          <div className="dashboard-panel worker-template-list-panel">
            <div className="dashboard-panel-head">
              <div>
                <h2>{copy("templates")}</h2>
                <p>workspace/workers</p>
              </div>
              <Button type="button" variant="ghost" onClick={startCreateTemplate}>
                <Plus data-icon="inline-start" />
                {copy("newTemplate")}
              </Button>
            </div>
            <div className="worker-template-list">
              {templates.length === 0 ? (
                <p className="dashboard-empty-row">{copy("noWorkerTemplates")}</p>
              ) : (
                templates.map((template) => (
                  <button
                    type="button"
                    className="worker-template-card"
                    key={template.id}
                    onClick={() => startEditTemplate(template)}
                  >
                    <span>
                      <strong>{template.name}</strong>
                      <small>{template.id}</small>
                    </span>
                    <p>{template.description}</p>
                    <Pencil />
                  </button>
                ))
              )}
            </div>
          </div>
        </div>
      </section>
    );
  }

  function renderSystem() {
    return (
      <section className="dashboard-system-view">
        {renderDashboardHeader({
          title: systemTitle,
          detail: systemDetail,
          latest: latestAction,
          latestRaw: latestActionRaw,
        })}
        {attention || dashboardError ? (
          <div className="dashboard-attention-panel" role="alert">
            <AlertTriangle />
            <div>
              <span className="dashboard-attention-kicker">{copy("runtimeStatusError")}</span>
              <h2 title={attentionTitle}>{attentionTitleText}</h2>
              <p title={attentionDetail}>{attentionDetail}</p>
              {attentionMeta ? <small>{attentionMeta}</small> : null}
            </div>
            <Button type="button" variant="secondary" onClick={openLogs}>
              <FileJson data-icon="inline-start" />
              {copy("openLogs")}
            </Button>
          </div>
        ) : null}
        <div className="system-grid">
          <div className="dashboard-panel system-card system-card-half">
            <h2>{copy("runtime")}</h2>
            <p>{copy("runtimeBody")}</p>
            <div className="system-actions">
              {runtimeView.state === "running" ? (
                <>
                  <Button type="button" variant="danger" onClick={onStop}>
                    <Square data-icon="inline-start" />
                    {copy("stopOctopal")}
                  </Button>
                  <Button type="button" variant="secondary" onClick={onRestart}>
                    <RotateCw data-icon="inline-start" />
                    {copy("restartOctopal")}
                  </Button>
                </>
              ) : (
                <Button type="button" variant="success" onClick={onStart}>
                  <Play data-icon="inline-start" />
                  {copy("startOctopal")}
                </Button>
              )}
              {snapshot?.dashboardEnabled && snapshot?.baseUrl ? (
                <Button type="button" variant="ghost" onClick={() => window.open(snapshot.baseUrl, "_blank")}>
                  <ExternalLink data-icon="inline-start" />
                  {copy("openDashboardUrl")}
                </Button>
              ) : null}
            </div>
          </div>

          <div className="dashboard-panel system-card system-card-half">
            <h2>{copy("updates")}</h2>
            <p>{copy("updatesBody")}</p>
            <div className="system-actions">
              <Button type="button" variant="primary" disabled={updateBusy || updateBlocked} onClick={onUpdateOctopal}>
                <Download data-icon="inline-start" />
                {updateBusy ? copy("updatingOctopal") : copy("checkRuntimeUpdate")}
              </Button>
              <Button type="button" variant="secondary" disabled={desktopUpdateBusy} onClick={onUpdateDesktopApp}>
                <Download data-icon="inline-start" />
                {desktopUpdateReady ? copy("installDesktopUpdate") : copy("checkDesktopUpdate")}
              </Button>
            </div>
          </div>

          <div className="dashboard-panel system-card">
            <h2>{copy("services")}</h2>
            <div className="service-pills">
              {services.length === 0 ? (
                <span className="dashboard-pill">{copy("noDashboardData")}</span>
              ) : (
                services.map((service) => (
                  <span className={statusClass(service.status)} title={service.reason} key={service.id}>
                    {service.name} {service.status}
                  </span>
                ))
              )}
            </div>
          </div>

          <div className="dashboard-panel system-card">
            <h2>{copy("connectedMcpServers")}</h2>
            {connectedMcpServers.length === 0 ? (
              <p>{copy("noConnectedMcpServers")}</p>
            ) : (
              <div className="mcp-server-list">
                {connectedMcpServers.map((server) => (
                  <article className="mcp-server-card" key={server.id} title={server.reason || server.id}>
                    <div>
                      <strong>{server.name}</strong>
                      <span>{server.id}</span>
                    </div>
                    <dl>
                      <div>
                        <dt>{copy("availableTools")}</dt>
                        <dd>{server.toolCount}</dd>
                      </div>
                      <div>
                        <dt>{copy("transport")}</dt>
                        <dd>{server.transport}</dd>
                      </div>
                    </dl>
                  </article>
                ))}
              </div>
            )}
          </div>

          <div className="dashboard-panel system-card">
            <h2>{copy("recentLogs")}</h2>
            {logs.length === 0 ? (
              <p>{copy("noLogs")}</p>
            ) : (
              <div className="log-list">
                {logs.slice(0, 8).map((log, index) => (
                  <p key={`${log.timestamp ?? ""}-${log.event ?? index}`}>
                    <span>{formatTime(log.timestamp)}</span> {log.service ?? "runtime"} · {log.event ?? ""}
                  </p>
                ))}
              </div>
            )}
          </div>
        </div>
      </section>
    );
  }

  function renderWorkerDetailModal() {
    if (!selectedWorker) {
      return null;
    }
    const timeline = selectedWorker.audit_timeline?.length
      ? selectedWorker.audit_timeline
      : [
          {
            id: `${selectedWorker.id}-created`,
            ts: selectedWorker.created_at,
            level: "info",
            event_type: "worker_spawned",
            data_preview: selectedWorker.task ?? "",
          },
          {
            id: `${selectedWorker.id}-updated`,
            ts: selectedWorker.updated_at,
            level: selectedWorker.error ? "error" : "info",
            event_type: selectedWorker.error ? "worker_failed" : "worker_result",
            data_preview: selectedWorker.result_preview ?? selectedWorker.summary ?? selectedWorker.error ?? "",
          },
        ].filter((event) => event.ts || event.data_preview);
    const outputText = jsonPreview(selectedWorker.output);
    const usedTools = countItems(selectedWorker.tools_used);
    const allowedTools = selectedWorker.template_config?.available_tools ?? [];
    const preview = selectedWorker.result_preview || selectedWorker.summary || selectedWorker.error || "No result yet.";

    return (
      <div className="worker-detail-backdrop" role="presentation">
        <section className="worker-detail-modal" role="dialog" aria-modal="true" aria-label="Worker details">
          <header className="worker-detail-header">
            <div>
              <p className="worker-detail-kicker">Worker run</p>
              <h2>{selectedWorker.template_name ?? selectedWorker.template_id ?? shortId(selectedWorker.id)}</h2>
            </div>
            <div className="worker-detail-header-actions">
              {selectedWorkerTemplate ? (
                <Button type="button" variant="ghost" onClick={openSelectedWorkerTemplate}>
                  <Pencil data-icon="inline-start" />
                  Edit template
                </Button>
              ) : null}
              <button type="button" className="template-icon-button" onClick={() => setSelectedWorkerId(null)}>
                <X />
              </button>
            </div>
          </header>

          <div className="worker-detail-summary">
            <div>
              <Clock />
              <span>Started</span>
              <strong>{formatDateTime(selectedWorker.created_at)}</strong>
            </div>
            <div>
              <Clock />
              <span>Updated</span>
              <strong>{formatDateTime(selectedWorker.updated_at)}</strong>
            </div>
            <div>
              <Clock />
              <span>Duration</span>
              <strong>{formatDuration(selectedWorker.created_at, selectedWorker.updated_at)}</strong>
            </div>
            <div>
              <GitBranch />
              <span>Lineage</span>
              <strong>{shortId(selectedWorker.lineage_id) || "-"}</strong>
            </div>
            <div>
              <Wrench />
              <span>Tools</span>
              <strong>{selectedWorker.tools_used?.length ?? 0}</strong>
            </div>
            <div>
              <ListChecks />
              <span>Status</span>
              <strong className={statusTextClass(selectedWorker.status)}>{selectedWorker.status ?? "unknown"}</strong>
            </div>
          </div>

          <div className="worker-detail-body">
            <section className="worker-detail-main">
              <div className="worker-detail-section worker-detail-section-result">
                <div className="worker-detail-section-head">
                  <h3>Result</h3>
                  <span>{selectedWorker.error ? "Needs attention" : selectedWorker.summary ? "Completed output" : "Waiting for output"}</span>
                </div>
                {selectedWorker.summary ? <p className="worker-detail-result">{selectedWorker.summary}</p> : null}
                {selectedWorker.error ? <p className="worker-detail-error">{selectedWorker.error}</p> : null}
                {!selectedWorker.summary && !selectedWorker.error ? <p className="worker-detail-muted">{preview}</p> : null}
              </div>

              <div className="worker-detail-section">
                <div className="worker-detail-section-head">
                  <h3>Action timeline</h3>
                  <span>{timeline.length} event{timeline.length === 1 ? "" : "s"}</span>
                </div>
                <ol className="worker-timeline">
                  {timeline.map((event, index) => (
                    <li key={event.id ?? `${event.ts}-${index}`} className={`worker-timeline-item worker-timeline-${event.level ?? "info"}`}>
                      <time>{formatTime(event.ts)}</time>
                      <div>
                        <strong>{formatEventName(event.event_type)}</strong>
                        {event.data_preview ? <p>{event.data_preview}</p> : null}
                      </div>
                    </li>
                  ))}
                </ol>
              </div>

              {outputText ? (
                <div className="worker-detail-section">
                  <div className="worker-detail-section-head">
                    <h3>Structured output</h3>
                    <span>JSON</span>
                  </div>
                  <pre className="worker-output-json">{outputText}</pre>
                </div>
              ) : null}
            </section>

            <aside className="worker-detail-side">
              <div className="worker-detail-section">
                <div className="worker-detail-section-head">
                  <h3>Run context</h3>
                  <span>{shortId(selectedWorker.id)}</span>
                </div>
                <dl className="worker-detail-facts">
                  <div>
                    <dt>ID</dt>
                    <dd>{selectedWorker.id ?? "-"}</dd>
                  </div>
                  <div>
                    <dt>Parent</dt>
                    <dd>{selectedWorker.parent_worker_id ? shortId(selectedWorker.parent_worker_id) : "root"}</dd>
                  </div>
                  <div>
                    <dt>Depth</dt>
                    <dd>{selectedWorker.spawn_depth ?? 0}</dd>
                  </div>
                  <div>
                    <dt>Template</dt>
                    <dd>{selectedWorker.template_id ?? selectedWorker.template_name ?? "-"}</dd>
                  </div>
                </dl>
              </div>

              <div className="worker-detail-section">
                <div className="worker-detail-section-head">
                  <h3>Tools used</h3>
                  <span>{usedTools.length ? `${usedTools.length} kind${usedTools.length === 1 ? "" : "s"}` : "None"}</span>
                </div>
                {usedTools.length > 0 ? (
                  <div className="worker-tool-cloud">
                    {usedTools.map((tool) => (
                      <span key={tool.name}>
                        {tool.name}
                        {tool.count > 1 ? ` x${tool.count}` : ""}
                      </span>
                    ))}
                  </div>
                ) : (
                  <p className="worker-detail-muted">No tool usage was reported for this run.</p>
                )}
              </div>

              <div className="worker-detail-section">
                <div className="worker-detail-section-head">
                  <h3>Template settings</h3>
                  {selectedWorkerTemplate ? (
                    <button type="button" className="worker-detail-link" onClick={openSelectedWorkerTemplate}>
                      Open editor
                    </button>
                  ) : (
                    <span>Snapshot</span>
                  )}
                </div>
                {selectedWorker.template_config ? (
                  <>
                    <dl className="worker-detail-facts">
                      <div>
                        <dt>Model</dt>
                        <dd>{selectedWorker.template_config.model || "default"}</dd>
                      </div>
                      <div>
                        <dt>Thinking steps</dt>
                        <dd>{selectedWorker.template_config.max_thinking_steps ?? "n/a"}</dd>
                      </div>
                      <div>
                        <dt>Timeout</dt>
                        <dd>{selectedWorker.template_config.default_timeout_seconds ?? "n/a"}s</dd>
                      </div>
                      <div>
                        <dt>Children</dt>
                        <dd>{selectedWorker.template_config.can_spawn_children ? "allowed" : "off"}</dd>
                      </div>
                    </dl>
                    {allowedTools.length > 0 ? (
                      <div className="worker-tool-cloud worker-tool-cloud-muted">
                        {allowedTools.map((toolName) => (
                          <span key={toolName}>{toolName}</span>
                        ))}
                      </div>
                    ) : null}
                  </>
                ) : (
                  <p className="worker-detail-muted">No template snapshot was available for this worker.</p>
                )}
              </div>

              <div className="worker-detail-section">
                <div className="worker-detail-section-head">
                  <h3>Task</h3>
                  <FileJson />
                </div>
                <p className="worker-detail-task">{selectedWorker.task ?? "-"}</p>
              </div>
            </aside>
          </div>
        </section>
      </div>
    );
  }

  return (
    <motion.section
      className="dashboard-screen"
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -16 }}
      transition={{ duration: 0.24 }}
    >
      <nav className="dashboard-tabs" aria-label="Dashboard">
        <button type="button" className={view === "control" ? "dashboard-tab dashboard-tab-active" : "dashboard-tab"} onClick={() => setView("control")}>
          {copy("control")}
        </button>
        <button type="button" className={view === "workers" ? "dashboard-tab dashboard-tab-active" : "dashboard-tab"} onClick={() => setView("workers")}>
          {copy("workers")}
        </button>
        <button type="button" className={view === "system" ? "dashboard-tab dashboard-tab-active" : "dashboard-tab"} onClick={() => setView("system")}>
          {copy("systemView")}
        </button>
      </nav>

      <div className="dashboard-content">
        {view === "control" ? renderControl() : null}
        {view === "workers" ? renderWorkers() : null}
        {view === "system" ? renderSystem() : null}
      </div>

      {renderWorkerDetailModal()}

      {editingTemplateId !== null ? (
        <div className="template-modal-backdrop" role="presentation">
          <section className="template-modal" role="dialog" aria-modal="true" aria-label={copy("editTemplate")}>
            <header>
              <div>
                <h2>{isCreatingTemplate ? copy("newTemplate") : copy("editTemplate")}</h2>
                <p>{copy("focusedWorkerEditor")}</p>
              </div>
              <button type="button" className="template-icon-button" onClick={() => setEditingTemplateId(null)}>
                <X />
              </button>
            </header>
            <div className="template-modal-body">
              <Field label="ID">
                <input
                  value={templateForm.id}
                  disabled={!isCreatingTemplate || templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, id: event.target.value }))}
                />
              </Field>
              <Field label={copy("templateName")}>
                <input
                  value={templateForm.name}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, name: event.target.value }))}
                />
              </Field>
              <Field label={copy("templateDescription")}>
                <input
                  value={templateForm.description}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, description: event.target.value }))}
                />
              </Field>
              <Field label={copy("modelOverride")}>
                <input
                  value={templateForm.model}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, model: event.target.value }))}
                />
              </Field>
              <Field label={copy("systemPrompt")} tall>
                <textarea
                  value={templateForm.system_prompt}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, system_prompt: event.target.value }))}
                />
              </Field>
              <Field label={copy("tools")}>
                <textarea
                  value={templateForm.available_tools}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, available_tools: event.target.value }))}
                />
              </Field>
              <Field label={copy("permissions")}>
                <textarea
                  value={templateForm.required_permissions}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, required_permissions: event.target.value }))}
                />
              </Field>
              <Field label={copy("thinkingSteps")}>
                <input
                  type="number"
                  min={1}
                  value={templateForm.max_thinking_steps}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, max_thinking_steps: event.target.value }))}
                />
              </Field>
              <Field label={copy("timeoutSeconds")}>
                <input
                  type="number"
                  min={1}
                  value={templateForm.default_timeout_seconds}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, default_timeout_seconds: event.target.value }))}
                />
              </Field>
              <label className="template-check">
                <input
                  type="checkbox"
                  checked={templateForm.can_spawn_children}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, can_spawn_children: event.target.checked }))}
                />
                {copy("allowChildWorkers")}
              </label>
              <Field label={copy("childTemplates")}>
                <input
                  value={templateForm.allowed_child_templates}
                  disabled={templateSaving}
                  onChange={(event) => setTemplateForm((current) => ({ ...current, allowed_child_templates: event.target.value }))}
                />
              </Field>
            </div>
            <footer>
              {!isCreatingTemplate ? (
                <Button type="button" variant="danger" disabled={templateSaving} onClick={() => void deleteTemplate()}>
                  <Trash2 data-icon="inline-start" />
                  {copy("deleteTemplate")}
                </Button>
              ) : null}
              <div className="template-modal-spacer" />
              <Button type="button" variant="ghost" disabled={templateSaving} onClick={() => setEditingTemplateId(null)}>
                {copy("cancel")}
              </Button>
              <Button type="button" variant="primary" disabled={templateSaving} onClick={() => void saveTemplate()}>
                {templateSaving ? copy("checking") : copy(isCreatingTemplate ? "createTemplate" : "saveTemplate")}
              </Button>
            </footer>
          </section>
        </div>
      ) : null}
    </motion.section>
  );
}
