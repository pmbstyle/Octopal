import { Download, ExternalLink, Pencil, Play, Plus, RotateCw, Square, Trash2, X } from "lucide-react";
import { motion } from "framer-motion";
import { useCallback, useEffect, useMemo, useState } from "react";

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

function limitDisplayText(value: string, maxLength: number): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength - 3).trim()}...` : normalized;
}

function linePoints(points: LoadPoint[], key: keyof Omit<LoadPoint, "at">): string {
  const values = points.length > 0 ? points.map((point) => point[key]) : [0];
  const max = Math.max(1, ...values);
  const width = 1000;
  const height = 300;
  const step = points.length > 1 ? width / (points.length - 1) : width;
  return values
    .map((value, index) => {
      const x = index * step;
      const y = height - (Number(value) / max) * 220 - 34;
      return `${x},${Math.max(24, Math.min(height - 12, y))}`;
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
  const [templateForm, setTemplateForm] = useState<WorkerTemplateForm>(emptyTemplateForm);
  const [templateSaving, setTemplateSaving] = useState(false);

  const refreshSnapshot = useCallback(async () => {
    if (!window.octopalDesktop || !installDir) {
      return;
    }
    const next = await window.octopalDesktop.getDashboardSnapshot(installDir);
    setSnapshot(next);
    if (!next.ok) {
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
  }, [installDir]);

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
    if (history.length > 1) {
      return history;
    }
    const load = snapshot?.load ?? { activeWorkers: 0, queueDepth: 0, octoQueue: 0 };
    const shape = [0.72, 0.82, 0.9, 1.28, 1.14, 1.04, 1.18, 1.06, 0.98, 1.12];
    return shape.map((ratio, index) => ({
      at: Date.now() - (7 - index) * 4000,
      activeWorkers: Math.max(0, Math.round(load.activeWorkers * ratio)),
      queueDepth: Math.max(0, Math.round(load.queueDepth * (ratio + 0.12))),
      octoQueue: Math.max(0, Math.round(load.octoQueue * (1.08 - (ratio - 1) / 2))),
    }));
  }, [history, snapshot?.load]);

  const recentWorkers = snapshot?.workers?.recent ?? [];
  const octoState = snapshot?.octo?.state || runtimeView.state || "idle";
  const octoHeadlineRaw = snapshot?.octo?.headline || runtimeView.title;
  const octoDetailRaw = snapshot?.octo?.detail || runtimeView.detail || copy("octopalStarted");
  const latestActionRaw = snapshot?.octo?.latestAction || copy("octoLatestFallback");
  const octoHeadline = limitDisplayText(octoHeadlineRaw, 74);
  const octoDetail = limitDisplayText(octoDetailRaw, 120);
  const latestAction = limitDisplayText(latestActionRaw, 130);
  const services = snapshot?.system?.services ?? [];
  const logs = snapshot?.system?.logs ?? [];
  const editingTemplate = editingTemplateId
    ? templates.find((template) => template.id === editingTemplateId) ?? null
    : null;
  const isCreatingTemplate = editingTemplateId === "";

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

  function renderControl() {
    return (
      <section className="dashboard-control">
        <div className="dashboard-assistant-head">
          <div className="dashboard-octo-stack">
            <img className="octo dashboard-octo" src={octoImage} alt="Octopal mascot" />
            <span className={statusClass(octoState)}>{octoState}</span>
          </div>
          <div className="dashboard-bubble">
            <h1 title={octoHeadlineRaw}>{octoHeadline}</h1>
            <p title={octoDetailRaw}>{octoDetail}</p>
            <p className="dashboard-latest" title={latestActionRaw}>
              <strong>{copy("latestAction")}:</strong> {latestAction}
            </p>
          </div>
          {updateAvailable || desktopUpdateAvailable ? (
            <div className="dashboard-actions">
              <Button type="button" variant="ghost" onClick={() => setView("system")}>
                {copy("updateReady")}
              </Button>
            </div>
          ) : null}
        </div>

        <div className="dashboard-panel">
          <div className="dashboard-panel-head">
            <div>
              <h2>{copy("liveLoad")}</h2>
              <p>{copy("liveLoadBody")}</p>
            </div>
            <span className="dashboard-pill">{copy("lastSamples")}</span>
          </div>
          <div className="dashboard-chart" aria-label={copy("liveLoad")}>
            <svg viewBox="0 0 1000 300" preserveAspectRatio="none">
              <polyline points={linePoints(graphPoints, "activeWorkers")} fill="none" stroke="var(--accent)" strokeWidth="7" />
              <polyline points={linePoints(graphPoints, "queueDepth")} fill="none" stroke="#f4b84f" strokeWidth="5" />
              <polyline points={linePoints(graphPoints, "octoQueue")} fill="none" stroke="var(--success)" strokeWidth="4" />
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
            </div>
            {recentWorkers.length === 0 ? (
              <div className="dashboard-empty-row">{copy("noRecentWorkers")}</div>
            ) : (
              recentWorkers.slice(0, 8).map((worker, index) => (
                <div className="dashboard-worker-row" key={worker.id ?? `${worker.updated_at}-${index}`}>
                  <strong>{shortId(worker.id)}</strong>
                  <span className={statusClass(worker.status)}>{worker.status ?? "unknown"}</span>
                  <span>{worker.template_name ?? worker.template_id ?? "-"}</span>
                  <span>{worker.task ?? worker.result_preview ?? worker.summary ?? worker.error ?? "-"}</span>
                  <span>{formatTime(worker.updated_at)}</span>
                </div>
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
        <div className="dashboard-assistant-head dashboard-assistant-head-compact">
          <img className="octo dashboard-octo-small" src={octoImage} alt="Octopal mascot" />
          <div className="dashboard-bubble">
            <h1>{copy("workerTemplates")}</h1>
            <p>{copy("workerTemplatesBody")}</p>
          </div>
        </div>
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
        <div className="dashboard-assistant-head dashboard-assistant-head-compact">
          <img className="octo dashboard-octo-small" src={octoImage} alt="Octopal mascot" />
          <div className="dashboard-bubble">
            <h1>{runtimeView.title}</h1>
            <p>{runtimeView.detail || copy("systemBody")}</p>
          </div>
        </div>
        <div className="system-grid">
          <div className="dashboard-panel system-card">
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
              {snapshot?.baseUrl ? (
                <Button type="button" variant="ghost" onClick={() => window.open(snapshot.baseUrl, "_blank")}>
                  <ExternalLink data-icon="inline-start" />
                  {copy("openDashboardUrl")}
                </Button>
              ) : null}
            </div>
          </div>

          <div className="dashboard-panel system-card">
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
