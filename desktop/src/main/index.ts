import {
  app,
  BrowserWindow,
  dialog,
  ipcMain,
  nativeTheme,
  shell,
  type OpenDialogOptions,
} from "electron";
import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import {
  access,
  copyFile,
  mkdir,
  readdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { promisify } from "node:util";

import {
  checkDesktopAppUpdate,
  downloadDesktopAppUpdate,
  getDesktopAppUpdateStatus,
  installDesktopAppUpdate,
  scheduleDesktopAppUpdateCheck,
} from "./appUpdater";
import {
  authorizeConnector,
  disconnectConnector,
  getConnectorStatus,
  type ConnectorAuthPayload,
  type ConnectorName,
} from "./connectors";
import { registerCodexAuthIPCHandlers, stopCodexAuthServer } from "./codexAuth";
import {
  checkOctopalUpdateSafely,
  ensureWorkspaceBootstrap,
  getOctopalStatusSafely,
  runInstall,
  startOctopalSafely,
  stopOctopalSafely,
  updateOctopalSafely,
  withLocalToolPaths,
  type InstallEvent,
  type InstallPayload,
} from "./installer";
import {
  getWhatsAppLinkStatus,
  startWhatsAppLink,
  stopWhatsAppLink,
} from "./whatsapp";

const execFileAsync = promisify(execFile);
const EXISTING_SECRET_VALUE = "__OCTOPAL_DESKTOP_EXISTING_SECRET__";
const DESKTOP_CHAT_ATTACHMENT_LIMIT = 8;
const DESKTOP_CHAT_PREVIEW_MAX_BYTES = 25 * 1024 * 1024;

type DesktopSettings = {
  language: "en" | "fr" | "es" | "zh";
  theme: "light" | "dark" | "system";
  installDir: string;
};

type InstallState = {
  installed: boolean;
  installDir: string;
  configPath: string;
  planPath: string;
  reason?: string;
};

type PrerequisiteCheck = {
  id: string;
  label: string;
  ok: boolean;
  required: boolean;
  detail: string;
};

type DashboardWorkerRun = {
  id?: string;
  template_name?: string;
  template_id?: string;
  status?: string;
  task?: string;
  created_at?: string;
  updated_at?: string;
  summary?: string;
  error?: string;
  tools_used?: string[];
  parent_worker_id?: string | null;
  lineage_id?: string | null;
  spawn_depth?: number;
  result_preview?: string;
  output?: Record<string, unknown> | null;
  plan_binding?: {
    run_id?: string | null;
    step_id?: string | null;
    status?: string | null;
    title?: string | null;
    kind?: string | null;
  } | null;
  template_config?: {
    model?: string | null;
    max_thinking_steps?: number | null;
    default_timeout_seconds?: number | null;
    available_tools?: string[];
    can_spawn_children?: boolean;
  } | null;
  audit_timeline?: Array<{
    id?: string;
    ts?: string;
    level?: string;
    event_type?: string;
    data_preview?: string;
  }>;
};

type DesktopMcpServer = {
  id: string;
  name: string;
  status: string;
  reason: string;
  transport: string;
  toolCount: number;
  reconnectAttempts: number;
  error?: string;
};

type DesktopDashboardSnapshot = {
  ok: boolean;
  detail: string;
  generatedAt?: string;
  baseUrl?: string;
  dashboardEnabled?: boolean;
  starting?: boolean;
  attention?: {
    title: string;
    detail: string;
    timestamp?: string;
    service?: string;
    level?: string;
  };
  load?: {
    activeWorkers: number;
    queueDepth: number;
    octoQueue: number;
  };
  octo?: {
    state: string;
    headline: string;
    detail: string;
    latestAction: string;
  };
  workers?: {
    recent: DashboardWorkerRun[];
  };
  system?: {
    services: Array<{
      id: string;
      name: string;
      status: string;
      reason: string;
    }>;
    mcpServers: DesktopMcpServer[];
    logs: Array<{
      timestamp?: string;
      level?: string;
      service?: string;
      event?: string;
    }>;
  };
};

type DesktopWorkerTemplate = {
  id: string;
  name: string;
  description: string;
  system_prompt: string;
  available_tools: string[];
  required_permissions: string[];
  model?: string | null;
  max_thinking_steps: number;
  default_timeout_seconds: number;
  can_spawn_children: boolean;
  allowed_child_templates: string[];
  created_at?: string;
  updated_at?: string;
};

type DesktopSkill = {
  id: string;
  name: string;
  description: string;
  scope: string;
  enabled: boolean;
  ready: boolean;
  status: string;
  reasons: string[];
  origin: string;
  source: {
    kind: string;
    label: string;
    path: string;
    installer_managed: boolean;
    auto_discovered: boolean;
  };
  trust: {
    trusted: boolean;
    has_scripts: boolean;
    scan_status: string;
    scan_findings_count: number;
  };
  runtime: {
    kind: string;
    required: boolean;
    recommended: boolean;
    prepared: boolean;
    next_step: string;
  };
  requirements: {
    missing_bins: string[];
    missing_env: string[];
    missing_config: string[];
  };
  actions: {
    can_enable: boolean;
    can_disable: boolean;
    can_remove: boolean;
    can_install: boolean;
  };
};

type DesktopSkillsResponse = {
  contract_version: string;
  count: number;
  registry_path: string;
  skills: DesktopSkill[];
  install: {
    supported_sources: string[];
    default_clawhub_site: string;
  };
};

type DesktopSkillInstallPayload = {
  source: string;
  clawhub_site?: string;
};

type DesktopChatConnectionStatus = {
  ok: boolean;
  state: "idle" | "connecting" | "connected" | "disconnected" | "error";
  detail: string;
  installDir?: string;
  url?: string;
};

type DesktopChatClientEvent = Record<string, unknown> & {
  type?: string;
};

type DesktopChatAttachment = {
  path: string;
  name: string;
  sizeBytes: number;
  previewUrl?: string;
};

type DesktopPastedChatImage = {
  name?: string;
  mimeType?: string;
  dataUrl?: string;
};

type DesktopChatSession = {
  installDir: string;
  socket: WebSocket | null;
  window: BrowserWindow | null;
  status: DesktopChatConnectionStatus;
};

let chatSession: DesktopChatSession | null = null;
let chatConnectPromise: Promise<DesktopChatConnectionStatus> | null = null;
let chatConnectAttempt = 0;

const defaultSettings: DesktopSettings = {
  language: "en",
  theme: "system",
  installDir: "",
};

function settingsPath(): string {
  return join(app.getPath("userData"), "octopal-desktop.json");
}

async function readSettings(): Promise<DesktopSettings> {
  try {
    const raw = await readFile(settingsPath(), "utf8");
    return { ...defaultSettings, ...JSON.parse(raw) };
  } catch {
    return defaultSettings;
  }
}

async function writeSettings(
  settings: DesktopSettings,
): Promise<DesktopSettings> {
  const next = { ...defaultSettings, ...settings };
  await mkdir(app.getPath("userData"), { recursive: true });
  await writeFile(settingsPath(), JSON.stringify(next, null, 2), "utf8");
  nativeTheme.themeSource = next.theme;
  return next;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function cloneJsonRecord(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    return {};
  }
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

function deepMergeRecords(
  existing: Record<string, unknown>,
  incoming: Record<string, unknown>,
): Record<string, unknown> {
  const merged: Record<string, unknown> = { ...existing };
  for (const [key, value] of Object.entries(incoming)) {
    const current = merged[key];
    if (isRecord(current) && isRecord(value)) {
      merged[key] = deepMergeRecords(current, value);
      continue;
    }
    merged[key] = value;
  }
  return merged;
}

function getNested(root: Record<string, unknown>, path: string[]): unknown {
  let current: unknown = root;
  for (const segment of path) {
    if (!isRecord(current)) {
      return undefined;
    }
    current = current[segment];
  }
  return current;
}

function setNested(
  root: Record<string, unknown>,
  path: string[],
  value: unknown,
): void {
  let current = root;
  for (const segment of path.slice(0, -1)) {
    const next = current[segment];
    if (!isRecord(next)) {
      current[segment] = {};
    }
    current = current[segment] as Record<string, unknown>;
  }
  current[path[path.length - 1]] = value;
}

function isBlankSecret(value: unknown): boolean {
  return (
    value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "")
  );
}

function preserveSecretIfBlank(
  merged: Record<string, unknown>,
  existing: Record<string, unknown>,
  path: string[],
  options: { sameProviderPath?: string[] } = {},
): void {
  const incomingValue = getNested(merged, path);
  const existingValue = getNested(existing, path);
  if (isBlankSecret(incomingValue) && !isBlankSecret(existingValue)) {
    if (options.sameProviderPath) {
      const mergedProvider = getNested(merged, options.sameProviderPath);
      const existingProvider = getNested(existing, options.sameProviderPath);
      if (mergedProvider !== existingProvider) {
        return;
      }
    }
    setNested(merged, path, existingValue);
  }
}

function mergeConfigForDesktopSave(
  existingConfig: unknown,
  incomingConfig: unknown,
): Record<string, unknown> {
  const existing = cloneJsonRecord(existingConfig);
  const incoming = cloneJsonRecord(incomingConfig);
  const merged = deepMergeRecords(existing, incoming);

  preserveSecretIfBlank(merged, existing, ["telegram", "bot_token"]);
  preserveSecretIfBlank(merged, existing, ["llm", "api_key"], {
    sameProviderPath: ["llm", "provider_id"],
  });
  preserveSecretIfBlank(merged, existing, ["worker_llm_default", "api_key"], {
    sameProviderPath: ["worker_llm_default", "provider_id"],
  });
  preserveSecretIfBlank(merged, existing, ["gateway", "dashboard_token"]);
  preserveSecretIfBlank(merged, existing, ["whatsapp", "callback_token"]);
  preserveSecretIfBlank(merged, existing, ["search", "brave_api_key"]);
  preserveSecretIfBlank(merged, existing, ["search", "firecrawl_api_key"]);
  preserveSecretIfBlank(
    merged,
    existing,
    ["connectors", "instances", "google", "credentials", "client_secret"],
    {
      sameProviderPath: [
        "connectors",
        "instances",
        "google",
        "credentials",
        "client_id",
      ],
    },
  );
  preserveSecretIfBlank(merged, existing, [
    "connectors",
    "instances",
    "google",
    "auth",
    "refresh_token",
  ]);
  preserveSecretIfBlank(merged, existing, [
    "connectors",
    "instances",
    "google",
    "auth",
    "access_token",
  ]);
  preserveSecretIfBlank(merged, existing, [
    "connectors",
    "instances",
    "github",
    "auth",
    "access_token",
  ]);

  return merged;
}

function sanitizeConfigForRenderer(config: unknown): Record<string, unknown> {
  const sanitized = cloneJsonRecord(config);
  const original = cloneJsonRecord(config);
  const maskedValue = (path: string[]) => {
    const value = getNested(original, path);
    return typeof value === "string" && value.trim()
      ? EXISTING_SECRET_VALUE
      : "";
  };
  const maskedNullableValue = (path: string[]) => {
    const value = getNested(original, path);
    return typeof value === "string" && value.trim()
      ? EXISTING_SECRET_VALUE
      : null;
  };
  setNested(
    sanitized,
    ["telegram", "bot_token"],
    maskedValue(["telegram", "bot_token"]),
  );
  setNested(
    sanitized,
    ["llm", "api_key"],
    maskedNullableValue(["llm", "api_key"]),
  );
  setNested(
    sanitized,
    ["worker_llm_default", "api_key"],
    maskedNullableValue(["worker_llm_default", "api_key"]),
  );
  setNested(
    sanitized,
    ["gateway", "dashboard_token"],
    maskedValue(["gateway", "dashboard_token"]),
  );
  setNested(
    sanitized,
    ["whatsapp", "callback_token"],
    maskedValue(["whatsapp", "callback_token"]),
  );
  setNested(
    sanitized,
    ["search", "brave_api_key"],
    maskedNullableValue(["search", "brave_api_key"]),
  );
  setNested(
    sanitized,
    ["search", "firecrawl_api_key"],
    maskedNullableValue(["search", "firecrawl_api_key"]),
  );
  setNested(
    sanitized,
    ["observability", "langfuse_secret_key"],
    maskedNullableValue(["observability", "langfuse_secret_key"]),
  );
  setNested(
    sanitized,
    ["connectors", "instances", "google", "credentials", "client_secret"],
    maskedNullableValue([
      "connectors",
      "instances",
      "google",
      "credentials",
      "client_secret",
    ]),
  );
  setNested(
    sanitized,
    ["connectors", "instances", "google", "auth", "refresh_token"],
    maskedNullableValue([
      "connectors",
      "instances",
      "google",
      "auth",
      "refresh_token",
    ]),
  );
  setNested(
    sanitized,
    ["connectors", "instances", "google", "auth", "access_token"],
    maskedNullableValue([
      "connectors",
      "instances",
      "google",
      "auth",
      "access_token",
    ]),
  );
  setNested(
    sanitized,
    ["connectors", "instances", "github", "auth", "access_token"],
    maskedNullableValue([
      "connectors",
      "instances",
      "github",
      "auth",
      "access_token",
    ]),
  );
  return sanitized;
}

async function scrubInstallPlan(planPath: string): Promise<void> {
  try {
    const raw = await readFile(planPath, "utf8");
    const plan = JSON.parse(raw) as Record<string, unknown>;
    if (!plan || typeof plan !== "object" || !("octopalConfig" in plan)) {
      return;
    }

    delete plan.octopalConfig;
    await writeFile(planPath, JSON.stringify(plan, null, 2), "utf8");
  } catch {
    // Legacy install plans are optional metadata; failures should not block app startup.
  }
}

async function getInstallState(): Promise<InstallState> {
  const settings = await readSettings();
  const installDir = settings.installDir;
  const configPath = installDir ? join(installDir, "config.json") : "";
  const planPath = installDir
    ? join(installDir, ".octopal-desktop", "install-plan.json")
    : "";

  if (!installDir) {
    return {
      installed: false,
      installDir,
      configPath,
      planPath,
      reason: "Install directory is not selected.",
    };
  }

  const hasProject = await pathExists(join(installDir, "pyproject.toml"));
  const hasConfig = await pathExists(configPath);
  if (hasProject && hasConfig) {
    await scrubInstallPlan(planPath);
  }

  return {
    installed: hasProject && hasConfig,
    installDir,
    configPath,
    planPath,
    reason:
      hasProject && hasConfig
        ? undefined
        : "Octopal project or config.json was not found.",
  };
}

async function loadInstalledConfig(): Promise<unknown> {
  const state = await getInstallState();
  if (!state.installed) {
    throw new Error(state.reason ?? "Octopal is not installed.");
  }

  return sanitizeConfigForRenderer(
    JSON.parse(await readFile(state.configPath, "utf8")),
  );
}

function stringValue(value: unknown, fallback = ""): string {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function numberValue(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function recordValue(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function listValue(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function compactOctoEvent(event: unknown): string {
  const raw = stringValue(event);
  if (!raw) {
    return "No recent activity";
  }

  const normalized = raw.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
  const lowered = normalized.toLowerCase();
  if (
    lowered.includes("agentworker context") ||
    lowered.includes("cwd=/workspace/workers")
  ) {
    return "Worker context updated";
  }
  if (
    lowered.includes("octo switched to websocket output channel") ||
    lowered.includes("octo attached websocket mirror channel")
  ) {
    return "Desktop chat mirror connected";
  }
  if (lowered.includes("octo detached websocket mirror channel")) {
    return "Desktop chat mirror disconnected";
  }

  const redacted = normalized
    .replace(/\b[a-f0-9]{8,}(?:[\s_-]+[a-f0-9]{4,}){2,}\b/gi, "worker")
    .replace(/\bcwd=\S+/gi, "cwd=worker workspace")
    .replace(/\s+/g, " ")
    .trim();
  const title = redacted.charAt(0).toUpperCase() + redacted.slice(1);
  return title.length > 80 ? `${title.slice(0, 77).trim()}...` : title;
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

function compactErrorDetail(value: unknown): string {
  const raw = stringValue(value);
  if (!raw) {
    return "";
  }

  const redacted = stripHtmlMarkup(raw)
    .replace(/\bGOCSPX-[A-Za-z0-9_-]{12,}\b/g, "[redacted-key]")
    .replace(/\b\d{7,12}:[A-Za-z0-9_-]{20,}\b/g, "[redacted-token]")
    .replace(/\bsk-[A-Za-z0-9_-]{16,}\b/g, "[redacted-key]");
  const lines = redacted
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const meaningful = lines.filter(
    (line) =>
      !line.startsWith("File ") &&
      !line.startsWith("Traceback ") &&
      !line.startsWith("During handling ") &&
      !line.startsWith("The above exception ") &&
      !/^\^+$/.test(line) &&
      !/^\.\.\.</.test(line),
  );
  const httpStatusLine = meaningful.find((line) =>
    /HTTPStatusError|Client error .* for url/i.test(line),
  );
  const preferred =
    httpStatusLine ??
    [...meaningful]
      .reverse()
      .find((line) =>
        /\b(error|exception|failed|not found|timeout|unauthorized|forbidden)\b/i.test(
          line,
        ),
      );
  const detail = preferred ?? meaningful.at(-1) ?? lines.at(-1) ?? redacted;
  return detail.length > 520 ? `${detail.slice(0, 517).trim()}...` : detail;
}

async function readLatestAttentionLog(
  installDir: string,
): Promise<DesktopDashboardSnapshot["attention"] | null> {
  const logPath = join(installDir, "data", "logs", "octopal.log");
  let raw = "";
  try {
    raw = await readFile(logPath, "utf8");
  } catch {
    return null;
  }

  const lines = raw.split(/\r?\n/).filter(Boolean).slice(-300).reverse();
  for (const line of lines) {
    let entry: Record<string, unknown>;
    try {
      entry = recordValue(JSON.parse(line));
    } catch {
      continue;
    }

    const level = stringValue(
      entry.level,
      stringValue(entry.log_level, "info"),
    ).toLowerCase();
    if (!["error", "critical"].includes(level)) {
      continue;
    }
    const timestamp = stringValue(entry.timestamp);
    const parsedTimestamp = timestamp ? Date.parse(timestamp) : Number.NaN;
    if (
      Number.isFinite(parsedTimestamp) &&
      Date.now() - parsedTimestamp > 60 * 60 * 1000
    ) {
      continue;
    }

    const title = compactOctoEvent(entry.event);
    const detail =
      compactErrorDetail(entry.error) ||
      compactErrorDetail(entry.exception) ||
      compactErrorDetail(entry.detail) ||
      compactErrorDetail(entry.message);
    return {
      title,
      detail: detail || title,
      timestamp,
      service: stringValue(entry.logger, stringValue(entry.service, "runtime")),
      level,
    };
  }

  return null;
}

function isAttentionState(value: unknown): boolean {
  return ["error", "failed", "critical"].includes(
    stringValue(value).toLowerCase(),
  );
}

function parseTimestampMs(value: unknown): number | null {
  const raw = stringValue(value);
  if (!raw) {
    return null;
  }
  const ms = Date.parse(raw);
  return Number.isFinite(ms) ? ms : null;
}

function isWithinStartupGrace(startedAt: unknown, graceMs = 45_000): boolean {
  const startedMs = parseTimestampMs(startedAt);
  return startedMs !== null && Date.now() - startedMs < graceMs;
}

async function loadRawConfigForInstall(
  installDir: string,
): Promise<Record<string, unknown>> {
  const configPath = join(installDir, "config.json");
  return cloneJsonRecord(JSON.parse(await readFile(configPath, "utf8")));
}

function dashboardBaseUrl(config: Record<string, unknown>): string {
  const gateway = recordValue(config.gateway);
  const host = stringValue(gateway.host, "127.0.0.1");
  const reachableHost =
    host === "0.0.0.0" || host === "::" ? "127.0.0.1" : host;
  const port = numberValue(gateway.port, 8798);
  return `http://${reachableHost}:${port}`;
}

function dashboardWsUrl(config: Record<string, unknown>): string {
  const gateway = recordValue(config.gateway);
  const token = stringValue(gateway.dashboard_token);
  const url = new URL("/ws", dashboardBaseUrl(config));
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  if (token) {
    url.searchParams.set("token", token);
  }
  return url.toString();
}

function dashboardWebappEnabled(config: Record<string, unknown>): boolean {
  return recordValue(config.gateway).webapp_enabled !== false;
}

async function fetchDashboardJson<T>(
  installDir: string,
  path: string,
  init?: RequestInit,
): Promise<T> {
  const config = await loadRawConfigForInstall(installDir);
  const gateway = recordValue(config.gateway);
  const token = stringValue(gateway.dashboard_token);
  const headers: HeadersInit = {
    "content-type": "application/json",
    ...(token ? { "x-octopal-token": token } : {}),
    ...(init?.headers ?? {}),
  };
  const url = `${dashboardBaseUrl(config)}${path}`;
  const response = await fetch(url, { ...init, headers });
  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    let parsedDetail = "";
    try {
      const parsed = JSON.parse(detail) as { detail?: unknown };
      if (typeof parsed.detail === "string" && parsed.detail.trim()) {
        parsedDetail = parsed.detail;
      }
    } catch {
      // Some local gateway failures are plain text.
    }
    throw new Error(
      parsedDetail || detail || `Dashboard request failed: ${response.status}`,
    );
  }
  return (await response.json()) as T;
}

function isDashboardConnectionError(error: unknown): boolean {
  if (error instanceof TypeError) {
    return true;
  }
  const message = error instanceof Error ? error.message : String(error);
  return /fetch failed|ECONNREFUSED|ECONNRESET|ENOTFOUND|ETIMEDOUT/i.test(
    message,
  );
}

function resolveConfiguredWorkspaceDir(
  installDir: string,
  config: Record<string, unknown>,
): string {
  const storage = recordValue(config.storage);
  const configured = stringValue(storage.workspace_dir, "workspace");
  return isAbsolute(configured) ? configured : join(installDir, configured);
}

function validateWorkerTemplateId(templateId: string): string {
  const id = templateId.trim();
  if (!/^[a-z0-9][a-z0-9_-]*$/.test(id)) {
    throw new Error(
      "Worker template id must use lowercase letters, numbers, '_' or '-' and start with a letter or digit.",
    );
  }
  return id;
}

function normalizeStringList(items: unknown): string[] {
  if (!Array.isArray(items)) {
    return [];
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of items) {
    const value = String(item ?? "").trim();
    if (!value || seen.has(value)) {
      continue;
    }
    seen.add(value);
    out.push(value);
  }
  return out;
}

function normalizeMcpServers(value: unknown): DesktopMcpServer[] {
  const servers = recordValue(value);
  return Object.entries(servers)
    .map(([id, payload]) => {
      const server = recordValue(payload);
      return {
        id,
        name: stringValue(server.name, id),
        status: stringValue(server.status, "unknown"),
        reason: stringValue(server.reason),
        transport: stringValue(server.transport, "auto"),
        toolCount: numberValue(server.tool_count),
        reconnectAttempts: numberValue(server.reconnect_attempts),
        error: stringValue(server.error) || undefined,
      };
    })
    .sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
}

function normalizeDesktopWorkerTemplate(
  template: DesktopWorkerTemplate,
  options: { expectedId?: string; updatedAt?: string } = {},
): DesktopWorkerTemplate {
  const id = validateWorkerTemplateId(template.id);
  if (options.expectedId && id !== options.expectedId) {
    throw new Error("Template id in path and payload must match.");
  }
  const name = stringValue(template.name);
  const description = stringValue(template.description);
  const systemPrompt = stringValue(template.system_prompt);
  if (!name) {
    throw new Error("Worker template name is required.");
  }
  if (!description) {
    throw new Error("Worker template description is required.");
  }
  if (!systemPrompt) {
    throw new Error("Worker template system_prompt is required.");
  }
  const maxThinkingSteps = numberValue(template.max_thinking_steps, 10);
  const defaultTimeoutSeconds = numberValue(
    template.default_timeout_seconds,
    300,
  );
  if (maxThinkingSteps <= 0) {
    throw new Error("max_thinking_steps must be greater than 0.");
  }
  if (defaultTimeoutSeconds <= 0) {
    throw new Error("default_timeout_seconds must be greater than 0.");
  }

  return {
    id,
    name,
    description,
    system_prompt: systemPrompt,
    available_tools: normalizeStringList(template.available_tools),
    required_permissions: normalizeStringList(template.required_permissions),
    model: stringValue(template.model) || null,
    max_thinking_steps: Math.trunc(maxThinkingSteps),
    default_timeout_seconds: Math.trunc(defaultTimeoutSeconds),
    can_spawn_children: Boolean(template.can_spawn_children),
    allowed_child_templates: normalizeStringList(
      template.allowed_child_templates,
    ),
    created_at: stringValue(template.created_at, options.updatedAt ?? ""),
    updated_at: stringValue(template.updated_at, options.updatedAt ?? ""),
  };
}

async function workerTemplateRoot(installDir: string): Promise<string> {
  const config = await loadRawConfigForInstall(installDir);
  await ensureWorkspaceBootstrap(installDir, config);
  const root = join(
    resolveConfiguredWorkspaceDir(installDir, config),
    "workers",
  );
  await mkdir(root, { recursive: true });
  return root;
}

function workerTemplateFile(workersRoot: string, templateId: string): string {
  const id = validateWorkerTemplateId(templateId);
  const root = resolve(workersRoot);
  const file = resolve(root, id, "worker.json");
  const rel = relative(root, file);
  if (rel.startsWith("..") || rel === "" || rel.includes(`..${sep}`)) {
    throw new Error("Invalid worker template path.");
  }
  return file;
}

async function readLocalWorkerTemplate(
  workerFile: string,
): Promise<DesktopWorkerTemplate | null> {
  try {
    const raw = cloneJsonRecord(JSON.parse(await readFile(workerFile, "utf8")));
    const stats = await stat(workerFile);
    const updatedAt = stats.mtime.toISOString();
    return normalizeDesktopWorkerTemplate(raw as DesktopWorkerTemplate, {
      updatedAt,
    });
  } catch {
    return null;
  }
}

async function listLocalWorkerTemplates(
  installDir: string,
): Promise<DesktopWorkerTemplate[]> {
  const workersRoot = await workerTemplateRoot(installDir);
  const entries = await readdir(workersRoot, { withFileTypes: true });
  const templates: DesktopWorkerTemplate[] = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const template = await readLocalWorkerTemplate(
      join(workersRoot, entry.name, "worker.json"),
    );
    if (template) {
      templates.push(template);
    }
  }
  return templates.sort(
    (a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id),
  );
}

async function saveLocalWorkerTemplate(
  installDir: string,
  template: DesktopWorkerTemplate,
  mode: "create" | "update",
): Promise<DesktopWorkerTemplate> {
  const workersRoot = await workerTemplateRoot(installDir);
  const normalized = normalizeDesktopWorkerTemplate(template);
  const file = workerTemplateFile(workersRoot, normalized.id);
  if (mode === "create" && existsSync(file)) {
    throw new Error(`Worker template '${normalized.id}' already exists.`);
  }
  if (mode === "update" && !existsSync(file)) {
    throw new Error(`Worker template '${normalized.id}' not found.`);
  }
  await mkdir(dirname(file), { recursive: true });
  const payload = {
    id: normalized.id,
    name: normalized.name,
    description: normalized.description,
    system_prompt: normalized.system_prompt,
    available_tools: normalized.available_tools,
    required_permissions: normalized.required_permissions,
    model: normalized.model,
    max_thinking_steps: normalized.max_thinking_steps,
    default_timeout_seconds: normalized.default_timeout_seconds,
    can_spawn_children: normalized.can_spawn_children,
    allowed_child_templates: normalized.allowed_child_templates,
  };
  await writeFile(file, JSON.stringify(payload, null, 2), "utf8");
  return (await readLocalWorkerTemplate(file)) ?? normalized;
}

async function deleteLocalWorkerTemplate(
  installDir: string,
  templateId: string,
): Promise<void> {
  const workersRoot = await workerTemplateRoot(installDir);
  const file = workerTemplateFile(workersRoot, templateId);
  await rm(dirname(file), { recursive: true, force: true });
}

function queryPath(path: string): string {
  const query = new URLSearchParams({
    window_minutes: "60",
    service: "all",
    environment: "all",
  });
  return `${path}?${query.toString()}`;
}

async function getDesktopDashboardSnapshot(
  installDir: string,
): Promise<DesktopDashboardSnapshot> {
  try {
    const [overview, workers, octo, system] = await Promise.all([
      fetchDashboardJson<Record<string, unknown>>(
        installDir,
        queryPath("/api/dashboard/v2/overview"),
      ),
      fetchDashboardJson<Record<string, unknown>>(
        installDir,
        `${queryPath("/api/dashboard/v2/workers")}&last=16`,
      ),
      fetchDashboardJson<Record<string, unknown>>(
        installDir,
        queryPath("/api/dashboard/v2/octo"),
      ),
      fetchDashboardJson<Record<string, unknown>>(
        installDir,
        queryPath("/api/dashboard/v2/system"),
      ),
    ]);

    const overviewHealth = recordValue(overview.health);
    const kpis = recordValue(overview.kpis);
    const workersNode = recordValue(workers.workers);
    const octoNode = recordValue(octo.octo);
    const octoHealth = recordValue(octo.health);
    const systemNode = recordValue(system.system);
    const connectivityNode = recordValue(system.connectivity);
    const systemLogs = listValue(system.logs) as Array<Record<string, unknown>>;
    const recentOctoLog =
      systemLogs.find((entry) =>
        stringValue(entry.service).toLowerCase().includes("octo"),
      ) ?? systemLogs[0];
    const recentOctoEvent = recentOctoLog
      ? compactOctoEvent(recentOctoLog.event)
      : "";
    const recentOctoLevel = recentOctoLog
      ? stringValue(recentOctoLog.level, "info").toLowerCase()
      : "";
    const services = listValue(system.services).map((entry, index) => {
      const service = recordValue(entry);
      return {
        id: stringValue(
          service.id,
          stringValue(service.name, `service-${index}`),
        ),
        name: stringValue(
          service.name,
          stringValue(service.id, `Service ${index + 1}`),
        ),
        status: stringValue(service.status, "unknown"),
        reason: stringValue(service.reason),
      };
    });
    const config = await loadRawConfigForInstall(installDir);
    const attention = await readLatestAttentionLog(installDir);
    const octoState = stringValue(octoNode.state, "idle");
    const schedulerFailed =
      stringValue(systemNode.last_scheduler_tick_status).toLowerCase() ===
      "error";
    const startupGrace = isWithinStartupGrace(systemNode.started_at);
    const shouldSurfaceAttention =
      Boolean(attention) &&
      !startupGrace &&
      (isAttentionState(octoState) || schedulerFailed);
    const visibleOctoEvent =
      shouldSurfaceAttention || !["error", "critical"].includes(recentOctoLevel)
        ? recentOctoEvent
        : "";

    return {
      ok: true,
      detail: shouldSurfaceAttention
        ? attention?.detail || ""
        : stringValue(overviewHealth.summary, "Dashboard data loaded."),
      generatedAt: stringValue(overview.generated_at),
      baseUrl: dashboardBaseUrl(config),
      dashboardEnabled: dashboardWebappEnabled(config),
      starting: startupGrace,
      attention: shouldSurfaceAttention ? (attention ?? undefined) : undefined,
      load: {
        activeWorkers: numberValue(workersNode.running),
        queueDepth: numberValue(recordValue(kpis.queue_depth).value),
        octoQueue:
          numberValue(octoNode.followup_queues) +
          numberValue(octoNode.internal_queues),
      },
      octo: {
        state: octoState,
        headline: shouldSurfaceAttention
          ? attention?.title || visibleOctoEvent
          : visibleOctoEvent || stringValue(octoHealth.summary, "Octo is idle"),
        detail: shouldSurfaceAttention
          ? attention?.detail || ""
          : visibleOctoEvent && recentOctoLog
            ? `${stringValue(recentOctoLog.service, "runtime")} · ${stringValue(recentOctoLog.level, "info")}`
            : listValue(octoHealth.reasons)
                .map((item) => String(item))
                .join(" · "),
        latestAction: shouldSurfaceAttention
          ? attention?.title || visibleOctoEvent || "No recent activity"
          : visibleOctoEvent || "No recent activity",
      },
      workers: {
        recent: listValue(workersNode.recent).map(
          (entry) => recordValue(entry) as DashboardWorkerRun,
        ),
      },
      system: {
        services,
        mcpServers: normalizeMcpServers(
          recordValue(connectivityNode.mcp_servers),
        ),
        logs: systemLogs.slice(0, 12).map((entry) => ({
          timestamp: stringValue(entry.timestamp),
          level: stringValue(entry.level, "info"),
          service: stringValue(entry.service, "runtime"),
          event: stringValue(entry.event),
        })),
      },
    };
  } catch (error) {
    return {
      ok: false,
      detail:
        error instanceof Error
          ? error.message
          : "Dashboard data is unavailable.",
    };
  }
}

async function getDesktopWorkerTemplates(
  installDir: string,
): Promise<DesktopWorkerTemplate[]> {
  try {
    const payload = await fetchDashboardJson<{
      templates?: DesktopWorkerTemplate[];
    }>(installDir, "/api/dashboard/worker-templates");
    return payload.templates ?? [];
  } catch {
    return listLocalWorkerTemplates(installDir);
  }
}

async function applyDesktopConnectorRuntime(
  installDir: string,
  name: ConnectorName,
): Promise<{
  ok: boolean;
  name: ConnectorName;
  status?: string;
  message: string;
  detail: string;
}> {
  try {
    const payload = await fetchDashboardJson<{
      status?: string;
      connectors?: Record<string, unknown>;
    }>(installDir, "/api/dashboard/connectors/apply", {
      method: "POST",
      body: JSON.stringify({ name }),
    });
    const connector = recordValue(recordValue(payload.connectors)[name]);
    return {
      ok: true,
      name,
      status: stringValue(connector.status, stringValue(payload.status, "applied")),
      message: "Connector applied to the running instance.",
      detail: stringValue(connector.message, "Connector runtime reconciled."),
    };
  } catch (error) {
    return {
      ok: false,
      name,
      message: "Connector settings were saved. Restart Octopal if the running instance does not pick them up.",
      detail:
        error instanceof Error
          ? error.message
          : "Connector runtime apply failed.",
    };
  }
}

async function saveDesktopWorkerTemplate(
  installDir: string,
  template: DesktopWorkerTemplate,
  mode: "create" | "update",
): Promise<DesktopWorkerTemplate> {
  const path =
    mode === "create"
      ? "/api/dashboard/worker-templates"
      : `/api/dashboard/worker-templates/${encodeURIComponent(template.id)}`;
  try {
    const payload = await fetchDashboardJson<{
      template?: DesktopWorkerTemplate;
    }>(installDir, path, {
      method: mode === "create" ? "POST" : "PUT",
      body: JSON.stringify(template),
    });
    return payload.template ?? template;
  } catch (error) {
    if (!isDashboardConnectionError(error)) {
      throw error;
    }
    return saveLocalWorkerTemplate(installDir, template, mode);
  }
}

async function deleteDesktopWorkerTemplate(
  installDir: string,
  templateId: string,
): Promise<void> {
  try {
    await fetchDashboardJson<{ status: string }>(
      installDir,
      `/api/dashboard/worker-templates/${encodeURIComponent(templateId)}`,
      { method: "DELETE" },
    );
  } catch (error) {
    if (!isDashboardConnectionError(error)) {
      throw error;
    }
    await deleteLocalWorkerTemplate(installDir, templateId);
  }
}

async function getDesktopSkills(
  installDir: string,
): Promise<DesktopSkillsResponse> {
  return fetchDashboardJson<DesktopSkillsResponse>(
    installDir,
    "/api/dashboard/skills",
  );
}

async function installDesktopSkill(
  installDir: string,
  payload: DesktopSkillInstallPayload,
): Promise<DesktopSkill> {
  const response = await fetchDashboardJson<{ skill?: DesktopSkill }>(
    installDir,
    "/api/dashboard/skills/install",
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
  );
  if (!response.skill) {
    throw new Error("Skill was installed but could not be reloaded.");
  }
  return response.skill;
}

async function setDesktopSkillEnabled(
  installDir: string,
  skillId: string,
  enabled: boolean,
): Promise<DesktopSkill> {
  const response = await fetchDashboardJson<{ skill?: DesktopSkill }>(
    installDir,
    `/api/dashboard/skills/${encodeURIComponent(skillId)}/${enabled ? "enable" : "disable"}`,
    { method: "POST" },
  );
  if (!response.skill) {
    throw new Error("Skill was updated but could not be reloaded.");
  }
  return response.skill;
}

async function deleteDesktopSkill(
  installDir: string,
  skillId: string,
): Promise<DesktopSkillsResponse> {
  const response = await fetchDashboardJson<{ skills?: DesktopSkillsResponse }>(
    installDir,
    `/api/dashboard/skills/${encodeURIComponent(skillId)}`,
    { method: "DELETE" },
  );
  if (!response.skills) {
    throw new Error("Skill was deleted but the skill list could not be reloaded.");
  }
  return response.skills;
}

function emitChatStatus(status: DesktopChatConnectionStatus): void {
  const target = chatSession?.window;
  if (!target || target.isDestroyed() || target.webContents.isDestroyed()) {
    return;
  }
  target.webContents.send("desktop:chat-status", status);
}

function emitChatEvent(payload: DesktopChatClientEvent): void {
  const target = chatSession?.window;
  if (!target || target.isDestroyed() || target.webContents.isDestroyed()) {
    return;
  }
  target.webContents.send("desktop:chat-event", payload);
}

function setChatStatus(
  status: DesktopChatConnectionStatus,
): DesktopChatConnectionStatus {
  if (chatSession) {
    chatSession.status = status;
  }
  emitChatStatus(status);
  return status;
}

function closeChatSocket(
  detail = "Disconnected.",
): DesktopChatConnectionStatus {
  chatConnectPromise = null;
  chatConnectAttempt += 1;
  const session = chatSession;
  if (!session) {
    return { ok: true, state: "idle", detail };
  }
  const socket = session.socket;
  session.socket = null;
  if (
    socket &&
    socket.readyState !== WebSocket.CLOSED &&
    socket.readyState !== WebSocket.CLOSING
  ) {
    socket.close(1000, "desktop disconnect");
  }
  return setChatStatus({
    ok: true,
    state: "disconnected",
    detail,
    installDir: session.installDir,
    url: session.status.url,
  });
}

async function connectDesktopChat(
  installDir: string,
  window: BrowserWindow | null,
): Promise<DesktopChatConnectionStatus> {
  const normalizedInstallDir = String(installDir || "").trim();
  if (!normalizedInstallDir) {
    return {
      ok: false,
      state: "error",
      detail: "Install directory is required.",
    };
  }

  if (chatSession?.installDir === normalizedInstallDir && chatSession.socket) {
    const readyState = chatSession.socket.readyState;
    if (readyState === WebSocket.OPEN || readyState === WebSocket.CONNECTING) {
      chatSession.window = window;
      emitChatStatus(chatSession.status);
      return chatSession.status;
    }
  }

  if (chatConnectPromise && chatSession?.installDir === normalizedInstallDir) {
    chatSession.window = window;
    return chatConnectPromise;
  }

  if (chatSession?.installDir !== normalizedInstallDir) {
    closeChatSocket("Reconnecting.");
  }

  const attempt = (chatConnectAttempt += 1);
  chatSession = {
    installDir: normalizedInstallDir,
    socket: null,
    window,
    status: {
      ok: true,
      state: "connecting",
      detail: "Connecting to Octopal chat.",
      installDir: normalizedInstallDir,
    },
  };
  emitChatStatus(chatSession.status);

  const connectPromise = (async () => {
    let url: string | undefined;
    try {
      const config = await loadRawConfigForInstall(normalizedInstallDir);
      url = dashboardWsUrl(config);
      if (
        !chatSession ||
        chatSession.installDir !== normalizedInstallDir ||
        attempt !== chatConnectAttempt
      ) {
        return {
          ok: true,
          state: "disconnected" as const,
          detail: "Chat connection was replaced.",
          installDir: normalizedInstallDir,
          ...(url ? { url } : {}),
        };
      }

      chatSession.status = { ...chatSession.status, url };
      const socket = new WebSocket(url);
      chatSession.socket = socket;

      const isCurrentSocket = () =>
        chatSession?.socket === socket &&
        chatSession.installDir === normalizedInstallDir &&
        attempt === chatConnectAttempt;

      socket.addEventListener("open", () => {
        if (!isCurrentSocket()) {
          return;
        }
        setChatStatus({
          ok: true,
          state: "connected",
          detail: "Connected to Octopal chat.",
          installDir: normalizedInstallDir,
          url,
        });
      });
      socket.addEventListener("message", (event) => {
        if (!isCurrentSocket()) {
          return;
        }
        try {
          emitChatEvent(
            JSON.parse(String(event.data)) as DesktopChatClientEvent,
          );
        } catch {
          emitChatEvent({ type: "raw", text: String(event.data) });
        }
      });
      socket.addEventListener("error", () => {
        if (!isCurrentSocket()) {
          return;
        }
        setChatStatus({
          ok: false,
          state: "error",
          detail: "Chat WebSocket failed.",
          installDir: normalizedInstallDir,
          url,
        });
      });
      socket.addEventListener("close", () => {
        if (isCurrentSocket()) {
          chatSession!.socket = null;
          setChatStatus({
            ok: true,
            state: "disconnected",
            detail: "Chat WebSocket disconnected.",
            installDir: normalizedInstallDir,
            url,
          });
        }
      });

      return chatSession.status;
    } catch (error) {
      if (attempt !== chatConnectAttempt) {
        return (
          chatSession?.status ?? {
            ok: true,
            state: "disconnected" as const,
            detail: "Chat connection was replaced.",
            installDir: normalizedInstallDir,
          }
        );
      }
      return setChatStatus({
        ok: false,
        state: "error",
        detail:
          error instanceof Error ? error.message : "Unable to connect to chat.",
        installDir: normalizedInstallDir,
        ...(url ? { url } : {}),
      });
    }
  })();

  chatConnectPromise = connectPromise;
  try {
    return await connectPromise;
  } finally {
    if (chatConnectPromise === connectPromise) {
      chatConnectPromise = null;
    }
  }
}

function sendDesktopChatPayload(payload: DesktopChatClientEvent): void {
  const socket = chatSession?.socket;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    throw new Error("Chat is not connected.");
  }
  socket.send(JSON.stringify(payload));
}

async function desktopChatAttachmentForPath(
  filePath: string,
  displayName?: string,
): Promise<DesktopChatAttachment | null> {
  const path = resolve(String(filePath || ""));
  const info = await stat(path);
  if (!info.isFile()) {
    return null;
  }
  const mimeType = imageMimeTypeForPath(path);
  const previewUrl = mimeType && info.size <= DESKTOP_CHAT_PREVIEW_MAX_BYTES
    ? `data:${mimeType};base64,${(await readFile(path)).toString("base64")}`
    : undefined;
  const name = String(displayName || basename(path)).trim() || basename(path);
  return {
    path,
    name,
    sizeBytes: info.size,
    ...(previewUrl ? { previewUrl } : {}),
  };
}

function imageMimeTypeForPath(path: string): string | null {
  const lower = path.toLowerCase();
  if (lower.endsWith(".avif")) {
    return "image/avif";
  }
  if (lower.endsWith(".gif")) {
    return "image/gif";
  }
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) {
    return "image/jpeg";
  }
  if (lower.endsWith(".png")) {
    return "image/png";
  }
  if (lower.endsWith(".webp")) {
    return "image/webp";
  }
  return null;
}

function imageExtensionForMimeType(mimeType: string): string {
  switch (mimeType.toLowerCase()) {
    case "image/avif":
      return ".avif";
    case "image/gif":
      return ".gif";
    case "image/jpeg":
    case "image/jpg":
      return ".jpg";
    case "image/png":
      return ".png";
    case "image/webp":
      return ".webp";
    default:
      return ".png";
  }
}

async function resolveDesktopChatAttachmentDir(installDir: string): Promise<string> {
  const normalizedInstallDir = resolve(String(installDir || ""));
  const config = await loadRawConfigForInstall(normalizedInstallDir);
  return join(resolveConfiguredWorkspaceDir(normalizedInstallDir, config), "tmp", "desktop_chat");
}

function safeDesktopChatFileName(name: string, fallback: string): string {
  const safeName = String(name || fallback)
    .replace(/[\\/]/g, "_")
    .replace(/^\.+$/, "")
    .trim();
  return safeName || fallback;
}

async function stageDesktopChatFile(
  installDir: string,
  sourcePath: string,
): Promise<DesktopChatAttachment | null> {
  const source = resolve(String(sourcePath || ""));
  const info = await stat(source);
  if (!info.isFile()) {
    return null;
  }
  const targetDir = await resolveDesktopChatAttachmentDir(installDir);
  await mkdir(targetDir, { recursive: true });
  const name = safeDesktopChatFileName(basename(source), `attachment-${randomUUID()}`);
  const targetPath = join(targetDir, `${randomUUID()}-${name}`);
  await copyFile(source, targetPath);
  return desktopChatAttachmentForPath(targetPath, name);
}

async function saveDesktopChatPastedImage(
  installDir: string,
  image: DesktopPastedChatImage,
): Promise<DesktopChatAttachment> {
  const normalizedInstallDir = String(installDir || "").trim();
  if (!normalizedInstallDir) {
    throw new Error("Install directory is required.");
  }

  const mimeType = stringValue(image.mimeType, "image/png").toLowerCase();
  if (!mimeType.startsWith("image/")) {
    throw new Error("Only image clipboard data can be pasted.");
  }

  const dataUrl = stringValue(image.dataUrl);
  const match = dataUrl.match(/^data:(?<mime>image\/[a-z0-9.+-]+);base64,(?<data>[a-z0-9+/=\s]+)$/i);
  if (!match?.groups?.data) {
    throw new Error("Clipboard image data is invalid.");
  }

  const binary = Buffer.from(match.groups.data.replace(/\s+/g, ""), "base64");
  if (binary.length === 0) {
    throw new Error("Clipboard image is empty.");
  }
  if (binary.length > 25 * 1024 * 1024) {
    throw new Error("Clipboard image is too large.");
  }

  const targetDir = await resolveDesktopChatAttachmentDir(normalizedInstallDir);
  await mkdir(targetDir, { recursive: true });
  const extension = imageExtensionForMimeType(match.groups.mime || mimeType);
  const name = safeDesktopChatFileName(
    stringValue(image.name, `pasted-image-${randomUUID()}${extension}`),
    `pasted-image-${randomUUID()}${extension}`,
  );
  const fileName = name.includes(".") ? name : `${name}${extension}`;
  const filePath = join(targetDir, `${randomUUID()}-${fileName}`);
  await writeFile(filePath, binary);

  const attachment = await desktopChatAttachmentForPath(filePath, fileName);
  if (!attachment) {
    throw new Error("Unable to save pasted image.");
  }
  return attachment;
}

async function chooseDesktopChatFiles(
  window: BrowserWindow | null,
  installDir: string,
): Promise<DesktopChatAttachment[]> {
  const options: OpenDialogOptions = {
    title: "Attach files",
    properties: ["openFile", "multiSelections"],
  };
  const result = window
    ? await dialog.showOpenDialog(window, options)
    : await dialog.showOpenDialog(options);
  if (result.canceled || result.filePaths.length === 0) {
    return [];
  }

  const attachments: DesktopChatAttachment[] = [];
  for (const filePath of result.filePaths.slice(0, DESKTOP_CHAT_ATTACHMENT_LIMIT)) {
    try {
      const attachment = await stageDesktopChatFile(installDir, filePath);
      if (attachment) {
        attachments.push(attachment);
      }
    } catch {
      // Ignore paths that disappear between picker selection and stat.
    }
  }
  return attachments;
}

function normalizeDesktopChatAttachments(value: unknown): DesktopChatAttachment[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .slice(0, 8)
    .map((entry): DesktopChatAttachment | null => {
      if (!isRecord(entry)) {
        return null;
      }
      const path = String(entry.path ?? "").trim();
      if (!path) {
        return null;
      }
      return {
        path: resolve(path),
        name: String(entry.name ?? basename(path)).trim() || basename(path),
        sizeBytes: numberValue(entry.sizeBytes, 0),
        ...(stringValue(entry.previewUrl) ? { previewUrl: stringValue(entry.previewUrl) } : {}),
      };
    })
    .filter((entry): entry is DesktopChatAttachment => entry !== null);
}

async function saveInstalledConfig(config: unknown): Promise<InstallState> {
  const state = await getInstallState();
  if (!state.installed) {
    throw new Error(state.reason ?? "Octopal is not installed.");
  }

  const existing = JSON.parse(await readFile(state.configPath, "utf8"));
  const merged = mergeConfigForDesktopSave(existing, config);
  await writeFile(state.configPath, JSON.stringify(merged, null, 2), "utf8");
  await ensureWorkspaceBootstrap(state.installDir, merged);
  return getInstallState();
}

function resolveBrandIcon(): string | undefined {
  const primaryIcon = process.platform === "darwin" ? "octo.png" : "octo.ico";
  const filenames = [
    primaryIcon,
    primaryIcon === "octo.ico" ? "octo.png" : "octo.ico",
  ];
  const roots = [process.cwd(), app.getAppPath(), process.resourcesPath];

  for (const root of roots) {
    for (const filename of filenames) {
      const candidate = join(root, "assets", filename);
      if (existsSync(candidate)) {
        return candidate;
      }
    }
  }

  return undefined;
}

function isExternalUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return ["http:", "https:", "tg:"].includes(parsed.protocol);
  } catch {
    return false;
  }
}

function openExternalUrl(url: string): boolean {
  if (!isExternalUrl(url)) {
    return false;
  }

  void shell.openExternal(url);
  return true;
}

function createWindow(): void {
  const icon = resolveBrandIcon();
  const mainWindow = new BrowserWindow({
    width: 1180,
    height: 820,
    minWidth: 920,
    minHeight: 680,
    title: "Octopal Desktop",
    ...(icon ? { icon } : {}),
    backgroundColor: "#00000000",
    frame: false,
    transparent: true,
    hasShadow: true,
    webPreferences: {
      preload: join(__dirname, "../preload/index.mjs"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    openExternalUrl(url);
    return { action: "deny" };
  });

  mainWindow.webContents.on("will-navigate", (event, url) => {
    if (url !== mainWindow.webContents.getURL() && openExternalUrl(url)) {
      event.preventDefault();
    }
  });

  if (process.env.ELECTRON_RENDERER_URL) {
    void mainWindow.loadURL(process.env.ELECTRON_RENDERER_URL);
  } else {
    void mainWindow.loadFile(join(__dirname, "../renderer/index.html"));
  }
}

async function checkCommand(
  command: string,
  args: string[],
): Promise<{ ok: boolean; detail: string }> {
  try {
    const { stdout, stderr } = await execFileAsync(command, args, {
      timeout: 5000,
      windowsHide: true,
      env: withLocalToolPaths(),
    });
    return {
      ok: true,
      detail: (stdout || stderr).trim().split(/\r?\n/)[0] || "Available",
    };
  } catch (error) {
    if (
      error &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ENOENT"
    ) {
      return {
        ok: false,
        detail: `${command} was not found in PATH. Install it or restart Octopal Desktop after updating PATH.`,
      };
    }
    const message = error instanceof Error ? error.message : "Unavailable";
    return { ok: false, detail: message };
  }
}

function parseNodeMajor(version: string): number | null {
  const match = version.trim().match(/^v?(?<major>\d+)/);
  if (!match?.groups?.major) {
    return null;
  }
  return Number.parseInt(match.groups.major, 10);
}

async function checkNode20(): Promise<{ ok: boolean; detail: string }> {
  const node = await checkCommand("node", ["--version"]);
  if (!node.ok) {
    return node;
  }

  const major = parseNodeMajor(node.detail);
  if (major === null) {
    return {
      ok: false,
      detail: `Could not read Node.js version: ${node.detail}`,
    };
  }

  if (major < 20) {
    return {
      ok: false,
      detail: `Node.js 20+ is required for WhatsApp bridge. Found ${node.detail}.`,
    };
  }

  return node;
}

async function checkDockerRuntime(): Promise<{ ok: boolean; detail: string }> {
  const docker = await checkCommand("docker", ["--version"]);
  if (!docker.ok) {
    return docker;
  }

  const daemon = await checkCommand("docker", [
    "info",
    "--format",
    "{{.ServerVersion}}",
  ]);
  if (!daemon.ok) {
    return {
      ok: false,
      detail: `Docker CLI is installed, but the daemon is unavailable: ${daemon.detail}`,
    };
  }

  return { ok: true, detail: `Docker ${daemon.detail}` };
}

ipcMain.handle("desktop:load-settings", async () => readSettings());
ipcMain.handle(
  "desktop:save-settings",
  async (_event, settings: DesktopSettings) => writeSettings(settings),
);
ipcMain.handle("desktop:get-install-state", async () => getInstallState());
ipcMain.handle("desktop:load-octopal-config", async () =>
  loadInstalledConfig(),
);
ipcMain.handle("desktop:save-octopal-config", async (_event, config: unknown) =>
  saveInstalledConfig(config),
);

ipcMain.handle("desktop:choose-install-dir", async (event) => {
  const parentWindow = BrowserWindow.fromWebContents(event.sender) ?? undefined;
  const options: OpenDialogOptions = {
    title: "Choose Octopal install folder",
    properties: ["openDirectory", "createDirectory"],
  };
  const result = parentWindow
    ? await dialog.showOpenDialog(parentWindow, options)
    : await dialog.showOpenDialog(options);

  if (result.canceled || result.filePaths.length === 0) {
    return null;
  }

  return result.filePaths[0];
});

ipcMain.handle(
  "desktop:window-control",
  (event, action: "close" | "minimize" | "maximize") => {
    const window = BrowserWindow.fromWebContents(event.sender);
    if (!window) {
      return;
    }

    if (action === "close") {
      window.close();
      return;
    }

    if (action === "minimize") {
      window.minimize();
      return;
    }

    if (window.isMaximized()) {
      window.unmaximize();
    } else {
      window.maximize();
    }
  },
);

ipcMain.handle(
  "desktop:open-octopal-logs",
  async (_event, installDir: string): Promise<boolean> => {
    const logPath = join(installDir, "data", "logs", "octopal.log");
    const target = (await pathExists(logPath))
      ? logPath
      : join(installDir, "data", "logs");
    const error = await shell.openPath(target);
    return !error;
  },
);

ipcMain.handle(
  "desktop:check-prerequisites",
  async (): Promise<PrerequisiteCheck[]> => {
    const checks = await Promise.all([
      checkCommand("git", ["--version"]),
      checkCommand("uv", ["--version"]),
      checkDockerRuntime(),
      checkNode20(),
    ]);

    return [
      { id: "git", label: "Git", required: true, ...checks[0] },
      { id: "uv", label: "uv", required: false, ...checks[1] },
      { id: "docker", label: "Docker runtime", required: false, ...checks[2] },
      { id: "node", label: "Node.js 20+", required: false, ...checks[3] },
    ];
  },
);

ipcMain.handle(
  "desktop:write-install-plan",
  async (_event, payload: unknown) => {
    const settings = await readSettings();
    if (!settings.installDir) {
      throw new Error("Install directory is not selected.");
    }

    const planDir = join(settings.installDir, ".octopal-desktop");
    await mkdir(planDir, { recursive: true });
    const planPath = join(planDir, "install-plan.json");
    const payloadRecord =
      payload && typeof payload === "object" && !Array.isArray(payload)
        ? { ...payload }
        : {};
    delete (payloadRecord as Record<string, unknown>).octopalConfig;
    await writeFile(planPath, JSON.stringify(payloadRecord, null, 2), "utf8");
    return { planPath };
  },
);

ipcMain.handle(
  "desktop:install-octopal",
  async (event, payload: InstallPayload) => {
    const sender = event.sender;
    const emit = (installEvent: InstallEvent) => {
      if (!sender.isDestroyed()) {
        sender.send("desktop:install-event", installEvent);
      }
    };

    try {
      return await runInstall(payload, emit);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Installation failed.";
      emit({ kind: "error", message });
      throw error;
    }
  },
);

ipcMain.handle("desktop:start-octopal", async (_event, installDir: string) =>
  startOctopalSafely(installDir),
);
ipcMain.handle("desktop:stop-octopal", async (_event, installDir: string) =>
  stopOctopalSafely(installDir),
);
ipcMain.handle(
  "desktop:get-octopal-status",
  async (_event, installDir: string) => getOctopalStatusSafely(installDir),
);
ipcMain.handle(
  "desktop:check-octopal-update",
  async (_event, installDir: string) => checkOctopalUpdateSafely(installDir),
);
ipcMain.handle("desktop:update-octopal", async (_event, installDir: string) =>
  updateOctopalSafely(installDir),
);
ipcMain.handle(
  "desktop:get-dashboard-snapshot",
  async (_event, installDir: string) => getDesktopDashboardSnapshot(installDir),
);
ipcMain.handle(
  "desktop:get-worker-templates",
  async (_event, installDir: string) => getDesktopWorkerTemplates(installDir),
);
ipcMain.handle("desktop:get-skills", async (_event, installDir: string) =>
  getDesktopSkills(installDir),
);
ipcMain.handle(
  "desktop:install-skill",
  async (_event, installDir: string, payload: DesktopSkillInstallPayload) =>
    installDesktopSkill(installDir, payload),
);
ipcMain.handle(
  "desktop:set-skill-enabled",
  async (_event, installDir: string, skillId: string, enabled: boolean) =>
    setDesktopSkillEnabled(installDir, skillId, enabled),
);
ipcMain.handle(
  "desktop:delete-skill",
  async (_event, installDir: string, skillId: string) =>
    deleteDesktopSkill(installDir, skillId),
);
ipcMain.handle("desktop:chat-connect", async (event, installDir: string) =>
  connectDesktopChat(installDir, BrowserWindow.fromWebContents(event.sender)),
);
ipcMain.handle("desktop:chat-disconnect", async () => closeChatSocket());
ipcMain.handle("desktop:chat-choose-files", async (event, installDir: string) =>
  chooseDesktopChatFiles(BrowserWindow.fromWebContents(event.sender), installDir),
);
ipcMain.handle(
  "desktop:chat-save-pasted-image",
  async (_event, installDir: string, image: DesktopPastedChatImage) =>
    saveDesktopChatPastedImage(installDir, image),
);
ipcMain.handle(
  "desktop:chat-send-message",
  async (
    _event,
    payload: { text?: string; chatId?: number | null; attachments?: DesktopChatAttachment[] },
  ) => {
    const text = String(payload?.text ?? "").trim();
    const attachments = normalizeDesktopChatAttachments(payload?.attachments);
    if (!text && attachments.length === 0) {
      throw new Error("Message or attachment is required.");
    }
    const message: DesktopChatClientEvent = { type: "message", text };
    if (attachments.length > 0) {
      message.attachments = attachments.map((attachment) => ({
        path: attachment.path,
        name: attachment.name,
        size_bytes: attachment.sizeBytes,
      }));
    }
    if (typeof payload?.chatId === "number" && payload.chatId > 0) {
      message.chat_id = payload.chatId;
    }
    sendDesktopChatPayload(message);
    return { ok: true };
  },
);
ipcMain.handle(
  "desktop:chat-approval-response",
  async (_event, intentId: string, approved: boolean) => {
    const normalizedIntentId = String(intentId || "").trim();
    if (!normalizedIntentId) {
      throw new Error("Approval request id is missing.");
    }
    sendDesktopChatPayload({
      type: "approval_response",
      intent_id: normalizedIntentId,
      approved: Boolean(approved),
    });
    return { ok: true };
  },
);
ipcMain.handle("desktop:chat-ping", async () => {
  sendDesktopChatPayload({ type: "ping" });
  return { ok: true };
});
ipcMain.handle(
  "desktop:save-worker-template",
  async (
    _event,
    installDir: string,
    template: DesktopWorkerTemplate,
    mode: "create" | "update",
  ) => saveDesktopWorkerTemplate(installDir, template, mode),
);
ipcMain.handle(
  "desktop:delete-worker-template",
  async (_event, installDir: string, templateId: string) =>
    deleteDesktopWorkerTemplate(installDir, templateId),
);
ipcMain.handle("desktop:get-app-update-status", () =>
  getDesktopAppUpdateStatus(),
);
ipcMain.handle("desktop:check-app-update", () => checkDesktopAppUpdate());
ipcMain.handle("desktop:download-app-update", () => downloadDesktopAppUpdate());
ipcMain.handle("desktop:install-app-update", () => installDesktopAppUpdate());
ipcMain.handle(
  "desktop:get-connector-status",
  async (_event, installDir: string) => getConnectorStatus(installDir),
);
ipcMain.handle(
  "desktop:authorize-connector",
  async (_event, installDir: string, payload: ConnectorAuthPayload) =>
    authorizeConnector(installDir, payload),
);
ipcMain.handle(
  "desktop:disconnect-connector",
  async (
    _event,
    installDir: string,
    name: ConnectorName,
    forgetCredentials: boolean,
  ) => disconnectConnector(installDir, name, forgetCredentials),
);
ipcMain.handle(
  "desktop:apply-connector-runtime",
  async (_event, installDir: string, name: ConnectorName) =>
    applyDesktopConnectorRuntime(installDir, name),
);
ipcMain.handle(
  "desktop:start-whatsapp-link",
  async (_event, installDir: string) => startWhatsAppLink(installDir),
);
ipcMain.handle(
  "desktop:get-whatsapp-link-status",
  async (_event, installDir: string) => getWhatsAppLinkStatus(installDir),
);
ipcMain.handle(
  "desktop:stop-whatsapp-link",
  async (_event, installDir: string) => stopWhatsAppLink(installDir),
);
registerCodexAuthIPCHandlers();

void app.whenReady().then(async () => {
  nativeTheme.themeSource = (await readSettings()).theme;
  createWindow();
  scheduleDesktopAppUpdateCheck();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  stopCodexAuthServer();
  if (process.platform !== "darwin") {
    app.quit();
  }
});
