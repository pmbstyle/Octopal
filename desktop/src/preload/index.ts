import { contextBridge, ipcRenderer } from "electron";

type DesktopSettings = {
  language: "en" | "fr" | "es" | "zh";
  theme: "light" | "dark" | "system";
  installDir: string;
};

type DesktopInstallEvent = {
  kind: "step" | "log" | "warning" | "error" | "done";
  message: string;
  detail?: string;
};

type DesktopInstallResult = {
  installDir: string;
  releaseTag: string;
  configPath: string;
  planPath: string;
};

type DesktopInstallState = {
  installed: boolean;
  installDir: string;
  configPath: string;
  planPath: string;
  reason?: string;
};

type DesktopStartResult = {
  ok: true;
  installDir: string;
  detail: string;
};

type DesktopStartFailure = {
  ok: false;
  error: string;
  detail: string;
};

type DesktopStopResult = {
  ok: true;
  installDir: string;
  detail: string;
};

type DesktopStopFailure = {
  ok: false;
  error: string;
  detail: string;
};

type DesktopRuntimeStatus = {
  ok: boolean;
  state: "running" | "stopped" | "error";
  title: string;
  detail: string;
  installDir: string;
  pid?: number | string | null;
  uptime?: string;
  channel?: string;
  octoState?: string;
  launcher?: string;
};

type DesktopUpdateStatus = {
  ok: boolean;
  status: string;
  localVersion?: string;
  latestVersion?: string | null;
  releaseUrl?: string | null;
  repo?: string;
  updateAvailable: boolean;
  canUpdate: boolean;
  gitBlocker?: string | null;
  updateCommand?: string;
  restartCommand?: string;
  detail: string;
};

type DesktopUpdateResult = {
  ok: boolean;
  installDir: string;
  detail: string;
  before?: DesktopUpdateStatus;
  after?: DesktopUpdateStatus;
  restarted?: boolean;
  error?: string;
};

type DesktopAppUpdateStatus = {
  ok: boolean;
  status: "idle" | "checking" | "available" | "not-available" | "downloading" | "downloaded" | "installing" | "error";
  currentVersion: string;
  latestVersion?: string;
  releaseName?: string;
  releaseDate?: string;
  detail: string;
  canDownload: boolean;
  canInstall: boolean;
  percent?: number;
  isPackaged: boolean;
  error?: string;
};

type DesktopPrerequisiteCheck = {
  id: string;
  label: string;
  ok: boolean;
  required: boolean;
  detail: string;
};

type DesktopWhatsAppLinkStatus = {
  ok: boolean;
  running: boolean;
  connected: boolean;
  linked: boolean;
  qr: string;
  terminal: string;
  self: string;
  detail: string;
};

type DesktopConnectorName = "google" | "github";

type DesktopConnectorStatusResult = {
  ok: boolean;
  connectors: Record<string, unknown>;
  detail: string;
};

type DesktopConnectorAuthPayload = {
  name: DesktopConnectorName;
  clientId?: string;
  clientSecret?: string;
  token?: string;
};

type DesktopConnectorActionResult = {
  ok: boolean;
  name: DesktopConnectorName;
  status?: string;
  message: string;
  detail: string;
};

type DesktopDashboardSnapshot = {
  ok: boolean;
  detail: string;
  generatedAt?: string;
  baseUrl?: string;
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
    recent: Array<{
      id?: string;
      template_name?: string;
      template_id?: string;
      status?: string;
      task?: string;
      updated_at?: string;
      summary?: string;
      error?: string;
      result_preview?: string;
    }>;
  };
  system?: {
    services: Array<{ id: string; name: string; status: string; reason: string }>;
    logs: Array<{ timestamp?: string; level?: string; service?: string; event?: string }>;
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

contextBridge.exposeInMainWorld("octopalDesktop", {
  loadSettings: () => ipcRenderer.invoke("desktop:load-settings") as Promise<DesktopSettings>,
  saveSettings: (settings: DesktopSettings) =>
    ipcRenderer.invoke("desktop:save-settings", settings) as Promise<DesktopSettings>,
  chooseInstallDir: () => ipcRenderer.invoke("desktop:choose-install-dir") as Promise<string | null>,
  closeWindow: () => ipcRenderer.invoke("desktop:window-control", "close") as Promise<void>,
  minimizeWindow: () => ipcRenderer.invoke("desktop:window-control", "minimize") as Promise<void>,
  toggleMaximizeWindow: () => ipcRenderer.invoke("desktop:window-control", "maximize") as Promise<void>,
  checkPrerequisites: () =>
    ipcRenderer.invoke("desktop:check-prerequisites") as Promise<DesktopPrerequisiteCheck[]>,
  getInstallState: () => ipcRenderer.invoke("desktop:get-install-state") as Promise<DesktopInstallState>,
  loadOctopalConfig: () => ipcRenderer.invoke("desktop:load-octopal-config") as Promise<unknown>,
  saveOctopalConfig: (config: unknown) =>
    ipcRenderer.invoke("desktop:save-octopal-config", config) as Promise<DesktopInstallState>,
  writeInstallPlan: (payload: unknown) =>
    ipcRenderer.invoke("desktop:write-install-plan", payload) as Promise<{ planPath: string }>,
  installOctopal: (payload: unknown) =>
    ipcRenderer.invoke("desktop:install-octopal", payload) as Promise<DesktopInstallResult>,
  startOctopal: (installDir: string) =>
    ipcRenderer.invoke("desktop:start-octopal", installDir) as Promise<DesktopStartResult | DesktopStartFailure>,
  stopOctopal: (installDir: string) =>
    ipcRenderer.invoke("desktop:stop-octopal", installDir) as Promise<DesktopStopResult | DesktopStopFailure>,
  getOctopalStatus: (installDir: string) =>
    ipcRenderer.invoke("desktop:get-octopal-status", installDir) as Promise<DesktopRuntimeStatus>,
  checkOctopalUpdate: (installDir: string) =>
    ipcRenderer.invoke("desktop:check-octopal-update", installDir) as Promise<DesktopUpdateStatus>,
  updateOctopal: (installDir: string) =>
    ipcRenderer.invoke("desktop:update-octopal", installDir) as Promise<DesktopUpdateResult>,
  getDashboardSnapshot: (installDir: string) =>
    ipcRenderer.invoke("desktop:get-dashboard-snapshot", installDir) as Promise<DesktopDashboardSnapshot>,
  getWorkerTemplates: (installDir: string) =>
    ipcRenderer.invoke("desktop:get-worker-templates", installDir) as Promise<DesktopWorkerTemplate[]>,
  saveWorkerTemplate: (installDir: string, template: DesktopWorkerTemplate, mode: "create" | "update") =>
    ipcRenderer.invoke("desktop:save-worker-template", installDir, template, mode) as Promise<DesktopWorkerTemplate>,
  deleteWorkerTemplate: (installDir: string, templateId: string) =>
    ipcRenderer.invoke("desktop:delete-worker-template", installDir, templateId) as Promise<void>,
  getAppUpdateStatus: () =>
    ipcRenderer.invoke("desktop:get-app-update-status") as Promise<DesktopAppUpdateStatus>,
  checkAppUpdate: () => ipcRenderer.invoke("desktop:check-app-update") as Promise<DesktopAppUpdateStatus>,
  downloadAppUpdate: () => ipcRenderer.invoke("desktop:download-app-update") as Promise<DesktopAppUpdateStatus>,
  installAppUpdate: () => ipcRenderer.invoke("desktop:install-app-update") as Promise<DesktopAppUpdateStatus>,
  getConnectorStatus: (installDir: string) =>
    ipcRenderer.invoke("desktop:get-connector-status", installDir) as Promise<DesktopConnectorStatusResult>,
  authorizeConnector: (installDir: string, payload: DesktopConnectorAuthPayload) =>
    ipcRenderer.invoke("desktop:authorize-connector", installDir, payload) as Promise<DesktopConnectorActionResult>,
  disconnectConnector: (installDir: string, name: DesktopConnectorName, forgetCredentials = false) =>
    ipcRenderer.invoke("desktop:disconnect-connector", installDir, name, forgetCredentials) as Promise<DesktopConnectorActionResult>,
  startWhatsAppLink: (installDir: string) =>
    ipcRenderer.invoke("desktop:start-whatsapp-link", installDir) as Promise<DesktopWhatsAppLinkStatus>,
  getWhatsAppLinkStatus: (installDir: string) =>
    ipcRenderer.invoke("desktop:get-whatsapp-link-status", installDir) as Promise<DesktopWhatsAppLinkStatus>,
  stopWhatsAppLink: (installDir: string) =>
    ipcRenderer.invoke("desktop:stop-whatsapp-link", installDir) as Promise<DesktopWhatsAppLinkStatus>,
  onInstallEvent: (callback: (event: DesktopInstallEvent) => void) => {
    const handler = (_event: Electron.IpcRendererEvent, installEvent: DesktopInstallEvent) => callback(installEvent);
    ipcRenderer.on("desktop:install-event", handler);
    return () => ipcRenderer.removeListener("desktop:install-event", handler);
  },
  onAppUpdateStatus: (callback: (status: DesktopAppUpdateStatus) => void) => {
    const handler = (_event: Electron.IpcRendererEvent, updateStatus: DesktopAppUpdateStatus) => callback(updateStatus);
    ipcRenderer.on("desktop:app-update-status", handler);
    return () => ipcRenderer.removeListener("desktop:app-update-status", handler);
  },
});
