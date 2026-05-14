/// <reference types="vite/client" />

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

type DesktopDashboardWorkerRun = {
  id?: string;
  template_name?: string;
  template_id?: string;
  status?: string;
  task?: string;
  updated_at?: string;
  summary?: string;
  error?: string;
  result_preview?: string;
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
    recent: DesktopDashboardWorkerRun[];
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

type OctopalDesktopApi = {
  loadSettings: () => Promise<{
    language: "en" | "fr" | "es" | "zh";
    theme: "light" | "dark" | "system";
    installDir: string;
  }>;
  saveSettings: (settings: {
    language: "en" | "fr" | "es" | "zh";
    theme: "light" | "dark" | "system";
    installDir: string;
  }) => Promise<{
    language: "en" | "fr" | "es" | "zh";
    theme: "light" | "dark" | "system";
    installDir: string;
  }>;
  chooseInstallDir: () => Promise<string | null>;
  closeWindow: () => Promise<void>;
  minimizeWindow: () => Promise<void>;
  toggleMaximizeWindow: () => Promise<void>;
  checkPrerequisites: () => Promise<DesktopPrerequisiteCheck[]>;
  getInstallState: () => Promise<DesktopInstallState>;
  loadOctopalConfig: () => Promise<unknown>;
  saveOctopalConfig: (config: unknown) => Promise<DesktopInstallState>;
  writeInstallPlan: (payload: unknown) => Promise<{ planPath: string }>;
  installOctopal: (payload: unknown) => Promise<DesktopInstallResult>;
  startOctopal: (installDir: string) => Promise<DesktopStartResult | DesktopStartFailure>;
  stopOctopal: (installDir: string) => Promise<DesktopStopResult | DesktopStopFailure>;
  getOctopalStatus: (installDir: string) => Promise<DesktopRuntimeStatus>;
  checkOctopalUpdate: (installDir: string) => Promise<DesktopUpdateStatus>;
  updateOctopal: (installDir: string) => Promise<DesktopUpdateResult>;
  getDashboardSnapshot: (installDir: string) => Promise<DesktopDashboardSnapshot>;
  getWorkerTemplates: (installDir: string) => Promise<DesktopWorkerTemplate[]>;
  saveWorkerTemplate: (
    installDir: string,
    template: DesktopWorkerTemplate,
    mode: "create" | "update",
  ) => Promise<DesktopWorkerTemplate>;
  deleteWorkerTemplate: (installDir: string, templateId: string) => Promise<void>;
  getAppUpdateStatus: () => Promise<DesktopAppUpdateStatus>;
  checkAppUpdate: () => Promise<DesktopAppUpdateStatus>;
  downloadAppUpdate: () => Promise<DesktopAppUpdateStatus>;
  installAppUpdate: () => Promise<DesktopAppUpdateStatus>;
  getConnectorStatus: (installDir: string) => Promise<DesktopConnectorStatusResult>;
  authorizeConnector: (installDir: string, payload: DesktopConnectorAuthPayload) => Promise<DesktopConnectorActionResult>;
  disconnectConnector: (
    installDir: string,
    name: DesktopConnectorName,
    forgetCredentials?: boolean,
  ) => Promise<DesktopConnectorActionResult>;
  startWhatsAppLink: (installDir: string) => Promise<DesktopWhatsAppLinkStatus>;
  getWhatsAppLinkStatus: (installDir: string) => Promise<DesktopWhatsAppLinkStatus>;
  stopWhatsAppLink: (installDir: string) => Promise<DesktopWhatsAppLinkStatus>;
  onInstallEvent: (callback: (event: DesktopInstallEvent) => void) => () => void;
  onAppUpdateStatus: (callback: (status: DesktopAppUpdateStatus) => void) => () => void;
};

interface Window {
  octopalDesktop?: OctopalDesktopApi;
}
