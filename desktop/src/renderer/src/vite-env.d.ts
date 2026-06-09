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
  status:
    | "idle"
    | "checking"
    | "available"
    | "not-available"
    | "downloading"
    | "downloaded"
    | "installing"
    | "error";
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

type DesktopCodexAuthStatus = {
  available: boolean;
  connected: boolean;
  accountLabel?: string;
  accountType?: string;
  requiresOpenAIAuth?: boolean;
  error?: string;
};

type DesktopCodexAuthStartResult = {
  success: boolean;
  authUrl?: string;
  loginId?: string;
  error?: string;
};

type DesktopCodexModelListResult = {
  success: boolean;
  models?: Array<{
    id: string;
    model: string;
    displayName: string;
    hidden?: boolean;
  }>;
  error?: string;
};

type DesktopDashboardWorkerRun = {
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
    recent: DesktopDashboardWorkerRun[];
  };
  system?: {
    services: Array<{
      id: string;
      name: string;
      status: string;
      reason: string;
    }>;
    mcpServers: Array<{
      id: string;
      name: string;
      status: string;
      reason: string;
      transport: string;
      toolCount: number;
      reconnectAttempts: number;
      error?: string;
    }>;
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

type DesktopChatEvent = Record<string, unknown> & {
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
  startOctopal: (
    installDir: string,
  ) => Promise<DesktopStartResult | DesktopStartFailure>;
  stopOctopal: (
    installDir: string,
  ) => Promise<DesktopStopResult | DesktopStopFailure>;
  getOctopalStatus: (installDir: string) => Promise<DesktopRuntimeStatus>;
  checkOctopalUpdate: (installDir: string) => Promise<DesktopUpdateStatus>;
  updateOctopal: (installDir: string) => Promise<DesktopUpdateResult>;
  getDashboardSnapshot: (
    installDir: string,
  ) => Promise<DesktopDashboardSnapshot>;
  openOctopalLogs: (installDir: string) => Promise<boolean>;
  getWorkerTemplates: (installDir: string) => Promise<DesktopWorkerTemplate[]>;
  getSkills: (installDir: string) => Promise<DesktopSkillsResponse>;
  installSkill: (
    installDir: string,
    payload: DesktopSkillInstallPayload,
  ) => Promise<DesktopSkill>;
  setSkillEnabled: (
    installDir: string,
    skillId: string,
    enabled: boolean,
  ) => Promise<DesktopSkill>;
  deleteSkill: (
    installDir: string,
    skillId: string,
  ) => Promise<DesktopSkillsResponse>;
  connectChat: (installDir: string) => Promise<DesktopChatConnectionStatus>;
  disconnectChat: () => Promise<DesktopChatConnectionStatus>;
  chooseChatFiles: (installDir: string) => Promise<DesktopChatAttachment[]>;
  savePastedChatImage: (
    installDir: string,
    image: DesktopPastedChatImage,
  ) => Promise<DesktopChatAttachment>;
  sendChatMessage: (payload: {
    text?: string;
    chatId?: number | null;
    attachments?: DesktopChatAttachment[];
  }) => Promise<{ ok: boolean }>;
  sendChatApprovalResponse: (
    intentId: string,
    approved: boolean,
  ) => Promise<{ ok: boolean }>;
  pingChat: () => Promise<{ ok: boolean }>;
  saveWorkerTemplate: (
    installDir: string,
    template: DesktopWorkerTemplate,
    mode: "create" | "update",
  ) => Promise<DesktopWorkerTemplate>;
  deleteWorkerTemplate: (
    installDir: string,
    templateId: string,
  ) => Promise<void>;
  getAppUpdateStatus: () => Promise<DesktopAppUpdateStatus>;
  checkAppUpdate: () => Promise<DesktopAppUpdateStatus>;
  downloadAppUpdate: () => Promise<DesktopAppUpdateStatus>;
  installAppUpdate: () => Promise<DesktopAppUpdateStatus>;
  getConnectorStatus: (
    installDir: string,
  ) => Promise<DesktopConnectorStatusResult>;
  authorizeConnector: (
    installDir: string,
    payload: DesktopConnectorAuthPayload,
  ) => Promise<DesktopConnectorActionResult>;
  disconnectConnector: (
    installDir: string,
    name: DesktopConnectorName,
    forgetCredentials?: boolean,
  ) => Promise<DesktopConnectorActionResult>;
  applyConnectorRuntime: (
    installDir: string,
    name: DesktopConnectorName,
  ) => Promise<DesktopConnectorActionResult>;
  getCodexAuthStatus: () => Promise<DesktopCodexAuthStatus>;
  startCodexAuth: () => Promise<DesktopCodexAuthStartResult>;
  disconnectCodexAuth: () => Promise<{ success: boolean; error?: string }>;
  listCodexModels: () => Promise<DesktopCodexModelListResult>;
  startWhatsAppLink: (installDir: string) => Promise<DesktopWhatsAppLinkStatus>;
  getWhatsAppLinkStatus: (
    installDir: string,
  ) => Promise<DesktopWhatsAppLinkStatus>;
  stopWhatsAppLink: (installDir: string) => Promise<DesktopWhatsAppLinkStatus>;
  onInstallEvent: (
    callback: (event: DesktopInstallEvent) => void,
  ) => () => void;
  onAppUpdateStatus: (
    callback: (status: DesktopAppUpdateStatus) => void,
  ) => () => void;
  onChatStatus: (
    callback: (status: DesktopChatConnectionStatus) => void,
  ) => () => void;
  onChatEvent: (callback: (event: DesktopChatEvent) => void) => () => void;
  onCodexAuthStatus: (
    callback: (status: DesktopCodexAuthStatus) => void,
  ) => () => void;
  onCodexAuthUpdated: (callback: () => void) => () => void;
};

interface Window {
  octopalDesktop?: OctopalDesktopApi;
}
