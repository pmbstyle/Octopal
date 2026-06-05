import { useEffect, useState } from "react";

import { AppShell } from "./AppShell";
import { DashboardScreen } from "./DashboardScreen";
import type { Theme } from "../lib/appTypes";
import { t, type Language } from "../lib/i18n";

const now = new Date("2026-06-04T18:24:00.000Z");

const previewTemplates: DesktopWorkerTemplate[] = [
  {
    id: "research",
    name: "Research Analyst",
    description: "Explores current context, summarizes evidence, and reports concise next steps.",
    system_prompt: "You are a careful research worker.",
    available_tools: ["web.search", "filesystem.read", "memory.query", "report.write"],
    required_permissions: ["network", "workspace_read"],
    model: "gpt-5.5",
    max_thinking_steps: 12,
    default_timeout_seconds: 420,
    can_spawn_children: true,
    allowed_child_templates: ["writer", "qa"],
  },
  {
    id: "qa",
    name: "QA Runner",
    description: "Runs checks, captures failures, and keeps reproducible notes.",
    system_prompt: "You are a pragmatic QA worker.",
    available_tools: ["shell", "browser", "screenshot", "logs"],
    required_permissions: ["workspace_read", "workspace_write"],
    model: "gpt-5.5",
    max_thinking_steps: 10,
    default_timeout_seconds: 360,
    can_spawn_children: false,
    allowed_child_templates: [],
  },
];

const longPreview =
  "Running a cross-page interface QA pass. Checking desktop dashboard pages, scroll containment, worker detail modal overflow, connector forms, and dense table layouts after the shadcn visual pass.";

const previewSnapshot: DesktopDashboardSnapshot = {
  ok: true,
  detail: "Preview dashboard is using mocked runtime data.",
  generatedAt: now.toISOString(),
  baseUrl: "http://127.0.0.1:8010",
  dashboardEnabled: true,
  load: {
    activeWorkers: 3,
    queueDepth: 5,
    octoQueue: 2,
  },
  octo: {
    state: "thinking",
    headline: "Octopal is running",
    detail: "PID 4157 · uptime 10:24:33 · channel Telegram",
    latestAction: longPreview,
  },
  workers: {
    recent: [
      {
        id: "worker-qa-scroll-0001",
        template_name: "QA Runner",
        template_id: "qa",
        status: "running",
        task: longPreview,
        created_at: new Date(now.getTime() - 13 * 60_000).toISOString(),
        updated_at: now.toISOString(),
        summary: "",
        result_preview: "Inspecting worker detail scroll behavior and dense dashboard sections.",
        tools_used: [
          "browser.open",
          "browser.screenshot",
          "dom.measure",
          "css.inspect",
          "browser.scroll",
          "build.run",
          "build.run",
          "dom.measure",
          "screenshot.compare",
        ],
        parent_worker_id: null,
        lineage_id: "lineage-ui-qa-20260604",
        spawn_depth: 0,
        output: {
          pages_checked: ["Control", "Workers", "Skills", "Connectors", "System"],
          issues: [
            "Worker detail body must scroll independently.",
            "Buttons should not overflow in compact panels.",
            "Cards should stay inside dashboard content bounds.",
          ],
          long_note:
            "This intentionally long structured output gives the modal enough content to prove that scrolling works in the main and side columns without pushing the dialog outside the viewport.",
        },
        template_config: {
          model: "gpt-5.5",
          max_thinking_steps: 10,
          default_timeout_seconds: 360,
          available_tools: [
            "browser.open",
            "browser.screenshot",
            "dom.measure",
            "css.inspect",
            "browser.scroll",
            "build.run",
            "logs.read",
            "shell.exec",
            "artifact.capture",
            "issue.report",
          ],
          can_spawn_children: false,
        },
        audit_timeline: Array.from({ length: 18 }, (_, index) => ({
          id: `qa-event-${index + 1}`,
          ts: new Date(now.getTime() - (18 - index) * 42_000).toISOString(),
          level: index % 7 === 0 ? "warning" : "info",
          event_type: index % 7 === 0 ? "layout_warning" : "dom_check",
          data_preview:
            index % 7 === 0
              ? "Detected a possible overflow condition; verifying scroll container metrics before reporting."
              : `Checked layout segment ${index + 1}: cards, tables, dialogs, and action rows remain inside their containers.`,
        })),
      },
      {
        id: "worker-research-0002",
        template_name: "Research Analyst",
        template_id: "research",
        status: "completed",
        task: "Summarize connector readiness and MCP server health.",
        created_at: new Date(now.getTime() - 42 * 60_000).toISOString(),
        updated_at: new Date(now.getTime() - 34 * 60_000).toISOString(),
        summary: "Connectors are configured and runtime MCP servers are visible.",
        tools_used: ["filesystem.read", "report.write"],
        lineage_id: "lineage-connectors",
        spawn_depth: 1,
      },
      {
        id: "worker-docs-0003",
        template_name: "Documentation Writer",
        template_id: "writer",
        status: "awaiting_instruction",
        task: "Prepare release notes draft for the desktop UI refresh.",
        created_at: new Date(now.getTime() - 75 * 60_000).toISOString(),
        updated_at: new Date(now.getTime() - 61 * 60_000).toISOString(),
        result_preview: "Waiting for final QA findings.",
        tools_used: ["report.write"],
        lineage_id: "lineage-release-notes",
        spawn_depth: 1,
      },
    ],
  },
  system: {
    services: [
      { id: "octo", name: "Octo", status: "running", reason: "Runtime loop active." },
      { id: "gateway", name: "Gateway", status: "running", reason: "FastAPI gateway reachable." },
      { id: "workers", name: "Workers", status: "warning", reason: "One run is still active." },
      { id: "memory", name: "Memory", status: "ok", reason: "SQLite store is available." },
    ],
    mcpServers: [
      {
        id: "google-gmail",
        name: "Gmail Connector",
        status: "connected",
        reason: "Gmail connector is ready.",
        transport: "stdio",
        toolCount: 21,
        reconnectAttempts: 0,
      },
      {
        id: "google-calendar",
        name: "Google Calendar Connector",
        status: "connected",
        reason: "Calendar connector is ready.",
        transport: "stdio",
        toolCount: 8,
        reconnectAttempts: 0,
      },
      {
        id: "google-drive",
        name: "Google Drive Connector",
        status: "connected",
        reason: "Drive connector is ready.",
        transport: "stdio",
        toolCount: 10,
        reconnectAttempts: 0,
      },
      {
        id: "github-core",
        name: "GitHub Connector",
        status: "connected",
        reason: "Repository tools are ready.",
        transport: "stdio",
        toolCount: 9,
        reconnectAttempts: 0,
      },
    ],
    logs: Array.from({ length: 12 }, (_, index) => ({
      timestamp: new Date(now.getTime() - index * 90_000).toISOString(),
      level: index % 5 === 0 ? "warning" : "info",
      service: index % 5 === 0 ? "workers" : "gateway",
      event:
        index % 5 === 0
          ? "worker queue pressure changed"
          : "dashboard snapshot emitted",
    })),
  },
};

const previewSkills: DesktopSkillsResponse = {
  contract_version: "1",
  count: 3,
  registry_path: "/preview/octopal/workspace/skills",
  install: {
    supported_sources: ["clawhub", "git", "path"],
    default_clawhub_site: "https://clawhub.dev",
  },
  skills: [
    {
      id: "desktop-qa",
      name: "Desktop QA",
      description: "Checks UI surfaces, screenshots, overflow, and keyboard-accessible flows.",
      scope: "both",
      enabled: true,
      ready: true,
      status: "ready",
      reasons: [],
      origin: "installed",
      source: {
        kind: "path",
        label: "/preview/skills/desktop-qa",
        path: "/preview/skills/desktop-qa",
        installer_managed: true,
        auto_discovered: false,
      },
      trust: {
        trusted: true,
        has_scripts: true,
        scan_status: "clean",
        scan_findings_count: 0,
      },
      runtime: {
        kind: "node",
        required: true,
        recommended: true,
        prepared: true,
        next_step: "",
      },
      requirements: { missing_bins: [], missing_env: [], missing_config: [] },
      actions: { can_enable: false, can_disable: true, can_remove: true, can_install: false },
    },
    {
      id: "release-notes",
      name: "Release Notes",
      description: "Drafts concise operator-facing release notes from checked changes.",
      scope: "octo",
      enabled: true,
      ready: false,
      status: "needs setup",
      reasons: ["Missing RELEASE_CHANNEL"],
      origin: "workspace",
      source: {
        kind: "path",
        label: "/preview/skills/release-notes",
        path: "/preview/skills/release-notes",
        installer_managed: false,
        auto_discovered: true,
      },
      trust: {
        trusted: true,
        has_scripts: false,
        scan_status: "clean",
        scan_findings_count: 0,
      },
      runtime: {
        kind: "python",
        required: false,
        recommended: true,
        prepared: false,
        next_step: "Set RELEASE_CHANNEL to enable publishing checks.",
      },
      requirements: {
        missing_bins: [],
        missing_env: ["RELEASE_CHANNEL"],
        missing_config: [],
      },
      actions: { can_enable: true, can_disable: true, can_remove: true, can_install: false },
    },
  ],
};

const previewChatHistory: DesktopChatEvent = {
  type: "chat_history",
  messages: [
    {
      id: "preview-chat-user-1",
      type: "chat_message",
      role: "user",
      direction: "outbound",
      channel: "desktop",
      created_at: new Date(now.getTime() - 5 * 60_000).toISOString(),
      text: "Can we close the Mini Shai-Hulu investigation?",
    },
    {
      id: "preview-chat-assistant-1",
      type: "chat_message",
      role: "assistant",
      direction: "inbound",
      channel: "desktop",
      created_at: new Date(now.getTime() - 3 * 60_000).toISOString(),
      text: "Chrome extension manifests are missing or empty. I will check Brave separately.",
    },
    {
      id: "preview-chat-user-2",
      type: "chat_message",
      role: "user",
      direction: "outbound",
      channel: "desktop",
      created_at: new Date(now.getTime() - 45_000).toISOString(),
      text: "Find the current water temperature near Port Colborne.",
    },
  ],
};

const previewChatActivities: DesktopChatEvent[] = [
  {
    type: "progress",
    state: "tool_start",
    text: "Octo using web_research",
    meta: { tool_name: "web_research" },
  },
  {
    type: "progress",
    state: "tool_start",
    text: "Octo checking latest lake conditions",
    meta: { tool_name: "get_worker_result" },
  },
];

const previewChatReply: DesktopChatEvent = {
  id: "preview-chat-assistant-2",
  type: "chat_message",
  role: "assistant",
  direction: "inbound",
  channel: "desktop",
  created_at: new Date(now.getTime()).toISOString(),
  text: "The latest nearby reading is from the Port Colborne shoreline station.\n\n| Location | Peak temp | Season |\n| --- | ---: | --- |\n| Erie (Port Colborne) | 24-26C | July-August |\n| Ontario | 22-25C | July-August |\n\nI can pull a fresher station if you want a narrower location.",
};

function installPreviewDesktopApi() {
  if (window.octopalDesktop) {
    return;
  }

  window.octopalDesktop = {
    loadSettings: async () => ({ language: "en", theme: "dark", installDir: "/preview/octopal" }),
    saveSettings: async (settings) => settings,
    chooseInstallDir: async () => "/preview/octopal",
    closeWindow: async () => undefined,
    minimizeWindow: async () => undefined,
    toggleMaximizeWindow: async () => undefined,
    checkPrerequisites: async () => [],
    getInstallState: async () => ({
      installed: true,
      installDir: "/preview/octopal",
      configPath: "/preview/octopal/config.json",
      planPath: "/preview/octopal/install-plan.json",
    }),
    loadOctopalConfig: async () => ({
      user_channel: "telegram",
      gateway: { webapp_enabled: true, port: 8010, dashboard_token: "preview" },
      connectors: {
        instances: {
          google: {
            enabled: true,
            enabled_services: ["gmail", "calendar", "drive"],
            credentials: { client_id: "preview-client", client_secret: "configured" },
          },
          github: {
            enabled: true,
            enabled_services: ["repos", "issues", "pull_requests"],
            auth: { access_token: "configured" },
          },
        },
      },
    }),
    saveOctopalConfig: async () => ({
      installed: true,
      installDir: "/preview/octopal",
      configPath: "/preview/octopal/config.json",
      planPath: "/preview/octopal/install-plan.json",
    }),
    writeInstallPlan: async () => ({ planPath: "/preview/octopal/install-plan.json" }),
    installOctopal: async () => ({
      installDir: "/preview/octopal",
      releaseTag: "preview",
      configPath: "/preview/octopal/config.json",
      planPath: "/preview/octopal/install-plan.json",
    }),
    startOctopal: async () => ({ ok: true, installDir: "/preview/octopal", detail: "Started" }),
    stopOctopal: async () => ({ ok: true, installDir: "/preview/octopal", detail: "Stopped" }),
    getOctopalStatus: async () => ({
      ok: true,
      state: "running",
      title: "Octopal is running",
      detail: "Preview runtime",
      installDir: "/preview/octopal",
    }),
    checkOctopalUpdate: async () => ({
      ok: true,
      status: "current",
      updateAvailable: false,
      canUpdate: true,
      detail: "No update available.",
    }),
    updateOctopal: async () => ({
      ok: true,
      installDir: "/preview/octopal",
      detail: "Already current.",
    }),
    getDashboardSnapshot: async () => previewSnapshot,
    openOctopalLogs: async () => true,
    getWorkerTemplates: async () => previewTemplates,
    getSkills: async () => previewSkills,
    installSkill: async () => previewSkills.skills[0],
    setSkillEnabled: async (_installDir, skillId, enabled) => ({
      ...(previewSkills.skills.find((skill) => skill.id === skillId) ?? previewSkills.skills[0]),
      enabled,
    }),
    deleteSkill: async () => previewSkills,
    connectChat: async () => ({ ok: true, state: "connected", detail: "Preview chat connected." }),
    disconnectChat: async () => ({ ok: true, state: "disconnected", detail: "Preview chat disconnected." }),
    chooseChatFiles: async () => [],
    savePastedChatImage: async () => ({
      path: "/preview/image.png",
      name: "image.png",
      sizeBytes: 1024,
    }),
    sendChatMessage: async () => ({ ok: true }),
    sendChatApprovalResponse: async () => ({ ok: true }),
    pingChat: async () => ({ ok: true }),
    saveWorkerTemplate: async (_installDir, template) => template,
    deleteWorkerTemplate: async () => undefined,
    getAppUpdateStatus: async () => ({
      ok: true,
      status: "not-available",
      currentVersion: "preview",
      detail: "No app update available.",
      canDownload: false,
      canInstall: false,
      isPackaged: false,
    }),
    checkAppUpdate: async () => ({
      ok: true,
      status: "not-available",
      currentVersion: "preview",
      detail: "No app update available.",
      canDownload: false,
      canInstall: false,
      isPackaged: false,
    }),
    downloadAppUpdate: async () => ({
      ok: true,
      status: "not-available",
      currentVersion: "preview",
      detail: "No app update available.",
      canDownload: false,
      canInstall: false,
      isPackaged: false,
    }),
    installAppUpdate: async () => ({
      ok: true,
      status: "not-available",
      currentVersion: "preview",
      detail: "No app update available.",
      canDownload: false,
      canInstall: false,
      isPackaged: false,
    }),
    getConnectorStatus: async () => ({
      ok: false,
      detail: "Connector status command did not return JSON in preview.",
      connectors: {},
    }),
    authorizeConnector: async (_installDir, payload) => ({
      ok: true,
      name: payload.name,
      status: "ready",
      message: "Authorized.",
      detail: "Authorized.",
    }),
    disconnectConnector: async (_installDir, name) => ({
      ok: true,
      name,
      status: "disconnected",
      message: "Disconnected.",
      detail: "Disconnected.",
    }),
    applyConnectorRuntime: async (_installDir, name) => ({
      ok: true,
      name,
      status: "ready",
      message: "Runtime updated.",
      detail: "Runtime updated.",
    }),
    getCodexAuthStatus: async () => ({ available: true, connected: true, accountLabel: "Preview" }),
    startCodexAuth: async () => ({ success: true }),
    disconnectCodexAuth: async () => ({ success: true }),
    listCodexModels: async () => ({ success: true, models: [] }),
    startWhatsAppLink: async () => ({
      ok: true,
      running: false,
      connected: false,
      linked: false,
      qr: "",
      terminal: "",
      self: "",
      detail: "Preview",
    }),
    getWhatsAppLinkStatus: async () => ({
      ok: true,
      running: false,
      connected: false,
      linked: false,
      qr: "",
      terminal: "",
      self: "",
      detail: "Preview",
    }),
    stopWhatsAppLink: async () => ({
      ok: true,
      running: false,
      connected: false,
      linked: false,
      qr: "",
      terminal: "",
      self: "",
      detail: "Preview",
    }),
    onInstallEvent: () => () => undefined,
    onAppUpdateStatus: () => () => undefined,
    onChatStatus: () => () => undefined,
    onChatEvent: (callback) => {
      window.setTimeout(() => callback(previewChatHistory), 0);
      previewChatActivities.forEach((event, index) => {
        window.setTimeout(() => callback(event), 450 + index * 900);
      });
      window.setTimeout(() => callback(previewChatReply), 4200);
      return () => undefined;
    },
    onCodexAuthStatus: () => () => undefined,
    onCodexAuthUpdated: () => () => undefined,
  };
}

export function DesignPreview() {
  const [previewLanguage, setPreviewLanguage] = useState<Language>("en");
  const [previewTheme, setPreviewTheme] = useState<Theme>("dark");

  installPreviewDesktopApi();

  useEffect(() => {
    document.documentElement.dataset.theme =
      previewTheme === "system"
        ? window.matchMedia("(prefers-color-scheme: dark)").matches
          ? "dark"
          : "light"
        : previewTheme;
  }, [previewTheme]);

  return (
    <AppShell
      title="Octopal Desktop"
      onClose={() => undefined}
      onMinimize={() => undefined}
      onMaximize={() => undefined}
    >
      <DashboardScreen
        copy={(key) => t(previewLanguage, key)}
        language={previewLanguage}
        theme={previewTheme}
        installDir="/preview/octopal"
        runtimeView={{
          state: "running",
          title: "Octopal is running",
          detail: "Local runtime is available and accepting work.",
        }}
        updateAvailable={false}
        updateBlocked={false}
        updateBusy={false}
        desktopUpdateAvailable={false}
        desktopUpdateReady={false}
        desktopUpdateBusy={false}
        onStart={() => undefined}
        onStop={() => undefined}
        onRestart={() => undefined}
        onUpdateOctopal={() => undefined}
        onUpdateDesktopApp={() => undefined}
        onLanguageChange={setPreviewLanguage}
        onThemeChange={setPreviewTheme}
      />
    </AppShell>
  );
}
