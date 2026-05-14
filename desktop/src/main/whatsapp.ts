import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, readFile, stat } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";

type WhatsAppConfig = {
  bridgeHost: string;
  bridgePort: number;
  authDir: string;
  nodeCommand: string;
  bridgeDir: string;
  stateDir: string;
};

export type WhatsAppLinkStatus = {
  ok: boolean;
  running: boolean;
  connected: boolean;
  linked: boolean;
  qr: string;
  terminal: string;
  self: string;
  detail: string;
};

type CommandResult = {
  stdout: string;
  stderr: string;
};

const linkProcesses = new Map<string, ChildProcessWithoutNullStreams>();

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function stringValue(value: unknown, fallback: string): string {
  return typeof value === "string" && value.trim() ? value : fallback;
}

function numberValue(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

function runCommand(command: string, args: string[], options: { cwd?: string; env?: NodeJS.ProcessEnv } = {}): Promise<CommandResult> {
  return new Promise((resolveResult, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env ?? process.env,
      shell: process.platform === "win32" && command === "npm",
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => reject(error));
    child.on("close", (code) => {
      if (code === 0) {
        resolveResult({ stdout, stderr });
        return;
      }
      reject(new Error(`${command} ${args.join(" ")} exited with code ${code}: ${(stderr || stdout).trim()}`));
    });
  });
}

async function readConfig(installDir: string): Promise<Record<string, unknown>> {
  const raw = await readFile(join(installDir, "config.json"), "utf8");
  return asRecord(JSON.parse(raw));
}

function resolveInstallPath(installDir: string, value: string): string {
  return isAbsolute(value) ? value : resolve(installDir, value);
}

async function resolveWhatsAppConfig(installDir: string): Promise<WhatsAppConfig> {
  if (!installDir || !(await pathExists(join(installDir, "pyproject.toml")))) {
    throw new Error("Install folder does not look like an Octopal checkout.");
  }

  const config = await readConfig(installDir);
  const whatsapp = asRecord(config.whatsapp);
  const storage = asRecord(config.storage);
  const stateDir = resolveInstallPath(installDir, stringValue(storage.state_dir, "data"));
  const authDir = resolveInstallPath(installDir, stringValue(whatsapp.auth_dir, join(stateDir, "whatsapp-auth")));
  const bridgeDir = join(installDir, "scripts", "whatsapp_bridge");
  return {
    bridgeHost: stringValue(whatsapp.bridge_host, "127.0.0.1"),
    bridgePort: numberValue(whatsapp.bridge_port, 8765),
    authDir,
    nodeCommand: stringValue(whatsapp.node_command, "node"),
    bridgeDir,
    stateDir,
  };
}

async function ensureBridgeReady(config: WhatsAppConfig): Promise<void> {
  if (!existsSync(join(config.bridgeDir, "package.json"))) {
    throw new Error(`WhatsApp bridge sources were not found at ${config.bridgeDir}.`);
  }
  if (!existsSync(join(config.bridgeDir, "node_modules", "@whiskeysockets", "baileys", "package.json"))) {
    await runCommand("npm", ["install"], { cwd: config.bridgeDir });
  }
}

async function fetchJson(url: string): Promise<Record<string, unknown>> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}`);
  }
  return asRecord(await response.json());
}

function linkBaseUrl(config: WhatsAppConfig): string {
  return `http://${config.bridgeHost}:${config.bridgePort}`;
}

async function waitUntilHttpReady(config: WhatsAppConfig): Promise<void> {
  const deadline = Date.now() + 20_000;
  let lastError = "";
  while (Date.now() < deadline) {
    try {
      await fetchJson(`${linkBaseUrl(config)}/status`);
      return;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      await new Promise((resolveTimer) => setTimeout(resolveTimer, 500));
    }
  }
  throw new Error(`WhatsApp bridge did not become ready: ${lastError || "timeout"}`);
}

function normalizeStatus(payload: Record<string, unknown>, detail = ""): WhatsAppLinkStatus {
  return {
    ok: true,
    running: true,
    connected: payload.connected === true,
    linked: payload.linked === true,
    qr: typeof payload.qr === "string" ? payload.qr : "",
    terminal: typeof payload.terminal === "string" ? payload.terminal : "",
    self: typeof payload.self === "string" ? payload.self : "",
    detail,
  };
}

function bridgeUnavailableStatus(detail: string): WhatsAppLinkStatus {
  return {
    ok: false,
    running: false,
    connected: false,
    linked: false,
    qr: "",
    terminal: "",
    self: "",
    detail,
  };
}

export async function startWhatsAppLink(installDir: string): Promise<WhatsAppLinkStatus> {
  const config = await resolveWhatsAppConfig(installDir);
  await ensureBridgeReady(config);

  const existing = await getWhatsAppLinkStatus(installDir).catch(() => null);
  if (existing?.running) {
    return existing;
  }

  await mkdir(config.authDir, { recursive: true });
  await mkdir(join(config.stateDir, "logs"), { recursive: true });

  const child = spawn(config.nodeCommand, ["bridge.mjs"], {
    cwd: config.bridgeDir,
    env: {
      ...process.env,
      OCTOPAL_WHATSAPP_BRIDGE_HOST: config.bridgeHost,
      OCTOPAL_WHATSAPP_BRIDGE_PORT: String(config.bridgePort),
      OCTOPAL_WHATSAPP_AUTH_DIR: config.authDir,
      OCTOPAL_WHATSAPP_CALLBACK_URL: "",
      OCTOPAL_WHATSAPP_CALLBACK_TOKEN: "",
    },
    shell: false,
    windowsHide: true,
  });

  linkProcesses.set(installDir, child);
  child.on("close", () => {
    if (linkProcesses.get(installDir) === child) {
      linkProcesses.delete(installDir);
    }
  });

  await waitUntilHttpReady(config);
  return getWhatsAppLinkStatus(installDir);
}

export async function getWhatsAppLinkStatus(installDir: string): Promise<WhatsAppLinkStatus> {
  const config = await resolveWhatsAppConfig(installDir);
  let status: Record<string, unknown>;
  try {
    status = await fetchJson(`${linkBaseUrl(config)}/status`);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    return bridgeUnavailableStatus(`WhatsApp bridge is not running yet. ${detail}`);
  }

  if (status.linked === true || status.connected === true) {
    return normalizeStatus(status);
  }

  const terminal = await fetchJson(`${linkBaseUrl(config)}/qr-terminal`).catch(() => ({}));
  return normalizeStatus({ ...status, ...terminal });
}

export async function stopWhatsAppLink(installDir: string): Promise<WhatsAppLinkStatus> {
  const child = linkProcesses.get(installDir);
  if (child && child.exitCode === null) {
    child.kill();
  }
  linkProcesses.delete(installDir);
  return {
    ok: true,
    running: false,
    connected: false,
    linked: false,
    qr: "",
    terminal: "",
    self: "",
    detail: "WhatsApp link bridge stopped.",
  };
}
