import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { copyFile, mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { delimiter, dirname, isAbsolute, join } from "node:path";

const REPO_URL = "https://github.com/pmbstyle/Octopal.git";
const LATEST_RELEASE_API_URL = "https://api.github.com/repos/pmbstyle/Octopal/releases/latest";

export type InstallEvent = {
  kind: "step" | "log" | "warning" | "error" | "done";
  message: string;
  detail?: string;
};

export type InstallPayload = {
  createdBy: string;
  createdAt: string;
  installDir: string;
  octopalConfig: unknown;
};

export type InstallResult = {
  installDir: string;
  releaseTag: string;
  configPath: string;
  planPath: string;
};

export type StartResult = {
  ok: true;
  installDir: string;
  detail: string;
};

export type StartFailure = {
  ok: false;
  error: string;
  detail: string;
};

export type StopResult = {
  ok: true;
  installDir: string;
  detail: string;
};

export type StopFailure = {
  ok: false;
  error: string;
  detail: string;
};

export type RuntimeStatusState = "running" | "stopped" | "error";

export type RuntimeStatusResult = {
  ok: boolean;
  state: RuntimeStatusState;
  title: string;
  detail: string;
  installDir: string;
  pid?: number | string | null;
  uptime?: string;
  channel?: string;
  octoState?: string;
  launcher?: string;
};

export type UpdateStatusResult = {
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

export type UpdateResult = {
  ok: boolean;
  installDir: string;
  detail: string;
  before?: UpdateStatusResult;
  after?: UpdateStatusResult;
  restarted?: boolean;
  error?: string;
};

type CommandResult = {
  stdout: string;
  stderr: string;
};

type RunOptions = {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  quiet?: boolean;
};

type DetachedStartResult = {
  stdout: string;
  stderr: string;
  exited: boolean;
  code: number | null;
};

function sanitizeOutput(text: string): string {
  return text
    .replace(/\bGOCSPX-[A-Za-z0-9_-]{12,}\b/g, "[redacted-key]")
    .replace(/\b\d{7,12}:[A-Za-z0-9_-]{20,}\b/g, "[redacted-token]")
    .replace(/\bsk-or-v1-[A-Za-z0-9_-]{16,}\b/g, "[redacted-key]")
    .replace(/\bsk-[A-Za-z0-9_-]{16,}\b/g, "[redacted-key]")
    .replace(/\bBS[A-Za-z0-9_-]{20,}\b/g, "[redacted-key]")
    .replace(
      /((?:api[_-]?key|bot[_-]?token|callback[_-]?token|telegram[_-]?bot[_-]?token|secret|token)\s*=\s*')[^']*(')/gi,
      "$1[redacted]$2",
    )
    .replace(
      /((?:api[_-]?key|bot[_-]?token|callback[_-]?token|telegram[_-]?bot[_-]?token|secret|token)"?\s*:\s*")[^"]*(")/gi,
      "$1[redacted]$2",
    );
}

function sanitizeCommandInvocation(command: string, args: string[]): string {
  const sensitiveFlags = new Set([
    "--api-key",
    "--bot-token",
    "--callback-token",
    "--client-secret",
    "--secret",
    "--telegram-token",
    "--token",
  ]);
  const safeArgs = args.map((arg, index) => {
    if (index > 0 && sensitiveFlags.has(args[index - 1])) {
      return "[redacted]";
    }
    const [flag, value] = arg.split("=", 2);
    if (value !== undefined && sensitiveFlags.has(flag)) {
      return `${flag}=[redacted]`;
    }
    return arg;
  });
  return sanitizeOutput([command, ...safeArgs].join(" "));
}

export function withPythonDesktopEnv(env: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  return {
    ...withLocalToolPaths(env),
    COLUMNS: "20000",
    FORCE_COLOR: "0",
    NO_COLOR: "1",
    PYTHONIOENCODING: "utf-8",
    PYTHONUTF8: "1",
  };
}

function emitStep(emit: (event: InstallEvent) => void, message: string, detail?: string) {
  emit({ kind: "step", message, detail });
}

function emitWarning(emit: (event: InstallEvent) => void, message: string, detail?: string) {
  emit({ kind: "warning", message, detail });
}

function getPathValue(env: NodeJS.ProcessEnv): string {
  return env.Path ?? env.PATH ?? "";
}

function withLocalToolPaths(env: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const home = homedir();
  const extraPaths =
    process.platform === "win32"
      ? [join(home, ".local", "bin"), join(home, ".cargo", "bin")]
      : [join(home, ".local", "bin"), join(home, ".cargo", "bin")];
  const pathKey = process.platform === "win32" ? "Path" : "PATH";
  return {
    ...env,
    [pathKey]: [...extraPaths, getPathValue(env)].filter(Boolean).join(delimiter),
  };
}

function getUvCandidates(): string[] {
  const home = homedir();
  return process.platform === "win32"
    ? [join(home, ".local", "bin", "uv.exe"), join(home, ".cargo", "bin", "uv.exe")]
    : [join(home, ".local", "bin", "uv"), join(home, ".cargo", "bin", "uv")];
}

function commandUsesShell(command: string): boolean {
  return process.platform === "win32" && command === "npm";
}

export function runCommand(
  command: string,
  args: string[],
  emit: (event: InstallEvent) => void,
  options: RunOptions = {},
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env ?? withLocalToolPaths(),
      shell: commandUsesShell(command),
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";

    child.stdout?.on("data", (chunk: Buffer) => {
      const text = sanitizeOutput(chunk.toString());
      stdout += text;
      if (!options.quiet) {
        emit({ kind: "log", message: text.trim() });
      }
    });

    child.stderr?.on("data", (chunk: Buffer) => {
      const text = sanitizeOutput(chunk.toString());
      stderr += text;
      if (!options.quiet) {
        emit({ kind: "log", message: text.trim() });
      }
    });

    child.on("error", (error) => reject(error));
    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }
      reject(new Error(`${sanitizeCommandInvocation(command, args)} exited with code ${code}: ${sanitizeOutput(stderr || stdout).trim()}`));
    });
  });
}

function extractProcessIds(text: string): number[] {
  const ids = new Set<number>();
  for (const match of text.matchAll(/\bPID\s+(\d+)\b/gi)) {
    ids.add(Number(match[1]));
  }

  const targetList = text.match(/\bprocess(?:\(es\))?\):\s*([0-9,\s]+)/i)?.[1] ?? "";
  for (const match of targetList.matchAll(/\d+/g)) {
    ids.add(Number(match[0]));
  }

  return [...ids].filter((id) => Number.isInteger(id) && id > 0).sort((left, right) => left - right);
}

function commandExecutableName(commandLine: string): string {
  const stripped = commandLine.trim();
  if (!stripped) {
    return "";
  }
  let token = "";
  if (stripped.startsWith("\"") || stripped.startsWith("'")) {
    const quote = stripped[0];
    const end = stripped.indexOf(quote, 1);
    token = end > 1 ? stripped.slice(1, end) : stripped.slice(1);
  } else {
    token = stripped.split(/\s+/, 1)[0] ?? "";
  }
  return token.replace(/\\/g, "/").split("/").pop()?.toLowerCase() ?? "";
}

function looksLikeOctopalRuntimeCommand(commandLine: string): boolean {
  const lowered = commandLine.toLowerCase();
  const executable = commandExecutableName(commandLine);
  if (!["octopal", "octopal.exe", "python", "python.exe", "python3", "python3.exe", "pythonw.exe"].includes(executable)) {
    return false;
  }
  if (lowered.includes("uv run octopal start") && !lowered.includes("--foreground")) {
    return false;
  }
  if (lowered.includes("octopal.cli start")) {
    return true;
  }
  if (` ${lowered}`.includes(" octopal start --foreground")) {
    return true;
  }
  return lowered.includes(" -m octopal.cli start");
}

function processIdValue(value: unknown): number | null {
  const pid = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  return Number.isInteger(pid) && pid > 0 ? pid : null;
}

async function listWindowsOctopalRuntimePids(): Promise<number[]> {
  if (process.platform !== "win32") {
    return [];
  }

  const { stdout } = await runCommand(
    "powershell.exe",
    [
      "-NoProfile",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      "Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress",
    ],
    () => undefined,
    { env: withPythonDesktopEnv(), quiet: true },
  );

  const parsed: unknown = JSON.parse(stdout.trim() || "[]");
  const rows = Array.isArray(parsed) ? parsed : parsed ? [parsed] : [];
  const pids = new Set<number>();
  for (const row of rows) {
    const record = asRecord(row);
    const pid = processIdValue(record.ProcessId);
    const commandLine = typeof record.CommandLine === "string" ? record.CommandLine : "";
    if (pid && commandLine && looksLikeOctopalRuntimeCommand(commandLine)) {
      pids.add(pid);
    }
  }

  return [...pids].sort((left, right) => left - right);
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitForProcessesToExit(pids: number[], timeoutMs = 5000): Promise<number[]> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const alive = pids.filter(isProcessAlive);
    if (alive.length === 0) {
      return [];
    }
    await sleep(200);
  }
  return pids.filter(isProcessAlive);
}

async function forceStopWindowsProcesses(pids: number[]): Promise<string> {
  const details: string[] = [];
  const failures: string[] = [];
  const uniquePids = [...new Set(pids)].filter((pid) => Number.isInteger(pid) && pid > 0);

  for (const pid of uniquePids) {
    try {
      await runCommand("taskkill", ["/T", "/F", "/PID", String(pid)], () => undefined, {
        env: withPythonDesktopEnv(),
        quiet: true,
      });
      details.push(`Stopped PID ${pid} with taskkill.`);
      continue;
    } catch (taskkillError) {
      if (!isProcessAlive(pid)) {
        details.push(`PID ${pid} is already stopped.`);
        continue;
      }
      failures.push(`taskkill PID ${pid}: ${taskkillError instanceof Error ? taskkillError.message : String(taskkillError)}`);
    }

    try {
      await runCommand(
        "powershell.exe",
        ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", `Stop-Process -Id ${pid} -Force -ErrorAction Stop`],
        () => undefined,
        { env: withPythonDesktopEnv(), quiet: true },
      );
      details.push(`Stopped PID ${pid} with Stop-Process.`);
    } catch (powershellError) {
      failures.push(
        `Stop-Process PID ${pid}: ${powershellError instanceof Error ? powershellError.message : String(powershellError)}`,
      );
    }
  }

  const alive = await waitForProcessesToExit(uniquePids);
  if (alive.length > 0) {
    throw new Error(
      [`Native Windows stop fallback could not stop PID(s): ${alive.join(", ")}.`, ...failures].join("\n"),
    );
  }

  return details.join("\n");
}

function runDetachedStart(command: string, args: string[], options: RunOptions = {}): Promise<DetachedStartResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      detached: true,
      env: options.env ?? withPythonDesktopEnv(),
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";
    let settled = false;

    const finish = (result: DetachedStartResult) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      resolve(result);
    };

    const timer = setTimeout(() => {
      child.unref();
      finish({ stdout, stderr, exited: false, code: null });
    }, 3500);

    child.stdout?.on("data", (chunk: Buffer) => {
      stdout += sanitizeOutput(chunk.toString());
    });

    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += sanitizeOutput(chunk.toString());
    });

    child.on("error", (error) => {
      if (!settled) {
        clearTimeout(timer);
        reject(error);
      }
    });

    child.on("close", (code) => {
      finish({ stdout, stderr, exited: true, code });
    });
  });
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function getRecentError(snapshot: Record<string, unknown>): string {
  const logs = Array.isArray(snapshot.logs) ? snapshot.logs : [];
  const latestEntry = asRecord(logs[logs.length - 1]);
  const latestLevel = String(latestEntry.level ?? "").toLowerCase();
  return ["error", "critical"].includes(latestLevel) ? String(latestEntry.event ?? "").trim() : "";
}

function statusFromDashboardSnapshot(snapshot: unknown, installDir: string): RuntimeStatusResult {
  const root = asRecord(snapshot);
  const system = asRecord(root.system);
  const octo = asRecord(root.octo);
  const launcher = asRecord(system.worker_launcher);
  const running = system.running === true;
  const pid = system.pid as number | string | null | undefined;
  const channel = typeof system.active_channel === "string" ? system.active_channel : "";
  const uptime = typeof system.uptime === "string" ? system.uptime : "";
  const octoState = typeof octo.state === "string" ? octo.state : "";
  const launcherReason = typeof launcher.reason === "string" ? launcher.reason : "";
  const recentError = getRecentError(root);

  if (running) {
    const detailParts = [
      pid ? `PID ${pid}` : "",
      uptime && uptime !== "N/A" ? `uptime ${uptime}` : "",
      channel ? `channel ${channel}` : "",
      octoState ? `Octo ${octoState}` : "",
    ].filter(Boolean);
    return {
      ok: true,
      state: "running",
      title: "Octopal is running",
      detail: detailParts.join(" · ") || "Runtime is active.",
      installDir,
      pid,
      uptime,
      channel,
      octoState,
      launcher: launcherReason,
    };
  }

  if (recentError) {
    return {
      ok: true,
      state: "error",
      title: "Octopal stopped with an error",
      detail: recentError,
      installDir,
      pid,
      uptime,
      channel,
      octoState,
      launcher: launcherReason,
    };
  }

  return {
    ok: true,
    state: "stopped",
    title: "Octopal is stopped",
    detail: "Runtime is not running.",
    installDir,
    pid,
    uptime,
    channel,
    octoState,
    launcher: launcherReason,
  };
}

async function windowsNativeRuntimeStatus(installDir: string): Promise<RuntimeStatusResult | null> {
  if (process.platform !== "win32") {
    return null;
  }

  const pids = await listWindowsOctopalRuntimePids().catch(() => []);
  const pid = pids[0] ?? null;
  if (pid) {
    return {
      ok: true,
      state: "running",
      title: "Octopal is running",
      detail: `PID ${pid}`,
      installDir,
      pid,
    };
  }

  return {
    ok: true,
    state: "stopped",
    title: "Octopal is stopped",
    detail: "Runtime is not running.",
    installDir,
    pid: null,
  };
}

async function reconcileWindowsRuntimeStatus(status: RuntimeStatusResult): Promise<RuntimeStatusResult> {
  const nativeStatus = await windowsNativeRuntimeStatus(status.installDir);
  if (!nativeStatus) {
    return status;
  }

  if (nativeStatus.state === "running") {
    if (status.state === "running" && status.pid === nativeStatus.pid) {
      return status;
    }
    const detailParts = [
      nativeStatus.pid ? `PID ${nativeStatus.pid}` : "",
      status.uptime && status.uptime !== "N/A" ? `uptime ${status.uptime}` : "",
      status.channel ? `channel ${status.channel}` : "",
      status.octoState ? `Octo ${status.octoState}` : "",
    ].filter(Boolean);
    return {
      ...status,
      ok: true,
      state: "running",
      title: "Octopal is running",
      detail: detailParts.join(" · ") || nativeStatus.detail,
      pid: nativeStatus.pid,
    };
  }

  if (status.state === "running") {
    return {
      ...status,
      ok: true,
      state: "stopped",
      title: "Octopal is stopped",
      detail: "Runtime is not running.",
      pid: null,
    };
  }

  return status;
}

function parseDashboardSnapshot(output: string): unknown {
  const trimmed = output.trim();
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) {
    throw new Error("Octopal status did not return JSON output.");
  }

  return JSON.parse(trimmed.slice(start, end + 1));
}

function parseJsonOutput(output: string): unknown {
  return parseDashboardSnapshot(output);
}

async function commandExists(command: string, emit: (event: InstallEvent) => void): Promise<boolean> {
  try {
    await runCommand(command, ["--version"], emit, { quiet: true });
    return true;
  } catch {
    return false;
  }
}

async function resolveUv(emit: (event: InstallEvent) => void): Promise<string | null> {
  if (await commandExists("uv", emit)) {
    return "uv";
  }

  return getUvCandidates().find((candidate) => existsSync(candidate)) ?? null;
}

async function resolveInstalledUv(): Promise<string> {
  const uvCommand = await resolveUv(() => undefined);
  if (!uvCommand) {
    throw new Error("uv is not available. Install uv or run the installer again.");
  }
  return uvCommand;
}

function assertOctopalCheckout(installDir: string): void {
  if (!installDir) {
    throw new Error("Install directory is not selected.");
  }

  if (!existsSync(join(installDir, "pyproject.toml"))) {
    throw new Error("Install folder does not look like an Octopal checkout.");
  }
}

async function ensureUv(emit: (event: InstallEvent) => void): Promise<string> {
  const existingUv = await resolveUv(emit);
  if (existingUv) {
    return existingUv;
  }

  emitStep(emit, "Installing uv");
  if (process.platform === "win32") {
    await runCommand(
      "powershell.exe",
      ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", "irm https://astral.sh/uv/install.ps1 | iex"],
      emit,
    );
  } else {
    await runCommand("sh", ["-c", "curl -LsSf https://astral.sh/uv/install.sh | sh"], emit);
  }

  const installedUv = await resolveUv(emit);
  if (!installedUv) {
    throw new Error("uv was installed, but it is not available on PATH yet. Restart the app or add uv to PATH.");
  }
  return installedUv;
}

async function getLatestReleaseTag(emit: (event: InstallEvent) => void): Promise<string> {
  emitStep(emit, "Resolving latest Octopal release");
  try {
    const response = await fetch(LATEST_RELEASE_API_URL, {
      headers: {
        Accept: "application/vnd.github+json",
        "User-Agent": "octopal-desktop-installer",
      },
    });
    if (response.ok) {
      const release = (await response.json()) as { tag_name?: string };
      if (release.tag_name) {
        return release.tag_name.trim();
      }
    }
  } catch {
    // Fall back to git tags below.
  }

  const { stdout } = await runCommand(
    "git",
    ["ls-remote", "--tags", "--sort=-version:refname", "--refs", REPO_URL],
    emit,
    { quiet: true },
  );
  const firstTag = stdout
    .split(/\r?\n/)
    .map((line) => line.match(/refs\/tags\/(.+)$/)?.[1])
    .find(Boolean);

  if (!firstTag) {
    throw new Error("Could not determine the latest Octopal release tag.");
  }
  return firstTag;
}

async function isDirectoryEmpty(path: string): Promise<boolean> {
  try {
    const entries = await readdir(path);
    return entries.length === 0;
  } catch {
    return true;
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

async function cloneOrCheckoutRelease(installDir: string, releaseTag: string, emit: (event: InstallEvent) => void) {
  const exists = await pathExists(installDir);
  const hasGit = existsSync(join(installDir, ".git"));
  const hasProject = existsSync(join(installDir, "pyproject.toml"));

  if (!exists || (await isDirectoryEmpty(installDir))) {
    await mkdir(dirname(installDir), { recursive: true });
    emitStep(emit, `Downloading Octopal ${releaseTag}`, installDir);
    await runCommand(
      "git",
      ["-c", "advice.detachedHead=false", "clone", "--branch", releaseTag, "--depth", "1", REPO_URL, installDir],
      emit,
    );
    return;
  }

  if (hasGit && hasProject) {
    emitStep(emit, `Checking out Octopal ${releaseTag}`, installDir);
    await runCommand("git", ["fetch", "--tags", "--force"], emit, { cwd: installDir });
    await runCommand("git", ["-c", "advice.detachedHead=false", "checkout", releaseTag], emit, { cwd: installDir });
    return;
  }

  throw new Error("Install folder is not empty and does not look like an Octopal checkout.");
}

async function writeInstallFiles(payload: InstallPayload, releaseTag: string, emit: (event: InstallEvent) => void): Promise<Pick<InstallResult, "configPath" | "planPath">> {
  emitStep(emit, "Writing config.json");
  const configPath = join(payload.installDir, "config.json");
  await writeFile(configPath, JSON.stringify(payload.octopalConfig, null, 2), "utf8");

  const planDir = join(payload.installDir, ".octopal-desktop");
  await mkdir(planDir, { recursive: true });
  const planPath = join(planDir, "install-plan.json");
  await writeFile(
    planPath,
    JSON.stringify(
      {
        createdBy: payload.createdBy,
        createdAt: payload.createdAt,
        installDir: payload.installDir,
        releaseTag,
        configPath,
      },
      null,
      2,
    ),
    "utf8",
  );

  return { configPath, planPath };
}

function resolveWorkspaceDir(installDir: string, config: unknown): string {
  const root = asRecord(config);
  const storage = asRecord(root.storage);
  const configured = typeof storage.workspace_dir === "string" && storage.workspace_dir.trim()
    ? storage.workspace_dir.trim()
    : "workspace";
  return isAbsolute(configured) ? configured : join(installDir, configured);
}

async function copyMissingTree(sourceRoot: string, targetRoot: string): Promise<{ copied: number; skipped: number }> {
  await mkdir(targetRoot, { recursive: true });
  let copied = 0;
  let skipped = 0;

  const entries = await readdir(sourceRoot, { withFileTypes: true });
  for (const entry of entries) {
    const source = join(sourceRoot, entry.name);
    const target = join(targetRoot, entry.name);
    if (entry.isDirectory()) {
      const child = await copyMissingTree(source, target);
      copied += child.copied;
      skipped += child.skipped;
      continue;
    }
    if (!entry.isFile()) {
      continue;
    }
    if (existsSync(target)) {
      skipped += 1;
      continue;
    }
    await mkdir(dirname(target), { recursive: true });
    await copyFile(source, target);
    copied += 1;
  }

  return { copied, skipped };
}

export async function ensureWorkspaceBootstrap(
  installDir: string,
  config: unknown,
  emit?: (event: InstallEvent) => void,
): Promise<{ copied: number; skipped: number; workspaceDir: string }> {
  const workspaceDir = resolveWorkspaceDir(installDir, config);
  const templateRoot = join(installDir, "workspace_templates");
  await mkdir(workspaceDir, { recursive: true });

  if (!existsSync(templateRoot)) {
    emitWarning(emit ?? (() => undefined), "Workspace templates were not found", templateRoot);
    return { copied: 0, skipped: 0, workspaceDir };
  }

  emitStep(emit ?? (() => undefined), "Bootstrapping workspace", workspaceDir);
  const result = await copyMissingTree(templateRoot, workspaceDir);
  return { ...result, workspaceDir };
}

async function installProject(installDir: string, uvCommand: string, emit: (event: InstallEvent) => void) {
  emitStep(emit, "Installing Python dependencies");
  await runCommand(uvCommand, ["sync"], emit, { cwd: installDir, env: withLocalToolPaths() });

  emitStep(emit, "Installing browser runtime");
  await runCommand(uvCommand, ["run", "playwright", "install", "chromium"], emit, {
    cwd: installDir,
    env: withLocalToolPaths(),
  });
}

async function installOptionalBridge(installDir: string, emit: (event: InstallEvent) => void) {
  const bridgeDir = join(installDir, "scripts", "whatsapp_bridge");
  if (!existsSync(bridgeDir)) {
    return;
  }

  if (!(await commandExists("npm", emit))) {
    emitWarning(emit, "npm was not found", "WhatsApp bridge dependencies were not installed.");
    return;
  }

  emitStep(emit, "Installing WhatsApp bridge dependencies");
  await runCommand("npm", ["install"], emit, { cwd: bridgeDir });
}

async function checkDocker(emit: (event: InstallEvent) => void) {
  if (!(await commandExists("docker", emit))) {
    emitWarning(emit, "Docker was not found", "Workers need Docker by default. Install Docker Desktop before running workers.");
  }
}

export async function runInstall(payload: InstallPayload, emit: (event: InstallEvent) => void): Promise<InstallResult> {
  if (!payload.installDir) {
    throw new Error("Install directory is not selected.");
  }

  emitStep(emit, "Checking Git");
  if (!(await commandExists("git", emit))) {
    throw new Error("Git is required to install Octopal. Install Git and try again.");
  }

  const releaseTag = await getLatestReleaseTag(emit);
  await cloneOrCheckoutRelease(payload.installDir, releaseTag, emit);
  const files = await writeInstallFiles(payload, releaseTag, emit);
  await ensureWorkspaceBootstrap(payload.installDir, payload.octopalConfig, emit);
  const uvCommand = await ensureUv(emit);
  await installProject(payload.installDir, uvCommand, emit);
  await installOptionalBridge(payload.installDir, emit);
  await checkDocker(emit);

  const result: InstallResult = {
    installDir: payload.installDir,
    releaseTag,
    ...files,
  };
  emit({ kind: "done", message: "Octopal installation is ready", detail: payload.installDir });
  return result;
}

export async function startOctopal(installDir: string): Promise<StartResult> {
  if (!installDir) {
    throw new Error("Install directory is not selected.");
  }

  if (!existsSync(join(installDir, "pyproject.toml"))) {
    throw new Error("Install folder does not look like an Octopal checkout.");
  }

  const uvCommand = await resolveUv(() => undefined);
  if (!uvCommand) {
    throw new Error("uv is not available. Install uv or run the installer again.");
  }

  const config = JSON.parse(await readFile(join(installDir, "config.json"), "utf8"));
  await ensureWorkspaceBootstrap(installDir, config);

  const result = await runDetachedStart(uvCommand, ["run", "octopal", "start"], {
    cwd: installDir,
    env: withPythonDesktopEnv(),
    quiet: true,
  });

  if (result.exited && result.code !== 0) {
    throw new Error(
      `${uvCommand} run octopal start exited with code ${result.code}: ${sanitizeOutput(result.stderr || result.stdout).trim()}`,
    );
  }

  return {
    ok: true,
    installDir,
    detail: sanitizeOutput(result.stdout || result.stderr).trim(),
  };
}

export async function startOctopalSafely(installDir: string): Promise<StartResult | StartFailure> {
  try {
    return await startOctopal(installDir);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Could not start Octopal.";
    return {
      ok: false,
      error: "Could not start Octopal.",
      detail: sanitizeOutput(message),
    };
  }
}

async function stopWindowsRuntimeFallback(installDir: string, cliDetail: string): Promise<StopResult | null> {
  if (process.platform !== "win32") {
    return null;
  }

  const pids = new Set(extractProcessIds(cliDetail));
  for (const pid of await listWindowsOctopalRuntimePids().catch(() => [])) {
    pids.add(pid);
  }

  if (pids.size === 0) {
    return null;
  }

  const fallbackDetails: string[] = [await forceStopWindowsProcesses([...pids])].filter(Boolean);
  let statusAfterFallback = await getOctopalStatus(installDir).catch(() => null);
  if (statusAfterFallback?.state === "running") {
    const retryPids = new Set<number>();
    const statusPid = processIdValue(statusAfterFallback.pid);
    if (statusPid) {
      retryPids.add(statusPid);
    }
    for (const pid of await listWindowsOctopalRuntimePids().catch(() => [])) {
      retryPids.add(pid);
    }

    const remainingPids = [...retryPids].filter((pid) => !pids.has(pid));
    if (remainingPids.length > 0) {
      fallbackDetails.push(await forceStopWindowsProcesses(remainingPids));
      statusAfterFallback = await getOctopalStatus(installDir).catch(() => null);
    }
  }

  if (!statusAfterFallback || statusAfterFallback.state === "stopped") {
    return {
      ok: true,
      installDir,
      detail: [...fallbackDetails, cliDetail].filter(Boolean).join("\n"),
    };
  }

  throw new Error(
    [
      cliDetail,
      ...fallbackDetails,
      `Octopal still reports ${statusAfterFallback.state}: ${statusAfterFallback.detail}`,
    ]
      .filter(Boolean)
      .join("\n"),
  );
}

export async function stopOctopal(installDir: string): Promise<StopResult> {
  if (!installDir) {
    throw new Error("Install directory is not selected.");
  }

  if (!existsSync(join(installDir, "pyproject.toml"))) {
    throw new Error("Install folder does not look like an Octopal checkout.");
  }

  const uvCommand = await resolveUv(() => undefined);
  if (!uvCommand) {
    throw new Error("uv is not available. Install uv or run the installer again.");
  }

  try {
    const { stdout, stderr } = await runCommand(uvCommand, ["run", "octopal", "stop"], () => undefined, {
      cwd: installDir,
      env: withPythonDesktopEnv(),
      quiet: true,
    });
    const cliDetail = sanitizeOutput(stdout || stderr).trim();
    const statusAfterCli = await getOctopalStatus(installDir).catch(() => null);
    if (statusAfterCli?.state === "running") {
      const fallbackResult = await stopWindowsRuntimeFallback(installDir, cliDetail);
      if (fallbackResult) {
        return fallbackResult;
      }
    }

    return {
      ok: true,
      installDir,
      detail: cliDetail,
    };
  } catch (error) {
    const cliDetail = sanitizeOutput(error instanceof Error ? error.message : String(error)).trim();
    const statusAfterCli = await getOctopalStatus(installDir).catch(() => null);
    if (statusAfterCli?.state === "stopped") {
      return {
        ok: true,
        installDir,
        detail: `Octopal is stopped.\n${cliDetail}`,
      };
    }

    const fallbackResult = await stopWindowsRuntimeFallback(installDir, cliDetail);
    if (fallbackResult) {
      return fallbackResult;
    }

    throw error;
  }
}

export async function stopOctopalSafely(installDir: string): Promise<StopResult | StopFailure> {
  try {
    return await stopOctopal(installDir);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Could not stop Octopal.";
    return {
      ok: false,
      error: "Could not stop Octopal.",
      detail: sanitizeOutput(message),
    };
  }
}

function normalizeUpdateStatus(payload: unknown, fallbackDetail = ""): UpdateStatusResult {
  const record = asRecord(payload);
  const status = typeof record.status === "string" ? record.status : "unknown";
  const updateAvailable = record.update_available === true;
  const canUpdate = record.can_update === true;
  const gitBlocker = typeof record.git_blocker === "string" && record.git_blocker.trim() ? record.git_blocker : null;
  const message = typeof record.message === "string" ? record.message : "";
  const detail = message || gitBlocker || fallbackDetail || (updateAvailable ? "Update available." : "No update available.");

  return {
    ok: status !== "error",
    status,
    localVersion: typeof record.local_version === "string" ? record.local_version : undefined,
    latestVersion: typeof record.latest_version === "string" ? record.latest_version : null,
    releaseUrl: typeof record.release_url === "string" ? record.release_url : null,
    repo: typeof record.repo === "string" ? record.repo : undefined,
    updateAvailable,
    canUpdate,
    gitBlocker,
    updateCommand: typeof record.update_command === "string" ? record.update_command : undefined,
    restartCommand: typeof record.restart_command === "string" ? record.restart_command : undefined,
    detail,
  };
}

function releaseTagFromUpdateStatus(status: UpdateStatusResult): string {
  const latestVersion = status.latestVersion?.trim();
  if (!latestVersion) {
    throw new Error("Could not determine the latest Octopal release version.");
  }
  return latestVersion.toLowerCase().startsWith("v") ? latestVersion : `v${latestVersion}`;
}

async function performDesktopReleaseUpdate(
  installDir: string,
  uvCommand: string,
  before: UpdateStatusResult,
): Promise<string> {
  const releaseTag = releaseTagFromUpdateStatus(before);

  await runCommand("git", ["fetch", "--tags", "--force", "origin"], () => undefined, {
    cwd: installDir,
    env: withPythonDesktopEnv(),
    quiet: true,
  });
  await runCommand("git", ["-c", "advice.detachedHead=false", "checkout", "--detach", releaseTag], () => undefined, {
    cwd: installDir,
    env: withPythonDesktopEnv(),
    quiet: true,
  });
  await runCommand(uvCommand, ["sync"], () => undefined, {
    cwd: installDir,
    env: withPythonDesktopEnv(),
    quiet: true,
  });

  return `Checked out release tag ${releaseTag}.`;
}

export async function checkOctopalUpdate(installDir: string): Promise<UpdateStatusResult> {
  assertOctopalCheckout(installDir);
  const uvCommand = await resolveInstalledUv();
  const snippet = [
    "import json",
    "from octopal.runtime.self_control import check_update_status",
    "print(json.dumps(check_update_status()))",
  ].join("; ");
  const { stdout } = await runCommand(uvCommand, ["run", "python", "-c", snippet], () => undefined, {
    cwd: installDir,
    env: withPythonDesktopEnv(),
    quiet: true,
  });

  return normalizeUpdateStatus(parseJsonOutput(stdout));
}

export async function checkOctopalUpdateSafely(installDir: string): Promise<UpdateStatusResult> {
  try {
    return await checkOctopalUpdate(installDir);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Could not check for updates.";
    return {
      ok: false,
      status: "error",
      updateAvailable: false,
      canUpdate: false,
      detail: sanitizeOutput(message),
    };
  }
}

export async function updateOctopal(installDir: string): Promise<UpdateResult> {
  assertOctopalCheckout(installDir);
  const uvCommand = await resolveInstalledUv();
  const before = await checkOctopalUpdate(installDir);
  if (!before.updateAvailable) {
    return {
      ok: false,
      installDir,
      before,
      detail: "No newer release is available.",
      error: "No update available.",
    };
  }
  if (!before.canUpdate) {
    return {
      ok: false,
      installDir,
      before,
      detail: before.detail || "Update is blocked by the current checkout state.",
      error: "Update blocked.",
    };
  }

  const statusBefore = await getOctopalStatus(installDir).catch(() => null);
  const wasRunning = statusBefore?.state === "running";
  const updateDetail = await performDesktopReleaseUpdate(installDir, uvCommand, before);
  let restartDetail = "";
  if (wasRunning) {
    const restart = await runCommand(uvCommand, ["run", "octopal", "restart"], () => undefined, {
      cwd: installDir,
      env: withPythonDesktopEnv(),
      quiet: true,
    });
    restartDetail = sanitizeOutput(restart.stdout || restart.stderr).trim();
  }

  const after = await checkOctopalUpdateSafely(installDir);
  if (after.ok && after.updateAvailable) {
    return {
      ok: false,
      installDir,
      before,
      after,
      restarted: wasRunning,
      detail: "Update failed. Try to restart Octopal Desktop or update manually.",
      error: "Update failed.",
    };
  }

  const detail = [
    updateDetail,
    wasRunning ? restartDetail || "Octopal restart requested after update." : "Octopal was stopped; start it when ready.",
  ]
    .filter(Boolean)
    .join("\n");
  return {
    ok: true,
    installDir,
    before,
    after,
    restarted: wasRunning,
    detail,
  };
}

export async function updateOctopalSafely(installDir: string): Promise<UpdateResult> {
  try {
    return await updateOctopal(installDir);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Could not update Octopal.";
    return {
      ok: false,
      installDir,
      detail: sanitizeOutput(message),
      error: "Could not update Octopal.",
    };
  }
}

export async function getOctopalStatus(installDir: string): Promise<RuntimeStatusResult> {
  assertOctopalCheckout(installDir);
  const uvCommand = await resolveInstalledUv();

  const { stdout } = await runCommand(uvCommand, ["run", "octopal", "dashboard", "--once", "--json"], () => undefined, {
    cwd: installDir,
    env: withPythonDesktopEnv(),
    quiet: true,
  });

  try {
    return await reconcileWindowsRuntimeStatus(statusFromDashboardSnapshot(parseDashboardSnapshot(stdout), installDir));
  } catch (error) {
    const nativeStatus = await windowsNativeRuntimeStatus(installDir);
    if (nativeStatus) {
      return nativeStatus;
    }
    throw error;
  }
}

export async function getOctopalStatusSafely(installDir: string): Promise<RuntimeStatusResult> {
  try {
    return await getOctopalStatus(installDir);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Could not read Octopal status.";
    return {
      ok: false,
      state: "error",
      title: "Could not read Octopal status",
      detail: sanitizeOutput(message),
      installDir,
    };
  }
}
