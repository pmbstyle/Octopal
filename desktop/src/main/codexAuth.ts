import { BrowserWindow, app, ipcMain, shell } from "electron";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { EventEmitter } from "node:events";
import readline from "node:readline";

type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

type RpcResponse = {
  id?: number | string;
  result?: JsonValue;
  error?: {
    code?: number;
    message?: string;
    data?: JsonValue;
  };
};

type RpcNotification = {
  method: string;
  params?: unknown;
};

type PendingRequest = {
  method: string;
  resolve: (value: JsonValue) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
};

export type CodexAuthStatus = {
  available: boolean;
  connected: boolean;
  accountLabel?: string;
  accountType?: string;
  requiresOpenAIAuth?: boolean;
  error?: string;
};

export type CodexModelInfo = {
  id: string;
  model: string;
  displayName: string;
  hidden?: boolean;
};

const CODEX_REQUEST_TIMEOUT_MS = 30_000;
const CODEX_LOGIN_TIMEOUT_MS = 10 * 60_000;

class CodexAppServerClient extends EventEmitter {
  private child: ChildProcessWithoutNullStreams | null = null;
  private lines: readline.Interface | null = null;
  private initialized = false;
  private closed = false;
  private nextId = 1;
  private stderrTail = "";
  private readonly pending = new Map<number | string, PendingRequest>();

  constructor(
    private readonly command: string,
    private readonly args: string[],
    private readonly env: NodeJS.ProcessEnv,
  ) {
    super();
  }

  async start(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.child = spawn(this.command, this.args, {
      env: this.env,
      shell: process.platform === "win32",
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    });

    this.child.once("error", (error) => this.closeWithError(error));
    this.child.once("exit", (code, signal) => {
      this.closeWithError(
        new Error(
          `codex app-server exited with code ${code ?? "null"} and signal ${signal ?? "null"}${
            this.stderrTail ? `: ${this.stderrTail}` : ""
          }`,
        ),
      );
    });
    this.child.stderr.on("data", (chunk: Buffer) => {
      const text = chunk.toString("utf8");
      this.stderrTail = `${this.stderrTail}${text}`.slice(-4000);
      const trimmed = text.trim();
      if (trimmed) {
        console.log("[CodexAppServer]", trimmed);
      }
    });

    this.lines = readline.createInterface({ input: this.child.stdout });
    this.lines.on("line", (line) => this.handleLine(line));

    await this.request("initialize", {
      clientInfo: {
        name: "octopal_desktop",
        title: "Octopal Desktop",
        version: app.getVersion(),
      },
      capabilities: {
        experimentalApi: true,
      },
    });
    this.notify("initialized", {});
    this.initialized = true;
  }

  async request(method: string, params?: JsonValue, timeoutMs = CODEX_REQUEST_TIMEOUT_MS): Promise<JsonValue> {
    if (this.closed) {
      throw new Error("codex app-server client is closed");
    }
    if (!this.child) {
      throw new Error("codex app-server is not running");
    }

    const id = this.nextId++;
    const message = { method, id, ...(params === undefined ? {} : { params }) };

    return await new Promise<JsonValue>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method} timed out`));
      }, timeoutMs);

      this.pending.set(id, { method, resolve, reject, timer });
      this.child?.stdin.write(`${JSON.stringify(message)}\n`, (error) => {
        if (error) {
          clearTimeout(timer);
          this.pending.delete(id);
          reject(error);
        }
      });
    });
  }

  notify(method: string, params?: JsonValue): void {
    if (!this.child || this.closed) {
      return;
    }
    const message = { method, ...(params === undefined ? {} : { params }) };
    this.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  close(): void {
    this.closed = true;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error("codex app-server client closed"));
    }
    this.pending.clear();
    this.lines?.close();
    this.lines = null;
    this.child?.kill();
    this.child = null;
  }

  private handleLine(line: string): void {
    const trimmed = line.trim();
    if (!trimmed) {
      return;
    }

    let message: RpcResponse | RpcNotification;
    try {
      message = JSON.parse(trimmed) as RpcResponse | RpcNotification;
    } catch {
      console.warn("[CodexAppServer] Failed to parse JSON-RPC line:", trimmed);
      return;
    }

    if ("id" in message && message.id !== undefined) {
      const pending = this.pending.get(message.id);
      if (!pending) {
        return;
      }
      clearTimeout(pending.timer);
      this.pending.delete(message.id);
      if ("error" in message && message.error) {
        pending.reject(new Error(message.error.message || `${pending.method} failed`));
      } else {
        pending.resolve(message.result ?? null);
      }
      return;
    }

    if ("method" in message && typeof message.method === "string") {
      this.emit("notification", message);
    }
  }

  private closeWithError(error: Error): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    this.emit("error", error);
  }
}

class CodexAuthManager {
  private client: CodexAppServerClient | null = null;
  private starting: Promise<CodexAppServerClient> | null = null;

  async getStatus(): Promise<CodexAuthStatus> {
    try {
      const client = await this.getClient();
      const result = (await client.request("account/read", { refreshToken: false })) as Record<string, unknown>;
      return normalizeAccountStatus(result);
    } catch (error) {
      return {
        available: false,
        connected: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async startLogin(): Promise<{ success: boolean; authUrl?: string; loginId?: string; error?: string }> {
    try {
      const client = await this.getClient();
      const response = (await client.request("account/login/start", {
        type: "chatgpt",
        codexStreamlinedLogin: true,
      })) as Record<string, unknown>;

      const authUrl = typeof response.authUrl === "string" ? response.authUrl : "";
      if (response.type !== "chatgpt" || !authUrl) {
        return { success: false, error: "Codex did not return a ChatGPT authorization URL." };
      }

      void shell.openExternal(authUrl);
      this.waitForLoginCompletion(client, typeof response.loginId === "string" ? response.loginId : undefined);
      return { success: true, authUrl, loginId: typeof response.loginId === "string" ? response.loginId : undefined };
    } catch (error) {
      return { success: false, error: error instanceof Error ? error.message : String(error) };
    }
  }

  async logout(): Promise<{ success: boolean; error?: string }> {
    try {
      const client = await this.getClient();
      await client.request("account/logout");
      this.broadcast("codex-auth-status-changed", { available: true, connected: false });
      return { success: true };
    } catch (error) {
      return { success: false, error: error instanceof Error ? error.message : String(error) };
    }
  }

  async listModels(): Promise<{ success: boolean; models?: CodexModelInfo[]; error?: string }> {
    try {
      const client = await this.getClient();
      const response = (await client.request("model/list", {
        cursor: null,
        limit: 100,
        includeHidden: false,
      })) as Record<string, unknown>;
      const data = Array.isArray(response.data) ? response.data : [];
      return { success: true, models: data.map((item) => normalizeModelInfo(item)).filter(Boolean) as CodexModelInfo[] };
    } catch (error) {
      return { success: false, error: error instanceof Error ? error.message : String(error) };
    }
  }

  stop(): void {
    this.client?.close();
    this.client = null;
    this.starting = null;
  }

  private async getClient(): Promise<CodexAppServerClient> {
    if (this.client) {
      return this.client;
    }
    if (!this.starting) {
      this.starting = this.createClient();
    }
    this.client = await this.starting;
    this.starting = null;
    return this.client;
  }

  private async createClient(): Promise<CodexAppServerClient> {
    const command = process.env.OCTOPAL_CODEX_COMMAND || "codex";
    const args = (process.env.OCTOPAL_CODEX_ARGS || "app-server")
      .split(/\s+/)
      .map((part) => part.trim())
      .filter(Boolean);

    const client = new CodexAppServerClient(command, args, { ...process.env });
    client.on("notification", (notification) => this.handleNotification(notification as RpcNotification));
    client.on("error", (error) => {
      console.error("[CodexAppServer] Client error:", error);
      if (this.client === client) {
        this.client = null;
      }
    });
    await client.start();
    return client;
  }

  private handleNotification(notification: RpcNotification): void {
    if (notification.method === "account/login/completed") {
      this.broadcast("codex-auth-login-completed", notification.params);
      return;
    }
    if (notification.method === "account/updated") {
      this.broadcast("codex-auth-updated", notification.params);
    }
  }

  private waitForLoginCompletion(client: CodexAppServerClient, loginId: string | undefined): void {
    const timer = setTimeout(() => {
      client.off("notification", onNotification);
    }, CODEX_LOGIN_TIMEOUT_MS);

    const onNotification = (notification: RpcNotification) => {
      if (notification.method !== "account/login/completed") {
        return;
      }
      const params = notification.params && typeof notification.params === "object" ? (notification.params as Record<string, unknown>) : {};
      if (loginId && typeof params.loginId === "string" && params.loginId !== loginId) {
        return;
      }
      clearTimeout(timer);
      client.off("notification", onNotification);
      void this.getStatus().then((status) => {
        this.broadcast("codex-auth-status-changed", status);
      });
    };

    client.on("notification", onNotification);
  }

  private broadcast(channel: string, payload: unknown): void {
    for (const win of BrowserWindow.getAllWindows()) {
      if (!win.isDestroyed()) {
        win.webContents.send(channel, payload);
      }
    }
  }
}

function normalizeAccountStatus(response: Record<string, unknown>): CodexAuthStatus {
  const account = response.account && typeof response.account === "object" ? (response.account as Record<string, unknown>) : null;
  if (!account) {
    return {
      available: true,
      connected: false,
      requiresOpenAIAuth: Boolean(response.requiresOpenAIAuth || response.requiresOpenaiAuth),
    };
  }

  const accountType = typeof account.type === "string" ? account.type : "connected";
  if (accountType === "chatgpt") {
    const email = typeof account.email === "string" && account.email.trim() ? account.email.trim() : "ChatGPT";
    const plan = typeof account.planType === "string" && account.planType.trim() ? ` (${account.planType.trim()})` : "";
    return {
      available: true,
      connected: true,
      accountType,
      accountLabel: `${email}${plan}`,
      requiresOpenAIAuth: false,
    };
  }

  if (accountType === "apiKey") {
    return {
      available: true,
      connected: true,
      accountType,
      accountLabel: "OpenAI API key",
      requiresOpenAIAuth: false,
    };
  }

  return {
    available: true,
    connected: true,
    accountType,
    accountLabel: accountType,
    requiresOpenAIAuth: false,
  };
}

function normalizeModelInfo(model: unknown): CodexModelInfo | null {
  if (!model || typeof model !== "object") {
    return null;
  }
  const record = model as Record<string, unknown>;
  const id = typeof record.id === "string" ? record.id : typeof record.model === "string" ? record.model : "";
  if (!id) {
    return null;
  }
  return {
    id,
    model: typeof record.model === "string" ? record.model : id,
    displayName: typeof record.displayName === "string" ? record.displayName : id,
    hidden: Boolean(record.hidden),
  };
}

export const codexAuthManager = new CodexAuthManager();

let codexAuthIPCHandlersRegistered = false;

export function registerCodexAuthIPCHandlers(): void {
  if (codexAuthIPCHandlersRegistered) {
    return;
  }
  codexAuthIPCHandlersRegistered = true;

  ipcMain.handle("codex-auth:status", async () => codexAuthManager.getStatus());
  ipcMain.handle("codex-auth:start-login", async () => codexAuthManager.startLogin());
  ipcMain.handle("codex-auth:disconnect", async () => codexAuthManager.logout());
  ipcMain.handle("codex-models:list", async () => codexAuthManager.listModels());
}

export function stopCodexAuthServer(): void {
  codexAuthManager.stop();
}
