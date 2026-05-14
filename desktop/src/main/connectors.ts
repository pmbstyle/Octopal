import { runCommand, withPythonDesktopEnv } from "./installer";

const EXISTING_SECRET_VALUE = "__OCTOPAL_DESKTOP_EXISTING_SECRET__";

export type ConnectorName = "google" | "github";

export type ConnectorStatusResult = {
  ok: boolean;
  connectors: Record<string, unknown>;
  detail: string;
};

export type ConnectorAuthPayload = {
  name: ConnectorName;
  clientId?: string;
  clientSecret?: string;
  token?: string;
};

export type ConnectorActionResult = {
  ok: boolean;
  name: ConnectorName;
  status?: string;
  message: string;
  detail: string;
};

function parseJsonFromOutput(output: string): Record<string, unknown> {
  const trimmed = output.trim();
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) {
    throw new Error("Connector command did not return JSON output.");
  }
  const parsed = JSON.parse(trimmed.slice(start, end + 1));
  return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : {};
}

function connectorMessage(payload: Record<string, unknown>, fallback: string): string {
  return typeof payload.message === "string"
    ? payload.message
    : typeof payload.error === "string"
      ? payload.error
      : fallback;
}

function addOptionalArg(args: string[], name: string, value: string | undefined): void {
  if (!value || value === EXISTING_SECRET_VALUE) {
    return;
  }
  args.push(name, value);
}

function addRequiredLegacyArg(args: string[], name: string, value: string | undefined): boolean {
  if (!value || value === EXISTING_SECRET_VALUE) {
    return false;
  }
  args.push(name, value);
  return true;
}

function looksLikeMissingJsonOption(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes("No such option: --json");
}

function buildJsonAuthArgs(payload: ConnectorAuthPayload): string[] {
  const args = ["run", "octopal", "connector", "auth", payload.name, "--json", "--no-manual"];
  if (payload.name === "google") {
    addOptionalArg(args, "--client-id", payload.clientId);
    addOptionalArg(args, "--client-secret", payload.clientSecret);
  }
  if (payload.name === "github") {
    addOptionalArg(args, "--token", payload.token);
  }
  return args;
}

function buildLegacyAuthArgs(payload: ConnectorAuthPayload): string[] | null {
  const args = ["run", "octopal", "connector", "auth", payload.name];
  if (payload.name === "google") {
    const hasClientId = addRequiredLegacyArg(args, "--client-id", payload.clientId);
    const hasClientSecret = addRequiredLegacyArg(args, "--client-secret", payload.clientSecret);
    return hasClientId && hasClientSecret ? args : null;
  }
  const hasToken = addRequiredLegacyArg(args, "--token", payload.token);
  return hasToken ? args : null;
}

export async function getConnectorStatus(installDir: string): Promise<ConnectorStatusResult> {
  try {
    const { stdout, stderr } = await runCommand(
      "uv",
      ["run", "octopal", "connector", "status", "--json"],
      () => undefined,
      { cwd: installDir, env: withPythonDesktopEnv(), quiet: true },
    );
    const payload = parseJsonFromOutput(stdout || stderr);
    const connectors =
      payload.connectors && typeof payload.connectors === "object" && !Array.isArray(payload.connectors)
        ? (payload.connectors as Record<string, unknown>)
        : {};
    return { ok: true, connectors, detail: "Connector status loaded." };
  } catch (error) {
    return {
      ok: false,
      connectors: {},
      detail: error instanceof Error ? error.message : "Could not load connector status.",
    };
  }
}

async function authorizeConnectorJson(installDir: string, payload: ConnectorAuthPayload): Promise<ConnectorActionResult> {
  const args = buildJsonAuthArgs(payload);
  try {
    const { stdout, stderr } = await runCommand("uv", args, () => undefined, {
      cwd: installDir,
      env: withPythonDesktopEnv(),
      quiet: true,
    });
    const result = parseJsonFromOutput(stdout || stderr);
    return {
      ok: result.status === "success",
      name: payload.name,
      status: typeof result.status === "string" ? result.status : undefined,
      message: connectorMessage(result, "Connector authorized."),
      detail: stdout || stderr,
    };
  } catch (error) {
    throw error;
  }
}

async function authorizeConnectorLegacy(installDir: string, payload: ConnectorAuthPayload): Promise<ConnectorActionResult> {
  const args = buildLegacyAuthArgs(payload);
  if (!args) {
    return {
      ok: false,
      name: payload.name,
      message: "Installed Octopal does not support desktop JSON auth yet. Re-enter the connector secret or update Octopal.",
      detail: "",
    };
  }

  try {
    const { stdout, stderr } = await runCommand("uv", args, () => undefined, {
      cwd: installDir,
      env: withPythonDesktopEnv(),
      quiet: true,
    });
    const output = (stdout || stderr).trim();
    return {
      ok: true,
      name: payload.name,
      status: "success",
      message: output.split(/\r?\n/).find((line) => line.includes("authorized")) || "Connector authorized.",
      detail: output,
    };
  } catch (error) {
    return {
      ok: false,
      name: payload.name,
      message: error instanceof Error ? error.message : "Connector authorization failed.",
      detail: error instanceof Error ? error.message : "",
    };
  }
}

export async function authorizeConnector(installDir: string, payload: ConnectorAuthPayload): Promise<ConnectorActionResult> {
  try {
    return await authorizeConnectorJson(installDir, payload);
  } catch (error) {
    if (looksLikeMissingJsonOption(error)) {
      return authorizeConnectorLegacy(installDir, payload);
    }
    return {
      ok: false,
      name: payload.name,
      message: error instanceof Error ? error.message : "Connector authorization failed.",
      detail: error instanceof Error ? error.message : "",
    };
  }
}

export async function disconnectConnector(
  installDir: string,
  name: ConnectorName,
  forgetCredentials = false,
): Promise<ConnectorActionResult> {
  const args = ["run", "octopal", "connector", "disconnect", name];
  if (forgetCredentials) {
    args.push("--forget-credentials");
  }

  try {
    const { stdout, stderr } = await runCommand("uv", args, () => undefined, {
      cwd: installDir,
      env: withPythonDesktopEnv(),
      quiet: true,
    });
    return {
      ok: true,
      name,
      status: "success",
      message: (stdout || stderr).trim() || "Connector disconnected.",
      detail: stdout || stderr,
    };
  } catch (error) {
    return {
      ok: false,
      name,
      message: error instanceof Error ? error.message : "Connector disconnect failed.",
      detail: error instanceof Error ? error.message : "",
    };
  }
}
