import { z } from "zod";

import { generateDashboardToken } from "./security";

export const EXISTING_SECRET_VALUE = "__OCTOPAL_DESKTOP_EXISTING_SECRET__";

export function isExistingSecret(value: string | undefined | null): boolean {
  return value === EXISTING_SECRET_VALUE;
}

export const providers = [
  {
    id: "openrouter",
    label: "OpenRouter",
    model: "x-ai/grok-4.3",
    apiBase: "https://openrouter.ai/api/v1",
  },
  { id: "zai", label: "Z.AI", model: "glm-5.1", apiBase: "https://api.z.ai/api/coding/paas/v4" },
  { id: "openai", label: "OpenAI", model: "gpt-5.5", apiBase: "https://api.openai.com/v1" },
  { id: "anthropic", label: "Anthropic", model: "claude-opus-4-7", apiBase: "https://api.anthropic.com" },
  { id: "google", label: "Google Gemini", model: "gemini-3.1-pro-preview", apiBase: "" },
  { id: "mistral", label: "Mistral", model: "mistral-medium-3-5+1", apiBase: "https://api.mistral.ai/v1" },
  {
    id: "together",
    label: "Together AI",
    model: "moonshotai/Kimi-K2.5",
    apiBase: "https://api.together.xyz/v1",
  },
  { id: "groq", label: "Groq", model: "openai/gpt-oss-120b", apiBase: "https://api.groq.com/openai/v1" },
  { id: "minimax", label: "Minimax", model: "MiniMax-M2.7", apiBase: "https://api.minimax.io/anthropic/v1" },
  { id: "custom", label: "Custom LiteLLM", model: "", apiBase: "" },
] as const;

export const searchProviders = [
  { id: "brave", label: "Brave Search", keyField: "braveApiKey" },
  { id: "firecrawl", label: "Firecrawl", keyField: "firecrawlApiKey" },
] as const;

export const connectorProviders = [
  {
    id: "google",
    label: "Google",
    services: [
      { id: "gmail", label: "Gmail" },
      { id: "calendar", label: "Calendar" },
      { id: "drive", label: "Drive" },
    ],
  },
  {
    id: "github",
    label: "GitHub",
    services: [
      { id: "repos", label: "Repositories" },
      { id: "issues", label: "Issues" },
      { id: "pull_requests", label: "Pull requests" },
    ],
  },
] as const;

const googleServiceSchema = z.enum(["gmail", "calendar", "drive"]);
const githubServiceSchema = z.enum(["repos", "issues", "pull_requests"]);

export const installSchema = z
  .object({
    installDir: z.string().trim().min(1),
    channel: z.enum(["telegram", "whatsapp"]),
    telegramToken: z.string().optional(),
    allowedChatIds: z.string().optional(),
    whatsappMode: z.enum(["personal", "separate"]),
    whatsappAllowedNumbers: z.string().optional(),
    providerId: z.string().trim().min(1),
    model: z.string().trim().min(1),
    apiKey: z.string().optional(),
    apiBase: z.string().optional(),
    sameWorker: z.boolean(),
    workerProviderId: z.string().optional(),
    workerModel: z.string().optional(),
    workerApiKey: z.string().optional(),
    workerApiBase: z.string().optional(),
    searchProvider: z.enum(["brave", "firecrawl"]).optional(),
    braveApiKey: z.string().optional(),
    firecrawlApiKey: z.string().optional(),
    dashboardEnabled: z.boolean(),
    dashboardPort: z.number().int().min(1).max(65535),
    dashboardToken: z.string().optional(),
    googleConnectorEnabled: z.boolean(),
    googleConnectorServices: z.array(googleServiceSchema),
    googleClientId: z.string().optional(),
    googleClientSecret: z.string().optional(),
    githubConnectorEnabled: z.boolean(),
    githubConnectorServices: z.array(githubServiceSchema),
    githubToken: z.string().optional(),
  })
  .superRefine((values, context) => {
    const requireField = (path: string, value: string | undefined) => {
      if (!value?.trim() && !isExistingSecret(value)) {
        context.addIssue({ code: "custom", path: [path], message: "Required" });
      }
    };

    if (values.channel === "telegram") {
      requireField("telegramToken", values.telegramToken);
    }

    if (values.channel === "whatsapp") {
      requireField("whatsappAllowedNumbers", values.whatsappAllowedNumbers);
    }

    if (values.providerId === "custom") {
      requireField("apiBase", values.apiBase);
    } else {
      requireField("apiKey", values.apiKey);
    }

    if (!values.sameWorker) {
      requireField("workerProviderId", values.workerProviderId);
      requireField("workerModel", values.workerModel);
      if (values.workerProviderId === "custom") {
        requireField("workerApiBase", values.workerApiBase);
      } else {
        requireField("workerApiKey", values.workerApiKey);
      }
    }

    if (values.searchProvider === "brave") {
      requireField("braveApiKey", values.braveApiKey);
    }

    if (values.searchProvider === "firecrawl") {
      requireField("firecrawlApiKey", values.firecrawlApiKey);
    }

    if (values.googleConnectorEnabled) {
      if (values.googleConnectorServices.length === 0) {
        context.addIssue({ code: "custom", path: ["googleConnectorServices"], message: "Required" });
      }
      requireField("googleClientId", values.googleClientId);
      requireField("googleClientSecret", values.googleClientSecret);
    }

    if (values.githubConnectorEnabled) {
      if (values.githubConnectorServices.length === 0) {
        context.addIssue({ code: "custom", path: ["githubConnectorServices"], message: "Required" });
      }
      requireField("githubToken", values.githubToken);
    }
  });

export type InstallForm = z.infer<typeof installSchema>;

function secretString(value: string | undefined): string {
  return isExistingSecret(value) ? "" : value || "";
}

function dashboardToken(value: string | undefined, webappEnabled: boolean): string {
  if (isExistingSecret(value)) {
    return "";
  }

  const configured = (value || "").trim();
  if (configured || !webappEnabled) {
    return configured;
  }
  return generateDashboardToken();
}

function secretNullable(value: string | undefined): string | null {
  return isExistingSecret(value) ? null : value || null;
}

export const defaultInstallValues: InstallForm = {
  installDir: "",
  channel: "telegram",
  telegramToken: "",
  allowedChatIds: "",
  whatsappMode: "separate",
  whatsappAllowedNumbers: "",
  providerId: "openrouter",
  model: "x-ai/grok-4.3",
  apiKey: "",
  apiBase: "",
  sameWorker: false,
  workerProviderId: "openrouter",
  workerModel: "x-ai/grok-4.3",
  workerApiKey: "",
  workerApiBase: "",
  searchProvider: undefined,
  braveApiKey: "",
  firecrawlApiKey: "",
  dashboardEnabled: true,
  dashboardPort: 8000,
  dashboardToken: "",
  googleConnectorEnabled: false,
  googleConnectorServices: ["gmail"],
  googleClientId: "",
  googleClientSecret: "",
  githubConnectorEnabled: false,
  githubConnectorServices: ["repos"],
  githubToken: "",
};

export function buildOctopalConfig(values: InstallForm) {
  const chatIds = values.allowedChatIds
    ?.split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  const whatsappNumbers = values.whatsappAllowedNumbers
    ?.split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  const workerProviderId = values.workerProviderId || values.providerId;
  const workerModel = values.workerModel || values.model;

  return {
    user_channel: values.channel,
    telegram: {
      bot_token: secretString(values.telegramToken),
      allowed_chat_ids: chatIds ?? [],
      parse_mode: "MarkdownV2",
    },
    llm: {
      provider_id: values.providerId,
      model: values.model,
      api_key: secretNullable(values.apiKey),
      api_base: values.apiBase || null,
      model_prefix: null,
    },
    worker_llm_default: values.sameWorker
      ? {
          provider_id: null,
          model: null,
          api_key: null,
          api_base: null,
          model_prefix: null,
        }
      : {
          provider_id: workerProviderId,
          model: workerModel,
          api_key: secretNullable(values.workerApiKey),
          api_base: values.workerApiBase || null,
          model_prefix: null,
        },
    worker_llm_overrides: {},
    storage: {
      state_dir: "data",
      workspace_dir: "workspace",
    },
    gateway: {
      host: "127.0.0.1",
      port: values.dashboardPort,
      dashboard_token: dashboardToken(values.dashboardToken, values.dashboardEnabled),
      tailscale_auto_serve: true,
      tailscale_ips: "",
      webapp_enabled: values.dashboardEnabled,
      webapp_dist_dir: null,
    },
    whatsapp: {
      mode: values.whatsappMode,
      allowed_numbers: whatsappNumbers ?? [],
      auth_dir: null,
      bridge_host: "127.0.0.1",
      bridge_port: 8765,
      callback_token: "",
      node_command: "node",
    },
    search: {
      brave_api_key: values.searchProvider === "brave" ? secretNullable(values.braveApiKey) : null,
      firecrawl_api_key: values.searchProvider === "firecrawl" ? secretNullable(values.firecrawlApiKey) : null,
    },
    connectors: {
      instances: {
        google: {
          enabled: values.googleConnectorEnabled,
          enabled_services: values.googleConnectorServices,
          credentials: {
            client_id: values.googleClientId || null,
            client_secret: secretNullable(values.googleClientSecret),
          },
        },
        github: {
          enabled: values.githubConnectorEnabled,
          enabled_services: values.githubConnectorServices,
          auth: {
            access_token: secretNullable(values.githubToken),
          },
        },
      },
    },
  };
}

function stringValue(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function numberValue(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function listValue(value: unknown): string {
  return Array.isArray(value) ? value.map((item) => String(item)).join(", ") : "";
}

function stringListValue<T extends string>(value: unknown, allowed: readonly T[], fallback: T[]): T[] {
  if (!Array.isArray(value)) {
    return fallback;
  }
  const allowedSet = new Set<string>(allowed);
  const selected = value.map((item) => String(item)).filter((item): item is T => allowedSet.has(item));
  return selected.length > 0 ? selected : fallback;
}

export function formValuesFromOctopalConfig(config: unknown, installDir: string): InstallForm {
  const root = recordValue(config);
  const telegram = recordValue(root.telegram);
  const whatsapp = recordValue(root.whatsapp);
  const llm = recordValue(root.llm);
  const workerLlm = recordValue(root.worker_llm_default);
  const gateway = recordValue(root.gateway);
  const search = recordValue(root.search);
  const connectors = recordValue(root.connectors);
  const connectorInstances = recordValue(connectors.instances);
  const googleConnector = recordValue(connectorInstances.google);
  const googleCredentials = recordValue(googleConnector.credentials);
  const githubConnector = recordValue(connectorInstances.github);
  const githubAuth = recordValue(githubConnector.auth);
  const braveApiKey = stringValue(search.brave_api_key);
  const firecrawlApiKey = stringValue(search.firecrawl_api_key);
  const workerProviderId = stringValue(workerLlm.provider_id);

  return {
    ...defaultInstallValues,
    installDir,
    channel: root.user_channel === "whatsapp" ? "whatsapp" : "telegram",
    telegramToken: stringValue(telegram.bot_token),
    allowedChatIds: listValue(telegram.allowed_chat_ids),
    whatsappMode: whatsapp.mode === "personal" ? "personal" : "separate",
    whatsappAllowedNumbers: listValue(whatsapp.allowed_numbers),
    providerId: stringValue(llm.provider_id, defaultInstallValues.providerId),
    model: stringValue(llm.model, defaultInstallValues.model),
    apiKey: stringValue(llm.api_key),
    apiBase: stringValue(llm.api_base),
    sameWorker: !workerProviderId,
    workerProviderId: workerProviderId || defaultInstallValues.workerProviderId,
    workerModel: stringValue(workerLlm.model, defaultInstallValues.workerModel),
    workerApiKey: stringValue(workerLlm.api_key),
    workerApiBase: stringValue(workerLlm.api_base),
    searchProvider: braveApiKey ? "brave" : firecrawlApiKey ? "firecrawl" : undefined,
    braveApiKey,
    firecrawlApiKey,
    dashboardEnabled: gateway.webapp_enabled !== false,
    dashboardPort: numberValue(gateway.port, defaultInstallValues.dashboardPort),
    dashboardToken: stringValue(gateway.dashboard_token),
    googleConnectorEnabled: googleConnector.enabled === true,
    googleConnectorServices: stringListValue(googleConnector.enabled_services, ["gmail", "calendar", "drive"], ["gmail"]),
    googleClientId: stringValue(googleCredentials.client_id),
    googleClientSecret: stringValue(googleCredentials.client_secret),
    githubConnectorEnabled: githubConnector.enabled === true,
    githubConnectorServices: stringListValue(githubConnector.enabled_services, ["repos", "issues", "pull_requests"], ["repos"]),
    githubToken: stringValue(githubAuth.access_token),
  };
}
