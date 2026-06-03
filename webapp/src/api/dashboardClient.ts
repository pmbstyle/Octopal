import type { paths } from "./types";

type OverviewResponse =
  paths["/api/dashboard/v2/overview"]["get"]["responses"]["200"]["content"]["application/json"];
type IncidentsResponse =
  paths["/api/dashboard/v2/incidents"]["get"]["responses"]["200"]["content"]["application/json"];
type OctoResponse =
  paths["/api/dashboard/v2/octo"]["get"]["responses"]["200"]["content"]["application/json"];
type WorkersResponse =
  paths["/api/dashboard/v2/workers"]["get"]["responses"]["200"]["content"]["application/json"];
type SystemResponse =
  paths["/api/dashboard/v2/system"]["get"]["responses"]["200"]["content"]["application/json"];

export type DashboardEditableConfig = {
  user_channel: string;
  telegram: {
    bot_token: string;
    allowed_chat_ids: string[];
    parse_mode: string;
  };
  llm: {
    provider_id: string | null;
    model: string | null;
    api_key: string | null;
    api_base: string | null;
    model_prefix: string | null;
  };
  worker_llm_default: {
    provider_id: string | null;
    model: string | null;
    api_key: string | null;
    api_base: string | null;
    model_prefix: string | null;
  };
  litellm: {
    num_retries: number;
    timeout: number;
    fallbacks: string | null;
    drop_params: boolean;
    caching: boolean;
    max_concurrency: number;
    rate_limit_max_retries: number;
    rate_limit_base_delay_seconds: number;
    rate_limit_max_delay_seconds: number;
  };
  storage: {
    state_dir: string;
    workspace_dir: string;
  };
  memory: {
    top_k: number;
    prefilter_k: number;
    min_score: number;
    max_chars: number;
    owner_id: string;
  };
  gateway: {
    host: string;
    port: number;
    tailscale_ips: string;
    dashboard_token: string;
    tailscale_auto_serve: boolean;
    webapp_enabled: boolean;
    webapp_dist_dir: string | null;
  };
  workers: {
    launcher: string;
    docker_image: string;
    docker_workspace: string;
    docker_host_workspace: string | null;
    max_spawn_depth: number;
    max_children_total: number;
    max_children_concurrent: number;
  };
  whatsapp: {
    mode: string;
    allowed_numbers: string[];
    auth_dir: string | null;
    bridge_host: string;
    bridge_port: number;
    callback_token: string;
    node_command: string;
  };
  search: {
    brave_api_key: string | null;
    firecrawl_api_key: string | null;
  };
  log_level: string;
  debug_prompts: boolean;
  heartbeat_interval_seconds: number;
  user_message_grace_seconds: number;
};

export type DashboardConfigResponse = {
  config: DashboardEditableConfig;
  providers: DashboardProviderOption[];
  worker_launcher: {
    configured: string;
    effective: string;
    available: boolean;
    reason: string;
    docker_image: string;
  };
  notes: string[];
};

export type DashboardConfigSaveResponse = {
  status: string;
  config: DashboardEditableConfig;
  providers: DashboardProviderOption[];
  worker_launcher: {
    configured: string;
    effective: string;
    available: boolean;
    reason: string;
    docker_image: string;
  };
};

export type DashboardProviderOption = {
  id: string;
  label: string;
  description: string;
  default_model: string;
  model_prefix: string | null;
  default_api_base: string | null;
  requires_api_key: boolean;
  supports_custom_base_url: boolean;
  supports_custom_model: boolean;
  supports_model_prefix_override: boolean;
  always_prefix_model: boolean;
  api_key_label: string;
  model_label: string;
  base_url_label: string;
};

export type WorkerTemplate = {
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

export type DashboardSkill = {
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

export type DashboardSkillsResponse = {
  contract_version: string;
  count: number;
  registry_path: string;
  skills: DashboardSkill[];
  install: {
    supported_sources: string[];
    default_clawhub_site: string;
  };
};

export type DashboardSkillInstallPayload = {
  source: string;
  clawhub_site?: string;
};

export type DashboardSkillMutationResponse = {
  status: string;
  skill_id: string;
  skill: DashboardSkill;
};

export type DashboardSkillDeleteResponse = {
  status: string;
  skill_id: string;
  skills: DashboardSkillsResponse;
};

export type DashboardQueryParams = {
  windowMinutes: 15 | 60 | 240 | 1440;
  service: "all" | "gateway" | "octo" | "telegram" | "exec_run" | "mcp" | "workers";
  environment: "all" | "local" | "dev" | "staging" | "prod";
  last?: number;
  token?: string;
};

const defaultHeaders: HeadersInit = { "content-type": "application/json" };

function withQuery(path: string, params: DashboardQueryParams): string {
  const query = new URLSearchParams();
  query.set("window_minutes", String(params.windowMinutes));
  query.set("service", params.service);
  query.set("environment", params.environment);
  if (params.last !== undefined) {
    query.set("last", String(params.last));
  }
  return `${path}?${query.toString()}`;
}

async function fetchJson<T>(url: string, token?: string): Promise<T> {
  const headers: HeadersInit = token
    ? { ...defaultHeaders, "x-octopal-token": token }
    : defaultHeaders;
  const response = await fetch(url, { method: "GET", headers });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

async function mutateJson<T>(url: string, method: "POST" | "PUT" | "DELETE", token?: string, body?: unknown): Promise<T> {
  const headers: HeadersInit = token
    ? { ...defaultHeaders, "x-octopal-token": token }
    : defaultHeaders;
  const response = await fetch(url, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    const detail = await response.text();
    let parsedDetail = "";
    try {
      const parsed = JSON.parse(detail) as { detail?: unknown };
      if (typeof parsed.detail === "string" && parsed.detail.trim()) {
        parsedDetail = parsed.detail;
      }
    } catch {
      // Plain-text errors are common for failed local gateway requests.
    }
    if (parsedDetail) {
      throw new Error(parsedDetail);
    }
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function fetchOverview(params: DashboardQueryParams): Promise<OverviewResponse> {
  return fetchJson<OverviewResponse>(withQuery("/api/dashboard/v2/overview", params), params.token);
}

export async function fetchIncidents(params: DashboardQueryParams): Promise<IncidentsResponse> {
  return fetchJson<IncidentsResponse>(withQuery("/api/dashboard/v2/incidents", params), params.token);
}

export async function fetchOcto(params: DashboardQueryParams): Promise<OctoResponse> {
  return fetchJson<OctoResponse>(withQuery("/api/dashboard/v2/octo", params), params.token);
}

export async function fetchWorkers(params: DashboardQueryParams): Promise<WorkersResponse> {
  return fetchJson<WorkersResponse>(withQuery("/api/dashboard/v2/workers", params), params.token);
}

export async function fetchSystem(params: DashboardQueryParams): Promise<SystemResponse> {
  return fetchJson<SystemResponse>(withQuery("/api/dashboard/v2/system", params), params.token);
}

export async function fetchDashboardConfig(token?: string): Promise<DashboardConfigResponse> {
  return fetchJson<DashboardConfigResponse>("/api/dashboard/config", token);
}

export async function updateDashboardConfig(
  payload: DashboardEditableConfig,
  token?: string,
): Promise<DashboardConfigSaveResponse> {
  return mutateJson<DashboardConfigSaveResponse>("/api/dashboard/config", "PUT", token, payload);
}

export async function requestSelfUpdate(token?: string): Promise<{ status: string; message?: string }> {
  return mutateJson<{ status: string; message?: string }>(
    "/api/dashboard/actions",
    "POST",
    token,
    {
      action: "request_self_update",
      confirm: true,
      reason: "Apply latest Octopal release from dashboard.",
      requested_by: "dashboard",
    },
  );
}

export async function fetchWorkerTemplates(token?: string): Promise<WorkerTemplate[]> {
  const payload = await fetchJson<{ count: number; templates: WorkerTemplate[] }>("/api/dashboard/worker-templates", token);
  return payload.templates ?? [];
}

export async function createWorkerTemplate(payload: WorkerTemplate, token?: string): Promise<WorkerTemplate> {
  const response = await mutateJson<{ status: string; template: WorkerTemplate }>(
    "/api/dashboard/worker-templates",
    "POST",
    token,
    payload,
  );
  return response.template;
}

export async function updateWorkerTemplate(payload: WorkerTemplate, token?: string): Promise<WorkerTemplate> {
  const response = await mutateJson<{ status: string; template: WorkerTemplate }>(
    `/api/dashboard/worker-templates/${encodeURIComponent(payload.id)}`,
    "PUT",
    token,
    payload,
  );
  return response.template;
}

export async function deleteWorkerTemplate(templateId: string, token?: string): Promise<void> {
  await mutateJson<{ status: string }>(
    `/api/dashboard/worker-templates/${encodeURIComponent(templateId)}`,
    "DELETE",
    token,
  );
}

export async function fetchSkills(token?: string): Promise<DashboardSkillsResponse> {
  return fetchJson<DashboardSkillsResponse>("/api/dashboard/skills", token);
}

export async function installSkill(
  payload: DashboardSkillInstallPayload,
  token?: string,
): Promise<DashboardSkillMutationResponse> {
  return mutateJson<DashboardSkillMutationResponse>("/api/dashboard/skills/install", "POST", token, payload);
}

export async function setSkillEnabled(
  skillId: string,
  enabled: boolean,
  token?: string,
): Promise<DashboardSkillMutationResponse> {
  return mutateJson<DashboardSkillMutationResponse>(
    `/api/dashboard/skills/${encodeURIComponent(skillId)}/${enabled ? "enable" : "disable"}`,
    "POST",
    token,
  );
}

export async function deleteSkill(skillId: string, token?: string): Promise<DashboardSkillDeleteResponse> {
  return mutateJson<DashboardSkillDeleteResponse>(
    `/api/dashboard/skills/${encodeURIComponent(skillId)}`,
    "DELETE",
    token,
  );
}
