import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useOutletContext } from "react-router";

import {
  fetchDashboardConfig,
  fetchSystem,
  requestSelfUpdate,
  updateDashboardConfig,
  type DashboardConfigResponse,
  type DashboardEditableConfig,
  type DashboardProviderOption,
} from "../api/dashboardClient";
import type { components } from "../api/types";
import type { AppShellOutletContext } from "../ui/AppShell";
import { formatLocalDateTime } from "../utils/dateTime";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";

type SystemPayload = components["schemas"]["DashboardSystemV2"];
type ServiceItem = { id?: string; name?: string; status?: string; reason?: string; updated_at?: string };
type LogItem = { timestamp?: string; level?: string; event?: string; service?: string };
type Connectivity = {
  mcp_servers?: Record<string, { status?: string; tool_count?: number; name?: string; reason?: string; transport?: string; reconnect_attempts?: number; error?: string | null }>;
};
type UpdateStatus = {
  status?: string;
  local_version?: string;
  latest_version?: string | null;
  release_url?: string | null;
  update_available?: boolean;
  can_update?: boolean;
  git_blocker?: string | null;
  repo?: string;
};
type SchedulerMetrics = {
  running?: boolean;
  interval_seconds?: number;
  max_tasks?: number;
  last_tick_status?: string;
  last_due_count?: number;
  last_dispatch_started?: number;
  last_dispatch_completed?: number;
  last_dispatch_duplicates?: number;
  last_dispatch_rejected_by_policy?: number;
  last_dispatch_errors?: number;
  last_policy_reasons?: Record<string, number>;
  ticks_total?: number;
  started_total?: number;
  completed_total?: number;
  duplicates_total?: number;
  rejected_by_policy_total?: number;
  errors_total?: number;
  failures_total?: number;
  updated_at?: string;
};

type FormState = {
  user_channel: string;
  log_level: string;
  debug_prompts: boolean;
  heartbeat_interval_seconds: string;
  user_message_grace_seconds: string;
  state_dir: string;
  workspace_dir: string;
  memory_top_k: string;
  memory_prefilter_k: string;
  memory_min_score: string;
  memory_max_chars: string;
  memory_owner_id: string;
  gateway_host: string;
  gateway_port: string;
  gateway_tailscale_ips: string;
  gateway_dashboard_token: string;
  gateway_tailscale_auto_serve: boolean;
  gateway_webapp_enabled: boolean;
  gateway_webapp_dist_dir: string;
  workers_launcher: string;
  workers_docker_image: string;
  workers_docker_workspace: string;
  workers_docker_host_workspace: string;
  workers_max_spawn_depth: string;
  workers_max_children_total: string;
  workers_max_children_concurrent: string;
  telegram_bot_token: string;
  telegram_allowed_chat_ids: string;
  telegram_parse_mode: string;
  whatsapp_mode: string;
  whatsapp_allowed_numbers: string;
  whatsapp_auth_dir: string;
  whatsapp_bridge_host: string;
  whatsapp_bridge_port: string;
  whatsapp_callback_token: string;
  whatsapp_node_command: string;
  llm_provider_id: string;
  llm_model: string;
  llm_api_key: string;
  llm_api_base: string;
  llm_model_prefix: string;
  worker_llm_enabled: boolean;
  worker_llm_provider_id: string;
  worker_llm_model: string;
  worker_llm_api_key: string;
  worker_llm_api_base: string;
  worker_llm_model_prefix: string;
  litellm_num_retries: string;
  litellm_timeout: string;
  litellm_fallbacks: string;
  litellm_drop_params: boolean;
  litellm_caching: boolean;
  litellm_max_concurrency: string;
  litellm_rate_limit_max_retries: string;
  litellm_rate_limit_base_delay_seconds: string;
  litellm_rate_limit_max_delay_seconds: string;
  search_brave_api_key: string;
  search_firecrawl_api_key: string;
};

const panel = "rounded-[30px] border border-white/6 bg-[var(--surface-panel)] p-5 shadow-[0_24px_80px_rgba(0,0,0,0.26)]";
const inputClass = "h-10 rounded-[18px] border-white/8 bg-[var(--surface-panel-strong)] px-3 text-[var(--text-strong)] placeholder:text-[var(--text-dim)]";
const textareaClass = "rounded-[18px] border-white/8 bg-[var(--surface-panel-strong)] px-3 py-2.5 text-[var(--text-strong)] placeholder:text-[var(--text-dim)]";
const selectTriggerClass = "h-10 w-full rounded-[18px] border-white/8 bg-[var(--surface-panel-strong)] px-3 text-[var(--text-strong)]";
const editorPanel = "rounded-[26px] border border-white/6 bg-[var(--surface-panel-strong)] p-5";

function statusTone(status?: string): string {
  const v = String(status ?? "").toLowerCase();
  if (v === "ok" || v === "connected" || v === "running") return "border-emerald-400/20 bg-emerald-400/10 text-emerald-200";
  if (v === "warning" || v === "reconnecting" || v === "configured") return "border-amber-300/20 bg-amber-300/10 text-amber-100";
  return "border-rose-300/20 bg-rose-400/10 text-rose-100";
}

const listToText = (value: string[]) => value.join("\n");
const parseList = (value: string) => value.split(/\r?\n|,/).map((item) => item.trim()).filter(Boolean);
const maybe = (value: string) => (value.trim() ? value.trim() : null);

function hasWorkerOverride(config: DashboardEditableConfig): boolean {
  return Boolean(
    config.worker_llm_default.provider_id ||
      config.worker_llm_default.model ||
      config.worker_llm_default.api_key ||
      config.worker_llm_default.api_base ||
      config.worker_llm_default.model_prefix,
  );
}

function buildForm(config: DashboardEditableConfig): FormState {
  return {
    user_channel: config.user_channel,
    log_level: config.log_level,
    debug_prompts: config.debug_prompts,
    heartbeat_interval_seconds: String(config.heartbeat_interval_seconds),
    user_message_grace_seconds: String(config.user_message_grace_seconds),
    state_dir: config.storage.state_dir,
    workspace_dir: config.storage.workspace_dir,
    memory_top_k: String(config.memory.top_k),
    memory_prefilter_k: String(config.memory.prefilter_k),
    memory_min_score: String(config.memory.min_score),
    memory_max_chars: String(config.memory.max_chars),
    memory_owner_id: config.memory.owner_id,
    gateway_host: config.gateway.host,
    gateway_port: String(config.gateway.port),
    gateway_tailscale_ips: config.gateway.tailscale_ips,
    gateway_dashboard_token: config.gateway.dashboard_token,
    gateway_tailscale_auto_serve: config.gateway.tailscale_auto_serve,
    gateway_webapp_enabled: config.gateway.webapp_enabled,
    gateway_webapp_dist_dir: config.gateway.webapp_dist_dir ?? "",
    workers_launcher: config.workers.launcher,
    workers_docker_image: config.workers.docker_image,
    workers_docker_workspace: config.workers.docker_workspace,
    workers_docker_host_workspace: config.workers.docker_host_workspace ?? "",
    workers_max_spawn_depth: String(config.workers.max_spawn_depth),
    workers_max_children_total: String(config.workers.max_children_total),
    workers_max_children_concurrent: String(config.workers.max_children_concurrent),
    telegram_bot_token: config.telegram.bot_token,
    telegram_allowed_chat_ids: listToText(config.telegram.allowed_chat_ids),
    telegram_parse_mode: config.telegram.parse_mode,
    whatsapp_mode: config.whatsapp.mode,
    whatsapp_allowed_numbers: listToText(config.whatsapp.allowed_numbers),
    whatsapp_auth_dir: config.whatsapp.auth_dir ?? "",
    whatsapp_bridge_host: config.whatsapp.bridge_host,
    whatsapp_bridge_port: String(config.whatsapp.bridge_port),
    whatsapp_callback_token: config.whatsapp.callback_token,
    whatsapp_node_command: config.whatsapp.node_command,
    llm_provider_id: config.llm.provider_id ?? "",
    llm_model: config.llm.model ?? "",
    llm_api_key: config.llm.api_key ?? "",
    llm_api_base: config.llm.api_base ?? "",
    llm_model_prefix: config.llm.model_prefix ?? "",
    worker_llm_enabled: hasWorkerOverride(config),
    worker_llm_provider_id: config.worker_llm_default.provider_id ?? "",
    worker_llm_model: config.worker_llm_default.model ?? "",
    worker_llm_api_key: config.worker_llm_default.api_key ?? "",
    worker_llm_api_base: config.worker_llm_default.api_base ?? "",
    worker_llm_model_prefix: config.worker_llm_default.model_prefix ?? "",
    litellm_num_retries: String(config.litellm.num_retries),
    litellm_timeout: String(config.litellm.timeout),
    litellm_fallbacks: config.litellm.fallbacks ?? "",
    litellm_drop_params: config.litellm.drop_params,
    litellm_caching: config.litellm.caching,
    litellm_max_concurrency: String(config.litellm.max_concurrency),
    litellm_rate_limit_max_retries: String(config.litellm.rate_limit_max_retries),
    litellm_rate_limit_base_delay_seconds: String(config.litellm.rate_limit_base_delay_seconds),
    litellm_rate_limit_max_delay_seconds: String(config.litellm.rate_limit_max_delay_seconds),
    search_brave_api_key: config.search.brave_api_key ?? "",
    search_firecrawl_api_key: config.search.firecrawl_api_key ?? "",
  };
}

function toPayload(form: FormState): DashboardEditableConfig {
  return {
    user_channel: form.user_channel,
    telegram: { bot_token: form.telegram_bot_token.trim(), allowed_chat_ids: parseList(form.telegram_allowed_chat_ids), parse_mode: form.telegram_parse_mode.trim() || "MarkdownV2" },
    llm: { provider_id: maybe(form.llm_provider_id), model: maybe(form.llm_model), api_key: maybe(form.llm_api_key), api_base: maybe(form.llm_api_base), model_prefix: maybe(form.llm_model_prefix) },
    worker_llm_default: form.worker_llm_enabled
      ? { provider_id: maybe(form.worker_llm_provider_id), model: maybe(form.worker_llm_model), api_key: maybe(form.worker_llm_api_key), api_base: maybe(form.worker_llm_api_base), model_prefix: maybe(form.worker_llm_model_prefix) }
      : { provider_id: null, model: null, api_key: null, api_base: null, model_prefix: null },
    litellm: { num_retries: Number(form.litellm_num_retries || 0), timeout: Number(form.litellm_timeout || 0), fallbacks: maybe(form.litellm_fallbacks), drop_params: form.litellm_drop_params, caching: form.litellm_caching, max_concurrency: Number(form.litellm_max_concurrency || 0), rate_limit_max_retries: Number(form.litellm_rate_limit_max_retries || 0), rate_limit_base_delay_seconds: Number(form.litellm_rate_limit_base_delay_seconds || 0), rate_limit_max_delay_seconds: Number(form.litellm_rate_limit_max_delay_seconds || 0) },
    storage: { state_dir: form.state_dir.trim(), workspace_dir: form.workspace_dir.trim() },
    memory: { top_k: Number(form.memory_top_k || 0), prefilter_k: Number(form.memory_prefilter_k || 0), min_score: Number(form.memory_min_score || 0), max_chars: Number(form.memory_max_chars || 0), owner_id: form.memory_owner_id.trim() },
    gateway: { host: form.gateway_host.trim(), port: Number(form.gateway_port || 0), tailscale_ips: form.gateway_tailscale_ips.trim(), dashboard_token: form.gateway_dashboard_token.trim(), tailscale_auto_serve: form.gateway_tailscale_auto_serve, webapp_enabled: form.gateway_webapp_enabled, webapp_dist_dir: maybe(form.gateway_webapp_dist_dir) },
    workers: { launcher: form.workers_launcher.trim(), docker_image: form.workers_docker_image.trim(), docker_workspace: form.workers_docker_workspace.trim(), docker_host_workspace: maybe(form.workers_docker_host_workspace), max_spawn_depth: Number(form.workers_max_spawn_depth || 0), max_children_total: Number(form.workers_max_children_total || 0), max_children_concurrent: Number(form.workers_max_children_concurrent || 0) },
    whatsapp: { mode: form.whatsapp_mode.trim(), allowed_numbers: parseList(form.whatsapp_allowed_numbers), auth_dir: maybe(form.whatsapp_auth_dir), bridge_host: form.whatsapp_bridge_host.trim(), bridge_port: Number(form.whatsapp_bridge_port || 0), callback_token: form.whatsapp_callback_token.trim(), node_command: form.whatsapp_node_command.trim() },
    search: { brave_api_key: maybe(form.search_brave_api_key), firecrawl_api_key: maybe(form.search_firecrawl_api_key) },
    log_level: form.log_level,
    debug_prompts: form.debug_prompts,
    heartbeat_interval_seconds: Number(form.heartbeat_interval_seconds || 0),
    user_message_grace_seconds: Number(form.user_message_grace_seconds || 0),
  };
}

function applyProviderDefaults(
  current: FormState,
  provider: DashboardProviderOption,
  scope: "main" | "worker",
): FormState {
  if (scope === "main") {
    return {
      ...current,
      llm_provider_id: provider.id,
      llm_model: provider.default_model ?? "",
      llm_api_base: provider.default_api_base ?? "",
      llm_model_prefix: provider.model_prefix ?? "",
      llm_api_key: "",
    };
  }
  return {
    ...current,
    worker_llm_provider_id: provider.id,
    worker_llm_model: provider.default_model ?? "",
    worker_llm_api_base: provider.default_api_base ?? "",
    worker_llm_model_prefix: provider.model_prefix ?? "",
    worker_llm_api_key: "",
  };
}

const FALLBACK_PROVIDERS: DashboardProviderOption[] = [
  {
    id: "openrouter",
    label: "OpenRouter",
    description: "Hosted model router with OpenRouter model ids.",
    default_model: "anthropic/claude-sonnet-4",
    model_prefix: "openrouter",
    default_api_base: "https://openrouter.ai/api/v1",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: true,
    api_key_label: "OpenRouter API key",
    model_label: "OpenRouter model",
    base_url_label: "OpenRouter base URL",
  },
  {
    id: "zai",
    label: "Z.ai (Coding plan)",
    description: "GLM and Coding Plan endpoints via OpenAI-compatible LiteLLM routing.",
    default_model: "glm-5",
    model_prefix: "openai",
    default_api_base: "https://api.z.ai/api/coding/paas/v4",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Z.ai API key",
    model_label: "Z.ai model",
    base_url_label: "Z.ai base URL",
  },
  {
    id: "openai",
    label: "OpenAI",
    description: "Direct OpenAI API access through LiteLLM.",
    default_model: "gpt-4.1-mini",
    model_prefix: "openai",
    default_api_base: "https://api.openai.com/v1",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "OpenAI API key",
    model_label: "OpenAI model",
    base_url_label: "OpenAI base URL",
  },
  {
    id: "anthropic",
    label: "Anthropic",
    description: "Direct Anthropic Messages API through LiteLLM.",
    default_model: "claude-sonnet-4-20250514",
    model_prefix: "anthropic",
    default_api_base: "https://api.anthropic.com",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Anthropic API key",
    model_label: "Anthropic model",
    base_url_label: "Anthropic base URL",
  },
  {
    id: "google",
    label: "Google Gemini",
    description: "Gemini API via LiteLLM.",
    default_model: "gemini-2.0-flash",
    model_prefix: "gemini",
    default_api_base: null,
    requires_api_key: true,
    supports_custom_base_url: false,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Gemini API key",
    model_label: "Gemini model",
    base_url_label: "Base URL",
  },
  {
    id: "mistral",
    label: "Mistral AI",
    description: "Hosted Mistral API.",
    default_model: "mistral-medium-latest",
    model_prefix: "mistral",
    default_api_base: "https://api.mistral.ai/v1",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Mistral API key",
    model_label: "Mistral model",
    base_url_label: "Mistral base URL",
  },
  {
    id: "together",
    label: "Together AI",
    description: "Hosted open-model access through Together AI.",
    default_model: "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    model_prefix: "together_ai",
    default_api_base: "https://api.together.xyz/v1",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Together API key",
    model_label: "Together model",
    base_url_label: "Together base URL",
  },
  {
    id: "groq",
    label: "Groq",
    description: "Fast hosted inference with OpenAI-compatible API surface.",
    default_model: "llama-3.3-70b-versatile",
    model_prefix: "groq",
    default_api_base: "https://api.groq.com/openai/v1",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Groq API key",
    model_label: "Groq model",
    base_url_label: "Groq base URL",
  },
  {
    id: "ollama",
    label: "Ollama",
    description: "Local Ollama instance using the OpenAI-compatible bridge.",
    default_model: "llama3.2",
    model_prefix: "ollama",
    default_api_base: "http://localhost:11434",
    requires_api_key: false,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Ollama API key (optional)",
    model_label: "Ollama model",
    base_url_label: "Ollama base URL",
  },
  {
    id: "minimax",
    label: "Minimax (Token plan)",
    description: "MiniMax API (M2.5, M2.7, etc.) via LiteLLM.",
    default_model: "minimax-m2.5",
    model_prefix: "minimax",
    default_api_base: "https://api.minimax.io/anthropic/v1",
    requires_api_key: true,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: false,
    always_prefix_model: false,
    api_key_label: "Minimax API key",
    model_label: "Minimax model",
    base_url_label: "Minimax base URL",
  },
  {
    id: "custom",
    label: "Custom OpenAI-compatible",
    description: "Any custom LiteLLM target with configurable base URL and model prefix.",
    default_model: "gpt-4.1-mini",
    model_prefix: "openai",
    default_api_base: "http://localhost:8000/v1",
    requires_api_key: false,
    supports_custom_base_url: true,
    supports_custom_model: true,
    supports_model_prefix_override: true,
    always_prefix_model: false,
    api_key_label: "API key (optional)",
    model_label: "Model name",
    base_url_label: "Base URL",
  },
];

function L({ label, children, hint }: { label: string; children: ReactNode; hint?: string }) {
  return (
    <label className="grid min-h-[112px] grid-rows-[auto_auto_1fr] gap-1.5 text-sm text-[var(--text-strong)]">
      <span className="text-[11px] uppercase tracking-[0.18em] text-white/92">{label}</span>
      {children}
      <span className="text-xs leading-5 text-[var(--text-dim)]">{hint ?? ""}</span>
    </label>
  );
}

function H({ title, text }: { title: string; text: string }) {
  return <div className="mb-4"><p className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-dim)]">{title}</p><p className="mt-2 text-sm text-[var(--text-muted)]">{text}</p></div>;
}

function FormInput(props: React.ComponentProps<typeof Input>) {
  return <Input {...props} className={[inputClass, props.className].filter(Boolean).join(" ")} />;
}

function FormTextarea(props: React.ComponentProps<typeof Textarea>) {
  return <Textarea {...props} className={[textareaClass, props.className].filter(Boolean).join(" ")} />;
}

function FormSelect({
  value,
  onValueChange,
  options,
  placeholder,
}: {
  value: string;
  onValueChange: (value: string) => void;
  options: Array<{ value: string; label: string }>;
  placeholder?: string;
}) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger className={selectTriggerClass}>
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        {options.map((option) => (
          <SelectItem key={option.value} value={option.value}>
            {option.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

function ToggleField({
  label,
  checked,
  onChange,
  hint,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
  hint?: string;
}) {
  return (
    <label className="grid gap-1 rounded-[18px] border border-white/6 bg-black/20 px-3 py-3 text-sm text-[var(--text-strong)]">
      <span className="flex items-center gap-3">
        <input
          type="checkbox"
          checked={checked}
          onChange={(event) => onChange(event.target.checked)}
          className="h-4 w-4 rounded border-white/20 bg-transparent"
        />
        {label}
      </span>
      {hint ? <span className="pl-7 text-xs text-[var(--text-dim)]">{hint}</span> : null}
    </label>
  );
}

function SectionCard({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <section className={editorPanel}>
      <H title={title} text={description} />
      {children}
    </section>
  );
}

function SummaryPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-full border border-white/8 bg-black/20 px-3 py-1.5 text-xs text-[var(--text-muted)]">
      <span className="uppercase tracking-[0.18em] text-[var(--text-dim)]">{label}</span>
      <span className="ml-2 text-[var(--text-strong)]">{value}</span>
    </div>
  );
}

function FieldsGrid({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={`grid items-start gap-4 lg:grid-cols-2 xl:grid-cols-4 ${className}`.trim()}>{children}</div>;
}

function Disclosure({
  title,
  description,
  children,
  defaultOpen = false,
}: {
  title: string;
  description?: string;
  children: ReactNode;
  defaultOpen?: boolean;
}) {
  return (
    <details open={defaultOpen} className="rounded-[22px] border border-white/6 bg-black/20 px-4 py-4">
      <summary className="cursor-pointer list-none">
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="text-sm font-medium text-white">{title}</p>
            {description ? <p className="mt-1 text-sm text-[var(--text-muted)]">{description}</p> : null}
          </div>
          <span className="text-xs uppercase tracking-[0.18em] text-[var(--text-dim)]">Toggle</span>
        </div>
      </summary>
      <div className="mt-4">{children}</div>
    </details>
  );
}

export function SystemPage() {
  const { filters } = useOutletContext<AppShellOutletContext>();
  const [data, setData] = useState<SystemPayload | null>(null);
  const [configData, setConfigData] = useState<DashboardConfigResponse | null>(null);
  const [form, setForm] = useState<FormState | null>(null);
  const [loading, setLoading] = useState(true);
  const [configLoading, setConfigLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [requestingUpdate, setRequestingUpdate] = useState(false);
  const [error, setError] = useState("");
  const [configError, setConfigError] = useState("");
  const [notice, setNotice] = useState("");

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    void fetchSystem({
      windowMinutes: filters.windowMinutes,
      service: filters.service,
      environment: filters.environment,
      token: filters.token || undefined,
    })
      .then((payload) => {
        if (active) setData(payload);
      })
      .catch((err: unknown) => {
        if (active) setError(err instanceof Error ? err.message : "Unknown request error");
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [filters.environment, filters.service, filters.token, filters.windowMinutes]);

  useEffect(() => {
    let active = true;
    setConfigLoading(true);
    setConfigError("");
    void fetchDashboardConfig(filters.token || undefined)
      .then((payload) => {
        if (!active) return;
        setConfigData(payload);
        setForm(buildForm(payload.config));
      })
      .catch((err: unknown) => {
        if (active) setConfigError(err instanceof Error ? err.message : "Failed to load config");
      })
      .finally(() => {
        if (active) setConfigLoading(false);
      });
    return () => {
      active = false;
    };
  }, [filters.token]);

  const initialForm = useMemo(() => (configData ? buildForm(configData.config) : null), [configData]);
  const providerOptions = configData?.providers?.length ? configData.providers : FALLBACK_PROVIDERS;
  const providerOptionsForSelect = useMemo(
    () => providerOptions.map((provider) => ({ value: provider.id, label: provider.label })),
    [providerOptions],
  );
  const providerMap = useMemo(
    () => new Map(providerOptions.map((provider) => [provider.id, provider])),
    [providerOptions],
  );
  const isDirty = Boolean(form && initialForm && JSON.stringify(form) !== JSON.stringify(initialForm));
  const selectedChannel = form?.user_channel ?? "telegram";
  const showTelegram = selectedChannel === "telegram";
  const showWhatsApp = selectedChannel === "whatsapp";
  const useDockerLauncher = (form?.workers_launcher ?? "docker") === "docker";
  const useSeparateWorkerInference = Boolean(form?.worker_llm_enabled);
  const useSeparateWhatsAppBridge = (form?.whatsapp_mode ?? "separate") === "separate";
  const mainProvider = form ? providerMap.get(form.llm_provider_id) ?? null : null;
  const workerProvider = form ? providerMap.get(form.worker_llm_provider_id) ?? null : null;

  useEffect(() => {
    if (!form || providerOptions.length === 0) {
      return;
    }
    const preferredProvider = providerMap.get("zai") ?? providerOptions[0] ?? null;
    if (!preferredProvider) {
      return;
    }

    let nextForm = form;
    let changed = false;

    if (!form.llm_provider_id || !providerMap.has(form.llm_provider_id)) {
      nextForm = applyProviderDefaults(nextForm, preferredProvider, "main");
      changed = true;
    }

    if (
      form.worker_llm_enabled &&
      (!form.worker_llm_provider_id || !providerMap.has(form.worker_llm_provider_id))
    ) {
      nextForm = applyProviderDefaults(nextForm, preferredProvider, "worker");
      changed = true;
    }

    if (changed) {
      setForm(nextForm);
    }
  }, [form, providerMap, providerOptions]);

  if (loading) {
    return <section className={panel}><h2 className="text-2xl font-semibold text-white">System</h2><p className="mt-2 text-sm text-[var(--text-muted)]">Loading system diagnostics...</p></section>;
  }

  if (error) {
    return <section className="rounded-[30px] border border-rose-400/30 bg-rose-950/20 p-8 text-rose-100"><h2 className="text-2xl font-semibold text-white">System</h2><p className="mt-2 text-sm">Failed to load system diagnostics: {error}</p></section>;
  }

  const system = (data?.system ?? {}) as {
    running?: boolean;
    pid?: number;
    uptime?: string;
    active_channel?: string;
    worker_launcher?: { configured?: string; effective?: string; available?: boolean; reason?: string };
    scheduler?: SchedulerMetrics;
    update?: UpdateStatus;
  };
  const services = ((data?.services ?? []) as ServiceItem[]).slice(0, 10);
  const logs = ((data?.logs ?? []) as LogItem[]).slice(0, 10);
  const connectivity = (data?.connectivity ?? {}) as Connectivity;
  const mcpServers = connectivity.mcp_servers ?? {};
  const scheduler = (system.scheduler ?? {}) as SchedulerMetrics;
  const updateStatus = (system.update ?? {}) as UpdateStatus;
  const updateAvailable = Boolean(updateStatus.update_available);
  const updateBlocked = updateAvailable && !Boolean(updateStatus.can_update);
  const schedulerAvailable = Object.keys(scheduler).length > 0;
  const schedulerBadgeStatus = !schedulerAvailable
    ? "ok"
    : scheduler.running
      ? (scheduler.last_tick_status === "failed" ? "critical" : "running")
      : "warning";
  const schedulerBadgeLabel = !schedulerAvailable
    ? "not reporting"
    : scheduler.running
      ? (scheduler.last_tick_status ?? "running")
      : "stopped";
  const launcher = configData?.worker_launcher ?? { configured: "n/a", effective: "n/a", available: false, reason: "No data", docker_image: "n/a" };
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) => setForm((current) => (current ? { ...current, [key]: value } : current));

  function setProvider(scope: "main" | "worker", providerId: string): void {
    const provider = providerMap.get(providerId);
    if (!provider) {
      return;
    }
    setForm((current) => {
      if (!current) return current;
      return applyProviderDefaults(current, provider, scope);
    });
  }

  async function save(): Promise<void> {
    if (!form) return;
    setSaving(true);
    setNotice("");
    setConfigError("");
    try {
      const payload = await updateDashboardConfig(toPayload(form), filters.token || undefined);
      setConfigData((current) => ({
        config: payload.config,
        providers: payload.providers,
        worker_launcher: payload.worker_launcher,
        notes: current?.notes ?? [],
      }));
      setForm(buildForm(payload.config));
      setNotice("Config saved to config.json.");
    } catch (err: unknown) {
      setConfigError(err instanceof Error ? err.message : "Failed to save config");
    } finally {
      setSaving(false);
    }
  }

  function cancelEdits(): void {
    if (!initialForm) return;
    setForm(initialForm);
    setNotice("");
    setConfigError("");
  }

  async function startSelfUpdate(): Promise<void> {
    setRequestingUpdate(true);
    setNotice("");
    setConfigError("");
    try {
      const result = await requestSelfUpdate(filters.token || undefined);
      setNotice(result.message || "Self-update requested.");
    } catch (err: unknown) {
      setConfigError(err instanceof Error ? err.message : "Failed to request update");
    } finally {
      setRequestingUpdate(false);
    }
  }

  return (
    <section className="grid gap-5">
      <section className={panel}>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-dim)]">System</p>
            <h2 className="mt-2 text-3xl font-semibold tracking-[-0.03em] text-white">{system.running ? "Runtime is healthy" : "Runtime is degraded"}</h2>
            <p className="mt-3 text-sm text-[var(--text-muted)]">PID {system.pid ?? "n/a"} | Channel {system.active_channel ?? "n/a"} | Uptime {system.uptime ?? "n/a"}</p>
          </div>
          <div className={`rounded-full border px-3 py-1.5 text-[11px] font-medium uppercase tracking-[0.16em] ${statusTone(system.running ? "running" : "critical")}`}>{system.running ? "running" : "down"}</div>
        </div>
      </section>

      {updateAvailable ? (
        <section className="rounded-[26px] border border-amber-300/30 bg-amber-500/10 p-5">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="text-[11px] uppercase tracking-[0.18em] text-amber-100/70">Update available</p>
              <h3 className="mt-2 text-xl font-semibold text-white">
                Octopal v{updateStatus.latest_version ?? "latest"} is ready
              </h3>
              <p className="mt-2 text-sm text-amber-50/75">
                Current v{updateStatus.local_version ?? "unknown"} | {updateStatus.repo ?? "release repo"}
                {updateBlocked && updateStatus.git_blocker ? ` | ${updateStatus.git_blocker}` : ""}
              </p>
            </div>
            <Button
              type="button"
              variant="secondary"
              onClick={() => void startSelfUpdate()}
              disabled={requestingUpdate || updateBlocked}
              className="rounded-2xl bg-amber-200 text-slate-950 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {requestingUpdate ? "Requesting..." : "Update & restart"}
            </Button>
          </div>
        </section>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-3">
        <article className={panel}>
          <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">Service health</h3>
          <div className="mt-4 space-y-3">
            {services.length === 0 ? <p className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4 text-sm text-[var(--text-muted)]">No services.</p> : services.map((service) => (
              <article key={service.id ?? service.name} className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div><h4 className="text-base font-semibold text-white">{service.name ?? "Service"}</h4><p className="mt-1 text-sm text-[var(--text-muted)]">{service.reason ?? "No reason"}</p></div>
                  <div className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-wide ${statusTone(service.status)}`}>{String(service.status ?? "unknown")}</div>
                </div>
              </article>
            ))}
          </div>
        </article>

        <article className={panel}>
          <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">MCP connectivity</h3>
          <div className="mt-4 space-y-3">
            {Object.keys(mcpServers).length === 0 ? <p className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4 text-sm text-[var(--text-muted)]">No MCP servers configured.</p> : Object.entries(mcpServers).map(([key, value]) => (
              <article key={key} className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h4 className="text-base font-semibold text-white">{value.name ?? key}</h4>
                    <p className="mt-1 text-sm text-[var(--text-muted)]">Available tools: {value.tool_count ?? 0} | Transport: {value.transport ?? "auto"}</p>
                    <p className="mt-1 text-sm text-[var(--text-dim)]">{value.reason ?? "No detail available"}{typeof value.reconnect_attempts === "number" && value.reconnect_attempts > 0 ? ` | Retry attempts: ${value.reconnect_attempts}` : ""}</p>
                    {value.error ? <p className="mt-1 text-sm text-rose-200">{value.error}</p> : null}
                  </div>
                  <div className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-wide ${statusTone(value.status)}`}>{String(value.status ?? "unknown")}</div>
                </div>
              </article>
            ))}
          </div>
        </article>

        <article className={panel}>
          <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">Launcher status</h3>
          <div className="mt-4 space-y-3">
            <div className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4"><div className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Configured</div><div className="mt-2 text-lg font-semibold text-white">{launcher.configured}</div></div>
            <div className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4"><div className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Effective</div><div className="mt-2 text-lg font-semibold text-white">{launcher.effective}</div></div>
            <div className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4"><div className="flex items-center justify-between gap-3"><div className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Availability</div><span className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-wide ${statusTone(launcher.available ? "running" : "warning")}`}>{launcher.available ? "ready" : "needs attention"}</span></div><p className="mt-3 text-sm text-[var(--text-muted)]">{launcher.reason}</p></div>
          </div>
        </article>

        <article className={panel}>
          <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">Scheduler</h3>
          <div className="mt-4 space-y-3">
            <div className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4">
              <div className="flex items-center justify-between gap-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Loop status</div>
                <span className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-wide ${statusTone(schedulerBadgeStatus)}`}>{schedulerBadgeLabel}</span>
              </div>
              <p className="mt-3 text-sm text-[var(--text-muted)]">
                {schedulerAvailable
                  ? `Interval ${scheduler.interval_seconds ?? "n/a"}s | Max tasks ${scheduler.max_tasks ?? "n/a"} | Updated ${scheduler.updated_at ? formatLocalDateTime(scheduler.updated_at) : "n/a"}`
                  : "Scheduler metrics are not reporting yet."}
              </p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4"><div className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Last tick</div><div className="mt-2 text-lg font-semibold text-white">{scheduler.last_due_count ?? 0} due</div><p className="mt-2 text-sm text-[var(--text-muted)]">started {scheduler.last_dispatch_started ?? 0} | done {scheduler.last_dispatch_completed ?? 0} | dup {scheduler.last_dispatch_duplicates ?? 0} | policy {scheduler.last_dispatch_rejected_by_policy ?? 0} | errors {scheduler.last_dispatch_errors ?? 0}</p><p className="mt-2 text-xs text-[var(--text-dim)]">{Object.entries(scheduler.last_policy_reasons ?? {}).length > 0 ? Object.entries(scheduler.last_policy_reasons ?? {}).map(([reason, count]) => `${reason}: ${count}`).join(" | ") : "No policy rejections on last tick."}</p></div>
              <div className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4"><div className="text-[11px] uppercase tracking-[0.18em] text-[var(--text-dim)]">Cumulative</div><div className="mt-2 text-lg font-semibold text-white">{scheduler.ticks_total ?? 0} ticks</div><p className="mt-2 text-sm text-[var(--text-muted)]">started {scheduler.started_total ?? 0} | done {scheduler.completed_total ?? 0} | dup {scheduler.duplicates_total ?? 0} | policy {scheduler.rejected_by_policy_total ?? 0} | failures {scheduler.failures_total ?? 0}</p></div>
            </div>
          </div>
        </article>
      </div>

      <article className={panel}>
        <div className="flex flex-wrap items-start justify-between gap-4 border-b border-white/6 pb-4">
          <div>
            <p className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-dim)]">Config editor</p>
            <h3 className="mt-2 text-2xl font-semibold text-white">Edit `config.json` from the dashboard</h3>
            <p className="mt-2 max-w-3xl text-sm text-[var(--text-muted)]">
              Settings are grouped by topic, and options you do not need right now stay hidden until they become relevant.
            </p>
          </div>
        </div>

        {configLoading ? <p className="mt-5 text-sm text-[var(--text-muted)]">Loading structured config...</p> : null}
        {configError ? <div className="mt-5 rounded-2xl border border-rose-400/30 bg-rose-950/20 p-4 text-sm text-rose-100">{configError}</div> : null}
        {notice ? <div className="mt-5 rounded-2xl border border-emerald-400/20 bg-emerald-400/10 p-4 text-sm text-emerald-100">{notice}</div> : null}
        {configData?.notes?.length ? <div className="mt-5 grid gap-2">{configData.notes.map((note) => <div key={note} className="rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] px-4 py-3 text-sm text-[var(--text-muted)]">{note}</div>)}</div> : null}

        {form ? (
          <div className="mt-6 space-y-8">
            <div className="flex flex-wrap gap-2">
              <SummaryPill label="Channel" value={showTelegram ? "Telegram" : "WhatsApp"} />
              <SummaryPill label="Worker launcher" value={useDockerLauncher ? "Docker" : "Same app"} />
              <SummaryPill label="Worker inference" value={useSeparateWorkerInference ? "Separate profile" : "Use Octo defaults"} />
            </div>

            <SectionCard title="App basics" description="General app behavior, dashboard access, and how the web interface is served.">
              <FieldsGrid>
                <L label="Primary chat app" hint="Choose where the app should talk to people by default."><FormSelect value={form.user_channel} onValueChange={(value) => set("user_channel", value)} options={[{ value: "telegram", label: "Telegram" }, { value: "whatsapp", label: "WhatsApp" }]} /></L>
                <L label="Log detail level" hint="More detail helps with troubleshooting, less detail keeps logs cleaner."><FormSelect value={form.log_level} onValueChange={(value) => set("log_level", value)} options={[{ value: "DEBUG", label: "Debug" }, { value: "INFO", label: "Info" }, { value: "WARNING", label: "Warning" }, { value: "ERROR", label: "Error" }]} /></L>
                <L label="Heartbeat interval (seconds)" hint="How often the app checks in with its background loop."><FormInput value={form.heartbeat_interval_seconds} onChange={(e) => set("heartbeat_interval_seconds", e.target.value)} /></L>
                <L label="Reply grace period (seconds)" hint="How long the app waits before it starts processing a new message."><FormInput value={form.user_message_grace_seconds} onChange={(e) => set("user_message_grace_seconds", e.target.value)} /></L>
                <L label="Server host"><FormInput value={form.gateway_host} onChange={(e) => set("gateway_host", e.target.value)} /></L>
                <L label="Server port"><FormInput value={form.gateway_port} onChange={(e) => set("gateway_port", e.target.value)} /></L>
                <L label="Tailscale addresses" hint="Comma-separated addresses that should point to this app through Tailscale."><FormInput value={form.gateway_tailscale_ips} onChange={(e) => set("gateway_tailscale_ips", e.target.value)} /></L>
                {form.gateway_webapp_enabled ? <L label="Web app build folder" hint="Only needed when the built-in web interface is turned on."><FormInput value={form.gateway_webapp_dist_dir} onChange={(e) => set("gateway_webapp_dist_dir", e.target.value)} /></L> : null}
              </FieldsGrid>
              <div className="mt-4 grid gap-3 lg:grid-cols-3">
                <ToggleField label="Show detailed AI debug info" checked={form.debug_prompts} onChange={(checked) => set("debug_prompts", checked)} />
                <ToggleField label="Auto-publish through Tailscale" checked={form.gateway_tailscale_auto_serve} onChange={(checked) => set("gateway_tailscale_auto_serve", checked)} />
                <ToggleField label="Enable built-in web interface" checked={form.gateway_webapp_enabled} onChange={(checked) => set("gateway_webapp_enabled", checked)} />
              </div>
              <div className="mt-4">
                <Disclosure title="Access secrets" description="Sensitive values used to protect access.">
                  <FieldsGrid className="xl:grid-cols-2">
                    <L label="Dashboard access token"><FormInput type="password" value={form.gateway_dashboard_token} onChange={(e) => set("gateway_dashboard_token", e.target.value)} placeholder="Leave blank to keep current token" /></L>
                  </FieldsGrid>
                </Disclosure>
              </div>
            </SectionCard>

            <SectionCard title="Messaging app" description={showTelegram ? "Telegram is selected, so only Telegram settings are shown." : "WhatsApp is selected, so only WhatsApp settings are shown."}>
              {showTelegram ? (
                <div className="space-y-4">
                  <div className="grid gap-4 lg:grid-cols-2">
                    <L label="Message formatting mode" hint="Controls how bold text, links, and other formatting work in Telegram.">
                      <FormSelect
                        value={form.telegram_parse_mode}
                        onValueChange={(value) => set("telegram_parse_mode", value)}
                        options={[
                          { value: "MarkdownV2", label: "MarkdownV2" },
                          { value: "Markdown", label: "Markdown" },
                          { value: "HTML", label: "HTML" },
                        ]}
                      />
                    </L>
                  </div>
                  <Disclosure title="Telegram access" description="Credentials for connecting the app to Telegram.">
                    <FieldsGrid className="xl:grid-cols-2">
                      <L label="Bot token"><FormInput type="password" value={form.telegram_bot_token} onChange={(e) => set("telegram_bot_token", e.target.value)} placeholder="Leave blank to keep current token" /></L>
                    </FieldsGrid>
                  </Disclosure>
                  <L label="Allowed chat IDs" hint="Only these Telegram chats will be allowed. Enter one per line or comma-separated."><FormTextarea value={form.telegram_allowed_chat_ids} onChange={(e) => set("telegram_allowed_chat_ids", e.target.value)} rows={5} /></L>
                </div>
              ) : null}
              {showWhatsApp ? (
                <div className="space-y-4">
                  <FieldsGrid>
                    <L label="Connection mode" hint="Use separate bridge if WhatsApp runs as its own helper service."><FormSelect value={form.whatsapp_mode} onValueChange={(value) => set("whatsapp_mode", value)} options={[{ value: "separate", label: "Separate bridge" }, { value: "embedded", label: "Embedded" }]} /></L>
                    <L label="Login data folder" hint="Where WhatsApp session data is stored on disk."><FormInput value={form.whatsapp_auth_dir} onChange={(e) => set("whatsapp_auth_dir", e.target.value)} /></L>
                    <L label="Node command" hint="Command used to run the WhatsApp bridge process."><FormInput value={form.whatsapp_node_command} onChange={(e) => set("whatsapp_node_command", e.target.value)} /></L>
                  </FieldsGrid>
                  {useSeparateWhatsAppBridge ? (
                    <div className="grid items-start gap-4 lg:grid-cols-3">
                      <L label="Bridge host"><FormInput value={form.whatsapp_bridge_host} onChange={(e) => set("whatsapp_bridge_host", e.target.value)} /></L>
                      <L label="Bridge port"><FormInput value={form.whatsapp_bridge_port} onChange={(e) => set("whatsapp_bridge_port", e.target.value)} /></L>
                    </div>
                  ) : null}
                  <Disclosure title="WhatsApp access" description="Credentials used to connect and protect the WhatsApp bridge.">
                    <div className="grid gap-4 lg:grid-cols-2">
                      <L label="Callback token"><FormInput type="password" value={form.whatsapp_callback_token} onChange={(e) => set("whatsapp_callback_token", e.target.value)} placeholder="Leave blank to keep current token" /></L>
                    </div>
                  </Disclosure>
                  <L label="Allowed phone numbers" hint="Only these numbers will be allowed. Enter one per line or comma-separated."><FormTextarea value={form.whatsapp_allowed_numbers} onChange={(e) => set("whatsapp_allowed_numbers", e.target.value)} rows={5} /></L>
                </div>
              ) : null}
            </SectionCard>

            <SectionCard title="Worker setup" description="How helper workers are launched and how much parallel work they are allowed to create.">
              <FieldsGrid>
                <L label="How workers run" hint="Docker reveals container image and folder settings."><FormSelect value={form.workers_launcher} onValueChange={(value) => set("workers_launcher", value)} options={[{ value: "docker", label: "Docker" }, { value: "same_env", label: "Same app environment" }]} /></L>
                <L label="Max worker depth" hint="Limits how many generations of workers can create more workers."><FormInput value={form.workers_max_spawn_depth} onChange={(e) => set("workers_max_spawn_depth", e.target.value)} /></L>
                <L label="Max total child workers" hint="Total number of helper workers one worker can create over time."><FormInput value={form.workers_max_children_total} onChange={(e) => set("workers_max_children_total", e.target.value)} /></L>
                <L label="Max concurrent child workers" hint="How many helper workers can run at the same time."><FormInput value={form.workers_max_children_concurrent} onChange={(e) => set("workers_max_children_concurrent", e.target.value)} /></L>
              </FieldsGrid>
              {useDockerLauncher ? (
                <div className="mt-4 grid items-start gap-4 lg:grid-cols-3">
                  <L label="Docker image"><FormInput value={form.workers_docker_image} onChange={(e) => set("workers_docker_image", e.target.value)} /></L>
                  <L label="Worker folder inside Docker" hint="Where the app workspace is mounted inside the container."><FormInput value={form.workers_docker_workspace} onChange={(e) => set("workers_docker_workspace", e.target.value)} /></L>
                  <L label="Worker folder on this machine" hint="The local folder that will be mounted into Docker."><FormInput value={form.workers_docker_host_workspace} onChange={(e) => set("workers_docker_host_workspace", e.target.value)} /></L>
                </div>
              ) : (
                <div className="mt-4 rounded-[22px] border border-white/6 bg-black/20 px-4 py-4 text-sm text-[var(--text-muted)]">
                  Workers will run in the same environment as the main app, so Docker-only settings are hidden.
                </div>
              )}
            </SectionCard>

            <SectionCard title="AI model settings" description="Choose the main model profile, optionally give workers their own profile, and adjust advanced request behavior only when needed.">
              <div className="grid items-start gap-5 xl:grid-cols-2">
                <div className="rounded-[22px] border border-white/6 bg-black/20 p-4">
                  <h4 className="text-base font-semibold text-white">Main app model</h4>
                  {mainProvider ? (
                    <div className="mt-3 rounded-[18px] border border-white/6 bg-white/[0.03] px-4 py-3 text-sm text-[var(--text-muted)]">
                      <div className="font-medium text-white">{mainProvider.label}</div>
                      <div className="mt-1">{mainProvider.description}</div>
                    </div>
                  ) : null}
                  <FieldsGrid className="xl:grid-cols-2">
                    <L label="AI provider" hint="Choose which AI service powers the main app.">
                      <FormSelect
                        value={form.llm_provider_id}
                        onValueChange={(value) => setProvider("main", value)}
                        options={providerOptionsForSelect}
                        placeholder="Choose a provider"
                      />
                    </L>
                    {mainProvider?.supports_custom_model !== false ? (
                      <L label={mainProvider?.model_label ?? "Model name"} hint={`Recommended default: ${mainProvider?.default_model ?? "n/a"}`}>
                        <FormInput value={form.llm_model} onChange={(e) => set("llm_model", e.target.value)} />
                      </L>
                    ) : null}
                    {mainProvider?.supports_custom_base_url ? (
                      <L label={mainProvider?.base_url_label ?? "Custom API URL"} hint="Shown only for providers that allow a custom endpoint.">
                        <FormInput value={form.llm_api_base} onChange={(e) => set("llm_api_base", e.target.value)} />
                      </L>
                    ) : null}
                    {mainProvider?.supports_model_prefix_override ? (
                      <L label="Model prefix" hint="Use this only when the provider expects a custom LiteLLM prefix.">
                        <FormInput value={form.llm_model_prefix} onChange={(e) => set("llm_model_prefix", e.target.value)} />
                      </L>
                    ) : null}
                  </FieldsGrid>
                  {mainProvider?.always_prefix_model ? (
                    <div className="mt-3 rounded-[18px] border border-white/6 bg-white/[0.03] px-4 py-3 text-sm text-[var(--text-muted)]">
                      This provider automatically uses the <span className="font-medium text-white">{mainProvider.model_prefix}</span> model prefix.
                    </div>
                  ) : null}
                  <div className="mt-4">
                    <Disclosure title="Main model access" description="Credentials for the main AI model profile.">
                      <FieldsGrid className="xl:grid-cols-2">
                        <L label={mainProvider?.api_key_label ?? "API key"} hint={mainProvider?.requires_api_key === false ? "Optional for this provider." : "Required for this provider."}>
                          <FormInput type="password" value={form.llm_api_key} onChange={(e) => set("llm_api_key", e.target.value)} placeholder={mainProvider?.requires_api_key === false ? "Optional" : "Leave blank to keep current key"} />
                        </L>
                      </FieldsGrid>
                    </Disclosure>
                  </div>
                </div>

                <div className="rounded-[22px] border border-white/6 bg-black/20 p-4">
                  <h4 className="text-base font-semibold text-white">Worker model</h4>
                  <div className="mt-4">
                    <ToggleField label="Use a separate model for workers" checked={form.worker_llm_enabled} onChange={(checked) => set("worker_llm_enabled", checked)} hint="Turn this off if worker tasks should use the same model settings as the main app." />
                  </div>
                  {useSeparateWorkerInference ? (
                    <>
                      {workerProvider ? (
                        <div className="mt-3 rounded-[18px] border border-white/6 bg-white/[0.03] px-4 py-3 text-sm text-[var(--text-muted)]">
                          <div className="font-medium text-white">{workerProvider.label}</div>
                          <div className="mt-1">{workerProvider.description}</div>
                        </div>
                      ) : null}
                      <FieldsGrid className="mt-4 xl:grid-cols-2">
                        <L label="AI provider" hint="Workers can use a different provider from the main app when needed.">
                          <FormSelect
                            value={form.worker_llm_provider_id}
                            onValueChange={(value) => setProvider("worker", value)}
                            options={providerOptionsForSelect}
                            placeholder="Choose a provider"
                          />
                        </L>
                        {workerProvider?.supports_custom_model !== false ? (
                          <L label={workerProvider?.model_label ?? "Model name"} hint={`Recommended default: ${workerProvider?.default_model ?? "n/a"}`}>
                            <FormInput value={form.worker_llm_model} onChange={(e) => set("worker_llm_model", e.target.value)} />
                          </L>
                        ) : null}
                        {workerProvider?.supports_custom_base_url ? (
                          <L label={workerProvider?.base_url_label ?? "Custom API URL"}>
                            <FormInput value={form.worker_llm_api_base} onChange={(e) => set("worker_llm_api_base", e.target.value)} />
                          </L>
                        ) : null}
                        {workerProvider?.supports_model_prefix_override ? (
                          <L label="Model prefix" hint="Use this only when the provider expects a custom LiteLLM prefix.">
                            <FormInput value={form.worker_llm_model_prefix} onChange={(e) => set("worker_llm_model_prefix", e.target.value)} />
                          </L>
                        ) : null}
                      </FieldsGrid>
                      {workerProvider?.always_prefix_model ? (
                        <div className="mt-3 rounded-[18px] border border-white/6 bg-white/[0.03] px-4 py-3 text-sm text-[var(--text-muted)]">
                          This provider automatically uses the <span className="font-medium text-white">{workerProvider.model_prefix}</span> model prefix.
                        </div>
                      ) : null}
                    </>
                  ) : (
                    <div className="mt-4 rounded-[18px] border border-white/6 bg-white/[0.03] px-4 py-3 text-sm text-[var(--text-muted)]">
                      Workers will reuse the main app model settings.
                    </div>
                  )}
                  {useSeparateWorkerInference ? (
                    <div className="mt-4">
                      <Disclosure title="Worker model access" description="Credentials used only when workers have their own model profile.">
                        <FieldsGrid className="xl:grid-cols-2">
                          <L label={workerProvider?.api_key_label ?? "API key"} hint={workerProvider?.requires_api_key === false ? "Optional for this provider." : "Required for this provider."}>
                            <FormInput type="password" value={form.worker_llm_api_key} onChange={(e) => set("worker_llm_api_key", e.target.value)} placeholder={workerProvider?.requires_api_key === false ? "Optional" : "Leave blank to keep current key"} />
                          </L>
                        </FieldsGrid>
                      </Disclosure>
                    </div>
                  ) : null}
                </div>
              </div>
              <div className="mt-5">
                <Disclosure title="Advanced model request settings" description="Retry, fallback and request behavior for AI calls." defaultOpen>
                  <FieldsGrid>
                    <L label="Retries"><FormInput value={form.litellm_num_retries} onChange={(e) => set("litellm_num_retries", e.target.value)} /></L>
                    <L label="Request timeout"><FormInput value={form.litellm_timeout} onChange={(e) => set("litellm_timeout", e.target.value)} /></L>
                    <L label="Max parallel requests" hint="Too high can trigger a wave of rate-limit errors."><FormInput value={form.litellm_max_concurrency} onChange={(e) => set("litellm_max_concurrency", e.target.value)} /></L>
                    <L label="Rate-limit retries"><FormInput value={form.litellm_rate_limit_max_retries} onChange={(e) => set("litellm_rate_limit_max_retries", e.target.value)} /></L>
                    <L label="Base retry delay (seconds)"><FormInput value={form.litellm_rate_limit_base_delay_seconds} onChange={(e) => set("litellm_rate_limit_base_delay_seconds", e.target.value)} /></L>
                    <L label="Max retry delay (seconds)"><FormInput value={form.litellm_rate_limit_max_delay_seconds} onChange={(e) => set("litellm_rate_limit_max_delay_seconds", e.target.value)} /></L>
                  </FieldsGrid>
                  <div className="mt-4">
                    <L label="Fallback settings (JSON)" hint="Advanced override. Keep this valid and compact so failover stays predictable."><FormTextarea value={form.litellm_fallbacks} onChange={(e) => set("litellm_fallbacks", e.target.value)} rows={3} /></L>
                  </div>
                  <div className="mt-4 grid gap-3 lg:grid-cols-2">
                    <ToggleField label="Ignore unsupported request fields" checked={form.litellm_drop_params} onChange={(checked) => set("litellm_drop_params", checked)} />
                    <ToggleField label="Enable response caching" checked={form.litellm_caching} onChange={(checked) => set("litellm_caching", checked)} />
                  </div>
                </Disclosure>
              </div>
            </SectionCard>

            <SectionCard title="Storage and memory" description="Where the app keeps its files, and how much past context it should pull into replies.">
              <FieldsGrid>
                <L label="App data folder"><FormInput value={form.state_dir} onChange={(e) => set("state_dir", e.target.value)} /></L>
                <L label="Workspace folder"><FormInput value={form.workspace_dir} onChange={(e) => set("workspace_dir", e.target.value)} /></L>
                <L label="Memory results to keep" hint="Higher values give the AI more past context, but also make prompts larger."><FormInput value={form.memory_top_k} onChange={(e) => set("memory_top_k", e.target.value)} /></L>
                <L label="Memory candidates to scan" hint="How many possible memory matches are checked before the final cut."><FormInput value={form.memory_prefilter_k} onChange={(e) => set("memory_prefilter_k", e.target.value)} /></L>
                <L label="Minimum memory match score" hint="Higher values make the app pick only closer matches from memory."><FormInput value={form.memory_min_score} onChange={(e) => set("memory_min_score", e.target.value)} /></L>
                <L label="Max memory text length" hint="Upper limit for how much saved context can be sent to the AI at once."><FormInput value={form.memory_max_chars} onChange={(e) => set("memory_max_chars", e.target.value)} /></L>
                <L label="Memory owner ID" hint="Useful when memory should stay tied to one app identity or owner."><FormInput value={form.memory_owner_id} onChange={(e) => set("memory_owner_id", e.target.value)} /></L>
              </FieldsGrid>
            </SectionCard>

            <SectionCard title="Web search services" description="Optional providers the app can use when it needs to search or fetch web content.">
              <Disclosure title="Service access keys" description="API keys for connected web search and crawling tools.">
                <FieldsGrid className="xl:grid-cols-2">
                  <L label="Brave API key"><FormInput type="password" value={form.search_brave_api_key} onChange={(e) => set("search_brave_api_key", e.target.value)} placeholder="Leave blank to keep current key" /></L>
                  <L label="Firecrawl API key"><FormInput type="password" value={form.search_firecrawl_api_key} onChange={(e) => set("search_firecrawl_api_key", e.target.value)} placeholder="Leave blank to keep current key" /></L>
                </FieldsGrid>
              </Disclosure>
            </SectionCard>

            <div className="sticky bottom-4 z-10 flex flex-wrap items-center justify-between gap-4 rounded-[24px] border border-white/8 bg-[var(--surface-panel)]/95 px-5 py-4 backdrop-blur-xl shadow-[0_20px_50px_rgba(0,0,0,0.28)]">
              <div>
                <div className="text-sm font-medium text-white">{isDirty ? "Unsaved changes" : "No pending changes"}</div>
                <div className="mt-1 text-sm text-[var(--text-muted)]">
                  {isDirty ? "Save will write your changes to config.json. Cancel will restore the last loaded settings." : "The form already matches the current saved settings."}
                </div>
              </div>
              <div className="flex gap-2">
                <Button type="button" variant="outline" onClick={cancelEdits} disabled={!form || configLoading || saving || !isDirty} className="rounded-2xl border-white/8 bg-transparent text-[var(--text-muted)] hover:bg-white/[0.04] hover:text-[var(--text-strong)]">
                  Cancel
                </Button>
                <Button type="button" variant="secondary" onClick={() => void save()} disabled={!form || configLoading || saving || !isDirty} className="rounded-2xl bg-white/[0.08] text-[var(--text-strong)] hover:bg-white/[0.12]">
                  {saving ? "Saving..." : "Save config"}
                </Button>
              </div>
            </div>
          </div>
        ) : null}
      </article>

      <article className={panel}>
        <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-[var(--text-strong)]">Recent logs</h3>
        {logs.length === 0 ? <p className="mt-4 rounded-2xl border border-white/6 bg-[var(--surface-panel-strong)] p-4 text-sm text-[var(--text-muted)]">No logs in current filter window.</p> : (
          <div className="mt-4 overflow-x-auto">
            <table className="w-full min-w-[720px] text-left text-sm">
              <thead className="text-[11px] uppercase tracking-wide text-[var(--text-dim)]"><tr><th className="border-b border-white/6 px-3 py-2">Time</th><th className="border-b border-white/6 px-3 py-2">Level</th><th className="border-b border-white/6 px-3 py-2">Service</th><th className="border-b border-white/6 px-3 py-2">Event</th></tr></thead>
              <tbody>{logs.map((log) => <tr key={`${log.timestamp ?? ""}-${log.event ?? ""}`} className="border-b border-white/6"><td className="px-3 py-3 text-[var(--text-muted)]">{formatLocalDateTime(log.timestamp)}</td><td className="px-3 py-3"><span className={`rounded-full border px-2 py-1 text-[11px] uppercase tracking-wide ${statusTone(log.level)}`}>{String(log.level ?? "info")}</span></td><td className="px-3 py-3 text-[var(--text-strong)]">{log.service ?? "gateway"}</td><td className="px-3 py-3 text-[var(--text-strong)]">{log.event ?? ""}</td></tr>)}</tbody>
            </table>
          </div>
        )}
      </article>
    </section>
  );
}
