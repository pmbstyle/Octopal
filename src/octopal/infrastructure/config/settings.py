from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from octopal.channels import DEFAULT_USER_CHANNEL
from octopal.infrastructure.config.models import ConnectorsConfig, OctopalConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    telegram_bot_token: str = Field("", alias="TELEGRAM_BOT_TOKEN")
    user_channel: str = Field(DEFAULT_USER_CHANNEL, alias="OCTOPAL_USER_CHANNEL")

    # LLM Provider Settings
    # Runtime stays on LiteLLM, while the active upstream provider is chosen via
    # the OCTOPAL_LITELLM_* profile fields. OPENROUTER_* and ZAI_* remain as
    # legacy fallbacks for existing installations.
    llm_provider: str = Field("litellm", alias="OCTOPAL_LLM_PROVIDER")
    litellm_provider_id: str | None = Field(default=None, alias="OCTOPAL_LITELLM_PROVIDER_ID")
    litellm_model: str | None = Field(default=None, alias="OCTOPAL_LITELLM_MODEL")
    litellm_api_key: str | None = Field(default=None, alias="OCTOPAL_LITELLM_API_KEY")
    litellm_api_base: str | None = Field(default=None, alias="OCTOPAL_LITELLM_API_BASE")
    litellm_model_prefix: str | None = Field(default=None, alias="OCTOPAL_LITELLM_MODEL_PREFIX")

    # LiteLLM Settings (unified provider runtime and transport tuning)
    litellm_num_retries: int = Field(3, alias="LITELLM_NUM_RETRIES")
    litellm_timeout: float = Field(120.0, alias="LITELLM_TIMEOUT")
    litellm_fallbacks: str | None = Field(default=None, alias="LITELLM_FALLBACKS")
    litellm_drop_params: bool = Field(True, alias="LITELLM_DROP_PARAMS")
    litellm_caching: bool = Field(False, alias="LITELLM_CACHING")
    litellm_max_concurrency: int = Field(2, alias="LITELLM_MAX_CONCURRENCY")
    litellm_rate_limit_max_retries: int = Field(6, alias="LITELLM_RATE_LIMIT_MAX_RETRIES")
    litellm_rate_limit_base_delay_seconds: float = Field(1.0, alias="LITELLM_RATE_LIMIT_BASE_DELAY_SECONDS")
    litellm_rate_limit_max_delay_seconds: float = Field(30.0, alias="LITELLM_RATE_LIMIT_MAX_DELAY_SECONDS")

    # OpenRouter Settings (used via LiteLLM with openrouter/ model prefix)
    openrouter_api_key: str | None = Field(default=None, alias="OPENROUTER_API_KEY")
    openrouter_base_url: str = Field("https://openrouter.ai/api/v1", alias="OPENROUTER_BASE_URL")
    openrouter_model: str = Field("anthropic/claude-sonnet-4", alias="OPENROUTER_MODEL")
    openrouter_timeout: float = Field(120.0, alias="OPENROUTER_TIMEOUT")

    # Legacy ZAI Settings (used as defaults for LiteLLM)
    zai_api_key: str | None = Field(default=None, alias="ZAI_API_KEY")
    zai_base_url: str = Field("https://api.z.ai/api/coding/paas/v4", alias="ZAI_BASE_URL")
    zai_chat_path: str = Field("/chat/completions", alias="ZAI_CHAT_PATH")
    zai_timeout_seconds: float = Field(45.0, alias="ZAI_TIMEOUT_SECONDS")
    zai_connect_timeout_seconds: float = Field(15.0, alias="ZAI_CONNECT_TIMEOUT_SECONDS")
    zai_accept_language: str = Field("en-US,en", alias="ZAI_ACCEPT_LANGUAGE")
    zai_model: str = Field("glm-5", alias="ZAI_MODEL")

    minimax_api_key: str | None = Field(default=None, alias="MINIMAX_API_KEY")

    brave_api_key: str | None = Field(default=None, alias="BRAVE_API_KEY")
    firecrawl_api_key: str | None = Field(default=None, alias="FIRECRAWL_API_KEY")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_base_url: str = Field("https://api.openai.com/v1", alias="OPENAI_BASE_URL")
    openai_embed_model: str = Field(
        "text-embedding-3-small", alias="OPENAI_EMBED_MODEL"
    )

    log_level: str = Field("INFO", alias="OCTOPAL_LOG_LEVEL")
    state_dir: Path = Field(Path("data"), alias="OCTOPAL_STATE_DIR")
    workspace_dir: Path = Field(Path("workspace"), alias="OCTOPAL_WORKSPACE_DIR")

    memory_top_k: int = Field(5, alias="OCTOPAL_MEMORY_TOP_K")
    memory_prefilter_k: int = Field(80, alias="OCTOPAL_MEMORY_PREFILTER_K")
    memory_min_score: float = Field(0.25, alias="OCTOPAL_MEMORY_MIN_SCORE")
    memory_max_chars: int = Field(2000, alias="OCTOPAL_MEMORY_MAX_CHARS")
    memory_owner_id: str = Field("default", alias="OCTOPAL_MEMORY_OWNER_ID")

    gateway_host: str = Field("0.0.0.0", alias="OCTOPAL_GATEWAY_HOST")
    gateway_port: int = Field(8000, alias="OCTOPAL_GATEWAY_PORT")
    tailscale_ips: str = Field("", alias="OCTOPAL_TAILSCALE_IPS")
    dashboard_token: str = Field("", alias="OCTOPAL_DASHBOARD_TOKEN")
    tailscale_auto_serve: bool = Field(True, alias="OCTOPAL_TAILSCALE_AUTO_SERVE")
    webapp_enabled: bool = Field(False, alias="OCTOPAL_WEBAPP_ENABLED")
    webapp_dist_dir: Path | None = Field(default=None, alias="OCTOPAL_WEBAPP_DIST_DIR")

    worker_launcher: str = Field("docker", alias="OCTOPAL_WORKER_LAUNCHER")
    worker_docker_image: str = Field("octopal-worker:latest", alias="OCTOPAL_WORKER_DOCKER_IMAGE")
    worker_docker_workspace: str = Field("/workspace", alias="OCTOPAL_WORKER_DOCKER_WORKSPACE")
    worker_docker_host_workspace: str | None = Field(
        default=None, alias="OCTOPAL_WORKER_DOCKER_HOST_WORKSPACE"
    )
    worker_max_spawn_depth: int = Field(2, alias="OCTOPAL_WORKER_MAX_SPAWN_DEPTH")
    worker_max_children_total: int = Field(20, alias="OCTOPAL_WORKER_MAX_CHILDREN_TOTAL")
    worker_max_children_concurrent: int = Field(10, alias="OCTOPAL_WORKER_MAX_CHILDREN_CONCURRENT")

    debug_prompts: bool = Field(False, alias="OCTOPAL_DEBUG_PROMPTS")

    heartbeat_interval_seconds: int = Field(900, alias="OCTOPAL_HEARTBEAT_INTERVAL_SECONDS")
    user_message_grace_seconds: float = Field(5.0, alias="OCTOPAL_USER_MESSAGE_GRACE_SECONDS")

    # Connectors
    connectors: ConnectorsConfig = Field(default_factory=ConnectorsConfig)

    # Comma-separated list of Telegram chat IDs allowed to interact with the octo
    # Get your chat ID by messaging @userinfobot on Telegram
    allowed_telegram_chat_ids: str = Field("", alias="ALLOWED_TELEGRAM_CHAT_IDS")
    telegram_parse_mode: str = Field("MarkdownV2", alias="OCTOPAL_TELEGRAM_PARSE_MODE")
    whatsapp_mode: str = Field("separate", alias="OCTOPAL_WHATSAPP_MODE")
    allowed_whatsapp_numbers: str = Field("", alias="ALLOWED_WHATSAPP_NUMBERS")
    whatsapp_auth_dir: Path | None = Field(default=None, alias="OCTOPAL_WHATSAPP_AUTH_DIR")
    whatsapp_bridge_host: str = Field("127.0.0.1", alias="OCTOPAL_WHATSAPP_BRIDGE_HOST")
    whatsapp_bridge_port: int = Field(8765, alias="OCTOPAL_WHATSAPP_BRIDGE_PORT")
    whatsapp_callback_token: str = Field("", alias="OCTOPAL_WHATSAPP_CALLBACK_TOKEN")
    whatsapp_node_command: str = Field("node", alias="OCTOPAL_WHATSAPP_NODE_COMMAND")

    # New structured config support
    config_obj: OctopalConfig | None = Field(default=None, exclude=True)


def _resolve_env_file() -> Path | None:
    explicit = os.getenv("OCTOPAL_ENV_FILE", "").strip()
    if explicit:
        candidate = Path(explicit).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        if candidate.exists():
            return candidate
        return None

    cwd_env = Path.cwd() / ".env"
    if cwd_env.exists():
        return cwd_env

    project_root_env = Path(__file__).resolve().parents[3] / ".env"
    if project_root_env.exists():
        return project_root_env

    return None


def _resolve_config_file() -> Path | None:
    explicit = os.getenv("OCTOPAL_CONFIG_FILE", "").strip()
    if explicit:
        candidate = Path(explicit).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        if candidate.exists():
            return candidate
        return None

    cwd_config = Path.cwd() / "config.json"
    if cwd_config.exists():
        return cwd_config

    project_root_config = Path(__file__).resolve().parents[3] / "config.json"
    if project_root_config.exists():
        return project_root_config

    return None


def load_config() -> OctopalConfig:
    config_file = _resolve_config_file()
    if config_file and config_file.exists():
        try:
            with config_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
                return OctopalConfig.model_validate(data)
        except Exception:
            # Fallback to default if JSON is malformed
            pass

    # If no config file, try to build one from environment for migration
    config = OctopalConfig()
    env_file = _resolve_env_file()
    temp_settings = Settings(_env_file=env_file) if env_file else Settings()

    # Map legacy settings to structured config
    config.user_channel = temp_settings.user_channel
    config.telegram.bot_token = temp_settings.telegram_bot_token
    if temp_settings.allowed_telegram_chat_ids:
        config.telegram.allowed_chat_ids = [
            cid.strip() for cid in temp_settings.allowed_telegram_chat_ids.split(",") if cid.strip()
        ]
    config.telegram.parse_mode = temp_settings.telegram_parse_mode

    # LLM (Octo) - try to resolve from LiteLLM settings first
    config.llm.provider_id = temp_settings.litellm_provider_id
    config.llm.model = temp_settings.litellm_model
    config.llm.api_key = temp_settings.litellm_api_key
    config.llm.api_base = temp_settings.litellm_api_base
    config.llm.model_prefix = temp_settings.litellm_model_prefix

    # If not set, try legacy fallbacks
    if not config.llm.provider_id:
        if temp_settings.zai_api_key:
            config.llm.provider_id = "zai"
        elif temp_settings.openrouter_api_key:
            config.llm.provider_id = "openrouter"
        elif temp_settings.minimax_api_key:
            config.llm.provider_id = "minimax"

    # LiteLLM Runtime
    config.litellm.num_retries = temp_settings.litellm_num_retries
    config.litellm.timeout = temp_settings.litellm_timeout
    config.litellm.fallbacks = temp_settings.litellm_fallbacks
    config.litellm.drop_params = temp_settings.litellm_drop_params
    config.litellm.caching = temp_settings.litellm_caching
    config.litellm.max_concurrency = temp_settings.litellm_max_concurrency
    config.litellm.rate_limit_max_retries = temp_settings.litellm_rate_limit_max_retries
    config.litellm.rate_limit_base_delay_seconds = temp_settings.litellm_rate_limit_base_delay_seconds
    config.litellm.rate_limit_max_delay_seconds = temp_settings.litellm_rate_limit_max_delay_seconds

    # Storage
    config.storage.state_dir = temp_settings.state_dir
    config.storage.workspace_dir = temp_settings.workspace_dir

    # Memory
    config.memory.top_k = temp_settings.memory_top_k
    config.memory.prefilter_k = temp_settings.memory_prefilter_k
    config.memory.min_score = temp_settings.memory_min_score
    config.memory.max_chars = temp_settings.memory_max_chars
    config.memory.owner_id = temp_settings.memory_owner_id

    # Gateway
    config.gateway.host = temp_settings.gateway_host
    config.gateway.port = temp_settings.gateway_port
    config.gateway.tailscale_ips = temp_settings.tailscale_ips
    config.gateway.dashboard_token = temp_settings.dashboard_token
    config.gateway.tailscale_auto_serve = temp_settings.tailscale_auto_serve
    config.gateway.webapp_enabled = temp_settings.webapp_enabled
    if temp_settings.webapp_dist_dir:
        config.gateway.webapp_dist_dir = temp_settings.webapp_dist_dir

    # Workers
    config.workers.launcher = temp_settings.worker_launcher
    config.workers.docker_image = temp_settings.worker_docker_image
    config.workers.docker_workspace = temp_settings.worker_docker_workspace
    if temp_settings.worker_docker_host_workspace:
        config.workers.docker_host_workspace = temp_settings.worker_docker_host_workspace
    config.workers.max_spawn_depth = temp_settings.worker_max_spawn_depth
    config.workers.max_children_total = temp_settings.worker_max_children_total
    config.workers.max_children_concurrent = temp_settings.worker_max_children_concurrent

    # WhatsApp
    config.whatsapp.mode = temp_settings.whatsapp_mode
    if temp_settings.allowed_whatsapp_numbers:
        config.whatsapp.allowed_numbers = [
            num.strip() for num in temp_settings.allowed_whatsapp_numbers.split(",") if num.strip()
        ]
    if temp_settings.whatsapp_auth_dir:
        config.whatsapp.auth_dir = temp_settings.whatsapp_auth_dir
    config.whatsapp.bridge_host = temp_settings.whatsapp_bridge_host
    config.whatsapp.bridge_port = temp_settings.whatsapp_bridge_port
    config.whatsapp.callback_token = temp_settings.whatsapp_callback_token
    config.whatsapp.node_command = temp_settings.whatsapp_node_command

    # Search
    config.search.brave_api_key = temp_settings.brave_api_key
    config.search.firecrawl_api_key = temp_settings.firecrawl_api_key

    # Common
    config.log_level = temp_settings.log_level
    config.debug_prompts = temp_settings.debug_prompts
    config.heartbeat_interval_seconds = temp_settings.heartbeat_interval_seconds
    config.user_message_grace_seconds = temp_settings.user_message_grace_seconds
    config.connectors = temp_settings.connectors

    return config


def save_config(config: OctopalConfig) -> None:
    config_file = _resolve_config_file()
    if not config_file:
        config_file = Path.cwd() / "config.json"

    with config_file.open("w", encoding="utf-8") as f:
        json.dump(config.model_dump(mode="json"), f, indent=2)


def load_settings() -> Settings:
    config = load_config()
    env_file = _resolve_env_file()
    settings = Settings(_env_file=env_file) if env_file is not None else Settings()

    # Apply structured config overrides to legacy settings
    _sync_settings_from_config(settings, config)

    if not settings.zai_api_key:
        legacy = os.getenv("Z_AI_API_KEY")
        if legacy:
            settings = settings.model_copy(update={"zai_api_key": legacy})

    # Store the config object for new code to use
    settings.config_obj = config
    return settings


def _sync_settings_from_config(settings: Settings, config: OctopalConfig) -> None:
    """Sync values from OctopalConfig to Settings for backward compatibility."""
    updates: dict[str, object | None] = {}

    updates["user_channel"] = config.user_channel

    # Telegram
    updates["telegram_bot_token"] = config.telegram.bot_token
    updates["allowed_telegram_chat_ids"] = ",".join(config.telegram.allowed_chat_ids)
    updates["telegram_parse_mode"] = config.telegram.parse_mode

    # LLM (Octo)
    updates["litellm_provider_id"] = config.llm.provider_id
    updates["litellm_model"] = config.llm.model
    updates["litellm_api_key"] = config.llm.api_key
    updates["litellm_api_base"] = config.llm.api_base
    updates["litellm_model_prefix"] = config.llm.model_prefix

    # Sync minimax_api_key for legacy support
    updates["minimax_api_key"] = config.llm.api_key if config.llm.provider_id == "minimax" else None

    # LiteLLM Runtime
    updates["litellm_num_retries"] = config.litellm.num_retries
    updates["litellm_timeout"] = config.litellm.timeout
    updates["litellm_fallbacks"] = config.litellm.fallbacks
    updates["litellm_drop_params"] = config.litellm.drop_params
    updates["litellm_caching"] = config.litellm.caching
    updates["litellm_max_concurrency"] = config.litellm.max_concurrency
    updates["litellm_rate_limit_max_retries"] = config.litellm.rate_limit_max_retries
    updates["litellm_rate_limit_base_delay_seconds"] = config.litellm.rate_limit_base_delay_seconds
    updates["litellm_rate_limit_max_delay_seconds"] = config.litellm.rate_limit_max_delay_seconds

    # Storage
    updates["state_dir"] = config.storage.state_dir
    updates["workspace_dir"] = config.storage.workspace_dir

    # Memory
    updates["memory_top_k"] = config.memory.top_k
    updates["memory_prefilter_k"] = config.memory.prefilter_k
    updates["memory_min_score"] = config.memory.min_score
    updates["memory_max_chars"] = config.memory.max_chars
    updates["memory_owner_id"] = config.memory.owner_id

    # Gateway
    updates["gateway_host"] = config.gateway.host
    updates["gateway_port"] = config.gateway.port
    updates["tailscale_ips"] = config.gateway.tailscale_ips
    updates["dashboard_token"] = config.gateway.dashboard_token
    updates["tailscale_auto_serve"] = config.gateway.tailscale_auto_serve
    updates["webapp_enabled"] = config.gateway.webapp_enabled
    updates["webapp_dist_dir"] = config.gateway.webapp_dist_dir

    # Workers
    updates["worker_launcher"] = config.workers.launcher
    updates["worker_docker_image"] = config.workers.docker_image
    updates["worker_docker_workspace"] = config.workers.docker_workspace
    updates["worker_docker_host_workspace"] = config.workers.docker_host_workspace
    updates["worker_max_spawn_depth"] = config.workers.max_spawn_depth
    updates["worker_max_children_total"] = config.workers.max_children_total
    updates["worker_max_children_concurrent"] = config.workers.max_children_concurrent

    # WhatsApp
    updates["whatsapp_mode"] = config.whatsapp.mode
    updates["allowed_whatsapp_numbers"] = ",".join(config.whatsapp.allowed_numbers)
    updates["whatsapp_auth_dir"] = config.whatsapp.auth_dir
    updates["whatsapp_bridge_host"] = config.whatsapp.bridge_host
    updates["whatsapp_bridge_port"] = config.whatsapp.bridge_port
    updates["whatsapp_callback_token"] = config.whatsapp.callback_token
    updates["whatsapp_node_command"] = config.whatsapp.node_command

    # Search
    updates["brave_api_key"] = config.search.brave_api_key
    updates["firecrawl_api_key"] = config.search.firecrawl_api_key

    # Common
    updates["log_level"] = config.log_level
    updates["debug_prompts"] = config.debug_prompts
    updates["heartbeat_interval_seconds"] = config.heartbeat_interval_seconds
    updates["user_message_grace_seconds"] = config.user_message_grace_seconds
    updates["connectors"] = config.connectors

    # Apply all updates
    settings.__dict__.update(updates)
