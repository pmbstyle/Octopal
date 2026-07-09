from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from octopal.channels import DEFAULT_USER_CHANNEL
from octopal.infrastructure.config.models import (
    A2AConfig,
    ConnectorsConfig,
    GatewayConfig,
    GroupAddressingConfig,
    LiteLLMRuntimeConfig,
    LLMConfig,
    MemoryConfig,
    ObservabilityConfig,
    OctopalConfig,
    SearchConfig,
    StorageConfig,
    TelegramConfig,
    WhatsAppConfig,
    WorkerRuntimeConfig,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
        env_file_encoding="utf-8",
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
    litellm_rate_limit_base_delay_seconds: float = Field(
        1.0, alias="LITELLM_RATE_LIMIT_BASE_DELAY_SECONDS"
    )
    litellm_rate_limit_max_delay_seconds: float = Field(
        30.0, alias="LITELLM_RATE_LIMIT_MAX_DELAY_SECONDS"
    )

    # OpenRouter Settings (used via LiteLLM with openrouter/ model prefix)
    openrouter_api_key: str | None = Field(default=None, alias="OPENROUTER_API_KEY")
    openrouter_base_url: str = Field("https://openrouter.ai/api/v1", alias="OPENROUTER_BASE_URL")
    openrouter_model: str = Field("x-ai/grok-4.3", alias="OPENROUTER_MODEL")
    openrouter_timeout: float = Field(120.0, alias="OPENROUTER_TIMEOUT")

    # Legacy ZAI Settings (used as defaults for LiteLLM)
    zai_api_key: str | None = Field(default=None, alias="ZAI_API_KEY")
    zai_base_url: str = Field("https://api.z.ai/api/coding/paas/v4", alias="ZAI_BASE_URL")
    zai_chat_path: str = Field("/chat/completions", alias="ZAI_CHAT_PATH")
    zai_timeout_seconds: float = Field(45.0, alias="ZAI_TIMEOUT_SECONDS")
    zai_connect_timeout_seconds: float = Field(15.0, alias="ZAI_CONNECT_TIMEOUT_SECONDS")
    zai_accept_language: str = Field("en-US,en", alias="ZAI_ACCEPT_LANGUAGE")
    zai_model: str = Field("glm-5.1", alias="ZAI_MODEL")

    minimax_api_key: str | None = Field(default=None, alias="MINIMAX_API_KEY")

    brave_api_key: str | None = Field(default=None, alias="BRAVE_API_KEY")
    firecrawl_api_key: str | None = Field(default=None, alias="FIRECRAWL_API_KEY")
    observability_enabled: bool = Field(False, alias="OCTOPAL_OBSERVABILITY_ENABLED")
    observability_backend: str = Field("noop", alias="OCTOPAL_OBSERVABILITY_BACKEND")
    observability_capture_content: bool = Field(
        False, alias="OCTOPAL_OBSERVABILITY_CAPTURE_CONTENT"
    )
    observability_preview_chars: int = Field(240, alias="OCTOPAL_OBSERVABILITY_PREVIEW_CHARS")
    observability_sample_rate: float = Field(1.0, alias="OCTOPAL_OBSERVABILITY_SAMPLE_RATE")
    langfuse_public_key: str | None = Field(default=None, alias="LANGFUSE_PUBLIC_KEY")
    langfuse_secret_key: str | None = Field(default=None, alias="LANGFUSE_SECRET_KEY")
    langfuse_host: str | None = Field(default=None, alias="LANGFUSE_HOST")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_base_url: str = Field("https://api.openai.com/v1", alias="OPENAI_BASE_URL")
    openai_embed_model: str = Field("text-embedding-3-small", alias="OPENAI_EMBED_MODEL")

    log_level: str = Field("INFO", alias="OCTOPAL_LOG_LEVEL")
    state_dir: Path = Field(Path("data"), alias="OCTOPAL_STATE_DIR")
    workspace_dir: Path = Field(Path("workspace"), alias="OCTOPAL_WORKSPACE_DIR")

    memory_top_k: int = Field(5, alias="OCTOPAL_MEMORY_TOP_K")
    memory_prefilter_k: int = Field(80, alias="OCTOPAL_MEMORY_PREFILTER_K")
    memory_min_score: float = Field(0.25, alias="OCTOPAL_MEMORY_MIN_SCORE")
    memory_max_chars: int = Field(2000, alias="OCTOPAL_MEMORY_MAX_CHARS")
    memory_owner_id: str = Field("default", alias="OCTOPAL_MEMORY_OWNER_ID")

    gateway_host: str = Field("127.0.0.1", alias="OCTOPAL_GATEWAY_HOST")
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
    group_addressing_enabled: bool = Field(True, alias="OCTOPAL_GROUP_ADDRESSING_ENABLED")
    group_agent_name: str = Field("", alias="OCTOPAL_GROUP_AGENT_NAME")
    group_agent_aliases: str = Field("", alias="OCTOPAL_GROUP_AGENT_ALIASES")
    group_collective_aliases: str = Field("", alias="OCTOPAL_GROUP_COLLECTIVE_ALIASES")

    # Connectors
    connectors: ConnectorsConfig = Field(default_factory=ConnectorsConfig)
    a2a: A2AConfig = Field(default_factory=A2AConfig)

    # Comma-separated list of Telegram chat IDs allowed to interact with the octo
    # Get your chat ID by messaging @userinfobot on Telegram
    allowed_telegram_chat_ids: str = Field("", alias="ALLOWED_TELEGRAM_CHAT_IDS")
    telegram_parse_mode: str = Field("MarkdownV2", alias="OCTOPAL_TELEGRAM_PARSE_MODE")
    whatsapp_mode: str = Field("separate", alias="OCTOPAL_WHATSAPP_MODE")
    allowed_whatsapp_numbers: str = Field("", alias="ALLOWED_WHATSAPP_NUMBERS")
    allowed_whatsapp_chats: str = Field("", alias="ALLOWED_WHATSAPP_CHATS")
    whatsapp_auth_dir: Path | None = Field(default=None, alias="OCTOPAL_WHATSAPP_AUTH_DIR")
    whatsapp_bridge_host: str = Field("127.0.0.1", alias="OCTOPAL_WHATSAPP_BRIDGE_HOST")
    whatsapp_bridge_port: int = Field(8765, alias="OCTOPAL_WHATSAPP_BRIDGE_PORT")
    whatsapp_callback_token: str = Field("", alias="OCTOPAL_WHATSAPP_CALLBACK_TOKEN")
    whatsapp_node_command: str = Field("node", alias="OCTOPAL_WHATSAPP_NODE_COMMAND")

    # New structured config support
    config_obj: OctopalConfig | None = Field(default=None, exclude=True)


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


def config_write_path() -> Path:
    """Return the config path used for the next write."""
    explicit = os.getenv("OCTOPAL_CONFIG_FILE", "").strip()
    if explicit:
        candidate = Path(explicit).expanduser()
        return candidate if candidate.is_absolute() else Path.cwd() / candidate
    return _resolve_config_file() or (Path.cwd() / "config.json")


def load_config() -> OctopalConfig:
    config_file = _resolve_config_file()
    if config_file is None:
        return OctopalConfig()
    try:
        with config_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return OctopalConfig.model_validate(data)
    except Exception as exc:
        raise ValueError(f"Invalid configuration file {config_file}: {exc}") from exc


def save_config(config: OctopalConfig) -> Path:
    config_file = config_write_path()
    config_file.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{config_file.name}.", suffix=".tmp", dir=str(config_file.parent)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(config.model_dump(mode="json"), f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, config_file)
        if os.name != "nt":
            os.chmod(config_file, 0o600)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return config_file


def load_settings() -> Settings:
    config_file = _resolve_config_file()
    config = load_config() if config_file is not None else None
    init_values = _settings_init_values_from_config(config) if config is not None else {}
    settings = Settings(**init_values)  # type: ignore[arg-type]

    if not settings.zai_api_key:
        legacy = os.getenv("Z_AI_API_KEY")
        if legacy:
            settings = settings.model_copy(update={"zai_api_key": legacy})

    if config is not None:
        # Structured config is authoritative when it exists.
        _sync_settings_from_config(settings, config)
    else:
        config = config_from_settings(settings)

    # Store the config object for new code to use
    settings.config_obj = config
    return settings


def config_from_settings(settings: Settings) -> OctopalConfig:
    """Build structured config from legacy environment-backed settings."""
    llm = _legacy_llm_config(settings)
    return OctopalConfig(
        user_channel=settings.user_channel,
        telegram=TelegramConfig(
            bot_token=settings.telegram_bot_token,
            allowed_chat_ids=_split_csv(settings.allowed_telegram_chat_ids),
            parse_mode=settings.telegram_parse_mode,
        ),
        group_addressing=GroupAddressingConfig(
            enabled=settings.group_addressing_enabled,
            agent_name=settings.group_agent_name or None,
            agent_aliases=_split_csv(settings.group_agent_aliases),
            collective_aliases=_split_csv(settings.group_collective_aliases),
        ),
        llm=llm,
        worker_llm_default=llm.model_copy(deep=True),
        litellm=LiteLLMRuntimeConfig(
            num_retries=settings.litellm_num_retries,
            timeout=settings.litellm_timeout,
            fallbacks=settings.litellm_fallbacks,
            drop_params=settings.litellm_drop_params,
            caching=settings.litellm_caching,
            max_concurrency=settings.litellm_max_concurrency,
            rate_limit_max_retries=settings.litellm_rate_limit_max_retries,
            rate_limit_base_delay_seconds=settings.litellm_rate_limit_base_delay_seconds,
            rate_limit_max_delay_seconds=settings.litellm_rate_limit_max_delay_seconds,
        ),
        storage=StorageConfig(
            state_dir=settings.state_dir,
            workspace_dir=settings.workspace_dir,
        ),
        memory=MemoryConfig(
            top_k=settings.memory_top_k,
            prefilter_k=settings.memory_prefilter_k,
            min_score=settings.memory_min_score,
            max_chars=settings.memory_max_chars,
            owner_id=settings.memory_owner_id,
        ),
        gateway=GatewayConfig(
            host=settings.gateway_host,
            port=settings.gateway_port,
            tailscale_ips=settings.tailscale_ips,
            dashboard_token=settings.dashboard_token,
            tailscale_auto_serve=settings.tailscale_auto_serve,
            webapp_enabled=settings.webapp_enabled,
            webapp_dist_dir=settings.webapp_dist_dir,
        ),
        workers=WorkerRuntimeConfig(
            launcher=settings.worker_launcher,
            docker_image=settings.worker_docker_image,
            docker_workspace=settings.worker_docker_workspace,
            docker_host_workspace=settings.worker_docker_host_workspace,
            max_spawn_depth=settings.worker_max_spawn_depth,
            max_children_total=settings.worker_max_children_total,
            max_children_concurrent=settings.worker_max_children_concurrent,
        ),
        whatsapp=WhatsAppConfig(
            mode=settings.whatsapp_mode,
            allowed_numbers=_split_csv(settings.allowed_whatsapp_numbers),
            allowed_chats=_split_csv(settings.allowed_whatsapp_chats),
            auth_dir=settings.whatsapp_auth_dir,
            bridge_host=settings.whatsapp_bridge_host,
            bridge_port=settings.whatsapp_bridge_port,
            callback_token=settings.whatsapp_callback_token,
            node_command=settings.whatsapp_node_command,
        ),
        search=SearchConfig(
            brave_api_key=settings.brave_api_key,
            firecrawl_api_key=settings.firecrawl_api_key,
        ),
        observability=ObservabilityConfig(
            enabled=settings.observability_enabled,
            backend=settings.observability_backend,
            capture_content=settings.observability_capture_content,
            preview_chars=settings.observability_preview_chars,
            sample_rate=settings.observability_sample_rate,
            langfuse_public_key=settings.langfuse_public_key,
            langfuse_secret_key=settings.langfuse_secret_key,
            langfuse_host=settings.langfuse_host,
        ),
        a2a=settings.a2a.model_copy(deep=True),
        connectors=settings.connectors.model_copy(deep=True),
        log_level=settings.log_level,
        debug_prompts=settings.debug_prompts,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        user_message_grace_seconds=settings.user_message_grace_seconds,
    )


def _legacy_llm_config(settings: Settings) -> LLMConfig:
    if settings.litellm_provider_id or settings.litellm_model or settings.litellm_api_key:
        return LLMConfig(
            provider_id=settings.litellm_provider_id,
            model=settings.litellm_model,
            api_key=settings.litellm_api_key,
            api_base=settings.litellm_api_base,
            model_prefix=settings.litellm_model_prefix,
        )
    if settings.openrouter_api_key:
        return LLMConfig(
            provider_id="openrouter",
            model=settings.openrouter_model,
            api_key=settings.openrouter_api_key,
            api_base=settings.openrouter_base_url,
        )
    if settings.zai_api_key:
        return LLMConfig(
            provider_id="zai",
            model=settings.zai_model,
            api_key=settings.zai_api_key,
            api_base=settings.zai_base_url,
        )
    return LLMConfig()


def _split_csv(value: object) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _sync_settings_from_config(settings: Settings, config: OctopalConfig) -> None:
    """Sync values from OctopalConfig to Settings for backward compatibility."""
    settings.__dict__.update(_settings_updates_from_config(config))


def _settings_init_values_from_config(config: OctopalConfig) -> dict[str, object | None]:
    """Mask legacy environment values for fields owned by structured config."""
    init_values: dict[str, object | None] = {}
    for field_name, value in _settings_updates_from_config(config).items():
        field = Settings.model_fields[field_name]
        init_values[str(field.alias or field_name)] = value
    return init_values


def _settings_updates_from_config(config: OctopalConfig) -> dict[str, object | None]:
    updates: dict[str, object | None] = {}

    updates["user_channel"] = config.user_channel

    # Telegram
    updates["telegram_bot_token"] = config.telegram.bot_token
    updates["allowed_telegram_chat_ids"] = ",".join(config.telegram.allowed_chat_ids)
    updates["telegram_parse_mode"] = config.telegram.parse_mode

    # Group addressing
    updates["group_addressing_enabled"] = config.group_addressing.enabled
    updates["group_agent_name"] = config.group_addressing.agent_name or ""
    updates["group_agent_aliases"] = ",".join(config.group_addressing.agent_aliases)
    updates["group_collective_aliases"] = ",".join(config.group_addressing.collective_aliases)

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
    updates["allowed_whatsapp_chats"] = ",".join(config.whatsapp.allowed_chats)
    updates["whatsapp_auth_dir"] = config.whatsapp.auth_dir
    updates["whatsapp_bridge_host"] = config.whatsapp.bridge_host
    updates["whatsapp_bridge_port"] = config.whatsapp.bridge_port
    updates["whatsapp_callback_token"] = config.whatsapp.callback_token
    updates["whatsapp_node_command"] = config.whatsapp.node_command

    # Search
    updates["brave_api_key"] = config.search.brave_api_key
    updates["firecrawl_api_key"] = config.search.firecrawl_api_key

    # Observability
    updates["observability_enabled"] = config.observability.enabled
    updates["observability_backend"] = config.observability.backend
    updates["observability_capture_content"] = config.observability.capture_content
    updates["observability_preview_chars"] = config.observability.preview_chars
    updates["observability_sample_rate"] = config.observability.sample_rate
    updates["langfuse_public_key"] = config.observability.langfuse_public_key
    updates["langfuse_secret_key"] = config.observability.langfuse_secret_key
    updates["langfuse_host"] = config.observability.langfuse_host

    # A2A interop
    updates["a2a"] = config.a2a

    # Common
    updates["log_level"] = config.log_level
    updates["debug_prompts"] = config.debug_prompts
    updates["heartbeat_interval_seconds"] = config.heartbeat_interval_seconds
    updates["user_message_grace_seconds"] = config.user_message_grace_seconds
    updates["connectors"] = config.connectors

    return updates
