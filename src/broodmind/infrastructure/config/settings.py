from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from broodmind.channels import DEFAULT_USER_CHANNEL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    telegram_bot_token: str = Field("", alias="TELEGRAM_BOT_TOKEN")
    user_channel: str = Field(DEFAULT_USER_CHANNEL, alias="BROODMIND_USER_CHANNEL")

    # LLM Provider Settings
    # Runtime stays on LiteLLM, while the active upstream provider is chosen via
    # the BROODMIND_LITELLM_* profile fields. OPENROUTER_* and ZAI_* remain as
    # legacy fallbacks for existing installations.
    llm_provider: str = Field("litellm", alias="BROODMIND_LLM_PROVIDER")
    litellm_provider_id: str | None = Field(default=None, alias="BROODMIND_LITELLM_PROVIDER_ID")
    litellm_model: str | None = Field(default=None, alias="BROODMIND_LITELLM_MODEL")
    litellm_api_key: str | None = Field(default=None, alias="BROODMIND_LITELLM_API_KEY")
    litellm_api_base: str | None = Field(default=None, alias="BROODMIND_LITELLM_API_BASE")
    litellm_model_prefix: str | None = Field(default=None, alias="BROODMIND_LITELLM_MODEL_PREFIX")

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

    brave_api_key: str | None = Field(default=None, alias="BRAVE_API_KEY")
    firecrawl_api_key: str | None = Field(default=None, alias="FIRECRAWL_API_KEY")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_base_url: str = Field("https://api.openai.com/v1", alias="OPENAI_BASE_URL")
    openai_embed_model: str = Field(
        "text-embedding-3-small", alias="OPENAI_EMBED_MODEL"
    )

    log_level: str = Field("INFO", alias="BROODMIND_LOG_LEVEL")
    state_dir: Path = Field(Path("data"), alias="BROODMIND_STATE_DIR")
    workspace_dir: Path = Field(Path("workspace"), alias="BROODMIND_WORKSPACE_DIR")

    memory_top_k: int = Field(5, alias="BROODMIND_MEMORY_TOP_K")
    memory_prefilter_k: int = Field(80, alias="BROODMIND_MEMORY_PREFILTER_K")
    memory_min_score: float = Field(0.25, alias="BROODMIND_MEMORY_MIN_SCORE")
    memory_max_chars: int = Field(2000, alias="BROODMIND_MEMORY_MAX_CHARS")
    memory_owner_id: str = Field("default", alias="BROODMIND_MEMORY_OWNER_ID")

    gateway_host: str = Field("0.0.0.0", alias="BROODMIND_GATEWAY_HOST")
    gateway_port: int = Field(8000, alias="BROODMIND_GATEWAY_PORT")
    tailscale_ips: str = Field("", alias="BROODMIND_TAILSCALE_IPS")
    dashboard_token: str = Field("", alias="BROODMIND_DASHBOARD_TOKEN")
    tailscale_auto_serve: bool = Field(True, alias="BROODMIND_TAILSCALE_AUTO_SERVE")
    webapp_enabled: bool = Field(False, alias="BROODMIND_WEBAPP_ENABLED")
    webapp_dist_dir: Path | None = Field(default=None, alias="BROODMIND_WEBAPP_DIST_DIR")

    worker_launcher: str = Field("same_env", alias="BROODMIND_WORKER_LAUNCHER")
    worker_docker_image: str = Field("broodmind-worker:latest", alias="BROODMIND_WORKER_DOCKER_IMAGE")
    worker_docker_workspace: str = Field("/workspace", alias="BROODMIND_WORKER_DOCKER_WORKSPACE")
    worker_docker_host_workspace: str | None = Field(
        default=None, alias="BROODMIND_WORKER_DOCKER_HOST_WORKSPACE"
    )
    worker_max_spawn_depth: int = Field(2, alias="BROODMIND_WORKER_MAX_SPAWN_DEPTH")
    worker_max_children_total: int = Field(20, alias="BROODMIND_WORKER_MAX_CHILDREN_TOTAL")
    worker_max_children_concurrent: int = Field(10, alias="BROODMIND_WORKER_MAX_CHILDREN_CONCURRENT")

    debug_prompts: bool = Field(False, alias="BROODMIND_DEBUG_PROMPTS")

    heartbeat_interval_seconds: int = Field(900, alias="BROODMIND_HEARTBEAT_INTERVAL_SECONDS")
    user_message_grace_seconds: float = Field(5.0, alias="BROODMIND_USER_MESSAGE_GRACE_SECONDS")

    # Comma-separated list of Telegram chat IDs allowed to interact with the queen
    # Get your chat ID by messaging @userinfobot on Telegram
    allowed_telegram_chat_ids: str = Field("", alias="ALLOWED_TELEGRAM_CHAT_IDS")
    telegram_parse_mode: str = Field("MarkdownV2", alias="BROODMIND_TELEGRAM_PARSE_MODE")
    whatsapp_mode: str = Field("separate", alias="BROODMIND_WHATSAPP_MODE")
    allowed_whatsapp_numbers: str = Field("", alias="ALLOWED_WHATSAPP_NUMBERS")
    whatsapp_auth_dir: Path | None = Field(default=None, alias="BROODMIND_WHATSAPP_AUTH_DIR")
    whatsapp_bridge_host: str = Field("127.0.0.1", alias="BROODMIND_WHATSAPP_BRIDGE_HOST")
    whatsapp_bridge_port: int = Field(8765, alias="BROODMIND_WHATSAPP_BRIDGE_PORT")
    whatsapp_callback_token: str = Field("", alias="BROODMIND_WHATSAPP_CALLBACK_TOKEN")
    whatsapp_node_command: str = Field("node", alias="BROODMIND_WHATSAPP_NODE_COMMAND")


def _resolve_env_file() -> Path | None:
    explicit = os.getenv("BROODMIND_ENV_FILE", "").strip()
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


def load_settings() -> Settings:
    env_file = _resolve_env_file()
    if env_file is not None:
        settings = Settings(_env_file=env_file)
    else:
        settings = Settings()
    if not settings.zai_api_key:
        legacy = os.getenv("Z_AI_API_KEY")
        if legacy:
            settings = settings.model_copy(update={"zai_api_key": legacy})
    return settings
