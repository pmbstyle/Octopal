from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from octopal.channels import DEFAULT_USER_CHANNEL


class TelegramConfig(BaseModel):
    bot_token: str = ""
    allowed_chat_ids: list[str] = Field(default_factory=list)
    parse_mode: str = "MarkdownV2"


class LLMConfig(BaseModel):
    provider_id: str | None = None
    model: str | None = None
    api_key: str | None = None
    api_base: str | None = None
    model_prefix: str | None = None


class LiteLLMRuntimeConfig(BaseModel):
    num_retries: int = 3
    timeout: float = 120.0
    fallbacks: str | None = None
    drop_params: bool = True
    caching: bool = False
    max_concurrency: int = 2
    rate_limit_max_retries: int = 6
    rate_limit_base_delay_seconds: float = 1.0
    rate_limit_max_delay_seconds: float = 30.0


class StorageConfig(BaseModel):
    state_dir: Path = Field(default=Path("data"))
    workspace_dir: Path = Field(default=Path("workspace"))


class MemoryConfig(BaseModel):
    top_k: int = 5
    prefilter_k: int = 80
    min_score: float = 0.25
    max_chars: int = 2000
    owner_id: str = "default"


class GatewayConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000
    tailscale_ips: str = ""
    dashboard_token: str = ""
    tailscale_auto_serve: bool = True
    webapp_enabled: bool = False
    webapp_dist_dir: Path | None = None


class WorkerRuntimeConfig(BaseModel):
    launcher: str = "docker"
    docker_image: str = "octopal-worker:latest"
    docker_workspace: str = "/workspace"
    docker_host_workspace: str | None = None
    max_spawn_depth: int = 2
    max_children_total: int = 20
    max_children_concurrent: int = 10


class WhatsAppConfig(BaseModel):
    mode: str = "separate"
    allowed_numbers: list[str] = Field(default_factory=list)
    auth_dir: Path | None = None
    bridge_host: str = "127.0.0.1"
    bridge_port: int = 8765
    callback_token: str = ""
    node_command: str = "node"


class SearchConfig(BaseModel):
    brave_api_key: str | None = None
    firecrawl_api_key: str | None = None


class ObservabilityConfig(BaseModel):
    enabled: bool = False
    backend: str = "noop"
    capture_content: bool = False
    preview_chars: int = 240
    sample_rate: float = 1.0
    langfuse_public_key: str | None = None
    langfuse_secret_key: str | None = None
    langfuse_host: str | None = None


class A2APeerConfig(BaseModel):
    enabled: bool = True
    name: str | None = None
    agent_card_url: str | None = None
    base_url: str | None = None
    token: str | None = None
    capabilities: list[str] = Field(default_factory=lambda: ["chat"])
    trust_level: str = "trusted"


class A2AConfig(BaseModel):
    enabled: bool = False
    public_base_url: str | None = None
    agent_name: str = "Octopal"
    agent_description: str = (
        "A personal AI agent with memory, scheduled tasks, and worker orchestration."
    )
    protocol_version: str = "1.0"
    max_payload_chars: int = 16000
    max_requests_per_minute: int = 30
    peers: dict[str, A2APeerConfig] = Field(default_factory=dict)


class ConnectorCredentials(BaseModel):
    client_id: str | None = None
    client_secret: str | None = None


class ConnectorAuthState(BaseModel):
    authorized_services: list[str] = Field(default_factory=list)
    refresh_token: str | None = None
    access_token: str | None = None
    last_error: str | None = None


class ConnectorInstanceConfig(BaseModel):
    enabled: bool = False
    enabled_services: list[str] = Field(default_factory=list)
    credentials: ConnectorCredentials = Field(default_factory=ConnectorCredentials)
    auth: ConnectorAuthState = Field(default_factory=ConnectorAuthState)

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_settings(cls, data: object):
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        legacy_settings = payload.pop("settings", None)
        if not isinstance(legacy_settings, dict):
            return payload

        payload.setdefault("enabled_services", legacy_settings.get("enabled_services", []))

        credentials = payload.get("credentials")
        if not isinstance(credentials, dict):
            credentials = {}
        credentials.setdefault("client_id", legacy_settings.get("client_id"))
        credentials.setdefault("client_secret", legacy_settings.get("client_secret"))
        payload["credentials"] = credentials

        auth = payload.get("auth")
        if not isinstance(auth, dict):
            auth = {}
        auth.setdefault("authorized_services", legacy_settings.get("authorized_services", []))
        auth.setdefault("refresh_token", legacy_settings.get("refresh_token"))
        auth.setdefault("access_token", legacy_settings.get("token"))
        payload["auth"] = auth
        return payload


class ConnectorsConfig(BaseModel):
    instances: dict[str, ConnectorInstanceConfig] = Field(default_factory=dict)


class OctopalConfig(BaseModel):
    user_channel: str = DEFAULT_USER_CHANNEL
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)

    # Octo LLM settings
    llm: LLMConfig = Field(default_factory=LLMConfig)

    # Worker LLM settings
    worker_llm_default: LLMConfig = Field(default_factory=LLMConfig)
    worker_llm_overrides: dict[str, LLMConfig] = Field(default_factory=dict)

    litellm: LiteLLMRuntimeConfig = Field(default_factory=LiteLLMRuntimeConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    memory: MemoryConfig = Field(default_factory=MemoryConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    workers: WorkerRuntimeConfig = Field(default_factory=WorkerRuntimeConfig)
    whatsapp: WhatsAppConfig = Field(default_factory=WhatsAppConfig)
    search: SearchConfig = Field(default_factory=SearchConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    a2a: A2AConfig = Field(default_factory=A2AConfig)
    connectors: ConnectorsConfig = Field(default_factory=ConnectorsConfig)

    log_level: str = "INFO"
    debug_prompts: bool = False
    heartbeat_interval_seconds: int = 900
    user_message_grace_seconds: float = 5.0
