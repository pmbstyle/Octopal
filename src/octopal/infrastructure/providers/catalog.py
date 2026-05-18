from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProviderCatalogEntry:
    id: str
    label: str
    description: str
    default_model: str
    model_prefix: str | None = None
    default_api_base: str | None = None
    requires_api_key: bool = True
    supports_custom_base_url: bool = True
    supports_custom_model: bool = True
    supports_model_prefix_override: bool = False
    always_prefix_model: bool = False
    api_key_label: str = "API key"
    model_label: str = "Model"
    base_url_label: str = "Base URL"


_CATALOG: tuple[ProviderCatalogEntry, ...] = (
    ProviderCatalogEntry(
        id="openrouter",
        label="OpenRouter",
        description="Hosted model router with OpenRouter model ids.",
        default_model="x-ai/grok-4.3",
        model_prefix="openrouter",
        always_prefix_model=True,
        default_api_base="https://openrouter.ai/api/v1",
        api_key_label="OpenRouter API key",
        model_label="OpenRouter model",
        base_url_label="OpenRouter base URL",
    ),
    ProviderCatalogEntry(
        id="zai",
        label="Z.ai (Coding plan)",
        description="GLM and Coding Plan endpoints via OpenAI-compatible LiteLLM routing.",
        default_model="glm-5.1",
        model_prefix="openai",
        default_api_base="https://api.z.ai/api/coding/paas/v4",
        api_key_label="Z.ai API key",
        model_label="Z.ai model",
        base_url_label="Z.ai base URL",
    ),
    ProviderCatalogEntry(
        id="openai",
        label="OpenAI",
        description="Direct OpenAI API access through LiteLLM.",
        default_model="gpt-5.5",
        model_prefix="openai",
        default_api_base="https://api.openai.com/v1",
        api_key_label="OpenAI API key",
        model_label="OpenAI model",
        base_url_label="OpenAI base URL",
    ),
    ProviderCatalogEntry(
        id="codex",
        label="ChatGPT Codex",
        description="ChatGPT subscription auth through the local Codex CLI app-server.",
        default_model="gpt-5.4",
        requires_api_key=False,
        supports_custom_base_url=False,
        api_key_label="ChatGPT login",
        model_label="Codex model",
    ),
    ProviderCatalogEntry(
        id="anthropic",
        label="Anthropic",
        description="Direct Anthropic Messages API through LiteLLM.",
        default_model="claude-opus-4-7",
        model_prefix="anthropic",
        default_api_base="https://api.anthropic.com",
        api_key_label="Anthropic API key",
        model_label="Anthropic model",
        base_url_label="Anthropic base URL",
    ),
    ProviderCatalogEntry(
        id="google",
        label="Google Gemini",
        description="Gemini API via LiteLLM.",
        default_model="gemini-3.1-pro-preview",
        model_prefix="gemini",
        default_api_base=None,
        supports_custom_base_url=False,
        api_key_label="Gemini API key",
        model_label="Gemini model",
    ),
    ProviderCatalogEntry(
        id="mistral",
        label="Mistral AI",
        description="Hosted Mistral API.",
        default_model="mistral-medium-3-5+1",
        model_prefix="mistral",
        default_api_base="https://api.mistral.ai/v1",
        api_key_label="Mistral API key",
        model_label="Mistral model",
        base_url_label="Mistral base URL",
    ),
    ProviderCatalogEntry(
        id="together",
        label="Together AI",
        description="Hosted open-model access through Together AI.",
        default_model="moonshotai/Kimi-K2.5",
        model_prefix="together_ai",
        default_api_base="https://api.together.xyz/v1",
        api_key_label="Together API key",
        model_label="Together model",
        base_url_label="Together base URL",
    ),
    ProviderCatalogEntry(
        id="groq",
        label="Groq",
        description="Fast hosted inference with OpenAI-compatible API surface.",
        default_model="openai/gpt-oss-120b",
        model_prefix="groq",
        default_api_base="https://api.groq.com/openai/v1",
        api_key_label="Groq API key",
        model_label="Groq model",
        base_url_label="Groq base URL",
    ),
    ProviderCatalogEntry(
        id="ollama",
        label="Ollama",
        description="Local Ollama instance using the OpenAI-compatible bridge.",
        default_model="llama3.2",
        model_prefix="ollama",
        default_api_base="http://localhost:11434",
        requires_api_key=False,
        api_key_label="Ollama API key (optional)",
        model_label="Ollama model",
        base_url_label="Ollama base URL",
    ),
    ProviderCatalogEntry(
        id="minimax",
        label="Minimax (Token plan)",
        description="MiniMax API (M2.5, M2.7, etc.) via LiteLLM.",
        default_model="MiniMax-M2.7",
        model_prefix="minimax",
        default_api_base="https://api.minimax.io/anthropic/v1",
        api_key_label="Minimax API key",
        model_label="Minimax model",
        base_url_label="Minimax base URL",
    ),
    ProviderCatalogEntry(
        id="custom",
        label="Custom OpenAI-compatible",
        description="Any custom LiteLLM target with configurable base URL and model prefix.",
        default_model="gpt-5.5",
        model_prefix="openai",
        default_api_base="http://localhost:8000/v1",
        requires_api_key=False,
        supports_model_prefix_override=True,
        api_key_label="API key (optional)",
        model_label="Model name",
        base_url_label="Base URL",
    ),
)


def list_registered_provider_ids(*, include_custom: bool = True) -> list[str]:
    ids = [entry.id for entry in _CATALOG if include_custom or entry.id != "custom"]
    return ids


def list_provider_catalog(*, include_custom: bool = True) -> list[ProviderCatalogEntry]:
    return [entry for entry in _CATALOG if include_custom or entry.id != "custom"]


def get_provider_catalog_entry(provider_id: str) -> ProviderCatalogEntry:
    normalized = (provider_id or "").strip().lower()
    for entry in _CATALOG:
        if entry.id == normalized:
            return entry
    return next(entry for entry in _CATALOG if entry.id == "custom")
