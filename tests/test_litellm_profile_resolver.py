from __future__ import annotations

from octopal.infrastructure.config.models import LLMConfig, OctopalConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers.profile_resolver import resolve_litellm_profile


def _base_settings(**overrides) -> Settings:
    defaults = {
        "telegram_bot_token": "test-token",
        "llm_provider": "litellm",
        "litellm_provider_id": None,
        "litellm_model": None,
        "litellm_api_key": None,
        "litellm_api_base": None,
        "litellm_model_prefix": None,
        "litellm_num_retries": 0,
        "litellm_timeout": 30.0,
        "litellm_fallbacks": None,
        "litellm_drop_params": True,
        "litellm_caching": False,
        "litellm_max_concurrency": 2,
        "litellm_rate_limit_max_retries": 0,
        "litellm_rate_limit_base_delay_seconds": 1.0,
        "litellm_rate_limit_max_delay_seconds": 30.0,
        "openrouter_api_key": None,
        "openrouter_model": "x-ai/grok-4.3",
        "openrouter_base_url": "https://openrouter.ai/api/v1",
        "openrouter_timeout": 30.0,
        "zai_api_key": None,
        "zai_model": "glm-5.1",
        "zai_base_url": "https://api.z.ai/api/paas/v4/",
        "zai_chat_path": "/chat/completions",
        "zai_timeout_seconds": 45.0,
        "zai_connect_timeout_seconds": 15.0,
        "zai_accept_language": "en-US,en",
        "debug_prompts": False,
    }
    defaults.update(overrides)
    return Settings.model_construct(**defaults)


def test_resolver_prefers_unified_profile_fields() -> None:
    settings = _base_settings(
        litellm_provider_id="openrouter",
        litellm_model="google/gemini-2.0-pro",
        litellm_api_key="unified-key",
        litellm_api_base="https://custom.router/v1",
        litellm_model_prefix="openrouter",
        openrouter_api_key="legacy-key",
    )

    profile = resolve_litellm_profile(settings)

    assert profile.provider_id == "openrouter"
    assert profile.source == "unified"
    assert profile.model == "openrouter/google/gemini-2.0-pro"
    assert profile.api_key == "unified-key"
    assert profile.api_base == "https://custom.router/v1"


def test_resolver_falls_back_to_legacy_openrouter_mode() -> None:
    settings = _base_settings(
        llm_provider="openrouter",
        openrouter_api_key="legacy-openrouter-key",
    )

    profile = resolve_litellm_profile(settings)

    assert profile.provider_id == "openrouter"
    assert profile.source == "legacy"
    assert profile.model == "openrouter/x-ai/grok-4.3"
    assert profile.api_key == "legacy-openrouter-key"


def test_resolver_supports_local_ollama_without_api_key() -> None:
    settings = _base_settings(
        litellm_provider_id="ollama",
        litellm_model="llama3.2:latest",
        litellm_api_key="",
        litellm_api_base="http://localhost:11434",
    )

    profile = resolve_litellm_profile(settings)

    assert profile.provider_id == "ollama"
    assert profile.requires_api_key is False
    assert profile.model == "ollama/llama3.2:latest"
    assert profile.api_key is None


def test_resolver_migrates_legacy_minimax_anthropic_base_to_openai_route() -> None:
    settings = _base_settings(
        litellm_provider_id="minimax",
        litellm_model="MiniMax-M3",
        litellm_api_key="mini-key",
        litellm_api_base="https://api.minimax.io/anthropic/v1",
    )

    profile = resolve_litellm_profile(settings)

    assert profile.model == "minimax/MiniMax-M3"
    assert profile.api_base == "https://api.minimax.io/v1"


def test_resolver_preserves_custom_minimax_anthropic_proxy_base() -> None:
    settings = _base_settings(
        litellm_provider_id="minimax",
        litellm_model="MiniMax-M3",
        litellm_api_key="mini-key",
        litellm_api_base="https://proxy.example/anthropic/v1",
    )

    profile = resolve_litellm_profile(settings)

    assert profile.api_base == "https://proxy.example/anthropic/v1"


def test_worker_override_does_not_inherit_octo_unified_api_key_for_other_provider() -> None:
    settings = _base_settings(
        litellm_provider_id="zai",
        litellm_model="glm-5.1",
        litellm_api_key="octo-zai-key",
        openrouter_api_key=None,
        config_obj=OctopalConfig(
            llm=LLMConfig(provider_id="zai", model="glm-5.1", api_key="octo-zai-key"),
            worker_llm_default=LLMConfig(provider_id="openrouter", model="x-ai/grok-4.3"),
        ),
    )

    profile = resolve_litellm_profile(
        settings,
        config_override=settings.config_obj.worker_llm_default,
    )

    assert profile.provider_id == "openrouter"
    assert profile.api_key is None
