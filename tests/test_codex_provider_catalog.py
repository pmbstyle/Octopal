from octopal.infrastructure.config.models import LLMConfig, OctopalConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers.catalog import get_provider_catalog_entry
from octopal.infrastructure.providers.codex_provider import CodexProvider
from octopal.infrastructure.providers.factory import build_inference_provider


def test_codex_catalog_does_not_require_api_key() -> None:
    entry = get_provider_catalog_entry("codex")

    assert entry.label == "ChatGPT Codex"
    assert entry.requires_api_key is False
    assert entry.supports_custom_base_url is False


def test_provider_factory_uses_codex_provider_for_codex_profile() -> None:
    settings = Settings.model_construct(
        config_obj=OctopalConfig(llm=LLMConfig(provider_id="codex", model="gpt-5.4")),
        litellm_provider_id=None,
        litellm_model=None,
        litellm_api_key=None,
        litellm_api_base=None,
        litellm_model_prefix=None,
        llm_provider="litellm",
        openrouter_api_key=None,
        zai_api_key=None,
    )

    provider = build_inference_provider(settings)

    assert isinstance(provider, CodexProvider)
    assert provider.provider_id == "codex"
