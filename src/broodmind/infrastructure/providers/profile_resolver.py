from __future__ import annotations

from dataclasses import dataclass

from broodmind.infrastructure.config.models import LLMConfig
from broodmind.infrastructure.config.settings import Settings
from broodmind.infrastructure.providers.catalog import (
    ProviderCatalogEntry,
    get_provider_catalog_entry,
)


@dataclass(frozen=True)
class ResolvedLiteLLMProfile:
    provider_id: str
    label: str
    description: str
    model: str
    raw_model: str
    api_key: str | None
    api_base: str | None
    model_prefix: str | None
    source: str
    requires_api_key: bool
    entry: ProviderCatalogEntry


def resolve_litellm_profile(
    settings: Settings,
    model_override: str | None = None,
    config_override: LLMConfig | None = None,
) -> ResolvedLiteLLMProfile:
    # Prefer config_override if provided, otherwise fallback to settings.config_obj.llm
    # (Queen default) or legacy settings.
    config = config_override or (settings.config_obj.llm if settings.config_obj else None)

    provider_id, source = _resolve_provider_id(settings, config)
    entry = get_provider_catalog_entry(provider_id)

    shared_profile_values = config_override is None

    raw_model = _first_non_empty(
        model_override,
        config.model if config else None,
        settings.litellm_model if shared_profile_values else None,
        *_legacy_model_candidates(settings, provider_id),
        entry.default_model,
    )
    model_prefix = _first_non_empty(
        config.model_prefix if config else None,
        settings.litellm_model_prefix if shared_profile_values else None,
        entry.model_prefix,
    )
    api_base = _first_non_empty(
        config.api_base if config else None,
        settings.litellm_api_base if shared_profile_values else None,
        *_legacy_base_candidates(settings, provider_id),
        entry.default_api_base,
    )
    api_key = _first_non_empty(
        config.api_key if config else None,
        settings.litellm_api_key if shared_profile_values else None,
        *_legacy_key_candidates(settings, provider_id),
    )

    return ResolvedLiteLLMProfile(
        provider_id=provider_id,
        label=entry.label,
        description=entry.description,
        model=_qualify_model_name(raw_model, model_prefix, always_prefix=entry.always_prefix_model),
        raw_model=raw_model,
        api_key=api_key,
        api_base=api_base,
        model_prefix=model_prefix,
        source=source,
        requires_api_key=entry.requires_api_key,
        entry=entry,
    )


def _resolve_provider_id(settings: Settings, config: LLMConfig | None = None) -> tuple[str, str]:
    if config and config.provider_id:
        return config.provider_id.strip().lower(), "unified-config"

    explicit_provider = (settings.litellm_provider_id or "").strip().lower()
    if explicit_provider:
        return explicit_provider, "unified"

    legacy_provider = (settings.llm_provider or "").strip().lower()
    if legacy_provider == "openrouter":
        return "openrouter", "legacy"

    if (settings.zai_api_key or "").strip():
        return "zai", "legacy"
    if (settings.openrouter_api_key or "").strip():
        return "openrouter", "legacy"

    return "zai", "default"


def _legacy_key_candidates(settings: Settings, provider_id: str) -> tuple[str | None, ...]:
    if provider_id == "openrouter":
        return (settings.openrouter_api_key,)
    if provider_id == "zai":
        return (settings.zai_api_key,)
    if provider_id == "minimax":
        return (settings.minimax_api_key,)
    return ()


def _legacy_base_candidates(settings: Settings, provider_id: str) -> tuple[str | None, ...]:
    if provider_id == "openrouter":
        return (settings.openrouter_base_url,)
    if provider_id == "zai":
        return (settings.zai_base_url,)
    return ()


def _legacy_model_candidates(settings: Settings, provider_id: str) -> tuple[str | None, ...]:
    if provider_id == "openrouter":
        return (settings.openrouter_model,)
    if provider_id == "zai":
        return (settings.zai_model,)
    return ()


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _qualify_model_name(raw_model: str | None, model_prefix: str | None, *, always_prefix: bool = False) -> str:
    model_name = (raw_model or "").strip()
    prefix = (model_prefix or "").strip()
    if not model_name:
        return ""
    if not prefix:
        return model_name
    if always_prefix:
        if model_name.startswith(f"{prefix}/"):
            return model_name
        return f"{prefix}/{model_name}"
    if "/" in model_name:
        return model_name
    return f"{prefix}/{model_name}"
