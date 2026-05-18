from __future__ import annotations

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.observability.base import TraceSink
from octopal.infrastructure.providers.base import InferenceProvider
from octopal.infrastructure.providers.codex_provider import CodexProvider
from octopal.infrastructure.providers.litellm_provider import LiteLLMProvider
from octopal.infrastructure.providers.profile_resolver import resolve_litellm_profile


def build_inference_provider(
    settings: Settings,
    *,
    model: str | None = None,
    config: LLMConfig | None = None,
    trace_sink: TraceSink | None = None,
) -> InferenceProvider:
    profile = resolve_litellm_profile(settings, model_override=model, config_override=config)
    if profile.provider_id == "codex":
        return CodexProvider(settings, model=model, config=config, trace_sink=trace_sink)
    return LiteLLMProvider(settings, model=model, config=config, trace_sink=trace_sink)
