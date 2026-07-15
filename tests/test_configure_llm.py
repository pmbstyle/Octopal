from __future__ import annotations

from octopal.cli.configure import _configure_llm
from octopal.infrastructure.config.models import LLMConfig, OctopalConfig


def test_configure_llm_keeps_existing_custom_base_url_for_same_provider(monkeypatch) -> None:
    config = OctopalConfig()
    llm = LLMConfig(
        provider_id="openrouter",
        api_base="https://custom.router/v1",
    )

    int_answers = iter([1])
    prompt_answers = iter(
        [
            "router-key",
            "x-ai/grok-4.3",
            "https://custom.router/v1",
        ]
    )

    monkeypatch.setattr(
        "octopal.cli.configure.IntPrompt.ask",
        lambda *args, **kwargs: next(int_answers),
    )
    monkeypatch.setattr(
        "octopal.cli.configure.Prompt.ask",
        lambda *args, **kwargs: next(prompt_answers),
    )

    _configure_llm(config, "Worker (Default)", llm)

    assert llm.provider_id == "openrouter"
    assert llm.api_base == "https://custom.router/v1"


def test_configure_llm_resets_base_url_to_provider_preset_when_switching(monkeypatch) -> None:
    config = OctopalConfig()
    llm = LLMConfig(
        provider_id="zai",
        api_base="https://custom.z.ai/router",
    )
    captured_defaults: dict[str, str] = {}

    int_answers = iter([2])

    def fake_prompt_ask(message: str, default: str | None = None, password: bool = False):
        if message == "Minimax API key":
            return "mini-key"
        if message == "Minimax model":
            return "MiniMax-M2.7"
        if message == "Minimax base URL":
            captured_defaults["base_url"] = default or ""
            return default or ""
        raise AssertionError(f"Unexpected prompt: {message}")

    monkeypatch.setattr(
        "octopal.cli.configure.IntPrompt.ask",
        lambda *args, **kwargs: next(int_answers),
    )
    monkeypatch.setattr(
        "octopal.cli.configure.Prompt.ask",
        fake_prompt_ask,
    )

    _configure_llm(config, "Worker (Default)", llm)

    assert llm.provider_id == "minimax"
    assert captured_defaults["base_url"] == "https://api.minimax.io/v1"
    assert llm.api_base == "https://api.minimax.io/v1"
