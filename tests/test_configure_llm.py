from __future__ import annotations

from broodmind.cli.configure import _configure_llm
from broodmind.infrastructure.config.models import BroodMindConfig, LLMConfig


def test_configure_llm_quick_mode_allows_custom_base_url(monkeypatch) -> None:
    config = BroodMindConfig()
    llm = LLMConfig(provider_id="openrouter")

    int_answers = iter([1])
    prompt_answers = iter(
        [
            "router-key",
            "anthropic/claude-sonnet-4",
            "https://custom.router/v1",
        ]
    )
    confirm_answers = iter([False])

    monkeypatch.setattr(
        "broodmind.cli.configure.IntPrompt.ask",
        lambda *args, **kwargs: next(int_answers),
    )
    monkeypatch.setattr(
        "broodmind.cli.configure.Prompt.ask",
        lambda *args, **kwargs: next(prompt_answers),
    )
    monkeypatch.setattr(
        "broodmind.cli.configure.Confirm.ask",
        lambda *args, **kwargs: next(confirm_answers),
    )

    _configure_llm(config, "Worker (Default)", llm, advanced=False)

    assert llm.provider_id == "openrouter"
    assert llm.api_base == "https://custom.router/v1"


def test_configure_llm_quick_mode_can_keep_recommended_base_url(monkeypatch) -> None:
    config = BroodMindConfig()
    llm = LLMConfig(provider_id="openrouter")

    int_answers = iter([1])
    prompt_answers = iter(
        [
            "router-key",
            "anthropic/claude-sonnet-4",
        ]
    )
    confirm_answers = iter([True])

    monkeypatch.setattr(
        "broodmind.cli.configure.IntPrompt.ask",
        lambda *args, **kwargs: next(int_answers),
    )
    monkeypatch.setattr(
        "broodmind.cli.configure.Prompt.ask",
        lambda *args, **kwargs: next(prompt_answers),
    )
    monkeypatch.setattr(
        "broodmind.cli.configure.Confirm.ask",
        lambda *args, **kwargs: next(confirm_answers),
    )

    _configure_llm(config, "Queen", llm, advanced=False)

    assert llm.api_base == "https://openrouter.ai/api/v1"
