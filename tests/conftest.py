from __future__ import annotations

import pytest

from octopal.infrastructure.config.settings import Settings


@pytest.fixture(autouse=True)
def isolate_settings_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep developer credentials and local config overrides out of tests."""
    aliases = {
        field.alias
        for field in Settings.model_fields.values()
        if isinstance(field.alias, str) and field.alias
    }
    aliases.update({"OCTOPAL_CONFIG_FILE", "Z_AI_API_KEY"})
    for alias in aliases:
        monkeypatch.delenv(alias, raising=False)
