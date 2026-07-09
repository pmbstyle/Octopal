from __future__ import annotations

import json
import os

from typer.testing import CliRunner

from octopal.cli.main import app

runner = CliRunner()


def test_config_migrate_reads_dotenv_and_writes_private_config(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "private" / "octopal.json"
    (tmp_path / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=legacy-token\n"
        "ALLOWED_TELEGRAM_CHAT_IDS=123,456\n"
        "OCTOPAL_LITELLM_PROVIDER_ID=openrouter\n"
        "OCTOPAL_LITELLM_MODEL=anthropic/claude-sonnet-4\n"
        "OCTOPAL_LITELLM_API_KEY=legacy-api-key\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OCTOPAL_CONFIG_FILE", str(config_path))

    result = runner.invoke(app, ["config", "migrate"])

    assert result.exit_code == 0, result.output
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["telegram"] == {
        "bot_token": "legacy-token",
        "allowed_chat_ids": ["123", "456"],
        "parse_mode": "MarkdownV2",
    }
    assert payload["llm"]["provider_id"] == "openrouter"
    assert payload["llm"]["model"] == "anthropic/claude-sonnet-4"
    assert payload["llm"]["api_key"] == "legacy-api-key"
    assert config_path.name in result.output
    if os.name != "nt":
        assert config_path.stat().st_mode & 0o777 == 0o600
