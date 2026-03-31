from __future__ import annotations

import json

from octopal.infrastructure.config.settings import load_settings


def test_load_settings_uses_user_channel_from_config_json(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "user_channel": "whatsapp",
                "whatsapp": {
                    "mode": "separate",
                    "allowed_numbers": ["+15551234567"],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.user_channel == "whatsapp"
    assert settings.allowed_whatsapp_numbers == "+15551234567"


def test_load_settings_allows_config_json_to_clear_legacy_env_values(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=OLDTOKEN\nALLOWED_TELEGRAM_CHAT_IDS=123\n",
        encoding="utf-8",
    )
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "user_channel": "telegram",
                "telegram": {
                    "bot_token": "",
                    "allowed_chat_ids": [],
                    "parse_mode": "MarkdownV2",
                },
                "llm": {
                    "provider_id": "zai",
                    "model": "glm-5",
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.telegram_bot_token == ""
    assert settings.allowed_telegram_chat_ids == ""


def test_load_settings_migrates_legacy_connector_settings_shape(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "settings": {
                                "enabled_services": ["gmail"],
                                "client_id": "legacy-client-id",
                                "client_secret": "legacy-client-secret",
                                "authorized_services": ["gmail"],
                                "refresh_token": "legacy-refresh-token",
                                "token": "legacy-access-token",
                            },
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()
    google = settings.connectors.instances["google"]

    assert google.enabled_services == ["gmail"]
    assert google.credentials.client_id == "legacy-client-id"
    assert google.credentials.client_secret == "legacy-client-secret"
    assert google.auth.authorized_services == ["gmail"]
    assert google.auth.refresh_token == "legacy-refresh-token"
    assert google.auth.access_token == "legacy-access-token"
