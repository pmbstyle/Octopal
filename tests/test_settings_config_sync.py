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


def test_load_settings_defaults_to_empty_telegram_values_without_config_json(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.telegram_bot_token == ""
    assert settings.allowed_telegram_chat_ids == ""


def test_load_settings_prefers_config_json_telegram_values(tmp_path, monkeypatch) -> None:
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
                    "model": "glm-5.1",
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


def test_load_settings_syncs_observability_config(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "observability": {
                    "enabled": True,
                    "backend": "langfuse",
                    "capture_content": True,
                    "preview_chars": 512,
                    "sample_rate": 0.25,
                    "langfuse_public_key": "pk-test",
                    "langfuse_secret_key": "sk-test",
                    "langfuse_host": "http://localhost:3000",
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.observability_enabled is True
    assert settings.observability_backend == "langfuse"
    assert settings.observability_capture_content is True
    assert settings.observability_preview_chars == 512
    assert settings.observability_sample_rate == 0.25
    assert settings.langfuse_public_key == "pk-test"
    assert settings.langfuse_secret_key == "sk-test"
    assert settings.langfuse_host == "http://localhost:3000"


def test_load_settings_syncs_a2a_config(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "a2a": {
                    "enabled": True,
                    "public_base_url": "https://octo.example",
                    "agent_name": "Alice",
                    "peers": {
                        "bob": {
                            "name": "Bob",
                            "base_url": "https://bob.example/a2a/v1",
                            "token": "peer-secret",
                            "capabilities": ["chat"],
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.a2a.enabled is True
    assert settings.a2a.public_base_url == "https://octo.example"
    assert settings.a2a.agent_name == "Alice"
    assert settings.a2a.peers["bob"].token == "peer-secret"
