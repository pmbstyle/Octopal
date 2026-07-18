from __future__ import annotations

import json
import os

import pytest

from octopal.infrastructure.config.models import OctopalConfig
from octopal.infrastructure.config.settings import load_config, load_settings, save_config


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


def test_load_settings_accepts_desktop_user_channel(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps({"user_channel": "desktop"}),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.user_channel == "desktop"


def test_load_settings_syncs_webclaw_runtime_config(tmp_path, monkeypatch) -> None:
    binary = tmp_path / "webclaw"
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "web": {
                    "webclaw_enabled": True,
                    "webclaw_binary": str(binary),
                    "webclaw_timeout_seconds": 17.5,
                    "webclaw_prefer_local": True,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.webclaw_enabled is True
    assert settings.webclaw_binary == str(binary)
    assert settings.webclaw_timeout_seconds == 17.5
    assert settings.webclaw_prefer_local is True


def test_load_settings_syncs_pinchtab_runtime_config(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "browser": {
                    "backend": "pinchtab",
                    "pinchtab_base_url": "http://127.0.0.1:9867",
                    "pinchtab_worker_base_url": "http://host.docker.internal:9867",
                    "pinchtab_token": "server-token",
                    "pinchtab_session": "ses_worker",
                    "pinchtab_browser": "cloak",
                    "pinchtab_timeout_seconds": 12.5,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.browser_backend == "pinchtab"
    assert settings.pinchtab_base_url == "http://127.0.0.1:9867"
    assert settings.pinchtab_worker_base_url == "http://host.docker.internal:9867"
    assert settings.pinchtab_token == "server-token"
    assert settings.pinchtab_session == "ses_worker"
    assert settings.pinchtab_browser == "cloak"
    assert settings.pinchtab_timeout_seconds == 12.5


def test_load_settings_syncs_local_onnx_embedding_config(tmp_path, monkeypatch) -> None:
    model_dir = tmp_path / "models" / "multilingual-e5-small"
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "memory": {
                    "local_model_dir": str(model_dir),
                    "local_threads": 2,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.memory_local_embedding_model_dir == model_dir
    assert settings.memory_local_embedding_threads == 2


def test_load_settings_defaults_to_empty_telegram_values_without_config_json(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.telegram_bot_token == ""
    assert settings.allowed_telegram_chat_ids == ""


def test_load_settings_adds_managed_web_defaults_to_legacy_config(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps({"user_channel": "desktop"}),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.webclaw_enabled is True
    assert settings.webclaw_prefer_local is True
    assert settings.browser_backend == "auto"
    assert settings.pinchtab_managed is True
    assert settings.pinchtab_image == "pinchtab/pinchtab:0.11.0"
    assert settings.pinchtab_fallback_to_playwright is True


def test_load_settings_keeps_environment_values_without_config_json(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "legacy-token")
    monkeypatch.setenv("ALLOWED_TELEGRAM_CHAT_IDS", "123,456")
    monkeypatch.setenv("OCTOPAL_GATEWAY_PORT", "9123")

    settings = load_settings()

    assert settings.telegram_bot_token == "legacy-token"
    assert settings.allowed_telegram_chat_ids == "123,456"
    assert settings.gateway_port == 9123
    assert settings.config_obj is not None
    assert settings.config_obj.telegram.bot_token == "legacy-token"
    assert settings.config_obj.telegram.allowed_chat_ids == ["123", "456"]
    assert settings.config_obj.gateway.port == 9123


def test_load_settings_preserves_runtime_provider_precedence_during_migration(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ZAI_API_KEY", "zai-key")
    monkeypatch.setenv("OPENROUTER_API_KEY", "openrouter-key")

    settings = load_settings()

    assert settings.config_obj is not None
    assert settings.config_obj.llm.provider_id == "zai"
    assert settings.config_obj.llm.model == settings.zai_model
    assert settings.config_obj.llm.api_key == "zai-key"


def test_load_settings_honors_explicit_legacy_openrouter_selection(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OCTOPAL_LLM_PROVIDER", "openrouter")
    monkeypatch.setenv("ZAI_API_KEY", "zai-key")
    monkeypatch.setenv("OPENROUTER_API_KEY", "openrouter-key")

    settings = load_settings()

    assert settings.config_obj is not None
    assert settings.config_obj.llm.provider_id == "openrouter"
    assert settings.config_obj.llm.model == settings.openrouter_model
    assert settings.config_obj.llm.api_key == "openrouter-key"


def test_load_settings_reads_dotenv_before_structured_config_exists(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=dotenv-token\nALLOWED_TELEGRAM_CHAT_IDS=777\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("ALLOWED_TELEGRAM_CHAT_IDS", raising=False)

    settings = load_settings()

    assert settings.telegram_bot_token == "dotenv-token"
    assert settings.allowed_telegram_chat_ids == "777"


def test_load_config_rejects_malformed_json(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{invalid", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="Invalid configuration file") as exc_info:
        load_config()

    assert str(config_path) in str(exc_info.value)


def test_save_config_is_atomic_private_and_honors_explicit_new_path(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "nested" / "custom.json"
    monkeypatch.setenv("OCTOPAL_CONFIG_FILE", str(config_path))

    save_config(OctopalConfig(telegram={"bot_token": "secret"}))

    assert json.loads(config_path.read_text(encoding="utf-8"))["telegram"]["bot_token"] == "secret"
    assert list(config_path.parent.glob(".custom.json.*.tmp")) == []
    if os.name != "nt":
        assert config_path.stat().st_mode & 0o777 == 0o600


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


@pytest.mark.parametrize("source", ["dotenv", "environment"])
def test_load_settings_ignores_invalid_legacy_value_owned_by_structured_config(
    tmp_path, monkeypatch, source
) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps({"gateway": {"port": 9123}}),
        encoding="utf-8",
    )
    if source == "dotenv":
        (tmp_path / ".env").write_text("OCTOPAL_GATEWAY_PORT=not-a-number\n", encoding="utf-8")
    else:
        monkeypatch.setenv("OCTOPAL_GATEWAY_PORT", "not-a-number")
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.gateway_port == 9123


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


def test_load_settings_syncs_group_addressing_and_whatsapp_group_chats(
    tmp_path, monkeypatch
) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "group_addressing": {
                    "enabled": True,
                    "agent_name": "Alice",
                    "agent_aliases": ["Alice", "AliceBot"],
                    "collective_aliases": ["Octopals", "agents"],
                },
                "whatsapp": {
                    "allowed_chats": ["120363123456789@g.us"],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = load_settings()

    assert settings.group_addressing_enabled is True
    assert settings.group_agent_name == "Alice"
    assert settings.group_agent_aliases == "Alice,AliceBot"
    assert settings.group_collective_aliases == "Octopals,agents"
    assert settings.allowed_whatsapp_chats == "120363123456789@g.us"
