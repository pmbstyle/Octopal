from __future__ import annotations

import json
import uuid

import pytest
from fastapi.testclient import TestClient

from octopal.gateway.app import build_app
from octopal.infrastructure.config.models import ConnectorInstanceConfig, OctopalConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.store.models import AuditEvent, WorkerRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.state import (
    update_last_internal_heartbeat,
    update_last_message,
    update_last_scheduler_tick,
    write_start_status,
)
from octopal.utils import utc_now


def _make_client(tmp_path, *, token: str = "") -> TestClient:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_DASHBOARD_TOKEN=token,
    )
    app = build_app(settings)
    return TestClient(app)


def test_dashboard_v2_routes_return_contract_envelopes(tmp_path) -> None:
    client = _make_client(tmp_path)
    cases = [
        ("/api/dashboard/v2/overview", "dashboard.v2.overview"),
        ("/api/dashboard/v2/incidents", "dashboard.v2.incidents"),
        ("/api/dashboard/v2/octo", "dashboard.v2.octo"),
        ("/api/dashboard/v2/workers", "dashboard.v2.workers"),
        ("/api/dashboard/v2/system", "dashboard.v2.system"),
        ("/api/dashboard/v2/actions", "dashboard.v2.actions"),
    ]

    for route, version in cases:
        response = client.get(route)
        assert response.status_code == 200
        payload = response.json()
        assert payload["contract_version"] == version
        assert "generated_at" in payload
        assert "filters" in payload


def test_dashboard_v2_routes_require_token_when_configured(tmp_path) -> None:
    client = _make_client(tmp_path, token="secret-token")

    unauthorized = client.get("/api/dashboard/v2/overview")
    assert unauthorized.status_code == 401

    authorized = client.get(
        "/api/dashboard/v2/overview",
        headers={"x-octopal-token": "secret-token"},
    )
    assert authorized.status_code == 200
    assert authorized.json()["contract_version"] == "dashboard.v2.overview"


def test_dashboard_skills_route_requires_token_when_configured(tmp_path) -> None:
    client = _make_client(tmp_path, token="secret-token")

    unauthorized = client.get("/api/dashboard/skills")
    assert unauthorized.status_code == 401

    authorized = client.get(
        "/api/dashboard/skills",
        headers={"x-octopal-token": "secret-token"},
    )
    assert authorized.status_code == 200
    assert authorized.json()["contract_version"] == "dashboard.skills.v1"


def test_dashboard_skills_api_manages_local_skill(tmp_path) -> None:
    client = _make_client(tmp_path)
    skill_dir = tmp_path / "workspace" / "skills" / "writer"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: Writer
description: Helps write copy
---

# Writer
""",
        encoding="utf-8",
    )

    listed = client.get("/api/dashboard/skills")

    assert listed.status_code == 200
    payload = listed.json()
    assert payload["contract_version"] == "dashboard.skills.v1"
    assert payload["count"] == 1
    skill = payload["skills"][0]
    assert skill["id"] == "writer"
    assert skill["name"] == "Writer"
    assert skill["description"] == "Helps write copy"
    assert skill["enabled"] is True
    assert skill["actions"]["can_disable"] is True

    disabled = client.post("/api/dashboard/skills/writer/disable")

    assert disabled.status_code == 200
    disabled_skill = disabled.json()["skill"]
    assert disabled_skill["enabled"] is False
    assert disabled_skill["status"] == "disabled"
    assert disabled_skill["actions"]["can_enable"] is True

    enabled = client.post("/api/dashboard/skills/writer/enable")

    assert enabled.status_code == 200
    assert enabled.json()["skill"]["enabled"] is True

    deleted = client.delete("/api/dashboard/skills/writer")

    assert deleted.status_code == 200
    assert deleted.json()["skills"]["count"] == 0
    assert not skill_dir.exists()


def test_dashboard_skills_api_installs_local_skill_source(tmp_path) -> None:
    client = _make_client(tmp_path)
    source_dir = tmp_path / "source-writer"
    source_dir.mkdir(parents=True)
    (source_dir / "SKILL.md").write_text(
        """---
name: Imported Writer
description: Imported copy helper
---
""",
        encoding="utf-8",
    )

    installed = client.post("/api/dashboard/skills/install", json={"source": str(source_dir)})

    assert installed.status_code == 200
    payload = installed.json()
    assert payload["status"] == "installed"
    assert payload["skill_id"] == "imported_writer"
    assert payload["install"]["source_kind"] == "local_dir"
    assert payload["skill"]["name"] == "Imported Writer"
    assert payload["skill"]["origin"] == "installed"

    listed = client.get("/api/dashboard/skills")
    assert listed.json()["count"] == 1
    assert listed.json()["skills"][0]["id"] == "imported_writer"




def test_dashboard_connector_apply_reloads_config_and_reconciles_runtime(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    app = build_app(settings)
    next_config = OctopalConfig()
    next_config.connectors.instances["github"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["repos"],
        auth={"authorized_services": ["repos"], "access_token": "ghp_test"},
    )

    class _Connector:
        async def get_status(self):
            return {
                "status": "ready",
                "services": ["repos"],
                "message": "GitHub connector is ready.",
            }

    class _ConnectorManager:
        def __init__(self) -> None:
            self.config = None
            self.octo_config = None
            self.reconciled: list[str] = []
            self.connector = _Connector()

        def get_connector(self, name: str):
            return self.connector if name == "github" else None

        async def reconcile_connector_runtime(self, name: str) -> None:
            self.reconciled.append(name)

        async def load_and_start_all(self) -> None:
            self.reconciled.append("*")

        async def get_all_statuses(self):
            return {"github": await self.connector.get_status()}

    manager = _ConnectorManager()
    app.state.octo = type("_Octo", (), {"connector_manager": manager})()
    monkeypatch.setattr("octopal.gateway.dashboard.load_config", lambda: next_config)

    client = TestClient(app)
    response = client.post("/api/dashboard/connectors/apply", json={"name": "github"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "applied"
    assert payload["connectors"]["github"]["status"] == "ready"
    assert manager.config is next_config.connectors
    assert manager.octo_config is next_config
    assert manager.reconciled == ["github"]


def test_dashboard_v2_stream_route_is_registered(tmp_path) -> None:
    client = _make_client(tmp_path)
    schema = client.get("/openapi.json")
    assert schema.status_code == 200
    payload = schema.json()
    assert "/api/dashboard/v2/stream" in payload.get("paths", {})


def test_dashboard_v2_workers_exposes_worker_result_details(tmp_path) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    app = build_app(settings)
    store = SQLiteStore(settings)
    now = utc_now()
    store.create_worker(
        WorkerRecord(
            id="worker-12345678",
            status="completed",
            task="Summarize latest sync",
            granted_caps=[],
            created_at=now,
            updated_at=now,
            summary="Sync finished successfully",
            output={"report": {"status": "ok", "items": 3}},
            tools_used=["web_search", "web_fetch"],
            template_name="Research Worker",
        )
    )
    store.append_audit(
        AuditEvent(
            id=str(uuid.uuid4()),
            ts=now,
            correlation_id="worker-12345678",
            level="info",
            event_type="worker_spawned",
            data={"template_id": "researcher", "task": "Summarize latest sync"},
        )
    )
    store.append_audit(
        AuditEvent(
            id=str(uuid.uuid4()),
            ts=now,
            correlation_id="worker-12345678",
            level="info",
            event_type="worker_result",
            data={"summary": "Sync finished successfully"},
        )
    )
    app.state.dashboard_store = store
    client = TestClient(app)

    headers = {"x-octopal-token": settings.dashboard_token} if settings.dashboard_token else {}
    response = client.get("/api/dashboard/v2/workers", headers=headers)
    assert response.status_code == 200

    payload = response.json()
    recent = payload["workers"]["recent"]
    assert len(recent) == 1
    assert recent[0]["summary"] == "Sync finished successfully"
    assert recent[0]["result_preview"] == "Sync finished successfully"
    assert recent[0]["output"] == {"report": {"status": "ok", "items": 3}}
    assert recent[0]["created_at"] == now.isoformat()
    assert [event["event_type"] for event in recent[0]["audit_timeline"]] == [
        "worker_spawned",
        "worker_result",
    ]
    assert "Sync finished successfully" in recent[0]["audit_timeline"][1]["data_preview"]


def test_dashboard_v2_workers_returns_16_recent_workers_by_default(tmp_path) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    app = build_app(settings)
    store = SQLiteStore(settings)
    now = utc_now()
    for index in range(20):
        created_at = now.replace(microsecond=index)
        store.create_worker(
            WorkerRecord(
                id=f"worker-{index:02d}",
                status="completed",
                task=f"Task {index}",
                granted_caps=[],
                created_at=created_at,
                updated_at=created_at,
            )
        )
    app.state.dashboard_store = store
    client = TestClient(app)

    headers = {"x-octopal-token": settings.dashboard_token} if settings.dashboard_token else {}
    response = client.get("/api/dashboard/v2/workers", headers=headers)
    assert response.status_code == 200

    payload = response.json()
    recent = payload["workers"]["recent"]
    assert len(recent) == 16
    assert recent[0]["id"] == "worker-19"
    assert recent[-1]["id"] == "worker-04"


def test_dashboard_v2_uses_whatsapp_metrics_for_active_channel(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_USER_CHANNEL="whatsapp",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    write_start_status(settings)
    (settings.state_dir / "runtime_metrics.json").write_text(
        json.dumps(
            {
                "telegram": {
                    "chat_queues": 0,
                    "send_tasks": 0,
                    "updated_at": "2026-03-01T00:00:00+00:00",
                },
                "whatsapp": {
                    "connected": 1,
                    "chat_mappings": 2,
                    "updated_at": utc_now().isoformat(),
                },
                "octo": {
                    "followup_queues": 0,
                    "internal_queues": 0,
                    "followup_tasks": 0,
                    "internal_tasks": 0,
                },
                "exec_run": {
                    "background_sessions_running": 0,
                    "background_sessions_total": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "octopal.gateway.dashboard.get_worker_launcher_status",
        lambda _settings: type(
            "_Status",
            (),
            {
                "configured_launcher": "docker",
                "effective_launcher": "docker",
                "available": True,
                "reason": "Docker worker runtime is ready.",
            },
        )(),
    )

    app = build_app(settings)
    client = TestClient(app)
    headers = {"x-octopal-token": settings.dashboard_token} if settings.dashboard_token else {}

    overview = client.get("/api/dashboard/v2/overview", headers=headers)
    assert overview.status_code == 200
    overview_payload = overview.json()
    assert overview_payload["health"]["status"] == "ok"
    assert "Telegram" not in overview_payload["health"]["summary"]
    assert all("Telegram:" not in reason for reason in overview_payload["health"]["reasons"])
    assert overview_payload["system"]["active_channel"] == "WhatsApp"
    assert overview_payload["system"]["active_channel_id"] == "whatsapp"

    octo = client.get("/api/dashboard/v2/octo", headers=headers)
    assert octo.status_code == 200
    octo_payload = octo.json()
    assert octo_payload["queues"]["active_channel"] == "whatsapp"
    assert octo_payload["queues"]["active_channel_label"] == "WhatsApp"
    assert octo_payload["queues"]["channel_connected"] == 1
    assert octo_payload["queues"]["channel_chat_mappings"] == 2
    assert octo_payload["queues"]["channel_queue_depth"] == 0
    assert octo_payload["queues"]["channel_send_tasks"] is None


def test_dashboard_system_and_settings_include_worker_launcher_health(tmp_path, monkeypatch) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    write_start_status(settings)

    monkeypatch.setattr(
        "octopal.gateway.dashboard.get_worker_launcher_status",
        lambda _settings: type(
            "_Status",
            (),
            {
                "configured_launcher": "docker",
                "effective_launcher": "same_env",
                "available": False,
                "reason": "Docker image 'octopal-worker:latest' is not available.",
            },
        )(),
    )

    app = build_app(settings)
    client = TestClient(app)
    headers = {"x-octopal-token": settings.dashboard_token} if settings.dashboard_token else {}

    system_resp = client.get("/api/dashboard/v2/system", headers=headers)
    assert system_resp.status_code == 200
    system_payload = system_resp.json()
    assert system_payload["system"]["worker_launcher"]["configured"] == "docker"
    assert system_payload["system"]["worker_launcher"]["effective"] == "same_env"
    assert system_payload["system"]["worker_launcher"]["available"] is False

    settings_resp = client.get("/api/dashboard/settings", headers=headers)
    assert settings_resp.status_code == 200
    settings_payload = settings_resp.json()
    assert settings_payload["worker_launcher"]["configured"] == "docker"
    assert settings_payload["worker_launcher"]["effective"] == "same_env"


def test_dashboard_system_uses_canonical_status_timestamps(tmp_path, monkeypatch) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    write_start_status(settings)
    update_last_message(settings)
    update_last_internal_heartbeat(settings)
    update_last_scheduler_tick(settings, status="ok")

    monkeypatch.setattr(
        "octopal.gateway.dashboard.get_worker_launcher_status",
        lambda _settings: type(
            "_Status",
            (),
            {
                "configured_launcher": "docker",
                "effective_launcher": "docker",
                "available": True,
                "reason": "Docker worker runtime is ready.",
            },
        )(),
    )

    app = build_app(settings)
    client = TestClient(app)
    headers = {"x-octopal-token": settings.dashboard_token} if settings.dashboard_token else {}

    response = client.get("/api/dashboard/v2/system", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    system = payload["system"]
    assert system["last_heartbeat"]
    assert system["last_user_message_at"]
    assert system["last_scheduler_tick_at"]
    assert system["last_scheduler_tick_status"] == "ok"

    gateway_service = next(service for service in payload["services"] if service["id"] == "gateway")
    assert gateway_service["updated_at"] == system["status_updated_at"]


def test_dashboard_config_can_be_read_and_updated(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_DASHBOARD_TOKEN="",
    )
    settings.config_obj = OctopalConfig()
    app = build_app(settings)
    client = TestClient(app)
    headers = {"x-octopal-token": settings.dashboard_token} if settings.dashboard_token else {}

    response = client.get("/api/dashboard/config", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["gateway"]["port"] == 8000
    assert payload["config"]["workers"]["launcher"] == "docker"
    assert any(item["id"] == "openai" for item in payload["providers"])

    updated = payload["config"]
    updated["user_channel"] = "whatsapp"
    updated["log_level"] = "DEBUG"
    updated["gateway"]["port"] = 9010
    updated["gateway"]["webapp_enabled"] = True
    updated["workers"]["docker_image"] = "octopal-worker:test"
    updated["memory"]["top_k"] = 9
    updated["search"]["brave_api_key"] = "brave-test"
    updated["telegram"]["allowed_chat_ids"] = ["12345"]
    updated["whatsapp"]["allowed_numbers"] = ["+15551234567"]

    save_response = client.put("/api/dashboard/config", json=updated, headers=headers)
    assert save_response.status_code == 200
    saved_payload = save_response.json()
    assert saved_payload["status"] == "saved"
    assert saved_payload["config"]["gateway"]["port"] == 9010
    assert saved_payload["config"]["workers"]["docker_image"] == "octopal-worker:test"

    assert settings.gateway_port == 9010
    assert settings.webapp_enabled is True
    assert settings.worker_docker_image == "octopal-worker:test"
    assert settings.memory_top_k == 9
    assert settings.user_channel == "whatsapp"

    config_file = tmp_path / "config.json"
    assert config_file.exists()
    persisted = json.loads(config_file.read_text(encoding="utf-8"))
    assert persisted["gateway"]["port"] == 9010
    assert persisted["workers"]["docker_image"] == "octopal-worker:test"
    assert persisted["search"]["brave_api_key"] == "brave-test"


def test_dashboard_config_redacts_and_preserves_secrets(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "telegram": {"bot_token": "telegram-secret", "allowed_chat_ids": [], "parse_mode": "MarkdownV2"},
                "llm": {"provider_id": "openrouter", "model": "x", "api_key": "llm-secret", "api_base": None, "model_prefix": None},
                "worker_llm_default": {"provider_id": "openrouter", "model": "y", "api_key": "worker-secret", "api_base": None, "model_prefix": None},
                "gateway": {"host": "0.0.0.0", "port": 8000, "tailscale_ips": "", "dashboard_token": "dash-secret", "tailscale_auto_serve": True, "webapp_enabled": False, "webapp_dist_dir": None},
                "whatsapp": {"mode": "separate", "allowed_numbers": [], "auth_dir": None, "bridge_host": "127.0.0.1", "bridge_port": 8765, "callback_token": "wa-secret", "node_command": "node"},
                "search": {"brave_api_key": "brave-secret", "firecrawl_api_key": "fire-secret"},
            }
        ),
        encoding="utf-8",
    )

    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_DASHBOARD_TOKEN="",
    )
    app = build_app(settings)
    client = TestClient(app)

    response = client.get("/api/dashboard/config")
    assert response.status_code == 200
    payload = response.json()["config"]
    assert payload["telegram"]["bot_token"] == ""
    assert payload["llm"]["api_key"] is None
    assert payload["worker_llm_default"]["api_key"] is None
    assert payload["gateway"]["dashboard_token"] == ""
    assert payload["whatsapp"]["callback_token"] == ""
    assert payload["search"]["brave_api_key"] is None
    assert payload["search"]["firecrawl_api_key"] is None
    assert any(item["id"] == "openrouter" for item in response.json()["providers"])

    payload["gateway"]["port"] = 9001
    save_response = client.put("/api/dashboard/config", json=payload)
    assert save_response.status_code == 200

    persisted = json.loads(config_path.read_text(encoding="utf-8"))
    assert persisted["telegram"]["bot_token"] == "telegram-secret"
    assert persisted["llm"]["api_key"] == "llm-secret"
    assert persisted["worker_llm_default"]["api_key"] == "worker-secret"
    assert persisted["gateway"]["dashboard_token"] == "dash-secret"
    assert persisted["whatsapp"]["callback_token"] == "wa-secret"
    assert persisted["search"]["brave_api_key"] == "brave-secret"
    assert persisted["search"]["firecrawl_api_key"] == "fire-secret"
    assert persisted["gateway"]["port"] == 9001

    payload["llm"]["provider_id"] = "openai"
    payload["worker_llm_default"]["provider_id"] = "openai"
    provider_changed_response = client.put("/api/dashboard/config", json=payload, headers={"x-octopal-token": "dash-secret"})
    assert provider_changed_response.status_code == 200

    changed = json.loads(config_path.read_text(encoding="utf-8"))
    assert changed["llm"]["api_key"] is None
    assert changed["worker_llm_default"]["api_key"] is None
