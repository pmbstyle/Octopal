from __future__ import annotations

import asyncio
import json
import math
import os
import shutil
from collections import deque
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from octopal.channels import normalize_user_channel, user_channel_label
from octopal.infrastructure.config.models import (
    GatewayConfig,
    LiteLLMRuntimeConfig,
    LLMConfig,
    MemoryConfig,
    OctopalConfig,
    SearchConfig,
    StorageConfig,
    TelegramConfig,
    WhatsAppConfig,
    WorkerRuntimeConfig,
)
from octopal.infrastructure.config.settings import (
    Settings,
    _sync_settings_from_config,
    load_config,
    save_config,
)
from octopal.infrastructure.providers.catalog import list_provider_catalog
from octopal.infrastructure.store.models import AuditEvent, WorkerRecord, WorkerTemplateRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.metrics import read_metrics_snapshot
from octopal.runtime.octo_status import build_octo_status
from octopal.runtime.self_control import (
    SELF_UPDATE_ACTION,
    SELF_UPDATE_REQUESTED_BY,
    append_control_request,
    check_update_status,
)
from octopal.runtime.state import is_pid_running, read_status
from octopal.runtime.workers.launcher_factory import (
    WorkerLauncherStatus,
    get_worker_launcher_status,
)
from octopal.tools.skills.installer import install_skill_from_source
from octopal.tools.skills.management import (
    list_skill_inventory,
    remove_skill,
    set_skill_enabled,
)

_WINDOW_CHOICES = {15, 60, 240, 1440}
_SERVICE_CHOICES = {"all", "gateway", "octo", "telegram", "whatsapp", "exec_run", "mcp", "workers"}
_STREAM_TOPICS = {"overview", "incidents", "octo", "workers", "system", "actions", "snapshot"}
_EMPTY_ACTION_PAYLOAD = Body(default={})
logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class DashboardFilters:
    window_minutes: int
    service: str
    environment: str


class DashboardV2Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: str
    generated_at: str
    filters: dict[str, Any]


class DashboardOverviewV2(DashboardV2Envelope):
    health: dict[str, Any]
    kpis: dict[str, Any]
    services: list[dict[str, Any]]
    system: dict[str, Any]
    incidents_summary: dict[str, int]


class DashboardIncidentsV2(DashboardV2Envelope):
    incidents: dict[str, Any]


class DashboardOctoV2(DashboardV2Envelope):
    octo: dict[str, Any]
    queues: dict[str, Any]
    control: dict[str, Any]
    health: dict[str, Any]


class DashboardWorkersV2(DashboardV2Envelope):
    workers: dict[str, Any]


class DashboardSystemV2(DashboardV2Envelope):
    system: dict[str, Any]
    services: list[dict[str, Any]]
    connectivity: dict[str, Any]
    logs: list[dict[str, Any]]


class DashboardActionsV2(DashboardV2Envelope):
    actions: dict[str, Any]


class WorkerTemplatePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    description: str
    system_prompt: str
    available_tools: list[str] = Field(default_factory=list)
    required_permissions: list[str] = Field(default_factory=list)
    model: str | None = None
    max_thinking_steps: int = 10
    default_timeout_seconds: int = 300
    can_spawn_children: bool = False
    allowed_child_templates: list[str] = Field(default_factory=list)


class DashboardSkillInstallPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    clawhub_site: str | None = None


class DashboardConnectorApplyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None


class DashboardConfigPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_channel: str
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    worker_llm_default: LLMConfig = Field(default_factory=LLMConfig)
    litellm: LiteLLMRuntimeConfig = Field(default_factory=LiteLLMRuntimeConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    memory: MemoryConfig = Field(default_factory=MemoryConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    workers: WorkerRuntimeConfig = Field(default_factory=WorkerRuntimeConfig)
    whatsapp: WhatsAppConfig = Field(default_factory=WhatsAppConfig)
    search: SearchConfig = Field(default_factory=SearchConfig)
    log_level: str = "INFO"
    debug_prompts: bool = False
    heartbeat_interval_seconds: int = 900
    user_message_grace_seconds: float = 5.0


class DashboardProviderOption(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    label: str
    description: str
    default_model: str
    model_prefix: str | None = None
    default_api_base: str | None = None
    requires_api_key: bool = True
    supports_custom_base_url: bool = True
    supports_custom_model: bool = True
    supports_model_prefix_override: bool = False
    always_prefix_model: bool = False
    api_key_label: str = "API key"
    model_label: str = "Model"
    base_url_label: str = "Base URL"


def _dashboard_provider_options_payload() -> list[dict[str, Any]]:
    return [DashboardProviderOption(**asdict(entry)).model_dump(mode="json") for entry in list_provider_catalog()]


def register_dashboard_routes(app: FastAPI) -> None:
    @app.get("/dashboard", response_class=HTMLResponse)
    async def dashboard_page():
        settings = _get_settings(app)
        webapp_index = _resolve_webapp_index(settings)
        if settings.webapp_enabled and webapp_index is not None:
            return FileResponse(webapp_index)
        return HTMLResponse(_dashboard_unavailable_html(settings), status_code=503)

    @app.get("/dashboard/{asset_path:path}")
    async def dashboard_assets(asset_path: str):
        return _serve_dashboard_asset(app, asset_path, spa_fallback=True)

    @app.get("/assets/{asset_path:path}")
    async def dashboard_root_assets(asset_path: str):
        # Vite build outputs absolute /assets/* URLs by default.
        return _serve_dashboard_asset(app, f"assets/{asset_path}", spa_fallback=False)

    @app.get("/vite.svg")
    async def dashboard_vite_icon():
        return _serve_dashboard_asset(app, "vite.svg", spa_fallback=False)

    @app.get("/api/dashboard/snapshot")
    async def dashboard_snapshot(
        request: Request,
        last: int = Query(8, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)
        filters = _build_filters(settings, window_minutes=window_minutes, service=service, environment=environment)
        return _build_snapshot(settings, store, last, filters)

    @app.get("/api/dashboard/logs")
    async def dashboard_logs(
        request: Request,
        lines: int = Query(50, ge=1, le=500),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        filters = _build_filters(settings, window_minutes=window_minutes, service=service, environment=environment)
        log_path = settings.state_dir / "logs" / "octopal.log"
        entries: list[dict[str, str]] = []
        for entry in _collect_logs(log_path, max_lines=1000, filters=filters):
            entries.append(
                {
                    "event": str(entry.get("event", ""))[:200],
                    "level": str(entry.get("level", "info")),
                    "timestamp": str(entry.get("timestamp", "")),
                    "service": str(entry.get("service", "unknown")),
                    "environment": str(entry.get("environment", filters.environment)),
                }
            )
            if len(entries) >= lines:
                break
        return {"count": len(entries), "entries": entries, "filters": _filters_payload(filters, settings)}

    @app.get("/api/dashboard/settings")
    async def dashboard_settings(request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        launcher_status = get_worker_launcher_status(settings)
        return {
            "gateway_host": settings.gateway_host,
            "gateway_port": settings.gateway_port,
            "state_dir": str(settings.state_dir),
            "workspace_dir": str(settings.workspace_dir),
            "log_level": settings.log_level,
            "tailscale_ips_configured": bool(settings.tailscale_ips.strip()),
            "dashboard_token_configured": bool(settings.dashboard_token.strip()),
            "worker_launcher": {
                "configured": launcher_status.configured_launcher,
                "effective": launcher_status.effective_launcher,
                "available": launcher_status.available,
                "reason": launcher_status.reason,
                "docker_image": settings.worker_docker_image,
            },
        }

    @app.get("/api/dashboard/config")
    async def dashboard_config(request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        config = _sanitize_dashboard_config_payload(_dashboard_editable_config_payload(settings))
        launcher_status = get_worker_launcher_status(settings)
        return {
            "config": config.model_dump(mode="json"),
            "providers": _dashboard_provider_options_payload(),
            "worker_launcher": {
                "configured": launcher_status.configured_launcher,
                "effective": launcher_status.effective_launcher,
                "available": launcher_status.available,
                "reason": launcher_status.reason,
                "docker_image": settings.worker_docker_image,
            },
            "notes": [
                "Changes are written to config.json.",
                "Some runtime services may need restart to fully apply updated settings.",
            ],
        }

    @app.put("/api/dashboard/config")
    async def dashboard_update_config(
        request: Request,
        payload: DashboardConfigPayload,
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        config = _settings_to_octopal_config(settings)
        payload = _merge_dashboard_secret_fields(payload, config)
        config.user_channel = normalize_user_channel(payload.user_channel)
        config.telegram = payload.telegram
        config.llm = payload.llm
        config.worker_llm_default = payload.worker_llm_default
        config.litellm = payload.litellm
        config.storage = payload.storage
        config.memory = payload.memory
        config.gateway = payload.gateway
        config.workers = payload.workers
        config.whatsapp = payload.whatsapp
        config.search = payload.search
        config.log_level = payload.log_level
        config.debug_prompts = payload.debug_prompts
        config.heartbeat_interval_seconds = payload.heartbeat_interval_seconds
        config.user_message_grace_seconds = payload.user_message_grace_seconds

        save_config(config)
        settings.config_obj = config
        _sync_settings_from_config(settings, config)

        launcher_status = get_worker_launcher_status(settings)
        return {
            "status": "saved",
            "config": _sanitize_dashboard_config_payload(_dashboard_editable_config_payload(settings)).model_dump(mode="json"),
            "providers": _dashboard_provider_options_payload(),
            "worker_launcher": {
                "configured": launcher_status.configured_launcher,
                "effective": launcher_status.effective_launcher,
                "available": launcher_status.available,
                "reason": launcher_status.reason,
                "docker_image": settings.worker_docker_image,
            },
        }

    @app.get("/api/dashboard/worker-templates")
    async def dashboard_worker_templates(request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)
        templates = [
            _serialize_worker_template(template)
            for template in sorted(store.list_worker_templates(), key=lambda item: (item.name.lower(), item.id.lower()))
        ]
        return {"count": len(templates), "templates": templates}

    @app.post("/api/dashboard/worker-templates")
    async def dashboard_create_worker_template(
        request: Request,
        payload: WorkerTemplatePayload,
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)
        return _create_worker_template(settings, store, payload)

    @app.put("/api/dashboard/worker-templates/{template_id}")
    async def dashboard_update_worker_template(
        template_id: str,
        request: Request,
        payload: WorkerTemplatePayload,
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)
        return _update_worker_template(settings, store, template_id, payload)

    @app.delete("/api/dashboard/worker-templates/{template_id}")
    async def dashboard_delete_worker_template(
        template_id: str,
        request: Request,
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return _delete_worker_template(settings, template_id)

    @app.get("/api/dashboard/skills")
    async def dashboard_skills(request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return await asyncio.to_thread(_dashboard_skills_payload, settings)

    @app.post("/api/dashboard/skills/install")
    async def dashboard_install_skill(
        request: Request,
        payload: DashboardSkillInstallPayload,
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return await asyncio.to_thread(_dashboard_install_skill, settings, payload)

    @app.post("/api/dashboard/skills/{skill_id}/enable")
    async def dashboard_enable_skill(skill_id: str, request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return await asyncio.to_thread(_dashboard_set_skill_enabled, settings, skill_id, True)

    @app.post("/api/dashboard/skills/{skill_id}/disable")
    async def dashboard_disable_skill(skill_id: str, request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return await asyncio.to_thread(_dashboard_set_skill_enabled, settings, skill_id, False)

    @app.delete("/api/dashboard/skills/{skill_id}")
    async def dashboard_delete_skill(skill_id: str, request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return await asyncio.to_thread(_dashboard_delete_skill, settings, skill_id)

    @app.post("/api/dashboard/connectors/apply")
    async def dashboard_apply_connectors(
        request: Request,
        payload: DashboardConnectorApplyPayload,
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return await _dashboard_apply_connectors(app, settings, payload)

    @app.post("/api/dashboard/actions")
    async def dashboard_actions(
        request: Request,
        payload: dict[str, Any] = _EMPTY_ACTION_PAYLOAD,
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)

        action = str(payload.get("action", "")).strip().lower()
        confirm = bool(payload.get("confirm", False))
        reason = str(payload.get("reason", "")).strip()
        requested_by = str(payload.get("requested_by", "dashboard")).strip() or "dashboard"
        worker_id = str(payload.get("worker_id", "")).strip() or None

        if action not in {"restart_worker", "retry_failed", "clear_control_queue", "request_self_update"}:
            raise HTTPException(status_code=400, detail="Unsupported action")
        if action in {"restart_worker", "clear_control_queue", "request_self_update"} and not confirm:
            raise HTTPException(status_code=400, detail="Confirmation required")

        result = await _execute_dashboard_action(
            app=app,
            settings=settings,
            store=store,
            action=action,
            worker_id=worker_id,
            reason=reason,
            requested_by=requested_by,
        )
        return result

    @app.get("/api/dashboard/actions/history")
    async def dashboard_actions_history(
        request: Request,
        limit: int = Query(15, ge=1, le=100),
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)
        return {"count": min(limit, 100), "entries": _list_dashboard_action_history(store, limit=limit)}

    @app.get("/api/dashboard/v2/overview", response_model=DashboardOverviewV2)
    async def dashboard_v2_overview(
        request: Request,
        last: int = Query(8, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> DashboardOverviewV2:
        snapshot = _build_dashboard_v2_snapshot(
            app=app,
            request=request,
            last=last,
            window_minutes=window_minutes,
            service=service,
            environment=environment,
        )
        incidents = snapshot.get("incidents", {})
        summary = incidents.get("summary", {}) if isinstance(incidents, dict) else {}
        return DashboardOverviewV2(
            contract_version="dashboard.v2.overview",
            generated_at=str(snapshot.get("generated_at", "")),
            filters=dict(snapshot.get("filters", {})),
            health=dict(snapshot.get("health", {})),
            kpis=dict(snapshot.get("kpis", {})),
            services=list(snapshot.get("services", [])),
            system=dict(snapshot.get("system", {})),
            incidents_summary={
                "open": int(summary.get("open", 0) or 0),
                "critical": int(summary.get("critical", 0) or 0),
                "warning": int(summary.get("warning", 0) or 0),
            },
        )

    @app.get("/api/dashboard/v2/incidents", response_model=DashboardIncidentsV2)
    async def dashboard_v2_incidents(
        request: Request,
        last: int = Query(8, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> DashboardIncidentsV2:
        snapshot = _build_dashboard_v2_snapshot(
            app=app,
            request=request,
            last=last,
            window_minutes=window_minutes,
            service=service,
            environment=environment,
        )
        return DashboardIncidentsV2(
            contract_version="dashboard.v2.incidents",
            generated_at=str(snapshot.get("generated_at", "")),
            filters=dict(snapshot.get("filters", {})),
            incidents=dict(snapshot.get("incidents", {})),
        )

    @app.get("/api/dashboard/v2/octo", response_model=DashboardOctoV2)
    async def dashboard_v2_octo(
        request: Request,
        last: int = Query(8, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> DashboardOctoV2:
        snapshot = _build_dashboard_v2_snapshot(
            app=app,
            request=request,
            last=last,
            window_minutes=window_minutes,
            service=service,
            environment=environment,
        )
        return DashboardOctoV2(
            contract_version="dashboard.v2.octo",
            generated_at=str(snapshot.get("generated_at", "")),
            filters=dict(snapshot.get("filters", {})),
            octo=dict(snapshot.get("octo", {})),
            queues=dict(snapshot.get("queues", {})),
            control=dict(snapshot.get("control", {})),
            health=dict(snapshot.get("health", {})),
        )

    @app.get("/api/dashboard/v2/workers", response_model=DashboardWorkersV2)
    async def dashboard_v2_workers(
        request: Request,
        last: int = Query(16, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> DashboardWorkersV2:
        snapshot = _build_dashboard_v2_snapshot(
            app=app,
            request=request,
            last=last,
            window_minutes=window_minutes,
            service=service,
            environment=environment,
        )
        return DashboardWorkersV2(
            contract_version="dashboard.v2.workers",
            generated_at=str(snapshot.get("generated_at", "")),
            filters=dict(snapshot.get("filters", {})),
            workers=dict(snapshot.get("workers", {})),
        )

    @app.get("/api/dashboard/v2/system", response_model=DashboardSystemV2)
    async def dashboard_v2_system(
        request: Request,
        last: int = Query(8, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> DashboardSystemV2:
        snapshot = _build_dashboard_v2_snapshot(
            app=app,
            request=request,
            last=last,
            window_minutes=window_minutes,
            service=service,
            environment=environment,
        )
        return DashboardSystemV2(
            contract_version="dashboard.v2.system",
            generated_at=str(snapshot.get("generated_at", "")),
            filters=dict(snapshot.get("filters", {})),
            system=dict(snapshot.get("system", {})),
            services=list(snapshot.get("services", [])),
            connectivity=dict(snapshot.get("connectivity", {})),
            logs=list(snapshot.get("logs", [])),
        )

    @app.get("/api/dashboard/v2/actions", response_model=DashboardActionsV2)
    async def dashboard_v2_actions(
        request: Request,
        last: int = Query(8, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
    ) -> DashboardActionsV2:
        snapshot = _build_dashboard_v2_snapshot(
            app=app,
            request=request,
            last=last,
            window_minutes=window_minutes,
            service=service,
            environment=environment,
        )
        return DashboardActionsV2(
            contract_version="dashboard.v2.actions",
            generated_at=str(snapshot.get("generated_at", "")),
            filters=dict(snapshot.get("filters", {})),
            actions=dict(snapshot.get("actions", {})),
        )

    @app.get("/api/dashboard/v2/stream")
    async def dashboard_v2_stream(
        request: Request,
        topic: str = Query("overview"),
        last: int = Query(8, ge=1, le=50),
        window_minutes: int = Query(60, ge=1, le=1440),
        service: str = Query("all"),
        environment: str = Query("all"),
        interval_seconds: float = Query(2.0, ge=0.2, le=10.0),
    ) -> StreamingResponse:
        normalized_topic = topic.strip().lower()
        if normalized_topic not in _STREAM_TOPICS:
            normalized_topic = "overview"

        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)
        filters = _build_filters(settings, window_minutes=window_minutes, service=service, environment=environment)

        async def _event_generator():
            while True:
                if await request.is_disconnected():
                    break
                snapshot = _build_snapshot(settings, store, last, filters)
                payload = _dashboard_v2_projection(snapshot, topic=normalized_topic)
                body = json.dumps(payload, ensure_ascii=False)
                yield f"event: {normalized_topic}\n"
                yield f"data: {body}\n\n"
                await asyncio.sleep(interval_seconds)

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return StreamingResponse(_event_generator(), media_type="text/event-stream", headers=headers)


def _build_dashboard_v2_snapshot(
    *,
    app: FastAPI,
    request: Request,
    last: int,
    window_minutes: int,
    service: str,
    environment: str,
) -> dict[str, Any]:
    settings = _get_settings(app)
    _verify_dashboard_token(request, settings)
    store = _get_store(app, settings)
    filters = _build_filters(settings, window_minutes=window_minutes, service=service, environment=environment)
    return _build_snapshot(settings, store, last, filters)


def _dashboard_v2_projection(snapshot: dict[str, Any], *, topic: str) -> dict[str, Any]:
    generated_at = str(snapshot.get("generated_at", ""))
    filters = dict(snapshot.get("filters", {}))
    if topic == "snapshot":
        return snapshot
    if topic == "incidents":
        return {
            "contract_version": "dashboard.v2.incidents",
            "generated_at": generated_at,
            "filters": filters,
            "incidents": dict(snapshot.get("incidents", {})),
        }
    if topic == "octo":
        return {
            "contract_version": "dashboard.v2.octo",
            "generated_at": generated_at,
            "filters": filters,
            "octo": dict(snapshot.get("octo", {})),
            "queues": dict(snapshot.get("queues", {})),
            "control": dict(snapshot.get("control", {})),
            "health": dict(snapshot.get("health", {})),
        }
    if topic == "workers":
        return {
            "contract_version": "dashboard.v2.workers",
            "generated_at": generated_at,
            "filters": filters,
            "workers": dict(snapshot.get("workers", {})),
        }
    if topic == "system":
        return {
            "contract_version": "dashboard.v2.system",
            "generated_at": generated_at,
            "filters": filters,
            "system": dict(snapshot.get("system", {})),
            "services": list(snapshot.get("services", [])),
            "connectivity": dict(snapshot.get("connectivity", {})),
            "logs": list(snapshot.get("logs", [])),
        }
    if topic == "actions":
        return {
            "contract_version": "dashboard.v2.actions",
            "generated_at": generated_at,
            "filters": filters,
            "actions": dict(snapshot.get("actions", {})),
        }
    incidents = dict(snapshot.get("incidents", {}))
    summary = incidents.get("summary", {}) if isinstance(incidents, dict) else {}

    return {
        "contract_version": "dashboard.v2.overview",
        "generated_at": generated_at,
        "filters": filters,
        "health": dict(snapshot.get("health", {})),
        "kpis": dict(snapshot.get("kpis", {})),
        "services": list(snapshot.get("services", [])),
        "system": dict(snapshot.get("system", {})),
        "incidents_summary": {
            "open": int(summary.get("open", 0) or 0),
            "critical": int(summary.get("critical", 0) or 0),
            "warning": int(summary.get("warning", 0) or 0),
        },
    }


def _get_settings(app: FastAPI) -> Settings:
    settings = getattr(app.state, "settings", None)
    if not isinstance(settings, Settings):
        raise HTTPException(status_code=500, detail="Settings not initialized")
    return settings


def _serve_dashboard_asset(app: FastAPI, asset_path: str, *, spa_fallback: bool) -> FileResponse:
    settings = _get_settings(app)
    webapp_dist = _resolve_webapp_dist_dir(settings)
    if not settings.webapp_enabled or webapp_dist is None:
        raise HTTPException(status_code=404, detail="Dashboard asset not found")

    normalized = asset_path.strip().replace("\\", "/")
    candidate = (webapp_dist / normalized).resolve()
    resolved_dist = webapp_dist.resolve()
    if not str(candidate).startswith(str(resolved_dist)):
        raise HTTPException(status_code=404, detail="Dashboard asset not found")

    if candidate.is_file():
        return FileResponse(candidate)

    if spa_fallback:
        index_path = webapp_dist / "index.html"
        if index_path.is_file():
            return FileResponse(index_path)
    raise HTTPException(status_code=404, detail="Dashboard asset not found")


def _get_store(app: FastAPI, settings: Settings) -> SQLiteStore:
    store = getattr(app.state, "dashboard_store", None)
    if isinstance(store, SQLiteStore):
        return store
    store = SQLiteStore(settings)
    app.state.dashboard_store = store
    return store


def _dashboard_skills_payload(settings: Settings) -> dict[str, Any]:
    workspace_dir = settings.workspace_dir.resolve()
    payload = list_skill_inventory(workspace_dir)
    skills = [
        _serialize_dashboard_skill(item)
        for item in sorted(
            payload.get("skills", []),
            key=lambda item: (
                str(item.get("name", "") or item.get("id", "")).lower(),
                str(item.get("id", "")).lower(),
            ),
        )
        if isinstance(item, dict)
    ]
    return {
        "contract_version": "dashboard.skills.v1",
        "count": len(skills),
        "registry_path": str(payload.get("registry_path", "")),
        "skills": skills,
        "install": {
            "supported_sources": [
                "clawhub_slug",
                "skill_md_url",
                "zip_url",
                "local_dir",
                "local_skill_md",
                "local_zip",
            ],
            "default_clawhub_site": "https://clawhub.ai",
        },
    }


def _dashboard_install_skill(settings: Settings, payload: DashboardSkillInstallPayload) -> dict[str, Any]:
    source = str(payload.source or "").strip()
    if not source:
        raise HTTPException(status_code=400, detail="Skill source is required")

    try:
        kwargs: dict[str, Any] = {"workspace_dir": settings.workspace_dir.resolve()}
        clawhub_site = str(payload.clawhub_site or "").strip()
        if clawhub_site:
            kwargs["clawhub_site"] = clawhub_site
        install_payload = install_skill_from_source(source, **kwargs)
    except Exception as exc:
        raise _skill_dashboard_http_error(exc) from exc

    skill_id = str(install_payload.get("skill_id", "")).strip()
    return {
        "status": str(install_payload.get("status", "installed")),
        "skill_id": skill_id,
        "skill": _require_dashboard_skill(settings, skill_id),
        "install": install_payload,
    }


def _dashboard_set_skill_enabled(settings: Settings, skill_id: str, enabled: bool) -> dict[str, Any]:
    normalized_id = str(skill_id or "").strip()
    try:
        action_payload = set_skill_enabled(
            normalized_id,
            workspace_dir=settings.workspace_dir.resolve(),
            enabled=enabled,
        )
    except Exception as exc:
        raise _skill_dashboard_http_error(exc) from exc

    return {
        "status": str(action_payload.get("status", "enabled" if enabled else "disabled")),
        "skill_id": normalized_id,
        "skill": _require_dashboard_skill(settings, normalized_id),
        "action": action_payload,
    }


def _dashboard_delete_skill(settings: Settings, skill_id: str) -> dict[str, Any]:
    normalized_id = str(skill_id or "").strip()
    try:
        payload = remove_skill(normalized_id, workspace_dir=settings.workspace_dir.resolve())
    except Exception as exc:
        raise _skill_dashboard_http_error(exc) from exc
    return {
        "status": str(payload.get("status", "removed")),
        "skill_id": normalized_id,
        "removed": payload,
        "skills": _dashboard_skills_payload(settings),
    }


def _find_dashboard_skill(settings: Settings, skill_id: str) -> dict[str, Any] | None:
    normalized_id = str(skill_id or "").strip()
    for item in _dashboard_skills_payload(settings).get("skills", []):
        if isinstance(item, dict) and str(item.get("id", "")) == normalized_id:
            return item
    return None


def _require_dashboard_skill(settings: Settings, skill_id: str) -> dict[str, Any]:
    skill = _find_dashboard_skill(settings, skill_id)
    if skill is None:
        raise HTTPException(
            status_code=500,
            detail="Skill operation succeeded but the updated skill could not be reloaded",
        )
    return skill


async def _dashboard_apply_connectors(
    app: FastAPI,
    settings: Settings,
    payload: DashboardConnectorApplyPayload,
) -> dict[str, Any]:
    octo = getattr(app.state, "octo", None)
    manager = getattr(octo, "connector_manager", None)
    if manager is None:
        raise HTTPException(
            status_code=409,
            detail="Connector runtime is not available in this gateway process",
        )

    config = load_config()
    settings.config_obj = config
    _sync_settings_from_config(settings, config)
    manager.config = config.connectors
    manager.octo_config = config

    name = (payload.name or "").strip().lower()
    if name:
        connector = manager.get_connector(name)
        if connector is None:
            raise HTTPException(status_code=404, detail=f"Unknown connector: {name}")
        await manager.reconcile_connector_runtime(name)
        statuses = {name: await connector.get_status()}
    else:
        await manager.load_and_start_all()
        statuses = await manager.get_all_statuses()

    return {
        "status": "applied",
        "connectors": statuses,
    }


def _serialize_dashboard_skill(item: dict[str, Any]) -> dict[str, Any]:
    skill_id = str(item.get("id", "")).strip()
    name = str(item.get("name", "")).strip() or skill_id
    description = str(item.get("description", "")).strip()
    enabled = bool(item.get("enabled", True))
    ready = bool(item.get("ready", False))
    installer_managed = bool(item.get("installer_managed", False))
    auto_discovered = bool(item.get("auto_discovered", False))
    origin = "installed" if installer_managed else "auto_discovered" if auto_discovered else "local"
    installed_source = str(item.get("installed_source", "")).strip()
    installed_source_kind = str(item.get("installed_source_kind", "")).strip()
    path = str(item.get("path", "")).strip()

    return {
        "id": skill_id,
        "name": name,
        "description": description,
        "scope": str(item.get("scope", "both")),
        "enabled": enabled,
        "ready": ready,
        "status": str(item.get("status", "")),
        "reasons": [str(reason) for reason in item.get("reasons", []) if str(reason).strip()],
        "origin": origin,
        "source": {
            "kind": installed_source_kind or str(item.get("source", "registry")),
            "label": installed_source or path or origin,
            "path": path,
            "installer_managed": installer_managed,
            "auto_discovered": auto_discovered,
        },
        "trust": {
            "trusted": bool(item.get("trusted", True)),
            "has_scripts": bool(item.get("has_scripts", False)),
            "scan_status": str(item.get("scan_status", "")),
            "scan_findings_count": int(item.get("scan_findings_count", 0)),
        },
        "runtime": {
            "kind": str(item.get("runtime_kind", "")),
            "required": bool(item.get("runtime_required", False)),
            "recommended": bool(item.get("runtime_recommended", False)),
            "prepared": bool(item.get("runtime_prepared", False)),
            "next_step": str(item.get("runtime_next_step", "")),
        },
        "requirements": {
            "missing_bins": [str(value) for value in item.get("missing_bins", [])],
            "missing_env": [str(value) for value in item.get("missing_env", [])],
            "missing_config": [str(value) for value in item.get("missing_config", [])],
        },
        "actions": {
            "can_enable": not enabled,
            "can_disable": enabled,
            "can_remove": bool(skill_id),
            "can_install": True,
        },
    }


def _skill_dashboard_http_error(exc: Exception) -> HTTPException:
    detail = str(exc).strip() or exc.__class__.__name__
    lowered = detail.lower()
    if "not found" in lowered or ("missing" in lowered and "installed bundle" in lowered):
        return HTTPException(status_code=404, detail=detail)
    if "already exists" in lowered or "different source" in lowered or "refusing to overwrite" in lowered:
        return HTTPException(status_code=409, detail=detail)
    return HTTPException(status_code=400, detail=detail)


def _serialize_worker_template(template: WorkerTemplateRecord) -> dict[str, Any]:
    return {
        "id": template.id,
        "name": template.name,
        "description": template.description,
        "system_prompt": template.system_prompt,
        "available_tools": list(template.available_tools),
        "required_permissions": list(template.required_permissions),
        "model": template.model,
        "max_thinking_steps": int(template.max_thinking_steps),
        "default_timeout_seconds": int(template.default_timeout_seconds),
        "can_spawn_children": bool(template.can_spawn_children),
        "allowed_child_templates": list(template.allowed_child_templates),
        "created_at": template.created_at.isoformat(),
        "updated_at": template.updated_at.isoformat(),
    }


def _create_worker_template(
    settings: Settings,
    store: SQLiteStore,
    payload: WorkerTemplatePayload,
) -> dict[str, Any]:
    data = _normalize_worker_template_payload(payload, expected_id=None)
    worker_id = str(data["id"])
    if store.get_worker_template(worker_id) is not None:
        raise HTTPException(status_code=409, detail=f"Worker template '{worker_id}' already exists")

    worker_file = _resolve_worker_template_file(settings.workspace_dir, worker_id)
    worker_file.parent.mkdir(parents=True, exist_ok=True)
    worker_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    created = store.get_worker_template(worker_id)
    if created is None:
        raise HTTPException(status_code=500, detail="Worker template was written but could not be reloaded")
    return {"status": "created", "template": _serialize_worker_template(created)}


def _update_worker_template(
    settings: Settings,
    store: SQLiteStore,
    template_id: str,
    payload: WorkerTemplatePayload,
) -> dict[str, Any]:
    existing = store.get_worker_template(template_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Worker template '{template_id}' not found")

    data = _normalize_worker_template_payload(payload, expected_id=template_id)
    worker_file = _resolve_worker_template_file(settings.workspace_dir, template_id)
    if not worker_file.exists():
        raise HTTPException(status_code=404, detail=f"Worker template file for '{template_id}' not found")
    worker_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    updated = store.get_worker_template(template_id)
    if updated is None:
        raise HTTPException(status_code=500, detail="Worker template was updated but could not be reloaded")
    return {"status": "updated", "template": _serialize_worker_template(updated)}


def _delete_worker_template(settings: Settings, template_id: str) -> dict[str, Any]:
    _validate_worker_template_id(template_id)
    worker_dir = _resolve_worker_template_file(settings.workspace_dir, template_id).parent
    if not worker_dir.exists():
        raise HTTPException(status_code=404, detail=f"Worker template '{template_id}' not found")
    shutil.rmtree(worker_dir)
    return {"status": "deleted", "template_id": template_id}


def _normalize_worker_template_payload(
    payload: WorkerTemplatePayload,
    *,
    expected_id: str | None,
) -> dict[str, Any]:
    worker_id = payload.id.strip()
    _validate_worker_template_id(worker_id)
    if expected_id is not None and worker_id != expected_id:
        raise HTTPException(status_code=400, detail="Template id in path and payload must match")

    name = payload.name.strip()
    description = payload.description.strip()
    system_prompt = payload.system_prompt.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Worker template name is required")
    if not description:
        raise HTTPException(status_code=400, detail="Worker template description is required")
    if not system_prompt:
        raise HTTPException(status_code=400, detail="Worker template system_prompt is required")

    max_steps = int(payload.max_thinking_steps)
    timeout_seconds = int(payload.default_timeout_seconds)
    if max_steps <= 0:
        raise HTTPException(status_code=400, detail="max_thinking_steps must be greater than 0")
    if timeout_seconds <= 0:
        raise HTTPException(status_code=400, detail="default_timeout_seconds must be greater than 0")

    return {
        "id": worker_id,
        "name": name,
        "description": description,
        "system_prompt": system_prompt,
        "available_tools": _normalize_string_list(payload.available_tools),
        "required_permissions": _normalize_string_list(payload.required_permissions),
        "model": (payload.model or "").strip() or None,
        "max_thinking_steps": max_steps,
        "default_timeout_seconds": timeout_seconds,
        "can_spawn_children": bool(payload.can_spawn_children),
        "allowed_child_templates": _normalize_string_list(payload.allowed_child_templates),
    }


def _normalize_string_list(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        out.append(text)
        seen.add(text)
    return out


def _validate_worker_template_id(template_id: str) -> None:
    text = template_id.strip()
    if not text or not all(ch.islower() or ch.isdigit() or ch in {"_", "-"} for ch in text):
        raise HTTPException(status_code=400, detail="Worker template id must use lowercase letters, numbers, '_' or '-'")
    if not text[0].isalnum():
        raise HTTPException(status_code=400, detail="Worker template id must start with a letter or digit")


def _resolve_worker_template_file(workspace_dir: Path, template_id: str) -> Path:
    workers_root = (workspace_dir / "workers").resolve()
    candidate = (workers_root / template_id / "worker.json").resolve()
    try:
        candidate.relative_to(workers_root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid worker template path") from exc
    return candidate


def _resolve_webapp_dist_dir(settings: Settings) -> Path | None:
    project_root = Path(__file__).resolve().parents[3]
    explicit = settings.webapp_dist_dir
    if explicit is not None:
        candidate = Path(explicit)
        if not candidate.is_absolute():
            candidate = project_root / candidate
    else:
        candidate = project_root / "webapp" / "dist"
    if candidate.is_dir():
        return candidate
    return None


def _resolve_webapp_index(settings: Settings) -> Path | None:
    dist_dir = _resolve_webapp_dist_dir(settings)
    if dist_dir is None:
        return None
    index_path = dist_dir / "index.html"
    if index_path.is_file():
        return index_path
    return None


def _build_filters(settings: Settings, *, window_minutes: int, service: str, environment: str) -> DashboardFilters:
    normalized_window = window_minutes if window_minutes in _WINDOW_CHOICES else 60
    normalized_service = service.strip().lower()
    if normalized_service not in _SERVICE_CHOICES:
        normalized_service = "all"
    env = environment.strip().lower()
    current_env = _resolve_environment(settings)
    if not env:
        env = "all"
    if env == "current":
        env = current_env
    return DashboardFilters(window_minutes=normalized_window, service=normalized_service, environment=env)


def _resolve_environment(settings: Settings) -> str:
    candidate = (
        os.getenv("OCTOPAL_ENV")
        or os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or "local"
    )
    raw = str(candidate or "local").strip().lower()
    return "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_"}) or "local"


def _filters_payload(filters: DashboardFilters, settings: Settings) -> dict[str, Any]:
    current_env = _resolve_environment(settings)
    environments = ["all", current_env, "dev", "staging", "prod"]
    deduped: list[str] = []
    for item in environments:
        if item not in deduped:
            deduped.append(item)
    return {
        "window_minutes": filters.window_minutes,
        "service": filters.service,
        "environment": filters.environment,
        "options": {
            "window_minutes": sorted(_WINDOW_CHOICES),
            "service": sorted(_SERVICE_CHOICES),
            "environment": deduped,
        },
        "current_environment": current_env,
    }


def _verify_dashboard_token(request: Request, settings: Settings) -> None:
    expected = settings.dashboard_token.strip()
    if not expected:
        return

    header_token = request.headers.get("x-octopal-token", "").strip()
    auth_header = request.headers.get("authorization", "").strip()
    bearer_token = ""
    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()
    query_token = str(request.query_params.get("token", "")).strip()

    provided = header_token or bearer_token or query_token
    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid dashboard token")


async def _execute_dashboard_action(
    *,
    app: FastAPI,
    settings: Settings,
    store: SQLiteStore,
    action: str,
    worker_id: str | None,
    reason: str,
    requested_by: str,
) -> dict[str, Any]:
    now = _now_utc()
    result: dict[str, Any] = {"status": "error", "action": action, "at": now.isoformat()}

    if action == "clear_control_queue":
        cleared = _clear_control_queue_requests(settings.state_dir, actor=requested_by)
        result = {
            "status": "ok",
            "action": action,
            "at": now.isoformat(),
            "cleared_requests": cleared,
            "message": f"Cleared {cleared} pending control request(s).",
        }
    elif action == "retry_failed":
        if not worker_id:
            result = {
                "status": "error",
                "action": action,
                "at": now.isoformat(),
                "message": "worker_id is required for retry_failed.",
            }
        else:
            recent_workers = store.list_recent_workers(limit=250)
            target = _select_retry_target(recent_workers, requested_worker_id=worker_id)
            if target is None:
                result = {
                    "status": "error",
                    "action": action,
                    "at": now.isoformat(),
                    "message": f"Failed worker '{worker_id}' not found.",
                }
            else:
                launch = await _launch_worker_from_record(app, target, reason=reason, requested_by=requested_by)
                if launch.get("status") == "ok":
                    result = {
                        "status": "ok",
                        "action": action,
                        "at": now.isoformat(),
                        "worker_id": target.id,
                        "new_worker_id": launch.get("new_worker_id"),
                        "message": f"Retried failed worker {target.id}.",
                    }
                else:
                    result = {
                        "status": "error",
                        "action": action,
                        "at": now.isoformat(),
                        "worker_id": target.id,
                        "message": str(launch.get("message", "Retry failed")),
                    }
    elif action == "restart_worker":
        if not worker_id:
            result = {
                "status": "error",
                "action": action,
                "at": now.isoformat(),
                "message": "worker_id is required for restart_worker.",
            }
        else:
            worker = store.get_worker(worker_id)
            if worker is None:
                result = {
                    "status": "error",
                    "action": action,
                    "at": now.isoformat(),
                    "message": f"Worker '{worker_id}' not found.",
                }
            else:
                stop_info = await _stop_worker_if_running(app, worker.id)
                launch = await _launch_worker_from_record(app, worker, reason=reason, requested_by=requested_by)
                if launch.get("status") == "ok":
                    result = {
                        "status": "ok",
                        "action": action,
                        "at": now.isoformat(),
                        "worker_id": worker.id,
                        "new_worker_id": launch.get("new_worker_id"),
                        "stopped": stop_info.get("stopped", False),
                        "message": f"Restarted worker {worker.id}.",
                    }
                else:
                    result = {
                        "status": "error",
                        "action": action,
                        "at": now.isoformat(),
                        "worker_id": worker.id,
                        "stopped": stop_info.get("stopped", False),
                        "message": str(launch.get("message", "Restart failed")),
                    }
    elif action == "request_self_update":
        update_status = _dashboard_update_status()
        if not bool(update_status.get("update_available")):
            result = {
                "status": "error",
                "action": action,
                "at": now.isoformat(),
                "update": update_status,
                "message": "No newer release is available.",
            }
        elif not bool(update_status.get("can_update")):
            result = {
                "status": "error",
                "action": action,
                "at": now.isoformat(),
                "update": update_status,
                "message": "Update is blocked by the current checkout state.",
            }
        else:
            request = append_control_request(
                settings.state_dir,
                action=SELF_UPDATE_ACTION,
                reason=reason or "Apply latest Octopal release from dashboard.",
                requested_by=SELF_UPDATE_REQUESTED_BY,
                delay_seconds=3,
                metadata={
                    "source": "dashboard",
                    "requested_by": requested_by,
                    "update": update_status,
                },
            )
            result = {
                "status": "ok",
                "action": action,
                "at": now.isoformat(),
                "request": request,
                "update": update_status,
                "message": "Self-update requested. Octopal will update and restart shortly.",
            }

    _append_dashboard_audit(
        store=store,
        action=action,
        result=result,
        requested_by=requested_by,
        worker_id=worker_id,
        reason=reason,
    )
    return result


def _dashboard_update_status() -> dict[str, Any]:
    try:
        return check_update_status()
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def _append_dashboard_audit(
    *,
    store: SQLiteStore,
    action: str,
    result: dict[str, Any],
    requested_by: str,
    worker_id: str | None,
    reason: str,
) -> None:
    level = "info" if str(result.get("status")) == "ok" else "warning"
    store.append_audit(
        AuditEvent(
            id=f"dashboard-action-{uuid4().hex}",
            ts=_now_utc(),
            correlation_id=str(worker_id or ""),
            level=level,
            event_type="dashboard_action",
            data={
                "action": action,
                "requested_by": requested_by,
                "worker_id": worker_id,
                "reason": reason,
                "result": result,
            },
        )
    )


async def _stop_worker_if_running(app: FastAPI, worker_id: str) -> dict[str, Any]:
    runtime = getattr(app.state, "runtime", None)
    if runtime is None or not hasattr(runtime, "stop_worker"):
        return {"stopped": False, "message": "Runtime unavailable"}
    try:
        stopped = bool(await runtime.stop_worker(worker_id))
    except Exception as exc:
        return {"stopped": False, "message": str(exc)}
    return {"stopped": stopped}


async def _launch_worker_from_record(
    app: FastAPI,
    worker: WorkerRecord,
    *,
    reason: str,
    requested_by: str,
) -> dict[str, Any]:
    octo = getattr(app.state, "octo", None)
    if octo is None or not hasattr(octo, "_start_worker_async"):
        return {"status": "error", "message": "Octo runtime is unavailable for worker launch."}
    template_id = str(worker.template_id or "").strip()
    if not template_id:
        return {"status": "error", "message": "Worker template_id missing; cannot restart/retry."}
    task = str(worker.task or "").strip()
    if not task:
        return {"status": "error", "message": "Worker task is empty; cannot restart/retry."}
    note = f"[dashboard:{requested_by}]"
    if reason:
        note += f" {reason}"
    launch = await octo._start_worker_async(
        worker_id=template_id,
        task=f"{task}\n\n{note}",
        chat_id=0,
        inputs={},
        tools=None,
        model=None,
        timeout_seconds=None,
    )
    new_worker_id = str(launch.get("worker_id", "")).strip() or None
    launch_status = str(launch.get("status", "")).strip().lower()
    if launch_status in {"started", "queued", "worker_started"} and new_worker_id:
        return {"status": "ok", "new_worker_id": new_worker_id, "launch": launch}
    if launch_status == "skipped_duplicate":
        return {"status": "error", "message": "Launch skipped as duplicate request."}
    return {"status": "error", "message": f"Launch failed with status={launch_status or 'unknown'}"}


def _select_retry_target(workers: list[WorkerRecord], requested_worker_id: str | None) -> WorkerRecord | None:
    if not requested_worker_id:
        return None
    for worker in workers:
        if worker.id == requested_worker_id and str(worker.status).lower() == "failed":
            return worker
    return None


def _clear_control_queue_requests(state_dir: Path, *, actor: str) -> int:
    reqs = _read_jsonl(state_dir / "control_requests.jsonl")
    acks = _read_jsonl(state_dir / "control_acks.jsonl")
    acked_ids = {str(a.get("request_id", "")).strip() for a in acks}
    pending = [r for r in reqs if str(r.get("request_id", "")).strip() not in acked_ids]
    if not pending:
        return 0
    ack_file = state_dir / "control_acks.jsonl"
    count = 0
    for req in pending:
        request_id = str(req.get("request_id", "")).strip()
        if not request_id:
            continue
        item = {
            "request_id": request_id,
            "acked_at": _now_utc().isoformat(),
            "status": "cleared",
            "source": "dashboard_action",
            "actor": actor,
        }
        _append_jsonl(ack_file, item)
        count += 1
    return count


def _list_dashboard_action_history(store: SQLiteStore, limit: int = 15) -> list[dict[str, Any]]:
    events = store.list_audit(limit=max(limit * 5, 40))
    out: list[dict[str, Any]] = []
    for event in events:
        if event.event_type != "dashboard_action":
            continue
        data = event.data if isinstance(event.data, dict) else {}
        out.append(
            {
                "id": event.id,
                "timestamp": event.ts.isoformat(),
                "level": event.level,
                "action": data.get("action", ""),
                "requested_by": data.get("requested_by", ""),
                "worker_id": data.get("worker_id"),
                "reason": data.get("reason", ""),
                "result": data.get("result", {}),
            }
        )
        if len(out) >= limit:
            break
    return out


def _build_snapshot(settings: Settings, store: SQLiteStore, last: int, filters: DashboardFilters) -> dict[str, Any]:
    status_data = read_status(settings) or {}
    pid = status_data.get("pid")
    running = is_pid_running(pid)
    metrics = read_metrics_snapshot(settings.state_dir) or {}
    octo_metrics = metrics.get("octo", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    whatsapp_metrics = metrics.get("whatsapp", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    connectivity_metrics = metrics.get("connectivity", {}) if isinstance(metrics, dict) else {}
    scheduler_metrics = metrics.get("scheduler", {}) if isinstance(metrics, dict) else {}
    launcher_status = get_worker_launcher_status(settings)
    active_channel = _resolve_active_channel(status_data, settings)
    active_channel_label = user_channel_label(active_channel)
    channel_metrics = _select_active_channel_metrics(
        active_channel=active_channel,
        telegram_metrics=telegram_metrics,
        whatsapp_metrics=whatsapp_metrics,
    )

    active_workers = store.get_active_workers(older_than_minutes=5)
    recent_workers = store.list_recent_workers(max(50, last))

    now = _now_utc()
    cutoff = now.timestamp() - 24 * 60 * 60
    spawned_24h = int(store.count_workers_created_since(datetime.fromtimestamp(cutoff, tz=UTC)))

    by_status: dict[str, int] = {}
    for worker in active_workers:
        by_status[worker.status] = by_status.get(worker.status, 0) + 1
    running_nodes = [
        w
        for w in active_workers
        if w.status in {"started", "running", "waiting_for_children", "awaiting_instruction"}
    ]
    root_running = sum(1 for w in running_nodes if not w.parent_worker_id)
    subworkers_running = sum(1 for w in running_nodes if bool(w.parent_worker_id))

    octo_status = build_octo_status(octo_metrics)
    followup_q = int(octo_status["followup_queues"])
    internal_q = int(octo_status["internal_queues"])
    octo_state = str(octo_status["state"])

    requests = _read_jsonl(settings.state_dir / "control_requests.jsonl")
    acks = _read_jsonl(settings.state_dir / "control_acks.jsonl")
    acked_ids = {str(a.get("request_id", "")) for a in acks}
    pending_requests = [r for r in requests if str(r.get("request_id", "")) not in acked_ids]
    last_ack = acks[-1] if acks else None

    log_path = settings.state_dir / "logs" / "octopal.log"
    incident_logs = _collect_logs(log_path, max_lines=600, filters=filters)
    recent_logs = _tail_logs(log_path, 12, filters=filters)
    log_health = _compute_log_health(log_path, now, window_minutes=filters.window_minutes, filters=filters)
    latency_p95_ms = _estimate_control_latency_p95_ms(requests, acks)
    queue_depth = followup_q + internal_q + int(channel_metrics.get("queue_depth", 0) or 0) + len(pending_requests)
    active_workers_kpi = (
        by_status.get("running", 0)
        + by_status.get("started", 0)
        + by_status.get("waiting_for_children", 0)
        + by_status.get("awaiting_instruction", 0)
    )
    mcp_servers = connectivity_metrics.get("mcp_servers", {})
    update_status = _dashboard_update_status()

    services_all = _build_service_health(
        active_channel=active_channel,
        now=now,
        system_running=running,
        system_last_heartbeat=status_data.get("last_internal_heartbeat_at"),
        system_status_updated_at=status_data.get("status_updated_at"),
        launcher_status=launcher_status,
        octo_metrics=octo_metrics,
        telegram_metrics=telegram_metrics,
        whatsapp_metrics=whatsapp_metrics,
        exec_metrics=exec_metrics,
        scheduler_metrics=scheduler_metrics,
        mcp_servers=mcp_servers if isinstance(mcp_servers, dict) else {},
    )
    services = [s for s in services_all if _service_matches_filter(str(s.get("id", "all")), filters.service)]
    if not services:
        services = services_all
    overall_status, overall_reasons = _derive_overall_health(
        services=services_all,
        failed_workers=by_status.get("failed", 0),
        control_pending=len(pending_requests),
        log_health=log_health,
        system_running=running,
    )
    kpis = _build_kpis(
        latency_p95_ms=latency_p95_ms,
        log_health=log_health,
        queue_depth=queue_depth,
        active_workers=active_workers_kpi,
    )
    incidents = _build_incidents(
        services=services_all,
        recent_workers=recent_workers,
        logs=incident_logs,
        control_pending=len(pending_requests),
        queue_depth=queue_depth,
    )
    slo = _build_slo_metrics(
        active_channel=active_channel,
        services=services_all,
        log_health=log_health,
        recent_workers=recent_workers,
    )
    noise_control = _build_noise_control(logs=incident_logs)
    template_cache: dict[str, WorkerTemplateRecord | None] = {}

    return {
        "contract_version": "dashboard.v1",
        "generated_at": now.isoformat(),
        "filters": _filters_payload(filters, settings),
        "health": {
            "status": overall_status,
            "summary": _health_summary(overall_status, overall_reasons),
            "reasons": overall_reasons,
        },
        "kpis": kpis,
        "services": services,
        "incidents": incidents,
        "slo": slo,
        "noise_control": noise_control,
        "system": {
            "running": running,
            "pid": pid,
            "active_channel": active_channel_label,
            "active_channel_id": active_channel,
            "started_at": status_data.get("started_at"),
            "last_heartbeat": status_data.get("last_internal_heartbeat_at"),
            "last_user_message_at": status_data.get("last_user_message_at")
            or status_data.get("last_message_at"),
            "last_scheduler_tick_at": status_data.get("last_scheduler_tick_at"),
            "last_scheduler_tick_status": status_data.get("last_scheduler_tick_status"),
            "status_updated_at": status_data.get("status_updated_at"),
            "uptime": _uptime_human(status_data.get("started_at")),
            "worker_launcher": {
                "configured": launcher_status.configured_launcher,
                "effective": launcher_status.effective_launcher,
                "available": launcher_status.available,
                "reason": launcher_status.reason,
            },
            "scheduler": dict(scheduler_metrics) if isinstance(scheduler_metrics, dict) else {},
            "update": update_status,
        },
        "octo": {
            "state": octo_state,
            "followup_queues": followup_q,
            "internal_queues": internal_q,
            "followup_tasks": int(octo_status["followup_tasks"]),
            "internal_tasks": int(octo_status["internal_tasks"]),
        },
        "connectivity": {"mcp_servers": mcp_servers if isinstance(mcp_servers, dict) else {}},
        "logs": recent_logs,
        "queues": {
            "active_channel": active_channel,
            "active_channel_label": active_channel_label,
            "active_channel_updated_at": channel_metrics.get("updated_at"),
            "channel_queue_depth": int(channel_metrics.get("queue_depth", 0) or 0),
            "channel_send_tasks": channel_metrics.get("send_tasks"),
            "channel_connected": channel_metrics.get("connected"),
            "channel_chat_mappings": channel_metrics.get("chat_mappings"),
            "telegram_send_tasks": int(telegram_metrics.get("send_tasks", 0) or 0),
            "telegram_queues": int(telegram_metrics.get("chat_queues", 0) or 0),
            "whatsapp_connected": int(whatsapp_metrics.get("connected", 0) or 0),
            "whatsapp_mapped_chats": int(whatsapp_metrics.get("chat_mappings", 0) or 0),
            "exec_sessions_running": int(exec_metrics.get("background_sessions_running", 0) or 0),
            "exec_sessions_total": int(exec_metrics.get("background_sessions_total", 0) or 0),
        },
        "workers": {
            "spawned_24h": spawned_24h,
            "running": active_workers_kpi,
            "root_running": root_running,
            "subworkers_running": subworkers_running,
            "completed": by_status.get("completed", 0),
            "failed": by_status.get("failed", 0),
            "stopped": by_status.get("stopped", 0),
            "topology": [
                {
                    "id": w.id,
                    "template_name": w.template_name or w.template_id or "",
                    "status": w.status,
                    "task": w.task,
                    "updated_at": w.updated_at.isoformat(),
                    "parent_worker_id": w.parent_worker_id,
                    "lineage_id": w.lineage_id,
                    "spawn_depth": w.spawn_depth,
                    "plan_binding": _worker_plan_binding_payload(w.id, store),
                }
                for w in running_nodes
            ],
            "recent": [
                _serialize_recent_worker(w, store=store, template_cache=template_cache)
                for w in recent_workers[:last]
            ],
        },
        "control": {
            "pending_requests": len(pending_requests),
            "last_ack": last_ack,
        },
        "actions": {
            "history": _list_dashboard_action_history(store, limit=8),
        },
    }


def _build_kpis(
    *,
    latency_p95_ms: int | None,
    log_health: dict[str, Any],
    queue_depth: int,
    active_workers: int,
) -> dict[str, Any]:
    error_rate = float(log_health.get("error_rate_5m", 0.0) or 0.0)
    latency_status = "unknown"
    if latency_p95_ms is not None:
        if latency_p95_ms >= 5000:
            latency_status = "critical"
        elif latency_p95_ms >= 2000:
            latency_status = "warning"
        else:
            latency_status = "ok"

    error_status = "ok"
    if error_rate >= 0.5:
        error_status = "critical"
    elif error_rate >= 0.2:
        error_status = "warning"

    queue_status = "ok"
    if queue_depth >= 30:
        queue_status = "critical"
    elif queue_depth >= 10:
        queue_status = "warning"

    worker_status = "ok" if active_workers > 0 else "warning"
    return {
        "latency_ms_p95": {"value": latency_p95_ms, "unit": "ms", "status": latency_status},
        "error_rate_5m": {"value": round(error_rate * 100, 1), "unit": "%", "status": error_status},
        "queue_depth": {"value": int(queue_depth), "unit": "count", "status": queue_status},
        "active_workers": {"value": int(active_workers), "unit": "count", "status": worker_status},
        "error_count_5m": int(log_health.get("error_count_5m", 0) or 0),
        "event_count_5m": int(log_health.get("event_count_5m", 0) or 0),
    }


def _build_service_health(
    *,
    active_channel: str,
    now: datetime,
    system_running: bool,
    system_last_heartbeat: str | None,
    system_status_updated_at: str | None,
    launcher_status: WorkerLauncherStatus,
    octo_metrics: dict[str, Any],
    telegram_metrics: dict[str, Any],
    whatsapp_metrics: dict[str, Any],
    exec_metrics: dict[str, Any],
    scheduler_metrics: dict[str, Any],
    mcp_servers: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    heartbeat_age = _age_seconds(system_last_heartbeat, now)
    gateway_status = "ok" if system_running else "critical"
    gateway_reason = "running" if system_running else "process is not running"
    if system_running and heartbeat_age is not None and heartbeat_age > 300:
        gateway_reason = f"running (idle heartbeat {int(heartbeat_age)}s)"
    out.append(
        {
            "id": "gateway",
            "name": "Gateway",
            "status": gateway_status,
            "reason": gateway_reason,
            "updated_at": system_status_updated_at or system_last_heartbeat,
        }
    )

    launcher_service_status = "ok" if launcher_status.effective_launcher == "docker" else "warning"
    if launcher_status.configured_launcher != "docker":
        launcher_service_status = "ok"
    out.append(
        {
            "id": "worker-launcher",
            "name": "Worker Launcher",
            "status": launcher_service_status,
            "reason": launcher_status.reason,
            "updated_at": system_status_updated_at or system_last_heartbeat,
            "metrics": {
                "configured": launcher_status.configured_launcher,
                "effective": launcher_status.effective_launcher,
                "available": launcher_status.available,
            },
        }
    )

    octo_runtime_status = build_octo_status(octo_metrics)
    followup_q = int(octo_runtime_status["followup_queues"])
    internal_q = int(octo_runtime_status["internal_queues"])
    thinking_count = int(octo_runtime_status["thinking_count"])
    out.append(
        {
            "id": "octo",
            "name": "Octo",
            "status": str(octo_runtime_status["service_status"]),
            "reason": str(octo_runtime_status["reason"]),
            "updated_at": octo_runtime_status.get("updated_at"),
            "metrics": {
                "followup_queues": followup_q,
                "internal_queues": internal_q,
                "thinking_count": thinking_count,
            },
        }
    )

    telegram_q = int(telegram_metrics.get("chat_queues", 0) or 0)
    telegram_status = "ok"
    telegram_reason = "inactive channel"
    telegram_age = _age_seconds(str(telegram_metrics.get("updated_at", "")), now)
    if active_channel == "telegram":
        telegram_reason = "healthy"
        if telegram_q >= 40:
            telegram_status = "critical"
            telegram_reason = f"chat queues overloaded ({telegram_q})"
        elif telegram_q >= 15:
            telegram_status = "warning"
            telegram_reason = f"chat queues elevated ({telegram_q})"
        elif telegram_age is not None and telegram_age > 240:
            telegram_status = "warning"
            telegram_reason = f"metrics stale for {int(telegram_age)}s"
    out.append(
        {
            "id": "telegram",
            "name": "Telegram",
            "status": telegram_status,
            "reason": telegram_reason,
            "updated_at": telegram_metrics.get("updated_at"),
            "metrics": {"chat_queues": telegram_q, "send_tasks": int(telegram_metrics.get("send_tasks", 0) or 0)},
        }
    )

    whatsapp_connected_raw = whatsapp_metrics.get("connected")
    whatsapp_connected = None if whatsapp_connected_raw is None else int(bool(whatsapp_connected_raw))
    whatsapp_mappings = int(whatsapp_metrics.get("chat_mappings", 0) or 0)
    whatsapp_status = "ok"
    whatsapp_reason = "inactive channel"
    whatsapp_age = _age_seconds(str(whatsapp_metrics.get("updated_at", "")), now)
    if active_channel == "whatsapp":
        if whatsapp_connected == 0:
            whatsapp_status = "critical"
            whatsapp_reason = "bridge disconnected"
        elif whatsapp_age is not None and whatsapp_age > 240:
            whatsapp_status = "warning"
            whatsapp_reason = f"metrics stale for {int(whatsapp_age)}s"
        elif whatsapp_connected == 1:
            whatsapp_reason = (
                f"connected ({whatsapp_mappings} mapped chat(s))"
                if whatsapp_mappings > 0
                else "connected"
            )
        else:
            whatsapp_status = "warning"
            whatsapp_reason = "awaiting bridge status"
    out.append(
        {
            "id": "whatsapp",
            "name": "WhatsApp",
            "status": whatsapp_status,
            "reason": whatsapp_reason,
            "updated_at": whatsapp_metrics.get("updated_at"),
            "metrics": {
                "connected": whatsapp_connected,
                "chat_mappings": whatsapp_mappings,
            },
        }
    )

    sessions_running = int(exec_metrics.get("background_sessions_running", 0) or 0)
    exec_status = "ok"
    exec_reason = "idle"
    if sessions_running >= 24:
        exec_status = "critical"
        exec_reason = f"many background sessions ({sessions_running})"
    elif sessions_running >= 8:
        exec_status = "warning"
        exec_reason = f"background sessions elevated ({sessions_running})"
    elif sessions_running > 0:
        exec_reason = f"{sessions_running} session(s) running"
    out.append(
        {
            "id": "exec_run",
            "name": "Exec Run",
            "status": exec_status,
            "reason": exec_reason,
            "updated_at": exec_metrics.get("updated_at"),
            "metrics": {"background_sessions_running": sessions_running},
        }
    )

    scheduler_status = "ok"
    scheduler_reason = "scheduler loop idle"
    scheduler_updated_at = scheduler_metrics.get("updated_at")
    scheduler_running = bool(scheduler_metrics.get("running"))
    scheduler_tick_status = str(scheduler_metrics.get("last_tick_status", "") or "").lower()
    scheduler_age = _age_seconds(str(scheduler_updated_at or ""), now)
    scheduler_dispatch_errors = int(scheduler_metrics.get("last_dispatch_errors", 0) or 0)
    scheduler_rejected = int(
        scheduler_metrics.get("last_dispatch_rejected_by_policy", 0) or 0
    )
    scheduler_completed = int(scheduler_metrics.get("last_dispatch_completed", 0) or 0)
    scheduler_started = int(scheduler_metrics.get("last_dispatch_started", 0) or 0)
    if not scheduler_metrics:
        scheduler_reason = "scheduler metrics unavailable"
    elif not scheduler_running:
        scheduler_status = "warning"
        scheduler_reason = "scheduler loop is not running"
    elif scheduler_tick_status == "failed":
        scheduler_status = "critical"
        scheduler_reason = "last scheduler tick failed"
    elif scheduler_age is not None and scheduler_age > 180:
        scheduler_status = "warning"
        scheduler_reason = f"scheduler metrics stale for {int(scheduler_age)}s"
    elif scheduler_dispatch_errors > 0:
        scheduler_status = "warning"
        scheduler_reason = f"{scheduler_dispatch_errors} scheduler dispatch error(s) on last tick"
    elif scheduler_rejected > 0:
        scheduler_status = "warning"
        scheduler_reason = (
            f"{scheduler_rejected} scheduled task(s) rejected by policy on last tick"
        )
    elif scheduler_completed > 0:
        scheduler_reason = f"completed {scheduler_completed} scheduled Octo task(s) on last tick"
    elif scheduler_started > 0:
        scheduler_reason = f"started {scheduler_started} scheduled task(s) on last tick"
    out.append(
        {
            "id": "scheduler",
            "name": "Scheduler",
            "status": scheduler_status,
            "reason": scheduler_reason,
            "updated_at": scheduler_updated_at,
            "metrics": {
                "running": scheduler_running,
                "last_tick_status": scheduler_tick_status or None,
                "last_due_count": int(scheduler_metrics.get("last_due_count", 0) or 0),
                "last_dispatch_started": scheduler_started,
                "last_dispatch_completed": scheduler_completed,
                "last_dispatch_duplicates": int(
                    scheduler_metrics.get("last_dispatch_duplicates", 0) or 0
                ),
                "last_dispatch_rejected_by_policy": scheduler_rejected,
                "last_dispatch_errors": scheduler_dispatch_errors,
                "last_policy_reasons": dict(scheduler_metrics.get("last_policy_reasons", {}) or {}),
            },
        }
    )

    mcp_error = 0
    mcp_warn = 0
    mcp_reconnecting = 0
    for payload in mcp_servers.values():
        if not isinstance(payload, dict):
            continue
        status = str(payload.get("status", "unknown")).lower()
        if status == "error":
            mcp_error += 1
        elif status == "reconnecting":
            mcp_reconnecting += 1
            mcp_warn += 1
        elif status != "connected":
            mcp_warn += 1
    mcp_total = len(mcp_servers)
    mcp_status = "ok"
    mcp_reason = "no MCP servers configured" if mcp_total == 0 else f"{mcp_total} server(s) connected"
    if mcp_error > 0:
        mcp_status = "critical"
        mcp_reason = f"{mcp_error} MCP server(s) in error"
    elif mcp_reconnecting > 0:
        mcp_status = "warning"
        mcp_reason = f"{mcp_reconnecting} MCP server(s) reconnecting"
    elif mcp_warn > 0:
        mcp_status = "warning"
        mcp_reason = f"{mcp_warn} MCP server(s) not connected"
    out.append(
        {
            "id": "mcp",
            "name": "MCP",
            "status": mcp_status,
            "reason": mcp_reason,
            "updated_at": None,
            "metrics": {"total": mcp_total, "error": mcp_error, "warning": mcp_warn, "reconnecting": mcp_reconnecting},
        }
    )
    return out


def _derive_overall_health(
    *,
    services: list[dict[str, Any]],
    failed_workers: int,
    control_pending: int,
    log_health: dict[str, Any],
    system_running: bool,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    severity = "ok"

    def _raise(level: str) -> None:
        nonlocal severity
        rank = {"ok": 0, "warning": 1, "critical": 2}
        if rank[level] > rank[severity]:
            severity = level

    for service in services:
        status = str(service.get("status", "ok"))
        if status in {"warning", "critical"}:
            _raise(status)
            reasons.append(f"{service.get('name', service.get('id', 'service'))}: {service.get('reason', status)}")

    if not system_running:
        _raise("critical")
        reasons.append("System process is not running")

    if failed_workers >= 5:
        _raise("critical")
        reasons.append(f"{failed_workers} failed workers active")
    elif failed_workers > 0:
        _raise("warning")
        reasons.append(f"{failed_workers} failed worker(s)")

    if control_pending >= 10:
        _raise("warning")
        reasons.append(f"{control_pending} pending control requests")

    error_rate = float(log_health.get("error_rate_5m", 0.0) or 0.0)
    event_count = int(log_health.get("event_count_5m", 0) or 0)
    if event_count >= 5 and error_rate >= 0.5:
        _raise("critical")
        reasons.append("High log error rate in last 5 minutes")
    elif event_count >= 5 and error_rate >= 0.2:
        _raise("warning")
        reasons.append("Elevated log error rate in last 5 minutes")

    return severity, reasons[:6]


def _health_summary(status: str, reasons: list[str]) -> str:
    label = status.upper()
    if not reasons:
        return f"{label}: all systems normal"
    return f"{label}: {reasons[0]}"


def _build_incidents(
    *,
    services: list[dict[str, Any]],
    recent_workers: list[WorkerRecord],
    logs: list[dict[str, Any]],
    control_pending: int,
    queue_depth: int,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []

    for service in services:
        status = str(service.get("status", "ok")).lower()
        if status not in {"warning", "critical"}:
            continue
        severity = "critical" if status == "critical" else "warning"
        impact = 90 if severity == "critical" else 60
        items.append(
            {
                "id": f"svc-{service.get('id', 'service')}",
                "service": str(service.get("id", "gateway")),
                "severity": severity,
                "impact": impact,
                "title": f"{service.get('name', service.get('id', 'Service'))} health {severity}",
                "summary": str(service.get("reason", "")),
                "count": 1,
                "latest_at": str(service.get("updated_at", "")),
                "source": "service_health",
            }
        )

    failed_workers = [w for w in recent_workers if str(w.status).lower() == "failed"]
    if failed_workers:
        worker_count = len(failed_workers)
        sev = "critical" if worker_count >= 3 else "warning"
        sample_ids = ", ".join(w.id[:8] for w in failed_workers[:3])
        items.append(
            {
                "id": "workers-failed",
                "service": "workers",
                "severity": sev,
                "impact": 70 + min(20, worker_count * 5),
                "title": "Worker failures",
                "summary": f"{worker_count} failed worker(s): {sample_ids}",
                "count": worker_count,
                "latest_at": failed_workers[0].updated_at.isoformat(),
                "source": "worker_status",
            }
        )

    log_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for log in logs:
        level = str(log.get("level", "info")).lower()
        if level not in {"warning", "error", "critical"}:
            continue
        severity = "critical" if level in {"error", "critical"} else "warning"
        service = str(log.get("service", "gateway"))
        category = _categorize_incident_event(str(log.get("event", "")))
        key = (service, category, severity)
        group = log_groups.get(key)
        if group is None:
            group = {
                "id": f"log-{service}-{category}-{severity}",
                "service": service,
                "severity": severity,
                "impact": 84 if severity == "critical" else 55,
                "title": f"{service} {category}",
                "summary": str(log.get("event", ""))[:140],
                "count": 0,
                "latest_at": str(log.get("timestamp", "")),
                "source": "logs",
            }
            log_groups[key] = group
        group["count"] = int(group.get("count", 0)) + 1
        ts = str(log.get("timestamp", ""))
        if ts and ts > str(group.get("latest_at", "")):
            group["latest_at"] = ts
    for group in log_groups.values():
        count = int(group.get("count", 0))
        if count < 2 and str(group.get("severity")) != "critical":
            continue
        group["impact"] = int(group.get("impact", 50)) + min(15, count * 2)
        group["summary"] = f"{group['summary']} ({count} events)"
        items.append(group)

    if control_pending >= 8:
        items.append(
            {
                "id": "control-queue-backlog",
                "service": "gateway",
                "severity": "warning",
                "impact": 62 + min(15, control_pending),
                "title": "Control queue backlog",
                "summary": f"{control_pending} pending control requests",
                "count": control_pending,
                "latest_at": "",
                "source": "control_queue",
            }
        )

    if queue_depth >= 20:
        items.append(
            {
                "id": "queue-depth-pressure",
                "service": "octo",
                "severity": "warning",
                "impact": 65 + min(20, queue_depth // 2),
                "title": "Queue pressure",
                "summary": f"Queue depth is {queue_depth}",
                "count": queue_depth,
                "latest_at": "",
                "source": "queues",
            }
        )

    items.sort(
        key=lambda x: (
            _severity_rank(str(x.get("severity", "warning"))),
            int(x.get("impact", 0)),
            int(x.get("count", 0)),
        ),
        reverse=True,
    )
    top_items = items[:5]
    critical = sum(1 for i in top_items if str(i.get("severity")) == "critical")
    warning = sum(1 for i in top_items if str(i.get("severity")) == "warning")
    return {
        "summary": {"open": len(top_items), "critical": critical, "warning": warning},
        "items": top_items,
    }


def _build_slo_metrics(
    *,
    active_channel: str,
    services: list[dict[str, Any]],
    log_health: dict[str, Any],
    recent_workers: list[WorkerRecord],
) -> dict[str, Any]:
    availability_target = 99.0
    error_budget_target = 1.0
    error_budget_fraction = error_budget_target / 100.0

    core_services = [s for s in services if str(s.get("id", "")) in {"gateway", "octo", active_channel, "exec_run"}]
    if not core_services:
        uptime_pct = 100.0
    else:
        points = 0.0
        for service in core_services:
            status = str(service.get("status", "ok")).lower()
            if status == "ok":
                points += 1.0
            elif status == "warning":
                points += 0.6
            else:
                points += 0.0
        uptime_pct = round((points / len(core_services)) * 100.0, 1)

    error_rate_fraction = float(log_health.get("error_rate_5m", 0.0) or 0.0)
    burn_rate = round(error_rate_fraction / max(error_budget_fraction, 1e-6), 2)
    error_budget_remaining_pct = round(max(0.0, 100.0 - (burn_rate * 10.0)), 1)

    mttr_minutes = _estimate_mttr_minutes(recent_workers)
    uptime_status = "ok" if uptime_pct >= availability_target else ("warning" if uptime_pct >= 95.0 else "critical")
    burn_status = "ok" if burn_rate <= 1.0 else ("warning" if burn_rate <= 2.0 else "critical")
    if mttr_minutes is None:
        mttr_status = "warning"
    elif mttr_minutes <= 15:
        mttr_status = "ok"
    elif mttr_minutes <= 45:
        mttr_status = "warning"
    else:
        mttr_status = "critical"

    return {
        "objectives": {"availability_target_pct": availability_target, "error_budget_pct": error_budget_target},
        "uptime_pct": {"value": uptime_pct, "status": uptime_status},
        "burn_rate": {"value": burn_rate, "status": burn_status},
        "error_budget_remaining_pct": {"value": error_budget_remaining_pct, "status": burn_status},
        "mttr_minutes": {"value": mttr_minutes, "status": mttr_status},
    }


def _estimate_mttr_minutes(recent_workers: list[WorkerRecord]) -> float | None:
    failed = [w for w in recent_workers if str(w.status).lower() == "failed"]
    if not failed:
        return 0.0
    completed = [w for w in recent_workers if str(w.status).lower() == "completed"]
    durations: list[float] = []
    for f in failed[:10]:
        candidate: WorkerRecord | None = None
        for c in completed:
            if c.updated_at <= f.updated_at:
                continue
            same_template = bool(f.template_id and c.template_id and f.template_id == c.template_id)
            if (same_template or not f.template_id) and (
                candidate is None or c.updated_at < candidate.updated_at
            ):
                candidate = c
        if candidate is None:
            continue
        delta_min = (candidate.updated_at - f.updated_at).total_seconds() / 60.0
        if delta_min >= 0:
            durations.append(delta_min)
    if not durations:
        return None
    return round(sum(durations) / len(durations), 1)


def _serialize_recent_worker(
    worker: WorkerRecord,
    *,
    store: SQLiteStore | None = None,
    template_cache: dict[str, WorkerTemplateRecord | None] | None = None,
) -> dict[str, Any]:
    output = worker.output if isinstance(worker.output, dict) else None
    template_config: dict[str, Any] | None = None
    template_id = str(worker.template_id or "").strip()

    if store is not None and template_id:
        cache = template_cache if template_cache is not None else {}
        template = cache.get(template_id)
        if template_id not in cache:
            template = store.get_worker_template(template_id)
            cache[template_id] = template
        if template is not None:
            template_config = {
                "model": template.model,
                "max_thinking_steps": template.max_thinking_steps,
                "default_timeout_seconds": template.default_timeout_seconds,
                "available_tools": template.available_tools,
                "can_spawn_children": template.can_spawn_children,
            }

    return {
        "id": worker.id,
        "template_name": worker.template_name or worker.template_id or "",
        "template_id": worker.template_id,
        "status": worker.status,
        "task": worker.task,
        "created_at": worker.created_at.isoformat(),
        "updated_at": worker.updated_at.isoformat(),
        "summary": worker.summary or "",
        "error": worker.error or "",
        "tools_used": worker.tools_used or [],
        "parent_worker_id": worker.parent_worker_id,
        "lineage_id": worker.lineage_id,
        "spawn_depth": worker.spawn_depth,
        "result_preview": _worker_result_preview(worker),
        "output": output,
        "template_config": template_config,
        "plan_binding": _worker_plan_binding_payload(worker.id, store),
        "audit_timeline": _worker_audit_timeline(worker.id, store),
    }


def _worker_plan_binding_payload(worker_id: str, store: SQLiteStore | None) -> dict[str, Any] | None:
    if store is None or not worker_id:
        return None
    try:
        step = store.get_plan_step_by_worker_run_id(worker_id)
    except Exception:
        logger.debug(
            "Failed to load worker plan binding",
            exc_info=True,
            extra={"worker_id": worker_id},
        )
        return None
    if step is None:
        return None
    return {
        "run_id": step.run_id,
        "step_id": step.step_id,
        "status": step.status,
        "title": step.title,
        "kind": step.kind,
    }


def _worker_audit_timeline(worker_id: str, store: SQLiteStore | None) -> list[dict[str, Any]]:
    if store is None:
        return []
    try:
        events = store.list_audit_for_correlation(worker_id, limit=40)
    except Exception:
        logger.debug("Failed to load worker audit timeline", exc_info=True, extra={"worker_id": worker_id})
        return []
    return [_serialize_worker_audit_event(event) for event in events]


def _serialize_worker_audit_event(event: AuditEvent) -> dict[str, Any]:
    data = event.data if isinstance(event.data, dict) else {}
    return {
        "id": event.id,
        "ts": event.ts.isoformat(),
        "level": event.level,
        "event_type": event.event_type,
        "data_preview": _truncate_preview(_safe_preview_json(data), 420) if data else "",
    }


def _worker_result_preview(worker: WorkerRecord) -> str:
    summary = str(worker.summary or "").strip()
    if summary:
        return _truncate_preview(summary, 280)

    error = str(worker.error or "").strip()
    if error:
        return _truncate_preview(error, 280)

    output = worker.output if isinstance(worker.output, dict) else None
    if output:
        serialized = _safe_preview_json(output)
        if serialized:
            return _truncate_preview(serialized, 280)
    return ""


def _safe_preview_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except Exception:
        return repr(value)


def _truncate_preview(text: str, limit: int) -> str:
    cleaned = " ".join(str(text).split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _build_noise_control(*, logs: list[dict[str, Any]]) -> dict[str, Any]:
    noisy = [
        log for log in logs if str(log.get("level", "")).lower() in {"warning", "error", "critical"}
    ]
    raw_alerts = len(noisy)
    groups: dict[tuple[str, str, str], int] = {}
    for log in noisy:
        service = str(log.get("service", "gateway"))
        severity = "critical" if str(log.get("level", "")).lower() in {"error", "critical"} else "warning"
        category = _categorize_incident_event(str(log.get("event", "")))
        key = (service, category, severity)
        groups[key] = groups.get(key, 0) + 1
    deduped_alerts = len(groups)
    suppressed = max(0, raw_alerts - deduped_alerts)
    reduction_pct = round((suppressed / raw_alerts) * 100.0, 1) if raw_alerts > 0 else 0.0
    top_groups = sorted(groups.items(), key=lambda kv: kv[1], reverse=True)[:4]
    top = [
        {"service": s, "category": c, "severity": sev, "count": count}
        for (s, c, sev), count in top_groups
    ]
    return {
        "raw_alerts": raw_alerts,
        "deduped_alerts": deduped_alerts,
        "suppressed_alerts": suppressed,
        "reduction_pct": reduction_pct,
        "top_groups": top,
    }


def _categorize_incident_event(event: str) -> str:
    text = event.lower()
    if "timeout" in text:
        return "timeouts"
    if "auth" in text or "unauthorized" in text:
        return "auth"
    if "queue" in text or "backlog" in text:
        return "queue"
    if "connection" in text or "socket" in text:
        return "connectivity"
    if "worker" in text and ("failed" in text or "error" in text):
        return "worker_failures"
    if "rate" in text and "limit" in text:
        return "rate_limits"
    return "errors"


def _severity_rank(severity: str) -> int:
    s = severity.strip().lower()
    if s == "critical":
        return 2
    if s == "warning":
        return 1
    return 0


def _compute_log_health(
    log_path: Path,
    now: datetime,
    window_minutes: int = 5,
    *,
    filters: DashboardFilters | None = None,
) -> dict[str, Any]:
    cutoff = now.timestamp() - window_minutes * 60
    total = 0
    errors = 0
    effective_filters = filters or DashboardFilters(window_minutes=window_minutes, service="all", environment="all")
    for data in _collect_logs(log_path, max_lines=1000, filters=effective_filters):
        ts = _parse_timestamp(str(data.get("timestamp", "")))
        if ts is not None and ts.timestamp() < cutoff:
            continue
        total += 1
        level = str(data.get("level", "info")).lower()
        if level in {"error", "critical"}:
            errors += 1
    return {
        "window_minutes": window_minutes,
        "event_count_5m": total,
        "error_count_5m": errors,
        "error_rate_5m": (errors / total) if total > 0 else 0.0,
    }


def _estimate_control_latency_p95_ms(requests: list[dict[str, Any]], acks: list[dict[str, Any]]) -> int | None:
    if not requests or not acks:
        return None
    by_request_id: dict[str, datetime] = {}
    for req in requests:
        rid = str(req.get("request_id", "")).strip()
        if not rid:
            continue
        ts = _extract_event_timestamp(req)
        if ts is not None:
            by_request_id[rid] = ts

    samples_ms: list[int] = []
    for ack in acks:
        rid = str(ack.get("request_id", "")).strip()
        if not rid:
            continue
        req_ts = by_request_id.get(rid)
        ack_ts = _extract_event_timestamp(ack)
        if req_ts is None or ack_ts is None:
            continue
        delta_ms = int((ack_ts - req_ts).total_seconds() * 1000)
        if 0 <= delta_ms <= 30 * 60 * 1000:
            samples_ms.append(delta_ms)
    if not samples_ms:
        return None
    samples_ms.sort()
    idx = int(math.ceil(0.95 * len(samples_ms))) - 1
    idx = max(0, min(idx, len(samples_ms) - 1))
    return samples_ms[idx]


def _extract_event_timestamp(payload: dict[str, Any]) -> datetime | None:
    for key in ("timestamp", "created_at", "requested_at", "acked_at", "updated_at"):
        raw = payload.get(key)
        if raw is None:
            continue
        ts = _parse_timestamp(str(raw))
        if ts is not None:
            return ts
    return None


def _parse_timestamp(raw: str) -> datetime | None:
    text = raw.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _age_seconds(value: str | None, now: datetime) -> float | None:
    if not value:
        return None
    dt = _parse_timestamp(str(value))
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds())


def _service_matches_filter(service: str, selected: str) -> bool:
    if selected == "all":
        return True
    return service.strip().lower() == selected.strip().lower()


def _detect_log_service(payload: dict[str, Any], event: str) -> str:
    candidates = [
        str(payload.get("service", "")),
        str(payload.get("component", "")),
        str(payload.get("module", "")),
        str(payload.get("logger", "")),
    ]
    haystack = " ".join(candidates + [event]).lower()
    if "whatsapp" in haystack:
        return "whatsapp"
    if "telegram" in haystack:
        return "telegram"
    if "octo" in haystack:
        return "octo"
    if "exec_run" in haystack or "exec run" in haystack:
        return "exec_run"
    if "mcp" in haystack:
        return "mcp"
    if "worker" in haystack:
        return "workers"
    if "gateway" in haystack or "websocket" in haystack or "fastapi" in haystack:
        return "gateway"
    return "gateway"


def _resolve_active_channel(status_data: dict[str, Any], settings: Settings) -> str:
    raw = str(status_data.get("active_channel", "")).strip()
    if raw:
        return normalize_user_channel(raw)
    return normalize_user_channel(settings.user_channel)


def _select_active_channel_metrics(
    *,
    active_channel: str,
    telegram_metrics: dict[str, Any],
    whatsapp_metrics: dict[str, Any],
) -> dict[str, Any]:
    if active_channel == "whatsapp":
        return {
            "queue_depth": 0,
            "send_tasks": None,
            "connected": int(whatsapp_metrics.get("connected", 0) or 0),
            "chat_mappings": int(whatsapp_metrics.get("chat_mappings", 0) or 0),
            "updated_at": whatsapp_metrics.get("updated_at"),
        }
    if active_channel == "desktop":
        return {
            "queue_depth": 0,
            "send_tasks": None,
            "connected": None,
            "chat_mappings": None,
            "updated_at": None,
        }
    return {
        "queue_depth": int(telegram_metrics.get("chat_queues", 0) or 0),
        "send_tasks": int(telegram_metrics.get("send_tasks", 0) or 0),
        "connected": None,
        "chat_mappings": None,
        "updated_at": telegram_metrics.get("updated_at"),
    }


def _extract_log_environment(payload: dict[str, Any]) -> str:
    for key in ("environment", "env", "stage"):
        raw = str(payload.get(key, "")).strip().lower()
        if raw:
            cleaned = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_"})
            if cleaned:
                return cleaned
    return "local"


def _tail_logs(path: Path, max_lines: int, *, filters: DashboardFilters) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for data in _collect_logs(path, max_lines=1000, filters=filters):
        out.append(
            {
                "event": str(data.get("event", ""))[:120],
                "level": str(data.get("level", "info")),
                "timestamp": str(data.get("timestamp", "")),
                "service": str(data.get("service", "unknown")),
            }
        )
        if len(out) >= max_lines:
            break
    return out


def _collect_logs(path: Path, max_lines: int, *, filters: DashboardFilters) -> list[dict[str, Any]]:
    now = _now_utc()
    cutoff = now.timestamp() - max(1, filters.window_minutes) * 60
    out: list[dict[str, Any]] = []
    for line in _read_last_lines(path, max_lines=max_lines):
        entry = _normalize_log_entry(line, filters=filters)
        if entry is None:
            continue
        ts = _parse_timestamp(str(entry.get("timestamp", "")))
        if ts is not None and ts.timestamp() < cutoff:
            continue
        out.append(entry)
    out.reverse()
    return out


def _normalize_log_entry(line: str, *, filters: DashboardFilters) -> dict[str, Any] | None:
    raw = line.strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        service = _detect_log_service({}, raw)
        if not _service_matches_filter(service, filters.service):
            return None
        environment = "local"
        if filters.environment != "all" and filters.environment != environment:
            return None
        return {"event": raw[:200], "level": "info", "timestamp": "", "service": service, "environment": environment}
    if not isinstance(parsed, dict):
        return None
    event = str(parsed.get("event", ""))[:200]
    level = str(parsed.get("level", "info"))
    timestamp = str(parsed.get("timestamp", ""))
    service = _detect_log_service(parsed, event)
    environment = _extract_log_environment(parsed)
    if not _service_matches_filter(service, filters.service):
        return None
    if filters.environment != "all" and filters.environment != environment:
        return None
    return {
        "event": event,
        "level": level,
        "timestamp": timestamp,
        "service": service,
        "environment": environment,
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in _read_last_lines(path, max_lines=250):
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            out.append(item)
    return out


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _read_last_lines(path: Path, max_lines: int = 200, max_bytes: int = 256 * 1024) -> list[str]:
    if not path.exists() or max_lines <= 0:
        return []
    try:
        size = path.stat().st_size
    except OSError:
        return []
    start = max(0, size - max(1, max_bytes))
    tail: deque[str] = deque(maxlen=max_lines)
    try:
        with path.open("rb") as handle:
            if start > 0:
                handle.seek(start)
                _ = handle.readline()
            for raw in handle:
                text = raw.decode("utf-8", errors="ignore").rstrip("\n\r")
                tail.append(text)
    except OSError:
        return []
    return list(tail)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _uptime_human(started_at: str | None) -> str:
    if not started_at:
        return "N/A"
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        return "N/A"
    delta = _now_utc() - start
    total = int(delta.total_seconds())
    if total < 0:
        return "N/A"
    hours = total // 3600
    minutes = (total % 3600) // 60
    seconds = total % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def _settings_to_octopal_config(settings: Settings) -> OctopalConfig:
    if settings.config_obj is not None:
        return settings.config_obj.model_copy(deep=True)
    try:
        return load_config()
    except Exception:
        pass

    config = OctopalConfig()
    config.user_channel = normalize_user_channel(settings.user_channel)
    config.telegram = TelegramConfig(
        bot_token=settings.telegram_bot_token,
        allowed_chat_ids=[
            item.strip() for item in str(settings.allowed_telegram_chat_ids or "").split(",") if item.strip()
        ],
        parse_mode=settings.telegram_parse_mode,
    )
    config.llm = LLMConfig(
        provider_id=settings.litellm_provider_id,
        model=settings.litellm_model,
        api_key=settings.litellm_api_key,
        api_base=settings.litellm_api_base,
        model_prefix=settings.litellm_model_prefix,
    )
    config.worker_llm_default = LLMConfig(
        provider_id=settings.litellm_provider_id,
        model=settings.litellm_model,
        api_key=settings.litellm_api_key,
        api_base=settings.litellm_api_base,
        model_prefix=settings.litellm_model_prefix,
    )
    config.litellm = LiteLLMRuntimeConfig(
        num_retries=settings.litellm_num_retries,
        timeout=settings.litellm_timeout,
        fallbacks=settings.litellm_fallbacks,
        drop_params=settings.litellm_drop_params,
        caching=settings.litellm_caching,
        max_concurrency=settings.litellm_max_concurrency,
        rate_limit_max_retries=settings.litellm_rate_limit_max_retries,
        rate_limit_base_delay_seconds=settings.litellm_rate_limit_base_delay_seconds,
        rate_limit_max_delay_seconds=settings.litellm_rate_limit_max_delay_seconds,
    )
    config.storage = StorageConfig(
        state_dir=settings.state_dir,
        workspace_dir=settings.workspace_dir,
    )
    config.memory = MemoryConfig(
        top_k=settings.memory_top_k,
        prefilter_k=settings.memory_prefilter_k,
        min_score=settings.memory_min_score,
        max_chars=settings.memory_max_chars,
        owner_id=settings.memory_owner_id,
    )
    config.gateway = GatewayConfig(
        host=settings.gateway_host,
        port=settings.gateway_port,
        tailscale_ips=settings.tailscale_ips,
        dashboard_token=settings.dashboard_token,
        tailscale_auto_serve=settings.tailscale_auto_serve,
        webapp_enabled=settings.webapp_enabled,
        webapp_dist_dir=settings.webapp_dist_dir,
    )
    config.workers = WorkerRuntimeConfig(
        launcher=settings.worker_launcher,
        docker_image=settings.worker_docker_image,
        docker_workspace=settings.worker_docker_workspace,
        docker_host_workspace=settings.worker_docker_host_workspace,
        max_spawn_depth=settings.worker_max_spawn_depth,
        max_children_total=settings.worker_max_children_total,
        max_children_concurrent=settings.worker_max_children_concurrent,
    )
    config.whatsapp = WhatsAppConfig(
        mode=settings.whatsapp_mode,
        allowed_numbers=[
            item.strip() for item in str(settings.allowed_whatsapp_numbers or "").split(",") if item.strip()
        ],
        auth_dir=settings.whatsapp_auth_dir,
        bridge_host=settings.whatsapp_bridge_host,
        bridge_port=settings.whatsapp_bridge_port,
        callback_token=settings.whatsapp_callback_token,
        node_command=settings.whatsapp_node_command,
    )
    config.search = SearchConfig(
        brave_api_key=settings.brave_api_key,
        firecrawl_api_key=settings.firecrawl_api_key,
    )
    config.log_level = settings.log_level
    config.debug_prompts = settings.debug_prompts
    config.heartbeat_interval_seconds = settings.heartbeat_interval_seconds
    config.user_message_grace_seconds = settings.user_message_grace_seconds
    return config


def _dashboard_editable_config_payload(settings: Settings) -> DashboardConfigPayload:
    config = _settings_to_octopal_config(settings)
    return DashboardConfigPayload(
        user_channel=config.user_channel,
        telegram=config.telegram,
        llm=config.llm,
        worker_llm_default=config.worker_llm_default,
        litellm=config.litellm,
        storage=config.storage,
        memory=config.memory,
        gateway=config.gateway,
        workers=config.workers,
        whatsapp=config.whatsapp,
        search=config.search,
        log_level=config.log_level,
        debug_prompts=config.debug_prompts,
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
        user_message_grace_seconds=config.user_message_grace_seconds,
    )


def _sanitize_dashboard_config_payload(payload: DashboardConfigPayload) -> DashboardConfigPayload:
    sanitized = payload.model_copy(deep=True)
    sanitized.telegram.bot_token = ""
    sanitized.llm.api_key = None
    sanitized.worker_llm_default.api_key = None
    sanitized.gateway.dashboard_token = ""
    sanitized.whatsapp.callback_token = ""
    sanitized.search.brave_api_key = None
    sanitized.search.firecrawl_api_key = None
    return sanitized


def _merge_dashboard_secret_fields(
    payload: DashboardConfigPayload,
    existing: OctopalConfig,
) -> DashboardConfigPayload:
    merged = payload.model_copy(deep=True)
    if not merged.telegram.bot_token.strip():
        merged.telegram.bot_token = existing.telegram.bot_token
    if merged.llm.provider_id == existing.llm.provider_id and not (merged.llm.api_key or "").strip():
        merged.llm.api_key = existing.llm.api_key
    if (
        merged.worker_llm_default.provider_id == existing.worker_llm_default.provider_id
        and not (merged.worker_llm_default.api_key or "").strip()
    ):
        merged.worker_llm_default.api_key = existing.worker_llm_default.api_key
    if not merged.gateway.dashboard_token.strip():
        merged.gateway.dashboard_token = existing.gateway.dashboard_token
    if not merged.whatsapp.callback_token.strip():
        merged.whatsapp.callback_token = existing.whatsapp.callback_token
    if not (merged.search.brave_api_key or "").strip():
        merged.search.brave_api_key = existing.search.brave_api_key
    if not (merged.search.firecrawl_api_key or "").strip():
        merged.search.firecrawl_api_key = existing.search.firecrawl_api_key
    return merged



def _dashboard_unavailable_html(settings: Settings) -> str:
    webapp_dist = _resolve_webapp_dist_dir(settings)
    project_root = Path(__file__).resolve().parents[3]
    dist_hint_path = Path(settings.webapp_dist_dir) if settings.webapp_dist_dir is not None else (project_root / "webapp" / "dist")
    if not dist_hint_path.is_absolute():
        dist_hint_path = project_root / dist_hint_path
    dist_hint = str(dist_hint_path)
    dist_status = 'found' if webapp_dist is not None else 'not found'
    enabled = 'enabled' if settings.webapp_enabled else 'disabled'
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Octopal Dashboard Unavailable</title>
  <style>
    body {{ font-family: Segoe UI, Tahoma, sans-serif; margin: 32px; color: #1f2937; }}
    .card {{ max-width: 820px; border: 1px solid #d0d5dd; border-radius: 12px; padding: 20px; background: #fff; }}
    code {{ background: #f2f4f7; padding: 2px 6px; border-radius: 6px; }}
  </style>
</head>
<body>
  <div class=\"card\">
    <h1>Dashboard Is Unavailable</h1>
    <p>The legacy inline dashboard has been removed. Build and enable the new web app.</p>
    <p>Flag status: <code>OCTOPAL_WEBAPP_ENABLED={enabled}</code></p>
    <p>Dist status: <code>{dist_status}</code> at <code>{dist_hint}</code></p>
    <ol>
      <li>Build frontend: <code>cd webapp && npm run build</code></li>
      <li>Enable flag: <code>OCTOPAL_WEBAPP_ENABLED=true</code></li>
      <li>Optional dist override: <code>OCTOPAL_WEBAPP_DIST_DIR=.../webapp/dist</code></li>
    </ol>
  </div>
</body>
</html>"""
