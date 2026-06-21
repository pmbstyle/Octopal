from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import structlog

from octopal.infrastructure.logging import correlation_id_var
from octopal.runtime.octo.runtime_config import _env_int
from octopal.tools.diagnostics import resolve_tool_diagnostics
from octopal.tools.registry import ToolPolicy, ToolPolicyPipelineStep, ToolSpec
from octopal.tools.tools import get_tools

logger = structlog.get_logger(__name__)
GetToolsFn = Callable[..., list[ToolSpec]]

_DEFAULT_MAX_TOOL_COUNT = 64
_MIN_TOOL_COUNT_ON_OVERFLOW = 12
_CATALOG_TOOL_EXPANSION_LIMIT = 12
_CATALOG_MCP_TOOL_EXPANSION_LIMIT = 1
_DEFAULT_INITIAL_OCTO_TOOL_COUNT = 42

_MANDATORY_OCTO_TOOL_NAMES = {
    "octo_restart_self",
    "octo_check_update",
    "octo_update_self",
    "octo_context_health",
    "check_schedule",
    "tool_catalog_search",
    "plan_create",
    "plan_status",
    "plan_update_step",
    "list_workers",
    "start_worker",
    "get_worker_status",
    "list_active_workers",
    "get_worker_result",
    "stop_worker",
    "fs_list",
    "fs_read",
    "fs_write",
    "fs_move",
    "fs_delete",
}
_PRIORITY_TOOL_NAMES = {
    "octo_restart_self",
    "octo_check_update",
    "octo_update_self",
    "octo_context_reset",
    "octo_context_health",
    "tool_catalog_search",
    "plan_create",
    "plan_status",
    "plan_update_step",
    "octo_self_queue_add",
    "execute_self_queue_item",
    "octo_self_queue_list",
    "octo_self_queue_take",
    "octo_self_queue_update",
    "octo_experiment_log",
    "check_schedule",
    "start_worker",
    "get_worker_result",
    "get_worker_output_path",
    "worker_yield",
    "exec_run",
    "gateway_status",
    "git_ops",
    "mcp_discover",
    "mcp_call",
    "manage_canon",
}
_ALWAYS_INCLUDE_TOOL_NAMES = {
    # Octo self-control baseline
    "octo_restart_self",
    "octo_check_update",
    "octo_update_self",
    "octo_context_reset",
    "octo_context_health",
    "check_schedule",
    "scheduler_status",
    "tool_catalog_search",
    "plan_create",
    "plan_status",
    "plan_update_step",
    # Self-queue controls are runtime state, not discoverable nice-to-haves.
    "octo_self_queue_add",
    "execute_self_queue_item",
    "octo_self_queue_list",
    "octo_self_queue_take",
    "octo_self_queue_update",
    "octo_opportunity_scan",
    # Scheduler control loop
    "list_schedule",
    "schedule_task",
    "remove_task",
    "repair_scheduled_tasks",
    # Worker lifecycle essentials
    "list_workers",
    "start_worker",
    "start_child_worker",
    "start_workers_parallel",
    "get_worker_status",
    "list_active_workers",
    "worker_session_status",
    "worker_yield",
    "get_worker_result",
    "get_worker_output_path",
    "stop_worker",
    "send_file_to_user",
    # Octo must always be able to inspect and mutate its workspace.
    "fs_list",
    "fs_read",
    "fs_write",
    "fs_move",
    "fs_delete",
    "exec_run",
    "git_ops",
    "mcp_call",
    "list_skills",
    "use_skill",
    "run_skill_script",
}
_A2A_TOOL_NAMES = {
    "a2a_list_peers",
    "a2a_send_message",
}
_INITIAL_OCTO_TOOL_NAMES = _ALWAYS_INCLUDE_TOOL_NAMES | {
    "manage_canon",
    "search_canon",
    "octo_opportunity_scan",
    "octo_self_queue_add",
    "execute_self_queue_item",
    "octo_self_queue_list",
    "octo_self_queue_take",
    "octo_self_queue_update",
    "octo_experiment_log",
    "octo_memchain_status",
    "octo_memchain_verify",
    "gateway_status",
    "mcp_discover",
}
_WORKER_FOLLOWUP_ALLOWED_TOOL_NAMES = {
    "answer_worker_instruction",
    "fs_write",
    "get_worker_output_path",
    "manage_canon",
    "octo_continue_from_control_route",
}
_HEARTBEAT_ALLOWED_TOOL_NAMES = {
    "octo_context_health",
    "scheduler_status",
    "check_schedule",
    "gateway_status",
}
_SCHEDULER_ALLOWED_TOOL_NAMES = {
    "check_schedule",
    "scheduler_status",
    "repair_scheduled_tasks",
    "octo_context_health",
    "list_workers",
    "list_active_workers",
}
_PROACTIVE_ALLOWED_TOOL_NAMES = {
    "check_schedule",
    "scheduler_status",
    "octo_context_health",
    "gateway_status",
    "octo_opportunity_scan",
    "repair_scheduled_tasks",
    "octo_self_queue_add",
    "execute_self_queue_item",
    "octo_self_queue_list",
    "octo_self_queue_take",
    "octo_self_queue_update",
}
_SCHEDULED_OCTO_CONTROL_ALLOWED_TOOL_NAMES = _SCHEDULER_ALLOWED_TOOL_NAMES | {
    "octo_continue_from_control_route",
    "octo_context_reset",
    "octo_memchain_status",
    "octo_memchain_verify",
    "manage_canon",
    "search_canon",
    "list_schedule",
    "schedule_task",
    "remove_task",
    "repair_scheduled_tasks",
    "gateway_status",
}
_INTERNAL_MAINTENANCE_ALLOWED_TOOL_NAMES = _HEARTBEAT_ALLOWED_TOOL_NAMES | {
    "list_workers",
    "list_active_workers",
}
_DURABLE_WORKSPACE_ROOTS = ("reports", "artifacts")

_FULL_ROUTE_PERMISSIONS = {
    "filesystem_read": True,
    "filesystem_write": True,
    "worker_manage": True,
    "llm_subtask": True,
    "canon_manage": True,
    "network": True,
    "exec": True,
    "service_read": True,
    "service_control": True,
    "deploy_control": True,
    "db_admin": True,
    "security_audit": True,
    "self_control": True,
    "mcp_exec": True,
    "desktop_control": True,
    "skill_use": True,
    "skill_exec": True,
    "skill_manage": True,
}
_CONTROL_PLANE_PERMISSIONS = {
    "canon_manage": True,
    "self_control": True,
    "service_read": True,
    "worker_manage": True,
}


def _workspace_root() -> Path:
    return Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()


def _resolve_get_tools(get_tools_fn: GetToolsFn | None) -> GetToolsFn:
    return get_tools if get_tools_fn is None else get_tools_fn


def _chat_turn_epoch_for_context(octo: Any, chat_id: int, correlation_id: str | None) -> int | None:
    resolver = getattr(octo, "chat_turn_epoch_for_correlation", None)
    if callable(resolver):
        try:
            bound_epoch = resolver(correlation_id, chat_id)
        except Exception:
            bound_epoch = None
        if bound_epoch is not None:
            return int(bound_epoch)
    current = getattr(octo, "current_chat_turn_epoch", None)
    if callable(current):
        try:
            return int(current(chat_id))
        except Exception:
            return None
    return None


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = str(raw).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _get_octo_tools(
    octo: Any, chat_id: int, *, get_tools_fn: GetToolsFn | None = None
) -> tuple[list[ToolSpec], dict[str, object]]:
    correlation_id = correlation_id_var.get()
    ctx = {
        "base_dir": _workspace_root(),
        "octo": octo,
        "chat_id": chat_id,
        "correlation_id": correlation_id,
        "chat_turn_epoch": _chat_turn_epoch_for_context(octo, chat_id, correlation_id),
        "mcp_manager": getattr(octo, "mcp_manager", None),
    }
    mcp_manager = ctx["mcp_manager"]
    policy_steps = [
        ToolPolicyPipelineStep(
            label="octo.raw_fetch_denylist",
            policy=ToolPolicy(deny=["web_fetch", "markdown_new_fetch", "fetch_plan_tool"]),
        ),
        ToolPolicyPipelineStep(
            label="octo.direct_exec_denylist",
            policy=ToolPolicy(deny=["test_run"]),
        ),
    ]
    resolved_get_tools = _resolve_get_tools(get_tools_fn)
    all_tools = resolved_get_tools(mcp_manager=mcp_manager)
    resolution_report = resolve_tool_diagnostics(
        all_tools,
        permissions=_FULL_ROUTE_PERMISSIONS,
        profile_name=os.getenv("OCTOPAL_OCTO_TOOL_PROFILE"),
        policy_pipeline_steps=policy_steps,
    )
    tool_specs = _ensure_mandatory_octo_tools(
        list(resolution_report.available_tools),
        all_tools,
    )
    tool_specs = _select_initial_octo_tool_specs(tool_specs)
    if _a2a_interop_enabled(octo):
        tool_specs = _ensure_named_tools(tool_specs, all_tools, _A2A_TOOL_NAMES)
    ctx["active_tool_specs"] = tool_specs
    ctx["tool_resolution_report"] = resolution_report
    ctx["all_tool_specs"] = all_tools
    deferred_count = max(0, len(resolution_report.available_tools) - len(tool_specs))
    if deferred_count:
        logger.info(
            "Octo deferred tool loading active",
            active_tool_count=len(tool_specs),
            deferred_tool_count=deferred_count,
        )
    return tool_specs, ctx


def _get_worker_followup_tools(
    octo: Any, chat_id: int, *, get_tools_fn: GetToolsFn | None = None
) -> tuple[list[ToolSpec], dict[str, object]]:
    workspace_root = _workspace_root()
    correlation_id = correlation_id_var.get()
    ctx = {
        "base_dir": workspace_root,
        "workspace_root": workspace_root,
        "allowed_paths": list(_DURABLE_WORKSPACE_ROOTS),
        "restrict_to_allowed_paths": True,
        "octo": octo,
        "chat_id": chat_id,
        "correlation_id": correlation_id,
        "chat_turn_epoch": _chat_turn_epoch_for_context(octo, chat_id, correlation_id),
    }
    policy_steps = [
        ToolPolicyPipelineStep(
            label="octo.worker_followup_allowlist",
            policy=ToolPolicy(allow=sorted(_WORKER_FOLLOWUP_ALLOWED_TOOL_NAMES)),
        )
    ]
    resolved_get_tools = _resolve_get_tools(get_tools_fn)
    known_tools = resolved_get_tools(mcp_manager=None)
    all_tools = _get_static_mode_tool_candidates(
        _WORKER_FOLLOWUP_ALLOWED_TOOL_NAMES,
        tool_candidates=known_tools,
        get_tools_fn=resolved_get_tools,
    )
    resolution_report = resolve_tool_diagnostics(
        all_tools,
        permissions=_FULL_ROUTE_PERMISSIONS,
        policy_pipeline_steps=policy_steps,
    )
    tool_specs = list(resolution_report.available_tools)
    tool_specs = _budget_tool_specs(tool_specs, max_count=len(_WORKER_FOLLOWUP_ALLOWED_TOOL_NAMES))
    ctx["active_tool_specs"] = tool_specs
    ctx["tool_resolution_report"] = resolution_report
    ctx["all_tool_specs"] = all_tools
    ctx["known_tool_specs"] = known_tools
    ctx["mcp_refresh_attempted"] = False
    return tool_specs, ctx


def _get_heartbeat_tools(
    octo: Any, chat_id: int, *, get_tools_fn: GetToolsFn | None = None
) -> tuple[list[ToolSpec], dict[str, object]]:
    return _get_control_plane_tools(
        octo,
        chat_id,
        allowed_tool_names=_HEARTBEAT_ALLOWED_TOOL_NAMES,
        policy_label="octo.heartbeat_allowlist",
        get_tools_fn=get_tools_fn,
    )


def _get_scheduler_tools(
    octo: Any, chat_id: int, *, get_tools_fn: GetToolsFn | None = None
) -> tuple[list[ToolSpec], dict[str, object]]:
    return _get_control_plane_tools(
        octo,
        chat_id,
        allowed_tool_names=_SCHEDULER_ALLOWED_TOOL_NAMES,
        policy_label="octo.scheduler_allowlist",
        get_tools_fn=get_tools_fn,
    )


def _get_proactive_tools(
    octo: Any, chat_id: int, *, get_tools_fn: GetToolsFn | None = None
) -> tuple[list[ToolSpec], dict[str, object]]:
    return _get_control_plane_tools(
        octo,
        chat_id,
        allowed_tool_names=_PROACTIVE_ALLOWED_TOOL_NAMES,
        policy_label="octo.proactive_allowlist",
        get_tools_fn=get_tools_fn,
    )


def _get_scheduled_octo_control_tools(
    octo: Any, chat_id: int, *, get_tools_fn: GetToolsFn | None = None
) -> tuple[list[ToolSpec], dict[str, object]]:
    return _get_control_plane_tools(
        octo,
        chat_id,
        allowed_tool_names=_SCHEDULED_OCTO_CONTROL_ALLOWED_TOOL_NAMES,
        policy_label="octo.scheduler_octo_control_allowlist",
        get_tools_fn=get_tools_fn,
    )


def _get_internal_maintenance_tools(
    octo: Any, chat_id: int, *, get_tools_fn: GetToolsFn | None = None
) -> tuple[list[ToolSpec], dict[str, object]]:
    return _get_control_plane_tools(
        octo,
        chat_id,
        allowed_tool_names=_INTERNAL_MAINTENANCE_ALLOWED_TOOL_NAMES,
        policy_label="octo.internal_maintenance_allowlist",
        get_tools_fn=get_tools_fn,
    )


def _get_control_plane_tools(
    octo: Any,
    chat_id: int,
    *,
    allowed_tool_names: set[str],
    policy_label: str,
    get_tools_fn: GetToolsFn | None = None,
) -> tuple[list[ToolSpec], dict[str, object]]:
    ctx = {"octo": octo, "chat_id": chat_id, "route_policy_label": policy_label}
    policy_steps = [
        ToolPolicyPipelineStep(
            label=policy_label,
            policy=ToolPolicy(allow=sorted(allowed_tool_names)),
        )
    ]
    resolved_get_tools = _resolve_get_tools(get_tools_fn)
    known_tools = resolved_get_tools(mcp_manager=None)
    all_tools = _get_static_mode_tool_candidates(
        allowed_tool_names,
        tool_candidates=known_tools,
        get_tools_fn=resolved_get_tools,
    )
    resolution_report = resolve_tool_diagnostics(
        all_tools,
        permissions=_CONTROL_PLANE_PERMISSIONS,
        policy_pipeline_steps=policy_steps,
    )
    tool_specs = list(resolution_report.available_tools)
    tool_specs = _budget_tool_specs(tool_specs, max_count=len(allowed_tool_names))
    ctx["active_tool_specs"] = tool_specs
    ctx["tool_resolution_report"] = resolution_report
    ctx["all_tool_specs"] = all_tools
    ctx["known_tool_specs"] = known_tools
    ctx["mcp_refresh_attempted"] = False
    return tool_specs, ctx


def _get_static_mode_tool_candidates(
    allowed_tool_names: set[str],
    *,
    tool_candidates: list[ToolSpec] | None = None,
    get_tools_fn: GetToolsFn | None = None,
) -> list[ToolSpec]:
    """Return only static tools needed by a bounded route mode.

    Control-plane paths must not hydrate dynamic MCP tools just to discard them
    through an allowlist. Passing no MCP manager keeps these routes cheap and
    avoids reconnecting or injecting the full external tool registry.
    """

    allowed = {str(name).strip().lower() for name in allowed_tool_names if str(name).strip()}
    resolved_get_tools = _resolve_get_tools(get_tools_fn)
    candidates = tool_candidates if tool_candidates is not None else resolved_get_tools(mcp_manager=None)
    return [tool for tool in candidates if str(tool.name).strip().lower() in allowed]


def _ensure_mandatory_octo_tools(
    active_tools: list[ToolSpec], all_tools: list[ToolSpec]
) -> list[ToolSpec]:
    return _ensure_named_tools(active_tools, all_tools, _MANDATORY_OCTO_TOOL_NAMES)


def _ensure_named_tools(
    active_tools: list[ToolSpec], all_tools: list[ToolSpec], names: set[str]
) -> list[ToolSpec]:
    by_name = {str(spec.name): spec for spec in active_tools}
    for spec in all_tools:
        name = str(spec.name)
        if name in names and name not in by_name:
            by_name[name] = spec
    return list(by_name.values())


def _a2a_config_from_octo(octo: Any) -> Any:
    runtime_settings = getattr(getattr(octo, "runtime", None), "settings", None)
    candidate = getattr(runtime_settings, "a2a", None)
    if candidate is not None:
        return candidate
    config_obj = getattr(runtime_settings, "config_obj", None)
    return getattr(config_obj, "a2a", None)


def _a2a_interop_enabled(octo: Any) -> bool:
    config = _a2a_config_from_octo(octo)
    return bool(getattr(config, "enabled", False))


def _tool_priority(spec: ToolSpec) -> tuple[int, str]:
    name = str(getattr(spec, "name", "") or "")
    if name in _PRIORITY_TOOL_NAMES:
        return (0, name)
    if _is_connector_tool(spec):
        return (1, name)
    return (2, name)


def _is_connector_tool(spec: ToolSpec) -> bool:
    metadata = getattr(spec, "metadata", None)
    category = str(getattr(metadata, "category", "") or "").strip().lower()
    if category == "connectors":
        return True

    name = str(getattr(spec, "name", "") or "").strip().lower()
    return name.startswith(("gmail_", "calendar_", "drive_", "connector_"))


def _budget_tool_specs(tool_specs: list[ToolSpec], *, max_count: int) -> list[ToolSpec]:
    if len(tool_specs) <= max_count:
        return tool_specs
    prioritized = sorted(tool_specs, key=_tool_priority)
    always = [
        spec for spec in prioritized if str(getattr(spec, "name", "")) in _ALWAYS_INCLUDE_TOOL_NAMES
    ]

    selected: list[ToolSpec] = list(always)
    selected_names = {str(getattr(spec, "name", "")) for spec in selected}
    remaining_budget = max_count - len(selected)

    if remaining_budget > 0:
        for spec in prioritized:
            name = str(getattr(spec, "name", ""))
            if name in selected_names:
                continue
            selected.append(spec)
            selected_names.add(name)
            if len(selected) >= max_count:
                break

    return selected


def _select_initial_octo_tool_specs(tool_specs: list[ToolSpec]) -> list[ToolSpec]:
    """
    Keep Octo's initial tool payload intentionally small.

    The full registry remains available through tool_catalog_search and
    subsequent expansion, but the first request should stay focused on the
    operational core that Octo needs for orchestration.
    """
    max_tools = _env_int("OCTOPAL_OCTO_MAX_TOOL_COUNT", _DEFAULT_MAX_TOOL_COUNT, minimum=8)
    if not _env_flag("OCTOPAL_OCTO_DEFER_TOOL_LOADING", True):
        return _budget_tool_specs(tool_specs, max_count=max_tools)

    prioritized = sorted(tool_specs, key=_tool_priority)
    selected: list[ToolSpec] = []
    selected_names: set[str] = set()

    for spec in prioritized:
        name = str(getattr(spec, "name", "") or "")
        if name not in _INITIAL_OCTO_TOOL_NAMES:
            continue
        if name in selected_names:
            continue
        selected.append(spec)
        selected_names.add(name)

    if not selected:
        return _budget_tool_specs(tool_specs, max_count=max_tools)

    initial_limit = _env_int(
        "OCTOPAL_OCTO_MAX_INITIAL_TOOL_COUNT",
        max(_DEFAULT_INITIAL_OCTO_TOOL_COUNT, len(_ALWAYS_INCLUDE_TOOL_NAMES)),
        minimum=8,
    )
    initial_limit = min(initial_limit, max_tools)
    return _budget_tool_specs(selected, max_count=initial_limit)


def _shrink_tool_specs_for_retry(tool_specs: list[ToolSpec]) -> list[ToolSpec]:
    if len(tool_specs) <= _MIN_TOOL_COUNT_ON_OVERFLOW:
        return tool_specs
    reduced = max(_MIN_TOOL_COUNT_ON_OVERFLOW, int(len(tool_specs) * 0.7))
    return _budget_tool_specs(tool_specs, max_count=reduced)


def _expand_active_tool_specs_from_catalog_result(
    tool_result: Any,
    *,
    active_tool_specs: list[ToolSpec],
    ctx: dict[str, object],
) -> tuple[list[ToolSpec], list[str]]:
    payload = tool_result if isinstance(tool_result, dict) else {}
    if isinstance(tool_result, str):
        try:
            parsed = json.loads(tool_result)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            payload = parsed
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return active_tool_specs, []
    query = _normalize_catalog_query(payload.get("query")) if isinstance(payload, dict) else ""

    all_specs = list(ctx.get("all_tool_specs") or [])
    by_name = {str(getattr(spec, "name", "") or ""): spec for spec in all_specs}
    selected = list(active_tool_specs)
    selected_names = {str(getattr(spec, "name", "") or "") for spec in selected}

    expanded_names: list[str] = []
    mcp_added = 0
    for item in results:
        if len(expanded_names) >= _CATALOG_TOOL_EXPANSION_LIMIT:
            break
        if not isinstance(item, dict):
            continue
        if bool(item.get("active_now")):
            continue
        name = str(item.get("name", "") or "").strip()
        if not name or name in selected_names:
            continue
        spec = by_name.get(name)
        if spec is None:
            continue
        is_mcp = _is_mcp_catalog_item(item, spec)
        if is_mcp:
            if mcp_added >= _CATALOG_MCP_TOOL_EXPANSION_LIMIT:
                continue
            if not _should_expand_mcp_catalog_item(item, spec=spec, query=query):
                continue
            mcp_added += 1
            spec = _hydrate_mcp_tool_spec_for_activation(spec, ctx)
        selected.append(spec)
        selected_names.add(name)
        expanded_names.append(name)

    if expanded_names:
        ctx["active_tool_specs"] = selected
    return selected, expanded_names


def _normalize_catalog_query(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return re.sub(r"\s+", " ", normalized)


def _is_mcp_catalog_item(item: dict[str, Any], spec: ToolSpec) -> bool:
    if bool(item.get("is_mcp")):
        return True
    if str(item.get("owner", "") or "").strip().lower() == "mcp":
        return True
    metadata = getattr(spec, "metadata", None)
    if str(getattr(metadata, "owner", "") or "").strip().lower() == "mcp":
        return True
    if str(getattr(metadata, "category", "") or "").strip().lower() == "mcp":
        return True
    return str(getattr(spec, "name", "") or "").strip().lower().startswith("mcp_")


def _should_expand_mcp_catalog_item(
    item: dict[str, Any],
    *,
    spec: ToolSpec,
    query: str,
) -> bool:
    if not query:
        return False

    name = str(item.get("name", "") or getattr(spec, "name", "") or "").strip().lower()
    remote_name = (
        str(item.get("remote_name", "") or getattr(spec, "remote_tool_name", "") or "")
        .strip()
        .lower()
    )
    server_id = (
        str(item.get("server_id", "") or getattr(spec, "server_id", "") or "").strip().lower()
    )
    description = (
        str(item.get("description", "") or getattr(spec, "description", "") or "").strip().lower()
    )
    query_terms = tuple(term for term in re.split(r"[\s_:/-]+", query) if term)
    content_haystacks = tuple(part for part in (name, remote_name, description) if part)

    if query in {name, remote_name}:
        return True
    if remote_name and query.endswith(remote_name):
        return True
    if server_id and query.startswith(f"{server_id} "):
        query_terms = tuple(term for term in query_terms if term != server_id)
    non_server_terms = tuple(term for term in query_terms if term and term != server_id)
    if not non_server_terms:
        return False
    return all(any(term in haystack for haystack in content_haystacks) for term in non_server_terms)


def _hydrate_mcp_tool_spec_for_activation(spec: ToolSpec, ctx: dict[str, object]) -> ToolSpec:
    manager = ctx.get("mcp_manager")
    if manager is None:
        octo = ctx.get("octo")
        manager = getattr(octo, "mcp_manager", None) if octo is not None else None
    hydrate = getattr(manager, "hydrate_tool_spec", None)
    if not callable(hydrate):
        return spec
    try:
        hydrated = hydrate(spec)
    except Exception:
        logger.warning(
            "Failed to hydrate MCP tool spec for activation", tool_name=spec.name, exc_info=True
        )
        return spec
    return hydrated if isinstance(hydrated, ToolSpec) else spec
