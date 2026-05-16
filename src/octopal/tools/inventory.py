from __future__ import annotations

from dataclasses import replace

from octopal.tools.metadata import ToolMetadata
from octopal.tools.registry import ToolSpec

_TOOL_METADATA_BY_NAME: dict[str, ToolMetadata] = {
    "a2a_list_peers": ToolMetadata(
        category="communication",
        profile_tags=("communication", "minimal"),
        capabilities=("agent_peer_read",),
    ),
    "a2a_send_message": ToolMetadata(
        category="communication",
        risk="guarded",
        profile_tags=("communication",),
        capabilities=("agent_peer_message", "network_fetch"),
    ),
    "send_file_to_user": ToolMetadata(
        category="communication",
        risk="guarded",
        profile_tags=("communication", "research", "coding", "ops"),
        capabilities=("user_delivery", "artifact_delivery"),
    ),
    "fs_list": ToolMetadata(
        category="filesystem",
        profile_tags=("coding", "ops"),
        capabilities=("read_workspace", "list_workspace"),
    ),
    "fs_read": ToolMetadata(
        category="filesystem",
        profile_tags=("coding", "research", "ops"),
        capabilities=("read_workspace",),
    ),
    "fs_write": ToolMetadata(
        category="filesystem",
        risk="guarded",
        profile_tags=("coding", "ops"),
        capabilities=("write_workspace",),
    ),
    "fs_move": ToolMetadata(
        category="filesystem",
        risk="guarded",
        profile_tags=("coding", "ops"),
        capabilities=("write_workspace",),
    ),
    "fs_delete": ToolMetadata(
        category="filesystem",
        risk="dangerous",
        profile_tags=("coding", "ops"),
        capabilities=("write_workspace", "delete_workspace"),
    ),
    "download_file": ToolMetadata(
        category="filesystem",
        risk="guarded",
        profile_tags=("coding", "research"),
        capabilities=("network_fetch", "write_workspace"),
    ),
    "web_search": ToolMetadata(
        category="web",
        profile_tags=("research", "minimal"),
        capabilities=("network_fetch", "search"),
    ),
    "web_fetch": ToolMetadata(
        category="web",
        profile_tags=("research",),
        capabilities=("network_fetch", "fetch"),
    ),
    "markdown_new_fetch": ToolMetadata(
        category="web",
        profile_tags=("research", "minimal"),
        capabilities=("network_fetch", "fetch"),
    ),
    "fetch_plan_tool": ToolMetadata(
        category="web",
        profile_tags=("research",),
        capabilities=("network_fetch", "planning"),
    ),
    "browser_open": ToolMetadata(
        category="browser",
        profile_tags=("research", "ops"),
        capabilities=("browser_navigate",),
    ),
    "browser_snapshot": ToolMetadata(
        category="browser",
        profile_tags=("research", "ops"),
        capabilities=("browser_read", "snapshot"),
    ),
    "browser_click": ToolMetadata(
        category="browser",
        risk="guarded",
        profile_tags=("research", "ops"),
        capabilities=("browser_interact",),
    ),
    "browser_type": ToolMetadata(
        category="browser",
        risk="guarded",
        profile_tags=("research", "ops"),
        capabilities=("browser_interact", "browser_write"),
    ),
    "browser_close": ToolMetadata(
        category="browser",
        profile_tags=("research", "ops"),
        capabilities=("browser_manage",),
    ),
    "browser_wait_for": ToolMetadata(
        category="browser",
        profile_tags=("research", "ops"),
        capabilities=("browser_wait",),
    ),
    "browser_extract": ToolMetadata(
        category="browser",
        profile_tags=("research", "ops"),
        capabilities=("browser_read", "extract"),
    ),
    "browser_workflow": ToolMetadata(
        category="browser",
        risk="guarded",
        profile_tags=("research", "ops"),
        capabilities=("browser_navigate", "browser_interact", "browser_read"),
    ),
    "run_llm_subtask": ToolMetadata(
        category="llm",
        profile_tags=("coding", "research"),
        capabilities=("llm_delegate",),
    ),
    "manage_canon": ToolMetadata(
        category="memory",
        risk="guarded",
        profile_tags=("research", "communication"),
        capabilities=("memory_read", "memory_write"),
    ),
    "search_canon": ToolMetadata(
        category="memory",
        profile_tags=("research", "communication", "minimal"),
        capabilities=("memory_read", "search"),
    ),
    "octo_context_health": ToolMetadata(
        category="runtime",
        profile_tags=("minimal", "ops"),
        capabilities=("self_observe",),
    ),
    "octo_context_reset": ToolMetadata(
        category="runtime",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("self_observe", "self_recover"),
    ),
    "octo_opportunity_scan": ToolMetadata(
        category="runtime",
        profile_tags=("ops", "research"),
        capabilities=("self_observe", "planning"),
    ),
    "octo_self_queue_add": ToolMetadata(
        category="runtime",
        risk="guarded",
        profile_tags=("ops", "communication"),
        capabilities=("self_queue_write",),
    ),
    "execute_self_queue_item": ToolMetadata(
        category="runtime",
        risk="guarded",
        profile_tags=("ops", "communication"),
        capabilities=("self_queue_execute", "worker_manage"),
    ),
    "octo_self_queue_list": ToolMetadata(
        category="runtime",
        profile_tags=("ops", "communication"),
        capabilities=("self_queue_read",),
    ),
    "octo_self_queue_take": ToolMetadata(
        category="runtime",
        risk="guarded",
        profile_tags=("ops", "communication"),
        capabilities=("self_queue_write",),
    ),
    "octo_self_queue_update": ToolMetadata(
        category="runtime",
        risk="guarded",
        profile_tags=("ops", "communication"),
        capabilities=("self_queue_write",),
    ),
    "octo_experiment_log": ToolMetadata(
        category="memory",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("memory_write", "self_improve"),
    ),
    "octo_memchain_status": ToolMetadata(
        category="runtime",
        profile_tags=("ops",),
        capabilities=("self_observe", "integrity_read"),
    ),
    "octo_memchain_verify": ToolMetadata(
        category="runtime",
        profile_tags=("ops",),
        capabilities=("self_observe", "integrity_verify"),
    ),
    "octo_memchain_record": ToolMetadata(
        category="runtime",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("integrity_write",),
    ),
    "octo_memchain_init": ToolMetadata(
        category="runtime",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("integrity_write", "bootstrap"),
    ),
    "list_schedule": ToolMetadata(
        category="scheduler",
        profile_tags=("ops",),
        capabilities=("schedule_read",),
    ),
    "check_schedule": ToolMetadata(
        category="scheduler",
        profile_tags=("ops",),
        capabilities=("schedule_read", "schedule_tick"),
    ),
    "scheduler_status": ToolMetadata(
        category="scheduler",
        profile_tags=("ops",),
        capabilities=("schedule_read",),
    ),
    "schedule_task": ToolMetadata(
        category="scheduler",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("schedule_write",),
    ),
    "remove_task": ToolMetadata(
        category="scheduler",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("schedule_write",),
    ),
    "repair_scheduled_tasks": ToolMetadata(
        category="scheduler",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("schedule_write",),
    ),
    "list_workers": ToolMetadata(
        category="workers",
        profile_tags=("communication", "coding", "research"),
        capabilities=("worker_read",),
    ),
    "start_worker": ToolMetadata(
        category="workers",
        risk="guarded",
        profile_tags=("communication", "coding", "research"),
        capabilities=("worker_spawn",),
    ),
    "start_child_worker": ToolMetadata(
        category="workers",
        risk="guarded",
        profile_tags=("coding",),
        capabilities=("worker_spawn", "child_spawn"),
    ),
    "start_workers_parallel": ToolMetadata(
        category="workers",
        risk="guarded",
        profile_tags=("communication", "research", "coding"),
        capabilities=("worker_spawn", "parallel_spawn"),
    ),
    "stop_worker": ToolMetadata(
        category="workers",
        risk="dangerous",
        profile_tags=("ops",),
        capabilities=("worker_control",),
    ),
    "get_worker_status": ToolMetadata(
        category="workers",
        profile_tags=("communication", "ops"),
        capabilities=("worker_read",),
    ),
    "list_active_workers": ToolMetadata(
        category="workers",
        profile_tags=("communication", "ops"),
        capabilities=("worker_read",),
    ),
    "worker_session_status": ToolMetadata(
        category="workers",
        profile_tags=("communication", "ops"),
        capabilities=("worker_read", "worker_observe"),
    ),
    "worker_yield": ToolMetadata(
        category="workers",
        profile_tags=("communication", "ops"),
        capabilities=("worker_read", "worker_observe", "worker_coordination"),
    ),
    "answer_worker_instruction": ToolMetadata(
        category="workers",
        risk="guarded",
        profile_tags=("communication", "coding", "ops"),
        capabilities=("worker_control", "worker_coordination"),
    ),
    "get_worker_result": ToolMetadata(
        category="workers",
        profile_tags=("communication", "coding", "research"),
        capabilities=("worker_read",),
    ),
    "get_worker_output_path": ToolMetadata(
        category="workers",
        profile_tags=("coding", "ops"),
        capabilities=("worker_read", "artifact_lookup"),
    ),
    "synthesize_worker_results": ToolMetadata(
        category="workers",
        profile_tags=("communication", "research"),
        capabilities=("worker_read", "synthesis"),
    ),
    "propose_knowledge": ToolMetadata(
        category="memory",
        risk="guarded",
        profile_tags=("communication",),
        capabilities=("memory_write", "knowledge_proposal"),
    ),
    "exec_run": ToolMetadata(
        category="ops",
        risk="dangerous",
        profile_tags=("coding", "ops"),
        capabilities=("shell_exec",),
    ),
    "service_health": ToolMetadata(
        category="ops",
        profile_tags=("ops",),
        capabilities=("service_read",),
    ),
    "service_logs": ToolMetadata(
        category="ops",
        profile_tags=("ops",),
        capabilities=("service_read", "log_read"),
    ),
    "docker_compose_control": ToolMetadata(
        category="ops",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("service_control", "container_control"),
    ),
    "process_inspect": ToolMetadata(
        category="ops",
        profile_tags=("ops",),
        capabilities=("process_read",),
    ),
    "self_control": ToolMetadata(
        category="ops",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("service_control",),
    ),
    "octo_restart_self": ToolMetadata(
        category="ops",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("service_control", "self_restart"),
    ),
    "octo_check_update": ToolMetadata(
        category="ops",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("service_observe", "update_check"),
    ),
    "octo_update_self": ToolMetadata(
        category="ops",
        risk="dangerous",
        profile_tags=("ops",),
        capabilities=("service_control", "self_update"),
    ),
    "config_audit": ToolMetadata(
        category="ops",
        profile_tags=("ops",),
        capabilities=("config_read", "audit"),
    ),
    "gateway_status": ToolMetadata(
        category="ops",
        profile_tags=("ops", "communication"),
        capabilities=("service_observe", "channel_observe"),
    ),
    "secret_scan": ToolMetadata(
        category="ops",
        profile_tags=("ops",),
        capabilities=("audit", "security_scan"),
    ),
    "git_ops": ToolMetadata(
        category="ops",
        risk="dangerous",
        profile_tags=("coding", "ops"),
        capabilities=("git_read", "git_write"),
    ),
    "test_run": ToolMetadata(
        category="ops",
        risk="guarded",
        profile_tags=("coding", "ops"),
        capabilities=("test_exec",),
    ),
    "coverage_report": ToolMetadata(
        category="ops",
        profile_tags=("coding", "ops"),
        capabilities=("test_read", "reporting"),
    ),
    "artifact_collect": ToolMetadata(
        category="ops",
        profile_tags=("ops",),
        capabilities=("artifact_read",),
    ),
    "db_backup": ToolMetadata(
        category="database",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("database_backup",),
    ),
    "db_restore": ToolMetadata(
        category="database",
        risk="dangerous",
        profile_tags=("ops",),
        capabilities=("database_restore",),
    ),
    "db_maintenance": ToolMetadata(
        category="database",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("database_maintenance",),
    ),
    "db_query_readonly": ToolMetadata(
        category="database",
        profile_tags=("ops",),
        capabilities=("database_read",),
    ),
    "release_snapshot": ToolMetadata(
        category="release",
        risk="guarded",
        profile_tags=("ops",),
        capabilities=("release_snapshot",),
    ),
    "rollback_release": ToolMetadata(
        category="release",
        risk="dangerous",
        profile_tags=("ops",),
        capabilities=("release_rollback",),
    ),
    "create_worker_template": ToolMetadata(
        category="templates",
        risk="guarded",
        profile_tags=("ops", "coding"),
        capabilities=("template_write",),
    ),
    "update_worker_template": ToolMetadata(
        category="templates",
        risk="guarded",
        profile_tags=("ops", "coding"),
        capabilities=("template_write",),
    ),
    "delete_worker_template": ToolMetadata(
        category="templates",
        risk="dangerous",
        profile_tags=("ops",),
        capabilities=("template_delete",),
    ),
    "mcp_connect": ToolMetadata(
        category="mcp",
        risk="guarded",
        capabilities=("mcp_manage",),
        profile_tags=("ops",),
    ),
    "mcp_disconnect": ToolMetadata(
        category="mcp",
        risk="guarded",
        capabilities=("mcp_manage",),
        profile_tags=("ops",),
    ),
    "mcp_list": ToolMetadata(
        category="mcp",
        capabilities=("mcp_read",),
        profile_tags=("ops",),
    ),
    "mcp_status": ToolMetadata(
        category="mcp",
        capabilities=("mcp_read",),
        profile_tags=("ops",),
    ),
    "mcp_discover": ToolMetadata(
        category="mcp",
        capabilities=("mcp_read",),
        profile_tags=("ops", "research"),
    ),
    "mcp_call": ToolMetadata(
        category="mcp",
        risk="guarded",
        capabilities=("mcp_exec",),
        profile_tags=("ops", "research", "coding"),
    ),
    "list_skills": ToolMetadata(
        category="skills",
        capabilities=("skill_read",),
        profile_tags=("ops",),
    ),
    "add_skill": ToolMetadata(
        category="skills",
        risk="guarded",
        capabilities=("skill_write",),
        profile_tags=("ops",),
    ),
    "remove_skill": ToolMetadata(
        category="skills",
        risk="guarded",
        capabilities=("skill_write",),
        profile_tags=("ops",),
    ),
}


def annotate_tool_specs(tools: list[ToolSpec]) -> list[ToolSpec]:
    annotated: list[ToolSpec] = []
    for spec in tools:
        desired = resolve_tool_metadata(spec.name, existing=spec.metadata)
        if desired == spec.metadata:
            annotated.append(spec)
            continue
        annotated.append(replace(spec, metadata=desired))
    return annotated


def resolve_tool_metadata(name: str, *, existing: ToolMetadata | None = None) -> ToolMetadata:
    normalized = str(name).strip().lower()
    if normalized in _TOOL_METADATA_BY_NAME:
        return _TOOL_METADATA_BY_NAME[normalized]
    if normalized.startswith("skill_"):
        return ToolMetadata(
            category="skills",
            owner="workspace",
            risk="guarded",
            profile_tags=("communication", "research", "coding"),
            capabilities=("skill_use",),
        )
    if normalized.startswith("mcp_"):
        return ToolMetadata(
            category="mcp",
            owner="mcp",
            risk="guarded",
            profile_tags=("research", "coding", "ops"),
            capabilities=("mcp_exec",),
        )
    if existing is not None:
        return existing
    return ToolMetadata()
