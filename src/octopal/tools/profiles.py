from __future__ import annotations

from dataclasses import dataclass

from octopal.tools.registry import ToolPolicy, ToolSpec, apply_tool_policy


@dataclass(frozen=True)
class ToolProfile:
    name: str
    description: str
    policy: ToolPolicy


DEFAULT_TOOL_PROFILES: dict[str, ToolProfile] = {
    "minimal": ToolProfile(
        name="minimal",
        description="Smallest broadly safe tool surface for simple conversational turns.",
        policy=ToolPolicy(
            allow=[
                "search_canon",
                "web_search",
                "web_fetch",
                "markdown_new_fetch",
                "octo_context_health",
            ]
        ),
    ),
    "research": ToolProfile(
        name="research",
        description="Web and memory oriented tools for analysis and synthesis tasks.",
        policy=ToolPolicy(
            allow=[
                "web_search",
                "web_fetch",
                "markdown_new_fetch",
                "fetch_plan_tool",
                "search_canon",
                "manage_canon",
                "run_llm_subtask",
                "list_workers",
                "start_worker",
                "start_workers_parallel",
                "answer_worker_instruction",
                "get_worker_result",
                "send_file_to_user",
            ]
        ),
    ),
    "coding": ToolProfile(
        name="coding",
        description="Workspace mutation, debugging, and implementation oriented tools.",
        policy=ToolPolicy(
            allow=[
                "fs_list",
                "fs_read",
                "fs_write",
                "fs_move",
                "fs_delete",
                "download_file",
                "exec_run",
                "test_run",
                "coverage_report",
                "git_ops",
                "run_llm_subtask",
                "list_workers",
                "start_worker",
                "start_child_worker",
                "start_workers_parallel",
                "answer_worker_instruction",
                "get_worker_result",
                "synthesize_worker_results",
                "send_file_to_user",
            ]
        ),
    ),
    "ops": ToolProfile(
        name="ops",
        description="Operational and maintenance tools for runtime, process, and release work.",
        policy=ToolPolicy(
            allow=[
                "gateway_status",
                "scheduler_status",
                "worker_session_status",
                "worker_yield",
                "answer_worker_instruction",
                "mcp_discover",
                "service_health",
                "service_logs",
                "process_inspect",
                "self_control",
                "octo_restart_self",
                "octo_check_update",
                "octo_update_self",
                "config_audit",
                "secret_scan",
                "db_backup",
                "db_restore",
                "db_maintenance",
                "db_query_readonly",
                "docker_compose_control",
                "release_snapshot",
                "rollback_release",
                "artifact_collect",
                "test_run",
                "coverage_report",
                "send_file_to_user",
            ]
        ),
    ),
    "communication": ToolProfile(
        name="communication",
        description="Coordination-oriented tools for worker delegation and canon updates.",
        policy=ToolPolicy(
            allow=[
                "search_canon",
                "manage_canon",
                "list_workers",
                "start_worker",
                "start_workers_parallel",
                "get_worker_status",
                "list_active_workers",
                "answer_worker_instruction",
                "get_worker_result",
                "synthesize_worker_results",
                "propose_knowledge",
                "a2a_list_peers",
                "a2a_send_message",
                "send_file_to_user",
            ]
        ),
    ),
}


def get_tool_profile(name: str) -> ToolProfile | None:
    return DEFAULT_TOOL_PROFILES.get(str(name).strip().lower())


def apply_tool_profile(tools: list[ToolSpec], profile_name: str | None) -> list[ToolSpec]:
    if not profile_name:
        return list(tools)
    profile = get_tool_profile(profile_name)
    if profile is None:
        return list(tools)
    return apply_tool_policy(tools, profile.policy)
